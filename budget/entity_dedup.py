"""
LLM-based semantic entity deduplication for budget.

After deterministic dedup, a residual set of near-duplicates remains:
  "Total: Australian Nuclear Science and"
  "Australian Nuclear Science and Technology Organisation"
  "AUSTRALIAN NUCLEAR SCIENCE AND TECHNOLOGY"

These are all the same entity but differ in prefix, truncation, and case.
Code cannot reliably collapse them — the LLM can.

Design:
  - Input:  small structured list of (entity_name, amount) pairs for one year
  - Task:   group by canonical institution, assign canonical_name
  - Output: entity_raw → canonical_name mapping (JSON, cached per year)
  - Cost:   ~$0.001 per country-year (Haiku, tiny input)
  - Cache:  keyed by hash of input rows → free on rerun, deterministic

Usage:
  from budget.entity_dedup import dedup_entities
  mapping = dedup_entities(df, country="Australia", year=2026, config=config)
  df["canonical_name"] = df["entity_raw"].map(mapping).fillna(df["entity_raw"])
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from budget import config as cfg

logger = logging.getLogger(__name__)

__all__ = ["dedup_entities"]

ENTITY_DEDUP_CACHE_DIR = cfg.LLM_CACHE_DIR / "entity_dedup"

# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

_SYSTEM = """\
You are a government budget analyst. You will receive a list of entity names
extracted from a government budget document. Some names refer to the same
institution but differ in:
  - Prefix ("Total: ", "FOR PAYMENT TO ")
  - Truncation (table cell width limits)
  - Case (ALL CAPS vs Title Case)
  - Language abbreviation (ANSTO vs Australian Nuclear Science and Technology Organisation)

Your task: group names that refer to the SAME institution and assign a single
canonical English name to each group.

Return ONLY a JSON object in this exact format:
{
  "groups": [
    {
      "canonical": "<standard English name for this institution>",
      "matches": ["<name1>", "<name2>", ...]
    }
  ]
}

Rules:
- Every input name must appear in exactly one group's "matches" list
- If a name is unique (no duplicates), it still appears as a group of 1
- canonical name should be the full official English name, not an abbreviation
- Do NOT merge institutions that are genuinely different
- Do NOT invent canonical names not derivable from the input
"""


def _build_prompt(entity_amounts: list[tuple[str, float]], country: str, year: int) -> str:
    lines = [f"Country: {country}, Year: {year}"]
    lines.append("Entity names extracted from budget document:")
    for entity, amount in sorted(entity_amounts, key=lambda x: -x[1]):
        lines.append(f"  - {entity}  (amount: {amount:,.0f})")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

def _cache_key(entity_amounts: list[tuple[str, float]], country: str, year: int) -> str:
    content = f"{country}|{year}|" + "|".join(
        f"{e}:{a:.0f}" for e, a in sorted(entity_amounts)
    )
    return hashlib.md5(content.encode()).hexdigest()


def _load_cache(key: str) -> Optional[dict]:
    ENTITY_DEDUP_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    p = ENTITY_DEDUP_CACHE_DIR / f"{key}.json"
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None


def _save_cache(key: str, data: dict) -> None:
    ENTITY_DEDUP_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    p = ENTITY_DEDUP_CACHE_DIR / f"{key}.json"
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Main function
# ---------------------------------------------------------------------------

def dedup_entities(
    df: pd.DataFrame,
    country: str,
    year: int,
    config: dict,
    cache_dir: Path = ENTITY_DEDUP_CACHE_DIR,
) -> dict[str, str]:
    """
    Semantically group entity names for one (country, year) using an LLM.

    Parameters
    ----------
    df      : DataFrame with columns [entity_raw, amount_current]
              (already deterministically deduped)
    country : country name
    year    : budget year
    config  : pipeline config dict (for LLM client)

    Returns
    -------
    dict mapping entity_raw → canonical_name
    If two rows have the same canonical_name, the compile step will
    keep the one with the largest amount (or lowest Act number).
    """
    from budget.llm_client import BudgetLLMClient

    # Build (entity, amount) pairs — unique entity names only
    year_df = df[(df["country"] == country) & (df["year"] == year)].copy()
    if year_df.empty:
        return {}

    entity_amounts = [
        (str(row["entity_raw"]).strip(), float(row["amount_current"]))
        for _, row in year_df.drop_duplicates("entity_raw").iterrows()
        if str(row["entity_raw"]).strip()
    ]

    if not entity_amounts:
        return {}

    # Check cache
    key = _cache_key(entity_amounts, country, year)
    cached = _load_cache(key)
    if cached is not None:
        logger.debug(f"Entity dedup cache hit: {country} {year}")
        return cached.get("mapping", {})

    # Call LLM (use whatever model is in config — no model switching)
    client = BudgetLLMClient.from_config(config)

    # Send in batches of 50 to avoid truncating large JSON responses
    BATCH_SIZE = 50
    mapping: dict[str, str] = {}
    n_groups_total = 0

    for batch_start in range(0, len(entity_amounts), BATCH_SIZE):
        batch = entity_amounts[batch_start:batch_start + BATCH_SIZE]
        user_prompt = _build_prompt(batch, country, year)

        result = client.call_json(
            system_prompt=_SYSTEM,
            user_prompt=user_prompt,
            max_tokens=4096,
            operation=client.OP_OTHER,
        )

        if "_parse_error" in result:
            logger.warning(
                f"Entity dedup parse error [{country} {year}] batch {batch_start}: "
                f"{result['_parse_error'][:100]}"
            )
            # Fallback: identity mapping for this batch
            for entity, _ in batch:
                mapping[entity] = entity
            continue

        for group in result.get("groups", []):
            canonical = group.get("canonical", "")
            for match in group.get("matches", []):
                mapping[match] = canonical
        n_groups_total += len(result.get("groups", []))

    # Ensure all input entities are covered (LLM may miss some)
    for entity, _ in entity_amounts:
        if entity not in mapping:
            mapping[entity] = entity  # identity fallback

    # Save cache
    _save_cache(key, {"mapping": mapping, "country": country, "year": year})

    n_collapsed = len(entity_amounts) - n_groups_total
    logger.info(
        f"Entity dedup [{country} {year}]: {len(entity_amounts)} names → "
        f"{n_groups_total} canonical entities ({n_collapsed} collapsed)"
    )

    client.save_usage()
    return mapping


def apply_entity_dedup(
    df: pd.DataFrame,
    config: dict,
    countries: Optional[list[str]] = None,
) -> pd.DataFrame:
    """
    Apply entity dedup across all (country, year) combinations in df.
    Adds a 'canonical_name' column.
    """
    df = df.copy()
    df["canonical_name"] = df["entity_raw"].astype(str)

    target_countries = countries or df["country"].unique().tolist()

    for country in target_countries:
        for year in df[df["country"] == country]["year"].unique():
            mapping = dedup_entities(df, country=country, year=int(year), config=config)
            mask = (df["country"] == country) & (df["year"] == year)
            df.loc[mask, "canonical_name"] = (
                df.loc[mask, "entity_raw"].map(mapping).fillna(df.loc[mask, "entity_raw"])
            )

    return df
