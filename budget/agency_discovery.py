"""
Automatic R&D agency discovery for budget.

After deterministic parsing + entity dedup, raw_rows contains every entity
extracted from every budget table. Many stable R&D agencies are present but
not yet in CANONICAL_AGENCIES — they need to be found and added.

This module:
  1. Find candidate entities: appear in 3+ years, significant amounts
  2. Filter noise deterministically (outcome descriptions, procurement lines, etc.)
  3. Skip entities already covered by existing canonical agency name_variants
  4. Classify remaining candidates via LLM (mini, one call per name, cached)
  5. Save confirmed agencies to Data/output/budget/discovered_agencies.json
  6. Output uncertain cases to discovery_review.csv for human check

The discovered_agencies.json is loaded by canonical_series.build_canonical_series()
and merged with the hardcoded CANONICAL_AGENCIES list automatically.

Usage:
  python -m budget.agency_discovery --country Australia
  python -m budget.agency_discovery --country Australia --dry-run
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from pathlib import Path
from typing import Optional

import pandas as pd

from budget import config as cfg

logger = logging.getLogger(__name__)

__all__ = ["discover_agencies", "load_discovered_agencies"]

DISCOVERED_FILE = cfg.OUTPUT_DIR / "discovered_agencies.json"
REVIEW_CSV = cfg.OUTPUT_DIR / "discovery_review.csv"
DISCOVERY_CACHE_DIR = cfg.LLM_CACHE_DIR / "agency_discovery"

# Thresholds
MIN_YEARS = 1          # must appear in at least this many years
MIN_AVG_AMOUNT = 1000  # thousands — filters tiny line items (< $1M avg)
CONFIDENCE_THRESHOLD = 0.75  # below this → goes to review CSV, not auto-added

# ---------------------------------------------------------------------------
# Noise filters — deterministic, no LLM
# ---------------------------------------------------------------------------

_NOISE_PREFIXES = re.compile(
    r"^(outcome\s+\d|"
    r"for\s+payment\s+to\s+|"
    r"payments?\s+to\s+(corporate\s+entities|entities)\s*:|"
    r"total\s+for\s+|"
    r"total\s+appropriations?\s+for|"
    r"non-operating|"
    r"equity\s+injection|"
    r"administered\s+items?\s+for|"
    r"departmental\s+items?\s+for|"
    r"section\s+\d+|"
    r"\d{2,3}\.\s+|"          # "01. " "265 " style codes
    r"[a-z]\d+\.\s+)",        # "a1. " style codes
    re.IGNORECASE,
)

_NOISE_PATTERNS = re.compile(
    r"(through\s+(provision|funding|support|research)|"
    r"promote\s+growth|"
    r"enable\s+australia|"
    r"ministerial\s+and\s+parliamentary|"
    r"corporate\s+and\s+enabling|"
    r"outcome\s+[0-9]|"
    r"appropriation\s+(act|bill)|"
    r"administered\s+appropriation|"
    r"annual\s+appropriation|"
    r"\$'000)",
    re.IGNORECASE,
)

_PROCUREMENT = re.compile(
    r"(furniture|fittings|motor\s+vehicle|printing|stationery|"
    r"accommodation|fitout|cleaning|security\s+services)",
    re.IGNORECASE,
)

# Sectors that are definitively NOT R&D regardless of how they appear.
# These catch cases where the LLM over-classifies industry reform programmes,
# cultural/arts bodies, financial vehicles, and regulatory authorities as R&D.
_NON_RD_SECTORS = re.compile(
    r"("
    # Industry structure / trade / labour
    r"shipping\s+industry|"
    r"textiles|clothing\s+and\s+footwear|"
    r"steel\s+industry|"
    r"industry\s+reform\s+program|"
    r"structural\s+adjustment|"
    # Law / justice / policing
    r"criminology|"
    r"law\s+reform|"
    r"human\s+rights\s+commission|"
    r"legal\s+aid|"
    r"corrective\s+services|"
    # Arts / culture / sport / film
    r"screen\s+australia|"
    r"film\s+(australia|commission|finance)|"
    r"arts\s+council|"
    r"national\s+gallery|"
    r"national\s+museum|"
    r"sport\s+and\s+recreation|"
    r"australian\s+sports\s+commission|"
    # Finance / investment vehicles (not R&D funding)
    r"reconstruction\s+fund|"
    r"clean\s+energy\s+finance\s+corporation|"
    r"future\s+fund|"
    r"infrastructure\s+fund|"
    # Regulation / standards (not R&D)
    r"pesticides\s+and\s+veterinary|"
    r"therapeutic\s+goods|"
    r"food\s+standards|"
    r"workplace\s+safety|"
    r"building\s+codes|"
    # General statistics (not R&D)
    r"bureau\s+of\s+statistics|"
    r"agricultural\s+and\s+resource\s+economics\b"  # ABARE — statistics not R&D
    r")",
    re.IGNORECASE,
)


def _is_noise(entity: str) -> bool:
    """Return True if entity is an outcome description, procurement line, or non-R&D sector."""
    e = entity.strip()
    # Too long → likely an outcome description
    if len(e) > 120:
        return True
    if _NOISE_PREFIXES.match(e):
        return True
    if _NOISE_PATTERNS.search(e):
        return True
    if _PROCUREMENT.search(e):
        return True
    if _NON_RD_SECTORS.search(e):
        return True
    # Mostly digits / codes
    alpha = sum(c.isalpha() for c in e)
    if alpha < 4:
        return True
    return False


def _purge_noisy_discovered(country: str) -> int:
    """
    Remove any entries from discovered_agencies.json that now fail the
    noise filter. This keeps the JSON clean as filter rules are tightened
    over time — no manual editing of the JSON needed.

    Returns the number of entries removed.
    """
    if not DISCOVERED_FILE.exists():
        return 0

    try:
        data = json.loads(DISCOVERED_FILE.read_text(encoding="utf-8"))
    except Exception:
        return 0

    existing = data.get(country, [])
    clean = [
        a for a in existing
        if not _is_noise(a.get("canonical_name", ""))
        and not _is_noise(a.get("source_entity", ""))
    ]
    removed = len(existing) - len(clean)

    if removed:
        data[country] = clean
        DISCOVERED_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info(
            f"[{country}] Purged {removed} noisy entries from discovered_agencies.json"
        )

    return removed


def _clean_name(entity: str) -> str:
    """Strip common prefixes to get a clean agency name for classification."""
    e = entity.strip()
    # Remove "Total: " / "Total:\t" prefix
    e = re.sub(r"^total\s*[:\t]\s*", "", e, flags=re.IGNORECASE)
    # Remove "FOR PAYMENT TO " prefix
    e = re.sub(r"^for\s+payment\s+to\s+", "", e, flags=re.IGNORECASE)
    # Remove "Payments to corporate entities: "
    e = re.sub(r"^payments?\s+to\s+(?:corporate\s+entities|entities)\s*:\s*", "", e, flags=re.IGNORECASE)
    # Remove leading number codes like "3.— " or "01. "
    e = re.sub(r"^\d+\.?[—\-]\s*", "", e)
    return e.strip()


# ---------------------------------------------------------------------------
# Check against existing canonical agencies
# ---------------------------------------------------------------------------

def _already_covered(entity: str, country: str) -> bool:
    """Return True if entity matches any existing canonical agency's name_variants."""
    from budget.canonical_series import CANONICAL_AGENCIES
    agencies = CANONICAL_AGENCIES.get(country, [])
    entity_lower = entity.lower()
    for agency in agencies:
        for variant in agency["name_variants"]:
            if variant.lower() in entity_lower or entity_lower in variant.lower():
                return True
    return False


def _already_discovered(clean_name: str, country: str) -> bool:
    """Return True if this name is already in discovered_agencies.json."""
    discovered = load_discovered_agencies(country)
    clean_lower = clean_name.lower()
    for agency in discovered:
        # Check canonical name
        if agency.get("canonical_name", "").lower() in clean_lower:
            return True
        # Check name variants
        for variant in agency.get("name_variants", []):
            if variant.lower() in clean_lower or clean_lower in variant.lower():
                return True
    return False


# ---------------------------------------------------------------------------
# LLM classification
# ---------------------------------------------------------------------------

_CLASSIFY_SYSTEM = """\
You are an expert in government R&D budget classification across OECD countries.

Given an entity name extracted from a government budget document, determine whether
this represents R&D-relevant public spending. This could be:
  - A dedicated R&D agency (like CSIRO in Australia, CNRS in France, DFG in Germany)
  - A specific R&D programme or fund within a ministry
  - A research council or grant-making body
  - A line item that is clearly and specifically for research (NOT broad ministry totals)

Note: countries differ in how they budget R&D. Some use dedicated agencies (Australia, UK).
Others channel R&D through programme lines within ministries (Denmark, France, Germany).
Both are valid — classify based on whether this entry represents identifiable R&D spending,
regardless of whether it is a standalone agency or a budget line within a ministry.

Return ONLY a JSON object:
{
  "is_rd_relevant": true | false,
  "confidence": <float 0.0-1.0>,
  "entry_type": "dedicated_rd_agency" | "rd_programme" | "rd_fund" | "mixed_ministry" | "not_rd",
  "canonical_name": "<standard English name>",
  "name_variants": ["<variant1>", "<variant2>"],
  "category": "science_agency" | "direct_rd" | "innovation_instruments" | "higher_education" | "unclear",
  "notes": "<one sentence reason>"
}

Definitions:
  dedicated_rd_agency : exists primarily to conduct or fund R&D.
                        Examples: CSIRO, ARC, NHMRC, DFG, CNRS, Atomenergikommissionen.
                        is_rd_relevant: true. confidence >= 0.85.

  rd_programme        : a specific named R&D scheme, grant programme, or research fund
                        within a government ministry. The name clearly signals R&D.
                        Examples: Cooperative Research Centres Programme,
                        Statens teknisk-videnskabelige Forskningsfond (Danish tech research fund),
                        Fonds National de la Recherche (French national research fund).
                        is_rd_relevant: true. confidence >= 0.75.

  rd_fund             : a line item that represents a payment to or for research,
                        e.g. "Health research including payments to the Medical Research Council".
                        is_rd_relevant: true. confidence >= 0.70.

  mixed_ministry      : a broad government department where R&D is one small component.
                        Examples: Department of Health, Ministry of Defence, Dept of Education.
                        is_rd_relevant: false. (We track specific R&D lines within these, not totals.)

  not_rd              : clearly non-R&D spending.
                        Examples: courts, housing, social transfers, transport, law enforcement.
                        is_rd_relevant: false.

name_variants should include:
  - Common abbreviations
  - Partial names that appear in truncated table cells
  - Translations of non-English names (include both original and English)
  - Historical names if you know this body was renamed

Return ONLY the JSON. No prose before or after.
"""


def _classify_prompt(clean_name: str, country: str, n_years: int,
                     min_amt: float, max_amt: float, currency: str) -> str:
    return (
        f"Country: {country}\n"
        f"Entity name: \"{clean_name}\"\n"
        f"Appears in {n_years} budget years\n"
        f"Amount range: {min_amt:,.0f} – {max_amt:,.0f} {currency} (thousands)"
    )


def _cache_key(clean_name: str, country: str) -> str:
    content = f"{country}|{clean_name.lower()}"
    return hashlib.md5(content.encode()).hexdigest()


def _load_classification_cache(key: str) -> Optional[dict]:
    DISCOVERY_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    p = DISCOVERY_CACHE_DIR / f"{key}.json"
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None


def _save_classification_cache(key: str, data: dict) -> None:
    DISCOVERY_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    p = DISCOVERY_CACHE_DIR / f"{key}.json"
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Load / save discovered agencies
# ---------------------------------------------------------------------------

def load_discovered_agencies(country: str) -> list[dict]:
    """Load auto-discovered agencies for a country from the JSON file."""
    if not DISCOVERED_FILE.exists():
        return []
    try:
        data = json.loads(DISCOVERED_FILE.read_text(encoding="utf-8"))
        return data.get(country, [])
    except Exception:
        return []


def _save_discovered_agencies(country: str, agencies: list[dict]) -> None:
    """Append new agencies to discovered_agencies.json (merges with existing)."""
    if DISCOVERED_FILE.exists():
        try:
            data = json.loads(DISCOVERED_FILE.read_text(encoding="utf-8"))
        except Exception:
            data = {}
    else:
        data = {}

    # Merge: add only truly new ones (by canonical_name)
    existing_names = {a["canonical_name"].lower() for a in data.get(country, [])}
    existing = data.get(country, [])
    added = 0
    for agency in agencies:
        if agency["canonical_name"].lower() not in existing_names:
            existing.append(agency)
            existing_names.add(agency["canonical_name"].lower())
            added += 1

    data[country] = existing
    DISCOVERED_FILE.parent.mkdir(parents=True, exist_ok=True)
    DISCOVERED_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"Saved {added} new discovered agencies for {country} → {DISCOVERED_FILE}")


# ---------------------------------------------------------------------------
# Main discovery function
# ---------------------------------------------------------------------------

def discover_agencies(
    raw_df: pd.DataFrame,
    country: str,
    config: dict,
    min_years: int = MIN_YEARS,
    min_avg_amount: float = MIN_AVG_AMOUNT,
    dry_run: bool = False,
) -> list[dict]:
    """
    Discover new R&D agencies from raw_rows for a country.

    Parameters
    ----------
    raw_df         : raw_rows DataFrame (already deduped)
    country        : country name
    config         : pipeline config (for LLM client)
    min_years      : minimum years an entity must appear in
    min_avg_amount : minimum average amount (in units of the file, usually thousands)
    dry_run        : classify but don't save

    Returns
    -------
    List of newly discovered agency dicts (same format as CANONICAL_AGENCIES entries).
    Also saves to discovered_agencies.json and discovery_review.csv.
    """
    from budget.llm_client import BudgetLLMClient

    # Purge any previously discovered entries that now fail the noise filter.
    # This runs every time so the JSON self-heals as filter rules are improved.
    _purge_noisy_discovered(country)

    currency = cfg.COUNTRY_CONTEXT.get(country, {}).get("currency", "LOCAL")
    aus_df = raw_df[raw_df["country"] == country].copy()
    aus_df["amount_current"] = pd.to_numeric(aus_df["amount_current"], errors="coerce")

    # ── Step 1: find stable, significant entities ─────────────────────────────
    stats = (
        aus_df.dropna(subset=["amount_current"])
        .groupby("entity_raw")
        .agg(
            n_years=("year", "nunique"),
            avg_amount=("amount_current", "mean"),
            min_amount=("amount_current", "min"),
            max_amount=("amount_current", "max"),
        )
        .query(f"n_years >= {min_years} and avg_amount >= {min_avg_amount}")
        .sort_values("avg_amount", ascending=False)
        .reset_index()
    )

    logger.info(f"[{country}] Discovery: {len(stats)} candidates after amount/year filter")

    # ── Step 2: filter noise deterministically ────────────────────────────────
    stats["_noise"] = stats["entity_raw"].apply(_is_noise)
    stats["_clean"] = stats["entity_raw"].apply(_clean_name)
    stats = stats[~stats["_noise"]].copy()

    logger.info(f"[{country}] After noise filter: {len(stats)} candidates")

    if stats.empty:
        logger.info(f"[{country}] No candidates after noise filter — try running on full year range")
        return []

    # ── Step 3: skip already covered ─────────────────────────────────────────
    stats["_covered"] = stats.apply(
        lambda r: _already_covered(r["entity_raw"], country)
                  or _already_covered(r["_clean"], country)
                  or _already_discovered(r["_clean"], country),
        axis=1,
    )
    new_candidates = stats[~stats["_covered"]].copy()

    logger.info(
        f"[{country}] {len(new_candidates)} genuinely new candidates "
        f"(not in existing canonical list or prior discovery runs)"
    )

    if new_candidates.empty:
        logger.info(f"[{country}] No new agencies to discover")
        return []

    if dry_run:
        print(f"\n[{country}] Would classify {len(new_candidates)} candidates:")
        for _, row in new_candidates.head(30).iterrows():
            print(f"  {row['_clean'][:80]}  (years={row['n_years']}, avg={row['avg_amount']:,.0f})")
        if len(new_candidates) > 30:
            print(f"  ... and {len(new_candidates)-30} more")
        return []

    # ── Step 4: classify via LLM (cached per clean name) ─────────────────────
    client = BudgetLLMClient.from_config(config)

    confirmed = []    # confidence >= threshold, is_rd_agency=True
    review = []       # confidence < threshold or agency_type unclear

    for _, row in new_candidates.iterrows():
        clean = row["_clean"]
        key = _cache_key(clean, country)

        result = _load_classification_cache(key)
        if result is None:
            prompt = _classify_prompt(
                clean, country,
                int(row["n_years"]),
                float(row["min_amount"]),
                float(row["max_amount"]),
                currency,
            )
            result = client.call_json(
                system_prompt=_CLASSIFY_SYSTEM,
                user_prompt=prompt,
                max_tokens=512,
                operation=client.OP_OTHER,
            )
            if "_parse_error" not in result:
                _save_classification_cache(key, result)
        else:
            logger.debug(f"Discovery cache hit: {clean[:50]}")

        if "_parse_error" in result:
            logger.warning(f"Discovery parse error for '{clean[:50]}': {result['_parse_error'][:80]}")
            continue

        is_rd = result.get("is_rd_relevant", False)
        confidence = float(result.get("confidence", 0.0))
        entry_type = result.get("entry_type", "not_rd")

        if not is_rd or entry_type in ("mixed_ministry", "not_rd"):
            continue

        agency_entry = {
            "canonical_name": result.get("canonical_name", clean),
            "category": result.get("category", "unclear"),
            "name_variants": result.get("name_variants", [clean.lower()]),
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1900, 2099),
            "notes": result.get("notes", "Auto-discovered"),
            "agency_type": entry_type,
            "confidence": confidence,
            "source_entity": row["entity_raw"],
            "n_years_seen": int(row["n_years"]),
            "avg_amount": float(row["avg_amount"]),
        }

        if confidence >= CONFIDENCE_THRESHOLD:
            confirmed.append(agency_entry)
            logger.info(
                f"[{country}] Discovered: {agency_entry['canonical_name'][:60]} "
                f"({entry_type}, conf={confidence:.2f}, years={row['n_years']})"
            )
        else:
            review.append({**agency_entry, "reason": "confidence below threshold"})
            logger.info(
                f"[{country}] Review needed: {agency_entry['canonical_name'][:60]} "
                f"(conf={confidence:.2f})"
            )

    # ── Step 5: save ─────────────────────────────────────────────────────────
    if confirmed:
        _save_discovered_agencies(country, confirmed)

    # Write review CSV (append)
    all_review = review
    review_path = cfg.OUTPUT_DIR / f"{country.lower().replace(' ','_')}_discovery_review.csv"
    if all_review:
        review_df = pd.DataFrame(all_review)
        review_df["country"] = country
        if review_path.exists():
            existing = pd.read_csv(review_path)
            existing = existing[existing["country"] != country]
            review_df = pd.concat([existing, review_df], ignore_index=True)
        review_df.to_csv(review_path, index=False)
        logger.info(f"Review candidates → {review_path} ({len(all_review)} entries)")

    client.save_usage()

    logger.info(
        f"[{country}] Discovery complete: {len(confirmed)} auto-added, "
        f"{len(review)} need review"
    )
    return confirmed


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import yaml

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")

    parser = argparse.ArgumentParser(description="Discover new R&D agencies from raw_rows")
    parser.add_argument("--country", required=True)
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--dry-run", action="store_true", help="Show candidates without classifying")
    parser.add_argument("--min-years", type=int, default=MIN_YEARS)
    parser.add_argument("--min-amount", type=float, default=MIN_AVG_AMOUNT)
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    raw_df = pd.read_csv(cfg.OUTPUT_DIR / "raw_rows.csv")

    discovered = discover_agencies(
        raw_df=raw_df,
        country=args.country,
        config=config,
        min_years=args.min_years,
        min_avg_amount=args.min_amount,
        dry_run=args.dry_run,
    )

    print(f"\n=== Discovered for {args.country} ===")
    for a in discovered:
        print(f"  {a['canonical_name']} ({a['agency_type']}, conf={a['confidence']:.2f})")

    existing = load_discovered_agencies(args.country)
    print(f"\nTotal in discovered_agencies.json for {args.country}: {len(existing)}")
