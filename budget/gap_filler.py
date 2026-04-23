"""
Gap filler for budget — closes gaps identified by gap_detector.

After gap_detector produces a reextract_queue, this module tries two passes
to find missing agency amounts before giving up:

  Phase 1 — Targeted DOCX text search (free, deterministic)
    Re-opens each flagged source document and searches ALL text
    (tables + paragraphs) for the missing agency's name variants.
    Uses broader matching than the main parser — no italic filter,
    no header exclusion — because we're looking for a specific agency.

  Phase 2 — LLM targeted extraction (cheap, ~$0.001/gap)
    For gaps still unresolved after Phase 1, extracts the relevant
    text sections from the DOCX and asks the LLM to find the amount.
    Only the sections mentioning the agency are sent (not the full doc),
    keeping cost very low.

After filling, the pipeline re-runs the canonical series + gap report
so the before/after improvement is visible.

Usage (standalone):
  python -m budget.gap_filler --country Australia

Called automatically from compile.py when --fill-gaps flag is set.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Optional

import pandas as pd

from budget import config as cfg
from budget.agency_text_utils import (
    agency_variants,
    extract_snippets_from_text,
    load_gzip_text,
    load_shared_agency_lookup_cache,
    save_shared_agency_lookup_cache,
)
from budget.canonical_series import _get_agencies_for_country

logger = logging.getLogger(__name__)

__all__ = ["fill_gaps"]

# ---------------------------------------------------------------------------
# Regex helpers
# ---------------------------------------------------------------------------

# Matches numbers like 1,234,567 or 1234567 or 1,234 — used to pull amounts
# from raw text. We strip commas before parsing.
_NUM_RE = re.compile(r"\b[\d]{1,3}(?:,\d{3})*(?:\.\d+)?\b|\b\d{4,}\b")

# Plausible amount range for a single R&D agency line in thousands AUD.
# Below $100k or above $50B → almost certainly a page number, date, or typo.
_AMOUNT_MIN = 100        # $100k
_AMOUNT_MAX = 50_000_000 # $50B


def _parse_amount(text: str) -> Optional[float]:
    """Extract the first plausible amount from a text string."""
    for m in _NUM_RE.finditer(text):
        raw = m.group().replace(",", "")
        try:
            val = float(raw)
            if _AMOUNT_MIN <= val <= _AMOUNT_MAX:
                return val
        except ValueError:
            continue
    return None


def _variants_lower(agency: dict) -> list[str]:
    return [v.lower() for v in agency_variants(agency)]


def _load_gap_cache(country: str, year: int, canonical_name: str, source_name: str) -> Optional[dict]:
    return load_shared_agency_lookup_cache(country, source_name, canonical_name)


def _save_gap_cache(country: str, year: int, canonical_name: str, source_name: str, data: dict) -> None:
    save_shared_agency_lookup_cache(country, source_name, canonical_name, data)


# ---------------------------------------------------------------------------
# Phase 1 — Targeted text-cache search
# ---------------------------------------------------------------------------

def _load_text_cache(path: Path) -> str:
    text = load_gzip_text(path)
    if not text:
        logger.warning(f"Could not read text cache {path.name}")
    return text


def _search_text_cache(path: Path, agency: dict, max_hits: int = 3) -> list[dict]:
    """
    Search extracted PDF text for agency name variants and nearby amounts.
    Returns small deterministic hits before any LLM call.
    """
    text = _load_text_cache(path)
    if not text:
        return []

    variants = _variants_lower(agency)
    low = text.lower()
    results = []
    snippets = extract_snippets_from_text(text, variants, max_snippets=max_hits, before=450, after=1200)
    for snip in snippets:
        amt = _parse_amount(snip["text"])
        if amt is None:
            continue
        results.append({
            "entity_raw": agency["canonical_name"],
            "amount_current": amt,
            "context": snip["text"][:500],
            "table_index": -1,
            "row_index": -1,
            "page_number": snip.get("page_number", ""),
            "method": "gap_fill_text_cache",
        })
    return results


# ---------------------------------------------------------------------------
# Phase 2 — LLM targeted extraction
# ---------------------------------------------------------------------------

_LLM_SYSTEM = """You are a government budget analyst. You will be given text
snippets from one government budget document. Your task is to find the budget
appropriation for a specific agency.

Respond ONLY with valid JSON:
{
  "found": true or false,
  "amount": <number in the document's unit, or null if not found>,
  "unit": "thousands" or "dollars" or "unknown",
  "raw_text": "<the exact line you found, or empty string>",
  "confidence": <0.0 to 1.0>
}

If the agency appears with multiple amounts, return the largest only when it
clearly represents the total appropriation rather than a sub-component.
"""


def _llm_extract_from_text_cache(
    country: str,
    cache_path: Path,
    agency: dict,
    config: dict,
    year: int,
) -> Optional[dict]:
    """
    Extract text snippets from a cached PDF text file that mention the agency,
    then ask the LLM to identify the budget amount.

    Returns dict with amount_current and confidence, or None.
    """
    try:
        from budget.llm_client import BudgetLLMClient
    except ImportError:
        logger.error("LLM client not available")
        return None

    variants = _variants_lower(agency)
    canonical = agency["canonical_name"]

    cached = _load_gap_cache(country, year, canonical, cache_path.name)
    if cached is not None:
        if not cached.get("found"):
            return None
        amount = cached.get("amount")
        try:
            amount = float(amount)
        except (TypeError, ValueError):
            return None
        if not (_AMOUNT_MIN <= amount <= _AMOUNT_MAX):
            return None
        return {
            "amount_current": amount,
            "entity_raw": f"{canonical} (LLM extracted)",
            "context": cached.get("raw_text", ""),
            "confidence": float(cached.get("confidence", 0.7)),
            "method": "gap_fill_llm",
            "page_number": cached.get("page_number", ""),
        }

    text = _load_text_cache(cache_path)
    if not text:
        return None

    snippets = [s["text"] for s in extract_snippets_from_text(text, variants, max_snippets=3, before=450, after=1200)]

    if not snippets:
        logger.debug(f"No text found for {canonical} in {cache_path.name}")
        _save_gap_cache(country, year, canonical, cache_path.name, {"found": False})
        return None

    context_text = "\n\n".join(snippets[:5])
    if len(context_text) > 4000:
        context_text = context_text[:4000]

    user_prompt = (
        f"Document: {cache_path.name} (year {year})\n"
        f"Find the budget appropriation for: {canonical}\n"
        f"Also known as: {', '.join(agency.get('name_variants', [])[:5])}\n\n"
        f"Relevant document sections:\n{context_text}"
    )

    try:
        client = BudgetLLMClient(config)
        result = client.call_json(
            system_prompt=_LLM_SYSTEM,
            user_prompt=user_prompt,
            max_tokens=256,
            operation=client.OP_EXTRACT,
        )
    except Exception as e:
        logger.warning(f"LLM call failed for {canonical} in {cache_path.name}: {e}")
        return None

    if "_parse_error" in result:
        logger.warning(f"LLM JSON parse error for {canonical}: {result['_parse_error']}")
        return None

    _save_gap_cache(
        country,
        year,
        canonical,
        cache_path.name,
        {
            "found": bool(result.get("found")),
            "amount": result.get("amount"),
            "raw_text": result.get("raw_text", ""),
            "confidence": float(result.get("confidence", 0.7)),
            "page_number": result.get("page_number", ""),
        },
    )

    if not result.get("found"):
        return None

    amount = result.get("amount")
    if not amount:
        return None

    try:
        amount = float(amount)
    except (TypeError, ValueError):
        return None

    if not (_AMOUNT_MIN <= amount <= _AMOUNT_MAX):
        return None

    return {
        "amount_current": amount,
        "entity_raw": f"{canonical} (LLM extracted)",
        "context": result.get("raw_text", ""),
        "confidence": float(result.get("confidence", 0.7)),
        "method": "gap_fill_llm",
        "page_number": result.get("page_number", ""),
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def fill_gaps(
    gap_df: pd.DataFrame,
    country: str,
    config: dict,
    pdf_root: Path = cfg.PDF_ROOT,
    use_llm: bool = True,
) -> pd.DataFrame:
    """
    For each gap in gap_df with action='reextract', attempt to find the missing
    amount by re-searching the source documents.

    Phase 1: Targeted DOCX text search (free)
    Phase 2: LLM targeted extraction (cheap) — only if use_llm=True and Phase 1 failed

    Returns a DataFrame of newly found rows in raw_rows format, ready to be
    appended to raw_df and fed back into build_canonical_series().

    The gap_df is also updated in-place with the results (action changed to
    'filled' or 'fill_failed').
    """
    reextract = gap_df[gap_df["action"] == "reextract"].copy()
    if reextract.empty:
        logger.info(f"[{country}] No gaps to fill")
        return pd.DataFrame()

    agencies = {a["canonical_name"]: a for a in _get_agencies_for_country(country)}
    new_rows = []
    filled = 0
    failed = 0

    logger.info(
        f"[{country}] Gap filler: {len(reextract)} gaps to process "
        f"(Phase 1: text search, Phase 2: {'LLM' if use_llm else 'disabled'})"
    )

    for (year, canonical), group in reextract.groupby(["year", "canonical_name"]):
        agency = agencies.get(canonical)
        if not agency:
            logger.debug(f"Agency {canonical} not in registry — skipping gap fill")
            continue

        # Find extracted text cache files for this year
        source_files = _find_source_text_files(country, year)
        if not source_files:
            logger.debug(f"No source file found for {country} {year}")
            failed += len(group)
            continue

        found_row = None
        found_source: Optional[Path] = None
        for source_file in source_files:
            # ── Phase 1: targeted text-cache search ─────────────────────────
            text_hits = _search_text_cache(source_file, agency)
            if text_hits:
                best = max(text_hits, key=lambda h: h["amount_current"])
                found_row = best
                found_source = source_file
                logger.info(
                    f"[{country}] Phase 1 found {canonical} {year}: "
                    f"{best['amount_current']:,.0f} in {source_file.name} "
                    f"via {best['method']}"
                )
                break

            # ── Phase 2: LLM extraction on text snippets ───────────────────
            if use_llm:
                llm_result = _llm_extract_from_text_cache(country, source_file, agency, config, year)
                if llm_result:
                    found_row = llm_result
                    found_source = source_file
                    logger.info(
                        f"[{country}] Phase 2 found {canonical} {year}: "
                        f"{llm_result['amount_current']:,.0f} in {source_file.name} "
                        f"(confidence={llm_result['confidence']:.2f})"
                    )
                    break

        if found_row:
            new_rows.append({
                "country": country,
                "year": year,
                "source_file": found_source.stem if found_source is not None else "",
                "table_index": found_row.get("table_index", -1),
                "row_index": found_row.get("row_index", -1),
                "section_name": canonical,
                "entity_raw": found_row["entity_raw"],
                "amount_current": found_row["amount_current"],
                "amount_prior": None,
                "is_header_row": False,
                "is_total_row": False,
                "has_italic_entity": False,
                "cells_raw": found_row.get("context", ""),
                "canonical_name": canonical,
                "unit_note": f"gap_fill ({found_row['method']})",
                "page_number": found_row.get("page_number", ""),
            })
            # Update gap_df
            gap_df.loc[group.index, "action"] = "filled"
            gap_df.loc[group.index, "diagnosis"] = (
                f"Gap filled via {found_row['method']}: "
                f"{found_row['amount_current']:,.0f}"
            )
            filled += 1
        else:
            gap_df.loc[group.index, "action"] = "fill_failed"
            failed += 1

    logger.info(
        f"[{country}] Gap filler complete: {filled} filled, {failed} still missing"
    )

    return pd.DataFrame(new_rows) if new_rows else pd.DataFrame()


def _find_source_text_files(country: str, year: int) -> list[Path]:
    """
    Find extracted text cache files for a given country/year.
    Returns all matching cache files so supplementary acts can also be searched.
    """
    country_dir = Path(cfg.PDF_TEXT_CACHE_DIR) / country
    if not country_dir.exists():
        return []

    year_pat = re.compile(r"(?<![0-9])(" + str(year) + r")(?![0-9])")
    candidates = [p for p in sorted(country_dir.glob("*.txt.gz")) if year_pat.search(p.name)]
    return candidates


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import yaml

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")

    parser = argparse.ArgumentParser(description="Fill gaps in canonical R&D series")
    parser.add_argument("--country", required=True)
    parser.add_argument("--gap-report", help="Path to gap_report CSV (default: auto)")
    parser.add_argument("--no-llm", action="store_true", help="Skip LLM phase")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    cname = args.country.lower().replace(" ", "_")
    gap_path = Path(args.gap_report) if args.gap_report else (
        cfg.OUTPUT_DIR / f"{cname}_gap_report.csv"
    )

    if not gap_path.exists():
        print(f"Gap report not found: {gap_path}")
        print("Run compile first: python -m budget.compile --country ...")
        exit(1)

    gap_df = pd.read_csv(gap_path)
    new_rows_df = fill_gaps(
        gap_df=gap_df,
        country=args.country,
        config=config,
        use_llm=not args.no_llm,
    )

    # Save updated gap report
    gap_df.to_csv(gap_path, index=False)

    n_reextract = len(gap_df[gap_df["action"] == "reextract"])
    n_filled = len(gap_df[gap_df["action"] == "filled"])
    n_failed = len(gap_df[gap_df["action"] == "fill_failed"])

    print(f"\n=== Gap filler results for {args.country} ===")
    print(f"Filled:        {n_filled}")
    print(f"Still missing: {n_failed}")
    print(f"Untouched:     {n_reextract}")

    if not new_rows_df.empty:
        print(f"\nNew rows found ({len(new_rows_df)}):")
        print(new_rows_df[["year", "canonical_name", "amount_current", "unit_note"]].to_string())
        print(f"\nRe-run compile to incorporate these into the series:")
        print(f"  python -m budget.compile --country {args.country} --config {args.config} --no-entity-dedup")
