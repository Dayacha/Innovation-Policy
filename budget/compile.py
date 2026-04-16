"""
Compile phase (Phase 2) for budget — deterministic after Phase 1.

Takes raw rows from docx_table_parser + agency_registry and produces:
  - raw_rows.csv          : every extracted row (full audit trail)
  - <country>_results.csv : classified rows (include/review)
  - <country>_series.csv  : one row per (country, agency, year)
  - <country>_gap_report.csv : gaps + outliers + reextract queue

This phase is almost free to rerun. The only LLM calls are:
  - entity_dedup: ~$0.001 per country-year (Haiku, cached)
  - agency_classifier: ~$0.0001 per unique entity name (Haiku, cached)

Pipeline:
  parse_to_raw_rows()       → docx_table_parser (no LLM)
  deterministic_dedup()     → code only
  apply_entity_dedup()      → LLM Haiku, cached per year
  classify_raw_rows()       → agency_classifier, cached per name
  build_classified_results()→ code only
  build_canonical_series()  → code only (canonical_series.py)
  build_gap_report()        → gap_detector.py

Usage:
  python -m budget.compile --country Australia
  python -m budget.compile --country Australia --years 2020-2026
  python -m budget.compile --country Australia --no-entity-dedup  # skip LLM dedup
"""

from __future__ import annotations

import csv
import logging
import re
from pathlib import Path
from typing import Optional

import pandas as pd

from budget import config as cfg
from budget.docx_table_parser import parse_country_docx_files, RawRow
from budget.agency_classifier import (
    load_registry, save_registry, classify_agencies, REGISTRY_FILE
)
from budget.canonical_series import build_canonical_series, build_totals_series
from budget.entity_dedup import apply_entity_dedup
from budget.gap_detector import build_gap_report
from budget.agency_discovery import discover_agencies
from budget.gap_filler import fill_gaps
from budget.text_cache_parser import parse_text_cache, TEXT_CACHE_DIR

logger = logging.getLogger(__name__)

RAW_ROWS_CSV = cfg.OUTPUT_DIR / "raw_rows.csv"

# ---------------------------------------------------------------------------
# Unit normalisation — convert all amounts to thousands
# ---------------------------------------------------------------------------
#
# Some countries change the denomination they use in budget documents over time.
# To make amounts comparable across all years in the series, we normalise
# everything to the same unit (thousands of local currency).
#
# Rules per country:
#   Australia:
#     - Pre-2000: full AUD dollars  → divide by 1000 → thousands
#     - 2000-2011: mixed (transition era, .doc files mostly missing)
#     - 2012+:     AUD thousands    → no change
#     Detection: if year <= 1999 AND amount_current > 500_000, it's in dollars.
#     (A genuine thousands-denominated amount above 500,000 would be $500M+
#     for a single agency in 1970s-1990s money — implausible for any R&D line.)

_UNIT_RULES: dict[str, list[dict]] = {
    "Australia": [
        {
            "years": (1900, 1999),
            "threshold": 1_000,     # amounts above $1,000 full dollars → divide
            # Was 500,000 but that missed mid-range amounts like $364,500
            # (Anglo-Australian Telescope) which stayed as 364,500 thousands = $364M.
            # For pre-2000 Finance Bills ALL amounts are in full AUD dollars.
            "divisor": 1_000,       # divide by 1000 to get thousands
            "note": "AU pre-2000: dollar→thousand conversion",
        },
    ],
    # Canada: Appropriation Act text files are in full CAD dollars for all years.
    # Divide by 1,000 to get thousands (consistent with Australia 2012+ and series standard).
    "Canada": [
        {
            "years": (1900, 2099),
            "threshold": 1_000,
            "divisor": 1_000,
            "note": "CA text cache: full dollar → thousand conversion",
        },
    ],
    # Add other countries here when needed, e.g.:
    # "Denmark": [{"years": (1970, 1984), "threshold": 1_000_000, "divisor": 1_000, ...}]
}


def _normalise_units(df: pd.DataFrame, country: str) -> pd.DataFrame:
    """
    Normalise amount_current to thousands of local currency.
    Applies country-specific rules from _UNIT_RULES.
    Adds a 'unit_note' column where corrections were made.
    """
    rules = _UNIT_RULES.get(country, [])
    if not rules:
        return df

    df = df.copy()
    df["amount_current"] = pd.to_numeric(df["amount_current"], errors="coerce")
    if "unit_note" not in df.columns:
        df["unit_note"] = ""

    total_fixed = 0
    for rule in rules:
        year_lo, year_hi = rule["years"]
        threshold = rule["threshold"]
        divisor = rule["divisor"]
        note = rule["note"]

        mask = (
            (df["year"] >= year_lo)
            & (df["year"] <= year_hi)
            & (df["amount_current"] > threshold)
        )
        n = mask.sum()
        if n:
            df.loc[mask, "amount_current"] = df.loc[mask, "amount_current"] / divisor
            df.loc[mask, "unit_note"] = note
            total_fixed += n

    if total_fixed:
        logger.info(
            f"[{country}] Unit normalisation: {total_fixed} amounts converted to thousands"
        )

    return df


# ---------------------------------------------------------------------------
# Step 1 — Parse all DOCX files → raw_rows.csv
# ---------------------------------------------------------------------------

RAW_ROW_COLUMNS = [
    "country", "year", "source_file", "table_index", "row_index",
    "section_name", "entity_raw", "amount_current", "amount_prior",
    "is_header_row", "is_total_row", "has_italic_entity", "cells_raw",
]


def parse_to_raw_rows(
    country: str,
    year_range: Optional[tuple[int, int]] = None,
    output_csv: Path = RAW_ROWS_CSV,
) -> pd.DataFrame:
    """
    Parse all source files for a country and write raw_rows.csv.

    Source priority:
      1. DOCX files under Data/input/finance_bills/{country}/
      2. Pre-extracted text cache under Data/output/budget/full_text/{country}/
         (used when the source is PDF and text has already been extracted)

    Returns DataFrame of rows with current-year amounts only.
    """
    rows = parse_country_docx_files(
        pdf_root=cfg.PDF_ROOT,
        country=country,
        year_range=year_range,
    )

    # If DOCX parsing produced nothing (or very little), try the text cache.
    # This covers countries where finance bills are PDFs, not DOCX.
    text_cache_country_dir = TEXT_CACHE_DIR / country
    if text_cache_country_dir.exists():
        docx_years = {r.year for r in rows if r.amount_current is not None}
        cache_rows = parse_text_cache(country=country, year_range=year_range)

        # Only add cache rows for years NOT already covered by DOCX
        new_rows = [r for r in cache_rows if r.year not in docx_years]
        if new_rows:
            logger.info(
                f"[{country}] Text cache added {len(new_rows)} rows "
                f"for years not in DOCX: "
                f"{sorted({r.year for r in new_rows})}"
            )
            rows = rows + new_rows

    # Filter: only rows with a current-year amount and a non-empty entity
    data_rows = [
        r for r in rows
        if r.amount_current is not None
        and r.entity_raw.strip()
        and not r.is_header_row
    ]

    logger.info(
        f"[{country}] {len(rows)} total rows parsed, "
        f"{len(data_rows)} with current-year amounts + entity name"
    )

    df = pd.DataFrame([r.to_dict() for r in data_rows])

    # ── Deduplication ────────────────────────────────────────────────────────
    # Modern DOCX budgets repeat each agency 3× per file:
    #   1. Portfolio summary table — bare entity name
    #   2. Portfolio summary table — outcome/description row (same amount)
    #   3. Individual agency table — "Total: <Agency>" row
    #
    # Strategy: for each (source_file, year, amount_current) group, keep only
    # ONE row, preferring in this order:
    #   a. "Total:" rows (most explicit — entity's own table)
    #   b. Rows where entity_raw matches amount context (not a description row)
    #   c. First occurrence
    #
    # Cross-file dedup (No1 vs No2 supplementary): for each (country, year,
    # entity_clean), keep the row from the lowest Act number.

    import re as _re

    _ACT_NO = _re.compile(r"\bNo\.?\s*(\d+)\b", re.IGNORECASE)

    def _act_num(fname):
        m = _ACT_NO.search(str(fname))
        return int(m.group(1)) if m else 999

    def _clean_entity(text):
        """Strip 'Total:' prefix and normalise for matching."""
        return _re.sub(r"^total[:\s]+", "", str(text), flags=_re.IGNORECASE).strip().lower()

    df["_act_no"] = df["source_file"].apply(_act_num)
    df["_entity_clean"] = df["entity_raw"].apply(_clean_entity)
    df["_is_total_row"] = df["entity_raw"].str.lower().str.startswith("total")

    # Within-file dedup: same (source_file, amount_current) → keep Total: row
    df = (
        df.sort_values(["_is_total_row"], ascending=False)  # Total: rows first
          .drop_duplicates(subset=["source_file", "amount_current", "_entity_clean"], keep="first")
    )

    # Cross-file dedup: same (country, year, entity_clean, amount) → keep lowest Act number
    df = (
        df.sort_values("_act_no")
          .drop_duplicates(subset=["country", "year", "_entity_clean", "amount_current"], keep="first")
    )

    n_after = len(df)
    logger.info(
        f"[{country}] After dedup: {n_after} unique rows "
        f"(removed {len(data_rows) - n_after} duplicates)"
    )

    df = df.drop(columns=["_act_no", "_entity_clean", "_is_total_row"])

    # Write
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    # Append or create
    if output_csv.exists():
        existing = pd.read_csv(output_csv)
        # Remove existing rows for this country (will re-add)
        existing = existing[existing["country"] != country]
        df = pd.concat([existing, df], ignore_index=True)

    df.to_csv(output_csv, index=False)
    logger.info(f"Raw rows written: {output_csv} ({len(df)} total rows)")

    return df[df["country"] == country].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Step 2 — Classify entities via agency_registry
# ---------------------------------------------------------------------------

def classify_raw_rows(
    raw_df: pd.DataFrame,
    config: dict,
    country: str,
    registry_file: Path = REGISTRY_FILE,
    dry_run: bool = False,
) -> pd.DataFrame:
    """
    For each unique entity_raw in raw_df, ensure it has an entry in
    agency_registry.csv. New entities are classified via LLM (once, cached).

    Returns updated registry DataFrame.
    """
    # Build a minimal results_df shaped like what agency_classifier expects
    # It needs: country, section_name_en, line_description_en, item_type
    results_like = pd.DataFrame({
        "country": raw_df["country"],
        "section_name_en": raw_df["section_name"],
        "line_description_en": raw_df["entity_raw"],
        "item_type": raw_df["is_total_row"].map(
            {True: "section_total", False: "line_item"}
        ),
        "decision": "include",  # dummy — classifier ignores this
    })

    registry = classify_agencies(
        results_df=results_like,
        config=config,
        country=country,
        registry_file=registry_file,
        dry_run=dry_run,
    )
    return registry


# ---------------------------------------------------------------------------
# Step 3 — Join raw rows × registry → classified results
# ---------------------------------------------------------------------------

def build_classified_results(
    raw_df: pd.DataFrame,
    registry: pd.DataFrame,
    country: str,
) -> pd.DataFrame:
    """
    Join raw rows against the agency registry to produce a classified
    results DataFrame compatible with canonical_series.build_canonical_series().

    Output columns match OUTPUT_COLUMNS in config.py.
    """
    registry_country = registry[registry["country"] == country].copy()

    # Build lookup: entity_name → registry row
    reg_lookup = {}
    for _, reg_row in registry_country.iterrows():
        reg_lookup[str(reg_row["agency_name"]).strip().lower()] = reg_row

    records = []
    for _, row in raw_df.iterrows():
        entity = str(row["entity_raw"]).strip()
        entity_lower = entity.lower()

        # Look up in registry (exact match first, then partial)
        reg = reg_lookup.get(entity_lower)
        if reg is None:
            # Partial match with two-layer validation:
            #
            # Layer 1 — word overlap against the entity name:
            #   Shared words must cover ≥60% of the longer name's words.
            #   "and technology" (2w) vs "australian nuclear science and technology" (5w)
            #   → 2/5 = 40% → rejected.
            #
            # Layer 2 — surrounding context check:
            #   If Layer 1 passes but the match is still below 80%, check that
            #   words from the registry name that are NOT in the entity also
            #   appear in the row's surrounding context (section_name, cells_raw).
            #   This catches truncated cells: "Australian Nuclear Science and"
            #   matched against "Australian Nuclear Science and Technology Organisation"
            #   — the missing word "Organisation" might not matter, but if
            #   "technology" appears in the section heading we boost confidence.
            context_lower = (
                str(row.get("section_name", "")) + " " +
                str(row.get("cells_raw", ""))
            ).lower()

            best_reg = None
            best_score = 0.0
            for reg_name, reg_row in reg_lookup.items():
                if reg_name not in entity_lower and entity_lower not in reg_name:
                    continue
                entity_words = set(entity_lower.split())
                reg_words = set(reg_name.split())
                shared = entity_words & reg_words
                longer = max(len(entity_words), len(reg_words))
                if longer == 0:
                    continue
                overlap = len(shared) / longer
                if overlap < 0.6:
                    continue

                # Layer 2: for borderline matches (60–80%), check that missing
                # registry words appear in the surrounding row context.
                if overlap < 0.80:
                    missing_words = reg_words - entity_words
                    context_hits = sum(1 for w in missing_words if w in context_lower)
                    # Boost overlap score by context hits
                    overlap += context_hits * 0.1

                if overlap > best_score:
                    best_score = overlap
                    best_reg = reg_row
            reg = best_reg

        if reg is None:
            # Not yet classified → review
            decision = "review"
            rd_category = "unclear"
            confidence = 0.5
            canonical_name = entity
        else:
            agency_type = str(reg.get("agency_type", "unclear"))
            include = str(reg.get("include_in_series", "false")).lower() in ("true", "1", "yes")

            if agency_type == "mixed_ministry":
                # Mixed ministries: totals are review, line items may be include
                decision = "review"
                confidence = 0.6
            elif include or agency_type in ("dedicated_rd", "rd_programme"):
                decision = "include"
                confidence = 0.9
            else:
                decision = "review"
                confidence = 0.5

            rd_category = {
                "dedicated_rd": "science_agency",
                "rd_programme": "direct_rd",
                "mixed_ministry": "unclear",
                "unclear": "unclear",
            }.get(agency_type, "unclear")

            canonical_name = str(reg.get("canonical_name", entity))

        # Detect item type from entity text
        entity_text = str(row["entity_raw"])
        is_total = str(row.get("is_total_row", "")).lower() in ("true", "1")
        item_type = "section_total" if is_total else "line_item"

        records.append({
            "country": row["country"],
            "year": row["year"],
            "source_file": row["source_file"],
            "page_number": str(row.get("table_index", "")),
            "item_type": item_type,
            "section_code": "",
            "section_name": str(row.get("section_name", "")),
            "section_name_en": canonical_name if not is_total else str(row.get("section_name", "")),
            "line_code": "",
            "line_description": entity_text,
            "line_description_en": canonical_name if not is_total else entity_text,
            "amount_local": row["amount_current"],
            "amount_prior": row.get("amount_prior"),
            "unit": "thousand",      # Australian acts: $'000
            "currency": cfg.COUNTRY_CONTEXT.get(country, {}).get("currency", "LOCAL"),
            "rd_category": rd_category,
            "decision": decision,
            "confidence": confidence,
            "llm_model": "docx_parser",
            "extraction_pass": "docx_table",
            "notes": f"Parsed from table {row.get('table_index')}, row {row.get('row_index')}",
        })

    df = pd.DataFrame(records)
    logger.info(
        f"[{country}] Classified {len(df)} rows: "
        f"include={len(df[df['decision']=='include'])}, "
        f"review={len(df[df['decision']=='review'])}"
    )
    return df


# ---------------------------------------------------------------------------
# Full audit database builder
# ---------------------------------------------------------------------------

def _build_full_audit(
    raw_df: pd.DataFrame,
    series_df: pd.DataFrame,
    country: str,
) -> pd.DataFrame:
    """
    Build a full audit database showing every appearance of every canonical agency
    across all documents, years, and Acts.

    For each canonical agency (from hardcoded + discovered registries), searches
    raw_df for all rows matching any of the agency's name_variants. Returns a
    DataFrame sorted by (canonical_name, year, source_file) so you can see the
    full history of e.g. 'Australian Institute of Marine Science' across every
    budget document.

    Columns:
      canonical_name   — standardised agency name
      category         — R&D category from canonical definition
      year             — budget year
      source_file      — filename of the source document
      entity_raw       — exact text as it appeared in the document
      amount_current   — amount extracted (in thousands local currency)
      section_name     — table/section heading in the document
      table_index      — table number within the file
      row_index        — row number within the table
      in_series        — True if this row contributed to the canonical series
    """
    from budget.canonical_series import _get_agencies_for_country

    agencies = _get_agencies_for_country(country)
    if not agencies:
        return pd.DataFrame()

    country_raw = raw_df[raw_df["country"] == country].copy()
    if country_raw.empty:
        return pd.DataFrame()

    # Build a quick set of (canonical_name, year) pairs that made it into the series
    if not series_df.empty:
        in_series_keys = set(
            zip(series_df["canonical_name"], series_df["year"])
        )
    else:
        in_series_keys = set()

    records = []
    for agency in agencies:
        canonical_name = agency["canonical_name"]
        category = agency.get("category", "")
        variants = [v.lower() for v in agency.get("name_variants", [canonical_name])]

        for _, row in country_raw.iterrows():
            entity_lower = str(row["entity_raw"]).lower()
            # Check if any variant matches this row's entity text
            matched = any(v in entity_lower or entity_lower in v for v in variants)
            if not matched:
                continue

            records.append({
                "canonical_name": canonical_name,
                "category": category,
                "year": row["year"],
                "source_file": row.get("source_file", ""),
                "entity_raw": row["entity_raw"],
                "amount_current": row["amount_current"],
                "section_name": row.get("section_name", ""),
                "table_index": row.get("table_index", ""),
                "row_index": row.get("row_index", ""),
                "in_series": (canonical_name, row["year"]) in in_series_keys,
            })

    if not records:
        return pd.DataFrame()

    audit_df = pd.DataFrame(records).sort_values(
        ["canonical_name", "year", "source_file"]
    ).reset_index(drop=True)

    logger.info(
        f"[{country}] Audit database: {len(audit_df)} rows across "
        f"{audit_df['canonical_name'].nunique()} agencies, "
        f"{audit_df['year'].nunique()} years"
    )
    return audit_df


# ---------------------------------------------------------------------------
# Main compile entry point
# ---------------------------------------------------------------------------

def compile_country(
    country: str,
    config: dict,
    year_range: Optional[tuple[int, int]] = None,
    output_dir: Path = cfg.OUTPUT_DIR,
    dry_run: bool = False,
    entity_dedup: bool = True,
    fill_gaps_flag: bool = False,
    fill_gaps_llm: bool = True,
) -> pd.DataFrame:
    """
    Full compile pipeline for one country.

    Steps:
      1. Parse all DOCX files → raw_rows (deterministic, no LLM)
      2. Deterministic dedup (same amount × same file)
      3. LLM entity dedup — collapse truncated/cased name variants (Haiku, cached)
      4. Agency classifier — classify unique canonical names (Haiku, cached)
      5. Build classified results (code only)
      6. Build canonical series (code only)
      7. Gap detection — find missing (agency, year), outliers, reextract queue

    Returns series DataFrame.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Per-country outputs go into a subfolder; shared files stay at root.
    # e.g. Data/output/budget/Australia/australia_docx_series.csv
    country_dir = output_dir / country
    country_dir.mkdir(parents=True, exist_ok=True)

    raw_rows_csv = output_dir / "raw_rows.csv"  # shared across all countries

    logger.info(f"=== COMPILE: {country} ===")

    # ── Step 1+2: parse + deterministic dedup ────────────────────────────────
    raw_df = parse_to_raw_rows(
        country=country,
        year_range=year_range,
        output_csv=raw_rows_csv,
    )

    if raw_df.empty:
        logger.warning(f"No raw rows for {country}")
        return pd.DataFrame()

    # ── Step 2.5: unit normalisation ─────────────────────────────────────────
    # Some countries change their denomination across eras.
    # Australia: pre-2000 acts use full AUD dollars; post-2012 use AUD thousands.
    # We normalise everything to thousands so amounts are comparable across years.
    raw_df = _normalise_units(raw_df, country)

    # ── Step 3: LLM entity dedup ─────────────────────────────────────────────
    if entity_dedup and not dry_run:
        logger.info(f"Entity dedup: {country} ({raw_df['year'].nunique()} years)")
        raw_df = apply_entity_dedup(raw_df, config=config, countries=[country])
    else:
        raw_df["canonical_name"] = raw_df["entity_raw"]

    # After entity dedup: drop rows whose canonical_name duplicates another
    # row in the same (country, year) with the same amount — keep the
    # one with the largest amount (or lowest act number already applied)
    raw_df["_act_no_check"] = raw_df["source_file"].apply(
        lambda f: int(re.search(r"\bNo\.?\s*(\d+)\b", str(f), re.IGNORECASE).group(1))
        if re.search(r"\bNo\.?\s*(\d+)\b", str(f), re.IGNORECASE) else 999
    )
    raw_df = (
        raw_df
        .sort_values(["_act_no_check", "amount_current"], ascending=[True, False])
        .drop_duplicates(subset=["country", "year", "canonical_name", "source_file"], keep="first")
        .drop(columns=["_act_no_check"])
        .reset_index(drop=True)
    )
    logger.info(f"After entity dedup + final dedup: {len(raw_df)} unique rows")

    # ── Step 3.5: agency discovery ────────────────────────────────────────────
    if not dry_run:
        logger.info(f"Agency discovery: {country}")
        discover_agencies(raw_df, country=country, config=config)

    # ── Step 4: agency classifier ─────────────────────────────────────────────
    registry = classify_raw_rows(
        raw_df=raw_df,
        config=config,
        country=country,
        dry_run=dry_run,
    )

    # ── Step 5: build classified results ─────────────────────────────────────
    results_df = build_classified_results(raw_df, registry, country)

    results_path = country_dir / f"{country.lower().replace(' ','_')}_docx_results.csv"
    results_df.to_csv(results_path, index=False)
    logger.info(f"Results → {results_path} ({len(results_df)} rows)")

    # ── Step 6: canonical series ──────────────────────────────────────────────
    series_df = build_canonical_series(results_df, country=country)

    if not series_df.empty:
        cname = country.lower().replace(" ", "_")

        # ── Detail series ─────────────────────────────────────────────────────
        # One row per (agency, year, source_file).
        # Use this to trace any figure back to the exact document and page.
        series_path = country_dir / f"{cname}_docx_series.csv"
        series_df.to_csv(series_path, index=False)
        logger.info(f"Detail series → {series_path}")

        # ── Totals series ─────────────────────────────────────────────────────
        # One row per (agency, year). Sums amounts across Acts with restatement flag.
        # This is the primary output for time-series analysis.
        totals_df = build_totals_series(series_df, country=country)
        if not totals_df.empty:
            totals_path = country_dir / f"{cname}_docx_totals.csv"
            totals_df.to_csv(totals_path, index=False)
            logger.info(f"Totals series → {totals_path}")

        # ── Full audit database ───────────────────────────────────────────────
        # All raw rows that matched a canonical agency, with every appearance
        # across all Acts and all years, sorted by (agency, year, source_file).
        # Shows every time e.g. "Australian Institute of Marine Science" appears
        # across the full history — all amounts, all documents.
        audit_df = _build_full_audit(raw_df, series_df, country)
        if not audit_df.empty:
            audit_path = country_dir / f"{cname}_docx_audit.csv"
            audit_df.to_csv(audit_path, index=False)
            logger.info(f"Full audit database → {audit_path} ({len(audit_df)} rows)")

    # ── Step 7: gap detection ─────────────────────────────────────────────────
    if not series_df.empty:
        gap_df, queue_df = build_gap_report(
            series_df=series_df,
            country=country,
            raw_rows_csv=raw_rows_csv,
            output_dir=country_dir,
        )
        # Print summary
        if not gap_df.empty:
            problems = gap_df[gap_df["gap_type"] != "ok"]
            if not problems.empty:
                logger.info(
                    f"\nGap summary for {country}:\n" +
                    problems[["year","canonical_name","gap_type","action","diagnosis"]]
                    .to_string(index=False)
                )

    # ── Step 8: gap filling ───────────────────────────────────────────────────
    # For gaps still marked 'reextract', try to find the missing amounts by
    # re-searching the source documents. Two phases:
    #   Phase 1: broad text search (free, deterministic)
    #   Phase 2: LLM targeted extraction (cheap, ~$0.001/gap)
    # If new rows are found, rebuild the series + gap report so the final
    # output reflects the filled gaps.
    if fill_gaps_flag and not series_df.empty and not gap_df.empty:
        n_before = len(gap_df[gap_df["gap_type"] == "missing"])
        logger.info(f"[{country}] Gap filling: {n_before} missing agency-years")

        new_rows_df = fill_gaps(
            gap_df=gap_df,
            country=country,
            config=config,
            use_llm=fill_gaps_llm,
        )

        if not new_rows_df.empty:
            logger.info(f"[{country}] Gap filler found {len(new_rows_df)} new rows — rebuilding series")

            # Merge new rows into results and rebuild series
            new_results = build_classified_results(new_rows_df, registry, country)
            results_df_updated = pd.concat([results_df, new_results], ignore_index=True)
            series_df = build_canonical_series(results_df_updated, country=country)

            if not series_df.empty:
                cname = country.lower().replace(" ", "_")
                series_df.to_csv(country_dir / f"{cname}_docx_series.csv", index=False)
                totals_df = build_totals_series(series_df, country=country)
                if not totals_df.empty:
                    totals_df.to_csv(country_dir / f"{cname}_docx_totals.csv", index=False)
                audit_df = _build_full_audit(new_rows_df if new_rows_df is not None else raw_df, series_df, country)
                if not audit_df.empty:
                    audit_df.to_csv(country_dir / f"{cname}_docx_audit.csv", index=False)

                # Re-run gap report to show what's still missing
                gap_df, queue_df = build_gap_report(
                    series_df=series_df,
                    country=country,
                    raw_rows_csv=raw_rows_csv,
                    output_dir=country_dir,
                )
                n_after = len(gap_df[gap_df["gap_type"] == "missing"])
                logger.info(
                    f"[{country}] Gap filling complete: "
                    f"{n_before} → {n_after} missing ({n_before - n_after} closed)"
                )

    # ── Step 9: rebuild combined database ────────────────────────────────────
    build_combined_database(output_dir=output_dir)

    return series_df


# ---------------------------------------------------------------------------
# Combined database — all countries, app-ready
# ---------------------------------------------------------------------------

def build_combined_database(output_dir: Path = cfg.OUTPUT_DIR) -> pd.DataFrame:
    """
    Combine all country series into a single clean database file.

    - One row per (country, canonical_name, year) — primary amount only
    - Gaps (NaN amounts) are excluded
    - Written to output_dir/rd_database.csv

    This is the app-facing output. The per-country detail series files
    preserve the full per-source-file breakdown for audit purposes.
    """
    output_dir = Path(output_dir)
    db_path = output_dir / "rd_database.csv"

    all_series = []
    for country_dir in sorted(output_dir.iterdir()):
        if not country_dir.is_dir():
            continue
        cname = country_dir.name.lower().replace(" ", "_")
        series_path = country_dir / f"{cname}_docx_series.csv"
        if not series_path.exists():
            continue
        df = pd.read_csv(series_path)
        all_series.append(df)

    if not all_series:
        logger.warning("No country series found — combined database not built")
        return pd.DataFrame()

    combined = pd.concat(all_series, ignore_index=True)

    # Drop gap rows (no amount) — keep all source-file rows that have a value
    combined = combined.dropna(subset=["amount_local"])
    combined = combined[combined["amount_local"].notna() & (combined["amount_local"] != 0)]

    combined = combined.sort_values(["country", "canonical_name", "year", "source_file"]).reset_index(drop=True)

    # Clean columns for app use
    keep_cols = [
        "country", "year", "canonical_name", "category",
        "amount_local", "unit", "currency",
        "item_type", "line_description_en", "source_file", "series_notes",
    ]
    keep_cols = [c for c in keep_cols if c in combined.columns]
    combined = combined[keep_cols]

    combined.to_csv(db_path, index=False)
    logger.info(
        f"Combined database → {db_path} "
        f"({len(combined)} rows, {combined['country'].nunique()} countries, "
        f"{combined['year'].nunique()} years)"
    )
    return combined


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import yaml
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")

    parser = argparse.ArgumentParser(description="Compile DOCX budget data (no LLM for extraction)")
    parser.add_argument("--country", help="Country to compile")
    parser.add_argument("--years", help="Year range e.g. 2000-2026")
    parser.add_argument("--dry-run", action="store_true", help="Don't call LLM for classification")
    parser.add_argument("--no-entity-dedup", action="store_true", help="Skip entity dedup (use cached canonical_name)")
    parser.add_argument("--fill-gaps", action="store_true", help="After gap detection, try to fill missing gaps from source documents")
    parser.add_argument("--no-gap-llm", action="store_true", help="Gap filling: use text search only, skip LLM phase")
    parser.add_argument("--build-database", action="store_true", help="Rebuild combined rd_database.csv from all country series (no extraction)")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    # Standalone database rebuild — no extraction needed
    if args.build_database:
        db = build_combined_database()
        if not db.empty:
            print(f"\nCombined database: {len(db)} rows")
            print(db.groupby("country").agg(
                agencies=("canonical_name", "nunique"),
                years=("year", "nunique"),
                min_year=("year", "min"),
                max_year=("year", "max"),
            ).to_string())
        sys.exit(0)

    if not args.country:
        parser.error("--country is required unless --build-database is used")

    with open(args.config) as f:
        config = yaml.safe_load(f)

    year_range = None
    if args.years:
        parts = args.years.split("-")
        if len(parts) == 2:
            year_range = (int(parts[0]), int(parts[1]))

    series = compile_country(
        country=args.country,
        config=config,
        year_range=year_range,
        dry_run=args.dry_run,
        entity_dedup=not args.no_entity_dedup,
        fill_gaps_flag=args.fill_gaps,
        fill_gaps_llm=not args.no_gap_llm,
    )

    if not series.empty:
        print(f"\nSeries summary for {args.country}:")
        print(series.groupby("canonical_name").agg(
            years=("year", "count"),
            min_year=("year", "min"),
            max_year=("year", "max"),
        ).to_string())
