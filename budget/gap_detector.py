"""
Gap detector and temporal smoother for budget.

After the canonical series is built, some (agency, year) cells are missing.
This module:

  1. DETECT gaps   — find which (country, agency, year) are missing from the series
  2. SEARCH raw    — check if the raw_rows.csv already has a match that was
                     not classified correctly (zero cost fix)
  3. FLAG for re-extraction — if not in raw_rows, flag the source documents
                     for that year so the user can re-run targeted extraction
  4. TEMPORAL CHECK — flag years where an agency's value is implausible given
                     neighbours (IQR-based), suggesting a unit error or wrong row

Output files:
  gap_report.csv       — one row per (country, agency, year) with gap diagnosis
  reextract_queue.csv  — list of (country, year, source_file) to re-extract

Usage:
  python -m budget.gap_detector --country Australia
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from budget import config as cfg
from budget.canonical_series import CANONICAL_AGENCIES, _get_agencies_for_country

logger = logging.getLogger(__name__)

__all__ = ["detect_gaps", "flag_temporal_outliers", "build_gap_report"]

GAP_REPORT_CSV = cfg.OUTPUT_DIR / "gap_report.csv"
REEXTRACT_QUEUE_CSV = cfg.OUTPUT_DIR / "reextract_queue.csv"

GAP_REPORT_COLUMNS = [
    "country", "year", "canonical_name", "category",
    "gap_type",         # "missing" | "outlier" | "ok"
    "diagnosis",        # human-readable explanation
    "raw_row_match",    # entity found in raw_rows.csv?  "yes" | "no" | "partial"
    "raw_row_amount",   # amount found in raw_rows if match exists
    "raw_row_file",     # source file of the raw row match
    "action",           # "reclassify" | "reextract" | "verify" | "none"
    "series_amount",    # current series value (None if gap)
    "prev_amount",      # previous year value (for context)
    "next_amount",      # next year value (for context)
]


# ---------------------------------------------------------------------------
# Step 1 — Detect gaps in canonical series
# ---------------------------------------------------------------------------

def detect_gaps(
    series_df: pd.DataFrame,
    country: str,
) -> pd.DataFrame:
    """
    For each canonical agency and each year where data exists for OTHER agencies,
    flag years where this agency has no value (gap) vs years where it has a value (ok).

    Returns DataFrame with one row per (agency, year) in the expected range.
    """
    agencies = _get_agencies_for_country(country)
    if not agencies:
        logger.warning(f"No canonical agencies defined for {country}")
        return pd.DataFrame()

    # Get all years present in the series for this country
    country_series = series_df[series_df["country"] == country].copy()
    if country_series.empty:
        logger.warning(f"No series data for {country}")
        return pd.DataFrame()

    all_years = sorted(country_series["year"].unique())

    records = []
    for agency in agencies:
        canonical_name = agency["canonical_name"]
        active_start, active_end = agency.get("active_years", (1800, 2099))

        # Aggregate to one value per year (sum across Acts) for gap detection.
        # The detail series may have multiple rows per year (one per source file).
        agency_by_year = (
            country_series[country_series["canonical_name"] == canonical_name]
            .groupby("year")["amount_local"]
            .sum()
        )

        def _get_year_amount(y):
            val = agency_by_year.get(y)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return None
            return float(val)

        active_years = [y for y in all_years if active_start <= y <= active_end]

        for year in active_years:
            amount = _get_year_amount(year)
            has_value = amount is not None

            # Get neighbours for context
            prev_years = [y for y in active_years if y < year and _get_year_amount(y) is not None]
            next_years = [y for y in active_years if y > year and _get_year_amount(y) is not None]
            prev_amount = _get_year_amount(prev_years[-1]) if prev_years else None
            next_amount = _get_year_amount(next_years[0]) if next_years else None

            if not has_value:
                records.append({
                    "country": country,
                    "year": year,
                    "canonical_name": canonical_name,
                    "category": agency["category"],
                    "gap_type": "missing",
                    "diagnosis": "No extracted row found for this agency-year",
                    "raw_row_match": None,
                    "raw_row_amount": None,
                    "raw_row_file": None,
                    "action": "reextract",
                    "series_amount": None,
                    "prev_amount": prev_amount,
                    "next_amount": next_amount,
                })
            else:
                records.append({
                    "country": country,
                    "year": year,
                    "canonical_name": canonical_name,
                    "category": agency["category"],
                    "gap_type": "ok",
                    "diagnosis": "",
                    "raw_row_match": None,
                    "raw_row_amount": None,
                    "raw_row_file": None,
                    "action": "none",
                    "series_amount": amount,
                    "prev_amount": prev_amount,
                    "next_amount": next_amount,
                })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Step 2 — Search raw_rows for matches to fill gaps
# ---------------------------------------------------------------------------

def search_raw_rows_for_gaps(
    gap_df: pd.DataFrame,
    raw_rows_csv: Path,
    country: str,
) -> pd.DataFrame:
    """
    For each 'missing' row in gap_df, search raw_rows.csv for a match.

    Match logic: for each agency's name_variants, check if any raw row
    entity_raw contains that variant (case-insensitive).

    Updates gap_df with raw_row_match, raw_row_amount, raw_row_file, action.
    """
    if not raw_rows_csv.exists():
        logger.warning(f"raw_rows.csv not found: {raw_rows_csv}")
        return gap_df

    raw_df = pd.read_csv(raw_rows_csv)
    raw_country = raw_df[raw_df["country"] == country].copy()
    raw_country["amount_current"] = pd.to_numeric(raw_country["amount_current"], errors="coerce")

    agencies = {a["canonical_name"]: a for a in _get_agencies_for_country(country)}
    gap_df = gap_df.copy()

    for idx, gap_row in gap_df[gap_df["gap_type"] == "missing"].iterrows():
        year = gap_row["year"]
        canonical = gap_row["canonical_name"]
        agency = agencies.get(canonical)
        if not agency:
            continue

        year_raw = raw_country[
            raw_country["year"] == year
        ].dropna(subset=["amount_current"])

        if year_raw.empty:
            gap_df.at[idx, "raw_row_match"] = "no"
            gap_df.at[idx, "diagnosis"] = "Year not in raw_rows — documents may not be parsed yet"
            gap_df.at[idx, "action"] = "reextract"
            continue

        # Try matching name variants
        matched = None
        for variant in agency["name_variants"]:
            v = variant.lower()
            matches = year_raw[
                year_raw["entity_raw"].str.lower().str.contains(v, na=False, regex=False)
            ]
            if not matches.empty:
                # Take the row with the largest amount
                matched = matches.loc[matches["amount_current"].idxmax()]
                break

        if matched is not None:
            gap_df.at[idx, "raw_row_match"] = "yes"
            gap_df.at[idx, "raw_row_amount"] = float(matched["amount_current"])
            gap_df.at[idx, "raw_row_file"] = str(matched.get("source_file", ""))
            gap_df.at[idx, "diagnosis"] = (
                f"Found in raw_rows: '{matched['entity_raw'][:50]}' = "
                f"{float(matched['amount_current']):,.0f} in {matched.get('source_file','?')}"
            )
            gap_df.at[idx, "action"] = "reclassify"
        else:
            # Check if the year's documents exist at all
            year_files = year_raw["source_file"].unique()
            if len(year_files) > 0:
                gap_df.at[idx, "raw_row_match"] = "no"
                gap_df.at[idx, "diagnosis"] = (
                    f"Year has {len(year_files)} parsed files but agency not found. "
                    f"May be in a non-table section or different document."
                )
                gap_df.at[idx, "action"] = "reextract"
            else:
                gap_df.at[idx, "raw_row_match"] = "no"
                gap_df.at[idx, "diagnosis"] = "No parsed documents for this year"
                gap_df.at[idx, "action"] = "reextract"

    return gap_df


# ---------------------------------------------------------------------------
# Step 3 — Temporal outlier detection
# ---------------------------------------------------------------------------

def flag_temporal_outliers(
    gap_df: pd.DataFrame,
    iqr_multiplier: float = 3.0,
    min_years: int = 5,
) -> pd.DataFrame:
    """
    For rows with gap_type='ok', check if the amount is implausible given
    the time series for that agency. Uses IQR-based outlier detection.

    Flags rows as 'outlier' with a diagnosis explaining the range.
    These are candidates for verify pass (check current vs prior year confusion).
    """
    gap_df = gap_df.copy()

    for canonical, agency_df in gap_df[gap_df["gap_type"] == "ok"].groupby("canonical_name"):
        amounts = agency_df["series_amount"].dropna()
        if len(amounts) < min_years:
            continue

        q1, q3 = amounts.quantile(0.25), amounts.quantile(0.75)
        iqr = q3 - q1
        lo = q1 - iqr_multiplier * iqr
        hi = q3 + iqr_multiplier * iqr

        for idx, row in agency_df.iterrows():
            amt = row["series_amount"]
            if amt is None or pd.isna(amt):
                continue
            if amt < lo or amt > hi:
                gap_df.at[idx, "gap_type"] = "outlier"
                gap_df.at[idx, "diagnosis"] = (
                    f"Amount {amt:,.0f} outside expected range "
                    f"[{lo:,.0f} – {hi:,.0f}] (IQR × {iqr_multiplier}). "
                    f"Possible current/prior year confusion or unit error."
                )
                gap_df.at[idx, "action"] = "verify"

    return gap_df


# ---------------------------------------------------------------------------
# Step 4 — Build reextract queue
# ---------------------------------------------------------------------------

def build_reextract_queue(
    gap_df: pd.DataFrame,
    pdf_root: Path = cfg.PDF_ROOT,
) -> pd.DataFrame:
    """
    For gaps marked action='reextract', find the source documents for that
    (country, year) and add them to the reextract queue.
    """
    reextract = gap_df[gap_df["action"] == "reextract"][
        ["country", "year", "canonical_name"]
    ].drop_duplicates(subset=["country", "year"])

    queue_rows = []
    for _, row in reextract.iterrows():
        country = row["country"]
        year = int(row["year"])
        country_dir = pdf_root / country

        if not country_dir.exists():
            continue

        # Find all files for this year
        _YEAR_PAT = re.compile(r"(?<![0-9])(1[89]\d{2}|20[012]\d)(?![0-9])")
        for path in sorted(country_dir.iterdir()):
            m = _YEAR_PAT.search(path.stem)
            if m and int(m.group(1)) == year:
                queue_rows.append({
                    "country": country,
                    "year": year,
                    "source_file": path.name,
                    "file_path": str(path),
                    "missing_agencies": row.get("canonical_name", ""),
                })

    return pd.DataFrame(queue_rows) if queue_rows else pd.DataFrame()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def build_gap_report(
    series_df: pd.DataFrame,
    country: str,
    raw_rows_csv: Path = cfg.OUTPUT_DIR / "raw_rows.csv",
    output_dir: Path = cfg.OUTPUT_DIR,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Full gap detection pipeline for one country.

    Returns (gap_report_df, reextract_queue_df).
    Both are also written to CSV.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: find gaps
    logger.info(f"Gap detection: {country}")
    gap_df = detect_gaps(series_df, country)

    if gap_df.empty:
        logger.warning(f"No gap data for {country}")
        return pd.DataFrame(), pd.DataFrame()

    # Step 2: search raw_rows for existing matches
    gap_df = search_raw_rows_for_gaps(gap_df, raw_rows_csv, country)

    # Step 3: flag temporal outliers
    gap_df = flag_temporal_outliers(gap_df)

    # Step 4: build reextract queue
    queue_df = build_reextract_queue(gap_df)

    # Summary
    n_gaps = len(gap_df[gap_df["gap_type"] == "missing"])
    n_reclassify = len(gap_df[gap_df["action"] == "reclassify"])
    n_reextract = len(gap_df[gap_df["action"] == "reextract"])
    n_outliers = len(gap_df[gap_df["gap_type"] == "outlier"])
    n_ok = len(gap_df[gap_df["gap_type"] == "ok"])

    logger.info(
        f"[{country}] Gap report: {n_ok} ok, {n_gaps} missing "
        f"({n_reclassify} can reclassify, {n_reextract} need reextract), "
        f"{n_outliers} outliers"
    )

    # Save
    gap_path = output_dir / f"{country.lower().replace(' ','_')}_gap_report.csv"
    queue_path = output_dir / f"{country.lower().replace(' ','_')}_reextract_queue.csv"

    gap_df.to_csv(gap_path, index=False)
    if not queue_df.empty:
        queue_df.to_csv(queue_path, index=False)

    logger.info(f"Gap report → {gap_path}")
    if not queue_df.empty:
        logger.info(f"Reextract queue → {queue_path} ({len(queue_df)} files)")

    return gap_df, queue_df


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import yaml

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")

    parser = argparse.ArgumentParser(description="Detect gaps in canonical R&D series")
    parser.add_argument("--country", required=True)
    parser.add_argument("--series", help="Path to canonical series CSV")
    parser.add_argument("--raw-rows", default=str(cfg.OUTPUT_DIR / "raw_rows.csv"))
    args = parser.parse_args()

    series_path = args.series or str(
        cfg.OUTPUT_DIR / f"{args.country.lower().replace(' ','_')}_docx_series.csv"
    )

    series_df = pd.read_csv(series_path)
    gap_df, queue_df = build_gap_report(
        series_df=series_df,
        country=args.country,
        raw_rows_csv=Path(args.raw_rows),
    )

    print(f"\n=== Gap report for {args.country} ===")
    if not gap_df.empty:
        print(gap_df[gap_df["gap_type"] != "ok"][
            ["year", "canonical_name", "gap_type", "action", "diagnosis"]
        ].to_string())

    if not queue_df.empty:
        print(f"\n=== Files to re-extract ({len(queue_df)}) ===")
        print(queue_df[["year", "source_file", "missing_agencies"]].to_string())
