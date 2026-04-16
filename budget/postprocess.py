"""
budget post-processing — full-dataset QA and deduplication.

Runs ONCE after all countries have been extracted.
Takes results.csv and produces:
  - results_clean.csv    : deduplicated, validated, QA-flagged rows
  - qa_report.csv        : one row per issue found (duplicates, outliers, gaps, etc.)
  - qa_summary.txt       : human-readable summary printed to console

Checks performed:
  1. Exact duplicates           — identical (country, year, section_code, line_description, amount)
  2. Near-duplicates            — same description + year, amount differs < 5% (OCR variation)
  3. Cross-year outliers        — a series value jumps > 5x vs adjacent years
  4. Missing year gaps          — a series present in year N and N+2 but missing N+1
  5. Unit inconsistency         — same series uses "million" some years, "thousand" others
  6. Currency inconsistency     — same country changes currency unexpectedly mid-series
  7. section_total double-count — section_total ≈ sum of its program_total children
  8. Zero / negative amounts    — rows where amount_local <= 0
  9. Low-confidence includes    — decision="include" but confidence < 0.5

No LLM call needed — all checks are deterministic.
The output qa_report.csv can be reviewed in Excel to manually adjudicate edge cases.
"""

from __future__ import annotations

import csv
import logging
import math
import re
from collections import defaultdict
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _float(val) -> Optional[float]:
    try:
        return float(str(val).replace(",", "").strip()) if val not in (None, "", "None") else None
    except (ValueError, TypeError):
        return None


def _to_millions(amount: Optional[float], unit: str) -> Optional[float]:
    """Normalise amount to millions for comparison purposes."""
    if amount is None:
        return None
    _MAP = {
        "billion": 1000.0, "milliard": 1000.0,
        "million": 1.0, "millions": 1.0, "m": 1.0,
        "thousand": 0.001, "thousands": 0.001, "k": 0.001,
        "as_printed": 1.0, "dollar": 1.0, "kr": 1.0, "": 1.0,
    }
    mult = _MAP.get(unit.strip().lower(), 1.0)
    return round(amount * mult, 6)


def _norm_desc(text: str) -> str:
    """Lowercase, collapse whitespace, strip punctuation — for duplicate matching."""
    text = text.lower().strip()
    text = re.sub(r"[^\w\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


# ---------------------------------------------------------------------------
# Issue types
# ---------------------------------------------------------------------------

ISSUE_TYPES = {
    "exact_duplicate":      "Exact duplicate row (same country/year/description/amount)",
    "near_duplicate":       "Near-duplicate: same description & year, amount differs < 5%",
    "cross_year_outlier":   "Cross-year spike: value > 5x compared to adjacent years",
    "missing_year_gap":     "Series gap: present in year N and N+2 but missing N+1",
    "unit_inconsistency":   "Unit changes mid-series (e.g. million vs thousand)",
    "currency_inconsistency": "Currency changes unexpectedly within country-series",
    "double_count":         "section_total ≈ sum of program_total children (double-count risk)",
    "zero_or_negative":     "amount_local is zero or negative",
    "low_confidence_include": "decision=include but confidence < 0.5",
}

QA_COLUMNS = [
    "issue_type", "severity",
    "country", "year", "series_id", "section_code",
    "line_description_en", "amount_local", "unit", "currency",
    "decision", "confidence",
    "detail", "source_file", "row_index",
]


def _issue(issue_type: str, row: dict, detail: str = "", row_index: int = 0) -> dict:
    severity = "high" if issue_type in (
        "exact_duplicate", "double_count", "cross_year_outlier"
    ) else "medium"
    return {
        "issue_type": issue_type,
        "severity": severity,
        "country": row.get("country", ""),
        "year": row.get("year", ""),
        "series_id": row.get("series_id", ""),
        "section_code": row.get("section_code", ""),
        "line_description_en": row.get("line_description_en", "")[:80],
        "amount_local": row.get("amount_local", ""),
        "unit": row.get("unit", ""),
        "currency": row.get("currency", ""),
        "decision": row.get("decision", ""),
        "confidence": row.get("confidence", ""),
        "detail": detail,
        "source_file": row.get("source_file", ""),
        "row_index": row_index,
    }


# ---------------------------------------------------------------------------
# Check 1 & 2: Exact and near duplicates
# ---------------------------------------------------------------------------

def check_duplicates(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Remove exact duplicates. Flag near-duplicates.
    Returns (deduplicated_rows, issues).
    """
    issues: list[dict] = []
    seen_exact: dict[tuple, int] = {}   # key → first row index
    deduplicated: list[dict] = []

    for i, row in enumerate(rows):
        key = (
            row.get("country", ""),
            row.get("year", ""),
            row.get("section_code", ""),
            _norm_desc(row.get("line_description_en") or row.get("line_description", "")),
            row.get("amount_local", ""),
            row.get("unit", ""),
        )
        if key in seen_exact:
            issues.append(_issue("exact_duplicate", row,
                detail=f"duplicate of row {seen_exact[key]}", row_index=i))
            continue  # drop the duplicate
        seen_exact[key] = i
        deduplicated.append(row)

    # Near-duplicate check on the deduplicated set
    # Group by (country, year, normalised_description)
    desc_groups: dict[tuple, list[tuple[int, dict]]] = defaultdict(list)
    for i, row in enumerate(deduplicated):
        desc_key = (
            row.get("country", ""),
            row.get("year", ""),
            _norm_desc(row.get("line_description_en") or row.get("line_description", "")),
        )
        desc_groups[desc_key].append((i, row))

    for group in desc_groups.values():
        if len(group) < 2:
            continue
        amounts = [(_to_millions(_float(r.get("amount_local")), r.get("unit", "")), r, i)
                   for i, r in group]
        amounts = [(a, r, i) for a, r, i in amounts if a is not None and a > 0]
        if len(amounts) < 2:
            continue
        vals = [a for a, _, _ in amounts]
        mean_val = sum(vals) / len(vals)
        for a, row, i in amounts:
            if mean_val > 0 and abs(a - mean_val) / mean_val < 0.05:
                issues.append(_issue("near_duplicate", row,
                    detail=f"amount {a:.3f}M vs group mean {mean_val:.3f}M", row_index=i))

    logger.info(f"  Duplicates: {len(rows) - len(deduplicated)} exact removed, "
                f"{sum(1 for iss in issues if iss['issue_type'] == 'near_duplicate')} near-dups flagged")
    return deduplicated, issues


# ---------------------------------------------------------------------------
# Check 3: Cross-year outliers
# ---------------------------------------------------------------------------

def check_cross_year_outliers(rows: list[dict]) -> list[dict]:
    """Flag rows where a series value jumps > 5x vs the median of adjacent years."""
    issues: list[dict] = []

    # Group by (country, series_id)
    series_groups: dict[tuple, list[dict]] = defaultdict(list)
    for row in rows:
        key = (row.get("country", ""), row.get("series_id", ""))
        series_groups[key].append(row)

    for (country, sid), group in series_groups.items():
        # Sort by year, keep only rows with valid amounts
        year_amounts: list[tuple[int, float, dict]] = []
        for row in group:
            try:
                yr = int(row.get("year", 0))
            except (ValueError, TypeError):
                continue
            amt = _to_millions(_float(row.get("amount_local")), row.get("unit", ""))
            if amt is not None and amt > 0:
                year_amounts.append((yr, amt, row))

        year_amounts.sort(key=lambda x: x[0])
        if len(year_amounts) < 3:
            continue

        for j, (yr, amt, row) in enumerate(year_amounts):
            neighbors = [a for i, (y, a, _) in enumerate(year_amounts)
                         if i != j and abs(y - yr) <= 3]
            if not neighbors:
                continue
            median_neighbors = sorted(neighbors)[len(neighbors) // 2]
            if median_neighbors > 0 and amt / median_neighbors > 5:
                issues.append(_issue("cross_year_outlier", row,
                    detail=f"{amt:.2f}M in {yr} vs median {median_neighbors:.2f}M nearby"))
            elif median_neighbors > 0 and median_neighbors / amt > 5:
                issues.append(_issue("cross_year_outlier", row,
                    detail=f"{amt:.2f}M in {yr} vs median {median_neighbors:.2f}M nearby (sharp drop)"))

    logger.info(f"  Cross-year outliers: {len(issues)} flagged")
    return issues


# ---------------------------------------------------------------------------
# Check 4: Missing year gaps
# ---------------------------------------------------------------------------

def check_missing_year_gaps(rows: list[dict]) -> list[dict]:
    """Flag series present in year N and N+2 but absent in N+1."""
    issues: list[dict] = []

    series_years: dict[tuple, set[int]] = defaultdict(set)
    series_sample: dict[tuple, dict] = {}

    for row in rows:
        key = (row.get("country", ""), row.get("series_id", ""))
        try:
            yr = int(row.get("year", 0))
            series_years[key].add(yr)
            series_sample[key] = row
        except (ValueError, TypeError):
            pass

    for key, years in series_years.items():
        if len(years) < 3:
            continue
        sorted_years = sorted(years)
        for i in range(len(sorted_years) - 2):
            y0, y2 = sorted_years[i], sorted_years[i + 2]
            if y2 - y0 == 2 and (y0 + 1) not in years:
                row = series_sample[key]
                issues.append(_issue("missing_year_gap", row,
                    detail=f"series present {y0} and {y2} but missing {y0+1}"))

    logger.info(f"  Missing year gaps: {len(issues)} flagged")
    return issues


# ---------------------------------------------------------------------------
# Check 5: Unit inconsistency
# ---------------------------------------------------------------------------

def check_unit_inconsistency(rows: list[dict]) -> list[dict]:
    """Flag series where the unit changes across years (e.g. million vs thousand)."""
    issues: list[dict] = []

    _SCALE_UNITS = {"billion", "milliard", "million", "millions", "thousand", "thousands", "k", "m"}

    series_units: dict[tuple, set[str]] = defaultdict(set)
    series_rows: dict[tuple, list[dict]] = defaultdict(list)

    for row in rows:
        key = (row.get("country", ""), row.get("series_id", ""))
        unit = row.get("unit", "").strip().lower()
        if unit in _SCALE_UNITS:
            series_units[key].add(unit)
            series_rows[key].append(row)

    for key, units in series_units.items():
        if len(units) > 1:
            for row in series_rows[key]:
                unit = row.get("unit", "").strip().lower()
                if unit in units:
                    issues.append(_issue("unit_inconsistency", row,
                        detail=f"unit='{unit}' but series also uses {units - {unit}}"))
                    break  # one flag per series is enough

    logger.info(f"  Unit inconsistencies: {len(issues)} series flagged")
    return issues


# ---------------------------------------------------------------------------
# Check 6: Currency inconsistency
# ---------------------------------------------------------------------------

def check_currency_inconsistency(rows: list[dict]) -> list[dict]:
    """Flag country-series where currency changes unexpectedly (not EUR adoption)."""
    issues: list[dict] = []

    # Countries that legitimately switched to EUR
    _EUR_ADOPTERS = {
        "Austria": 1999, "Belgium": 1999, "Finland": 1999, "France": 1999,
        "Germany": 1999, "Ireland": 1999, "Italy": 1999, "Luxembourg": 1999,
        "Netherlands": 1999, "Portugal": 1999, "Spain": 1999,
        "Greece": 2001, "Estonia": 2011,
    }

    series_currencies: dict[tuple, dict[str, list[int]]] = defaultdict(lambda: defaultdict(list))

    for row in rows:
        key = (row.get("country", ""), row.get("series_id", ""))
        currency = row.get("currency", "").upper()
        try:
            yr = int(row.get("year", 0))
        except (ValueError, TypeError):
            continue
        if currency:
            series_currencies[key][currency].append(yr)

    for (country, sid), currency_years in series_currencies.items():
        if len(currency_years) <= 1:
            continue
        # Allow EUR adoption
        if set(currency_years.keys()) == {"EUR", "LOCAL"} or set(currency_years.keys()) <= {"EUR", "USD"}:
            continue
        eur_adoption = _EUR_ADOPTERS.get(country, 9999)
        # Check if it's a legit EUR switch
        currencies = set(currency_years.keys())
        if "EUR" in currencies and len(currencies) == 2:
            other = (currencies - {"EUR"}).pop()
            pre_eur = currency_years.get(other, [])
            post_eur = currency_years.get("EUR", [])
            if pre_eur and post_eur and min(post_eur) >= eur_adoption - 1:
                continue  # legitimate EUR adoption

        # Flag unexpected currency change
        detail = ", ".join(f"{c}: {sorted(ys)}" for c, ys in currency_years.items())
        # Use the first row of this series as the representative row
        sample = next((r for r in rows
                       if r.get("country") == country and r.get("series_id") == sid), {})
        issues.append(_issue("currency_inconsistency", sample,
            detail=f"currencies: {detail}"))

    logger.info(f"  Currency inconsistencies: {len(issues)} flagged")
    return issues


# ---------------------------------------------------------------------------
# Check 7: double-count (section_total ≈ sum of children)
# ---------------------------------------------------------------------------

def check_double_counts(rows: list[dict]) -> list[dict]:
    """Flag section_totals whose amount ≈ sum of their program_total children."""
    issues: list[dict] = []

    groups: dict[tuple, list[dict]] = defaultdict(list)
    for row in rows:
        key = (row.get("country"), row.get("year"), row.get("section_code"))
        groups[key].append(row)

    for group in groups.values():
        section_totals = [r for r in group if r.get("item_type") == "section_total"]
        program_totals = [r for r in group if r.get("item_type") == "program_total"]
        if not section_totals or not program_totals:
            continue

        prog_sum = sum(
            _to_millions(_float(r.get("amount_local")), r.get("unit", "")) or 0
            for r in program_totals
        )

        for st in section_totals:
            st_amt = _to_millions(_float(st.get("amount_local")), st.get("unit", ""))
            if st_amt and st_amt > 0 and prog_sum > 0:
                ratio = abs(st_amt - prog_sum) / st_amt
                if ratio < 0.05:
                    issues.append(_issue("double_count", st,
                        detail=f"section_total={st_amt:.2f}M ≈ sum of {len(program_totals)} program_totals={prog_sum:.2f}M"))

    logger.info(f"  Double-count risks: {len(issues)} flagged")
    return issues


# ---------------------------------------------------------------------------
# Check 8 & 9: Zero/negative amounts and low-confidence includes
# ---------------------------------------------------------------------------

def check_amount_and_confidence(rows: list[dict]) -> list[dict]:
    """Flag zero/negative amounts and low-confidence include rows."""
    issues: list[dict] = []
    for i, row in enumerate(rows):
        amt = _float(row.get("amount_local"))
        if amt is not None and amt <= 0:
            issues.append(_issue("zero_or_negative", row,
                detail=f"amount_local={amt}", row_index=i))
        conf = _float(row.get("confidence"))
        if row.get("decision") == "include" and conf is not None and conf < 0.5:
            issues.append(_issue("low_confidence_include", row,
                detail=f"confidence={conf:.2f}", row_index=i))
    return issues


# ---------------------------------------------------------------------------
# Main post-processing entry point
# ---------------------------------------------------------------------------

def run_postprocess(
    results_csv: Path,
    output_dir: Path,
    include_review: bool = True,
) -> tuple[Path, Path]:
    """
    Run all QA checks on results.csv and write clean output + QA report.

    Args:
        results_csv:    Raw extraction results.
        output_dir:     Where to write results_clean.csv and qa_report.csv.
        include_review: Include 'review' rows (alongside 'include').

    Returns:
        (results_clean_path, qa_report_path)
    """
    if not results_csv.exists():
        raise FileNotFoundError(f"results.csv not found: {results_csv}")

    with open(results_csv, newline="", encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))

    original_count = len(all_rows)
    logger.info(f"Post-processing: {original_count} rows from {results_csv.name}")

    # Filter to include/review
    valid = {"include", "review"} if include_review else {"include"}
    rows = [r for r in all_rows if r.get("decision", "").strip() in valid]
    logger.info(f"  After decision filter: {len(rows)} rows (kept {valid})")

    all_issues: list[dict] = []

    # 1+2: Dedup (modifies rows list — removes exact duplicates)
    rows, dup_issues = check_duplicates(rows)
    all_issues.extend(dup_issues)

    # 3: Cross-year outliers
    all_issues.extend(check_cross_year_outliers(rows))

    # 4: Missing year gaps
    all_issues.extend(check_missing_year_gaps(rows))

    # 5: Unit inconsistency
    all_issues.extend(check_unit_inconsistency(rows))

    # 6: Currency inconsistency
    all_issues.extend(check_currency_inconsistency(rows))

    # 7: Double-count
    all_issues.extend(check_double_counts(rows))

    # 8+9: Amount and confidence
    all_issues.extend(check_amount_and_confidence(rows))

    # Annotate clean rows with qa_flag
    flagged_indices = {iss.get("row_index") for iss in all_issues if iss.get("row_index")}
    issue_map: dict[int, list[str]] = defaultdict(list)
    for iss in all_issues:
        idx = iss.get("row_index")
        if idx is not None:
            issue_map[idx].append(iss["issue_type"])

    for i, row in enumerate(rows):
        row["qa_flags"] = "|".join(issue_map.get(i, [])) or "ok"

    # Write results_clean.csv
    output_dir.mkdir(parents=True, exist_ok=True)
    clean_path = output_dir / "results_clean.csv"
    fieldnames = list(rows[0].keys()) if rows else []
    if "qa_flags" not in fieldnames:
        fieldnames.append("qa_flags")
    with open(clean_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Clean results: {clean_path} ({len(rows)} rows)")

    # Write qa_report.csv
    report_path = output_dir / "qa_report.csv"
    with open(report_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=QA_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_issues)
    logger.info(f"QA report: {report_path} ({len(all_issues)} issues)")

    # Print summary
    _print_summary(all_rows, rows, all_issues, original_count)

    return clean_path, report_path


def _print_summary(all_rows, clean_rows, issues, original_count: int) -> None:
    """Print a human-readable QA summary."""
    by_type: dict[str, int] = defaultdict(int)
    by_severity: dict[str, int] = defaultdict(int)
    for iss in issues:
        by_type[iss["issue_type"]] += 1
        by_severity[iss["severity"]] += 1

    removed = original_count - len(clean_rows)
    countries = sorted({r.get("country", "") for r in clean_rows})
    year_vals = [int(r.get("year", 0)) for r in clean_rows if r.get("year", "").isdigit()]

    lines = [
        "",
        "=" * 60,
        "  BUDGET LLM — POST-PROCESSING QA SUMMARY",
        "=" * 60,
        f"  Input rows:        {original_count}",
        f"  Clean rows:        {len(clean_rows)}  ({removed} removed as duplicates)",
        f"  Countries:         {', '.join(countries)}",
        f"  Year range:        {min(year_vals) if year_vals else '?'} – {max(year_vals) if year_vals else '?'}",
        "",
        "  Issues found:",
    ]
    for issue_type, count in sorted(by_type.items(), key=lambda x: -x[1]):
        desc = ISSUE_TYPES.get(issue_type, issue_type)
        lines.append(f"    {count:4d}  {issue_type:<28}  {desc[:50]}")
    lines += [
        "",
        f"  Severity:  high={by_severity.get('high',0)}  medium={by_severity.get('medium',0)}",
        "",
        "  → Review qa_report.csv in Excel to adjudicate flagged cases.",
        "  → results_clean.csv has a 'qa_flags' column for filtering.",
        "=" * 60,
        "",
    ]
    for line in lines:
        logger.info(line)
