"""Audit dedicated country extractors and their output coverage.

This script is intended to support the country-by-country hardening workflow:
1. inventory PDF coverage
2. inspect output coverage in results / budget_items_detected
3. check taxonomy/context completeness for AI validation
4. flag continuity anomalies in yearly totals

Usage:
    python -m budget.country_audit
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
import math
import re

import pandas as pd

from budget.config import BUDGET_ITEMS_FILE, PDF_ROOT, PROCESSED_DIR, RESULTS_CSV_FILE
from budget.dedicated_pipeline import COUNTRY_DEDICATED_EXTRACTORS


AUDIT_DIR = PROCESSED_DIR / "audits"
SUMMARY_CSV = AUDIT_DIR / "country_audit_summary.csv"
YEAR_TOTALS_CSV = AUDIT_DIR / "country_audit_year_totals.csv"
ANOMALIES_CSV = AUDIT_DIR / "country_audit_anomalies.csv"
SUMMARY_JSON = AUDIT_DIR / "country_audit_summary.json"


@dataclass(frozen=True)
class CountryAudit:
    country: str
    pdf_files: int
    pdf_years: int
    min_pdf_year: str
    max_pdf_year: str
    results_rows: int
    results_years: int
    budget_items_rows: int
    budget_items_years: int
    missing_years_vs_pdfs: int
    context_before_pct: float
    context_after_pct: float
    raw_line_pct: float
    merged_line_pct: float
    taxonomy_hits_pct: float
    taxonomy_positive_pct: float
    rationale_pct: float
    ai_ready_pct: float
    low_score_rows: int
    review_rows: int
    include_rows: int
    anomaly_count: int


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    results_df = _read_csv(RESULTS_CSV_FILE)
    items_df = _read_csv(BUDGET_ITEMS_FILE)

    countries = sorted(COUNTRY_DEDICATED_EXTRACTORS)
    inventory = _build_pdf_inventory(countries)
    year_totals_df = _build_year_totals(results_df, items_df, countries)
    anomalies_df = _build_anomalies(year_totals_df)
    summary_df = _build_summary(countries, inventory, results_df, items_df, anomalies_df)

    summary_df.to_csv(SUMMARY_CSV, index=False)
    year_totals_df.to_csv(YEAR_TOTALS_CSV, index=False)
    anomalies_df.to_csv(ANOMALIES_CSV, index=False)
    SUMMARY_JSON.write_text(
        json.dumps(
            {
                "generated_files": {
                    "summary_csv": str(SUMMARY_CSV),
                    "year_totals_csv": str(YEAR_TOTALS_CSV),
                    "anomalies_csv": str(ANOMALIES_CSV),
                },
                "countries": summary_df.to_dict(orient="records"),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print(f"saved {SUMMARY_CSV}")
    print(f"saved {YEAR_TOTALS_CSV}")
    print(f"saved {ANOMALIES_CSV}")
    print(f"saved {SUMMARY_JSON}")


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _build_pdf_inventory(countries: list[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for country in countries:
        country_dir = _country_dir(country)
        files = [p for p in country_dir.iterdir()] if country_dir.exists() else []
        years = sorted({y for p in files if (y := _infer_year(p.name))})
        out[country] = {
            "pdf_files": len(files),
            "pdf_years": len(years),
            "pdf_year_list": years,
            "min_pdf_year": years[0] if years else "",
            "max_pdf_year": years[-1] if years else "",
        }
    return out


def _country_dir(country: str) -> Path:
    if country == "United Kingdom":
        return PDF_ROOT / "UK"
    return PDF_ROOT / country


def _infer_year(name: str) -> str:
    m = re.search(r"(19|20)\d{2}", name)
    return m.group(0) if m else ""


def _build_year_totals(results_df: pd.DataFrame, items_df: pd.DataFrame, countries: list[str]) -> pd.DataFrame:
    rows: list[dict] = []
    for country in countries:
        country_results = _country_rows(results_df, country)
        country_items = _country_rows(items_df, country)
        years = sorted(
            {_normalize_year(y) for y in country_results.get("year", pd.Series(dtype=str)).dropna()}
            | {_normalize_year(y) for y in country_items.get("year", pd.Series(dtype=str)).dropna()}
        )
        years = [y for y in years if y]
        for year in years:
            result_rows = country_results[country_results["year_norm"] == year] if not country_results.empty else country_results
            item_rows = country_items[country_items["year_norm"] == year] if not country_items.empty else country_items
            rows.append(
                {
                    "country": country,
                    "year": year,
                    "results_rows": len(result_rows),
                    "budget_items_rows": len(item_rows),
                    "results_total_amount_local": _safe_sum(result_rows, "amount_local"),
                    "budget_items_total_amount_local": _safe_sum(item_rows, "amount_local"),
                    "median_taxonomy_score": _safe_median(item_rows, "taxonomy_score"),
                    "include_rows": _decision_count(item_rows, "include"),
                    "review_rows": _decision_count(item_rows, "review"),
                }
            )
    return pd.DataFrame(rows).sort_values(["country", "year"]).reset_index(drop=True)


def _build_anomalies(year_totals_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    if year_totals_df.empty:
        return pd.DataFrame(columns=["country", "year", "flag_type", "detail"])

    for country, grp in year_totals_df.groupby("country", sort=True):
        grp = grp.copy()
        grp["year_num"] = pd.to_numeric(grp["year"], errors="coerce")
        grp = grp.sort_values("year_num")
        prev_total = None
        prev_year = None
        for row in grp.itertuples(index=False):
            total = float(getattr(row, "results_total_amount_local") or 0.0)
            year = _normalize_year(row.year)
            if int(getattr(row, "results_rows") or 0) == 0:
                rows.append({"country": country, "year": year, "flag_type": "missing_results_rows", "detail": "PDF year exists but no results rows"})
            if total <= 0 and int(getattr(row, "budget_items_rows") or 0) > 0:
                rows.append({"country": country, "year": year, "flag_type": "zero_total", "detail": "Budget items rows exist but total amount is zero"})
            if prev_total and total > 0:
                ratio = total / prev_total if prev_total else math.nan
                if ratio >= 2.0:
                    rows.append({"country": country, "year": year, "flag_type": "spike_up", "detail": f"year-over-year ratio={ratio:.2f} vs {prev_year}"})
                elif ratio <= 0.5:
                    rows.append({"country": country, "year": year, "flag_type": "spike_down", "detail": f"year-over-year ratio={ratio:.2f} vs {prev_year}"})
            prev_total = total if total > 0 else prev_total
            prev_year = year
    return pd.DataFrame(rows).sort_values(["country", "year", "flag_type"]).reset_index(drop=True) if rows else pd.DataFrame(columns=["country", "year", "flag_type", "detail"])


def _build_summary(
    countries: list[str],
    inventory: dict[str, dict],
    results_df: pd.DataFrame,
    items_df: pd.DataFrame,
    anomalies_df: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict] = []
    for country in countries:
        info = inventory[country]
        country_results = _country_rows(results_df, country)
        country_items = _country_rows(items_df, country)
        result_years = _distinct_years(country_results)
        item_years = _distinct_years(country_items)
        pdf_years = set(info["pdf_year_list"])
        missing_years = sorted(pdf_years - item_years)
        anomaly_count = int((anomalies_df["country"] == country).sum()) if not anomalies_df.empty else 0

        rows.append(
            CountryAudit(
                country=country,
                pdf_files=info["pdf_files"],
                pdf_years=info["pdf_years"],
                min_pdf_year=info["min_pdf_year"],
                max_pdf_year=info["max_pdf_year"],
                results_rows=len(country_results),
                results_years=len(result_years),
                budget_items_rows=len(country_items),
                budget_items_years=len(item_years),
                missing_years_vs_pdfs=len(missing_years),
                context_before_pct=_nonempty_pct(country_items, "context_before"),
                context_after_pct=_nonempty_pct(country_items, "context_after"),
                raw_line_pct=_nonempty_pct(country_items, "raw_line"),
                merged_line_pct=_nonempty_pct(country_items, "merged_line"),
                taxonomy_hits_pct=_nonempty_pct(country_items, "taxonomy_hits"),
                taxonomy_positive_pct=_positive_pct(country_items, "taxonomy_score"),
                rationale_pct=_nonempty_pct(country_items, "rationale"),
                ai_ready_pct=_ai_ready_pct(country_items),
                low_score_rows=int((pd.to_numeric(country_items.get("taxonomy_score"), errors="coerce").fillna(0) < 1).sum()) if not country_items.empty else 0,
                review_rows=_decision_count(country_items, "review"),
                include_rows=_decision_count(country_items, "include"),
                anomaly_count=anomaly_count,
            ).__dict__
        )
    return pd.DataFrame(rows).sort_values(["anomaly_count", "missing_years_vs_pdfs", "country"], ascending=[False, False, True]).reset_index(drop=True)


def _country_rows(df: pd.DataFrame, country: str) -> pd.DataFrame:
    if df.empty or "country" not in df.columns:
        return pd.DataFrame()
    normalized = df.copy()
    normalized["country"] = normalized["country"].fillna("").astype(str)
    if "year" in normalized.columns:
        normalized["year_norm"] = normalized["year"].map(_normalize_year)
    return normalized[normalized["country"] == country].copy()


def _distinct_years(df: pd.DataFrame) -> set[str]:
    if df.empty:
        return set()
    if "year_norm" in df.columns:
        return {y for y in df["year_norm"].dropna().astype(str) if y}
    if "year" in df.columns:
        return {_normalize_year(y) for y in df["year"].dropna()}
    return set()


def _normalize_year(value) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    text = str(value).strip()
    m = re.search(r"(19|20)\d{2}", text)
    return m.group(0) if m else ""


def _safe_sum(df: pd.DataFrame, col: str) -> float:
    if df.empty or col not in df.columns:
        return 0.0
    return float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())


def _safe_median(df: pd.DataFrame, col: str) -> float:
    if df.empty or col not in df.columns:
        return 0.0
    series = pd.to_numeric(df[col], errors="coerce").dropna()
    return float(series.median()) if not series.empty else 0.0


def _decision_count(df: pd.DataFrame, decision: str) -> int:
    if df.empty or "decision" not in df.columns:
        return 0
    return int((df["decision"].fillna("").astype(str) == decision).sum())


def _nonempty_pct(df: pd.DataFrame, col: str) -> float:
    if df.empty or col not in df.columns:
        return 0.0
    series = df[col].fillna("").astype(str).str.strip()
    return round(100.0 * float((series != "").mean()), 2)


def _positive_pct(df: pd.DataFrame, col: str) -> float:
    if df.empty or col not in df.columns:
        return 0.0
    series = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return round(100.0 * float((series > 0).mean()), 2)


def _ai_ready_pct(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    required = ["section_name", "program_description", "line_description", "context_before", "context_after", "raw_line", "taxonomy_hits", "rationale"]
    working = df.copy()
    for col in required:
        if col not in working.columns:
            working[col] = ""
        working[col] = working[col].fillna("").astype(str).str.strip()
    mask = (working[required] != "").all(axis=1)
    return round(100.0 * float(mask.mean()), 2)


if __name__ == "__main__":
    main()
