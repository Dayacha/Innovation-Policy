from __future__ import annotations

from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
COUNTRY = "New Zealand"
COUNTRY_DIR = ROOT / "Data" / "output" / "budget" / COUNTRY
RESEARCH_DIR = COUNTRY_DIR / "research_ready"


def main() -> None:
    trace = pd.read_csv(COUNTRY_DIR / "new_zealand_series_traceability.csv")
    rollup = pd.read_csv(COUNTRY_DIR / "new_zealand_analytical_rollup.csv")
    gap = pd.read_csv(COUNTRY_DIR / "new_zealand_gap_report.csv")
    rd = pd.read_csv(ROOT / "Data" / "output" / "budget" / "rd_database.csv")
    rd_nz = rd[rd["country"].eq(COUNTRY)].copy()
    inst = pd.read_csv(RESEARCH_DIR / "new_zealand_research_panel_institutional.csv")
    anal = pd.read_csv(RESEARCH_DIR / "new_zealand_research_panel_analytical.csv")
    yearbook = pd.read_csv(RESEARCH_DIR / "new_zealand_research_yearbook.csv")
    catalog = pd.read_csv(RESEARCH_DIR / "new_zealand_research_series_catalog.csv")

    observed_trace = trace[trace["amount_local"].notna()].copy()

    checks = {
        "rd_rows": int(len(rd_nz)),
        "rd_canonicals": int(rd_nz["canonical_name"].nunique()),
        "rd_units": sorted(rd_nz["unit"].dropna().astype(str).unique().tolist()),
        "rd_currencies": sorted(rd_nz["currency"].dropna().astype(str).unique().tolist()),
        "trace_rows": int(len(trace)),
        "trace_observed_rows": int(len(observed_trace)),
        "trace_missing_excerpt": int((observed_trace["trace_excerpt"].fillna("").str.strip() == "").sum()),
        "trace_missing_pdf": int((observed_trace["pdf_path"].fillna("").str.strip() == "").sum()),
        "trace_missing_cache": int((observed_trace["full_text_cache"].fillna("").str.strip() == "").sum()),
        "trace_missing_page": int(observed_trace["page_number"].isna().sum()),
        "rollup_rows": int(len(rollup)),
        "rollup_non_null_years": int(rollup["amount_local"].notna().sum()),
        "rollup_coverage_counts": rollup["coverage_status"].value_counts(dropna=False).to_dict(),
        "gap_counts": gap["gap_type"].value_counts(dropna=False).to_dict(),
        "research_inst_rows": int(len(inst)),
        "research_anal_rows": int(len(anal)),
        "research_yearbook_rows": int(len(yearbook)),
        "research_catalog_rows": int(len(catalog)),
    }

    assert checks["rd_units"] == ["dollar"], checks["rd_units"]
    assert checks["rd_currencies"] == ["NZD"], checks["rd_currencies"]
    assert checks["trace_missing_excerpt"] == 0, checks["trace_missing_excerpt"]
    assert checks["trace_missing_pdf"] == 0, checks["trace_missing_pdf"]
    assert checks["trace_missing_cache"] == 0, checks["trace_missing_cache"]
    assert checks["trace_missing_page"] == 0, checks["trace_missing_page"]
    assert checks["research_anal_rows"] == checks["rollup_rows"], (checks["research_anal_rows"], checks["rollup_rows"])
    assert checks["research_catalog_rows"] == checks["rd_canonicals"], (checks["research_catalog_rows"], checks["rd_canonicals"])

    for key, value in checks.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
