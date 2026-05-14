from __future__ import annotations

from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
COUNTRY = "New Zealand"
COUNTRY_SLUG = "new_zealand"
COUNTRY_DIR = ROOT / "Data" / "output" / "budget" / COUNTRY
RESEARCH_DIR = COUNTRY_DIR / "research_ready"

TRACE_PATH = COUNTRY_DIR / f"{COUNTRY_SLUG}_series_traceability.csv"
ROLLUP_PATH = COUNTRY_DIR / f"{COUNTRY_SLUG}_analytical_rollup.csv"
GAP_PATH = COUNTRY_DIR / f"{COUNTRY_SLUG}_gap_report.csv"
RD_PATH = ROOT / "Data" / "output" / "budget" / "rd_database.csv"


ROLE_MAP = {
    "DSIR (New Zealand)": ("historical_anchor", "Direct historical institution anchor. Use for DSIR-era institutional research."),
    "Research, Science and Technology Vote (New Zealand)": ("vote_anchor", "Best direct vote-level anchor for the pre-MBIE transition era."),
    "Crown Research Institutes (New Zealand)": ("transition_anchor", "Transition-era portfolio anchor through explicit CRI core funding."),
    "Marsden Fund (New Zealand)": ("partial_proxy_component", "Narrow but durable competitive-funding proxy. Useful where vote totals are missing."),
    "Callaghan Innovation": ("modern_portfolio_component", "Modern portfolio component tracked on the comparable operations line."),
    "Catalyst Fund (New Zealand)": ("modern_portfolio_component", "Modern portfolio component. International research collaboration fund."),
    "Endeavour Fund (New Zealand)": ("modern_portfolio_component", "Modern portfolio component. Large contestable fund."),
    "Health Research Fund (New Zealand)": ("modern_portfolio_component", "Modern portfolio component for health-related public research funding."),
    "Partnered Research Fund (New Zealand)": ("modern_portfolio_component", "Modern portfolio component for co-funded or partnered research."),
    "Regional Research Institutes": ("modern_portfolio_component", "Modern portfolio component with narrower regional scope."),
    "Strategic Science Investment Fund (New Zealand)": ("modern_portfolio_component", "Modern portfolio anchor-like component replacing earlier broad science-vote structures."),
}


def _load_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    trace = pd.read_csv(TRACE_PATH)
    rollup = pd.read_csv(ROLLUP_PATH)
    gap = pd.read_csv(GAP_PATH)
    rd = pd.read_csv(RD_PATH)
    rd_nz = rd[rd["country"].eq(COUNTRY)].copy()
    return trace, rollup, gap, rd_nz


def _build_institutional_panel(trace: pd.DataFrame, gap: pd.DataFrame) -> pd.DataFrame:
    out = trace.copy()
    out["dataset"] = "institutional_panel"
    out["series_name"] = out["canonical_name"]
    out["observation_status"] = out["amount_local"].notna().map({True: "observed", False: "gap"})
    out["research_role"] = out["canonical_name"].map(lambda value: ROLE_MAP.get(value, ("other", ""))[0])
    out["research_role_note"] = out["canonical_name"].map(lambda value: ROLE_MAP.get(value, ("", ""))[1])

    gap_cols = ["canonical_name", "year", "gap_type", "diagnosis", "action"]
    out = out.merge(gap[gap_cols], on=["canonical_name", "year"], how="left")
    out["gap_type"] = out["gap_type"].fillna("ok")
    out["diagnosis"] = out["diagnosis"].fillna("")
    out["action"] = out["action"].fillna("")

    ordered = [
        "dataset",
        "country",
        "year",
        "series_name",
        "canonical_name",
        "category",
        "research_role",
        "research_role_note",
        "observation_status",
        "gap_type",
        "amount_local",
        "unit",
        "currency",
        "item_type",
        "source_file",
        "page_number",
        "pdf_path",
        "full_text_cache",
        "line_description_en",
        "traceability_status",
        "trace_excerpt",
        "diagnosis",
        "action",
        "series_notes",
    ]
    out["country"] = COUNTRY
    return out[ordered].sort_values(["series_name", "year"], kind="stable")


def _build_analytical_panel(rollup: pd.DataFrame) -> pd.DataFrame:
    out = rollup.copy()
    out["dataset"] = "analytical_rollup"
    out["series_name"] = out["rollup_name"]
    out["observation_status"] = out["coverage_status"].map(
        {
            "anchor": "observed_anchor",
            "broad_proxy": "observed_proxy",
            "partial_proxy": "observed_partial_proxy",
            "missing": "missing",
        }
    ).fillna("missing")
    ordered = [
        "dataset",
        "country",
        "year",
        "series_name",
        "rollup_name",
        "era_name",
        "rollup_method",
        "coverage_status",
        "observation_status",
        "amount_local",
        "unit",
        "currency",
        "expected_components",
        "optional_components",
        "included_components",
        "expected_component_count",
        "included_component_count",
        "comparability_note",
    ]
    return out[ordered].sort_values(["year"], kind="stable")


def _build_yearbook(rd_nz: pd.DataFrame, rollup: pd.DataFrame, institutional: pd.DataFrame) -> pd.DataFrame:
    inst_obs = institutional[institutional["observation_status"].eq("observed")].copy()
    grouped = (
        inst_obs.groupby("year", dropna=False)
        .agg(
            institutional_total_nzd=("amount_local", "sum"),
            institutional_series_count=("canonical_name", "nunique"),
            institutional_rows=("canonical_name", "size"),
            canonical_list=("canonical_name", lambda values: " | ".join(sorted(set(map(str, values))))),
            source_file_count=("source_file", lambda values: len({str(v) for v in values if pd.notna(v) and str(v).strip()})),
        )
        .reset_index()
    )
    out = rollup[["year", "amount_local", "coverage_status", "era_name", "rollup_method", "included_components"]].rename(
        columns={
            "amount_local": "analytical_rollup_nzd",
            "coverage_status": "analytical_coverage_status",
            "included_components": "analytical_components",
        }
    )
    out = out.merge(grouped, on="year", how="left")
    out["country"] = COUNTRY
    out["institutional_total_nzd"] = out["institutional_total_nzd"].astype("Float64")
    out["institutional_series_count"] = out["institutional_series_count"].fillna(0).astype(int)
    out["institutional_rows"] = out["institutional_rows"].fillna(0).astype(int)
    out["source_file_count"] = out["source_file_count"].fillna(0).astype(int)
    return out[
        [
            "country",
            "year",
            "era_name",
            "rollup_method",
            "analytical_coverage_status",
            "analytical_rollup_nzd",
            "analytical_components",
            "institutional_total_nzd",
            "institutional_series_count",
            "institutional_rows",
            "source_file_count",
            "canonical_list",
        ]
    ].sort_values("year", kind="stable")


def _build_series_catalog(institutional: pd.DataFrame) -> pd.DataFrame:
    obs = institutional[institutional["observation_status"].eq("observed")].copy()
    gaps = institutional[institutional["observation_status"].eq("gap")].copy()

    observed_stats = (
        obs.groupby("canonical_name", dropna=False)
        .agg(
            category=("category", "first"),
            research_role=("research_role", "first"),
            first_observed_year=("year", "min"),
            last_observed_year=("year", "max"),
            observed_years=("year", "nunique"),
            dominant_item_type=("item_type", lambda values: pd.Series(list(values)).mode().iloc[0] if len(values) else ""),
            source_files=("source_file", lambda values: " | ".join(sorted({str(v) for v in values if pd.notna(v) and str(v).strip()}))),
        )
        .reset_index()
    )
    gap_stats = (
        gaps.groupby("canonical_name", dropna=False)
        .agg(
            gap_years=("year", "nunique"),
            gap_list=("year", lambda values: " | ".join(str(int(v)) for v in sorted(set(values)))),
        )
        .reset_index()
    )
    out = observed_stats.merge(gap_stats, on="canonical_name", how="left")
    out["gap_years"] = out["gap_years"].fillna(0).astype(int)
    out["gap_list"] = out["gap_list"].fillna("")
    out["research_role_note"] = out["canonical_name"].map(lambda value: ROLE_MAP.get(value, ("", ""))[1])
    return out.sort_values(["research_role", "first_observed_year", "canonical_name"], kind="stable")


def _write_readme(
    institutional: pd.DataFrame,
    analytical: pd.DataFrame,
    yearbook: pd.DataFrame,
    catalog: pd.DataFrame,
) -> None:
    readme = RESEARCH_DIR / "README.md"
    text = f"""# New Zealand Research-Ready Budget Pack

This folder is generated from the curated New Zealand budget outputs and is intended for downstream research use.

## Files

- `new_zealand_research_panel_institutional.csv`
  - The conservative institutional panel.
  - Use this when you want document-traceable agency, vote, and fund observations.
- `new_zealand_research_panel_analytical.csv`
  - A separate annual rollup that improves longitudinal interpretability.
  - Use this for time-series plotting or macro trend analysis across eras.
- `new_zealand_research_yearbook.csv`
  - One row per year combining the analytical rollup and the sum of observed institutional rows.
- `new_zealand_research_series_catalog.csv`
  - Series-level metadata: first year, last year, role, dominant item type, and gap years.

## Recommended Use

- For institution-level research, start with `new_zealand_research_panel_institutional.csv`.
- For trend analysis across `1975-2025`, start with `new_zealand_research_panel_analytical.csv`.
- Treat `coverage_status = anchor` as the strongest annual evidence.
- Treat `coverage_status = broad_proxy` as a strong but constructed portfolio proxy.
- Treat `coverage_status = partial_proxy` as analytically useful but narrower than a full science-budget anchor.
- Treat `coverage_status = missing` as a true evidence gap, not zero spending.

## Current Counts

- Institutional rows: {len(institutional)}
- Institutional observed rows: {int(institutional['observation_status'].eq('observed').sum())}
- Analytical annual rows: {len(analytical)}
- Analytical non-null years: {int(analytical['amount_local'].notna().sum())}
- Catalog series: {len(catalog)}
- Yearbook years: {len(yearbook)}

## Rebuild

```bash
./venv/bin/python scripts/build_new_zealand_traceability.py
./venv/bin/python scripts/build_new_zealand_rollup.py
./venv/bin/python scripts/build_new_zealand_research_pack.py
```
"""
    readme.write_text(text, encoding="utf-8")


def main() -> None:
    trace, rollup, gap, rd_nz = _load_inputs()
    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)

    institutional = _build_institutional_panel(trace, gap)
    analytical = _build_analytical_panel(rollup)
    yearbook = _build_yearbook(rd_nz, rollup, institutional)
    catalog = _build_series_catalog(institutional)

    institutional.to_csv(RESEARCH_DIR / f"{COUNTRY_SLUG}_research_panel_institutional.csv", index=False)
    analytical.to_csv(RESEARCH_DIR / f"{COUNTRY_SLUG}_research_panel_analytical.csv", index=False)
    yearbook.to_csv(RESEARCH_DIR / f"{COUNTRY_SLUG}_research_yearbook.csv", index=False)
    catalog.to_csv(RESEARCH_DIR / f"{COUNTRY_SLUG}_research_series_catalog.csv", index=False)
    _write_readme(institutional, analytical, yearbook, catalog)

    print(f"Wrote research pack to {RESEARCH_DIR}")
    print(f"institutional_rows: {len(institutional)}")
    print(f"institutional_observed_rows: {int(institutional['observation_status'].eq('observed').sum())}")
    print(f"analytical_rows: {len(analytical)}")
    print(f"analytical_non_null_years: {int(analytical['amount_local'].notna().sum())}")
    print(f"catalog_series: {len(catalog)}")


if __name__ == "__main__":
    main()
