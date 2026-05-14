from __future__ import annotations

from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
COUNTRY = "New Zealand"
COUNTRY_SLUG = "new_zealand"
OUTPUT_DIR = ROOT / "Data" / "output" / "budget" / COUNTRY
RD_PATH = ROOT / "Data" / "output" / "budget" / "rd_database.csv"
OUTPUT_PATH = OUTPUT_DIR / f"{COUNTRY_SLUG}_analytical_rollup.csv"


ROLLUP_NAME = "New Zealand public science budget proxy"

DSIR = "DSIR (New Zealand)"
RST_VOTE = "Research, Science and Technology Vote (New Zealand)"
CRI = "Crown Research Institutes (New Zealand)"
MARSDEN = "Marsden Fund (New Zealand)"
CALLAGHAN = "Callaghan Innovation"
CATALYST = "Catalyst Fund (New Zealand)"
ENDEAVOUR = "Endeavour Fund (New Zealand)"
HEALTH = "Health Research Fund (New Zealand)"
PARTNERED = "Partnered Research Fund (New Zealand)"
RRI = "Regional Research Institutes"
SSIF = "Strategic Science Investment Fund (New Zealand)"


def _components_for_year(year: int) -> tuple[str, str, list[str], list[str], str]:
    if year <= 1990:
        return (
            "dsir_era",
            "dsir_anchor",
            [DSIR],
            [],
            "Single-institution anchor from the DSIR era. Conservative institutional series, not a whole-of-system science budget.",
        )
    if year <= 2010:
        return (
            "rst_transition_era",
            "rst_vote_anchor_or_partial_proxy",
            [RST_VOTE],
            [MARSDEN],
            "Prefer the explicit Research, Science and Technology vote when available. Fall back to Marsden only as a partial proxy when the vote total is not defensible in the source.",
        )
    if year <= 2015:
        return (
            "cri_transition_era",
            "transitional_portfolio_sum",
            [CRI, MARSDEN],
            [CALLAGHAN],
            "Transition-era proxy built from the explicit Crown Research Institute core funding anchor plus Marsden, with Callaghan added only where it already exists in the final panel.",
        )

    expected = [CALLAGHAN, CATALYST, HEALTH, MARSDEN, PARTNERED]
    optional = []
    if year >= 2017:
        expected.append(SSIF)
        optional.append(RRI)
    if year >= 2018:
        expected.append(ENDEAVOUR)
    return (
        "modern_portfolio_era",
        "modern_portfolio_sum",
        expected,
        optional,
        "Modern proxy built as the sum of explicit science and innovation portfolio components retained in the final institutional panel.",
    )


def _build_rollup(rd_nz: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    years = range(1975, 2026)

    for year in years:
        era_name, method, expected_components, optional_components, note = _components_for_year(year)
        year_df = rd_nz[rd_nz["year"].eq(year)].copy()

        present_names = set(year_df["canonical_name"])
        expected_present = [name for name in expected_components if name in present_names]
        optional_present = [name for name in optional_components if name in present_names]

        coverage_status = "missing"
        amount_local = pd.NA
        included_components: list[str] = []

        if method == "dsir_anchor":
            if expected_present:
                included_components = expected_present
                amount_local = float(year_df[year_df["canonical_name"].isin(included_components)]["amount_local"].sum())
                coverage_status = "anchor"
        elif method == "rst_vote_anchor_or_partial_proxy":
            if RST_VOTE in expected_present:
                included_components = [RST_VOTE]
                amount_local = float(year_df[year_df["canonical_name"].eq(RST_VOTE)]["amount_local"].sum())
                coverage_status = "anchor"
            elif MARSDEN in present_names:
                included_components = [MARSDEN]
                amount_local = float(year_df[year_df["canonical_name"].eq(MARSDEN)]["amount_local"].sum())
                coverage_status = "partial_proxy"
        else:
            included_components = expected_present + optional_present
            if included_components:
                amount_local = float(year_df[year_df["canonical_name"].isin(included_components)]["amount_local"].sum())
                coverage_status = "broad_proxy" if len(expected_present) == len(expected_components) else "partial_proxy"

        rows.append(
            {
                "country": COUNTRY,
                "year": year,
                "rollup_name": ROLLUP_NAME,
                "era_name": era_name,
                "rollup_method": method,
                "coverage_status": coverage_status,
                "amount_local": amount_local,
                "unit": "dollar",
                "currency": "NZD",
                "expected_components": " | ".join(expected_components),
                "optional_components": " | ".join(optional_components),
                "included_components": " | ".join(included_components),
                "expected_component_count": len(expected_components),
                "included_component_count": len(included_components),
                "comparability_note": note,
            }
        )

    return pd.DataFrame(rows)


def main() -> None:
    rd = pd.read_csv(RD_PATH)
    rd_nz = rd[rd["country"].eq(COUNTRY)].copy()
    if rd_nz.empty:
        raise SystemExit(f"No rows for {COUNTRY} in {RD_PATH}")

    rollup = _build_rollup(rd_nz)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    rollup.to_csv(OUTPUT_PATH, index=False)
    summary = {
        "rows": int(len(rollup)),
        "non_null_amounts": int(rollup["amount_local"].notna().sum()),
        "anchor_years": int((rollup["coverage_status"] == "anchor").sum()),
        "broad_proxy_years": int((rollup["coverage_status"] == "broad_proxy").sum()),
        "partial_proxy_years": int((rollup["coverage_status"] == "partial_proxy").sum()),
        "missing_years": int((rollup["coverage_status"] == "missing").sum()),
    }
    print(f"Wrote {OUTPUT_PATH}")
    for key, value in summary.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
