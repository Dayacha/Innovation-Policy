#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-./venv/bin/python}"
COUNTRY="New Zealand"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Python interpreter not found or not executable: $PYTHON_BIN" >&2
  exit 1
fi

echo "[NZ] Targeted recovery"
"$PYTHON_BIN" -m budget.pipeline --targeted-recovery-only --countries "$COUNTRY"

echo "[NZ] Postprocess"
"$PYTHON_BIN" -m budget.pipeline --postprocess-only

echo "[NZ] Compile base"
"$PYTHON_BIN" -m budget.compile --country "$COUNTRY"

echo "[NZ] Build country outputs"
"$PYTHON_BIN" main.py --budget --country "$COUNTRY"

echo "[NZ] Fill gaps"
"$PYTHON_BIN" main.py --budget --country "$COUNTRY" --fill-gaps

echo "[NZ] Gap review"
"$PYTHON_BIN" -m budget.gap_review --country "$COUNTRY"

echo "[NZ] Gap review apply"
"$PYTHON_BIN" -m budget.gap_review_apply --country "$COUNTRY"

echo "[NZ] Build combined database"
"$PYTHON_BIN" main.py --budget --build-database

echo "[NZ] Rebuild enriched traceability"
"$PYTHON_BIN" scripts/build_new_zealand_traceability.py

echo "[NZ] Build analytical rollup"
"$PYTHON_BIN" scripts/build_new_zealand_rollup.py

echo "[NZ] Build research-ready pack"
"$PYTHON_BIN" scripts/build_new_zealand_research_pack.py

echo "[NZ] Verify research-ready pack"
"$PYTHON_BIN" scripts/verify_new_zealand_research_pack.py

echo "[NZ] Final validation"
"$PYTHON_BIN" - <<'PY'
import pandas as pd
from pathlib import Path

root = Path("Data/output/budget")
country_dir = root / "New Zealand"

series = pd.read_csv(country_dir / "new_zealand_docx_series.csv")
gap = pd.read_csv(country_dir / "new_zealand_gap_report.csv")
rollup = pd.read_csv(country_dir / "new_zealand_analytical_rollup.csv")
research_inst = pd.read_csv(country_dir / "research_ready" / "new_zealand_research_panel_institutional.csv")
research_roll = pd.read_csv(country_dir / "research_ready" / "new_zealand_research_panel_analytical.csv")
rd = pd.read_csv(root / "rd_database.csv")
rd_nz = rd[rd["country"].eq("New Zealand")].copy()

assert not rd_nz.empty, "rd_database.csv has no New Zealand rows"
assert set(rd_nz["unit"].dropna().astype(str)) == {"dollar"}, f"Unexpected NZ units: {rd_nz['unit'].dropna().unique().tolist()}"
assert set(rd_nz["currency"].dropna().astype(str)) == {"NZD"}, f"Unexpected NZ currencies: {rd_nz['currency'].dropna().unique().tolist()}"
assert set(rollup["unit"].dropna().astype(str)) == {"dollar"}, f"Unexpected NZ rollup units: {rollup['unit'].dropna().unique().tolist()}"
assert set(rollup["currency"].dropna().astype(str)) == {"NZD"}, f"Unexpected NZ rollup currencies: {rollup['currency'].dropna().unique().tolist()}"
assert len(research_inst) >= len(series), "Research institutional panel unexpectedly shorter than series traceability"
assert len(research_roll) == len(rollup), "Research analytical panel row count mismatch"

summary = {
    "series_non_null": int(series["amount_local"].notna().sum()),
    "series_canonicals": int(series["canonical_name"].nunique()),
    "rd_rows": int(len(rd_nz)),
    "rd_observed_years": int(rd_nz["year"].nunique()),
    "rd_min_year": int(rd_nz["year"].min()),
    "rd_max_year": int(rd_nz["year"].max()),
    "gap_counts": gap["gap_type"].value_counts(dropna=False).to_dict(),
    "rollup_non_null_years": int(rollup["amount_local"].notna().sum()),
    "rollup_coverage_counts": rollup["coverage_status"].value_counts(dropna=False).to_dict(),
    "research_institutional_rows": int(len(research_inst)),
    "research_analytical_rows": int(len(research_roll)),
}

for key, value in summary.items():
    print(f"{key}: {value}")
PY

echo "[NZ] Done"
