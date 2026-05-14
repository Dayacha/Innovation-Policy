# New Zealand Budget Audit Summary

## Status

- Compile-side New Zealand now builds from `Data/output/budget/results.csv`, not from the stale country DOCX artifact fallback.
- The final New Zealand panel is now constrained to explicit, document-traceable science vote / agency / fund rows.
- A reproducible traceability build is available in `scripts/build_new_zealand_traceability.py` and populates the country traceability CSV with full-text cache paths and excerpts.
- A reproducible analytical rollup build is available in `scripts/build_new_zealand_rollup.py` and populates a separate annual proxy series without altering the final institutional panel.
- A reproducible research pack build is available in `scripts/build_new_zealand_research_pack.py` and writes research-ready CSV outputs under `Data/output/budget/New Zealand/research_ready/`.
- Final New Zealand outputs are normalized to base currency units:
  - `currency = NZD`
  - `unit = dollar`
- The current compile base closes with:
  - `111` non-null final observations in `new_zealand_docx_series.csv`
  - `11` canonicals
  - `41` observed years in `rd_database.csv`
  - `7` remaining gaps in `new_zealand_gap_report.csv`
  - `0` outliers in `new_zealand_gap_report.csv`

## Code Changes Applied

- `budget/compile.py`
  - Added `New Zealand` to `_PIPELINE_FIRST_COUNTRIES` so compile prefers the pipeline extraction in `results.csv`.
- `budget/canonical_series.py`
  - Restricted New Zealand canonicals to explicit, defensible institutional or fund rows.
  - Split the modern umbrella science vote into explicit series:
    - `Research, Science and Technology Vote (New Zealand)`
    - `Strategic Science Investment Fund (New Zealand)`
    - `Endeavour Fund (New Zealand)`
    - `Health Research Fund (New Zealand)`
    - `Partnered Research Fund (New Zealand)`
    - `Catalyst Fund (New Zealand)`
  - Restricted `Crown Research Institutes (New Zealand)` to explicit `Crown Research Institute Core Funding` rows only.
  - Reframed `Callaghan Innovation` as the comparable agency / operations series rather than mixing in early strategic-investment rows or generic agency fragments.
  - Blocked generic New Zealand discovery rows with `agency_type` in `rd_programme` and `rd_fund` from entering the final panel automatically.
  - Added verified compile-side overrides for `Callaghan Innovation` in:
    - `2015`
    - `2019`
    - `2020`
    - `2021`
    - `2023`
  - Added verified compile-side drops for:
    - `1975` `DSIR (New Zealand)`
    - `1977` `DSIR (New Zealand)`
    - `1984` `DSIR (New Zealand)`
    - `1992` `Crown Research Institutes (New Zealand)`
    - `1990` `Research, Science and Technology Vote (New Zealand)`
    - `1995` `Research, Science and Technology Vote (New Zealand)`
    - `2001` `Research, Science and Technology Vote (New Zealand)`
    - `1996` `Marsden Fund (New Zealand)`
    - `1997` `Marsden Fund (New Zealand)`

## Final Series

The current final New Zealand panel includes:

- `DSIR (New Zealand)`:
  - accepted observations across `1976-1990`, with conservative gaps retained where the source is not defensible
- `Research, Science and Technology Vote (New Zealand)`:
  - explicit accepted observations between `1996-2010`
- `Crown Research Institutes (New Zealand)`:
  - `2011-2015`
- `Marsden Fund (New Zealand)`:
  - `1998-2025`
- `Callaghan Innovation`:
  - `2015-2025`
- `Strategic Science Investment Fund (New Zealand)`:
  - `2017-2025`
- `Endeavour Fund (New Zealand)`:
  - `2018-2025`
- `Health Research Fund (New Zealand)`:
  - `2016-2025`
- `Partnered Research Fund (New Zealand)`:
  - `2016-2025`
- `Catalyst Fund (New Zealand)`:
  - `2016-2025`
- `Regional Research Institutes`:
  - `2017-2022`

## Analytical Rollup

The final institutional panel remains conservative and intentionally sparse where the underlying documents are ambiguous. To support time-series interpretation without weakening those standards, a second output is now built:

- `Data/output/budget/New Zealand/new_zealand_analytical_rollup.csv`

This file is derived from the final `rd_database.csv` and marks each year with:

- `coverage_status = anchor` when the year is covered by a direct top-level anchor such as `DSIR` or `Research, Science and Technology Vote`
- `coverage_status = broad_proxy` when the year is represented by a fuller sum of explicit modern science-fund components
- `coverage_status = partial_proxy` when only a narrower fallback such as `Marsden Fund` is available
- `coverage_status = missing` when no defensible annual proxy exists in the final panel

This preserves the strict institutional database while also producing a more interpretable annual proxy for New Zealand.

## Research-Ready Outputs

The New Zealand workflow now also produces a dedicated research pack:

- `Data/output/budget/New Zealand/research_ready/new_zealand_research_panel_institutional.csv`
- `Data/output/budget/New Zealand/research_ready/new_zealand_research_panel_analytical.csv`
- `Data/output/budget/New Zealand/research_ready/new_zealand_research_yearbook.csv`
- `Data/output/budget/New Zealand/research_ready/new_zealand_research_series_catalog.csv`
- `Data/output/budget/New Zealand/research_ready/README.md`

Recommended use:

- Use the institutional panel for document-traceable institution and fund analysis.
- Use the analytical panel for long-run plotting and macro trend analysis.
- Use the yearbook to compare the annual institutional total against the analytical rollup.
- Use the series catalog to understand which series are anchors, partial proxies, or modern portfolio components.

## Documentary Verification

Representative source checks were confirmed directly against the cached original full text under `Data/output/budget/full_text/New Zealand/`.

Verified examples:

- `1975` `DSIR (New Zealand)`
  - source file: `1975 aa19751975n128203.pdf`
  - result: dropped from the final panel
  - rationale: the extracted `724,077` row is the Works and Trading Account total, not the DSIR budget appropriation
  - supporting cache: `pdf_a1ce3fb3453c__1975_aa19751975n128203.txt.gz`
- `2019` `Callaghan Innovation`
  - source file: `2019_Appropriation Estimates Act.pdf`
  - final row: `75,151,000 NZD`
  - rationale: manual override to the explicit `Callaghan Innovation - Operations` row visible in the source text
  - supporting cache: `pdf_1e4f80dc63ee__2019_Appropriation_Estimates_Act.txt.gz`
- `1996` `Research, Science and Technology Vote (New Zealand)`
  - source file: `1996_Appropriation Estimates Act.pdf`
  - final row: `267,699,000 NZD`
  - supporting cache: `pdf_b8a73c2e1b0d__1996_Appropriation_Estimates_Act.txt.gz`
- `2011` `Crown Research Institutes (New Zealand)`
  - source file: `2011_Appropriation Estimates Act.pdf`
  - final row: `215,145,000 NZD`
  - supporting cache: `pdf_815c4695074b__2011_Appropriation_Estimates_Act.txt.gz`
- `2015` `Crown Research Institutes (New Zealand)`
  - source file: `2015_Appropriation Estimates Act.pdf`
  - final row: `201,622,000 NZD`
  - supporting cache: `pdf_5178008fefe9__2015_Appropriation_Estimates_Act.txt.gz`
- `2024` modern science funds
  - source file: `2024_Appropriation Estimates Act.pdf`
  - verified final rows include:
    - `Strategic Science Investment Fund` `358,566,000 NZD`
    - `Endeavour Fund` `246,857,000 NZD`
    - `Callaghan Innovation - Operations` `85,844,000 NZD`
  - supporting cache: `pdf_03fa7bcc1884__2024_Appropriation_Estimates_Act.txt.gz`

## Remaining Gaps

The remaining gaps are intentional conservative holds, not unresolved unit bugs:

- `1977` `DSIR (New Zealand)`
- `1975` `DSIR (New Zealand)`
- `1984` `DSIR (New Zealand)`
- `1990` `Research, Science and Technology Vote (New Zealand)`
- `1995` `Research, Science and Technology Vote (New Zealand)`
- `2001` `Research, Science and Technology Vote (New Zealand)`
- `2020` `Regional Research Institutes`

These rows were not kept because the original cached evidence was either absent, ambiguous, or not clean enough to defend as a reproducible institutional series observation.

## Unit And Comparability Note

- The original New Zealand budget tables often print values in thousands.
- The final country outputs do not preserve those printed units literally.
- Compile expands accepted amounts into base NZD and writes:
  - `unit = dollar`
  - `currency = NZD`
- This is intentional and should remain invariant unless the underlying compile normalization logic changes.

## Targeted Recovery Note

The cheap robust-layer prep was run after compile-side stabilization:

- `python -m budget.pipeline --targeted-recovery-only --countries "New Zealand"`
- `python -m budget.pipeline --postprocess-only`

Observed result:

- targeted recovery added `3` rows to `Data/output/budget/results.csv`
- postprocess regenerated `results_clean.csv` and `qa_report.csv`
- one API recovery failed for:
  - `2020_Appropriation Estimates Act.pdf`
  - target row: `Regional Research Institutes`

That failure did not corrupt the compile base. It only left the existing `2020` gap unresolved.

## Reproducible Rerun Commands

Compile-only rerun:

```bash
python -m budget.compile --country "New Zealand"
python main.py --budget --build-database
```

Robust-layer rerun after compile is already stable:

```bash
python -m budget.pipeline --targeted-recovery-only --countries "New Zealand"
python -m budget.pipeline --postprocess-only
python -m budget.compile --country "New Zealand"
python main.py --budget --build-database
```

Analytical outputs rebuild:

```bash
python scripts/build_new_zealand_traceability.py
python scripts/build_new_zealand_rollup.py
python scripts/build_new_zealand_research_pack.py
```

## Methodological Note

New Zealand is now framed as a traceable panel of observed science agencies, explicit science vote totals, and named research funds.

The panel is intentionally conservative:

- explicit institutional or fund rows are preferred over broad page summaries
- ambiguous vote totals are dropped rather than interpolated
- discovery remains available in the extraction layer, but unverified programme noise is blocked from the final series

Everything retained in the final New Zealand series is now intended to be reproducible from code and traceable back to the original document set.
