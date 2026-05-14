# New Zealand Post-Robust Checklist

## Objective

Validate that the robust pipeline layer does not overwrite or degrade the curated New Zealand compile base.

Target commands:

- `python -m budget.pipeline --targeted-recovery-only --countries "New Zealand"`
- `python -m budget.pipeline --postprocess-only`
- `python -m budget.compile --country "New Zealand"`
- `python main.py --budget --build-database`

Do not treat `fill-gaps` or `gap_review_apply` as safe by default for New Zealand. Re-check the compile outputs first.

## Files To Check

- `Data/output/budget/New Zealand/new_zealand_docx_results.csv`
- `Data/output/budget/New Zealand/new_zealand_docx_series.csv`
- `Data/output/budget/New Zealand/new_zealand_gap_report.csv`
- `Data/output/budget/New Zealand/new_zealand_analytical_rollup.csv`
- `Data/output/budget/New Zealand/research_ready/new_zealand_research_panel_institutional.csv`
- `Data/output/budget/New Zealand/research_ready/new_zealand_research_panel_analytical.csv`
- `Data/output/budget/New Zealand/research_ready/new_zealand_research_yearbook.csv`
- `Data/output/budget/New Zealand/research_ready/new_zealand_research_series_catalog.csv`
- `Data/output/budget/rd_database.csv`
- `Data/output/budget/results_clean.csv`
- `Data/output/budget/qa_report.csv`

## Hard Invariants

The following conditions should remain true after a valid rerun:

- `rd_database.csv` still has `111` rows for `New Zealand`
- `rd_database.csv` still has `11` New Zealand series
- `rd_database.csv` still has `41` observed New Zealand years
- `new_zealand_analytical_rollup.csv` still has `51` annual rows and `41` non-null annual proxy values
- the research-ready pack is rebuilt and remains aligned with the institutional panel and analytical rollup
- all New Zealand final rows still have:
  - `currency = NZD`
  - `unit = dollar`
- `new_zealand_gap_report.csv` still has:
  - `7` `missing`
  - `0` `outlier`

The accepted New Zealand canonicals should remain exactly:

- `Callaghan Innovation`
- `Catalyst Fund (New Zealand)`
- `Crown Research Institutes (New Zealand)`
- `DSIR (New Zealand)`
- `Endeavour Fund (New Zealand)`
- `Health Research Fund (New Zealand)`
- `Marsden Fund (New Zealand)`
- `Partnered Research Fund (New Zealand)`
- `Regional Research Institutes`
- `Research, Science and Technology Vote (New Zealand)`
- `Strategic Science Investment Fund (New Zealand)`

## Locked Conservative Gaps

These gaps are deliberate and should not disappear unless supported by explicit documentary evidence:

- `1975` `DSIR (New Zealand)`
- `1977` `DSIR (New Zealand)`
- `1984` `DSIR (New Zealand)`
- `1990` `Research, Science and Technology Vote (New Zealand)`
- `1995` `Research, Science and Technology Vote (New Zealand)`
- `2001` `Research, Science and Technology Vote (New Zealand)`
- `2020` `Regional Research Institutes`

If any of these rows become non-null after a rerun, manually confirm the original source document before accepting the change.

## Required Checks

1. `new_zealand_docx_series.csv`

- Confirm there are still `111` non-null observations.
- Confirm there are still `11` canonicals.
- Confirm all non-null rows for New Zealand still have:
  - `unit = dollar`
  - `currency = NZD`
- Confirm no new generic discovery canonicals entered the panel.
- Confirm `Callaghan Innovation` remains the operations-based series from `2015` onward rather than reverting to strategic-investment or generic non-operations rows.

2. `new_zealand_gap_report.csv`

- Confirm there are still:
  - `7` rows with `gap_type = missing`
  - `0` rows with `gap_type = outlier`
- Confirm the seven conservative gap rows listed above remain the only missing rows unless new documentary evidence was added.

3. `rd_database.csv`

- Confirm New Zealand still has:
  - `111` rows
  - `11` series
  - `41` observed years
- Confirm the canonical list matches the compile output exactly.
- Confirm no row for New Zealand reverted to `thousand`, `million`, or blank unit.

4. `new_zealand_analytical_rollup.csv`

- Confirm the rollup still has:
  - `51` annual rows covering `1975-2025`
  - `41` non-null annual proxy values
- Confirm `2000-2003` and `2005-2009` remain `partial_proxy` years based on `Marsden Fund`.
- Confirm `2004` remains `missing` unless new documentary evidence is added.
- Confirm modern years continue to distinguish:
  - `anchor`
  - `broad_proxy`
  - `partial_proxy`
  - `missing`

5. `results_clean.csv` and `qa_report.csv`

- Confirm postprocess completed successfully.
- Confirm no recovery noise introduced broad ministry totals or unnamed programme fragments into the New Zealand final panel.

6. `research_ready/*`

- Confirm `new_zealand_research_panel_institutional.csv` still contains all `111` observed rows plus the conservative gap rows.
- Confirm `new_zealand_research_panel_analytical.csv` matches `new_zealand_analytical_rollup.csv` year coverage exactly.
- Confirm `new_zealand_research_yearbook.csv` still covers `1975-2025`.
- Confirm `new_zealand_research_series_catalog.csv` still has `11` series.

## Failure Signals

Treat any of these as a regression:

- a New Zealand final row flips away from `unit = dollar`
- a New Zealand final row flips away from `currency = NZD`
- a new generic programme or fund canonical appears in the final panel
- one of the seven conservative gap years becomes populated without explicit documentary support
- `new_zealand_gap_report.csv` gains new `outlier` rows
- the accepted canonical count increases above `11`
- the accepted New Zealand rows in `rd_database.csv` no longer match `new_zealand_docx_series.csv`

## Quick Triage

If the robust layer changed New Zealand unexpectedly:

1. Re-run compile:

```bash
python -m budget.compile --country "New Zealand"
```

2. Rebuild the database:

```bash
python main.py --budget --build-database
```

3. Re-check:

- `new_zealand_docx_series.csv`
- `new_zealand_gap_report.csv`
- `rd_database.csv`

If compile restores the expected values, the regression came from the robust layer rather than from the New Zealand compile curation.
