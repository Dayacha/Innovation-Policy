# Israel Post-Robust Checklist

## Objective

Validate that the robust pipeline layer did not overwrite or degrade the manually curated Israel series.

Target files after running:

- `python -m budget.pipeline --targeted-recovery-only --countries Israel`
- `python -m budget.pipeline --postprocess-only`
- `python main.py --budget --country Israel`
- `python main.py --budget --country Israel --fill-gaps`
- `python -m budget.gap_review --country Israel`
- `python -m budget.gap_review_apply --country Israel`
- `python main.py --budget --build-database`

## Files To Check

- `Data/output/budget/Israel/israel_docx_series.csv`
- `Data/output/budget/Israel/israel_docx_totals.csv`
- `Data/output/budget/Israel/israel_gap_report.csv`
- `Data/output/budget/Israel/israel_series_traceability.csv`
- `Data/output/budget/rd_database.csv`

## Hard Invariants

These rows are locked manual curation and should not change:

- `National Council for R&D (Israel, pre-1992)`:
  - `1975-1991`
- `Ministry of Science and Technology (Israel)`:
  - `1992, 1994-2009, 2011, 2013, 2015, 2017, 2021-2025`
- `Israel Innovation Authority (from 2016)`:
  - `2017, 2025`
- `Office of the Chief Scientist (Israel, pre-2016)`:
  - `1986, 2001, 2013`
- `KAMEA Fund (קרן קמ"ח)`:
  - `1987, 1991, 2001`
- `Israeli Space Agency (סוכנות החלל הישראלית)`:
  - `1995, 2001, 2004, 2005, 2007, 2009, 2011, 2019, 2021, 2022, 2023, 2024`

## Required Checks

1. `israel_docx_series.csv`

- Confirm there are still `61` non-null observations.
- Confirm all locked rows still exist with the same:
  - `year`
  - `canonical_name`
  - `amount_local`
  - `unit`
  - `currency`
  - `source_file`
  - `page_number`
- Confirm locked rows still include `locked manual curation` or `manual override from original Israel budget file` in `series_notes`.
- Confirm no new canonical names appeared for Israel.

2. `israel_gap_report.csv`

- Confirm there are still:
  - `0 missing`
  - `0 outliers`
- Confirm no locked observation is flagged for `verify`, `reextract`, `reclassify`, or `drop`.

3. `rd_database.csv`

- Confirm Israel still has:
  - `6` canonicals
  - `37` years represented
- Confirm the locked observations are identical to the country series.

4. `israel_series_traceability.csv`

- Confirm traceability rows still align with the final series.
- Confirm `2025` still maps to:
  - `Ministry of Science and Technology (Israel)` → `2025_Israel.pdf`, page `6`
  - `Israel Innovation Authority (from 2016)` → `2025_Israel.pdf`, page `48`

## Failure Signals

Treat any of these as regression:

- a locked year disappears
- a locked amount changes
- `unit` flips for locked rows
- `currency` flips for locked rows
- `2019` ministry total comes back
- `2013` Israeli Space Agency comes back from the 2014 half of the file
- new Israel canonicals appear from discovery / recovery noise
- `gap_review_apply` modifies any locked row

## Quick Triage

If the robust layer changed Israel:

1. Re-run:
   - `python -m budget.compile --country Israel`
2. Rebuild:
   - `python main.py --budget --build-database`
3. Re-check the locked rows in:
   - `israel_docx_series.csv`
   - `rd_database.csv`

If compile restores the expected values, the regression came from the robust layer rather than from the manual curation code.
