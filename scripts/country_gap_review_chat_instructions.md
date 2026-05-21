# Country Gap Review Instructions

Use these instructions when reviewing one budget country at a time in a fresh Codex chat.

## Goal

Diagnose why a country's budget series has gaps, abrupt breaks, or suspicious missing years, then fix what is fixable and document what is not.

## Scope

Work on one country only, unless a shared code path must be updated to fix that country.

## First files to inspect

Global summaries:

- `Data/output/budget/country_gap_deepdive_summary.csv`
- `Data/output/budget/country_gap_deepdive_detail.csv`
- `Data/output/budget/country_gap_source_audit.csv`
- `Data/output/budget/run_log.jsonl`

Country-specific folders:

- `Data/output/budget/<COUNTRY>/`
- `Data/input/finance_bills/<COUNTRY>/`

## Country files to check

When present, inspect:

- `*_gap_report.csv`
- `*_reextract_queue.csv`
- `*_docx_results.csv`
- `*_docx_audit.csv`
- `*_docx_series.csv`
- `*_docx_totals.csv`
- `*_discovery_review.csv`
- `*_verified_overrides.csv`
- `*_source_traceability.csv`
- `SOURCE_NOTES.md`
- `QUALITY_NOTE.md`

## Questions to answer

For the target country, determine:

1. Which years are missing or suspicious.
2. Which source files correspond to those years.
3. Whether the file ran and returned zero rows.
4. Whether rows were extracted but not retained into the final series.
5. Whether the document structure changed.
6. Whether the issue is comparability rather than extraction failure.
7. Whether a code, cleaner, mapping, or country-note change can improve the country now.

## Typical causes

Classify each problem year/source into one of these:

- no extractable data
- document ran but returned zero rows
- OCR / text-layer failure
- unsupported format
- document changed structure
- source is legal wrapper / summary / aggregate only
- currency or unit break
- comparability issue
- outlier needing verification
- extracted rows exist but are not making it into the final series

## Required table

Produce a review table with one row per `year-source_file` and these columns:

- `country`
- `year`
- `source_file`
- `run_status`
- `run_log_rows_extracted`
- `docx_results_rows`
- `docx_audit_in_series_rows`
- `extracted_entities`
- `missing_agencies_from_queue`
- `year_issue_label`
- `year_document_change_flag`
- `country_document_change_signal`
- `document_change_note`
- `diagnosis_excerpt`
- `recommended_action`

## Action rules

If the issue is fixable now, modify what is necessary. Examples:

- cleaner logic
- canonical mapping
- gap logic
- documented country rule
- country notes
- regeneration of country outputs

If the issue is not fixable with the current files, document that clearly.

Do not make unrelated changes for other countries.

## Final output

Return:

1. A short diagnosis.
2. The main reason the gaps exist.
3. What you changed.
4. What remains unresolved.
5. The compact review table.
