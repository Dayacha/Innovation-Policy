# Country Gap Review Agent Prompt

Use this prompt when you want an AI agent to investigate one country at a time and make the necessary changes.

## Invocation pattern

Replace `{{COUNTRY}}` with the country you want reviewed.

```bash
codex "Read scripts/country_gap_review_agent_prompt.md, set COUNTRY={{COUNTRY}}, and execute the task."
```

## Task

You are reviewing one country's budget time series in depth.

Target country: `{{COUNTRY}}`

The economist has already identified suspicious gaps or discontinuities in the budget charts for this country. Your job is to act like the data scientist who must:

1. inspect the current evidence for this country,
2. explain where the gaps are and why they happen,
3. modify the necessary pipeline files, notes, or outputs if the diagnosis shows a fix is possible,
4. leave the country in a better state than you found it.

## Files to inspect first

Global cross-country outputs:

- `Data/output/budget/country_gap_deepdive_summary.csv`
- `Data/output/budget/country_gap_deepdive_detail.csv`
- `Data/output/budget/country_gap_source_audit.csv`
- `Data/output/budget/run_log.jsonl`

Country-specific outputs:

- `Data/output/budget/{{COUNTRY}}/`
- `Data/input/finance_bills/{{COUNTRY}}/`

Relevant country files may include:

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

Also inspect code paths that affect this country if needed, including:

- `budget/canonical_series.py`
- `budget/gap_detector.py`
- `budget/cleaners/`
- `budget/country_profiles.py`
- `budget/pipeline.py`
- any country-specific cleaner or hardcoded exception logic

## Required questions to answer

For `{{COUNTRY}}`, determine:

- which years are missing
- which source files correspond to those years
- whether the source file ran and returned zero rows
- whether extraction found rows but they were not retained into the final series
- whether the source family changed structure
- whether this is a comparability problem rather than an extraction failure
- whether a code or rule change can improve the country immediately

## Required output

Produce a compact review table for `{{COUNTRY}}` with one row per `year-source_file` and at least these columns:

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

- adjust country cleaner logic
- improve classification / canonical mapping
- update a country note
- add or refine a documented exception
- regenerate a country summary output

If the issue is not fixable with current files, document that clearly and say why.

Do not make broad unrelated changes to other countries.

## Final response

Return:

1. a short diagnosis for `{{COUNTRY}}`
2. the main reason the gaps exist
3. what you changed
4. what still remains unresolved
5. the review table path or summary
