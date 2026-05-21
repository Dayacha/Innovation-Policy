# Gap Investigation Agent Prompt

Use this prompt for an AI agent that must investigate budget-series gaps by country and explain why they occur.

## Suggested command

```bash
codex "Read scripts/gap_investigation_agent_prompt.md and execute the task using the files referenced there."
```

If your agent supports passing a prompt file directly, use the contents below as the task.

## Task

You are supporting an economist who reviewed country budget charts and found abrupt gaps or discontinuities in some country-year series.

Your job is to act like the data scientist assigned to diagnose those gaps and produce a review table the researcher can inspect.

### Objective

For each country, identify where the budget series appears to break, disappear, or behave suspiciously across years, then explain the most likely reason:

- no extractable data in that year
- source document exists but extraction returned zero rows
- document structure changed
- source changed from detailed annex to legal wrapper / summary document
- OCR / text-layer failure
- unsupported format
- currency / unit break
- comparability problem rather than true missing extraction
- outlier that needs verification rather than a true gap

### Main outputs to use

Read and use these files first:

- `Data/output/budget/country_gap_deepdive_summary.csv`
- `Data/output/budget/country_gap_deepdive_detail.csv`
- `Data/output/budget/country_gap_source_audit.csv`
- `Data/output/budget/run_log.jsonl`

Then, when needed, inspect country-level files such as:

- `Data/output/budget/<Country>/<country>_gap_report.csv`
- `Data/output/budget/<Country>/<country>_reextract_queue.csv`
- `Data/output/budget/<Country>/<country>_docx_results.csv`
- `Data/output/budget/<Country>/<country>_docx_audit.csv`
- `Data/input/finance_bills/<Country>/SOURCE_NOTES.md`
- `Data/input/finance_bills/<Country>/QUALITY_NOTE.md`

### Deliverable

Produce a table with one row per `country-year-source_file` with at least these columns:

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

### Required analysis

1. Rank countries by urgency for review.
2. Separate three kinds of cases:
   - extraction failure
   - source/document change
   - methodological comparability issue
3. Recommend which country to start with first, and why.
4. Highlight countries where charts show gaps but the real problem is not “missing data” but “non-comparable source structure”.

### Decision rule for prioritization

Treat these as highest priority:

- many missing agency-years
- many years where documents ran but returned zero rows
- long uninterrupted missing spans
- countries where the same source file pattern repeatedly fails

Treat these as lower priority:

- countries with no open gaps and only known methodological caveats
- countries where the issue is already documented and accepted as non-comparable

### Final write-up

Return:

1. A short executive summary.
2. A ranked list of the top countries to investigate first.
3. A compact table of the most important `country-year-source_file` cases.
4. Clear language for a researcher, not engineering jargon unless necessary.
