# Hungary Source Notes

## PEINSA Quality Note
- Hungary is now usable as a traced institutional series with explicit documented gaps.
- The current file inventory supports a strong modern and mid-period series, but not a complete annual panel.
- The final Hungary database should be interpreted as a reproducible audited series, not as proof of continuous full annual coverage from `1991–2025`.
- The main unresolved issue is the `MTA` historical chapter total in a small set of early/mid years where the chapter is visible but the total is truncated in the PDF text layer.

## Current Final Coverage
- Final Hungary rows in `rd_database.csv`: `68`
- Final observed years:
  - `1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2001, 2005, 2007, 2010–2025`
- Final canonicals currently retained:
  - `Hungarian Academy of Sciences (MTA)`
  - `MTA Library and Information Centre`
  - `Research and Technological Innovation Fund`
  - `National Research, Development and Innovation Fund (Hungary)`
  - `Eötvös Loránd Research Network`
  - `Hungarian Research Network`
  - `INTERREG IVC Programme`
  - `35th National Research, Development and Innovation Fund`
- All final Hungary rows are stored as `HUF` in full `forint`, not `thousand`.

## Traceability Status
- Row-level detail series:
  - `Data/output/budget/Hungary/hungary_docx_series.csv`
- Verified override audit:
  - `Data/output/budget/Hungary/hungary_verified_overrides.csv`
- MTA-specific extractor audit:
  - `Data/output/budget/Hungary/hungary_mta_extractor_audit.csv`
- Canonical audit appearances:
  - `Data/output/budget/Hungary/hungary_docx_audit.csv`
- Current verified override split:
  - `67` rows: `verified_override`
  - `40` rows sourced from `parsed_budget_text`
  - `27` rows sourced from `original_pdf`
- Current gap split:
  - `68` rows: `ok`
  - `7` rows: `missing`
  - `0` rows: `outlier`

## Strong Years / Source Pattern
- Strong direct `MTA` chapter-total years from original or stable parsed text:
  - `1992–1999`
  - `2001`
  - `2005`
  - `2007`
  - `2010–2025`
- Strong fund / network years:
  - `Research and Technological Innovation Fund`: `2010–2014`
  - `National Research, Development and Innovation Fund (Hungary)`: `2015–2025`
  - `Eötvös Loránd Research Network`: `2020–2023`
  - `Hungarian Research Network`: `2024–2025`
  - `MTA Library and Information Centre`: `2013–2025`

## Years That Need Special Interpretation
- `2015–2025` `National Research, Development and Innovation Fund (Hungary)`:
  - retained from verified chapter or fund totals, not from broad ministry reconstruction
- `2020–2025` research-network rows:
  - these are chapter-total style recoveries for the network successor institutions
  - they are traceable and reproducible, but they come from institutional chapter totals rather than narrow programme lines
- `INTERREG IVC Programme`:
  - retained only for the years where the appropriation line is explicit and defensible

## Remaining Non-Recoverable Years
- `Hungarian Academy of Sciences (MTA)` remains unresolved for:
  - `1991`
  - `2000`
  - `2003`
  - `2004`
  - `2006`
  - `2008`
  - `2009`
- These years are left as explicit gaps because the current original-text evidence does not yield a fully recoverable annual total.

## Stage-of-Loss Diagnosis For Remaining MTA Gaps
- `1991`
  - the cached original text does not contain a detectable `Magyar Tudományos Akadémia` / `MTA` chapter heading
  - treat this as a source-text / pre-parser extraction failure
- `2000`, `2003`, `2004`, `2006`, `2008`, `2009`
  - the cached original text does contain `XXXIII. MAGYAR TUDOMÁNYOS AKADÉMIA`
  - however, no `MTA` row reaches `raw_rows`
  - the failure point is the text-cache parser on a multi-column chapter total whose amount is truncated in the PDF text layer
- `hungary_mta_extractor_audit.csv` confirms the same split:
  - `1991`: `source_text_missing_mta`
  - `2000`, `2003`, `2004`, `2006`, `2008`, `2009`: `total_truncated_in_layout`

## Removed or Rejected Rows
- `National Agricultural Research and Innovation Centre (Hungary)` was intentionally removed from the audited panel.
- Reason:
  - the current file inventory does not yield a stable institution-level annual total that can be reconstructed comparably across reruns
- Do not reintroduce it unless a traceable annual total is recovered from original files.

## Practical Gap Interpretation
- Missing years in Hungary do not all mean generic extraction failure.
- They fall into two distinct categories:
  - source text layer itself does not expose the institution chapter
  - source chapter is present, but the parser cannot recover the total because the multi-column total is truncated
- For the current file inventory, the non-recoverable MTA years are:
  - `1991`
  - `2000`
  - `2003`
  - `2004`
  - `2006`
  - `2008`
  - `2009`

## Likely R&D Actors In Hungary
- `Magyar Tudományos Akadémia (MTA)`
- `MTA Könyvtár és Információs Központ`
- `Nemzeti Kutatási, Fejlesztési és Innovációs Alap`
- `Kutatási és Technológiai Innovációs Alap`
- `Eötvös Loránd Kutatási Hálózat` / `Magyar Kutatási Hálózat`
- explicit `kutatás`, `fejlesztés`, `innováció`, `kutatóközpont`, `kutatóintézet` lines

## Likely False Positives
- `fejezet összesen`, `cím összesen`, `alcím összesen`, `mindösszesen` without an institution-level mapping
- broad government or ministry chapter totals
- generic infrastructure and development lines
- defence, police, welfare, pension, and non-research service operations
- broad agricultural or university operations without a defendable R&D interpretation

## Audit Artifacts To Preserve
- `Data/output/budget/Hungary/hungary_docx_series.csv`
- `Data/output/budget/Hungary/hungary_verified_overrides.csv`
- `Data/output/budget/Hungary/hungary_mta_extractor_audit.csv`
- `Data/output/budget/Hungary/hungary_docx_audit.csv`
- `Data/output/budget/Hungary/hungary_gap_report.csv`
- `Data/output/budget/Hungary/hungary_reextract_queue.csv`

## Recommended PEINSA Interpretation
- Hungary is usable now as a traced institutional series with clearly documented historical gaps.
- Do not present it as a complete annual panel from `1991–2025`.
- Treat the retained modern fund, library, and research-network rows as strong and reproducible.
- Treat the remaining missing `MTA` years as documented source/parser limitations unless better originals or a stronger regional OCR extractor are added later.
