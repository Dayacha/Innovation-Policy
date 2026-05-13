# Israel Quality Note

## Bottom Line
- Israel is usable as a traceable budget panel, but not as a perfectly uniform annual institutional ledger.
- The audited series is intentionally conservative and is anchored in code-level manual curation.
- Sparse coverage for some institutions is a methodological choice, not necessarily a parser failure.

## Main Comparability Problem
- The original files change structure and evidentiary quality across eras:
  - old scanned PDFs with unstable OCR
  - multiple currency regimes (`ILP`, `ILS_OLD`, `ILS`)
  - occasional biannual budgets
  - modern summary-style budget pages mixed with cleaner institutional tables
- A high-recall extraction therefore overstates comparability if all rows are treated as equivalent annual R&D observations.

## Units By Era
- `1975–1979`: `ILP`, `unit`
- `1980–1984`: `ILS_OLD`, `thousand`
- `1985`: `ILS_OLD`, `million`
- `1986–2025`: `ILS`, mostly `thousand`
- These are based on the printed source scale in the original files, not on a single normalization rule carried across all years.

## Conservative Audit Rule
- Keep only observations that are explicit, traceable, and reasonably comparable.
- Prefer:
  - named ministry totals with a clear science/R&D object
  - explicit `National Council for R&D` totals in the pre-1992 era
  - explicit `Chief Scientist` / `Innovation Authority` lines
  - named agency or fund rows such as `Israeli Space Agency` and `KAMEA`
- Exclude:
  - noisy summary ghosts
  - broad page totals with weak institutional attribution
  - OCR fragments that look numeric but are not defendable annual appropriations
  - opportunistic discovery rows that do not recur as a stable institutional series

## Conservative Comparable Backbone
- `National Council for R&D (Israel, pre-1992)`: 1975–1991
- `Ministry of Science and Technology (Israel)`: 1992–2025
- `Office of the Chief Scientist (Israel, pre-2016)`: 1986, 2013
- `Israel Innovation Authority (from 2016)`: 2017, 2025
- `Israeli Space Agency`: sparse but explicit appearances in 1995, 2004, 2005, 2007, 2011, 2019, 2021, and 2022
- `KAMEA Fund`: sparse but explicit appearances in 1987 and 1991

## Explicitly Weaker But Included
- `1980–1984`: included because the source files do contain usable `National Council for R&D` evidence, but these scans are among the weakest in the series.
- `2023–2025`: included because the files do contain usable budget evidence, but the ministry rows behave as `Section 19` bundle totals rather than clean ministry-only science appropriations.
- `2025`: the `Israel Innovation Authority` row is explicit and defendable, but the accompanying ministry row should still be read as the broader `science + culture` bundle.

## Interpretation Guidance
- Read Israel as a traceability-first institutional panel.
- The strongest continuity is the bridge from `National Council for R&D` before 1992 to `Ministry of Science and Technology` from 1992 onward.
- `2023–2025` ministry should be read as `Section 19 total (science + culture bundle)`, not as a pure ministry-of-science series.
- Smaller institutions and funds should be interpreted as explicit source-presence series, not as a promise of dense annual continuity.
- If some institutions look sparse after cleaning, that is usually methodological conservatism rather than missing data engineering.
