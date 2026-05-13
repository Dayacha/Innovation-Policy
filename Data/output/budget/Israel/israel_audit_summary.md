# Israel Budget Audit Summary

## Status

- Compile-side Israel now builds from `Data/output/budget/results.csv`, not from the OCR-heavy text-cache/docx fallback.
- `1980-1985` no longer receives the erroneous compile-side `unit="million"` rewrite that was inflating the series by 1,000x.
- The compile base now closes with `0` missing rows and `0` temporal outliers in `israel_gap_report.csv`.
- Manual compile overrides were applied for ministry totals in:
  - `1980_Israel.pdf`, page `97`
  - `1981_Israel.pdf`, page `27`
  - `1982_Israel.pdf`, page `102`
  - `1983_Israel.pdf`, page `11`
  - `1984_Israel.pdf`, page `12`
  - `2011-2012_Israel.pdf`, page `200`
  - `2013-2014_Israel.pdf`, page `28`
  - `2015-2016_Israel.pdf`, page `31`
  - `2025_Israel.pdf`, page `48`
- Manual drops were applied for:
  - `2013` Israeli Space Agency row pulled from the `2014` half of the biannual file
  - `2019` ministry total
  - `2025` ministry total
  - `2025` Innovation Authority fragment

## Traceability Files

- `israel_series_traceability.csv`
  - one row per non-null final observation
  - includes source PDF, full-text cache path, selected page, and page excerpt
- `israel_source_traceability.csv`
  - one row per original Israel PDF
  - shows whether the file contributes to the final series or was reviewed and excluded

## Remaining Caveats

- The reincorporated `1980-1984` `National Council for R&D` values are now kept as observed original-budget totals, but they remain the weakest part of the pre-1992 panel because the source is in the `ILS_OLD` regime and OCR quality is poor.
- The reincorporated `2025` rows are now included because they appear in the source, but they are lower-confidence than the 2011/2013/2015 overrides.
- The recent `2021-2024` ministry / space rows are traceable to the original files, but some come from summary-style pages rather than clean ministry sub-tables.
  - Treat them as usable but lower-confidence than the hard overrides.

## Methodological Note

Israel is now framed as a traceable panel of observed science / innovation appropriations, including lower-confidence but explicit rows when they appear in the original budget files.

Everything retained in the final series is now traceable in the output folder.
