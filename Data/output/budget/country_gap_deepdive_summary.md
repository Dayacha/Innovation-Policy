# Country Gap Deep Dive

Generated from `gap_report`, `reextract_queue`, `run_log`, and source notes.

## Highest-priority countries

| Rank | Country | Severity | Score | Missing agency-years | Outliers | Primary issue | Missing years |
| --- | --- | --- | ---: | ---: | ---: | --- | --- |
| 1 | Germany | critical | 10576.0 | 10152 | 0 | Document ran but returned zero rows | 1955, 1975-2025 |
| 2 | Denmark | critical | 6025.5 | 5555 | 10 | Document ran but returned zero rows | 1975-2001, 2003-2025 |
| 3 | Australia | critical | 1444.7 | 928 | 9 | Document ran but returned zero rows | 1975-2026 |
| 4 | Austria | critical | 1031.0 | 731 | 0 | Needs targeted re-extraction | 1976-1986, 1988-2026 |
| 5 | UK | critical | 972.0 | 782 | 0 | Document ran but returned zero rows | 1982, 1987, 1989, 1993-1996, 1998-1999, 2003-2018, 2020-2021, 2023-2025 |
| 6 | Netherlands | critical | 911.0 | 474 | 3 | Document changed / not comparable | 1975-2025 |
| 7 | Sweden | critical | 909.7 | 624 | 62 | Outlier or unit/currency break | 1975, 1980-1995, 1997-2005, 2007, 2009-2025 |
| 8 | Iceland | critical | 504.6 | 197 | 9 | Outlier or unit/currency break | 1975-1989, 1991-2025 |
| 9 | Norway | high | 381.2 | 0 | 792 | Outlier or unit/currency break | — |
| 10 | Colombia | high | 343.0 | 125 | 0 | Document changed / not comparable | 1996-1998, 2002-2005, 2007, 2012-2014, 2016-2025 |
| 11 | Slovakia | high | 336.4 | 144 | 1 | Outlier or unit/currency break | 1992-2022, 2025 |
| 12 | Portugal | high | 288.5 | 62 | 0 | Raw rows exist but need reclassification | 1977, 1985-1986, 1988-2006, 2009-2015, 2021-2025 |
| 13 | France | high | 259.0 | 169 | 0 | Raw rows exist but need reclassification | 2005, 2007-2018 |
| 14 | Poland | high | 258.0 | 80 | 0 | Document changed / not comparable | 1991-1993, 1996-1999, 2008, 2010-2011, 2013-2025 |
| 15 | Mexico | high | 246.6 | 109 | 3 | Outlier or unit/currency break | 1986-1987, 2001-2003, 2006-2017, 2020-2021 |

## Files

- Summary CSV: `Data/output/budget/country_gap_deepdive_summary.csv`
- Detail CSV: `Data/output/budget/country_gap_deepdive_detail.csv`
- Source audit CSV: `Data/output/budget/country_gap_source_audit.csv`
