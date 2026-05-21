# Slovenia Pending Gap Manual Review

Updated after manual PDF review and recompilation on 2026-05-20.

## Resolved now

| Years | Canonical | What we verified | Action taken |
| --- | --- | --- | --- |
| 2016-2018 | `Programme 0503 — Mladi raziskovalci / Človeški viri v podporo znanosti` | The original PDFs contain explicit `050302 Mladi raziskovalci, mobilnost in spodbude najboljšim raziskovalcem` amounts: `170,000 EUR` in 2016, 2017, and 2018. | Added locked `verified_override` rows in `budget/manual_curation.py` and recompiled Slovenia. These years are no longer pending gaps. |

## Remaining pending gaps

| Years | Canonical | Source evidence from original documents | What failed in pipeline / audit | Can we fix it ourselves now? | Recommended action |
| --- | --- | --- | --- | --- | --- |
| 1995-2000 | `Programme 0502 — Znanstveno raziskovalna dejavnost` | The source family contains research-related text, but the surviving 0502 rows are zero totals, OCR artefacts, or tiny sub-lines rather than a defendable annual programme total. In `1998 u1998034.pdf` and `1999 u1998091.pdf`, the extracted `Skupaj 0502` amount is `0`. | Not a parser crash. The extractor saw 0502-like rows, but the audit rejected them as non-comparable / unusable. | No safe automatic fix with current files. | Keep the gaps unless a manual page-by-page audit finds a clean annual 0502 total in the original scans. |
| 2001, 2003-2015, 2025 | `Programme 0503 — Mladi raziskovalci / Človeški viri v podporo znanosti` | The files mostly expose generic `0503` bundles, SAZU support lines, electronic-communications spillover, zero totals, or implausible package amounts. `2004 2005 u2013102.pdf` is not a budget annex at all: page 1 identifies it as `Zaključni račun proračuna Republike Slovenije za leto 2012`, published in 2013. In `2025 u2024104.pdf`, the PDF shows `0503` aggregate totals and `050301` SAZU support, but no defendable annual `050302` line survives. | Mostly downstream audit / comparability rejection, not extraction failure. One reviewed file is misfiled source inventory (`2004 2005 u2013102.pdf`). | Only partially. We already closed the years with explicit `050302` lines; the remaining years still lack a defendable comparable target row. | Keep the gaps. For 2004/2005, exclude the misfiled closing-account PDF and only revisit if a true annual budget source is found. |
| 2010 | `ARRS — Agencija za raziskovalno dejavnost Republike Slovenije` | `2010 u2009099.pdf` exposes ARRS-adjacent rows such as `CRP za ARRS` and ARRS equipment/project lines, but not a clean `Delovanje ARRS/ARIS` operating appropriation consistent with the locked ARRS series. | Downstream canonical-series issue: extraction found ARRS text, but not the defendable operating row used elsewhere in the final panel. | Not safely. Auto-filling from `CRP za ARRS` would change the object being measured. | Keep the gap unless the original budget table yields an explicit `Delovanje ARRS/ARIS` annual operating amount. |
| 2011-2012 | `SAZU — Slovenska akademija znanosti in umetnosti` | The 2011/2012 source family does not expose a defendable SAZU institution-total row in the extracted evidence. The 2012 fragments are sub-lines such as donations, investments, and support activities, not the institution total. | Extraction returned rows, but the audit rejected them because they are not institution-total observations comparable with the retained SAZU series. | No safe automatic fix with current evidence. | Keep the gaps unless a manual audit of the original budget summary pages finds an explicit SAZU institution-total row for those two years. |

## Files touched in this review

- `budget/manual_curation.py`
- `Data/output/budget/Slovenia/slovenia_docx_series.csv`
- `Data/output/budget/Slovenia/slovenia_gap_report.csv`
- `Data/output/budget/Slovenia/slovenia_verified_overrides.csv`
- `Data/output/budget/Slovenia/slovenia_country_gap_review_table.csv`
- `Data/input/finance_bills/Slovenia/SOURCE_NOTES.md`
