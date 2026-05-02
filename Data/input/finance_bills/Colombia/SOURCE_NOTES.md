# Colombia Source Notes

## PEINSA Quality Note
- Colombia is a mixed-quality source family.
- The current source inventory supports a defendable final series, but not a continuous year-by-year panel.
- Some `Ley` files are full institutional budget tables.
- Some `Ley` files are only legal wrappers and say `Ver tabla` or omit annex detail.
- For modern years, the usable institutional appropriations often come from `Decreto` or `Anexo` files rather than from the `Ley` body.
- The final Colombia database should be interpreted as a traced institutional series, not as proof of complete annual coverage.

## Current Final Coverage
- Final Colombia rows in `rd_database.csv`: `50`
- Final observed years: `2002, 2004, 2005, 2012, 2013, 2014, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025`
- Final canonicals currently retained:
  - `COLCIENCIAS`
  - `MinCiencias`
  - `ICA`
  - `IDEAM`
  - `INM`
  - `Instituto Nacional de Salud`
  - `INGEOMINAS`
  - `SENA — R&D and Innovation` (`2018` only)
- All final Colombia rows are stored as `COP` in full `unit`, not `thousand`.

## Traceability Status
- Row-level audit file:
  - `Data/output/budget/Colombia/colombia_final_traceability_audit.csv`
- Current traceability split:
  - `38` rows: `verified_strong`
  - `11` rows: `verified_component_sum`
  - `1` row: `review_broad_institution`
- Historical non-recoverable gap audit:
  - `Data/output/budget/Colombia/colombia_historical_source_audit.csv`

## Strong Years / Source Pattern
- Strong direct `Ley` years:
  - `2002`
  - `2004`
  - `2005`
  - `2012`
  - `2013`
  - `2018`
- Strong modern `Decreto` / `Anexo` years:
  - `2019`
  - `2020`
  - `2021`
  - `2022`
  - `2023`
  - `2024`
  - `2025`
- Component-sum recovery years from original text:
  - `2014`
  - `2016`
  - `2017`

## Years That Need Special Interpretation
- `2014`, `2016`, `2017`:
  - Several retained rows were recovered by summing explicit institution-level components in the original text.
  - These are traceable and auditable, but not always printed as one clean `TOTAL PRESUPUESTO` line.
  - Treat them as valid, but weaker than the strongest direct institutional totals.
- `2012 INGEOMINAS`:
  - Traceable to the original file.
  - Still methodologically broad.
  - It is an institutional total, not an explicitly R&D-labelled sub-line.
  - This is the one remaining retained Colombia row that should be treated as `review_broad_institution`.

## Files / Years That Do Not Give Recoverable Information
- `1996`
  - Legal wrapper style source.
  - Budget annex detail is not preserved in the current cached text.
- `1997`
  - Legal wrapper style source.
  - No reliable institutional extraction from the current file.
- `1998`
  - The current source does not preserve the detailed numerical tables needed for traced institutional extraction.
- `2003`
  - Current file behaves as weak legal wrapper text rather than a usable institutional annex source.
- `2007`
  - Original text mentions SENA-to-COLCIENCIAS transfer language.
  - No traceable institutional appropriation amount is recoverable from the current file.
- `2009`
  - Original text mentions SENA transfer to Colciencias / Fondo Francisco Jose de Caldas.
  - No traceable institutional appropriation amount is recoverable from the current file.
- `2010`
  - Original text mentions SENA transfer to Colciencias.
  - No traceable institutional appropriation amount is recoverable from the current file.
- `2015`
  - Current cached original is truncated and does not preserve the tables.
  - Not recoverable from the present file inventory.

## Modern Source Selection Rule
- Do not assume the `Ley` is the best source for `2019+`.
- For `2019–2025`, the preferred institutional source is usually the `Decreto` or `Decreto_..._Anexo` text under:
  - `Data/output/budget/full_text/Colombia/Decreto/`
- This rule is critical for:
  - `MinCiencias`
  - `INM`
  - `INS`
- Without decree/anexo use, modern Colombia will appear artificially sparse.

## Removed or Rejected Rows
- The following were intentionally excluded from the final Colombia database even when traceable:
  - broad pre-2018 `SENA` institutional totals
  - `National Environmental Fund` / `Fondo Nacional Ambiental`
- Reason:
  - they are real budget lines or broad institutional totals,
  - but they are not sufficiently defendable as final R&D rows for PEINSA.

## Practical Gap Interpretation
- Missing years in Colombia do not all mean extraction failure.
- They fall into three different categories:
  - source genuinely weak or wrapper-only
  - source present but truncated or incomplete
  - source present but only gives transfer language without a traceable institutional amount
- For the current file inventory, the main non-recoverable years are:
  - `2007`
  - `2009`
  - `2010`
  - `2015`
- Earlier weak-wrapper years should also not be treated as ordinary fixable extraction misses:
  - `1996`
  - `1997`
  - `1998`
  - `2003`

## Likely R&D Actors In Colombia
- `COLCIENCIAS`
- `MinCiencias`
- `ICA`
- `IDEAM`
- `INM`
- `Instituto Nacional de Salud`
- `INGEOMINAS` / geology-mining research-related institutional lines
- explicit R&D-labelled `SENA` innovation lines

## Likely False Positives
- whole-institution totals with no research wording
- broad ministry section totals
- environmental or health service institutions where the line is operational rather than research-related
- transfer clauses that mention science entities but do not print an appropriation amount nearby
- programme labels that look science-adjacent but are not institutional appropriations

## Audit Artifacts To Preserve
- `Data/output/budget/Colombia/colombia_final_traceability_audit.csv`
- `Data/output/budget/Colombia/colombia_historical_source_audit.csv`
- `Data/output/budget/Colombia/colombia_gap_report.csv`
- `Data/output/budget/Colombia/colombia_reextract_queue.csv`

## Recommended PEINSA Interpretation
- Colombia is usable now as a traced institutional series with documented gaps.
- Do not present it as a complete annual budget series.
- Treat `2014`, `2016`, `2017` as valid but slightly weaker than clean direct-total years.
- Treat `2012 INGEOMINAS` as real and traceable, but methodologically cautious.
- Treat `2007`, `2009`, `2010`, and `2015` as documented source limitations unless better originals are added later.
