# Costa Rica Source Notes

## Source Family
- Annual `Ley de presupuesto`
- Mostly `PDF`
- One auxiliary spreadsheet:
  - `Base DE DATOS Presupuestos DE LA REPUBLICA PublicO.xls`

## File Coverage
- Visible historical coverage includes:
  - `1989`
  - `2010–2025`
- Some years are split into multiple volumes / tomos:
  - `2013` has at least tomos `2, 4, 5, 6`
  - `2014` has at least tomos `1, 4, 5`
  - `2017` has at least tomos `1, 2, 3`
- This is important:
  - one fiscal year may require multiple files to recover the full budget picture

## Language / Number Format
- Spanish
- Sample check:
  - `2025` front matter explicitly says `(en colones corrientes)`
- Expect:
  - period `.` thousands separator
  - comma `,` decimal separator
- Unit is not yet fully audited across the entire historical run, so the extraction must rely on the document header.

## Structural Notes
- `2025` is very large (`1832` pages)
- Multi-volume years likely contain separate sections of the same annual budget
- `1989` sample file opens with execution and miscellaneous provisions rather than a clean modern annex structure
- The `.xls` file will NOT be discovered by the pipeline for the LLM run because pipeline discovery only includes `.pdf`, `.docx`, `.doc`
- Keep the spreadsheet as a reference source, not part of the overnight LLM batch

## Likely R&D Actors
- science / technology ministry lines
- named university transfers with research content
- health, agriculture, and environment research institutes
- innovation and applied research funds if explicitly named

## Likely False Positives
- broad education totals
- generic university operating transfers without research language
- social programmes
- roads / public works / infrastructure
- legal execution clauses and administrative boilerplate

## First Audit Targets
- `1989 act no numbers ley-7111.pdf`
- `2011_Ley de presupuesto.pdf`
- `2013_Tomo2_Ley de presupuesto.pdf`
- `2017_Tomo1_Ley de presupuesto.pdf`
- `2025_Ley de presupuesto.pdf`

## Extraction Notes
- Costa Rica is a good candidate for a broad extraction, but multi-volume years are the key risk.
- A sparse first result for a multi-volume year does NOT necessarily mean the year is weak; it may simply mean the R&D lines are in a different tomo.
- After the first compile, check whether multi-volume years are undercovered before deciding on targeted recovery.

## Quality Note
- Costa Rica is not a stable long-run panel if "R&D budget" is interpreted loosely.
- The originals mix several different objects across years:
  - annual international quotas (`CATIE`)
  - institutional transfer bundles (`CONICIT`, `PCII`)
  - programme totals (`MICITT`, some `INTA`, modern `INCIENSA`)
  - narrow sub-lines or earmarked university transfers (`UCR`)
- This means a high-recall extraction can look dense while still being methodologically inconsistent year to year.
- Current audit conclusion:
  - treat Costa Rica as a conservative, traceability-first country
  - prefer explicit programme totals or institution-level transfer bundles
  - avoid generic ministry objects, salary rows, `ING-*` revenue tables, and university sub-lines unless the research object is unambiguous
- Conservative comparable backbone retained after manual audit:
  - `CATIE`: 2011, 2013, 2017, 2020, 2021, 2024, 2025
  - `CONICIT`: 2010, 2021
  - `INTA`: 2011, 2013, 2021
  - `MICITT`: 2017, 2021, 2024, 2025
  - `INCIENSA`: 2024, 2025
  - `PCII`: 2024, 2025
- Explicitly not comparable enough for the canonical backbone:
  - `UCR` research-linked sub-lines
  - `ITCR / TEC`
  - `FEES`
  - partial `CONICIT` years sourced only from `ING-*` pages or narrow transfer fragments
  - `INTA` years represented only by third-party transfers under the INTA law
- Working assumption for future reruns:
  - if Costa Rica looks sparse after cleaning, that is usually a methodological feature, not necessarily a parser failure.
