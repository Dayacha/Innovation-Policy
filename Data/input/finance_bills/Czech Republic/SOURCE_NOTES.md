# Czech Republic Source Notes

## Source Family
- State budget law (`Zákon o státním rozpočtu`)
- Important annex / appendix files (`Přílohy`)
- Mixed corpus:
  - `PDF`
  - `DOCX`

## File Coverage
- Visible coverage begins in `1993`
- Broad continuity through `2025`
- Important split in the source family:
  - some years are main law PDFs only
  - many years have annex PDFs (`Přílohy`) that likely carry the real detail
  - `2002`, `2003`, `2004` include `.docx` annex-content files

## Language / Number Format
- Czech
- Number format:
  - space or period for thousands
  - comma for decimal
- Sample checks:
  - `1997` annex says `v mil. Kč`
  - `2024` law text shows full `Kč` totals in the statute itself

## Structural Notes
- The main law text may be too aggregated for institution-level recovery.
- The annexes are likely the critical files for detailed spending lines.
- Therefore:
  - do not evaluate Czech extraction quality using only the main annual law PDFs
  - the annex family is probably where R&D lines actually live
- There is one visible anomaly:
  - `1993 2025-12-20_Zakon-c-434-2024-Sb.pdf`
  - filename likely misfiled or mislabeled and should be treated cautiously

## Likely R&D Actors
- Academy of Sciences of the Czech Republic
- Grant Agency / research grant bodies
- technology / innovation agencies
- named ministries with science and research chapters
- universities and research institutes where explicitly identified

## Likely False Positives
- broad chapter totals
- municipal/regional transfers
- transport infrastructure
- defence and interior spending without research signal
- legal macro aggregates from the main law text

## First Audit Targets
- `1997 Zak_1996-315_Prilohy-k-zakonu-c-3151996-Sb.pdf`
- `2002 czech_budget_annexes_CONTENT_2002.docx`
- `2005 Zak_2004-675_Prilohy-k-zakonu-c-6752004-Sb-o-statnim-rozpoctu-Ceske-republiky-na-rok-2005.pdf`
- `2018 Zak_2017-474_Zakon-o-statnim-rozpoctu-Ceske-republiky-na-rok-2018.pdf`
- `2024 2023-12-29_Zakon-o-statnim-rozpoctu-Ceske-republiky-na-rok-2024.pdf`

## Extraction Notes
- Czech Republic is worth the expensive run, but only if the annexes are included as first-class sources.
- Expect the main law text to overproduce aggregate totals unless the cleaner/profile/canonical logic is conservative.
- After first extraction, compare annex years against non-annex years before drawing conclusions about long-run gaps.
