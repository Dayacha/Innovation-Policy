# Belgium Source Notes

## Source Family
- Annual finance laws / federal budget law PDFs
- Mostly French-titled files in the current folder
- Likely line-item legal budget annexes rather than narrative speeches

## File Coverage
- Current folder begins at `1993`
- Visible coverage runs through the 2000s and 2010s
- Some years have multiple files:
  - `2000` has at least two PDFs
  - `2001` has at least two PDFs

## Language / Number Format
- Likely French and/or Dutch
- Expect European number formatting:
  - period `.` as thousands separator
  - comma `,` as decimal separator

## Likely R&D Actors
- Belgian federal science policy body / science policy office
- FNRS / FWO-style research councils may appear, but check whether federal or regional
- Royal observatory / meteorology / nuclear / space contributions may appear

## Likely False Positives
- broad federal ministry totals
- social security / pensions
- transport / infrastructure
- defence unless explicitly research-labelled

## First Audit Targets
- `1993 08_1.pdf`
- `2000 Belgium 50K0197001.pdf`
- `2001 Belgium 50K0904001.pdf`
- `Loi de finances 2004.pdf`
- one recent law file

## Extraction Notes
- Verify whether the budget is federal only or mixes regional/community structures
- Check if R&D is carried by agencies or by programme/chapter lines
- Be cautious with duplicate files in the same year

## Audited Belgium-Specific Findings
- `1994`, `1998`, and `2000` BELSPO were closed from verified historical columns in the following year's budget tables, not from the current-year OCR rows.
- `2012` and `2013` science-policy tables in the current corpus are `janvier-mars` / `januari-maart` provisional appropriations rather than full-year annual budgets. Do not use them as annual BELSPO series points.
- `2008`, `2022`, `2023`, and `2024` remain genuine BELSPO extraction gaps:
  - the current source files do contain Section 46 / `PROGRAMME 60/2` science-policy text;
  - however, the extracted rows are narrative / `no_amount`, not annual numeric budget lines;
  - `2023` is additionally contaminated by source/page-selection drift, with parsed output mixing other sections instead of a clean science-policy annex.
- Deep-dive on later files:
  - `2021` and `2022` do expose the expected `PROGRAMME 60/2` / `PROGRAMMA 60/2` headings for international R&D and list the usual ESA / Eureka / von Karman lines, but the current text extraction still carries no usable amounts.
  - `2024` contains the correct science-policy section and likely a later OCR table fragment, but that fragment is too garbled to map cleanly and defensibly back to annual BELSPO line items.
  - `2025` is effectively a short legal authorization page for Section 46 and does not expose a usable annual amount table in the current file.
- Practical implication:
  - do not force late-year Belgium BELSPO values from the current OCR text;
  - prefer better annex selection, page-scoped extraction, or a different source document for those years.
