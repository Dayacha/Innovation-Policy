# Lithuania — source notes

## Source family

- Annual Lithuanian budget-law texts and related budget annex documents.
- Folder coverage appears broad from the early 1990s through 2025.
- The source family is mixed:
  - some years expose useful programme tables and institution lines
  - some years behave more like short legal wrappers with thin budget detail
- A practical extraction issue was confirmed during audit: several `.docx` files yield empty text through `python-docx` but do produce usable text through `textutil`. The pipeline fallback was patched for this.

## 20-file manual audit used for prep

- Batch 1:
  - `1992 TAR.143EC1A56B4B.pdf`
  - `1993 TAR.8C4914C2ACED.docx`
  - `1994 TAIS_33485.docx`
  - `1995 TAIS_25247.docx`
  - `1996 TAR.11F4B795287C.docx`
- Batch 2:
  - `2000 TAR.689A5959367D.docx`
  - `2003 TAR.BCA0F623B8BA.docx`
  - `2005 TAIS_259480.pdf`
  - `2006 TAR.1BAE24CE65A7.docx`
  - `2007 TAR.802CCF0B0455.docx`
- Batch 3:
  - `2008 TAR.E51A2DE98B9E.docx`
  - `2010 TAR.E5C7DCAD90FA.docx`
  - `2011 TAR.FE51590E2B56.docx`
  - `2013 TAR.CABB5B7DAFB1.docx`
  - `2016 12-2161.docx`
- Batch 4:
  - `2019 XIII-1710.docx`
  - `2020 XIII-2695.docx`
  - `2021 AR_2021-07-01.pdf`
  - `2022 XIV-745.docx`
  - `2025 XV-89.docx`

## What the samples show

- Lithuania is usable for a first large extraction, but the likely output is hybrid/programmatic rather than a perfectly homogeneous institutional panel.
- Strong recurring R&D signals observed:
  - `Mokslas ir studijos`
  - `Valstybinė mokslo, studijų ir technologijų tarnyba`
  - `Lietuvos mokslo taryba`
  - `moksliniai tyrimai`
  - `valstybinės mokslo ir studijų institucijos`
- Stronger early evidence:
  - `1992`: science/technology progress and academy-related lines
  - `1993–1995`: state science/studies/technology service and science/studies language
  - `2000/2003/2005/2006`: explicit research/studies institution and ministry lines visible after fallback extraction
- Modern years often contain many education/ministry references but fewer directly extractable R&D rows.

## Unit and currency stance

- Pre-2015 budgets are expected in `LTL` full units unless the page/header says otherwise.
- 2015+ budgets are expected in `EUR` full units unless the page/header says otherwise.
- Always prioritize the table header if it explicitly states a different scale.

## Likely in-scope entities

- `Lietuvos mokslo taryba`
- `Valstybinė mokslo, studijų ir technologijų tarnyba`
- `Mokslas ir studijos`
- Explicit `moksliniai tyrimai` / scientific research lines
- Explicit state research and studies institution appropriations

## Likely false positives / noise

- Broad `Švietimo ir mokslo ministerija` totals without explicit research content
- Student support, study credits, tuition, and `moksleivio krepšelis`
- Sports lines
- Generic higher-education operating transfers without a research signal
- Generic innovation/economic support lines without explicit science/research content

## Extraction stance

- Lithuania is ready for a first expensive large extraction after scaffold prep.
- Expect the best yield in years where the source exposes research/studies programme tables.
- Expect weaker coverage in short-wrapper years and in modern years dominated by ministry/legal framing.
