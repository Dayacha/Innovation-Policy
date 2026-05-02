# Korea — source notes

## Source family

- The folder mixes PDF and HWP files.
- Current pipeline supports `.pdf`, `.docx`, `.doc` discovery only; `.hwp` is not yet part of the extraction path.
- The sampled PDFs are mostly budget proposal summaries / briefs / 홍보자료, not classic appropriation ledgers.

## 5-file manual audit used for prep

- `2018년도 예산안 개요.pdf`
- `2019년도 예산안 개요.pdf`
- `3. 2022년 예산안.pdf`
- `2023년 예산안_건전재정기조 확립.pdf`
- `2. 2025 예산안 홍보자료.pdf`

## What the samples show

- Strong thematic R&D language appears often:
  - `R&D`
  - `연구개발`
  - `AI`
  - `반도체`
  - `혁신`
  - `과학기술`
- The family is weak for institutional appropriation extraction.
- `2018`, `2019`, and `2022` PDFs contain extractable text but mostly high-level narrative and macro budget framing.
- `2023년 예산안_건전재정기조 확립.pdf` yields almost no usable text via `pdftotext`.
- `2017년도 예산안.pdf`, `3. 2020년 예산안.pdf`, and `3. 2021년 예산안.pdf` also yielded effectively zero machine-readable text via `pdftotext`.
- The currently visible evidence supports programme/theme-level extraction, not a clean agency panel.

## Concrete programme-level positives from the larger review

- `2. 2024년 예산안 홍보자료.pdf` contains extractable strategic-technology R&D lines such as:
  - `AI` with `7,371억원`
  - `첨단바이오` with `9,626억원`
  - `양자` with `1,252억원`
  - `KARPA-H 프로젝트` with `495억원`
  - `우주산업 클러스터` with `100억원`
- These are usable as conservative `program_total` style rows if the extractor is strict.

## Unit / number notes

- Units are document-dependent and explicitly stated in the text:
  - `조원`
  - `억원`
  - sometimes full `원`
- Do not infer scale from number size alone.

## Likely in-scope material

- Explicit amount + named R&D programme/theme pairs only
- Ministry of Science and ICT totals only when they are clear annual appropriations
- Programme lines tied to `연구개발`, `국가연구개발`, `AI`, `반도체`, `우주`, `바이오`, `양자`

## Likely false positives / noise

- `총지출`, `총수입`, `재정수지`, `국가채무`
- Macro narrative / PR / infographic pages
- Loans, guarantees, and industrial financing announcements without explicit R&D appropriation wording
- Welfare, housing, labour, or regional-support pages without R&D signal

## Extraction stance

- Korea is not ready for a clean institutional series from this source family.
- It is still viable for a conservative first large extraction if the goal is to capture programme-level R&D budget signals from the PDFs already supported by the pipeline.
- Real upgrade path later: add an HWP ingestion path, because some better source detail may live there.
