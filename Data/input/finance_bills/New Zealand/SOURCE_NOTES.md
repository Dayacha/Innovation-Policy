# New Zealand — source notes

## Source family

- Annual New Zealand appropriation estimates / appropriations acts.
- Coverage in folder appears strong from 1975 through 2025.
- Text extraction quality is generally good; this is not primarily an OCR problem.

## 20-file manual audit used for prep

- Batch 1:
  - `1975_Appropriation Estimates Act.pdf`
  - `1976_Appropriation Estimates Act.pdf`
  - `1977_Appropriation Estimates Act.pdf`
  - `1978_Appropriation Estimates Act.pdf`
  - `1979_Appropriation Estimates Act.pdf`
- Batch 2:
  - `1985_Appropriation Estimates Act.pdf`
  - `1987_Appropriation Estimates Act.pdf`
  - `1989_Appropriation Estimates Act.pdf`
  - `1990_Appropriation Estimates Act.pdf`
  - `1994_Appropriation Estimates Act.pdf`
- Batch 3:
  - `2002_Appropriation Estimates Act.pdf`
  - `2003_Appropriation Estimates Act.pdf`
  - `2007_Appropriation Estimates Act.pdf`
  - `2012_Appropriation Estimates Act.pdf`
  - `2013_Appropriation Estimates Act.pdf`
- Batch 4:
  - `2014_Appropriation Estimates Act.pdf`
  - `2015_Appropriation Estimates Act.pdf`
  - `2021_Appropriation Estimates Act.pdf`
  - `2023_Appropriation Estimates Act.pdf`
  - `2025_Appropriation Estimates Act.pdf`

## What the samples show

- New Zealand is ready for a real large extraction.
- The source family supports a defendable hybrid panel across three eras:
  - DSIR / Scientific and Industrial Research
  - Crown Research / FRST transition
  - Modern Science, Innovation and Technology appropriations
- Strong observed signals:
  - `Department of Scientific and Industrial Research`
  - `Research, Science and Technology`
  - `Crown Research Institutes`
  - `Science, Innovation and Technology`
  - `Endeavour Fund`
  - `Health Research Fund`
  - `Marsden Fund`
  - `Partnered Research Fund`
  - `Catalyst Fund`
  - `Callaghan Innovation - Operations`

## Main extraction risk

- The word `Development` is a major false-positive source in New Zealand appropriations.
- Many non-R&D programmes contain `Development` with no science content.
- The cleaner/profile therefore demote non-science development lines unless they also contain an explicit research/science signal.

## Unit and currency stance

- Treat amounts as `NZD` in `thousand` units unless the source explicitly states otherwise.
- Modern appropriations typically behave consistently enough for this stance.

## Likely in-scope entities

- `Department of Scientific and Industrial Research (DSIR)`
- `Foundation for Research, Science and Technology (FRST)`
- `Crown Research Institutes`
- `Marsden Fund`
- `Callaghan Innovation`
- `Endeavour Fund`
- `Health Research Fund`
- `Partnered Research Fund`
- `Catalyst Fund`
- Named CRIs such as `AgResearch`, `NIWA`, `GNS Science`, `Plant and Food Research`

## Likely false positives / noise

- Generic `Development` lines without science/research wording
- Housing, regional, social, or war-development appropriations
- Generic tertiary support without explicit research wording
- Broad ministry administration without a science/research signal

## Extraction stance

- New Zealand is ready for a first expensive large extraction.
- Expect a stronger and cleaner panel than Lithuania.
- Modern years should produce the best structured R&D output.
