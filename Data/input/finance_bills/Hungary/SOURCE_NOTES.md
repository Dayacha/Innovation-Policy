# Hungary — source notes

## Source family

- Annual Hungarian budget laws / `Magyar Közlöny` budget texts.
- Coverage in folder appears broad from the early 1990s to 2025.
- Text extraction from sampled PDFs is usable with `pdftotext`; this is not primarily an OCR problem.

## 5-file manual audit used for prep

- `1991 1990. évi CIV. törvény.pdf`
- `2004 2003. évi CXVI. törvény.pdf`
- `2010 MK_09_179.pdf`
- `2019 MK_18_123.pdf`
- `2024 MK_23_104.pdf`

## What the samples show

- The family is line-item friendly enough for a real large extraction, but not uniformly across time.
- Early 1990s samples look more like legal wrapper text with sparse extractable budget detail.
- Mid/late 2000s onward looks materially better for named R&D rows and programme lines.
- Amounts are commonly stated in `millió forint`; use `currency=HUF`, `unit=million` unless a table says otherwise.
- Strong R&D signals observed:
  - `MTA Könyvtár és Információs Központ`
  - `Nemzeti Kutatási, Fejlesztési és Innovációs Alap`
  - `Hazai innováció támogatása`
  - `Agrárkutatás támogatása`
  - `MTA kutatóközpontok / kutatóintézetek`
- Example evidence from sampled files:
  - `2024 MK_23_104.pdf`: explicit line saying `MTA Könyvtár és Információs Központ` receives `1950,0 millió forint` from the `Nemzeti Kutatási, Fejlesztési és Innovációs Alap`.
  - `2019 MK_18_123.pdf`: explicit `Hazai innováció támogatása`, `Nemzeti Kutatási, Fejlesztési és Innovációs Alap befizetése`, and multiple `MTA` institution lines.

## Likely in-scope entities

- `Magyar Tudományos Akadémia (MTA)`
- `MTA Könyvtár és Információs Központ`
- `Nemzeti Kutatási, Fejlesztési és Innovációs Alap`
- `Nemzeti Agrárkutatási és Innovációs Központ`
- Explicit `kutatás`, `fejlesztés`, `innováció`, `kutatóközpont`, `kutatóintézet` lines

## Likely false positives / noise

- `fejezet összesen`, `cím összesen`, `alcím összesen`, `mindösszesen`
- Road / rail / motorway / generic infrastructure development
- Defence and police lines without explicit research wording
- Social / welfare / pension / health-service operations without research wording

## Extraction stance

- Hungary is ready for a first expensive large extraction after scaffold prep.
- Expect a hybrid output:
  - named institutions/funds where present
  - programme totals when the line is clearly R&D-specific
- Expect weaker yield in the earliest years and stronger yield from the mid-2000s onward.
