# Latvia — source notes

## Source family

- Annual Latvian budget laws.
- The family is mixed:
  - early 1990s and many 2014+ files behave like short legal wrappers;
  - mid-1990s through the 2000s include much richer programme tables.
- Folder coverage is broad from 1991 to 2025.
- There is at least one legacy Word file: `2003 BUDZETS.DOC`.

## Extractor finding

- The pipeline previously failed on legacy `.doc` because it tried to read `.doc` with the `.docx` path.
- This has now been fixed in `budget/pdf_reader.py` using `textutil` fallback for `.doc`.

## 20-document audit summary

- Audited 20 source files in 4 blocks of 5.
- Strong recurring R&D signals found:
  - `Kopā zinātnes finansēšanai`
  - `Zinātne`
  - `Fundamentālie zinātniskie pētījumi`
  - `Valsts pārvaldes institūciju pasūtītie zinātniskie pētījumi`
  - `Zinātnes bāzes finansējums`
  - `Latvijas Zinātnes padomes darbības nodrošināšana`
  - `Zinātniskās darbības attīstība universitātēs`
  - `Zinātniskās infrastruktūras nodrošināšana un attīstība augstskolās`
  - `Investīcijas zinātnei`
  - `Latvijas Zinātņu akadēmija`

## What the samples show

- Latvia is viable for a first large extraction.
- The best extractable series is likely hybrid/programmatic rather than purely institutional.
- Detailed examples:
  - `1993`: explicit `Kopā zinātnes finansēšanai` and `Latvijas Zinātņu akadēmija kopā`
  - `1996`: explicit science programme block with `Fundamentālie zinātniskie pētījumi`, `LZA Kodolpētniecības centrs`, `Investīcijas zinātnei`
  - `2006`: explicit `Zinātnes bāzes finansējums`, `Latvijas Zinātnes padomes darbības nodrošināšana`, `Zinātnes konkurētspējas veicināšana`
  - `2016`: earmark for `zinātniskās darbības attīstībai augstskolās un koledžās` with `5 000 000 euro`
  - `2019`: earmark for innovative ICT study programme support

## Units and currency

- Working rule from audited files:
  - pre-2014: `LVL`, full currency units
  - 2014+: `EUR`, full currency units
- Do not assume thousands or millions unless the header explicitly says so.

## Likely false positives

- Broad `Izglītības un zinātnes ministrija` totals
- Police / defence academies
- University hospitals
- Sports and student-credit lines
- Short legal-wrapper pages with ministry mentions but no science row

## Extraction stance

- Latvia is ready for a first expensive run after scaffold prep.
- Expect:
  - richer yield in 1996–2013
  - weaker yield in 1991–1995 and many 2014+ short-law files
  - a hybrid panel based on science programmes plus named science institutions
