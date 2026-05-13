# Korea Source Notes

## PEINSA Quality Note
- Korea is a mixed-quality source family.
- The current source inventory supports a defendable final series, but not a clean institutional budget panel.
- Most usable PDFs are budget briefs, summaries, `홍보자료`, and fiscal-plan pages rather than classic appropriation ledgers.
- The final Korea database should be interpreted as a conservative programme/theme series with a small audited ministry subtotal, not as proof of complete annual ministry coverage.

## Current Final Coverage
- Final Korea rows in `rd_database.csv`: `17`
- Final observed years: `2018, 2019, 2021, 2022, 2023, 2024, 2025`
- Final canonicals currently retained:
  - `Ministry of Science and ICT (Korea)`
  - `National R&D Programmes (Korea)`
  - `Strategic Technology R&D Programmes (Korea)`
- All final Korea rows are stored as `KRW` in `thousand` units.

## Traceability Status
- Row-level verified override file:
  - `Data/output/budget/Korea/korea_verified_overrides.csv`
- Current traceability status:
  - `17` rows: manually verified against the current original Korea files
- Current gap audit:
  - `Data/output/budget/Korea/korea_gap_report.csv`

## Source Family
- The folder mixes `PDF` and `HWP` files.
- Current pipeline discovery supports `.pdf`, `.docx`, `.doc`.
- `.hwp` is not yet part of the extraction path.
- This matters for Korea:
  - some potentially better institutional detail may exist in the HWP files,
  - but the current final panel only uses the supported PDF family.

## File Coverage
- Current visible year coverage in the Korea input folder is:
  - `2018`
  - `2019`
  - `2021`
  - `2022`
  - `2023`
  - `2024`
  - `2025`
- There is no recoverable `2020` source in the current folder.
- There is no long pre-2018 annual run in the current supported PDF inventory.
- The folder also contains:
  - `leaflet.pdf`
  - `예산안 인포그래픽 1.pdf`
  - two `HWP` files

## What The Current Audit Showed
- Korea extraction does not fail mainly because of postprocess.
- The real bottlenecks are:
  - short source coverage,
  - unsupported `HWP`,
  - OCR-poor or infographic-style PDFs,
  - summary documents that mix macro totals, programme packages, and ministry examples.
- `results.csv` for Korea was materially richer than the legacy text-cache path.
- The current final Korea panel therefore depends on:
  - pipeline-first extraction,
  - manual verification of a small number of defensible totals/subtotals.

## Strong Years / Source Pattern
- Strong annual `National R&D` total years:
  - `2018`
  - `2019`
  - `2021`
  - `2022`
  - `2023`
  - `2024`
  - `2025`
- Strong `Strategic Technology` subtotal years:
  - `2018`
  - `2019`
  - `2022`
  - `2023`
  - `2024`
  - `2025`
- Strong `MSIT` subtotal years:
  - `2022`
  - `2023`
  - `2024`
  - `2025`

## Years That Need Special Interpretation
- `2018` and `2019`:
  - usable for `National R&D` and a strategic-technology-style subtotal,
  - but weak for a clean `MSIT` annual subtotal.
- `2021`:
  - usable for the annual `National R&D` total,
  - not currently strong enough for a defendable `MSIT` subtotal,
  - not currently strong enough for a defendable `Strategic Technology` subtotal.
- `2022–2025`:
  - the strongest part of the current Korea panel,
  - but still based mainly on budget briefs and summary pages rather than line-by-line institutional ledgers.

## Files / Years That Do Not Give Recoverable Information
- `2023년 예산안_건전재정기조 확립.pdf`
  - OCR-poor summary brief.
  - Produced no usable final extraction rows.
- `3. 2023년 예산안 인포그래픽★.pdf`
  - infographic-style source.
  - Produced no usable final extraction rows.
- `2024 예산안-성장동력 확보를 위한 미래준비 투자.pdf`
  - weak automatic extraction.
  - Useful for targeted manual reading, but not a strong general extraction source.
- `leaflet.pdf`
  - contains some budget language,
  - but behaves like a PR / overview leaflet rather than a stable annual budget source.
- `예산안 인포그래픽 1.pdf`
  - infographic-style source with poor extraction value.
- `2020`
  - not recoverable from the current source folder because the file is not present.

## Unit / Number Notes
- Korea source units are document-dependent and must be read from the page:
  - `조원`
  - `억원`
  - sometimes full `원`
- Do not infer scale from number size alone.
- Several Korea summary pages mix abbreviated display figures with translated extraction rows.
- For the final panel, the retained Korea values were manually normalised to `thousand KRW`.

## Likely R&D Actors In Korea
- `Ministry of Science and ICT`
- `National R&D` / `Government R&D` annual totals
- strategic technology / super-gap technology budget groupings
- explicit programme lines tied to:
  - `AI`
  - `semiconductor`
  - `quantum`
  - `bio`
  - `space`

## Likely False Positives
- broad macro totals such as:
  - `총지출`
  - `총수입`
  - `재정수지`
  - `국가채무`
- infographic pages
- PR / summary narrative pages
- defence, welfare, housing, labour, or regional support pages without clear R&D appropriation wording
- one-off thematic captions that look science-adjacent but are not stable annual comparables

## Final Korea Interpretation
- The current Korea panel is usable, but narrow.
- It should be interpreted as:
  - one annual `National R&D` total series,
  - one audited `MSIT` subtotal series where explicit pages exist,
  - one audited `Strategic Technology` subtotal series where explicit pages exist.
- It should NOT be interpreted as:
  - a complete ministry-by-ministry public R&D panel,
  - a complete annual institutional budget history,
  - a full inventory of Korean public R&D actors.

## Remaining Documented Gaps
- `MSIT`:
  - `2018`
  - `2019`
  - `2021`
- `Strategic Technology`:
  - `2021`
- These are currently documented source/extraction limits in the present inventory, not ordinary postprocess misses.

## Audit Artifacts To Preserve
- `Data/output/budget/Korea/korea_verified_overrides.csv`
- `Data/output/budget/Korea/korea_gap_report.csv`
- `Data/output/budget/Korea/korea_reextract_queue.csv`

## Recommended PEINSA Interpretation
- Korea is usable now as a conservative audited panel.
- Do not present it as a complete institutional R&D budget history.
- The strongest interpretation is:
  - annual `National R&D` totals,
  - audited `MSIT` subtotals in the years where they are explicit,
  - audited strategic-technology subtotals in the years where they are explicit.
- `2021 MSIT` and `2021 Strategic Technology` should remain open unless better originals or HWP support are added later.
