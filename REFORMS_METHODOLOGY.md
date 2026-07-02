# Reform Dataset — Methodology Note
*For internal use by the econometric team. Last updated: 2026-06-25.*

---

## 1. Source material

Reforms are extracted from **OECD Economic Surveys** — the annual or biennial peer-review reports published for each member country. The extraction covers **962 survey PDFs** spanning **38 countries** from 1961 to 2026. PDF text is extracted with PyMuPDF; for scanned/older documents, pytesseract OCR is applied as a fallback.

> **Coverage caveat — pre-1990 surveys**: 69 surveys from 1961–1969 have extracted text but the reform extraction pipeline was not yet applied to them. Additionally, 22 surveys from 1980–2016 have readable R&D content but produced empty extraction outputs (likely LLM batch failures). These are queued in `zero_extraction_requeue.csv` for a future re-run. Users relying on pre-1980 data should treat coverage as incomplete.

---

## 2. Extraction pipeline

Each survey text is chunked and passed to an LLM (GPT-4o-mini) with a structured prompt that instructs it to identify policy reform events mentioning R&D, innovation, technology, or science policy. The LLM returns structured JSON with the following fields per reform:

**Three-layer cross-verification (CAN, JPN, USA only)**: For Canada, Japan, and the United States, a second independent extraction was run using `claude-haiku-4-5-20251001`, followed by a third GPT-4o-mini extraction on a different API account. Reforms found by two or more models enter `reforms_json_merged/` with a `cross_verification_status` of `two_model_included` or `one_model_included`. This recovered 83 additional reforms (JPN:43, CAN:27, USA:13) that the single-model run missed. The remaining 35 countries use single-model (GPT-4o-mini) extraction only.

| Field | Description |
|---|---|
| `reform_id` | Unique ID: `{ISO3}_{survey_year}_{seq}` |
| `survey_year` | Year the OECD survey was published |
| `implementation_year` | Year the reform was (or is planned to be) implemented |
| `theme` / `sub_theme` | Thematic classification |
| `description` | LLM-generated summary |
| `source_quote` | Verbatim quote from the survey text (≤ 100 words) |
| `source_page_start` | Page number in the survey PDF |

**Note on source quotes**: Source quotes are copied verbatim from the extracted text. For older surveys, OCR artifacts (merged words, missing spaces) will appear in the quote. These are text extraction artifacts, not LLM confabulation. The column `source_quote_verified` (added by `reforms/source_recovery.py`) provides the exact OCR text window around the quote and a `source_match_score` (0–1) indicating match confidence. Page numbers recovered for 99.5% of reforms.

---

## 3. Two-pass filtering

All extracted reforms pass through two filtering stages before entering `reforms_kept.csv`.

### Pass 1 — Taxonomy scoring (`reforms/scoring_filter.py`)

Each reform's description and source quote are scored against a keyword taxonomy (sheets A–J of the OECD search library). The score reflects how many R&D-relevant terms appear and how prominently:

| Score | Band | Action |
|---|---|---|
| ≥ 3 | `keep` | Rule-based inclusion |
| 1–2 | `borderline` | Escalated to LLM (Pass 2) |
| ≤ 0 | `drop` | Rule-based exclusion |

**Pass 1 results**: 2,120 keep / 1,115 borderline / 1,169 drop (out of 4,404 total mentions, including 83 recovered from three-layer pipeline).

### Pass 2 — LLM adjudication (`reforms/adjudicator.py`)

- **Borderline rows** are sent to the LLM for include/exclude adjudication. The LLM also assigns K/L lens codes.
- **Keep rows** are sent to the LLM for K/L lens assignment only. However, if the LLM judges a keep-band row to be a false positive (e.g., general health funding scored high on "research funding" keywords), it returns `llm_decision = "exclude"`. **This signal is respected** — keep-band rows with `llm_decision = "exclude"` are removed from `reforms_kept.csv`.

**Pass 2 results**:
- Borderline rescued (LLM = include): 387
- Borderline excluded (LLM = exclude): 728
- Keep-band LLM-overridden (LLM = exclude): 508 ← removed from dataset

---

## 4. Final dataset: `reforms_kept.csv`

**1,999 reforms** across **38 countries**, **541 country-years**, **1962–2026**.

```
reforms_kept = (score_band == "keep" AND llm_decision ≠ "exclude")
             OR (score_band == "borderline" AND llm_decision == "include")
```

### Sub-theme distribution

| Sub-theme | Count |
|---|---|
| innovation_instruments | 608 |
| rd_funding | 602 |
| startup_ecosystem | 242 |
| knowledge_transfer | 187 |
| sectoral_rd | 113 |
| human_capital | 94 |
| research_infrastructure | 62 |
| other | 8 |

### Activity lens (K-pillar) — 98.8% coverage

| Code | Count | Description |
|---|---|---|
| K2 | 554 | Direct public R&D funding |
| K5 | 483 | Innovation instruments for firms |
| K4 | 480 | Human capital / research training |
| K6 | 149 | Knowledge transfer & commercialisation |
| K8 | 114 | Startup / venture ecosystem |
| K7 | 65 | Research infrastructure |
| K1 | 30 | Basic research / science system |
| K3 | 18 | Sectoral / mission R&D |
| NaN | 23 | Unclassified |

---

## 5. Temporal variables — what `survey_year` and `implementation_year` mean

**`survey_year`**: The year the OECD survey was published. This is when the OECD *observed and discussed* the reform. It is NOT when the reform took effect.

**`implementation_year`**: The year the reform was implemented or came into force, as stated in the survey. If the survey reports a future plan, this may be *after* `survey_year`.

**Econometric guidance**:
- For **contemporaneous analysis** (does the presence of a reform relate to that year's outcomes?): use `survey_year`.
- For **lead/lag analysis** (does a reform in year t predict outcomes in t+k?): use `implementation_year`, but filter to `year_flag == "ok"` (1,844 reforms) or `year_flag == "future_impl"` (62 reforms) only. **Exclude `year_flag == "suspicious"` (10 reforms)** — these have implementation years more than 10 years from the survey year and are likely LLM extraction errors.

---

## 6. Companion files

| File | Description |
|---|---|
| `reforms_kept.csv` | **Primary dataset** — 1,916 clean reforms |
| `reforms_events_clean.csv` | `reforms_kept` IDs filtered from cross-survey deduplicated events (1,423 rows) |
| `reforms_borderline.csv` | 728 borderline reforms excluded by LLM — for manual review |
| `reforms_dropped.csv` | 1,169 Pass-1 drops (score ≤ 0) |
| `reforms_mentions.csv` | All 4,321 raw extracted mentions with all scoring columns |
| `zero_extraction_requeue.csv` | 36 surveys (22 high-priority, 14 medium) with R&D content but zero extractions — queued for re-run |
| `reform_panel_clean.csv` | Country × year × sub-theme binary panel |
| `reform_intensity_score.csv` | Country × year weighted reform intensity metric |

---

## 7. Known limitations

1. **Pre-1980 coverage is sparse by design**: Early surveys discuss macroeconomic policy; systematic R&D policy chapters only become standard in the late 1980s. Do not interpret zero reforms before 1980 as absence of innovation policy.

2. **Survey frequency varies**: Before ~1995, many countries were surveyed biennially or irregularly. `survey_year` gaps do not imply no reform activity in intervening years.

3. **Cross-survey double-counting**: A persistent policy (e.g., a permanent R&D tax credit) may appear as a "reform" in multiple consecutive surveys. `reforms_events_clean.csv` applies cross-survey deduplication to reduce this, but some duplication remains.

4. **22 high-priority re-extraction failures**: ITA_2009, FIN_2010, JPN_1991, JPN_1992, ISR_2016, USA_1988, NOR_2005, USA_2008, CHE_1989, SWE_2012 and others produced empty extractions despite 20–95 R&D keyword hits. Results for these country-years should be treated as missing, not zero.
