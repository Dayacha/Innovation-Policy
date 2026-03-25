# Technical Reference

Full developer reference for the Innovation Policy Pipeline.
For the research context and quick start, see [README.md](README.md).

---

## Table of contents

1. [Project structure](#project-structure)
2. [Configuration](#configuration)
3. [Running the pipeline](#running-the-pipeline)
4. [Pipeline 1 — Budget extraction](#pipeline-1--budget-extraction)
5. [Pipeline 2 — Reform extraction](#pipeline-2--reform-extraction)
6. [Output schemas](#output-schemas)
7. [Incremental behavior and caching](#incremental-behavior-and-caching)
8. [Adding new countries](#adding-new-countries)
9. [Cost estimation (reform pipeline)](#cost-estimation-reform-pipeline)
10. [Dependencies](#dependencies)

---

## Project structure

```
Innovation-Policy/
│
├── main.py                   Unified entry point — runs both pipelines
├── pipeline_reforms.py       Reform pipeline orchestration logic
├── config.yaml.example       Configuration template (copy to config.yaml)
├── requirements.txt          Python dependencies
│
├── budget/                   Budget pipeline modules (Finance Bills)
│   ├── config.py             All path constants and extraction parameters
│   ├── inventory.py          PDF file discovery and content hashing
│   ├── pdf_extract.py        Text extraction with OCR fallback
│   ├── budget_extractor.py   Taxonomy scoring and section parsing
│   ├── keyword_detection.py  Multilingual keyword matching
│   ├── reporting.py          Budget item detection entry point
│   ├── taxonomy.py           Excel/JSON taxonomy loader and scorer
│   ├── section_parser.py     Danish § structure and amount parser
│   ├── exporters.py          Full-text export (.txt.gz per document)
│   ├── ai_validation.py      Optional LLM post-processing pass
│   ├── ai_client.py          OpenAI client wrapper
│   ├── translation_utils.py  Glossary-based translation helper
│   └── utils.py              Logging, hashing, normalization helpers
│
├── reforms/                  Reform pipeline modules (OECD Economic Surveys)
│   ├── reform_analyzer.py    Core LLM extraction + within-survey dedup
│   ├── panel_builder.py      Cross-survey dedup + panel construction
│   ├── llm_client.py         Unified Anthropic/OpenAI client with retry + cost tracking
│   ├── prompts.py            LLM prompt templates + innovation taxonomy (8 sub-types)
│   ├── extractor.py          PDF-to-text extraction (pdfplumber)
│   ├── catalog.py            Survey catalog management
│   ├── countries.py          OECD country list and ISO codes
│   ├── pipeline_reforms.py   Reform pipeline orchestration logic
│   └── downloader.py         Best-effort PDF download from OECD iLibrary
│
└── data/
    ├── input/
    │   ├── finance_bills/    Finance Bill PDFs  → budget pipeline
    │   │   └── <Country>/    one subfolder per country
    │   ├── surveys/          OECD Economic Survey PDFs  → reform pipeline
    │   │   └── <ISO3>_<YEAR>.pdf
    │   └── taxonomy/         Reference files used by the budget pipeline
    │       ├── search_library.json       pre-processed taxonomy (fast)
    │       ├── Full search library.xlsx  original taxonomy (Balazs)
    │       └── translation_glossary.json glossary for EN translation
    └── output/
        ├── budget/           All budget pipeline outputs
        │   ├── results.csv              main results (human-readable)
        │   ├── results.xlsx             same, formatted for Excel review
        │   ├── results.json             same, structured for downstream code
        │   ├── results_ai_verified.csv  rows confirmed by AI validation
        │   ├── results_review_status.csv each row marked reviewed/pending
        │   ├── intermediate/            pipeline cache files (re-runnable)
        │   │   ├── file_inventory.csv
        │   │   ├── page_text.csv
        │   │   ├── file_text_summary.csv
        │   │   ├── budget_items_detected.csv
        │   │   ├── innovation_candidates.csv
        │   │   └── keyword_hits.csv
        │   ├── full_text/               OCR text cache (.txt.gz per PDF)
        │   ├── ai_validation/           AI validation run outputs
        │   └── runs/                    timestamped snapshots of past runs
        └── reforms/          All reform pipeline outputs
            ├── extracted_text/          plain text extracted from survey PDFs
            ├── reforms_json/            per-survey LLM extraction results
            └── output/
                ├── reform_panel.csv
                ├── reform_panel_subtheme.csv
                ├── reforms_events.csv
                ├── reforms_mentions.csv
                ├── oecd_recommendations.csv
                └── summary_statistics.txt
```

---

## Configuration

All settings live in `config.yaml` (copy from `config.yaml.example`).

### LLM settings (shared across both pipelines)

```yaml
llm:
  provider: "anthropic"       # "anthropic" or "openai"
  api_key: "sk-..."           # or set ANTHROPIC_API_KEY / OPENAI_API_KEY env var
  model: "claude-sonnet-4-6"  # see cost table below for alternatives
  temperature: 0              # 0 = deterministic (recommended)
  max_tokens: 4096
```

### Budget pipeline settings

```yaml
budget:
  ai_validation_model: "claude-sonnet-4-6"  # model for the optional AI validation step
```

Most budget pipeline settings (OCR thresholds, taxonomy scoring rules) live directly
in `budget/config.py` — they rarely need tuning.

### Reform pipeline settings

```yaml
reforms:
  pdf_dir:        "data/input/surveys"
  extracted_text: "data/output/reforms/extracted_text"
  reforms_json:   "data/output/reforms/reforms_json"
  output:         "data/output/reforms/output"

  countries: []          # [] = all countries found in pdf_dir
  year_range:
    start: 1995
    end: 2025

  panel:
    year_assignment: "implementation"   # "implementation" | "legislation" | "announcement"
    mode: "inclusive"                   # "inclusive" | "strict"
```

**`year_assignment`** controls which date is used as the "reform year" in the panel.
Change it and run `--reforms-build-panel-only` to rebuild instantly with no API cost.

---

## Running the pipeline

### Basic usage

```bash
python main.py                      # both pipelines
python main.py --budget-only        # Finance Bill extraction only
python main.py --reforms-only       # OECD Survey reform extraction only
```

### Reform pipeline options

```bash
# Process only one country
python main.py --reforms-only --reforms-country FRA

# Process only one survey year
python main.py --reforms-only --reforms-country DNK --reforms-year 2019

# Rebuild the panel from cached extractions (no LLM calls, seconds to run)
python main.py --reforms-build-panel-only

# Use a different config file
python main.py --reforms-config path/to/other_config.yaml
```

### Budget pipeline — AI validation (optional post-processing)

The AI validation step reviews already-extracted budget lines using an LLM and flags
likely false positives. It does **not** re-run OCR or extraction.

```bash
# Run extraction + AI validation in one command
python main.py --run-ai-validation

# AI validation only (skip extraction, use existing results.csv)
python main.py --ai-only --run-ai-validation

# Filter to a specific country or year
python main.py --ai-only --run-ai-validation \
  --ai-filter-country Denmark \
  --ai-filter-year 1979

# Skip rows already reviewed in results_ai_verified.csv
python main.py --ai-only --run-ai-validation --skip-verified-records

# Control batch size and record cap
python main.py --run-ai-validation \
  --batch-size 4 \
  --max-records-to-send 100 \
  --min-amount-threshold 500000

# Include surrounding page context in prompts (better quality, more tokens)
python main.py --run-ai-validation --ai-include-context --ai-group-by-page
```

AI validation outputs go to `data/output/budget/ai_validation/<run_name>/`:

| File | Description |
|------|-------------|
| `ai_validated_candidates_raw.csv` | Full LLM response flattened to CSV |
| `ai_ready_for_verification.csv` | Cleaned and filtered candidates ready for human review |
| `baseline_vs_ai_comparison.csv` | Side-by-side: rule-based vs LLM decision |
| `ai_cache.jsonl` | Response cache (avoids re-sending identical records) |
| `ai_validation_run_summary.json` | Counts, config, cost summary |
| `failed_batches.jsonl` | Batches that failed after all retries |

---

## Pipeline 1 — Budget extraction

### How it works

```
Finance Bill PDFs
      │
      ▼
[1] Inventory         scan Data/input/finance_bills/, hash each PDF
      │
      ▼
[2] Text extraction   PyMuPDF direct → OCR fallback (pytesseract) if quality too low
      │
      ▼
[3] Keyword detection multilingual keyword matching → candidate pages
      │
      ▼
[4] Budget extraction taxonomy scoring (J-Rules) + § section parsing + amount detection
      │
      ▼
[5] Output            results.csv / results.xlsx / results.json
      │
      ▼  (optional)
[6] AI validation     LLM reviews candidate lines, flags false positives
```

### Taxonomy scoring (J-Rules)

The budget extractor scores each text block against a multilingual taxonomy derived
from `data/input/taxonomy/search_library.json` (pre-processed from Balazs's Excel file).

| Sheet | Rule | Score |
|-------|------|-------|
| A (Direct R&D) | core R&D terms | +3 (A1), +2 (A2) |
| B (Innovation) | innovation terms | +2 |
| C (Infrastructure) | research infrastructure | +1 |
| D (Institutional) | universities, research councils | +2 |
| E (Sectoral) | sectoral R&D | +1 |
| F (Instruments) | budget instruments | +1 |
| G (Ambiguous) | require nearby anchors | −2 if unanchored |
| H (Exclusions) | market research, regional development | −3 |

**Decision thresholds (J-Rules):**
- Score ≥ 3 → **INCLUDE** (the line itself contains R&D terms)
- Score 1–2 → **REVIEW** (context suggests R&D proximity)
- Score ≤ 0 → **SKIP**

Two-level scoring prevents false positives from context contamination:
- `content_score` = score(section_name + line_description) → used for INCLUDE decision
- `context_score` = score(full context including neighbours) → used for REVIEW decision

### Budget output schema

One row per budget line in `results.csv`:

| Column | Description |
|--------|-------------|
| `country` | Country name |
| `year` | Budget year |
| `section_code` | Ministry § code (e.g. `§20`) |
| `section_name` | Ministry name in original language |
| `section_name_en` | English translation |
| `program_code` | Budget program code |
| `program_description` | Program name (original) |
| `line_description` | Budget line description (original) |
| `line_description_en` | English translation |
| `budget_type` | Driftsudgifter / Anlægsudgifter / etc. |
| `amount_local` | Amount in local currency |
| `currency` | Currency code |
| `rd_category` | direct_rd / innovation / institutional / etc. |
| `taxonomy_score` | Raw J-Rule score |
| `decision` | include / review / skip |
| `confidence` | 0–1 confidence estimate |
| `source_file` | Source PDF filename |
| `page_number` | Page number in source PDF |

---

## Pipeline 2 — Reform extraction

### How it works

```
OECD Economic Survey PDFs
      │
      ▼
[1] Catalog           scan data/input/surveys/, match to OECD country list
      │
      ▼
[2] Text extraction   pdfplumber (born-digital PDFs, no OCR needed)
      │               identifies priority sections (Assessment & Recommendations)
      ▼
[3] LLM extraction    chunks → LLM (Claude/GPT-4o) → structured reform mentions
      │               each mention: description, theme, year, status, direction, importance
      ▼
[4] Within-survey     pass 1: text similarity dedup
    deduplication     pass 2: LLM-assisted grouping by theme
      │
      ▼  (one JSON per survey cached to data/output/reforms/reforms_json/)
      │
      ▼
[5] Cross-survey      connected-components algorithm matches same reform across surveys
    deduplication     (e.g. France's 2023 pension reform in both 2023 and 2024 surveys)
      │
      ▼
[6] Panel construction mentions → events → country×year panel
```

### Innovation taxonomy

This pipeline extracts the `innovation` theme exclusively. Each reform is classified
into one of 8 sub-types, plus two analytical dimensions:

**Sub-types (`sub_theme`)**

| Key | Label |
|-----|-------|
| `rd_funding` | Public R&D Funding |
| `innovation_instruments` | Innovation Instruments & Governance |
| `research_infrastructure` | Research Infrastructure |
| `knowledge_transfer` | Knowledge Transfer & Commercialisation |
| `startup_ecosystem` | Startup & Venture Ecosystem |
| `human_capital` | Human Capital for Innovation |
| `sectoral_rd` | Sectoral / Mission R&D |
| `other` | Other Innovation Policy (use sparingly) |

**R&D Actor (`rd_actor`)** — primary beneficiary: `public`, `private`, `public_private`, `unknown`

**R&D Stage (`rd_stage`)** — pipeline position: `basic`, `applied`, `commercialization`, `adoption`, `unknown`

### Reform output schemas

**`reforms_events.csv`** — one row per deduplicated real-world reform event:

| Column | Description |
|--------|-------------|
| `event_id` | Unique identifier (e.g. `EVT_DNK_2019_0001`) |
| `country_code` / `country_name` | Country |
| `implementation_year` | When the reform took effect |
| `implementation_year_source` | `explicit` / `inferred` / `imputed_survey_year` |
| `theme` / `sub_theme` | Primary classification (theme is always `innovation`) |
| `secondary_type` | Second innovation sub-type if reform spans two types |
| `rd_actor` | `public` / `private` / `public_private` / `unknown` |
| `rd_stage` | `basic` / `applied` / `commercialization` / `adoption` / `unknown` |
| `growth_orientation` | `growth_supporting` / `growth_hindering` / `mixed` / `unclear_or_neutral` |
| `is_major_reform` | Boolean — major structural change |
| `importance_bucket` | 1 (minor) / 2 (moderate) / 3 (major) |
| `status` | `implemented` / `legislated` / `announced` / `recommended` |
| `description` | Canonical reform description |
| `source_quote` | Verbatim quote from survey |
| `n_mentions` | How many surveys discussed this event |

**`reform_panel.csv`** — one row per country×year:

Column patterns: `has_{theme}`, `{theme}_count`, `major_has_{theme}`, `growth_supporting_has_{theme}`, etc.

### Rebuilding the panel with different settings

Because extraction results are cached as JSON files, changing `year_assignment` or
`mode` in `config.yaml` and rebuilding the panel takes seconds with no API cost:

```bash
# Edit config.yaml: panel.year_assignment = "legislation"
python main.py --reforms-build-panel-only
```

---

## Incremental behavior and caching

### Budget pipeline caching

| What changed | What happens |
|---|---|
| New PDF added | OCR + extraction only for that file |
| PDF removed from disk | Results **kept** in database (not deleted) |
| Same PDF, renamed | `content_hash` matches → OCR cache reused, no re-extraction |
| Same PDF, same name | No work done |
| Code/taxonomy changed | Must delete `data/output/budget/intermediate/budget_items_detected.csv` to force re-extraction |

### Reform pipeline caching

Each survey is cached as `data/output/reforms/reforms_json/<ISO3>_<YEAR>.json`.
The `skip_existing: true` config option (default) skips surveys with an existing JSON.
To force re-extraction of a specific survey, delete its JSON file.

The panel can be rebuilt any number of times from cached JSONs at zero API cost.

---

## Adding new countries

### Budget pipeline (Finance Bills)

1. Create a subfolder: `data/input/finance_bills/<CountryName>/`
2. Add PDFs — filenames should contain the year (e.g. `1985_finanslov.pdf`)
3. If the country isn't auto-detected, add a token to `COUNTRY_TOKEN_MAP` in `budget/config.py`
4. If the language isn't covered, add keywords to `KEYWORDS_BY_LANGUAGE` in `budget/config.py`
   and Danish/French/German extensions in `budget/taxonomy.py` `_LANGUAGE_EXTENSIONS`
5. Run `python main.py --budget-only`

### Reform pipeline (OECD Economic Surveys)

1. Add PDFs to `data/input/surveys/` using the naming convention `<ISO3>_<YEAR>.pdf`
   (ISO 3166-1 alpha-3 codes — see `reforms/countries.py` for the full OECD list)
2. Run `python main.py --reforms-only`

To process a specific country only: `python main.py --reforms-only --reforms-country FRA`

---

## Cost estimation (reform pipeline)

API costs per OECD Economic Survey (~100 pages):

| Model | Per survey | All OECD (≈570 surveys) |
|-------|-----------|------------------------|
| Claude Sonnet 4.6 | ~$0.30–0.60 | ~$170–340 |
| Claude Haiku 4.5 | ~$0.05–0.10 | ~$30–60 |
| GPT-4o | ~$0.20–0.40 | ~$115–230 |
| GPT-4o-mini | ~$0.03–0.06 | ~$17–35 |

Cross-survey dedup adds ~5% on top. Panel construction is free (no LLM calls).

**Cost control tips:**
- Process one country at a time: `--reforms-country DNK`
- Use `skip_existing: true` (default) — already-processed surveys are never re-sent
- Use Claude Haiku or GPT-4o-mini for initial broad runs, then re-run key surveys
  with a stronger model using `output_suffix` to compare

---

## Dependencies

Install all dependencies: `pip install -r requirements.txt`

### Budget pipeline
| Package | Purpose |
|---------|---------|
| `pymupdf` (fitz) | PDF text extraction |
| `pytesseract` | OCR fallback for scanned pages |
| `pandas` | Data manipulation |
| `openpyxl` | Excel output |
| `openai` | AI validation (optional) |
| `anthropic` | AI validation (optional) |

### Reform pipeline
| Package | Purpose |
|---------|---------|
| `pdfplumber` | PDF text extraction (born-digital) |
| `anthropic` / `openai` | LLM extraction |
| `pyyaml` | Config file parsing |
| `tenacity` | Retry logic for API calls |
| `requests` / `beautifulsoup4` | PDF catalog / download |
| `pandas` | Panel construction |

### Tesseract (system dependency, budget pipeline only)
The `pytesseract` Python package requires the Tesseract binary installed at the OS level.
Language packs needed: `eng` + `dan` (for Danish Finance Bills) + `fra` (for French).

```bash
# macOS
brew install tesseract tesseract-lang

# Ubuntu/Debian
sudo apt install tesseract-ocr tesseract-ocr-dan tesseract-ocr-fra

# Windows
# Download installer from https://github.com/UB-Mannheim/tesseract/wiki
# Add tesseract.exe directory to PATH
```
