# Innovation Policy Pipeline

Automated pipeline for building cross-country time-series datasets on government
investment in innovation and structural reform activity, using two complementary data sources.

---

## What this does

Measuring innovation policy across countries and decades requires two types of evidence:

**How much money governments spent on R&D** is buried in scanned Finance Bill PDFs —
thousands of budget line items in Danish, French, German, and other languages.
Traditional approaches require manual coding of each document.

**What structural reforms governments enacted** is described in OECD Economic Survey
narratives — rich text that requires reading comprehension to extract reform events,
classify their direction, and assign them to policy themes.

This pipeline automates both.

---

## Two pipelines, one project

### Pipeline 1 — Budget Extraction (Finance Bills)
*Scanned government budget PDFs → R&D spending time-series*

- Reads Finance Bill PDFs for multiple countries and years
- Applies OCR where needed (scanned documents)
- Scores each budget line against a multilingual R&D/innovation taxonomy
- Outputs a structured dataset: `country | year | section | amount | rd_category`

Input:  `data/input/finance_bills/<Country>/<filename>.pdf`
Output: `data/output/budget/results.csv` and `results.xlsx`

### Pipeline 2 — Reform Extraction (OECD Economic Surveys)
*OECD Economic Survey PDFs → structural reform panel dataset*

- Downloads Survey PDFs automatically via the OECD Kappa API (or manually placed)
- Uses an LLM (Claude or GPT-4o) to extract innovation policy reform events
- Classifies each reform by sub-type (`rd_funding`, `knowledge_transfer`, etc.),
  R&D actor (`public`/`private`/`public_private`), R&D stage (`basic`/`applied`/
  `commercialization`/`adoption`), growth orientation, and implementation year
- Outputs a country×year panel ready for econometric analysis

Input:  `Data/input/surveys/<ISO3>_<YEAR>.pdf`  (e.g. `DNK_2019.pdf`)
Output: `Data/output/reforms/output/reform_panel.csv`

---

## Quick start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

For Pipeline 1 (scanned PDFs), also install Tesseract OCR:
- **macOS:** `brew install tesseract tesseract-lang`
- **Windows:** [UB Mannheim builds](https://github.com/UB-Mannheim/tesseract/wiki)
- **Linux:** `sudo apt install tesseract-ocr tesseract-ocr-dan tesseract-ocr-fra`

### 2. Configure

```bash
cp config.yaml.example config.yaml
```

Open `config.yaml` and fill in your keys:

```yaml
llm:
  provider: "anthropic"   # or "openai"
  api_key: "sk-ant-..."   # LLM key for reform extraction

reforms:
  kappa_api_key: "..."    # OECD Kappa key for auto-downloading Survey PDFs
                          # Leave empty to place PDFs manually (see below)
```

> **API keys are never committed to git** — `config.yaml` is in `.gitignore`.
> You can also use environment variables: `ANTHROPIC_API_KEY` and `KAPPA_API_KEY`.

### 3. Get OECD Economic Survey PDFs

**Option A — Auto-download via OECD Kappa API** (recommended):

```bash
# Build a catalog of all available surveys (saves kappa_catalog.json)
python main.py --reforms-fetch-catalog

# Download all surveys
python main.py --reforms-download

# Or download selectively
python main.py --reforms-download --reforms-country DNK          # one country, all years
python main.py --reforms-download --reforms-year 2024            # all countries, one year
python main.py --reforms-download --reforms-country DNK --reforms-year 2024  # one survey
```

**Option B — No Kappa key** (public URL fallback or manual):

Without a Kappa key, `--reforms-download` will try public OECD iLibrary URL patterns.
Many surveys require institutional access, so not all will be available this way.

You can also place PDFs manually — naming must be exact:

```
data/input/surveys/
├── DNK_2019.pdf
├── FRA_2022.pdf
└── DEU_2021.pdf
```

### 4. Add Finance Bill PDFs (Pipeline 1)

```
data/input/finance_bills/
└── Denmark/
    ├── 1975_finanslov.pdf
    └── 1976_finanslov.pdf
```

### 5. Run

```bash
# Run both pipelines
python main.py

# Run only Finance Bill extraction (no API key needed)
python main.py --budget-only

# Run only OECD Economic Survey reform extraction
python main.py --reforms-only

# Process a single country or year
python main.py --reforms-only --reforms-country DNK
python main.py --reforms-only --reforms-country DNK --reforms-year 2024

# Rebuild the reform panel without any LLM calls (free, runs in seconds)
python main.py --reforms-build-panel-only
```

---

## Output files

### Budget pipeline
| File | Description |
|------|-------------|
| `data/output/budget/results.csv` | Main output — one row per budget line identified as R&D-related |
| `data/output/budget/results.xlsx` | Same, formatted for review in Excel |
| `data/output/budget/results_ai_verified.csv` | Rows confirmed by AI validation pass |

### Reform pipeline
| File | Description |
|------|-------------|
| `data/output/reforms/output/reform_panel.csv` | Country×year panel with reform indicators by theme |
| `data/output/reforms/output/reforms_events.csv` | One row per deduplicated reform event |
| `data/output/reforms/output/reforms_mentions.csv` | Raw extractions — full audit trail |

---

## Re-running is safe

Both pipelines are **incremental and non-destructive**:

- Adding new PDFs → only the new files are processed
- Removing a PDF from disk → its results are **kept** in the database
- Changing nothing → nothing is re-processed (full cache hit)
- Re-running the reform panel with different settings → `--reforms-build-panel-only` rebuilds in seconds with no API cost

---

## Project structure

```
Innovation-Policy/
│
├── main.py                   unified entry point
├── config.yaml.example       configuration template (copy to config.yaml)
│
├── budget/                   Finance Bill extraction modules
├── reforms/                  OECD Economic Survey extraction modules
│   └── pipeline_reforms.py   reform pipeline orchestration
│
├── app/                      Streamlit dashboard
│   └── streamlit_app.py      interactive visualization (run with: streamlit run app/streamlit_app.py)
│
└── Data/
    ├── input/
    │   ├── finance_bills/    Finance Bill PDFs (tracked in git)
    │   ├── surveys/          Economic Survey PDFs (gitignored — download via Kappa)
    │   └── taxonomy/         Reference taxonomy files (tracked in git)
    └── output/               All pipeline outputs
        ├── budget/
        └── reforms/
```

For the full technical reference — pipeline architecture, all CLI flags, config options,
output schemas, and how to extend to new countries — see [TECHNICAL.md](TECHNICAL.md).
