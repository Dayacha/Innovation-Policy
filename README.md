# Innovation Policy Pipeline

Automated pipeline for building cross-country datasets on government investment in R&D and structural reform activity from two complementary data sources: government Finance Bills (Stream 1) and OECD Economic Surveys (Stream 2).

---

## Research design

The project builds two distinct but complementary indicators:

**Stream 1 — R&D Appropriations (what governments actually spent)**
Extracted from scanned Finance Bill PDFs across 25+ OECD countries.
Measures the *revealed preference*: annual R&D budget line items in local currency, comparable across decades.

**Stream 2 — Innovation Policy Reforms (what governments said they were doing)**
Extracted from OECD Economic Survey narratives. Measures the *stated intent*: reform events classified by sub-type (R&D funding, knowledge transfer, etc.), direction (growth-supporting vs. growth-hindering), and R&D activity type. Aggregated into a reform intensity score per country × year.

**The research question** connects both streams: do reform announcements predict budget changes? Which reform sub-types best predict sustained R&D investment? Is there a "say-do gap" between stated intent and actual spending?

---

## Two pipelines

### Pipeline 1 — Budget Extraction (Finance Bills)
*Scanned government budget PDFs → R&D spending time-series*

- Reads Finance Bill PDFs for 25 countries with country-specific extractors
- Applies OCR where needed (scanned documents)
- Scores each budget line against a multilingual R&D/innovation taxonomy (search_library.json, pillars A–L)
- Country-specific extractors in `budget/country_extractor/` handle exact budget formats (program codes, currency scaling, era-specific layouts)

**Covered countries:** Australia, Belgium, Canada, Chile, Colombia, Costa Rica, Czech Republic, Denmark, Estonia, Finland, France, Germany, Hungary, Iceland, Israel, Japan, Korea, Latvia, Lithuania, Netherlands, New Zealand, Norway, Spain, Switzerland, United Kingdom

Input:  `Data/input/finance_bills/<Country>/<filename>.pdf`
Output: `Data/output/budget/results.csv` and `results.xlsx`

### Pipeline 2 — Reform Extraction + Cleaning (OECD Economic Surveys)
*OECD Economic Survey PDFs → clean reform panel + intensity score*

Three sub-steps:

**Step 2a — Extraction** (`reforms/pipeline_reforms.py`)
Downloads Survey PDFs via the OECD Kappa API and uses an LLM to extract innovation policy reform events. Classifies each reform by sub-theme, R&D actor, R&D stage, growth orientation, and implementation year.
Output: `reforms_mentions.csv` (raw, ~3 900 rows with ~27 % contamination)

**Step 2b — Two-pass cleaning** (`reforms/clean_pipeline.py`)
Removes contaminated non-R&D rows using a rule-based taxonomy filter followed by targeted LLM adjudication for borderline cases. All cleaning decisions are written as new columns directly into `reforms_mentions.csv` — no extra intermediate files are created. See [Cleaning methodology](#cleaning-methodology) below.

**Step 2c — Reform intensity score** (built inside `clean_pipeline.py`)
Aggregates clean reforms into a country × year panel with a weighted reform intensity score. This is the indicator used in the research paper.
Output: `reform_intensity_score.csv` and `reform_panel_clean.csv`

---

## Cleaning methodology

### Why cleaning is needed

The extraction step uses a broad pre-filter to maximise recall — at the cost of capturing non-R&D policies. An audit of the raw `reforms_mentions.csv` found:

| Band | Rows | % |
|------|------|---|
| Clearly relevant (taxonomy score ≥ 3) | ~1 978 | 48 % |
| Borderline (score 1–2) | ~901 | 23 % |
| Contaminated (score ≤ 0) | ~1 051 | 27 % |

Contamination examples: VET / skills training tagged as `human_capital`; feed-in tariffs tagged as `innovation_instruments`; EV charging infrastructure tagged as `research_infrastructure`.

### Pass 1 — Rule-based taxonomy filter (`reforms/scoring_filter.py`)

Applies `search_library.json` (pillars A–L) to score each row on keyword co-occurrence.

```
score ≥ 3  →  KEEP        (~48 %): R&D terms clearly present
score 1–2  →  BORDERLINE  (~23 %): escalate to LLM
score ≤ 0  →  DROP        (~27 %): no R&D signal
```

Strength: deterministic, reproducible, zero API cost, auditable.

Quality by sub-theme (% kept without LLM):

| Sub-theme | % kept | Notes |
|-----------|--------|-------|
| `rd_funding` | 84 % | Highest quality — direct R&D appropriation language |
| `knowledge_transfer` | 75 % | Good quality — TTO, spinoff, patent language |
| `innovation_instruments` | 52 % | Mixed — many indirect-language rows |
| `sectoral_rd` | 39 % | Contaminated with non-R&D sectoral policies |
| `startup_ecosystem` | 33 % | Contaminated with general SME support |
| `human_capital` | 30 % | Contaminated with VET, lifelong learning |
| `research_infrastructure` | 30 % | Contaminated with general infrastructure |
| `other` | 14 % | Mostly irrelevant |

### Pass 2 — LLM adjudication + K/L lens classification (`reforms/adjudicator.py`)

Two tasks in a single LLM pass:

**Task A — Adjudication (borderline rows only)**
The LLM reads each borderline description + source quote with the full taxonomy as context and decides: include or exclude. Expected outcome: ~200–300 genuinely relevant reforms rescued from the borderline band.

**Task B — K/L lens classification (all kept rows)**
Every clean reform receives two new analytical dimensions:

*Activity Lens (K-pillar)* — type of R&D activity targeted:

| Code | Label |
|------|-------|
| K1 | Basic / fundamental research |
| K2 | Applied research |
| K3 | Experimental development |
| K4 | General R&D (undifferentiated) |
| K5 | Innovation activities (non-R&D: design, organisational) |
| K6 | Knowledge bridge (TTOs, tech transfer, university–industry) |
| K7 | Research infrastructure (labs, supercomputers, data systems) |
| K8 | System support (governance, evaluation, coordination) |

*Defence Lens (L-pillar)* — civilian vs. defence scope:

| Code | Label |
|------|-------|
| L1 | Primarily defence R&D |
| L2 | Primarily defence innovation |
| L3 | Dual-use (military and civilian) |
| L4 | Explicitly civilian R&D |
| L5 | Explicitly civilian innovation |
| L6 | Exclude — defence context makes it non-R&D |

Batching: 10 rows per LLM call with checkpoint-based resumption.
Estimated cost: ~$0.40–0.60 total (gpt-4o-mini) or ~$1–2 (Claude Sonnet).

### Reform intensity score

For each country × year:

```
weighted_score     = Σ weight[sub_theme]  for each positive (growth-supporting) reform
weighted_net_score = Σ weight[sub_theme] × direction  for all reforms
net_reforms        = n_positive − n_negative
```

Sub-theme weights (reflecting direct R&D relevance):

| Sub-theme | Weight | Rationale |
|-----------|--------|-----------|
| `rd_funding` | 1.0 | Direct public R&D appropriation |
| `knowledge_transfer` | 0.9 | Direct output from research to market |
| `research_infrastructure` | 0.8 | Shared research capacity |
| `innovation_instruments` | 0.7 | Indirect R&D support |
| `sectoral_rd` | 0.6 | Mission-oriented R&D |
| `startup_ecosystem` | 0.4 | Innovation-adjacent |
| `human_capital` | 0.3 | Research pipeline |
| `other` | 0.1 | Fallback |

**Note:** Scores are raw weighted counts, not normalised to 0–6. Normalisation should be applied in the analysis step (percentile rank within sample) so the scaling choice is transparent.

---

## Quick start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

For Pipeline 1 (scanned PDFs), also install Tesseract OCR:
```bash
brew install tesseract tesseract-lang          # macOS
sudo apt install tesseract-ocr tesseract-ocr-dan tesseract-ocr-fra  # Linux
```

### 2. Configure

```bash
cp config.yaml.example config.yaml
```

Edit `config.yaml`:

```yaml
llm:
  provider: "anthropic"                    # or "openai"
  api_key: "sk-ant-..."                    # or set ANTHROPIC_API_KEY env var
  model: "claude-haiku-4-5-20251001"       # cheap for high-volume cleaning runs

reforms:
  kappa_api_key: "..."                     # OECD Kappa key (leave empty for manual PDFs)
```

> `config.yaml` is gitignored — API keys are never committed.

---

## Commands

### Stream 1 — Budget extraction

```bash
# Extract R&D budget lines from all Finance Bill PDFs
python main.py --budget-only

# Single country
python main.py --budget-only --country Denmark
```

### Stream 2a — Reform extraction from OECD Surveys

```bash
# Download and extract reforms (all countries, all years)
python main.py --reforms-only

# Single country
python main.py --reforms-only --reforms-country DNK

# Single country + year
python main.py --reforms-only --reforms-country DNK --reforms-year 2024

# Rebuild the raw panel without LLM calls (free)
python main.py --reforms-build-panel-only
```

### Stream 2b — Cleaning (runs automatically after extraction, or standalone)

Cleaning runs **automatically** at the end of `python main.py --reforms-only`.
To run it manually on an existing `reforms_mentions.csv`:

```bash
# ── Standalone cleaning script (score + LLM + panels) ──────────────────────
python -m reforms.run_cleaning

# ── Pass 1 only — taxonomy scoring, free and instant ───────────────────────
python -m reforms.run_cleaning --skip-llm

# ── Resume interrupted LLM pass (checkpoint read automatically) ────────────
python -m reforms.run_cleaning

# ── Custom batch size ───────────────────────────────────────────────────────
python -m reforms.run_cleaning --batch-size 20

# ── Both pipelines (extraction + cleaning runs end-to-end) ──────────────────
python main.py
```

---

## Output files

### Budget pipeline (`Data/output/budget/`)

| File | Description |
|------|-------------|
| `results.csv` | Main output — one row per R&D budget line identified |
| `results.xlsx` | Same, formatted for Excel review |
| `results_ai_verified.csv` | Rows confirmed by optional AI validation pass |
| `audits/country_audit_summary.csv` | Per-country extraction quality summary |

### Reform pipeline — raw (`Data/output/reforms/output/`)

| File | Description |
|------|-------------|
| `reforms_mentions.csv` | Raw LLM extractions (3 930 rows, ~27 % contaminated) |
| `reforms_events.csv` | Deduplicated events (cross-survey) |
| `reform_panel.csv` | Country×year panel (raw, not cleaned) |
| `reform_panel_subtheme.csv` | Country×year×subtheme panel (raw) |

### Reform pipeline — after cleaning (`Data/output/reforms/output/`)

All cleaning results are written as **new columns into `reforms_mentions.csv`** — no extra intermediate CSVs are created.  Re-running simply overwrites those columns.

New columns added to `reforms_mentions.csv`:

| Column | Description |
|--------|-------------|
| `tax_score` | Taxonomy relevance score (Pass 1) |
| `score_band` | `"keep"` / `"borderline"` / `"drop"` |
| `filter_decision` | `"keep_rule_based"` / `"escalate_to_llm"` / `"drop_rule_based"` |
| `llm_decision` | `"include"` / `"exclude"` / `"n/a"` (Pass 2) |
| `llm_rationale` | LLM explanation for the decision |
| `activity_lens` | K1–K8 — type of R&D activity |
| `defence_scope` | L1–L6 — civilian vs. defence scope |

The **clean view** is simply: rows where `score_band == "keep"` OR `(score_band == "borderline"` AND `llm_decision == "include")`.

Two aggregated output files (different shape from the mentions file):

| File | Description |
|------|-------------|
| `reform_intensity_score.csv` | **Country×year reform intensity scores** |
| `reform_panel_clean.csv` | Country×year×subtheme binary panel (clean) |
| `cleaning_report.json` | Diagnostics: counts, K/L distribution, API cost |
| `adjudicator_checkpoint.json` | LLM progress state (allows resumption if interrupted) |
| `adjudicator_llm_usage.json` | API call counts and cost breakdown |

---

## Project structure

```
Innovation-Policy/
│
├── main.py                         Unified entry point
├── config.yaml.example             Configuration template
│
├── budget/                         Pipeline 1 — Finance Bill extraction
│   ├── budget_extractor.py         Orchestration engine
│   ├── dedicated_pipeline.py       Country dispatcher (25 countries)
│   ├── taxonomy.py                 Taxonomy loader + K/L classification
│   └── country_extractor/          Country-specific extractors (25 files)
│
├── reforms/                        Pipeline 2 — OECD Survey reform extraction
│   ├── pipeline_reforms.py         Step 2a: extraction
│   ├── scoring_filter.py           Step 2b Pass 1: taxonomy scoring
│   ├── adjudicator.py              Step 2b Pass 2: LLM adjudication + K/L
│   ├── clean_pipeline.py           Step 2b orchestrator + intensity score
│   ├── prompts.py                  LLM prompt templates
│   ├── llm_client.py               Unified LLM client (Anthropic + OpenAI)
│   └── panel_builder.py            Raw panel construction
│
└── Data/
    ├── input/
    │   ├── finance_bills/          Finance Bill PDFs by country
    │   ├── surveys/                OECD Economic Survey PDFs
    │   └── taxonomy/
    │       └── search_library.json Taxonomy (pillars A–L, K/L lenses)
    └── output/
        ├── budget/
        └── reforms/output/
```

---

## Taxonomy reference (`search_library.json`)

| Pillar | Type | Scoring effect |
|--------|------|----------------|
| A | Direct R&D terms | +3 per hit |
| B | Innovation terms | +2 per hit |
| C | Research infrastructure | +1 per hit |
| D | Institutions (universities, research councils) | +2 per hit |
| E | Sectoral R&D | +1 per hit |
| F | Budget instruments | +1 per hit |
| G | Ambiguous terms | −2 if unanchored |
| H | Exclusions (market research, housing, etc.) | −3 per hit |
| I | Stems / regex patterns | Pattern matching |
| J | Decision rules | Include ≥3, Review 1–2, Exclude ≤0 |
| K | Activity lens (K1–K8) | Type classification |
| L | Defence lens (L1–L6) | Scope classification |

---

## Re-running is safe

- Adding new PDFs → only new files are processed
- The LLM adjudicator checkpoints after every batch → safe to interrupt and resume
- `--reforms-build-panel-only` rebuilds the raw panel in seconds at zero API cost
- `--skip-llm` gives a fast taxonomy-only cleaning pass for inspection before committing to LLM calls
