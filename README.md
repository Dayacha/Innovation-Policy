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

### Pipeline 1b — R&D Budget Extraction from Finance Bills (Structured Documents)

> **Used for countries that publish Finance Bills as Word documents (.docx): Australia, Canada, UK, New Zealand.**

---

#### What this pipeline does and why

Government Finance Bills list every agency that receives public money, along with the exact amount. Hidden inside these documents is the R&D budget: agencies like CSIRO, the Australian Research Council, or the National Health and Medical Research Council appear as line items alongside hundreds of other government programmes (courts, roads, welfare, defence).

The goal of this pipeline is to go from a stack of Word documents — one per year, sometimes multiple Acts per year — to a clean, researcher-ready spreadsheet with one row per R&D agency per year, covering decades of history.

The challenge is that:
- The same agency can appear multiple times in the same document (summary table, detail table, supplementary Act)
- The same agency changes its name over time (the Atomic Energy Commission became ANSTO in 1987)
- Budget tables show **two years side by side**: the current year's budget (what we want) and the prior year's figure for comparison (which we must not confuse with the current year)
- Broad ministries like the Department of Health appear in the same tables as pure R&D agencies — we need to include NHMRC but not the entire health budget

---

#### How we solved the prior-year problem

In Australian Appropriation Acts, the **current year's amount is printed in plain text** and the **prior year's amount is printed in italic**. This is a formatting convention baked into the Word document structure — it has nothing to do with the content. Our parser reads this formatting signal directly from the file, so it never confuses the two years. This is something an AI reading raw text cannot reliably detect.

---

#### The seven steps, explained plainly

**Step 1 — Read every table in every document**

The pipeline opens every Word file and reads every single table row. It does not pre-filter or guess which pages are relevant. Every agency that appears in a budget table, from CSIRO to the Australian Law Reform Commission, gets extracted. This produces a complete record called `raw_rows.csv` — the audit trail for everything that follows. At this stage, nothing is an AI call; it is pure mechanical reading.

**Step 2 — Remove duplicates within and across documents**

Each agency typically appears three times in a modern Australian Act: once in a portfolio summary table, once in its own agency table, and again in a "Total:" row. Across multiple Acts in the same year (No1, No2, No3), the same figure may appear again. The pipeline keeps one authoritative row per agency per year: preferring the "Total:" row (most explicit) and the lowest Act number (primary appropriation), and dropping the rest.

**Step 3 — Collapse name variants (AI, cheap, run once)**

After deduplication, some agencies still appear under slightly different names — a table cell cut off mid-word, an ALL CAPS variant, a "Total: " prefix. The pipeline sends the list of entity names for each year to an AI model (GPT-4o-mini), which groups variants that refer to the same institution and assigns a single canonical name. For example:

> "Total: Australian Nuclear Science and" → **Australian Nuclear Science and Technology Organisation**
> "ANSTO" → **Australian Nuclear Science and Technology Organisation**

This costs roughly $0.001 per country-year and the result is saved permanently — the AI is never called again for the same year unless the underlying data changes.

**Step 4 — Discover new R&D budget entries automatically (AI, cheap, run once)**

The pipeline looks for entries that appear consistently across multiple years with significant budgets but are not yet on the tracked list. It filters out noise (outcome descriptions, procurement lines, anything longer than a normal agency or programme name) and asks the AI: is this R&D-relevant spending?

Importantly, this step is designed to work differently across countries. Some countries (Australia, UK) organise their R&D budget around dedicated agencies — CSIRO, the ARC, NHMRC each have their own appropriation line. Other countries (Denmark, France, Germany) channel research spending through programme lines within broader ministries — the Statens teknisk-videnskabelige Forskningsfond appears as a line item under the Ministry of Education, not as a standalone agency. The discovery step recognises both patterns: it looks for anything that is clearly and specifically R&D-related, whether it is a standalone institution or a named programme within a ministry. Entries the AI is confident about are added automatically. Uncertain cases go to a review file for a human decision.

**Step 5 — Classify every entity (AI, very cheap, run once)**

Every unique agency name that has ever appeared in any document gets classified into one of four types:
- **Dedicated R&D agency** — exists solely to fund or conduct research (CSIRO, ARC, NHMRC). *Included in the series.*
- **R&D programme** — a specific research scheme within a ministry (Cooperative Research Centres). *Included.*
- **Mixed ministry** — a broad department where R&D is a small fraction (Department of Health, Department of Defence). *Excluded from totals; only specific R&D line items within it are tracked.*
- **Unclear** — flagged for human review.

These classifications are saved in `agency_registry.csv`, which is the main human review checkpoint. A researcher can open this file, check any classification, and correct it — the series will update on the next free rerun.

**Step 6 — Build the time series**

The pipeline produces two complementary output files:

The **detail series** (`australia_docx_series.csv`) keeps one row per agency per year per source document. This means that if CSIRO received $587M in the main Appropriation Act (No1) and an additional $12M in the supplementary Act (No2), both rows appear separately with their source file clearly labelled. This is the file for tracing any number back to the original document.

The **totals series** (`australia_docx_totals.csv`) has one row per agency per year and sums the amounts across Acts — but with an important safeguard. Sometimes a supplementary Act does not add new money; it simply re-states or corrects an amount from the main Act. If two Acts show the exact same dollar figure for the same agency, the pipeline flags this as a possible restatement and uses only the primary amount rather than doubling it. Each row in the totals file has an `additive_flag` column explaining how the total was calculated: `single` (only one Act), `additive` (multiple Acts with genuinely different amounts, summed), or `restatement` (same amount appeared in two Acts — primary amount used, not summed).

Both files handle historical name changes via a list of known variants: "Bureau of Mineral Resources" and "Geoscience Australia" are the same institution and appear as a single continuous series going back to 1946.

**Step 7 — Find what is missing and why**

The pipeline checks for gaps: years where an agency should have data but does not. For each gap it searches the raw extraction to see if the data was actually captured but not classified correctly (fixable at zero cost) or if the source documents were never parsed (requires re-running the parser on those files). It also flags statistical outliers — years where an agency's budget is implausibly far from its historical trend, which usually indicates a unit error (dollars vs thousands) or a prior-year figure accidentally included.

---

#### What the pipeline costs

| Step | Cost | Notes |
|---|---|---|
| Reading and parsing documents | Free | No AI involved |
| Deduplication | Free | Pure code |
| Name variant grouping | ~$0.001 per country-year | Cached — free on reruns |
| Agency discovery | ~$0.0001 per new agency name | Cached — free on reruns |
| Agency classification | ~$0.0001 per unique name | Cached — free on reruns |
| Building the series | Free | Pure code |
| Gap detection | Free | Pure code |

A full run for Australia (50+ years, 300+ documents) costs under $1 in total AI calls. Every subsequent rerun is free because all AI results are saved locally.

---

#### What you can trust and what to check

**You can trust:**
- The amounts — they come directly from table cells in the source documents, not from AI interpretation
- The prior-year exclusion — detected from document formatting, not guesswork
- The deduplication — the same figure cannot appear twice in the series

**Always check:**
- `agency_registry.csv` — open this and scan the `agency_type` column. Any `unclear` entry is an agency the AI was not confident about. These are the rows most likely to need a human decision.
- `australia_discovery_review.csv` — new agencies found automatically but below the confidence threshold. Review these and either add them to the tracked list or confirm they should be excluded.
- `australia_gap_report.csv` — any row with `action = verify` is a year where an agency's amount is statistically unusual. Check the original document.

---

#### Running the pipeline

```bash
# First run for a country — does everything including AI calls
python -m budget.compile --country Australia --config config.yaml

# Rerun after fixing name variants or registry — free, no new AI calls
python -m budget.compile --country Australia --config config.yaml --no-entity-dedup

# Specific years only (e.g. to test before running the full history)
python -m budget.compile --country Australia --years 2020-2026 --config config.yaml

# Preview what new agencies would be discovered — no AI calls, no changes
python -m budget.agency_discovery --country Australia --config config.yaml --dry-run
```

---

#### Output files (all saved to `Data/output/budget/`)

| File | What it contains | Who should look at it |
|---|---|---|
| `raw_rows.csv` | Every single row extracted from every table in every document. The complete audit trail. | Anyone verifying a specific figure |
| `australia_docx_results.csv` | All extracted rows with their classification (include / review) and R&D category | Researcher checking coverage |
| `australia_docx_series.csv` | Detail dataset — one row per agency per year per source Act, with amount and source reference | Tracing individual figures to source |
| `australia_docx_totals.csv` | **Summary dataset** — one row per agency per year, amounts summed across Acts with restatement flag | Main research output for analysis |
| `australia_gap_report.csv` | Which agency-years are missing, why, and what action is needed | Quality control |
| `agency_registry.csv` | Every unique entity name ever seen, with its type classification | Human review checkpoint |
| `discovered_agencies.json` | Agencies found automatically by the discovery step | Background reference |
| `australia_discovery_review.csv` | Agencies the AI was uncertain about — needs a human decision | Human review |

---

#### Adding a new country

1. Place the Word documents in `Data/input/finance_bills/<CountryName>/` — each filename must contain a 4-digit year somewhere (e.g. `2023 No1 Budget.docx`)
2. Add the country's currency and language to `budget/config.py`
3. Optionally add known agencies to `budget/canonical_series.py` — or leave it empty and let the discovery step find them
4. Run: `python -m budget.compile --country <CountryName> --config config.yaml`

---

#### Current limitations

**Old Word format (.doc files before 2007):** The pipeline cannot read the old `.doc` format. Files from roughly 2000–2007 for Australia exist in this format and are currently skipped. To include them, convert with LibreOffice (free, command line):
```bash
soffice --headless --convert-to docx "Data/input/finance_bills/Australia/"*.doc \
  --outdir "Data/input/finance_bills/Australia/"
```

**Scanned PDFs:** Countries that only have scanned PDF Finance Bills (Denmark 1970s, France, Germany) use a different pipeline (see Pipeline 1 below) that relies on OCR and AI reading of running text rather than table parsing.

**Unit changes over time:** Early Australian Acts (pre-2000) report amounts in full dollars rather than thousands. The series currently flags these years as statistical outliers. A correction is planned.

---

### Pipeline 1 — Budget Extraction (Finance Bills)
*Scanned government budget PDFs → R&D spending time-series*

**Step 1a — Extraction**
- Reads Finance Bill PDFs for 25+ countries with country-specific extractors
- Applies OCR where needed (scanned documents, pytesseract)
- Scores each budget line against a multilingual R&D/innovation taxonomy (`search_library.json`, pillars A–L)
- Routing: each file is dispatched to a **dedicated country extractor** (`budget/country_extractor/<country>_extractor.py`) if one exists, or to the **generic taxonomy-driven parser** otherwise
  - Dedicated extractors handle country-specific document formats (program codes, currency scaling, table structures, era-specific layouts)
  - The generic parser is language-neutral — no country-specific keyword filters — and uses only taxonomy scoring + section structure detection
- Results are accumulated **incrementally by file ID**: only new files are re-extracted on each run; existing results are preserved

**Step 1b — Optional AI validation** (see [Budget AI validation](#budget-ai-validation) below)

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

## Budget AI validation

An optional post-extraction validation layer (`budget/ai_validation.py`) runs after `results.csv` is produced. It improves data quality for the R&D time-series database through three sequential AI passes.

> The AI is **grounded in the OECD researchers' own taxonomy** (`search_library.json`). All prompts explicitly load the taxonomy's keyword lists and instruct the model to base decisions strictly on those definitions — not on general intuitions about R&D. Strict anti-hallucination rules are enforced: the AI may not invent data, must return `null` for uncertain fields, and must cite the specific taxonomy term that drove each decision.

### Pass 1 — Individual record validation (split by decision tier)

Records are routed to two different prompts depending on their taxonomy score:

| Tier | Records | AI task |
|------|---------|---------|
| **Include** (taxonomy score ≥ 3) | High-confidence R&D lines | Validate amount, classify by Frascati type, flag double-counting risk, clean and translate description |
| **Review** (taxonomy score 1–2) | Borderline lines | Binary **include / exclude** decision — `review` is not a valid output; must cite the taxonomy term that drove the decision; default to `exclude` when uncertain |

New fields added per record:

| Field | Description |
|-------|-------------|
| `frascati_type` | `intramural_rd` / `extramural_grants` / `rd_coordination` / `rd_infrastructure` / `higher_ed_rd` / `not_rd` |
| `ai_rd_category` | `direct_rd` / `innovation_system` / `possible_rd` / `not_rd` |
| `ai_decision` | `include` / `review` / `exclude` |
| `ai_confidence` | 0–1 float |
| `ai_rationale` | One sentence citing the taxonomy term that drove the decision |
| `validated_amount_local` | Corrected amount if a unit error (e.g. `$'000` header) was detected in the surrounding context |
| `double_counting_risk` | `true` if the record may duplicate another line in the same batch |
| `parse_issue` | `none` / `legal_reference_noise` / `merged_adjacent_items` / `unit_conversion_applied` / etc. |

Cache key: `MD5(year | section_code | line_description | amount_local)` — year is included so per-year evolution is captured independently.

### Pass 2 — Country-year aggregation

After all individual records are validated, one AI call is made **per (country, year)**. The AI receives every validated include-decision record for that country-year and:

1. Identifies double-counting (e.g. a department total that already includes a specific agency listed separately)
2. Produces a deduplicated total R&D appropriation estimate with confidence rating
3. Lists which record IDs were included in / excluded from the total and why

Output: `aggregation_results.csv` — one row per (country, year) with `estimated_total_rd`, `double_counting_flags`, `included_record_ids`, `excluded_record_ids`, `confidence`, `coverage_notes`.

### Pass 3 — Time-series anomaly detection

After the full time series is assembled, one AI call is made **per country** across all years. The AI receives per-program time series (amount by year) and flags:

- **Unit errors** — amounts that are 10× or 0.1× neighboring years (likely `$'000` vs full-dollar confusion)
- **Implausible spikes / drops** — year-over-year ratio > 5× or < 0.1× with no programmatic explanation
- **Gaps** — missing years in an otherwise continuous series

The AI may suggest a corrected amount only when the evidence is strong (explicit unit header in the extracted data). Otherwise `suggested_amount` is `null`. When uncertain, the AI does not flag — the conservative default is to not raise a false alarm.

Output: `anomaly_flags.csv` — one row per flagged program-year with `anomaly_type`, `suspected_cause`, `suggested_amount`, `confidence`.

### Running AI validation

```bash
# Full AI validation on all results (all 3 passes)
python main.py --run-ai-validation

# Single country only
python main.py --run-ai-validation --filter-country Australia

# Skip aggregation or anomaly passes
python main.py --run-ai-validation --no-aggregation-pass
python main.py --run-ai-validation --no-anomaly-pass

# Review-tier records only (fastest, resolves borderline cases)
python main.py --run-ai-validation --include-review-only
```

Each pass has its own cache file so re-runs are free for already-processed records:
- Individual validation: `ai_cache.jsonl`
- Aggregation: `aggregation_cache.jsonl`
- Anomaly detection: `anomaly_cache.jsonl`

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
| `results.json` | Same, grouped by country / year / source file |
| `results_ai_verified.csv` | Rows confirmed by AI validation (Pass 1 decisions) |
| `results_review_status.csv` | Tracks which rows are pending / reviewed |
| `audits/country_audit_summary.csv` | Per-country extraction quality summary |

AI validation outputs (under `Data/output/budget/ai_validation/<run_name>/`):

| File | Description |
|------|-------------|
| `ai_validated_candidates_raw.csv` | Raw AI output — one row per record sent |
| `ai_validated_candidates_clean.csv` | Merged baseline + AI fields side-by-side |
| `baseline_vs_ai_comparison.csv` | Taxonomy decision vs AI decision for review |
| `aggregation_results.csv` | **Pass 2** — per (country, year): estimated total R&D, double-counting flags, included / excluded record IDs, confidence |
| `anomaly_flags.csv` | **Pass 3** — per program-year: anomaly type, suspected cause, suggested corrected amount |
| `ai_cache.jsonl` | Individual validation cache (keyed by year + section + description + amount) |
| `aggregation_cache.jsonl` | Aggregation pass cache (keyed by country + year) |
| `anomaly_cache.jsonl` | Anomaly pass cache (keyed by country + program) |
| `ai_validation_run_summary.json` | Stats: cache hits, API calls, records per tier, flags |
| `failed_batches.jsonl` | Any batches that failed after retries (for debugging) |

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
├── budget/                     Pipeline 1b — Deterministic DOCX table parser (NEW)
│   ├── compile.py                  Main orchestrator — runs all 7 stages
│   ├── docx_table_parser.py        Deterministic table extractor (no LLM)
│   ├── entity_dedup.py             LLM entity name deduplication (cached)
│   ├── agency_discovery.py         Auto-discovers new R&D agencies from raw data
│   ├── agency_classifier.py        Classifies agencies into dedicated_rd / mixed / etc
│   ├── canonical_series.py         Builds clean (agency, year) time series
│   ├── gap_detector.py             Finds missing years, outliers, reextract queue
│   ├── config.py                   Paths, models, country context
│   └── llm_client.py               Thin wrapper around reforms/llm_client.py
│
├── budget/                         Pipeline 1 — Finance Bill extraction
│   ├── budget_extractor.py         Orchestration engine (file grouping, routing)
│   ├── dedicated_pipeline.py       Country dispatcher → dedicated or generic parser
│   ├── generic_budget_pipeline.py  Generic taxonomy-driven parser (language-neutral)
│   ├── extractor_common.py         Shared helpers (currency, pillar, filepath utils)
│   ├── taxonomy.py                 Taxonomy loader + multilingual extensions
│   ├── section_parser.py           § section structure parser (OCR-aware)
│   ├── ai_client.py                AI client — 4 prompt modes (include/review/agg/anomaly)
│   ├── ai_validation.py            3-stage AI validation pipeline
│   ├── ai_batch_runner.py          Batching + retry logic
│   ├── temporal_smoothing.py       Cross-year score smoothing + anomaly thresholds
│   └── country_extractor/          Country-specific extractors (25+ files)
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

The taxonomy is the OECD researchers' authoritative definition of what counts as R&D in this project. It is used at two points: (1) deterministic keyword scoring during extraction, and (2) as grounding material injected into every AI prompt so the model cannot substitute its own judgment.

### Scoring pillars

| Pillar | Type | Scoring effect |
|--------|------|----------------|
| A | Direct R&D terms | +3 per hit |
| B | Innovation terms | +2 per hit |
| C | Research infrastructure | +1 per hit |
| D | Institutions (universities, research councils) | +2 per hit |
| E | Sectoral R&D | +1 per hit |
| F | Budget instruments | +1 per hit |
| G | Ambiguous terms | −2 if unanchored, +0.5 if anchored |
| H | Exclusions (market research, housing, etc.) | −3 per hit |
| I | Stems / regex patterns | Pattern matching |
| J | Decision rules | Include ≥3 · Review 1–2 · Exclude ≤0 |
| K | Activity lens (K1–K8) | Type classification |
| L | Defence lens (L1–L6) | Scope classification |

### Frascati budget types (GBARD classification — AI Pass 1 output)

The AI validation layer classifies each included record by Frascati Manual Chapter 12 budget type:

| Type | Description |
|------|-------------|
| `intramural_rd` | R&D performed inside a government agency or national laboratory |
| `extramural_grants` | Grants / contracts from government to universities, firms, or research institutes |
| `rd_coordination` | Funding for research councils, academies, or bodies that allocate R&D spending |
| `rd_infrastructure` | Large research facilities, equipment, observatories, scientific databases |
| `higher_ed_rd` | Block grants to universities where the R&D share cannot be separated from teaching |
| `not_rd` | Education (non-research), administration, social transfers, infrastructure maintenance |

---

## Re-running is safe

**Budget pipeline**
- Adding new PDFs → only new files are processed (tracked by `file_id`)
- Deleting a PDF → its rows are intentionally kept in `results.csv` (time-series preservation)
- Re-running `--rerun-countries Australia` → all Australia rows are replaced from scratch
- AI validation caches (`ai_cache.jsonl`, `aggregation_cache.jsonl`, `anomaly_cache.jsonl`) are append-only — re-running costs nothing for already-processed records
- AI decisions for confirmed records are never overwritten by re-extraction

**Reform pipeline**
- The LLM adjudicator checkpoints after every batch → safe to interrupt and resume
- `--reforms-build-panel-only` rebuilds the raw panel in seconds at zero API cost
- `--skip-llm` gives a fast taxonomy-only cleaning pass for inspection before committing to LLM calls
