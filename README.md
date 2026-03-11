# Innovation Policy PDF Pipeline (Prototype)

Reusable Python pipeline for batch-processing government finance bill PDFs in `data/pdf/`, including OCR fallback and multilingual keyword detection (English, French, Danish).

## Project tree

```text
Innovation Policy/
├─ data/
│  ├─ pdf/                          # Input PDFs (existing)
│  └─ processed/                    # Generated outputs
│     ├─ file_inventory.csv
│     ├─ innovation_candidates.csv
│     ├─ full_text/
│     │  ├─ <file_id>_<name>.txt
│     │  └─ <file_id>_<name>.docx   # optional if python-docx is available
│     └─ intermediate/
│        ├─ page_text.csv
│        ├─ file_text_summary.csv
│        ├─ keyword_hits.csv
│        └─ full_text_exports.csv
├─ src/
│  ├─ __init__.py
│  ├─ config.py
│  ├─ utils.py
│  ├─ inventory.py
│  ├─ ocr_utils.py
│  ├─ pdf_extract.py
│  ├─ language_utils.py
│  ├─ keyword_detection.py
│  └─ classifier.py
├─ main.py
├─ requirements.txt
└─ README.md
```

## What the pipeline does

1. File inventory stage
- Recursively scans `data/pdf/` for `.pdf` files.
- Builds `data/processed/file_inventory.csv` with:
  - `file_id`, `filepath`, `filename`, `country_guess`, `year_guess`, `extension`, `file_size`
- Country/year are inferred from filename/path using heuristics.

2. PDF extraction stage
- Reads each PDF page-by-page using PyMuPDF.
- Uses direct extraction first.
- If text is low quality (short or noisy), attempts OCR fallback (pytesseract).
- Exports one combined full-document text file per PDF to `data/processed/full_text/*.txt`.
- Also exports `.docx` when `python-docx` is installed.
- Saves:
  - `data/processed/intermediate/page_text.csv`
  - `data/processed/intermediate/file_text_summary.csv`
  - `data/processed/intermediate/full_text_exports.csv`
- One bad PDF is logged and skipped without stopping the batch.

3. Language-aware keyword detection
- Uses multilingual keyword dictionaries from `src/config.py`.
- Detects candidate innovation-policy pages.
- Saves `data/processed/intermediate/keyword_hits.csv` with:
  - `file_id`, `page_number`, `matched_keywords`, `keyword_count`, `candidate_score`, `text_snippet`, etc.

4. Placeholder classification stage
- Applies a rule-based placeholder classifier (future LLM swap point).
- Adds:
  - `innovation_relevant`, `category_guess`, `confidence`, `rationale`
- Final output:
  - `data/processed/innovation_candidates.csv`

## Setup

## 1) Create and activate a virtual environment (recommended)

Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

## 2) Install Python dependencies

```powershell
pip install -r requirements.txt
```

## 3) Optional OCR system dependency (recommended for scanned PDFs)

`pytesseract` requires the external Tesseract OCR binary installed on your OS.

Windows:
- Install Tesseract OCR (for example from UB Mannheim builds).
- Ensure `tesseract.exe` is on PATH.

If Tesseract is missing:
- The pipeline still runs for born-digital PDFs.
- OCR fallback is skipped gracefully.
- You will see a warning in logs.

## Run

From the project root:

```powershell
python main.py
```

## Expected outputs after running

- `data/processed/file_inventory.csv`:
  - One row per PDF discovered.
- `data/processed/intermediate/page_text.csv`:
  - One row per extracted page with method (`direct_text`, `ocr_fallback`, etc.).
- `data/processed/intermediate/file_text_summary.csv`:
  - Per-file stats (`total_pages`, `direct_pages`, `ocr_pages`, `status`).
- `data/processed/full_text/*.txt`:
  - Full text per PDF with page separators.
- `data/processed/full_text/*.docx`:
  - Same content in Word format when `python-docx` is available.
- `data/processed/intermediate/full_text_exports.csv`:
  - Export manifest with txt/docx paths and status.
- `data/processed/intermediate/keyword_hits.csv`:
  - Candidate pages with multilingual keyword matches.
- `data/processed/innovation_candidates.csv`:
  - Candidate pages + placeholder classification labels.

## Customization notes

- Keyword dictionaries: edit `KEYWORDS_BY_LANGUAGE` in `src/config.py`.
- OCR quality thresholds: adjust `MIN_DIRECT_TEXT_CHARS`, `MIN_ALNUM_RATIO` in `src/config.py`.
- Future LLM integration: replace logic inside `src/classifier.py` while keeping output schema.
