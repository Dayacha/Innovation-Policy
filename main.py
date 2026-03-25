"""Unified entry point for the Innovation Policy pipeline.

Budget pipeline (Stream 1):
    Extracts R&D / innovation spending time-series from scanned Finance Bill PDFs.
    Input:  Data/PDF/<Country>/*.pdf
    Output: data/processed/results.csv  |  results.xlsx  |  results.json

Reform pipeline (Stream 2):
    Extracts structural reform events from OECD Economic Survey PDFs using an LLM.
    Input:  Data/surveys/<COUNTRY_CODE>_<YEAR>.pdf
    Output: data/processed/reforms/output/reform_panel.csv  |  reforms_events.csv

Usage:
    python main.py                          # run both pipelines
    python main.py --budget-only            # only budget extraction
    python main.py --reforms-only           # only reform extraction
    python main.py --reforms-country FRA    # reforms for one country
    python main.py --reforms-year 2019      # reforms for one year
    python main.py --reforms-build-panel-only  # rebuild reform panel, no LLM calls
    python main.py --reforms-fetch-catalog     # query Kappa API, update kappa_catalog.json
    python main.py --reforms-download          # download PDFs from catalog
    python main.py --reforms-download --reforms-country DNK --reforms-year 2024
    python main.py --run-ai-validation ...  # budget AI validation flags
"""

import argparse
import json
from datetime import datetime
from pathlib import Path
import shutil

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent

from budget.ai_validation import AIValidationConfig, run_ai_validation, _load_baseline
from budget.config import (
    BUDGET_ITEMS_FILE,
    CANDIDATES_FILE,
    FILE_INVENTORY_FILE,
    FULLTEXT_DIR,
    FULLTEXT_EXPORT_MANIFEST_FILE,
    PAGE_EXTRACTION_FILE,
    PER_FILE_SUMMARY_FILE,
    PDF_ROOT,
    PROCESSED_DIR,
    RESULTS_AI_VERIFIED_FILE,
    RESULTS_CSV_FILE,
    RESULTS_EXCEL_FILE,
    RESULTS_JSON_FILE,
    RESULTS_REVIEW_STATUS_FILE,
    RUNS_DIR,
)
from budget.exporters import export_full_documents
from budget.inventory import build_file_inventory
from budget.keyword_detection import detect_candidate_pages
from budget.pdf_extract import extract_text_for_inventory
from budget.reporting import build_results_json_records, detect_budget_items
from budget.ai_validation_filter import filter_ai_validated
from budget.utils import configure_logging, ensure_directories, logger

LEGACY_DEMO_RESULTS_FILE = PROCESSED_DIR / "demo_results.txt"
LEGACY_CANDIDATE_PAGES_FILE = PROCESSED_DIR / "candidate_pages_detected.csv"

# Columns exported to results.csv / results.xlsx — ordered for readability.
# Original-language fields sit next to their English translations so reviewers
# can verify without opening the source PDF.
_RESULTS_EXPORT_COLS = [
    "country",
    "year",
    "section_code",
    "section_name",          # original language
    "section_name_en",       # English translation
    "program_code",
    "program_description",   # original language
    "program_description_en",
    "line_description",      # original language
    "line_description_en",   # English translation
    "budget_type",
    "amount_local",
    "currency",
    "rd_category",
    "pillar",
    "taxonomy_score",
    "decision",
    "confidence",
    "rationale",
    "source_file",
    "page_number",
]

_REVIEW_STATUS_KEY_COLS = [
    "country",
    "year",
    "section_code",
    "program_code",
    "program_description",
    "line_description",
    "amount_local",
    "page_number",
    "source_file",
]

def save_run_snapshot(files_to_copy: list[Path]) -> Path:
    """Save a timestamped copy of selected outputs for this run."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = RUNS_DIR / f"run_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)

    for file_path in files_to_copy:
        if file_path.exists():
            shutil.copy2(file_path, run_dir / file_path.name)

    return run_dir


def _review_match_key(row: pd.Series) -> str:
    parts: list[str] = []
    for col in _REVIEW_STATUS_KEY_COLS:
        value = row[col] if col in row.index else ""
        if pd.isna(value):
            value = ""
        if col == "amount_local":
            try:
                value = f"{float(value):.6f}" if value != "" else ""
            except (TypeError, ValueError):
                value = str(value)
        parts.append(str(value).strip().lower())
    return "|".join(parts)


def write_review_status_file(
    baseline_file: Path = RESULTS_CSV_FILE,
    verified_file: Path = RESULTS_AI_VERIFIED_FILE,
    output_file: Path = RESULTS_REVIEW_STATUS_FILE,
) -> Path | None:
    """Write a current-status file that marks each baseline row as reviewed or pending."""
    if not baseline_file.exists():
        return None

    baseline_df = _load_baseline(baseline_file)
    if baseline_df.empty:
        baseline_df["review_status"] = pd.Series(dtype="object")
        baseline_df.to_csv(output_file, index=False, encoding="utf-8")
        return output_file

    baseline_df = baseline_df.copy()
    baseline_df["review_key"] = baseline_df.apply(_review_match_key, axis=1)

    reviewed_keys: set[str] = set()
    if verified_file.exists():
        try:
            verified_df = pd.read_csv(verified_file)
            if not verified_df.empty:
                verified_df["review_key"] = verified_df.apply(_review_match_key, axis=1)
                reviewed_keys = set(verified_df["review_key"].dropna().astype(str))
        except Exception as exc:
            logger.warning("Could not build review status from %s: %s", verified_file, exc)

    baseline_df["review_status"] = baseline_df["review_key"].apply(
        lambda value: "reviewed" if value in reviewed_keys else "pending_ai_review"
    )
    baseline_df = baseline_df.drop(columns="review_key")
    baseline_df.to_csv(output_file, index=False, encoding="utf-8")
    reviewed_count = int((baseline_df["review_status"] == "reviewed").sum())
    pending_count = int((baseline_df["review_status"] == "pending_ai_review").sum())
    logger.info(
        "Review status saved: %s (reviewed=%s, pending=%s)",
        output_file,
        reviewed_count,
        pending_count,
    )
    return output_file


def _load_existing_dataframe(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as exc:
        logger.warning("Could not read %s: %s", path, exc)
        return pd.DataFrame()


def merge_incremental_budget_results(
    current_budget_df: pd.DataFrame,
    pages_df: pd.DataFrame,
    budget_items_file: Path = BUDGET_ITEMS_FILE,
) -> pd.DataFrame:
    """Merge newly extracted budget items with previously saved ones.

    Previous results for deleted PDFs are intentionally preserved so that
    removing a file from Data/PDF does not erase its contribution to the
    cumulative time-series database.  Only records whose file_id is being
    actively re-processed in this run are replaced.
    """
    previous_budget_df = _load_existing_dataframe(budget_items_file)
    if previous_budget_df.empty:
        return current_budget_df

    current_file_ids = set()
    if not current_budget_df.empty and "file_id" in current_budget_df.columns:
        current_file_ids = set(current_budget_df["file_id"].dropna().astype(str))

    # Keep ALL previous rows except those being replaced in this run.
    # Deleted PDFs are intentionally kept — their data is not lost.
    keep_previous = previous_budget_df.copy()
    if current_file_ids and "file_id" in keep_previous.columns:
        keep_previous = keep_previous[~keep_previous["file_id"].astype(str).isin(current_file_ids)]

    if current_budget_df.empty:
        return keep_previous.reset_index(drop=True)

    merged = pd.concat([keep_previous, current_budget_df], ignore_index=True, sort=False)
    return merged.reset_index(drop=True)


def refresh_budget_metadata_from_pages(
    budget_df: pd.DataFrame,
    pages_df: pd.DataFrame,
) -> pd.DataFrame:
    """Refresh filename/country/year metadata for cached budget rows using current pages_df."""
    if budget_df.empty or pages_df.empty or "file_id" not in budget_df.columns or "file_id" not in pages_df.columns:
        return budget_df

    meta_cols = ["file_id", "filepath", "country_guess", "year_guess"]
    meta_df = (
        pages_df[meta_cols]
        .dropna(subset=["file_id"])
        .drop_duplicates(subset=["file_id"], keep="last")
        .copy()
    )
    meta_df["source_file"] = meta_df["filepath"].astype(str).map(lambda value: Path(value).name)
    meta_df = meta_df.rename(
        columns={
            "country_guess": "__country_guess",
            "year_guess": "__year_guess",
            "source_file": "__source_file",
        }
    )

    merged = budget_df.merge(meta_df[["file_id", "__source_file", "__country_guess", "__year_guess"]], on="file_id", how="left")
    if "source_file" in merged.columns:
        merged["source_file"] = merged["__source_file"].fillna(merged["source_file"])
    if "country" in merged.columns:
        merged["country"] = merged["__country_guess"].fillna(merged["country"])
    if "year" in merged.columns:
        merged["year"] = merged["__year_guess"].fillna(merged["year"])
    merged = merged.drop(columns=["__source_file", "__country_guess", "__year_guess"])
    return merged


def run_budget_pipeline() -> None:
    """Run the full budget pipeline from inventory to budget item extraction."""
    configure_logging()
    ensure_directories(
        [PDF_ROOT, PROCESSED_DIR, PAGE_EXTRACTION_FILE.parent, FULLTEXT_DIR, RUNS_DIR]
    )

    # Remove legacy files from old pipeline versions
    for legacy in (LEGACY_DEMO_RESULTS_FILE, LEGACY_CANDIDATE_PAGES_FILE):
        if legacy.exists():
            try:
                legacy.unlink()
                logger.info("Removed legacy file: %s", legacy)
            except Exception as exc:
                logger.warning("Could not remove legacy file %s: %s", legacy, exc)

    logger.info("Starting innovation policy PDF pipeline")
    logger.info("Input PDF directory:    %s", PDF_ROOT.resolve())
    logger.info("Processed output:       %s", PROCESSED_DIR.resolve())

    # ── Stage 1: Inventory ────────────────────────────────────────────────────
    inventory_df = build_file_inventory(PDF_ROOT)
    inventory_df.to_csv(FILE_INVENTORY_FILE, index=False, encoding="utf-8")
    logger.info("Inventory saved: %s (rows=%s)", FILE_INVENTORY_FILE, len(inventory_df))

    if inventory_df.empty:
        logger.warning("No PDF files found. Pipeline ended after inventory stage.")
        return

    # ── Stage 2: OCR / text extraction ───────────────────────────────────────
    pages_df, summary_df = extract_text_for_inventory(inventory_df)
    pages_df.to_csv(PAGE_EXTRACTION_FILE, index=False, encoding="utf-8")
    summary_df.to_csv(PER_FILE_SUMMARY_FILE, index=False, encoding="utf-8")
    logger.info("Page extraction saved: %s (rows=%s)", PAGE_EXTRACTION_FILE, len(pages_df))

    # Save full raw text per document (useful for OCR quality inspection)
    exports_df = export_full_documents(pages_df)
    exports_df.to_csv(FULLTEXT_EXPORT_MANIFEST_FILE, index=False, encoding="utf-8")
    logger.info("Full text exports: %s files", len(exports_df))

    # ── Stage 3: Keyword detection (intermediate output, not used for budget) ─
    # Saves keyword_hits.csv — useful for debugging which pages were flagged
    candidates_df = detect_candidate_pages(pages_df)
    candidates_df.to_csv(CANDIDATES_FILE, index=False, encoding="utf-8")
    logger.info("Keyword candidates saved: %s (rows=%s)", CANDIDATES_FILE, len(candidates_df))

    # ── Stage 4: Budget extraction ────────────────────────────────────────────
    # Uses full pages_df (not just candidates) so the section parser can track
    # § context across consecutive pages of each document.
    previous_budget_df = _load_existing_dataframe(BUDGET_ITEMS_FILE)
    previous_budget_file_ids = (
        set(previous_budget_df["file_id"].dropna().astype(str))
        if not previous_budget_df.empty and "file_id" in previous_budget_df.columns
        else set()
    )
    current_page_file_ids = (
        set(pages_df["file_id"].dropna().astype(str))
        if not pages_df.empty and "file_id" in pages_df.columns
        else set()
    )
    file_ids_to_process = sorted(current_page_file_ids - previous_budget_file_ids)

    if file_ids_to_process:
        logger.info("Budget extraction will process %s new/changed PDF(s)", len(file_ids_to_process))
        budget_input_df = pages_df[pages_df["file_id"].astype(str).isin(file_ids_to_process)].copy()
        incremental_budget_df = detect_budget_items(budget_input_df)
    else:
        logger.info("No new PDF content detected; reusing existing budget extraction results.")
        incremental_budget_df = pd.DataFrame(columns=previous_budget_df.columns if not previous_budget_df.empty else None)

    budget_df = merge_incremental_budget_results(incremental_budget_df, pages_df, BUDGET_ITEMS_FILE)
    budget_df.to_csv(BUDGET_ITEMS_FILE, index=False, encoding="utf-8")
    logger.info("Budget items saved: %s (rows=%s)", BUDGET_ITEMS_FILE, len(budget_df))

    # ── Stage 5: Outputs ──────────────────────────────────────────────────────
    budget_df = refresh_budget_metadata_from_pages(budget_df, pages_df)
    available_cols = [c for c in _RESULTS_EXPORT_COLS if c in budget_df.columns]
    results_df = budget_df[available_cols].copy() if not budget_df.empty else pd.DataFrame(columns=available_cols)

    # CSV — machine-readable, UTF-8
    results_df.to_csv(RESULTS_CSV_FILE, index=False, encoding="utf-8")
    logger.info("Results CSV saved: %s (rows=%s)", RESULTS_CSV_FILE, len(results_df))

    # Excel — human review (Balazs): original + English side-by-side
    try:
        results_df.to_excel(RESULTS_EXCEL_FILE, index=False, engine="openpyxl")
        logger.info("Results Excel saved: %s", RESULTS_EXCEL_FILE)
    except Exception as exc:
        logger.warning("Excel export failed (openpyxl not installed?): %s", exc)

    # results.txt — human-readable top-10 summary
    # results.json — structured records for downstream analysis
    results_json_records = build_results_json_records(budget_df)
    RESULTS_JSON_FILE.write_text(
        json.dumps(results_json_records, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info("Results JSON saved: %s", RESULTS_JSON_FILE)

    # ── Stage 6: Clean AI validation candidates from previous runs ────────
    ai_root = PROCESSED_DIR / "ai_validation"
    written = filter_ai_validated(ai_root)
    if written:
        logger.info("AI validation cleaned files: %s", ", ".join(str(p) for p in written))

    logger.info("Pipeline finished successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Innovation Policy Pipeline — budget extraction + reform analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    # ── Pipeline selection ────────────────────────────────────────────────────
    pipeline_group = parser.add_mutually_exclusive_group()
    pipeline_group.add_argument(
        "--budget-only", action="store_true",
        help="Run only the Finance Bill budget extraction pipeline",
    )
    pipeline_group.add_argument(
        "--reforms-only", action="store_true",
        help="Run only the OECD Economic Survey reform extraction pipeline",
    )

    # ── Reform pipeline flags (defined in pipeline_reforms.py) ───────────────
    from reforms.pipeline_reforms import add_arguments as _add_reform_args
    _add_reform_args(parser)

    # ── Budget pipeline flags ─────────────────────────────────────────────────
    parser.add_argument("--ai-only", action="store_true", help="Skip PDF extraction and run only AI validation/status checks on existing results")
    parser.add_argument("--run-ai-validation", action="store_true", help="Run optional AI validation on extracted candidates")
    parser.add_argument("--input-file", type=Path, default=RESULTS_CSV_FILE, help="Baseline extraction file for AI validation (CSV or JSON)")
    parser.add_argument("--max-records-to-send", type=int, default=None, help="Cap the number of records sent to the AI model")
    parser.add_argument("--min-amount-threshold", type=float, default=None, help="Optional minimum amount filter before AI")
    parser.add_argument("--include-review-only", action="store_true", help="Only send baseline records marked as review")
    parser.add_argument("--batch-size", type=int, default=None, help="Batch size for AI requests (default 4 when AI is enabled)")
    parser.add_argument("--ai-model", type=str, default=None, help="Model for budget AI validation (default: reads from config.yaml llm.model, falls back to gpt-4o-mini)")
    parser.add_argument("--ai-temperature", type=float, default=0.1, help="Sampling temperature for AI validation")
    parser.add_argument(
        "--ai-output-format",
        type=str,
        choices=["csv", "json", "both"],
        default="both",
        help="Format for baseline vs AI comparison output",
    )
    parser.add_argument(
        "--ai-group-by-page",
        action="store_true",
        default=None,
        help="Batch AI calls by page_number to avoid repeating page context (default on when AI enabled)",
    )
    parser.add_argument(
        "--ai-include-context",
        action="store_true",
        default=None,
        help="Include optional context fields (context_before/context_after/raw_page_text_excerpt) in AI prompts (default on when AI enabled)",
    )
    parser.add_argument(
        "--ai-run-name",
        type=str,
        default=None,
        help="Name of the AI run; outputs go to data/processed/ai_validation/<run_name>/ (auto-generated if omitted)",
    )
    parser.add_argument("--ai-filter-country", type=str, default=None, help="Optional country filter for AI validation")
    parser.add_argument("--ai-filter-year", type=str, default=None, help="Optional year filter for AI validation")
    parser.add_argument(
        "--skip-verified-records",
        action="store_true",
        help="Skip rows already present in results_ai_verified.csv when sending data to AI",
    )

    args = parser.parse_args()

    try:
        run_budget = not args.reforms_only
        run_reforms = not args.budget_only

        if run_budget:
            if not args.ai_only:
                run_budget_pipeline()
            else:
                configure_logging()
                logger.info("AI-only mode enabled; skipping PDF extraction pipeline.")

        write_review_status_file()
        if args.run_ai_validation:
            # Resolve AI model: CLI arg → config.yaml llm.model → fallback
            ai_model = args.ai_model
            if not ai_model:
                try:
                    import yaml
                    _cfg_path = PROJECT_ROOT / "config.yaml"
                    if _cfg_path.exists():
                        with open(_cfg_path) as _f:
                            _cfg = yaml.safe_load(_f)
                        ai_model = (
                            _cfg.get("budget", {}).get("ai_validation_model")
                            or _cfg.get("llm", {}).get("model")
                        )
                except Exception:
                    pass
            ai_model = ai_model or "gpt-4o-mini"

            # Apply sensible defaults when AI validation is requested
            ai_run_name = args.ai_run_name or "ai_run"
            ai_batch_size = args.batch_size or 4
            ai_include_context = True if args.ai_include_context is None else args.ai_include_context
            ai_group_by_page = True if args.ai_group_by_page is None else args.ai_group_by_page

            # Determine which years to send. If user didn't pass --ai-filter-year,
            # iterate per year (after optional country filter) to merge gradually.
            baseline_df = _load_baseline(Path(args.input_file))
            if args.ai_filter_country:
                baseline_df = baseline_df[baseline_df["country"].astype(str).str.lower() == str(args.ai_filter_country).lower()]
            if args.ai_filter_year:
                years_to_run = [args.ai_filter_year]
            else:
                years_to_run = sorted(baseline_df["year"].dropna().unique().tolist())

            for yr in years_to_run:
                ai_config = AIValidationConfig(
                    input_file=args.input_file,
                    max_records_to_send=args.max_records_to_send,
                    min_amount_threshold=args.min_amount_threshold,
                    include_review_only=args.include_review_only,
                    batch_size=ai_batch_size,
                    model=ai_model,
                    temperature=args.ai_temperature,
                    output_format=args.ai_output_format,
                    group_by_page=ai_group_by_page,
                    include_context=ai_include_context,
                    run_name=ai_run_name,
                    filter_country=args.ai_filter_country,
                    filter_year=yr,
                    skip_verified_records=args.skip_verified_records,
                )
                logger.info("AI validation pass for year=%s country=%s", yr, args.ai_filter_country or "ALL")
                ai_completed = run_ai_validation(ai_config)
                if not ai_completed:
                    logger.warning("Skipping merge for year=%s because AI validation did not complete.", yr)
                    continue

                # Re-run cleaning for the current AI run to produce ai_ready_for_verification.csv
                run_root = PROCESSED_DIR / "ai_validation" / ai_run_name
                filter_ai_validated(run_root)

                # Copy (and merge) final verified file next to main results
                verified_src = run_root / "ai_ready_for_verification.csv"
                verified_dst = RESULTS_AI_VERIFIED_FILE
                if verified_src.exists():
                    new_df = pd.read_csv(verified_src)
                    if verified_dst.exists():
                        try:
                            prior_df = pd.read_csv(verified_dst)
                            combined = (
                                pd.concat([prior_df, new_df], ignore_index=True)
                                .drop_duplicates(
                                    subset=[
                                        "country",
                                        "year",
                                        "section_code",
                                        "program_code",
                                        "program_description",
                                        "amount_local",
                                        "page_number",
                                    ]
                                )
                            )
                            combined.to_csv(verified_dst, index=False)
                            logger.info(
                                "Merged AI verified results (%s new, %s total) into %s",
                                len(new_df),
                                len(combined),
                                verified_dst,
                            )
                        except Exception as exc:
                            logger.warning("Merge failed, copying new file only: %s", exc)
                            shutil.copy2(verified_src, verified_dst)
                    else:
                        shutil.copy2(verified_src, verified_dst)
                        logger.info("Results AI verified saved to: %s", verified_dst)
                    write_review_status_file()
                else:
                    logger.warning("AI verified file not found: %s", verified_src)

        # ── Reform pipeline ───────────────────────────────────────────────────
        if run_reforms:
            from reforms.pipeline_reforms import run_from_args as _run_reforms
            _run_reforms(args)

    except Exception as exc:
        logger.exception("Pipeline failed with an unexpected error: %s", exc)
