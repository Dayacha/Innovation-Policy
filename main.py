"""Main entry point for the innovation policy PDF processing pipeline."""

import argparse
import json
from datetime import datetime
from pathlib import Path
import shutil

import pandas as pd

from src.ai_validation import AIValidationConfig, run_ai_validation
from src.config import (
    BUDGET_ITEMS_FILE,
    CANDIDATES_FILE,
    FILE_INVENTORY_FILE,
    FULLTEXT_DIR,
    FULLTEXT_EXPORT_MANIFEST_FILE,
    PAGE_EXTRACTION_FILE,
    PER_FILE_SUMMARY_FILE,
    PDF_ROOT,
    PROCESSED_DIR,
    RESULTS_CSV_FILE,
    RESULTS_JSON_FILE,
    RUNS_DIR,
)
from src.exporters import export_full_documents
from src.inventory import build_file_inventory
from src.keyword_detection import detect_candidate_pages
from src.pdf_extract import extract_text_for_inventory
from src.reporting import build_results_json_records, detect_budget_items
from src.ai_validation_filter import filter_ai_validated
from src.utils import configure_logging, ensure_directories, logger

LEGACY_DEMO_RESULTS_FILE = PROCESSED_DIR / "demo_results.txt"
LEGACY_CANDIDATE_PAGES_FILE = PROCESSED_DIR / "candidate_pages_detected.csv"

# Columns shown to Balazs in results.xlsx — human-readable, English
_RESULTS_EXCEL_COLS = [
    "country",
    "year",
    "section_code",
    "section_name_en",
    "program_code",
    "program_description",
    "line_description",
    "amount_local",
    "currency",
    "rd_category",
    "pillar",
    "decision",
    "confidence",
    "source_file",
    "page_number",
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


def run_pipeline() -> None:
    """Run the full pipeline from inventory to budget item extraction."""
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
    budget_df = detect_budget_items(pages_df)
    budget_df.to_csv(BUDGET_ITEMS_FILE, index=False, encoding="utf-8")
    logger.info("Budget items saved: %s (rows=%s)", BUDGET_ITEMS_FILE, len(budget_df))

    # ── Stage 5: Outputs for Balazs ───────────────────────────────────────────
    # results.xlsx — clean English-language view for non-Danish readers
    available_cols = [c for c in _RESULTS_EXCEL_COLS if c in budget_df.columns]
    results_csv_df = budget_df[available_cols].copy() if not budget_df.empty else pd.DataFrame(columns=available_cols)
    results_csv_df.to_csv(RESULTS_CSV_FILE, index=False, encoding="utf-8")
    logger.info("Results CSV saved: %s (rows=%s)", RESULTS_CSV_FILE, len(results_csv_df))

    # results.txt — human-readable top-10 summary
    # results.json — structured records for downstream analysis
    results_json_records = build_results_json_records(budget_df)
    RESULTS_JSON_FILE.write_text(
        json.dumps(results_json_records, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info("Results JSON saved: %s", RESULTS_JSON_FILE)

    # ── Stage 6: Clean AI validation candidates if present ────────────────
    ai_root = PROCESSED_DIR / "ai_validation"
    written = filter_ai_validated(ai_root)
    if written:
        logger.info("AI validation cleaned files: %s", ", ".join(str(p) for p in written))

    logger.info("Pipeline finished successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Innovation policy PDF pipeline with optional AI validation")
    parser.add_argument("--run-ai-validation", action="store_true", help="Run optional AI validation on extracted candidates")
    parser.add_argument("--input-file", type=Path, default=RESULTS_CSV_FILE, help="Baseline extraction file for AI validation (CSV or JSON)")
    parser.add_argument("--max-records-to-send", type=int, default=None, help="Cap the number of records sent to the AI model")
    parser.add_argument("--min-amount-threshold", type=float, default=None, help="Optional minimum amount filter before AI")
    parser.add_argument("--include-review-only", action="store_true", help="Only send baseline records marked as review")
    parser.add_argument("--batch-size", type=int, default=None, help="Batch size for AI requests (default 4 when AI is enabled)")
    parser.add_argument("--ai-model", type=str, default="gpt-4o-mini", help="Model name for AI validation")
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

    args = parser.parse_args()

    try:
        run_pipeline()
        if args.run_ai_validation:
            # Apply sensible defaults when AI validation is requested
            ai_run_name = args.ai_run_name or "ai_run"
            ai_batch_size = args.batch_size or 4
            ai_include_context = True if args.ai_include_context is None else args.ai_include_context
            ai_group_by_page = True if args.ai_group_by_page is None else args.ai_group_by_page

            ai_config = AIValidationConfig(
                input_file=args.input_file,
                max_records_to_send=args.max_records_to_send,
                min_amount_threshold=args.min_amount_threshold,
                include_review_only=args.include_review_only,
                batch_size=ai_batch_size,
                model=args.ai_model,
                temperature=args.ai_temperature,
                output_format=args.ai_output_format,
                group_by_page=ai_group_by_page,
                include_context=ai_include_context,
                run_name=ai_run_name,
                filter_country=args.ai_filter_country,
                filter_year=args.ai_filter_year,
            )
            run_ai_validation(ai_config)
            # Copy final verified file next to main results
            verified_src = PROCESSED_DIR / "ai_validation" / ai_run_name / "ai_ready_for_verification.csv"
            verified_dst = PROCESSED_DIR / "results_ai_verified.csv"
            if verified_src.exists():
                shutil.copy2(verified_src, verified_dst)
                logger.info("Results AI verified saved to: %s", verified_dst)
            else:
                logger.warning("AI verified file not found: %s", verified_src)
    except Exception as exc:
        logger.exception("Pipeline failed with an unexpected error: %s", exc)
