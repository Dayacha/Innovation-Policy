"""Main entry point for the innovation policy PDF processing pipeline."""

from datetime import datetime
import json
from pathlib import Path
import shutil

import pandas as pd

from src.classifier import classify_candidates
from src.config import (
    BUDGET_ITEMS_FILE,
    CANDIDATES_FILE,
    FILE_INVENTORY_FILE,
    FULLTEXT_DIR,
    FULLTEXT_EN_DIR,
    FULLTEXT_EN_EXPORT_MANIFEST_FILE,
    FULLTEXT_EXPORT_MANIFEST_FILE,
    PAGE_EXTRACTION_FILE,
    PER_FILE_SUMMARY_FILE,
    PDF_ROOT,
    PROCESSED_DIR,
    RESULTS_EXCEL_FILE,
    RESULTS_FILE,
    RESULTS_JSON_FILE,
    RUNS_DIR,
)
from src.exporters import export_full_documents, export_full_documents_english
from src.inventory import build_file_inventory
from src.keyword_detection import detect_candidate_pages
from src.pdf_extract import extract_text_for_inventory
from src.reporting import (
    build_results_json_records,
    build_results_text,
    detect_budget_items,
)
from src.utils import configure_logging, ensure_directories, logger

LEGACY_DEMO_RESULTS_FILE = PROCESSED_DIR / "demo_results.txt"
LEGACY_CANDIDATE_PAGES_FILE = PROCESSED_DIR / "candidate_pages_detected.csv"


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
    """Run the full pipeline from inventory to candidate aggregation."""
    configure_logging()
    ensure_directories(
        [PDF_ROOT, PROCESSED_DIR, PAGE_EXTRACTION_FILE.parent, FULLTEXT_DIR, FULLTEXT_EN_DIR, RUNS_DIR]
    )
    if LEGACY_DEMO_RESULTS_FILE.exists():
        try:
            LEGACY_DEMO_RESULTS_FILE.unlink()
            logger.info("Removed legacy file: %s", LEGACY_DEMO_RESULTS_FILE)
        except Exception as exc:
            logger.warning("Could not remove legacy file %s: %s", LEGACY_DEMO_RESULTS_FILE, exc)
    if LEGACY_CANDIDATE_PAGES_FILE.exists():
        try:
            LEGACY_CANDIDATE_PAGES_FILE.unlink()
            logger.info("Removed legacy file: %s", LEGACY_CANDIDATE_PAGES_FILE)
        except Exception as exc:
            logger.warning("Could not remove legacy file %s: %s", LEGACY_CANDIDATE_PAGES_FILE, exc)

    logger.info("Starting innovation policy PDF pipeline")
    logger.info("Input PDF directory: %s", PDF_ROOT.resolve())
    logger.info("Processed output directory: %s", PROCESSED_DIR.resolve())

    inventory_df = build_file_inventory(PDF_ROOT)
    inventory_df.to_csv(FILE_INVENTORY_FILE, index=False, encoding="utf-8")
    logger.info("Inventory saved: %s (rows=%s)", FILE_INVENTORY_FILE, len(inventory_df))

    if inventory_df.empty:
        logger.warning("No PDF files found. Pipeline ended after inventory stage.")
        return

    pages_df, summary_df = extract_text_for_inventory(inventory_df)
    pages_df.to_csv(PAGE_EXTRACTION_FILE, index=False, encoding="utf-8")
    summary_df.to_csv(PER_FILE_SUMMARY_FILE, index=False, encoding="utf-8")
    logger.info("Page-level extraction saved: %s (rows=%s)", PAGE_EXTRACTION_FILE, len(pages_df))
    logger.info("Per-file extraction summary saved: %s (rows=%s)", PER_FILE_SUMMARY_FILE, len(summary_df))

    exports_df = export_full_documents(pages_df)
    exports_df.to_csv(FULLTEXT_EXPORT_MANIFEST_FILE, index=False, encoding="utf-8")
    logger.info(
        "Full text export manifest saved: %s (rows=%s)",
        FULLTEXT_EXPORT_MANIFEST_FILE,
        len(exports_df),
    )
    exports_en_df = export_full_documents_english(pages_df)
    exports_en_df.to_csv(FULLTEXT_EN_EXPORT_MANIFEST_FILE, index=False, encoding="utf-8")
    logger.info(
        "Full text English export manifest saved: %s (rows=%s)",
        FULLTEXT_EN_EXPORT_MANIFEST_FILE,
        len(exports_en_df),
    )

    candidates_df = detect_candidate_pages(pages_df)
    if not candidates_df.empty:
        classified_df = classify_candidates(candidates_df)
    else:
        classified_df = pd.DataFrame()

    classified_df.to_csv(CANDIDATES_FILE, index=False, encoding="utf-8")
    logger.info("Innovation candidates saved: %s (rows=%s)", CANDIDATES_FILE, len(classified_df))

    relevant_ids = set()
    if (
        not classified_df.empty
        and "candidate_id" in classified_df.columns
        and "innovation_relevant" in classified_df.columns
    ):
        relevant_ids = set(
            classified_df.loc[classified_df["innovation_relevant"] == True, "candidate_id"]  # noqa: E712
        )
    relevant_keyword_hits_df = (
        candidates_df[candidates_df["candidate_id"].isin(relevant_ids)].copy()
        if (not candidates_df.empty and "candidate_id" in candidates_df.columns and relevant_ids)
        else candidates_df.head(0).copy()
    )

    budget_df = detect_budget_items(relevant_keyword_hits_df)
    budget_df.to_csv(BUDGET_ITEMS_FILE, index=False, encoding="utf-8")
    logger.info("Budget item output saved: %s (rows=%s)", BUDGET_ITEMS_FILE, len(budget_df))

    budget_db_df = budget_df[
        [
            "file_label",
            "source_filename",
            "page_number",
            "keywords_matched",
            "detected_amount_raw",
            "detected_amount_value",
            "detected_currency",
            "category_guess",
        ]
    ].copy() if not budget_df.empty else pd.DataFrame(
        columns=[
            "file_label",
            "source_filename",
            "page_number",
            "keywords_matched",
            "detected_amount_raw",
            "detected_amount_value",
            "detected_currency",
            "category_guess",
        ]
    )
    budget_db_df.to_excel(RESULTS_EXCEL_FILE, index=False)
    logger.info("Results Excel saved: %s (rows=%s)", RESULTS_EXCEL_FILE, len(budget_db_df))

    results_text = build_results_text(budget_df)
    RESULTS_FILE.write_text(results_text, encoding="utf-8")
    logger.info("Results text saved: %s", RESULTS_FILE)

    results_json_records = build_results_json_records(budget_df)
    RESULTS_JSON_FILE.write_text(
        json.dumps(results_json_records, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info("Results JSON saved: %s", RESULTS_JSON_FILE)

    snapshot_dir = save_run_snapshot(
        [
            RESULTS_FILE,
            RESULTS_JSON_FILE,
            RESULTS_EXCEL_FILE,
            BUDGET_ITEMS_FILE,
            CANDIDATES_FILE,
            FILE_INVENTORY_FILE,
        ]
    )
    logger.info("Run snapshot saved to: %s", snapshot_dir)
    logger.info("Pipeline finished successfully.")


if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as exc:
        logger.exception("Pipeline failed with an unexpected error: %s", exc)
