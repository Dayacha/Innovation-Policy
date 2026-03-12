"""Reporting utilities: budget item detection and output formatting."""

from __future__ import annotations

import pandas as pd

from src.budget_extractor import extract_budget_items
from src.utils import logger


def detect_budget_items(pages_df: pd.DataFrame) -> pd.DataFrame:
    """Detect R&D-relevant budget amounts using taxonomy scoring and section-aware parsing.

    Delegates to budget_extractor.extract_budget_items which:
    - Processes pages in document order to track § section context
    - Applies the boss's Excel taxonomy (J_Rules scoring model)
    - Handles Danish Finanslov structure and number format
    """
    if pages_df.empty:
        logger.warning("detect_budget_items: received empty DataFrame.")
        return extract_budget_items(pages_df)

    logger.info("detect_budget_items: processing %s pages across %s files",
                len(pages_df),
                pages_df["file_id"].nunique() if "file_id" in pages_df.columns else "?")
    return extract_budget_items(pages_df)


def build_results_json_records(budget_df: pd.DataFrame) -> list[dict]:
    """Build a compact JSON structure grouped by (country, year, source_file)."""
    if budget_df.empty:
        return []

    keep_cols = [
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
        "page_number",
        "source_file",
    ]

    records: list[dict] = []
    grouped = budget_df.groupby(["country", "year", "source_file"], dropna=False, sort=True)
    for (country, year, source_file), grp in grouped:
        items = grp[keep_cols].to_dict(orient="records")
        records.append(
            {
                "country": country,
                "year": year,
                "source_file": source_file,
                "items": items,
            }
        )
    return records
