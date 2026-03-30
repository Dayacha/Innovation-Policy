"""Budget extraction engine orchestration.

This module now coordinates:
1. file-level grouping and taxonomy selection
2. skip / dedicated-country routing
3. fallback to the generic Danish-structure parser
4. final post-processing
"""

from __future__ import annotations

import pandas as pd

from budget.dedicated_pipeline import (
    COUNTRY_SKIP_EXTRACTORS,
    handle_dedicated_country,
)
from budget.extractor_common import empty_df, filepath_from_row
from budget.generic_budget_pipeline import postprocess_records, process_generic_file
from budget.taxonomy import load_taxonomy
from budget.utils import logger


# Country → primary document language (for taxonomy extensions)
_COUNTRY_LANGUAGES: dict[str, tuple[str, ...]] = {
    "Denmark": ("danish",),
    "France": ("french",),
    "Germany": ("german",),
    "Japan": ("japanese",),
    "Sweden": ("swedish",),
    "Norway": ("norwegian",),
    "Finland": ("finnish", "swedish"),
    "Netherlands": (),
    "Belgium": ("french",),
    "United Kingdom": (),
}


def extract_budget_items(
    pages_df: pd.DataFrame,
    prior_results_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Extract R&D-relevant budget line items from page-level text."""
    if pages_df.empty:
        return empty_df()

    records: list[dict] = []
    filepath_col = "filepath" if "filepath" in pages_df.columns else "source_filepath"

    for file_id, file_df in pages_df.groupby("file_id", sort=False):
        sorted_pages = file_df.sort_values("page_number")
        first_row = sorted_pages.iloc[0]
        country_for_file = str(
            first_row.get("country_guess", "Unknown")
            if hasattr(first_row, "get") else getattr(first_row, "country_guess", "Unknown")
        )
        year_for_file = str(
            first_row.get("year_guess", "Unknown")
            if hasattr(first_row, "get") else getattr(first_row, "year_guess", "Unknown")
        )
        langs = _COUNTRY_LANGUAGES.get(country_for_file, ())
        tax = load_taxonomy(languages=tuple(langs))

        if country_for_file in COUNTRY_SKIP_EXTRACTORS:
            filepath_val = filepath_from_row(first_row, filepath_col)
            logger.debug(
                "Skipping %s (country=%s): PDF type not suitable for R&D extraction.",
                filepath_val.split("/")[-1], country_for_file,
            )
            continue

        was_handled = handle_dedicated_country(
            records=records,
            sorted_pages=sorted_pages,
            file_id=str(file_id),
            country_for_file=country_for_file,
            year_for_file=year_for_file,
            filepath_col=filepath_col,
            tax=tax,
            prior_results_df=prior_results_df,
        )
        if was_handled:
            continue

        process_generic_file(
            sorted_pages=sorted_pages,
            file_id=file_id,
            filepath_col=filepath_col,
            prior_results_df=prior_results_df,
            records=records,
            tax=tax,
        )

    df = postprocess_records(pd.DataFrame(records))
    logger.info("Budget items extracted: %s", len(df))
    return df


__all__ = ["extract_budget_items"]
