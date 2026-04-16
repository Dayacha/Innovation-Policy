"""
Country-specific post-extraction cleaners for the budget pipeline.

Each cleaner is a module in this package named after the country
(lowercase, spaces replaced with underscores): australia.py, united_kingdom.py, etc.

Each module must expose a single function:
    clean(df: pd.DataFrame) -> pd.DataFrame

The function receives the full results DataFrame for ONE country and returns
a modified copy. It may:
  - Change `decision` from 'include' → 'review' or add a note
  - Drop rows that are confirmed false positives
  - Correct misclassified `item_type` values
  - Fix unit/amount errors found during manual audit

All changes should be recorded in the `cleaning_notes` column so the
audit trail is preserved.

Usage:
    from budget.cleaners import apply_country_cleaner
    df = apply_country_cleaner(df, country="Australia")
"""

from __future__ import annotations

import importlib
import logging
import pandas as pd

logger = logging.getLogger(__name__)

__all__ = ["apply_country_cleaner"]


def apply_country_cleaner(df: pd.DataFrame, country: str) -> pd.DataFrame:
    """Apply the country-specific cleaner to a results DataFrame.

    Returns the cleaned DataFrame. If no cleaner exists for the country,
    returns df unchanged.
    """
    if df.empty:
        return df

    # Ensure cleaning_notes column exists
    if "cleaning_notes" not in df.columns:
        df = df.copy()
        df["cleaning_notes"] = ""

    module_name = country.lower().replace(" ", "_").replace("-", "_")
    try:
        mod = importlib.import_module(f"budget.cleaners.{module_name}")
    except ModuleNotFoundError:
        logger.debug(f"No country cleaner for '{country}' (budget/cleaners/{module_name}.py)")
        return df

    if not hasattr(mod, "clean"):
        logger.warning(f"Cleaner module {module_name}.py has no clean() function — skipping.")
        return df

    before = len(df)
    df = mod.clean(df.copy())
    after = len(df)

    dropped = before - after
    changed = (df["cleaning_notes"] != "").sum()
    logger.info(
        f"[{country}] country cleaner: {dropped} rows dropped, "
        f"{changed} rows annotated."
    )
    return df
