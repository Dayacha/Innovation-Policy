"""
New Zealand-specific post-extraction cleaner.
Populate after first run and manual audit.
"""
from __future__ import annotations
import pandas as pd

__all__ = ["clean"]


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """NZ-specific cleaning rules — to be populated after first audit."""
    # Add rules here as you discover false positives.
    # See australia.py for the pattern to follow.
    return df
