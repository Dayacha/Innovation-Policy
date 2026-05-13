"""
New Zealand-specific post-extraction cleaner.
Populate after first run and manual audit.
"""
from __future__ import annotations
import re
import pandas as pd

__all__ = ["clean"]

_RESEARCH_SIGNAL = re.compile(
    r"science|research|innovation|marsden|callaghan|endeavour|catalyst|partnered research|health research|"
    r"crown research|dsir|frst|niwa|gns|agresearch|plant and food",
    re.IGNORECASE,
)
_NON_RD_DEVELOPMENT = re.compile(
    r"war|housing|regional development|social development|maori development|telecommunications development levy|"
    r"development of national memorials|major events development|business development",
    re.IGNORECASE,
)
_SECTION_TOTAL = re.compile(r"vote science|science, innovation and technology|research, science", re.IGNORECASE)


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "cleaning_notes" not in df.columns:
        df["cleaning_notes"] = ""
    if "aggregation_role" not in df.columns:
        df["aggregation_role"] = ""

    desc_col = "line_description_en" if "line_description_en" in df.columns else "line_description"
    raw_col = "line_description" if "line_description" in df.columns else desc_col
    desc = df[desc_col].fillna("").astype(str) + " " + df[raw_col].fillna("").astype(str)
    has_research = desc.str.contains(_RESEARCH_SIGNAL, regex=True)

    non_rd_dev = desc.str.contains(_NON_RD_DEVELOPMENT, regex=True) & ~has_research
    if non_rd_dev.any():
        df.loc[non_rd_dev, "decision"] = "review"
        df.loc[non_rd_dev, "cleaning_notes"] = df.loc[non_rd_dev, "cleaning_notes"].fillna("") + "[nz_non_rd_development]"

    section = desc.str.contains(_SECTION_TOTAL, regex=True)
    if "item_type" in df.columns:
        section = section | df["item_type"].eq("section_total")
    if section.any():
        df.loc[section, "aggregation_role"] = df.loc[section, "aggregation_role"].replace("", "section")
        df.loc[section, "cleaning_notes"] = df.loc[section, "cleaning_notes"].fillna("") + "[nz_section_total]"

    return df.reset_index(drop=True)
