from __future__ import annotations

import re
import pandas as pd

__all__ = ["clean"]

_RESEARCH_SIGNAL = re.compile(
    r"zinātn|zinatn|pētn|petn|inov|tehnoloģ|tehnolog|latvijas zinātnes padome|latvijas zinātņu akadēm|"
    r"fundament[aā]lie zin[aā]tniskie p[eē]t[iī]jumi|zinātnes bāzes finansējums|investīcijas zinātnei",
    re.IGNORECASE,
)
_DEFENCE_POLICE = re.compile(r"aizsardzības akadēm|aizsardzibas akadem|policijas akadēm|policijas akadem", re.IGNORECASE)
_HOSPITAL = re.compile(r"universitātes slimnī|universitates slimni|klīniskā universitātes slimnī|kliniska univers", re.IGNORECASE)
_SPORTS_CREDIT = re.compile(r"\bsports\b|studiju kred|studējošo kred|studentu kred", re.IGNORECASE)
_SECTION_TOTAL = re.compile(r"ministrija\s*[—-]\s*(kopā|kopa)|\bkopā\b|\bkopa\b", re.IGNORECASE)


def _note(df: pd.DataFrame, mask: pd.Series, text: str) -> None:
    df.loc[mask, "cleaning_notes"] = df.loc[mask, "cleaning_notes"].fillna("") + text


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

    defence = desc.str.contains(_DEFENCE_POLICE, regex=True) & ~has_research
    if defence.any():
        df.loc[defence, "decision"] = "review"
        _note(df, defence, "[latvia_defence_police_non_rd]")

    hospital = desc.str.contains(_HOSPITAL, regex=True) & ~has_research
    if hospital.any():
        df.loc[hospital, "decision"] = "review"
        _note(df, hospital, "[latvia_hospital_non_rd]")

    sports_credit = desc.str.contains(_SPORTS_CREDIT, regex=True) & ~has_research
    if sports_credit.any():
        df.loc[sports_credit, "decision"] = "review"
        _note(df, sports_credit, "[latvia_sports_credit_non_rd]")

    section = desc.str.contains(_SECTION_TOTAL, regex=True)
    if "item_type" in df.columns:
        section = section | df["item_type"].eq("section_total")
    if section.any():
        df.loc[section, "aggregation_role"] = df.loc[section, "aggregation_role"].replace("", "section")
        _note(df, section, "[latvia_section_total]")

    return df.reset_index(drop=True)
