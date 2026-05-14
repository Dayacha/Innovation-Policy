from __future__ import annotations

import re
import pandas as pd

__all__ = ["clean"]

_RESEARCH_SIGNAL = re.compile(
    r"moksl|tyrim|inov|technolog|mokslo taryba|mokslo, studijų|mokslo ir studijų|fundamental|research|science",
    re.IGNORECASE,
)
_STUDENT = re.compile(r"moksleivio krepšel|moksleivio krepsel|studij[ųu] kredit|student|tuition", re.IGNORECASE)
_SPORTS = re.compile(r"\bsport\b", re.IGNORECASE)
_SECTION_TOTAL = re.compile(r"ministerija|ministerijos|mokslas ir studijos", re.IGNORECASE)
_UNIVERSITY = re.compile(
    r"universitet|akademij|kolegij|pedagog|muzikos ir teatro|sveikatos moksl|sporto universitet",
    re.IGNORECASE,
)
_EXPLICIT_RESEARCH_ENTITY = re.compile(
    r"mokslini[ųu]\s+tyrim|research institute|institut|mokslo taryba|fundamental",
    re.IGNORECASE,
)
_SPECIAL_INVESTIGATION = re.compile(
    r"speciali[ųu]j[ųu]\s+tyrim[ųu]\s+tarnyb|special investigation service",
    re.IGNORECASE,
)


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
    entity = df[raw_col].fillna("").astype(str)
    has_research = desc.str.contains(_RESEARCH_SIGNAL, regex=True)

    student = desc.str.contains(_STUDENT, regex=True) & ~has_research
    if student.any():
        df.loc[student, "decision"] = "review"
        _note(df, student, "[lithuania_student_non_rd]")

    sports = desc.str.contains(_SPORTS, regex=True) & ~has_research
    if sports.any():
        df.loc[sports, "decision"] = "review"
        _note(df, sports, "[lithuania_sports_non_rd]")

    university_operating = entity.str.contains(_UNIVERSITY, regex=True) & ~entity.str.contains(
        _EXPLICIT_RESEARCH_ENTITY, regex=True
    )
    if university_operating.any():
        df.loc[university_operating, "decision"] = "exclude"
        _note(df, university_operating, "[lithuania_university_operating_non_rd]")

    special_investigation = desc.str.contains(_SPECIAL_INVESTIGATION, regex=True)
    if special_investigation.any():
        df.loc[special_investigation, "decision"] = "exclude"
        _note(df, special_investigation, "[lithuania_special_investigation_false_positive]")

    section = desc.str.contains(_SECTION_TOTAL, regex=True)
    if "item_type" in df.columns:
        section = section | df["item_type"].eq("section_total")
    if section.any():
        df.loc[section, "aggregation_role"] = df.loc[section, "aggregation_role"].replace("", "section")
        _note(df, section, "[lithuania_section_total]")

    return df.reset_index(drop=True)
