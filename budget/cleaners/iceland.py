from __future__ import annotations

import re
import pandas as pd

__all__ = ["clean"]

_RESEARCH_SIGNAL = re.compile(
    r"ranns[oó]kn|v[íi]sind|research|science|innovation|rann[ií]s|h[aá]sk[oó]li",
    re.IGNORECASE,
)
_SOCIAL = re.compile(r"l[íi]feyr|trygg|b[óo]tur|almanna", re.IGNORECASE)
_INFRA = re.compile(r"vegager|samg[oö]ng|hafnir|framkv[aæ]md|fiskvei(?!.*ranns)", re.IGNORECASE)
_SECTION_TOTAL = re.compile(r"\b(samtals|alls|total)\b", re.IGNORECASE)


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

    social = desc.str.contains(_SOCIAL, regex=True) & ~has_research
    if social.any():
        df.loc[social, "aggregation_role"] = "non_rd"
        df.loc[social, "decision"] = "review"
        _note(df, social, "[iceland_social_non_rd]")

    infra = desc.str.contains(_INFRA, regex=True) & ~has_research
    if infra.any():
        df.loc[infra, "decision"] = "review"
        _note(df, infra, "[iceland_infrastructure_or_fisheries_non_rd]")

    if "item_type" in df.columns:
        section_mask = (df["item_type"] == "section_total") | desc.str.contains(_SECTION_TOTAL, regex=True)
    else:
        section_mask = desc.str.contains(_SECTION_TOTAL, regex=True)
    if section_mask.any():
        df.loc[section_mask, "aggregation_role"] = df.loc[section_mask, "aggregation_role"].replace("", "section")
        _note(df, section_mask, "[iceland_section_total]")

    return df.reset_index(drop=True)
