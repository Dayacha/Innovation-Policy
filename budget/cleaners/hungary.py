from __future__ import annotations

import re
import pandas as pd

__all__ = ["clean"]

_RESEARCH_SIGNAL = re.compile(
    r"kutat|fejleszt|innov|tudom[aá]ny|mta|nemzeti kutat[aá]si|kutat[oó]k[oö]zpont|kutat[oó]int[eé]zet|agr[aá]rkutat",
    re.IGNORECASE,
)
_DEFENCE = re.compile(r"honv[eé]d|rend[őo]r|katonai|biztons[aá]g", re.IGNORECASE)
_INFRA = re.compile(r"[úu]th[aá]l[oó]zat|aut[oó]p[aá]lya|vas[uú]t|k[oö]zleked|infrastruktur", re.IGNORECASE)
_SOCIAL = re.compile(r"nyugd[ií]j|szoci[aá]lis|j[oó]l[eé]ti|csal[aá]d|lakhat[aá]s|eg[eé]szs[eé]gbiztos", re.IGNORECASE)
_SECTION_TOTAL = re.compile(r"fejezet [öo]sszesen|c[ií]m [öo]sszesen|alc[ií]m [öo]sszesen|mind[öo]sszesen|[öo]sszesen", re.IGNORECASE)


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

    defence = desc.str.contains(_DEFENCE, regex=True) & ~has_research
    if defence.any():
        df.loc[defence, "decision"] = "review"
        _note(df, defence, "[hungary_defence_non_rd]")

    infra = desc.str.contains(_INFRA, regex=True) & ~has_research
    if infra.any():
        df.loc[infra, "decision"] = "review"
        _note(df, infra, "[hungary_infrastructure_non_rd]")

    social = desc.str.contains(_SOCIAL, regex=True) & ~has_research
    if social.any():
        df.loc[social, "aggregation_role"] = "non_rd"
        df.loc[social, "decision"] = "review"
        _note(df, social, "[hungary_social_non_rd]")

    section = desc.str.contains(_SECTION_TOTAL, regex=True)
    if "item_type" in df.columns:
        section = section | df["item_type"].eq("section_total")
    if section.any():
        df.loc[section, "aggregation_role"] = df.loc[section, "aggregation_role"].replace("", "section")
        _note(df, section, "[hungary_section_total]")

    return df.reset_index(drop=True)
