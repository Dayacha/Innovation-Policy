from __future__ import annotations

import re
import pandas as pd

__all__ = ["clean"]

_RESEARCH_SIGNAL = re.compile(
    r"investigaci|ciencia|tecnolog|innovaci|conicyt|anid|inia|ifop|desarrollo cient",
    re.IGNORECASE,
)
_SOCIAL = re.compile(r"pensiones|subsidio|vivienda|familia|salud p[úu]blica(?!.*investig)", re.IGNORECASE)
_DEFENCE = re.compile(r"defensa|ej[ée]rcito|armada|fuerza a[ée]rea", re.IGNORECASE)
_INFRA = re.compile(r"obras p[úu]blicas|vialidad|carreteras|ferrocarril|metro|puertos", re.IGNORECASE)
_SECTION_TOTAL = re.compile(r"\b(total|subtotal|suma)\b", re.IGNORECASE)


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
        _note(df, social, "[chile_social_non_rd]")

    defence = desc.str.contains(_DEFENCE, regex=True) & ~has_research
    if defence.any():
        df.loc[defence, "decision"] = "review"
        _note(df, defence, "[chile_defence_non_rd]")

    infra = desc.str.contains(_INFRA, regex=True) & ~has_research
    if infra.any():
        df.loc[infra, "decision"] = "review"
        _note(df, infra, "[chile_infrastructure_non_rd]")

    if "item_type" in df.columns:
        section_mask = (df["item_type"] == "section_total") | desc.str.contains(_SECTION_TOTAL, regex=True)
    else:
        section_mask = desc.str.contains(_SECTION_TOTAL, regex=True)
    if section_mask.any():
        df.loc[section_mask, "aggregation_role"] = df.loc[section_mask, "aggregation_role"].replace("", "section")
        _note(df, section_mask, "[chile_section_total]")

    return df.reset_index(drop=True)
