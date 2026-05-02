from __future__ import annotations

import re
import pandas as pd

__all__ = ["clean"]

_RESEARCH_SIGNAL = re.compile(
    r"teadus|uurimis|research|science|innovation|tehnoloogia|haridus- ja teadus",
    re.IGNORECASE,
)
_SOCIAL = re.compile(r"sotsiaal|pension|toetus|h[üu]vitis", re.IGNORECASE)
_INFRA = re.compile(r"transport|tee|raudtee|infrastruktuur|maantee", re.IGNORECASE)
_SECTION_TOTAL = re.compile(r"\b(kokku|total|summa)\b", re.IGNORECASE)
_GENERIC_PROGRAM = re.compile(
    r"allocated|allocation|basic financing|funding for|grant|infrastructure costs|"
    r"ordered research|research support|target financing|targeted financing",
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
    year_num = pd.to_numeric(df.get("year", pd.Series(dtype=float)), errors="coerce")
    has_research = desc.str.contains(_RESEARCH_SIGNAL, regex=True)

    social = desc.str.contains(_SOCIAL, regex=True) & ~has_research
    if social.any():
        df.loc[social, "aggregation_role"] = "non_rd"
        df.loc[social, "decision"] = "review"
        _note(df, social, "[estonia_social_non_rd]")

    infra = desc.str.contains(_INFRA, regex=True) & ~has_research
    if infra.any():
        df.loc[infra, "decision"] = "review"
        _note(df, infra, "[estonia_infrastructure_non_rd]")

    if "item_type" in df.columns:
        section_mask = (df["item_type"] == "section_total") | desc.str.contains(_SECTION_TOTAL, regex=True)
    else:
        section_mask = desc.str.contains(_SECTION_TOTAL, regex=True)
    if section_mask.any():
        df.loc[section_mask, "aggregation_role"] = df.loc[section_mask, "aggregation_role"].replace("", "section")
        _note(df, section_mask, "[estonia_section_total]")

    if "currency" in df.columns:
        eur_pre2011 = (year_num < 2011) & (df["currency"] == "EUR")
        if eur_pre2011.any():
            df.loc[eur_pre2011, "currency"] = "EEK"
            _note(df, eur_pre2011, "[estonia_currency_check_pre_euro_expected_eek_or_header_check]")

    if "amount_local" in df.columns and "unit" in df.columns:
        amount_num = pd.to_numeric(df["amount_local"], errors="coerce")
        full_value_mask = df["unit"].fillna("").str.lower().eq("thousand") & amount_num.ge(1_000_000)
        if full_value_mask.any():
            df.loc[full_value_mask, "unit"] = "unit"
            _note(df, full_value_mask, "[estonia_large_full_value_not_thousand]")

    generic_program = desc.str.contains(_GENERIC_PROGRAM, regex=True) & ~desc.str.contains(
        r"academy|agency|council|foundation|institute|university|archimedes",
        case=False,
        regex=True,
    )
    if generic_program.any():
        df.loc[generic_program, "decision"] = "review"
        _note(df, generic_program, "[estonia_generic_programme_row]")

    return df.reset_index(drop=True)
