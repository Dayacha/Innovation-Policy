from __future__ import annotations

import re
import pandas as pd

__all__ = ["clean"]

_RESEARCH_SIGNAL = re.compile(
    r"r&d|연구개발|국가연구개발|과학기술|인공지능|ai|반도체|우주|바이오|양자|혁신",
    re.IGNORECASE,
)
_MACRO = re.compile(
    r"총지출|총수입|재정수지|국가채무|경제성장|물가|세입|세출|재정운용|건전재정|예산안 모습",
    re.IGNORECASE,
)
_SOCIAL = re.compile(r"주거|복지|고용|돌봄|보육|연금|청년주택", re.IGNORECASE)
_LOANS = re.compile(r"대출|융자|보증|펀드|금융지원", re.IGNORECASE)
_SECTION_TOTAL = re.compile(r"\btotal\b|총계|합계", re.IGNORECASE)


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

    macro = desc.str.contains(_MACRO, regex=True) & ~has_research
    if macro.any():
        df.loc[macro, "decision"] = "review"
        df.loc[macro, "aggregation_role"] = df.loc[macro, "aggregation_role"].replace("", "macro")
        _note(df, macro, "[korea_macro_budget_summary]")

    social = desc.str.contains(_SOCIAL, regex=True) & ~has_research
    if social.any():
        df.loc[social, "decision"] = "review"
        _note(df, social, "[korea_social_non_rd]")

    loans = desc.str.contains(_LOANS, regex=True) & ~has_research
    if loans.any():
        df.loc[loans, "decision"] = "review"
        _note(df, loans, "[korea_financial_instrument_non_rd]")

    section = desc.str.contains(_SECTION_TOTAL, regex=True)
    if "item_type" in df.columns:
        section = section | df["item_type"].eq("section_total")
    if section.any():
        df.loc[section, "aggregation_role"] = df.loc[section, "aggregation_role"].replace("", "section")
        _note(df, section, "[korea_section_total]")

    return df.reset_index(drop=True)
