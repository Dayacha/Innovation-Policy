"""
Czech Republic-specific post-extraction cleaner.

Documents: Zákon o státním rozpočtu České republiky + annexes (Přílohy), 1993–2025.

SOURCE GUIDE:
  - 1993-2000: many useful rows are in annex PDFs and often state amounts in
    'v mil. Kč' (millions of CZK)
  - 2001-2015: many annex/docx years use 'v tis. Kč' / 'v tisících Kč'
    (thousands of CZK)
  - 2016+: legal text often includes full Kč totals, but annex-style pages are
    still where detailed institution rows tend to live

KEY R&D ACTORS:
  - Akademie věd České republiky / AV ČR
  - Grantová agentura České republiky / GA ČR
  - Technologická agentura České republiky / TA ČR
  - named university/research-institute lines with výzkum / vývoj / věda / inovace

KNOWN RISKS:
  - broad legal totals: CELKOVÝ PŘEHLED, PŘÍJMY/VÝDAJE CELKEM, state debt, etc.
  - broad ministry chapter totals without a named research body
  - municipal/regional transfers
  - defence, interior, transport, and social-spending lines without research signal
  - one visible source anomaly: a 1993-labelled file appears to actually be a 2024 law
"""

from __future__ import annotations

import re

import pandas as pd

__all__ = ["clean"]

_RESEARCH_RE = re.compile(
    r"výzkum|vyzkum|vývoj|vyvoj|věd|ved|inovac|technolog|"
    r"grantová agentura|grantova agentura|ga čr|ga cr|"
    r"akademie věd|akademie ved|av čr|av cr|"
    r"technologická agentura|technologicka agentura|ta čr|ta cr|"
    r"univerzit|vysok[ée] škol|výzkumn[ýy]|vyzkumn[ýy]",
    re.IGNORECASE,
)

_DEFENCE_INTERIOR_RE = re.compile(
    r"\b(ministerstvo obrany|obrana|armáda|armada|vojensk|"
    r"ministerstvo vnitra|vnitra|policie|hasič|hasic|bezpečnostn|bezpecnostn)\b",
    re.IGNORECASE,
)

_SOCIAL_RE = re.compile(
    r"\b(ministerstvo práce a sociálních věcí|ministerstvo prace a socialnich veci|"
    r"sociáln|socialn|důchod|duchod|dávk|davk|pojistn|zaměstnanost|zamestnanost)\b",
    re.IGNORECASE,
)

_TRANSPORT_INFRA_RE = re.compile(
    r"\b(ministerstvo dopravy|dopravy a spojů|dopravy a spoju|"
    r"dálnic|dalnic|silnic|železnic|zeleznic|dopravn|infrastruktur|"
    r"most|letišt|letist|vodní cesty|vodni cesty)\b",
    re.IGNORECASE,
)

_MACRO_TOTAL_RE = re.compile(
    r"\b(celkov[ýy]\s+přehled|celkov[ýy]\s+prehled|"
    r"příjmy celkem|prijmy celkem|výdaje celkem|vydaje celkem|"
    r"úhrnná bilance|uhrnna bilance|státní dluh|statni dluh|"
    r"operace státních finančních aktiv|operace statnich financnich aktiv|"
    r"všeobecná pokladní správa|vseobecna pokladni sprava|"
    r"c e l k e m|kapitola celkem|kapitoly celkem)\b",
    re.IGNORECASE,
)

_MUNICIPAL_RE = re.compile(
    r"\b(kraj[ůu]?|obc[íi]|okresn[íi]|územn|uzemn|rozpočtům obcí|rozpoctum obci|"
    r"rozpočtům krajů|rozpoctum kraju|hlavního města prahy|hlavniho mesta prahy)\b",
    re.IGNORECASE,
)

_ANNEX_RE = re.compile(r"pr[iř]loh|annex", re.IGNORECASE)
_UNIT_MILLION_HINT_RE = re.compile(r"v\s+mil\.\s*kč|v\s+mil\.\s*kc", re.IGNORECASE)
_UNIT_THOUSAND_HINT_RE = re.compile(
    r"v\s+tis\.\s*kč|v\s+tis\.\s*kc|v\s+tisících\s*kč|v\s+tisicich\s*kc",
    re.IGNORECASE,
)


def _note(df: pd.DataFrame, mask: pd.Series, text: str) -> None:
    df.loc[mask, "cleaning_notes"] = df.loc[mask, "cleaning_notes"].fillna("") + text


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Czech Republic-specific post-extraction corrections."""
    df = df.copy()

    if "cleaning_notes" not in df.columns:
        df["cleaning_notes"] = ""
    if "aggregation_role" not in df.columns:
        df["aggregation_role"] = ""

    desc_col = "line_description_en" if "line_description_en" in df.columns else "line_description"
    raw_desc_col = "line_description" if "line_description" in df.columns else desc_col
    descs = df[desc_col].fillna("").astype(str)
    raw_descs = df[raw_desc_col].fillna("").astype(str)
    sections = df.get("section_name", pd.Series("", index=df.index)).fillna("").astype(str)
    sections_en = df.get("section_name_en", pd.Series("", index=df.index)).fillna("").astype(str)
    source_files = df.get("source_file", pd.Series("", index=df.index)).fillna("").astype(str)
    combined = descs + " " + raw_descs + " " + sections + " " + sections_en

    year_num = pd.to_numeric(df.get("year", pd.Series(dtype=float)), errors="coerce")
    has_research = combined.str.contains(_RESEARCH_RE, regex=True)

    macro_total_mask = combined.str.contains(_MACRO_TOTAL_RE, regex=True)
    if "item_type" in df.columns:
        macro_total_mask = macro_total_mask | (df["item_type"] == "section_total")
    if macro_total_mask.any():
        df.loc[macro_total_mask, "aggregation_role"] = "section"
        df.loc[macro_total_mask, "decision"] = df.loc[macro_total_mask, "decision"].replace(
            "include", "review"
        )
        _note(df, macro_total_mask, "[macro_total: Czech legal/chapter aggregate, verify] ")

    municipal_mask = combined.str.contains(_MUNICIPAL_RE, regex=True) & ~has_research
    if municipal_mask.any():
        df.loc[municipal_mask, "aggregation_role"] = "non_rd"
        df.loc[municipal_mask, "decision"] = "review"
        _note(df, municipal_mask, "[municipal_transfer: regional/municipal transfer, not direct R&D appropriation] ")

    defence_mask = combined.str.contains(_DEFENCE_INTERIOR_RE, regex=True) & ~has_research
    if defence_mask.any():
        df.loc[defence_mask, "decision"] = "review"
        _note(df, defence_mask, "[defence_or_interior: no research signal] ")

    social_mask = combined.str.contains(_SOCIAL_RE, regex=True) & ~has_research
    if social_mask.any():
        df.loc[social_mask, "aggregation_role"] = "non_rd"
        df.loc[social_mask, "decision"] = "review"
        _note(df, social_mask, "[social_transfer: welfare/social spending, not R&D] ")

    infra_mask = combined.str.contains(_TRANSPORT_INFRA_RE, regex=True) & ~has_research
    if infra_mask.any():
        df.loc[infra_mask, "decision"] = "review"
        _note(df, infra_mask, "[transport_infrastructure: no research signal] ")

    misfiled_mask = (year_num == 1993) & source_files.str.contains(r"2024|434-2024", case=False, regex=True)
    if misfiled_mask.any():
        df.loc[misfiled_mask, "decision"] = "review"
        _note(df, misfiled_mask, "[source_anomaly: 1993-labelled source file appears to be a 2024 law, verify year/source] ")

    if "unit" in df.columns:
        annex_mask = source_files.str.contains(_ANNEX_RE, regex=True)

        early_annex_unit = annex_mask & (year_num <= 2000) & (df["unit"] == "unit")
        if early_annex_unit.any():
            _note(df, early_annex_unit, "[unit_check: early annex years often use million CZK rather than full CZK, verify] ")

        mid_annex_unit = annex_mask & year_num.between(2001, 2015, inclusive="both") & (df["unit"] == "unit")
        if mid_annex_unit.any():
            _note(df, mid_annex_unit, "[unit_check: many 2001-2015 annex/docx years use thousand CZK, verify] ")

        million_hint_mismatch = combined.str.contains(_UNIT_MILLION_HINT_RE, regex=True) & (df["unit"] != "million")
        if million_hint_mismatch.any():
            df.loc[million_hint_mismatch, "decision"] = "review"
            _note(df, million_hint_mismatch, "[unit_hint_mismatch: text says 'v mil. Kč' but parsed unit is not million] ")

        thousand_hint_mismatch = combined.str.contains(_UNIT_THOUSAND_HINT_RE, regex=True) & (df["unit"] != "thousand")
        if thousand_hint_mismatch.any():
            df.loc[thousand_hint_mismatch, "decision"] = "review"
            _note(df, thousand_hint_mismatch, "[unit_hint_mismatch: text says 'v tis. Kč' but parsed unit is not thousand] ")

    return df.reset_index(drop=True)
