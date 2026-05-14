"""
Poland-specific post-extraction cleaner.

Documents: Ustawa Budżetowa (Budget Act), Dziennik Ustaw RP, 1990–2025.

SOURCE GUIDE:
  - 1990-1994: OLD złoty (stary złoty). 1 new PLN = 10,000 old PLN (redenomination 1995).
    Early annex pages often say "w milionach złotych"; preserve that as unit='million'.
    If the header says "w tys. zł", preserve unit='thousand'.
  - 1995+:     NEW PLN (nowy złoty). Same unit='thousand', currency='PLN'.

KEY R&D STRUCTURE:
  - Część 28: Szkolnictwo wyższe i nauka (Higher Education and Science) — PRIMARY R&D PART
    - Dział 730 Szkolnictwo wyższe: university subwencje, HE grants
    - Dział 740 Działalność badawcza i rozwojowa: NCN, NCBiR, direct R&D
  - Część 67: Polska Akademia Nauk (PAN) — own budget part
  - KBN (Komitet Badań Naukowych, 1991-2005): pre-NCN/NCBiR R&D body
  - NCBiR (from 2007): Narodowe Centrum Badań i Rozwoju
  - NCN (from 2011): Narodowe Centrum Nauki

KNOWN RISKS:
  - ZUS/KRUS (social insurance) — very large, always non-R&D
  - Public debt service — always non-R&D
  - Primary/secondary education (oświata i wychowanie) — not R&D
  - Defence without research label
  - Transport infrastructure (GDDKiA, PKP) — not R&D
"""

from __future__ import annotations

import re

import pandas as pd

__all__ = ["clean"]

_RESEARCH_RE = re.compile(
    r"badani|nauk[ao]\b|badawczo|b\+r\b|"
    r"ncn\b|ncbir\b|ncbr\b|kbn\b|pan\b|"
    r"centrum.nauki|centrum.badań|centrum.badan|"
    r"polska.akademia.nauk|"
    r"szkolnictwo.wyższe|szkolnictwo.wyzsze|"
    r"innowacj|technologi|"
    r"cern\b|esa\b|horyzont|horizon|"
    r"politechnika|uniwersytet.(?!ekonomiczny)|"
    r"działalność.statutowa|dzialalnosc.statutowa|"
    r"740\b|dział.740|dzial.740",
    re.IGNORECASE,
)

_ZUS_RE = re.compile(
    r"\bzus\b|zakład.ubezpieczeń.społecznych|zaklad.ubezpieczen.spolecznych|"
    r"\bkrus\b|kasa.rolniczego.ubezpieczenia|"
    r"ubezpieczenia.społeczne|ubezpieczenia.spoleczne|"
    r"emerytury\b|renty\b|składki.ubezpieczeniowe|"
    r"część.73\b|czesc.73\b|część.74\b|czesc.74\b",
    re.IGNORECASE,
)

_DEBT_RE = re.compile(
    r"obsługa.długu|obsluga.dlugu|"
    r"dług.publiczny|dlug.publiczny|"
    r"obsługa.zobowiązań.skarbu|obsluga.zobowiazan.skarbu|"
    r"spłata.kredytów|splata.kredytow|"
    r"odsetki.od.długu|odsetki.od.dlugu|"
    r"finansowanie.potrzeb.pożyczkowych|finansowanie.potrzeb.pozyczkowych",
    re.IGNORECASE,
)

_DEFENCE_RE = re.compile(
    r"obrona.narodowa|ministerstwo.obrony|"
    r"wojsko\b|wojskow|żołnierz|zolnierz|"
    r"siły.zbrojne|sily.zbrojne|"
    r"uzbrojenie\b|sprzęt.wojskowy|sprzet.wojskowy|"
    r"część.29\b|czesc.29\b",
    re.IGNORECASE,
)

_SCHOOL_RE = re.compile(
    r"oświata.i.wychowanie|oswiata.i.wychowanie|"
    r"szkolnictwo.podstawowe|szkolnictwo.ponadpodstawowe|"
    r"szkolnictwo.ponadgimnazjalne|"
    r"przedszkola\b|przedszkole\b|"
    r"szkoła.podstawowa|szkola.podstawowa|"
    r"szkoła.średnia|szkola.srednia|"
    r"gimnazjum\b|licea\b|liceum\b",
    re.IGNORECASE,
)

_INFRA_RE = re.compile(
    r"gddkia\b|generalna.dyrekcja.dróg|generalna.dyrekcja.drog|"
    r"pkp\b|polskie.koleje|infrastruktura.kolejowa|"
    r"budowa.dróg|budowa.drog|"
    r"autostrady\b|drogi.krajowe|"
    r"lotnisko\b|port.lotniczy|"
    r"utrzymanie.dróg|utrzymanie.drog",
    re.IGNORECASE,
)

_SOCIAL_RE = re.compile(
    r"pomoc.społeczna|pomoc.spoleczna|"
    r"zasiłki\b|zasilki\b|"
    r"świadczenia.socjalne|swiadczenia.socjalne|"
    r"opieka.społeczna|opieka.spoleczna|"
    r"zasiłek.dla.bezrobotnych|zasilek.dla.bezrobotnych|"
    r"część.44\b|czesc.44\b",
    re.IGNORECASE,
)

_MACRO_TOTAL_RE = re.compile(
    r"ogółem\b(?!.*badania)|ogolne.wydatki|"
    r"wydatki.budżetu.państwa|wydatki.budzetu.panstwa|"
    r"łączne.wydatki|laczne.wydatki|"
    r"razem.wydatki|suma.wydatków|suma.wydatkow|"
    r"ogólna.rezerwa|rezerwa.ogólna|rezerwa.ogolna|"
    r"zestawienie.zbiorcze|zestawienie.ogólne|zestawienie.ogolne",
    re.IGNORECASE,
)

_OLD_ZLOTY_HINT_RE = re.compile(
    r"stary.złoty|stara.waluta|stary.zł\b|"
    r"tys\.\s*zł\.?\s*\(?stary|old\s+zloty",
    re.IGNORECASE,
)


def _note(df: pd.DataFrame, mask: pd.Series, text: str) -> None:
    df.loc[mask, "cleaning_notes"] = df.loc[mask, "cleaning_notes"].fillna("") + text


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Poland-specific post-extraction corrections."""
    df = df.copy()

    if "cleaning_notes" not in df.columns:
        df["cleaning_notes"] = ""
    if "aggregation_role" not in df.columns:
        df["aggregation_role"] = ""

    desc_col = "line_description_en" if "line_description_en" in df.columns else "line_description"
    raw_col = "line_description" if "line_description" in df.columns else desc_col
    descs = df[desc_col].fillna("").astype(str)
    raw_descs = df[raw_col].fillna("").astype(str)
    sections = df.get("section_name", pd.Series("", index=df.index)).fillna("").astype(str)
    sections_en = df.get("section_name_en", pd.Series("", index=df.index)).fillna("").astype(str)
    combined = descs + " " + raw_descs + " " + sections + " " + sections_en

    year_num = pd.to_numeric(df.get("year", pd.Series(dtype=float)), errors="coerce")
    has_research = combined.str.contains(_RESEARCH_RE, regex=True)

    # ZUS/KRUS social insurance — always non-R&D
    zus_mask = combined.str.contains(_ZUS_RE, regex=True)
    if zus_mask.any():
        df.loc[zus_mask, "aggregation_role"] = "non_rd"
        df.loc[zus_mask, "decision"] = "review"
        _note(df, zus_mask, "[poland_zus_krus: social insurance — non-R&D] ")

    # Social transfers without research signal — non-R&D
    social_mask = combined.str.contains(_SOCIAL_RE, regex=True) & ~has_research
    if social_mask.any():
        df.loc[social_mask, "aggregation_role"] = "non_rd"
        df.loc[social_mask, "decision"] = "review"
        _note(df, social_mask, "[poland_social_transfer: non-R&D] ")

    # Public debt service — always non-R&D
    debt_mask = combined.str.contains(_DEBT_RE, regex=True)
    if debt_mask.any():
        df.loc[debt_mask, "aggregation_role"] = "non_rd"
        df.loc[debt_mask, "decision"] = "review"
        _note(df, debt_mask, "[poland_debt_service: non-R&D] ")

    # Defence without research signal
    defence_mask = combined.str.contains(_DEFENCE_RE, regex=True) & ~has_research
    if defence_mask.any():
        df.loc[defence_mask, "decision"] = "review"
        _note(df, defence_mask, "[poland_defence_non_rd: no research signal] ")

    # Primary/secondary education without research signal — non-R&D
    school_mask = combined.str.contains(_SCHOOL_RE, regex=True) & ~has_research
    if school_mask.any():
        df.loc[school_mask, "aggregation_role"] = "non_rd"
        df.loc[school_mask, "decision"] = "review"
        _note(df, school_mask, "[poland_primary_secondary_education: non-R&D] ")

    # Transport infrastructure without research signal
    infra_mask = combined.str.contains(_INFRA_RE, regex=True) & ~has_research
    if infra_mask.any():
        df.loc[infra_mask, "decision"] = "review"
        _note(df, infra_mask, "[poland_infrastructure_non_rd] ")

    # Macro totals and section aggregates
    macro_mask = combined.str.contains(_MACRO_TOTAL_RE, regex=True)
    if "item_type" in df.columns:
        macro_mask = macro_mask | (df["item_type"] == "section_total")
    if macro_mask.any():
        df.loc[macro_mask, "aggregation_role"] = df.loc[macro_mask, "aggregation_role"].replace(
            "", "section"
        )
        _note(df, macro_mask, "[poland_macro_total: section aggregate] ")

    # Pre-1995 old złoty flag
    pre_1995 = year_num < 1995
    if pre_1995.any():
        _note(df, pre_1995,
              "[poland_pre1995_old_zloty: amounts in old złoty (stary złoty); "
              "1 new PLN = 10,000 old PLN (redenomination 1995)] ")

    # Unit check: post-1995 Poland should be thousands; early old-zloty annexes
    # often use millions, so do not flag those as unit errors.
    if "unit" in df.columns:
        unit_s = df["unit"].fillna("").str.lower()
        wrong_post_1995 = (
            year_num.ge(1995)
            & unit_s.isin(["unit", "million", "milion", "billion"])
            & ~df["aggregation_role"].isin(["non_rd", "redundant", "section"])
        )
        wrong_pre_1995 = (
            year_num.lt(1995)
            & unit_s.isin(["unit", "billion"])
            & ~df["aggregation_role"].isin(["non_rd", "redundant", "section"])
        )
        wrong_unit_mask = wrong_post_1995 | wrong_pre_1995
        if wrong_unit_mask.any():
            _note(df, wrong_unit_mask,
                  "[poland_unit_check: verify page header; post-1995 rows should usually be tys. zł, "
                  "while some 1990-1994 annexes are explicitly in millions of old złoty] ")

    # Outlier: single R&D line > 100B tys. zł is implausible
    amount_col = "amount_local" if "amount_local" in df.columns else None
    if amount_col:
        unit_thou = df.get("unit", pd.Series("", index=df.index)).fillna("").str.lower() == "thousand"
        implausible = (
            unit_thou
            & (pd.to_numeric(df[amount_col], errors="coerce") > 100_000_000)
            & (df["decision"] == "include")
        )
        if implausible.any():
            df.loc[implausible, "decision"] = "review"
            _note(df, implausible,
                  "[poland_implausible_amount: >100B tys. zł on single R&D line, likely budget total] ")

    return df.reset_index(drop=True)
