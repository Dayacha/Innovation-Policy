"""
Slovakia-specific post-extraction cleaner.

Documents: zákon o štátnom rozpočte, Zbierka zákonov SR, 1992–2025.

SOURCE GUIDE:
  - 1992–2008: SKK (Slovak koruna), amounts in THOUSANDS of SKK ('tis. Sk').
               unit='thousand', currency='SKK'.
  - 2009+:     EUR, full euros (NOT thousands). unit='unit', currency='EUR'.
               Slovakia joined eurozone 1 January 2009 at 30.1260 SKK/EUR.

KEY R&D STRUCTURE:
  - Kapitola 20: Ministerstvo školstva SR / MŠ SR
    (primary chapter for science and higher education)
  - Kapitola 51: Slovenská akadémia vied (SAV) — basic research
  - Oblasť 740: veda a výskum (science and research) — direct R&D division code
  - APVV: Agentúra na podporu výskumu a vývoja (from 2005)
  - VEGA: Vedecká grantová agentúra (MŠ SR and SAV joint grant scheme)

KNOWN RISKS:
  - 1990 file is actually a Polish budget document — must be excluded
  - Debt service (dlhová služba) — always non-R&D
  - Social transfers (sociálne dávky, Sociálna poisťovňa) — always non-R&D
  - Defence (Ministerstvo obrany SR, kapitola 21) without research label
  - Primary/secondary education without research signal
"""

from __future__ import annotations

import re

import pandas as pd

__all__ = ["clean"]

_RESEARCH_RE = re.compile(
    r"výskum|vývoj\b|veda\b|vedeck|inováci|technologi|"
    r"apvv\b|vega\b|sav\b|akadémia.vied|akademia.vied|"
    r"740\b|veda.a.výskum|veda.a.vyskum|"
    r"výskumný.ústav|vyskumny.ustav|"
    r"cern\b|esa\b|horizont|horizon|"
    r"kapitola.51|aplikovaný.výskum|aplikovany.vyskum|"
    r"základný.výskum|zakladny.vyskum",
    re.IGNORECASE,
)

_DEBT_RE = re.compile(
    r"dlhová.služba|dlhova.sluzba|obsluha.štátneho.dlhu|obsluha.statneho.dlhu|"
    r"splátky.dlhu|splatky.dlhu|úroky.z.dlhu|uroky.z.dlhu|"
    r"záväzky.z.dlhu|zavazky.z.dlhu|štátny.dlh\b|statny.dlh\b|"
    r"emisie.štátnych.dlhopisov|emisia.dlhopisov",
    re.IGNORECASE,
)

_SOCIAL_RE = re.compile(
    r"sociálna.poisťovňa|socialna.poistovna|"
    r"sociálne.dávky|socialne.davky|sociálne.poistenie|socialne.poistenie|"
    r"nemocenské\b|nemocenske\b|dôchodky\b|dochodky\b|"
    r"starobné.dôchodky|starobne.dochodky|"
    r"invalidné.dôchodky|invalidne.dochodky|"
    r"materské\b|materske\b|rodinné.prídavky|rodinne.pridavky|"
    r"prídavky.na.deti|pridavky.na.deti|"
    r"zdravotné.poistenie\b|zdravotne.poistenie\b|"
    r"zamestnanosť\b|zamestnanost\b",
    re.IGNORECASE,
)

_DEFENCE_RE = re.compile(
    r"ministerstvo.obrany|"
    r"ozbrojené.sily|ozbrojene.sily|"
    r"vojenský\b|vojensky\b|vojsko\b|"
    r"obrana\b|obranné.výdavky|obranne.vydavky|"
    r"materiálno-technické.zabezpečenie.armády",
    re.IGNORECASE,
)

_SCHOOL_RE = re.compile(
    r"základné.školstvo|zakladne.skolstvo|"
    r"stredné.školstvo|stredne.skolstvo|"
    r"materské.školy|materskej.školy|materske.skoly|"
    r"základná.škola|zakladna.skola|"
    r"stredná.škola|stredna.skola|"
    r"špeciálne.školy(?!.*výskum)|specialne.skoly(?!.*vyskum)|"
    r"predprimárne.vzdelávanie|predprimarne.vzdelavanie",
    re.IGNORECASE,
)

_INFRA_RE = re.compile(
    r"cestná.infraštruktúra|cestna.infrastruktura|"
    r"diaľnice\b|dialnice\b|"
    r"železnice\b|zeleznice\b|"
    r"nds\b|národná.diaľničná.spoločnosť|"
    r"žsr\b|železnice.sr|"
    r"letisko\b|prístav\b|pristavy\b",
    re.IGNORECASE,
)

_MACRO_TOTAL_RE = re.compile(
    r"celkové.výdavky|celkove.vydavky|"
    r"celkové.príjmy|celkove.prijmy|"
    r"spolu.výdavky|spolu.vydavky|"
    r"výdavky.celkom|vydavky.celkom|"
    r"súhrnný.prehľad|suhrnny.prehlad|"
    r"rekapitulácia|rekapitulacia|"
    r"súhrnná.tabuľka|suhrnna.tabulka",
    re.IGNORECASE,
)

_POLISH_FILE_RE = re.compile(
    r"dziennik.ustaw.rzeczypospolitej|"
    r"ustawa.budżetowa|ustawa.budzetowa|"
    r"rzeczypospolitej.polskiej|"
    r"minister.finansów.rzeczypospolitej",
    re.IGNORECASE,
)

_SKK_HINT_RE = re.compile(
    r"tis\.\s*sk\b|tisíc\s+sk\b|tisic\s+sk\b|"
    r"v\s+tis\.\s*sk|v\s+tisícoch|v\s+tisicoch|"
    r"slovenská\s+koruna|slovenska\s+koruna|skk\b",
    re.IGNORECASE,
)

_EUR_HINT_RE = re.compile(
    r"\bv\s+eur\b|\beurách\b|\beurach\b|\beurom\b|€",
    re.IGNORECASE,
)


def _note(df: pd.DataFrame, mask: pd.Series, text: str) -> None:
    df.loc[mask, "cleaning_notes"] = df.loc[mask, "cleaning_notes"].fillna("") + text


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Slovakia-specific post-extraction corrections."""
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
    source_files = df.get("source_file", pd.Series("", index=df.index)).fillna("").astype(str)
    combined = descs + " " + raw_descs + " " + sections + " " + sections_en

    year_num = pd.to_numeric(df.get("year", pd.Series(dtype=float)), errors="coerce")
    has_research = combined.str.contains(_RESEARCH_RE, regex=True)

    # Exclude year 1990: the file is actually a Polish budget document
    year_1990_mask = year_num == 1990
    polish_mask = combined.str.contains(_POLISH_FILE_RE, regex=True)
    mislabelled = year_1990_mask | polish_mask
    if mislabelled.any():
        df.loc[mislabelled, "decision"] = "exclude"
        _note(df, mislabelled,
              "[slovakia_mislabelled_file: 1990 text.pdf is actually a Polish budget document — excluded] ")

    # Debt service — always non-R&D
    debt_mask = combined.str.contains(_DEBT_RE, regex=True)
    if debt_mask.any():
        df.loc[debt_mask, "aggregation_role"] = "non_rd"
        df.loc[debt_mask, "decision"] = "review"
        _note(df, debt_mask, "[slovakia_debt_service: non-R&D] ")

    # Social transfers without research signal — non-R&D
    social_mask = combined.str.contains(_SOCIAL_RE, regex=True) & ~has_research
    if social_mask.any():
        df.loc[social_mask, "aggregation_role"] = "non_rd"
        df.loc[social_mask, "decision"] = "review"
        _note(df, social_mask, "[slovakia_social_transfer: non-R&D] ")

    # Defence without research signal
    defence_mask = combined.str.contains(_DEFENCE_RE, regex=True) & ~has_research
    if defence_mask.any():
        df.loc[defence_mask, "decision"] = "review"
        _note(df, defence_mask, "[slovakia_defence_non_rd: no research signal] ")

    # Primary/secondary school without research signal
    school_mask = combined.str.contains(_SCHOOL_RE, regex=True) & ~has_research
    if school_mask.any():
        df.loc[school_mask, "aggregation_role"] = "non_rd"
        df.loc[school_mask, "decision"] = "review"
        _note(df, school_mask, "[slovakia_primary_secondary_education: non-R&D] ")

    # Transport infrastructure without research signal
    infra_mask = combined.str.contains(_INFRA_RE, regex=True) & ~has_research
    if infra_mask.any():
        df.loc[infra_mask, "decision"] = "review"
        _note(df, infra_mask, "[slovakia_infrastructure_non_rd] ")

    # Macro totals and section aggregates
    macro_mask = combined.str.contains(_MACRO_TOTAL_RE, regex=True)
    if "item_type" in df.columns:
        macro_mask = macro_mask | (df["item_type"] == "section_total")
    if macro_mask.any():
        df.loc[macro_mask, "aggregation_role"] = df.loc[macro_mask, "aggregation_role"].replace(
            "", "section"
        )
        _note(df, macro_mask, "[slovakia_macro_total: section aggregate] ")

    # Currency/unit mismatch flags
    if "unit" in df.columns and "currency" in df.columns:
        # Pre-2009 rows should be SKK thousands
        pre_2009 = year_num < 2009
        skk_hint = combined.str.contains(_SKK_HINT_RE, regex=True)
        wrong_currency_skk = (
            pre_2009 & skk_hint
            & (df["currency"].fillna("").str.upper().isin(["EUR", "€"]))
        )
        if wrong_currency_skk.any():
            df.loc[wrong_currency_skk, "decision"] = "review"
            _note(df, wrong_currency_skk,
                  "[slovakia_currency_mismatch: pre-2009 text says SKK but currency is EUR] ")

        # Post-2009 rows should be EUR full units. Manual Slovakia audit of the
        # extracted source rows (2009 p6-7, 2010 p4, 2018 p5-6, 2022 p6-7 and
        # p24-25) found that EUR-era appropriations are repeatedly mislabeled as
        # 'thousand' even when the page values themselves are already the full
        # euro amount shown in the law.
        post_2009 = year_num >= 2009
        eur_hint = combined.str.contains(_EUR_HINT_RE, regex=True)
        wrong_unit_eur = (
            post_2009 & eur_hint
            & (df["unit"].fillna("").str.lower().isin(["thousand", "tis", "1000"]))
        )
        if wrong_unit_eur.any():
            df.loc[wrong_unit_eur, "unit"] = "unit"
            _note(df, wrong_unit_eur,
                  "[slovakia_unit_corrected: post-2009 EUR row relabelled from thousand to full-euro unit after source audit] ")

    # Outlier: single R&D line > 1B EUR in EUR era is implausible
    amount_col = "amount_local" if "amount_local" in df.columns else None
    if amount_col:
        post_2009_eur = year_num >= 2009
        unit_full = df.get("unit", pd.Series("", index=df.index)).fillna("").str.lower() == "unit"
        implausible_eur = (
            post_2009_eur & unit_full
            & (pd.to_numeric(df[amount_col], errors="coerce") > 1_000_000_000)
        )
        if implausible_eur.any():
            df.loc[implausible_eur, "decision"] = "review"
            _note(df, implausible_eur,
                  "[slovakia_implausible_eur_amount: >1B EUR on single R&D line, likely chapter total] ")

        # Pre-2009 SKK: amounts > 100B tis. Sk implausible for single R&D line
        pre_2009_skk = year_num < 2009
        unit_thou = df.get("unit", pd.Series("", index=df.index)).fillna("").str.lower() == "thousand"
        implausible_skk = (
            pre_2009_skk & unit_thou
            & (pd.to_numeric(df[amount_col], errors="coerce") > 100_000_000)
        )
        if implausible_skk.any():
            df.loc[implausible_skk, "decision"] = "review"
            _note(df, implausible_skk,
                  "[slovakia_implausible_skk_amount: >100B tis. Sk on single R&D line, likely chapter total] ")

    return df.reset_index(drop=True)
