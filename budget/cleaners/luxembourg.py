"""
Luxembourg-specific post-extraction cleaner.

Documents: Loi concernant le budget des recettes et des dépenses de l'Etat,
published in Mémorial A (1975-2009) and standalone budget files (2010+).

SOURCE GUIDE:
  - ALL YEARS: period is thousands separator (1.234.567 = 1,234,567).
  - 1975-2001: LUF (Luxembourg franc). 1 EUR = 40.3399 LUF.
  - 2002+:     EUR, full euros (unit='unit'). Small country — research lines
               are typically €100K–€50M.
  - MISSING:   Year 1986 has no file.

KEY R&D STRUCTURE:
  - Section 03: Ministère de l'Enseignement Supérieur et de la Recherche
    (name varies by year; pre-2009 may be a département within another ministry)
  - FNR: Fonds National de la Recherche (from 1999) — primary competitive funder
  - Université du Luxembourg (from 2003)
  - Public research institutes: LIST/CRP Henri Tudor, LISER/CEPS-INSTEAD,
    LIH/CRP Santé, CRP Gabriel Lippmann

KNOWN RISKS:
  - CNAP pensions — large social insurance; non-R&D
  - Debt service (service de la dette) — always non-R&D
  - Defence (Ministère de la Défense) — minimal in Luxembourg; usually non-R&D
  - Broad education totals without named research line
"""

from __future__ import annotations

import re

import pandas as pd

__all__ = ["clean"]

_RESEARCH_RE = re.compile(
    r"recherche\b|fnr\b|fonds.national.de.la.recherche|"
    r"universit[eé].du.luxembourg|uni\.lu\b|unilu\b|"
    r"list\b|luxembourg.institute.of.science|"
    r"liser\b|ceps.instead\b|"
    r"lih\b|luxembourg.institute.of.health|"
    r"crp\b|centre.de.recherche.public|"
    r"scri\b|coordination.de.la.recherche|"
    r"innovation\b|technologie\b|"
    r"programme.de.recherche|subvention.de.recherche|"
    r"03\.0|03\.1|03\.2|enseignement.sup[eé]rieur.et.recherche",
    re.IGNORECASE,
)

_PENSION_RE = re.compile(
    r"caisse.nationale.d.assurance.pension|cnap\b|"
    r"pensions\s+civiles|r[eé]gime.g[eé]n[eé]ral.de.pension|"
    r"caisse.de.retraite|fonds.de.pension|"
    r"charges.de.retraite|contributions.pension",
    re.IGNORECASE,
)

_DEBT_RE = re.compile(
    r"service.de.la.dette|int[eé]r[eê]ts.de.la.dette|"
    r"dette.publique|amortissement.de.la.dette|"
    r"charges.de.la.dette|remboursement.de.la.dette|"
    r"co[uû]t.de.la.dette",
    re.IGNORECASE,
)

_DEFENCE_RE = re.compile(
    r"minist[eè]re.de.la.d[eé]fense|"
    r"arm[eé]e.luxembourgeoise|"
    r"d[eé]penses.militaires",
    re.IGNORECASE,
)

_SOCIAL_RE = re.compile(
    r"s[eé]curit[eé].sociale|"
    r"inspection.g[eé]n[eé]rale.de.la.s[eé]curit[eé].sociale\b|igss\b|"
    r"allocations.familiales|"
    r"ch[ôo]mage.indemnit[eé]|"
    r"aides.sociales\b|assistance.sociale\b",
    re.IGNORECASE,
)

_TRANSPORT_RE = re.compile(
    r"administration.des.ponts.et.chauss[eé]es|"
    r"autoroutes\b|routes.nationales|"
    r"infrastructure.routi[eè]re|"
    r"chemin.de.fer\b|cfl\b",
    re.IGNORECASE,
)

_EDUCATION_BROAD_RE = re.compile(
    r"minist[eè]re.de.l.?[eé]ducation.nationale|"
    r"enseignement.fondamental\b|"
    r"enseignement.secondaire\b|"
    r"enseignement.pr[eé]scolaire\b|"
    r"subsides.scolaires",
    re.IGNORECASE,
)

_MACRO_TOTAL_RE = re.compile(
    r"total.du.budget|"
    r"r[eé]capitulatif.g[eé]n[eé]ral|"
    r"total.g[eé]n[eé]ral.des.d[eé]penses|"
    r"tableau.synoptique",
    re.IGNORECASE,
)


def _note(df: pd.DataFrame, mask: pd.Series, text: str) -> None:
    df.loc[mask, "cleaning_notes"] = df.loc[mask, "cleaning_notes"].fillna("") + text


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Luxembourg-specific post-extraction corrections."""
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

    # Pension/social insurance — non-R&D
    pension_mask = combined.str.contains(_PENSION_RE, regex=True) & ~has_research
    if pension_mask.any():
        df.loc[pension_mask, "aggregation_role"] = "non_rd"
        df.loc[pension_mask, "decision"] = "review"
        _note(df, pension_mask, "[luxembourg_pension: social insurance — non-R&D] ")

    # Social transfers without research signal
    social_mask = combined.str.contains(_SOCIAL_RE, regex=True) & ~has_research
    if social_mask.any():
        df.loc[social_mask, "aggregation_role"] = "non_rd"
        df.loc[social_mask, "decision"] = "review"
        _note(df, social_mask, "[luxembourg_social_transfer: non-R&D] ")

    # Debt service — always non-R&D
    debt_mask = combined.str.contains(_DEBT_RE, regex=True)
    if debt_mask.any():
        df.loc[debt_mask, "aggregation_role"] = "non_rd"
        df.loc[debt_mask, "decision"] = "review"
        _note(df, debt_mask, "[luxembourg_debt_service: non-R&D] ")

    # Defence without research signal
    defence_mask = combined.str.contains(_DEFENCE_RE, regex=True) & ~has_research
    if defence_mask.any():
        df.loc[defence_mask, "decision"] = "review"
        _note(df, defence_mask, "[luxembourg_defence_non_rd] ")

    # Transport infrastructure
    transport_mask = combined.str.contains(_TRANSPORT_RE, regex=True) & ~has_research
    if transport_mask.any():
        df.loc[transport_mask, "aggregation_role"] = "non_rd"
        df.loc[transport_mask, "decision"] = "review"
        _note(df, transport_mask, "[luxembourg_transport_non_rd] ")

    # Broad primary/secondary education without research signal
    educ_mask = combined.str.contains(_EDUCATION_BROAD_RE, regex=True) & ~has_research
    if educ_mask.any():
        df.loc[educ_mask, "aggregation_role"] = "non_rd"
        df.loc[educ_mask, "decision"] = "review"
        _note(df, educ_mask, "[luxembourg_broad_education: non-R&D] ")

    # Macro totals
    macro_mask = combined.str.contains(_MACRO_TOTAL_RE, regex=True)
    if "item_type" in df.columns:
        macro_mask = macro_mask | (df["item_type"] == "section_total")
    if macro_mask.any():
        df.loc[macro_mask, "aggregation_role"] = df.loc[macro_mask, "aggregation_role"].replace("", "section")
        _note(df, macro_mask, "[luxembourg_macro_total: section aggregate] ")

    # Pre-EUR era note
    pre_eur = year_num < 2002
    if pre_eur.any():
        _note(df, pre_eur,
              "[luxembourg_luf_era: amounts in LUF (Luxembourg franc); "
              "1 EUR = 40.3399 LUF (fixed rate from 1999)] ")

    # Implausible single R&D line > 1B EUR
    amount_col = "amount_local" if "amount_local" in df.columns else None
    if amount_col:
        unit_s = df.get("unit", pd.Series("", index=df.index)).fillna("").str.lower()
        implausible = (
            (unit_s == "unit")
            & (pd.to_numeric(df[amount_col], errors="coerce") > 1_000_000_000)
            & (year_num >= 2002)
            & (df["decision"] == "include")
        )
        if implausible.any():
            df.loc[implausible, "decision"] = "review"
            _note(df, implausible,
                  "[luxembourg_implausible_amount: >1B EUR single line — "
                  "likely budget total or wrong unit] ")

    return df.reset_index(drop=True)
