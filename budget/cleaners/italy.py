"""
Italy-specific post-extraction cleaner.

Documents: Bilancio di previsione dello Stato — Gazzetta Ufficiale Supplemento ordinario, 1986–2025.

SOURCE GUIDE:
  - 1986–1996: scanned image PDFs — expect zero or garbled OCR text.
    One-character-per-line artefacts indicate rotated-column tables (common in Gazzetta Ufficiale).
  - 1997–2001: partially machine-readable; ITL (lire), amounts in milioni or miliardi di lire.
  - 2002–2016: EUR, amounts in MIGLIAIA DI EURO (thousands). Tables labelled '(MIGLIAIA DI EURO)'.
  - 2017+:     EUR, full euros (euro interi). BILANCIO PER AZIONI format.

TWO-FILE STRUCTURE: each year has a main-law file (legal text) and a companion file
(ALLEGATI / stato di previsione tables). The companion is the primary source for amounts.

KEY R&D STRUCTURE (2010+ budget reform):
  - Missione 17: Ricerca e innovazione (cross-ministry R&D mission)
  - Missione 23: Istruzione universitaria e formazione post-universitaria
  - Ministry: MUR (2020+) / MIUR (1999-2020) / MURST (1989-1999)

KEY FUNDS: FOE (enti di ricerca block grant), FIRST/FAR/FIRB, PRIN, CNR, ENEA, ASI, INFN, INAF

KNOWN RISKS:
  - FFO (Fondo di Finanziamento Ordinario) is a bulk university teaching transfer — NOT R&D specific
  - Broad ministry totals and section aggregates (totale generale, quadro riassuntivo)
  - Debt service: interessi passivi, rimborso titoli, ammortamento
  - Social/pension transfers: pensioni, TFR, previdenza
  - Transport/infrastructure without research signal
  - Defence procurement without research label
  - Regional transfers: fondo perequativo, trasferimenti a regioni
"""

from __future__ import annotations

import re

import pandas as pd

__all__ = ["clean"]

_RESEARCH_RE = re.compile(
    r"ricerca|innovazi|scienza|scientifico|tecnologi|sviluppo.{0,10}tecnol|"
    r"r&s\b|r\s*&\s*d\b|murst|miur\b|cnr\b|enea\b|infn\b|inaf\b|ingv\b|ogs\b|"
    r"asi\b|agenzia.spaziale|prima|prin\b|firb\b|first\b|foe\b|far\b|"
    r"fondo.ordinario.per.gli.enti|fondo.per.gli.investimenti.nella.ricerca|"
    r"fondo.agevolazioni.alla.ricerca|missione.17|ricerca.e.innovazione|"
    r"università|universita|dottorato|ricercatori|enti.di.ricerca",
    re.IGNORECASE,
)

_FFO_BULK_RE = re.compile(
    r"fondo.di.finanziamento.ordinario|ffo\b|finanziamento.ordinario.delle.universit",
    re.IGNORECASE,
)

_DEBT_RE = re.compile(
    r"interessi.passivi|rimborso.titoli|ammortamento.debito|servizio.del.debito|"
    r"debito.pubblico|oneri.del.debito|interessi.sul.debito",
    re.IGNORECASE,
)

_SOCIAL_RE = re.compile(
    r"pensioni|tfr\b|trattamento.di.fine.rapporto|assegni.familiari|"
    r"previdenza.sociale|assistenza.sociale|indennit.di.disoccupazione|"
    r"cassa.integrazione",
    re.IGNORECASE,
)

_INFRA_RE = re.compile(
    r"autostrade|ferrovie|porti\b|aeroporti|strade\b|viabilità|"
    r"infrastrutture.viarie|infrastrutture.stradali|costruzione.opere",
    re.IGNORECASE,
)

_DEFENCE_PROCURE_RE = re.compile(
    r"programma.navale|programma.aereo|materiale.militare|procurement.difesa|"
    r"sistemi.d.arma|approvvigionamento.militare",
    re.IGNORECASE,
)

_MACRO_TOTAL_RE = re.compile(
    r"totale.generale|quadro.generale.riassuntivo|totale.entrate|totale.spese|"
    r"saldo.netto|avanzo.di.bilancio|disavanzo.di.bilancio|fondi.da.ripartire|"
    r"riepilogo.generale|totale.ministero\b",
    re.IGNORECASE,
)

_REGIONAL_TRANSFER_RE = re.compile(
    r"fondo.perequativo|trasferimenti.a.regioni|trasferimenti.agli.enti.locali|"
    r"fondo.di.solidariet.comunale|fondo.sperimentale.di.riequilibrio",
    re.IGNORECASE,
)

_GARBLED_OCR_RE = re.compile(
    r"^[A-Z]\s*$|^[A-Z]\n[A-Z]\n",
    re.MULTILINE,
)

_UNIT_THOUSAND_HINT_RE = re.compile(
    r"migliaia.di.euro|mila.euro|\(migliaia\)",
    re.IGNORECASE,
)

_UNIT_MILLION_HINT_RE = re.compile(
    r"milioni.di.euro|milioni.di.lire|miliardi.di.lire|miliardi.di.euro",
    re.IGNORECASE,
)


def _note(df: pd.DataFrame, mask: pd.Series, text: str) -> None:
    df.loc[mask, "cleaning_notes"] = df.loc[mask, "cleaning_notes"].fillna("") + text


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Italy-specific post-extraction corrections."""
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

    # FFO bulk transfer — teaching fund, not R&D
    ffo_mask = combined.str.contains(_FFO_BULK_RE, regex=True) & ~has_research
    if ffo_mask.any():
        df.loc[ffo_mask, "aggregation_role"] = df.loc[ffo_mask, "aggregation_role"].replace("", "section")
        df.loc[ffo_mask, "decision"] = "review"
        _note(df, ffo_mask, "[italy_ffo_bulk_transfer: teaching fund, not R&D — verify if research component] ")

    # Debt service — always non-R&D
    debt_mask = combined.str.contains(_DEBT_RE, regex=True)
    if debt_mask.any():
        df.loc[debt_mask, "aggregation_role"] = "non_rd"
        df.loc[debt_mask, "decision"] = "review"
        _note(df, debt_mask, "[italy_debt_service: non-R&D] ")

    # Social/pension — non-R&D
    social_mask = combined.str.contains(_SOCIAL_RE, regex=True) & ~has_research
    if social_mask.any():
        df.loc[social_mask, "aggregation_role"] = "non_rd"
        df.loc[social_mask, "decision"] = "review"
        _note(df, social_mask, "[italy_social_pension: non-R&D] ")

    # Infrastructure without research signal
    infra_mask = combined.str.contains(_INFRA_RE, regex=True) & ~has_research
    if infra_mask.any():
        df.loc[infra_mask, "decision"] = "review"
        _note(df, infra_mask, "[italy_infrastructure_non_rd] ")

    # Defence procurement without research signal
    def_mask = combined.str.contains(_DEFENCE_PROCURE_RE, regex=True) & ~has_research
    if def_mask.any():
        df.loc[def_mask, "decision"] = "review"
        _note(df, def_mask, "[italy_defence_procurement_non_rd] ")

    # Macro totals / section aggregates
    macro_mask = combined.str.contains(_MACRO_TOTAL_RE, regex=True)
    if "item_type" in df.columns:
        macro_mask = macro_mask | (df["item_type"] == "section_total")
    if macro_mask.any():
        df.loc[macro_mask, "aggregation_role"] = df.loc[macro_mask, "aggregation_role"].replace("", "section")
        _note(df, macro_mask, "[italy_macro_total: section aggregate] ")

    # Regional transfers without research signal
    regional_mask = combined.str.contains(_REGIONAL_TRANSFER_RE, regex=True) & ~has_research
    if regional_mask.any():
        df.loc[regional_mask, "aggregation_role"] = "non_rd"
        df.loc[regional_mask, "decision"] = "review"
        _note(df, regional_mask, "[italy_regional_transfer: non-R&D] ")

    # Unit mismatch flags: document says thousands/millions but extracted unit differs
    if "unit" in df.columns:
        thousand_hint = combined.str.contains(_UNIT_THOUSAND_HINT_RE, regex=True)
        mismatch_thousand = thousand_hint & (df["unit"] != "thousand")
        if mismatch_thousand.any():
            df.loc[mismatch_thousand, "decision"] = "review"
            _note(df, mismatch_thousand, "[italy_unit_hint: text says migliaia di euro but unit not 'thousand'] ")

        million_hint = combined.str.contains(_UNIT_MILLION_HINT_RE, regex=True)
        mismatch_million = million_hint & (df["unit"] != "million")
        if mismatch_million.any():
            df.loc[mismatch_million, "decision"] = "review"
            _note(df, mismatch_million, "[italy_unit_hint: text says milioni/miliardi but unit not 'million'] ")

    # Early years (pre-1997): warn that text may be garbled OCR from scanned images
    early_mask = year_num <= 1996
    if early_mask.any():
        _note(df, early_mask, "[italy_pre1997_scanned: expect garbled/empty OCR from rotated-column Gazzetta Ufficiale] ")

    # Sanity: EUR amounts above ~5 billion on a single R&D line are implausible
    amount_col = "amount_local" if "amount_local" in df.columns else None
    if amount_col and "unit" in df.columns:
        unit_is_unit = df["unit"].fillna("").str.lower() == "unit"
        implausible = unit_is_unit & (pd.to_numeric(df[amount_col], errors="coerce") > 5_000_000_000)
        if implausible.any():
            df.loc[implausible, "decision"] = "review"
            _note(df, implausible, "[italy_implausible_amount: >5B EUR on single line, likely section total] ")

    return df.reset_index(drop=True)
