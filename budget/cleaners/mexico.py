"""
Mexico-specific post-extraction cleaner.

Documents: Presupuesto de Egresos de la Federación (PEF), Diario Oficial de la
Federación (DOF), 1975–2025.

SOURCE GUIDE:
  - ALL YEARS: Mexican peso (MXN). Unit varies by table — read header.
  - 1975-1992: OLD peso (peso antiguo). 1 new MXN = 1,000 old pesos (redenomination 1993).
  - 1993+:     NEW peso (MXN). Millones de pesos is common for summary tables.
  - 2022+:     CONAHCyT replaces CONACYT; Ramo 38 code unchanged.

KEY R&D STRUCTURE:
  - Ramo 38: CONACYT (1971-2022) / CONAHCyT (2022+) — PRIMARY R&D ramo
  - Ramo 11: SEP — includes UNAM, IPN, CINVESTAV, other CPIs
  - Ramo 18: SENER — includes ININ (nuclear research)
  - Ramo 08: SAGARPA/SADER — includes INIFAP (agricultural R&D)
  - CPIs: Centros Públicos de Investigación supervised by CONACYT/CONAHCyT

KNOWN RISKS:
  - IMSS/ISSSTE (Ramos 19/50) — social security pensions, huge, never R&D
  - Ramo 06/24 debt service — always non-R&D
  - SEDENA/SEMAR (Ramos 07/13) defence without research label
  - SCT/SICT roads/transport without research label
  - Ramos 28/33/39 federal transfers to states — not R&D
  - "investigación" in criminal/police context (PGR, Guardia Nacional) — NOT science R&D
  - Duplicates: 1999 MEX / 2000 MEX files are exact duplicates of the same-year MAT
"""

from __future__ import annotations

import re

import pandas as pd

__all__ = ["clean"]

_RESEARCH_RE = re.compile(
    r"investigaci[oó]n\s+(?:cient[ií]fica|y\s+desarrollo|b[aá]sica|aplicada)|"
    r"ciencia\s+y\s+tecnolog[ií]a|"
    r"conacyt\b|conahcyt\b|"
    r"ramo\s+38\b|"
    r"cinvestav\b|cicese\b|ciesas\b|cidesi\b|ciqa\b|ciad\b|ciatej\b|cicy\b|"
    r"cenapred\b|infotec\b|cenidet\b|cimav\b|"
    r"inin\b|inifap\b|"
    r"centros\s+p[uú]blicos\s+de\s+investigaci[oó]n|"
    r"fondo\s+sectorial|fondo\s+mixto|fondo\s+institucional|foins\b|"
    r"pronaces\b|"
    r"agencia\s+espacial\s+mexicana|aem\b|"
    r"posgrado\s+e\s+investigaci[oó]n|"
    r"tecnolog[ií]a\s+e\s+innovaci[oó]n",
    re.IGNORECASE,
)

_IMSS_ISSSTE_RE = re.compile(
    r"\bimss\b|instituto\s+mexicano\s+del\s+seguro\s+social|"
    r"\bissste\b|instituto\s+de\s+seguridad\s+y\s+servicios\s+sociales|"
    r"ramo\s+19\b|ramo\s+50\b|"
    r"pensiones\s+civiles|"
    r"seguridad\s+social\s+(para|de)\s+(los\s+)?trabajadores|"
    r"seguro\s+de\s+retiro|cuotas\s+y\s+aportaciones",
    re.IGNORECASE,
)

_DEBT_RE = re.compile(
    r"servicio\s+de\s+la\s+deuda|"
    r"deuda\s+p[uú]blica|"
    r"intereses\s+(de|del|sobre)\s+la\s+deuda|"
    r"amortizaci[oó]n\s+de\s+la\s+deuda|"
    r"costo\s+financiero|"
    r"ramo\s+06\b|ramo\s+24\b|"
    r"provisi[oó]n\s+para\s+contingencias",
    re.IGNORECASE,
)

_DEFENCE_RE = re.compile(
    r"secretar[ií]a\s+de\s+la\s+defensa\s+nacional|sedena\b|"
    r"secretar[ií]a\s+de\s+marina\b|semar\b|"
    r"ramo\s+07\b|ramo\s+13\b|"
    r"fuerzas\s+armadas|"
    r"ejercito\s+mexicano|marina\s+armada",
    re.IGNORECASE,
)

_TRANSPORT_RE = re.compile(
    r"secretar[ií]a\s+de\s+comunicaciones\s+y\s+transportes|"
    r"\bsct\b|\bsict\b|"
    r"ramo\s+09\b|"
    r"infraestructura\s+(carretera|vial|ferroviaria)|"
    r"construcci[oó]n\s+de\s+(carreteras|autopistas|caminos)|"
    r"mantenimiento\s+de\s+carreteras",
    re.IGNORECASE,
)

_SOCIAL_RE = re.compile(
    r"secretar[ií]a\s+de\s+bienestar|bienestar\b|sedesol\b|"
    r"ramo\s+20\b|"
    r"programas\s+sociales|"
    r"subsidios\s+a\s+la\s+pobreza|"
    r"programa\s+oportunidades|programa\s+prospera|"
    r"sembrando\s+vida|benef[ií]cate\b",
    re.IGNORECASE,
)

_TRANSFERS_RE = re.compile(
    r"aportaciones\s+federales\s+(a|para)\s+(estados|municipios)|"
    r"ramo\s+28\b|ramo\s+33\b|ramo\s+39\b|"
    r"participaciones\s+a\s+entidades\s+federativas|"
    r"fondo\s+general\s+de\s+participaciones",
    re.IGNORECASE,
)

_CRIMINAL_INVEST_RE = re.compile(
    r"procuradur[ií]a\s+general\s+de\s+la\s+rep[uú]blica|pgr\b|"
    r"guardia\s+nacional|"
    r"\bpolicía\s+federal\b|policia\s+federal\b|"
    r"investigaci[oó]n\s+(de|y\s+persecuci[oó]n)\s+de\s+delitos|"
    r"investigaci[oó]n\s+criminal|investigaci[oó]n\s+ministerial",
    re.IGNORECASE,
)

_MACRO_TOTAL_RE = re.compile(
    r"total\s+del\s+sector\s+p[uú]blico|"
    r"gasto\s+neto\s+total|"
    r"presupuesto\s+total|"
    r"total\s+de\s+erogaciones\b|"
    r"resumen\s+por\s+ramos\b|"
    r"clasificaci[oó]n\s+por\s+ramos",
    re.IGNORECASE,
)

_DUPLICATE_FILE_RE = re.compile(
    r"\d{4}\s+mex\s+\d{8}-mat",
    re.IGNORECASE,
)

_OLD_PESO_HINT_RE = re.compile(
    r"peso\s+antiguo|moneda\s+nacional\s+\(vieja\)|"
    r"viejos\s+pesos|pesos\s+viejos",
    re.IGNORECASE,
)


def _note(df: pd.DataFrame, mask: pd.Series, text: str) -> None:
    df.loc[mask, "cleaning_notes"] = df.loc[mask, "cleaning_notes"].fillna("") + text


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Mexico-specific post-extraction corrections."""
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

    # Duplicate MEX files (1999 MEX, 2000 MEX) — exclude entirely
    dup_mask = source_files.str.contains(_DUPLICATE_FILE_RE, regex=True, na=False)
    if dup_mask.any():
        df.loc[dup_mask, "decision"] = "exclude"
        _note(df, dup_mask, "[mexico_duplicate_file: exact copy of same-year MAT file — excluded] ")

    # IMSS/ISSSTE social security — always non-R&D
    imss_mask = combined.str.contains(_IMSS_ISSSTE_RE, regex=True) & ~has_research
    if imss_mask.any():
        df.loc[imss_mask, "aggregation_role"] = "non_rd"
        df.loc[imss_mask, "decision"] = "review"
        _note(df, imss_mask, "[mexico_imss_issste: social security — non-R&D] ")

    # Public debt service — always non-R&D
    debt_mask = combined.str.contains(_DEBT_RE, regex=True)
    if debt_mask.any():
        df.loc[debt_mask, "aggregation_role"] = "non_rd"
        df.loc[debt_mask, "decision"] = "review"
        _note(df, debt_mask, "[mexico_debt_service: non-R&D] ")

    # Defence without research signal
    defence_mask = combined.str.contains(_DEFENCE_RE, regex=True) & ~has_research
    if defence_mask.any():
        df.loc[defence_mask, "decision"] = "review"
        _note(df, defence_mask, "[mexico_defence_non_rd: no research signal] ")

    # Transport infrastructure without research signal
    transport_mask = combined.str.contains(_TRANSPORT_RE, regex=True) & ~has_research
    if transport_mask.any():
        df.loc[transport_mask, "aggregation_role"] = "non_rd"
        df.loc[transport_mask, "decision"] = "review"
        _note(df, transport_mask, "[mexico_transport_infrastructure: non-R&D] ")

    # Social welfare programmes without research signal
    social_mask = combined.str.contains(_SOCIAL_RE, regex=True) & ~has_research
    if social_mask.any():
        df.loc[social_mask, "aggregation_role"] = "non_rd"
        df.loc[social_mask, "decision"] = "review"
        _note(df, social_mask, "[mexico_social_welfare: non-R&D] ")

    # Federal transfers to states/municipalities
    transfers_mask = combined.str.contains(_TRANSFERS_RE, regex=True) & ~has_research
    if transfers_mask.any():
        df.loc[transfers_mask, "aggregation_role"] = "non_rd"
        df.loc[transfers_mask, "decision"] = "review"
        _note(df, transfers_mask, "[mexico_federal_transfers: participaciones/aportaciones — non-R&D] ")

    # Criminal investigation context — "investigación" means crime investigation, not R&D
    criminal_mask = combined.str.contains(_CRIMINAL_INVEST_RE, regex=True) & ~has_research
    if criminal_mask.any():
        df.loc[criminal_mask, "aggregation_role"] = "non_rd"
        df.loc[criminal_mask, "decision"] = "review"
        _note(df, criminal_mask, "[mexico_criminal_investigation: investigación = criminal, not scientific] ")

    # Macro totals
    macro_mask = combined.str.contains(_MACRO_TOTAL_RE, regex=True)
    if "item_type" in df.columns:
        macro_mask = macro_mask | (df["item_type"] == "section_total")
    if macro_mask.any():
        df.loc[macro_mask, "aggregation_role"] = df.loc[macro_mask, "aggregation_role"].replace(
            "", "section"
        )
        _note(df, macro_mask, "[mexico_macro_total: section aggregate] ")

    # Pre-1993 old peso flag
    pre_1993 = year_num < 1993
    if pre_1993.any():
        _note(df, pre_1993,
              "[mexico_pre1993_old_peso: amounts in old peso (peso antiguo); "
              "1 new MXN = 1,000 old pesos (redenomination January 1993)] ")

    # Unit check: warn if million/billion on pre-2000 files where unit is often full pesos
    amount_col = "amount_local" if "amount_local" in df.columns else None
    if amount_col:
        unit_s = df.get("unit", pd.Series("", index=df.index)).fillna("").str.lower()
        # A single Ramo 38 line > 100 billion pesos is implausible
        implausible = (
            (pd.to_numeric(df[amount_col], errors="coerce") > 100_000_000_000)
            & (unit_s == "unit")
            & (df["decision"] == "include")
        )
        if implausible.any():
            df.loc[implausible, "decision"] = "review"
            _note(df, implausible,
                  "[mexico_implausible_amount: >100B pesos on single R&D line — likely budget total or wrong unit] ")

    return df.reset_index(drop=True)
