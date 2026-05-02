from __future__ import annotations

import re
import pandas as pd

__all__ = ["clean"]

_RESEARCH_SIGNAL = re.compile(
    r"investig|ciencia|tecnolog|innovaci|colciencias|minciencias|agrosavia|corpoica|ideam|"
    r"metrolog|i\+d|mou?p|fondo.*caldas|caldas.*fondo",
    re.IGNORECASE,
)
_DEFENCE = re.compile(
    r"defensa|fuerzas.?militares|ejército|armada|fuerza.?aérea|policía.?nacional|inteligencia",
    re.IGNORECASE,
)
_SOCIAL = re.compile(
    r"bienestar.?familiar|icbf|seguridad.?social|pensiones|subsidio|vivienda.*social|"
    r"protección.?social|atención.?víctimas",
    re.IGNORECASE,
)
_INFRA = re.compile(
    r"carreteras|vialidad|obras.?públicas|concesiones.?viales|puertos|aeropuertos|"
    r"acueducto|alcantarillado|vivienda.?urbana",
    re.IGNORECASE,
)
_SECTION_TOTAL = re.compile(
    r"\btotal.?sección|\btotal.?sector|\btotal.?presupuesto|\bresumen\b|\bintersubsectorial\b",
    re.IGNORECASE,
)
# SENA's vocational (non-R&D) lines to downgrade
_SENA_NON_RD = re.compile(
    r"formación.?profesional|aprendizaje.?titulada|centros.?de.?formación|bienestar.?al.?aprendiz|"
    r"gestión.?de.?empleo",
    re.IGNORECASE,
)
# Legal wrapper pages (narrative law text, no budget tables)
_LEGAL_NARRATIVE = re.compile(
    r"artículo\s+\d+|parágrafo|por.?cuanto|en.?ejercicio|haciéndole.?saber|diario.?oficial",
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
    has_research = desc.str.contains(_RESEARCH_SIGNAL, regex=True)

    # Defence without explicit research signal → review
    defence = desc.str.contains(_DEFENCE, regex=True) & ~has_research
    if defence.any():
        df.loc[defence, "decision"] = "review"
        _note(df, defence, "[colombia_defence_non_rd]")

    # Social protection → non_rd
    social = desc.str.contains(_SOCIAL, regex=True) & ~has_research
    if social.any():
        df.loc[social, "aggregation_role"] = "non_rd"
        df.loc[social, "decision"] = "review"
        _note(df, social, "[colombia_social_non_rd]")

    # Infrastructure without research → review
    infra = desc.str.contains(_INFRA, regex=True) & ~has_research
    if infra.any():
        df.loc[infra, "decision"] = "review"
        _note(df, infra, "[colombia_infra_non_rd]")

    # Section totals and cross-ministry aggregates
    section = desc.str.contains(_SECTION_TOTAL, regex=True)
    if "item_type" in df.columns:
        section = section | (df["item_type"] == "section_total")
    if section.any():
        df.loc[section, "aggregation_role"] = df.loc[section, "aggregation_role"].replace("", "section")
        _note(df, section, "[colombia_section_total]")

    # SENA non-R&D vocational lines → review
    sena_non_rd = desc.str.contains(_SENA_NON_RD, regex=True) & ~has_research
    if sena_non_rd.any():
        df.loc[sena_non_rd, "decision"] = "review"
        _note(df, sena_non_rd, "[colombia_sena_vocational_not_rd]")

    # Legal narrative pages extracted without amounts → exclude
    legal = desc.str.contains(_LEGAL_NARRATIVE, regex=True)
    amount_col = "amount_local" if "amount_local" in df.columns else None
    if amount_col:
        no_amount = df[amount_col].isna() | (df[amount_col] == 0)
        legal_no_amount = legal & no_amount
        if legal_no_amount.any():
            df.loc[legal_no_amount, "decision"] = "exclude"
            _note(df, legal_no_amount, "[colombia_legal_narrative_no_amount]")

    # Sanity check: COP amounts above ~50 trillion on a single line are implausible for an R&D line
    if amount_col and "unit" in df.columns:
        unit_is_unit = df["unit"].fillna("").str.lower() == "unit"
        implausible = unit_is_unit & (pd.to_numeric(df[amount_col], errors="coerce") > 50_000_000_000_000)
        if implausible.any():
            df.loc[implausible, "decision"] = "review"
            _note(df, implausible, "[colombia_implausible_amount]")

    return df.reset_index(drop=True)
