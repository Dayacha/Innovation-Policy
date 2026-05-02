from __future__ import annotations

import re
import pandas as pd

__all__ = ["clean"]

_RESEARCH_SIGNAL = re.compile(
    r"investig|ciencia|tecnolog|innovaci|conicit|micitt|pcii|inta|inciensa|catie|"
    r"lanamme|itcr|tec\b|vínculo.?externo|vinculo.?externo|transferencia.?tecnol",
    re.IGNORECASE,
)
_INFRA = re.compile(
    r"mopt|carreteras|vialidad|obras.?públicas|puertos|aeropuertos|tránsito|cosevi|"
    r"ferrocarril|acueducto|alcantarillado",
    re.IGNORECASE,
)
_SOCIAL = re.compile(
    r"pensiones|régimen.?pensiones|imas\b|bienestar.?social|asignaciones.?familiares|"
    r"fondo.?nacional.?de.?becas(?!.*investig)",
    re.IGNORECASE,
)
_DEBT = re.compile(
    r"amortización|servicio.?de.?la.?deuda|intereses.?deuda|títulos.?valores|bonos.?del.?tesoro",
    re.IGNORECASE,
)
_SECTION_TOTAL = re.compile(
    r"\btotal\b|\bsubtotal\b|\bsuma.?total\b",
    re.IGNORECASE,
)
# FEES bulk transfer line — mark as higher_education aggregate, not direct R&D
_FEES_BULK = re.compile(
    r"fondo.?especial.?de.?educación.?superior|fondo.?especial.?de.?educacion.?superior|\bfees\b",
    re.IGNORECASE,
)
# Multi-tomo note trigger
_TOMO = re.compile(r"\btomo\s*\d+\b", re.IGNORECASE)


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
    source_col = "source_file" if "source_file" in df.columns else None

    desc = df[desc_col].fillna("").astype(str) + " " + df[raw_col].fillna("").astype(str)
    has_research = desc.str.contains(_RESEARCH_SIGNAL, regex=True)

    # Infrastructure without research → review
    infra = desc.str.contains(_INFRA, regex=True) & ~has_research
    if infra.any():
        df.loc[infra, "decision"] = "review"
        _note(df, infra, "[costa_rica_infrastructure_non_rd]")

    # Social/pension without research → non_rd
    social = desc.str.contains(_SOCIAL, regex=True) & ~has_research
    if social.any():
        df.loc[social, "aggregation_role"] = "non_rd"
        df.loc[social, "decision"] = "review"
        _note(df, social, "[costa_rica_social_non_rd]")

    # Debt service → non_rd
    debt = desc.str.contains(_DEBT, regex=True)
    if debt.any():
        df.loc[debt, "aggregation_role"] = "non_rd"
        df.loc[debt, "decision"] = "review"
        _note(df, debt, "[costa_rica_debt_service]")

    # Section totals
    section = desc.str.contains(_SECTION_TOTAL, regex=True)
    if "item_type" in df.columns:
        section = section | (df["item_type"] == "section_total")
    if section.any():
        df.loc[section, "aggregation_role"] = df.loc[section, "aggregation_role"].replace("", "section")
        _note(df, section, "[costa_rica_section_total]")

    # FEES bulk transfer → higher_education aggregate
    fees = desc.str.contains(_FEES_BULK, regex=True)
    if fees.any():
        df.loc[fees, "aggregation_role"] = df.loc[fees, "aggregation_role"].replace("", "section")
        _note(df, fees, "[costa_rica_fees_bulk_transfer]")

    # Multi-tomo rows: flag source file so compile can aggregate correctly
    if source_col:
        tomo_rows = df[source_col].fillna("").astype(str).str.contains(_TOMO, regex=True)
        if tomo_rows.any():
            _note(df, tomo_rows, "[costa_rica_multi_tomo_check_aggregation]")

    # Sanity: CRC amounts above ~5 trillion on a single R&D line are implausible
    amount_col = "amount_local" if "amount_local" in df.columns else None
    if amount_col and "unit" in df.columns:
        unit_is_unit = df["unit"].fillna("").str.lower() == "unit"
        implausible = unit_is_unit & (pd.to_numeric(df[amount_col], errors="coerce") > 5_000_000_000_000)
        if implausible.any():
            df.loc[implausible, "decision"] = "review"
            _note(df, implausible, "[costa_rica_implausible_amount]")

    return df.reset_index(drop=True)
