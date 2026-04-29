"""
Spain-specific post-extraction cleaner.

Documents: Presupuestos Generales del Estado (PGE), BOE, 1979–2023.

ERA GUIDE:
  Pre-2002  (ESP era): amounts in MILLONES de pesetas (unit='million', currency='ESP').
  2002+     (EUR era): amounts in MILES de euros (unit='thousand', currency='EUR').

KEY R&D AGENCIES:
  CSIC     — Consejo Superior de Investigaciones Científicas (always R&D)
  AEI      — Agencia Estatal de Investigación (from 2017, organism 28.303)
  CDTI     — Centro para el Desarrollo Tecnológico e Industrial
  ISCIII   — Instituto de Salud Carlos III (organism 28.106)
  CIEMAT   — Centro de Investigaciones Energéticas, Medioambientales y Tecnológicas (28.103)
  FECYT    — Fundación Española para la Ciencia y la Tecnología
  INIA     — Instituto Nacional de Investigación y Tecnología Agraria y Alimentaria

KNOWN FALSE-POSITIVE PATTERNS:
  - Defence lines (Defensa, Ejército, Fuerzas Armadas, CIFAS) without 'investigación'
  - Social Security transfers (Seguridad Social, pensiones, prestaciones)
  - Generic student grants (becas de estudio — note: FPI/FPU fellowships ARE R&D)
  - Regional development overhead (FEDER, fondos estructurales, Plan de Empleo) without R&D label
  - Section/ministry totals (Total Sección, Total Ministerio, Resumen del Estado)
"""

from __future__ import annotations

import re

import pandas as pd

__all__ = ["clean"]

# ---------------------------------------------------------------------------
# Research signal — presence means the line is plausibly R&D
# ---------------------------------------------------------------------------
_RESEARCH_RE = re.compile(
    r"investigaci|ciencia|I\+D|tecnolog|CSIC|CDTI|AEI|ISCIII|CIEMAT|FECYT|INIA|Plan Nacional",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Defence patterns — downgrade to review unless research signal present
# ---------------------------------------------------------------------------
_DEFENCE_RE = re.compile(
    r"\b(defensa|ejército|ejercito|fuerzas armadas|cifas|guardia civil|"
    r"policía nacional|policia nacional|seguridad del estado)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Social Security / pension transfers — mark non_rd
# ---------------------------------------------------------------------------
_SOCIAL_RE = re.compile(
    r"\b(seguridad social|pensiones|prestaciones (sociales|por desempleo)|"
    r"clases pasivas|mutualidades|subsidio de desempleo|desempleo)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Generic student grants (NOT FPI/FPU which are research fellowships)
# ---------------------------------------------------------------------------
_GENERIC_GRANT_RE = re.compile(
    r"\bbecas? de estudi(o|os)\b(?!.*\b(investigaci|fpi|fpu|doctorado|posdoctoral)\b)",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Regional / structural fund overhead lines without R&D label
# ---------------------------------------------------------------------------
_REGIONAL_RE = re.compile(
    r"\b(feder|fondos? estructurales?|plan de empleo|cohesión|"
    r"desarrollo regional|programa operativo)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Section / ministry totals → aggregation_role='section'
# ---------------------------------------------------------------------------
_SECTION_TOTAL_RE = re.compile(
    r"\b(total secci[oó]n|total ministerio|resumen del estado|"
    r"total org[aá]nismo|total programa|cap[íi]tulo [ivxlcdm]+[.:]?\s*total|"
    r"total cap[íi]tulo|total presupuesto)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Implausible outlier thresholds
# ---------------------------------------------------------------------------
_EUR_OUTLIER_THRESHOLD = 50_000_000   # > 50 billion EUR in thousands → implausible
_ESP_OUTLIER_THRESHOLD = 5_000_000    # > 5 trillion pesetas in millions → implausible


def _note(df: pd.DataFrame, mask: pd.Series, text: str) -> None:
    df.loc[mask, "cleaning_notes"] = df.loc[mask, "cleaning_notes"].fillna("") + text


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Spain-specific post-extraction corrections. Returns cleaned copy."""
    df = df.copy()

    if "cleaning_notes" not in df.columns:
        df["cleaning_notes"] = ""
    if "aggregation_role" not in df.columns:
        df["aggregation_role"] = ""

    desc_col = "line_description_en" if "line_description_en" in df.columns else "line_description"
    raw_desc_col = "line_description" if "line_description" in df.columns else desc_col
    descs = df[desc_col].fillna("").astype(str)
    raw_descs = df[raw_desc_col].fillna("").astype(str)
    combined = descs + " " + raw_descs

    year_num = pd.to_numeric(df.get("year", pd.Series(dtype=float)), errors="coerce")

    # ------------------------------------------------------------------
    # 1. Defence lines: downgrade to review unless research signal present
    # ------------------------------------------------------------------
    defence_mask = combined.str.contains(_DEFENCE_RE, regex=True)
    has_research = combined.str.contains(_RESEARCH_RE, regex=True)
    downgrade_defence = defence_mask & ~has_research & (df["decision"] == "include")
    if downgrade_defence.any():
        df.loc[downgrade_defence, "decision"] = "review"
        _note(df, downgrade_defence,
              "[defence_line: no research signal — downgraded to review]")

    # ------------------------------------------------------------------
    # 2. Social Security transfers → non_rd
    # ------------------------------------------------------------------
    social_mask = combined.str.contains(_SOCIAL_RE, regex=True)
    if social_mask.any():
        df.loc[social_mask, "aggregation_role"] = "non_rd"
        df.loc[social_mask, "decision"] = "review"
        _note(df, social_mask,
              "[social_security: transfer payment, not R&D appropriation]")

    # ------------------------------------------------------------------
    # 3. Generic student grants → review (FPI/FPU fellowships preserved)
    # ------------------------------------------------------------------
    generic_grant_mask = combined.str.contains(_GENERIC_GRANT_RE, regex=True)
    if generic_grant_mask.any():
        df.loc[generic_grant_mask, "decision"] = "review"
        _note(df, generic_grant_mask,
              "[generic_student_grant: becas de estudio without research label]")

    # ------------------------------------------------------------------
    # 4. Regional development overhead without R&D → review
    # ------------------------------------------------------------------
    regional_mask = combined.str.contains(_REGIONAL_RE, regex=True) & ~has_research
    if regional_mask.any():
        df.loc[regional_mask, "decision"] = "review"
        _note(df, regional_mask,
              "[regional_development: structural fund overhead, no R&D label]")

    # ------------------------------------------------------------------
    # 5. Unit/currency era checks
    #    Pre-2002: expect currency='ESP', unit='million'
    #    2002+:    expect currency='EUR', unit='thousand'
    # ------------------------------------------------------------------
    if "currency" in df.columns and "unit" in df.columns:
        pre_2002 = year_num < 2002
        post_2002 = year_num >= 2002

        wrong_unit_pre = pre_2002 & (df["unit"] == "thousand") & (df["currency"] == "ESP")
        if wrong_unit_pre.any():
            _note(df, wrong_unit_pre,
                  "[unit_mismatch: ESP era expects unit=million, got thousand — verify]")

        wrong_unit_post = post_2002 & (df["unit"] == "million") & (df["currency"] == "EUR")
        if wrong_unit_post.any():
            _note(df, wrong_unit_post,
                  "[unit_mismatch: EUR era expects unit=thousand, got million — verify]")

    # ------------------------------------------------------------------
    # 6. Section/ministry totals → aggregation_role='section'
    # ------------------------------------------------------------------
    section_total_mask = combined.str.contains(_SECTION_TOTAL_RE, regex=True)
    if "item_type" in df.columns:
        section_total_mask = section_total_mask | (df["item_type"] == "section_total")
    if section_total_mask.any():
        df.loc[section_total_mask, "aggregation_role"] = "section"
        df.loc[section_total_mask, "decision"] = df.loc[section_total_mask, "decision"].replace(
            "include", "review"
        )
        _note(df, section_total_mask,
              "[section_total: ministry/section aggregate]")

    # ------------------------------------------------------------------
    # 7. Outlier check: post-2002 EUR thousands > 50 billion → review
    # ------------------------------------------------------------------
    if "amount_local" in df.columns and "unit" in df.columns:
        amounts = pd.to_numeric(df["amount_local"], errors="coerce")
        eur_outlier = (
            (year_num >= 2002)
            & (df.get("currency", pd.Series("", index=df.index)) == "EUR")
            & (df.get("unit", pd.Series("", index=df.index)) == "thousand")
            & (amounts > _EUR_OUTLIER_THRESHOLD)
        )
        if eur_outlier.any():
            df.loc[eur_outlier, "decision"] = "review"
            _note(df, eur_outlier,
                  f"[outlier_eur: amount_local > {_EUR_OUTLIER_THRESHOLD} thousand EUR "
                  f"(> €50B) on single R&D line — implausible, verify]")

        # ------------------------------------------------------------------
        # 8. Outlier check: pre-2002 ESP millions > 5 trillion pesetas → review
        # ------------------------------------------------------------------
        esp_outlier = (
            (year_num < 2002)
            & (df.get("currency", pd.Series("", index=df.index)) == "ESP")
            & (df.get("unit", pd.Series("", index=df.index)) == "million")
            & (amounts > _ESP_OUTLIER_THRESHOLD)
        )
        if esp_outlier.any():
            df.loc[esp_outlier, "decision"] = "review"
            _note(df, esp_outlier,
                  f"[outlier_esp: amount_local > {_ESP_OUTLIER_THRESHOLD} million pesetas "
                  f"(> 5 trillion pts) on single line — implausible, verify]")

    return df.reset_index(drop=True)
