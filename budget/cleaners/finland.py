"""
Finland-specific post-extraction cleaner.

Documents: Valtion talousarvio (State Budget), 1985–2025.

ERA GUIDE:
  1985-1991 (FIM, scanned):  currency='FIM', unit='unit'. OCR quality poor.
  1992-2001 (FIM, digital):  currency='FIM', unit='unit'. Full markka amounts.
  2002+     (EUR, digital):  currency='EUR', unit='unit'. Full euro amounts.
  UNIT RULE: ALL amounts are in FULL currency units (no thousands/millions scaling).
  Space is the thousands separator: '169 941 000' = 169,941,000 EUR.

KEY R&D MOMENTS:
  29.60.50  Suomen Akatemian tutkimusmäärärahat  (research grants — KEY series)
  29.60.01  Suomen Akatemian toimintamenot        (Academy operating costs)
  32.20.06  Tekes toimintamenot                   (pre-2018)
  32.20.05  Business Finland toimintamenot        (from 2018)
  32.20.40  Julkinen tutkimus- ja kehittämistoiminta (public R&D grants)
  32.01.02  VTT institutional grant
  32.01.04  GTK (Geologian tutkimuskeskus)

FALSE-POSITIVE PATTERNS TO FLAG:
  - Student grants (opintotuki, opintolaina, Kela education benefits)
  - Defence lines (chapter 27, Puolustusvoimat, Maavoimat) without 'tutkimus'
  - Lottery transfers (Veikkaus, raha-arpajaisten voittovarat) for arts/sports
  - Pure infrastructure (tieverkko, rataverkko, Väylävirasto) without tutkimus
  - Unit='million' or 'thousand' → likely wrong (should always be 'unit')
  - FIM-era rows from 2018+ named 'Tekes' → probably mislabelled (→ Business Finland)
"""

from __future__ import annotations

import re

import pandas as pd

__all__ = ["clean"]

# ---------------------------------------------------------------------------
# Research signal
# ---------------------------------------------------------------------------
_RESEARCH_RE = re.compile(
    r"tutkimus|tiede|akatemia|innovaatio|VTT|Tekes|Business Finland|kehittäminen",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Student grants → non_rd
# ---------------------------------------------------------------------------
_STUDENT_RE = re.compile(
    r"\b(opintotuki|opintolaina|opintoraha|kela.*opinto|"
    r"opintoetuus|opintoavustus|koulumatkatuki)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Defence lines → review unless research present
# ---------------------------------------------------------------------------
_DEFENCE_RE = re.compile(
    r"\b(puolustus(?:voimat|ministeri)|maavoimat|merivoimat|ilmavoimat|"
    r"puolustushaara|sotilas|varuskunta|puolustusmateriaali)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Lottery/arts transfers (Veikkaus for sports/culture) → review
# Only science-earmarked Veikkaus funds are R&D
# ---------------------------------------------------------------------------
_LOTTERY_RE = re.compile(
    r"\b(veikkaus(?:voittovaroista|tuotto)?|raha-arpajaisten voittovarat|"
    r"taide|kulttuuri|liikunta|urheilu)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Infrastructure without research → review
# ---------------------------------------------------------------------------
_INFRA_RE = re.compile(
    r"\b(tieverkko|rataverkko|väylävirasto|liikennevirasto|traficom|"
    r"tiehallinto|ratahallinto|maantie|rautatie)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Section/chapter totals → aggregation_role='section'
# ---------------------------------------------------------------------------
_SECTION_TOTAL_RE = re.compile(
    r"\b(yhteensä|pääluokka yhteensä|momentti yhteensä|luku yhteensä|"
    r"käyttötalous yhteensä|yhteensä[,.]?\s*\d|\d\d\.\s*yhteensä)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Tekes rename guard: rows from 2018+ named "Tekes" are likely mislabelled
# ---------------------------------------------------------------------------
_TEKES_RE = re.compile(r"\btekes\b", re.IGNORECASE)

# ---------------------------------------------------------------------------
# Outlier: single R&D line > 2 billion EUR is implausible
# ---------------------------------------------------------------------------
_EUR_OUTLIER = 2_000_000_000


def _note(df: pd.DataFrame, mask: pd.Series, text: str) -> None:
    df.loc[mask, "cleaning_notes"] = df.loc[mask, "cleaning_notes"].fillna("") + text


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Finland-specific post-extraction corrections. Returns cleaned copy."""
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
    # 1. Student grants → non_rd
    # ------------------------------------------------------------------
    student_mask = combined.str.contains(_STUDENT_RE, regex=True)
    if student_mask.any():
        df.loc[student_mask, "aggregation_role"] = "non_rd"
        df.loc[student_mask, "decision"] = "review"
        _note(df, student_mask,
              "[student_grant: opintotuki/opintolaina — not R&D]")

    # ------------------------------------------------------------------
    # 2. Defence lines → review unless research signal present
    # ------------------------------------------------------------------
    has_research = combined.str.contains(_RESEARCH_RE, regex=True)
    defence_mask = combined.str.contains(_DEFENCE_RE, regex=True) & ~has_research
    downgrade_defence = defence_mask & (df["decision"] == "include")
    if downgrade_defence.any():
        df.loc[downgrade_defence, "decision"] = "review"
        _note(df, downgrade_defence,
              "[defence_line: no tutkimus signal — downgraded to review]")

    # ------------------------------------------------------------------
    # 3. Lottery/arts Veikkaus transfers → review
    # ------------------------------------------------------------------
    lottery_mask = combined.str.contains(_LOTTERY_RE, regex=True) & ~has_research
    if lottery_mask.any():
        df.loc[lottery_mask, "decision"] = "review"
        _note(df, lottery_mask,
              "[lottery_transfer: Veikkaus for arts/sports — not R&D unless science-earmarked]")

    # ------------------------------------------------------------------
    # 4. Pure infrastructure without research → review
    # ------------------------------------------------------------------
    infra_mask = combined.str.contains(_INFRA_RE, regex=True) & ~has_research
    if infra_mask.any():
        df.loc[infra_mask, "decision"] = "review"
        _note(df, infra_mask,
              "[infrastructure: no tutkimus signal — downgraded to review]")

    # ------------------------------------------------------------------
    # 5. Unit check: all amounts should be unit='unit' (full EUR or FIM)
    #    Flag unit='million' or 'thousand' as likely wrong
    # ------------------------------------------------------------------
    if "unit" in df.columns:
        wrong_unit = df["unit"].isin(["million", "thousand"])
        if wrong_unit.any():
            df.loc[wrong_unit, "decision"] = "review"
            _note(df, wrong_unit,
                  "[unit_error: Finland amounts should be unit='unit' (full EUR/FIM); "
                  "unit=million/thousand is likely wrong — re-verify]")

    # ------------------------------------------------------------------
    # 6. Section/chapter totals → aggregation_role='section'
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
              "[section_total: chapter/pääluokka aggregate]")

    # ------------------------------------------------------------------
    # 7. Outlier: single EUR line > 2 billion → review
    # ------------------------------------------------------------------
    if "amount_local" in df.columns:
        amounts = pd.to_numeric(df["amount_local"], errors="coerce")
        eur_outlier = (
            (year_num >= 2002)
            & (df.get("currency", pd.Series("", index=df.index)) == "EUR")
            & (amounts > _EUR_OUTLIER)
        )
        if eur_outlier.any():
            df.loc[eur_outlier, "decision"] = "review"
            _note(df, eur_outlier,
                  f"[outlier_eur: amount_local > {_EUR_OUTLIER} EUR "
                  f"(> €2B) on single R&D line — implausible, verify]")

    # ------------------------------------------------------------------
    # 8. FIM era: pre-2002 rows should have currency='FIM'
    # ------------------------------------------------------------------
    if "currency" in df.columns:
        fim_era_wrong = (year_num < 2002) & (df["currency"] == "EUR")
        if fim_era_wrong.any():
            _note(df, fim_era_wrong,
                  "[currency_mismatch: pre-2002 row has currency=EUR, expected FIM — verify]")

    # ------------------------------------------------------------------
    # 9. Tekes rename guard: rows from 2018+ still named 'Tekes' → flag
    # ------------------------------------------------------------------
    tekes_mask = combined.str.contains(_TEKES_RE, regex=True) & (year_num >= 2018)
    if tekes_mask.any():
        _note(df, tekes_mask,
              "[tekes_rename: Tekes became Business Finland in 2018 — "
              "verify this row should not be labelled Business Finland]")

    return df.reset_index(drop=True)
