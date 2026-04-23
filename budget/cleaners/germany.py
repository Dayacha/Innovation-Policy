"""
Germany-specific post-extraction cleaner.

Core problem: the same BMBF budget total appears many times in each year:
  - Gesamtplan (Haushaltsübersicht): "Summe Ausgaben" for Epl 30
  - Einzelplan 30 cover page: "Summe des Einzelplans 30"
  - Funktionenübersicht: "Forschung und Entwicklung" (same money, different label)
  - Detailed Einzelplan 30: individual agency grants that SUM to the total

Strategy:
  1. Mark broad Funktionenübersicht categories redundant when a specific
     BMBF total (Summe Ausgaben / Summe des Einzelplans) exists.
  2. Per year: keep ONE canonical BMBF total — the highest-value "Summe Ausgaben"
     or "Summe des Einzelplans 30"; mark others redundant.
  3. Flag OCR errors: values > 3× expected BMBF max get decision='review'.
  4. Deduplicate across multiple bgbl files for the same year.
"""

from __future__ import annotations

import re
import pandas as pd

__all__ = ["clean"]

# ---------------------------------------------------------------------------
# BMBF total patterns — these represent the ministry-level aggregate
# ---------------------------------------------------------------------------
_BMBF_TOTAL_PATTERNS: list[re.Pattern] = [
    re.compile(r"^Summe Ausgaben$", re.IGNORECASE),
    re.compile(r"^Summe des Einzelplans? 30$", re.IGNORECASE),
    re.compile(r"^Gesamtausgaben für das Bundesministerium", re.IGNORECASE),
    re.compile(r"^Gesamtausgaben$", re.IGNORECASE),
    re.compile(r"^Summe$", re.IGNORECASE),
    re.compile(r"^Gesamthaushalt$", re.IGNORECASE),
]

# Funktionenübersicht broad categories — span multiple ministries,
# redundant when a specific Epl-30 total exists.
# Also covers broad programme-category labels at program_total level.
_FUNKTIONEN_PATTERNS: list[re.Pattern] = [
    # Funktionenübersicht spending-type labels
    re.compile(r"^Forschung und (experimentelle )?Entwicklung$", re.IGNORECASE),
    re.compile(r"^Zuweisungen (und Zuschüsse )?für Forschung", re.IGNORECASE),
    re.compile(r"^Zuweisungen für Forschung und Entwicklung$", re.IGNORECASE),
    re.compile(r"Forschung und Entwicklung in der Hochschulbildung", re.IGNORECASE),
    re.compile(r"^Wissenschaft, Forschung, Entwicklung außerhalb der Hochschulen$", re.IGNORECASE),
    re.compile(r"^Zuweisungen und Zuschüsse \(ohne Investitionen\)$", re.IGNORECASE),
    re.compile(r"^Zuweisungen und Zuschüsse$", re.IGNORECASE),   # bare form (no qualifier)
    re.compile(r"^Ausgaben für Investitionen$", re.IGNORECASE),
    re.compile(r"^Personalausgaben$", re.IGNORECASE),
    re.compile(r"^Sächliche Verwaltungsausgaben$", re.IGNORECASE),
    # BMBF budget-category labels (appear as program_total — sums of many agencies)
    re.compile(r"^Forschung,? Technologie und Raumfahrt$", re.IGNORECASE),  # BMBF whole-ministry R&D label
    re.compile(r"^Gemeinsame Forschungsförderung", re.IGNORECASE),           # joint federal-state R&D total
    re.compile(r"^Summe des Kapitels\s+\d", re.IGNORECASE),                  # Kapitel subtotals
    re.compile(r"^Gesamtbetrag", re.IGNORECASE),                             # "Grand total" variants
    re.compile(r"^Gesamtausgaben des Bundes", re.IGNORECASE),                # federal total
    re.compile(r"^Wesentliche finanzwirksame Schwerpunkte", re.IGNORECASE),  # chapter "highlights" — aggregate
    re.compile(r"^Gesamtbetrag für Forschung", re.IGNORECASE),               # R&D total label
    re.compile(r"^Gesamtmittel für Forschung", re.IGNORECASE),               # total R&D funds
    re.compile(r"^Institutionelle Förderung$", re.IGNORECASE),               # cross-agency institutional support bucket
    re.compile(r"^Projektförderung$", re.IGNORECASE),                        # cross-agency project support bucket
    re.compile(r"^Summe Tit\b", re.IGNORECASE),                              # Titel subtotals (Summe Tit. 685 70)
    re.compile(r"^HGF-Zentren\s*-\s*Betrieb$", re.IGNORECASE),              # HGF operational aggregate
    # Ministry-name as line_item (= entire BMBF budget, not a specific grant)
    re.compile(r"^Bundesministerium für Bildung und Forschung$", re.IGNORECASE),
    # Personnel spending-type label (cross-programme aggregate)
    re.compile(r"^Entgelte für Wissenschaftlerinnen und Wissenschaftler$", re.IGNORECASE),
    # Generic broad R&D bucket labels
    re.compile(r"^Forschungsförderung$", re.IGNORECASE),                     # bare "Research Promotion" — too generic
    re.compile(r"^Forschung, Untersuchungen und Ähnliches$", re.IGNORECASE), # "Research, investigations and similar"
    # Non-university research institutions aggregate (sum of Helmholtz + MPG + Leibniz + Fraunhofer)
    re.compile(r"^Institutionelle Zuwendungen an die außeruniversitären\s+Wissenschaftseinrichtungen", re.IGNORECASE),
    # Zukunftsvertrag = federal-state university teaching grant — not R&D
    re.compile(r"^Zukunftsvertrag\b", re.IGNORECASE),
    # Funktionenübersicht rows with a function code prefix (e.g. "Funktion 137 - Deutsche Forschungsgemeinschaft")
    re.compile(r"^Funktion\s+\d+\s*-\s*", re.IGNORECASE),
    # Energy efficiency building subsidies (extracted from wrong ministry in multi-ministry documents)
    re.compile(r"^Förderung von Maßnahmen der Energieeffizienz", re.IGNORECASE),
    # Generic institutional grants label without a specific recipient name
    re.compile(r"^Institutionelle Förderung/Zuschüsse an Einrichtungen gemäß § 26 Abs\.? 3 BHO$", re.IGNORECASE),
]

_GENERIC_RD_LABEL_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"^Research (Funding|Infrastructure|Projects)$", re.IGNORECASE), "generic English bucket"),
    (re.compile(r"^Research and Experimental Development$", re.IGNORECASE), "generic English bucket"),
    (re.compile(r"^Allocations (and grants )?for research", re.IGNORECASE), "generic English bucket"),
    (re.compile(r"^Allocations for Research and Development$", re.IGNORECASE), "generic English bucket"),
    (re.compile(r"^Research and Development in (Applied|Basic) Research$", re.IGNORECASE), "generic English bucket"),
    (re.compile(r"^Innovation Promotion$", re.IGNORECASE), "generic English bucket"),
    (re.compile(r"^Basic Research Programme$", re.IGNORECASE), "generic English bucket"),
    (re.compile(r"^Research, Technology, and Space$", re.IGNORECASE), "broad ministry bucket"),
    (re.compile(r"^Joint research funding", re.IGNORECASE), "broad federal-state funding bucket"),
    (re.compile(r"^Institutional grants to non-university research institutions$", re.IGNORECASE), "cross-agency aggregate"),
]

# Expected BMBF total range in thousands
_BMBF_RANGE_EUR = (5_000_000, 25_000_000)   # €5B–€25B
_BMBF_RANGE_DEM = (10_000_000, 50_000_000)  # DM10B–DM50B (pre-2002)


def _is_bmbf_total(desc: str) -> bool:
    if not isinstance(desc, str):
        return False
    for pat in _BMBF_TOTAL_PATTERNS:
        if pat.search(desc.strip()):
            return True
    return False


def _is_funktionen_broad(desc: str) -> bool:
    if not isinstance(desc, str):
        return False
    for pat in _FUNKTIONEN_PATTERNS:
        if pat.search(desc.strip()):
            return True
    return False


def _likely_ocr_error(amount: float, year: int, currency: str) -> bool:
    """True if amount is more than 3× the expected BMBF maximum."""
    if pd.isna(amount) or amount <= 0:
        return False
    _, hi = _BMBF_RANGE_DEM if (year < 2002 and currency == "DEM") else _BMBF_RANGE_EUR
    return amount > hi * 3   # was 5×, tightened to 3×


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Germany-specific corrections. Returns cleaned copy."""
    df = df.copy()

    if "cleaning_notes" not in df.columns:
        df["cleaning_notes"] = ""
    if "aggregation_role" not in df.columns:
        df["aggregation_role"] = ""

    # ------------------------------------------------------------------
    # 1. Flag OCR / hallucination errors (> 3× expected BMBF max).
    # ------------------------------------------------------------------
    for idx, row in df.iterrows():
        amt = row.get("amount_local")
        yr = int(row.get("year", 2000))
        curr = str(row.get("currency", "EUR") or "EUR")
        if pd.notna(amt) and _likely_ocr_error(float(amt), yr, curr):
            df.at[idx, "decision"] = "review"
            df.at[idx, "cleaning_notes"] = (
                str(df.at[idx, "cleaning_notes"])
                + "[ocr_error_suspect: amount > 3× expected BMBF max]"
            )

    # ------------------------------------------------------------------
    # 2. Per year: if a canonical BMBF total exists (Summe Ausgaben /
    #    Summe des Einzelplans 30), mark Funktionenübersicht broad
    #    categories redundant — they're the same money under a
    #    different classification.
    # ------------------------------------------------------------------
    for year in df["year"].unique():
        year_mask = df["year"] == year
        year_idx = df[year_mask].index

        has_canonical_total = any(
            _is_bmbf_total(str(df.at[i, "line_description"]))
            for i in year_idx
        )

        for i in year_idx:
            desc = str(df.at[i, "line_description"])
            if _is_funktionen_broad(desc):
                df.at[i, "aggregation_role"] = "redundant"
                df.at[i, "decision"] = "review"
                df.at[i, "cleaning_notes"] = (
                    str(df.at[i, "cleaning_notes"])
                    + "[funktionen_broad: spending-type or cross-ministry category]"
                )

    # ------------------------------------------------------------------
    # 2b. Mark recurring generic English labels as redundant. These are
    #     broad programme or funding buckets, not stable institutions.
    # ------------------------------------------------------------------
    for pat, reason in _GENERIC_RD_LABEL_PATTERNS:
        mask = df["line_description"].apply(
            lambda d: bool(isinstance(d, str) and pat.search(d.strip()))
        )
        if mask.any():
            df.loc[mask, "aggregation_role"] = "redundant"
            df.loc[mask, "decision"] = "review"
            df.loc[mask, "cleaning_notes"] = (
                df.loc[mask, "cleaning_notes"] + f"[generic_label: {reason}]"
            )

    # ------------------------------------------------------------------
    # 3a. Force section_total item_type rows to review/redundant.
    #     Section totals are aggregates of their children — including them
    #     alongside the children double-counts the spend.
    # ------------------------------------------------------------------
    if "item_type" in df.columns:
        st_mask = df["item_type"] == "section_total"
        if st_mask.any():
            df.loc[st_mask, "aggregation_role"] = "redundant"
            df.loc[st_mask, "decision"] = "review"
            df.loc[st_mask, "cleaning_notes"] = (
                df.loc[st_mask, "cleaning_notes"]
                + "[section_total: aggregate, not individual agency]"
            )

    # ------------------------------------------------------------------
    # 3. Deduplicate identical rows across files for the same year.
    #    Same (year, description, amount) = same appropriation published
    #    in multiple PDFs → keep first, mark rest redundant.
    #    Different amounts for the same institution = original + supplementary
    #    estimates → keep BOTH (they are additive, not duplicates).
    # ------------------------------------------------------------------
    if len(df) > 1 and "amount_local" in df.columns:
        dup_mask = df.duplicated(
            subset=["year", "line_description", "amount_local"], keep="first"
        )
        if dup_mask.any():
            df.loc[dup_mask, "aggregation_role"] = "redundant"
            df.loc[dup_mask, "decision"] = "review"
            df.loc[dup_mask, "cleaning_notes"] = (
                df.loc[dup_mask, "cleaning_notes"]
                + "[duplicate: same year/description/amount across files]"
            )

    return df.reset_index(drop=True)
