"""
France-specific post-extraction cleaner.

Audit findings (JORF Loi de Finances 1970–2025, April 2026):

DOCUMENT TYPE: JORF (Loi de Finances) is the budget law text.
  - 2006+ (LOLF): Amounts are mission/programme-level crédits de paiement (CP),
    not individual agency grants (CNRS, ANR, CEA etc. come from PAP annexes).
  - Pre-2006: État B/C tables by ministry chapter.

UNIT: LOLF era (2006+) credit table amounts are in FULL EUROS.
  The LLM should extract and convert to millions (divide by 1,000,000, unit='million').
  If it instead extracts from the ETPT headcount table, amounts will be:
    - Very large (e.g. 203,561 FTEs × 1000 → 203,561,000 with unit='thousand') — wrong
    - Or in the 1,000–300,000 range with unit='million' → also wrong (FTEs read as millions)
  Detection heuristics (steps 7 & 8 below) flag these.

KNOWN ISSUES:
  1. "Crédits de paiement" as bare line description = column header extracted
     by mistake, not a programme line. Drop.
  2. NaN amounts — rows where LLM found a description but no amount. Drop.
  3. Bilingual duplicates: 2020–2021 JORF pages contain both French and English
     translations of the same programme name. The LLM extracts both, creating
     pairs with slightly different amounts. Keep the French version (canonical);
     mark the English "Total for …" versions redundant.
  4. Mission totals ("Total pour la mission …", "Total budget for Research and
     Higher Education") are mission-level sums — redundant if sub-programmes
     are also present.
  5. Exact duplicates: same (year, description, amount) across pages. Keep first.
  6. FTE headcount rows: LLM extracts from ETPT staffing table instead of credit
     table. Detected by implausible amount×unit combinations (steps 7 & 8).
  7. Pre-2006 Legifrance placeholders: some euro-era pre-LOLF pages say
     "Vous pouvez consulter le tableau dans le JO..." instead of showing the
     actual Etat B/C table. These references are not extractable budget rows.
"""

from __future__ import annotations

import re
import pandas as pd

__all__ = ["clean"]

# ---------------------------------------------------------------------------
# Patterns for bare column-header descriptions (not programme names)
# ---------------------------------------------------------------------------
_BARE_CP_PATTERN = re.compile(r"^Crédits de paiement$", re.IGNORECASE)

# ---------------------------------------------------------------------------
# Mission/programme totals — redundant when sub-programmes are present
# ---------------------------------------------------------------------------
_TOTAL_PATTERNS: list[re.Pattern] = [
    re.compile(r"^Total (pour |for |budget for |du |de la |des )", re.IGNORECASE),
    re.compile(r"^Total mission\b", re.IGNORECASE),
    re.compile(r"^Total pour la mission\b", re.IGNORECASE),
    re.compile(r"^Ensemble (de la mission|du programme)\b", re.IGNORECASE),
    re.compile(r"\bTotal budget for Research and Higher Education\b", re.IGNORECASE),
]

# ---------------------------------------------------------------------------
# English-language description patterns: 2020–2021 JORF bilingual pages
# Keep the French canonical form; mark these review/redundant.
# ---------------------------------------------------------------------------
_ENGLISH_TOTAL_PATTERNS: list[re.Pattern] = [
    re.compile(r"^Total for\b", re.IGNORECASE),
    re.compile(r"^Total budget for\b", re.IGNORECASE),
    re.compile(r"^Total of\b", re.IGNORECASE),
]

_PRE_2006_REFERENCE_PATTERNS: list[re.Pattern] = [
    re.compile(r"vous pouvez consulter le tableau dans le jo", re.IGNORECASE),
    re.compile(r"tableau.*journal officiel", re.IGNORECASE),
]

_PRE_2006_STRUCTURAL_PATTERNS: list[re.Pattern] = [
    re.compile(r"^e\s*t\s*a\s*t\s*[abcde]\b", re.IGNORECASE),
    re.compile(r"^etat\s*[abcde]\b", re.IGNORECASE),
    re.compile(r"^titre\s*[ivx]+\b", re.IGNORECASE),
    re.compile(r"^mesures nouvelles\b", re.IGNORECASE),
    re.compile(r"^r[ée]partition,\s*par titre et par minist[èe]re", re.IGNORECASE),
]

_GENERIC_PROGRAMME_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"^Payment credits for\b", re.IGNORECASE), "generic payment-credit programme bucket"),
    (re.compile(r"^Research in the fields? of\b", re.IGNORECASE), "broad thematic programme label"),
    (re.compile(r"^Multidisciplinary Scientific and Technological Research$", re.IGNORECASE), "broad mission label"),
    (re.compile(r"^Space Research$", re.IGNORECASE), "broad mission label"),
    (re.compile(r"^Dual Research Programme$", re.IGNORECASE), "broad programme bucket"),
    (re.compile(r"^Applied Research and Innovation in Agriculture$", re.IGNORECASE), "broad programme bucket"),
]


def _is_total(desc: str) -> bool:
    if not isinstance(desc, str):
        return False
    for pat in _TOTAL_PATTERNS:
        if pat.search(desc.strip()):
            return True
    return False


def _is_english_total(desc: str) -> bool:
    if not isinstance(desc, str):
        return False
    for pat in _ENGLISH_TOTAL_PATTERNS:
        if pat.search(desc.strip()):
            return True
    return False


def _is_pre2006_reference(desc: str) -> bool:
    if not isinstance(desc, str):
        return False
    text = desc.strip()
    return any(pat.search(text) for pat in _PRE_2006_REFERENCE_PATTERNS)


def _is_pre2006_structural(desc: str) -> bool:
    if not isinstance(desc, str):
        return False
    text = desc.strip()
    return any(pat.search(text) for pat in _PRE_2006_STRUCTURAL_PATTERNS)


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply France-specific corrections. Returns cleaned copy."""
    df = df.copy()

    if "cleaning_notes" not in df.columns:
        df["cleaning_notes"] = ""
    if "aggregation_role" not in df.columns:
        df["aggregation_role"] = ""

    # ------------------------------------------------------------------
    # 1. Drop bare "Crédits de paiement" rows (column header, no content).
    # ------------------------------------------------------------------
    bare_cp = df["line_description"].apply(
        lambda d: bool(isinstance(d, str) and _BARE_CP_PATTERN.match(d.strip()))
    )
    if bare_cp.any():
        df = df[~bare_cp].copy()

    # ------------------------------------------------------------------
    # 2. Drop rows with NaN amounts.
    # ------------------------------------------------------------------
    no_amount = df["amount_local"].isna()
    if no_amount.any():
        df = df[~no_amount].copy()

    # ------------------------------------------------------------------
    # 3. Mark mission-level totals as redundant.
    # ------------------------------------------------------------------
    total_mask = df["line_description"].apply(_is_total)
    if total_mask.any():
        df.loc[total_mask, "aggregation_role"] = "redundant"
        df.loc[total_mask, "decision"] = "review"
        df.loc[total_mask, "cleaning_notes"] += "[mission_total: aggregate of sub-programmes]"

    # ------------------------------------------------------------------
    # 3b. Mark broad mission/programme labels that should not become
    #     pseudo-agencies in the canonical series.
    # ------------------------------------------------------------------
    for pat, reason in _GENERIC_PROGRAMME_PATTERNS:
        mask = df["line_description"].apply(
            lambda d: bool(isinstance(d, str) and pat.search(d.strip()))
        )
        if mask.any():
            df.loc[mask, "aggregation_role"] = "redundant"
            df.loc[mask, "decision"] = "review"
            df.loc[mask, "cleaning_notes"] += f"[generic_programme: {reason}]"

    # ------------------------------------------------------------------
    # 4. Mark section_total item_type rows as redundant (same logic as
    #    Japan / Germany cleaners).
    # ------------------------------------------------------------------
    if "item_type" in df.columns:
        st_mask = df["item_type"] == "section_total"
        if st_mask.any():
            df.loc[st_mask, "aggregation_role"] = "redundant"
            df.loc[st_mask, "decision"] = "review"
            df.loc[st_mask, "cleaning_notes"] += "[section_total: aggregate, not individual programme]"

    # ------------------------------------------------------------------
    # 5. Mark English "Total for …" descriptions as redundant (bilingual
    #    duplicates of French canonical descriptions).
    # ------------------------------------------------------------------
    eng_mask = df["line_description"].apply(_is_english_total)
    if eng_mask.any():
        df.loc[eng_mask, "aggregation_role"] = "redundant"
        df.loc[eng_mask, "decision"] = "review"
        df.loc[eng_mask, "cleaning_notes"] += "[bilingual_duplicate: English translation of French programme total]"

    # ------------------------------------------------------------------
    # 6. Deduplicate exact (year, description, amount) matches.
    # ------------------------------------------------------------------
    if len(df) > 1 and "amount_local" in df.columns:
        dup_mask = df.duplicated(
            subset=["year", "line_description", "amount_local"], keep="first"
        )
        if dup_mask.any():
            df.loc[dup_mask, "aggregation_role"] = "redundant"
            df.loc[dup_mask, "decision"] = "review"
            df.loc[dup_mask, "cleaning_notes"] += "[duplicate: same year/description/amount]"

    # ------------------------------------------------------------------
    # 6b. Pre-2006 Legifrance extracts sometimes contain only a reference to
    #     the official table, not the table itself. Drop those placeholders.
    # ------------------------------------------------------------------
    if "year" in df.columns:
        ref_mask = (
            (pd.to_numeric(df["year"], errors="coerce") <= 2005)
            & df["line_description"].apply(_is_pre2006_reference)
        )
        if ref_mask.any():
            df = df[~ref_mask].copy()

        structural_mask = (
            (pd.to_numeric(df["year"], errors="coerce") <= 2005)
            & df["line_description"].apply(_is_pre2006_structural)
        )
        if structural_mask.any():
            df = df[~structural_mask].copy()

    # ------------------------------------------------------------------
    # 7. LOLF era (year >= 2006): Flag all wrong-unit and FTE extractions.
    #
    #    The extraction profile instructs the LLM to set unit='million' for
    #    all LOLF-era JORF documents (credit table amounts are in FULL EUROS,
    #    convert by dividing by 1,000,000). Therefore:
    #
    #    (A) unit='thousand' for year >= 2006 is ALWAYS wrong — either:
    #        • LLM extracted from the ETPT headcount table (not the credit table)
    #        • LLM extracted the credit table amount but applied the wrong unit
    #        Either way: flag as suspicious.
    #
    #    (B) unit='million' with amount_local > 50,000 for year >= 2006:
    #        The largest plausible programme budget is ~€30B = 30,000 million.
    #        If amount_local > 50,000, the LLM read a headcount (e.g. 203,561
    #        FTEs) as a million-EUR figure (= €203 trillion — impossible).
    # ------------------------------------------------------------------
    if "year" in df.columns and "unit" in df.columns:
        lolf_mask = df["year"] >= 2006

        # Signature A: any unit='thousand' in LOLF era — wrong by construction
        fte_sig_a = lolf_mask & (df["unit"] == "thousand")
        if fte_sig_a.any():
            df.loc[fte_sig_a, "aggregation_role"] = "redundant"
            df.loc[fte_sig_a, "decision"] = "review"
            df.loc[fte_sig_a, "cleaning_notes"] += (
                "[wrong_unit_lolf: unit=thousand in LOLF era (2006+) — "
                "JORF credit table is in full EUR; profile requires unit=million. "
                "Likely ETPT headcount or unit-conversion error. Re-extract.]"
            )

        # Signature B: unit='million' with implausibly large amount
        # (> 50,000 million = > €50 trillion — impossible for any programme)
        fte_sig_b = (
            lolf_mask
            & (df["unit"] == "million")
            & (df["amount_local"] > 50_000)
        )
        if fte_sig_b.any():
            df.loc[fte_sig_b, "aggregation_role"] = "redundant"
            df.loc[fte_sig_b, "decision"] = "review"
            df.loc[fte_sig_b, "cleaning_notes"] += (
                "[fte_headcount_suspected: unit=million with amount>50000 "
                "— likely ETPT headcount misread as million EUR (e.g. 203,561 FTEs → €203T)]"
            )

    return df.reset_index(drop=True)
