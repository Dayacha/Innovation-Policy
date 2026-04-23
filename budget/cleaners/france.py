"""
France-specific post-extraction cleaner.

Audit findings (JORF Loi de Finances 2010–2025, April 2026):

DOCUMENT TYPE: JORF (Loi de Finances) is the budget law text.
  - Amounts are mission/programme-level crédits de paiement (CP),
    not individual agency grants (CNRS, ANR, CEA etc. come from PAP annexes).

UNIT: JORF tables show amounts in MILLIONS d'euros.  The LLM frequently
  returns unit='thousand' with the PDF number as-is. This means:
    - amount_local=929_000 with unit='thousand' → €929M ✓ (amount already in thousands)
    - amount_local=2_417 with unit='thousand' → €2.4M ✗ (should be €2.4B; off by 1000x)
  We cannot reliably auto-correct the off-by-1000x cases without ground-truth
  ranges per programme. Flag and review.

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

    return df.reset_index(drop=True)
