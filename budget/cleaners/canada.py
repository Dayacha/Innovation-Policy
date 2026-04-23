"""
Canada-specific post-extraction cleaner.

Audit findings (Appropriation Acts 2010–2024, April 2026):

DOCUMENT TYPE: Canada Appropriation Acts (federal statutes) are passed 3–5 times
  per fiscal year:
  - Interim Supply (early in year, ~12% of annual need → small amounts)
  - Main Estimates Appropriation (the full annual budget → largest amounts)
  - Supplementary Estimates A, B, C (incremental adjustments → smaller amounts)

  Each act contains a schedule listing appropriations by department / agency.
  The schedule is bilingual: every English line appears again in French.

UNIT: Canada Main Estimates use THOUSANDS of dollars.
  amount_local=1_321_627, unit='thousand' → C$1.322 billion (NSERC grants, 2021 ✓).

KEY ISSUES IDENTIFIED:

1. Bilingual duplicates:
   Every appropriation line appears in both English and French within the same
   schedule. The LLM extracts both, creating exact (year, source_file, amount)
   duplicate pairs. Keep the English version; mark the French redundant.
   French signal words: dépenses, subventions, inscrites, Instituts, Conseil(s),
   Agence spatiale, fonctionnement, recherches, santé.

2. Section/agency totals in include:
   item_type='section_total' rows represent agency-wide totals (sum of all
   program lines within that section). These must be review/redundant to avoid
   double-counting.

3. Bare "Total" and "Total appropriations for X" descriptions:
   Sometimes extracted as a generic row without proper item_type classification.
   These are always aggregates — mark redundant.

4. Outlier line items (> C$3 billion from a single line):
   Likely the LLM read a cumulative "voted to date" column rather than the
   increment column. Flag as review for manual inspection.

5. Exact (year, line_description, amount) duplicates across documents:
   Same agency appears in multiple acts. Keep first occurrence; mark remainder
   redundant.
"""

from __future__ import annotations

import re
import pandas as pd

__all__ = ["clean"]

# ---------------------------------------------------------------------------
# French-language signal: if a description contains any of these the row is
# likely the French translation of an English line in the same file.
# ---------------------------------------------------------------------------
_FRENCH_SIGNALS = re.compile(
    r"\b(dépenses|fonctionnement|subventions|inscrites|Instituts?\s+de\s+recherche|"
    r"Conseil\s+de\s+recherches|Conseil\s+national\s+de\s+recherches|"
    r"recherches\s+en\s+sciences|Agence\s+spatiale|santé\s+du\s+Canada|"
    r"sciences\s+humaines|sciences\s+naturelles|génie|paiements\s+à\s+la\s+société|"
    r"Autoriser|présent\s+budget|transferts\s+de\s+crédits)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Total / aggregate description patterns
# ---------------------------------------------------------------------------
_TOTAL_PATTERNS: list[re.Pattern] = [
    re.compile(r"^Total\s*$", re.IGNORECASE),
    re.compile(r"^Total\s+for\b", re.IGNORECASE),
    re.compile(r"^Total\s+appropriations\s+for\b", re.IGNORECASE),
    re.compile(r"^Total\s+budget\s+for\b", re.IGNORECASE),
    re.compile(r"^Total\s+expenditures\b", re.IGNORECASE),
    re.compile(r"^Total\s+for\s+\w", re.IGNORECASE),
    re.compile(r"\bTotal\s+appropriations\b", re.IGNORECASE),
    re.compile(r"\bTotal\s+budget\b", re.IGNORECASE),
]

_GENERIC_GRANT_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"^Industry, Science and Technology Grants$", re.IGNORECASE), "generic grant bucket"),
    (re.compile(r"^Industry and Science Development Grants$", re.IGNORECASE), "generic grant bucket"),
    (re.compile(r"^Science and Technology Grants$", re.IGNORECASE), "generic grant bucket"),
    (re.compile(r"^Health Research Grants$", re.IGNORECASE), "generic grant bucket"),
    (re.compile(r"^Grants and Contributions for Research Activities$", re.IGNORECASE), "generic grant bucket"),
    (re.compile(r"^Grants for Research Projects$", re.IGNORECASE), "generic grant bucket"),
    (re.compile(r"^Research grants for natural sciences and engineering$", re.IGNORECASE), "generic grant bucket"),
    (re.compile(r"^Research grants for university projects$", re.IGNORECASE), "generic grant bucket"),
    (re.compile(r"^Medical Research Council - (Operating Expenses|Budgeted Grants)$", re.IGNORECASE), "sub-component of MRC, not canonical agency"),
    (re.compile(r"^Telefilm Canada$", re.IGNORECASE), "cultural funding body, not R&D"),
    (re.compile(r"^Canada Council for the Arts$", re.IGNORECASE), "arts funding body, not R&D"),
]

# Single-line-item amount threshold (thousands CAD) above which a line is
# almost certainly a cumulative aggregate, not a programme increment.
# C$3 billion → 3_000_000 thousand
_OUTLIER_THRESHOLD_K = 3_000_000.0


def _is_total(desc: str) -> bool:
    if not isinstance(desc, str):
        return False
    for pat in _TOTAL_PATTERNS:
        if pat.search(desc.strip()):
            return True
    return False


def _is_french(desc: str) -> bool:
    if not isinstance(desc, str):
        return False
    return bool(_FRENCH_SIGNALS.search(desc))


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Canada-specific corrections. Returns cleaned copy."""
    df = df.copy()

    if "cleaning_notes" not in df.columns:
        df["cleaning_notes"] = ""
    if "aggregation_role" not in df.columns:
        df["aggregation_role"] = ""

    # ------------------------------------------------------------------
    # 1. Mark section_total item_type rows as redundant.
    #    These are agency-wide sums; including them double-counts every
    #    programme line already in include.
    # ------------------------------------------------------------------
    if "item_type" in df.columns:
        st_mask = df["item_type"] == "section_total"
        if st_mask.any():
            df.loc[st_mask, "aggregation_role"] = "redundant"
            df.loc[st_mask, "decision"] = "review"
            df.loc[st_mask, "cleaning_notes"] += "[section_total: agency-wide aggregate]"

    # ------------------------------------------------------------------
    # 2. Mark bare "Total" / "Total appropriations for X" descriptions.
    # ------------------------------------------------------------------
    total_mask = df["line_description"].apply(_is_total)
    if total_mask.any():
        df.loc[total_mask, "aggregation_role"] = "redundant"
        df.loc[total_mask, "decision"] = "review"
        df.loc[total_mask, "cleaning_notes"] += "[total_description: aggregate row]"

    # ------------------------------------------------------------------
    # 2b. Mark generic grant buckets and obvious non-R&D cultural bodies.
    #     These create fake canonical "agencies" in the Canada series.
    # ------------------------------------------------------------------
    for pat, reason in _GENERIC_GRANT_PATTERNS:
        mask = df["line_description"].apply(
            lambda d: bool(isinstance(d, str) and pat.search(d.strip()))
        )
        if "section_name" in df.columns:
            mask = mask | df["section_name"].apply(
                lambda d: bool(isinstance(d, str) and pat.search(d.strip()))
            )
        if mask.any():
            df.loc[mask, "aggregation_role"] = "redundant"
            df.loc[mask, "decision"] = "review"
            df.loc[mask, "cleaning_notes"] += f"[generic_label: {reason}]"

    # ------------------------------------------------------------------
    # 3. Mark French-language descriptions as bilingual duplicates.
    #    Within each (year, source_file, amount_local) group, the English
    #    version is the canonical row; the French is redundant.
    # ------------------------------------------------------------------
    french_mask = df["line_description"].apply(_is_french)
    if french_mask.any():
        df.loc[french_mask, "aggregation_role"] = "redundant"
        df.loc[french_mask, "decision"] = "review"
        df.loc[french_mask, "cleaning_notes"] += "[bilingual_duplicate: French translation of English schedule line]"

    # ------------------------------------------------------------------
    # 4. Flag anomalously large single line items (> C$3B in thousands).
    #    These almost certainly represent a cumulative "voted to date"
    #    column extraction error, not a single programme increment.
    # ------------------------------------------------------------------
    outlier_mask = (
        df["amount_local"].notna()
        & (df["amount_local"] > _OUTLIER_THRESHOLD_K)
        & (df["item_type"] == "line_item")
        & (df["decision"] == "include")
    )
    if outlier_mask.any():
        df.loc[outlier_mask, "decision"] = "review"
        df.loc[outlier_mask, "confidence"] = 0.2
        df.loc[outlier_mask, "cleaning_notes"] += (
            f"[outlier: amount > C$3B — likely cumulative column extraction error]"
        )

    # ------------------------------------------------------------------
    # 5. Deduplicate same appropriation appearing in multiple acts.
    #
    #    Canada's acts each contain a schedule that may re-list prior
    #    appropriations for context (cumulative view). This creates
    #    duplicates where the same agency has the same amount in two
    #    different acts, sometimes with slightly different description text.
    #
    #    Two-pass dedup (prefer line_item > program_total > section_total):
    #
    #    Pass A: exact (year, line_description, amount) match
    #    Pass B: (year, section_name_upper, amount) match — catches
    #            same agency/amount with different description wording
    #            across acts.
    # ------------------------------------------------------------------
    if len(df) > 1 and "amount_local" in df.columns:
        _type_rank = {"line_item": 0, "program_total": 1, "section_total": 2}
        df["_type_rank"] = df["item_type"].map(
            lambda t: _type_rank.get(str(t), 1)
        ) if "item_type" in df.columns else 1
        # Sort so best item_type comes first (stable = preserves relative order for ties)
        df = df.sort_values("_type_rank", kind="stable").reset_index(drop=True)

        # Pass A — exact description match
        dup_a = df.duplicated(
            subset=["year", "line_description", "amount_local"], keep="first"
        )
        if dup_a.any():
            df.loc[dup_a, "aggregation_role"] = "redundant"
            df.loc[dup_a, "decision"] = "review"
            df.loc[dup_a, "cleaning_notes"] += "[duplicate: same year/description/amount across acts]"

        # Pass B — same agency + amount, different description text.
        # Normalize section_name to uppercase for fuzzy section matching.
        if "section_name" in df.columns:
            df["_sec_upper"] = df["section_name"].str.upper().str.strip()
            dup_b = df.duplicated(
                subset=["year", "_sec_upper", "amount_local"], keep="first"
            ) & ~dup_a  # don't re-mark already-marked rows
            if dup_b.any():
                df.loc[dup_b, "aggregation_role"] = "redundant"
                df.loc[dup_b, "decision"] = "review"
                df.loc[dup_b, "cleaning_notes"] += (
                    "[duplicate: same year/agency/amount, different description wording]"
                )
            df = df.drop(columns=["_sec_upper"])

        df = df.drop(columns=["_type_rank"])

    return df.reset_index(drop=True)
