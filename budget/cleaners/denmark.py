"""
Denmark-specific post-extraction cleaner for the budget pipeline.

Document type: Finanslov (annual Finance Bill / Budget Act), Danish.

KEY ISSUES TO HANDLE:

1. Unit era transition (CRITICAL):
   - 1975–2000: amounts in 1.000 kr. (thousands DKK)  → unit='thousand'
   - 2001+:      amounts in Mio. kr.  (millions DKK)   → unit='million'
   The LLM is instructed to detect the unit from the page header, but if it
   defaults to 'thousand' for post-2001 years (where amounts are Mio. kr.) the
   canonical series values will be 1000× too large.  This cleaner detects and
   flags any rows where the inferred unit looks implausible for the year.

2. Student grant / SU rows:
   Lines for Statens Uddannelsesstøtte (SU), Statens Lånekasse, or
   'uddannelsesstøtte' are student financial aid — not R&D.  Mark as
   aggregation_role='non_rd' and decision='review'.

3. Section-overview totals:
   The Finanslov opens each § with a one-line ministry/section total
   (e.g. "§ 19. I alt 28.789,7 mio. kr."). The LLM may extract these
   as item_type='section_total'.  Mark aggregation_role='section' so
   compile.py can decide whether to use them; do NOT mark redundant
   because Denmark lacks better individual-line data for some years.

4. Generic Driftsudgifter (operating expenditure) lines in mixed ministries:
   Lines labelled only "Driftsudgifter" (without a research institution name)
   from non-R&D ministries should be demoted to review.

5. Pension/overhead lines:
   Lines mentioning 'tjenestemandspension', 'pension', 'lønsum' that are
   clearly payroll overhead — demote to review.

6. Outlier detection:
   Post-2001 (Mio. kr.) amounts > 100,000 Mio. kr. are almost certainly unit
   errors (should be thousands, not millions).  Flag for review.
   Pre-2001 (thousand DKK) line items > 5,000,000 thousand (5 billion DKK)
   are suspicious — flag for review.
"""

from __future__ import annotations

import re
import pandas as pd

__all__ = ["clean"]

# ---------------------------------------------------------------------------
# SU / student-aid patterns (not R&D)
# ---------------------------------------------------------------------------
_SU_PATTERNS = re.compile(
    r"\b(su\b|uddannelsesstøtte|uddannelsesstotte|laanekasse|lånekasse"
    r"|studiestøtte|studiestotte|studiegæld|studiegaeld"
    r"|statens uddannelses|bostipend|stipendier(?!\s*til\s*forskning))\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Pension / payroll overhead patterns
# ---------------------------------------------------------------------------
_PENSION_PATTERNS = re.compile(
    r"\b(tjenestemandspension|tjenestemænd|tjenestepension"
    r"|lønsum|loensum|pensionsbidrag|arbejdsgiverbidrag)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Generic Driftsudgifter without a research tag (operating expenditure)
# Match lines that are *exactly* "Driftsudgifter" (possibly with whitespace)
# or "Driftsudgifter" followed by only a period/dash.
# ---------------------------------------------------------------------------
_BARE_DRIFTS = re.compile(r"^driftsudgifter[\s.–-]*$", re.IGNORECASE)

# ---------------------------------------------------------------------------
# Section-total description patterns
# "§ 19. I alt", "I alt", "Totalt", "Sum"
# ---------------------------------------------------------------------------
_SECTION_TOTAL_PATTERNS = re.compile(
    r"\bi\s+alt\b|\btotal\b|\btotalt\b|\bsum\b|\bsummen\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Non-R&D ministry keywords — generic Driftsudgifter in these ministries
# should be flagged.
# ---------------------------------------------------------------------------
_NON_RD_MINISTRIES = re.compile(
    r"\b(socialministeri|indenrigsministeri|justitsministeri"
    r"|finansministeri|skatteministeri|trafikministeri|transportministeri"
    r"|forsvarsministeri|udenrigsministeri)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Unit plausibility thresholds (amount_local values)
# ---------------------------------------------------------------------------
# Post-2001 Mio. kr.: a single R&D line > 100,000 Mio. kr. is implausible
_OUTLIER_POST_2001_MILLION = 100_000.0   # 100 billion DKK in Mio. kr. — impossible
# Pre-2001 thousand DKK: a single R&D line > 5,000,000 thousand (5 billion DKK)
# is suspicious for a single line item (possible for a university total but
# worth flagging).
_OUTLIER_PRE_2001_THOUSAND = 5_000_000.0  # 5 billion DKK in thousands


def _note(df: pd.DataFrame, mask: pd.Series, text: str) -> None:
    """Append text to cleaning_notes for rows matching mask."""
    df.loc[mask, "cleaning_notes"] = df.loc[mask, "cleaning_notes"].fillna("") + text


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Denmark-specific corrections.  Returns a cleaned copy."""
    df = df.copy()

    if "cleaning_notes" not in df.columns:
        df["cleaning_notes"] = ""
    if "aggregation_role" not in df.columns:
        df["aggregation_role"] = ""

    # Helper: resolve description column (try line_description_en first, then
    # line_description; fall back to empty string series)
    def _desc(row: str | None) -> str:
        return str(row) if isinstance(row, str) else ""

    desc_col = "line_description_en" if "line_description_en" in df.columns else "line_description"
    raw_desc_col = "line_description" if "line_description" in df.columns else desc_col
    section_col = "section_name_en" if "section_name_en" in df.columns else (
        "section_name" if "section_name" in df.columns else None
    )

    # Vectorised description series (lower already handled by regex flags)
    descs = df[desc_col].fillna("").astype(str)
    raw_descs = df[raw_desc_col].fillna("").astype(str)

    section_descs = (
        df[section_col].fillna("").astype(str)
        if section_col and section_col in df.columns
        else pd.Series("", index=df.index)
    )

    # -----------------------------------------------------------------------
    # 1. Section-overview totals → aggregation_role='section'
    #    The Finanslov § overview totals are legitimate reference points;
    #    tag them so compile.py can treat them as section-level aggregates.
    # -----------------------------------------------------------------------
    if "item_type" in df.columns:
        st_mask = df["item_type"] == "section_total"
        if st_mask.any():
            df.loc[st_mask, "aggregation_role"] = df.loc[st_mask, "aggregation_role"].where(
                df.loc[st_mask, "aggregation_role"] != "", "section"
            )
            _note(df, st_mask, "[section_total: § overview aggregate]")

    # Also catch description-level "I alt" / "Total" patterns
    i_alt_mask = (
        (descs.str.contains(r"\bi\s+alt\b", case=False, regex=True) |
         raw_descs.str.contains(r"\bi\s+alt\b", case=False, regex=True))
        & (df["aggregation_role"] == "")
    )
    if i_alt_mask.any():
        df.loc[i_alt_mask, "aggregation_role"] = "section"
        _note(df, i_alt_mask, "[i_alt_description: § section total]")

    # -----------------------------------------------------------------------
    # 2. Student grant / SU lines → non_rd
    # -----------------------------------------------------------------------
    su_mask = (
        descs.apply(lambda d: bool(_SU_PATTERNS.search(d)))
        | raw_descs.apply(lambda d: bool(_SU_PATTERNS.search(d)))
    )
    if su_mask.any():
        df.loc[su_mask, "aggregation_role"] = "non_rd"
        df.loc[su_mask, "decision"] = "review"
        _note(df, su_mask, "[su_line: student financial aid, not R&D]")

    # -----------------------------------------------------------------------
    # 3. Pension / payroll overhead lines → review
    # -----------------------------------------------------------------------
    pension_mask = (
        descs.apply(lambda d: bool(_PENSION_PATTERNS.search(d)))
        | raw_descs.apply(lambda d: bool(_PENSION_PATTERNS.search(d)))
    )
    if pension_mask.any():
        df.loc[pension_mask, "decision"] = df.loc[pension_mask, "decision"].apply(
            lambda d: "review" if d == "include" else d
        )
        _note(df, pension_mask, "[pension_overhead: payroll/pension line]")

    # -----------------------------------------------------------------------
    # 4. Bare "Driftsudgifter" lines in non-R&D ministry sections → review
    #    Only demote if: (a) description is bare Driftsudgifter AND
    #                    (b) the section is a non-R&D ministry.
    # -----------------------------------------------------------------------
    bare_drifts_mask = (
        raw_descs.apply(lambda d: bool(_BARE_DRIFTS.match(d)))
        | descs.apply(lambda d: bool(_BARE_DRIFTS.match(d)))
    )
    non_rd_section_mask = section_descs.apply(lambda s: bool(_NON_RD_MINISTRIES.search(s)))
    bare_in_nonrd = bare_drifts_mask & non_rd_section_mask
    if bare_in_nonrd.any():
        df.loc[bare_in_nonrd, "decision"] = df.loc[bare_in_nonrd, "decision"].apply(
            lambda d: "review" if d == "include" else d
        )
        _note(df, bare_in_nonrd, "[bare_driftsudgifter: non-R&D ministry operating line]")

    # -----------------------------------------------------------------------
    # 5. Unit plausibility / outlier detection
    #    Post-2001 (unit='million'): flag amounts > 100,000 Mio. kr.
    #    Pre-2001  (unit='thousand'): flag line items > 5,000,000 thousand DKK
    # -----------------------------------------------------------------------
    if "amount_local" in df.columns and "unit" in df.columns and "year" in df.columns:
        year_num = pd.to_numeric(df["year"], errors="coerce")

        # Post-2001 rows
        post2001 = (
            (year_num >= 2001)
            & df["unit"].fillna("").str.lower().isin(["million", "mio"])
            & df["amount_local"].notna()
            & (df["amount_local"] > _OUTLIER_POST_2001_MILLION)
            & (df["decision"] == "include")
        )
        if post2001.any():
            df.loc[post2001, "decision"] = "review"
            df.loc[post2001, "confidence"] = 0.2
            _note(df, post2001,
                  "[outlier: amount > 100,000 Mio. kr. — likely unit error or multi-year cumulative]")

        # Pre-2001 line items
        pre2001_outlier = (
            (year_num < 2001)
            & df["unit"].fillna("").str.lower().isin(["thousand", "1000", "1.000"])
            & df["amount_local"].notna()
            & (df["amount_local"] > _OUTLIER_PRE_2001_THOUSAND)
            & (df.get("item_type", pd.Series("", index=df.index)) == "line_item")
            & (df["decision"] == "include")
        )
        if pre2001_outlier.any():
            df.loc[pre2001_outlier, "decision"] = "review"
            df.loc[pre2001_outlier, "confidence"] = 0.3
            _note(df, pre2001_outlier,
                  "[outlier: pre-2001 line item > 5B DKK (thousands) — verify against source]")

    # -----------------------------------------------------------------------
    # 6. Unit mismatch detection:
    #    If year >= 2001 and unit is 'thousand', the LLM may have missed the
    #    Mio. kr. header.  Flag these rows so the operator can verify.
    # -----------------------------------------------------------------------
    if "unit" in df.columns and "year" in df.columns:
        year_num = pd.to_numeric(df["year"], errors="coerce")
        unit_mismatch = (
            (year_num >= 2001)
            & df["unit"].fillna("").str.lower().isin(["thousand", "1000", "1.000"])
            & ~df["aggregation_role"].isin(["non_rd", "redundant"])
        )
        if unit_mismatch.any():
            _note(df, unit_mismatch,
                  "[unit_check: post-2001 row has unit=thousand — verify Finanslov header; "
                  "from 2001 onwards amounts are Mio. kr. (millions)]")

    return df
