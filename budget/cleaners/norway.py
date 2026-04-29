"""
Norway-specific post-extraction cleaner for the budget pipeline.

Document type: Statsbudsjettet Blåbok (State Budget Blue Book), Norwegian.

KEY ISSUES TO HANDLE:

1. Scanned / low-quality years (1975–1992):
   These documents are fully scanned images with no machine-readable text.
   The LLM will extract near-zero rows, but any rows that do appear should
   be flagged with low confidence and marked for manual review.

2. Dual-scale unit ambiguity (CRITICAL):
   - Part I overview table:  amounts in 1 000 kroner (unit='thousand')
   - Kap./Post detail pages: amounts in FULL NOK  (unit='unit')
   If the LLM extracts an overview row but labels it unit='unit', the value
   is 1000x too small.  Conversely, if it extracts a detail-page row but
   labels it unit='thousand', the value is 1000x too large.
   This cleaner detects implausible amounts for each unit label and flags
   them for review.

3. Overview-table rows vs detail-page rows:
   Prefer detail-page (full NOK) values; mark Part I overview rows as
   aggregation_role='section' when a detail equivalent likely exists.

4. Non-R&D lines to demote:
   - Statens laanekasse (student loans, Kap. 2410): not R&D
   - Plain infrastructure (Vegvesen, Bane NOR, Avinor, Kystverket)
   - Oil/gas production subsidies without 'forskning'
   - Defence procurement (materiel, munisjon, personell)
   - Pension/payroll overhead

5. Section-total rows:
   Kunnskapsdepartementet chapter totals or 'I alt' / 'Sum' lines:
   tag as aggregation_role='section'; keep for reference but do not
   double-count with individual Post lines.

6. NFR appears under multiple ministries:
   The Research Council (Norges forskningsrad) gets grants from
   Kunnskapsdepartementet AND sector ministries (NFD, OED, HOD, etc.).
   All such lines are legitimate -- do NOT deduplicate across ministries.
   Each ministry's NFR grant is additive in the canonical series.
"""

from __future__ import annotations

import re
import pandas as pd

__all__ = ["clean"]

# ---------------------------------------------------------------------------
# Scanned-year threshold
# ---------------------------------------------------------------------------
_SCANNED_YEARS = set(range(1975, 1993))   # 1975-1992 inclusive: fully scanned
_LOW_QUALITY_YEARS = set(range(1993, 2010))  # 1993-2009: partially digital

# ---------------------------------------------------------------------------
# Student loan / non-R&D patterns
# ---------------------------------------------------------------------------
_STUDENT_LOAN_PATTERNS = re.compile(
    r"\b(laanekassen|l[aå]nekassen|statens\s+l[aå]nekasse"
    r"|studiestipend|bostipend|utdanningsstipend"
    r"|stipend(?!.*forskning))\b",
    re.IGNORECASE,
)

# Chapter 2410 = Statens laanekasse (student loans) -- always non-R&D
_KAP_2410 = re.compile(r"\bkap\.?\s*2410\b", re.IGNORECASE)

# ---------------------------------------------------------------------------
# Plain infrastructure -- not R&D unless 'forskning' / 'FoU' present
# ---------------------------------------------------------------------------
_INFRA_AGENCIES = re.compile(
    r"\b(statens\s+vegvesen|bane\s+nor|avinor|kystverket|vegdirektorate"
    r"|jernbanedirektorate|luftfartstilsynet|sj[o\u00f8]fartsdirektorate)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Oil/gas production subsidies (without research label)
# ---------------------------------------------------------------------------
_OIL_PRODUCTION = re.compile(
    r"\b(petoro|sd[o\u00f8]e|leteboring|oljeproduksjon|gass-?\s*transport"
    r"|produksjonslisens|leteareal)\b",
    re.IGNORECASE,
)
_RESEARCH_SIGNAL = re.compile(
    r"\b(forskning|fou|fors\.?\s*og\s+utv|forskningsprogram)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Defence procurement (non-R&D)
# ---------------------------------------------------------------------------
_DEFENCE_PROCUREMENT = re.compile(
    r"\b(materiellanskaffelse|kampfly|munisjon|stridsvogn|fregatt|ub[a\u00e5]t"
    r"|forsvarsinvesteringer|nansen-?\s*klasse)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Pension / payroll overhead
# ---------------------------------------------------------------------------
_PENSION_PATTERNS = re.compile(
    r"\b(pensjonspremie|arbeidsgiveravgift|pensjonskasse"
    r"|fellesutgifter(?!\s*til\s+forskning)|l[o\u00f8]nnsregulering)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Overview / section-total descriptions
# ---------------------------------------------------------------------------
_SECTION_TOTAL_PATTERNS = re.compile(
    r"\bi\s+alt\b|\btotal\b|\bsum\b|\bsummen\b|\btotalt\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Unit plausibility thresholds
# Full-NOK detail page: NFR ~4.7B, universities ~10B -- > 50B is implausible
_OUTLIER_FULL_NOK = 50_000_000_000.0
# Thousand-NOK: > 50,000,000 thousand = > 50 billion NOK -- implausible single line
_OUTLIER_THOUSAND_NOK = 50_000_000.0


def _note(df: pd.DataFrame, mask: pd.Series, text: str) -> None:
    """Append text to cleaning_notes for rows matching mask."""
    df.loc[mask, "cleaning_notes"] = df.loc[mask, "cleaning_notes"].fillna("") + text


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Norway-specific corrections.  Returns a cleaned copy."""
    df = df.copy()

    if "cleaning_notes" not in df.columns:
        df["cleaning_notes"] = ""
    if "aggregation_role" not in df.columns:
        df["aggregation_role"] = ""

    desc_col = "line_description_en" if "line_description_en" in df.columns else "line_description"
    raw_desc_col = "line_description" if "line_description" in df.columns else desc_col
    section_col = "section_name_en" if "section_name_en" in df.columns else (
        "section_name" if "section_name" in df.columns else None
    )

    descs = df[desc_col].fillna("").astype(str)
    raw_descs = df[raw_desc_col].fillna("").astype(str)
    combined = descs + " " + raw_descs

    section_descs = (
        df[section_col].fillna("").astype(str)
        if section_col and section_col in df.columns
        else pd.Series("", index=df.index)
    )

    year_num = pd.to_numeric(df.get("year", pd.Series(dtype=float)), errors="coerce")

    # -----------------------------------------------------------------------
    # 1. Scanned / low-quality years
    # -----------------------------------------------------------------------
    scanned_mask = year_num.isin(_SCANNED_YEARS)
    if scanned_mask.any():
        if "confidence" in df.columns:
            df.loc[scanned_mask, "confidence"] = df.loc[scanned_mask, "confidence"].apply(
                lambda c: min(float(c) if pd.notna(c) else 1.0, 0.2)
            )
        df.loc[scanned_mask, "decision"] = df.loc[scanned_mask, "decision"].apply(
            lambda d: "review" if d == "include" else d
        )
        _note(df, scanned_mask,
              "[scanned_year: 1975-1992 Statsbudsjettet fully scanned -- "
              "text extraction unreliable, very low confidence]")

    low_q_mask = year_num.isin(_LOW_QUALITY_YEARS)
    if low_q_mask.any():
        _note(df, low_q_mask,
              "[low_quality_year: 1993-2009 may be partially scanned -- "
              "verify amounts against source PDF]")

    # -----------------------------------------------------------------------
    # 2. Student loans / Kap. 2410 -- non_rd
    # -----------------------------------------------------------------------
    student_mask = (
        combined.apply(lambda d: bool(_STUDENT_LOAN_PATTERNS.search(d)))
        | combined.apply(lambda d: bool(_KAP_2410.search(d)))
        | section_descs.apply(lambda d: bool(_KAP_2410.search(d)))
    )
    if student_mask.any():
        df.loc[student_mask, "aggregation_role"] = "non_rd"
        df.loc[student_mask, "decision"] = "review"
        _note(df, student_mask, "[student_loan: Laanekassen / student aid, not R&D]")

    # -----------------------------------------------------------------------
    # 3. Plain infrastructure -- demote unless 'forskning' present
    # -----------------------------------------------------------------------
    infra_mask = (
        combined.apply(lambda d: bool(_INFRA_AGENCIES.search(d)))
        & ~combined.apply(lambda d: bool(_RESEARCH_SIGNAL.search(d)))
    )
    if infra_mask.any():
        df.loc[infra_mask, "decision"] = df.loc[infra_mask, "decision"].apply(
            lambda d: "review" if d == "include" else d
        )
        _note(df, infra_mask, "[infrastructure: road/rail/aviation agency without R&D label]")

    # -----------------------------------------------------------------------
    # 4. Oil/gas production subsidies -- demote unless 'forskning' present
    # -----------------------------------------------------------------------
    oil_mask = (
        combined.apply(lambda d: bool(_OIL_PRODUCTION.search(d)))
        & ~combined.apply(lambda d: bool(_RESEARCH_SIGNAL.search(d)))
    )
    if oil_mask.any():
        df.loc[oil_mask, "decision"] = df.loc[oil_mask, "decision"].apply(
            lambda d: "review" if d == "include" else d
        )
        _note(df, oil_mask, "[oil_production: production subsidy without R&D label]")

    # -----------------------------------------------------------------------
    # 5. Defence procurement -- demote
    # -----------------------------------------------------------------------
    defence_mask = combined.apply(lambda d: bool(_DEFENCE_PROCUREMENT.search(d)))
    if defence_mask.any():
        df.loc[defence_mask, "decision"] = df.loc[defence_mask, "decision"].apply(
            lambda d: "review" if d == "include" else d
        )
        _note(df, defence_mask, "[defence_procurement: weapons/materiel, not R&D]")

    # -----------------------------------------------------------------------
    # 6. Pension / overhead lines -- demote
    # -----------------------------------------------------------------------
    pension_mask = combined.apply(lambda d: bool(_PENSION_PATTERNS.search(d)))
    if pension_mask.any():
        df.loc[pension_mask, "decision"] = df.loc[pension_mask, "decision"].apply(
            lambda d: "review" if d == "include" else d
        )
        _note(df, pension_mask, "[pension_overhead: payroll/pension overhead]")

    # -----------------------------------------------------------------------
    # 7. Section-total / 'I alt' descriptions -- tag aggregation_role='section'
    # -----------------------------------------------------------------------
    if "item_type" in df.columns:
        st_mask = df["item_type"] == "section_total"
        if st_mask.any():
            df.loc[st_mask, "aggregation_role"] = df.loc[st_mask, "aggregation_role"].where(
                df.loc[st_mask, "aggregation_role"] != "", "section"
            )
            _note(df, st_mask, "[section_total: ministry/chapter aggregate]")

    i_alt_mask = (
        combined.apply(lambda d: bool(_SECTION_TOTAL_PATTERNS.search(d)))
        & (df["aggregation_role"] == "")
    )
    if i_alt_mask.any():
        df.loc[i_alt_mask, "aggregation_role"] = "section"
        _note(df, i_alt_mask, "[i_alt_description: aggregate total line]")

    # -----------------------------------------------------------------------
    # 8. Unit plausibility / outlier detection
    # -----------------------------------------------------------------------
    if "amount_local" in df.columns and "unit" in df.columns:
        unit_s = df["unit"].fillna("").str.lower()
        amt = pd.to_numeric(df["amount_local"], errors="coerce")

        # Full-NOK outliers (unit='unit' or blank -- assumed full NOK on detail pages)
        full_nok_mask = (
            unit_s.isin(["unit", "krone", "nok", ""])
            & amt.notna()
            & (amt > _OUTLIER_FULL_NOK)
            & (df["decision"] == "include")
        )
        if full_nok_mask.any():
            df.loc[full_nok_mask, "decision"] = "review"
            df.loc[full_nok_mask, "confidence"] = 0.2
            _note(df, full_nok_mask,
                  "[outlier: amount > 50B full NOK -- likely overview table value "
                  "with wrong unit, or multi-year cumulative]")

        # Thousand-NOK outliers
        thousand_mask = (
            unit_s.isin(["thousand", "1000", "1 000"])
            & amt.notna()
            & (amt > _OUTLIER_THOUSAND_NOK)
            & (df["decision"] == "include")
        )
        if thousand_mask.any():
            df.loc[thousand_mask, "decision"] = "review"
            df.loc[thousand_mask, "confidence"] = 0.2
            _note(df, thousand_mask,
                  "[outlier: amount > 50B (in thousands NOK) -- likely full-NOK detail page "
                  "mis-labelled as unit='thousand'; verify against source]")

    # -----------------------------------------------------------------------
    # 9. Overview-table unit check (post-2010 digital years)
    #    Any post-2010 row with unit='thousand' may be from the Part I
    #    overview table rather than the Kap./Post detail pages.
    #    Flag so the operator can verify.
    # -----------------------------------------------------------------------
    if "unit" in df.columns and "year" in df.columns:
        unit_s = df["unit"].fillna("").str.lower()
        overview_candidate = (
            (year_num >= 2010)
            & unit_s.isin(["thousand", "1000", "1 000"])
            & ~scanned_mask
            & ~df["aggregation_role"].isin(["non_rd", "redundant", "section"])
        )
        if overview_candidate.any():
            _note(df, overview_candidate,
                  "[unit_check: post-2010 row has unit=thousand -- likely Part I overview table; "
                  "prefer full-NOK Kap./Post detail-page value if available]")

    return df