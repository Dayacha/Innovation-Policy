"""
Sweden-specific post-extraction cleaner for the budget pipeline.

Document type: Statsbudget / Budgetproposition (Prop. XXXX/XX:1), Swedish.

KEY ISSUES TO HANDLE:

1. Unit (all eras): amounts in THOUSANDS of Swedish kronor (tusental kronor / tkr).
   unit='thousand' throughout. Swedish number format: SPACE is the thousands separator,
   comma ',' is the decimal. '3 500 000' = 3,500,000 tkr = 3.5 billion SEK.
   Flag any row with unit='million' or unit='billion' for verification.

2. Utgiftsområde structure (post-1994 reform):
   From 1994 the budget uses 27 Utgiftsområden (UO).
   R&D primarily in UO 16 (universities, VR, Formas, Forte), UO 24 (VINNOVA),
   UO 20 (SMHI, environmental research).
   Lines from other UOs without 'forskning'/'FoU'/'vetenskap' → demote to review.

3. Student grants/loans (CSN, studiemedel, studiebidrag):
   These are the single largest items in UO 15 and look like R&D at first glance.
   Mark aggregation_role='non_rd'. Never include.

4. Defence lines without research signal:
   Försvarsmakten, FMV (Försvarets materielverk), totalförsvar, materielanskaffning,
   flygsystem, marksystem — procurement/operational, not R&D.
   Demote to review unless 'forskning' or 'FoU' is explicitly present.
   FOI / FOA (Totalförsvarets forskningsinstitut) IS legitimate civilian defence research.

5. Pure infrastructure without research signal:
   Trafikverket, Vägverket, Banverket, Sjöfartsverket, väghållning, järnvägsunderhåll
   → review unless 'forskning' in description.

6. Cultural subsidies without research signal:
   Teater, opera, film, museer, konsert lines → review unless 'forskning' present.

7. Social transfers (never R&D):
   Försäkringskassan, Pensionsmyndigheten, Arbetsförmedlingen, A-kassa,
   sjukpenning, barnbidrag, bostadsbidrag, äldreomsorgen → aggregation_role='non_rd'.

8. Section-total rows → aggregation_role='section':
   'Summa anslag', 'totalt', 'summa', 'utgiftsområde total', item_type='section_total'.

9. Outlier detection (SEK thousands):
   Single R&D line > 50,000,000 thousand (= 50B SEK) is implausible → review.
   (VR full appropriation is ~8-10B SEK in 2024; KTH block grant ~5B SEK.)

10. Pre-2001 research council names (merged into Vetenskapsrådet 2001):
    NFR, TFR, MFR, HSFR, SJFR — all legitimate, keep as include.
"""

from __future__ import annotations

import re
import pandas as pd

__all__ = ["clean"]

# ---------------------------------------------------------------------------
# Compiled patterns
# ---------------------------------------------------------------------------

_STUDENT_AID = re.compile(
    r"\b(studiemedel|studiebidrag|studiest[öo]d|studiel[åa]n"
    r"|csn\b|centrala\s+studiест[öo]dsn[äa]mnden"
    r"|kunskapslyftet(?!\s*forskning)|centrala\s+studiest[öo]ds)\b",
    re.IGNORECASE,
)

_SOCIAL_TRANSFERS = re.compile(
    r"\b(f[öo]rs[äa]kringskassan|pensionsmyndigheten|arbetsf[öo]rmedlingen"
    r"|a-kassa|arbetsl[öo]shetsers[äa]ttning|sjukpenning|sjukf[öo]rs[äa]kring"
    r"|barnbidrag|bostadsbidrag|[äa]ldreomsorgen|socialbidrag|socialf[öo]rs[äa]kring"
    r"|f[öo]r[äa]ldraf[öo]rs[äa]kring|aktivitetsef\b)\b",
    re.IGNORECASE,
)

_DEFENCE_LINES = re.compile(
    r"\b(f[öo]rsvarsmakten|f[öo]rsvarsdepartementet"
    r"|fmv\b|f[öo]rsvarets\s+materielverk"
    r"|totalf[öo]rsvar\b|milj[öo]f[öo]rsvar"
    r"|materielanskaffning|flygsystem|marksystem|sj[öo]system"
    r"|amfibie|hemv[äa]rnet|bef[äa]lsutbildning|kompetensf[öo]rs[öo]rjning\s+f[öo]rsvar)\b",
    re.IGNORECASE,
)

# FOI / FOA is legitimate defence research — exclude from defence demote
_DEFENCE_RESEARCH = re.compile(
    r"\b(foi\b|foa\b|totalf[öo]rsvarets\s+forskningsinstitut"
    r"|f[öo]rsvarets\s+forskningsanstalt)\b",
    re.IGNORECASE,
)

_INFRASTRUCTURE = re.compile(
    r"\b(trafikverket|v[äa]gverket|banverket|sj[öo]fartsverket|luftfartsverket"
    r"|v[äa]gh[åa]llning|j[äa]rnv[äa]gsunderh[åa]ll|j[äa]rnv[äa]gsinvesteringar"
    r"|v[äa]ginvesteringar|sjöfartsstöd|luftfartsst[öo]d)\b",
    re.IGNORECASE,
)

_CULTURAL = re.compile(
    r"\b(teater|opera\b|filmpolitik|filmst[öo]d|konsertst[öo]d"
    r"|museer\b|museum\b|bibliotek(?!\s*forskning)|folkbildning"
    r"|kulturarv(?!\s*forskning)|scenkonst)\b",
    re.IGNORECASE,
)

_RESEARCH_SIGNAL = re.compile(
    r"\b(forskning|fou\b|f\.?o\.?u\.?\b|vetenskap|forskningsr[åa]d"
    r"|vr\b|vinnova\b|formas\b|forte\b|smhi\b|rymdstyrelsen"
    r"|esa\b|kth\b|chalmers\b|foi\b|foa\b|karolinska"
    r"|innovation|laborator)\b",
    re.IGNORECASE,
)

_SECTION_TOTAL_PATTERNS = re.compile(
    r"\b(summa\s+anslag|totalt\s+f[öo]r\s+utg|utg[åa]rdsomr[åa]des\s+totalt"
    r"|uo[- ]total|totalt\b|summa\b|summan\b|i\s+alt\b)\b",
    re.IGNORECASE,
)

# Outlier: single R&D anslag > 50B SEK (in thousands) is implausible
_OUTLIER_SEK_THOUSAND = 50_000_000.0


def _note(df: pd.DataFrame, mask: pd.Series, text: str) -> None:
    """Append text to cleaning_notes for rows matching mask."""
    df.loc[mask, "cleaning_notes"] = df.loc[mask, "cleaning_notes"].fillna("") + text


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Sweden-specific corrections. Returns a cleaned copy."""
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
    has_research = combined.apply(lambda d: bool(_RESEARCH_SIGNAL.search(d)))
    is_foi = combined.apply(lambda d: bool(_DEFENCE_RESEARCH.search(d)))

    # -------------------------------------------------------------------
    # 1. Student aid (CSN, studiemedel, studiebidrag) → non_rd
    # -------------------------------------------------------------------
    student_mask = combined.apply(lambda d: bool(_STUDENT_AID.search(d)))
    if student_mask.any():
        df.loc[student_mask, "aggregation_role"] = "non_rd"
        df.loc[student_mask, "decision"] = "review"
        _note(df, student_mask,
              "[student_aid: CSN/studiemedel/studiebidrag — student grants, not R&D]")

    # -------------------------------------------------------------------
    # 2. Social transfers → non_rd
    # -------------------------------------------------------------------
    social_mask = combined.apply(lambda d: bool(_SOCIAL_TRANSFERS.search(d)))
    if social_mask.any():
        df.loc[social_mask, "aggregation_role"] = "non_rd"
        df.loc[social_mask, "decision"] = "review"
        _note(df, social_mask,
              "[social_transfer: Försäkringskassan/pension/A-kassa — social transfers, not R&D]")

    # -------------------------------------------------------------------
    # 3. Defence lines without research signal → review
    #    Exception: FOI/FOA is legitimate defence research — keep as-is
    # -------------------------------------------------------------------
    defence_mask = (
        combined.apply(lambda d: bool(_DEFENCE_LINES.search(d)))
        & ~has_research
        & ~is_foi
    )
    if defence_mask.any():
        df.loc[defence_mask, "decision"] = df.loc[defence_mask, "decision"].apply(
            lambda d: "review" if d == "include" else d
        )
        _note(df, defence_mask,
              "[defence: Försvarsmakten/FMV procurement without forskning label]")

    # -------------------------------------------------------------------
    # 4. Pure infrastructure without research signal → review
    # -------------------------------------------------------------------
    infra_mask = (
        combined.apply(lambda d: bool(_INFRASTRUCTURE.search(d)))
        & ~has_research
    )
    if infra_mask.any():
        df.loc[infra_mask, "decision"] = df.loc[infra_mask, "decision"].apply(
            lambda d: "review" if d == "include" else d
        )
        _note(df, infra_mask,
              "[infrastructure: Trafikverket/Vägverket/Banverket without forskning label]")

    # -------------------------------------------------------------------
    # 5. Cultural subsidies without research signal → review
    # -------------------------------------------------------------------
    cultural_mask = (
        combined.apply(lambda d: bool(_CULTURAL.search(d)))
        & ~has_research
    )
    if cultural_mask.any():
        df.loc[cultural_mask, "decision"] = df.loc[cultural_mask, "decision"].apply(
            lambda d: "review" if d == "include" else d
        )
        _note(df, cultural_mask,
              "[cultural: teater/opera/museer without forskning label — not R&D]")

    # -------------------------------------------------------------------
    # 6. Section-total rows → aggregation_role='section'
    # -------------------------------------------------------------------
    if "item_type" in df.columns:
        st_mask = df["item_type"] == "section_total"
        if st_mask.any():
            df.loc[st_mask, "aggregation_role"] = df.loc[st_mask, "aggregation_role"].where(
                df.loc[st_mask, "aggregation_role"] != "", "section"
            )
            _note(df, st_mask, "[section_total: item_type=section_total → aggregate]")

    text_total_mask = (
        combined.apply(lambda d: bool(_SECTION_TOTAL_PATTERNS.search(d)))
        & (df["aggregation_role"] == "")
    )
    if text_total_mask.any():
        df.loc[text_total_mask, "aggregation_role"] = "section"
        _note(df, text_total_mask,
              "[section_total: summa anslag / UO-total aggregate row]")

    # -------------------------------------------------------------------
    # 7. Outlier detection (SEK thousands)
    #    > 50B SEK in a single R&D anslag is implausible
    # -------------------------------------------------------------------
    if "amount_local" in df.columns and "unit" in df.columns:
        unit_s = df["unit"].fillna("").str.lower()
        amt = pd.to_numeric(df["amount_local"], errors="coerce")

        outlier_mask = (
            unit_s.isin(["thousand", "1000", "tkr", "tusental"])
            & amt.notna()
            & (amt > _OUTLIER_SEK_THOUSAND)
            & (df["decision"] == "include")
        )
        if outlier_mask.any():
            df.loc[outlier_mask, "decision"] = "review"
            if "confidence" in df.columns:
                df.loc[outlier_mask, "confidence"] = 0.2
            _note(df, outlier_mask,
                  "[outlier: > 50B SEK (thousands) — likely UO total, not single R&D anslag]")

    # -------------------------------------------------------------------
    # 8. Unit check: SEK should always be thousands.
    #    Flag unit='million' or 'billion' for verification.
    # -------------------------------------------------------------------
    if "unit" in df.columns:
        unit_s = df["unit"].fillna("").str.lower()
        wrong_unit_mask = (
            unit_s.isin(["million", "miljoner", "billion", "miljarder"])
            & ~df["aggregation_role"].isin(["non_rd", "redundant", "section"])
        )
        if wrong_unit_mask.any():
            _note(df, wrong_unit_mask,
                  "[unit_check: Swedish Budgetproposition uses tusental kronor (unit=thousand); "
                  "unit=million/billion is unusual — verify source page header]")

    return df
