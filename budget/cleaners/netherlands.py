"""
Netherlands-specific post-extraction cleaner for the budget pipeline.

Document type: Rijksbegroting (State Budget).

KEY ISSUES TO HANDLE:

1. Unit era transition (CRITICAL):
   - 1975-2001: amounts in MILLIONS of guilders (miljoenen guldens, NLG)
                → unit='million', currency='NLG'
   - 2002+:     amounts in THOUSANDS of euros (bedragen x € 1.000, EUR)
                → unit='thousand', currency='EUR'
   Mis-labelling (e.g. unit='million' in post-2002 EUR files) will produce
   1000× too-large values.  This cleaner detects and flags implausible combos.

2. Student grants / DUO lines:
   'Studiefinanciering', 'studietoelage', 'studentenreisproduct',
   'DUO' (Dienst Uitvoering Onderwijs) — student financial aid, not R&D.
   Mark as aggregation_role='non_rd'.

3. Per-ministry file structure (2002+):
   Ministry10 = Defensie (Defence): skip unless explicitly science.
   Ministry12 = IenW (Infrastructure & Water): skip unless 'onderzoek'.
   Ministry16 = VWS (Health): skip unless RIVM line present.
   These are mixed-ministry files; the LLM may extract non-R&D lines from them.

4. Generic 'Overige' and 'Overige programma-uitgaven' lines:
   In non-R&D ministries these are catch-all operating lines — demote to review.

5. Section-total rows:
   OCW Art. 07/16 totals and EZ Art. 02/03 totals are legitimate reference
   aggregates.  Tag as aggregation_role='section' so compile.py can use them
   when individual sub-lines are unavailable.

6. Outlier detection:
   Post-2001 (EUR thousands): a single R&D line > 10,000,000 thousand
   (= €10 billion) is implausible for the Netherlands.
   Pre-2002 (NLG millions): a single R&D line > 100,000 million NLG
   is implausible — flag for review.
"""

from __future__ import annotations

import re
import pandas as pd

__all__ = ["clean"]

# ---------------------------------------------------------------------------
# Student aid / DUO patterns (not R&D)
# ---------------------------------------------------------------------------
_STUDENT_AID_PATTERNS = re.compile(
    r"\b(studiefinanciering|studietoelage|studiebeurs|studietoelage"
    r"|studentenreisproduct|ov-kaart\s+studenten"
    r"|duo(?!\s+naam|\s+campus))"   # DUO = Dienst Uitvoering Onderwijs
    r"|\bdienst\s+uitvoering\s+onderwijs\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Non-R&D ministry patterns (ministry10, 12, 16 by number or name)
# ---------------------------------------------------------------------------
_NON_RD_MINISTRY_PATTERNS = re.compile(
    r"\b(defensie|ministry\s+x\b|ministerie\s+x\b"
    r"|infrastructuur\s+en\s+waterstaat|ienw|rijkswaterstaat(?!\s*onderzoek)"
    r"|volksgezondheidswet|vws(?!\s+rivm)"
    r"|ministry\s*(?:10|12|16)\b|ministry(?:10|12|16)\.pdf)"
    r"|\bministry\s+of\s+(?:defence|infrastructure|health)\b",
    re.IGNORECASE,
)

_RESEARCH_SIGNAL = re.compile(
    r"\b(onderzoek|wetenschap|kennis|r&d|research|rivm|knmi|deltares|nioz)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Generic 'Overige' lines in non-R&D sections
# ---------------------------------------------------------------------------
_OVERIGE_PATTERN = re.compile(
    r"^\s*overige\s*(?:programma-?\s*uitgaven)?\s*$",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Pension / social overhead patterns
# ---------------------------------------------------------------------------
_PENSION_PATTERNS = re.compile(
    r"\b(abp-premie|pensioenpremie|werkloosheidswet|ww-uitkering"
    r"|zorgverzekering\s+werkgever|loonkosten\s+overhead"
    r"|sociale\s+lasten|werkgeverslasten)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Infrastructure without research signal
# ---------------------------------------------------------------------------
_INFRA_PATTERNS = re.compile(
    r"\b(rijksinfrastructuur|aanleg\s+(?:weg|spoor|water)"
    r"|onderhoud\s+(?:weg|spoor|vaarweg|kanaal)"
    r"|prorail|luchtvaartinfrastructuur"
    r"|rijkswaterstaat|hoofdvaarwegen|hoofdwegennet)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Cultural subsidies without research
# ---------------------------------------------------------------------------
_CULTURE_PATTERNS = re.compile(
    r"\b(podiumkunsten|musea(?!\s*onderzoek)|erfgoed(?!\s*onderzoek)"
    r"|cultuurfonds|amateurkunst|monumentenzorg(?!\s*onderzoek)"
    r"|filmfonds|letterenfonds)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Section-total descriptions
# ---------------------------------------------------------------------------
_SECTION_TOTAL_PATTERNS = re.compile(
    r"\btotaal\b|\bi\s+alt\b|\bsummen?\b|\bartikeltotaal\b|\btotale\s+uitgaven\b"
    r"|\btotale\s+verplichtingen\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Unit plausibility thresholds
# ---------------------------------------------------------------------------
# Post-2001 EUR thousands: single R&D line > 10B EUR is implausible
_OUTLIER_EUR_THOUSAND = 10_000_000.0   # 10 billion EUR in thousands
# Pre-2002 NLG millions: single R&D line > 100,000 million NLG implausible
_OUTLIER_NLG_MILLION = 100_000.0       # 100 billion NLG in millions


def _note(df: pd.DataFrame, mask: pd.Series, text: str) -> None:
    """Append text to cleaning_notes for rows matching mask."""
    df.loc[mask, "cleaning_notes"] = df.loc[mask, "cleaning_notes"].fillna("") + text


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Netherlands-specific corrections.  Returns a cleaned copy."""
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
    source_col = "source_file" if "source_file" in df.columns else None

    descs = df[desc_col].fillna("").astype(str)
    raw_descs = df[raw_desc_col].fillna("").astype(str)
    combined = descs + " " + raw_descs

    section_descs = (
        df[section_col].fillna("").astype(str)
        if section_col and section_col in df.columns
        else pd.Series("", index=df.index)
    )
    source_s = (
        df[source_col].fillna("").astype(str)
        if source_col and source_col in df.columns
        else pd.Series("", index=df.index)
    )
    year_num = pd.to_numeric(df.get("year", pd.Series(dtype=float)), errors="coerce")

    # -----------------------------------------------------------------------
    # 1. Student financial aid → non_rd
    # -----------------------------------------------------------------------
    student_mask = (
        combined.apply(lambda d: bool(_STUDENT_AID_PATTERNS.search(d)))
        | section_descs.apply(lambda d: bool(_STUDENT_AID_PATTERNS.search(d)))
    )
    if student_mask.any():
        df.loc[student_mask, "aggregation_role"] = "non_rd"
        df.loc[student_mask, "decision"] = "review"
        _note(df, student_mask, "[student_aid: studiefinanciering/DUO — student grants, not R&D]")

    # -----------------------------------------------------------------------
    # 2. Non-R&D ministry lines (Defensie, IenW, VWS) without research signal
    # -----------------------------------------------------------------------
    # Detect from source file name (ministry10/12/16) or section name
    non_rd_source_mask = (
        source_s.apply(lambda s: bool(re.search(
            r"ministry(?:10|12|16)\.", s, re.IGNORECASE
        )))
    )
    non_rd_section_mask = (
        section_descs.apply(lambda d: bool(_NON_RD_MINISTRY_PATTERNS.search(d)))
        | combined.apply(lambda d: bool(_NON_RD_MINISTRY_PATTERNS.search(d)))
    )
    has_research = combined.apply(lambda d: bool(_RESEARCH_SIGNAL.search(d)))
    non_rd_mask = (non_rd_source_mask | non_rd_section_mask) & ~has_research
    if non_rd_mask.any():
        df.loc[non_rd_mask, "decision"] = df.loc[non_rd_mask, "decision"].apply(
            lambda d: "review" if d == "include" else d
        )
        _note(df, non_rd_mask, "[non_rd_ministry: Defensie/IenW/VWS without research signal]")

    # -----------------------------------------------------------------------
    # 3. Pension / social overhead → review
    # -----------------------------------------------------------------------
    pension_mask = combined.apply(lambda d: bool(_PENSION_PATTERNS.search(d)))
    if pension_mask.any():
        df.loc[pension_mask, "decision"] = df.loc[pension_mask, "decision"].apply(
            lambda d: "review" if d == "include" else d
        )
        _note(df, pension_mask, "[pension_overhead: social insurance / pension overhead]")

    # -----------------------------------------------------------------------
    # 4. Infrastructure without research signal → review
    # -----------------------------------------------------------------------
    infra_mask = (
        combined.apply(lambda d: bool(_INFRA_PATTERNS.search(d)))
        & ~has_research
    )
    if infra_mask.any():
        df.loc[infra_mask, "decision"] = df.loc[infra_mask, "decision"].apply(
            lambda d: "review" if d == "include" else d
        )
        _note(df, infra_mask, "[infrastructure: road/rail/water infrastructure without R&D label]")

    # -----------------------------------------------------------------------
    # 5. Cultural subsidies without research signal → review
    # -----------------------------------------------------------------------
    culture_mask = (
        combined.apply(lambda d: bool(_CULTURE_PATTERNS.search(d)))
        & ~has_research
    )
    if culture_mask.any():
        df.loc[culture_mask, "decision"] = df.loc[culture_mask, "decision"].apply(
            lambda d: "review" if d == "include" else d
        )
        _note(df, culture_mask, "[cultural_subsidy: arts/heritage without research signal]")

    # -----------------------------------------------------------------------
    # 6. Generic 'Overige' lines without research context → review
    # -----------------------------------------------------------------------
    overige_mask = (
        (descs.apply(lambda d: bool(_OVERIGE_PATTERN.match(d)))
         | raw_descs.apply(lambda d: bool(_OVERIGE_PATTERN.match(d))))
        & ~has_research
    )
    if overige_mask.any():
        df.loc[overige_mask, "decision"] = df.loc[overige_mask, "decision"].apply(
            lambda d: "review" if d == "include" else d
        )
        _note(df, overige_mask, "[bare_overige: generic 'overige' line without R&D context]")

    # -----------------------------------------------------------------------
    # 7. Section-total / article-total rows → aggregation_role='section'
    # -----------------------------------------------------------------------
    if "item_type" in df.columns:
        st_mask = df["item_type"] == "section_total"
        if st_mask.any():
            df.loc[st_mask, "aggregation_role"] = df.loc[st_mask, "aggregation_role"].where(
                df.loc[st_mask, "aggregation_role"] != "", "section"
            )
            _note(df, st_mask, "[section_total: article/ministry aggregate]")

    i_alt_mask = (
        combined.apply(lambda d: bool(_SECTION_TOTAL_PATTERNS.search(d)))
        & (df["aggregation_role"] == "")
    )
    if i_alt_mask.any():
        df.loc[i_alt_mask, "aggregation_role"] = "section"
        _note(df, i_alt_mask, "[section_total_desc: totaal/artikeltotaal aggregate line]")

    # -----------------------------------------------------------------------
    # 8. Unit plausibility / outlier detection
    # -----------------------------------------------------------------------
    if "amount_local" in df.columns and "unit" in df.columns:
        unit_s = df["unit"].fillna("").str.lower()
        amt = pd.to_numeric(df["amount_local"], errors="coerce")

        # Post-2001 EUR thousands: flag amounts > 10B EUR
        eur_outlier = (
            (year_num >= 2002)
            & unit_s.isin(["thousand", "1000", "duizend"])
            & amt.notna()
            & (amt > _OUTLIER_EUR_THOUSAND)
            & (df["decision"] == "include")
        )
        if eur_outlier.any():
            df.loc[eur_outlier, "decision"] = "review"
            df.loc[eur_outlier, "confidence"] = 0.2
            _note(df, eur_outlier,
                  "[outlier: amount > 10B EUR (thousands) — likely ministry total, not R&D line]")

        # Pre-2002 NLG millions: flag amounts > 100,000 million NLG
        nlg_outlier = (
            (year_num < 2002)
            & unit_s.isin(["million", "miljoen", "miljoenen"])
            & amt.notna()
            & (amt > _OUTLIER_NLG_MILLION)
            & (df["decision"] == "include")
        )
        if nlg_outlier.any():
            df.loc[nlg_outlier, "decision"] = "review"
            df.loc[nlg_outlier, "confidence"] = 0.3
            _note(df, nlg_outlier,
                  "[outlier: amount > 100,000 million NLG — likely total-national-budget row]")

    # -----------------------------------------------------------------------
    # 9. Unit-era mismatch flags
    #    Post-2001 rows with unit='million' may have missed the 'bedragen x € 1.000'
    #    header — flag so the operator can verify.
    # -----------------------------------------------------------------------
    if "unit" in df.columns:
        unit_s = df["unit"].fillna("").str.lower()
        post2001_million = (
            (year_num >= 2002)
            & unit_s.isin(["million", "miljoen", "miljoenen"])
            & ~df["aggregation_role"].isin(["non_rd", "redundant", "section"])
        )
        if post2001_million.any():
            _note(df, post2001_million,
                  "[unit_check: post-2001 row has unit=million — "
                  "verify; from 2002 onwards Rijksbegroting amounts are x € 1.000 (thousands)]")

        pre2002_thousand = (
            (year_num < 2002)
            & unit_s.isin(["thousand", "1000", "duizend"])
            & ~df["aggregation_role"].isin(["non_rd", "redundant", "section"])
        )
        if pre2002_thousand.any():
            _note(df, pre2002_thousand,
                  "[unit_check: pre-2002 row has unit=thousand — "
                  "verify; Miljoenennota amounts before 2002 are in miljoenen guldens (millions NLG)]")

    return df
