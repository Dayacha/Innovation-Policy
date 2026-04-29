"""
Switzerland-specific post-extraction cleaner for the budget pipeline.

Document type: Voranschlag der Schweizerischen Eidgenossenschaft.

KEY ISSUES TO HANDLE:

1. Two-era document structure (CRITICAL):
   - 1975-2020: Bundesblatt Bundesbeschluss — SHORT (3-10 pages), aggregate only.
     Contains only top-level totals (Erfolgsrechnung, Investitionsrechnung) plus
     any explicitly listed Verpflichtungskredite.  Yield: 1-5 R&D rows maximum.
     DO NOT accept sub-institution breakdowns — they are not in these documents.
   - 2021+: VA-Band3-d.pdf — full departmental detail in section C (Budgetpositionen).
     Key section: WBF (Wirtschaft, Bildung, Forschung) with ETH-Bereich, SNF,
     Innosuisse, CERN, ESA lines.

2. Unit (CRITICAL):
   All amounts are in FULL SWISS FRANCS (unit='unit', currency='CHF').
   Space is the thousands separator.  NEVER scale or divide.
   Example: '3 714 600 000' = CHF 3,714,600,000 (3.7 billion).
   Only use unit='million' if the text explicitly says 'Mio. Fr.' before the number.

3. Defence R&D (VBS/DDPS / armasuisse):
   VBS is a mixed-mandate ministry.  Skip lines unless they explicitly name
   a civilian research programme ('Forschung', 'RUAG Forschung', 'Spiez').

4. Social insurance transfers:
   AHV, IV, EO, EL, ALV lines are social security payments — not R&D.
   Mark as aggregation_role='non_rd'.

5. Infrastructure without research label:
   Strasseninfrastruktur, Eisenbahninfrastruktur, Bauprogramm (unless ETH-related)
   — demote to review unless 'Forschung'/'Wissenschaft' present.

6. Pre-2021 hallucination guard:
   Sub-institution lines (individual ETH Zürich, EPFL, PSI etc.) do NOT appear
   separately in the Bundesblatt files (1975-2020).  Flag any such row from
   pre-2021 documents as potentially hallucinated.

7. Section-total / Gesamttotal rows:
   Tag as aggregation_role='section'.  Keep for reference but do not
   double-count with individual institute lines.

8. Outlier detection:
   Full CHF unit: a single R&D line > 10,000,000,000 CHF (10 billion) is
   implausible — flag for review.
"""

from __future__ import annotations

import re
import pandas as pd

__all__ = ["clean"]

# ---------------------------------------------------------------------------
# Sub-institution names expected ONLY in VA-Band3 (2021+) detail documents.
# In pre-2021 Bundesblatt files, these should NOT appear as separate rows.
# ---------------------------------------------------------------------------
_ETH_SUB_INSTITUTIONS = re.compile(
    r"\b(eth\s+z[üu]rich|epfl|paul\s+scherrer|psi\b|empa\b|eawag\b|wsl\b"
    r"|eidgen[öo]ssische\s+technische\s+hochschule\s+z[üu]rich"
    r"|ecole\s+polytechnique\s+f[ée]d[ée]rale\s+de\s+lausanne)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Defence / VBS patterns (not R&D unless civilian research label present)
# ---------------------------------------------------------------------------
_DEFENCE_PATTERNS = re.compile(
    r"\b(vbs\b|ddps\b|armasuisse|verteidigung(?!\s+forschung)"
    r"|milit[äa]r|kampfflugzeug|r[üu]stung|sicherheitspolizei"
    r"|armeelogistik|aktivdienst)\b",
    re.IGNORECASE,
)

_RESEARCH_SIGNAL = re.compile(
    r"\b(forschung|wissenschaft|snf|eth|epfl|innosuisse|kti\b"
    r"|cern\b|esa\b|spiez|agroscope)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Social insurance transfers (not R&D)
# ---------------------------------------------------------------------------
_SOCIAL_INSURANCE_PATTERNS = re.compile(
    r"\b(ahv\b|alters-\s*und\s+hinterlassenenversicherung"
    r"|iv\b|invalidenversicherung"
    r"|eo\b|erwerbsersatzordnung"
    r"|el\b|erg[äa]nzungsleistungen"
    r"|alv\b|arbeitslosenversicherung"
    r"|prämienverbilligung\s+krankenversicherung"
    r"|familienzulagen|mutterschaftsversicherung)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Infrastructure without research label (transport, buildings)
# ---------------------------------------------------------------------------
_INFRA_PATTERNS = re.compile(
    r"\b(strasseninfrastruktur|nationalstrassen"
    r"|eisenbahninfrastruktur|schienennetz|bahninfrastruktur"
    r"|luftfahrtinfrastruktur|flughafenbeitrag"
    r"|bauprogramm(?!\s+eth|\s+hochschule|\s+forschung)"
    r"|hochbauten(?!\s+forschung))\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Pension / personnel overhead
# ---------------------------------------------------------------------------
_OVERHEAD_PATTERNS = re.compile(
    r"\b(personalaufwand|verwaltungsaufwand|raumaufwand"
    r"|sachaufwand\s+verwaltung|lohnkosten\s+overhead"
    r"|pensionskasse\s+bund|publica\b)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Section-total / Gesamttotal descriptions
# ---------------------------------------------------------------------------
_SECTION_TOTAL_PATTERNS = re.compile(
    r"\bgesamttotal\b|\btotal\b|\bsumme\b|\bgesamt\b|\bi\s+alt\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Outlier threshold (full CHF)
# ---------------------------------------------------------------------------
_OUTLIER_FULL_CHF = 10_000_000_000.0   # 10 billion CHF for a single R&D line


def _note(df: pd.DataFrame, mask: pd.Series, text: str) -> None:
    """Append text to cleaning_notes for rows matching mask."""
    df.loc[mask, "cleaning_notes"] = df.loc[mask, "cleaning_notes"].fillna("") + text


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Switzerland-specific corrections.  Returns a cleaned copy."""
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

    # -----------------------------------------------------------------------
    # 1. Social insurance transfers → non_rd
    # -----------------------------------------------------------------------
    social_mask = combined.apply(lambda d: bool(_SOCIAL_INSURANCE_PATTERNS.search(d)))
    if social_mask.any():
        df.loc[social_mask, "aggregation_role"] = "non_rd"
        df.loc[social_mask, "decision"] = "review"
        _note(df, social_mask, "[social_insurance: AHV/IV/EO/EL/ALV — social security, not R&D]")

    # -----------------------------------------------------------------------
    # 2. Defence lines → review unless they name a specific civilian agency
    #
    # Swiss federal budget instructions state: "Skip VBS/DDPS lines unless
    # they explicitly name a civilian research programme (Forschung, RUAG
    # Forschung, Spiez)."  Generic 'Forschung' or 'R&D' in a VBS chapter
    # heading is NOT sufficient to treat the row as civilian R&D — the
    # military department runs its own testing/development programmes.
    # We require a named civilian institution to pass the filter.
    # -----------------------------------------------------------------------
    _CIVILIAN_INSTITUTIONS = re.compile(
        r"\b(eth\b|epfl|psi\b|empa\b|eawag\b|wsl\b|snf\b|snsf\b"
        r"|innosuisse|kti\b|cern\b|esa\b|spiez|agroscope)\b",
        re.IGNORECASE,
    )
    defence_mask = combined.apply(lambda d: bool(_DEFENCE_PATTERNS.search(d)))
    civilian_inst_mask = combined.apply(lambda d: bool(_CIVILIAN_INSTITUTIONS.search(d)))

    # Case A: defence + NO civilian institution named → demote even if "Forschung" present
    defence_no_civilian = defence_mask & ~civilian_inst_mask
    if defence_no_civilian.any():
        df.loc[defence_no_civilian, "decision"] = df.loc[defence_no_civilian, "decision"].apply(
            lambda d: "review" if d == "include" else d
        )
        _note(df, defence_no_civilian,
              "[defence: VBS/military R&D line without named civilian institution — "
              "generic Forschung keyword is not sufficient to pass as civilian R&D]")

    # Case B: defence + no research signal AND no civilian institution → already covered above
    # but keep old pattern for rows without _DEFENCE_PATTERNS that lack research signal
    defence_no_research = (
        combined.apply(lambda d: bool(_DEFENCE_PATTERNS.search(d)))
        & ~has_research
        & civilian_inst_mask  # only if it has civilian institution (edge case)
    )
    # (this case is intentionally a no-op; just documenting the logic split)

    # -----------------------------------------------------------------------
    # 3. Infrastructure without research signal → review
    # -----------------------------------------------------------------------
    infra_mask = (
        combined.apply(lambda d: bool(_INFRA_PATTERNS.search(d)))
        & ~has_research
    )
    if infra_mask.any():
        df.loc[infra_mask, "decision"] = df.loc[infra_mask, "decision"].apply(
            lambda d: "review" if d == "include" else d
        )
        _note(df, infra_mask, "[infrastructure: transport/buildings without R&D label]")

    # -----------------------------------------------------------------------
    # 4. Overhead / personnel lines → review
    # -----------------------------------------------------------------------
    overhead_mask = combined.apply(lambda d: bool(_OVERHEAD_PATTERNS.search(d)))
    if overhead_mask.any():
        df.loc[overhead_mask, "decision"] = df.loc[overhead_mask, "decision"].apply(
            lambda d: "review" if d == "include" else d
        )
        _note(df, overhead_mask, "[overhead: Personalaufwand / admin overhead, not programme R&D]")

    # -----------------------------------------------------------------------
    # 5. Pre-2021 sub-institution hallucination guard
    #    Individual ETH Zürich, EPFL, PSI etc. do NOT appear as separate lines
    #    in the short Bundesblatt files (1975-2020).  Flag with low confidence.
    # -----------------------------------------------------------------------
    pre2021_mask = year_num < 2021
    sub_inst_mask = (
        pre2021_mask
        & combined.apply(lambda d: bool(_ETH_SUB_INSTITUTIONS.search(d)))
    )
    if sub_inst_mask.any():
        if "confidence" in df.columns:
            df.loc[sub_inst_mask, "confidence"] = df.loc[sub_inst_mask, "confidence"].apply(
                lambda c: min(float(c) if pd.notna(c) else 1.0, 0.3)
            )
        df.loc[sub_inst_mask, "decision"] = df.loc[sub_inst_mask, "decision"].apply(
            lambda d: "review" if d == "include" else d
        )
        _note(df, sub_inst_mask,
              "[pre2021_subinst: individual ETH-sub-institution line in Bundesblatt year — "
              "these documents contain only aggregate totals; row may be hallucinated]")

    # -----------------------------------------------------------------------
    # 6. Section-total / Gesamttotal rows → aggregation_role='section'
    # -----------------------------------------------------------------------
    if "item_type" in df.columns:
        st_mask = df["item_type"] == "section_total"
        if st_mask.any():
            df.loc[st_mask, "aggregation_role"] = df.loc[st_mask, "aggregation_role"].where(
                df.loc[st_mask, "aggregation_role"] != "", "section"
            )
            _note(df, st_mask, "[section_total: department / WBF aggregate]")

    i_alt_mask = (
        combined.apply(lambda d: bool(_SECTION_TOTAL_PATTERNS.search(d)))
        & (df["aggregation_role"] == "")
    )
    if i_alt_mask.any():
        df.loc[i_alt_mask, "aggregation_role"] = "section"
        _note(df, i_alt_mask, "[section_total_desc: Gesamttotal / Total / Summe aggregate line]")

    # -----------------------------------------------------------------------
    # 7. Outlier detection (full CHF > 10B on a single R&D line)
    # -----------------------------------------------------------------------
    if "amount_local" in df.columns and "unit" in df.columns:
        unit_s = df["unit"].fillna("").str.lower()
        amt = pd.to_numeric(df["amount_local"], errors="coerce")

        full_chf_outlier = (
            unit_s.isin(["unit", "chf", "franken", ""])
            & amt.notna()
            & (amt > _OUTLIER_FULL_CHF)
            & (df["decision"] == "include")
            & ~df["aggregation_role"].isin(["section"])
        )
        if full_chf_outlier.any():
            df.loc[full_chf_outlier, "decision"] = "review"
            df.loc[full_chf_outlier, "confidence"] = 0.2
            _note(df, full_chf_outlier,
                  "[outlier: amount > 10B CHF — likely total-budget row, not a single R&D line; "
                  "verify against source (ETH-Bereich block grant is ~3.7B CHF, the largest single line)]")

    # -----------------------------------------------------------------------
    # 8. Unit check: post-2020 row with unit='million' or 'thousand'
    #    All Swiss federal budget amounts are full CHF — flag if otherwise.
    # -----------------------------------------------------------------------
    if "unit" in df.columns:
        unit_s = df["unit"].fillna("").str.lower()
        scaled_unit_mask = (
            unit_s.isin(["thousand", "million", "1000", "mio"])
            & ~df["aggregation_role"].isin(["non_rd", "redundant", "section"])
        )
        if scaled_unit_mask.any():
            _note(df, scaled_unit_mask,
                  "[unit_check: Swiss federal budget amounts are in FULL CHF (unit='unit'); "
                  "a scaled unit (thousand/million) may indicate a narrative 'Mio. Fr.' figure "
                  "— verify this is not a full-CHF amount mislabelled]")

    return df
