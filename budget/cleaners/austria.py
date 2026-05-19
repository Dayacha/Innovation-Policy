"""
Austria-specific post-extraction cleaner for the budget pipeline.

Document type: Bundesfinanzgesetz (BFG) / Bundesvoranschlag (BVA), German.

KEY ISSUES TO HANDLE:

1. Unit era (CRITICAL):
   - 1975-2001: amounts in MILLIONS of Austrian Schillings
                ("Beträge in Millionen Schilling").
                → unit='million', currency='ATS'
   - 2002+:     amounts in MILLIONS of euros
                ("Beträge in Millionen Euro").
                → unit='million', currency='EUR'
   Many Austria rows are currently mislabelled as unit='thousand'. In addition,
   values like "20,421" are often parsed as 20421 instead of 20.421, so some
   rows need both a unit relabel and a divide-by-1000 correction.

2. Budget structure reform 2013 (Haushaltsrechtsreform):
   Pre-2013:  Einzelpläne (Kapitel system).
              Einzelplan 13 = Wissenschaft und Forschung (BMWF) — key R&D chapter.
              Einzelplan 07 = Verkehr, Innovation, Technologie (BMVIT — mixed).
   Post-2013: Untergliederungen (UG system).
              UG 31 = Wissenschaft und Forschung — FWF, ÖAW, universities, IST Austria.
              UG 33 = Wirtschaft — FFG, AWS, AIT.
              UG 34 = Verkehr, Innovation und Technologie (BMVIT — mixed mandate).
   Lines outside these UGs without explicit 'Forschung'/'Wissenschaft' → demote to review.

3. Defence lines (non-R&D unless research is explicitly named):
   Bundesministerium für Landesverteidigung, Heer, Miliz, Rüstung/Rüstungsamt,
   materiel procurement → review unless 'Forschung' or 'Rüstungsforschung' named.

4. Social transfer lines (never R&D):
   Pensionsversicherung, Krankenversicherung, Arbeitslosengeld/AMS, Familienbeihilfe,
   Pflegegeld, Sozialhilfe, Notstandshilfe → aggregation_role='non_rd'.

5. Pure infrastructure without research signal (→ review):
   Straßenbau, Schieneninfrastruktur, ASFINAG, ÖBB-Infrastruktur,
   Hochbau/Bundesgebäude without Forschung.

6. Section-total rows → aggregation_role='section':
   'Gesamtsumme', 'Summe', 'Gesamt', 'UG-Gesamt', 'I.alt', 'Summe UG',
   or item_type='section_total'.

7. Outlier detection:
   Post-2001 (EUR millions): single R&D line > 5,000 million (= €5B) implausible.
   Pre-2002  (ATS millions): single R&D line > 100,000 million (= 100B ATS) implausible.

8. UG 31 signal: rows with a clear UG 31 / Einzelplan 13 context and research signal
   should be high-confidence (leave decision='include' if already set).
"""

from __future__ import annotations

import re
import pandas as pd

__all__ = ["clean"]

# ---------------------------------------------------------------------------
# Compiled patterns
# ---------------------------------------------------------------------------

_SOCIAL_TRANSFERS = re.compile(
    r"\b(pensionsversicherung|krankenversicherung|arbeitslosengeld"
    r"|arbeitslosenversicherung|arbeitsmarktservice\b|ams\b"
    r"|familienbeihilfe|pflegegeld|sozialhilfe|notstandshilfe"
    r"|kinderbetreuungsgeld|ausgleichszulagen|wochengeld"
    r"|invalidit[äa]tspension|witwen(?:renten|pension))\b",
    re.IGNORECASE,
)

_DEFENCE_LINES = re.compile(
    r"\b(landesverteidigung|bundesministerium\s+f[üu]r\s+landesverteidigung"
    r"|bmlv\b|heer\b|miliz\b|milizwesen"
    r"|r[üu]stungsamt|r[üu]stungsauskr[üu]stung"
    r"|kampfpanzer|hubschrauber(?!\s+forschung)|kampfflugzeug"
    r"|munition|fliegerkr[äa]fte|marinekr[äa]fte)\b",
    re.IGNORECASE,
)

_INFRASTRUCTURE = re.compile(
    r"\b(stra[sß]enbau|schieneninfrastruktur|schienenweg"
    r"|asfinag\b|[öo]bb-infrastruktur|bundesstra[sß]en"
    r"|hochbau\b|bundesgeb[äa]ude|wohnbauf[öo]rderung"
    r"|kanal(?!forschung)|wasserversorgung(?!\s*forschung)"
    r"|abwasserentsorgung)\b",
    re.IGNORECASE,
)

_RESEARCH_SIGNAL = re.compile(
    r"\b(forschung|wissenschaft|f\.?&?e\.?\b|f&e\b|fwf\b|ffg\b|fff\b"
    r"|[öo][äa]w\b|oeaw\b|ait\b|arsenal\s+research"
    r"|innovation|technologie|universit[äa]t|hochschule"
    r"|akademie\s+der\s+wissenschaften|ista\b|ist\s+austria"
    r"|cern\b|esa\b|raumfahrt|r[üu]stungsforschung"
    r"|cd-labor|christian\s+doppler)\b",
    re.IGNORECASE,
)

_SECTION_TOTAL_PATTERNS = re.compile(
    r"\b(gesamtsumme|gesamtbetrag|ug-gesamt|summe\s+ug"
    r"|gesamt\b|summe\b|i\.?\s*alt\.?\b|i\s+alt\b"
    r"|kapitelgesamt|einzelplangesamt|budgetgesamt)\b",
    re.IGNORECASE,
)

# Outlier thresholds (in amount_local units — i.e. MILLIONS of currency)
_OUTLIER_EUR_MILLION = 5_000.0      # > €5B (5,000 million) implausible for single R&D line
_OUTLIER_ATS_MILLION = 100_000.0    # > 100B ATS (100,000 million) implausible


def _note(df: pd.DataFrame, mask: pd.Series, text: str) -> None:
    """Append text to cleaning_notes for rows matching mask."""
    df.loc[mask, "cleaning_notes"] = df.loc[mask, "cleaning_notes"].fillna("") + text


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Austria-specific corrections. Returns a cleaned copy."""
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

    # -------------------------------------------------------------------
    # 0. Fix Austria's documented currency/unit regime before any ranking.
    # -------------------------------------------------------------------
    if "currency" in df.columns:
        expected_currency = pd.Series(pd.NA, index=df.index, dtype="object")
        expected_currency.loc[year_num < 2002] = "ATS"
        expected_currency.loc[year_num >= 2002] = "EUR"
        currency_mask = (
            expected_currency.notna()
            & df["currency"].fillna("").astype(str).str.upper().ne(expected_currency)
        )
        if currency_mask.any():
            df.loc[currency_mask, "currency"] = expected_currency.loc[currency_mask]
            _note(df, currency_mask, "[currency_fix: Austria pre-2002=ATS, 2002+=EUR]")

    if "unit" in df.columns and "amount_local" in df.columns:
        unit_s = df["unit"].fillna("").astype(str).str.lower().str.strip()
        amount_num = pd.to_numeric(df["amount_local"], errors="coerce")
        thousand_mask = unit_s.isin(["thousand", "tausend", "1000"])
        if thousand_mask.any():
            rounded = amount_num.round()
            integer_like = thousand_mask & amount_num.notna() & ((amount_num - rounded).abs() < 1e-9)
            extra_thousands_mask = integer_like & rounded.abs().ge(1_000_000) & rounded.mod(1000).eq(0)
            decimal_comma_mask = integer_like & ~extra_thousands_mask

            if extra_thousands_mask.any():
                df.loc[extra_thousands_mask, "amount_local"] = amount_num.loc[extra_thousands_mask] / 1_000_000.0
                _note(
                    df,
                    extra_thousands_mask,
                    "[unit_fix: Austria value had extra trailing 000s; converted thousand→million and ÷1,000,000]",
                )

            if decimal_comma_mask.any():
                df.loc[decimal_comma_mask, "amount_local"] = amount_num.loc[decimal_comma_mask] / 1000.0
                _note(
                    df,
                    decimal_comma_mask,
                    "[unit_fix: Austria decimal comma lost; converted thousand→million and ÷1,000]",
                )
            df.loc[thousand_mask, "unit"] = "million"
            _note(df, thousand_mask, "[unit_fix: Austria budget tables use Millionen, not thousand]")

    # -------------------------------------------------------------------
    # 1. Social transfers → non_rd
    # -------------------------------------------------------------------
    social_mask = combined.apply(lambda d: bool(_SOCIAL_TRANSFERS.search(d)))
    if social_mask.any():
        df.loc[social_mask, "aggregation_role"] = "non_rd"
        df.loc[social_mask, "decision"] = "review"
        _note(df, social_mask,
              "[social_transfer: Pensionsversicherung/AMS/Familienbeihilfe — social transfers, not R&D]")

    # -------------------------------------------------------------------
    # 2. Defence lines without research signal → review
    # -------------------------------------------------------------------
    defence_mask = (
        combined.apply(lambda d: bool(_DEFENCE_LINES.search(d)))
        & ~has_research
    )
    if defence_mask.any():
        df.loc[defence_mask, "decision"] = df.loc[defence_mask, "decision"].apply(
            lambda d: "review" if d == "include" else d
        )
        _note(df, defence_mask,
              "[defence: Landesverteidigung/Heer/Miliz without Forschung label]")

    # -------------------------------------------------------------------
    # 3. Pure infrastructure without research signal → review
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
              "[infrastructure: Straßenbau/ASFINAG/ÖBB-Infrastruktur without Forschung label]")

    # -------------------------------------------------------------------
    # 4. Section-total rows → aggregation_role='section'
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
              "[section_total: Gesamtsumme/UG-Gesamt/I alt aggregate row]")

    # -------------------------------------------------------------------
    # 5. Outlier detection
    # -------------------------------------------------------------------
    if "amount_local" in df.columns and "unit" in df.columns:
        unit_s = df["unit"].fillna("").str.lower()
        amt = pd.to_numeric(df["amount_local"], errors="coerce")

        # Post-2001 EUR millions: > €5B implausible for a single R&D line
        eur_outlier = (
            (year_num >= 2002)
            & unit_s.isin(["million", "millionen", "million eur"])
            & amt.notna()
            & (amt > _OUTLIER_EUR_MILLION)
            & (df["decision"] == "include")
        )
        if eur_outlier.any():
            df.loc[eur_outlier, "decision"] = "review"
            if "confidence" in df.columns:
                df.loc[eur_outlier, "confidence"] = 0.2
            _note(df, eur_outlier,
                  "[outlier: > €5B (millions EUR) — likely UG total, not single R&D line]")

        # Pre-2002 ATS millions: > 100B ATS implausible
        ats_outlier = (
            (year_num < 2002)
            & unit_s.isin(["million", "millionen", "million ats"])
            & amt.notna()
            & (amt > _OUTLIER_ATS_MILLION)
            & (df["decision"] == "include")
        )
        if ats_outlier.any():
            df.loc[ats_outlier, "decision"] = "review"
            if "confidence" in df.columns:
                df.loc[ats_outlier, "confidence"] = 0.3
            _note(df, ats_outlier,
                  "[outlier: > 100B ATS (millions) — likely chapter total, not single R&D line]")

    return df
