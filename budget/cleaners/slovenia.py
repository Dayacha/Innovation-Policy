"""
Slovenia-specific post-extraction cleaner.

Documents: Proračun Republike Slovenije — Uradni list Republike Slovenije, 1991–2025.

SOURCE GUIDE:
  - 1991–1994: scanned image PDFs — expect zero or minimal text.
  - 1995–2006: SIT (Slovenian tolar), amounts in thousands of SIT ('v 000 tolarjih' / 'v tisoč tolarjih').
               unit='thousand', currency='SIT'.
  - 2007+:     EUR, full euros ('v EUR'). unit='unit', currency='EUR'.

MULTI-FILE STRUCTURE:
  - Main 'u{year}XXX.pdf' files: general budget overview and ministry summary tables.
  - Companion 'RS_-YYYY-NNN-...P001/P002/P003.pdf' files: project-level detailed tables.
  - Some files are ZIPRS (budget execution laws) or other laws — these lack appropriation tables.
  - Biennial budgets: 2014+2015, 2016+2017, 2018+2019 covered in single documents.

KEY R&D STRUCTURE:
  - Policy area 05: ZNANOST IN TEHNOLOŠKI RAZVOJ / ZNANOST IN INFORMACIJSKA DRUŽBA
  - Programme 0502: Znanstveno raziskovalna dejavnost (PRIMARY R&D code)
  - Programme 0503: Mladi raziskovalci / Človeški viri v podporo znanosti
  - Programme 0504: Tehnološki razvoj
  - ARRS (Agencija za raziskovalno dejavnost RS, from 2004): main R&D funder
  - Ministry codes: 3211 (MVZT 2004–2012), 3330 (MIZŠ 2012+)
  - SAZU (3911): Academy of Sciences

KNOWN RISKS:
  - Debt service: servisiranje javnega dolga (account C section) — always non-R&D
  - Social/pension transfers: pokojnine, socialna varnost
  - Defence without R&D signal: obramba, vojska
  - Primary/pre-school education: osnovna šola, vrtci
  - Transport infrastructure without R&D signal: ceste, železnice
  - ZIPRS execution law pages (legal text without appropriation tables)
  - Standalone YYYY.pdf files containing other laws (RTV, amendments)
"""

from __future__ import annotations

import re

import pandas as pd

__all__ = ["clean"]

_RESEARCH_RE = re.compile(
    r"raziskov|razvojn|inovaci|tehnologi|znanje|veda|vednost|"
    r"arrs\b|agencija.za.raziskovalno|sazu\b|slovenska.akademija|"
    r"0502|05020|0503|05030|mladi.raziskovalci|"
    r"programi.in.projekti|ciljni.raziskovalni",
    re.IGNORECASE,
)

_DEBT_RE = re.compile(
    r"servisiranje.javnega.dolga|odplačevanje.dolga|obveznosti.iz.naslova.dolga|"
    r"financiranje.izvrševanja.proračuna|obveznice|zadolžitev|"
    r"22011[0-9]{3}|2201\s",
    re.IGNORECASE,
)

_SOCIAL_RE = re.compile(
    r"pokojnine|socialna.varnost|nadomestila.za.brezposelnost|"
    r"socialni.transferji|varstvo.otrok|otroški.dodatek|"
    r"starševsko.nadomestilo|socialna.pomoč",
    re.IGNORECASE,
)

_DEFENCE_RE = re.compile(
    r"ministrstvo.za.obrambo|obramba\b|vojska|vojaški|"
    r"varstvo.pred.naravnimi|civilna.zaščita(?!.*raziskov)",
    re.IGNORECASE,
)

_INFRA_RE = re.compile(
    r"\bceste\b|avtoceste|železnice|pristanišča|letališča|"
    r"prometna.infrastruktura|vodni.promet",
    re.IGNORECASE,
)

_SCHOOL_RE = re.compile(
    r"osnovna.šola|osnovna.sola|vrtci|predšolska|predsolska|"
    r"osnovnošolsko.izobraževanje",
    re.IGNORECASE,
)

_MACRO_TOTAL_RE = re.compile(
    r"skupaj.prihodki|skupaj.odhodki|bilanca.prihodkov.in.odhodkov|"
    r"splošni.del.proračuna|račun.financiranja|"
    r"i\.\s*skupaj.prihodki|i\s*skupaj",
    re.IGNORECASE,
)

_ZIPRS_RE = re.compile(
    r"zakon.o.izvrševanju.proračuna|ziprs|ziprs\d{4}|"
    r"o.razglasitvi.zakona.o.izvrševanju",
    re.IGNORECASE,
)

_SIT_HINT_RE = re.compile(
    r"v\s+000\s+tolarjih|v\s+tisoč\s+tolarjih|v\s+tis\.\s+sit|"
    r"v\s+1000\s+sit",
    re.IGNORECASE,
)

_EUR_HINT_RE = re.compile(
    r"\bv\s+eur\b|\bv\s+€\b",
    re.IGNORECASE,
)


def _note(df: pd.DataFrame, mask: pd.Series, text: str) -> None:
    df.loc[mask, "cleaning_notes"] = df.loc[mask, "cleaning_notes"].fillna("") + text


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Slovenia-specific post-extraction corrections."""
    df = df.copy()

    if "cleaning_notes" not in df.columns:
        df["cleaning_notes"] = ""
    if "aggregation_role" not in df.columns:
        df["aggregation_role"] = ""

    desc_col = "line_description_en" if "line_description_en" in df.columns else "line_description"
    raw_col = "line_description" if "line_description" in df.columns else desc_col
    descs = df[desc_col].fillna("").astype(str)
    raw_descs = df[raw_col].fillna("").astype(str)
    sections = df.get("section_name", pd.Series("", index=df.index)).fillna("").astype(str)
    sections_en = df.get("section_name_en", pd.Series("", index=df.index)).fillna("").astype(str)
    source_files = df.get("source_file", pd.Series("", index=df.index)).fillna("").astype(str)
    combined = descs + " " + raw_descs + " " + sections + " " + sections_en

    year_num = pd.to_numeric(df.get("year", pd.Series(dtype=float)), errors="coerce")
    has_research = combined.str.contains(_RESEARCH_RE, regex=True)

    # Misfiled source: this file is actually the 2012 closing account published
    # in December 2013, not the 2004/2005 budget law. Keep it out of Slovenia's
    # 2004-2005 compile path to avoid manufacturing fake historical rows.
    misfiled_2004_2005 = source_files.str.strip().eq("2004 2005 u2013102.pdf") & year_num.isin([2004, 2005])
    if misfiled_2004_2005.any():
        df.loc[misfiled_2004_2005, "decision"] = "exclude"
        _note(df, misfiled_2004_2005, "[slovenia_misfiled_2004_2005_source: 2012 closing-account PDF misnamed as 2004/2005 budget] ")

    # Debt service — always non-R&D
    debt_mask = combined.str.contains(_DEBT_RE, regex=True)
    if debt_mask.any():
        df.loc[debt_mask, "aggregation_role"] = "non_rd"
        df.loc[debt_mask, "decision"] = "review"
        _note(df, debt_mask, "[slovenia_debt_service: non-R&D] ")

    # Social/pension without research signal — non-R&D
    social_mask = combined.str.contains(_SOCIAL_RE, regex=True) & ~has_research
    if social_mask.any():
        df.loc[social_mask, "aggregation_role"] = "non_rd"
        df.loc[social_mask, "decision"] = "review"
        _note(df, social_mask, "[slovenia_social_transfer: non-R&D] ")

    # Defence without research signal
    defence_mask = combined.str.contains(_DEFENCE_RE, regex=True) & ~has_research
    if defence_mask.any():
        df.loc[defence_mask, "decision"] = "review"
        _note(df, defence_mask, "[slovenia_defence_non_rd: no research signal] ")

    # Infrastructure without research signal
    infra_mask = combined.str.contains(_INFRA_RE, regex=True) & ~has_research
    if infra_mask.any():
        df.loc[infra_mask, "decision"] = "review"
        _note(df, infra_mask, "[slovenia_infrastructure_non_rd] ")

    # Primary/pre-school education — non-R&D
    school_mask = combined.str.contains(_SCHOOL_RE, regex=True) & ~has_research
    if school_mask.any():
        df.loc[school_mask, "aggregation_role"] = "non_rd"
        df.loc[school_mask, "decision"] = "review"
        _note(df, school_mask, "[slovenia_primary_education_non_rd] ")

    # Macro totals and section aggregates
    macro_mask = combined.str.contains(_MACRO_TOTAL_RE, regex=True)
    if "item_type" in df.columns:
        macro_mask = macro_mask | (df["item_type"] == "section_total")
    if macro_mask.any():
        df.loc[macro_mask, "aggregation_role"] = df.loc[macro_mask, "aggregation_role"].replace("", "section")
        _note(df, macro_mask, "[slovenia_macro_total: section aggregate] ")

    # ZIPRS execution law pages — legal text without amounts
    ziprs_mask = combined.str.contains(_ZIPRS_RE, regex=True)
    amount_col = "amount_local" if "amount_local" in df.columns else None
    if amount_col:
        no_amount = df[amount_col].isna() | (pd.to_numeric(df[amount_col], errors="coerce") == 0)
        ziprs_no_amount = ziprs_mask & no_amount
        if ziprs_no_amount.any():
            df.loc[ziprs_no_amount, "decision"] = "exclude"
            _note(df, ziprs_no_amount, "[slovenia_ziprs_legal_wrapper: execution law text, no appropriation amount] ")

    # Unit/currency mismatch flags
    if "unit" in df.columns and "currency" in df.columns:
        # Pre-2007 rows should be SIT thousands
        pre_2007 = year_num < 2007
        sit_hint = combined.str.contains(_SIT_HINT_RE, regex=True)
        wrong_currency_sit = pre_2007 & sit_hint & (df["currency"].fillna("").str.upper() != "SIT")
        if wrong_currency_sit.any():
            df.loc[wrong_currency_sit, "decision"] = "review"
            _note(df, wrong_currency_sit, "[slovenia_currency_mismatch: pre-2007 text says SIT but currency is not SIT] ")

        # Post-2007 rows should be EUR
        post_2007 = year_num >= 2007
        eur_hint = combined.str.contains(_EUR_HINT_RE, regex=True)
        wrong_currency_eur = post_2007 & eur_hint & (df["currency"].fillna("").str.upper() != "EUR")
        if wrong_currency_eur.any():
            df.loc[wrong_currency_eur, "decision"] = "review"
            _note(df, wrong_currency_eur, "[slovenia_currency_mismatch: post-2007 text says EUR but currency is not EUR] ")

    # Early years (pre-1995): warn about image PDFs
    early_mask = year_num < 1995
    if early_mask.any():
        _note(df, early_mask, "[slovenia_pre1995_scanned: likely image PDF — expect minimal OCR text] ")

    # Biennial budget flag: if year is from a known biennial pair, note for validation
    biennial_years = {2014, 2015, 2016, 2017, 2018, 2019}
    biennial_mask = year_num.isin(biennial_years)
    if biennial_mask.any():
        _note(df, biennial_mask, "[slovenia_biennial_budget: year may be part of biennial budget law — verify year-specific amounts] ")

    # Sanity: EUR amounts above ~500M on a single R&D line are implausible
    if amount_col:
        post_2007_eur = year_num >= 2007
        unit_is_unit = df.get("unit", pd.Series("", index=df.index)).fillna("").str.lower() == "unit"
        implausible = post_2007_eur & unit_is_unit & (pd.to_numeric(df[amount_col], errors="coerce") > 500_000_000)
        if implausible.any():
            df.loc[implausible, "decision"] = "review"
            _note(df, implausible, "[slovenia_implausible_eur_amount: >500M EUR on single R&D line, likely section total] ")

        # Pre-2007 SIT: amounts above ~100B SIT on a single R&D line are implausible (thousands unit)
        pre_2007_sit = year_num < 2007
        unit_is_thousand = df.get("unit", pd.Series("", index=df.index)).fillna("").str.lower() == "thousand"
        implausible_sit = pre_2007_sit & unit_is_thousand & (pd.to_numeric(df[amount_col], errors="coerce") > 100_000_000)
        if implausible_sit.any():
            df.loc[implausible_sit, "decision"] = "review"
            _note(df, implausible_sit, "[slovenia_implausible_sit_amount: >100B SIT thousands on single line, likely section total] ")

    return df.reset_index(drop=True)
