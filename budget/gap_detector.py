"""
Gap detector and temporal smoother for budget.

After the canonical series is built, some (agency, year) cells are missing.
This module:

  1. DETECT gaps   — find which (country, agency, year) are missing from the series
  2. SEARCH raw    — check if the raw_rows.csv already has a match that was
                     not classified correctly (zero cost fix)
  3. FLAG for re-extraction — if not in raw_rows, flag the source documents
                     for that year so the user can re-run targeted extraction
  4. TEMPORAL CHECK — flag years where an agency's value is implausible given
                     neighbours (IQR-based), suggesting a unit error or wrong row

Output files:
  gap_report.csv       — one row per (country, agency, year) with gap diagnosis
  reextract_queue.csv  — list of (country, year, source_file) to re-extract

Usage:
  python -m budget.gap_detector --country Australia
"""

from __future__ import annotations

import gzip
import logging
import re
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from budget import config as cfg
from budget.canonical_series import (
    _BELGIUM_VERIFIED_DROPS,
    _COSTA_RICA_VERIFIED_DROPS,
    CANONICAL_AGENCIES,
    _get_agencies_for_country,
)

logger = logging.getLogger(__name__)

__all__ = ["detect_gaps", "flag_temporal_outliers", "build_gap_report"]

GAP_REPORT_CSV = cfg.OUTPUT_DIR / "gap_report.csv"
REEXTRACT_QUEUE_CSV = cfg.OUTPUT_DIR / "reextract_queue.csv"
_COLOMBIA_FULL_TEXT_DIR = cfg.OUTPUT_DIR / "full_text" / "Colombia"
_HUNGARY_FULL_TEXT_DIR = cfg.OUTPUT_DIR / "full_text" / "Hungary"

_VERIFIED_TEMPORAL_OUTLIERS = {
    ("Costa Rica", "CATIE (Centro Agronómico Tropical de Investigación y Enseñanza)", 2011),
    ("Costa Rica", "UCR (Universidad de Costa Rica)", 2014),
    ("Estonia", "Archimedes Foundation", 2009),
    ("Chile", "Fisheries Research Fund", 2014),
    ("Chile", "Fisheries Research Fund", 2016),
    ("Chile", "Technological Consortiums - CORFO", 2021),
    ("Costa Rica", "UCR (Universidad de Costa Rica)", 2025),
    # Latvia audited directly in the original budget files:
    # 2006 likumi_lv_121006_09.11.2006__lv.pdf page 313 explicitly lists
    # "Total Science Base Funding" = 1,000,000 LVL. This is a real programme
    # discontinuity / redesign, not a unit error.
    ("Latvia", "Science Base Funding (Latvia)", 2006),
    # Verified directly in the original Iceland budget files:
    # 1976 pdf_06a77b1e920d__1976_0292.txt.gz -> 232 Rannsóknaráð ríkisins,
    # "Gjöld samtals" = 691.265 thousand ISK.
    ("Iceland", "Rannsóknaráð ríkisins (National Research Council)", 1976),
    # 1977 pdf_fb1513389f08__1977_0282.txt.gz -> explicit "Total for
    # Rannsóknasjóður" = 556.226 thousand ISK.
    ("Iceland", "Rannsóknasjóður (Research Fund)", 1977),
    # Verified directly in the original Iceland budget files:
    # 1998 pdf_b7ffe7ae530e__1998_0706.txt.gz -> Vísindaráð includes explicit
    # research lines for basic natural-science and humanities research.
    ("Iceland", "Vísindaráð (Science Council)", 1998),
    # 2001 pdf_6078fab6c4c8__2001_0489.txt.gz -> Vísindaráð includes explicit
    # "Frumrannsóknir í raunvísindum" research line at 201.5 m.kr.
    ("Iceland", "Vísindaráð (Science Council)", 2001),
    # 2015 pdf_3eddd998eb96__2015_0801.txt.gz -> "04-415 Sjóður til
    # síldarrannsókna" lists 15,0 m.kr. directly in the original budget file.
    ("Iceland", "Sjóður til síldarrannsókna (Fund for Herring Research)", 2015),
    # Verified directly in the original Iceland budget files:
    # 1981 pdf_7e3e5aebe8ca__1981_0382.txt.gz -> "276 Byggingarsjóður
    # rannsókna í þágu atvinnuveganna" with "Gjöld samtals" = 466.700.
    ("Iceland", "Byggingarsjóður rannsókna í þágu atvinnuveganna (Building Fund for Industry Research)", 1981),
    # 2006 pdf_b638da4bdd5e__2006_0540.txt.gz -> 1.01 Orkustofnun = 660,6
    # within the Byggingarsjóður block in the original budget file.
    ("Iceland", "Byggingarsjóður rannsókna í þágu atvinnuveganna (Building Fund for Industry Research)", 2006),
    # Verified directly in the original Iceland budget files:
    # 2011 pdf_849f7b589fe1__2011_0556.txt.gz -> 02-201 Háskóli Íslands 12.740,1
    # 2016 pdf_9921de542109__2016_0703.txt.gz -> 02-201 Háskóli Íslands 18.129,5
    # 2019 pdf_b983142dcb94__2019_s0632-f_I.txt.gz -> University of Iceland 22.445,3
    # 2020 pdf_9f939d0d30b5__2020_s0561-f_I.txt.gz -> 02-201 Háskóli Íslands 22.020,8
    # 2021 pdf_23be52953bcc__2021_s0726-f_I.txt.gz -> University of Iceland 26.224,0
    # 2022 pdf_181fe6f2e332__2022_s0286-f_I.txt.gz -> University of Iceland 29.792,0
    # 2023 pdf_6a299423e689__2023_s0881-f_I.txt.gz -> 17-201 Háskóli Íslands 32.529,0
    # 2024 pdf_12a028d2507b__2024_s0854-f_I.txt.gz -> 17-201 Háskóli Íslands 32.407,3
    # 2025 pdf_ae7f5d8bbf89__2025_s0411-f_I.txt.gz -> 17-201 Háskóli Íslands 39.340,2
    ("Iceland", "Háskóli Íslands (University of Iceland)", 2011),
    ("Iceland", "Háskóli Íslands (University of Iceland)", 2016),
    ("Iceland", "Háskóli Íslands (University of Iceland)", 2019),
    ("Iceland", "Háskóli Íslands (University of Iceland)", 2020),
    ("Iceland", "Háskóli Íslands (University of Iceland)", 2021),
    ("Iceland", "Háskóli Íslands (University of Iceland)", 2022),
    ("Iceland", "Háskóli Íslands (University of Iceland)", 2023),
    ("Iceland", "Háskóli Íslands (University of Iceland)", 2024),
    ("Iceland", "Háskóli Íslands (University of Iceland)", 2025),
    # Israel audited directly in the original budget files:
    # 1975 page 16 shows National Council total in ILP-era budget units.
    # 1985 page 167 explicitly reports the National Council total in millions of
    # old shekels. Both are real values, but they sit in different currency
    # regimes from the later ILS series and should not be flagged as compile bugs.
    ("Israel", "National Council for R&D (Israel, pre-1992)", 1975),
    ("Israel", "National Council for R&D (Israel, pre-1992)", 1980),
    ("Israel", "National Council for R&D (Israel, pre-1992)", 1981),
    ("Israel", "National Council for R&D (Israel, pre-1992)", 1982),
    ("Israel", "National Council for R&D (Israel, pre-1992)", 1983),
    ("Israel", "National Council for R&D (Israel, pre-1992)", 1984),
    ("Israel", "National Council for R&D (Israel, pre-1992)", 1985),
}
_SKIP_EXPECTED_YEARS = {
    "Belgium": {(canonical_name, year) for canonical_name, year in _BELGIUM_VERIFIED_DROPS},
    "Costa Rica": {(canonical_name, year) for year, canonical_name in _COSTA_RICA_VERIFIED_DROPS},
}

_COSTA_RICA_IGNORE_RAW_RECLASSIFY = {
    (2021, "INCIENSA (health and nutrition research)"),
    (2022, "INTA (Instituto Nacional de Innovación y Transferencia en Tecnología Agropecuaria)"),
    (2010, "ITCR / TEC (Instituto Tecnológico de Costa Rica)"),
    (2013, "ITCR / TEC (Instituto Tecnológico de Costa Rica)"),
    (2010, "MICITT (Ministerio de Ciencia, Innovación, Tecnología y Telecomunicaciones)"),
    (2020, "MICITT (Ministerio de Ciencia, Innovación, Tecnología y Telecomunicaciones)"),
    (2023, "MICITT (Ministerio de Ciencia, Innovación, Tecnología y Telecomunicaciones)"),
    (2021, "Promotora Costarricense de Innovación e Investigación (PCII)"),
}

_BELGIUM_LATE_BELSPO_YEARS = {2008, 2022, 2023, 2024}

GAP_REPORT_COLUMNS = [
    "country", "year", "canonical_name", "category",
    "gap_type",         # "missing" | "outlier" | "ok"
    "diagnosis",        # human-readable explanation
    "raw_row_match",    # entity found in raw_rows.csv?  "yes" | "no" | "partial"
    "raw_row_amount",   # amount found in raw_rows if match exists
    "raw_row_file",     # source file of the raw row match
    "action",           # "reclassify" | "reextract" | "verify" | "none"
    "series_amount",    # current series value (None if gap)
    "prev_amount",      # previous year value (for context)
    "next_amount",      # next year value (for context)
]


def _belgium_gap_diagnosis_from_results(
    country: str,
    year: int,
    canonical: str,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Provide a more specific Belgium diagnosis than raw_rows coverage can offer.

    Belgium's raw_rows currently come from the older text-cache path and do not
    cover the later pipeline-only years consistently. For the remaining BELSPO
    gaps we can still inspect the per-country results file and distinguish
    between:
      - no extracted science-policy rows at all
      - science-policy rows present but all `no_amount` / narrative
      - likely wrong page or source-file selection
    """
    if country != "Belgium":
        return None, None, None
    if canonical != "BELSPO / Belgian Federal Science Policy":
        return None, None, None
    if year not in _BELGIUM_LATE_BELSPO_YEARS:
        return None, None, None

    results_path = cfg.OUTPUT_DIR / country / f"{country.lower()}_docx_results.csv"
    if not results_path.exists():
        return None, None, None

    try:
        results_df = pd.read_csv(results_path)
    except Exception:
        return None, None, None

    year_df = results_df[pd.to_numeric(results_df.get("year"), errors="coerce").eq(year)].copy()
    if year_df.empty:
        return (
            "No Belgium pipeline rows for this year; likely missing extraction output for the target science-policy file.",
            "reextract",
            None,
        )

    source_file = None
    files = year_df.get("source_file")
    if files is not None:
        file_values = [str(v).strip() for v in files.dropna().tolist() if str(v).strip()]
        if file_values:
            source_file = file_values[0]

    science_mask = (
        year_df.get("section_name_en", pd.Series("", index=year_df.index)).fillna("").astype(str).str.contains(
            r"science policy|federal scientific policy|federal science policy",
            case=False,
            regex=True,
        )
        | year_df.get("section_name", pd.Series("", index=year_df.index)).fillna("").astype(str).str.contains(
            r"wetenschapsbeleid|politique scientifique",
            case=False,
            regex=True,
        )
        | year_df.get("line_description_en", pd.Series("", index=year_df.index)).fillna("").astype(str).str.contains(
            r"european space agency|research and development at the international level|science policy",
            case=False,
            regex=True,
        )
        | year_df.get("line_description", pd.Series("", index=year_df.index)).fillna("").astype(str).str.contains(
            r"europees ruimtevaart|agence spatiale européenne|onderzoek en ontwikkeling op internationaal|recherche et d[ée]veloppement dans le cadre international",
            case=False,
            regex=True,
        )
    )
    science_df = year_df[science_mask].copy()

    if science_df.empty:
        return (
            "Belgium pipeline output exists for this year, but no usable BELSPO / science-policy rows were extracted. This points to wrong page/source selection or to a legal-text-only source file rather than a numeric budget annex.",
            "reextract",
            source_file,
        )

    amount_series = pd.to_numeric(science_df.get("amount_local"), errors="coerce")
    if amount_series.notna().any():
        return (
            "Science-policy rows exist with numeric amounts in pipeline output; likely reclassification / matching issue rather than extraction failure.",
            "reclassify",
            source_file,
        )

    notes = science_df.get("notes", pd.Series("", index=science_df.index)).fillna("").astype(str)
    if notes.str.contains(r"no_amount", case=False, regex=True).all():
        return (
            "Science-policy section is present in pipeline output, but the extracted BELSPO rows are all narrative / `no_amount`. The current source behaves like legal text without a recoverable annual amount table.",
            "reextract",
            source_file,
        )

    return (
        "Belgium science-policy rows were parsed for this year, but none produced a usable annual amount. Re-extract from a better annex or page selection.",
        "reextract",
        source_file,
    )


def _colombia_gap_diagnosis_from_results(
    country: str,
    year: int,
    canonical: str,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Detect Colombia years where the available source is only a legal wrapper
    (articles/capitulos) rather than a numeric annex with institutional tables.
    """
    if country != "Colombia":
        return None, None, None
    if year in {2007, 2009, 2010, 2015}:
        candidates = sorted(_COLOMBIA_FULL_TEXT_DIR.glob(f"*__{year}_*.txt.gz"))
        if candidates:
            path = candidates[0]
            try:
                with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as fh:
                    text = fh.read()
            except OSError:
                text = ""

            if year == 2015 and len(text) < 2_000:
                return (
                    "Available Colombia source for 2015 is truncated/incomplete in the text cache and does not preserve budget tables. Re-extraction from the same cached text is unlikely to recover traceable institutional amounts.",
                    "none",
                    path.stem,
                )

            transfer_only = bool(
                re.search(
                    r"servicio nacional de aprendizaje.*transferir[áa].{0,250}colciencias|"
                    r"sena.*transferir[áa].{0,250}colciencias",
                    text,
                    re.IGNORECASE | re.DOTALL,
                )
            )
            has_nearby_amount = bool(
                re.search(
                    r"colciencias.{0,120}\b\d[\d\.,]{5,}\b|\b\d[\d\.,]{5,}\b.{0,120}colciencias",
                    text,
                    re.IGNORECASE | re.DOTALL,
                )
            )
            if year in {2007, 2009, 2010} and transfer_only and not has_nearby_amount:
                return (
                    "Available Colombia source for this year contains a transfer provision from SENA to COLCIENCIAS, but no traceable institutional appropriation amount near the science entity in the cached original text. Treat as legal/programmatic mention, not a recoverable budget total from the current source.",
                    "none",
                    path.stem,
                )
    if year not in {2019, 2024, 2025}:
        return None, None, None

    results_path = cfg.OUTPUT_DIR / country / f"{country.lower()}_docx_results.csv"
    if not results_path.exists():
        return None, None, None

    try:
        results_df = pd.read_csv(results_path)
    except Exception:
        return None, None, None

    year_df = results_df[pd.to_numeric(results_df.get("year"), errors="coerce").eq(year)].copy()
    if year_df.empty:
        return None, None, None

    source_file = None
    files = year_df.get("source_file")
    if files is not None:
        file_values = [str(v).strip() for v in files.dropna().tolist() if str(v).strip()]
        if file_values:
            source_file = file_values[0]

    text = (
        year_df.get("section_name_en", pd.Series("", index=year_df.index)).fillna("").astype(str)
        + " "
        + year_df.get("section_name", pd.Series("", index=year_df.index)).fillna("").astype(str)
        + " "
        + year_df.get("line_description_en", pd.Series("", index=year_df.index)).fillna("").astype(str)
        + " "
        + year_df.get("line_description", pd.Series("", index=year_df.index)).fillna("").astype(str)
    ).str.lower()

    has_science_table = text.str.contains(
        r"3901|3902|3505|1903|3602|minist(?:erio|ry) de ciencia|"
        r"research with quality and impact|investigaci[óo]n con calidad e impacto|"
        r"instituto nacional de metrolog|instituto nacional de salud|"
        r"servicio nacional de aprendizaje",
        regex=True,
        na=False,
    ).any()
    legal_wrapper_only = text.str.contains(
        r"art[íi]culo|capitulo|disposiciones varias|fondo de estabilizaci[óo]n|"
        r"defensa de los derechos e intereses colectivos",
        regex=True,
        na=False,
    ).all()

    if not has_science_table and legal_wrapper_only:
        return (
            "Available Colombia source for this year behaves like a legal wrapper only (articles/capitulo text) rather than a numeric science-budget annex. Re-extraction from the same file is unlikely to recover institutional series.",
            "none",
            source_file,
        )

    return None, None, source_file


def _hungary_gap_diagnosis_from_full_text(
    country: str,
    year: int,
    canonical: str,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Diagnose Hungary MTA gaps by checking whether the original cached text
    contains the chapter heading at all, versus containing it but failing to
    expose a recoverable annual total to the parser.
    """
    if country != "Hungary":
        return None, None, None
    if canonical != "Hungarian Academy of Sciences (MTA)":
        return None, None, None

    candidates = sorted(_HUNGARY_FULL_TEXT_DIR.glob(f"*__{year}_*.txt.gz"))
    if not candidates:
        return (
            "No Hungary full-text cache file found for this year, so the loss happens before parser-level canonical matching.",
            "reextract",
            None,
        )

    path = candidates[0]
    try:
        with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as fh:
            text = fh.read()
    except OSError:
        return (
            "Hungary full-text cache exists but could not be read; re-extract before any parser-level recovery.",
            "reextract",
            path.stem,
        )

    has_mta_heading = bool(
        re.search(r"magyar tudom[aá]nyos akad", text, re.IGNORECASE)
        or re.search(r"\bMTA\b", text)
    )
    has_chapter_heading = bool(re.search(r"XXXIII\.\s+MAGYAR\s+TUDOM", text, re.IGNORECASE))

    if not has_mta_heading and not has_chapter_heading:
        return (
            "Original Hungary cached text for this year does not contain a detectable MTA / 'Magyar Tudományos Akadémia' chapter heading. The gap originates in the source text layer or pre-parser extraction, not in canonical matching.",
            "reextract",
            path.stem,
        )

    return (
        "Original Hungary cached text contains the MTA chapter heading, but no MTA row reaches raw_rows. The loss occurs inside text-cache parsing: the multi-column chapter total is fragmented/truncated in the PDF text layer, so the parser cannot recover a defendable numeric annual total.",
        "reextract",
        path.stem,
    )


# ---------------------------------------------------------------------------
# Step 1 — Detect gaps in canonical series
# ---------------------------------------------------------------------------

def detect_gaps(
    series_df: pd.DataFrame,
    country: str,
) -> pd.DataFrame:
    """
    For each canonical agency and each year where data exists for OTHER agencies,
    flag years where this agency has no value (gap) vs years where it has a value (ok).

    Returns DataFrame with one row per (agency, year) in the expected range.
    """
    country_series = series_df[series_df["country"] == country].copy()
    if country_series.empty:
        logger.warning(f"No series data for {country}")
        return pd.DataFrame()

    agencies = _get_agencies_for_country(country)
    if not agencies:
        logger.warning(f"No canonical agencies defined for {country}")
        return pd.DataFrame()

    # Respect the canonicals that survived series construction. Some country-
    # specific compile rules intentionally drop low-signal hardcoded agencies or
    # clip them to observed years to avoid manufacturing decades of fake gaps.
    present_canonicals = set(country_series["canonical_name"].dropna().astype(str))
    agencies = [a for a in agencies if a["canonical_name"] in present_canonicals]
    if not agencies:
        logger.warning(f"No active canonical agencies present in series for {country}")
        return pd.DataFrame()

    # Get all years present in the series for this country
    all_years = sorted(country_series["year"].unique())

    records = []
    skip_expected = _SKIP_EXPECTED_YEARS.get(country, set())
    for agency in agencies:
        canonical_name = agency["canonical_name"]
        active_start, active_end = agency.get("active_years", (1800, 2099))

        # Aggregate to one value per year (sum across Acts) for gap detection.
        # The detail series may have multiple rows per year (one per source file).
        agency_by_year = (
            country_series[country_series["canonical_name"] == canonical_name]
            .groupby("year")["amount_local"]
            .sum(min_count=1)
        )

        def _get_year_amount(y):
            val = agency_by_year.get(y)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return None
            return float(val)

        explicit_years = agency.get("expected_years")
        if explicit_years:
            active_years = [int(y) for y in explicit_years if int(y) in all_years]
        else:
            active_years = [y for y in all_years if active_start <= y <= active_end]

        for year in active_years:
            if (canonical_name, year) in skip_expected:
                continue
            amount = _get_year_amount(year)
            has_value = amount is not None

            # Get neighbours for context
            prev_years = [y for y in active_years if y < year and _get_year_amount(y) is not None]
            next_years = [y for y in active_years if y > year and _get_year_amount(y) is not None]
            prev_amount = _get_year_amount(prev_years[-1]) if prev_years else None
            next_amount = _get_year_amount(next_years[0]) if next_years else None

            if not has_value:
                records.append({
                    "country": country,
                    "year": year,
                    "canonical_name": canonical_name,
                    "category": agency["category"],
                    "gap_type": "missing",
                    "diagnosis": "No extracted row found for this agency-year",
                    "raw_row_match": None,
                    "raw_row_amount": None,
                    "raw_row_file": None,
                    "action": "reextract",
                    "series_amount": None,
                    "prev_amount": prev_amount,
                    "next_amount": next_amount,
                })
            else:
                records.append({
                    "country": country,
                    "year": year,
                    "canonical_name": canonical_name,
                    "category": agency["category"],
                    "gap_type": "ok",
                    "diagnosis": "",
                    "raw_row_match": None,
                    "raw_row_amount": None,
                    "raw_row_file": None,
                    "action": "none",
                    "series_amount": amount,
                    "prev_amount": prev_amount,
                    "next_amount": next_amount,
                })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Step 2 — Search raw_rows for matches to fill gaps
# ---------------------------------------------------------------------------

def search_raw_rows_for_gaps(
    gap_df: pd.DataFrame,
    raw_rows_csv: Path,
    country: str,
) -> pd.DataFrame:
    """
    For each 'missing' row in gap_df, search raw_rows.csv for a match.

    Match logic: for each agency's name_variants, check if any raw row
    entity_raw contains that variant (case-insensitive).

    Updates gap_df with raw_row_match, raw_row_amount, raw_row_file, action.
    """
    if not raw_rows_csv.exists():
        logger.warning(f"raw_rows.csv not found: {raw_rows_csv}")
        return gap_df

    raw_df = pd.read_csv(raw_rows_csv)
    raw_country = raw_df[raw_df["country"] == country].copy()
    raw_country["amount_current"] = pd.to_numeric(raw_country["amount_current"], errors="coerce")

    agencies = {a["canonical_name"]: a for a in _get_agencies_for_country(country)}
    gap_df = gap_df.copy()

    def _raw_row_matches_variant(entity_text: str, section_text: str, variant: str) -> bool:
        v = str(variant or "").strip().lower()
        if not v:
            return False
        combined = f"{str(entity_text or '').lower()} {str(section_text or '').lower()}"
        if len(v) <= 4:
            return bool(re.search(r"(?<![a-z])" + re.escape(v) + r"(?![a-z])", combined))
        return v in combined

    for idx, gap_row in gap_df[gap_df["gap_type"] == "missing"].iterrows():
        year = gap_row["year"]
        canonical = gap_row["canonical_name"]
        agency = agencies.get(canonical)
        if not agency:
            continue

        year_raw = raw_country[
            raw_country["year"] == year
        ].dropna(subset=["amount_current"])

        if year_raw.empty:
            specific_diag, specific_action, specific_file = _belgium_gap_diagnosis_from_results(
                country=country,
                year=int(year),
                canonical=canonical,
            )
            if not specific_diag:
                specific_diag, specific_action, specific_file = _colombia_gap_diagnosis_from_results(
                    country=country,
                    year=int(year),
                    canonical=canonical,
                )
            if not specific_diag:
                specific_diag, specific_action, specific_file = _hungary_gap_diagnosis_from_full_text(
                    country=country,
                    year=int(year),
                    canonical=canonical,
                )
            gap_df.at[idx, "raw_row_match"] = "no"
            gap_df.at[idx, "diagnosis"] = (
                specific_diag or "Year not in raw_rows — documents may not be parsed yet"
            )
            gap_df.at[idx, "action"] = specific_action or "reextract"
            if specific_file:
                gap_df.at[idx, "raw_row_file"] = specific_file
            continue

        # Try matching name variants
        matched = None
        for variant in agency["name_variants"]:
            matches = year_raw[
                year_raw.apply(
                    lambda r: _raw_row_matches_variant(
                        r.get("entity_raw", ""),
                        r.get("section_name", ""),
                        variant,
                    ),
                    axis=1,
                )
            ]
            if not matches.empty:
                # Take the row with the largest amount
                matched = matches.loc[matches["amount_current"].idxmax()]
                break

        if matched is not None:
            if country == "Costa Rica" and (int(year), canonical) in _COSTA_RICA_IGNORE_RAW_RECLASSIFY:
                gap_df.at[idx, "raw_row_match"] = "no"
                gap_df.at[idx, "diagnosis"] = (
                    "Raw rows contain only noisy salary / generic classification / non-institutional transfer hits for this agency-year; no defendable institutional amount found in the original source."
                )
                gap_df.at[idx, "action"] = "reextract"
                continue
            gap_df.at[idx, "raw_row_match"] = "yes"
            gap_df.at[idx, "raw_row_amount"] = float(matched["amount_current"])
            gap_df.at[idx, "raw_row_file"] = str(matched.get("source_file", ""))
            gap_df.at[idx, "diagnosis"] = (
                f"Found in raw_rows: '{matched['entity_raw'][:50]}' = "
                f"{float(matched['amount_current']):,.0f} in {matched.get('source_file','?')}"
            )
            gap_df.at[idx, "action"] = "reclassify"
        else:
            # Check if the year's documents exist at all
            year_files = year_raw["source_file"].unique()
            specific_diag, specific_action, specific_file = _colombia_gap_diagnosis_from_results(
                country=country,
                year=int(year),
                canonical=canonical,
            )
            if not specific_diag:
                specific_diag, specific_action, specific_file = _hungary_gap_diagnosis_from_full_text(
                    country=country,
                    year=int(year),
                    canonical=canonical,
                )
            if specific_diag:
                gap_df.at[idx, "raw_row_match"] = "no"
                gap_df.at[idx, "diagnosis"] = specific_diag
                gap_df.at[idx, "action"] = specific_action or "none"
                if specific_file:
                    gap_df.at[idx, "raw_row_file"] = specific_file
                continue
            if len(year_files) > 0:
                gap_df.at[idx, "raw_row_match"] = "no"
                gap_df.at[idx, "diagnosis"] = (
                    f"Year has {len(year_files)} parsed files but agency not found. "
                    f"May be in a non-table section or different document."
                )
                gap_df.at[idx, "action"] = "reextract"
            else:
                gap_df.at[idx, "raw_row_match"] = "no"
                gap_df.at[idx, "diagnosis"] = "No parsed documents for this year"
                gap_df.at[idx, "action"] = "reextract"

    return gap_df


# ---------------------------------------------------------------------------
# Step 3 — Temporal outlier detection
# ---------------------------------------------------------------------------

def flag_temporal_outliers(
    gap_df: pd.DataFrame,
    iqr_multiplier: float = 3.0,
    min_years: int = 5,
) -> pd.DataFrame:
    """
    For rows with gap_type='ok', check if the amount is implausible given
    the time series for that agency. Uses IQR-based outlier detection.

    Flags rows as 'outlier' with a diagnosis explaining the range.
    These are candidates for verify pass (check current vs prior year confusion).
    """
    gap_df = gap_df.copy()

    for canonical, agency_df in gap_df[gap_df["gap_type"] == "ok"].groupby("canonical_name"):
        amounts = agency_df["series_amount"].dropna()
        if len(amounts) < min_years:
            continue

        q1, q3 = amounts.quantile(0.25), amounts.quantile(0.75)
        iqr = q3 - q1
        lo = q1 - iqr_multiplier * iqr
        hi = q3 + iqr_multiplier * iqr

        for idx, row in agency_df.iterrows():
            amt = row["series_amount"]
            if amt is None or pd.isna(amt):
                continue
            if (row.get("country"), canonical, int(row.get("year"))) in _VERIFIED_TEMPORAL_OUTLIERS:
                continue
            if amt < lo or amt > hi:
                gap_df.at[idx, "gap_type"] = "outlier"
                gap_df.at[idx, "diagnosis"] = (
                    f"Amount {amt:,.0f} outside expected range "
                    f"[{lo:,.0f} – {hi:,.0f}] (IQR × {iqr_multiplier}). "
                    f"Possible current/prior year confusion or unit error."
                )
                gap_df.at[idx, "action"] = "verify"

    return gap_df


# ---------------------------------------------------------------------------
# Step 4 — Build reextract queue
# ---------------------------------------------------------------------------

def build_reextract_queue(
    gap_df: pd.DataFrame,
    pdf_root: Path = cfg.PDF_ROOT,
) -> pd.DataFrame:
    """
    For gaps marked action='reextract', find the source documents for that
    (country, year) and add them to the reextract queue.
    """
    reextract = gap_df[gap_df["action"] == "reextract"][
        ["country", "year", "canonical_name"]
    ].drop_duplicates(subset=["country", "year"])

    queue_rows = []
    for _, row in reextract.iterrows():
        country = row["country"]
        year = int(row["year"])
        country_dir = pdf_root / country

        if not country_dir.exists():
            continue

        # Find all files for this year
        _YEAR_PAT = re.compile(r"(?<![0-9])(1[89]\d{2}|20[012]\d)(?![0-9])")
        for path in sorted(country_dir.iterdir()):
            m = _YEAR_PAT.search(path.stem)
            if m and int(m.group(1)) == year:
                queue_rows.append({
                    "country": country,
                    "year": year,
                    "source_file": path.name,
                    "file_path": str(path),
                    "missing_agencies": row.get("canonical_name", ""),
                })

    return pd.DataFrame(queue_rows) if queue_rows else pd.DataFrame()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def build_gap_report(
    series_df: pd.DataFrame,
    country: str,
    raw_rows_csv: Path = cfg.OUTPUT_DIR / "raw_rows.csv",
    output_dir: Path = cfg.OUTPUT_DIR,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Full gap detection pipeline for one country.

    Returns (gap_report_df, reextract_queue_df).
    Both are also written to CSV.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: find gaps
    logger.info(f"Gap detection: {country}")
    gap_df = detect_gaps(series_df, country)

    if gap_df.empty:
        logger.warning(f"No gap data for {country}")
        return pd.DataFrame(), pd.DataFrame()

    # Step 2: search raw_rows for existing matches
    gap_df = search_raw_rows_for_gaps(gap_df, raw_rows_csv, country)

    # Step 3: flag temporal outliers
    gap_df = flag_temporal_outliers(gap_df)

    # Step 4: build reextract queue
    queue_df = build_reextract_queue(gap_df)

    # Summary
    n_gaps = len(gap_df[gap_df["gap_type"] == "missing"])
    n_reclassify = len(gap_df[gap_df["action"] == "reclassify"])
    n_reextract = len(gap_df[gap_df["action"] == "reextract"])
    n_outliers = len(gap_df[gap_df["gap_type"] == "outlier"])
    n_ok = len(gap_df[gap_df["gap_type"] == "ok"])

    logger.info(
        f"[{country}] Gap report: {n_ok} ok, {n_gaps} missing "
        f"({n_reclassify} can reclassify, {n_reextract} need reextract), "
        f"{n_outliers} outliers"
    )

    # Save
    gap_path = output_dir / f"{country.lower().replace(' ','_')}_gap_report.csv"
    queue_path = output_dir / f"{country.lower().replace(' ','_')}_reextract_queue.csv"

    gap_df.to_csv(gap_path, index=False)
    if not queue_df.empty:
        queue_df.to_csv(queue_path, index=False)

    logger.info(f"Gap report → {gap_path}")
    if not queue_df.empty:
        logger.info(f"Reextract queue → {queue_path} ({len(queue_df)} files)")

    return gap_df, queue_df


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import yaml

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")

    parser = argparse.ArgumentParser(description="Detect gaps in canonical R&D series")
    parser.add_argument("--country", required=True)
    parser.add_argument("--series", help="Path to canonical series CSV")
    parser.add_argument("--raw-rows", default=str(cfg.OUTPUT_DIR / "raw_rows.csv"))
    args = parser.parse_args()

    series_path = args.series or str(
        cfg.OUTPUT_DIR / f"{args.country.lower().replace(' ','_')}_docx_series.csv"
    )

    series_df = pd.read_csv(series_path)
    gap_df, queue_df = build_gap_report(
        series_df=series_df,
        country=args.country,
        raw_rows_csv=Path(args.raw_rows),
    )

    print(f"\n=== Gap report for {args.country} ===")
    if not gap_df.empty:
        print(gap_df[gap_df["gap_type"] != "ok"][
            ["year", "canonical_name", "gap_type", "action", "diagnosis"]
        ].to_string())

    if not queue_df.empty:
        print(f"\n=== Files to re-extract ({len(queue_df)}) ===")
        print(queue_df[["year", "source_file", "missing_agencies"]].to_string())
