"""
Compile phase (Phase 2) for budget — deterministic after Phase 1.

Takes raw rows from docx_table_parser + agency_registry and produces:
  - raw_rows.csv          : every extracted row (full audit trail)
  - <country>_results.csv : classified rows (include/review)
  - <country>_series.csv  : one row per (country, agency, year)
  - <country>_gap_report.csv : gaps + outliers + reextract queue

This phase is almost free to rerun. The only LLM calls are:
  - entity_dedup: ~$0.001 per country-year (Haiku, cached)
  - agency_classifier: ~$0.0001 per unique entity name (Haiku, cached)

Pipeline:
  parse_to_raw_rows()       → docx_table_parser (no LLM)
  deterministic_dedup()     → code only
  apply_entity_dedup()      → LLM Haiku, cached per year
  classify_raw_rows()       → agency_classifier, cached per name
  build_classified_results()→ code only
  build_canonical_series()  → code only (canonical_series.py)
  build_gap_report()        → gap_detector.py

Usage:
  python -m budget.compile --country Australia
  python -m budget.compile --country Australia --years 2020-2026
  python -m budget.compile --country Australia --no-entity-dedup  # skip LLM dedup
"""

from __future__ import annotations

import csv
import gzip
import logging
import re
from pathlib import Path
from typing import Optional

import pandas as pd

from budget import config as cfg
from budget.docx_table_parser import parse_country_docx_files, RawRow
from budget.agency_classifier import (
    load_registry, save_registry, classify_agencies, REGISTRY_FILE
)
from budget.canonical_series import build_canonical_series, build_totals_series
from budget.entity_dedup import apply_entity_dedup
from budget.gap_detector import build_gap_report
from budget.agency_discovery import discover_agencies
from budget.gap_filler import fill_gaps
from budget.text_cache_parser import parse_text_cache, TEXT_CACHE_DIR

logger = logging.getLogger(__name__)

RAW_ROWS_CSV = cfg.OUTPUT_DIR / "raw_rows.csv"

_OUTPUT_UNIT_BY_CURRENCY = {
    "AUD": "dollar",
    "ATS": "schilling",
    "CAD": "dollar",
    "NZD": "dollar",
    "USD": "dollar",
    "SKK": "koruna",
    "PLN": "zloty",
    "JPY": "yen",
    "EUR": "euro",
    "DEM": "mark",
    "FRF": "franc",
    "LUF": "franc",
    "GBP": "pound",
    "DKK": "krone",
    "NOK": "krone",
    "SEK": "krona",
    "ISK": "krona",
}

_SCALE_TO_BASE_UNIT = {
    "thousand": 1_000.0,
    "thousands": 1_000.0,
    "k": 1_000.0,
    "million": 1_000_000.0,
    "millions": 1_000_000.0,
    "billion": 1_000_000_000.0,
    "milliard": 1_000_000_000.0,
}

_FRANCE_FULL_TEXT_DIR = Path("Data/output/budget/full_text/France")
_COLOMBIA_FULL_TEXT_DIR = Path("Data/output/budget/full_text/Colombia")
_FRANCE_PROGRAMME_LABELS = [
    "Recherches scientifiques et technologiques pluridisciplinaires",
    "Recherche spatiale",
    "Recherche dans les domaines de l’énergie, du développement et de la mobilité durables",
    "Recherche et enseignement supérieur en matière économique et industrielle",
    "Recherche culturelle et culture scientifique",
    "Enseignement supérieur et recherche agricoles",
    "Recherche appliquée et innovation en agriculture",
]
_FRANCE_MISSION_TOTAL_PATTERNS = [
    re.compile(r"^total budget for (?:the )?research and higher education(?: mission)?$", re.IGNORECASE),
    re.compile(r"^total pour la mission recherche et enseignement superieur$", re.IGNORECASE),
]
_FRANCE_PRE_LOLF_TOTAL_RE = re.compile(
    r"^total(?: des)? cr[ée]dits? de paiement pour\b|^total pour\b|^total for\b",
    re.IGNORECASE,
)
_COLOMBIA_PAGE_RE = re.compile(r"=== Page (\d+)\.0")
_COLOMBIA_AMOUNT_RE = re.compile(r"\d[\d\.,]{4,}")
_COLOMBIA_TARGETED_AGENCIES = [
    {
        "entity_raw": "MINISTERIO DE CIENCIA, TECNOLOGIA E INNOVACION",
        "section_name": "3901 MINISTERIO DE CIENCIA, TECNOLOGIA E INNOVACION",
        "patterns": [
            re.compile(r"MINISTERIO DE CIENCIA.{0,300}TOTAL PRESUPUESTO", re.IGNORECASE | re.DOTALL),
            re.compile(r"M[IL]N[IL]STERIO DE CIENCIA.{0,300}TOTAL PRESUPUESTO", re.IGNORECASE | re.DOTALL),
            re.compile(r"\b3901\b.{0,120}MINISTERIO DE CIENCIA", re.IGNORECASE | re.DOTALL),
            re.compile(r"\b3901\b.{0,160}M[IL]N[IL]STERIO DE CIENCIA", re.IGNORECASE | re.DOTALL),
            re.compile(r"\b3901\b.{0,160}DEPARTAMENTO ADMINISTRATIVO DE LA CIENCIA", re.IGNORECASE | re.DOTALL),
        ],
    },
    {
        "entity_raw": "INSTITUTO NACIONAL DE METROLOGIA - INM",
        "section_name": "3505 INSTITUTO NACIONAL DE METROLOGIA - INM",
        "patterns": [
            re.compile(r"INSTITUTO NACIONAL DE METROLOG[IÍ]A.{0,300}TOTAL PRESUPUESTO", re.IGNORECASE | re.DOTALL),
        ],
    },
    {
        "entity_raw": "INSTITUTO NACIONAL DE SALUD (INS)",
        "section_name": "1903 INSTITUTO NACIONAL DE SALUD (INS)",
        "patterns": [
            re.compile(r"INSTITUTO NACIONAL DE SALUD.{0,300}TOTAL PRESUPUESTO", re.IGNORECASE | re.DOTALL),
        ],
    },
]
_COLOMBIA_COMPONENT_AGENCIES = [
    {
        "entity_raw": "SERVICIO NACIONAL DE APRENDIZAJE (SENA)",
        "section_name": "3602 SERVICIO NACIONAL DE APRENDIZAJE (SENA)",
        "pattern": re.compile(r"SERVICIO NACIONAL DE APRENDIZAJE\s*\(SENA\).{0,260}", re.IGNORECASE | re.DOTALL),
        "components": 3,
        "max_amount": 3_000_000_000_000,
    },
    {
        "entity_raw": "INSTITUTO COLOMBIANO AGROPECUARIO (ICA)",
        "section_name": "1702 INSTITUTO COLOMBIANO AGROPECUARIO (ICA)",
        "pattern": re.compile(r"INSTITUTO COLOMBIANO AGROPECUARIO\s*\(ICA\).{0,220}", re.IGNORECASE | re.DOTALL),
        "components": 2,
        "max_amount": 100_000_000_000,
    },
    {
        "entity_raw": "INSTITUTO NACIONAL DE SALUD (INS)",
        "section_name": "1903 INSTITUTO NACIONAL DE SALUD (INS)",
        "pattern": re.compile(r"INSTITUTO NACIONAL DE SALUD\s*\(INS\).{0,220}", re.IGNORECASE | re.DOTALL),
        "components": 2,
        "max_amount": 20_000_000_000,
    },
    {
        "entity_raw": "INSTITUTO NACIONAL DE METROLOGIA - INM",
        "section_name": "3505 INSTITUTO NACIONAL DE METROLOGIA - INM",
        "pattern": re.compile(r"INSTITUTO NACIONAL DE METROLOG[IÍ]A(?:\s*[-–]\s*INM|\s*\(INM\))?.{0,180}", re.IGNORECASE | re.DOTALL),
        "components": 2,
        "max_amount": 10_000_000_000,
    },
    {
        "entity_raw": "IDEAM",
        "section_name": "3201/3204 IDEAM",
        "pattern": re.compile(r"IDEAM\)?.{0,220}", re.IGNORECASE | re.DOTALL),
        "components": 2,
        "max_amount": 20_000_000_000,
    },
]


def _agency_discovery_kwargs(country: str) -> dict:
    """Country-specific guardrails for automatic agency discovery."""
    if country == "Latvia":
        # Latvia's source family mixes durable science programmes with many
        # one-off legal earmarks. Require recurrence so discovery surfaces
        # stable institutions/programmes without blocking future additions.
        return {"min_years": 2}
    if country == "Japan":
        # Japan extraction produces many broad MEXT/METI budget buckets. Require
        # recurrence and a material amount before sending candidates to the LLM.
        return {"min_years": 2, "min_avg_amount": 1_000_000}
    if country == "Germany":
        # Germany BMBF budgets produce ~400 R&D programme lines. Require 3+ years
        # of recurrence to keep only stable institutions, not one-off grants.
        return {"min_years": 3}
    if country == "Netherlands":
        # Netherlands results contain many generic budget-memorandum programme labels
        # (e.g. "Research and Development Work", "Practical Research") that span only
        # 1-2 years. Require 3+ years of recurrence AND a meaningful average amount
        # before adding to the discovered list; this keeps only durable institutions.
        return {"min_years": 3, "min_avg_amount": 50_000_000}
    if country == "Chile":
        # Chile full_text recovery surfaces many regional programme labels and
        # one-off transfer lines. Keep automatic discovery focused on durable,
        # material institutions instead of project descriptions.
        return {"min_years": 3, "min_avg_amount": 500_000}
    if country == "Belgium":
        # Belgium pipeline output contains many bilingual programme buckets
        # (e.g. "Wetenschapsbeleid", "R&D op nationaal vlak") alongside the
        # named institutes we actually want discovery to learn. Require at
        # least 2 years of recurrence so discovery focuses on stable entities.
        return {"min_years": 2, "min_avg_amount": 1_000}
    if country == "Korea":
        # Korea budget briefs surface many one-off programme captions. Keep
        # discovery focused on recurring candidates so compile iterations do
        # not spend tokens on ephemeral summary labels.
        return {"min_years": 2}
    if country == "Poland":
        # Poland results contain many one-off programme captions and placeholder
        # section totals. Requiring recurrence keeps discovery open to new
        # institutions while avoiding token spend on single-year noise.
        return {"min_years": 2}
    if country == "Portugal":
        # Portugal extraction surfaces many one-off legal transfers, chapter
        # aggregates and programme captions. Keep discovery open to real new
        # entities, but only when they recur and are financially material.
        return {"min_years": 2, "min_avg_amount": 1_000_000}
    return {}


def _france_full_text_path(source_file: str) -> Optional[Path]:
    stem = Path(str(source_file or "")).stem
    if not stem:
        return None
    matches = sorted(_FRANCE_FULL_TEXT_DIR.glob(f"*__{stem}.txt.gz"))
    return matches[0] if matches else None


def _france_extract_programmes_from_full_text(source_file: str, year: int) -> pd.DataFrame:
    """
    Recover LOLF-era programme CP rows directly from the cached JORF text.

    This is a compile-only safety net for years where extraction retained only
    the mission total (e.g. France 2014) or dropped the programme rows. We scan
    the later AE/CP budget pages and take the *highest-page* occurrence of each
    programme label, which reliably prefers the budget table over earlier ETPT
    headcount pages.
    """
    if year < 2006:
        return pd.DataFrame()

    path = _france_full_text_path(source_file)
    if path is None:
        return pd.DataFrame()

    try:
        with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as fh:
            text = fh.read()
    except OSError:
        return pd.DataFrame()

    page_matches = list(
        re.finditer(r"=== Page (\d+)\.0 \|.*?(?=(?:=== Page \d+\.0 \|)|\Z)", text, flags=re.DOTALL)
    )
    if not page_matches:
        return pd.DataFrame()

    records: list[dict] = []
    for label in _FRANCE_PROGRAMME_LABELS:
        best: Optional[dict] = None
        for m in page_matches:
            page_no = int(m.group(1))
            page_text = m.group(0)
            if label not in page_text:
                continue

            lines = [line.strip() for line in page_text.splitlines() if line.strip()]
            for i, line in enumerate(lines):
                if label not in line:
                    continue
                values: list[int] = []
                for follower in lines[i:i + 4]:
                    for token in re.findall(r"\d[\d ]{2,}", follower):
                        digits = re.sub(r"\s+", "", token)
                        if len(digits) < 4:
                            continue
                        values.append(int(digits))
                    if len(values) >= 2:
                        break
                if len(values) < 2:
                    continue
                cp_full_eur = values[1]
                candidate = {
                    "country": "France",
                    "year": int(year),
                    "source_file": str(source_file),
                    "page_number": int(page_no),
                    "section_name": "Recherche et enseignement supérieur",
                    "section_name_en": "Research and Higher Education",
                    "line_description": label,
                    "line_description_en": label,
                    "amount_local": float(cp_full_eur) / 1000.0,
                    "unit": "thousand",
                    "currency": "EUR",
                    "item_type": "program_total",
                    "decision": "review",
                    "confidence": 0.95,
                    "rd_category": "rd_programme",
                    "aggregation_role": "",
                }
                if best is None or candidate["page_number"] > best["page_number"]:
                    best = candidate
                break
        if best is not None:
            records.append(best)

    return pd.DataFrame.from_records(records)


def _colombia_full_text_candidates(year: int) -> list[Path]:
    candidates = sorted(_COLOMBIA_FULL_TEXT_DIR.rglob(f"*__{year}_*.txt.gz"))
    if not candidates:
        return []
    decree_candidates = [path for path in candidates if "Decreto" in path.parts or "Decreto" in path.name]
    if decree_candidates:
        annexes = [path for path in decree_candidates if "Anexo" in path.name]
        regular = [path for path in decree_candidates if "Anexo" not in path.name]
        return annexes + regular
    return candidates


def _colombia_page_number(text: str, pos: int) -> int:
    page = 0
    for match in _COLOMBIA_PAGE_RE.finditer(text):
        if match.start() > pos:
            break
        page = int(match.group(1))
    return page


def _colombia_extract_amount(snippet: str) -> Optional[float]:
    amounts: list[float] = []
    for raw in _COLOMBIA_AMOUNT_RE.findall(snippet):
        digits = re.sub(r"[^\d]", "", raw)
        if len(digits) < 6:
            continue
        try:
            value = float(digits)
        except ValueError:
            continue
        if 1_000_000 <= value <= 10_000_000_000_000:
            amounts.append(value)
    return max(amounts) if amounts else None


def _colombia_extract_component_sum(snippet: str, components: int) -> Optional[float]:
    values: list[float] = []
    for raw in _COLOMBIA_AMOUNT_RE.findall(snippet):
        digits = re.sub(r"[^\d]", "", raw)
        if len(digits) < 6:
            continue
        try:
            value = float(digits)
        except ValueError:
            continue
        if 1_000_000 <= value <= 10_000_000_000_000:
            if values and value > values[0] * 20:
                break
            values.append(value)
        if len(values) >= components:
            break
    if not values:
        return None
    return sum(values)


def _colombia_component_candidates(year: int) -> list[Path]:
    if year == 2014:
        return sorted(_COLOMBIA_FULL_TEXT_DIR.rglob("*__2014_*.txt.gz"))
    if year == 2016:
        preferred = sorted(_COLOMBIA_FULL_TEXT_DIR.rglob("*__2016_Decreto_2550*.txt.gz"))
        return preferred or sorted(_COLOMBIA_FULL_TEXT_DIR.rglob("*__2016_*.txt.gz"))
    if year == 2017:
        preferred = sorted(_COLOMBIA_FULL_TEXT_DIR.rglob("*__2017_LEY_1815*.txt.gz"))
        fallback = sorted(_COLOMBIA_FULL_TEXT_DIR.rglob("*__2017_Decreto_2170*.txt.gz"))
        if preferred or fallback:
            return preferred + [path for path in fallback if path not in preferred]
        return sorted(_COLOMBIA_FULL_TEXT_DIR.rglob("*__2017_*.txt.gz"))
    return []


def _extract_colombia_targeted_raw_rows(year_range: Optional[tuple[int, int]]) -> pd.DataFrame:
    years = range(2019, 2026)
    if year_range is not None:
        start, end = year_range
        years = range(max(2019, start), min(2025, end) + 1)

    records: list[dict] = []
    for year in years:
        best_by_entity: dict[str, dict] = {}
        for path in _colombia_full_text_candidates(year):
            try:
                with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as fh:
                    text = fh.read()
            except OSError:
                continue

            for agency in _COLOMBIA_TARGETED_AGENCIES:
                best_match: Optional[dict] = None
                for pattern in agency["patterns"]:
                    best_for_pattern: Optional[dict] = None
                    for match in pattern.finditer(text):
                        snippet = text[match.start(): match.start() + 800]
                        amount = _colombia_extract_amount(snippet)
                        if amount is None:
                            continue
                        candidate = {
                            "country": "Colombia",
                            "year": year,
                            "source_file": path.stem,
                            "table_index": -1,
                            "row_index": -1,
                            "section_name": agency["section_name"],
                            "entity_raw": agency["entity_raw"],
                            "amount_current": amount,
                            "amount_prior": None,
                            "is_header_row": False,
                            "is_total_row": True,
                            "has_italic_entity": False,
                            "cells_raw": "[]",
                            "page_number": _colombia_page_number(text, match.start()),
                        }
                        if best_for_pattern is None or candidate["amount_current"] > best_for_pattern["amount_current"]:
                            best_for_pattern = candidate
                    if best_for_pattern is not None:
                        best_match = best_for_pattern
                        break
                if best_match is None:
                    continue
                current_best = best_by_entity.get(agency["entity_raw"])
                if current_best is None or best_match["amount_current"] > current_best["amount_current"]:
                    best_by_entity[agency["entity_raw"]] = best_match
        records.extend(best_by_entity.values())

    if not records:
        return pd.DataFrame(columns=RAW_ROW_COLUMNS + ["page_number"])
    return pd.DataFrame.from_records(records)


def _extract_colombia_component_raw_rows(year_range: Optional[tuple[int, int]]) -> pd.DataFrame:
    years = [2014, 2016, 2017]
    if year_range is not None:
        start, end = year_range
        years = [year for year in years if start <= year <= end]

    records: list[dict] = []
    for year in years:
        best_by_entity: dict[str, dict] = {}
        for path in _colombia_component_candidates(year):
            try:
                with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as fh:
                    text = fh.read()
            except OSError:
                continue

            for agency in _COLOMBIA_COMPONENT_AGENCIES:
                match = agency["pattern"].search(text)
                if not match:
                    continue
                snippet = match.group(0)
                amount = _colombia_extract_component_sum(snippet, agency["components"])
                if amount is None:
                    continue
                max_amount = agency.get("max_amount")
                if isinstance(max_amount, (int, float)) and amount > float(max_amount):
                    continue
                candidate = {
                    "country": "Colombia",
                    "year": year,
                    "source_file": path.stem,
                    "table_index": -1,
                    "row_index": -1,
                    "section_name": agency["section_name"],
                    "entity_raw": agency["entity_raw"],
                    "amount_current": amount,
                    "amount_prior": None,
                    "is_header_row": False,
                    "is_total_row": True,
                    "has_italic_entity": False,
                    "cells_raw": "[]",
                    "page_number": _colombia_page_number(text, match.start()),
                }
                current_best = best_by_entity.get(agency["entity_raw"])
                if current_best is None or candidate["amount_current"] > current_best["amount_current"]:
                    best_by_entity[agency["entity_raw"]] = candidate
        records.extend(best_by_entity.values())

    if not records:
        return pd.DataFrame(columns=RAW_ROW_COLUMNS + ["page_number"])
    return pd.DataFrame.from_records(records)


def _augment_france_pipeline_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    additions: list[pd.DataFrame] = []
    for (year, source_file), chunk in df.groupby(["year", "source_file"]):
        if int(year) < 2006 or "JORF_" not in str(source_file):
            continue
        existing_desc = {
            str(v).strip().lower()
            for v in chunk.get("line_description", pd.Series(index=chunk.index, dtype="object")).fillna("")
        }
        recovered = _france_extract_programmes_from_full_text(str(source_file), int(year))
        if recovered.empty:
            continue
        recovered = recovered[
            ~recovered["line_description"].astype(str).str.strip().str.lower().isin(existing_desc)
        ].copy()
        if not recovered.empty:
            additions.append(recovered)

    if not additions:
        return df

    out = pd.concat([df, *additions], ignore_index=True, sort=False)
    out = out.drop_duplicates(
        subset=["country", "year", "source_file", "page_number", "line_description", "amount_local"],
        keep="first",
    ).reset_index(drop=True)
    logger.info(f"[France] Added {sum(len(x) for x in additions)} programme rows from full_text fallback")
    return out


def _trim_france_etpt_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    page_num = pd.to_numeric(df.get("page_number"), errors="coerce")
    desc_norm = (
        df.get("line_description", pd.Series("", index=df.index))
        .astype(str)
        .str.strip()
    )
    programme_exact = desc_norm.isin(_FRANCE_PROGRAMME_LABELS)
    mission_total = desc_norm.apply(
        lambda s: any(p.search(s) for p in _FRANCE_MISSION_TOTAL_PATTERNS)
    )
    early_jorf = (
        df.get("source_file", pd.Series("", index=df.index)).astype(str).str.contains(r"JORF_", case=False, na=False)
        & page_num.lt(80)
    )

    drop_mask = early_jorf & (programme_exact | mission_total)
    if not drop_mask.any():
        return df

    kept = df.loc[~drop_mask].copy()
    logger.info(f"[France] Dropped {int(drop_mask.sum())} early-page ETPT-like programme rows from series input")
    return kept.reset_index(drop=True)


def _normalise_france_pre_lolf_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    years = pd.to_numeric(out.get("year"), errors="coerce")
    unit_norm = out.get("unit", pd.Series("", index=out.index)).astype(str).str.lower().str.strip()
    amt = pd.to_numeric(out.get("amount_local"), errors="coerce")

    # Pre-euro France rows are often extracted with full-franc amounts while
    # still labelled as "thousand". Recover the intended value before compile
    # expands to base currency. This is visible in original JORF rows such as
    # "637 524 095" for CNES 1986/1987, which is a plausible number of francs
    # but absurd as "thousand francs".
    pre_euro_overscaled = (
        years.le(2001)
        & unit_norm.eq("thousand")
        & amt.ge(1_000_000)
    )
    if pre_euro_overscaled.any():
        out.loc[pre_euro_overscaled, "amount_local"] = amt.loc[pre_euro_overscaled] / 1000.0
        logger.info(f"[France] Rescaled {int(pre_euro_overscaled.sum())} pre-euro rows by 1/1000")

    # 2002–2005 Etat C rows are in "milliers d'euros". Some extraction runs
    # already append three extra zeros while still labelling the row as
    # thousand. Recover the intended value before compile expands to euros.
    euro_overscaled = (
        years.between(2002, 2005)
        & unit_norm.eq("thousand")
        & amt.ge(100_000_000)
    )
    if euro_overscaled.any():
        out.loc[euro_overscaled, "amount_local"] = amt.loc[euro_overscaled] / 1000.0
        logger.info(f"[France] Rescaled {int(euro_overscaled.sum())} 2002-2005 pre-LOLF rows by 1/1000")

    # Pre-LOLF total-payment rows are useful for audit but should not become a
    # separate canonical series next to the chapter itself.
    desc = out.get("line_description", pd.Series("", index=out.index)).astype(str)
    total_rows = years.between(1970, 2005) & desc.apply(lambda s: bool(_FRANCE_PRE_LOLF_TOTAL_RE.search(s.strip())))
    if total_rows.any():
        out = out.loc[~total_rows].copy()
        logger.info(f"[France] Dropped {int(total_rows.sum())} pre-LOLF total rows from series input")

    return out.reset_index(drop=True)


def _apply_korea_audited_pipeline_repairs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Patch a tiny set of Korea pipeline rows that were confirmed against the
    original budget briefs during audit.

    This is intentionally narrow. Korea summary PDFs mix several extraction
    scale failures on the same pages, so broad heuristics are riskier than
    audited row-level fixes.
    """
    if df.empty:
        return df

    out = df.copy()
    source = out.get("source_file", pd.Series("", index=out.index)).fillna("").astype(str)
    page = out.get("page_number", pd.Series("", index=out.index)).fillna("").astype(str)
    desc = out.get("line_description_en", pd.Series("", index=out.index)).fillna("").astype(str)

    audited_rows = [
        {
            "year": 2022,
            "source_file": "3. 2022년 예산안.pdf",
            "page_number": "101",
            "section_name_en": "Ministry of Science and ICT",
            "line_description_en": "Science and Technology, Communication Sector",
            "amount_local": 9_626_200_000.0,
        },
        {
            "year": 2023,
            "source_file": "2. 2023년 예산안 홍보자료★.pdf",
            "page_number": "60",
            "section_name_en": "Ministry of Science and ICT",
            "line_description_en": "Science and Technology, Communication Sector",
            "amount_local": 9_977_500_000.0,
        },
        {
            "year": 2024,
            "source_file": "2. 2024년  예산안 홍보자료.pdf",
            "page_number": "55",
            "section_name_en": "Ministry of Science and ICT",
            "line_description_en": "Science and Technology, Communication Sector",
            "amount_local": 9_076_800_000.0,
        },
    ]

    repaired = 0
    appended = 0
    for spec in audited_rows:
        mask = (
            pd.to_numeric(out.get("year"), errors="coerce").eq(spec["year"])
            & source.eq(spec["source_file"])
            & page.eq(spec["page_number"])
            & desc.eq(spec["line_description_en"])
        )
        note = (
            "compile audited Korea row from original budget brief table "
            "(억원 subtotal converted to thousand KRW)"
        )
        if mask.any():
            out.loc[mask, "amount_local"] = float(spec["amount_local"])
            out.loc[mask, "unit"] = "thousand"
            out.loc[mask, "currency"] = "KRW"
            notes = out.loc[mask, "notes"].fillna("").astype(str).str.strip()
            out.loc[mask, "notes"] = notes.apply(
                lambda s: f"{s}; {note}".strip("; ").strip()
            )
            repaired += int(mask.sum())
            continue

        new_row = {col: None for col in out.columns}
        new_row.update(
            {
                "country": "Korea",
                "year": spec["year"],
                "item_type": "section_total",
                "section_name_en": spec["section_name_en"],
                "line_description_en": spec["line_description_en"],
                "amount_local": float(spec["amount_local"]),
                "unit": "thousand",
                "currency": "KRW",
                "rd_category": "rd_ministry",
                "decision": "include",
                "confidence": "high",
                "source_file": spec["source_file"],
                "page_number": spec["page_number"],
                "notes": note,
            }
        )
        out = pd.concat([out, pd.DataFrame([new_row])], ignore_index=True)
        source = out.get("source_file", pd.Series("", index=out.index)).fillna("").astype(str)
        page = out.get("page_number", pd.Series("", index=out.index)).fillna("").astype(str)
        desc = out.get("line_description_en", pd.Series("", index=out.index)).fillna("").astype(str)
        appended += 1

    if repaired or appended:
        logger.info(
            f"[Korea] Applied {repaired} audited pipeline row repairs and appended {appended} audited rows"
        )

    return out.reset_index(drop=True)


_KOREA_AUDITED_THEME_ROWS = [
    {
        "country": "Korea",
        "year": 2019,
        "theme_bucket": "AI / Data Economy",
        "theme_label": "Data / AI economy",
        "source_amount_display": "10,493억원",
        "amount_local": 1_049_300_000.0,
        "currency": "KRW",
        "unit": "thousand",
        "source_unit": "억원",
        "source_file": "2019년도 예산안 개요.pdf",
        "page_number": 27,
        "comparability_note": "Broad thematic subtotal from a budget-summary table; useful for theme tracking, not a ministry appropriation.",
    },
    {
        "country": "Korea",
        "year": 2022,
        "theme_bucket": "Strategic Technology",
        "theme_label": "Future industry strategic R&D investment",
        "source_amount_display": "6.2조원",
        "amount_local": 6_200_000_000.0,
        "currency": "KRW",
        "unit": "thousand",
        "source_unit": "조원",
        "source_file": "3. 2022년 예산안.pdf",
        "page_number": 49,
        "comparability_note": "Broad strategic-technology subtotal from the 2022 R&D summary pages.",
    },
    {
        "country": "Korea",
        "year": 2022,
        "theme_bucket": "Semiconductor",
        "theme_label": "BIG3+ semiconductor-related R&D",
        "source_amount_display": "0.4조원",
        "amount_local": 400_000_000.0,
        "currency": "KRW",
        "unit": "thousand",
        "source_unit": "조원",
        "source_file": "3. 2022년 예산안.pdf",
        "page_number": 100,
        "comparability_note": "Broad semiconductor theme from the 2022 R&D field summary; not directly comparable to narrower project lines.",
    },
    {
        "country": "Korea",
        "year": 2022,
        "theme_bucket": "Space",
        "theme_label": "Space-related R&D",
        "source_amount_display": "0.64조원",
        "amount_local": 640_000_000.0,
        "currency": "KRW",
        "unit": "thousand",
        "source_unit": "조원",
        "source_file": "3. 2022년 예산안.pdf",
        "page_number": 101,
        "comparability_note": "Broad space subtotal from the 2022 frontier-strategy section.",
    },
    {
        "country": "Korea",
        "year": 2022,
        "theme_bucket": "Quantum / 6G",
        "theme_label": "Quantum / 6G-related R&D",
        "source_amount_display": "927억원",
        "amount_local": 92_700_000.0,
        "currency": "KRW",
        "unit": "thousand",
        "source_unit": "억원",
        "source_file": "3. 2022년 예산안.pdf",
        "page_number": 101,
        "comparability_note": "Combined next-generation communications and quantum subtotal from the 2022 frontier-strategy section.",
    },
    {
        "country": "Korea",
        "year": 2023,
        "theme_bucket": "Strategic Technology",
        "theme_label": "Core strategic technologies",
        "source_amount_display": "45,123억원",
        "amount_local": 4_512_300_000.0,
        "currency": "KRW",
        "unit": "thousand",
        "source_unit": "억원",
        "source_file": "2. 2023년 예산안 홍보자료★.pdf",
        "page_number": 33,
        "comparability_note": "Broad strategic-technology subtotal from an audited table in the 2023 budget brief.",
    },
    {
        "country": "Korea",
        "year": 2023,
        "theme_bucket": "Semiconductor",
        "theme_label": "Semiconductor",
        "source_amount_display": "6,098억원",
        "amount_local": 609_800_000.0,
        "currency": "KRW",
        "unit": "thousand",
        "source_unit": "억원",
        "source_file": "2. 2023년 예산안 홍보자료★.pdf",
        "page_number": 33,
        "comparability_note": "Explicit semiconductor subtotal from the 2023 strategic-technology table.",
    },
    {
        "country": "Korea",
        "year": 2023,
        "theme_bucket": "Future Mobility",
        "theme_label": "Future mobility",
        "source_amount_display": "7,846억원",
        "amount_local": 784_600_000.0,
        "currency": "KRW",
        "unit": "thousand",
        "source_unit": "억원",
        "source_file": "2. 2023년 예산안 홍보자료★.pdf",
        "page_number": 33,
        "comparability_note": "Explicit future-mobility subtotal from the 2023 strategic-technology table.",
    },
    {
        "country": "Korea",
        "year": 2023,
        "theme_bucket": "Quantum / 5G / 6G",
        "theme_label": "5G / 6G / quantum",
        "source_amount_display": "2,952억원",
        "amount_local": 295_200_000.0,
        "currency": "KRW",
        "unit": "thousand",
        "source_unit": "억원",
        "source_file": "2. 2023년 예산안 홍보자료★.pdf",
        "page_number": 33,
        "comparability_note": "Combined communications-and-quantum subtotal from the 2023 strategic-technology table.",
    },
    {
        "country": "Korea",
        "year": 2024,
        "theme_bucket": "AI",
        "theme_label": "AI-related projects",
        "source_amount_display": "12,028억원",
        "amount_local": 1_202_800_000.0,
        "currency": "KRW",
        "unit": "thousand",
        "source_unit": "억원",
        "source_file": "2. 2024년  예산안 홍보자료.pdf",
        "page_number": 29,
        "comparability_note": "Explicit theme subtotal from the 2024 advanced-services table.",
    },
    {
        "country": "Korea",
        "year": 2024,
        "theme_bucket": "Bio",
        "theme_label": "Bio-related projects",
        "source_amount_display": "19,442억원",
        "amount_local": 1_944_200_000.0,
        "currency": "KRW",
        "unit": "thousand",
        "source_unit": "억원",
        "source_file": "2. 2024년  예산안 홍보자료.pdf",
        "page_number": 29,
        "comparability_note": "Explicit theme subtotal from the 2024 advanced-services table.",
    },
    {
        "country": "Korea",
        "year": 2024,
        "theme_bucket": "Cyber Security",
        "theme_label": "Cyber security projects",
        "source_amount_display": "3,656억원",
        "amount_local": 365_600_000.0,
        "currency": "KRW",
        "unit": "thousand",
        "source_unit": "억원",
        "source_file": "2. 2024년  예산안 홍보자료.pdf",
        "page_number": 29,
        "comparability_note": "Explicit theme subtotal from the 2024 advanced-services table.",
    },
    {
        "country": "Korea",
        "year": 2024,
        "theme_bucket": "Digital Platform Government",
        "theme_label": "Digital platform government projects",
        "source_amount_display": "9,262억원",
        "amount_local": 926_200_000.0,
        "currency": "KRW",
        "unit": "thousand",
        "source_unit": "억원",
        "source_file": "2. 2024년  예산안 홍보자료.pdf",
        "page_number": 29,
        "comparability_note": "Explicit theme subtotal from the 2024 advanced-services table.",
    },
    {
        "country": "Korea",
        "year": 2025,
        "theme_bucket": "Strategic Technology",
        "theme_label": "National strategic technologies / future challenge",
        "source_amount_display": "7.1조원",
        "amount_local": 7_100_000_000.0,
        "currency": "KRW",
        "unit": "thousand",
        "source_unit": "조원",
        "source_file": "2. 2025 예산안 홍보자료.pdf",
        "page_number": 50,
        "comparability_note": "Broad strategic-technology subtotal from the 2025 R&D field summary.",
    },
    {
        "country": "Korea",
        "year": 2025,
        "theme_bucket": "3 Game Changers",
        "theme_label": "Three game changers",
        "source_amount_display": "35,446억원",
        "amount_local": 3_544_600_000.0,
        "currency": "KRW",
        "unit": "thousand",
        "source_unit": "억원",
        "source_file": "2. 2025 예산안 홍보자료.pdf",
        "page_number": 20,
        "comparability_note": "Explicit game-changer subtotal from the 2025 R&D reform table.",
    },
    {
        "country": "Korea",
        "year": 2025,
        "theme_bucket": "AI",
        "theme_label": "AI-related R&D",
        "source_amount_display": "1.2조원",
        "amount_local": 1_200_000_000.0,
        "currency": "KRW",
        "unit": "thousand",
        "source_unit": "조원",
        "source_file": "2. 2025 예산안 홍보자료.pdf",
        "page_number": 50,
        "comparability_note": "Broad AI subtotal from the 2025 3-game-changer summary.",
    },
    {
        "country": "Korea",
        "year": 2025,
        "theme_bucket": "Bio",
        "theme_label": "Bio-related R&D",
        "source_amount_display": "2.1조원",
        "amount_local": 2_100_000_000.0,
        "currency": "KRW",
        "unit": "thousand",
        "source_unit": "조원",
        "source_file": "2. 2025 예산안 홍보자료.pdf",
        "page_number": 50,
        "comparability_note": "Broad bio subtotal from the 2025 3-game-changer summary.",
    },
    {
        "country": "Korea",
        "year": 2025,
        "theme_bucket": "Quantum",
        "theme_label": "Quantum-related R&D",
        "source_amount_display": "0.20조원",
        "amount_local": 200_000_000.0,
        "currency": "KRW",
        "unit": "thousand",
        "source_unit": "조원",
        "source_file": "2. 2025 예산안 홍보자료.pdf",
        "page_number": 50,
        "comparability_note": "Broad quantum subtotal from the 2025 3-game-changer summary.",
    },
]


def _build_korea_theme_panel(year_range: Optional[tuple[int, int]] = None) -> pd.DataFrame:
    out = pd.DataFrame(_KOREA_AUDITED_THEME_ROWS)
    if out.empty:
        return out
    if year_range:
        start, end = int(year_range[0]), int(year_range[1])
        out = out[(out["year"] >= start) & (out["year"] <= end)].copy()
    out["amount_local"] = pd.to_numeric(out["amount_local"], errors="coerce")
    return out.sort_values(["year", "theme_bucket", "source_file", "page_number"], kind="stable").reset_index(drop=True)


def _output_unit_from_currency(currency: str, fallback_unit: str) -> str:
    return _OUTPUT_UNIT_BY_CURRENCY.get(str(currency or "").upper(), fallback_unit)


def _expand_output_amount(amount: object, unit: object, currency: object) -> tuple[object, object]:
    try:
        amt = float(amount)
    except (TypeError, ValueError):
        return amount, unit

    unit_norm = str(unit or "").strip().lower()
    factor = _SCALE_TO_BASE_UNIT.get(unit_norm)
    if factor is None:
        return amt, unit

    return amt * factor, _output_unit_from_currency(str(currency or ""), str(unit or ""))

# ---------------------------------------------------------------------------
# Unit normalisation — convert all amounts to thousands
# ---------------------------------------------------------------------------


def _write_year_slice(
    path: Path,
    df: pd.DataFrame,
    year_range: Optional[tuple[int, int]] = None,
    sort_cols: Optional[list[str]] = None,
) -> None:
    """
    Write a per-country output file, preserving rows outside the requested year
    slice when year_range is provided.

    This prevents partial reruns like --years 1987-1999 from clobbering a
    country file that already contains 2000-2024.
    """
    out = df.copy()

    if year_range and path.exists():
        try:
            existing = pd.read_csv(path)
        except Exception:
            existing = pd.DataFrame()

        if not existing.empty and "year" in existing.columns and "year" in out.columns:
            existing_year = pd.to_numeric(existing["year"], errors="coerce")
            keep_existing = existing[
                existing_year.isna()
                | (existing_year < year_range[0])
                | (existing_year > year_range[1])
            ].copy()
            out = pd.concat([keep_existing, out], ignore_index=True, sort=False)

    if sort_cols:
        valid_sort = [c for c in sort_cols if c in out.columns]
        if valid_sort:
            out = out.sort_values(valid_sort, kind="stable").reset_index(drop=True)

    path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(path, index=False)


def _build_verified_override_audit(series_df: pd.DataFrame) -> pd.DataFrame:
    """Return a traceability table for rows manually verified against originals."""
    if series_df.empty or "item_type" not in series_df.columns:
        return pd.DataFrame()

    mask = series_df["item_type"].fillna("").eq("verified_override")
    if not mask.any():
        return pd.DataFrame()

    out = series_df.loc[mask].copy()
    out["source_kind"] = out["source_file"].fillna("").map(
        lambda value: "original_pdf" if str(value).lower().endswith(".pdf") else "parsed_budget_text"
    )
    out["traceability_status"] = "verified_against_original_file"

    preferred_cols = [
        "country",
        "year",
        "canonical_name",
        "category",
        "amount_local",
        "currency",
        "unit",
        "item_type",
        "source_kind",
        "source_file",
        "page_number",
        "line_description_en",
        "series_notes",
        "traceability_status",
    ]
    cols = [col for col in preferred_cols if col in out.columns]
    return out.loc[:, cols].reset_index(drop=True)


def _find_full_text_cache(country: str, source_file: str) -> str:
    stem = Path(str(source_file)).stem
    cache_dir = Path("Data/output/budget/full_text") / country
    if not cache_dir.exists():
        return ""
    matches = sorted(cache_dir.glob(f"*__{stem}.txt.gz"))
    return str(matches[0]) if matches else ""


def _normalise_trace_search(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(text or "").lower()).strip()


def _trace_amount_tokens(amount: object) -> list[str]:
    try:
        value = int(round(float(amount)))
    except (TypeError, ValueError):
        return []
    if value <= 0:
        return []

    plain = str(value)
    dot_grouped = f"{value:,}".replace(",", ".")
    space_grouped = f"{value:,}".replace(",", " ")
    compact_space = space_grouped.replace(" ", "")
    return list(dict.fromkeys([plain, dot_grouped, space_grouped, compact_space]))


def _snippet_from_lines(lines: list[str], idx: int, before: int = 3, after: int = 4) -> str:
    start = max(0, idx - before)
    end = min(len(lines), idx + after)
    snippet = [segment.strip() for segment in lines[start:end] if segment.strip()]
    return " | ".join(snippet)


_TURKEY_TRACE_ALIAS_MAP = {
    "tubitak": [
        "turkiye bilimsel ve teknolojik arastirma kurumu",
        "tubitak",
    ],
    "tuba": [
        "turkiye bilimler akademisi baskanligi",
        "turkiye bilimler akademisi",
        "tuba",
    ],
    "taek": [
        "turkiye atom enerjisi kurumu",
        "atom enerjisi kurumu",
        "taek",
    ],
    "kosgeb": [
        "kucuk ve orta olcekli sanayi gelistirme ve destekleme idaresi baskanligi",
        "kucuk ve orta olcekli sanayi gelistirme ve destekleme",
        "kosgeb",
    ],
}


def _compact_trace_search(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", _normalise_trace_search(text))


def _turkey_trace_aliases(row: pd.Series) -> list[str]:
    canonical_norm = _normalise_trace_search(str(row.get("canonical_name") or ""))
    canonical_compact = _compact_trace_search(str(row.get("canonical_name") or ""))
    for key, aliases in _TURKEY_TRACE_ALIAS_MAP.items():
        if key in canonical_norm or key in canonical_compact:
            return aliases
    line_desc_norm = _normalise_trace_search(str(row.get("line_description_en") or ""))
    if "atom" in line_desc_norm or "nukleer" in line_desc_norm:
        return _TURKEY_TRACE_ALIAS_MAP["taek"]
    return []


def _turkey_amount_line(line: str) -> bool:
    stripped = str(line or "").strip()
    return bool(stripped) and bool(re.fullmatch(r"[\d\., ]+", stripped))


def _trace_number_compact(text: str) -> str:
    return re.sub(r"\D+", "", str(text or ""))


def _extract_turkey_exact_excerpt(page_lines: list[str], row: pd.Series) -> str:
    year = pd.to_numeric(pd.Series([row.get("year")]), errors="coerce").iloc[0]
    if pd.isna(year):
        return ""
    year = int(year)
    amount_tokens = set(_trace_amount_tokens(row.get("amount_local")))
    if not amount_tokens:
        return ""
    amount_compacts = {_trace_number_compact(token) for token in amount_tokens if _trace_number_compact(token)}

    if year == 2006:
        try:
            name_header_idx = next(i for i, line in enumerate(page_lines) if "İDARENİN ADI" in line)
            offer_idx = next(i for i, line in enumerate(page_lines) if "Hükümetin Teklifi" in line)
        except StopIteration:
            return ""

        agency_lines = [line.strip() for line in page_lines[name_header_idx + 1:offer_idx] if str(line).strip()]
        amount_lines = [line.strip() for line in page_lines[offer_idx + 1:] if _turkey_amount_line(line)]
        target_idx = next(
            (
                i for i, line in enumerate(amount_lines)
                if line in amount_tokens or _trace_number_compact(line) in amount_compacts
            ),
            None,
        )
        if target_idx is None or target_idx >= len(amount_lines):
            return ""
        amount_line = amount_lines[target_idx]
        if target_idx >= len(agency_lines):
            return ""
        return " | ".join(
            [
                "(TABLO 2-b)",
                agency_lines[target_idx],
                "Hükümetin Teklifi",
                amount_line,
            ]
        )

    if year in {2007, 2008, 2009}:
        for idx, line in enumerate(page_lines):
            if line.strip() not in amount_tokens and _trace_number_compact(line) not in amount_compacts:
                continue
            agency_idx = next(
                (
                    j for j in range(idx - 1, max(-1, idx - 8), -1)
                    if not _turkey_amount_line(page_lines[j]) and page_lines[j].strip()
                ),
                None,
            )
            if agency_idx is None:
                continue
            amount_lines: list[str] = []
            for candidate in page_lines[agency_idx + 1:]:
                if _turkey_amount_line(candidate):
                    amount_lines.append(candidate.strip())
                    if len(amount_lines) >= 5:
                        break
                elif amount_lines:
                    break
            if any(item in amount_tokens or _trace_number_compact(item) in amount_compacts for item in amount_lines):
                header = next((segment.strip() for segment in page_lines[:3] if segment.strip()), "2009 Yılı Bütçe Gerekçesi")
                return " | ".join([header, page_lines[agency_idx].strip(), *amount_lines])

    if year in {1976, 1977}:
        for idx, line in enumerate(page_lines):
            line_compact = _trace_number_compact(line)
            if not any(token in line for token in amount_tokens) and line_compact not in amount_compacts:
                continue
            snippet = _snippet_from_lines(page_lines, idx, before=10, after=6)
            if any(term in _normalise_trace_search(snippet) for term in ["nukleer", "arastirma", "bilimsel", "teknik"]):
                return snippet

    return ""


def _turkey_trace_snippet_ok(snippet: str, row: pd.Series) -> bool:
    snippet_norm = _normalise_trace_search(snippet)
    if not snippet_norm:
        return False

    year = pd.to_numeric(pd.Series([row.get("year")]), errors="coerce").iloc[0]
    canonical_norm = _normalise_trace_search(str(row.get("canonical_name") or ""))

    reject_terms = [
        "vergi",
        "istihsal",
        "akaryakit",
        "ihale",
        "bankamiza",
        "muteahhitlik",
        "kapali zarf",
        "universitesine",
    ]
    if any(term in snippet_norm for term in reject_terms):
        return False

    if "=== page" in snippet.lower() and pd.notna(year) and int(year) <= 1982:
        return False

    positive_terms = [
        "arastirma",
        "gelistirme",
        "nukleer",
        "atom enerjisi",
        "tubitak",
        "bilimler akademisi",
        "kosgeb",
        "bilimsel",
        "teknik",
    ]
    has_positive_term = any(term in snippet_norm for term in positive_terms)
    if has_positive_term:
        if "taek" in canonical_norm and not any(term in snippet_norm for term in ["taek", "atom enerjisi", "nukleer"]):
            return False
        if "tubitak" in canonical_norm and not any(
            term in snippet_norm for term in ["tubitak", "bilimsel", "teknik", "sanayi arastirma"]
        ):
            return False
        return True

    for key in ["canonical_name", "line_description_en"]:
        value_norm = _normalise_trace_search(str(row.get(key) or ""))
        tokens = [tok for tok in value_norm.split() if len(tok) >= 5]
        if tokens and sum(tok in snippet_norm for tok in tokens) >= 2:
            return True

    return False


def _extract_trace_excerpt(cache_path: str, row: pd.Series) -> str:
    if not cache_path:
        return ""

    try:
        with gzip.open(cache_path, "rt", encoding="utf-8", errors="ignore") as fh:
            lines = fh.read().splitlines()
    except OSError:
        return ""

    page_number = pd.to_numeric(pd.Series([row.get("page_number")]), errors="coerce").iloc[0]
    page_lines = lines
    page_scoped = False
    if pd.notna(page_number):
        marker = f"=== Page {int(page_number)}.0 |"
        start_idx = next((idx for idx, line in enumerate(lines) if line.startswith(marker)), None)
        if start_idx is not None:
            end_idx = next(
                (idx for idx in range(start_idx + 1, len(lines)) if lines[idx].startswith("=== Page ")),
                len(lines),
            )
            page_lines = lines[start_idx:end_idx]
            page_scoped = True

    terms: list[str] = []
    for key in ["line_description_en", "canonical_name"]:
        value = str(row.get(key) or "").strip()
        if value:
            terms.append(value)
    line_desc = str(row.get("line_description_en") or "").strip()
    if line_desc and ":" in line_desc:
        terms.append(line_desc.split(":", 1)[-1].strip())

    seen: set[str] = set()
    terms = [term for term in terms if not (term in seen or seen.add(term))]

    amount_tokens = _trace_amount_tokens(row.get("amount_local"))
    country = str(row.get("country") or "")

    if country == "Turkey" and page_scoped:
        turkey_exact = _extract_turkey_exact_excerpt(page_lines, row)
        if turkey_exact:
            return turkey_exact

    def _accept_snippet(snippet: str) -> bool:
        if not snippet:
            return False
        if country == "Turkey":
            return _turkey_trace_snippet_ok(snippet, row)
        return True

    def _search_lines(search_lines: list[str], *, use_amounts: bool) -> str:
        for term in terms:
            term_norm = _normalise_trace_search(term)
            if not term_norm:
                continue
            for idx, line in enumerate(search_lines):
                window = " ".join(segment.strip() for segment in search_lines[idx:idx + 5] if segment.strip())
                if term.lower() in window.lower() or term_norm in _normalise_trace_search(window):
                    snippet = _snippet_from_lines(search_lines, idx, before=2, after=6)
                    if _accept_snippet(snippet):
                        return snippet

        if use_amounts:
            for token in amount_tokens:
                for idx, line in enumerate(search_lines):
                    if token in line:
                        snippet = _snippet_from_lines(search_lines, idx, before=6, after=3)
                        if _accept_snippet(snippet):
                            return snippet

        return ""

    excerpt = _search_lines(page_lines, use_amounts=True)
    if excerpt:
        return excerpt

    if page_lines is not lines:
        excerpt = _search_lines(lines, use_amounts=True)
        if excerpt:
            return excerpt

    if page_scoped:
        fallback = [segment.strip() for segment in page_lines[:12] if segment.strip()]
        if fallback:
            snippet = " | ".join(fallback)
            if _accept_snippet(snippet):
                return snippet

    if country == "Turkey":
        page_label = str(row.get("page_number") or "").strip()
        desc = str(row.get("line_description_en") or "").strip()
        amount = str(row.get("amount_local") or "").strip()
        currency = str(row.get("currency") or "").strip()
        anchor_parts = [part for part in [f"page {page_label}" if page_label else "", desc, amount, currency] if part]
        if anchor_parts:
            return "[anchor] " + " | ".join(anchor_parts)

    return ""


def _trace_method_label(excerpt: str) -> str:
    text = str(excerpt or "").strip()
    if not text:
        return ""
    if text.startswith("[anchor]"):
        return "page_anchor"
    if "(TABLO 2-b)" in text:
        return "table_row_aligned"
    if "=== Page 89.0" in text:
        return "table_row_multiyear"
    if "=== Page " in text:
        return "page_context_excerpt"
    return "page_excerpt"


def _build_series_traceability(series_df: pd.DataFrame, country: str) -> pd.DataFrame:
    if series_df.empty:
        return pd.DataFrame()

    cname = country.lower().replace(" ", "_")
    pdf_root = Path("Data/input/finance_bills") / country
    out = series_df.copy()
    out["pdf_path"] = out["source_file"].fillna("").map(lambda s: str(pdf_root / str(s)) if str(s).lower().endswith(".pdf") else "")
    out["full_text_cache"] = out["source_file"].fillna("").map(lambda s: _find_full_text_cache(country, s))
    out["traceability_status"] = out["item_type"].fillna("").map(
        lambda s: "verified_against_original_file" if str(s) == "verified_override" else "covered_by_final_series"
    )
    out["trace_excerpt"] = out.apply(
        lambda row: _extract_trace_excerpt(str(row.get("full_text_cache") or ""), row)
        if str(row.get("full_text_cache") or "").strip()
        else "",
        axis=1,
    )
    out["trace_method"] = out["trace_excerpt"].map(_trace_method_label)

    preferred_cols = [
        "year",
        "canonical_name",
        "category",
        "amount_local",
        "unit",
        "currency",
        "item_type",
        "source_file",
        "page_number",
        "pdf_path",
        "full_text_cache",
        "line_description_en",
        "traceability_status",
        "trace_method",
        "trace_excerpt",
        "series_notes",
    ]
    cols = [col for col in preferred_cols if col in out.columns]
    return out.loc[:, cols].sort_values(["year", "canonical_name", "source_file"], kind="stable").reset_index(drop=True)


def _build_source_traceability(series_df: pd.DataFrame, country: str) -> pd.DataFrame:
    cname = country.lower().replace(" ", "_")
    pdf_root = Path("Data/input/finance_bills") / country
    pdfs = sorted(p.name for p in pdf_root.glob(f"*_{country}.pdf"))
    if not pdfs:
        pdfs = sorted(p.name for p in pdf_root.glob("*.pdf"))
    if not pdfs and series_df.empty:
        return pd.DataFrame()

    known_source_notes: dict[str, dict[str, str]] = {
        "Slovakia": {
            "2023 zakonypreludi_sk_526_2022_zz_20230101.pdf": "Manual source audit: the original PDF is the legal budget act text with references to annexes, but it does not include the numeric annex tables needed for traceable agency/programme appropriations.",
            "2024 20240101_5598091-2.pdf": "Manual source audit: the original PDF is only a one-page aggregate state-budget balance summary, not the detailed expenditure annex needed for traceable R&D agency appropriations.",
        },
        "Slovenia": {
            "2004 2005 u2013102.pdf": "Manual source audit: file is mislabelled. Page 1 shows a 2012 closing-account / execution document, not the 2004/2005 national budget annex, so it must not drive annual series selection for those years.",
            "2014 u2013101.pdf": "Manual source audit: this is the ZIPRS1415 budget-execution law text for 2014-2015. It does not contain the national budget annex tables needed for traceable agency/programme appropriations.",
            "2014.pdf": "Manual source audit: available file is an Uradni list legal/gazette wrapper, not the numeric state-budget annex. Re-running extraction on the same PDF is unlikely to recover defendable R&D appropriations.",
        },
        "Turkey": {
            "2006 GenelFaaliyetRaporu_2006.pdf": "Manual source audit: annual activity report. Uses execution / realization tables rather than budget-law appropriations, so it is excluded from the final budget series for comparability.",
            "2007 GenelFaaliyetRaporu_2007.pdf": "Manual source audit: annual activity report. Uses execution / realization tables rather than budget-law appropriations, so it is excluded from the final budget series for comparability.",
            "2008 GenelFaaliyetRaporu_2008.pdf": "Manual source audit: annual activity report. Uses execution / realization tables rather than budget-law appropriations, so it is excluded from the final budget series for comparability.",
            "2009 GenelFaaliyetRaporu_2009.pdf": "Manual source audit: annual activity report. Page 110 explicitly states '(Bin TL)' and reports initial / year-end / realization values, not a clean budget appropriation row.",
            "2008 2008-Merkezi-Yonetim-Kesin-Hesabi-.pdf": "Manual source audit: final-account / closing-account source, not a budget-law appropriation source. Excluded from the final Turkey budget panel to avoid mixing budget and execution concepts.",
            "2009-Merkezi-Yönetim-Kesin-Hesabı-compressed.pdf": "Manual source audit: final-account / closing-account source, not a budget-law appropriation source. Excluded from the final Turkey budget panel to avoid mixing budget and execution concepts.",
        },
    }.get(country, {})

    rows = []
    for source_file in pdfs:
        subset = series_df[series_df["source_file"].astype(str) == source_file].copy() if not series_df.empty else pd.DataFrame()
        source_note = known_source_notes.get(source_file, "")
        if country == "Luxembourg" and not source_note:
            match = re.match(r"^(\d{4})", str(source_file))
            source_year = int(match.group(1)) if match else None
            if source_year is not None and source_year <= 2000:
                source_note = (
                    "Manual source audit: early Luxembourg aggregate ministry/section totals were excluded from the "
                    "final panel after page-level review found mixed or non-matching sections. Rebuilding a "
                    "defendable pre-2001 aggregate series would require a fresh original-file audit rather than "
                    "reusing these traced totals."
                )
        if not subset.empty:
            traceability_status = "covered_by_final_series"
        elif "mislabelled" in source_note.lower() or "misfiled" in source_note.lower():
            traceability_status = "source_audited_misfiled"
        elif (
            "does not contain the national budget annex" in source_note.lower()
            or "not the numeric state-budget annex" in source_note.lower()
            or "does not include the numeric annex tables" in source_note.lower()
            or "not the detailed expenditure annex" in source_note.lower()
        ):
            traceability_status = "source_audited_not_budget_annex"
        else:
            traceability_status = "not_used_in_final_series"
        rows.append(
            {
                "source_file": source_file,
                "pdf_path": str(pdf_root / source_file),
                "full_text_cache": _find_full_text_cache(country, source_file),
                "selected_rows": int(len(subset)),
                "selected_canonicals": " | ".join(subset["canonical_name"].astype(str).tolist()) if not subset.empty else "",
                "selected_pages": " | ".join(map(str, subset["page_number"].tolist())) if not subset.empty else "",
                "selected_line_descriptions": " | ".join(subset["line_description_en"].fillna("").astype(str).tolist()) if not subset.empty else "",
                "selected_amounts": " | ".join(subset["amount_local"].astype(str).tolist()) if not subset.empty else "",
                "traceability_status": traceability_status,
                "source_audit_note": source_note,
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["source_file"], kind="stable").reset_index(drop=True)
#
# Some countries change the denomination they use in budget documents over time.
# Rules here only apply when we intentionally rewrite the parsed amount to a
# different unit before building the series.
#
# Rules per country:
#   Australia:
#     - Pre-2000: full AUD dollars  → divide by 1000 → thousands
#     - 2000-2011: mixed (transition era, .doc files mostly missing)
#     - 2012+:     AUD thousands    → no change
#     Detection: if year <= 1999 AND amount_current > 500_000, it's in dollars.
#     (A genuine thousands-denominated amount above 500,000 would be $500M+
#     for a single agency in 1970s-1990s money — implausible for any R&D line.)

_UNIT_RULES: dict[str, list[dict]] = {
    "Australia": [
        {
            "years": (1900, 1999),
            "threshold": 1_000,     # amounts above $1,000 full dollars → divide
            # Was 500,000 but that missed mid-range amounts like $364,500
            # (Anglo-Australian Telescope) which stayed as 364,500 thousands = $364M.
            # For pre-2000 Finance Bills ALL amounts are in full AUD dollars.
            "divisor": 1_000,       # divide by 1000 to get thousands
            "note": "AU pre-2000: dollar→thousand conversion",
        },
    ],
    # Canada: Appropriation Act text files are in full CAD dollars for all years.
    # Keep the printed full-dollar amount in outputs, with unit='dollar'.
    "Canada": [],
    # -------------------------------------------------------------------------
    # UK: Supply Estimates txt.gz amounts appear to be in thousands of GBP (£000).
    # No conversion needed. Add a rule here if first-run amounts look wrong.
    # "UK": [],

    # -------------------------------------------------------------------------
    # France: Loi de Finances amounts — verify unit after first run.
    # Typically reported in millions of EUR (post-2002) / millions of FRF (pre-2002).
    # If amounts in the series look like millions (e.g. CNRS ≈ 500) instead of
    # thousands (≈ 500,000), enable this rule:
    # "France": [
    #     {"years": (1900, 2099), "threshold": 0, "divisor": 0.001,
    #      "note": "FR budget: millions → thousands conversion"},
    # ],

    # -------------------------------------------------------------------------
    # Germany: Bundeshaushalt amounts are in thousands of EUR (Tausend Euro).
    # No conversion needed — already in thousands. Add rule if this proves wrong.
    # "Germany": [],

    # -------------------------------------------------------------------------
    # Japan: Budget amounts are in millions of yen (百万円).
    # After first run, enable this rule if series values look like millions:
    # "Japan": [
    #     {"years": (1900, 2099), "threshold": 0, "divisor": 0.001,
    #      "note": "JP budget: millions yen → thousands conversion"},
    # ],
}


def _normalise_units(df: pd.DataFrame, country: str) -> pd.DataFrame:
    """
    Normalise amount_current to thousands of local currency.
    Applies country-specific rules from _UNIT_RULES.
    Adds a 'unit_note' column where corrections were made.
    """
    rules = _UNIT_RULES.get(country, [])
    if not rules:
        return df

    df = df.copy()
    df["amount_current"] = pd.to_numeric(df["amount_current"], errors="coerce")
    if "unit_note" not in df.columns:
        df["unit_note"] = ""

    total_fixed = 0
    for rule in rules:
        year_lo, year_hi = rule["years"]
        threshold = rule["threshold"]
        divisor = rule["divisor"]
        note = rule["note"]

        mask = (
            (df["year"] >= year_lo)
            & (df["year"] <= year_hi)
            & (df["amount_current"] > threshold)
        )
        n = mask.sum()
        if n:
            df.loc[mask, "amount_current"] = df.loc[mask, "amount_current"] / divisor
            df.loc[mask, "unit_note"] = note
            total_fixed += n

    if total_fixed:
        logger.info(
            f"[{country}] Unit normalisation: {total_fixed} amounts converted to thousands"
        )

    return df


def _output_unit_for_country(country: str) -> str:
    """Return the unit used for compile outputs after country-specific handling."""
    if country == "Canada":
        return "dollar"
    if country == "Colombia":
        return "unit"
    if country == "Luxembourg":
        return "unit"
    return "thousand"


def _materialize_country_output_units(df: pd.DataFrame, country: str) -> pd.DataFrame:
    """
    Expand country-specific scaled units into full local-currency amounts for
    human-auditable output files.

    This keeps the country-level `*_docx_results.csv` directly comparable to the
    final series/totals outputs instead of mixing `thousand` with base units.
    """
    if df.empty or country != "Italy":
        return df

    out = df.copy()
    year_num = pd.to_numeric(out.get("year", pd.Series(dtype=float)), errors="coerce")
    amount_num = pd.to_numeric(out.get("amount_local", pd.Series(dtype=float)), errors="coerce")
    currency = out.get("currency", pd.Series("", index=out.index)).fillna("").astype(str).str.upper()
    unit_norm = out.get("unit", pd.Series("", index=out.index)).fillna("").astype(str).str.strip().str.lower()

    pre_euro_any = year_num.le(2001) & currency.eq("ITL")
    pre_euro = pre_euro_any & amount_num.notna()
    pre_full_lira = pre_euro & unit_norm.eq("thousand") & amount_num.ge(10_000_000)
    pre_million_lira = pre_euro & unit_norm.eq("thousand") & amount_num.lt(10_000_000)
    pre_literal = pre_euro_any & unit_norm.isin(["", "unit", "lira", "lire"])
    pre_missing_scaled = pre_euro_any & unit_norm.eq("thousand") & amount_num.isna()

    if pre_full_lira.any():
        out.loc[pre_full_lira, "unit"] = "lira"
    if pre_million_lira.any():
        out.loc[pre_million_lira, "amount_local"] = amount_num.loc[pre_million_lira] * 1_000_000.0
        out.loc[pre_million_lira, "unit"] = "lira"
    if pre_literal.any():
        out.loc[pre_literal, "unit"] = "lira"
    if pre_missing_scaled.any():
        out.loc[pre_missing_scaled, "unit"] = "lira"

    post_euro_any = year_num.ge(2002) & currency.eq("EUR")
    post_euro = post_euro_any & amount_num.notna()
    post_thousand = post_euro & unit_norm.eq("thousand")
    post_literal = post_euro_any & unit_norm.isin(["", "unit", "euro"])
    post_missing_scaled = post_euro_any & unit_norm.eq("thousand") & amount_num.isna()
    if post_thousand.any():
        out.loc[post_thousand, "amount_local"] = amount_num.loc[post_thousand] * 1000.0
        out.loc[post_thousand, "unit"] = "euro"
    if post_literal.any():
        out.loc[post_literal, "unit"] = "euro"
    if post_missing_scaled.any():
        out.loc[post_missing_scaled, "unit"] = "euro"

    return out


def _filter_country_raw_noise(df: pd.DataFrame, country: str) -> pd.DataFrame:
    """
    Drop obviously non-entity legislative/table artefacts before entity dedup.

    These rows come from OCR/text-cache parsing of acts and schedules and can
    explode the per-year entity set, causing unnecessary LLM dedup work.
    """
    if df.empty or "entity_raw" not in df.columns:
        return df

    if country == "Chile":
        entity_upper = df["entity_raw"].fillna("").astype(str).str.upper().str.strip()
        noise_patterns = [
            r"^\s*[-–—]+\s*SUBSECRETAR[IÍ]A\b",
            r"^\s*SUBSECRETAR[IÍ]A\b",
            r"^\s*DIRECCI[OÓ]N DE SANIDAD\b",
            r"^\s*OFICINA DE ESTUDIOS Y POL[IÍ]TICAS AGRARIAS\b",
            r"\bINGRESOS\b",
            r"\bPATENTES\b",
            r"\bGOBIERNO REGIONAL\b",
            r"\bBONIFICACI[OÓ]N\b",
            r"\bRECONVERSI[OÓ]N\b",
            r"\bPOLIC[IÍ]A FORESTAL\b",
            r"\bPROGRAMA\s+0\d+\b",
            r"\bSUBSECRETAR[IÍ]A DE BIENES NACIONALES\b",
            r"\bSUBSECRETAR[IÍ]A DE VIVIENDA Y URBANISMO\b",
            r"\bSUBSECRETAR[IÍ]A DE PLANIFICACI[OÓ]N Y COOPERACI[OÓ]N\b",
            r"\bCOMISI[OÓ]N NACIONAL DEL MEDIO AMBIENTE\b",
        ]
        mask = pd.Series(False, index=df.index)
        for pattern in noise_patterns:
            mask = mask | entity_upper.str.contains(pattern, regex=True, na=False)
        filtered = df.loc[~mask].copy()
        dropped = int(mask.sum())
        if dropped:
            logger.info(f"[{country}] Dropped {dropped} Chile raw noise rows before dedup/classification")
        return filtered

    if country != "Canada":
        return df

    entity_upper = df["entity_raw"].fillna("").astype(str).str.upper().str.strip()
    noise_patterns = [
        r"^\s*ANNEXE\b",
        r"^\s*SCHEDULE\b",
        r"^\s*PARTIE\b",
        r"^\s*PART\b",
        r"^\s*DIVISION\b",
        r"^\s*SECTION\b",
        r"^\s*CHAPTER\b",
        r"\bTABLE OF PROVISIONS\b",
        r"\bCOMING INTO FORCE\b",
        r"\bDISPOSITIONS G[ÉE]N[ÉE]RALES\b",
        r"\bGENERAL PROVISIONS\b",
        r"\bINTERPR[ÉE]TATION\b",
        r"^\s*CUSTOMS TARIFF\b",
        r"^\s*TARIF DES DOUANES\b",
        r"\bINCOME TAX ACT\b",
        r"\bLOI DE L['’]IMP[ÔO]T SUR LE REVENU\b",
        r"^\s*DEPARTMENT\s*$",
        r"^\s*MINIST[ÈE]RE\s*$",
    ]

    noise_mask = pd.Series(False, index=df.index)
    for pattern in noise_patterns:
        noise_mask = noise_mask | entity_upper.str.contains(pattern, regex=True, na=False)

    removed = int(noise_mask.sum())
    if removed:
        logger.info(f"[{country}] Raw-row noise filter removed {removed} legislative/header rows")
        return df.loc[~noise_mask].copy()

    return df


# ---------------------------------------------------------------------------
# Step 1 — Parse all DOCX files → raw_rows.csv
# ---------------------------------------------------------------------------

RAW_ROW_COLUMNS = [
    "country", "year", "source_file", "table_index", "row_index",
    "section_name", "entity_raw", "amount_current", "amount_prior",
    "is_header_row", "is_total_row", "has_italic_entity", "cells_raw",
]


def parse_to_raw_rows(
    country: str,
    year_range: Optional[tuple[int, int]] = None,
    output_csv: Path = RAW_ROWS_CSV,
) -> pd.DataFrame:
    """
    Parse all source files for a country and write raw_rows.csv.

    Source priority:
      1. DOCX files under Data/input/finance_bills/{country}/
      2. Pre-extracted text cache under Data/output/budget/full_text/{country}/
         (used when the source is PDF and text has already been extracted)

    Returns DataFrame of rows with current-year amounts only.
    """
    rows = parse_country_docx_files(
        pdf_root=cfg.PDF_ROOT,
        country=country,
        year_range=year_range,
    )

    # If DOCX parsing produced nothing (or very little), try the text cache.
    # This covers countries where finance bills are PDFs, not DOCX.
    text_cache_country_dir = TEXT_CACHE_DIR / country
    if text_cache_country_dir.exists():
        docx_years = {r.year for r in rows if r.amount_current is not None}
        cache_rows = parse_text_cache(country=country, year_range=year_range)

        # Only add cache rows for years NOT already covered by DOCX
        new_rows = [r for r in cache_rows if r.year not in docx_years]
        if new_rows:
            logger.info(
                f"[{country}] Text cache added {len(new_rows)} rows "
                f"for years not in DOCX: "
                f"{sorted({r.year for r in new_rows})}"
            )
            rows = rows + new_rows

    # Supplement with LLM pipeline output (results.csv) for years still missing
    # after DOCX + text-cache parsing. This matters most for true biannual/biennial
    # source documents (e.g. Hungary's "2001-2002 ... törvény.pdf"), where
    # _parse_fiscal_year()/text_cache_parser assign the ENTIRE file to a single
    # fiscal year — the deterministic regex parser has no way to split a
    # multi-year law into per-year appropriation columns. The LLM extraction
    # pipeline (budget/pipeline.py) already re-processes such files once per
    # covered year via its own biannual-aware doc_hint tagging, so its output
    # legitimately has rows for the "second" year the deterministic parser can
    # never produce. Only years with ZERO rows from DOCX/text-cache are filled
    # this way — this never overwrites or competes with existing data.
    covered_years = {r.year for r in rows if r.amount_current is not None}
    results_csv = cfg.OUTPUT_DIR / "results.csv"
    if results_csv.exists():
        try:
            pipeline_df = pd.read_csv(results_csv, low_memory=False)
        except Exception as exc:
            pipeline_df = pd.DataFrame()
            logger.warning(f"[{country}] Could not read {results_csv} for gap-year supplement: {exc}")
        if not pipeline_df.empty and "country" in pipeline_df.columns:
            pdf_country = pipeline_df[pipeline_df["country"] == country].copy()
            if not pdf_country.empty:
                pdf_country["year"] = pd.to_numeric(pdf_country["year"], errors="coerce")
                pdf_country = pdf_country.dropna(subset=["year"])
                pdf_country["year"] = pdf_country["year"].astype(int)
                if year_range:
                    pdf_country = pdf_country[
                        (pdf_country["year"] >= year_range[0]) & (pdf_country["year"] <= year_range[1])
                    ]
                if "decision" in pdf_country.columns:
                    pdf_country = pdf_country[pdf_country["decision"].isin(["include", "review"])]
                missing_years = sorted(set(pdf_country["year"].unique()) - covered_years)
                if missing_years:
                    supplement = pdf_country[pdf_country["year"].isin(missing_years)]
                    supplement_rows = []
                    for _, r in supplement.iterrows():
                        amount = pd.to_numeric(r.get("amount_local"), errors="coerce")
                        if pd.isna(amount):
                            continue
                        # NOTE: deliberately NOT applying a unit-scale multiplier here.
                        # results.csv's 'unit' field (thousand/million/...) was tried and
                        # made things worse for Hungary — the raw_rows/canonical_series
                        # pipeline downstream evidently expects amount_local as printed,
                        # not pre-scaled. Verified this is correct for Hungary 2002 (the
                        # only year this path currently fires for); if this supplement is
                        # ever extended to other countries, re-check unit handling first.
                        entity = str(r.get("line_description") or r.get("line_description_en") or "").strip()
                        if not entity:
                            entity = str(r.get("section_name") or r.get("section_name_en") or "").strip()
                        if not entity:
                            continue
                        supplement_rows.append(RawRow(
                            source_file=str(r.get("source_file", "")),
                            country=country,
                            year=int(r["year"]),
                            section_name=str(r.get("section_name") or r.get("section_name_en") or ""),
                            entity_raw=entity,
                            amount_current=float(amount),
                            is_total_row=str(r.get("item_type", "")) == "section_total",
                            cells_raw=[],
                        ))
                    if supplement_rows:
                        logger.info(
                            f"[{country}] LLM pipeline output (results.csv) added {len(supplement_rows)} rows "
                            f"for year(s) entirely missing from DOCX/text-cache parsing: {missing_years} "
                            f"(e.g. biannual source documents the deterministic parser cannot split by year)"
                        )
                        rows = rows + supplement_rows

                # NOTE: an earlier version of this function also auto-inserted Hungary
                # MTA (Hungarian Academy of Sciences) rows recovered from results.csv
                # for years where text_cache_parser loses the chapter total to
                # multi-column fragmentation (2000, 2003, 2004, 2006, 2008, 2009).
                # That was reverted: results.csv's amount_local/unit combination for
                # these specific rows produces internally inconsistent scale (some
                # rows read as thousands, others as if already in full HUF, and mixing
                # them created outliers off by up to 1000x). Silently inserting
                # unverified numbers into a research series is worse than leaving the
                # gap — see gap_detector._hungary_gap_diagnosis_from_full_text, which
                # now points at the same candidate rows for manual verification
                # against the original PDF instead.

    # Filter: only rows with a current-year amount and a non-empty entity
    data_rows = [
        r for r in rows
        if r.amount_current is not None
        and r.entity_raw.strip()
        and not r.is_header_row
    ]

    logger.info(
        f"[{country}] {len(rows)} total rows parsed, "
        f"{len(data_rows)} with current-year amounts + entity name"
    )

    if not data_rows:
        return pd.DataFrame()

    df = pd.DataFrame([r.to_dict() for r in data_rows])
    df = _filter_country_raw_noise(df, country=country)

    # ── Deduplication ────────────────────────────────────────────────────────
    # Modern DOCX budgets repeat each agency 3× per file:
    #   1. Portfolio summary table — bare entity name
    #   2. Portfolio summary table — outcome/description row (same amount)
    #   3. Individual agency table — "Total: <Agency>" row
    #
    # Strategy: for each (source_file, year, amount_current) group, keep only
    # ONE row, preferring in this order:
    #   a. "Total:" rows (most explicit — entity's own table)
    #   b. Rows where entity_raw matches amount context (not a description row)
    #   c. First occurrence
    #
    # Cross-file dedup (No1 vs No2 supplementary): for each (country, year,
    # entity_clean), keep the row from the lowest Act number.

    import re as _re

    _ACT_NO = _re.compile(r"\bNo\.?\s*(\d+)\b", re.IGNORECASE)

    def _act_num(fname):
        m = _ACT_NO.search(str(fname))
        return int(m.group(1)) if m else 999

    def _clean_entity(text):
        """Strip 'Total:' prefix and normalise for matching."""
        return _re.sub(r"^total[:\s]+", "", str(text), flags=_re.IGNORECASE).strip().lower()

    df["_act_no"] = df["source_file"].apply(_act_num)
    df["_entity_clean"] = df["entity_raw"].apply(_clean_entity)
    df["_is_total_row"] = df["entity_raw"].str.lower().str.startswith("total")

    # Within-file dedup: same (source_file, amount_current) → keep Total: row
    df = (
        df.sort_values(["_is_total_row"], ascending=False)  # Total: rows first
          .drop_duplicates(subset=["source_file", "amount_current", "_entity_clean"], keep="first")
    )

    # Cross-file dedup: same (country, year, entity_clean, amount) → keep lowest Act number
    df = (
        df.sort_values("_act_no")
          .drop_duplicates(subset=["country", "year", "_entity_clean", "amount_current"], keep="first")
    )

    n_after = len(df)
    logger.info(
        f"[{country}] After dedup: {n_after} unique rows "
        f"(removed {len(data_rows) - n_after} duplicates)"
    )

    df = df.drop(columns=["_act_no", "_entity_clean", "_is_total_row"])

    # Write
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    # Append or create
    if output_csv.exists():
        existing = pd.read_csv(output_csv)
        # Remove existing rows for this country (will re-add)
        existing = existing[existing["country"] != country]
        df = pd.concat([existing, df], ignore_index=True)

    df.to_csv(output_csv, index=False)
    logger.info(f"Raw rows written: {output_csv} ({len(df)} total rows)")

    return df[df["country"] == country].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Step 2 — Classify entities via agency_registry
# ---------------------------------------------------------------------------

def classify_raw_rows(
    raw_df: pd.DataFrame,
    config: dict,
    country: str,
    registry_file: Path = REGISTRY_FILE,
    dry_run: bool = False,
) -> pd.DataFrame:
    """
    For each unique entity_raw in raw_df, ensure it has an entry in
    agency_registry.csv. New entities are classified via LLM (once, cached).

    Returns updated registry DataFrame.
    """
    # Build a minimal results_df shaped like what agency_classifier expects
    # It needs: country, section_name_en, line_description_en, item_type
    has_total_rows = raw_df["is_total_row"].fillna(False).astype(bool).any()
    item_type = raw_df["is_total_row"].map({True: "section_total", False: "line_item"})
    if not has_total_rows:
        item_type = pd.Series("program_total", index=raw_df.index)

    results_like = pd.DataFrame({
        "country": raw_df["country"],
        "section_name_en": raw_df["section_name"],
        "line_description_en": raw_df["entity_raw"],
        "item_type": item_type,
        "decision": "include",  # dummy — classifier ignores this
    })

    registry = classify_agencies(
        results_df=results_like,
        config=config,
        country=country,
        registry_file=registry_file,
        dry_run=dry_run,
    )
    return registry


# ---------------------------------------------------------------------------
# Step 3 — Join raw rows × registry → classified results
# ---------------------------------------------------------------------------

def build_classified_results(
    raw_df: pd.DataFrame,
    registry: pd.DataFrame,
    country: str,
) -> pd.DataFrame:
    """
    Join raw rows against the agency registry to produce a classified
    results DataFrame compatible with canonical_series.build_canonical_series().

    Output columns match OUTPUT_COLUMNS in config.py.
    """
    registry_country = registry[registry["country"] == country].copy()

    # Build lookup: entity_name → registry row
    reg_lookup = {}
    for _, reg_row in registry_country.iterrows():
        reg_lookup[str(reg_row["agency_name"]).strip().lower()] = reg_row

    records = []
    for _, row in raw_df.iterrows():
        entity = str(row["entity_raw"]).strip()
        entity_lower = entity.lower()

        # Look up in registry (exact match first, then partial)
        reg = reg_lookup.get(entity_lower)
        if reg is None:
            # Partial match with two-layer validation:
            #
            # Layer 1 — word overlap against the entity name:
            #   Shared words must cover ≥60% of the longer name's words.
            #   "and technology" (2w) vs "australian nuclear science and technology" (5w)
            #   → 2/5 = 40% → rejected.
            #
            # Layer 2 — surrounding context check:
            #   If Layer 1 passes but the match is still below 80%, check that
            #   words from the registry name that are NOT in the entity also
            #   appear in the row's surrounding context (section_name, cells_raw).
            #   This catches truncated cells: "Australian Nuclear Science and"
            #   matched against "Australian Nuclear Science and Technology Organisation"
            #   — the missing word "Organisation" might not matter, but if
            #   "technology" appears in the section heading we boost confidence.
            context_lower = (
                str(row.get("section_name", "")) + " " +
                str(row.get("cells_raw", ""))
            ).lower()

            best_reg = None
            best_score = 0.0
            for reg_name, reg_row in reg_lookup.items():
                if reg_name not in entity_lower and entity_lower not in reg_name:
                    continue
                entity_words = set(entity_lower.split())
                reg_words = set(reg_name.split())
                shared = entity_words & reg_words
                longer = max(len(entity_words), len(reg_words))
                if longer == 0:
                    continue
                overlap = len(shared) / longer
                if overlap < 0.6:
                    continue

                # Layer 2: for borderline matches (60–80%), check that missing
                # registry words appear in the surrounding row context.
                if overlap < 0.80:
                    missing_words = reg_words - entity_words
                    context_hits = sum(1 for w in missing_words if w in context_lower)
                    # Boost overlap score by context hits
                    overlap += context_hits * 0.1

                if overlap > best_score:
                    best_score = overlap
                    best_reg = reg_row
            reg = best_reg

        if reg is None:
            # Not yet classified → review
            decision = "review"
            rd_category = "unclear"
            confidence = 0.5
            canonical_name = entity
        else:
            agency_type = str(reg.get("agency_type", "unclear"))
            include = str(reg.get("include_in_series", "false")).lower() in ("true", "1", "yes")

            if agency_type == "mixed_ministry":
                # Mixed ministries: totals are review, line items may be include
                decision = "review"
                confidence = 0.6
            elif include or agency_type in ("dedicated_rd", "rd_programme"):
                decision = "include"
                confidence = 0.9
            else:
                decision = "review"
                confidence = 0.5

            rd_category = {
                "dedicated_rd": "science_agency",
                "rd_programme": "direct_rd",
                "mixed_ministry": "unclear",
                "unclear": "unclear",
            }.get(agency_type, "unclear")

            canonical_name = str(reg.get("canonical_name", entity))

        # Detect item type from entity text
        entity_text = str(row["entity_raw"])
        is_total = str(row.get("is_total_row", "")).lower() in ("true", "1")
        item_type = "section_total" if is_total else "line_item"

        currency = cfg.COUNTRY_CONTEXT.get(country, {}).get("currency", "LOCAL")
        if country == "Chile":
            amount_local = pd.to_numeric(row.get("amount_current"), errors="coerce")
            amount_prior = pd.to_numeric(row.get("amount_prior"), errors="coerce")
            unit = "thousand"
        elif country == "Costa Rica":
            # Costa Rica text-cache parsing already yields amounts in the final
            # reporting unit used elsewhere in the pipeline: thousand CRC.
            # Expanding here to full CRC and then still carrying unit='thousand'
            # inflates the entire docx_* branch by 1,000x.
            amount_local = pd.to_numeric(row.get("amount_current"), errors="coerce")
            amount_prior = pd.to_numeric(row.get("amount_prior"), errors="coerce")
            unit = "thousand"
        else:
            amount_local, unit = _expand_output_amount(
                row["amount_current"],
                _output_unit_for_country(country),
                currency,
            )
            amount_prior, _ = _expand_output_amount(
                row.get("amount_prior"),
                _output_unit_for_country(country),
                currency,
            )

        records.append({
                "country": row["country"],
                "year": row["year"],
                "source_file": row["source_file"],
                "page_number": str(row.get("table_index", "")),
                "item_type": item_type,
            "section_code": "",
                "section_name": str(row.get("section_name", "")),
                "section_name_en": canonical_name if not is_total else str(row.get("section_name", "")),
                "line_code": "",
                "line_description": entity_text,
                "line_description_en": canonical_name if not is_total else entity_text,
                "amount_local": amount_local,
                "amount_prior": amount_prior,
                "unit": unit,
                "currency": currency,
                "rd_category": rd_category,
                "decision": decision,
                "confidence": confidence,
            "llm_model": "docx_parser",
            "extraction_pass": "docx_table",
            "notes": f"Parsed from table {row.get('table_index')}, row {row.get('row_index')}",
        })

    df = pd.DataFrame(records)
    logger.info(
        f"[{country}] Classified {len(df)} rows: "
        f"include={len(df[df['decision']=='include'])}, "
        f"review={len(df[df['decision']=='review'])}"
    )
    return df


# ---------------------------------------------------------------------------
# Full audit database builder
# ---------------------------------------------------------------------------

def _build_full_audit(
    raw_df: pd.DataFrame,
    series_df: pd.DataFrame,
    country: str,
) -> pd.DataFrame:
    """
    Build a full audit database showing every appearance of every canonical agency
    across all documents, years, and Acts.

    For each canonical agency (from hardcoded + discovered registries), searches
    raw_df for all rows matching any of the agency's name_variants. Returns a
    DataFrame sorted by (canonical_name, year, source_file) so you can see the
    full history of e.g. 'Australian Institute of Marine Science' across every
    budget document.

    Columns:
      canonical_name   — standardised agency name
      category         — R&D category from canonical definition
      year             — budget year
      source_file      — filename of the source document
      entity_raw       — exact text as it appeared in the document
      amount_current   — amount extracted after country-specific unit handling
      section_name     — table/section heading in the document
      table_index      — table number within the file
      row_index        — row number within the table
      in_series        — True if this row contributed to the canonical series
    """
    from budget.canonical_series import _get_agencies_for_country

    agencies = _get_agencies_for_country(country)
    if not agencies:
        return pd.DataFrame()

    country_raw = raw_df[raw_df["country"] == country].copy()
    if country_raw.empty:
        return pd.DataFrame()

    # Build a quick set of (canonical_name, year) pairs that made it into the series
    if not series_df.empty:
        in_series_keys = set(
            zip(series_df["canonical_name"], series_df["year"])
        )
    else:
        in_series_keys = set()

    def _audit_entity_matches(entity_text: str, variants: list[str]) -> bool:
        entity_lower = str(entity_text or "").lower().strip()
        if not entity_lower:
            return False
        for variant in variants:
            v = str(variant or "").lower().strip()
            if not v:
                continue
            if len(v) <= 4:
                if re.search(r"(?<![a-z])" + re.escape(v) + r"(?![a-z])", entity_lower):
                    return True
            elif v in entity_lower:
                return True
        return False

    records = []
    for agency in agencies:
        canonical_name = agency["canonical_name"]
        category = agency.get("category", "")
        variants = [v.lower() for v in agency.get("name_variants", [canonical_name])]

        for _, row in country_raw.iterrows():
            if not _audit_entity_matches(row["entity_raw"], variants):
                continue

            records.append({
                "canonical_name": canonical_name,
                "category": category,
                "year": row["year"],
                "source_file": row.get("source_file", ""),
                "entity_raw": row["entity_raw"],
                "amount_current": row["amount_current"],
                "section_name": row.get("section_name", ""),
                "table_index": row.get("table_index", ""),
                "row_index": row.get("row_index", ""),
                "in_series": (canonical_name, row["year"]) in in_series_keys,
            })

    if not records:
        return pd.DataFrame()

    audit_df = pd.DataFrame(records).sort_values(
        ["canonical_name", "year", "source_file"]
    ).reset_index(drop=True)

    logger.info(
        f"[{country}] Audit database: {len(audit_df)} rows across "
        f"{audit_df['canonical_name'].nunique()} agencies, "
        f"{audit_df['year'].nunique()} years"
    )
    return audit_df


# ---------------------------------------------------------------------------
# Pipeline output reader — for narrative PDF countries (UK, France, Germany, Japan)
# ---------------------------------------------------------------------------

def _load_pipeline_results(
    results_csv: Path,
    country: str,
    year_range: Optional[tuple[int, int]] = None,
) -> pd.DataFrame:
    """
    Read pipeline.py's results.csv and return rows for the given country/year range.

    The pipeline output already has line_description_en, section_name_en,
    amount_local, item_type, rd_category, decision, confidence — everything
    build_canonical_series() needs. We just filter and return.
    """
    try:
        df = pd.read_csv(results_csv)
    except Exception as e:
        logger.warning(f"Could not read pipeline results {results_csv}: {e}")
        return pd.DataFrame()

    if "country" not in df.columns:
        return pd.DataFrame()

    df = df[df["country"] == country].copy()
    if df.empty:
        return df

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)

    if year_range:
        df = df[(df["year"] >= year_range[0]) & (df["year"] <= year_range[1])]

    # Ensure required columns are present (pipeline output may have extra or missing)
    for col in ["line_description_en", "section_name_en", "line_description",
                "amount_local", "unit", "currency", "item_type",
                "decision", "confidence", "source_file", "page_number", "rd_category"]:
        if col not in df.columns:
            df[col] = ""

    df["amount_local"] = pd.to_numeric(df["amount_local"], errors="coerce")

    if country == "Israel" and not df.empty:
        year_num = pd.to_numeric(df["year"], errors="coerce")
        # Israel requires only one compile-side unit correction here:
        # 1975-1979 are statutory lira amounts in full units, not thousands.
        # Later Israeli budget tables should keep the printed scale from the
        # source page. In particular, do not rewrite 2021-2024 away from
        # `thousand`: the original budget family continues to use
        # "באלפי שקלים חדשים" / "thousands of new shekels".
        df.loc[year_num.between(1975, 1979, inclusive="both"), "unit"] = "unit"

    # Apply the same country cleaner used by pipeline postprocess so compile can
    # safely consume root results.csv without requiring a separate cleaner pass.
    try:
        from budget.cleaners import apply_country_cleaner
        df = apply_country_cleaner(df, country=country)
    except Exception as exc:
        logger.warning(f"[{country}] Country cleaner failed during compile ingest: {exc}")

    # Keep only include/review rows (skip rows were explicitly rejected)
    df = df[df["decision"].isin(["include", "review"])].copy()

    if country == "France":
        df = _augment_france_pipeline_rows(df)
        df = _trim_france_etpt_rows(df)
        df = _normalise_france_pre_lolf_rows(df)

    if country == "Finland":
        # Finland 2002: two source files are duplicates of the same budget document
        # ("2002 Finland budget download.jsp.pdf" and "2002 download.jsp.pdf").
        # Keep only the more descriptive filename to avoid double-counting in totals.
        dup_source = "2002 download.jsp.pdf"
        full_source = "2002 Finland budget download.jsp.pdf"
        has_full = df["source_file"].astype(str).str.contains(
            re.escape(full_source), regex=True, na=False
        ).any()
        if has_full:
            n_before = len(df)
            df = df[~(df["source_file"].astype(str) == dup_source)].copy()
            dropped = n_before - len(df)
            if dropped:
                logger.info(
                    f"[Finland] Dropped {dropped} rows from duplicate 2002 source "
                    f"'{dup_source}' (same content as '{full_source}')"
                )

    # Drop rows marked as redundant aggregates (ministry totals, section totals).
    # These are kept in results.csv for audit purposes but should not feed the series.
    if "aggregation_role" in df.columns:
        if country in {"France", "UK", "Germany"}:
            # France's JORF programme totals and the UK's budget-package totals
            # are often tagged as redundant during cleaning to suppress discovery
            # noise, but compile/canonical still needs them to build the final
            # programme/package series.
            # Germany: Beschlussempfehlung / Haushaltsübersicht docs produce only
            # section-level totals (Epl.30 = BMBF total); without these rows
            # those years have zero canonical matches.
            logger.info(f"[{country}] Keeping review/redundant programme rows for canonical series input")
        else:
            n_before = len(df)
            df = df[df["aggregation_role"].fillna("") != "redundant"].copy()
            dropped = n_before - len(df)
            if dropped:
                logger.info(f"[{country}] Dropped {dropped} redundant aggregate rows from series input")

    logger.info(
        f"[{country}] Loaded {len(df)} rows from pipeline output "
        f"({results_csv.name}), years: {sorted(df['year'].unique().tolist())}"
    )
    return df.reset_index(drop=True)


def _load_country_results_snapshot(
    results_csv: Path,
    year_range: Optional[tuple[int, int]] = None,
) -> pd.DataFrame:
    """
    Load an existing per-country compile snapshot if one already exists.

    This is a safety net for countries where a prior country-local CSV is
    materially richer than the shared root results.csv. It prevents compile
    reruns from overwriting a stronger audited country snapshot with a stale
    global ledger slice.
    """
    try:
        df = pd.read_csv(results_csv)
    except Exception:
        return pd.DataFrame()

    if "year" not in df.columns:
        return pd.DataFrame()

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    if df.empty:
        return df
    df["year"] = df["year"].astype(int)

    if year_range:
        df = df[(df["year"] >= year_range[0]) & (df["year"] <= year_range[1])]

    for col in [
        "line_description_en",
        "section_name_en",
        "line_description",
        "amount_local",
        "unit",
        "currency",
        "item_type",
        "decision",
        "confidence",
        "source_file",
        "page_number",
        "rd_category",
    ]:
        if col not in df.columns:
            df[col] = ""

    df["amount_local"] = pd.to_numeric(df["amount_local"], errors="coerce")
    if "decision" in df.columns:
        df = df[df["decision"].isin(["include", "review"])].copy()

    logger.info(
        f"Loaded {len(df)} rows from existing country snapshot "
        f"({results_csv.name}), years: {sorted(df['year'].unique().tolist()) if not df.empty else []}"
    )
    return df.reset_index(drop=True)


def _load_compile_recovery_rows(
    results_csv: Path,
    country: str,
    year_range: Optional[tuple[int, int]] = None,
) -> pd.DataFrame:
    """
    Load document-level targeted recovery rows and convert them into compile-style
    raw rows.

    For Canada this lets a cheap `budget.pipeline --targeted-recovery-only` pass
    feed the real compile/database path without rerunning full extraction.
    Only total-like recovery rows are imported to avoid double-counting
    component lines such as operating/capital.
    """
    try:
        df = pd.read_csv(results_csv)
    except Exception as e:
        logger.warning(f"Could not read pipeline recovery rows {results_csv}: {e}")
        return pd.DataFrame()

    if "country" not in df.columns or "extraction_pass" not in df.columns:
        return pd.DataFrame()

    df = df[
        (df["country"] == country)
        & (df["extraction_pass"] == "targeted_recovery")
    ].copy()
    if df.empty:
        return df

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["amount_local"] = pd.to_numeric(df["amount_local"], errors="coerce")
    df = df.dropna(subset=["year", "amount_local", "section_name"])
    df["year"] = df["year"].astype(int)

    if year_range:
        df = df[(df["year"] >= year_range[0]) & (df["year"] <= year_range[1])]

    total_mask = (
        df.get("line_description", pd.Series(index=df.index, dtype="object"))
        .astype(str)
        .str.contains(r"\btotal\b", case=False, na=False)
    )
    df = df[total_mask].copy()
    if df.empty:
        return df

    # Canada compile keeps full CAD dollars. However, `budget.pipeline --postprocess-only`
    # normalises results.csv amounts to thousands in-place. If recovery rows were
    # postprocessed before compile, convert them back to full dollars here.
    if country == "Canada" and "unit" in df.columns:
        unit_lower = df["unit"].astype(str).str.lower().str.strip()
        thousand_mask = unit_lower.eq("thousand")
        if thousand_mask.any():
            df.loc[thousand_mask, "amount_local"] = df.loc[thousand_mask, "amount_local"] * 1000.0

    out = pd.DataFrame({
        "source_file": df["source_file"].astype(str),
        "country": df["country"].astype(str),
        "year": df["year"].astype(int),
        "page_number": pd.to_numeric(df.get("page_number", 0), errors="coerce").fillna(0).astype(int),
        "table_index": pd.to_numeric(df.get("page_number", 0), errors="coerce").fillna(0).astype(int),
        "row_index": range(len(df)),
        "section_name": df["section_name"].astype(str),
        "entity_raw": df["section_name"].astype(str),
        "amount_current": df["amount_local"].astype(float),
        "amount_prior": None,
        "cells_raw": (
            df["section_name"].astype(str)
            + " | "
            + df.get("line_description", "").astype(str)
            + " | "
            + df["amount_local"].astype(str)
        ),
        "is_header_row": False,
        "is_total_row": True,
        "has_italic_entity": False,
    })

    out = out.drop_duplicates(
        subset=["country", "year", "entity_raw", "amount_current", "source_file"],
        keep="first",
    ).reset_index(drop=True)

    logger.info(
        f"[{country}] Loaded {len(out)} targeted recovery total rows from "
        f"{results_csv.name}"
    )
    return out


def _load_pipeline_supplement_rows(
    results_csv: Path,
    country: str,
    year_range: Optional[tuple[int, int]] = None,
    target_years: Optional[set[int]] = None,
) -> pd.DataFrame:
    """
    Convert existing pipeline results.csv rows into compile-style raw rows.

    Used as a cheap fallback when deterministic text-cache parsing is materially
    weaker than already-available pipeline output for specific country-years.
    """
    try:
        df = pd.read_csv(results_csv)
    except Exception as e:
        logger.warning(f"Could not read pipeline supplement rows {results_csv}: {e}")
        return pd.DataFrame()

    if "country" not in df.columns:
        return pd.DataFrame()

    df = df[df["country"] == country].copy()
    if df.empty:
        return df

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["amount_local"] = pd.to_numeric(df["amount_local"], errors="coerce")
    df = df.dropna(subset=["year", "amount_local"])
    df["year"] = df["year"].astype(int)

    if year_range:
        df = df[(df["year"] >= year_range[0]) & (df["year"] <= year_range[1])]
    if target_years:
        df = df[df["year"].isin(target_years)]

    if df.empty:
        return df

    for col in ["decision", "line_description", "line_description_en", "section_name", "section_name_en", "source_file"]:
        if col not in df.columns:
            df[col] = ""

    df = df[df["decision"].isin(["include", "review"])].copy()
    if df.empty:
        return df

    entity_text = df["line_description"].fillna("").astype(str).str.strip()
    section_text = df["section_name"].fillna("").astype(str).str.strip()
    entity_en = df["line_description_en"].fillna("").astype(str).str.strip()
    section_en = df["section_name_en"].fillna("").astype(str).str.strip()

    # For section totals, the entity should be the institution/section heading.
    is_section_total = df.get("item_type", pd.Series("", index=df.index)).astype(str).eq("section_total")
    entity_raw = entity_text.where(~is_section_total, section_text)
    entity_raw = entity_raw.where(entity_raw.str.len() > 0, entity_en)
    entity_raw = entity_raw.where(entity_raw.str.len() > 0, section_en)
    entity_raw = entity_raw.where(entity_raw.str.len() > 0, section_text)

    out = pd.DataFrame({
        "source_file": df["source_file"].astype(str),
        "country": df["country"].astype(str),
        "year": df["year"].astype(int),
        "page_number": pd.to_numeric(df.get("page_number", 0), errors="coerce").fillna(0).astype(int),
        "table_index": pd.to_numeric(df.get("page_number", 0), errors="coerce").fillna(0).astype(int),
        "row_index": range(len(df)),
        "section_name": section_text.where(section_text.str.len() > 0, section_en),
        "entity_raw": entity_raw,
        "amount_current": df["amount_local"].astype(float),
        "amount_prior": None,
        "cells_raw": (
            section_text.where(section_text.str.len() > 0, section_en)
            + " | "
            + entity_text.where(entity_text.str.len() > 0, entity_en)
            + " | "
            + df["amount_local"].astype(str)
        ),
        "is_header_row": False,
        "is_total_row": is_section_total.fillna(False),
        "has_italic_entity": False,
    })

    out = out[out["entity_raw"].fillna("").astype(str).str.strip() != ""].copy()
    out = out.drop_duplicates(
        subset=["country", "year", "entity_raw", "amount_current", "source_file"],
        keep="first",
    ).reset_index(drop=True)

    logger.info(
        f"[{country}] Loaded {len(out)} pipeline supplement rows from "
        f"{results_csv.name}"
    )
    return out


# ---------------------------------------------------------------------------
# Main compile entry point
# ---------------------------------------------------------------------------

def compile_country(
    country: str,
    config: dict,
    year_range: Optional[tuple[int, int]] = None,
    output_dir: Path = cfg.OUTPUT_DIR,
    dry_run: bool = False,
    entity_dedup: bool = True,
    fill_gaps_flag: bool = False,
    fill_gaps_llm: bool = True,
) -> pd.DataFrame:
    """
    Full compile pipeline for one country.

    For countries with structured DOCX / pre-extracted text (Australia, Canada):
      1. Parse all DOCX files → raw_rows (deterministic, no LLM)
      2. Deterministic dedup (same amount × same file)
      3. LLM entity dedup — collapse truncated/cased name variants (Haiku, cached)
      4. Agency classifier — classify unique canonical names (Haiku, cached)
      5. Build classified results (code only)

    For narrative PDF countries (UK, France, Germany, Japan) where DOCX/text-cache
    parsing yields nothing, auto-detects LLM pipeline output in results.csv and
    uses that directly (skipping steps 1-5 — the pipeline already did that work).

      6. Build canonical series (code only)
      7. Gap detection — find missing (agency, year), outliers, reextract queue

    Returns series DataFrame.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Per-country outputs go into a subfolder; shared files stay at root.
    # e.g. Data/output/budget/Australia/australia_docx_series.csv
    country_dir = output_dir / country
    country_dir.mkdir(parents=True, exist_ok=True)
    country_results_csv = country_dir / f"{country.lower().replace(' ','_')}_docx_results.csv"

    raw_rows_csv = output_dir / "raw_rows.csv"  # shared across all countries

    logger.info(f"=== COMPILE: {country} ===")

    # ── Step 1+2: parse + deterministic dedup ────────────────────────────────
    raw_df = parse_to_raw_rows(
        country=country,
        year_range=year_range,
        output_csv=raw_rows_csv,
    )

    pipeline_csv = output_dir / "results.csv"

    def _append_compile_rows(base_df: pd.DataFrame, extra_df: pd.DataFrame, label: str) -> pd.DataFrame:
        if extra_df.empty:
            return base_df
        if not base_df.empty:
            existing_keys = set(
                zip(
                    base_df["country"].astype(str),
                    base_df["year"].astype(int),
                    base_df["entity_raw"].astype(str).str.upper(),
                    pd.to_numeric(base_df["amount_current"], errors="coerce").astype(float),
                )
            )
            extra_df = extra_df[
                ~extra_df.apply(
                    lambda r: (
                        str(r["country"]),
                        int(r["year"]),
                        str(r["entity_raw"]).upper(),
                        float(r["amount_current"]),
                    ) in existing_keys,
                    axis=1,
                )
            ].reset_index(drop=True)
        if extra_df.empty:
            return base_df
        merged_df = pd.concat([base_df, extra_df], ignore_index=True)
        logger.info(f"[{country}] Appended {len(extra_df)} {label} into compile input")
        return merged_df

    # Canada-specific integration: allow cheap document-level targeted recovery
    # rows from pipeline results.csv to feed the real compile/database path.
    if country == "Canada":
        recovery_df = _load_compile_recovery_rows(pipeline_csv, country, year_range)
        raw_df = _append_compile_rows(raw_df, recovery_df, "targeted recovery totals")

    if country == "Colombia":
        targeted_full_text_df = _extract_colombia_targeted_raw_rows(year_range)
        raw_df = _append_compile_rows(raw_df, targeted_full_text_df, "targeted decree/anexo totals")
        component_full_text_df = _extract_colombia_component_raw_rows(year_range)
        raw_df = _append_compile_rows(raw_df, component_full_text_df, "targeted institutional component rows")
        if not targeted_full_text_df.empty:
            existing = pd.read_csv(raw_rows_csv) if raw_rows_csv.exists() else pd.DataFrame()
            if not existing.empty and "country" in existing.columns:
                existing = existing[existing["country"] != country]
            combined_raw = pd.concat([existing, raw_df], ignore_index=True)
            combined_raw.to_csv(raw_rows_csv, index=False)
            logger.info(f"[{country}] Persisted targeted decree/anexo totals → {raw_rows_csv}")

    # Colombia-specific integration: the deterministic text-cache parser is
    # materially weaker in modern budget years like 2020, while results.csv
    # already contains clean extracted rows from the same source. Supplement
    # only years where raw parsing produced almost nothing.
    colombia_weak_years: set[int] = set()
    if country == "Colombia":
        year_counts = (
            raw_df.groupby("year").size().to_dict()
            if not raw_df.empty and "year" in raw_df.columns
            else {}
        )
        colombia_weak_years = {int(y) for y, n in year_counts.items() if int(y) >= 2019 and int(n) < 5}
        if colombia_weak_years:
            supplement_df = _load_pipeline_supplement_rows(
                pipeline_csv,
                country,
                year_range=year_range,
                target_years=colombia_weak_years,
            )
            raw_df = _append_compile_rows(raw_df, supplement_df, "pipeline supplement rows")
            if not supplement_df.empty:
                existing = pd.read_csv(raw_rows_csv) if raw_rows_csv.exists() else pd.DataFrame()
                if not existing.empty and "country" in existing.columns:
                    existing = existing[existing["country"] != country]
                combined_raw = pd.concat([existing, raw_df], ignore_index=True)
                combined_raw.to_csv(raw_rows_csv, index=False)
                logger.info(f"[{country}] Persisted supplemented raw rows → {raw_rows_csv}")

    # Costa Rica-specific integration: the deterministic text-cache parser finds
    # many rows, but it often misses the cleaner institutional transfers that
    # already exist in pipeline results.csv. Supplement the compile input with
    # those pre-existing rows so canonical selection can choose between both
    # sources while keeping the conservative, hardcoded institutional panel.
    if country == "Costa Rica":
        costa_rica_target_years: set[int] = set()
        if year_range:
            costa_rica_target_years = set(range(int(year_range[0]), int(year_range[1]) + 1))
        elif not raw_df.empty and "year" in raw_df.columns:
            costa_rica_target_years = {
                int(y)
                for y in pd.to_numeric(raw_df["year"], errors="coerce").dropna().astype(int).tolist()
            }
        supplement_df = _load_pipeline_supplement_rows(
            pipeline_csv,
            country,
            year_range=year_range,
            target_years=costa_rica_target_years or None,
        )
        raw_df = _append_compile_rows(raw_df, supplement_df, "pipeline supplement rows")
        if not supplement_df.empty:
            existing = pd.read_csv(raw_rows_csv) if raw_rows_csv.exists() else pd.DataFrame()
            if not existing.empty and "country" in existing.columns:
                existing = existing[existing["country"] != country]
            combined_raw = pd.concat([existing, raw_df], ignore_index=True)
            combined_raw.to_csv(raw_rows_csv, index=False)
            logger.info(f"[{country}] Persisted supplemented raw rows → {raw_rows_csv}")

    # Countries where LLM pipeline output (results.csv) always takes precedence
    # over DOCX/text-cache, even when the text-cache has partial data.
    # These countries were processed through pipeline.py (LLM extraction) which
    # produces far richer, higher-quality results than the legacy text-cache parser.
    # Norway/Denmark: have partial text-cache (12-15 years, ~22 rows) that would
    # otherwise block the 2000+ row LLM pipeline output from being used.
    _PIPELINE_FIRST_COUNTRIES = {
        "UK", "France", "Germany", "Japan",           # original narrative-PDF set
        "Norway", "Denmark",                           # text-cache exists but LLM wins
        "Estonia",                                     # text-cache parse is materially weaker than pipeline output
        "Belgium",                                     # text-cache/docx path is materially weaker than pipeline output
        "Austria",                                     # docx/text-cache path reintroduces KPI/summary artefacts; cleaned pipeline rows are the safer compile seed
        "Czech Republic",                              # local parser latches onto noisy annex/legal artefacts; pipeline output retains the useful agency/RDI rows
        "Latvia",                                      # local parser collapses to sparse DOCX rows; pipeline output preserves the richer science programme tables
        "Iceland",                                     # text-cache parse is noisy; pipeline output preserves institution lines
        "Korea",                                       # budget-summary PDFs are materially richer in pipeline output than text-cache parser
        "Israel",                                      # text-cache/docx path latches onto OCR-heavy table summaries; pipeline rows are materially cleaner
        "New Zealand",                                 # country DOCX artifact is sparse/noisy; pipeline results preserve the real science vote/fund rows
        "Portugal",                                    # targeted text parsing is useful for audit trails, but results.csv remains richer and safer as compile seed
        "Italy",                                       # cleaned pipeline rows are materially safer than legacy parser output for the mixed ITL/EUR budget corpus
        "Spain",                                       # pipeline output is materially richer than legacy text-cache parsing
        "Sweden", "Netherlands", "Switzerland",        # future: no text-cache expected
        "Turkey",                                      # local DOCX parser only has 2008-2009; pipeline results.csv covers 1975-1983 historical TÜBİTAK/TAEK data
        "Luxembourg",                                  # local DOCX parser misses 1975-2000; pipeline results.csv covers full range with clean LUF ministry totals
    }

    _use_pipeline = raw_df.empty or country in _PIPELINE_FIRST_COUNTRIES

    if _use_pipeline:
        # ── LLM pipeline output for narrative PDF countries ────────────────────
        # For UK, France, Germany, Japan the DOCX parser and text-cache parser
        # return nothing (narrative prose, not structured tables). Check if
        # pipeline.py has already run LLM extraction and left results.csv.
        # If so, use that output directly — no double LLM cost.
        pipeline_csv = output_dir / "results.csv"
        if country == "Italy":
            italy_clean_csv = output_dir / "results_clean.csv"
            if italy_clean_csv.exists():
                pipeline_csv = italy_clean_csv
        pipeline_df = _load_pipeline_results(pipeline_csv, country, year_range)
        _fresh_pipeline_df = pipeline_df  # keep a handle to the un-swapped fresh pull, see backfill guard below

        if not pipeline_df.empty:
            logger.info(
                f"[{country}] Using LLM pipeline output instead of DOCX/text-cache"
            )
            results_df = pipeline_df
            if country == "Korea":
                results_df = _apply_korea_audited_pipeline_repairs(results_df)
            existing_country_df = _load_country_results_snapshot(country_results_csv, year_range)
            chosen_label = pipeline_csv.name
            if not existing_country_df.empty and country != "Luxembourg":
                pipeline_mtime = pipeline_csv.stat().st_mtime if pipeline_csv.exists() else -1.0
                existing_mtime = country_results_csv.stat().st_mtime if country_results_csv.exists() else -1.0
                pipeline_score = (
                    int(pipeline_df["year"].nunique()) if not pipeline_df.empty else 0,
                    int(pipeline_df["year"].max()) if not pipeline_df.empty else -1,
                    len(pipeline_df),
                )
                existing_score = (
                    int(existing_country_df["year"].nunique()),
                    int(existing_country_df["year"].max()),
                    len(existing_country_df),
                )
                if pipeline_df.empty or (existing_score > pipeline_score and existing_mtime >= pipeline_mtime):
                    logger.info(
                        f"[{country}] Reusing richer country-local results snapshot "
                        f"({country_results_csv.name}) instead of shared results.csv"
                    )
                    pipeline_df = existing_country_df
                    chosen_label = country_results_csv.name
                elif existing_score > pipeline_score and existing_mtime < pipeline_mtime:
                    logger.info(
                        f"[{country}] Ignoring stale country-local results snapshot "
                        f"({country_results_csv.name}); shared results.csv is newer"
                    )

                # Guard against a "richer by row count" snapshot silently hiding a
                # regression in *year coverage*. A stale country-local CSV can have
                # more total rows than a fresh pipeline pull (e.g. accumulated
                # manual curation) while still being missing whole years the fresh
                # pull now has — this happened for Hungary's biannual 2001-2002
                # source file, where a re-extraction added a real 2002 row set that
                # kept losing to the older, larger-by-count snapshot every compile.
                # If the side we did NOT choose covers years the chosen side lacks
                # entirely, backfill those years' rows rather than losing them.
                chosen_years = set(pd.to_numeric(pipeline_df["year"], errors="coerce").dropna().astype(int))
                other_df = existing_country_df if chosen_label == pipeline_csv.name else _fresh_pipeline_df
                if not other_df.empty:
                    other_years = set(pd.to_numeric(other_df["year"], errors="coerce").dropna().astype(int))
                    missing_years = other_years - chosen_years
                    if missing_years:
                        backfill = other_df[pd.to_numeric(other_df["year"], errors="coerce").astype("Int64").isin(missing_years)]
                        logger.info(
                            f"[{country}] Backfilling {len(backfill)} rows for year(s) {sorted(missing_years)} "
                            f"present in the non-chosen source but entirely absent from the chosen snapshot "
                            f"({chosen_label})"
                        )
                        pipeline_df = pd.concat([pipeline_df, backfill], ignore_index=True, sort=False)

            results_df = pipeline_df
            results_df = _materialize_country_output_units(results_df, country)
            logger.info(f"[{country}] Seeded compile from {chosen_label}")
            _write_year_slice(
                country_results_csv,
                results_df,
                year_range=year_range,
                sort_cols=["country", "year", "source_file", "page_number", "line_description_en"],
            )
            logger.info(f"Results → {country_results_csv} ({len(results_df)} rows)")

            # ── Agency discovery on pipeline output ───────────────────────────
            # Convert pipeline output columns → raw_rows format so discover_agencies()
            # can identify agencies the LLM found that aren't in the canonical list.
            # This ensures we don't miss R&D budget lines that aren't yet hardcoded.
            pipeline_as_raw = pd.DataFrame({
                "country":       results_df["country"],
                "year":          results_df["year"],
                "source_file":   results_df.get("source_file", ""),
                "amount_current": pd.to_numeric(results_df["amount_local"], errors="coerce"),
                "section_name":  results_df.get("section_name_en", ""),
                "is_total_row":  results_df.get("item_type", "").eq("section_total"),
                "is_header_row": False,
                "table_index":   0,
                "row_index":     range(len(results_df)),
                "amount_prior":  None,
                "has_italic_entity": False,
                "cells_raw":     "[]",
            })
            entity_text = results_df.get("line_description", results_df.get("line_description_en", "")).fillna("").astype(str).str.strip()
            section_text = results_df.get("section_name", results_df.get("section_name_en", "")).fillna("").astype(str).str.strip()
            entity_en = results_df.get("line_description_en", "").fillna("").astype(str).str.strip()
            section_en = results_df.get("section_name_en", "").fillna("").astype(str).str.strip()
            is_section_total = results_df.get("item_type", pd.Series("", index=results_df.index)).astype(str).eq("section_total")
            entity_raw = entity_text.where(~is_section_total, section_text)
            entity_raw = entity_raw.where(entity_raw.str.len() > 0, entity_en)
            entity_raw = entity_raw.where(entity_raw.str.len() > 0, section_en)
            entity_raw = entity_raw.where(entity_raw.str.len() > 0, section_text)
            pipeline_as_raw["entity_raw"] = entity_raw

            if not dry_run and country != "Israel":
                logger.info(f"Agency discovery: {country}")
                try:
                    discover_agencies(
                        pipeline_as_raw,
                        country=country,
                        config=config,
                        output_dir=country_dir,
                        **_agency_discovery_kwargs(country),
                    )
                except Exception as exc:
                    logger.warning(
                        f"[{country}] Agency discovery failed in pipeline-first compile; "
                        f"continuing with existing canonical/discovered agencies. Error: {exc}"
                    )

            # ── Build canonical series ─────────────────────────────────────────
            series_df = build_canonical_series(results_df, country=country)

            if not series_df.empty:
                cname = country.lower().replace(" ", "_")
                series_path = country_dir / f"{cname}_docx_series.csv"
                _write_year_slice(
                    series_path,
                    series_df,
                    year_range=year_range,
                    sort_cols=["country", "canonical_name", "year", "source_file"],
                )
                logger.info(f"Detail series → {series_path}")

                series_trace_df = _build_series_traceability(series_df, country)
                if not series_trace_df.empty:
                    series_trace_path = country_dir / f"{cname}_series_traceability.csv"
                    _write_year_slice(
                        series_trace_path,
                        series_trace_df,
                        year_range=year_range,
                        sort_cols=["year", "canonical_name", "source_file"],
                    )
                    logger.info(f"Series traceability → {series_trace_path}")

                source_trace_df = _build_source_traceability(series_df, country)
                if not source_trace_df.empty:
                    source_trace_path = country_dir / f"{cname}_source_traceability.csv"
                    source_trace_df.to_csv(source_trace_path, index=False)
                    logger.info(f"Source traceability → {source_trace_path}")

                verified_df = _build_verified_override_audit(series_df)
                if not verified_df.empty:
                    verified_path = country_dir / f"{cname}_verified_overrides.csv"
                    _write_year_slice(
                        verified_path,
                        verified_df,
                        year_range=year_range,
                        sort_cols=["country", "canonical_name", "year", "source_file"],
                    )
                    logger.info(f"Verified overrides → {verified_path}")

                if country == "Korea":
                    theme_df = _build_korea_theme_panel(year_range=year_range)
                    if not theme_df.empty:
                        theme_path = country_dir / "korea_theme_panel.csv"
                        _write_year_slice(
                            theme_path,
                            theme_df,
                            year_range=year_range,
                            sort_cols=["year", "theme_bucket", "source_file", "page_number"],
                        )
                        logger.info(f"Korea theme panel → {theme_path}")

                totals_df = build_totals_series(series_df, country=country)
                if not totals_df.empty:
                    totals_path = country_dir / f"{cname}_docx_totals.csv"
                    _write_year_slice(
                        totals_path,
                        totals_df,
                        year_range=year_range,
                        sort_cols=["country", "canonical_name", "year"],
                    )
                    logger.info(f"Totals series → {totals_path}")

                audit_df = _build_full_audit(pipeline_as_raw, series_df, country)
                if not audit_df.empty:
                    audit_path = country_dir / f"{cname}_docx_audit.csv"
                    _write_year_slice(
                        audit_path,
                        audit_df,
                        year_range=year_range,
                        sort_cols=["canonical_name", "year", "source_file"],
                    )
                    logger.info(f"Full audit database → {audit_path} ({len(audit_df)} rows)")

                gap_df, _ = build_gap_report(
                    series_df=series_df,
                    country=country,
                    raw_rows_csv=raw_rows_csv,
                    output_dir=country_dir,
                )
                if not gap_df.empty:
                    problems = gap_df[gap_df["gap_type"] != "ok"]
                    if not problems.empty:
                        logger.info(
                            f"\nGap summary for {country}:\n" +
                            problems[["year","canonical_name","gap_type","action","diagnosis"]]
                            .to_string(index=False)
                        )

            build_combined_database(output_dir=output_dir)
            return series_df

        logger.warning(
            f"No raw rows for {country}. "
            f"For narrative PDF countries (UK/France/Germany/Japan), run "
            f"pipeline.py first: python main.py --budget --country {country} --llm-pipeline"
        )
        return pd.DataFrame()

    # ── Step 2.5: unit handling ──────────────────────────────────────────────
    # Some countries change their denomination across eras. Apply only the
    # explicit rules above; Canada intentionally remains full CAD dollars.
    raw_df = _normalise_units(raw_df, country)

    # ── Step 3: LLM entity dedup ─────────────────────────────────────────────
    if entity_dedup and not dry_run:
        logger.info(f"Entity dedup: {country} ({raw_df['year'].nunique()} years)")
        raw_df = apply_entity_dedup(raw_df, config=config, countries=[country])
    else:
        raw_df["canonical_name"] = raw_df["entity_raw"]

    # After entity dedup: drop rows whose canonical_name duplicates another
    # row in the same (country, year) with the same amount — keep the
    # one with the largest amount (or lowest act number already applied)
    raw_df["_act_no_check"] = raw_df["source_file"].apply(
        lambda f: int(re.search(r"\bNo\.?\s*(\d+)\b", str(f), re.IGNORECASE).group(1))
        if re.search(r"\bNo\.?\s*(\d+)\b", str(f), re.IGNORECASE) else 999
    )
    raw_df = (
        raw_df
        .sort_values(["_act_no_check", "amount_current"], ascending=[True, False])
        .drop_duplicates(subset=["country", "year", "canonical_name", "source_file"], keep="first")
        .drop(columns=["_act_no_check"])
        .reset_index(drop=True)
    )
    logger.info(f"After entity dedup + final dedup: {len(raw_df)} unique rows")

    # ── Step 3.5: agency discovery ────────────────────────────────────────────
    if not dry_run:
        logger.info(f"Agency discovery: {country}")
        discover_agencies(
            raw_df,
            country=country,
            config=config,
            output_dir=country_dir,
            **_agency_discovery_kwargs(country),
        )

    # ── Step 4: agency classifier ─────────────────────────────────────────────
    registry = classify_raw_rows(
        raw_df=raw_df,
        config=config,
        country=country,
        dry_run=dry_run,
    )

    # ── Step 5: build classified results ─────────────────────────────────────
    results_df = build_classified_results(raw_df, registry, country)

    if country == "Colombia" and colombia_weak_years:
        supplement_results_df = _load_pipeline_results(pipeline_csv, country, year_range)
        if not supplement_results_df.empty:
            supplement_results_df = supplement_results_df[
                supplement_results_df["year"].isin(colombia_weak_years)
            ].copy()
            if not supplement_results_df.empty:
                results_df = pd.concat([results_df, supplement_results_df], ignore_index=True)
                dedup_cols = [
                    "country", "year", "source_file", "section_name_en",
                    "line_description_en", "amount_local"
                ]
                available_cols = [c for c in dedup_cols if c in results_df.columns]
                results_df = results_df.drop_duplicates(subset=available_cols, keep="first").reset_index(drop=True)
                logger.info(
                    f"[{country}] Appended {len(supplement_results_df)} pipeline result rows "
                    f"for weak years {sorted(colombia_weak_years)}"
                )

    results_path = country_dir / f"{country.lower().replace(' ','_')}_docx_results.csv"
    _write_year_slice(
        results_path,
        results_df,
        year_range=year_range,
        sort_cols=["country", "year", "source_file", "page_number", "line_description_en"],
    )
    logger.info(f"Results → {results_path} ({len(results_df)} rows)")

    # ── Step 6: canonical series ──────────────────────────────────────────────
    series_df = build_canonical_series(results_df, country=country)

    if not series_df.empty:
        cname = country.lower().replace(" ", "_")

        # ── Detail series ─────────────────────────────────────────────────────
        # One row per (agency, year, source_file).
        # Use this to trace any figure back to the exact document and page.
        series_path = country_dir / f"{cname}_docx_series.csv"
        _write_year_slice(
            series_path,
            series_df,
            year_range=year_range,
            sort_cols=["country", "canonical_name", "year", "source_file"],
        )
        logger.info(f"Detail series → {series_path}")

        series_trace_df = _build_series_traceability(series_df, country)
        if not series_trace_df.empty:
            series_trace_path = country_dir / f"{cname}_series_traceability.csv"
            _write_year_slice(
                series_trace_path,
                series_trace_df,
                year_range=year_range,
                sort_cols=["year", "canonical_name", "source_file"],
            )
            logger.info(f"Series traceability → {series_trace_path}")

        source_trace_df = _build_source_traceability(series_df, country)
        if not source_trace_df.empty:
            source_trace_path = country_dir / f"{cname}_source_traceability.csv"
            source_trace_df.to_csv(source_trace_path, index=False)
            logger.info(f"Source traceability → {source_trace_path}")

        verified_df = _build_verified_override_audit(series_df)
        if not verified_df.empty:
            verified_path = country_dir / f"{cname}_verified_overrides.csv"
            _write_year_slice(
                verified_path,
                verified_df,
                year_range=year_range,
                sort_cols=["country", "canonical_name", "year", "source_file"],
            )
            logger.info(f"Verified overrides → {verified_path}")

        if country == "Korea":
            theme_df = _build_korea_theme_panel(year_range=year_range)
            if not theme_df.empty:
                theme_path = country_dir / "korea_theme_panel.csv"
                _write_year_slice(
                    theme_path,
                    theme_df,
                    year_range=year_range,
                    sort_cols=["year", "theme_bucket", "source_file", "page_number"],
                )
                logger.info(f"Korea theme panel → {theme_path}")

        # ── Totals series ─────────────────────────────────────────────────────
        # One row per (agency, year). Sums amounts across Acts with restatement flag.
        # This is the primary output for time-series analysis.
        totals_df = build_totals_series(series_df, country=country)
        if not totals_df.empty:
            totals_path = country_dir / f"{cname}_docx_totals.csv"
            _write_year_slice(
                totals_path,
                totals_df,
                year_range=year_range,
                sort_cols=["country", "canonical_name", "year"],
            )
            logger.info(f"Totals series → {totals_path}")

        # ── Full audit database ───────────────────────────────────────────────
        # All raw rows that matched a canonical agency, with every appearance
        # across all Acts and all years, sorted by (agency, year, source_file).
        # Shows every time e.g. "Australian Institute of Marine Science" appears
        # across the full history — all amounts, all documents.
        audit_df = _build_full_audit(raw_df, series_df, country)
        if not audit_df.empty:
            audit_path = country_dir / f"{cname}_docx_audit.csv"
            _write_year_slice(
                audit_path,
                audit_df,
                year_range=year_range,
                sort_cols=["canonical_name", "year", "source_file"],
            )
            logger.info(f"Full audit database → {audit_path} ({len(audit_df)} rows)")

    # ── Step 7: gap detection ─────────────────────────────────────────────────
    if not series_df.empty:
        gap_df, queue_df = build_gap_report(
            series_df=series_df,
            country=country,
            raw_rows_csv=raw_rows_csv,
            output_dir=country_dir,
        )
        # Print summary
        if not gap_df.empty:
            problems = gap_df[gap_df["gap_type"] != "ok"]
            if not problems.empty:
                logger.info(
                    f"\nGap summary for {country}:\n" +
                    problems[["year","canonical_name","gap_type","action","diagnosis"]]
                    .to_string(index=False)
                )

    # ── Step 8: gap filling ───────────────────────────────────────────────────
    # For gaps still marked 'reextract', try to find the missing amounts by
    # re-searching the source documents. Two phases:
    #   Phase 1: broad text search (free, deterministic)
    #   Phase 2: LLM targeted extraction (cheap, ~$0.001/gap)
    # If new rows are found, rebuild the series + gap report so the final
    # output reflects the filled gaps.
    if fill_gaps_flag and not series_df.empty and not gap_df.empty:
        n_before = len(gap_df[gap_df["gap_type"] == "missing"])
        logger.info(f"[{country}] Gap filling: {n_before} missing agency-years")

        new_rows_df = fill_gaps(
            gap_df=gap_df,
            country=country,
            config=config,
            use_llm=fill_gaps_llm,
        )

        if not new_rows_df.empty:
            logger.info(f"[{country}] Gap filler found {len(new_rows_df)} new rows — rebuilding series")

            # Merge new rows into results and rebuild series
            new_results = build_classified_results(new_rows_df, registry, country)
            results_df_updated = pd.concat([results_df, new_results], ignore_index=True)
            series_df = build_canonical_series(results_df_updated, country=country)

            if not series_df.empty:
                cname = country.lower().replace(" ", "_")
                _write_year_slice(
                    country_dir / f"{cname}_docx_series.csv",
                    series_df,
                    year_range=year_range,
                    sort_cols=["country", "canonical_name", "year", "source_file"],
                )
                totals_df = build_totals_series(series_df, country=country)
                if not totals_df.empty:
                    _write_year_slice(
                        country_dir / f"{cname}_docx_totals.csv",
                        totals_df,
                        year_range=year_range,
                        sort_cols=["country", "canonical_name", "year"],
                    )
                audit_df = _build_full_audit(new_rows_df if new_rows_df is not None else raw_df, series_df, country)
                if not audit_df.empty:
                    _write_year_slice(
                        country_dir / f"{cname}_docx_audit.csv",
                        audit_df,
                        year_range=year_range,
                        sort_cols=["canonical_name", "year", "source_file"],
                    )

                # Re-run gap report to show what's still missing
                gap_df, queue_df = build_gap_report(
                    series_df=series_df,
                    country=country,
                    raw_rows_csv=raw_rows_csv,
                    output_dir=country_dir,
                )
                n_after = len(gap_df[gap_df["gap_type"] == "missing"])
                logger.info(
                    f"[{country}] Gap filling complete: "
                    f"{n_before} → {n_after} missing ({n_before - n_after} closed)"
                )

    # ── Step 9: rebuild combined database ────────────────────────────────────
    build_combined_database(output_dir=output_dir)

    return series_df


# ---------------------------------------------------------------------------
# Combined database — all countries, app-ready
# ---------------------------------------------------------------------------

def build_combined_database(output_dir: Path = cfg.OUTPUT_DIR) -> pd.DataFrame:
    """
    Combine all country series into a single clean database file.

    - One row per (country, canonical_name, year) — primary amount only
    - Gaps (NaN amounts) are excluded
    - Written to output_dir/rd_database.csv

    This is the app-facing output. The per-country detail series files
    preserve the full per-source-file breakdown for audit purposes.
    """
    output_dir = Path(output_dir)
    db_path = output_dir / "rd_database.csv"

    all_series = []
    for country_dir in sorted(output_dir.iterdir()):
        if not country_dir.is_dir():
            continue
        cname = country_dir.name.lower().replace(" ", "_")
        series_path = country_dir / f"{cname}_docx_series.csv"
        if not series_path.exists():
            continue
        df = pd.read_csv(series_path)
        all_series.append(df)

    if not all_series:
        logger.warning("No country series found — combined database not built")
        return pd.DataFrame()

    combined = pd.concat(all_series, ignore_index=True)

    # Drop gap rows (no amount) — keep all source-file rows that have a value
    combined = combined.dropna(subset=["amount_local"])
    combined = combined[combined["amount_local"].notna() & (combined["amount_local"] != 0)]

    combined = combined.sort_values(["country", "canonical_name", "year", "source_file"]).reset_index(drop=True)

    # Clean columns for app use
    keep_cols = [
        "country", "year", "canonical_name", "category",
        "amount_local", "unit", "currency",
        "item_type", "line_description_en", "source_file", "series_notes",
    ]
    keep_cols = [c for c in keep_cols if c in combined.columns]
    combined = combined[keep_cols]

    combined.to_csv(db_path, index=False)
    logger.info(
        f"Combined database → {db_path} "
        f"({len(combined)} rows, {combined['country'].nunique()} countries, "
        f"{combined['year'].nunique()} years)"
    )
    return combined


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import yaml
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")

    parser = argparse.ArgumentParser(description="Compile DOCX budget data (no LLM for extraction)")
    parser.add_argument("--country", help="Country to compile")
    parser.add_argument("--years", help="Year range e.g. 2000-2026")
    parser.add_argument("--dry-run", action="store_true", help="Don't call LLM for classification")
    parser.add_argument("--no-entity-dedup", action="store_true", help="Skip entity dedup (use cached canonical_name)")
    parser.add_argument("--fill-gaps", action="store_true", help="After gap detection, try to fill missing gaps from source documents")
    parser.add_argument("--no-gap-llm", action="store_true", help="Gap filling: use text search only, skip LLM phase")
    parser.add_argument("--build-database", action="store_true", help="Rebuild combined rd_database.csv from all country series (no extraction)")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    # Standalone database rebuild — no extraction needed
    if args.build_database:
        db = build_combined_database()
        if not db.empty:
            print(f"\nCombined database: {len(db)} rows")
            print(db.groupby("country").agg(
                agencies=("canonical_name", "nunique"),
                years=("year", "nunique"),
                min_year=("year", "min"),
                max_year=("year", "max"),
            ).to_string())
        sys.exit(0)

    if not args.country:
        parser.error("--country is required unless --build-database is used")

    with open(args.config) as f:
        config = yaml.safe_load(f)

    year_range = None
    if args.years:
        parts = args.years.split("-")
        if len(parts) == 2:
            year_range = (int(parts[0]), int(parts[1]))

    series = compile_country(
        country=args.country,
        config=config,
        year_range=year_range,
        dry_run=args.dry_run,
        entity_dedup=not args.no_entity_dedup,
        fill_gaps_flag=args.fill_gaps,
        fill_gaps_llm=not args.no_gap_llm,
    )

    if not series.empty:
        print(f"\nSeries summary for {args.country}:")
        print(series.groupby("canonical_name").agg(
            years=("year", "count"),
            min_year=("year", "min"),
            max_year=("year", "max"),
        ).to_string())
