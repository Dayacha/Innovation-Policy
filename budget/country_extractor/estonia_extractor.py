"""Estonian State Budget (Riigieelarve) extractor.

Estonian budget law ("seadus «{year}. aasta riigieelarve»") is organised by
ministry sections ("Jagu") and their subordinate units.

Key structure observed in scanned files:
  Jagu 2.
  HARIDUS- JA TEADUSMINISTEERIUMI
  valitsemisala
  {total_amount}
  ...
  {agency_code}. Haridus- ja Teadusministeerium
  {total_amount}
  ...
  Sihtasutus Eesti Teadusfond (Estonian Research Council / ETF)
  {amount}
  ...
  teaduse sihtfinantseerimine (research core funding)
  {amount}

Currency:
  - Pre-2011: EEK (Estonian kroon), amounts in kroons (full values)
  - 2011+: EUR (euro), amounts in euros (full values)

Programs extracted:
  EE_SCIENCE_MINISTRY : Haridus- ja Teadusministeerium total (Jagu 2)
  EE_ETAG             : Estonian Research Council / Teadusfond
  EE_RESEARCH_FUNDING : teaduse sihtfinantseerimine (core research funding)
"""

from __future__ import annotations

import re
import logging
from typing import Optional

logger = logging.getLogger("innovation_pipeline")

# ── Regex constants ─────────────────────────────────────────────────────────────

# Ministry of Education and Research section header
# "HARIDUS- JA TEADUSMINISTEERIUM" or "Haridus- ja Teadusministeerium"
_MINISTRY_HEADER_RE = re.compile(
    r'HARIDUS[-\s]+JA\s+TEADUSMINISTEERIUMI?\b'
    r'|Haridus[-\s]+ja\s+Teadusministeerium\b',
    re.IGNORECASE,
)

# Estonian Research Council / Foundation
# "Sihtasutus Eesti Teadusfond" or "Eesti Teadusagentuur" (ETAg, from 2012)
_ETAG_RE = re.compile(
    r'(?:Sihtasutus\s+)?Eesti\s+Teadusfond'
    r'|Eesti\s+Teadusagentuur'
    r'|ETAg\b',
    re.IGNORECASE,
)

# Core research funding line
_RESEARCH_FUNDING_RE = re.compile(
    r'teaduse\s+sihtfinantseerimine'
    r'|teadus[-–]\s*ja\s+arendusasutuste',
    re.IGNORECASE,
)

# Amount pattern: space-grouped thousands (Estonian style) e.g. "1 830 348 000"
# or plain digits e.g. "286553500"
_AMOUNT_RE = re.compile(
    r'\b(\d{1,3}(?:\s\d{3})+|\d{6,})\b'
)

# "Jagu 2." section marker (Ministry of Education and Research)
_JAGU2_RE = re.compile(r'Jagu\s*2\.?\s*\n', re.IGNORECASE)

# Next section boundary
_NEXT_JAGU_RE = re.compile(r'Jagu\s*[3-9]\d*\.?\s*\n', re.IGNORECASE)


def _determine_currency(year: str) -> str:
    """Determine currency based on year. Estonia adopted EUR on 1 Jan 2011."""
    try:
        y = int(year)
        return "EUR" if y >= 2011 else "EEK"
    except (ValueError, TypeError):
        return "EEK"


def _parse_amount(raw: str) -> Optional[float]:
    """Parse Estonian amount string (space-thousands separator) to float."""
    cleaned = raw.replace(' ', '').strip()
    try:
        val = float(cleaned)
        return val if val > 0 else None
    except ValueError:
        return None


def _first_large_amount(text: str, min_val: float = 1_000_000) -> Optional[float]:
    """Return the first large amount found in text (after window start)."""
    for m in _AMOUNT_RE.finditer(text):
        val = _parse_amount(m.group(1))
        if val and val >= min_val:
            return val
    return None


def _extract_ministry_total(full_text: str) -> Optional[float]:
    """Extract the Haridus- ja Teadusministeerium section total."""
    # Find Jagu 2 (Ministry of Education and Research)
    jagu_m = _JAGU2_RE.search(full_text)
    if not jagu_m:
        # Fallback: search by ministry name
        mm = _MINISTRY_HEADER_RE.search(full_text)
        if not mm:
            return None
        start = mm.start()
    else:
        start = jagu_m.start()

    # Find end of this section (start of Jagu 3+)
    end_m = _NEXT_JAGU_RE.search(full_text, start + 10)
    end = end_m.start() if end_m else min(start + 8000, len(full_text))
    section_text = full_text[start:end]

    # The ministry total appears as the first large amount in the section header
    # After "valitsemisala" or right after the ministry name
    for pattern in [
        r'valitsemisala\s*\n\s*([\d\s]+)',
        r'TEADUSMINISTEERIUMI?\s*\n[^\n]*\n\s*([\d\s]+)',
    ]:
        m = re.search(pattern, section_text, re.IGNORECASE)
        if m:
            val = _parse_amount(m.group(1).strip())
            if val and val >= 1_000_000:
                return val

    # Fallback: first large amount in section header
    header = section_text[:300]
    return _first_large_amount(header, min_val=10_000_000)


def _extract_etag_amount(full_text: str) -> Optional[float]:
    """Extract the Estonian Research Council / Teadusfond amount."""
    for m in _ETAG_RE.finditer(full_text):
        window = full_text[m.start(): m.start() + 400]
        val = _first_large_amount(window, min_val=1_000_000)
        if val:
            return val
    return None


def _extract_research_funding(full_text: str) -> Optional[float]:
    """Extract core research funding (teaduse sihtfinantseerimine)."""
    for m in _RESEARCH_FUNDING_RE.finditer(full_text):
        window = full_text[m.start(): m.start() + 300]
        val = _first_large_amount(window, min_val=1_000_000)
        if val:
            return val
    return None


# ── Public API ─────────────────────────────────────────────────────────────────

def extract_estonia_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract science/research budget records from an Estonian Riigieelarve PDF.

    Extracts:
    - EE_SCIENCE_MINISTRY: total budget of Ministry of Education and Research
    - EE_ETAG: Estonian Research Council (Teadusfond / ETAg) allocation
    - EE_RESEARCH_FUNDING: core research funding (sihtfinantseerimine)

    Returns [] on failure.
    """
    records: list[dict] = []

    try:
        all_text = "\n".join(
            (row.text if isinstance(row.text, str) else "")
            for row in sorted_pages.itertuples(index=False)
        )
    except Exception as exc:
        logger.warning("Estonia extractor: failed to read pages for %s: %s", source_filename, exc)
        return []

    if not all_text.strip():
        logger.debug("Estonia extractor: no text in %s.", source_filename)
        return []

    currency = _determine_currency(year)

    # ── 1. Ministry of Education and Research total ───────────────────────────
    ministry_total = _extract_ministry_total(all_text)
    if ministry_total and ministry_total >= 1_000_000:
        records.append({
            "country": country,
            "year": year,
            "section_code": "EE_SCIENCE",
            "section_name": "Haridus- ja Teadusministeeriumi valitsemisala",
            "section_name_en": "Ministry of Education and Research - governance area",
            "program_code": "EE_SCIENCE_MINISTRY",
            "line_description": "Haridus- ja Teadusministeerium (Jagu 2) - kogueelarve",
            "line_description_en": "Ministry of Education and Research - total budget",
            "amount_local": ministry_total,
            "currency": currency,
            "unit": currency,
            "rd_category": "direct_rd",
            "taxonomy_score": 7.0,
            "decision": "include",
            "confidence": 0.80,
            "source_file": source_filename,
            "file_id": file_id,
            "page_number": 1,
        })

    # ── 2. Estonian Research Council ─────────────────────────────────────────
    etag_amount = _extract_etag_amount(all_text)
    if etag_amount and etag_amount >= 100_000:
        records.append({
            "country": country,
            "year": year,
            "section_code": "EE_SCIENCE",
            "section_name": "Eesti Teadusfond / ETAg",
            "section_name_en": "Estonian Research Council",
            "program_code": "EE_ETAG",
            "line_description": "Sihtasutus Eesti Teadusfond / Eesti Teadusagentuur",
            "line_description_en": "Estonian Research Council (ETF / ETAg)",
            "amount_local": etag_amount,
            "currency": currency,
            "unit": currency,
            "rd_category": "direct_rd",
            "taxonomy_score": 9.0,
            "decision": "include",
            "confidence": 0.85,
            "source_file": source_filename,
            "file_id": file_id,
            "page_number": 1,
        })

    # ── 3. Core research funding ───────────────────────────────────────────────
    rf_amount = _extract_research_funding(all_text)
    if rf_amount and rf_amount >= 100_000:
        records.append({
            "country": country,
            "year": year,
            "section_code": "EE_SCIENCE",
            "section_name": "Teaduse sihtfinantseerimine",
            "section_name_en": "Research core funding (targeted financing)",
            "program_code": "EE_RESEARCH_FUNDING",
            "line_description": "teaduse sihtfinantseerimine / teadus- ja arendusasutuste kulud",
            "line_description_en": "Research targeted financing / R&D institution expenditure",
            "amount_local": rf_amount,
            "currency": currency,
            "unit": currency,
            "rd_category": "direct_rd",
            "taxonomy_score": 9.0,
            "decision": "include",
            "confidence": 0.80,
            "source_file": source_filename,
            "file_id": file_id,
            "page_number": 1,
        })

    if records:
        logger.info(
            "Estonia extractor: %s (year %s) → %d records",
            source_filename, year, len(records),
        )
    else:
        logger.debug(
            "Estonia extractor: no science section found in %s (year %s).",
            source_filename, year,
        )

    return records
