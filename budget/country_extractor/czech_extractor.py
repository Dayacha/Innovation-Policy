"""Czech Republic State Budget (Zákon o státním rozpočtu) extractor.

Czech R&D spending tracked through:
- **Chapter 321** — Grantová agentura České republiky (Czech Science Foundation / GAČR)
  Main competitive R&D grant agency.
- **Chapter 361** — Akademie věd České republiky (Academy of Sciences of the Czech Republic)
  Main public research institute network.

Document formats handled
------------------------
1. **PDF annexes 1997–2000**: "Přílohy k zákonu o státním rozpočtu" PDFs with
   chapter-level detail sections titled "Ukazatele kapitoly {code} {name} v tisících Kč".
   The key line is "Výdaje celkem {amount}".

2. **DOCX annexes 2001–2004**: "czech_budget_annexes_CONTENT_{year}.docx" files with
   an "Annex 3" section listing all chapters and total expenditures in tis. Kč:
   "{code} {name} {amount}" format (all on one line in extracted text).

3. **PDF 2009**: Same "Ukazatele kapitoly" format as 1997-2000 PDFs.

Note: 2005-2008 and 2010+ PDFs have scanned annexes — chapter amounts are images,
not extractable via OCR (or insufficient OCR coverage). These years are skipped.

Currency: CZK (Czech Crown / koruna česká), amounts in tis. Kč (multiply by 1000).
"""

from __future__ import annotations

import re
import logging
from typing import Optional

logger = logging.getLogger("innovation_pipeline")

# ── Chapter patterns ───────────────────────────────────────────────────────────

# Chapter 321 — Czech Science Foundation (GAČR)
_GACR_RE = re.compile(
    r"321\s+Grantov[aá]\s+agentura\s+[CČ]esk[eé]\s+republiky"
    r"|Grantov[aá]\s+agentura\s+[CČ]esk[eé]\s+republiky"
    r"|GRANTOV[AÁ]\s+AGENTURA\s+[CČ]ESK[EÉ]\s+REPUBLIKY",
    re.IGNORECASE,
)

# Chapter 361 — Academy of Sciences
_AVCR_RE = re.compile(
    r"361\s+Akademie\s+v[eě]d\s+[CČ]esk[eé]\s+republiky"
    r"|Akademie\s+v[eě]d\s+[CČ]esk[eé]\s+republiky"
    r"|AKADEMIE\s+V[EĚ]D\s+[CČ]ESK[EÉ]\s+REPUBLIKY"
    # Handle garbled encoding (MFKD decomposition artifacts in some PDFs)
    r"|Akademie\s+ve\xcc\x8f\s*d\s+C\xcc\x8c",
    re.IGNORECASE,
)

# "Výdaje celkem {amount}" pattern (in chapter-level sections)
_VYDAJE_RE = re.compile(
    r"V[yý]daje\s+celkem\s+([\d\s\xa0]+)",
    re.IGNORECASE,
)

# Annex 3 summary table format: "{3-digit code} {name} {space-separated amount}"
# e.g. "321 Grantová agentura České republiky 1 197 097"
_ANNEX3_GACR_RE = re.compile(
    r"321\s+Grantov[aá]\s+agentura\s+[CČ]esk[eé]\s+republiky\s+([\d\s]+)",
    re.IGNORECASE,
)
_ANNEX3_AVCR_RE = re.compile(
    r"361\s+Akademie\s+v[eě]d\s+[CČ]esk[eé]\s+republiky\s+([\d\s]+)",
    re.IGNORECASE,
)

_CELKEM_GACR_RE = re.compile(
    r"Grantov.{0,40}?agentura.{0,80}?celkem\s+([\d\s]+)",
    re.IGNORECASE | re.DOTALL,
)
_CELKEM_AVCR_RE = re.compile(
    r"Akademie.{0,80}?celkem\s+([\d\s]+)",
    re.IGNORECASE | re.DOTALL,
)


def _parse_czk_tis(raw: str) -> Optional[float]:
    """Parse a CZK amount in tisíce (thousands) to full CZK value.

    Handles space-separated thousands: "1 197 097" → 1197097 * 1000
    """
    cleaned = raw.replace("\xa0", " ").strip()
    # Take first token sequence that looks like a space-separated number
    m = re.match(r"(\d[\d\s]*)", cleaned)
    if not m:
        return None
    num_str = re.sub(r"\s+", "", m.group(1)).strip()
    try:
        val = float(num_str)
        return val * 1000  # convert tis. Kč → Kč
    except ValueError:
        return None


def _extract_vydaje_after_chapter(text: str, chapter_re: re.Pattern) -> Optional[float]:
    """Find a chapter header in text, then extract 'Výdaje celkem' amount nearby."""
    m = chapter_re.search(text)
    if not m:
        return None
    # Look within 500 chars after the chapter header
    window = text[m.start(): min(m.start() + 500, len(text))]
    vm = _VYDAJE_RE.search(window)
    if not vm:
        return None
    raw = vm.group(1)
    return _parse_czk_tis(raw)


def _extract_annex3_amount(text: str, annex_re: re.Pattern) -> Optional[float]:
    """Extract amount from Annex 3 summary table (docx format)."""
    m = annex_re.search(text)
    if not m:
        return None
    raw = m.group(1)
    # Take the first space-delimited number group (may have multiple columns)
    # Format: "1 197 097 ..." — take first 7+ digit group
    nums = re.findall(r"\d[\d\s]+\d", raw[:50])
    for n in nums:
        val = _parse_czk_tis(n)
        if val and val >= 100_000_000:  # at least 100M CZK
            return val
    return None


def _extract_celkem_amount(text: str, celkem_re: re.Pattern) -> Optional[float]:
    """Extract amount from '{agency} celkem {amount}' lines."""
    m = celkem_re.search(text)
    if not m:
        return None
    for line in m.group(1).splitlines():
        for token in re.findall(r"\d{1,3}(?:[ \xa0]\d{3})+|\d{6,}", line):
            val = _parse_czk_tis(token)
            if val and val >= 100_000_000:
                return val
    return None


# ── Public API ─────────────────────────────────────────────────────────────────

def extract_czech_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract R&D budget records from a Czech State Budget annex file.

    Returns list of dicts matching the standard budget item schema.
    Amounts are in CZK (full, not thousands).
    """
    records: list[dict] = []

    # Concatenate all pages
    all_pages = [(int(row.page_number), row.text if isinstance(row.text, str) else "")
                 for row in sorted_pages.itertuples(index=False)]
    all_text = " ".join(t for _, t in all_pages)

    if not all_text.strip():
        return []

    # ── Try "Ukazatele kapitoly" format (PDFs 1997-2000, 2009) ────────────────
    agencies = [
        ("CZ_GACR", "Grantová agentura České republiky (GAČR)", _GACR_RE),
        ("CZ_AVCR", "Akademie věd České republiky (AV ČR)", _AVCR_RE),
    ]

    for code, name, re_pattern in agencies:
        amount = _extract_vydaje_after_chapter(all_text, re_pattern)
        if amount and amount >= 100_000_000:  # at least 100M CZK
            records.append({
                "country": country,
                "year": year,
                "section_code": "CZ_RD",
                "section_name": "Věda a výzkum (státní rozpočet)",
                "section_name_en": "Science and Research (State Budget)",
                "program_code": code,
                "line_description": name,
                "line_description_en": name,
                "amount_local": amount,
                "currency": "CZK",
                "unit": "CZK",
                "rd_category": "direct_rd",
                "taxonomy_score": 8.0,
                "decision": "include",
                "confidence": 0.85,
                "source_file": source_filename,
                "file_id": file_id,
                "page_number": 1,
            })

    if records:
        logger.info(
            "Czech extractor: %s (year %s) → %d records (Ukazatele format)",
            source_filename, year, len(records),
        )
        return records

    # ── Fallback: agency 'celkem' totals visible in appendix tables ──────────
    celkem_agencies = [
        ("CZ_GACR", "Grantová agentura České republiky (GAČR)", _CELKEM_GACR_RE),
        ("CZ_AVCR", "Akademie věd České republiky (AV ČR)", _CELKEM_AVCR_RE),
    ]
    for code, name, celkem_re in celkem_agencies:
        amount = _extract_celkem_amount(all_text, celkem_re)
        if amount and amount >= 100_000_000:
            records.append({
                "country": country,
                "year": year,
                "section_code": "CZ_RD",
                "section_name": "Věda a výzkum (státní rozpočet)",
                "section_name_en": "Science and Research (State Budget)",
                "program_code": code,
                "line_description": name,
                "line_description_en": name,
                "amount_local": amount,
                "currency": "CZK",
                "unit": "CZK",
                "rd_category": "direct_rd",
                "taxonomy_score": 8.0,
                "decision": "include",
                "confidence": 0.80,
                "source_file": source_filename,
                "file_id": file_id,
                "page_number": 1,
            })

    if records:
        logger.info(
            "Czech extractor: %s (year %s) → %d records (celkem fallback)",
            source_filename, year, len(records),
        )
        return records

    # ── Try Annex 3 summary table format (docx 2001-2004) ────────────────────
    annex_agencies = [
        ("CZ_GACR", "Grantová agentura České republiky (GAČR)", _ANNEX3_GACR_RE),
        ("CZ_AVCR", "Akademie věd České republiky (AV ČR)", _ANNEX3_AVCR_RE),
    ]

    for code, name, annex_re in annex_agencies:
        amount = _extract_annex3_amount(all_text, annex_re)
        if amount and amount >= 100_000_000:
            records.append({
                "country": country,
                "year": year,
                "section_code": "CZ_RD",
                "section_name": "Věda a výzkum (státní rozpočet)",
                "section_name_en": "Science and Research (State Budget)",
                "program_code": code,
                "line_description": name,
                "line_description_en": name,
                "amount_local": amount,
                "currency": "CZK",
                "unit": "CZK",
                "rd_category": "direct_rd",
                "taxonomy_score": 8.0,
                "decision": "include",
                "confidence": 0.85,
                "source_file": source_filename,
                "file_id": file_id,
                "page_number": 1,
            })

    if records:
        logger.info(
            "Czech extractor: %s (year %s) → %d records (Annex 3 format)",
            source_filename, year, len(records),
        )
    else:
        logger.debug(
            "Czech extractor: no extractable amounts in %s (year %s). "
            "PDF annexes for this year are likely scanned images.",
            source_filename, year,
        )

    return records
