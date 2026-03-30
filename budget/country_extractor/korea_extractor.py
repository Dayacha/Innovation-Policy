"""Korean Budget (예산안) extractor.

Korean budget files (2018–2025) are budget SUMMARY/HIGHLIGHTS documents in Korean.
They contain sector-level breakdowns with R&D shown as a line item in the sector
allocation table.

Document structure (from actual scans):
  Sector table line example:
    "5. R&D  19.5  19.6  0.1  0.9"
  where columns are: sector | prior year | current year | change | % change
  Amounts are in 조원 (jo-won = trillion KRW = 10^12 won).

  For national R&D total (전체 R&D):
    "· 전체 R&D  27.4조원  29.8조원  +8.8%"
  or:
    "· 전체 R&D\n27.4조원\n29.8조원\n+8.8%"

Korean number notation:
  조 (jo)  = 1 trillion = 10^12
  억 (eok) = 100 million = 10^8
  만 (man) = 10,000 = 10^4

Programs extracted:
  KR_RD_TOTAL : national government R&D total budget (전체 R&D / 국가연구개발예산)
  KR_MSIT     : Ministry of Science and ICT (과학기술정보통신부) budget

Currency: KRW (Korean won), full values.
"""

from __future__ import annotations

import re
import logging
from typing import Optional

logger = logging.getLogger("innovation_pipeline")

# ── Korean number parsing ───────────────────────────────────────────────────────

# Korean unit markers
_JO_RE = re.compile(r'([\d,\.]+)\s*조')     # 조 = 1 trillion
_EOK_RE = re.compile(r'([\d,\.]+)\s*억')    # 억 = 100 million
_MAN_RE = re.compile(r'([\d,\.]+)\s*만')    # 만 = 10,000


def _parse_krw(text: str) -> Optional[float]:
    """Parse Korean amount text to full KRW value.

    Handles formats like:
      "30조 7,000억원"  → 30 * 10^12 + 7000 * 10^8
      "29.8조원"        → 29.8 * 10^12
      "19조 6,000억원"  → 19 * 10^12 + 6000 * 10^8
    """
    total = 0.0
    found = False

    jo_m = _JO_RE.search(text)
    if jo_m:
        try:
            total += float(jo_m.group(1).replace(',', '')) * 1e12
            found = True
        except ValueError:
            pass

    eok_m = _EOK_RE.search(text)
    if eok_m:
        try:
            total += float(eok_m.group(1).replace(',', '')) * 1e8
            found = True
        except ValueError:
            pass

    man_m = _MAN_RE.search(text)
    if man_m:
        try:
            total += float(man_m.group(1).replace(',', '')) * 1e4
            found = True
        except ValueError:
            pass

    return total if found and total > 0 else None


def _parse_sector_table_amount(text: str) -> Optional[float]:
    """Parse amounts from the sector breakdown table.

    Sector table format: "5. R&D  19.5  19.6  0.1  0.9"
    The second numeric column is the current-year budget in 조원.
    """
    # Match plain decimal number (in 조원) — e.g., "19.6"
    # The pattern: R&D line with 2+ numbers following
    m = re.search(
        r'R\s*&\s*D\s+'
        r'([\d\.]+)\s+'    # prior year
        r'([\d\.]+)',      # current year  ← this is what we want
        text,
    )
    if m:
        try:
            val = float(m.group(2)) * 1e12  # convert 조원 to KRW
            return val if val > 0 else None
        except ValueError:
            pass
    return None


# ── Regex patterns ──────────────────────────────────────────────────────────────

# National R&D total — "전체 R&D" or "국가연구개발예산" or R&D sector line
_RD_TOTAL_PATTERNS = [
    # Explicit R&D total line: "전체 R&D  29.8조원" or "전체 R&D\n29.8조원"
    re.compile(
        r'전체\s*R\s*[&＆]\s*D\s*[\n\s]*([\d,\.]+\s*조\s*\S*)',
        re.IGNORECASE,
    ),
    # With + sign: "· 전체 R&D  27.4조원  29.8조원"
    re.compile(
        r'전체\s*R\s*[&＆]\s*D\s+'
        r'[\d,\.]+\s*조원?\s+'    # prior year
        r'([\d,\.]+\s*조원?)',    # current year
        re.IGNORECASE,
    ),
    # Generic R&D sector in budget table: "R&D 19.5 19.6"
    re.compile(
        r'(?:^|\n)[^전체\n]*\bR\s*&\s*D\b[^\n]*\n?'
        r'\s*([\d\.]+)\s+'
        r'([\d\.]+)',
        re.IGNORECASE | re.MULTILINE,
    ),
    # "국가연구개발예산" (national R&D budget): "국가연구개발예산 XX조 YY억원"
    re.compile(
        r'국가연구개발(?:예산|비)?\s*[:\s]?\s*([\d,\.]+\s*조[^\n]{0,30})',
        re.IGNORECASE,
    ),
]

# Ministry of Science and ICT
_MSIT_RE = re.compile(
    r'과학기술정보통신부'
    r'|Ministry\s+of\s+Science\s+and\s+ICT',
    re.IGNORECASE,
)

# Typical amount after MSIT mention
_MSIT_AMOUNT_RE = re.compile(
    r'과학기술정보통신부[^\n]*\n?[^\n]*?([\d,\.]+\s*조[^\n]{0,20})',
    re.IGNORECASE,
)


def _extract_rd_total(full_text: str) -> Optional[float]:
    """Extract total national R&D budget from the summary document."""

    # Pattern 1: "전체 R&D  {prior}조원  {current}조원"
    m = re.search(
        r'전체\s*R\s*[&＆]\s*D\s+'
        r'([\d,\.]+)\s*조원?\s+'
        r'([\d,\.]+)\s*조원?',
        full_text, re.IGNORECASE,
    )
    if m:
        try:
            val = float(m.group(2).replace(',', '')) * 1e12
            if val > 0:
                return val
        except ValueError:
            pass

    # Pattern 2: "전체 R&D\n{amount}" (newline separated)
    m = re.search(
        r'전체\s*R\s*[&＆]\s*D[^\n]*\n\s*([\d,\.]+\s*조[^\n]{0,40})',
        full_text, re.IGNORECASE,
    )
    if m:
        val = _parse_krw(m.group(1))
        if val:
            return val

    # Pattern 3: sector table "5. R&D  {prior}  {current}"
    m = re.search(
        r'5\.\s*R\s*&\s*D\s+'
        r'([\d\.]+)\s+'
        r'([\d\.]+)',
        full_text, re.IGNORECASE,
    )
    if m:
        try:
            # Amounts are in 조원 in the sector table
            val = float(m.group(2).replace(',', '')) * 1e12
            if val > 0:
                return val
        except ValueError:
            pass

    # Pattern 4: "국가연구개발예산 XX조원" or "R&D 예산 XX조원"
    m = re.search(
        r'(?:국가연구개발|R\s*&\s*D)\s*(?:예산|비)\s*(?:[:\s])'
        r'\s*([\d,\.]+\s*조[^\n]{0,30})',
        full_text, re.IGNORECASE,
    )
    if m:
        val = _parse_krw(m.group(1))
        if val:
            return val

    # Pattern 5: "R&D {prior}조원 {current}조원" (any R&D mention)
    for m in re.finditer(
        r'\bR\s*&\s*D\b[^\n]{0,30}'
        r'([\d,\.]+\s*조원?)\s+'
        r'([\d,\.]+\s*조원?)',
        full_text, re.IGNORECASE,
    ):
        val = _parse_krw(m.group(2))
        if val and val >= 1e12:  # at least 1 trillion KRW
            return val

    return None


def _extract_msit_amount(full_text: str) -> Optional[float]:
    """Extract Ministry of Science and ICT budget."""
    for m in _MSIT_RE.finditer(full_text):
        window = full_text[m.start(): m.start() + 500]
        # Look for 조원 amount nearby
        am = re.search(r'([\d,\.]+\s*조[^\n]{0,30})', window)
        if am:
            val = _parse_krw(am.group(1))
            if val and val >= 1e11:  # at least 100 billion KRW
                return val
    return None


# ── Public API ─────────────────────────────────────────────────────────────────

def extract_korea_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract R&D budget records from a Korean budget summary PDF.

    Korean files (2018–2025) are budget highlights/summary documents.
    They contain a sector allocation table showing R&D as one line item.
    Amounts are in 조원 (1 조원 = 1 trillion KRW).

    Extracts:
    - KR_RD_TOTAL: total government R&D budget
    - KR_MSIT: Ministry of Science and ICT budget (if found)

    Returns [] on failure (Korean PDFs may have encoding issues).
    """
    records: list[dict] = []

    try:
        all_text = "\n".join(
            (row.text if isinstance(row.text, str) else "")
            for row in sorted_pages.itertuples(index=False)
        )
    except Exception as exc:
        logger.warning("Korea extractor: failed to read pages for %s: %s", source_filename, exc)
        return []

    if not all_text.strip():
        logger.debug("Korea extractor: no text in %s.", source_filename)
        return []

    # ── 1. National R&D total ─────────────────────────────────────────────────
    rd_total = _extract_rd_total(all_text)
    if rd_total and rd_total >= 1e12:  # at least 1 trillion KRW
        records.append({
            "country": country,
            "year": year,
            "section_code": "KR_SCIENCE",
            "section_name": "R&D 예산 (국가연구개발)",
            "section_name_en": "National R&D budget",
            "program_code": "KR_RD_TOTAL",
            "line_description": "전체 R&D 예산 - 정부 총 연구개발비",
            "line_description_en": "Total government R&D budget",
            "amount_local": rd_total,
            "currency": "KRW",
            "unit": "KRW",
            "rd_category": "direct_rd",
            "taxonomy_score": 9.0,
            "decision": "include",
            "confidence": 0.85,
            "source_file": source_filename,
            "file_id": file_id,
            "page_number": 1,
        })

    # ── 2. Ministry of Science and ICT ────────────────────────────────────────
    msit_amount = _extract_msit_amount(all_text)
    if msit_amount and msit_amount >= 1e11:
        records.append({
            "country": country,
            "year": year,
            "section_code": "KR_SCIENCE",
            "section_name": "과학기술정보통신부",
            "section_name_en": "Ministry of Science and ICT (MSIT)",
            "program_code": "KR_MSIT",
            "line_description": "과학기술정보통신부 예산",
            "line_description_en": "Ministry of Science and ICT - total budget",
            "amount_local": msit_amount,
            "currency": "KRW",
            "unit": "KRW",
            "rd_category": "direct_rd",
            "taxonomy_score": 8.0,
            "decision": "include",
            "confidence": 0.75,
            "source_file": source_filename,
            "file_id": file_id,
            "page_number": 1,
        })

    if records:
        logger.info(
            "Korea extractor: %s (year %s) → %d records",
            source_filename, year, len(records),
        )
    else:
        logger.debug(
            "Korea extractor: no R&D budget found in %s (year %s).",
            source_filename, year,
        )

    return records
