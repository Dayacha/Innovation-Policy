"""Israeli State Budget (Hok HaTakaziv) extractor.

The Israeli budget is published as the "חוק התקציב" (State Budget Law).
Structure:
- Budget amounts are in thousands of new Israeli shekels (NIS) in older files
  (pre-2000) and the denomination shifts over time.
- The summary table (first supplement / נספת ראשונה) lists ministries by
  section number, each on a line like:
    19והפיתוח\nהמדע\nמשרד6,540\n453\n54\n50.0
  (right-to-left Hebrew, section number + ministry name fragments + amounts)
- Ministry of Science section number: 19 (Ministry of Science and Development,
  משרד המדע והפיתוח) in earlier years; section number may change in later years.
- Currency: ILS (new shekel). Amounts in the summary table are in thousands NIS.
  Full value = extracted_amount × 1000.

Key Hebrew terms:
  מחקר ופיתוח  = R&D (research and development)
  מדע           = science
  משרד המדע     = Ministry of Science
  פיתוח         = development
  ישראל מדע קרן = Israel Science Foundation (ISF)

OCR quality varies significantly — many files are scanned images with poor text
quality.  The extractor tries multiple search strategies and returns [] on failure.
"""

from __future__ import annotations

import re
import logging
from typing import Optional

logger = logging.getLogger("innovation_pipeline")

# ── Regex patterns ─────────────────────────────────────────────────────────────

# Hebrew ministry-of-science name fragments (RTL text may be split across lines)
# "משרד המדע" = Ministry of Science
_SCIENCE_MINISTRY_RE = re.compile(
    r'(?:'
    r'(?:19)\s*\n?\s*(?:והפיתוח|המדע|מדע)'      # section 19 + science word
    r'|'
    r'(?:המדע|מדע)\s*\n?\s*(?:משרד|והפיתוח)'     # science + ministry word
    r'|'
    r'משרד\s*\n?\s*המדע'                          # explicit "Ministry of Science"
    r'|'
    r'Ministry\s+of\s+Science'                    # English fallback
    r')',
    re.IGNORECASE,
)

# Section 19 at the start of a summary line
# Pattern matches: "19" followed (possibly with newlines) by Hebrew ministry name
# and then an amount like "6,540" or "6540"
_SECTION19_RE = re.compile(
    r'19\s*\n?\s*'
    r'(?:[^\d\n]{0,60}\n?){0,5}'   # ministry name fragments (up to 5 lines)
    r'([\d,]+(?:\.\d+)?)',           # first numeric amount
    re.DOTALL,
)

# Standalone amount at start/end of a line — matches NIS amounts in summary tables
# Amounts are comma-grouped thousands: "6,540" or "147,093,437"
_AMOUNT_RE = re.compile(r'(?<!\d)([\d]{1,3}(?:,\d{3})+)(?!\d)')

# Pattern for the summary table line with section 19 and amount on same/adjacent lines
# Actual OCR text: "14מפלגות\nמימון22,123\n19והפיתוח\nהמדע\nמשרד6,540\n453\n54\n50.0"
# Note: "19" is immediately followed by Hebrew with NO whitespace, amount may be on same
# or next lines embedded in Hebrew text.
# We need to find amounts that appear AFTER the "19" marker, not before.
# The ministry name spans 2-3 lines before the first amount appears.
# The summary table in early Israeli budgets looks like:
#   "14מפלגות\nמימון22,123\n19והפיתוח\nהמדע\nמשרד6,540\n"
# Section 19 is immediately followed by Hebrew text (Unicode range U+0590-U+05FF),
# NOT by whitespace/newlines alone.
_HEBREW_CHAR = r'[\u0590-\u05FF\uFB1D-\uFB4F]'

_SUMMARY_LINE_RE = re.compile(
    r'(?<!\d)19(?!\d)'              # standalone "19"
    + _HEBREW_CHAR +                # MUST be immediately followed by Hebrew char
    r'[^\d\n]*\n'                   # rest of first line (Hebrew, no digits)
    r'(?:[^\d\n]*\n){0,4}'          # up to 4 more name lines (Hebrew, no digits)
    r'[^\d\n]*'                     # prefix on amount line
    r'([\d,]{3,})',                 # amount (first numeric value after name)
    re.DOTALL,
)


def _parse_ils(raw: str) -> Optional[float]:
    """Parse ILS amount string to float (removes commas)."""
    cleaned = raw.replace(',', '').strip()
    try:
        val = float(cleaned)
        return val if val > 0 else None
    except ValueError:
        return None


def _extract_amount_from_section19(text: str) -> Optional[float]:
    """Try to extract the Ministry of Science budget from section 19 in the summary table.

    The summary table in the first supplement lists sections as:
        {section_number}{ministry_name_fragments}{amount}{...other columns}

    For section 19 (Ministry of Science):
        "19והפיתוח\nהמדע\nמשרד6,540\n453\n54\n50.0"

    Returns the first (expenditure) amount, or None.
    """
    # Strategy 1: direct SUMMARY_LINE_RE
    m = _SUMMARY_LINE_RE.search(text)
    if m:
        val = _parse_ils(m.group(1))
        if val and val > 0:
            return val

    # Strategy 2: find standalone "19" then walk forward for amounts near science words
    for m19 in re.finditer(r'(?<!\d)19(?!\d)', text):
        window = text[m19.start(): m19.start() + 500]
        # Check that science-related Hebrew text is nearby
        if re.search(r'מדע|פיתוח|Ministry', window, re.IGNORECASE):
            amounts = _AMOUNT_RE.findall(window[:300])
            for amt_str in amounts:
                val = _parse_ils(amt_str)
                if val and val >= 100:   # at least 100 (thousands NIS = 100K NIS)
                    return val

    return None


def _search_full_text(text: str) -> Optional[float]:
    """Search full document text for science/R&D appropriation amounts.

    Used as fallback when section 19 approach fails.
    """
    # Look for R&D / science context
    for m in _SCIENCE_MINISTRY_RE.finditer(text):
        window = text[max(0, m.start() - 50): m.start() + 400]
        amounts = _AMOUNT_RE.findall(window)
        for amt_str in amounts:
            val = _parse_ils(amt_str)
            if val and val >= 100:
                return val
    return None


# ── Public API ─────────────────────────────────────────────────────────────────

def extract_israel_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract Ministry of Science budget from an Israeli State Budget PDF.

    Strategy:
    1. Concatenate all page texts.
    2. Search for section 19 (Ministry of Science and Development) in the budget
       summary table — amounts are in thousands of NIS.
    3. Multiply extracted amount by 1000 to get full NIS value.
    4. Return [] if no reliable amount found (Hebrew OCR often fails).

    Note: Hebrew is right-to-left; PyMuPDF may reverse word/character order.
    The extractor searches for section number 19 and science keywords in proximity.
    """
    records: list[dict] = []

    try:
        all_text = "\n".join(
            (row.text if isinstance(row.text, str) else "")
            for row in sorted_pages.itertuples(index=False)
        )
    except Exception as exc:
        logger.warning("Israel extractor: failed to read pages for %s: %s", source_filename, exc)
        return []

    if not all_text.strip():
        logger.debug("Israel extractor: no text in %s — likely scanned image.", source_filename)
        return []

    # Try section 19 extraction (primary strategy)
    amount_thousands = _extract_amount_from_section19(all_text)

    # Fallback: search by science ministry name
    if amount_thousands is None:
        amount_thousands = _search_full_text(all_text)

    if amount_thousands is None:
        logger.debug(
            "Israel extractor: no science section found in %s (year %s).",
            source_filename, year,
        )
        return []

    # Amounts in the summary table are in thousands of NIS
    full_amount = amount_thousands * 1000

    # Sanity check: Ministry of Science budget should be reasonable
    # In 1990: ~6,540 thousands NIS = 6.5M NIS; by 2000s it grows significantly
    if full_amount < 1_000_000 or full_amount > 50_000_000_000:
        logger.debug(
            "Israel extractor: amount %s outside plausible range for %s (year %s).",
            full_amount, source_filename, year,
        )
        return []

    records.append({
        "country": country,
        "year": year,
        "section_code": "IL_SCIENCE",
        "section_name": "משרד המדע והפיתוח",
        "section_name_en": "Ministry of Science and Development",
        "program_code": "IL_SCIENCE_MINISTRY",
        "line_description": "Ministry of Science - total budget (section 19)",
        "line_description_en": "Ministry of Science and Development - total appropriation",
        "amount_local": full_amount,
        "currency": "ILS",
        "unit": "ILS",
        "rd_category": "direct_rd",
        "taxonomy_score": 8.0,
        "decision": "include",
        "confidence": 0.75,
        "source_file": source_filename,
        "file_id": file_id,
        "page_number": 1,
    })

    logger.info(
        "Israel extractor: %s (year %s) → %d records, amount=%.0f ILS",
        source_filename, year, len(records), full_amount,
    )
    return records
