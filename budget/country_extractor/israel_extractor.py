"""Israeli State Budget (Hok HaTakaziv) extractor.

The Israeli budget archive mixes several OCR regimes:

1. Historical summary tables with section 19 for the Ministry of Science
2. Detail pages for section 19 ("סעיף 19")
3. Modern summary tables where section 19 is often a combined ministry row
   such as "משרד המדע, הטכנולוגיה והחלל" or
   "משרד המדע החדשנות והטכנולוגיה, משרד התרבות"

This extractor is page-local and row-driven. It prefers explicit section-19
summary rows and falls back to section detail pages when the summary OCR is
too noisy.
"""

from __future__ import annotations

import logging
import re
from typing import Optional


logger = logging.getLogger("innovation_pipeline")

_AMOUNT_RE = r"(\d{1,3}(?:,\d{3})+|\d{4,})"
_HEBREW_SCIENCE_RE = (
    r"(?:משרד\s*)?(?:"
    r"המדע(?:\s*והפיתוח)?"
    r"|מדע(?:\s*וטכנולוגיה)?"
    r"|משרד\s*מדע\s*וטכנולוגיה"
    r"|משרד\s*המדע\s*,?\s*הטכנולוגיה\s*והחלל"
    r"|משרד\s*המדע\s*התרבות\s*והספורט"
    r"|משרד\s*המדע\s*החדשנות\s*והטכנולוגיה"
    r")"
)

_SUMMARY_HEADER_RE = re.compile(
    r"(?:ריכוז\s+התוספת\s+הראשונה|הראשונה\s+התוספת\s+ריכוז|תוספת\s+ראשונה)",
    re.IGNORECASE,
)
_MODERN_ROW_RE = re.compile(
    rf"(?<!\d)19(?!\d)\s*{_HEBREW_SCIENCE_RE}[\s\S]{{0,100}}?{_AMOUNT_RE}",
    re.IGNORECASE,
)
_LEGACY_ROW_RE = re.compile(
    rf"(?<!\d)19(?!\d)[\s\S]{{0,40}}?(?:והפיתוח|פיתוח|טכנולוגיה)?[\s\S]{{0,40}}?(?:המדע|מדע|המרע)"
    rf"[\s\S]{{0,80}}?{_AMOUNT_RE}",
    re.IGNORECASE,
)
_DETAIL_ROW_RE = re.compile(
    rf"סעיף[\s\S]{{0,60}}?{_HEBREW_SCIENCE_RE}[\s\S]{{0,20}}?19[\s\S]{{0,120}}?{_AMOUNT_RE}",
    re.IGNORECASE,
)
_ENGLISH_ROW_RE = re.compile(
    rf"(?<!\d)19(?!\d)[\s\S]{{0,60}}?Ministry\s+of\s+Science[\s\S]{{0,80}}?{_AMOUNT_RE}",
    re.IGNORECASE,
)


def _parse_thousands(raw: str) -> Optional[float]:
    cleaned = raw.replace(",", "").strip()
    try:
        val = float(cleaned)
        return val if val > 0 else None
    except ValueError:
        return None


def _build_context(text: str, start: int, end: int, radius: int = 220) -> tuple[str, str, str, str]:
    lo = max(0, start - radius)
    hi = min(len(text), end + radius)
    before = text[lo:start].strip()
    raw = text[start:end].strip()
    after = text[end:hi].strip()
    merged = text[lo:hi].strip()
    return before, raw, after, merged


def _plausible(amount_thousands: float) -> bool:
    return 1_000 <= amount_thousands <= 30_000_000


def _is_summary_like(page_text: str, page_number: int) -> bool:
    head = page_text[:600]
    return page_number <= 15 and any(token in head for token in ("ריכוז", "תוספת", "תקציב לשנת", "הצעת התקציב"))


def _scan_legacy_section_19(page_text: str) -> Optional[tuple[float, str, int, int, str, str]]:
    for sec in re.finditer(r"(?<!\d)19(?!\d)", page_text):
        window = page_text[sec.start():sec.start() + 120]
        science_match = re.search(r"מדע|המרע|והפיתוח|פיתוח|טכנולוג", window)
        if not science_match:
            continue
        amount_match = re.search(_AMOUNT_RE, window[science_match.end():science_match.end() + 60])
        if not amount_match:
            continue
        amount = _parse_thousands(amount_match.group(1))
        if amount and 1_000 <= amount <= 1_000_000:
            offset = science_match.end()
            absolute_start = sec.start() + offset + amount_match.start(1)
            absolute_end = sec.start() + offset + amount_match.end(1)
            return amount, amount_match.group(1), absolute_start, absolute_end, "legacy_summary_row", (
                "Historical section-19 science row on a summary page."
            )
    return None


def _extract_from_page(page_text: str, page_number: int) -> Optional[tuple[float, str, int, int, str, str]]:
    if not page_text.strip():
        return None

    # Modern clean summary page.
    if _SUMMARY_HEADER_RE.search(page_text):
        match = _MODERN_ROW_RE.search(page_text)
        if match:
            amount = _parse_thousands(match.group(1))
            if amount and _plausible(amount):
                return amount, match.group(1), match.start(1), match.end(1), "summary_row", (
                    "Explicit section-19 science-ministry row on the summary table."
                )

        match = _LEGACY_ROW_RE.search(page_text)
        if match:
            amount = _parse_thousands(match.group(1))
            if amount and _plausible(amount):
                return amount, match.group(1), match.start(1), match.end(1), "legacy_summary_row", (
                    "Legacy section-19 science row on the summary table."
                )

        match = _ENGLISH_ROW_RE.search(page_text)
        if match:
            amount = _parse_thousands(match.group(1))
            if amount and _plausible(amount):
                return amount, match.group(1), match.start(1), match.end(1), "summary_row", (
                    "English OCR fallback for the section-19 science row."
                )

    # Detail pages for section 19, common in mid-period files.
    match = _DETAIL_ROW_RE.search(page_text)
    if match:
        amount = _parse_thousands(match.group(1))
        if amount and _plausible(amount):
            return amount, match.group(1), match.start(1), match.end(1), "detail_section_page", (
                "Section 19 detail page for the science ministry."
            )

    # Historical OCR where summary pages lack the explicit header but still carry the row.
    if _is_summary_like(page_text, page_number):
        hit = _scan_legacy_section_19(page_text)
        if hit:
            return hit
    return None


def _build_record(
    *,
    country: str,
    year: str,
    source_filename: str,
    file_id: str,
    page_number: int,
    amount_thousands: float,
    amount_raw: str,
    start: int,
    end: int,
    source_variant: str,
    rationale: str,
    page_text: str,
) -> dict:
    before, raw_line, after, merged = _build_context(page_text, start, end)
    return {
        "country": country,
        "year": year,
        "section_code": "IL_SCIENCE",
        "section_name": "משרד המדע",
        "section_name_en": "Ministry of Science",
        "program_code": "IL_SCIENCE_MINISTRY",
        "line_description": "סעיף 19 - משרד המדע",
        "line_description_en": "Section 19 - Ministry of Science total appropriation",
        "amount_local": amount_thousands * 1000,
        "currency": "ILS",
        "unit": "ILS",
        "rd_category": "direct_rd",
        "taxonomy_score": 8.0,
        "decision": "include",
        "confidence": 0.86 if source_variant == "summary_row" else 0.8 if source_variant == "legacy_summary_row" else 0.74,
        "source_file": source_filename,
        "file_id": file_id,
        "page_number": page_number,
        "amount_raw": amount_raw,
        "raw_line": raw_line,
        "merged_line": merged,
        "context_before": before,
        "context_after": after,
        "text_snippet": merged,
        "source_variant": source_variant,
        "rationale": rationale,
    }


def _record_key(rec: dict) -> tuple:
    variant_order = {
        "summary_row": 4,
        "legacy_summary_row": 3,
        "detail_section_page": 2,
    }
    return (
        variant_order.get(str(rec.get("source_variant", "")), 0),
        float(rec.get("confidence") or 0),
        float(rec.get("amount_local") or 0),
        -int(rec.get("page_number") or 0),
    )


def extract_israel_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract the Israel science-ministry budget from one PDF."""
    best: Optional[dict] = None

    for row in sorted_pages.itertuples(index=False):
        page_text = row.text if isinstance(row.text, str) else ""
        if not page_text.strip():
            continue
        page_number = int(getattr(row, "page_number", 1) or 1)
        hit = _extract_from_page(page_text, page_number)
        if not hit:
            continue
        amount_thousands, amount_raw, start, end, source_variant, rationale = hit
        rec = _build_record(
            country=country,
            year=year,
            source_filename=source_filename,
            file_id=file_id,
            page_number=page_number,
            amount_thousands=amount_thousands,
            amount_raw=amount_raw,
            start=start,
            end=end,
            source_variant=source_variant,
            rationale=rationale,
            page_text=page_text,
        )
        if best is None or _record_key(rec) > _record_key(best):
            best = rec

    if best is None:
        logger.debug("Israel extractor: no section-19 science row found in %s", source_filename)
        return []
    return [best]


__all__ = ["extract_israel_items"]
