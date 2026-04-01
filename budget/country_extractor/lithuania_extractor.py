"""Lithuanian State Budget (Valstybes Biudzetas) extractor.

This extractor is archive-aware rather than a broad whole-document scraper.
The Lithuania archive mixes early functional-chapter rows with later
assignor-level institution tables:

1. Early/broad science rows:
   - ``XII. Mokslas ir studijos``
   - ``Mokslas ir studijos ...``
   - in some years the chapter block ends with ``Is viso``

2. Modern/narrow institution row:
   - ``Lietuvos mokslo taryba``

The extractor intentionally keeps these as separate programmes because they are
not directly comparable across the full time series.
"""

from __future__ import annotations

import logging
import re
from typing import Optional


logger = logging.getLogger("innovation_pipeline")

_NBSP = "\xa0"

_SCIENCE_CHAPTER_RE = re.compile(
    r"(?:XII\.\s*)?Mokslas\s+ir\s+studijos",
    re.IGNORECASE,
)
_SCIENCE_TOTAL_RE = re.compile(
    r"(?:XII\.\s*)?Mokslas\s+ir\s+studijos(?P<tail>[\s\S]{0,240})",
    re.IGNORECASE,
)
_SCIENCE_BLOCK_TOTAL_RE = re.compile(
    r"Mokslas\s+ir\s+studijos(?P<tail>[\s\S]{0,600}?Is\s+viso[\s:]{0,20}(?P<amount>\d{1,7}(?:[ \u00a0]\d{3})?))",
    re.IGNORECASE,
)
_RESEARCH_COUNCIL_RE = re.compile(
    r"Lietuvos\s+mokslo\s+taryba(?P<tail>[\s\S]{0,120})",
    re.IGNORECASE,
)
_MINISTRY_ROW_RE = re.compile(
    r"(?:S[vš]vietimo|Švietimo),?\s*mokslo(?:\s+ir\s+sporto)?\s+ministerija(?P<tail>[\s\S]{0,120})",
    re.IGNORECASE,
)
_TEXT_NOISE_RE = re.compile(
    r"\b(straipsnis|ministerijai\s+suteikiama\s+teise|paskirstyti|perskirsto)\b",
    re.IGNORECASE,
)


def _currency(year: str) -> str:
    try:
        return "EUR" if int(year) >= 2015 else "LTL"
    except (TypeError, ValueError):
        return "LTL"


def _scale_amount(year: int, amount: float) -> float:
    if year >= 2015:
        return amount * 1000
    return amount * 1000


def _parse_number_tokens(text: str) -> list[str]:
    return re.findall(r"\d+", text)


def _first_amount_from_tail(tail: str, *, prefer_grouped: bool = False) -> Optional[float]:
    tokens = _parse_number_tokens(tail)
    if not tokens:
        return None
    first = tokens[0]
    if len(tokens) >= 2 and len(tokens[1]) == 3 and (len(first) <= 2 or (prefer_grouped and len(first) <= 3)):
        try:
            return float(first + tokens[1])
        except ValueError:
            return None
    try:
        return float(first)
    except ValueError:
        return None


def _first_amount_from_group(raw: str) -> Optional[float]:
    cleaned = raw.replace(_NBSP, " ").replace(" ", "").strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _snippet(text: str, start: int, end: int, radius: int = 220) -> tuple[str, str, str, str]:
    lo = max(0, start - radius)
    hi = min(len(text), end + radius)
    before = text[lo:start].strip()
    raw = text[start:end].strip()
    after = text[end:hi].strip()
    merged = text[lo:hi].strip()
    return before, raw, after, merged


def _build_record(
    *,
    country: str,
    year: str,
    source_filename: str,
    file_id: str,
    page_number: int,
    currency: str,
    program_code: str,
    section_name: str,
    section_name_en: str,
    line_description: str,
    line_description_en: str,
    amount_local: float,
    amount_raw: str,
    raw_line: str,
    context_before: str,
    context_after: str,
    merged_line: str,
    source_variant: str,
    rationale: str,
    confidence: float,
) -> dict:
    return {
        "country": country,
        "year": year,
        "section_code": "LT_SCIENCE",
        "section_name": section_name,
        "section_name_en": section_name_en,
        "program_code": program_code,
        "line_description": line_description,
        "line_description_en": line_description_en,
        "amount_local": amount_local,
        "currency": currency,
        "unit": currency,
        "rd_category": "direct_rd",
        "taxonomy_score": 8.0,
        "decision": "include",
        "confidence": confidence,
        "source_file": source_filename,
        "file_id": file_id,
        "page_number": page_number,
        "amount_raw": amount_raw,
        "raw_line": raw_line,
        "merged_line": merged_line,
        "context_before": context_before,
        "context_after": context_after,
        "text_snippet": merged_line,
        "source_variant": source_variant,
        "rationale": rationale,
    }


def _extract_science_chapter(page_text: str) -> Optional[tuple[float, str, int, int, str]]:
    block_match = _SCIENCE_BLOCK_TOTAL_RE.search(page_text)
    if block_match:
        amount = _first_amount_from_group(block_match.group("amount"))
        if amount is not None and amount >= 10_000:
            return amount, block_match.group("amount"), block_match.start("amount"), block_match.end("amount"), (
                "Broad Mokslas ir studijos block with explicit Is viso total."
            )

    match = _SCIENCE_TOTAL_RE.search(page_text)
    if not match:
        return None
    amount = _first_amount_from_tail(match.group("tail"))
    if amount is None or amount < 10_000:
        return None
    amount_match = re.search(r"\d{1,7}(?:[ \u00a0]\d{3})?", match.group("tail"))
    if not amount_match:
        return None
    absolute_start = match.start("tail") + amount_match.start()
    absolute_end = match.start("tail") + amount_match.end()
    return amount, amount_match.group(0), absolute_start, absolute_end, (
        "Broad functional chapter row for Mokslas ir studijos."
    )


def _extract_research_council(page_text: str, year: int) -> Optional[tuple[float, str, int, int, str]]:
    if _TEXT_NOISE_RE.search(page_text[:1200]):
        ministry_like = _MINISTRY_ROW_RE.search(page_text)
    else:
        ministry_like = None
    if ministry_like and not _RESEARCH_COUNCIL_RE.search(page_text):
        return None

    match = _RESEARCH_COUNCIL_RE.search(page_text)
    if not match:
        return None
    amount = _first_amount_from_tail(match.group("tail"), prefer_grouped=year >= 2010)
    if amount is None:
        return None
    amount_match = re.search(r"\d{1,3}(?:[ \u00a0]\d{3})?|\d{4,7}", match.group("tail"))
    if not amount_match:
        return None
    absolute_start = match.start("tail") + amount_match.start()
    absolute_end = match.start("tail") + amount_match.end()
    return amount, amount_match.group(0), absolute_start, absolute_end, (
        "Explicit Lietuvos mokslo taryba row in assignor/institution table."
    )


def extract_lithuania_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract Lithuanian science budget rows from PDF/DOCX text."""
    try:
        year_int = int(year)
    except (TypeError, ValueError):
        year_int = 0

    currency = _currency(year)
    records: list[dict] = []

    for row in sorted_pages.itertuples(index=False):
        page_text = row.text if isinstance(row.text, str) else ""
        if not page_text.strip():
            continue
        page_number = int(getattr(row, "page_number", 1) or 1)

        chapter_hit = _extract_science_chapter(page_text)
        if chapter_hit and year_int <= 2002:
            amount, raw_amount, start, end, rationale = chapter_hit
            before, raw_line, after, merged = _snippet(page_text, start, end)
            records.append(
                _build_record(
                    country=country,
                    year=year,
                    source_filename=source_filename,
                    file_id=file_id,
                    page_number=page_number,
                    currency=currency,
                    program_code="LT_SCIENCE_CHAPTER",
                    section_name="Mokslas ir studijos",
                    section_name_en="Science and Studies",
                    line_description="Mokslas ir studijos",
                    line_description_en="Science and Studies chapter total",
                    amount_local=_scale_amount(year_int, amount),
                    amount_raw=raw_amount,
                    raw_line=raw_line,
                    context_before=before,
                    context_after=after,
                    merged_line=merged,
                    source_variant="science_chapter",
                    rationale=rationale,
                    confidence=0.82,
                )
            )

        council_hit = _extract_research_council(page_text, year_int)
        if council_hit and year_int >= 2003:
            amount, raw_amount, start, end, rationale = council_hit
            before, raw_line, after, merged = _snippet(page_text, start, end)
            records.append(
                _build_record(
                    country=country,
                    year=year,
                    source_filename=source_filename,
                    file_id=file_id,
                    page_number=page_number,
                    currency=currency,
                    program_code="LT_RESEARCH_COUNCIL",
                    section_name="Lietuvos mokslo taryba",
                    section_name_en="Research Council of Lithuania",
                    line_description="Lietuvos mokslo taryba",
                    line_description_en="Research Council of Lithuania",
                    amount_local=_scale_amount(year_int, amount),
                    amount_raw=raw_amount,
                    raw_line=raw_line,
                    context_before=before,
                    context_after=after,
                    merged_line=merged,
                    source_variant="research_council_row",
                    rationale=rationale,
                    confidence=0.88 if year_int >= 2010 else 0.74,
                )
            )

    deduped: dict[str, dict] = {}
    for rec in records:
        code = str(rec.get("program_code"))
        existing = deduped.get(code)
        if existing is None:
            deduped[code] = rec
            continue
        if float(rec.get("confidence") or 0) > float(existing.get("confidence") or 0):
            deduped[code] = rec
            continue
        if float(rec.get("amount_local") or 0) > float(existing.get("amount_local") or 0):
            deduped[code] = rec

    return list(deduped.values())
