"""Estonian State Budget (Riigieelarve) extractor.

This extractor is page-local and era-aware. Estonian budget PDFs use several
distinct layouts:

1. 1990s numbered science lines under the education ministry, e.g.
   "81. Teadus- ja arendustegevus", "82. Sihtasutus Eesti Teadusfond"
2. 2001-2011 ministry spending pages with "Jagu 2" / "Osa 130" and explicit
   "teaduse sihtfinantseerimine" lines
3. 2024+ compact programme pages such as "Teadussüsteemi programm" with
   negative thousand-euro totals in OCR output

The extractor intentionally prefers explicit science lines and ministry
spending pages, and skips revenue pages and weak full-document matches.
"""

from __future__ import annotations

import logging
import re
from typing import Optional


logger = logging.getLogger("innovation_pipeline")

_NBSP = "\xa0"
_AMOUNT_RE = re.compile(r"(?<!\d)(-?\d{1,3}(?:[ \u00a0]\d{3})+|-?\d{6,})(?!\d)")

_LEGACY_SCIENCE_TOTAL_RE = re.compile(
    r"\b81\.\s*Teadus-\s*ja\s*arendustegevus\b[\s\S]{0,160}?"
    r"(-?\d{1,3}(?:[ \u00a0]\d{3})+|-?\d{6,})",
    re.IGNORECASE,
)
_TARGETED_FUNDING_RE = re.compile(
    r"(?:teaduse\s*sihtfinantseerimine|teadusesihtfinantseerimine|"
    r"teadus-\s*ja\s*arendusasutuste\s+teadusteemade\s+sihtfinantseerimine)"
    r"[\s\S]{0,180}?(-?\d{1,3}(?:[ \u00a0]\d{3})+|-?\d{6,})",
    re.IGNORECASE,
)
_BASE_FUNDING_RE = re.compile(
    r"teadus-\s*ja\s*arendusasutuste\s+baasfinantseerimine"
    r"[\s\S]{0,120}?(-?\d{1,3}(?:[ \u00a0]\d{3})+|-?\d{6,})",
    re.IGNORECASE,
)
_ETF_RE = re.compile(
    r"(?:Sihtasutus\s+)?Eesti\s+Teadusfond|Eesti\s+Teadusagentuur|ETAg\b",
    re.IGNORECASE,
)
_ETF_LINE_RE = re.compile(
    r"((?:Sihtasutus\s+)?Eesti\s+Teadusfond|Eesti\s+Teadusagentuur|ETAg\b)"
    r"[\s\S]{0,180}?(-?\d{1,3}(?:[ \u00a0]\d{3})+|-?\d{6,})",
    re.IGNORECASE,
)
_MINISTRY_HEADER_RE = re.compile(
    r"(?:Jagu\s*2\.?|Osa\s*130\.?)\s*HARIDUS-\s*JA\s*TEADUSMINISTEERIUMI?\s+valitsemisala"
    r"|HARIDUS-\s*JA\s*TEADUSMINISTEERIUMI?\s+valitsemisala",
    re.IGNORECASE,
)
_MINISTRY_AGENCY_RE = re.compile(
    r"70000740\.\s*Haridus-\s*ja\s*Teadusministeerium"
    r"[\s\S]{0,160}?(-?\d{1,3}(?:[ \u00a0]\d{3})+|-?\d{6,})",
    re.IGNORECASE,
)
_MINISTRY_TOTAL_RE = re.compile(
    r"RIIGIEELARVE\s+KULUD\s+KOKKU[\s\S]{0,120}?"
    r"(-?\d{1,3}(?:[ \u00a0]\d{3})+|-?\d{6,})",
    re.IGNORECASE,
)
_MODERN_PROGRAM_RE = re.compile(
    r"(?:Teaduss[üu]steemi\s+programm|Tulemusvaldkond:\s*TEADUS-\s*JA\s*ARENDUSTEGEVUS)"
    r"(?P<window>.{0,900})",
    re.IGNORECASE | re.DOTALL,
)
_BAD_CONTEXT_RE = re.compile(
    r"\b(TULUD\s+KOKKU|Kavandatavad\s+tulud|Eelarve\s+tulud|RIIGIEELARVE\s+TULUD\s+KOKKU)\b",
    re.IGNORECASE,
)
_ETF_BREAK_RE = re.compile(
    r"(Sihtasutus\s+Archimedes|Elukestva|Eesti\s+Infotehnoloogia|Kutsekvalifikatsiooni|Teaduskeskus|4500\.)",
    re.IGNORECASE,
)


def _determine_currency(year: str) -> str:
    try:
        return "EUR" if int(year) >= 2011 else "EEK"
    except (TypeError, ValueError):
        return "EEK"


def _parse_amount(raw: str) -> Optional[float]:
    cleaned = raw.replace(_NBSP, " ").replace(" ", "").strip()
    if not cleaned:
        return None
    try:
        return abs(float(cleaned))
    except ValueError:
        return None


def _resolve_split_amount(text: str, raw: str, end: int) -> Optional[float]:
    amount = _parse_amount(raw)
    if amount is None:
        return None
    tail = text[end:end + 24]
    cont = re.match(r"\s*(\d{3})(?!\d)", tail)
    compact_raw = raw.replace(_NBSP, " ").replace(" ", "")
    if cont and 50_000 <= amount < 600_000 and not compact_raw.endswith("000"):
        amount = amount * 1000 + int(cont.group(1))
    return amount


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
        "section_code": "EE_SCIENCE",
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


def _extract_ministry_total(page_text: str, year: int) -> Optional[tuple[float, str, int, int, str, str]]:
    if not _MINISTRY_HEADER_RE.search(page_text):
        return None
    if _BAD_CONTEXT_RE.search(page_text[:600]) and "§2" not in page_text[:600] and "KULUD" not in page_text[:600]:
        return None

    if year >= 2011:
        match = _MINISTRY_TOTAL_RE.search(page_text)
        if match:
            amount = _parse_amount(match.group(1))
            if amount and amount >= 50_000_000:
                return amount, match.group(1), match.start(1), match.end(1), "ministry_spending_page", (
                    "Explicit ministry spending page with RIIGIEELARVE KULUD KOKKU under Jagu 2."
                )

    if "§2" in page_text or "Määratud kulud kokku" in page_text:
        match = _MINISTRY_AGENCY_RE.search(page_text)
        if match:
            amount = _parse_amount(match.group(1))
            if amount and amount >= 100_000_000:
                return amount, match.group(1), match.start(1), match.end(1), "ministry_agency_page", (
                    "Haridus- ja Teadusministeerium agency total on ministry spending page."
                )

        header_text = page_text[:1200]
        if "Jagu 2" in header_text:
            amounts = []
            for m in _AMOUNT_RE.finditer(header_text):
                amount = _parse_amount(m.group(1))
                if amount and amount >= 100_000_000:
                    amounts.append((amount, m.group(1), m.start(1), m.end(1)))
            if amounts:
                amount, raw, start, end = max(amounts, key=lambda item: item[0])
                return amount, raw, start, end, "ministry_header_total", (
                    "Largest spending-side amount in the Jagu 2 ministry header block."
                )
    return None


def _extract_etf(page_text: str, year: int) -> Optional[tuple[float, str, int, int, str, str]]:
    for label_match in _ETF_RE.finditer(page_text):
        window = page_text[label_match.start():label_match.start() + 260]
        amount_match = _AMOUNT_RE.search(window)
        if not amount_match:
            continue
        break_match = _ETF_BREAK_RE.search(window)
        if break_match and break_match.start() < amount_match.start():
            continue
        raw = amount_match.group(1)
        absolute_start = label_match.start() + amount_match.start(1)
        absolute_end = label_match.start() + amount_match.end(1)
        amount = _resolve_split_amount(page_text, raw, absolute_end)
        if amount is None or amount < 500_000:
            continue
        if year >= 2007 and amount < 5_000_000:
            continue
        if year < 2011 and amount < 1_000_000:
            amount *= 1000
        label = label_match.group(0)
        return amount, raw, absolute_start, absolute_end, "legacy_line_item", (
            f"Explicit Estonia research-funder line matched: {label}."
        )
    return None


def _extract_research_funding(page_text: str, year: int) -> Optional[tuple[float, str, int, int, str, str, str]]:
    match = _TARGETED_FUNDING_RE.search(page_text)
    if match:
        amount = _resolve_split_amount(page_text, match.group(1), match.end(1))
        if amount and amount >= 1_000_000:
            return (
                amount,
                match.group(1),
                match.start(1),
                match.end(1),
                "legacy_science_line",
                "Explicit targeted/core science funding line under the education ministry.",
                "teaduse sihtfinantseerimine / teadusteemade sihtfinantseerimine",
            )

    match = _LEGACY_SCIENCE_TOTAL_RE.search(page_text)
    if match:
        amount = _resolve_split_amount(page_text, match.group(1), match.end(1))
        if amount and amount >= 1_000_000:
            return (
                amount,
                match.group(1),
                match.start(1),
                match.end(1),
                "legacy_science_section",
                "Explicit 81. Teadus- ja arendustegevus section total.",
                "81. Teadus- ja arendustegevus",
            )

    if year >= 2022:
        match = _MODERN_PROGRAM_RE.search(page_text)
        if match and _MINISTRY_HEADER_RE.search(page_text):
            window = match.group("window")
            candidates = []
            for amt_match in _AMOUNT_RE.finditer(window):
                amount = _parse_amount(amt_match.group(1))
                if amount and 20_000 <= amount <= 600_000:
                    candidates.append((amount, amt_match.group(1), amt_match.start(1), amt_match.end(1)))
            if candidates:
                amount, raw, rel_start, rel_end = max(candidates, key=lambda item: item[0])
                return (
                    amount * 1000,
                    raw,
                    match.start("window") + rel_start,
                    match.start("window") + rel_end,
                    "modern_program_page",
                    "Modern science-program total in thousand euros on the ministry science page.",
                    "Teadussüsteemi programm / Teadus- ja arendustegevus",
                )
    return None


def _best_record(existing: Optional[dict], candidate: dict) -> dict:
    if existing is None:
        return candidate

    variant_order = {
        "modern_program_page": 5,
        "legacy_science_line": 4,
        "legacy_science_section": 3,
        "ministry_spending_page": 3,
        "ministry_agency_page": 2,
        "ministry_header_total": 1,
        "legacy_line_item": 2,
    }
    current_key = (
        variant_order.get(str(existing.get("source_variant", "")), 0),
        float(existing.get("confidence") or 0),
        float(existing.get("amount_local") or 0),
        -int(existing.get("page_number") or 0),
    )
    candidate_key = (
        variant_order.get(str(candidate.get("source_variant", "")), 0),
        float(candidate.get("confidence") or 0),
        float(candidate.get("amount_local") or 0),
        -int(candidate.get("page_number") or 0),
    )
    return candidate if candidate_key > current_key else existing


def extract_estonia_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract Estonia science-budget records from one PDF."""
    currency = _determine_currency(year)
    try:
        numeric_year = int(year)
    except (TypeError, ValueError):
        numeric_year = 0

    best_by_program: dict[str, dict] = {}

    for row in sorted_pages.itertuples(index=False):
        page_text = row.text if isinstance(row.text, str) else ""
        if not page_text.strip():
            continue
        page_number = int(getattr(row, "page_number", 1) or 1)

        ministry_hit = _extract_ministry_total(page_text, numeric_year)
        if ministry_hit:
            amount, raw_amount, start, end, source_variant, rationale = ministry_hit
            before, raw_line, after, merged = _snippet(page_text, start, end)
            rec = _build_record(
                country=country,
                year=year,
                source_filename=source_filename,
                file_id=file_id,
                page_number=page_number,
                currency=currency,
                program_code="EE_SCIENCE_MINISTRY",
                section_name="Haridus- ja Teadusministeeriumi valitsemisala",
                section_name_en="Ministry of Education and Research - governance area",
                line_description="Haridus- ja Teadusministeeriumi valitsemisala kulud kokku",
                line_description_en="Ministry of Education and Research - total expenditure",
                amount_local=amount,
                amount_raw=raw_amount,
                raw_line=raw_line,
                context_before=before,
                context_after=after,
                merged_line=merged,
                source_variant=source_variant,
                rationale=rationale,
                confidence=0.82,
            )
            best_by_program["EE_SCIENCE_MINISTRY"] = _best_record(best_by_program.get("EE_SCIENCE_MINISTRY"), rec)

        etf_hit = _extract_etf(page_text, numeric_year)
        if etf_hit:
            amount, raw_amount, start, end, source_variant, rationale = etf_hit
            before, raw_line, after, merged = _snippet(page_text, start, end)
            rec = _build_record(
                country=country,
                year=year,
                source_filename=source_filename,
                file_id=file_id,
                page_number=page_number,
                currency=currency,
                program_code="EE_ETAG",
                section_name="Eesti Teadusfond / Eesti Teadusagentuur",
                section_name_en="Estonian Research Foundation / Research Council",
                line_description="Sihtasutus Eesti Teadusfond / Eesti Teadusagentuur",
                line_description_en="Estonian Research Foundation / Estonian Research Council",
                amount_local=amount,
                amount_raw=raw_amount,
                raw_line=raw_line,
                context_before=before,
                context_after=after,
                merged_line=merged,
                source_variant=source_variant,
                rationale=rationale,
                confidence=0.88,
            )
            best_by_program["EE_ETAG"] = _best_record(best_by_program.get("EE_ETAG"), rec)

        funding_hit = _extract_research_funding(page_text, numeric_year)
        if funding_hit:
            amount, raw_amount, start, end, source_variant, rationale, description = funding_hit
            before, raw_line, after, merged = _snippet(page_text, start, end)
            rec = _build_record(
                country=country,
                year=year,
                source_filename=source_filename,
                file_id=file_id,
                page_number=page_number,
                currency=currency,
                program_code="EE_RESEARCH_FUNDING",
                section_name="Teaduse rahastamine",
                section_name_en="Research funding",
                line_description=description,
                line_description_en="Research funding / science programme total",
                amount_local=amount,
                amount_raw=raw_amount,
                raw_line=raw_line,
                context_before=before,
                context_after=after,
                merged_line=merged,
                source_variant=source_variant,
                rationale=rationale,
                confidence=0.90 if source_variant != "modern_program_page" else 0.76,
            )
            best_by_program["EE_RESEARCH_FUNDING"] = _best_record(best_by_program.get("EE_RESEARCH_FUNDING"), rec)

    records = list(best_by_program.values())
    logger.debug("Estonia extractor: %s -> %s records", source_filename, len(records))
    return records


__all__ = ["extract_estonia_items"]
