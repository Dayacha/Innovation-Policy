"""Korean budget-briefing extractor.

The Korea archive is mostly made of budget overview / publicity PDFs rather than
appropriation schedules. The reliable signals are page-local summary tables:

1. Annual budget briefings with a sector row:
   ``5. R&D  19.5  19.6  0.1  0.9``
   where the current-year budget is the second numeric column.

2. Multi-year fiscal-plan tables:
   ``< 2021~2025년 분야별재원배분계획 >``
   ``5. R&D  27.4  29.8  32.3  34.0  35.4``
   where the value for the file year should be selected explicitly.

The archive does contain occasional Ministry of Science mentions, but they are
usually project blurbs rather than ministry-total pages. This extractor only
returns a ministry row when the page looks like a ministry-total context.
"""

from __future__ import annotations

import logging
import re
from typing import Optional


logger = logging.getLogger("innovation_pipeline")

_FLOAT_RE = r"\d{1,3}(?:\.\d+)?"
_AMOUNT_TEXT_RE = re.compile(
    r"(?P<amount>"
    r"\d{1,3}(?:\.\d+)?\s*조(?:\s*\d{1,4}(?:,\d{3})?\s*억)?(?:원)?"
    r"|\d{1,4}(?:,\d{3})+\s*억원?"
    r")"
)
_ANNUAL_HEADER_RE = re.compile(r"분야별\s*재원배분(?:\s*모습)?")
_FISCAL_PLAN_HEADER_RE = re.compile(r"(?P<start>20\d{2})\s*~\s*(?P<end>20\d{2})년\s*분야별\s*재원배분계획")
_ANNUAL_RD_ROW_RE = re.compile(
    rf"(?:^|\n)\s*(?:5\.\s*)?R\s*&\s*D\s+"
    rf"(?P<prior>{_FLOAT_RE})\s+"
    rf"(?P<current>{_FLOAT_RE})\s+"
    rf"(?P<delta>[+△\-]?\s*{_FLOAT_RE})",
    re.IGNORECASE,
)
_FISCAL_PLAN_RD_ROW_RE = re.compile(
    rf"(?:^|\n)\s*(?:5\.\s*)?R\s*&\s*D\s+"
    rf"(?P<v1>{_FLOAT_RE})\s+"
    rf"(?P<v2>{_FLOAT_RE})\s+"
    rf"(?P<v3>{_FLOAT_RE})\s+"
    rf"(?P<v4>{_FLOAT_RE})\s+"
    rf"(?P<v5>{_FLOAT_RE})",
    re.IGNORECASE,
)
_RD_TOTAL_LINE_RE = re.compile(
    r"(?:전체|정부|국가)\s*R\s*&\s*D[^\n]{0,40}?(?P<amount>\d{1,3}(?:\.\d+)?\s*조(?:\s*\d{1,4}(?:,\d{3})?\s*억)?(?:원)?)",
    re.IGNORECASE,
)
_MSIT_RE = re.compile(r"과학기술정보통신부|미래창조과학부|과학기술부|과학기술처")
_MINISTRY_TOTAL_RE = re.compile(
    r"(?:총예산|총지출|소관|예산안|예산)\s*[:：]?\s*"
    r"(?P<amount>\d{1,3}(?:\.\d+)?\s*조(?:\s*\d{1,4}(?:,\d{3})?\s*억)?(?:원)?|\d{1,4}(?:,\d{3})+\s*억원?)"
)


def _parse_krw(raw: str) -> Optional[float]:
    text = str(raw or "").replace(" ", "")
    total = 0.0
    found = False

    jo_match = re.search(r"(\d{1,3}(?:\.\d+)?)조", text)
    if jo_match:
        total += float(jo_match.group(1)) * 1e12
        found = True

    eok_match = re.search(r"(\d{1,4}(?:,\d{3})+|\d{1,4})억", text)
    if eok_match:
        total += float(eok_match.group(1).replace(",", "")) * 1e8
        found = True

    if not found and text.endswith("억원"):
        bare = text.replace("억원", "").replace("원", "").replace(",", "")
        if bare.isdigit():
            return float(bare) * 1e8

    return total if found and total > 0 else None


def _build_context(text: str, start: int, end: int, radius: int = 220) -> tuple[str, str, str, str]:
    lo = max(0, start - radius)
    hi = min(len(text), end + radius)
    before = text[lo:start].strip()
    raw = text[start:end].strip()
    after = text[end:hi].strip()
    merged = text[lo:hi].strip()
    return before, raw, after, merged


def _rd_total_plausible(amount: float) -> bool:
    return 1e13 <= amount <= 5e13


def _msit_plausible(amount: float) -> bool:
    return 5e11 <= amount <= 2e13


def _extract_annual_budget_row(page_text: str) -> Optional[tuple[float, str, int, int, str]]:
    if not _ANNUAL_HEADER_RE.search(page_text):
        return None
    match = _ANNUAL_RD_ROW_RE.search(page_text)
    if not match:
        return None
    amount = float(match.group("current")) * 1e12
    if not _rd_total_plausible(amount):
        return None
    return amount, match.group("current"), match.start("current"), match.end("current"), (
        "Annual sector-allocation table row for R&D."
    )


def _extract_fiscal_plan_row(page_text: str, file_year: int) -> Optional[tuple[float, str, int, int, str]]:
    header = _FISCAL_PLAN_HEADER_RE.search(page_text)
    if not header:
        return None
    match = _FISCAL_PLAN_RD_ROW_RE.search(page_text)
    if not match:
        return None
    start_year = int(header.group("start"))
    idx = file_year - start_year
    if idx < 0 or idx > 4:
        return None
    group_name = f"v{idx + 1}"
    raw = match.group(group_name)
    amount = float(raw) * 1e12
    if not _rd_total_plausible(amount):
        return None
    return amount, raw, match.start(group_name), match.end(group_name), (
        f"Multi-year fiscal-plan R&D row; selected the {file_year} column explicitly."
    )


def _extract_rd_total_line(page_text: str) -> Optional[tuple[float, str, int, int, str]]:
    match = _RD_TOTAL_LINE_RE.search(page_text)
    if not match:
        return None
    amount = _parse_krw(match.group("amount"))
    if not amount or not _rd_total_plausible(amount):
        return None
    return amount, match.group("amount"), match.start("amount"), match.end("amount"), (
        "Explicit total-government R&D amount on a page-local highlight line."
    )


def _extract_msit_total(page_text: str) -> Optional[tuple[float, str, int, int, str]]:
    ministry = _MSIT_RE.search(page_text)
    if not ministry:
        return None
    window_end = min(len(page_text), ministry.end() + 260)
    window = page_text[ministry.start() - 120 if ministry.start() > 120 else 0:window_end]
    if not re.search(r"총예산|총지출|소관|부문|예산안", window):
        return None
    amount_match = _MINISTRY_TOTAL_RE.search(window)
    if not amount_match:
        return None
    amount = _parse_krw(amount_match.group("amount"))
    if not amount or not _msit_plausible(amount):
        return None
    base = max(0, ministry.start() - 120)
    start = base + amount_match.start("amount")
    end = base + amount_match.end("amount")
    return amount, amount_match.group("amount"), start, end, (
        "Explicit ministry-total page for the science ministry."
    )


def _build_record(
    *,
    country: str,
    year: str,
    source_filename: str,
    file_id: str,
    page_number: int,
    section_name: str,
    section_name_en: str,
    program_code: str,
    line_description: str,
    line_description_en: str,
    amount_local: float,
    amount_raw: str,
    start: int,
    end: int,
    source_variant: str,
    rationale: str,
    page_text: str,
    confidence: float,
) -> dict:
    before, raw_line, after, merged = _build_context(page_text, start, end)
    return {
        "country": country,
        "year": year,
        "section_code": "KR_SCIENCE",
        "section_name": section_name,
        "section_name_en": section_name_en,
        "program_code": program_code,
        "line_description": line_description,
        "line_description_en": line_description_en,
        "amount_local": amount_local,
        "currency": "KRW",
        "unit": "KRW",
        "rd_category": "direct_rd",
        "taxonomy_score": 8.5 if program_code == "KR_RD_TOTAL" else 7.5,
        "decision": "include",
        "confidence": confidence,
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
        "annual_budget_table": 4,
        "rd_total_line": 3,
        "fiscal_plan_table": 2,
        "ministry_total_page": 1,
    }
    return (
        variant_order.get(str(rec.get("source_variant", "")), 0),
        float(rec.get("confidence") or 0),
        float(rec.get("amount_local") or 0),
        -int(rec.get("page_number") or 0),
    )


def extract_korea_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract Korea R&D totals from one PDF."""
    best_by_program: dict[str, dict] = {}
    try:
        year_int = int(str(year)[:4])
    except ValueError:
        year_int = 0

    for row in sorted_pages.itertuples(index=False):
        page_text = row.text if isinstance(row.text, str) else ""
        if not page_text.strip():
            continue
        page_number = int(getattr(row, "page_number", 1) or 1)

        fiscal_hit = _extract_fiscal_plan_row(page_text, year_int)
        annual_hit = None if fiscal_hit else _extract_annual_budget_row(page_text)
        rd_hit = fiscal_hit or annual_hit or _extract_rd_total_line(page_text)
        if rd_hit:
            amount_local, amount_raw, start, end, rationale = rd_hit
            source_variant = (
                "fiscal_plan_table" if fiscal_hit
                else "annual_budget_table" if annual_hit
                else "rd_total_line"
            )
            rec = _build_record(
                country=country,
                year=year,
                source_filename=source_filename,
                file_id=file_id,
                page_number=page_number,
                section_name="국가연구개발예산",
                section_name_en="National R&D budget",
                program_code="KR_RD_TOTAL",
                line_description="정부 전체 R&D 예산",
                line_description_en="Total government R&D budget",
                amount_local=amount_local,
                amount_raw=amount_raw,
                start=start,
                end=end,
                source_variant=source_variant,
                rationale=rationale,
                page_text=page_text,
                confidence=0.9 if source_variant == "annual_budget_table" else 0.82 if source_variant == "fiscal_plan_table" else 0.76,
            )
            current = best_by_program.get("KR_RD_TOTAL")
            if current is None or _record_key(rec) > _record_key(current):
                best_by_program["KR_RD_TOTAL"] = rec

        msit_hit = _extract_msit_total(page_text)
        if msit_hit:
            amount_local, amount_raw, start, end, rationale = msit_hit
            rec = _build_record(
                country=country,
                year=year,
                source_filename=source_filename,
                file_id=file_id,
                page_number=page_number,
                section_name="과학기술정보통신부",
                section_name_en="Ministry of Science and ICT",
                program_code="KR_MSIT",
                line_description="과학기술정보통신부 총예산",
                line_description_en="Ministry of Science and ICT total budget",
                amount_local=amount_local,
                amount_raw=amount_raw,
                start=start,
                end=end,
                source_variant="ministry_total_page",
                rationale=rationale,
                page_text=page_text,
                confidence=0.72,
            )
            current = best_by_program.get("KR_MSIT")
            if current is None or _record_key(rec) > _record_key(current):
                best_by_program["KR_MSIT"] = rec

    if not best_by_program:
        logger.debug("Korea extractor: no anchored science budget row found in %s", source_filename)
        return []
    return list(best_by_program.values())


__all__ = ["extract_korea_items"]
