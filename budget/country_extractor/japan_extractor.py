"""Japanese 一般会計予算 (General Account Budget) extractor.

Japan's science budget is most visible in three budget authorities:
- **文部省** (Monbusho, pre-2001)
- **科学技術庁** (Science and Technology Agency, pre-2001)
- **文部科学省** (MEXT, 2001+)

Document structure
------------------
The PDFs usually expose two useful representations after `pdftotext -layout`:

1. A summary table:

       甲号 歳入歳出予算 歳出
       所管 組織 項 金額 (千円)
       文 部 科 学 省 所 管 合 計  5,338,440,212

   or, for the pre-2001 Science and Technology Agency, a block total:

       科 学 技 術 庁  ... item lines ...
                  計  492,634,362

2. A detailed ministry section:

       平成7年度 文部省所管
       10 文部省所管合計 5,639,306,971

Amounts are typically in 千円, so this extractor multiplies parsed values by
1000 to return full JPY.

Strategy
--------
1. Work page-by-page instead of using the first whole-document ministry hit.
   This avoids table-of-contents false positives.
2. Prefer summary-table matches (`甲号 歳入歳出予算 歳出`).
3. Fall back to detailed `所管合計` pages.
4. For `科学技術庁`, which often appears under `総理府所管`, use the largest
   plausible block total (`計 {amount}`) near the agency name.

Graceful degradation: if OCR/text extraction is poor, return an empty list.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger("innovation_pipeline")


@dataclass(frozen=True)
class _Ministry:
    program_code: str
    section_name: str
    section_name_en: str
    line_description: str
    line_description_en: str
    active_from: int
    active_to: Optional[int]
    aliases: tuple[str, ...]
    min_raw: int


_MINISTRIES: tuple[_Ministry, ...] = (
    _Ministry(
        program_code="JP_MEXT",
        section_name="文部科学省",
        section_name_en="Ministry of Education, Culture, Sports, Science and Technology (MEXT)",
        line_description="文部科学省 所管合計",
        line_description_en="MEXT Total Expenditure",
        active_from=2001,
        active_to=None,
        aliases=("文部科学省", "文部科学省所管", "文部科学本省"),
        min_raw=1_000_000_000,
    ),
    _Ministry(
        program_code="JP_MONBUSHO",
        section_name="文部省",
        section_name_en="Ministry of Education (Monbusho)",
        line_description="文部省 所管合計",
        line_description_en="Monbusho Total Expenditure",
        active_from=1900,
        active_to=2000,
        aliases=("文部省", "文部省所管", "文部本省"),
        min_raw=1_000_000_000,
    ),
    _Ministry(
        program_code="JP_STA",
        section_name="科学技術庁",
        section_name_en="Science and Technology Agency (STA)",
        line_description="科学技術庁 計",
        line_description_en="Science and Technology Agency Total Expenditure",
        active_from=1956,
        active_to=2000,
        aliases=("科学技術庁", "科学技術庁所管", "科学技術振興費"),
        min_raw=100_000_000,
    ),
)

_AMOUNT_RE = re.compile(r"(?<!\d)(\d{1,3}(?:,\d{3}){2,}|\d{8,})(?!\d)")
_DETAIL_TOTAL_RE_TEMPLATE = r"{alias}(?:所管|主管)?合計(\d{{1,3}}(?:,\d{{3}}){{2,}}|\d{{8,}})"
_BLOCK_TOTAL_RE = re.compile(r"計(\d{1,3}(?:,\d{3}){2,}|\d{8,})", re.UNICODE)
_PAGE_HEADER_RE = re.compile(r"^\s*\d+\s*$")

_SUMMARY_LOOKAHEAD_PAGES = 5
_STA_SUMMARY_WINDOW = 700
_MIN_RAW_DEFAULT = 50_000_000
_MAX_RAW = 30_000_000_000


def _normalize_text(text: str) -> str:
    """Collapse layout whitespace so spaced Japanese OCR still matches."""
    return re.sub(r"[\s\u3000\xa0]+", "", text or "")


def _parse_amount(raw: str) -> Optional[float]:
    cleaned = re.sub(r"[,\s\u3000\xa0]", "", raw)
    try:
        value = float(cleaned)
    except ValueError:
        return None
    return value if value > 0 else None


def _is_plausible_raw(value: float, ministry: _Ministry) -> bool:
    return ministry.min_raw <= value <= _MAX_RAW


def _looks_like_summary_page(compact: str) -> bool:
    return "甲号歳入歳出予算歳出" in compact and "所管組織項金額" in compact


def _spaced_name_pattern(name: str) -> str:
    return r"\s*".join(re.escape(ch) for ch in name)


def _page_texts(sorted_pages) -> list[dict]:
    pages: list[dict] = []
    for row in sorted_pages.itertuples(index=False):
        text = row.text if isinstance(row.text, str) else ""
        page_number = int(getattr(row, "page_number", 1) or 1)
        pages.append({
            "page_number": page_number,
            "text": text,
            "compact": _normalize_text(text),
        })
    return pages


def _snippet(text: str, raw_value: str, width: int = 240) -> tuple[str, str, str]:
    idx = text.find(raw_value)
    if idx < 0:
        idx = 0
    start = max(0, idx - width)
    end = min(len(text), idx + len(raw_value) + width)
    center = text[start:end].strip()
    before = text[max(0, start - width):start].strip()
    after = text[end:min(len(text), end + width)].strip()
    return before, center, after


def _record(
    ministry: _Ministry,
    country: str,
    year: str,
    file_id: str,
    source_filename: str,
    page_number: int,
    raw_amount: str,
    center_text: str,
    context_before: str,
    context_after: str,
    confidence: float,
) -> dict:
    raw_value = _parse_amount(raw_amount)
    amount_local = (raw_value or 0.0) * 1000.0
    return {
        "country": country,
        "year": year,
        "section_code": "JP_SCIENCE",
        "section_name": ministry.section_name,
        "section_name_en": ministry.section_name_en,
        "program_code": ministry.program_code,
        "program_description": ministry.line_description,
        "program_description_en": ministry.line_description_en,
        "line_description": ministry.line_description,
        "line_description_en": ministry.line_description_en,
        "amount_local": amount_local,
        "amount_raw": raw_amount,
        "currency": "JPY",
        "unit": "JPY",
        "rd_category": "direct_rd",
        "taxonomy_score": 7.5,
        "decision": "include",
        "confidence": confidence,
        "source_file": source_filename,
        "source_filename": source_filename,
        "file_id": file_id,
        "page_number": page_number,
        "text_snippet": center_text,
        "context_before": context_before,
        "context_after": context_after,
        "raw_line": center_text,
        "merged_line": center_text,
    }


def _match_detail_total(page: dict, ministry: _Ministry) -> Optional[dict]:
    compact = page["compact"]
    for alias in ministry.aliases:
        pattern = re.compile(_DETAIL_TOTAL_RE_TEMPLATE.format(alias=re.escape(alias)))
        match = pattern.search(compact)
        if not match:
            continue
        raw_amount = match.group(1)
        raw_value = _parse_amount(raw_amount)
        if raw_value is None or not _is_plausible_raw(raw_value, ministry):
            continue
        before, center, after = _snippet(page["text"], raw_amount)
        return _record(
            ministry=ministry,
            country="",
            year="",
            file_id="",
            source_filename="",
            page_number=page["page_number"],
            raw_amount=raw_amount,
            center_text=center,
            context_before=before,
            context_after=after,
            confidence=0.84,
        )
    return None


def _summary_window(pages: list[dict], start_idx: int) -> tuple[str, str]:
    selected = pages[start_idx: start_idx + _SUMMARY_LOOKAHEAD_PAGES]
    raw = "\n".join(page["text"] for page in selected)
    compact = "".join(page["compact"] for page in selected)
    return raw, compact


def _find_explicit_summary_total(pages: list[dict], ministry: _Ministry) -> Optional[dict]:
    aliases = tuple(alias for alias in ministry.aliases if alias.endswith("省") or alias.endswith("庁"))
    patterns = [
        re.compile(rf"{re.escape(alias)}(?:所管|主管)?合計(\d{{1,3}}(?:,\d{{3}}){{2,}}|\d{{8,}})")
        for alias in aliases
    ]

    for idx, page in enumerate(pages):
        if not _looks_like_summary_page(page["compact"]) and not any(alias in page["compact"] for alias in aliases):
            continue
        raw_window, compact_window = _summary_window(pages, idx)
        for pattern in patterns:
            match = pattern.search(compact_window)
            if not match:
                continue
            raw_amount = match.group(1)
            raw_value = _parse_amount(raw_amount)
            if raw_value is None or not _is_plausible_raw(raw_value, ministry):
                continue
            for target_page in pages[idx: idx + _SUMMARY_LOOKAHEAD_PAGES]:
                if raw_amount in target_page["text"] and any(alias in target_page["compact"] for alias in aliases):
                    before, center, after = _snippet(target_page["text"], raw_amount)
                    return _record(
                        ministry=ministry,
                        country="",
                        year="",
                        file_id="",
                        source_filename="",
                        page_number=target_page["page_number"],
                        raw_amount=raw_amount,
                        center_text=center,
                        context_before=before,
                        context_after=after,
                        confidence=0.88,
                    )
            before, center, after = _snippet(raw_window, raw_amount)
            return _record(
                ministry=ministry,
                country="",
                year="",
                file_id="",
                source_filename="",
                page_number=page["page_number"],
                raw_amount=raw_amount,
                center_text=center,
                context_before=before,
                context_after=after,
                confidence=0.86,
            )
    return None


def _find_sta_summary_total(pages: list[dict], ministry: _Ministry) -> Optional[dict]:
    repeated_name_re = re.compile(
        rf"{_spaced_name_pattern('科学技術庁')}\s+{_spaced_name_pattern('科学技術庁')}",
        re.UNICODE,
    )
    single_name_re = re.compile(_spaced_name_pattern("科学技術庁"), re.UNICODE)
    block_total_re = re.compile(r"計\s*(\d{1,3}(?:,\d{3}){2,}|\d{8,})", re.UNICODE)

    for idx, page in enumerate(pages):
        compact = page["compact"]
        if "科学技術庁" not in compact:
            continue
        if not _looks_like_summary_page(compact):
            continue

        raw_window, _ = _summary_window(pages, idx)
        start_match = repeated_name_re.search(raw_window) or single_name_re.search(raw_window)
        if not start_match:
            continue
        section = raw_window[start_match.start(): start_match.start() + _STA_SUMMARY_WINDOW]
        matches: list[tuple[float, str]] = []
        for match in block_total_re.finditer(section):
            raw_amount = match.group(1)
            value = _parse_amount(raw_amount)
            if value is not None and _is_plausible_raw(value, ministry):
                matches.append((value, raw_amount))
        if not matches:
            continue
        raw_value, raw_amount = max(matches, key=lambda item: item[0])

        for target_page in pages[idx: idx + _SUMMARY_LOOKAHEAD_PAGES]:
            if raw_amount in target_page["text"]:
                before, center, after = _snippet(target_page["text"], raw_amount)
                return _record(
                    ministry=ministry,
                    country="",
                    year="",
                    file_id="",
                    source_filename="",
                    page_number=target_page["page_number"],
                    raw_amount=raw_amount,
                    center_text=center,
                    context_before=before,
                    context_after=after,
                    confidence=0.84,
                )

        raw_window, _ = _summary_window(pages, idx)
        before, center, after = _snippet(raw_window, raw_amount)
        return _record(
            ministry=ministry,
            country="",
            year="",
            file_id="",
            source_filename="",
            page_number=page["page_number"],
            raw_amount=raw_amount,
            center_text=center,
            context_before=before,
            context_after=after,
            confidence=0.8,
        )
    return None


def _extract_ministry(pages: list[dict], ministry: _Ministry) -> Optional[dict]:
    if ministry.program_code == "JP_STA":
        record = _find_sta_summary_total(pages, ministry)
        if record:
            return record

    record = _find_explicit_summary_total(pages, ministry)
    if record:
        return record

    for page in pages:
        record = _match_detail_total(page, ministry)
        if record:
            return record

    return None


def extract_japan_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract Japan science-budget ministry totals from a General Account PDF."""
    if sorted_pages is None or getattr(sorted_pages, "empty", False):
        return []

    pages = _page_texts(sorted_pages)
    if not any(page["compact"] for page in pages):
        logger.debug(
            "Japan extractor: no usable text for %s (%s).",
            source_filename,
            year,
        )
        return []

    try:
        year_int = int(float(str(year)))
    except (TypeError, ValueError):
        year_int = 9999

    records: list[dict] = []

    for ministry in _MINISTRIES:
        if year_int < ministry.active_from:
            continue
        if ministry.active_to is not None and year_int > ministry.active_to:
            continue

        record = _extract_ministry(pages, ministry)
        if not record:
            logger.debug(
                "Japan extractor: %s not found in %s (%s).",
                ministry.section_name,
                source_filename,
                year,
            )
            continue

        record.update({
            "country": country,
            "year": year,
            "file_id": file_id,
            "source_file": source_filename,
            "source_filename": source_filename,
        })
        records.append(record)

    if records:
        logger.info(
            "Japan extractor: %s (year %s) -> %d records",
            source_filename,
            year,
            len(records),
        )

    return records
