"""Latvian State Budget (Valsts Budzets) extractor.

This extractor is page-local and prefers explicit science programme lines in
the older annex-style ministry tables. Many newer Latvia PDFs in the archive
preserve mostly legal articles and references to annexes rather than the annex
tables themselves, so the extractor intentionally skips those weak files
instead of forcing ministry totals from legal prose.

Primary signals confirmed manually from the archive:
  - Chapter 15: ``Izglitibas un zinatnes ministrija``
  - Narrow science lines such as:
      * ``05.01.00 01.310 Zinatniskas darbibas nodrosinasana``
      * ``05.02.00 01.310 Zinatnes bazes finansējums``
      * ``05.05.00 01.320 Tirgus ... petijumi``
      * ``05.06.00 01.330 Valsts parvaldes instituciju pasutitie petijumi``
      * ``05.12.00 Zinatnes konkurētspejas veicinasana``
      * ``03.12.00 Zinatniskas darbibas attistiba universitates``
      * ``03.15.00 Zinatniskas infrastrukturas nodrosinasana``

Currency:
  - LVL before 2014
  - EUR from 2014 onward
"""

from __future__ import annotations

import logging
import re
from typing import Optional


logger = logging.getLogger("innovation_pipeline")

_NBSP = "\xa0"
_AMOUNT_RE = re.compile(r"(?<!\d)(\d{1,3}(?:[ \u00a0]\d{3})+|\d{5,})(?!\d)")

_MINISTRY_HEADER_RE = re.compile(
    r"15\.\s*Izgl[iī]t[iī]bas\s+un\s+zin[aā]tnes\s+ministrija",
    re.IGNORECASE,
)
_MINISTRY_TOTAL_RE = re.compile(
    r"Izdevumi\s*[-–—]?\s*kop[aā][\s:]{0,20}"
    r"(?P<amount>\d{1,3}(?:[ \u00a0]\d{3})+|\d{5,})",
    re.IGNORECASE,
)

_SCIENCE_LINE_SPECS: tuple[tuple[str, str], ...] = (
    ("05.01.00", r"05\.01\.00[\s\S]{0,260}?Zin[aā]tnisk[āa]s\s+darb[iī]bas\s+nodro[sš]in[aā](?:jums|[sš]ana)"),
    ("05.02.00", r"05\.02\.00[\s\S]{0,260}?Zin[aā]tnes\s+b[aā]zes\s+finans[eē]jums"),
    ("05.05.00", r"05\.05\.00[\s\S]{0,260}?Tirgus[\s\S]{0,90}?p[eē]t[iī]jumi"),
    ("05.06.00", r"05\.06\.00[\s\S]{0,280}?Valsts\s+p[aā]rvaldes\s+instit[uū]ciju\s+pas[uū]t[iī]tie\s+p[eē]t[iī]jumi|05\.06\.00[\s\S]{0,220}?Valsts\s+instit[uū]ciju\s+pas[uū]t[iī]tie\s+p[eē]t[iī]jumi"),
    ("05.12.00", r"05\.12\.00[\s\S]{0,260}?Zin[aā]tnes\s+konkur[eē]tsp[eē]jas\s+veicin[aā](?:[sš]ana)?"),
)

_COUNCIL_RE = re.compile(
    r"Latvijas\s+Zin[aā]tnes\s+padomes\s+darb[iī]bas\s+nodro[sš]in[aā][sš]ana",
    re.IGNORECASE,
)
_ARTICLE_ONLY_RE = re.compile(
    r"\b\d+\.\s*pants\b|P[aā]rejas\s+noteikumi|Ministru\s+kabinets",
    re.IGNORECASE,
)
_NEXT_PROGRAM_RE = re.compile(r"\n\d{2}\.\d{2}\.\d{2}")
_SPENDING_LINE_RE = re.compile(
    r"(?:Izdevumi\s*[-–—]?\s*kop[aā]|Resursi\s+izdevumu\s+seg[sš]anai)[\s:]{0,20}"
    r"(?P<amount>\d{1,3}(?:[ \u00a0]\d{3})+|\d{5,})",
    re.IGNORECASE,
)


def _currency(year: str) -> str:
    try:
        return "EUR" if int(year) >= 2014 else "LVL"
    except (TypeError, ValueError):
        return "LVL"


def _parse_amount(raw: str) -> Optional[float]:
    cleaned = raw.replace(_NBSP, " ").replace(" ", "").strip()
    if not cleaned:
        return None
    try:
        value = float(cleaned)
    except ValueError:
        return None
    return value if value > 0 else None


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
        "section_code": "LV_SCIENCE",
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


def _extract_science_lines(page_text: str) -> list[tuple[str, float, str, int, int, str]]:
    hits: list[tuple[str, float, str, int, int, str]] = []
    seen_codes: set[str] = set()

    for code, pattern in _SCIENCE_LINE_SPECS:
        match = re.search(pattern, page_text, re.IGNORECASE)
        if not match:
            continue
        block_end = min(len(page_text), match.end() + 900)
        next_program = _NEXT_PROGRAM_RE.search(page_text, match.end())
        if next_program and next_program.start() < block_end:
            block_end = next_program.start()
        window = page_text[match.start(): block_end]
        amount_match = _SPENDING_LINE_RE.search(window)
        if not amount_match:
            amount_match = _AMOUNT_RE.search(window)
        if not amount_match:
            continue
        raw_amount = amount_match.group("amount") if "amount" in amount_match.re.groupindex else amount_match.group(1)
        amount = _parse_amount(raw_amount)
        if amount is None or amount < 50_000:
            continue
        if code in seen_codes:
            continue
        group_name = "amount" if "amount" in amount_match.re.groupindex else 1
        absolute_start = match.start() + amount_match.start(group_name)
        absolute_end = match.start() + amount_match.end(group_name)
        label = re.sub(r"\s+", " ", match.group(0)).strip()
        hits.append((code, amount, raw_amount, absolute_start, absolute_end, label))
        seen_codes.add(code)

    council_match = _COUNCIL_RE.search(page_text)
    if council_match and "05.15.00" not in seen_codes:
        window = page_text[council_match.start(): min(len(page_text), council_match.end() + 200)]
        amount_match = _AMOUNT_RE.search(window)
        if amount_match:
            amount = _parse_amount(amount_match.group(1))
            if amount is not None and amount >= 10_000:
                absolute_start = council_match.start() + amount_match.start(1)
                absolute_end = council_match.start() + amount_match.end(1)
                label = re.sub(r"\s+", " ", council_match.group(0)).strip()
                hits.append(("05.15.00", amount, amount_match.group(1), absolute_start, absolute_end, label))

    return hits


def _extract_ministry_total(page_text: str) -> Optional[tuple[float, str, int, int]]:
    header = _MINISTRY_HEADER_RE.search(page_text)
    if not header:
        return None
    window = page_text[header.start(): min(len(page_text), header.start() + 1200)]
    total_match = _MINISTRY_TOTAL_RE.search(window)
    if not total_match:
        return None
    amount = _parse_amount(total_match.group("amount"))
    if amount is None or amount < 5_000_000:
        return None
    absolute_start = header.start() + total_match.start("amount")
    absolute_end = header.start() + total_match.end("amount")
    return amount, total_match.group("amount"), absolute_start, absolute_end


def _dedupe_programme_records(records: list[dict]) -> list[dict]:
    best: dict[str, dict] = {}
    for rec in records:
        code = str(rec.get("program_code", ""))
        existing = best.get(code)
        if existing is None:
            best[code] = rec
            continue

        current_amount = float(existing.get("amount_local") or 0)
        candidate_amount = float(rec.get("amount_local") or 0)

        low = min(current_amount, candidate_amount)
        high = max(current_amount, candidate_amount)
        if low > 0 and high / low >= 2.5:
            best[code] = rec if candidate_amount == low else existing
            continue

        if float(rec.get("confidence") or 0) > float(existing.get("confidence") or 0):
            best[code] = rec
            continue

        if candidate_amount < current_amount:
            best[code] = rec

    return sorted(best.values(), key=lambda item: (item.get("page_number", 0), item.get("program_code", "")))


def extract_latvia_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract Latvian science budget lines from annex-style budget tables."""
    try:
        year_int = int(year)
    except (TypeError, ValueError):
        year_int = 0

    currency = _currency(year)
    records: list[dict] = []
    programme_hits: list[tuple[int, str, float, str, str, str, str]] = []
    fallback_record: Optional[dict] = None

    for row in sorted_pages.itertuples(index=False):
        page_text = row.text if isinstance(row.text, str) else ""
        if not page_text.strip():
            continue
        page_number = int(getattr(row, "page_number", 1) or 1)

        if year_int >= 2010 and _ARTICLE_ONLY_RE.search(page_text[:1200]) and not _MINISTRY_HEADER_RE.search(page_text):
            continue

        science_hits = _extract_science_lines(page_text)
        for code, amount, raw_amount, start, end, label in science_hits:
            before, raw_line, after, merged = _snippet(page_text, start, end)
            programme_hits.append(
                (
                    page_number,
                    code,
                    amount,
                    raw_amount,
                    label,
                    before,
                    after,
                )
            )
            records.append(
                _build_record(
                    country=country,
                    year=year,
                    source_filename=source_filename,
                    file_id=file_id,
                    page_number=page_number,
                    currency=currency,
                    program_code=f"LV_{code.replace('.', '_')}",
                    section_name="Izglītības un zinātnes ministrija",
                    section_name_en="Ministry of Education and Science",
                    line_description=label,
                    line_description_en=f"Latvia science programme {code}",
                    amount_local=amount,
                    amount_raw=raw_amount,
                    raw_line=raw_line,
                    context_before=before,
                    context_after=after,
                    merged_line=merged,
                    source_variant="science_programme_line",
                    rationale=f"Explicit Latvia science programme line matched in annex-style budget table: {label}.",
                    confidence=0.88 if code != "05.15.00" else 0.76,
                )
            )

        if programme_hits:
            continue

        if year_int and year_int <= 2006:
            ministry_total = _extract_ministry_total(page_text)
            if ministry_total:
                amount, raw_amount, start, end = ministry_total
                before, raw_line, after, merged = _snippet(page_text, start, end)
                fallback_record = _build_record(
                    country=country,
                    year=year,
                    source_filename=source_filename,
                    file_id=file_id,
                    page_number=page_number,
                    currency=currency,
                    program_code="LV_SCIENCE_MINISTRY",
                    section_name="Izglītības un zinātnes ministrija",
                    section_name_en="Ministry of Education and Science",
                    line_description="15. Izglītības un zinātnes ministrija - Izdevumi kopā",
                    line_description_en="Chapter 15 Ministry of Education and Science total expenditure",
                    amount_local=amount,
                    amount_raw=raw_amount,
                    raw_line=raw_line,
                    context_before=before,
                    context_after=after,
                    merged_line=merged,
                    source_variant="chapter15_total",
                    rationale="Chapter 15 ministry table with explicit Izdevumi - kopa total; kept only as fallback when narrow science lines are absent.",
                    confidence=0.62,
                )

    if programme_hits:
        return _dedupe_programme_records(records)
    if fallback_record is not None:
        return [fallback_record]

    logger.debug("Latvia extractor: no science budget found in %s (year %s).", source_filename, year)
    return []
