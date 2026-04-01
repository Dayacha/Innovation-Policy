"""Hungarian state budget extractor.

Manual review of the CompLex / Magyar Kozlony budget laws shows two stable
science-policy anchors:

1. OTKA / "Orszagos Tudomanyos Kutatasi Alapprogramok" in chapter XXXIII
   through the early 2010s.
2. "LXII. Nemzeti Kutatasi, Fejlesztesi es Innovacios Alap" from the mid-2010s
   onward.

The extractor therefore uses a two-era approach and avoids broad name scraping
for "Magyar Tudomanyos Akademia", because in many modern files that appears
mainly in legal boilerplate rather than in the appropriations tables.

Amounts are printed in "millio forintban", so all captured values are scaled to
full HUF.
"""

from __future__ import annotations

import logging
import re
from typing import Iterable, Optional


logger = logging.getLogger("innovation_pipeline")

_NBSP = "\xa0"

_OTKA_LINE_RE = re.compile(
    r"Orsz[aá]gos\s+Tudom[aá]nyos\s+Kutat[aá]si\s+Alap(?:programok)?"
    r"[^\n]{0,140}?(\d{1,3}(?:[ \u00a0]\d{3})*(?:,\d+)?)",
    re.IGNORECASE,
)
_OTKA_LABEL_RE = re.compile(
    r"Orsz[aá]gos\s+Tudom[aá]nyos\s+Kutat[aá]si\s+Alap(?:programok)?",
    re.IGNORECASE,
)
_OTKA_BLOCK_END_RE = re.compile(
    r"(?:[IVXLCDM]+\.\s*fejezet\s+összesen|[IVXLCDM]+\.\s+[A-ZÁÉÍÓÖŐÚÜŰ]|KIADÁSI FŐÖSSZEG)",
    re.IGNORECASE,
)
_OTKA_COMPONENT_RE = re.compile(
    r"^\s*\d+\s+([^\n]+?)\s+(\d{1,3}(?:[ \u00a0]\d{3})*(?:,\d+)?)\s*$",
    re.IGNORECASE | re.MULTILINE,
)
_OTKA_COMPONENT_SKIP_RE = re.compile(
    r"(m[űu]k[öo]d[eé]si k[öo]lts[eé]gvet[eé]s|szem[eé]lyi juttat[aá]sok|"
    r"munkaad[oó]kat terhel[őo] j[aá]rul[eé]kok|dologi kiad[aá]sok|"
    r"felhalmoz[aá]si k[öo]lts[eé]gvet[eé]s|int[eé]zm[eé]nyi beruh[aá]z[aá]si kiad[aá]sok|"
    r"fel[uú]j[ií]t[aá]s|egy[eé]b m[űu]k[öo]d[eé]si c[eé]l[úu])",
    re.IGNORECASE,
)

_NKFI_HEADER_RE = re.compile(
    r"LXII\.\s*(?:NEMZETI\s+KUTAT[AÁ]SI,\s*FEJLESZT[EÉ]SI\s+[EÉ]S\s+INNOV[AÁ]CI[ÓO]S\s+ALAP|"
    r"Nemzeti\s+Kutat[aá]si,\s*Fejleszt[eé]si\s+[eé]s\s+Innov[aá]ci[oó]s\s+Alap)",
    re.IGNORECASE,
)
_NKFI_TOTAL_RE = re.compile(
    r"LXII\.\s*fejezet\s+[öo]sszesen:?\s*"
    r"(\d{1,3}(?:[ \u00a0]\d{3})*(?:,\d+)?)",
    re.IGNORECASE,
)
_NKFI_RESEARCH_ROW_RE = re.compile(
    r"Kutat[aá]si\s+Alapr[eé]sz[^\n]{0,120}?(\d{1,3}(?:[ \u00a0]\d{3})*(?:,\d+)?)",
    re.IGNORECASE,
)
_NKFI_INNOVATION_ROW_RE = re.compile(
    r"Innov[aá]ci[oó]s\s+Alapr[eé]sz[^\n]{0,120}?(\d{1,3}(?:[ \u00a0]\d{3})*(?:,\d+)?)",
    re.IGNORECASE,
)


def _year_int(year: str) -> int:
    try:
        return int(year)
    except (TypeError, ValueError):
        return 0


def _parse_million_huf(raw: str) -> Optional[float]:
    cleaned = raw.replace(_NBSP, " ").replace(" ", "").strip().replace(",", ".")
    if not cleaned:
        return None
    try:
        return float(cleaned) * 1_000_000
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
        "section_code": "HU_SCIENCE",
        "section_name": section_name,
        "section_name_en": section_name_en,
        "program_code": program_code,
        "line_description": line_description,
        "line_description_en": line_description_en,
        "amount_local": amount_local,
        "currency": "HUF",
        "unit": "HUF",
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


def _iter_pages(sorted_pages) -> Iterable[tuple[int, str]]:
    for row in sorted_pages.itertuples(index=False):
        page_number = int(getattr(row, "page_number", 1) or 1)
        text = getattr(row, "text", "") or ""
        if isinstance(text, str) and text.strip():
            yield page_number, text


def _extract_otka(page_text: str) -> Optional[tuple[float, str, int, int, str, str]]:
    explicit = _OTKA_LINE_RE.search(page_text)
    if explicit:
        amount = _parse_million_huf(explicit.group(1))
        if amount and amount >= 500_000_000:
            return (
                amount,
                explicit.group(1),
                explicit.start(1),
                explicit.end(1),
                "explicit_program_line",
                "Explicit OTKA / scientific research fund line in chapter XXXIII.",
            )

    label_match = _OTKA_LABEL_RE.search(page_text)
    if not label_match:
        return None

    block = page_text[label_match.start():]
    end_match = _OTKA_BLOCK_END_RE.search(block[1:])
    block = block[: end_match.start() + 1] if end_match else block[:1600]

    component_sum = 0.0
    component_raw: list[str] = []
    for match in _OTKA_COMPONENT_RE.finditer(block):
        label = match.group(1).strip()
        if _OTKA_COMPONENT_SKIP_RE.search(label):
            continue
        amount = _parse_million_huf(match.group(2))
        if amount is None or amount < 50_000_000:
            continue
        component_sum += amount
        component_raw.append(match.group(2))

    if component_sum >= 500_000_000:
        amount_text = " + ".join(component_raw)
        amount_start = label_match.start()
        amount_end = label_match.end()
        return (
            component_sum,
            amount_text,
            amount_start,
            amount_end,
            "component_sum",
            "Summed OTKA programme components in the local chapter block when no explicit total was printed on the title line.",
        )
    return None


def _extract_nkfi(full_text: str, year_num: int) -> Optional[tuple[float, str, int, int, str, str]]:
    best: Optional[tuple[float, str, int, int, str, str]] = None
    best_component: Optional[tuple[float, str, int, int, str, str]] = None

    if 2015 <= year_num <= 2019:
        for total_match in _NKFI_TOTAL_RE.finditer(full_text):
            amount = _parse_million_huf(total_match.group(1))
            if amount and amount >= 50_000_000_000:
                candidate = (
                    amount,
                    total_match.group(1),
                    total_match.start(1),
                    total_match.end(1),
                    "chapter_total",
                    "Explicit LXII chapter total for the National Research, Development and Innovation Fund.",
                )
                if best is None or candidate[0] > best[0]:
                    best = candidate

    for total_match in _NKFI_TOTAL_RE.finditer(full_text):
        context = full_text[max(0, total_match.start() - 4000): total_match.end() + 200]
        if not (
            _NKFI_HEADER_RE.search(context)
            or "Kutatási Alaprész" in context
            or "Innovációs Alaprész" in context
        ):
            continue
        amount = _parse_million_huf(total_match.group(1))
        if amount and amount >= 5_000_000_000:
            candidate = (
                amount,
                total_match.group(1),
                total_match.start(1),
                total_match.end(1),
                "chapter_total",
                "Explicit LXII chapter total for the National Research, Development and Innovation Fund.",
            )
            if best is None or candidate[0] > best[0]:
                best = candidate

    for header in _NKFI_HEADER_RE.finditer(full_text):
        window = full_text[header.start(): header.start() + 5000]
        total_match = _NKFI_TOTAL_RE.search(window)
        if total_match:
            amount = _parse_million_huf(total_match.group(1))
            if amount and amount >= 5_000_000_000:
                candidate = (
                    amount,
                    total_match.group(1),
                    header.start() + total_match.start(1),
                    header.start() + total_match.end(1),
                    "chapter_total",
                    "Explicit LXII chapter total for the National Research, Development and Innovation Fund.",
                )
                if best is None or candidate[0] > best[0]:
                    best = candidate
                if year_num < 2020:
                    continue

        research_match = _NKFI_RESEARCH_ROW_RE.search(window)
        innovation_match = _NKFI_INNOVATION_ROW_RE.search(window)
        if research_match and innovation_match:
            research_amount = _parse_million_huf(research_match.group(1)) or 0.0
            innovation_amount = _parse_million_huf(innovation_match.group(1)) or 0.0
            component_sum = research_amount + innovation_amount
            if component_sum >= 20_000_000_000:
                raw = f"{research_match.group(1)} + {innovation_match.group(1)}"
                candidate = (
                    component_sum,
                    raw,
                    header.start() + research_match.start(1),
                    header.start() + innovation_match.end(1),
                    "component_sum",
                    "Summed Kutatasi Alapresz and Innovacios Alapresz within the LXII fund block.",
                )
                if best_component is None or candidate[0] > best_component[0]:
                    best_component = candidate

    if year_num >= 2020 and best_component is not None:
        return best_component
    return best


def extract_hungary_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract Hungary science-policy items from budget law PDFs."""
    records: list[dict] = []
    pages = list(_iter_pages(sorted_pages))
    if not pages:
        logger.debug("Hungary extractor: no text in %s.", source_filename)
        return []

    year_num = _year_int(year)
    full_text = "\n".join(text for _, text in pages)

    if year_num and year_num <= 2014:
        for page_number, page_text in pages:
            found = _extract_otka(page_text)
            if not found:
                continue
            amount, raw_amount, start, end, source_variant, rationale = found
            before, raw_line, after, merged = _snippet(page_text, start, end)
            records.append(
                _build_record(
                    country=country,
                    year=year,
                    source_filename=source_filename,
                    file_id=file_id,
                    page_number=page_number,
                    program_code="HU_OTKA",
                    section_name="Országos Tudományos Kutatási Alapprogramok",
                    section_name_en="National Scientific Research Programmes (OTKA)",
                    line_description="Országos Tudományos Kutatási Alapprogramok",
                    line_description_en="National Scientific Research Programmes (OTKA)",
                    amount_local=amount,
                    amount_raw=raw_amount,
                    raw_line=raw_line,
                    context_before=before,
                    context_after=after,
                    merged_line=merged,
                    source_variant=source_variant,
                    rationale=rationale,
                    confidence=0.9 if source_variant == "explicit_program_line" else 0.75,
                )
            )
            break
    else:
        found = _extract_nkfi(full_text, year_num)
        if found:
            amount, raw_amount, start, end, source_variant, rationale = found
            before, raw_line, after, merged = _snippet(full_text, start, end)
            records.append(
                _build_record(
                    country=country,
                    year=year,
                    source_filename=source_filename,
                    file_id=file_id,
                    page_number=1,
                    program_code="HU_NKFI_ALAP",
                    section_name="Nemzeti Kutatási, Fejlesztési és Innovációs Alap",
                    section_name_en="National Research, Development and Innovation Fund",
                    line_description="LXII. fejezet összesen",
                    line_description_en="Chapter LXII total expenditure",
                    amount_local=amount,
                    amount_raw=raw_amount,
                    raw_line=raw_line,
                    context_before=before,
                    context_after=after,
                    merged_line=merged,
                    source_variant=source_variant,
                    rationale=rationale,
                    confidence=0.88 if source_variant == "chapter_total" else 0.78,
                )
            )

    if records:
        logger.info(
            "Hungary extractor: %s (year %s) -> %d records",
            source_filename,
            year,
            len(records),
        )
    else:
        logger.debug("Hungary extractor: no science budget found in %s.", source_filename)
    return records
