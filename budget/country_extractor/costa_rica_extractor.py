"""Costa Rican Presupuesto Ordinario y Extraordinario extractor.

The Costa Rican archive is not stable enough for whole-document keyword scans.
Manual review across the available PDFs showed two reliable sources:

1. Ministry title pages for section `1.1.1.1.218.000`, which list the
   programme totals for the science ministry (MICIT / MICITT / MICIITT).
2. Older `1.1.1.1.218.000-893-00` transfer blocks, which list the explicit
   CONICIT transfers line by line.

The extractor therefore anchors extraction to those page-local layouts and
ignores later summary annexes that produce inflated ministry-wide totals.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

logger = logging.getLogger("innovation_pipeline")


_MINISTRY_TITLE_RE = re.compile(
    r"1\.1\.1\.1\.218\.000-(MINISTERIO DE CIENCIA[^\n]+)",
    re.IGNORECASE,
)
_PROGRAMS_RE = re.compile(r"PROGRAMAS PRESUPUESTARIOS", re.IGNORECASE)
_TITLE_TOTAL_RE = re.compile(r"([0-9][0-9\.,]{5,})\s+Totales\b", re.IGNORECASE)
_PROGRAM_893_RE = re.compile(r"(?:1\.1\.1\.1\.218\.000-893-00|218-893-00)", re.IGNORECASE)
_PROGRAM_893_TOTAL_RE = re.compile(
    r"(?:Programa:\s*893-00\s*Total Aumento del|RESUMEN (?:POR FUENTE DE FINANCIAMIENTO|DE FUENTE DE FINANCIAMIENTO).*?TOTAL(?:\s+C[ÓO]DIGO)?\s+CONCEPTO)\s*([0-9][0-9\.,]{5,})",
    re.IGNORECASE | re.DOTALL,
)
_CONICIT_RE = re.compile(
    r"CONSEJO\s+NACIONAL\s+DE\s+INVESTIGACIONES\s+CIENT[IÍ]FICAS\s+Y\s+TECNOL[OÓ]GICAS\s*\(CONICIT\)"
    r"|CONICIT\)",
    re.IGNORECASE,
)
_CRC_RE = re.compile(r"(?<!\d)(\d{1,3}(?:[\.,]\d{3})+|\d{7,})(?!\d)")
_CONICIT_RECORD_RE = re.compile(r"(?m)^\s*(210|211|212)\s*$")


def _parse_crc(raw: str) -> Optional[float]:
    raw = raw.strip()
    if not raw:
        return None
    cleaned = re.sub(r"[.,]", "", raw)
    try:
        value = float(cleaned)
    except ValueError:
        return None
    return value if value > 0 else None


def _normalize_snippet(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _page_text(row: object) -> str:
    text = getattr(row, "text", "")
    return text if isinstance(text, str) else ""


def _extract_title_page(sorted_pages) -> Optional[dict]:
    best: Optional[dict] = None

    for row in sorted_pages.itertuples(index=False):
        text = _page_text(row)
        if not text or not _MINISTRY_TITLE_RE.search(text):
            continue
        if not _PROGRAMS_RE.search(text):
            continue

        total_match = _TITLE_TOTAL_RE.search(text)
        if not total_match:
            continue

        amount = _parse_crc(total_match.group(1))
        if not amount or amount < 500_000_000 or amount > 20_000_000_000:
            continue

        ministry_name = _MINISTRY_TITLE_RE.search(text)
        section_name = ministry_name.group(1).strip() if ministry_name else (
            "Ministerio de Ciencia, Tecnología y Telecomunicaciones"
        )
        candidate = {
            "amount_local": amount,
            "amount_raw": total_match.group(1),
            "page_number": int(getattr(row, "page_number", 1) or 1),
            "section_name": section_name,
            "text_snippet": _normalize_snippet(text[:1600]),
            "raw_line": total_match.group(0).strip(),
            "merged_line": total_match.group(0).strip(),
            "source_variant": "title_page",
            "confidence": 0.92,
        }
        if best is None or candidate["amount_local"] > best["amount_local"]:
            best = candidate

    return best


def _extract_program_893_total(sorted_pages) -> Optional[dict]:
    best: Optional[dict] = None

    for row in sorted_pages.itertuples(index=False):
        text = _page_text(row)
        if not text or not _PROGRAM_893_RE.search(text):
            continue

        match = _PROGRAM_893_TOTAL_RE.search(text)
        if not match:
            continue

        amount = _parse_crc(match.group(1))
        if not amount or amount < 500_000_000 or amount > 15_000_000_000:
            continue

        candidate = {
            "amount_local": amount,
            "amount_raw": match.group(1),
            "page_number": int(getattr(row, "page_number", 1) or 1),
            "text_snippet": _normalize_snippet(text[:1600]),
            "raw_line": match.group(0).strip(),
            "merged_line": match.group(0).strip(),
            "source_variant": "program_total",
            "confidence": 0.74,
        }
        if best is None or candidate["amount_local"] > best["amount_local"]:
            best = candidate

    return best


def _extract_conicit_total(sorted_pages) -> Optional[dict]:
    best: Optional[dict] = None

    for row in sorted_pages.itertuples(index=False):
        text = _page_text(row)
        if not text or not _PROGRAM_893_RE.search(text):
            continue
        if not _CONICIT_RE.search(text):
            continue

        amounts: list[tuple[float, str, str]] = []
        seen: set[int] = set()
        record_matches = list(_CONICIT_RECORD_RE.finditer(text))
        if record_matches:
            for idx, record_match in enumerate(record_matches):
                block_start = record_match.start()
                block_end = record_matches[idx + 1].start() if idx + 1 < len(record_matches) else len(text)
                block = text[block_start:block_end]
                if not _CONICIT_RE.search(block):
                    continue
                values = []
                for raw_match in _CRC_RE.finditer(block):
                    value = _parse_crc(raw_match.group(1))
                    if value and 1_000_000 <= value <= 5_000_000_000:
                        values.append((value, raw_match.group(1)))
                if not values:
                    continue
                value, raw_amount = max(values, key=lambda item: item[0])
                rounded = int(round(value))
                if rounded in seen:
                    continue
                seen.add(rounded)
                amounts.append((value, raw_amount, _normalize_snippet(block[:700])))

        for match in _CONICIT_RE.finditer(text):
            before = text[max(0, match.start() - 180): match.start()]
            after = text[match.start(): min(len(text), match.end() + 180)]
            local_values = []
            for raw_match in _CRC_RE.finditer(before):
                value = _parse_crc(raw_match.group(1))
                if value and 1_000_000 <= value <= 5_000_000_000:
                    local_values.append((value, raw_match.group(1)))
            for raw_match in _CRC_RE.finditer(after):
                value = _parse_crc(raw_match.group(1))
                if value and 1_000_000 <= value <= 5_000_000_000:
                    local_values.append((value, raw_match.group(1)))

            if not local_values:
                continue
            value, raw_amount = max(local_values, key=lambda item: item[0])
            rounded = int(round(value))
            if rounded in seen:
                continue
            seen.add(rounded)
            line = _normalize_snippet(text[max(0, match.start() - 120): min(len(text), match.end() + 260)])
            amounts.append((value, raw_amount, line))

        if not amounts:
            continue

        total = sum(v for v, _, _ in amounts)
        if total < 50_000_000 or total > 5_000_000_000:
            continue

        detail_lines = [line for _, _, line in amounts]
        candidate = {
            "amount_local": float(total),
            "amount_raw": " + ".join(raw for _, raw, _ in amounts),
            "page_number": int(getattr(row, "page_number", 1) or 1),
            "text_snippet": _normalize_snippet(text[:2000]),
            "raw_line": " | ".join(detail_lines),
            "merged_line": " | ".join(detail_lines),
            "source_variant": "program_transfer_block",
            "confidence": 0.93,
        }
        if best is None or candidate["amount_local"] > best["amount_local"]:
            best = candidate

    return best


def extract_costa_rica_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract science budget records from Costa Rican budget files."""
    records: list[dict] = []

    title_page = _extract_title_page(sorted_pages)
    program_total = _extract_program_893_total(sorted_pages)
    conicit_total = _extract_conicit_total(sorted_pages)

    micit_source = title_page or program_total
    if micit_source:
        section_name = micit_source.get(
            "section_name",
            "Ministerio de Ciencia, Tecnología y Telecomunicaciones",
        )
        records.append({
            "country": country,
            "year": year,
            "section_code": "CR_SCIENCE",
            "section_name": section_name,
            "section_name_en": "Ministry of Science, Technology and Telecommunications",
            "program_code": "CR_MICIT",
            "program_description": "Presupuesto total del ministerio de ciencia",
            "program_description_en": "Total appropriation of the science ministry",
            "line_description": "MICIT/MICITT/MICIITT - total presupuestario",
            "line_description_en": "Science ministry total appropriation",
            "amount_local": micit_source["amount_local"],
            "currency": "CRC",
            "unit": "CRC",
            "rd_category": "direct_rd",
            "taxonomy_score": 8.5,
            "decision": "include",
            "confidence": micit_source["confidence"],
            "source_file": source_filename,
            "file_id": file_id,
            "page_number": micit_source["page_number"],
            "amount_raw": micit_source["amount_raw"],
            "source_variant": micit_source["source_variant"],
            "text_snippet": micit_source["text_snippet"],
            "raw_line": micit_source["raw_line"],
            "merged_line": micit_source["merged_line"],
            "context_before": section_name,
            "context_after": "Programa presupuestario 893 / Totales",
            "rationale": "Costa Rica ministry title page or programme summary anchored to section 218.",
        })

    if conicit_total:
        records.append({
            "country": country,
            "year": year,
            "section_code": "CR_SCIENCE",
            "section_name": "Consejo Nacional de Investigaciones Científicas y Tecnológicas",
            "section_name_en": "National Council for Scientific and Technological Research",
            "program_code": "CR_CONICIT",
            "program_description": "Transferencias del CONICIT dentro del programa 893",
            "program_description_en": "CONICIT transfers within programme 893",
            "line_description": "CONICIT - transferencias corrientes y fondos de incentivos",
            "line_description_en": "CONICIT transfers and incentives funds",
            "amount_local": conicit_total["amount_local"],
            "currency": "CRC",
            "unit": "CRC",
            "rd_category": "direct_rd",
            "taxonomy_score": 9.0,
            "decision": "include",
            "confidence": conicit_total["confidence"],
            "source_file": source_filename,
            "file_id": file_id,
            "page_number": conicit_total["page_number"],
            "amount_raw": conicit_total["amount_raw"],
            "source_variant": conicit_total["source_variant"],
            "text_snippet": conicit_total["text_snippet"],
            "raw_line": conicit_total["raw_line"],
            "merged_line": conicit_total["merged_line"],
            "context_before": "1.1.1.1.218.000-893-00 Registro Contable",
            "context_after": "Transferencias corrientes al sector público",
            "rationale": "Costa Rica CONICIT lines extracted only from anchored 218-893 transfer blocks.",
        })

    if records:
        logger.info(
            "Costa Rica extractor: %s (year %s) -> %d records",
            source_filename, year, len(records),
        )
    else:
        logger.debug(
            "Costa Rica extractor: no science budget found in %s (year %s).",
            source_filename, year,
        )

    return records
