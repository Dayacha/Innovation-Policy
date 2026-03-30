"""Colombian Presupuesto General de la Nacion extractor.

Colombian science spending appears in two main archive regimes:

1. Older law tables, where COLCIENCIAS is section ``0320`` and the usable
   amount is the section total for the appropriations page.
2. Modern decree / annex tables, where MinCiencias is section ``3901`` and
   the usable amount is the page-level ``TOTAL PRESUPUESTO`` for that section.

The most reliable approach in the current archive is page-anchored extraction:
find the page that explicitly contains ``SECCION 0320`` or ``SECCION 3901``,
read the local page window, then extract the total that belongs to that same
section. When a total is not printed cleanly, fall back to the sum of the
section's A/B/C budget components.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from typing import Iterable, Optional

logger = logging.getLogger("innovation_pipeline")


_SECTION_RE = re.compile(
    r"SECCI[ÓO]N\s*:?\s*(0320|3901)\b|SECCI[ÓO]N\s*:?\s*(0320|3901)\b",
    re.IGNORECASE,
)
_SCIENCE_NAME_RE = re.compile(
    r"COLCIENCIAS"
    r"|INSTITUTO\s+COLOMBIANO\s+PARA\s+EL\s+DESARROLLO\s+DE\s+LA\s+CIENCIA"
    r"|DEPARTAMENTO\s+ADMINISTRATIVO\s+DE\s+CIENCIA"
    r"|MINISTERIO\s+DE\s+CIENCIA[\s,\.]+TECNOLOG"
    r"|MINISTERIO\s+DE\s+CIENCIA\s+TECNOLOG[ÍI]A\s+E\s+INNOVACI[ÓO]N",
    re.IGNORECASE,
)
_NEXT_SECTION_RE = re.compile(r"SECCI[ÓO]N\s*:?\s*\d{3,4}\b", re.IGNORECASE)
_TOTAL_LINE_RE = re.compile(
    r"TOTAL\s+PRESUPUESTO(?:\s+SECCI[ÓO]N)?\s*[:\-]?\s*([\d\.,\s\xa0]+)",
    re.IGNORECASE,
)
_TOTAL_TOKEN_RE = re.compile(
    r"TOTAL\s+PRESUPUESTO(?:\s+SECCI[ÓO]N)?\s*[:\-]?", re.IGNORECASE
)
_BUDGET_COMPONENT_RE = re.compile(
    r"\b([ABC])\s*[\.,]?\s*PRESUPUESTO\s+DE\s+"
    r"(FUNCIONAMIENTO|SERVICIO\s+DE\s+LA\s+DEUDA(?:\s+P[ÚU]BLICA)?|INVERSION|INVERSI[ÓO]N)"
    r"\s*([\d\.,\s\xa0]+)",
    re.IGNORECASE,
)
_COMPONENT_LABEL_PATTERNS = {
    "A": re.compile(r"A\s*[\.,]?\s*PRESUPUESTO\s+DE\s+FUNCIONAMIENTO", re.IGNORECASE),
    "B": re.compile(r"B\s*[\.,]?\s*PRESUPUESTO\s+DE\s+SERVICIO\s+DE\s+LA\s+DEUDA(?:\s+P[ÚU]BLICA)?", re.IGNORECASE),
    "C": re.compile(r"C\s*[\.,]?\s*PRESUPUESTO\s+DE\s+INVERSI?[ÓO]?N|C\s*[\.,]?\s*PRESUPUESTO\s+DE\s+INVERSION", re.IGNORECASE),
}
_AMOUNT_RE = re.compile(r"\d{1,3}(?:[.,]\d{3})+(?:[.,]\d+)?|\d{7,}")


@dataclass
class _Candidate:
    amount: float
    page_number: int
    program_code: str
    source_variant: str
    source_score: int
    confidence: float
    section_text: str
    rationale: str
    amount_raw: str
    section_code_raw: str


def _parse_amount(raw: str) -> Optional[float]:
    token = re.sub(r"\s+", "", raw or "")
    token = token.strip(".,;:-")
    if not token:
        return None

    if re.fullmatch(r"\d{1,3}(?:[.,]\d{3})+", token):
        return float(token.replace(".", "").replace(",", ""))

    if re.fullmatch(r"\d{7,}", token):
        return float(token)

    cleaned = re.sub(r"[^\d]", "", token)
    if len(cleaned) < 7:
        return None
    return float(cleaned)


def _extract_amounts(text: str) -> list[tuple[str, float]]:
    out: list[tuple[str, float]] = []
    seen: set[tuple[str, float]] = set()
    for match in _AMOUNT_RE.finditer(text or ""):
        raw = match.group(0)
        value = _parse_amount(raw)
        if value is None:
            continue
        key = (raw, value)
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _normalize_section_text(text: str) -> str:
    return re.sub(r"[ \t]+", " ", (text or "").replace("\x0c", " ")).strip()


def _window_text(pages: Iterable[str]) -> str:
    return "\n".join(_normalize_section_text(page) for page in pages if page)


def _section_program(code: str, text: str) -> tuple[str, str]:
    if re.search(r"DEPARTAMENTO\s+ADMINISTRATIVO\s+DE\s+LA?\s*CIENCIA|COLCIENCIAS", text, re.IGNORECASE):
        return (
            "CO_COLCIENCIAS",
            "Departamento Administrativo de Ciencia, Tecnologia e Innovacion (Colciencias)",
        )
    if code == "3901" or re.search(r"MINISTERIO\s+DE\s+CIENCIA", text, re.IGNORECASE):
        return (
            "CO_MINCIENCIAS",
            "Ministerio de Ciencia, Tecnologia e Innovacion (MinCiencias)",
        )
    return (
        "CO_COLCIENCIAS",
        "Departamento Administrativo de Ciencia, Tecnologia e Innovacion (Colciencias)",
    )


def _extract_total_amount(section_text: str) -> tuple[Optional[float], str]:
    total_matches = list(_TOTAL_LINE_RE.finditer(section_text))
    for total_match in reversed(total_matches):
        amounts = _extract_amounts(total_match.group(1)[:160])
        if amounts:
            return amounts[-1][1], amounts[-1][0]

    token_matches = list(_TOTAL_TOKEN_RE.finditer(section_text))
    for token_match in reversed(token_matches):
        tail = section_text[token_match.end(): token_match.end() + 500]
        amounts = [item for item in _extract_amounts(tail) if item[1] >= 1_000_000]
        if amounts:
            return amounts[-1][1], amounts[-1][0]
    return None, ""


def _extract_component_sum(section_text: str) -> tuple[Optional[float], list[tuple[str, float]]]:
    components: dict[str, float] = {}
    for match in _BUDGET_COMPONENT_RE.finditer(section_text):
        letter = match.group(1).upper()
        values = [value for _, value in _extract_amounts(match.group(3)[:120]) if value >= 1_000_000]
        if not values:
            continue
        components[letter] = values[-1]

    for letter, label_re in _COMPONENT_LABEL_PATTERNS.items():
        if letter in components:
            continue
        label_match = label_re.search(section_text)
        if not label_match:
            continue
        tail = section_text[label_match.end(): label_match.end() + 220]
        values = [value for _, value in _extract_amounts(tail) if value >= 1_000_000]
        if values:
            components[letter] = values[0]

    if not components:
        return None, []

    ordered = [(key, components[key]) for key in ("A", "B", "C") if key in components]
    return sum(value for _, value in ordered), ordered


def _section_window(sorted_pages, start_idx: int) -> str:
    pages: list[str] = []
    upper = min(len(sorted_pages), start_idx + 3)
    for idx in range(start_idx, upper):
        row = sorted_pages.iloc[idx]
        text = row.text if isinstance(row.text, str) else ""
        pages.append(text)
        if idx > start_idx and _NEXT_SECTION_RE.search(text):
            break
    return _window_text(pages)


def _slice_science_section(window_text: str) -> tuple[str, str]:
    section_match = _SECTION_RE.search(window_text)
    name_match = _SCIENCE_NAME_RE.search(window_text)

    anchors: list[tuple[int, str]] = []
    if section_match:
        code = next((group for group in section_match.groups() if group), "")
        anchors.append((section_match.start(), code))
    if name_match:
        anchors.append((name_match.start(), "3901" if re.search(r"MINISTERIO\s+DE\s+CIENCIA", name_match.group(0), re.IGNORECASE) else "0320"))

    if not anchors:
        return "", ""

    start_pos, code = min(anchors, key=lambda item: item[0])
    text = window_text[start_pos:]
    next_match = _NEXT_SECTION_RE.search(text[1:])
    if next_match:
        text = text[: 1 + next_match.start()]
    return text, code


def _candidate_from_page(sorted_pages, idx: int, source_filename: str) -> Optional[_Candidate]:
    row = sorted_pages.iloc[idx]
    page_text = row.text if isinstance(row.text, str) else ""
    page_number = int(row.page_number or 1)
    local_text = _section_window(sorted_pages, idx)
    section_text, section_code = _slice_science_section(local_text)
    if not section_text:
        return None
    if not section_code and not re.search(r"PRESUPUESTO\s+DE\s+FUNCIONAMIENTO|PRESUPUESTO\s+DE\s+INVERSION|TOTAL\s+PRESUPUESTO", section_text, re.IGNORECASE):
        return None

    program_code, _ = _section_program(section_code, section_text)
    total_amount, amount_raw = _extract_total_amount(section_text)
    component_sum, components = _extract_component_sum(section_text)

    amount: Optional[float] = None
    rationale_bits: list[str] = []
    confidence = 0.74

    if total_amount and total_amount >= 1_000_000_000:
        amount = total_amount
        rationale_bits.append("explicit total")
        confidence = 0.88

    if component_sum and component_sum >= 1_000_000_000:
        if amount is None:
            amount = component_sum
            amount_raw = "+".join(label for label, _ in components)
            rationale_bits.append("sum of section components")
            confidence = 0.82
        elif abs(component_sum - amount) <= 2:
            rationale_bits.append("components reconcile with total")
            confidence = 0.93
        elif abs(component_sum - amount) / max(amount, component_sum) <= 0.02:
            rationale_bits.append("components close to total")
            confidence = max(confidence, 0.9)

    if amount is None or amount < 1_000_000_000:
        return None

    source_variant = "law"
    source_score = 1
    source_name = source_filename.lower()
    if "anexo" in source_name:
        source_variant = "decree_annex"
        source_score = 4
    elif "decreto" in source_name:
        source_variant = "decree"
        source_score = 3
    elif "ley" in source_name:
        source_variant = "law"
        source_score = 2

    if page_number > 1:
        source_score += 1

    return _Candidate(
        amount=amount,
        page_number=page_number,
        program_code=program_code,
        source_variant=source_variant,
        source_score=source_score,
        confidence=confidence,
        section_text=section_text[:4000],
        rationale="; ".join(rationale_bits) or "page-anchored section extraction",
        amount_raw=amount_raw or str(int(amount)),
        section_code_raw=section_code,
    )


def extract_colombia_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract Colombian science budget records from law / decree budget PDFs."""
    if sorted_pages is None or getattr(sorted_pages, "empty", True):
        return []

    candidates: list[_Candidate] = []

    for idx in range(len(sorted_pages)):
        candidate = _candidate_from_page(sorted_pages, idx, source_filename)
        if candidate is not None:
            candidates.append(candidate)

    if not candidates:
        logger.debug(
            "Colombia extractor: no anchored science section found in %s (year %s).",
            source_filename,
            year,
        )
        return []

    best_by_program: dict[str, _Candidate] = {}
    for cand in candidates:
        current = best_by_program.get(cand.program_code)
        if current is None:
            best_by_program[cand.program_code] = cand
            continue
        cand_priority = (cand.source_score, cand.confidence, cand.amount, -cand.page_number)
        current_priority = (current.source_score, current.confidence, current.amount, -current.page_number)
        if cand_priority > current_priority:
            best_by_program[cand.program_code] = cand

    records: list[dict] = []
    for program_code, cand in best_by_program.items():
        _, agency_name = _section_program(cand.section_code_raw, cand.section_text)
        section_name = "Ciencia, Tecnologia e Innovacion"
        section_name_en = "Science, Technology and Innovation"
        line_description = (
            "Ministerio de Ciencia, Tecnologia e Innovacion - Total presupuesto seccion"
            if program_code == "CO_MINCIENCIAS"
            else "Colciencias - Total presupuesto seccion"
        )
        records.append({
            "country": country,
            "year": year,
            "section_code": "CO_SCIENCE",
            "section_name": section_name,
            "section_name_en": section_name_en,
            "program_code": program_code,
            "program_description": agency_name,
            "program_description_en": agency_name,
            "line_description": line_description,
            "line_description_en": line_description,
            "amount_local": cand.amount,
            "currency": "COP",
            "unit": "COP",
            "rd_category": "direct_rd",
            "taxonomy_score": 8.4,
            "decision": "include",
            "confidence": cand.confidence,
            "source_file": source_filename,
            "source_filename": source_filename,
            "source_variant": cand.source_variant,
            "file_id": file_id,
            "page_number": cand.page_number,
            "amount_raw": cand.amount_raw,
            "detected_amount_raw": cand.amount_raw,
            "detected_amount_value": cand.amount,
            "text_snippet": cand.section_text[:1500],
            "raw_line": cand.section_text[:800],
            "merged_line": line_description,
            "context_before": f"SECCION {cand.section_code_raw} | {agency_name}",
            "context_after": f"TOTAL PRESUPUESTO | {cand.amount_raw} | {int(cand.amount)} COP",
            "rationale": (
                f"Colombia page-anchored section extraction; {cand.rationale}; "
                f"variant={cand.source_variant}; page={cand.page_number}"
            ),
            "temporal_prior_match_type": "country_series_anchor",
        })

    logger.info(
        "Colombia extractor: %s (year %s) -> %d records",
        source_filename,
        year,
        len(records),
    )
    return records
