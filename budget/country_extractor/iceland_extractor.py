"""Icelandic Fjarlög extractor.

Pragmatic scope after manual archive review:
- searchable budget tables from 2003-2016
- science-administration block:
  - ``02-231 Rannsóknarráð Íslands`` in the early years
  - ``02-231 Rannsóknamiðstöð Íslands`` in the later years
- science-fund block:
  - ``02-236 Vísindasjóður``
  - ``02-236 Rannsóknasjóður``

The Iceland files in this era expose stable local section blocks with explicit
treasury-financed totals such as ``Greitt úr ríkissjóði`` or ``Gjöld umfram
tekjur``. Those amounts are more comparable than the broader gross totals and
much more defensible than the full university chapter.

Later files (2017+) change presentation substantially in the currently
available text extraction, so this extractor prefers omission over weak totals.
"""

from __future__ import annotations

import logging
import re
from typing import Optional


logger = logging.getLogger("innovation_pipeline")

_SECTION_HEADER_RE = re.compile(r"^\s*(\d{2}-\d{3})\s+(.+?)\s*$")
_AMOUNT_RE = re.compile(r"(?<!\d)(\d{1,3}(?:\.\d{3})*,\d)(?!\d)")
_MILLION_KR_RE = re.compile(r"m\.\s*kr\.", re.IGNORECASE)
_THOUSAND_KR_RE = re.compile(r"Þús\.\s*kr\.?", re.IGNORECASE)

_ADMIN_HEADER_RE = re.compile(
    r"^\s*02-231\s+(Rannsóknarráð\s+Íslands|Rannsóknamiðstöð\s+Íslands)\b",
    re.IGNORECASE,
)
_SCIENCE_FUND_HEADER_RE = re.compile(
    r"^\s*02-236\s+(Vísindasjóður|Rannsóknasjóður)\b",
    re.IGNORECASE,
)

_STATE_LABELS = (
    "Greitt úr ríkissjóði",
    "Gjöld umfram tekjur",
    "Gjöld samtals",
    "Almennur rekstur samtals",
)


def _parse_amount(raw: str, factor: float) -> Optional[float]:
    cleaned = raw.strip().replace(".", "").replace(",", ".")
    try:
        value = float(cleaned)
    except ValueError:
        return None
    if value <= 0:
        return None
    return value * factor


def _detect_unit_factor(text: str) -> float:
    head = text[:20_000]
    if _MILLION_KR_RE.search(head):
        return 1_000_000.0
    if _THOUSAND_KR_RE.search(head):
        return 1_000.0
    return 1.0


def _extract_amount_from_line(line: str, factor: float) -> Optional[float]:
    matches = _AMOUNT_RE.findall(line)
    if not matches:
        return None
    return _parse_amount(matches[-1], factor)


def _build_entries(sorted_pages) -> list[tuple[int, str]]:
    entries: list[tuple[int, str]] = []
    for row in sorted_pages.itertuples(index=False):
        page_number = int(getattr(row, "page_number", 1) or 1)
        text = row.text if isinstance(row.text, str) else ""
        if not text.strip():
            continue
        for line in text.splitlines():
            entries.append((page_number, line.rstrip()))
    return entries


def _find_block(entries: list[tuple[int, str]], header_re: re.Pattern[str]) -> Optional[dict]:
    for idx, (page_number, line) in enumerate(entries):
        if not header_re.search(line):
            continue
        block_lines = [line.strip()]
        j = idx + 1
        while j < len(entries):
            _, nxt = entries[j]
            if _SECTION_HEADER_RE.match(nxt):
                break
            if nxt.strip():
                block_lines.append(nxt.strip())
            j += 1
        return {
            "page_number": page_number,
            "header_line": line.strip(),
            "block_lines": block_lines,
            "start_idx": idx,
        }
    return None


def _pick_amount(block_lines: list[str], factor: float) -> tuple[Optional[float], str, float]:
    for label in _STATE_LABELS:
        for idx, line in enumerate(block_lines):
            if label.lower() not in line.lower():
                continue
            candidate = line
            if idx + 1 < len(block_lines):
                candidate = f"{line} {block_lines[idx + 1]}".strip()
            amount = _extract_amount_from_line(candidate, factor)
            if amount is not None:
                confidence = 0.9 if label == "Greitt úr ríkissjóði" else 0.86
                raw_line = candidate if candidate != line else line
                return amount, raw_line, confidence

    for idx, line in enumerate(block_lines):
        if not re.search(r"\b(?:1\.0[15]|1\.10|6\.60)\b", line):
            continue
        candidate = line
        if idx + 1 < len(block_lines):
            candidate = f"{line} {block_lines[idx + 1]}".strip()
        amount = _extract_amount_from_line(candidate, factor)
        if amount is not None:
            raw_line = candidate if candidate != line else line
            return amount, raw_line, 0.74

    return None, "", 0.0


def _context(entries: list[tuple[int, str]], start_idx: int, block_lines: list[str], raw_line: str) -> tuple[str, str, str]:
    before = "\n".join(
        ln.strip() for _, ln in entries[max(0, start_idx - 2):start_idx] if ln.strip()
    )
    after_start = min(len(entries), start_idx + max(1, len(block_lines)))
    after = "\n".join(
        ln.strip() for _, ln in entries[after_start:min(len(entries), after_start + 2)] if ln.strip()
    )
    merged_lines = block_lines[:10]
    merged = "\n".join(ln for ln in merged_lines if ln.strip())
    if raw_line and raw_line not in merged:
        merged = f"{merged}\n{raw_line}".strip()
    return before, after, merged


def _build_record(
    *,
    country: str,
    year: str,
    source_filename: str,
    file_id: str,
    page_number: int,
    amount_local: float,
    amount_raw: str,
    raw_line: str,
    merged_line: str,
    context_before: str,
    context_after: str,
    text_snippet: str,
    source_variant: str,
    rationale: str,
    confidence: float,
    program_code: str,
    section_name: str,
    section_name_en: str,
    line_description: str,
    line_description_en: str,
    taxonomy_score: float,
) -> dict:
    return {
        "country": country,
        "year": year,
        "section_code": "IS_SCIENCE",
        "section_name": section_name,
        "section_name_en": section_name_en,
        "program_code": program_code,
        "line_description": line_description,
        "line_description_en": line_description_en,
        "amount_local": amount_local,
        "currency": "ISK",
        "unit": "ISK",
        "rd_category": "direct_rd",
        "taxonomy_score": taxonomy_score,
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
        "text_snippet": text_snippet,
        "source_variant": source_variant,
        "rationale": rationale,
    }


def extract_iceland_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract Iceland science-budget items from searchable Fjarlög tables."""
    try:
        year_int = int(year)
    except ValueError:
        return []

    if year_int < 2003 or year_int > 2016:
        return []

    all_text = "\n".join(
        row.text if isinstance(row.text, str) else ""
        for row in sorted_pages.itertuples(index=False)
    )
    if not all_text.strip():
        return []

    factor = _detect_unit_factor(all_text)
    entries = _build_entries(sorted_pages)
    if not entries:
        return []

    records: list[dict] = []

    for kind, header_re in (("admin", _ADMIN_HEADER_RE), ("fund", _SCIENCE_FUND_HEADER_RE)):
        block = _find_block(entries, header_re)
        if not block:
            continue

        amount, raw_line, confidence = _pick_amount(block["block_lines"], factor)
        if amount is None or amount < 10_000_000:
            continue

        context_before, context_after, merged_line = _context(
            entries,
            block["start_idx"],
            block["block_lines"],
            raw_line,
        )

        header_line = block["header_line"]
        if kind == "admin":
            if "ráð" in header_line.lower():
                program_code = "IS_RESEARCH_COUNCIL"
                section_name = "Rannsóknarráð Íslands"
                section_name_en = "Research Council of Iceland"
                line_description = "02-231 Rannsóknarráð Íslands - framlag úr ríkissjóði"
                line_description_en = "02-231 Research Council of Iceland - treasury-financed appropriation"
                source_variant = "section_02_231_research_council"
                rationale = "Local 02-231 budget block for the research council with treasury-financed amount extracted from the section totals."
            else:
                program_code = "IS_RANNIS"
                section_name = "Rannsóknamiðstöð Íslands (RANNÍS)"
                section_name_en = "Icelandic Centre for Research (RANNÍS)"
                line_description = "02-231 RANNÍS - framlag úr ríkissjóði"
                line_description_en = "02-231 RANNÍS - treasury-financed appropriation"
                source_variant = "section_02_231_rannis"
                rationale = "Local 02-231 RANNÍS block with treasury-financed amount extracted from the section totals."
            taxonomy_score = 9.0
        else:
            program_code = "IS_SCIENCE_FUND"
            if "Vísindasjóður" in header_line:
                section_name = "Vísindasjóður"
                section_name_en = "Science Fund"
            else:
                section_name = "Rannsóknasjóður"
                section_name_en = "Research Fund"
            line_description = "02-236 Vísinda-/Rannsóknasjóður - framlag úr ríkissjóði"
            line_description_en = "02-236 Science/Research Fund - treasury-financed appropriation"
            source_variant = "section_02_236_science_fund"
            rationale = "Local 02-236 science-fund block with treasury-financed amount extracted from the section totals."
            taxonomy_score = 9.2

        records.append(
            _build_record(
                country=country,
                year=year,
                source_filename=source_filename,
                file_id=file_id,
                page_number=block["page_number"],
                amount_local=amount,
                amount_raw=raw_line,
                raw_line=raw_line,
                merged_line=merged_line,
                context_before=context_before,
                context_after=context_after,
                text_snippet=merged_line,
                source_variant=source_variant,
                rationale=rationale,
                confidence=confidence,
                program_code=program_code,
                section_name=section_name,
                section_name_en=section_name_en,
                line_description=line_description,
                line_description_en=line_description_en,
                taxonomy_score=taxonomy_score,
            )
        )

    return records


__all__ = ["extract_iceland_items"]
