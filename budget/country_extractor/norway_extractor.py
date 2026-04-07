"""Norway Statsbudsjettet / Blaabok extractor.

Pragmatic scope:
- 2002+ searchable budget files
- chapter ``285 Norges forskningsråd`` total
- sum of explicit transfer lines mentioning ``Norges forskningsråd`` outside
  chapter 285

The files expose a stable research chapter for the Research Council of Norway.
That chapter is more comparable than the full Kunnskapsdepartementet total,
which is far broader than R&D.
"""

from __future__ import annotations

import logging
import re
from typing import Optional


logger = logging.getLogger("innovation_pipeline")

_AMOUNT_RE = re.compile(r"(?<!\d)(\d{1,3}(?:[ \xa0]\d{3})+)(?!\d)")
# Chapter 285 header: may be "285 Norges forskningsråd" on one line, OR
# "285" on one line and "Norges forskningsråd" on the next (columnar PDF layout)
_NFR_HEADER_RE = re.compile(r"^\s*285\s+Norges\s+forskningsr[åa]d\b", re.IGNORECASE)
_NFR_285_NUM_RE = re.compile(r"^\s*285\s*$")
_NFR_NAME_RE = re.compile(r"^\s*Norges\s+forskningsr[åa]d\b", re.IGNORECASE)
_NFR_LINE_RE = re.compile(r"Norges\s+forskningsr[åa]d", re.IGNORECASE)


def _parse_amount(raw: str) -> Optional[float]:
    cleaned = re.sub(r"[ \xa0]", "", raw.strip())
    try:
        value = float(cleaned)
    except ValueError:
        return None
    return value if value > 0 else None


def _amounts_in(text: str) -> list[float]:
    values: list[float] = []
    for raw in _AMOUNT_RE.findall(text):
        val = _parse_amount(raw)
        if val is not None:
            values.append(val)
    return values


def _last_amount_in_lines(lines: list[str]) -> Optional[float]:
    values: list[float] = []
    for line in lines:
        values.extend(_amounts_in(line))
    if not values:
        return None
    return values[-1]


def _build_context(lines: list[str], idx: int, raw_line: str) -> tuple[str, str, str, str]:
    before = "\n".join(ln.strip() for ln in lines[max(0, idx - 2):idx] if ln.strip())
    after = "\n".join(ln.strip() for ln in lines[idx + 1:min(len(lines), idx + 3)] if ln.strip())
    merged = "\n".join(ln.strip() for ln in lines[max(0, idx - 1):min(len(lines), idx + 3)] if ln.strip())
    if raw_line and raw_line not in merged:
        merged = f"{merged}\n{raw_line}".strip()
    return before, raw_line, after, merged


def _extract_chapter_285(sorted_pages, year_int: int) -> Optional[dict]:
    for row in sorted_pages.itertuples(index=False):
        page_number = int(getattr(row, "page_number", 1) or 1)
        page_text = row.text if isinstance(row.text, str) else ""
        if not page_text.strip():
            continue
        lines = page_text.splitlines()
        for idx, line in enumerate(lines):
            # Match either "285 Norges forskningsråd" on one line, or "285" then "Norges forskningsråd" on next
            if _NFR_HEADER_RE.search(line):
                header_idx = idx
            elif _NFR_285_NUM_RE.match(line) and idx + 1 < len(lines) and _NFR_NAME_RE.match(lines[idx + 1]):
                header_idx = idx
            else:
                continue
            # Collect up to 30 lines, stopping when the NEXT chapter number appears as
            # a standalone short line (not as part of a large number like "289 240 000")
            _next_chapter_re = re.compile(r"^\s*(28[6-9]|29\d|[3-9]\d\d)\s*$")
            raw_block = []
            for k in range(header_idx, min(len(lines), header_idx + 30)):
                ln = lines[k].rstrip()
                if k > header_idx + 1 and _next_chapter_re.match(ln.strip()):
                    break
                if ln.strip():
                    raw_block.append(ln)
            block_text = "\n".join(raw_block)
            total = _last_amount_in_lines(raw_block)
            if total is None:
                continue
            if total < 100_000_000:
                continue
            before, raw_line, after, merged = _build_context(lines, header_idx, block_text)
            rationale = "Chapter 285 Norges forskningsråd total extracted from the local chapter block."
            if year_int >= 2025:
                rationale = "Modern chapter 285 Norges forskningsråd block; final chapter total extracted after the new post structure."
            return {
                "program_code": "NO_NFR",
                "section_code": "NO_SCIENCE",
                "section_name": "Norges forskningsråd",
                "section_name_en": "Research Council of Norway",
                "line_description": "Kap. 285 Norges forskningsråd - samlet bevilgning",
                "line_description_en": "Chapter 285 Research Council of Norway - total appropriation",
                "amount_local": total,
                "page_number": page_number,
                "amount_raw": str(int(total)),
                "raw_line": raw_line,
                "merged_line": merged,
                "context_before": before,
                "context_after": after,
                "text_snippet": merged,
                "source_variant": "chapter_285_total",
                "rationale": rationale,
                "confidence": 0.9,
                "taxonomy_score": 9.0,
            }
    return None


def _extract_nfr_transfers(sorted_pages) -> Optional[dict]:
    total = 0.0
    parts: list[str] = []
    first_page = 1
    seen = 0
    for row in sorted_pages.itertuples(index=False):
        page_number = int(getattr(row, "page_number", 1) or 1)
        page_text = row.text if isinstance(row.text, str) else ""
        if not page_text.strip():
            continue
        lines = page_text.splitlines()
        for idx, line in enumerate(lines):
            if not _NFR_LINE_RE.search(line):
                continue
            # Skip the chapter 285 header line itself (both single-line and split-line formats)
            if _NFR_HEADER_RE.search(line):
                continue
            if _NFR_NAME_RE.match(line) and idx > 0 and _NFR_285_NUM_RE.match(lines[idx - 1]):
                continue
            candidate = line.strip()
            if "sum" in candidate.lower() or "kap." in candidate.lower():
                continue
            amounts = _amounts_in(candidate)
            if not amounts:
                continue
            val = amounts[0]
            if val < 10_000_000:
                continue
            if val > 1_000_000_000:
                continue
            total += val
            if seen == 0:
                first_page = page_number
            seen += 1
            if len(parts) < 4:
                parts.append(candidate)
    if total < 50_000_000 or seen == 0:
        return None
    snippet = "\n".join(parts)
    return {
        "program_code": "NO_NFR_TRANSFERS",
        "section_code": "NO_SCIENCE",
        "section_name": "Norges forskningsråd - tilskudd",
        "section_name_en": "Research Council of Norway - transfers",
        "line_description": "Sum eksplisitte tilskuddslinjer til Norges forskningsråd utenfor kap. 285",
        "line_description_en": "Sum of explicit transfer lines to the Research Council of Norway outside chapter 285",
        "amount_local": total,
        "page_number": first_page,
        "amount_raw": str(int(total)),
        "raw_line": snippet,
        "merged_line": snippet,
        "context_before": "",
        "context_after": "",
        "text_snippet": snippet,
        "source_variant": "nfr_transfer_sum",
        "rationale": "Summed explicit transfer lines naming Norges forskningsråd outside chapter 285.",
        "confidence": 0.72,
        "taxonomy_score": 8.5,
    }


def extract_norway_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract Norway research-budget items from Statsbudsjettet PDFs."""
    try:
        year_int = int(year)
    except ValueError:
        return []

    if year_int < 2002:
        return []

    records: list[dict] = []

    chapter = _extract_chapter_285(sorted_pages, year_int)
    transfers = _extract_nfr_transfers(sorted_pages)

    for item in (chapter, transfers):
        if item is None:
            continue
        records.append(
            {
                "country": country,
                "year": year,
                "section_code": item["section_code"],
                "section_name": item["section_name"],
                "section_name_en": item["section_name_en"],
                "program_code": item["program_code"],
                "line_description": item["line_description"],
                "line_description_en": item["line_description_en"],
                "amount_local": item["amount_local"],
                "currency": "NOK",
                "unit": "NOK",
                "rd_category": "direct_rd",
                "taxonomy_score": item["taxonomy_score"],
                "decision": "include",
                "confidence": item["confidence"],
                "source_file": source_filename,
                "file_id": file_id,
                "page_number": item["page_number"],
                "amount_raw": item["amount_raw"],
                "raw_line": item["raw_line"],
                "merged_line": item["merged_line"],
                "context_before": item["context_before"],
                "context_after": item["context_after"],
                "text_snippet": item["text_snippet"],
                "source_variant": item["source_variant"],
                "rationale": item["rationale"],
            }
        )

    return records


__all__ = ["extract_norway_items"]
