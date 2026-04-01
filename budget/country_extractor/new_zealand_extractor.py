"""New Zealand appropriation extractor.

Pragmatic scope:
- 1975-1990: DSIR / Scientific and Industrial Research summary-line totals
- 2002+: explicit science appropriations in the Estimates acts

The searchable modern files do not expose one stable science vote total. They
instead list a cluster of recurring science appropriations such as Endeavour
Fund, Marsden Fund, Health Research Fund, Strategic Science Investment Fund,
and Callaghan Innovation. This extractor sums that explicit science package.
"""

from __future__ import annotations

import re

from budget.utils import normalize_text


_AMOUNT_RE = re.compile(r"(?<!\d)(\d{1,3}(?:,\d{3})+|\d{1,3}(?:\.\d{3})+|\d{1,3}(?:\s\d{3})+|\d{4,})(?!\d)")

_DSIR_LINE_RE = re.compile(r"Scientific\s+and\s+Industrial\s+Research", re.IGNORECASE)

_SCIENCE_TERMS = (
    "biological industries research",
    "building business innovation",
    "business research and development contract management",
    "callaghan innovation",
    "callaghan innovation - operations",
    "contract management",
    "crown research institute core funding",
    "cri capability fund",
    "endeavour fund",
    "engaging new zealanders with science and technology",
    "energy and minerals research",
    "environmental research",
    "fellowships for excellence",
    "founder and startup support",
    "hazards and infrastructure research",
    "health and society research",
    "health research fund",
    "marsden fund",
    "national measurement standards",
    "new economy research fund",
    "partnered research fund",
    "promoting an innovation culture",
    "research and development facilitation and promotion service",
    "research contract management",
    "strategic science investment fund",
    "supporting promising individuals",
    "talent and science promotion",
    "vision matauranga capability fund",
    "innovative partnerships",
    "digital technologies sector initiatives",
    "gene technology regulatory functions",
)

_SCIENCE_PREFIXES = (
    "science and innovation:",
    "research, science and innovation:",
    "science, innovation and technology:",
)


def _parse_amount(raw: str) -> float:
    return float(raw.replace(",", "").replace(".", "").replace(" ", ""))


def _build_context(lines: list[str], idx: int, raw_line: str) -> tuple[str, str, str, str]:
    before = "\n".join(ln.strip() for ln in lines[max(0, idx - 2):idx] if ln.strip())
    after = "\n".join(ln.strip() for ln in lines[idx + 1:min(len(lines), idx + 3)] if ln.strip())
    merged = "\n".join(ln.strip() for ln in lines[max(0, idx - 1):min(len(lines), idx + 3)] if ln.strip())
    if raw_line and raw_line not in merged:
        merged = f"{merged}\n{raw_line}".strip()
    return before, raw_line, after, merged


def _normalize_without_amounts(text: str) -> str:
    cleaned = _AMOUNT_RE.sub(" ", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return normalize_text(cleaned)


def _extract_dsir(sorted_pages) -> dict | None:
    best: dict | None = None
    for row in sorted_pages.itertuples(index=False):
        page_number = int(getattr(row, "page_number", 1) or 1)
        text = row.text if isinstance(row.text, str) else ""
        if not text.strip():
            continue
        for idx, line in enumerate(text.splitlines()):
            if not _DSIR_LINE_RE.search(line):
                continue
            amounts = _AMOUNT_RE.findall(line)
            if not amounts:
                continue
            amount = max(_parse_amount(a) for a in amounts) * 1000.0
            if amount < 1_000_000:
                continue
            lines = text.splitlines()
            before, raw_line, after, merged = _build_context(lines, idx, line.strip())
            record = {
                "program_code": "NZ_DSIR",
                "section_code": "NZ_SCIENCE",
                "section_name": "Scientific and Industrial Research",
                "section_name_en": "Department of Scientific and Industrial Research",
                "line_description": "Scientific and Industrial Research - summary appropriation",
                "line_description_en": "Scientific and Industrial Research - summary appropriation",
                "amount_local": amount,
                "page_number": page_number,
                "amount_raw": max(amounts, key=lambda a: _parse_amount(a)),
                "raw_line": raw_line,
                "merged_line": merged,
                "context_before": before,
                "context_after": after,
                "text_snippet": merged,
                "source_variant": "dsir_summary",
                "rationale": "DSIR summary-table line; rightmost/largest line amount interpreted as the appropriation in NZD thousands.",
                "confidence": 0.86,
                "taxonomy_score": 9.0,
            }
            if best is None or amount > best["amount_local"]:
                best = record
    return best


def _extract_science_package(sorted_pages) -> tuple[dict | None, dict | None]:
    total = 0.0
    snippets: list[str] = []
    first_page = 1
    seen_labels: set[str] = set()
    callaghan_amount = None
    callaghan_page = 1
    callaghan_line = ""

    for row in sorted_pages.itertuples(index=False):
        page_number = int(getattr(row, "page_number", 1) or 1)
        text = row.text if isinstance(row.text, str) else ""
        if not text.strip():
            continue
        lines = text.splitlines()
        idx = 0
        while idx < len(lines):
            stripped = lines[idx].strip()
            lower = normalize_text(stripped)
            if not any(prefix in lower for prefix in _SCIENCE_PREFIXES):
                idx += 1
                continue

            candidate_lines = [stripped]
            amount_line_idx = idx
            for step in (1, 2):
                if idx + step >= len(lines):
                    break
                nxt = lines[idx + step].strip()
                if nxt:
                    candidate_lines.append(nxt)
                    amount_line_idx = idx + step

            candidate = " ".join(part for part in candidate_lines if part)
            candidate_norm = _normalize_without_amounts(candidate)
            term = next((t for t in _SCIENCE_TERMS if t in candidate_norm), None)
            if term is None:
                idx += 1
                continue

            amounts = _AMOUNT_RE.findall(candidate)
            if not amounts:
                idx += 1
                continue
            amount = _parse_amount(amounts[0]) * 1000.0
            if amount < 500_000:
                idx += 1
                continue

            if term not in seen_labels:
                seen_labels.add(term)
                total += amount
                if not snippets:
                    first_page = page_number
                if len(snippets) < 8:
                    snippets.append(candidate)

            if "callaghan innovation" in term and (callaghan_amount is None or amount > callaghan_amount):
                callaghan_amount = amount
                callaghan_page = page_number
                callaghan_line = candidate

            idx = amount_line_idx + 1

    package_record = None
    if total >= 50_000_000:
        snippet = "\n".join(snippets)
        package_record = {
            "program_code": "NZ_SCIENCE_PACKAGE",
            "section_code": "NZ_SCIENCE",
            "section_name": "Research, Science and Innovation appropriations",
            "section_name_en": "Research, Science and Innovation appropriations",
            "line_description": "Samlet eksplisitt vitenskaps- og innovasjonspakke i appropriation schedule",
            "line_description_en": "Summed explicit science and innovation package in the appropriation schedule",
            "amount_local": total,
            "page_number": first_page,
            "amount_raw": str(int(total)),
            "raw_line": snippet,
            "merged_line": snippet,
            "context_before": "",
            "context_after": "",
            "text_snippet": snippet,
            "source_variant": "science_package_sum",
            "rationale": "Sum of explicit science appropriations with recurring science-package labels in the Estimates act.",
            "confidence": 0.8,
            "taxonomy_score": 8.5,
        }

    callaghan_record = None
    if callaghan_amount is not None and callaghan_amount >= 1_000_000:
        callaghan_record = {
            "program_code": "NZ_CALLAGHAN",
            "section_code": "NZ_SCIENCE",
            "section_name": "Callaghan Innovation",
            "section_name_en": "Callaghan Innovation",
            "line_description": "Callaghan Innovation - appropriation",
            "line_description_en": "Callaghan Innovation - appropriation",
            "amount_local": callaghan_amount,
            "page_number": callaghan_page,
            "amount_raw": str(int(callaghan_amount)),
            "raw_line": callaghan_line,
            "merged_line": callaghan_line,
            "context_before": "",
            "context_after": "",
            "text_snippet": callaghan_line,
            "source_variant": "callaghan_line",
            "rationale": "Explicit Callaghan Innovation line in the Estimates appropriation schedule.",
            "confidence": 0.82,
            "taxonomy_score": 8.0,
        }

    return package_record, callaghan_record


def extract_new_zealand_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract New Zealand science-budget items."""
    try:
        year_int = int(year)
    except ValueError:
        return []

    items: list[dict] = []

    if year_int <= 1990:
        dsir = _extract_dsir(sorted_pages)
        if dsir is not None:
            items.append(dsir)

    if year_int >= 2002:
        package_record, callaghan_record = _extract_science_package(sorted_pages)
        if package_record is not None:
            items.append(package_record)
        if callaghan_record is not None:
            items.append(callaghan_record)

    records: list[dict] = []
    for item in items:
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
                "currency": "NZD",
                "unit": "NZD",
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


__all__ = ["extract_new_zealand_items"]
