"""France JORF finance-law extractor.

First-pass scope:
- modern LOLF mission era only (roughly 2006+)
- mission-level total for ``Recherche et enseignement supérieur``
- selected programme lines when the annex text is visible in extracted pages

The JORF PDFs are full official-journal publications, so the extractor stays
deliberately narrow and anchored. It only returns rows when it finds the actual
mission/programme credit lines with two monetary columns; the second column
(``crédits de paiement``) is used as the extracted amount.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

from budget.utils import normalize_text


logger = logging.getLogger("innovation_pipeline")

_AMOUNT_PAIR_RE = re.compile(
    r"(?P<ae>\d{1,3}(?:\s\d{3}){2,})\s+(?P<cp>\d{1,3}(?:\s\d{3}){2,})$"
)
_SINGLE_AMOUNT_RE = re.compile(r"(?P<amount>\d{1,3}(?:\s\d{3}){2,})$")

_TRACKED_LABELS: list[tuple[str, dict[str, str]]] = [
    (
        normalize_text("Recherche et enseignement supérieur"),
        {
            "program_code": "FR_MIRES",
            "section_name": "Mission Recherche et enseignement supérieur",
            "section_name_en": "Research and Higher Education Mission",
            "line_description": "Mission Recherche et enseignement supérieur - crédits de paiement",
            "line_description_en": "Research and Higher Education Mission - payment appropriations",
        },
    ),
    (
        normalize_text("Formations supérieures et recherche universitaire"),
        {
            "program_code": "FR_150",
            "section_name": "Programme 150",
            "section_name_en": "Programme 150",
            "line_description": "Formations supérieures et recherche universitaire - crédits de paiement",
            "line_description_en": "Higher education and university research - payment appropriations",
        },
    ),
    (
        normalize_text("Vie étudiante"),
        {
            "program_code": "FR_231",
            "section_name": "Programme 231",
            "section_name_en": "Programme 231",
            "line_description": "Vie étudiante - crédits de paiement",
            "line_description_en": "Student life - payment appropriations",
        },
    ),
    (
        normalize_text("Recherches scientifiques et technologiques pluridisciplinaires"),
        {
            "program_code": "FR_172",
            "section_name": "Programme 172",
            "section_name_en": "Programme 172",
            "line_description": "Recherches scientifiques et technologiques pluridisciplinaires - crédits de paiement",
            "line_description_en": "Multidisciplinary scientific and technological research - payment appropriations",
        },
    ),
    (
        normalize_text("Recherche dans le domaine de la gestion des milieux et des ressources"),
        {
            "program_code": "FR_187",
            "section_name": "Programme 187",
            "section_name_en": "Programme 187",
            "line_description": "Recherche dans le domaine de la gestion des milieux et des ressources - crédits de paiement",
            "line_description_en": "Research on environment and resources management - payment appropriations",
        },
    ),
    (
        normalize_text("Recherche spatiale"),
        {
            "program_code": "FR_193",
            "section_name": "Programme 193",
            "section_name_en": "Programme 193",
            "line_description": "Recherche spatiale - crédits de paiement",
            "line_description_en": "Space research - payment appropriations",
        },
    ),
    (
        normalize_text("Recherche dans les domaines de l'énergie, du développement et de la mobilité durables"),
        {
            "program_code": "FR_190",
            "section_name": "Programme 190",
            "section_name_en": "Programme 190",
            "line_description": "Recherche énergie, développement et mobilité durables - crédits de paiement",
            "line_description_en": "Research on energy, sustainable development and mobility - payment appropriations",
        },
    ),
    (
        normalize_text("Recherche dans les domaines de l'énergie, du développement et de l'aménagement durables"),
        {
            "program_code": "FR_190",
            "section_name": "Programme 190",
            "section_name_en": "Programme 190",
            "line_description": "Recherche énergie, développement et aménagement durables - crédits de paiement",
            "line_description_en": "Research on energy, sustainable development and planning - payment appropriations",
        },
    ),
    (
        normalize_text("Recherche et enseignement supérieur en matière économique et industrielle"),
        {
            "program_code": "FR_192",
            "section_name": "Programme 192",
            "section_name_en": "Programme 192",
            "line_description": "Recherche et enseignement supérieur en matière économique et industrielle - crédits de paiement",
            "line_description_en": "Economic and industrial research and higher education - payment appropriations",
        },
    ),
    (
        normalize_text("Recherche culturelle et culture scientifique"),
        {
            "program_code": "FR_186",
            "section_name": "Programme 186",
            "section_name_en": "Programme 186",
            "line_description": "Recherche culturelle et culture scientifique - crédits de paiement",
            "line_description_en": "Cultural research and scientific culture - payment appropriations",
        },
    ),
    (
        normalize_text("Enseignement supérieur et recherche agricoles"),
        {
            "program_code": "FR_142",
            "section_name": "Programme 142",
            "section_name_en": "Programme 142",
            "line_description": "Enseignement supérieur et recherche agricoles - crédits de paiement",
            "line_description_en": "Higher education and agricultural research - payment appropriations",
        },
    ),
]


def _parse_amount(raw: str) -> float:
    """Parse French grouped integers from OCR/PDF text.

    JORF lines often use non-breaking spaces as thousands separators, so
    stripping only ASCII spaces is not sufficient.
    """
    normalized = re.sub(r"\s+", "", raw or "")
    return float(normalized)


def _build_candidates(lines: list[str], idx: int) -> list[str]:
    parts = [lines[idx].strip()]
    candidates = [" ".join(part for part in parts if part)]
    for step in (1, 2):
        if idx + step >= len(lines):
            break
        nxt = lines[idx + step].strip()
        if nxt:
            parts.append(nxt)
            candidates.append(" ".join(parts))
    return candidates


def _build_context(lines: list[str], idx: int, raw_line: str) -> tuple[str, str, str, str]:
    before = "\n".join(ln.strip() for ln in lines[max(0, idx - 2):idx] if ln.strip())
    after = "\n".join(ln.strip() for ln in lines[idx + 1:min(len(lines), idx + 3)] if ln.strip())
    merged = "\n".join(ln.strip() for ln in lines[max(0, idx - 1):min(len(lines), idx + 3)] if ln.strip())
    if raw_line and raw_line not in merged:
        merged = f"{merged}\n{raw_line}".strip()
    return before, raw_line, after, merged


def _extract_from_page(lines: list[str], page_number: int) -> list[dict]:
    hits: list[dict] = []
    seen_codes: set[str] = set()

    for idx, line in enumerate(lines):
        if not line.strip():
            continue
        for candidate in _build_candidates(lines, idx):
            norm = normalize_text(candidate)
            amount_match = _AMOUNT_PAIR_RE.search(candidate)
            single_amount_match: Optional[re.Match[str]] = None
            if not amount_match:
                single_amount_match = _SINGLE_AMOUNT_RE.search(candidate)
                if not single_amount_match:
                    continue
            for label_norm, meta in _TRACKED_LABELS:
                if label_norm not in norm:
                    continue
                code = meta["program_code"]
                if code in seen_codes:
                    continue
                seen_codes.add(code)
                amount_raw = (
                    amount_match.group("cp")
                    if amount_match
                    else single_amount_match.group("amount")
                )
                cp_val = _parse_amount(amount_raw)
                before, raw_line, after, merged = _build_context(lines, idx, candidate)
                source_variant = "mission_total" if code == "FR_MIRES" else "programme_line"
                if not amount_match:
                    source_variant = f"{source_variant}_single"
                hits.append(
                    {
                        "page_number": page_number,
                        "program_code": code,
                        "section_name": meta["section_name"],
                        "section_name_en": meta["section_name_en"],
                        "line_description": meta["line_description"],
                        "line_description_en": meta["line_description_en"],
                        "amount_local": cp_val,
                        "amount_raw": amount_raw,
                        "raw_line": raw_line,
                        "merged_line": merged,
                        "context_before": before,
                        "context_after": after,
                        "text_snippet": merged,
                        "source_variant": source_variant,
                        "rationale": (
                            "Modern JORF mission/programme credit line; second amount interpreted as "
                            "crédits de paiement."
                            if amount_match
                            else "Modern JORF mission/programme credit line with a single displayed amount."
                        ),
                    }
                )
                break
            else:
                continue
            break

    return hits


def _record_key(rec: dict) -> tuple:
    variant_score = 2 if rec.get("source_variant") == "mission_total" else 1
    return (variant_score, float(rec.get("amount_local") or 0), -int(rec.get("page_number") or 0))


def extract_france_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract France research-mission amounts from JORF finance-law PDFs."""
    try:
        if int(year) < 2006:
            return []
    except ValueError:
        return []

    best_by_code: dict[str, dict] = {}
    for row in sorted_pages.itertuples(index=False):
        page_number = int(getattr(row, "page_number", 1) or 1)
        page_text = row.text if isinstance(row.text, str) else ""
        if not page_text.strip():
            continue
        page_hits = _extract_from_page(page_text.splitlines(), page_number)
        for hit in page_hits:
            rec = {
                "country": country,
                "year": year,
                "section_code": "FR_RESEARCH",
                "section_name": hit["section_name"],
                "section_name_en": hit["section_name_en"],
                "program_code": hit["program_code"],
                "line_description": hit["line_description"],
                "line_description_en": hit["line_description_en"],
                "amount_local": hit["amount_local"],
                "currency": "EUR",
                "unit": "EUR",
                "rd_category": "direct_rd",
                "taxonomy_score": 8.5 if hit["program_code"] == "FR_MIRES" else 8.0,
                "decision": "include",
                "confidence": 0.86 if hit["program_code"] == "FR_MIRES" else 0.8,
                "source_file": source_filename,
                "file_id": file_id,
                "page_number": hit["page_number"],
                "amount_raw": hit["amount_raw"],
                "raw_line": hit["raw_line"],
                "merged_line": hit["merged_line"],
                "context_before": hit["context_before"],
                "context_after": hit["context_after"],
                "text_snippet": hit["text_snippet"],
                "source_variant": hit["source_variant"],
                "rationale": hit["rationale"],
            }
            current = best_by_code.get(rec["program_code"])
            if current is None or _record_key(rec) > _record_key(current):
                best_by_code[rec["program_code"]] = rec

    if not best_by_code:
        logger.debug("France extractor: no modern research-mission rows found in %s", source_filename)
        return []
    return list(best_by_code.values())


__all__ = ["extract_france_items"]
