"""Netherlands budget extractor.

Scope:
- 2000+ ministry budget PDFs with article-level tables
- Ministry 8 / VIII (OCW): ``Onderzoek en wetenschapsbeleid``
- Ministry 13 / XIII (Economic Affairs): innovation / technology-policy article

The Dutch ministry files expose article tables in ``x f 1.000`` or
``x € 1.000``. The extractor reads the policy-article row and takes the
``Uitgaven`` amount, returning full NLG/EUR values.
"""

from __future__ import annotations

import re

from budget.utils import normalize_text


_AMOUNT_RE = re.compile(r"\d{1,3}(?:[ .]\d{3})+")

_OCW_LABELS = (
    normalize_text("Onderzoek en wetenschapsbeleid"),
    normalize_text("Onderzoek en wetenschappen"),
    normalize_text("Coordinatie wetenschapsbeleid"),
)

_EZ_LABELS = (
    normalize_text("Industrieel en Algemeen Technologiebeleid"),
    normalize_text("Bevorderen van innovatiekracht"),
    normalize_text("Een sterk innovatievermogen"),
    normalize_text("Bedrijvenbeleid innovatief en duurzaam ondernemen"),
    normalize_text("Bedrijvenbeleid: innovatief en duurzaam ondernemen"),
    normalize_text("Bedrijvenbeleid innovatie en ondernemerschap voor duurzame welvaartsgroei"),
    normalize_text("Bedrijvenbeleid: innovatie en ondernemerschap voor duurzame welvaartsgroei"),
)


def _parse_amount(raw: str) -> float:
    return float(raw.replace(".", "").replace(" ", ""))


def _select_uitgaven(amounts: list[str]) -> float | None:
    if len(amounts) >= 3:
        return _parse_amount(amounts[1])
    if len(amounts) == 2:
        return _parse_amount(amounts[-1])
    return None


def _build_candidates(lines: list[str], idx: int) -> list[str]:
    parts = [lines[idx].strip()]
    candidates = [parts[0]]
    for step in (1, 2):
        if idx + step >= len(lines):
            break
        nxt = lines[idx + step].strip()
        if not nxt:
            continue
        parts.append(nxt)
        candidates.append(" ".join(parts))
    return [candidate for candidate in candidates if candidate.strip()]


def _build_context(lines: list[str], idx: int, raw_line: str) -> tuple[str, str, str, str]:
    before = "\n".join(ln.strip() for ln in lines[max(0, idx - 2):idx] if ln.strip())
    after = "\n".join(ln.strip() for ln in lines[idx + 1:min(len(lines), idx + 3)] if ln.strip())
    merged = "\n".join(ln.strip() for ln in lines[max(0, idx - 1):min(len(lines), idx + 3)] if ln.strip())
    if raw_line and raw_line not in merged:
        merged = f"{merged}\n{raw_line}".strip()
    return before, raw_line, after, merged


def _normalize_without_amounts(text: str) -> str:
    cleaned = _AMOUNT_RE.sub(" ", text)
    cleaned = re.sub(r"\b\d+\b", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return normalize_text(cleaned)


def _meta_for_source(source_filename: str, year: int) -> dict | None:
    lower = source_filename.lower()
    currency = "EUR" if year >= 2002 else "NLG"
    if "ministry8" in lower:
        return {
            "labels": _OCW_LABELS,
            "program_code": "NL_OCW_SCIENCE",
            "section_code": "NL_SCIENCE",
            "section_name": "Ministerie van Onderwijs, Cultuur en Wetenschap",
            "section_name_en": "Ministry of Education, Culture and Science",
            "line_description": "Onderzoek en wetenschapsbeleid - uitgaven",
            "line_description_en": "Research and science policy - expenditure",
            "source_variant": "ocw_article",
            "currency": currency,
            "unit": currency,
            "confidence": 0.88,
            "taxonomy_score": 8.5,
        }
    if "ministry13" in lower:
        return {
            "labels": _EZ_LABELS,
            "program_code": "NL_EZ_INNOVATION",
            "section_code": "NL_INNOVATION",
            "section_name": "Ministerie van Economische Zaken",
            "section_name_en": "Ministry of Economic Affairs",
            "line_description": "Innovatie- en technologiebeleid - uitgaven",
            "line_description_en": "Innovation and technology policy - expenditure",
            "source_variant": "ez_article",
            "currency": currency,
            "unit": currency,
            "confidence": 0.86,
            "taxonomy_score": 8.2,
        }
    return None


def _gather_amounts_lookahead(lines: list[str], label_idx: int, max_extra: int = 6) -> list[str]:
    """Collect amount tokens starting from label_idx+1 across up to max_extra lines."""
    amounts: list[str] = []
    for step in range(1, max_extra + 1):
        j = label_idx + step
        if j >= len(lines):
            break
        ln = lines[j].strip()
        if not ln:
            continue
        found = _AMOUNT_RE.findall(ln)
        if found:
            amounts.extend(found)
        elif len(amounts) > 0:
            # Non-amount, non-empty line after at least one amount → stop
            break
    return amounts


def extract_netherlands_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract Netherlands science and innovation budget rows."""
    try:
        year_int = int(year)
    except ValueError:
        return []

    if year_int < 2000:
        return []

    meta = _meta_for_source(source_filename, year_int)
    if meta is None:
        return []

    best: dict | None = None
    for row in sorted_pages.itertuples(index=False):
        page_number = int(getattr(row, "page_number", 1) or 1)
        page_text = row.text if isinstance(row.text, str) else ""
        if not page_text.strip():
            continue
        lines = page_text.splitlines()
        for idx, line in enumerate(lines):
            stripped = line.strip()
            if not stripped:
                continue
            line_norm = _normalize_without_amounts(stripped)
            line_has_label = any(label in line_norm for label in meta["labels"])
            line_has_prefix = bool(line_norm) and any(label.startswith(line_norm) for label in meta["labels"])
            line_has_amount = bool(_AMOUNT_RE.search(stripped))
            if not (line_has_label or line_has_prefix) and line_has_amount:
                continue
            label_match_idx = idx
            for candidate in _build_candidates(lines, idx):
                norm = _normalize_without_amounts(candidate)
                if not any(label in norm for label in meta["labels"]):
                    # Track how far label spans
                    label_match_idx = idx + 1
                    continue
                amounts = _AMOUNT_RE.findall(candidate)
                # If amounts aren't on the same lines as the label, look ahead
                if _select_uitgaven(amounts) is None:
                    lookahead = _gather_amounts_lookahead(lines, label_match_idx + candidate.count("\n") + 1)
                    amounts = amounts + lookahead
                amount_local = _select_uitgaven(amounts)
                if amount_local is None:
                    continue
                before, raw_line, after, merged = _build_context(lines, idx, candidate)
                record = {
                    "country": country,
                    "year": year,
                    "section_code": meta["section_code"],
                    "section_name": meta["section_name"],
                    "section_name_en": meta["section_name_en"],
                    "program_code": meta["program_code"],
                    "line_description": meta["line_description"],
                    "line_description_en": meta["line_description_en"],
                    "amount_local": amount_local * 1000.0,
                    "currency": meta["currency"],
                    "unit": meta["unit"],
                    "rd_category": "direct_rd",
                    "taxonomy_score": meta["taxonomy_score"],
                    "decision": "include",
                    "confidence": meta["confidence"],
                    "source_file": source_filename,
                    "file_id": file_id,
                    "page_number": page_number,
                    "amount_raw": amounts[1] if len(amounts) >= 3 else amounts[-1],
                    "raw_line": raw_line,
                    "merged_line": merged,
                    "context_before": before,
                    "context_after": after,
                    "text_snippet": merged,
                    "source_variant": meta["source_variant"],
                    "rationale": "Dutch ministry article row; uitgaven column extracted from the article-level budget table.",
                }
                if best is None or (
                    float(record["amount_local"]),
                    -page_number,
                ) > (
                    float(best["amount_local"]),
                    -int(best["page_number"]),
                ):
                    best = record
                break

    return [best] if best else []


__all__ = ["extract_netherlands_items"]
