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
_ROW_VALUE_RE = r"(\d+(?:\.\d{3})*)"

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

_OCW_ARTICLE_ROW_RE = re.compile(
    r"(?:^|\n)\s*16\s*\n\s*"
    r"(Onderzoek\s+en\s+wetenschapsbeleid|Onderzoek\s+en\s+wetenschappen|Coordinatie\s+wetenschapsbeleid)"
    r"\s*\n\s*" + _ROW_VALUE_RE + r"\s*\n\s*" + _ROW_VALUE_RE + r"\s*\n\s*" + _ROW_VALUE_RE,
    re.IGNORECASE,
)

_EZ_ARTICLE_ROW_RE = re.compile(
    r"(?:^|\n)\s*2\s+"
    r"(Industrieel\s+en\s+Algemeen\s+Technologiebeleid|"
    r"Bevorderen\s+van\s+innovatiekracht|"
    r"Een\s+sterk\s+innovatievermogen|"
    r"Bedrijvenbeleid(?::)?\s+innovatie(?:f)?\s+en\s+duurzaam\s+ondernemen|"
    r"Bedrijvenbeleid(?::)?\s+innovatie\s+en\s+ondernemerschap\s+voor\s+duurzame\s+welvaartsgroei)"
    r"\s*\n\s*" + _ROW_VALUE_RE + r"\s*\n\s*" + _ROW_VALUE_RE + r"\s*\n\s*" + _ROW_VALUE_RE,
    re.IGNORECASE,
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


def _extract_article_row(page_text: str, source_filename: str) -> tuple[str, float, str] | None:
    lower = source_filename.lower()
    pattern = _OCW_ARTICLE_ROW_RE if "ministry8" in lower else _EZ_ARTICLE_ROW_RE if "ministry13" in lower else None
    if pattern is None:
        return None
    match = pattern.search(page_text)
    if not match:
        return None
    amounts = [match.group(i) for i in range(2, 5)]
    amount_local = _parse_amount(amounts[1])
    return match.group(0).strip(), amount_local * 1000.0, amounts[1]


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

    for row in sorted_pages.itertuples(index=False):
        page_number = int(getattr(row, "page_number", 1) or 1)
        page_text = row.text if isinstance(row.text, str) else ""
        if not page_text.strip():
            continue
        extracted = _extract_article_row(page_text, source_filename)
        if extracted is None:
            continue
        raw_block, amount_local, amount_raw = extracted
        return [{
            "country": country,
            "year": year,
            "section_code": meta["section_code"],
            "section_name": meta["section_name"],
            "section_name_en": meta["section_name_en"],
            "program_code": meta["program_code"],
            "line_description": meta["line_description"],
            "line_description_en": meta["line_description_en"],
            "amount_local": amount_local,
            "currency": meta["currency"],
            "unit": meta["unit"],
            "rd_category": "direct_rd",
            "taxonomy_score": meta["taxonomy_score"],
            "decision": "include",
            "confidence": meta["confidence"],
            "source_file": source_filename,
            "file_id": file_id,
            "page_number": page_number,
            "amount_raw": amount_raw,
            "raw_line": raw_block,
            "merged_line": raw_block,
            "context_before": "",
            "context_after": "",
            "text_snippet": raw_block,
            "source_variant": meta["source_variant"],
            "rationale": "Dutch ministry article row; uitgaven column extracted from the article-level budget table.",
        }]

    return []


__all__ = ["extract_netherlands_items"]
