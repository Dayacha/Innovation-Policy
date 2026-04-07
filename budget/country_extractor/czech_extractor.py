"""Czech Republic state-budget extractor.

Manual review of the Czech archive showed three usable source layouts:

1. Chapter pages headed `Ukazatele kapitoly 321/361 ...`, where the relevant
   amount is the local `Výdaje celkem` line.
2. Appendix expenditure tables such as `Příloha č. 3` / `Celkový přehled výdajů
   státního rozpočtu podle kapitol`, where chapters `321` and `361` appear as
   rows with their total expenditures.
3. Older investment-only tables using `{agency} celkem`, which are weaker but
   still useful as a last resort for 1997-style annexes.

The extractor therefore works page-first and prefers locally anchored chapter
totals over whole-document keyword scans.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

from budget.utils import normalize_text


logger = logging.getLogger("innovation_pipeline")


_SECTION_CODE = "CZ_RD"
_SECTION_NAME = "Věda a výzkum (státní rozpočet)"
_SECTION_NAME_EN = "Science and Research (State Budget)"

_APPENDIX_EXP_RE = re.compile(
    r"celkov\w*\s+prehled\s+vydaj\w*\s+statniho\s+rozpoctu\s+podle\s+kapitol",
    re.IGNORECASE,
)
_OLDER_EXP_TABLE_RE = re.compile(
    r"(?:rozdeleni|celkove?)\s+vydaj\w*.*podle\s+kapitol",
    re.IGNORECASE,
)
_INVESTMENT_TABLE_RE = re.compile(r"investic\w*\s+vydaj\w*", re.IGNORECASE)
_VYDAJE_CELKEM_RE = re.compile(r"v\w*daje\s+celkem", re.IGNORECASE)
_AMOUNT_RE = re.compile(r"(?<!\d)(\d{1,4}(?:[ \xa0]\d{3})+|\d{6,})(?!\d)")
_CHAPTER_CODE_LINE_RE = re.compile(r"^\s*(3\d{2})\s*$")
_ANY_CHAPTER_LINE_RE = re.compile(r"^\s*(3\d{2})\b")


_AGENCIES = (
    {
        "program_code": "CZ_GACR",
        "name": "Grantová agentura České republiky (GAČR)",
        "name_en": "Czech Science Foundation (GAČR)",
        "chapter_code": "321",
        "line_description": "Grantová agentura České republiky - výdaje celkem",
        "line_description_en": "Czech Science Foundation total expenditure",
        "name_re": re.compile(
            r"grantov\w*\s+agentur\w*(?:\s+c\w*esk\w*\s+republik\w*)?",
            re.IGNORECASE,
        ),
    },
    {
        "program_code": "CZ_AVCR",
        "name": "Akademie věd České republiky (AV ČR)",
        "name_en": "Academy of Sciences of the Czech Republic",
        "chapter_code": "361",
        "line_description": "Akademie věd České republiky - výdaje celkem",
        "line_description_en": "Academy of Sciences total expenditure",
        "name_re": re.compile(
            r"akademi\w*\s+v\w*d(?:\s+c\w*esk\w*\s+republik\w*)?",
            re.IGNORECASE,
        ),
    },
)


def _parse_czk_tis(raw: str) -> Optional[float]:
    cleaned = re.sub(r"[^\d]", "", raw or "")
    if not cleaned:
        return None
    try:
        value = float(cleaned)
    except ValueError:
        return None
    if value <= 0:
        return None
    return value * 1000.0


def _normalize_lines(text: str) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    for raw_line in text.splitlines():
        raw = raw_line.strip()
        if not raw:
            continue
        rows.append((raw, normalize_text(raw)))
    return rows


def _normalize_snippet(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _extract_last_amount(lines: list[tuple[str, str]]) -> tuple[Optional[float], str]:
    best_value: Optional[float] = None
    best_raw = ""
    for raw_line, _ in lines:
        for match in _AMOUNT_RE.finditer(raw_line):
            value = _parse_czk_tis(match.group(1))
            if value is None:
                continue
            if best_value is None or value > best_value:
                best_value = value
                best_raw = match.group(1)
    return best_value, best_raw


def _find_vydaje_amount(
    lines: list[tuple[str, str]],
    start_idx: int,
) -> tuple[Optional[float], str, str]:
    for idx in range(start_idx, min(len(lines), start_idx + 20)):
        raw_line, norm_line = lines[idx]
        if not _VYDAJE_CELKEM_RE.search(norm_line):
            continue
        local_lines = lines[idx:min(len(lines), idx + 3)]
        value, raw_amount = _extract_last_amount(local_lines)
        if value is not None:
            merged = " | ".join(raw for raw, _ in local_lines)
            return value, raw_amount, merged
    return None, "", ""


def _find_row_block(
    lines: list[tuple[str, str]],
    chapter_code: str,
    name_re: re.Pattern,
) -> tuple[Optional[float], str, str]:
    for idx, (raw_line, norm_line) in enumerate(lines):
        if not _ANY_CHAPTER_LINE_RE.match(norm_line):
            continue
        if not norm_line.startswith(chapter_code):
            continue

        block = [lines[idx]]
        for next_idx in range(idx + 1, min(len(lines), idx + 8)):
            next_norm = lines[next_idx][1]
            if _ANY_CHAPTER_LINE_RE.match(next_norm):
                break
            block.append(lines[next_idx])

        block_norm = " ".join(norm for _, norm in block)
        if not name_re.search(block_norm):
            continue

        value, raw_amount = _extract_last_amount(block)
        if value is None:
            continue

        merged = " | ".join(raw for raw, _ in block)
        return value, raw_amount, merged

    return None, "", ""


def _build_candidate(
    *,
    agency: dict,
    amount: float,
    amount_raw: str,
    page_number: int,
    source_variant: str,
    confidence: float,
    merged_line: str,
    page_text: str,
    context_before: str,
    context_after: str,
) -> Optional[dict]:
    if amount < 100_000_000 or amount > 20_000_000_000:
        return None
    decision = "review" if source_variant == "investment_fallback" else "include"
    adjusted_confidence = min(confidence, 0.58) if source_variant == "investment_fallback" else confidence
    return {
        "country": "",
        "year": "",
        "section_code": _SECTION_CODE,
        "section_name": _SECTION_NAME,
        "section_name_en": _SECTION_NAME_EN,
        "program_code": agency["program_code"],
        "program_description": agency["name"],
        "program_description_en": agency["name_en"],
        "line_description": agency["line_description"],
        "line_description_en": agency["line_description_en"],
        "amount_local": amount,
        "currency": "CZK",
        "unit": "CZK",
        "rd_category": "direct_rd",
        "taxonomy_score": 8.0,
        "decision": decision,
        "confidence": adjusted_confidence,
        "page_number": page_number,
        "amount_raw": amount_raw,
        "source_variant": source_variant,
        "text_snippet": _normalize_snippet(page_text[:1800]),
        "raw_line": merged_line,
        "merged_line": merged_line,
        "context_before": context_before,
        "context_after": context_after,
        "rationale": (
            f"Czech dedicated extractor; source_variant={source_variant}; "
            f"page={page_number}; amount_raw={amount_raw}"
        ),
    }


def _extract_chapter_page(page: dict, agency: dict) -> Optional[dict]:
    chapter_match = re.search(
        rf"ukazatele\s+kapitoly\s+{agency['chapter_code']}\b.*?v\w*daje\s+celkem\s+(\d{{1,4}}(?:[ \xa0]\d{{3}})+|\d{{6,}})",
        page["norm_text"],
        re.IGNORECASE | re.DOTALL,
    )
    if chapter_match:
        amount_raw = chapter_match.group(1)
        amount = _parse_czk_tis(amount_raw)
        if amount is not None:
            return _build_candidate(
                agency=agency,
                amount=amount,
                amount_raw=amount_raw,
                page_number=page["page_number"],
                source_variant="chapter_page",
                confidence=0.96,
                merged_line=chapter_match.group(0),
                page_text=page["raw_text"],
                context_before=f"Ukazatele kapitoly {agency['chapter_code']}",
                context_after="Výdaje celkem",
            )

    lines = page["lines"]
    for idx, (_, norm_line) in enumerate(lines):
        if "ukazatele kapitoly" not in norm_line:
            continue
        if agency["chapter_code"] not in norm_line:
            continue
        window_norm = " ".join(norm for _, norm in lines[idx:min(len(lines), idx + 8)])
        if not agency["name_re"].search(window_norm):
            continue

        amount, raw_amount, merged = _find_vydaje_amount(lines, idx)
        if amount is None:
            continue
        return _build_candidate(
            agency=agency,
            amount=amount,
            amount_raw=raw_amount,
            page_number=page["page_number"],
            source_variant="chapter_page",
            confidence=0.96,
            merged_line=merged,
            page_text=page["raw_text"],
            context_before=f"Ukazatele kapitoly {agency['chapter_code']}",
            context_after="Výdaje celkem",
        )
    return None


def _extract_appendix_row(page: dict, agency: dict) -> Optional[dict]:
    lines = page["lines"]
    norm_text = page["norm_text"]
    if not (_APPENDIX_EXP_RE.search(norm_text) or _OLDER_EXP_TABLE_RE.search(norm_text)):
        return None

    anchor_positions = [
        norm_text.rfind("annex 3"),
        norm_text.rfind("priloha c. 3"),
        norm_text.rfind("celkovy prehled vydaju"),
    ]
    anchor_pos = max(pos for pos in anchor_positions if pos >= 0) if any(pos >= 0 for pos in anchor_positions) else 0
    scoped_norm = norm_text[anchor_pos:]
    row_match = re.search(
        rf"{agency['chapter_code']}\s+{agency['name_re'].pattern}\s+(\d{{1,4}}(?:[ \xa0]\d{{3}})+|\d{{6,}})",
        scoped_norm,
        re.IGNORECASE,
    )
    if row_match:
        amount_raw = row_match.group(1)
        amount = _parse_czk_tis(amount_raw)
        if amount is not None:
            variant = "appendix3_row" if _APPENDIX_EXP_RE.search(norm_text) else "chapter_table"
            confidence = 0.92 if variant == "appendix3_row" else 0.84
            return _build_candidate(
                agency=agency,
                amount=amount,
                amount_raw=amount_raw,
                page_number=page["page_number"],
                source_variant=variant,
                confidence=confidence,
                merged_line=row_match.group(0),
                page_text=page["raw_text"],
                context_before="Celkový přehled výdajů státního rozpočtu podle kapitol",
                context_after=f"Kapitola {agency['chapter_code']}",
            )

    anchor_idx = 0
    for idx, (_, norm_line) in enumerate(lines):
        if _APPENDIX_EXP_RE.search(norm_line) or _OLDER_EXP_TABLE_RE.search(norm_line):
            anchor_idx = idx
            break

    scoped_lines = lines[anchor_idx:]
    amount, raw_amount, merged = _find_row_block(scoped_lines, agency["chapter_code"], agency["name_re"])
    if amount is None:
        section_norm = " ".join(norm for _, norm in scoped_lines)
        row_match = re.search(
            rf"{agency['chapter_code']}\s+{agency['name_re'].pattern}\s+(\d{{1,4}}(?:[ \xa0]\d{{3}})+|\d{{6,}})",
            section_norm,
            re.IGNORECASE,
        )
        if row_match:
            raw_amount = row_match.group(1)
            amount = _parse_czk_tis(raw_amount)
            merged = row_match.group(0)
        if amount is None:
            return None

    variant = "appendix3_row" if _APPENDIX_EXP_RE.search(norm_text) else "chapter_table"
    confidence = 0.92 if variant == "appendix3_row" else 0.84
    return _build_candidate(
        agency=agency,
        amount=amount,
        amount_raw=raw_amount,
        page_number=page["page_number"],
        source_variant=variant,
        confidence=confidence,
        merged_line=merged,
        page_text=page["raw_text"],
        context_before="Celkový přehled výdajů státního rozpočtu podle kapitol",
        context_after=f"Kapitola {agency['chapter_code']}",
    )


def _extract_investment_fallback(page: dict, agency: dict) -> Optional[dict]:
    lines = page["lines"]
    norm_text = page["norm_text"]

    row_match = re.search(
        rf"{agency['name_re'].pattern}\s+celkem\s+(\d{{1,4}}(?:[ \xa0]\d{{3}})+|\d{{6,}})",
        norm_text,
        re.IGNORECASE,
    )
    if row_match:
        amount_raw = row_match.group(1)
        amount = _parse_czk_tis(amount_raw)
        if amount is not None:
            return _build_candidate(
                agency=agency,
                amount=amount,
                amount_raw=amount_raw,
                page_number=page["page_number"],
                source_variant="investment_fallback",
                confidence=0.68,
                merged_line=row_match.group(0),
                page_text=page["raw_text"],
                context_before="Investiční výdaje / reprodukce investičního majetku",
                context_after="celkem",
            )

    if not _INVESTMENT_TABLE_RE.search(norm_text):
        return None

    for idx, (_, norm_line) in enumerate(lines):
        if not agency["name_re"].search(norm_line):
            continue
        block = lines[idx:min(len(lines), idx + 5)]
        block_norm = " ".join(norm for _, norm in block)
        if "celkem" not in block_norm:
            continue
        value, raw_amount = _extract_last_amount(block)
        if value is None:
            continue
        merged = " | ".join(raw for raw, _ in block)
        return _build_candidate(
            agency=agency,
            amount=value,
            amount_raw=raw_amount,
            page_number=page["page_number"],
            source_variant="investment_fallback",
            confidence=0.68,
            merged_line=merged,
            page_text=page["raw_text"],
            context_before="Investiční výdaje / reprodukce investičního majetku",
            context_after="celkem",
        )
    return None


def _best_candidate(candidates: list[dict]) -> Optional[dict]:
    if not candidates:
        return None
    priority = {
        "chapter_page": 4,
        "appendix3_row": 3,
        "chapter_table": 2,
        "investment_fallback": 1,
    }
    return max(
        candidates,
        key=lambda rec: (
            priority.get(str(rec.get("source_variant", "")), 0),
            float(rec.get("confidence") or 0),
            float(rec.get("amount_local") or 0),
            -int(rec.get("page_number") or 0),
        ),
    )


def extract_czech_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract Czech R&D budget records from page-level budget text."""
    pages: list[dict] = []
    for row in sorted_pages.itertuples(index=False):
        raw_text = row.text if isinstance(row.text, str) else ""
        if not raw_text.strip():
            continue
        pages.append({
            "page_number": int(getattr(row, "page_number", 1) or 1),
            "raw_text": raw_text,
            "norm_text": normalize_text(raw_text),
            "lines": _normalize_lines(raw_text),
        })

    if not pages:
        return []

    records: list[dict] = []
    for agency in _AGENCIES:
        candidates: list[dict] = []
        for page in pages:
            chapter_candidate = _extract_chapter_page(page, agency)
            if chapter_candidate:
                candidates.append(chapter_candidate)

            appendix_candidate = _extract_appendix_row(page, agency)
            if appendix_candidate:
                candidates.append(appendix_candidate)

            investment_candidate = _extract_investment_fallback(page, agency)
            if investment_candidate:
                candidates.append(investment_candidate)

        best = _best_candidate(candidates)
        if best is None:
            continue

        best.update({
            "country": country,
            "year": year,
            "source_file": source_filename,
            "file_id": file_id,
        })
        records.append(best)

    if records:
        logger.info(
            "Czech extractor: %s (year %s) -> %d records",
            source_filename,
            year,
            len(records),
        )
    else:
        logger.debug(
            "Czech extractor: no extractable science chapter totals in %s (year %s)",
            source_filename,
            year,
        )

    return records


__all__ = ["extract_czech_items"]
