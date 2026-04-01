"""Swiss federal budget extractor.

Manual review shows two broad source eras:

1. 1975-1999 decree-style ``Bundesbeschluss`` files:
   no stable institution-level science appropriations in the extracted text.
2. 2007+ detailed ``VA3`` / ``VA_Band3`` files:
   stable credit-code rows with current-year appropriations in ``Tsd. CHF``.

This extractor therefore focuses on the stable, comparable federal research lines:
- ``CH_SNF``  : federal research-support line
  - ``A2310.0193 Stiftung Schweizerischer Nationalfonds`` (2007-2012)
  - ``A2310.0505 Institutionen der Forschungsförderung`` (2013-2017)
  - ``A231.0272 Institutionen der Forschungsförderung`` (2018+)
- ``CH_ETH``  : ``Finanzierungsbeitrag an ETH-Bereich``
  - ``A2310.0346`` (2007-2017)
  - ``A231.0181`` (2018+)
- ``CH_KTI``  : innovation-promotion agency line
  - ``A2310.0107 Technologie- und Innovationsförderung`` (2007-2012)
  - ``A2310.0477 Technologie- und Innovationsförderung KTI`` (2013-2017)
  - ``A231.0380 Finanzierungsbeitrag an Innosuisse`` (2018+)

Amounts are expressed in ``Tsd. CHF`` in these table rows and are scaled to full CHF.
"""

from __future__ import annotations

import logging
import re
from typing import Optional


logger = logging.getLogger("innovation_pipeline")

_ROW_AMOUNT_RE = re.compile(r"(?<!\d)(\d{1,3}(?:[ '\xa0]\d{3})+|\d{6,})(?!\d)")
_TSD_CHF_RE = re.compile(r"Tsd\.\s*CHF", re.IGNORECASE)

_SNF_ROW_RE = re.compile(
    r"(A2310\.0193|A2310\.0505|A2310\.0526|A231\.0272)\s+"
    r"(Stiftung\s+Schweizerischer\s+Nationalfonds|Institutionen\s+der.*Forschungsf[öo]rderung)",
    re.IGNORECASE | re.DOTALL,
)
_ETH_ROW_RE = re.compile(
    r"(A2310\.0346|A2310\.0542|A231\.0181)\s+Finanzierungsbeitrag\s+an\s+ETH-Bereich",
    re.IGNORECASE,
)
_KTI_ROW_RE = re.compile(
    r"(A2310\.0107|A2310\.0477|A231\.0380)\s+"
    r"(Technologie-\s+und\s+Innovationsf[öo]rderung(?:\s+KTI)?|Finanzierungsbeitrag\s+an\s+Innosuisse)",
    re.IGNORECASE,
)


def _parse_amount(raw: str) -> Optional[float]:
    cleaned = re.sub(r"[ '\xa0]", "", raw.strip())
    try:
        value = float(cleaned)
    except ValueError:
        return None
    if value <= 0:
        return None
    return value


def _row_amounts(line: str) -> list[float]:
    values: list[float] = []
    for raw in _ROW_AMOUNT_RE.findall(line):
        val = _parse_amount(raw)
        if val is not None:
            values.append(val)
    return values


def _pick_current_amount(values: list[float]) -> Optional[float]:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    if len(values) == 2:
        return max(values[0], values[1])
    if values[2] < values[1] * 0.5:
        return values[1]
    return values[2]


def _scale_factor(text: str) -> float:
    return 1000.0 if _TSD_CHF_RE.search(text) else 1.0


def _build_context(lines: list[str], idx: int, raw_line: str) -> tuple[str, str, str]:
    before = "\n".join(ln.strip() for ln in lines[max(0, idx - 2):idx] if ln.strip())
    after = "\n".join(ln.strip() for ln in lines[idx + 1:min(len(lines), idx + 3)] if ln.strip())
    merged = "\n".join(ln.strip() for ln in lines[max(0, idx - 1):min(len(lines), idx + 2)] if ln.strip())
    if raw_line and raw_line not in merged:
        merged = f"{merged}\n{raw_line}".strip()
    return before, after, merged


def _extract_row(sorted_pages, row_re: re.Pattern[str], min_amount: float) -> Optional[dict]:
    for row in sorted_pages.itertuples(index=False):
        page_number = int(getattr(row, "page_number", 1) or 1)
        page_text = row.text if isinstance(row.text, str) else ""
        if not page_text.strip():
            continue
        factor = _scale_factor(page_text)
        lines = page_text.splitlines()
        for idx, line in enumerate(lines):
            next_line = lines[idx + 1].strip() if idx + 1 < len(lines) else ""
            line_match = row_re.search(line)
            next_match = row_re.search(next_line) if next_line else None
            combined_match = row_re.search(f"{line} {next_line}") if next_line else None
            if not (line_match or next_match or combined_match):
                continue
            if line_match:
                candidate = line.strip()
            elif next_match:
                candidate = next_line
            else:
                candidate = f"{line.strip()} {next_line}".strip()
            values = _row_amounts(candidate)
            if not values and idx + 1 < len(lines):
                candidate = f"{candidate} {lines[idx + 1].strip()}".strip()
                values = _row_amounts(candidate)
            amount = _pick_current_amount(values)
            if amount is None:
                continue
            amount *= factor
            if amount < min_amount:
                continue
            before, after, merged = _build_context(lines, idx, candidate)
            return {
                "page_number": page_number,
                "raw_line": candidate,
                "merged_line": merged,
                "context_before": before,
                "context_after": after,
                "text_snippet": merged,
                "amount_local": amount,
            }
    return None


def _record_base(
    *,
    country: str,
    year: str,
    source_filename: str,
    file_id: str,
    page_number: int,
    raw_line: str,
    merged_line: str,
    context_before: str,
    context_after: str,
    text_snippet: str,
    amount_local: float,
    amount_raw: str,
    rationale: str,
    source_variant: str,
    section_name: str,
    section_name_en: str,
    program_code: str,
    line_description: str,
    line_description_en: str,
    confidence: float,
    taxonomy_score: float,
) -> dict:
    return {
        "country": country,
        "year": year,
        "section_code": "CH_SCIENCE",
        "section_name": section_name,
        "section_name_en": section_name_en,
        "program_code": program_code,
        "line_description": line_description,
        "line_description_en": line_description_en,
        "amount_local": amount_local,
        "currency": "CHF",
        "unit": "CHF",
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


def extract_switzerland_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract Swiss federal research-budget lines from detailed VA3 tables."""
    try:
        year_int = int(year)
    except ValueError:
        return []

    if year_int < 2007:
        return []

    records: list[dict] = []

    snf = _extract_row(sorted_pages, _SNF_ROW_RE, min_amount=100_000_000)
    if snf:
        records.append(
            _record_base(
                country=country,
                year=year,
                source_filename=source_filename,
                file_id=file_id,
                page_number=snf["page_number"],
                raw_line=snf["raw_line"],
                merged_line=snf["merged_line"],
                context_before=snf["context_before"],
                context_after=snf["context_after"],
                text_snippet=snf["text_snippet"],
                amount_local=snf["amount_local"],
                amount_raw=snf["raw_line"],
                rationale="Current-year federal research-support credit extracted from the exact Swiss budget row for SNF / institutions of research support.",
                source_variant="row_credit_snf",
                section_name="Schweizerischer Nationalfonds / Institutionen der Forschungsförderung",
                section_name_en="Swiss National Science Foundation / research support institutions",
                program_code="CH_SNF",
                line_description="Bundesbeitrag Forschungsförderung (SNF / Institutionen der Forschungsförderung)",
                line_description_en="Federal research-support contribution (SNF / research support institutions)",
                confidence=0.92,
                taxonomy_score=9.2,
            )
        )

    eth = _extract_row(sorted_pages, _ETH_ROW_RE, min_amount=1_000_000_000)
    if eth:
        records.append(
            _record_base(
                country=country,
                year=year,
                source_filename=source_filename,
                file_id=file_id,
                page_number=eth["page_number"],
                raw_line=eth["raw_line"],
                merged_line=eth["merged_line"],
                context_before=eth["context_before"],
                context_after=eth["context_after"],
                text_snippet=eth["text_snippet"],
                amount_local=eth["amount_local"],
                amount_raw=eth["raw_line"],
                rationale="Current-year ETH-domain financing contribution extracted from the exact Swiss federal budget row.",
                source_variant="row_credit_eth",
                section_name="ETH-Bereich",
                section_name_en="ETH Domain",
                program_code="CH_ETH",
                line_description="Finanzierungsbeitrag an ETH-Bereich",
                line_description_en="Financing contribution to the ETH domain",
                confidence=0.93,
                taxonomy_score=8.8,
            )
        )

    kti = _extract_row(sorted_pages, _KTI_ROW_RE, min_amount=50_000_000)
    if kti:
        records.append(
            _record_base(
                country=country,
                year=year,
                source_filename=source_filename,
                file_id=file_id,
                page_number=kti["page_number"],
                raw_line=kti["raw_line"],
                merged_line=kti["merged_line"],
                context_before=kti["context_before"],
                context_after=kti["context_after"],
                text_snippet=kti["text_snippet"],
                amount_local=kti["amount_local"],
                amount_raw=kti["raw_line"],
                rationale="Current-year innovation-promotion credit extracted from the exact KTI / Innosuisse row in the Swiss federal budget table.",
                source_variant="row_credit_kti",
                section_name="KTI / Innosuisse",
                section_name_en="Commission for Technology and Innovation / Innosuisse",
                program_code="CH_KTI",
                line_description="Technologie- und Innovationsförderung / Finanzierungsbeitrag an Innosuisse",
                line_description_en="Technology and innovation promotion / financing contribution to Innosuisse",
                confidence=0.9,
                taxonomy_score=8.6,
            )
        )

    return records


__all__ = ["extract_switzerland_items"]
