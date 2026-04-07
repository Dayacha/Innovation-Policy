"""UK Budget Red Book extractor.

The UK Red Book is a narrative budget document, not an appropriations table.
For consistency, this extractor only returns records when the document states an
explicit annual aggregate for science/R&D spending. It intentionally skips small
package announcements, historical retrospective figures, and programme-specific
subcomponents that are not comparable across years.

Empirically validated anchor styles
-----------------------------------
- 1996: "Total central government spending on Science and Technology ... about £6 billion"
- 2020: "public R&D investment to £22 billion per year by 2024-25"
- 2021 Autumn Budget: "providing £20 billion across the UK by 2024-25"
- 2025: "annual government investment in R&D will grow to £22.6 billion by 2029-30"

The middle Red Books (roughly 2000s to late 2010s) often contain only packages
and programme announcements rather than a clean annual aggregate. Those years
should return no rows rather than low-quality pseudo-totals.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

logger = logging.getLogger("innovation_pipeline")


_GBP_AMOUNT_RE = re.compile(
    r"£\s*([\d,]+(?:\.\d+)?)\s*(billion|bn|million|m(?:illion)?)\b",
    re.IGNORECASE,
)

_AGGREGATE_PATTERNS: list[tuple[re.Pattern, str, str, str, float]] = [
    (
        re.compile(
            r"total\s+central\s+government\s+spending\s+on\s+science\s+and\s+technology"
            r".{0,120}?£\s*[\d,]+(?:\.\d+)?\s*(?:billion|bn)",
            re.IGNORECASE | re.DOTALL,
        ),
        "UK_ST_TOTAL",
        "Science and Technology",
        "Total central government spending on Science and Technology",
        0.92,
    ),
    (
        re.compile(
            r"public\s+r&d\s+investment.{0,120}?£\s*[\d,]+(?:\.\d+)?\s*(?:billion|bn)"
            r".{0,60}?(?:per\s+year|by\s+\d{4}(?:-\d{2})?)",
            re.IGNORECASE | re.DOTALL,
        ),
        "UK_RD_TOTAL",
        "Public R&D investment",
        "Public R&D investment",
        0.95,
    ),
    (
        re.compile(
            r"total\s+direct\s+r&d\s+spending\s+to\s+£\s*[\d,]+(?:\.\d+)?\s*(?:billion|bn)"
            r".{0,60}?(?:per\s+annum|per\s+year|by\s+\d{4}(?:-\d{2})?)",
            re.IGNORECASE | re.DOTALL,
        ),
        "UK_RD_TOTAL",
        "Total direct R&D spending",
        "Total direct R&D spending",
        0.95,
    ),
    (
        re.compile(
            r"annual\s+government\s+investment\s+in\s+r&d\s+will\s+grow\s+to\s+£\s*[\d,]+(?:\.\d+)?\s*(?:billion|bn)",
            re.IGNORECASE | re.DOTALL,
        ),
        "UK_RD_TOTAL",
        "Government R&D investment",
        "Annual government investment in R&D",
        0.95,
    ),
    (
        re.compile(
            r"increase\s+investment\s+in\s+science,\s*innovation\s+and\s+technology\s+to\s+£\s*[\d,]+(?:\.\d+)?\s*(?:billion|bn)",
            re.IGNORECASE | re.DOTALL,
        ),
        "UK_RD_TOTAL",
        "Science, innovation and technology investment",
        "Science, innovation and technology investment",
        0.90,
    ),
]

_EXCLUDE_CONTEXT_RE = re.compile(
    r"(tax\s+relief|r&d\s+tax|businesses?\s+spend|private\s+sector\s+r&d|"
    r"metascience|women\s+in\s+innovation|fellowships?|catapult|satellite|"
    r"quantum|faraday|green\s+future|marketplace|concrete|ai\s+for\s+science)",
    re.IGNORECASE,
)


def _parse_amount_from_snippet(snippet: str) -> Optional[float]:
    match = _GBP_AMOUNT_RE.search(snippet)
    if not match:
        return None
    value = float(match.group(1).replace(",", ""))
    unit = match.group(2).lower()
    if unit in ("billion", "bn"):
        return value * 1e9
    return value * 1e6


def _build_record(
    *,
    country: str,
    year: str,
    source_filename: str,
    file_id: str,
    page_number: int,
    program_code: str,
    label: str,
    amount: float,
    snippet: str,
    confidence: float,
) -> dict:
    return {
        "country": country,
        "year": year,
        "section_code": "UK_SCIENCE",
        "section_name": "Science and innovation",
        "section_name_en": "Science and innovation",
        "program_code": program_code,
        "line_description": snippet,
        "line_description_en": snippet,
        "amount_local": amount,
        "currency": "GBP",
        "unit": "GBP",
        "rd_category": "direct_rd",
        "taxonomy_score": 8.5,
        "decision": "include",
        "confidence": confidence,
        "source_file": source_filename,
        "file_id": file_id,
        "page_number": page_number,
        "program_description": label,
        "program_description_en": label,
    }


def extract_uk_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract explicit aggregate science/R&D totals from UK Red Books."""
    records: list[dict] = []
    seen_keys: set[tuple[str, int]] = set()

    for row in sorted_pages.itertuples(index=False):
        page_number = int(row.page_number)
        text = row.text if isinstance(row.text, str) else ""
        if not text.strip():
            continue

        for pattern, program_code, label, description, confidence in _AGGREGATE_PATTERNS:
            match = pattern.search(text)
            if not match:
                continue

            snippet = " ".join(match.group(0).split())
            if _EXCLUDE_CONTEXT_RE.search(snippet):
                continue

            amount = _parse_amount_from_snippet(snippet)
            if amount is None:
                continue
            if amount < 1_000_000_000:
                continue
            dedupe_key = (program_code, int(round(amount)))
            if dedupe_key in seen_keys:
                break
            seen_keys.add(dedupe_key)

            records.append(
                _build_record(
                    country=country,
                    year=year,
                    source_filename=source_filename,
                    file_id=file_id,
                    page_number=page_number,
                    program_code=program_code,
                    label=description,
                    amount=amount,
                    snippet=snippet[:240],
                    confidence=confidence,
                )
            )
            break

    if not records:
        logger.debug(
            "UK extractor: no explicit aggregate science/R&D total found in %s (year %s).",
            source_filename,
            year,
        )

    return records
