"""UK Budget Red Book extractor.

UK Budget documents (Financial Statement and Budget Report / Budget Red Book)
are narrative policy documents, not structured appropriation tables. R&D and
science spending appears as:

  - 1975-1991: Short FSBRs. Education + Science is ONE combined departmental
    line in the Supply Services table. No R&D-specific breakdown.
  - 1992-1995: Scanned/image PDFs — no extractable text.
  - 1996+: Longer Budget documents with dedicated "Science and Technology"
    or "Science and Innovation" sections containing specific £ amounts.

The extractor focuses on 1996+ where the Science Budget figure is mentioned
explicitly and returns records in the standard budget item schema.

Budget year convention
----------------------
UK Budget is typically delivered in March for the *current* fiscal year
(e.g. Budget 2007 → fiscal year 2007-08). The extractor tags records with
the calendar year shown in the filename (e.g. "2007_UK.pdf" → year=2007).

Where two budgets are available in the same year (e.g. 2010_03_UK.pdf and
2010_06_UK.pdf) the extractor processes both but year-level deduplication
in budget_extractor.py keeps only the first set of records for each year.
"""

from __future__ import annotations

import re
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger("innovation_pipeline")

# ── Amount patterns ────────────────────────────────────────────────────────────

# Matches "£1.6 billion", "£750 million", "£6bn", "£400m" etc.
_RE_GBP = re.compile(
    r"£\s*([\d,]+(?:\.\d+)?)\s*(billion|bn|million|m(?:illion)?)\b",
    re.IGNORECASE,
)

# Context patterns that indicate science/R&D spending announcements
# (ordered by priority — first match wins)
_SCIENCE_CONTEXT_PATTERNS: list[tuple[str, str]] = [
    # Aggregate totals (highest priority)
    (r"total\b.{0,60}\bscience.{0,80}" + _RE_GBP.pattern, "Total Science & Technology"),
    (r"total\b.{0,40}\bpublic.{0,20}R&D.{0,80}" + _RE_GBP.pattern, "Total Public R&D"),
    (r"public.{0,20}R&D investment.{0,120}" + _RE_GBP.pattern, "Public R&D Investment"),
    (r"science spending.{0,120}" + _RE_GBP.pattern, "Science Spending"),
    # Science Budget (OST/BIS/UKRI-managed)
    (r"science budget.{0,120}" + _RE_GBP.pattern, "Science Budget (OST/UKRI)"),
    (r"research councils?.{0,120}" + _RE_GBP.pattern, "Research Councils"),
    (r"UKRI.{0,120}" + _RE_GBP.pattern, "UKRI"),
    # Specific R&D investment announcements
    (r"R&D.{0,80}" + _RE_GBP.pattern, "R&D Investment"),
    (r"science.{0,20}innovation.{0,80}" + _RE_GBP.pattern, "Science & Innovation"),
    (r"science.{0,80}" + _RE_GBP.pattern, "Science Spending"),
]

# Compile all patterns
_COMPILED_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(pat, re.IGNORECASE | re.DOTALL), label)
    for pat, label in _SCIENCE_CONTEXT_PATTERNS
]

# Pages that are clearly NOT about science spending (skip these)
_SKIP_HEADING_RE = re.compile(
    r"(chapter\s+[1-4]\b|fiscal\s+(framework|policy)|economic\s+(outlook|forecast)|"
    r"taxation|income\s+tax|corporation\s+tax|vat\s+|national\s+insurance|housing|"
    r"welfare|benefits|NHS\s+budget)",
    re.IGNORECASE,
)

# Patterns that indicate the matched amount is NOT government science spending:
# - Business R&D statistics (£47.5B companies claimed)
# - R&D tax relief/credit figures
# - General investment funds not specific to science (NPIF)
# - Retrospective statistics ("invested X between 2010 and 2019")
_EXCLUDE_CONTEXT_RE = re.compile(
    r"(compan(?:y|ies)\s+claimed"
    r"|businesses?\s+(?:spend|spent|invest|claimed)"
    r"|private\s+sector\s+R&D"
    r"|R&D\s+tax\s+(?:credit|relief|incentive)"
    r"|tax\s+(?:credit|relief)\s+on\s+R&D"
    r"|tax\s+relief\s+on\s+[£\d]"
    r"|NPIF\b"
    r"|National\s+Productivity\s+Investment\s+Fund"
    r"|between\s+20\d\d\s+and\s+20\d\d"
    r"|by\s+20\d\d[,\s])"
    ,
    re.IGNORECASE,
)

# Headings that signal a science/innovation section
_SCIENCE_HEADING_RE = re.compile(
    r"(science\s+and\s+(technology|innovation)|"
    r"investment\s+in\s+(science|innovation|r&d)|"
    r"innovation\s+and\s+science|"
    r"science,\s+research|"
    r"science\s+budget)",
    re.IGNORECASE,
)


def _parse_amount(m: re.Match) -> Optional[float]:
    """Convert a regex match of _RE_GBP to a float in GBP."""
    try:
        value = float(m.group(1).replace(",", ""))
        unit = m.group(2).lower()
        if unit in ("billion", "bn"):
            return value * 1e9
        else:
            return value * 1e6
    except (ValueError, AttributeError):
        return None


def _extract_from_page(text: str) -> list[tuple[str, float, str]]:
    """
    Extract (description, amount_gbp, context_snippet) tuples from a page.

    Returns up to 3 items per page to avoid over-extraction.
    """
    results: list[tuple[str, float, str]] = []
    seen_amounts: set[float] = set()

    for pat, label in _COMPILED_PATTERNS:
        for m in pat.finditer(text):
            amt_m = _RE_GBP.search(m.group())
            if not amt_m:
                continue
            amount = _parse_amount(amt_m)
            if amount is None or amount < 1e6:  # ignore amounts < £1 million
                continue
            snippet = m.group()[:300].replace("\n", " ").strip()
            # Skip if the matched context indicates business R&D stats, tax relief, or
            # non-science-specific funds (NPIF, retrospective comparisons, etc.)
            if _EXCLUDE_CONTEXT_RE.search(snippet):
                continue
            # Deduplicate by amount (same figure mentioned twice)
            rounded = round(amount, -5)
            if rounded in seen_amounts:
                continue
            seen_amounts.add(rounded)
            snippet = snippet[:200]
            results.append((label, amount, snippet))
            if len(results) >= 5:
                break
        if len(results) >= 5:
            break

    return results


def _is_science_page(text: str) -> bool:
    """Return True if the page likely contains science/R&D budget content."""
    text_lower = text.lower()
    has_science = any(
        kw in text_lower
        for kw in ["science", "r&d", "research and development", "research council",
                   "innovation fund", "ukri", "horizon europe"]
    )
    has_amount = bool(_RE_GBP.search(text))
    return has_science and has_amount


def extract_uk_items(
    sorted_pages,   # DataFrame with page_number, text columns
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract science/R&D budget records from a UK Budget Red Book PDF.

    Parameters
    ----------
    sorted_pages : DataFrame with columns [page_number, text, ...]
    file_id, country, year, source_filename : metadata

    Returns
    -------
    List of dicts matching the standard budget item schema.
    """
    records: list[dict] = []
    seen_amounts: set[float] = set()

    # Collect all pages
    all_pages: dict[int, str] = {}
    for row in sorted_pages.itertuples(index=False):
        pg = int(row.page_number)
        text = row.text if isinstance(row.text, str) else ""
        all_pages[pg] = text

    # Find science/R&D relevant pages
    science_pages: list[tuple[int, str]] = []
    for pg in sorted(all_pages.keys()):
        text = all_pages[pg]
        if _is_science_page(text):
            science_pages.append((pg, text))

    if not science_pages:
        logger.warning(
            "UK extractor: no science/R&D content found in %s (year %s).",
            source_filename, year,
        )
        return []

    # Extract amounts from science pages
    for pg, text in science_pages:
        items = _extract_from_page(text)
        for label, amount, snippet in items:
            rounded = round(amount, -5)
            if rounded in seen_amounts:
                continue
            seen_amounts.add(rounded)

            # Build synthetic program code
            code_map = {
                "Total Science & Technology": "UK_ST_TOTAL",
                "Total Public R&D": "UK_RD_TOTAL",
                "Public R&D Investment": "UK_RD_TOTAL",
                "Science Spending": "UK_SCIENCE_TOTAL",
                "Science Budget (OST/UKRI)": "UK_SCIENCE_BUDGET",
                "Research Councils": "UK_RESEARCH_COUNCILS",
                "UKRI": "UK_UKRI",
                "R&D Investment": "UK_RD_INVESTMENT",
                "Science & Innovation": "UK_SCIENCE_INNOVATION",
            }
            prog_code = code_map.get(label, "UK_SCIENCE_OTHER")

            records.append({
                "country": country,
                "year": year,
                "section_code": "UK_SCIENCE",
                "section_name": "Science and Technology",
                "section_name_en": "Science and Technology",
                "program_code": prog_code,
                "line_description": snippet[:200],
                "line_description_en": snippet[:200],
                "amount_local": amount,
                "currency": "GBP",
                "unit": "GBP",
                "rd_category": "direct_rd",
                "taxonomy_score": 8.0,
                "decision": "include",
                "confidence": 0.65,
                "source_file": source_filename,
                "file_id": file_id,
                "page_number": pg,
            })

    if not records:
        logger.warning(
            "UK extractor: science pages found but no amounts extracted in %s (year %s).",
            source_filename, year,
        )

    return records
