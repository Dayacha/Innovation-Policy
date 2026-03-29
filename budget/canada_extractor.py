"""Canada Appropriation Act extractor.

Canadian R&D spending is appropriated through Appropriation Acts (Main Estimates
and Supplementary Estimates). Key R&D agencies appear as named blocks in schedule
tables.

Document formats handled
------------------------
1. Old English-only (1987–2001): Tabular schedule with "No. of Vote / Service /
   Amount / Total" columns. Amounts appear directly after description lines.
   Numbers use commas as thousand separators.

2. Bilingual English/French (2002–2017): Similar tabular structure, now with
   both English headings (ALL CAPS) and French sub-headings. Numbers use commas.

3. Bilingual with space separators (2018+): Main Estimates column uses spaces
   as thousands separators (French formatting). Each Vote line shows two numbers:
   "Amount in Main Estimates ($)" and "Interim Appropriation Granted by this Act ($)".
   We take the first (larger) number as the main estimate.

Key R&D agencies tracked
------------------------
- National Research Council of Canada (NRC)
- Natural Sciences and Engineering Research Council (NSERC)
- Social Sciences and Humanities Research Council (SSHRC)
- Canadian Institutes of Health Research (CIHR)  [est. 2000]
- Canada Foundation for Innovation (CFI)  [est. 1997]
- Atomic Energy of Canada Limited (AECL)
- Genome Canada  [est. 2000]
- Canadian High Arctic Research Station (CHARS)  [est. 2014]
- Science and Technology (Ministry of State)  [1971–1993]

Deduplication
-------------
Multiple Appropriation Acts pass each fiscal year (Main + 1–3 Supplementary).
Records are keyed by (year, program_code, amount_bin) so that different Acts
in the same year that authorize the same agency at similar amounts are counted
only once.  Acts with genuinely different amounts (different Supplementary
batches) are kept as separate records.
"""

from __future__ import annotations

import re
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger("innovation_pipeline")


# ── Agency registry ────────────────────────────────────────────────────────────

_AGENCY_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    # (heading_regex, program_code, canonical_name)
    (
        re.compile(r"NATIONAL RESEARCH COUNCIL OF CANADA", re.IGNORECASE),
        "CA_NRC",
        "National Research Council of Canada",
    ),
    (
        re.compile(r"NATURAL SCIENCES AND ENGINEERING RESEARCH COUNCIL", re.IGNORECASE),
        "CA_NSERC",
        "Natural Sciences and Engineering Research Council",
    ),
    # French variant for 2020+ bilingual pages
    (
        re.compile(r"CONSEIL DE RECHERCHES EN SCIENCES NATURELLES ET EN G[EÉ]NIE", re.IGNORECASE),
        "CA_NSERC",
        "Natural Sciences and Engineering Research Council",
    ),
    (
        re.compile(r"SOCIAL SCIENCES AND HUMANITIES RESEARCH COUNCIL", re.IGNORECASE),
        "CA_SSHRC",
        "Social Sciences and Humanities Research Council",
    ),
    (
        re.compile(r"CONSEIL DE RECHERCHES EN SCIENCES HUMAINES", re.IGNORECASE),
        "CA_SSHRC",
        "Social Sciences and Humanities Research Council",
    ),
    (
        re.compile(r"CANADIAN INSTITUTES OF HEALTH RESEARCH", re.IGNORECASE),
        "CA_CIHR",
        "Canadian Institutes of Health Research",
    ),
    (
        re.compile(r"INSTITUTS DE RECHERCHE EN SANT[EÉ] DU CANADA", re.IGNORECASE),
        "CA_CIHR",
        "Canadian Institutes of Health Research",
    ),
    (
        re.compile(r"CANADA FOUNDATION FOR INNOVATION", re.IGNORECASE),
        "CA_CFI",
        "Canada Foundation for Innovation",
    ),
    (
        re.compile(r"FONDATION CANADIENNE POUR L.INNOVATION", re.IGNORECASE),
        "CA_CFI",
        "Canada Foundation for Innovation",
    ),
    (
        re.compile(r"ATOMIC ENERGY OF CANADA", re.IGNORECASE),
        "CA_AECL",
        "Atomic Energy of Canada Limited",
    ),
    (
        re.compile(r"[EÉ]NERGIE ATOMIQUE DU CANADA", re.IGNORECASE),
        "CA_AECL",
        "Atomic Energy of Canada Limited",
    ),
    # French heading for NRC (2018+ bilingual docs)
    (
        re.compile(r"CONSEIL NATIONAL DE RECHERCHES DU CANADA", re.IGNORECASE),
        "CA_NRC",
        "National Research Council of Canada",
    ),
    (
        re.compile(r"GENOME CANADA", re.IGNORECASE),
        "CA_GENOME",
        "Genome Canada",
    ),
    (
        re.compile(r"CANADIAN HIGH ARCTIC RESEARCH STATION", re.IGNORECASE),
        "CA_CHARS",
        "Canadian High Arctic Research Station",
    ),
    # Early Ministry of State for Science and Technology (1971–1993)
    (
        re.compile(
            r"(?:MINISTRY OF STATE|MINIST[EÈ]RE D[''E]TAT).{0,40}SCIENCE AND TECHNOLOGY",
            re.IGNORECASE,
        ),
        "CA_SCITECH_MINISTRY",
        "Ministry of State for Science and Technology",
    ),
    (
        re.compile(
            r"SCIENCE AND TECHNOLOGY\s*\n\s*MINISTRY OF STATE",
            re.IGNORECASE,
        ),
        "CA_SCITECH_MINISTRY",
        "Ministry of State for Science and Technology",
    ),
]

# Compile all heading regexes into a single pattern for fast page-level screening
_ANY_AGENCY_RE = re.compile(
    "|".join(pat.pattern for pat, _, _ in _AGENCY_PATTERNS),
    re.IGNORECASE,
)

# ── Amount patterns ────────────────────────────────────────────────────────────

# English format: 1,234,567 or 1,234,567.00
_RE_AMT_COMMA = re.compile(r"\b(\d{1,3}(?:,\d{3})+(?:\.\d+)?)\b")

# French/2018+ format: thousands separated by space, no-break space (U+00A0),
# or narrow no-break space (U+202F).  Must be ≥ 3 groups (≥ 100 000).
_SPACE_CHARS = r"[ \u00a0\u202f]"
_RE_AMT_SPACE = re.compile(
    r"\b(\d{1,3}(?:" + _SPACE_CHARS + r"\d{3}){2,})\b"
)


def _parse_amount(raw: str) -> Optional[float]:
    """Parse a raw amount string to float, returning None if unparseable."""
    try:
        # Strip commas and all space variants used as thousands separators
        cleaned = raw.replace(",", "").replace("\u00a0", "").replace("\u202f", "").replace(" ", "").strip()
        return float(cleaned)
    except ValueError:
        return None


def _extract_amounts_from_block(block: str) -> list[float]:
    """Extract all dollar amounts from a text block, returning unique sorted values."""
    amounts: set[float] = set()
    for m in _RE_AMT_COMMA.finditer(block):
        v = _parse_amount(m.group(1))
        if v and v >= 100_000:
            amounts.add(v)
    for m in _RE_AMT_SPACE.finditer(block):
        v = _parse_amount(m.group(1))
        if v and v >= 100_000:
            amounts.add(v)
    return sorted(amounts)


MAX_SINGLE_AGENCY_CAD = 3_000_000_000  # $3B — no R&D agency ever exceeded this


def _get_block_total(amounts: list[float]) -> Optional[float]:
    """
    Given the sorted list of amounts found in an agency block, return the most
    likely total for that specific agency.

    Strategy:
    1. Exclude amounts > MAX_SINGLE_AGENCY_CAD (no R&D agency had such a budget).
       This handles schedule-level totals like '$23B for all departments'.
    2. Return the maximum of remaining amounts (the agency subtotal or the
       largest vote amount if no explicit subtotal).

    Note: The jump-filter approach was removed because operating ($46M) to
    grants ($1.2B) looks like a suspicious jump but is completely legitimate.
    The $3B hard cap is sufficient to filter out real schedule totals.
    """
    if not amounts:
        return None
    # Hard cap: no Canadian R&D agency ever had >$3B in a single appropriation
    filtered = [a for a in amounts if a <= MAX_SINGLE_AGENCY_CAD]
    return max(filtered) if filtered else None


# ── Page-level extraction ──────────────────────────────────────────────────────

# Pattern that marks the START of a new top-level agency/department heading.
# We use ALL-CAPS lines (≥ 10 chars) as block boundaries.
# Allow: periods ("VIA RAIL CANADA INC."), straight apostrophes ("L'IMMIGRATION"),
# curly apostrophes, hyphens, parentheses.
_RE_HEADING = re.compile(r"^[A-ZÀ-ÖØ-Ü][A-ZÀ-ÖØ-Ü\s,\u0027\u2018\u2019()\-.]{9,}$", re.MULTILINE)


def _split_into_blocks(text: str) -> list[tuple[str, str]]:
    """
    Split page text into (heading, body) pairs at ALL-CAPS headings.

    Heuristic: a line is a heading if it is ALL-CAPS (allowing accents, commas,
    hyphens, apostrophes, periods) and at least 10 characters long.

    Body is limited to MAX_BODY_LINES lines after the heading to handle two-column
    OCR layouts where amounts from all agencies on a page appear at the bottom
    (after all descriptions). This prevents the last agency's block from
    absorbing amounts belonging to earlier agencies.
    """
    MAX_BODY_LINES = 25  # generous window for any agency's votes

    lines = text.splitlines()
    blocks: list[tuple[str, str]] = []
    current_heading = ""
    current_body_lines: list[str] = []

    for line in lines:
        stripped = line.strip()
        if _RE_HEADING.fullmatch(stripped) and len(stripped) >= 10:
            # Save previous block
            if current_heading or current_body_lines:
                blocks.append((current_heading, "\n".join(current_body_lines)))
            current_heading = stripped
            current_body_lines = []
        else:
            # Only add to body if within window (prevents two-column layout issues)
            if len(current_body_lines) < MAX_BODY_LINES:
                current_body_lines.append(stripped)

    # Don't forget final block
    if current_heading or current_body_lines:
        blocks.append((current_heading, "\n".join(current_body_lines)))

    return blocks


def _extract_from_page(
    text: str,
    year: str,
    source_filename: str,
    page_number: int,
    file_id: str,
    country: str,
) -> list[dict]:
    """Extract R&D agency records from a single page."""
    if not _ANY_AGENCY_RE.search(text):
        return []

    records: list[dict] = []
    blocks = _split_into_blocks(text)

    for heading, body in blocks:
        block_text = heading + "\n" + body

        for agency_re, prog_code, canonical_name in _AGENCY_PATTERNS:
            # Only match agency names in the HEADING line (ALL-CAPS), not in body text.
            # This prevents footnotes/references to agency names from triggering false matches.
            if not agency_re.search(heading):
                continue

            amounts = _extract_amounts_from_block(block_text)
            total = _get_block_total(amounts)
            if total is None or total < 100_000:
                continue

            snippet = block_text[:300].replace("\n", " ").strip()
            records.append(
                {
                    "country": country,
                    "year": year,
                    "section_code": "CA_RD",
                    "section_name": "Science, Technology and Innovation",
                    "section_name_en": "Science, Technology and Innovation",
                    "program_code": prog_code,
                    "line_description": canonical_name,
                    "line_description_en": canonical_name,
                    "amount_local": total,
                    "currency": "CAD",
                    "unit": "CAD",
                    "rd_category": "direct_rd",
                    "taxonomy_score": 8.0,
                    "decision": "include",
                    "confidence": 0.75,
                    "source_file": source_filename,
                    "file_id": file_id,
                    "page_number": page_number,
                }
            )
            # Only match each agency once per block
            break

    return records


# ── Public API ─────────────────────────────────────────────────────────────────

def extract_canada_items(
    sorted_pages,   # DataFrame with page_number, text columns
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract R&D spending records from a Canadian Appropriation Act PDF.

    Parameters
    ----------
    sorted_pages : DataFrame with columns [page_number, text, ...]
    file_id, country, year, source_filename : metadata

    Returns
    -------
    List of dicts matching the standard budget item schema.
    """
    all_records: list[dict] = []

    for row in sorted_pages.itertuples(index=False):
        pg = int(row.page_number)
        text = row.text if isinstance(row.text, str) else ""
        if not text.strip():
            continue
        page_records = _extract_from_page(
            text, year=year, source_filename=source_filename,
            page_number=pg, file_id=file_id, country=country,
        )
        all_records.extend(page_records)

    if not all_records:
        logger.debug(
            "Canada extractor: no R&D agency records found in %s (year %s).",
            source_filename, year,
        )
        return []

    # Deduplicate within this file: same (prog_code, amount_bin) → keep first
    # This prevents the same agency total appearing on multiple pages of the same Act.
    seen: set[tuple[str, int]] = set()
    deduped: list[dict] = []
    for rec in all_records:
        key = (rec["program_code"], int(round(rec["amount_local"], -4)))
        if key not in seen:
            seen.add(key)
            deduped.append(rec)

    logger.info(
        "Canada extractor: %s (year %s) → %d agency records (from %d raw)",
        source_filename, year, len(deduped), len(all_records),
    )
    return deduped
