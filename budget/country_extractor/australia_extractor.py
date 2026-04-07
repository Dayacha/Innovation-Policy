"""Australian Appropriation Act extractor.

Australian R&D spending is appropriated through Appropriation Acts
(typically Acts No. 1 and No. 2 for the main budget, plus supplementary acts).

Document formats handled
------------------------
1. Old format (1975–1999): Table-based with Division numbers, department and
   sub-agency rows. CSIRO appears as "Commonwealth Scientific and Industrial
   Research Organisation/Organization" within Department of Science / Department
   of Industry tables. NHMRC appears as a health sub-item.

2. Modern format (2000+): Portfolio-based tables with entities listed separately.
   Summary tables show Departmental + Administered + Total for each entity.
   Amounts in "$'000" (thousands of AUD).

Key R&D agencies tracked
------------------------
- CSIRO  (Commonwealth Scientific and Industrial Research Organisation)
- ARC    (Australian Research Council)
- NHMRC  (National Health and Medical Research Council)
- ANSTO  (Australian Nuclear Science and Technology Organisation)
- AIMS   (Australian Institute of Marine Science)
- Department of Science (old format, includes CSIRO)
- Department of Industry, Science and Technology (transition era)

Currency note
-------------
Pre-1966 (decimal currency): amounts in Australian pounds (£). Not handled.
Post-1966: AUD. Amounts in old-format docs are in whole dollars; in modern
docs they are in thousands of dollars ("$'000") — multiply by 1000.
"""

from __future__ import annotations

import re
import logging
from typing import Optional

logger = logging.getLogger("innovation_pipeline")

# ── Agency patterns ────────────────────────────────────────────────────────────

# Each entry: (pattern, program_code, canonical_name, is_department_level)
# is_department_level=True means the whole department is our R&D proxy
# (used for old-format docs where CSIRO isn't a standalone table row)
_AGENCY_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    # Match full name OR just the distinctive first part (for split-row tables)
    (
        re.compile(r"COMMONWEALTH SCIENTIFIC AND INDUSTRIAL", re.IGNORECASE),
        "AU_CSIRO",
        "Commonwealth Scientific and Industrial Research Organisation",
    ),
    (
        re.compile(r"\bCSIRO\b", re.IGNORECASE),
        "AU_CSIRO",
        "Commonwealth Scientific and Industrial Research Organisation",
    ),
    (
        re.compile(r"AUSTRALIAN RESEARCH COUNCIL", re.IGNORECASE),
        "AU_ARC",
        "Australian Research Council",
    ),
    (
        re.compile(r"NATIONAL HEALTH AND MEDICAL RESEARCH", re.IGNORECASE),
        "AU_NHMRC",
        "National Health and Medical Research Council",
    ),
    (
        re.compile(r"\bNHMRC\b", re.IGNORECASE),
        "AU_NHMRC",
        "National Health and Medical Research Council",
    ),
    (
        re.compile(r"AUSTRALIAN NUCLEAR SCIENCE AND TECHNOLOGY", re.IGNORECASE),
        "AU_ANSTO",
        "Australian Nuclear Science and Technology Organisation",
    ),
    (
        re.compile(r"\bANSTO\b", re.IGNORECASE),
        "AU_ANSTO",
        "Australian Nuclear Science and Technology Organisation",
    ),
    (
        re.compile(r"AUSTRALIAN INSTITUTE OF MARINE SCIENCE", re.IGNORECASE),
        "AU_AIMS",
        "Australian Institute of Marine Science",
    ),
    # Old-format department-level proxies for CSIRO
    (
        re.compile(r"DEPARTMENT OF SCIENCE(?:\s+AND\s+(?:TECHNOLOGY|ENVIRONMENT))?$", re.IGNORECASE),
        "AU_DEPT_SCIENCE",
        "Department of Science",
    ),
    (
        re.compile(r"DEPARTMENT OF INDUSTRY.*SCIENCE", re.IGNORECASE),
        "AU_DEPT_SCIENCE",
        "Department of Industry and Science",
    ),
]

_ANY_AGENCY_RE = re.compile(
    "|".join(p.pattern for p, _, _ in _AGENCY_PATTERNS),
    re.IGNORECASE,
)

# ── Amount patterns ────────────────────────────────────────────────────────────

# Modern: comma-separated thousands  e.g. "1,234,567" or "1,234"
_RE_AMT_COMMA = re.compile(r"\b(\d{1,3}(?:,\d{3})+)\b")

# Old format: space-separated thousands  e.g. "136 301 000"
_RE_AMT_SPACE = re.compile(r"\b(\d{1,3}(?: \d{3}){1,4})\b")

# $'000 indicator (modern format tables)
_RE_THOUSANDS_HEADER = re.compile(r"\$'?000", re.IGNORECASE)

# Em-dash/dash = nil/zero
_RE_NIL = re.compile(r"^[—–\-\.]+$")


def _parse_dollar_amount(raw: str, is_thousands: bool = False) -> Optional[float]:
    """Parse a dollar amount string to float (in AUD)."""
    cleaned = raw.replace(",", "").replace(" ", "").strip()
    try:
        val = float(cleaned)
        return val * 1000 if is_thousands else val
    except ValueError:
        return None


def _largest_amount(candidates: list[str], is_thousands: bool) -> Optional[float]:
    """Return the largest parseable amount from a list of cell values."""
    amounts = []
    for c in candidates:
        c = c.strip()
        if _RE_NIL.match(c):
            continue
        v = _parse_dollar_amount(c, is_thousands)
        if v is not None and v >= 100_000:
            amounts.append(v)
    return max(amounts) if amounts else None


def _line_agency_codes(line: str) -> set[str]:
    """Return agency codes matched on a single line."""
    codes: set[str] = set()
    for agency_re, prog_code, _ in _AGENCY_PATTERNS:
        if agency_re.search(line):
            codes.add(prog_code)
    return codes


def _select_row_amount(numeric_rows: list[list[str]], is_thousands: bool) -> tuple[Optional[float], bool]:
    """Pick the current-year amount from row-local numeric cells.

    Heuristic:
    - modern summary rows usually have 3 columns: departmental, administered, total
      -> choose the last numeric value from the first row
    - old-format lines usually have 1-2 year columns
      -> choose the first numeric value from the first row
    - if the matched row has no numeric cells, fall back to the first continuation row

    Returns
    -------
    (amount, needs_review)

    `needs_review=True` flags the transition-year pattern where a corporate-entity
    row only shows a single current-year figure and the immediately following
    comparison row is an order of magnitude larger. In those cases we keep the
    current-year amount but ask for manual review instead of silently upgrading
    to the prior-year figure.
    """
    suspicious_single_amount = False

    if len(numeric_rows) >= 2 and len(numeric_rows[0]) == 1 and len(numeric_rows[1]) == 1:
        current_amt = _parse_dollar_amount(numeric_rows[0][0], is_thousands)
        prior_amt = _parse_dollar_amount(numeric_rows[1][0], is_thousands)
        if (
            current_amt is not None
            and prior_amt is not None
            and current_amt >= 100_000
            and current_amt < 50_000_000
            and prior_amt >= current_amt * 10
        ):
            suspicious_single_amount = True

    for row_values in numeric_rows:
        if not row_values:
            continue
        chosen = row_values[-1] if len(row_values) >= 3 else row_values[0]
        amount = _parse_dollar_amount(chosen, is_thousands)
        if amount is not None and 100_000 <= amount <= 50_000_000_000:
            return amount, suspicious_single_amount
    return None, False


# ── Table-level extraction ─────────────────────────────────────────────────────

def _extract_from_table_text(table_text: str) -> list[tuple[str, str, float]]:
    """Extract (prog_code, canonical_name, amount) from a single table's text.

    Table text is tab-separated rows (one row per line), as produced by
    `_extract_docx_pages` in pdf_extract.py.

    Handles split-row agency names: Word tables sometimes wrap long entity
    names across two rows. When the matched row has no amounts, we look at
    the IMMEDIATELY following row — but only if that row doesn't start a new
    agency name.
    """
    if not _ANY_AGENCY_RE.search(table_text):
        return []

    # If the table already mentions CSIRO directly, skip the department-level
    # proxy (AU_DEPT_SCIENCE) to avoid double-counting.
    has_csiro = bool(re.search(r"COMMONWEALTH SCIENTIFIC AND INDUSTRIAL|\bCSIRO\b", table_text, re.IGNORECASE))

    # Detect if this table uses $'000
    is_thousands = bool(_RE_THOUSANDS_HEADER.search(table_text))

    results: list[tuple[str, str, float]] = []
    seen_codes: set[str] = set()

    lines = table_text.splitlines()

    def _numeric_cells_from_line(line: str) -> list[str]:
        cells = [c.strip() for c in line.split("\t")]
        return [
            c for c in cells
            if c and re.match(r"^[\d,\s\.—–\-]+$", c) and re.search(r"\d", c)
        ]

    def _collect_row_numeric_lines(start_idx: int, prog_code: str) -> list[list[str]]:
        """Collect numeric cells for the matched agency row only.

        We keep the current line plus immediate continuation lines when the
        label wraps. We do not scan until the portfolio total because that can
        incorrectly capture `Total:` rows instead of the entity amount.
        """
        collected: list[list[str]] = []
        for j in range(start_idx, min(len(lines), start_idx + 3)):
            line_j = lines[j]
            if j > start_idx and re.search(r"^\s*Total:\s", line_j, re.IGNORECASE):
                break

            codes = _line_agency_codes(line_j)
            if j > start_idx and codes and prog_code not in codes:
                break

            numerics = _numeric_cells_from_line(line_j)
            if numerics:
                collected.append(numerics)

            # Stop once we have a row with numbers unless the next physical line
            # is still clearly part of the same wrapped label.
            if numerics and j == start_idx:
                continue
            if numerics and j > start_idx:
                break
        return collected

    for i, line in enumerate(lines):
        cells = [c.strip() for c in line.split("\t")]

        for agency_re, prog_code, canonical_name in _AGENCY_PATTERNS:
            if not any(agency_re.search(c) for c in cells):
                continue
            if prog_code in seen_codes:
                continue
            # Skip department-level proxy when the specific agency is present
            if prog_code == "AU_DEPT_SCIENCE" and has_csiro:
                continue

            numeric_rows = _collect_row_numeric_lines(i, prog_code)
            amount, needs_review = _select_row_amount(numeric_rows, is_thousands)
            if amount is None or amount < 100_000:
                continue

            # Sanity cap: no single Australian R&D agency had > $50B
            if amount > 50_000_000_000:
                continue

            results.append((prog_code, canonical_name, amount, needs_review))
            seen_codes.add(prog_code)
            break  # Only one agency match per line

    return results


# ── Public API ─────────────────────────────────────────────────────────────────

def extract_australia_items(
    sorted_pages,   # DataFrame with page_number, text, extraction_method columns
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract R&D spending records from an Australian Appropriation Act.

    Parameters
    ----------
    sorted_pages : DataFrame with columns [page_number, text, extraction_method, ...]
    file_id, country, year, source_filename : metadata

    Returns
    -------
    List of dicts matching the standard budget item schema.
    """
    records: list[dict] = []
    seen_keys: set[tuple[str, int]] = set()

    # First pass: check if any page mentions CSIRO directly
    # (if so, suppress AU_DEPT_SCIENCE proxy across the whole file)
    all_text = " ".join(
        (row.text if isinstance(row.text, str) else "")
        for row in sorted_pages.itertuples(index=False)
    )
    file_has_csiro = bool(re.search(r"COMMONWEALTH SCIENTIFIC AND INDUSTRIAL|\bCSIRO\b", all_text, re.IGNORECASE))

    for row in sorted_pages.itertuples(index=False):
        pg = int(row.page_number)
        text = row.text if isinstance(row.text, str) else ""
        extraction_method = getattr(row, "extraction_method", "")

        if not text.strip():
            continue

        # Only process table pages (docx_table) or any page with agency keywords
        if extraction_method == "docx_table" or _ANY_AGENCY_RE.search(text):
            items = _extract_from_table_text(text)
            for prog_code, canonical_name, amount, needs_review in items:
                # Suppress department-level proxy when specific agency is present
                if prog_code == "AU_DEPT_SCIENCE" and file_has_csiro:
                    continue
                key = (prog_code, int(round(amount, -4)))
                if key in seen_keys:
                    continue
                seen_keys.add(key)

                snippet = text[:200].replace("\n", " ").replace("\t", " ").strip()
                records.append({
                    "country": country,
                    "year": year,
                    "section_code": "AU_RD",
                    "section_name": "Science, Research and Innovation",
                    "section_name_en": "Science, Research and Innovation",
                    "program_code": prog_code,
                    "line_description": canonical_name,
                    "line_description_en": canonical_name,
                    "amount_local": amount,
                    "currency": "AUD",
                    "unit": "AUD",
                    "rd_category": "direct_rd",
                    "taxonomy_score": 8.0,
                    "decision": "review" if needs_review else "include",
                    "confidence": 0.62 if needs_review else 0.80,
                    "source_file": source_filename,
                    "file_id": file_id,
                    "page_number": pg,
                })

    # Per-agency deduplication: keep the LARGEST amount for each agency code.
    # Old-format files produce multiple records for the same agency (sub-totals
    # and grand totals). Modern files may also have both portfolio summary and
    # per-entity detail tables.
    best: dict[str, dict] = {}
    for rec in records:
        code = rec["program_code"]
        if code not in best or rec["amount_local"] > best[code]["amount_local"]:
            best[code] = rec

    records = list(best.values())

    if not records:
        logger.debug(
            "Australia extractor: no R&D agency records found in %s (year %s).",
            source_filename, year,
        )
    else:
        logger.debug(
            "Australia extractor: %s (year %s) -> %d records",
            source_filename, year, len(records),
        )

    return records
