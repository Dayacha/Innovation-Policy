"""Colombian Presupuesto General de la Nación extractor.

Colombian R&D spending is managed by:
- **COLCIENCIAS** (Departamento Administrativo de Ciencia, Tecnología e Innovación)
  Old section code: 0320 (pre-2019)
- **MinCiencias** (Ministerio de Ciencia, Tecnología e Innovación)
  New section code: 3901 (from 2019)

Document structure
------------------
The "Ley de Apropiaciones" lists all budget entities. Each section has:

  SECCION: {code}
  {ENTITY NAME}
  A. PRESUPUESTO DE FUNCIONAMIENTO  {national}  {external}  {total}
  C. PRESUPUESTO DE INVERSION       {national}  {external}  {total}
  ...sub-programs...
  TOTAL PRESUPUESTO SECCIÓN  {national}  {external}  {total}
  SECCION: {next_code}

The three columns are: national resources | external resources | grand total.
Amounts are in Colombian Pesos (COP), unscaled.

NOTE: Sections often span multiple PDF pages. The approach is to process the
full document text, locate each science section, and extract the TOTAL line.
"""

from __future__ import annotations

import re
import logging
from typing import Optional

logger = logging.getLogger("innovation_pipeline")

# ── Section detection ─────────────────────────────────────────────────────────

_SECTION_CODE_RE = re.compile(r"SECCI[ÓO]N[:\s]+(\d{3,4})", re.IGNORECASE)

# Science section identifiers (section codes 0320 / 3901 — and name variations)
_SCIENCE_SECTION_NAMES = re.compile(
    r"INSTITUTO\s+COLOMBIANO\s+PARA\s+EL\s+DESARROLLO\s+DE\s+LA\s+CIENCIA"
    r"|DEPARTAMENTO\s+ADMINISTRATIVO\s+DE(?:\s+LA)?\s+CIENCIA[,\s]+TECNOLOG"
    r"|MINISTERIO\s+DE\s+CIENCIA[,\s]+TECNOLOG",
    re.IGNORECASE,
)

# TOTAL PRESUPUESTO SECCIÓN line — last column is the grand total
# Format: "TOTAL PRESUPUESTO SECCION  {nat}  {ext}  {total}"
# or:     "TOTAL PRESUPUESTO SECCIÓN\n{nat}\n{ext}\n{total}"
_TOTAL_RE = re.compile(
    r"TOTAL\s+PRESUPUESTO\s+SECCI[ÓO]N[:\s]*([\d,\.\s\xa0]+)",
    re.IGNORECASE,
)

# COP amount: comma-thousands (most files) or dot-thousands (some older files)
_COP_RE = re.compile(r"([\d]{1,3}(?:[,\.]\d{3})+)", re.IGNORECASE)


def _parse_cop(raw: str) -> Optional[float]:
    """Parse COP amount string to float. Handles comma or dot thousands separators."""
    raw = raw.strip()
    # "52,602,221,417" — comma-thousands
    if re.match(r"^\d{1,3}(?:,\d{3})+$", raw):
        return float(raw.replace(",", ""))
    # "52.602.221.417" — dot-thousands (European style)
    if re.match(r"^\d{1,3}(?:\.\d{3})+$", raw):
        return float(raw.replace(".", ""))
    # Space or plain digits
    cleaned = re.sub(r"[,\.\s]", "", raw)
    try:
        val = float(cleaned)
        return val if val > 0 else None
    except ValueError:
        return None


def _extract_total_from_section_text(section_text: str) -> Optional[float]:
    """Extract the TOTAL PRESUPUESTO SECCIÓN from a section text window.

    Handles both single-column and three-column (national|external|total) formats.
    Returns the grand total (last column when 3 columns, or first when 1 column).
    """
    m = _TOTAL_RE.search(section_text)
    if not m:
        return None

    raw = m.group(1)
    # Extract all large COP amounts after the TOTAL keyword
    amounts = []
    for am in _COP_RE.finditer(raw[:200]):
        val = _parse_cop(am.group(1))
        if val and val >= 1_000_000_000:  # at least 1B COP
            amounts.append(val)

    if not amounts:
        return None

    # If 3 columns: [national, external, total] — take last (largest or last listed)
    # If 1 column: take the only value
    # Heuristic: the grand total column is often the last value
    return amounts[-1]


# ── Public API ─────────────────────────────────────────────────────────────────

def extract_colombia_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract science budget records from a Colombian Ley de Apropiaciones PDF.

    Strategy: concatenate all page texts, locate science section header, then
    find the TOTAL PRESUPUESTO SECCIÓN line that follows it (before the next
    SECCION: header starts).
    """
    records: list[dict] = []

    # Concatenate all page texts
    all_pages_text = "\n".join(
        (row.text if isinstance(row.text, str) else "")
        for row in sorted_pages.itertuples(index=False)
    )

    if not all_pages_text.strip():
        return []

    # Find all SECCION: headers and their positions
    section_positions = [(m.start(), m.group(1))
                         for m in _SECTION_CODE_RE.finditer(all_pages_text)]

    # Target section codes for science
    science_codes = {"0320", "3901"}

    # Process each section
    for idx, (start_pos, code) in enumerate(section_positions):
        if code not in science_codes:
            # Also check by name if no code match
            name_window = all_pages_text[start_pos: start_pos + 200]
            if not _SCIENCE_SECTION_NAMES.search(name_window):
                continue

        # Extract text from this section to the next section header
        end_pos = (section_positions[idx + 1][0]
                   if idx + 1 < len(section_positions)
                   else start_pos + 5000)
        section_text = all_pages_text[start_pos: min(end_pos, start_pos + 5000)]

        # Determine agency identity
        is_minciencias = bool(re.search(
            r"MINISTERIO\s+DE\s+CIENCIA|3901", section_text[:300], re.IGNORECASE
        ))
        if is_minciencias:
            agency_code = "CO_MINCIENCIAS"
            agency_name = "Ministerio de Ciencia, Tecnología e Innovación (MinCiencias)"
        else:
            agency_code = "CO_COLCIENCIAS"
            agency_name = "Departamento Administrativo de Ciencia, Tecnología e Innovación (Colciencias)"

        amount = _extract_total_from_section_text(section_text)
        if amount is None or amount < 1_000_000_000:
            # Fallback: sum FUNCIONAMIENTO + INVERSIÓN
            func_m = re.search(
                r"A\.\s*PRESUPUESTO\s+DE\s+FUNCIONAMIENTO\s*([\d,\.\s]+)",
                section_text, re.IGNORECASE
            )
            inv_m = re.search(
                r"C\.\s*PRESUPUESTO\s+DE\s+INVERS[IÍ]ON\s*([\d,\.\s]+)",
                section_text, re.IGNORECASE
            )
            func_amt = None
            inv_amt = None
            if func_m:
                amounts = [_parse_cop(a.group(1)) for a in _COP_RE.finditer(func_m.group(1)[:100])
                           if _parse_cop(a.group(1)) and _parse_cop(a.group(1)) > 1e8]
                func_amt = amounts[-1] if amounts else None  # last = total column
            if inv_m:
                amounts = [_parse_cop(a.group(1)) for a in _COP_RE.finditer(inv_m.group(1)[:100])
                           if _parse_cop(a.group(1)) and _parse_cop(a.group(1)) > 1e8]
                inv_amt = amounts[-1] if amounts else None
            if func_amt or inv_amt:
                amount = (func_amt or 0) + (inv_amt or 0)

        if not amount or amount < 1_000_000_000:
            continue

        records.append({
            "country": country,
            "year": year,
            "section_code": "CO_SCIENCE",
            "section_name": "Ciencia, Tecnología e Innovación",
            "section_name_en": "Science, Technology and Innovation",
            "program_code": agency_code,
            "line_description": agency_name,
            "line_description_en": agency_name,
            "amount_local": amount,
            "currency": "COP",
            "unit": "COP",
            "rd_category": "direct_rd",
            "taxonomy_score": 8.0,
            "decision": "include",
            "confidence": 0.85,
            "source_file": source_filename,
            "file_id": file_id,
            "page_number": 1,
        })

    # Also search by name if no section code found
    if not records and _SCIENCE_SECTION_NAMES.search(all_pages_text):
        for m in _SCIENCE_SECTION_NAMES.finditer(all_pages_text):
            window = all_pages_text[m.start(): m.start() + 4000]
            amount = _extract_total_from_section_text(window)
            if amount and amount >= 1_000_000_000:
                is_minciencias = bool(re.search(r"MINISTERIO\s+DE\s+CIENCIA", window[:200], re.IGNORECASE))
                records.append({
                    "country": country,
                    "year": year,
                    "section_code": "CO_SCIENCE",
                    "section_name": "Ciencia, Tecnología e Innovación",
                    "section_name_en": "Science, Technology and Innovation",
                    "program_code": "CO_MINCIENCIAS" if is_minciencias else "CO_COLCIENCIAS",
                    "line_description": ("Ministerio de Ciencia, Tecnología e Innovación"
                                         if is_minciencias else
                                         "Departamento Administrativo de Ciencia (Colciencias)"),
                    "line_description_en": ("Ministry of Science, Technology and Innovation"
                                            if is_minciencias else
                                            "Department of Science (Colciencias)"),
                    "amount_local": amount,
                    "currency": "COP",
                    "unit": "COP",
                    "rd_category": "direct_rd",
                    "taxonomy_score": 8.0,
                    "decision": "include",
                    "confidence": 0.80,
                    "source_file": source_filename,
                    "file_id": file_id,
                    "page_number": 1,
                })
                break  # one record per file

    if records:
        logger.info(
            "Colombia extractor: %s (year %s) → %d records",
            source_filename, year, len(records),
        )
    else:
        logger.debug(
            "Colombia extractor: no science section found in %s (year %s).",
            source_filename, year,
        )

    return records
