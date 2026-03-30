"""Costa Rican Presupuesto Ordinario y Extraordinario de la República extractor.

The Costa Rican budget ("Ley de Presupuesto Ordinario y Extraordinario de la
República") allocates funds to government institutions via transfer lines.

Structure observed in files (2010–2021):
  Transfer lines appear in the expenditure section with format:
    {registro_number}
    60103       ← object code (transfer to decentralized institution)
    001         ← source
    {programa}  ← program code
    {ff}        ← funding source
    {INSTITUTION NAME AND DESCRIPTION}
    {amount_in_colones}

Key entities:
  CONICIT  : Consejo Nacional de Investigaciones Científicas y Tecnológicas
             (Ley No. 5048, 1972) — national science funding agency
  MICIT    : Ministerio de Ciencia, Tecnología y Telecomunicaciones
             (renamed MICITT after 2012)
  UCR      : Universidad de Costa Rica (substantial research)

Programs extracted:
  CR_CONICIT : CONICIT operating budget + incentives fund
  CR_MICIT   : MICIT/MICITT total appropriation

Currency: CRC (Costa Rican colón, ¢), full values (no scaling needed).
Amounts use dot as thousands separator: "1.250.000.000" = 1,250,000,000 CRC.
"""

from __future__ import annotations

import re
import logging
from typing import Optional

logger = logging.getLogger("innovation_pipeline")

# ── Regex constants ─────────────────────────────────────────────────────────────

# CONICIT institution name — may appear with slight variations
_CONICIT_RE = re.compile(
    r'CONSEJO\s+NACIONAL\s+DE\s+INVESTIGACIONES\s+CIENT[IÍ]FICAS'
    r'(?:\s+Y\s+TECNOL[OÓ]GICAS?)?'
    r'|CONICIT\b',
    re.IGNORECASE,
)

# MICIT / MICITT ministry
_MICIT_RE = re.compile(
    r'MINISTERIO\s+DE\s+CIENCIA[,\s]+TECNOLOG[IÍ]A'
    r'|MICITT?\b',
    re.IGNORECASE,
)

# CRC amount: dot-thousands format "1.250.000.000" or comma "1,250,000,000"
# or space-separated, or plain
_CRC_RE = re.compile(
    r'(?<!\d)'
    r'(\d{1,3}(?:\.\d{3})+|\d{1,3}(?:,\d{3})+|\d{7,})'
    r'(?!\d)',
)

# Transfer object code 60103 or 60402 or 60199 (transfers to non-financial entities)
_TRANSFER_CODE_RE = re.compile(r'\b6010[1-9]\b|\b604\d{2}\b|\b601\d{2}\b')

# Ministry section header — "Título" or "SECCIÓN" followed by MICIT/science
_TITLE_RE = re.compile(
    r'(?:T[IÍ]TULO|SECCI[OÓ]N)\s*[:\s]*(?:\d+\s*)?\n?'
    r'(?:[^\n]*\n){0,3}'
    r'(?:MINISTERIO|MICIT|MICITT)',
    re.IGNORECASE | re.DOTALL,
)


def _parse_crc(raw: str) -> Optional[float]:
    """Parse CRC amount string to float.

    Handles:
      "1.250.000.000" — dot-thousands (most common in CR)
      "1,250,000,000" — comma-thousands
      "1250000000"    — plain digits
    """
    raw = raw.strip()
    # Dot-thousands: "1.250.000.000"
    if re.match(r'^\d{1,3}(?:\.\d{3})+$', raw):
        return float(raw.replace('.', ''))
    # Comma-thousands: "1,250,000,000"
    if re.match(r'^\d{1,3}(?:,\d{3})+$', raw):
        return float(raw.replace(',', ''))
    # Plain digits
    cleaned = re.sub(r'[,\.]', '', raw)
    try:
        val = float(cleaned)
        return val if val > 0 else None
    except ValueError:
        return None


def _first_large_crc(text: str, min_val: float = 1_000_000) -> Optional[float]:
    """Return the first CRC amount >= min_val in the text window."""
    for m in _CRC_RE.finditer(text):
        val = _parse_crc(m.group(1))
        if val and val >= min_val:
            return val
    return None


def _extract_conicit_total(full_text: str) -> Optional[float]:
    """Extract CONICIT total appropriation by summing transfer lines.

    CONICIT appears as multiple transfer lines (operating budget + incentives fund).
    We extract the largest individual transfer, which is typically the incentives fund
    (~650M-1.25B CRC), or sum all transfers if clearly labelled.
    """
    total = 0.0
    found_transfers = []

    for m in _CONICIT_RE.finditer(full_text):
        # Look backwards for the amount (comes before the text in some formats)
        # and forwards for the amount (comes after in other formats)
        window_back = full_text[max(0, m.start() - 200): m.start()]
        window_fwd  = full_text[m.start(): m.start() + 600]

        # Check forward first (amount usually comes after institution name)
        val_fwd = _first_large_crc(window_fwd, min_val=1_000_000)
        val_bck = _first_large_crc(window_back, min_val=1_000_000)

        val = val_fwd or val_bck
        if val and val not in found_transfers:
            found_transfers.append(val)
            total += val

    if found_transfers:
        # Return sum if multiple transfers found, else just the one
        return total if total > 0 else None
    return None


def _extract_micit_total(full_text: str) -> Optional[float]:
    """Extract MICIT/MICITT ministry total appropriation.

    MICIT/MICITT appears as a Título (budget title) section header in the full
    budget law. Look for the ministry name as a section title followed by a
    programme total.

    Note: In many Costa Rican budget files, MICIT appears only in cross-references
    to enabling laws (e.g. "Ley 7169 MICIT"). Only extract when MICITT/MICIT
    appears as a proper budget section header, not a reference.
    """
    # Strategy: find MICIT as a section/título header (not as a law reference)
    # Típical format: "MINISTERIO DE CIENCIA, TECNOLOGÍA Y TELECOMUNICACIONES" as section
    for m in re.finditer(
        r'MINISTERIO\s+DE\s+CIENCIA[,\s]+TECNOLOG[IÍ]A\b',
        full_text, re.IGNORECASE,
    ):
        # Check it's a section header (appears on its own line, not inside parentheses)
        before = full_text[max(0, m.start()-5): m.start()]
        if '(' in before or 'LEY' in before.upper():
            continue  # Skip law references like "(Ley Nº 7169 MICIT)"

        window = full_text[m.start(): m.start() + 3000]
        # Look for a "Total" line or "Programa: {xxx}-00 Total" line
        for total_pat in [
            r'(?:Total\s+T[ií]tulo|TOTAL\s+PRESUPUESTO|Total\s+General)[^\n]*\n?\s*([\d\.,]{7,})',
            r'Total\s+(?:del\s+)?T[ií]tulo[^\n]*\n?\s*([\d\.,]{7,})',
        ]:
            tm = re.search(total_pat, window, re.IGNORECASE)
            if tm:
                val = _parse_crc(tm.group(1))
                if val and val >= 1_000_000:
                    return val

        # Fallback: first very large amount (>= 500M CRC = 0.5B) in the section
        val = _first_large_crc(window[:2000], min_val=500_000_000)
        if val:
            return val

    return None


# ── Public API ─────────────────────────────────────────────────────────────────

def extract_costa_rica_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract science budget records from a Costa Rican Ley de Presupuesto PDF.

    Extracts:
    - CR_CONICIT: CONICIT total appropriation (operating + incentives fund)
    - CR_MICIT:   Ministry of Science (MICIT/MICITT) total (if identifiable)

    All amounts are in CRC (Costa Rican colón), full values.
    Returns [] on failure.
    """
    records: list[dict] = []

    try:
        all_text = "\n".join(
            (row.text if isinstance(row.text, str) else "")
            for row in sorted_pages.itertuples(index=False)
        )
    except Exception as exc:
        logger.warning(
            "Costa Rica extractor: failed to read pages for %s: %s", source_filename, exc
        )
        return []

    if not all_text.strip():
        logger.debug("Costa Rica extractor: no text in %s.", source_filename)
        return []

    # ── 1. CONICIT ─────────────────────────────────────────────────────────────
    conicit_total = _extract_conicit_total(all_text)
    if conicit_total and conicit_total >= 1_000_000:
        records.append({
            "country": country,
            "year": year,
            "section_code": "CR_SCIENCE",
            "section_name": "Consejo Nacional de Investigaciones Científicas y Tecnológicas",
            "section_name_en": "National Council for Scientific and Technological Research",
            "program_code": "CR_CONICIT",
            "line_description": "CONICIT - presupuesto total (gastos operativos + fondo de incentivos)",
            "line_description_en": "CONICIT - total appropriation (operating + incentives fund)",
            "amount_local": conicit_total,
            "currency": "CRC",
            "unit": "CRC",
            "rd_category": "direct_rd",
            "taxonomy_score": 9.0,
            "decision": "include",
            "confidence": 0.85,
            "source_file": source_filename,
            "file_id": file_id,
            "page_number": 1,
        })

    # ── 2. MICIT/MICITT ────────────────────────────────────────────────────────
    micit_total = _extract_micit_total(all_text)
    if micit_total and micit_total >= 1_000_000:
        records.append({
            "country": country,
            "year": year,
            "section_code": "CR_SCIENCE",
            "section_name": "Ministerio de Ciencia, Tecnología y Telecomunicaciones",
            "section_name_en": "Ministry of Science, Technology and Telecommunications (MICITT)",
            "program_code": "CR_MICIT",
            "line_description": "MICIT/MICITT - transferencias y presupuesto total",
            "line_description_en": "Ministry of Science (MICIT/MICITT) - total transfers and budget",
            "amount_local": micit_total,
            "currency": "CRC",
            "unit": "CRC",
            "rd_category": "direct_rd",
            "taxonomy_score": 8.0,
            "decision": "include",
            "confidence": 0.75,
            "source_file": source_filename,
            "file_id": file_id,
            "page_number": 1,
        })

    if records:
        logger.info(
            "Costa Rica extractor: %s (year %s) → %d records",
            source_filename, year, len(records),
        )
    else:
        logger.debug(
            "Costa Rica extractor: no science budget found in %s (year %s).",
            source_filename, year,
        )

    return records
