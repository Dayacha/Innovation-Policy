"""Chilean Ley de Presupuestos extractor.

Chilean R&D spending tracked through:
- **CONICYT** (Comisión Nacional de Investigación Científica y Tecnológica)
  Location: Partida 09 (Ministerio de Educación), Capítulo 08
  Years: ~1976–2018
- **ANID** (Agencia Nacional de Investigación y Desarrollo)
  Location: Partida 30 (Ministerio de Ciencia, Tecnología, Conocimiento e Innovación)
  Years: 2019+

Document structure
------------------
Chilean Ley de Presupuestos uses:
  PARTIDA  : {nn}
  CAPITULO : {nn}
  PROGRAMA : {nn}
  {Entity Name}
  ...
  INGRESOS  {total}
  GASTOS    {total}

Amounts are in **miles de pesos** (thousands of CLP).
The "GASTOS" total at the entity level is the key figure.

For CONICYT (pre-2019): Partida 09, Capítulo 08
  - "Aporte Fiscal" (line 09) = direct government appropriation
  - Full GASTOS = total spending including transfers from other budgets

For ANID (2019+): Partida 30, within Ministerio de Ciencia
  - Sub-chapter within Partida 30 for ANID
  - "Aporte Fiscal" line 09 is cleanest R&D proxy

Older files (1976-2000): CONICYT may appear as a sub-chapter under different partidas.
For these, we capture the "Aporte Fiscal" or total GASTOS.
"""

from __future__ import annotations

import re
import logging
from typing import Optional

logger = logging.getLogger("innovation_pipeline")

# ── Entity patterns ────────────────────────────────────────────────────────────

# CONICYT — all name variations
_CONICYT_RE = re.compile(
    r"COMISI[ÓO]N\s+NACIONAL\s+DE\s+INVESTIGACI[ÓO]N\s+CIENT[IÍ]FICA\s+Y\s+TECNOL[ÓO]GICA"
    r"|CONICYT",
    re.IGNORECASE,
)

# ANID — post-2018
_ANID_RE = re.compile(
    r"AGENCIA\s+NACIONAL\s+DE\s+INVESTIGACI[ÓO]N\s+Y\s+DESARROLLO"
    r"|\bANID\b",
    re.IGNORECASE,
)

# Ministerio de Ciencia (Partida 30, 2019+)
_MIN_CIENCIA_RE = re.compile(
    r"MINISTERIO\s+DE\s+CIENCIA[,\s]+TECNOLOG[IÍ]A[,\s]+CONOCIMIENTO\s+E\s+INNOVACI[ÓO]N"
    r"|PARTIDA\s*:\s*30",
    re.IGNORECASE,
)

_CONICYT_HEADER_RE = re.compile(
    r"(?=.*PARTIDA\s*:?\s*09)"
    r"(?=.*CAP[IÍ]TULO\s*:?\s*08)"
    r"(?=.*PROGRAMA\s*:?\s*01)"
    r"(?=.*(?:COMISI[ÓO]N\s+NACIONAL\s+DE\s+INVESTIGACI[ÓO]N\s+CIENT[IÍ]FICA\s+Y\s+TECNOL[ÓO]GICA|CONICYT))",
    re.IGNORECASE | re.S,
)

_ANID_HEADER_RE = re.compile(
    r"(?=.*PARTIDA\s*:?\s*30)"
    r"(?=.*CAP[IÍ]TULO\s*:?\s*02)"
    r"(?=.*PROGRAMA\s*:?\s*01)"
    r"(?=.*(?:AGENCIA\s+NACIONAL\s+DE\s+INVESTIGACI[ÓO]N\s+Y\s+DESARROLLO|\bANID\b))",
    re.IGNORECASE | re.S,
)

_MIN_CIENCIA_SUMMARY_RE = re.compile(
    r"MINISTERIO\s+DE\s+CIENCIA[,\s]+TECNOLOG[IÍ]A[,\s]+CONOCIMIENTO\s+E\s+INNOVACI[ÓO]N",
    re.IGNORECASE,
)

# PARTIDA/CAPITULO headers
_PARTIDA_RE = re.compile(r"PARTIDA\s*:\s*(\d{1,2})\s*\nCAPITULO\s*:\s*(\d{1,2})", re.IGNORECASE)

# Summary table format (2013+): entity name on one line, amount on next line.
# Some years wrap the entity name across two lines before the amount:
#   "Comisión Nacional de Investigación Científica y \nTecnológica \n171.607.682"
# Allow up to 2 lines of text/whitespace between name start and amount.
# Summary table format: entity full name (may wrap across lines), then amount on next line.
# \s+ allows \n between words so the name matches even when line-wrapped.
# After the full name, look for optional trailing text then \n then the amount.
_SUMMARY_TABLE_CONICYT_RE = re.compile(
    r"Comisi[oó]n\s+Nacional\s+de\s+Investigaci[oó]n\s+Cient[ií]fica\s+y\s+Tecnol[oó]gica"
    r"\s*\n\s*([\d\.]{7,})",
    re.IGNORECASE,
)
_SUMMARY_TABLE_ANID_RE = re.compile(
    r"Agencia\s+Nacional\s+de\s+Investigaci[oó]n\s+y\s+Desarrollo"
    r"\s*\n\s*([\d\.]{7,})",
    re.IGNORECASE,
)
_SUMMARY_TABLE_MIN_CIENCIA_RE = re.compile(
    r"MINISTERIO\s+DE\s+CIENCIA(?:[,\s]+TECNOLOG[IÍ]A[^\n]*)?"
    r"\s*\n\s*([\d\.]{7,})",
    re.IGNORECASE,
)

# GASTOS and Aporte Fiscal lines — restricted to current line only (no newline crossing)
_GASTOS_RE = re.compile(r"\bGASTOS\b[\s\xa0:]*([\d\.]{4,}(?:[ \t]+[\d\.]{4,})*)", re.IGNORECASE)
_APORTE_FISCAL_RE = re.compile(r"APORTE\s+FISCAL\b[^\n]*([\d\.]{7,})", re.IGNORECASE)
_INGRESOS_RE = re.compile(r"\bINGRESOS\b[\s\xa0:]*([\d\.]{4,}(?:[ \t]+[\d\.]{4,})*)", re.IGNORECASE)

# Chilean peso amounts: dot-thousands "196.985.773" (no spaces, to avoid merging rows)
_CLP_RE = re.compile(r"(\d{1,3}(?:\.\d{3})+)", re.IGNORECASE)


def _parse_miles_clp(raw: str) -> Optional[float]:
    """Parse a Chilean peso amount in miles (thousands) to full CLP value.

    Handles dot-thousands: "196.985.773" → 196985773 * 1000
    Only accepts single dot-separated numbers (no multi-line merging).
    """
    raw = raw.strip()
    # Dot as thousands separator: "196.985.773" (must be 7+ digits to be plausible)
    if re.match(r"^\d{1,3}(?:\.\d{3})+$", raw):
        val = float(raw.replace(".", ""))
        return val * 1000  # miles → full CLP
    # Plain number (no separators)
    if re.match(r"^\d+$", raw):
        val = float(raw)
        return val * 1000 if val > 1000 else None
    return None


def _extract_gastos_from_section(section_text: str, take_last: bool = False) -> Optional[float]:
    """Extract the main GASTOS (expenditure) total from an entity section.

    Finds the first GASTOS line with a large amount.
    If take_last=True, returns the last (largest/rightmost) amount on the line
    instead of the first — useful for two-column summary tables where ANID
    appears as the second column.
    """
    for m in _GASTOS_RE.finditer(section_text[:500]):
        raw = m.group(1).strip()
        # Get all large numbers on this line
        vals = []
        for am in _CLP_RE.finditer(raw):
            val = _parse_miles_clp(am.group(1))
            if val and val >= 1_000_000_000:  # at least 1B CLP
                vals.append(val)
        if vals:
            return vals[-1] if take_last else vals[0]
    return None


def _extract_top_total(page_text: str, label_re: re.Pattern, take_last: bool = False) -> Optional[float]:
    """Extract the top-of-page total for INGRESOS/GASTOS from an anchored budget page."""
    head = page_text[:2200]
    for m in label_re.finditer(head):
        raw = m.group(1).strip()
        vals = []
        for am in _CLP_RE.finditer(raw):
            val = _parse_miles_clp(am.group(1))
            if val and 1_000_000_000 <= val <= 2_000_000_000_000:
                vals.append(val)
        if vals:
            return vals[-1] if take_last else vals[0]
    return None


def _page_candidates(sorted_pages, header_re: re.Pattern) -> list[tuple[int, str]]:
    candidates: list[tuple[int, str]] = []
    for row in sorted_pages.itertuples(index=False):
        text = row.text if isinstance(row.text, str) else ""
        if not text.strip():
            continue
        if header_re.search(text):
            candidates.append((int(row.page_number), text))
    return candidates


def _build_record(
    *,
    country: str,
    year: str,
    file_id: str,
    source_filename: str,
    page_number: int,
    section_name: str,
    section_name_en: str,
    program_code: str,
    line_description: str,
    line_description_en: str,
    amount_local: float,
    confidence: float,
    source_variant: str,
    context_text: str,
) -> dict:
    snippet = context_text[:2200].strip()
    return {
        "country": country,
        "year": year,
        "section_code": "CL_SCIENCE",
        "section_name": section_name,
        "section_name_en": section_name_en,
        "program_code": program_code,
        "line_description": line_description,
        "line_description_en": line_description_en,
        "program_description": line_description_en,
        "program_description_en": line_description_en,
        "amount_local": amount_local,
        "currency": "CLP",
        "unit": "CLP",
        "rd_category": "direct_rd",
        "taxonomy_score": 9.0,
        "decision": "include",
        "confidence": confidence,
        "source_file": source_filename,
        "file_id": file_id,
        "page_number": page_number,
        "source_variant": source_variant,
        "text_snippet": snippet[:700].replace("\n", " ").strip(),
        "raw_line": snippet,
        "merged_line": line_description,
        "context_before": snippet[:500].replace("\n", " ").strip(),
        "context_after": snippet[500:1400].replace("\n", " ").strip(),
        "parse_quality": "high" if source_variant == "anchored_detail" else "medium",
    }


def _extract_aporte_fiscal(section_text: str) -> Optional[float]:
    """Extract Aporte Fiscal (direct government funding) from section."""
    m = _APORTE_FISCAL_RE.search(section_text)
    if not m:
        return None
    raw = m.group(1).strip()
    for am in _CLP_RE.finditer(raw[:80]):
        val = _parse_miles_clp(am.group(1))
        if val and val >= 1_000_000_000:
            return val
    return None


# ── Public API ─────────────────────────────────────────────────────────────────

def extract_chile_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract R&D spending records from a Chilean Ley de Presupuestos PDF.

    Returns list of dicts matching the standard budget item schema.
    Amounts are in full CLP (Chilean pesos), converted from miles de pesos.
    """
    records: list[dict] = []

    all_text = "\n".join(
        (row.text if isinstance(row.text, str) else "")
        for row in sorted_pages.itertuples(index=False)
    )

    if not all_text.strip():
        return []

    year_int = int(year) if year.isdigit() else 2000

    # ── Anchored detail pages: preferred path for recoverable years ───────────
    conicyt_pages = _page_candidates(sorted_pages, _CONICYT_HEADER_RE)
    if conicyt_pages:
        page_number, page_text = conicyt_pages[0]
        gastos = _extract_top_total(page_text, _GASTOS_RE)
        if gastos:
            records.append(
                _build_record(
                    country=country,
                    year=year,
                    file_id=file_id,
                    source_filename=source_filename,
                    page_number=page_number,
                    section_name="CONICYT - Educación",
                    section_name_en="National Commission for Scientific Research (CONICYT)",
                    program_code="CL_CONICYT",
                    line_description="Comisión Nacional de Investigación Científica y Tecnológica (CONICYT)",
                    line_description_en="National Commission for Scientific Research (CONICYT)",
                    amount_local=gastos,
                    confidence=0.97,
                    source_variant="anchored_detail",
                    context_text=page_text,
                )
            )

    anid_pages = _page_candidates(sorted_pages, _ANID_HEADER_RE)
    if anid_pages:
        page_number, page_text = anid_pages[0]
        gastos = _extract_top_total(page_text, _GASTOS_RE)
        if gastos:
            records.append(
                _build_record(
                    country=country,
                    year=year,
                    file_id=file_id,
                    source_filename=source_filename,
                    page_number=page_number,
                    section_name="Ministerio de Ciencia, Tecnología, Conocimiento e Innovación",
                    section_name_en="Ministry of Science, Technology, Knowledge and Innovation",
                    program_code="CL_ANID",
                    line_description="Agencia Nacional de Investigación y Desarrollo (ANID)",
                    line_description_en="National Research and Development Agency (ANID)",
                    amount_local=gastos,
                    confidence=0.97,
                    source_variant="anchored_detail",
                    context_text=page_text,
                )
            )

    # ── Summary table format (2013+): entity name then amount on next line ───
    # e.g. "Comisión Nacional de Investigación...\n267.254.295"
    def _try_summary_table(name_re, program_code, section_name, section_name_en, line_desc, line_desc_en, min_amt=1_000_000_000):
        m = name_re.search(all_text)
        if not m:
            return None
        raw = m.group(1).strip()
        val = _parse_miles_clp(raw)
        if val and min_amt <= val <= 2_000_000_000_000:
            return {
                "country": country, "year": year,
                "section_code": "CL_SCIENCE",
                "section_name": section_name,
                "section_name_en": section_name_en,
                "program_code": program_code,
                "line_description": line_desc,
                "line_description_en": line_desc_en,
                "amount_local": val,
                "currency": "CLP", "unit": "CLP",
                "rd_category": "direct_rd", "taxonomy_score": 9.0,
                "decision": "include", "confidence": 0.90,
                "source_file": source_filename, "file_id": file_id, "page_number": 1,
            }
        return None

    # CONICYT from summary table (pre-2019), only if no anchored detail page was found.
    if not any(r["program_code"] == "CL_CONICYT" for r in records):
        rec = _try_summary_table(
            _SUMMARY_TABLE_CONICYT_RE, "CL_CONICYT",
            "CONICYT - Educación", "National Commission for Scientific Research (CONICYT)",
            "Comisión Nacional de Investigación Científica y Tecnológica (CONICYT)",
            "National Commission for Scientific Research (CONICYT)",
        )
        if rec:
            records.append(rec)

    # ANID from summary table (2019+)
    if not any(r["program_code"] == "CL_ANID" for r in records):
        rec = _try_summary_table(
            _SUMMARY_TABLE_ANID_RE, "CL_ANID",
            "Ministerio de Ciencia, Tecnología, Conocimiento e Innovación",
            "Ministry of Science, Technology, Knowledge and Innovation",
            "Agencia Nacional de Investigación y Desarrollo (ANID)",
            "National Research and Development Agency (ANID)",
        )
        if rec:
            records.append(rec)

    # Ministry of Science total from summary table
    rec = _try_summary_table(
        _SUMMARY_TABLE_MIN_CIENCIA_RE, "CL_MIN_CIENCIA",
        "Ministerio de Ciencia, Tecnología, Conocimiento e Innovación",
        "Ministry of Science, Technology, Knowledge and Innovation",
        "Ministerio de Ciencia, Tecnología, Conocimiento e Innovación (total)",
        "Ministry of Science (total)",
        min_amt=5_000_000_000,
    )
    if rec and not any(r["program_code"] == "CL_MIN_CIENCIA" for r in records):
        rec["source_variant"] = "summary_table"
        rec["parse_quality"] = "medium"
        records.append(rec)

    if records:
        logger.info("Chile extractor (summary table): %s (year %s) → %d records", source_filename, year, len(records))
        return records

    # ── Modern format: ANID (2019+) ──────────────────────────────────────────
    if year_int >= 2019 or _ANID_RE.search(all_text):
        # Find ANID section within Partida 30
        for m in _ANID_RE.finditer(all_text):
            section = all_text[m.start(): m.start() + 3000]
            gastos = _extract_gastos_from_section(section, take_last=True)
            if not gastos:
                gastos = _extract_aporte_fiscal(section)
            # Sanity: ANID budget should be well under 2 trillion CLP
            if gastos and gastos > 2_000_000_000_000:
                gastos = None
            if gastos and gastos >= 1_000_000_000:
                records.append({
                    "country": country,
                    "year": year,
                    "section_code": "CL_SCIENCE",
                    "section_name": "Ministerio de Ciencia, Tecnología, Conocimiento e Innovación",
                    "section_name_en": "Ministry of Science, Technology, Knowledge and Innovation",
                    "program_code": "CL_ANID",
                    "line_description": "Agencia Nacional de Investigación y Desarrollo (ANID)",
                    "line_description_en": "National Research and Development Agency (ANID)",
                    "amount_local": gastos,
                    "currency": "CLP",
                    "unit": "CLP",
                    "rd_category": "direct_rd",
                    "taxonomy_score": 9.0,
                    "decision": "include",
                    "confidence": 0.90,
                    "source_file": source_filename,
                    "file_id": file_id,
                    "page_number": 1,
                })
                break

        # Also add Ministerio total (Ministry of Science includes Subsecretaría + ANID)
        if _MIN_CIENCIA_RE.search(all_text):
            for m in _MIN_CIENCIA_RE.finditer(all_text):
                section = all_text[m.start(): m.start() + 5000]
                gastos = _extract_gastos_from_section(section)
                if gastos and gastos >= 5_000_000_000:  # > 5B CLP
                    # Only add ministry total if ANID not already captured
                    if not any(r["program_code"] == "CL_MIN_CIENCIA" for r in records):
                        records.append({
                            "country": country,
                            "year": year,
                            "section_code": "CL_SCIENCE",
                            "section_name": "Ministerio de Ciencia, Tecnología, Conocimiento e Innovación",
                            "section_name_en": "Ministry of Science, Technology, Knowledge and Innovation",
                            "program_code": "CL_MIN_CIENCIA",
                            "line_description": "Ministerio de Ciencia, Tecnología, Conocimiento e Innovación (total)",
                            "line_description_en": "Ministry of Science (total)",
                            "amount_local": gastos,
                            "currency": "CLP",
                            "unit": "CLP",
                            "rd_category": "direct_rd",
                            "taxonomy_score": 8.5,
                            "decision": "include",
                            "confidence": 0.85,
                            "source_file": source_filename,
                            "file_id": file_id,
                            "page_number": 1,
                        })
                    break

    # ── Classic format: CONICYT under Education (pre-2019) ───────────────────
    if year_int < 2019 or not records:
        for m in _CONICYT_RE.finditer(all_text):
            section = all_text[m.start(): m.start() + 4000]
            # Skip TOC entries (short context without GASTOS)
            if not re.search(r'GASTOS|INGRESOS|Aporte\s+Fiscal', section[:1500], re.IGNORECASE):
                continue
            gastos = _extract_gastos_from_section(section)
            if not gastos:
                gastos = _extract_aporte_fiscal(section)
            if gastos and gastos >= 1_000_000_000:
                records.append({
                    "country": country,
                    "year": year,
                    "section_code": "CL_SCIENCE",
                    "section_name": "CONICYT - Educación",
                    "section_name_en": "National Commission for Scientific Research (CONICYT)",
                    "program_code": "CL_CONICYT",
                    "line_description": "Comisión Nacional de Investigación Científica y Tecnológica (CONICYT)",
                    "line_description_en": "National Commission for Scientific Research (CONICYT)",
                    "amount_local": gastos,
                    "currency": "CLP",
                    "unit": "CLP",
                    "rd_category": "direct_rd",
                    "taxonomy_score": 9.0,
                    "decision": "include",
                    "confidence": 0.90,
                    "source_file": source_filename,
                    "file_id": file_id,
                    "page_number": 1,
                })
                break

    if records:
        logger.info("Chile extractor: %s (year %s) → %d records", source_filename, year, len(records))
    else:
        logger.debug("Chile extractor: no R&D agency found in %s (year %s).", source_filename, year)

    return records
