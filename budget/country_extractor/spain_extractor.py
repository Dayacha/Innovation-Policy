"""Spain Finance Bill (BOE / Presupuestos Generales del Estado) extractor.

All programs under Función 46 (Investigación científica, técnica y aplicada)
are R&D by the Spanish budget classification — no taxonomy scoring needed.

=== Actual PDF structure (Resumen por Programas / Clasif. por programas) ===

Each program entry occupies exactly 5 lines in the extracted text:

    462M                                   ← program code
    Investigación y estudios sociológicos  ← description
    14.537,16                              ← Cap. 1-8 (operating + transfers)
                                           ← Cap. 9  (financial, blank if zero)
    14.537,16                              ← Total (= Cap 1-8 + Cap 9)

Amounts are in miles de euros (year ≥ 2002) or millones de pesetas (year < 2002).
We always extract the TOTAL column (line index + 4 relative to code line) and
scale it to full EUR / ESP values.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from budget.utils import logger

# ── Patterns ──────────────────────────────────────────────────────────────────

# Program code under Función 46 (2005+): 3-digit code starting with 46 + letter
# e.g.  462M  463A  463B  464A  464B  465A  466A  467A … 467I
# Also 460A-E (COVID recovery) and 461M (2023+)
_RE_CODE_46 = re.compile(r"^46[0-9][A-Z](?:\.\d{2,3})?$", re.IGNORECASE)

# Old pre-2005 R&D program codes: 54x-Y (Función 54 = Investigación)
# e.g.  541-A  542-B  542-M  543-A
# Code may be alone on a line OR followed by the description on the same line
_RE_CODE_54X = re.compile(r"^(54[0-9]-[A-Z])\s*(.*)", re.IGNORECASE)
_RE_CODE_54X_STANDALONE = re.compile(r"^54[0-9]-[A-Z]$", re.IGNORECASE)

# Spanish number: dots as thousands separator, optional comma decimal
# e.g.  1.234.567,89   or   14.537,16   or   983.655,59
_RE_ESP_NUM = re.compile(r"^-?\d{1,3}(?:\.\d{3})*(?:,\d{1,2})?$")

# Loose amount check: also allow numbers without thousands sep (older budgets)
# e.g.  12345  or  123.456  (ambiguous with years, so require ≥5 digits OR dot+3)
_RE_LOOSE_NUM = re.compile(r"^\d{4,9}(?:[.,]\d{1,2})?$|^\d{1,6}\.\d{3}(?:,\d{1,2})?$")

# Function 46 header (to confirm we're in the right section; not strictly needed
# since we match by code prefix, but useful for diagnostics)
_RE_FUNCION_46 = re.compile(r"funci[oó]n[\s.:]*46\b", re.IGNORECASE)
_RE_FUNCION_46_TOTAL = re.compile(
    r"Investigaci[oó]n\s+Cient[ií]fica,\s*T[eé]cnica\s+y\s+Aplicada"
    r"[.\s]+(?P<amount>\d{1,3}(?:\.\d{3})*(?:,\d{1,2})?)",
    re.IGNORECASE,
)

# Pre-2003 name-based R&D program detection (no codes in these PDFs)
# These descriptions are stable across 1990-2002 BOE editions
_RD_PROGRAM_NAMES: list[tuple[str, str]] = [
    # (regex pattern, synthetic code)
    (r"investigaci[oó]n\s+cient[ií]fica$", "541A"),
    (r"astronom[ií]a\s+y\s+astrof[ií]sica", "541B"),
    (r"investigaci[oó]n\s+y\s+estudios\s+sociol[oó]g", "542B"),
    (r"investigaci[oó]n\s+y\s+estudios\s+de\s+las\s+fuerzas\s+armadas", "542C"),
    (r"investigaci[oó]n\s+y\s+experimentaci[oó]n\s+de?\s+obras\s+p[uú]blicas", "542D"),
    (r"investigaci[oó]n\s+y\s+desarrollo\s+tecnol[oó]gico$", "542E"),
    (r"investigaci[oó]n\s+y\s+evaluaci[oó]n\s+educativa", "542G"),
    (r"investigaci[oó]n\s+sanitaria", "542H"),
    (r"investigaci[oó]n\s+y\s+estudios\s+estad[ií]sticos", "542I"),
    (r"investigaci[oó]n\s+y\s+experimentaci[oó]n\s+agraria(?:\s+y\s+pesquera)?", "542J"),
    (r"investigaci[oó]n\s+oceanogr[aá]fica\s+y\s+pesquera", "542K"),
    (r"investigaci[oó]n\s+geol[oó]gico.minera", "542L"),
    (r"fomento\s+y\s+coordinaci[oó]n\s+de\s+la\s+investigaci[oó]n", "542M"),
    (r"investigaci[oó]n\s+y\s+desarrollo\s+de\s+la\s+sociedad\s+de\s+la\s+informaci[oó]n", "542N"),
    (r"investigaci[oó]n\s+energ[eé]tica", "542P"),
    (r"direcci[oó]n\s+y\s+servicios\s+generales\s+de\s+ciencia", "543A"),
    (r"innovaci[oó]n\s+tecnol[oó]gica\s+de\s+las\s+telecomunicaciones", "542Q"),
    (r"investigaci[oó]n\s+y\s+desarrollo\s+de\s+las\s+fuerzas\s+armadas", "542C2"),
    (r"investigaci[oó]n\s+y\s+estudios\s+estad[ií]sticos\s+y\s+econ[oó]micos", "542I"),
    (r"investigaci[oó]n\s+t[eé]cnica$", "541C"),
]

_RE_RD_PROGRAMS = [
    (re.compile(pat, re.IGNORECASE), code)
    for pat, code in _RD_PROGRAM_NAMES
]

# Dash used as "zero" in pre-2003 Cap 9 column
_RE_DASH = re.compile(r"^[–—\-]+$")

# ── Helpers ───────────────────────────────────────────────────────────────────


def _is_amount_line(text: str, strict: bool = False) -> bool:
    """Return True if text looks like a numeric amount.

    strict=True requires a well-formed Spanish number (used for the Total column).
    strict=False also allows OCR-corrupted amounts like "65.749,.63" (Cap 1-8).
    """
    if not text:
        return False
    if _RE_ESP_NUM.match(text) or _RE_LOOSE_NUM.match(text):
        return True
    if strict:
        return False
    # Lenient: text is composed mostly of digits, dots, commas, and a leading minus
    cleaned = text.lstrip("-")
    return bool(cleaned) and all(c in "0123456789.," for c in cleaned)


def _parse_esp_number(text: str) -> float:
    """Parse Spanish number format to float.

    '1.234.567,89' → 1234567.89
    '14.537,16'    → 14537.16
    '-234.567'     → -234567.0
    """
    t = text.strip()
    negative = t.startswith("-")
    t = t.lstrip("-").replace(".", "").replace(",", ".")
    try:
        val = float(t)
        return -val if negative else val
    except ValueError:
        return 0.0


def _currency_for_year(year: str) -> str:
    """ESP (pesetas) before 2002, EUR after."""
    try:
        return "ESP" if int(year) < 2002 else "EUR"
    except (ValueError, TypeError):
        return "EUR"


def _source_unit_label(year: str) -> str:
    try:
        y = int(year)
    except (ValueError, TypeError):
        return "miles de euros"
    return "miles de pesetas" if y < 2002 else "miles de euros"


def _scale_amount(total_val: float) -> float:
    """Convert source-table amounts to full currency units."""
    return total_val * 1000.0


def _summary_scale_factor(page_text: str) -> int:
    lowered = page_text.lower()
    if "miles de euros" in lowered or "miles de pesetas" in lowered:
        return 1000
    return 1


def _build_line_context(lines: list[str], line_idx: int, raw_total: str) -> tuple[str, str, str, str]:
    lo = max(0, line_idx - 1)
    hi = min(len(lines), line_idx + 5)
    block = [ln.strip() for ln in lines[lo:hi]]
    merged = "\n".join(ln for ln in block if ln)
    raw_line = lines[line_idx].strip() if 0 <= line_idx < len(lines) else ""
    before = "\n".join(ln.strip() for ln in lines[max(0, line_idx - 2):line_idx] if ln.strip())
    after = "\n".join(ln.strip() for ln in lines[line_idx + 1:min(len(lines), line_idx + 4)] if ln.strip())
    if raw_total and raw_total not in merged:
        merged = f"{merged}\n{raw_total}".strip()
    return before, raw_line, after, merged


def _find_function_total(page_text: str) -> tuple[float, str, int] | None:
    match = _RE_FUNCION_46_TOTAL.search(page_text)
    if not match:
        return None
    amount_raw = match.group("amount")
    amount_val = _parse_esp_number(amount_raw)
    if amount_val <= 0:
        return None
    line_idx = max(0, page_text[:match.start()].count("\n"))
    return amount_val, amount_raw, line_idx


# ── State-machine parser ──────────────────────────────────────────────────────


def _parse_program_blocks(lines: list[str]) -> list[tuple[str, str, float, str, int]]:
    """Parse program blocks from page lines using an adaptive state machine.

    Returns list of (code, description, total_value, raw_total, line_index).

    The block structure varies by year:

    2009 format (3 amount lines, blank for Cap 9 = 0):
        [i+0]  code        e.g. "462M"
        [i+1]  description e.g. "Investigación científica"
        [i+2]  Cap 1-8     e.g. "975.729,86"
        [i+3]  Cap 9       e.g. "" (blank when zero) or "7.925,73"
        [i+4]  Total       e.g. "983.655,59"

    2023 format (2 amount lines, no blank):
        [i+0]  code        e.g. "462M"
        [i+1]  description (may wrap to [i+2])
        [i+2 or +3]  Cap 1-8 (= Total when Cap 9 = 0)
        [i+3 or +4]  Total

    Strategy: after the code line, collect description lines (non-amount,
    non-code) then amount lines (amount or blank) until the next code.
    The LAST non-empty amount is the Total column.
    """
    results = []
    i = 0
    while i < len(lines):
        code_raw = lines[i].strip()

        # Try modern 46x code (standalone line)
        desc_inline = ""
        if _RE_CODE_46.match(code_raw):
            code = code_raw.upper()
        else:
            # Try old pre-2005 54x-Y code (may have description inline)
            m54 = _RE_CODE_54X.match(code_raw)
            if not m54:
                i += 1
                continue
            code = m54.group(1).upper()
            desc_inline = m54.group(2).strip()

        desc_parts: list[str] = [desc_inline] if desc_inline else []
        amounts: list[str] = []
        in_amounts = False

        j = i + 1
        while j < min(i + 12, len(lines)):
            ln = lines[j].strip()

            # Next 46x or 54x code → end of block
            if (_RE_CODE_46.match(ln) or _RE_CODE_54X_STANDALONE.match(ln)
                    or _RE_CODE_54X.match(ln)) and j > i + 1:
                break

            if not in_amounts and not _is_amount_line(ln) and ln != "":
                # Still in description territory
                desc_parts.append(ln)
            elif in_amounts and not _is_amount_line(ln) and ln != "":
                # Non-amount text after amounts = end of block (e.g. next program
                # code from a different Función, or a label row)
                break
            else:
                # Amount territory: collect amounts and blanks
                in_amounts = True
                amounts.append(ln)

            j += 1

        # Total = last non-empty amount
        non_empty_amounts = [a for a in amounts if a]
        if non_empty_amounts:
            raw_total = non_empty_amounts[-1]
            if _is_amount_line(raw_total, strict=True):
                total_val = _parse_esp_number(raw_total)
                if total_val > 0:
                    desc = " ".join(desc_parts).strip().rstrip(".")
                    results.append((code, desc, total_val, raw_total, i))

        i = j

    return results


def _parse_name_blocks(lines: list[str]) -> list[tuple[str, str, float, str, int]]:
    """Parse R&D programs by description name (pre-2003 format, no codes).

    Structure per block (4 lines):
        [i+0]  description  e.g. "Investigación científica"
        [i+1]  Cap 1-8      e.g. "385.688,81"
        [i+2]  Cap 9        e.g. "1,89"  (or "–")
        [i+3]  Total        e.g. "385.690,70"
    """
    results = []
    for i, raw_line in enumerate(lines):
        desc = raw_line.strip()
        desc_norm = desc.rstrip(" .;:")

        # Match against known R&D program names
        code = None
        for regex, synthetic_code in _RE_RD_PROGRAMS:
            if regex.search(desc_norm):
                code = synthetic_code
                break
        if code is None:
            continue

        # Extract total: lines[i+3] or lines[i+4] (desc may wrap)
        for offset in (3, 4):
            if i + offset >= len(lines):
                continue
            total_raw = lines[i + offset].strip()
            # Skip dashes (Cap 9 = 0)
            if _RE_DASH.match(total_raw):
                continue
            if _is_amount_line(total_raw, strict=True):
                total_val = _parse_esp_number(total_raw)
                if total_val > 0:
                    results.append((code, desc, total_val, total_raw, i))
                    break

    return results


# ── Main extraction function ──────────────────────────────────────────────────


def extract_spain_items(
    sorted_pages: pd.DataFrame,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract Función 46 R&D items from Spain Finance Bill pages.

    Parameters
    ----------
    sorted_pages : DataFrame with columns [page_number, text, ...]
    file_id, country, year, source_filename : metadata

    Returns
    -------
    List of dicts matching the standard budget item schema used by
    budget_extractor.extract_budget_items().
    """
    currency = _currency_for_year(year)
    source_unit = _source_unit_label(year)
    records: list[dict] = []
    seen_codes: set[str] = set()

    # ── Collect all pages ────────────────────────────────────────────────────
    all_texts: dict[int, str] = {}
    for row in sorted_pages.itertuples(index=False):
        pg = int(row.page_number)
        text = row.text if isinstance(row.text, str) else ""
        all_texts[pg] = text

    # ── Scan every page for 46x or old 54x-Y program codes ──────────────────
    # (The Resumen por Programas is usually 1-2 pages but we scan all to be safe)
    def _has_rd_code(lines: list[str]) -> bool:
        return any(
            _RE_CODE_46.match(ln.strip()) or _RE_CODE_54X.match(ln.strip())
            for ln in lines
        )

    pages_with_matches = []
    for pg in sorted(all_texts.keys()):
        lines = all_texts[pg].splitlines()
        if _has_rd_code(lines):
            pages_with_matches.append(pg)

    # ── Fallback: name-based scan for pre-2003 PDFs (no codes) ───────────────
    use_name_scanner = not pages_with_matches
    if use_name_scanner:
        pages_with_matches = []
        for pg in sorted(all_texts.keys()):
            lines = all_texts[pg].splitlines()
            if any(regex.search(ln.strip()) for ln in lines for regex, _ in _RE_RD_PROGRAMS):
                pages_with_matches.append(pg)

    function_total_hits: list[tuple[int, float, str, int]] = []
    if not pages_with_matches:
        for pg in sorted(all_texts.keys()):
            hit = _find_function_total(all_texts[pg])
            if hit:
                total_val, raw_total, line_idx = hit
                function_total_hits.append((pg, total_val, raw_total, line_idx))

    if not pages_with_matches and not function_total_hits:
        logger.warning(
            "Spain extractor: no R&D program codes, names, or Función 46 summary found in %s (year %s).",
            source_filename, year,
        )
        return []

    logger.info(
        "Spain extractor: %s (year %s) — scanning %d pages (%s)",
        source_filename, year, len(pages_with_matches) if pages_with_matches else len(function_total_hits),
        "function-total" if function_total_hits and not pages_with_matches else "name-based" if use_name_scanner else "code-based",
    )

    # ── Extract program blocks ────────────────────────────────────────────────
    for pg in pages_with_matches:
        lines = all_texts[pg].splitlines()
        blocks = _parse_name_blocks(lines) if use_name_scanner else _parse_program_blocks(lines)

        for code, desc, total_val, raw_total, line_idx in blocks:
            if code in seen_codes:
                logger.debug("Spain: skipping duplicate %s (already seen)", code)
                continue
            seen_codes.add(code)
            before, raw_line, after, merged = _build_line_context(lines, line_idx, raw_total)
            scaled_val = _scale_amount(total_val)

            records.append({
                # ── Time-series key ──────────────────────────────
                "country": country,
                "year": year,
                # ── Budget structure ─────────────────────────────
                "section_code": "46",
                "section_name": "Función 46: Investigación científica, técnica y aplicada",
                "section_name_en": "Function 46: Scientific, Technical and Applied Research",
                "program_code": code,
                "program_description": desc,
                "program_description_en": "",
                "budget_type": "total",
                "item_code": code,
                "item_description": desc,
                "line_code": code,
                "line_description": f"{code} {desc}".strip(),
                "line_description_en": "",
                # ── Amount ───────────────────────────────────────
                "amount_local": scaled_val,
                "currency": currency,
                "amount_raw": raw_total,
                "unit": currency,
                # ── Classification ───────────────────────────────
                "rd_category": "direct_rd",
                "pillar": "Direct R&D",
                "rd_label": "Direct R&D",
                "taxonomy_score": 10.0,
                "smoothed_taxonomy_score": 10.0,
                "content_score": 10.0,
                "context_score": 10.0,
                "taxonomy_hits": "Función 46 (Spain R&D classification)",
                "decision": "include",
                "confidence": 0.99,
                "parse_error": False,
                "temporal_prior_boost": 0,
                "temporal_prior_match_type": "",
                "temporal_prior_years": "",
                "rationale": f"Función 46 program; source_unit={source_unit}; scaled_to_full_{currency}; page={pg}; line={line_idx}",
                # ── Provenance ───────────────────────────────────
                "source_file": source_filename,
                "page_number": pg,
                "file_id": file_id,
                # ── Legacy aliases ───────────────────────────────
                "file_label": f"Spain {year}",
                "source_filename": source_filename,
                "keywords_matched": "Función 46",
                "text_snippet": merged,
                "text_snippet_en": "",
                "detected_amount_raw": raw_total,
                "detected_amount_value": scaled_val,
                "detected_currency": currency,
                "is_header_total": False,
                "is_program_level": True,
                "raw_line": raw_line,
                "merged_line": merged,
                "context_before": before,
                "context_after": after,
                "source_variant": "program_name_block" if use_name_scanner else "program_block",
            })

    if function_total_hits and not records:
        best_pg, best_val, best_raw, best_line_idx = max(function_total_hits, key=lambda item: item[1])
        page_text = all_texts[best_pg]
        lines = page_text.splitlines()
        before, raw_line, after, merged = _build_line_context(lines, best_line_idx, best_raw)
        scale_factor = _summary_scale_factor(page_text)
        scaled_val = best_val * scale_factor
        summary_unit = source_unit if scale_factor == 1000 else currency
        records.append({
            "country": country,
            "year": year,
            "section_code": "46",
            "section_name": "Función 46: Investigación científica, técnica y aplicada",
            "section_name_en": "Function 46: Scientific, Technical and Applied Research",
            "program_code": "FUNC46_TOTAL",
            "program_description": "Investigación científica, técnica y aplicada",
            "program_description_en": "Scientific, technical and applied research",
            "budget_type": "function_total",
            "item_code": "FUNC46_TOTAL",
            "item_description": "Función 46 total",
            "line_code": "FUNC46_TOTAL",
            "line_description": "Función 46 total",
            "line_description_en": "Function 46 total",
            "amount_local": scaled_val,
            "currency": currency,
            "amount_raw": best_raw,
            "unit": currency,
            "rd_category": "direct_rd",
            "pillar": "Direct R&D",
            "rd_label": "Direct R&D",
            "taxonomy_score": 9.5,
            "smoothed_taxonomy_score": 9.5,
            "content_score": 9.5,
            "context_score": 9.0,
            "taxonomy_hits": "Función 46 summary total (Spain R&D classification)",
            "decision": "include",
            "confidence": 0.9,
            "parse_error": False,
            "temporal_prior_boost": 0,
            "temporal_prior_match_type": "",
            "temporal_prior_years": "",
            "rationale": f"Función 46 summary total; source_unit={summary_unit}; scale_factor={scale_factor}; page={best_pg}; line={best_line_idx}",
            "source_file": source_filename,
            "page_number": best_pg,
            "file_id": file_id,
            "file_label": f"Spain {year}",
            "source_filename": source_filename,
            "keywords_matched": "Función 46 summary",
            "text_snippet": merged,
            "text_snippet_en": "",
            "detected_amount_raw": best_raw,
            "detected_amount_value": scaled_val,
            "detected_currency": currency,
            "is_header_total": True,
            "is_program_level": False,
            "raw_line": raw_line,
            "merged_line": merged,
            "context_before": before,
            "context_after": after,
            "source_variant": "function_total_summary",
        })

    logger.info(
        "Spain extractor: %s (year %s) → %d programs, total = %s %s %s",
        source_filename, year, len(records),
        f"{sum(r['amount_local'] for r in records):,.2f}", currency, currency,
    )

    return records
