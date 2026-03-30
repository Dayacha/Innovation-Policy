"""German Federal Budget (Bundeshaushaltsplan) extractor.

German R&D spending is tracked through the Bundeshaushaltsplan summary tables,
primarily in the "Gesamtplan Teil I: Haushaltsübersicht" section.

Key Einzelpläne (budget chapters) tracked
------------------------------------------
- **Einzelplan 30** (through 1994): "Bundesminister(ium) für Forschung und
  Technologie" (BMFT)
- **Einzelplan 31** (through 1994): "Bundesminister(ium) für Bildung und
  Wissenschaft" (BMBW)
- **Einzelplan 30** (from 1995 onward): merged education/research ministry,
  appearing as "Bundesministerium für Bildung und Forschung" or transitional
  variants such as "Bundesministerium für Bildung, Wissenschaft, Forschung und
  Technologie"

Document formats handled
------------------------
1. **Gesamtplan Teil I: Haushaltsübersicht / Ausgaben** (primary method)
   The expenditure summary is usually split across two consecutive pages:

   Page A:
     Epl.  Bezeichnung  Personalausgaben ...
       30  Bundesminister für Forschung und Technologie
       31  Bundesminister für Bildung und Wissenschaft

   Page B:
     ... Zuweisungen | Investitionen | Besondere Finanzierungsausgaben |
         Summe Ausgaben | Vorjahr | Veränderung | Epl.

   The extractor pairs both pages, matches the row order, and reads the amount
   closest to the "Summe Ausgaben" column.

2. **Name-based fallback**
   If the paired summary table is not recoverable, search for ministry-name
   variants in the full text and take the most plausible nearby total.

3. **Einzelplan title page fallback**
   Search individual pages for "Einzelplan 30/31" headings combined with ministry
   names and total-expenditure wording.

Currency and units
------------------
- Until 2001: DEM (Deutsche Mark), amounts in 1 000 DM → multiply by 1 000
- From 2002:  EUR (Euro),          amounts in 1 000 EUR → multiply by 1 000
"""

from __future__ import annotations

import logging
import re
from typing import Optional

logger = logging.getLogger("innovation_pipeline")

# ── Regex constants ────────────────────────────────────────────────────────────

_HAUSHALTSUEBERSICHT_RE = re.compile(r"Haushalts[uü]bersicht", re.IGNORECASE)
_AUSGABEN_RE = re.compile(r"\bAusgaben\b", re.IGNORECASE)
_EPL_HEADER_RE = re.compile(r"\bEpl\.\s+Bezeichnung\b", re.IGNORECASE)
_SUMME_AUSGABEN_RE = re.compile(r"Summe\s+Ausgaben", re.IGNORECASE)
_EINZELPLAN_TITLE_RE = re.compile(r"Einzelplan\s+(30|31)\b", re.IGNORECASE)
_TOTAL_LINE_RE = re.compile(
    r"Gesamtausgaben|Summe\s+(?:der\s+)?Ausgaben|Ausgaben\s+insgesamt|insgesamt",
    re.IGNORECASE,
)

_ROW_START_RE = re.compile(r"^\s*(\d{2})\s{2,}(.*\S)?$")
_ROW_END_EPL_RE = re.compile(r"\b(30|31)\s*$")
_AMOUNT_RE = re.compile(r"\d{1,3}(?:[ \.]\d{3})+(?:,\d+)?|\d{4,}(?:,\d+)?")

_BMFT_RE = re.compile(
    r"Forschung\s+und\s+Technologie",
    re.IGNORECASE,
)
_BMBW_RE = re.compile(
    r"Bildung\s+und\s+Wissenschaft",
    re.IGNORECASE,
)
_BMBF_RE = re.compile(
    r"Bildung\s+und\s+Forschung"
    r"|Bildung,\s*Wissenschaft,\s*Forschung\s+und\s+Technologie"
    r"|BMBF\b",
    re.IGNORECASE,
)

_PAGE_NOISE_RE = re.compile(
    r"^(?:Teil\s+I:|Gesamtplan|Zu\s+Spalte|gegen[uü]ber|mehr\s*\(\+\)|weniger\s*\(-\)|"
    r"Summe\s+Haushalt|Drucksache|Deutscher\s+Bundestag)",
    re.IGNORECASE,
)

_MIN_TOTAL_AMOUNT = 50_000_000
_ABSOLUTE_MAX_TOTAL = 100_000_000_000


# ── Amount parsing ─────────────────────────────────────────────────────────────

def _parse_thousand_amount(raw: str) -> Optional[float]:
    """Parse a 1,000-unit DEM/EUR amount into a full-value float."""
    cleaned = raw.replace("\xa0", " ").strip()
    cleaned = cleaned.split(",", 1)[0]
    cleaned = cleaned.replace(" ", "").replace(".", "")
    if not re.fullmatch(r"\d+", cleaned):
        return None
    try:
        value = float(cleaned) * 1000
    except ValueError:
        return None
    return value if value > 0 else None


def _amount_matches(line: str) -> list[tuple[float, int, int, str]]:
    """Return parsed amounts with source positions for a line of text."""
    values: list[tuple[float, int, int, str]] = []
    for match in _AMOUNT_RE.finditer(line):
        value = _parse_thousand_amount(match.group(0))
        if value is None:
            continue
        values.append((value, match.start(), match.end(), match.group(0)))
    return values


def _max_total_for_year(year_int: int) -> float:
    """Return a conservative ministry-budget ceiling for the given year."""
    if year_int < 1995:
        return 20_000_000_000
    if year_int < 2002:
        return 25_000_000_000
    return 30_000_000_000


def _pick_amount_near_column(
    line: str,
    target_center: Optional[float],
    *,
    year_int: int,
) -> Optional[float]:
    """Pick the amount nearest the Summe-Ausgaben column center."""
    matches = [
        item for item in _amount_matches(line)
        if _MIN_TOTAL_AMOUNT <= item[0] <= min(_ABSOLUTE_MAX_TOTAL, _max_total_for_year(year_int))
    ]
    if not matches:
        return None
    if target_center is None:
        return max(matches, key=lambda item: item[0])[0]
    best = min(matches, key=lambda item: abs(((item[1] + item[2]) / 2.0) - target_center))
    return best[0]


# ── Row parsing helpers ───────────────────────────────────────────────────────

def _normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _parse_left_rows(page_text: str) -> list[tuple[int, str]]:
    """Parse the left half of the Ausgaben table: Epl code + ministry name."""
    rows: list[tuple[int, str]] = []
    current_code: Optional[int] = None
    current_parts: list[str] = []
    in_table = False

    for raw_line in page_text.splitlines():
        line = raw_line.rstrip()
        if not in_table and _EPL_HEADER_RE.search(line):
            in_table = True
            continue
        if not in_table:
            continue

        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("Summe Haushalt"):
            break

        row_match = _ROW_START_RE.match(line)
        if row_match:
            if current_code is not None:
                rows.append((current_code, _normalize_spaces(" ".join(current_parts))))
            current_code = int(row_match.group(1))
            current_parts = [row_match.group(2) or ""]
            continue

        if current_code is None:
            continue
        if _PAGE_NOISE_RE.match(stripped):
            continue
        current_parts.append(stripped)

    if current_code is not None:
        rows.append((current_code, _normalize_spaces(" ".join(current_parts))))

    return rows


def _summe_column_center(page_text: str) -> Optional[float]:
    """Estimate the horizontal center of the total-expenditure column."""
    for raw_line in page_text.splitlines():
        match = re.search(r"(?<!\d)10(?!\d)", raw_line)
        if match and re.search(r"(?<!\d)11(?!\d)", raw_line):
            return (match.start() + match.end()) / 2.0
    for raw_line in page_text.splitlines():
        match = _SUMME_AUSGABEN_RE.search(raw_line)
        if match:
            return (match.start() + match.end()) / 2.0
    return None


def _parse_right_rows(page_text: str) -> list[str]:
    """Parse the right half of the Ausgaben table: numeric continuation rows."""
    rows: list[str] = []
    in_table = False

    for raw_line in page_text.splitlines():
        line = raw_line.rstrip()
        if not in_table and _SUMME_AUSGABEN_RE.search(line):
            in_table = True
            continue
        if not in_table:
            continue

        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("Summe Haushalt"):
            break
        if _PAGE_NOISE_RE.match(stripped):
            continue
        if stripped.startswith("Zu Spalte"):
            break

        if len(_amount_matches(line)) >= 3:
            rows.append(line)

    return rows


def _target_metadata(year_int: int, epl_code: int) -> Optional[tuple[str, str, str, str]]:
    """Map an Einzelplan row to program metadata."""
    if year_int < 1995:
        if epl_code == 30:
            return (
                "DE_BMFT",
                "Bundesminister für Forschung und Technologie",
                "Federal Ministry for Research and Technology",
                "Einzelplan 30: BMFT Gesamtausgaben",
            )
        if epl_code == 31:
            return (
                "DE_BMBW",
                "Bundesminister für Bildung und Wissenschaft",
                "Federal Ministry for Education and Science",
                "Einzelplan 31: BMBW Gesamtausgaben",
            )
        return None

    if epl_code == 30:
        return (
            "DE_BMBF",
            "Bundesministerium für Bildung und Forschung",
            "Federal Ministry of Education and Research",
            "Einzelplan 30: BMBF Gesamtausgaben",
        )
    return None


def _extract_from_total_pages(
    pages: list[tuple[int, str]],
    year_int: int,
) -> list[tuple[str, str, str, str, float, int]]:
    """Extract from right-hand Ausgaben pages that contain Summe-Ausgaben rows.

    Handles both inline-row layout and the column-stacked PDF extraction pattern
    where each Epl row is split across many lines: the Epl number appears as a
    standalone line AFTER the numeric columns it belongs to.  We look back up
    to _LOOKBACK lines to find the relevant amount group.
    """
    results: list[tuple[str, str, str, str, float, int]] = []
    seen_codes: set[str] = set()
    _LOOKBACK = 12  # lines to look back when Epl number is standalone

    for page_number, page_text in pages:
        if not (_HAUSHALTSUEBERSICHT_RE.search(page_text) and _AUSGABEN_RE.search(page_text)):
            continue
        if not _SUMME_AUSGABEN_RE.search(page_text):
            continue

        center = _summe_column_center(page_text)
        lines = page_text.splitlines()

        for i, raw_line in enumerate(lines):
            line = raw_line.rstrip()
            if _PAGE_NOISE_RE.match(line.strip()):
                continue

            epl_match = _ROW_END_EPL_RE.search(line)
            if not epl_match:
                continue
            epl_code = int(epl_match.group(1))
            metadata = _target_metadata(year_int, epl_code)
            if metadata is None:
                continue
            program_code, section_name, section_name_en, line_description = metadata
            if program_code in seen_codes:
                continue

            # First try: amount on the same line
            amount = _pick_amount_near_column(line, center, year_int=year_int)

            # Second try: Epl number is a standalone token on its own line.
            # The PDF table columns are extracted row-by-row in column order:
            #   ... [col1] [col2] [col3] [Summe] [prev_year] [+/-sign] [delta] [Epl_N]
            # So "Summe Ausgaben" for this Epl is at offset -4 from the Epl line.
            # We try offsets -4 to -7 to handle layout variations.
            if amount is None and line.strip() in (str(epl_code), f"{epl_code} ", str(epl_code) + " "):
                for offset in range(4, 9):
                    candidate_idx = i - offset
                    if candidate_idx < 0:
                        break
                    candidate_line = lines[candidate_idx]
                    cand_amounts = [
                        v for v, *_ in _amount_matches(candidate_line)
                        if v >= _MIN_TOTAL_AMOUNT
                    ]
                    if len(cand_amounts) == 1:
                        amount = cand_amounts[0]
                        break
                    elif len(cand_amounts) > 1:
                        # Multiple amounts on same line → unlikely to be clean Summe row; skip
                        continue

            # Apply a sanity cap: Epl 30/31 should never exceed ~50B DEM/EUR
            # (the largest BMBF budget is ~17B EUR in 2019)
            if amount is None:
                continue
            if amount > _max_total_for_year(year_int):
                continue

            results.append(
                (
                    program_code,
                    section_name,
                    section_name_en,
                    line_description,
                    amount,
                    page_number,
                )
            )
            seen_codes.add(program_code)

    return results


def _extract_from_summary_pairs(
    pages: list[tuple[int, str]],
    year_int: int,
) -> list[tuple[str, str, str, str, float, int]]:
    """Extract target rows from paired Ausgaben summary-table pages."""
    results: list[tuple[str, str, str, str, float, int]] = []
    seen_codes: set[str] = set()

    for idx in range(len(pages) - 1):
        left_page_num, left_text = pages[idx]
        right_page_num, right_text = pages[idx + 1]

        if not (_HAUSHALTSUEBERSICHT_RE.search(left_text) and _AUSGABEN_RE.search(left_text)):
            continue
        if not _EPL_HEADER_RE.search(left_text):
            continue
        if not (_HAUSHALTSUEBERSICHT_RE.search(right_text) and _SUMME_AUSGABEN_RE.search(right_text)):
            continue

        left_rows = _parse_left_rows(left_text)
        right_rows = _parse_right_rows(right_text)
        if not left_rows or not right_rows:
            continue

        center = _summe_column_center(right_text)
        row_count = min(len(left_rows), len(right_rows))
        for row_index in range(row_count):
            epl_code, row_text = left_rows[row_index]
            metadata = _target_metadata(year_int, epl_code)
            if metadata is None:
                continue
            program_code, section_name, section_name_en, line_description = metadata
            if program_code in seen_codes:
                continue
            amount = _pick_amount_near_column(right_rows[row_index], center, year_int=year_int)
            if amount is None or amount > _max_total_for_year(year_int):
                continue
            results.append(
                (
                    program_code,
                    section_name,
                    section_name_en,
                    line_description,
                    amount,
                    left_page_num,
                )
            )
            seen_codes.add(program_code)

    return results


# ── Fallback extraction methods ───────────────────────────────────────────────

def _name_patterns(year_int: int) -> list[tuple[re.Pattern, str, str, str, str]]:
    if year_int < 1995:
        return [
            (
                re.compile(
                    r"Bundes(?:minister(?:ium)?)?\s+f[uü]r\s+Forschung\s+und\s+Technologie",
                    re.IGNORECASE,
                ),
                "DE_BMFT",
                "Bundesminister für Forschung und Technologie",
                "Federal Ministry for Research and Technology",
                "Einzelplan 30: BMFT Gesamtausgaben",
            ),
            (
                re.compile(
                    r"Bundes(?:minister(?:ium)?)?\s+f[uü]r\s+Bildung\s+und\s+Wissenschaft",
                    re.IGNORECASE,
                ),
                "DE_BMBW",
                "Bundesminister für Bildung und Wissenschaft",
                "Federal Ministry for Education and Science",
                "Einzelplan 31: BMBW Gesamtausgaben",
            ),
        ]

    return [
        (
            re.compile(
                r"Bundesministerium\s+f[uü]r\s+Bildung\s+und\s+Forschung"
                r"|Bundesministerium\s+f[uü]r\s+Bildung,\s*Wissenschaft,\s*Forschung\s+und\s+Technologie"
                r"|BMBF\b",
                re.IGNORECASE,
            ),
            "DE_BMBF",
            "Bundesministerium für Bildung und Forschung",
            "Federal Ministry of Education and Research",
            "Einzelplan 30: BMBF Gesamtausgaben",
        )
    ]


def _is_summary_like_source(source_filename: str) -> bool:
    name = source_filename.lower()
    preferred_tokens = ("gesamtplan", "haushalt", "uebersichten", "übersichten", "bgbl")
    if any(token in name for token in preferred_tokens):
        return True
    if re.match(r"^\d{4}\s+\d{6,7}\.pdf$", source_filename):
        return True
    return False


def _has_summary_markers(text: str) -> bool:
    lowered = text.lower()
    return "haushaltsübersicht" in lowered or "haushaltsubersicht" in lowered


def _extract_from_modern_stacked_pages(
    pages: list[tuple[int, str]],
    year_int: int,
) -> list[tuple[str, str, str, str, float, int]]:
    """Extract ministry totals from modern stacked B. Ausgaben summary pages."""
    if year_int < 2005:
        return []

    full_text = "\n".join(text for _, text in pages)
    if not _has_summary_markers(full_text):
        return []

    best_hits: dict[str, tuple[str, str, str, str, float, int]] = {}
    max_total = _max_total_for_year(year_int)
    min_total = 5_000_000_000
    start_idx: Optional[int] = None
    end_idx: Optional[int] = None

    for idx, (_, page_text) in enumerate(pages):
        if start_idx is None and (
            ("B. Ausgaben" in page_text)
            or (
                "Gesamtplan - Teil I: Haushaltsübersicht" in page_text
                and "Ausgaben" in page_text
                and "Einnahmen" not in page_text
                and "Verpflichtungsermächtigungen" not in page_text
                and "Flexibilisierte Ausgaben" not in page_text
            )
        ):
            start_idx = idx
            continue
        if start_idx is not None and (
            "C. Verpflichtungsermächtigungen" in page_text
            or "Verpflichtungsermächtigungen" in page_text
            or "Flexibilisierte Ausgaben" in page_text
            or "Teil II:" in page_text
        ):
            end_idx = idx
            break

    if start_idx is None:
        return []
    relevant_pages = pages[start_idx:end_idx] if end_idx is not None else pages[start_idx:]

    for page_number, page_text in relevant_pages:
        lines = [line.strip() for line in page_text.splitlines() if line.strip()]
        idx = 0
        while idx < len(lines):
            line = lines[idx]
            if line not in {"30", "31"}:
                idx += 1
                continue

            epl_code = int(line)
            metadata = _target_metadata(year_int, epl_code)
            if metadata is None:
                idx += 1
                continue
            program_code, section_name, section_name_en, line_description = metadata

            window: list[str] = []
            for candidate in lines[idx + 1: idx + 14]:
                if re.fullmatch(r"\d{2}", candidate):
                    break
                window.append(candidate)
            if not any(
                keyword in " ".join(window).lower()
                for keyword in ("bildung", "forschung", "technologie", "wissenschaft")
            ):
                idx += 1
                continue

            candidate_amounts: list[float] = []
            for candidate in window:
                if re.fullmatch(r"[+\-–].*", candidate):
                    continue
                parsed = _parse_thousand_amount(candidate)
                if parsed is None:
                    continue
                if min_total <= parsed <= max_total:
                    candidate_amounts.append(parsed)

            if candidate_amounts:
                amount = max(candidate_amounts)
                existing = best_hits.get(program_code)
                if existing is None or amount > existing[4]:
                    best_hits[program_code] = (
                        program_code,
                        section_name,
                        section_name_en,
                        line_description,
                        amount,
                        page_number,
                    )

            idx += 1

    return list(best_hits.values())


def _extract_from_name_search(
    all_text: str,
    year_int: int,
    source_filename: str,
) -> list[tuple[str, str, str, str, float]]:
    """Fallback: search for ministry names and nearby total-like amounts."""
    if not _is_summary_like_source(source_filename) or not _has_summary_markers(all_text):
        return []

    results: list[tuple[str, str, str, str, float]] = []
    seen_codes: set[str] = set()
    max_total = _max_total_for_year(year_int)
    min_total = 5_000_000_000 if year_int >= 1995 else 1_000_000_000

    for pattern, program_code, section_name, section_name_en, line_description in _name_patterns(year_int):
        for match in pattern.finditer(all_text):
            if program_code in seen_codes:
                break
            window = all_text[match.start(): min(len(all_text), match.start() + 1800)]
            total_match = _TOTAL_LINE_RE.search(window)
            if total_match:
                total_window = window[total_match.start(): min(len(window), total_match.start() + 250)]
                amount = _pick_amount_near_column(total_window, None, year_int=year_int)
            else:
                amounts = [item[0] for item in _amount_matches(window) if min_total <= item[0] <= max_total]
                amount = max(amounts) if amounts else None
            if amount is None or amount > max_total or amount < min_total:
                continue
            results.append((program_code, section_name, section_name_en, line_description, amount))
            seen_codes.add(program_code)

    return results


def _extract_from_einzelplan_pages(
    pages: list[tuple[int, str]],
    year_int: int,
    source_filename: str,
) -> list[tuple[str, str, str, str, float, int]]:
    """Last resort: search single pages for Einzelplan headings and totals."""
    full_text = "\n".join(text for _, text in pages)
    if not _is_summary_like_source(source_filename) or not _has_summary_markers(full_text):
        return []

    results: list[tuple[str, str, str, str, float, int]] = []
    seen_codes: set[str] = set()
    max_total = _max_total_for_year(year_int)
    min_total = 5_000_000_000 if year_int >= 1995 else 1_000_000_000

    patterns = _name_patterns(year_int)
    for page_number, page_text in pages:
        if not page_text.strip():
            continue
        title_match = _EINZELPLAN_TITLE_RE.search(page_text)
        if not title_match:
            continue

        for pattern, program_code, section_name, section_name_en, line_description in patterns:
            if program_code in seen_codes:
                continue
            if not pattern.search(page_text):
                continue
            window = page_text[title_match.start(): min(len(page_text), title_match.start() + 1500)]
            total_match = _TOTAL_LINE_RE.search(window)
            if total_match:
                total_window = window[total_match.start(): min(len(window), total_match.start() + 250)]
                amount = _pick_amount_near_column(total_window, None, year_int=year_int)
            else:
                amounts = [item[0] for item in _amount_matches(window) if min_total <= item[0] <= max_total]
                amount = max(amounts) if amounts else None
            if amount is None or amount > max_total or amount < min_total:
                continue
            results.append(
                (program_code, section_name, section_name_en, line_description, amount, page_number)
            )
            seen_codes.add(program_code)

    return results


# ── Record builder ─────────────────────────────────────────────────────────────

def _build_record(
    *,
    country: str,
    year: str,
    program_code: str,
    section_name: str,
    section_name_en: str,
    line_description: str,
    amount_local: float,
    currency: str,
    source_filename: str,
    file_id: str,
    page_number: int,
    confidence: float,
) -> dict:
    return {
        "country": country,
        "year": year,
        "section_code": "DE_RESEARCH",
        "section_name": section_name,
        "section_name_en": section_name_en,
        "program_code": program_code,
        "line_description": line_description,
        "line_description_en": line_description.replace("Gesamtausgaben", "Total Expenditure"),
        "amount_local": amount_local,
        "currency": currency,
        "unit": currency,
        "rd_category": "direct_rd",
        "taxonomy_score": 8.5,
        "decision": "include",
        "confidence": confidence,
        "source_file": source_filename,
        "file_id": file_id,
        "page_number": page_number,
    }


# ── Public API ─────────────────────────────────────────────────────────────────

def extract_germany_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract German federal research-ministry budget records."""
    year_int = int(year) if str(year).isdigit() else 0
    currency = "EUR" if year_int >= 2002 else "DEM"
    pages: list[tuple[int, str]] = [
        (int(row.page_number), row.text if isinstance(row.text, str) else "")
        for row in sorted_pages.itertuples(index=False)
    ]
    all_text = "\n".join(text for _, text in pages)
    if not all_text.strip():
        return []

    records: list[dict] = []

    if year_int >= 2005:
        summary_hits = _extract_from_modern_stacked_pages(pages, year_int)
        if not summary_hits:
            summary_hits = _extract_from_total_pages(pages, year_int)
        if not summary_hits:
            summary_hits = _extract_from_summary_pairs(pages, year_int)
    else:
        summary_hits = _extract_from_total_pages(pages, year_int)
        if not summary_hits:
            summary_hits = _extract_from_summary_pairs(pages, year_int)
        if not summary_hits:
            summary_hits = _extract_from_modern_stacked_pages(pages, year_int)
    if summary_hits:
        for program_code, section_name, section_name_en, line_description, amount, page_number in summary_hits:
            records.append(
                _build_record(
                    country=country,
                    year=year,
                    program_code=program_code,
                    section_name=section_name,
                    section_name_en=section_name_en,
                    line_description=line_description,
                    amount_local=amount,
                    currency=currency,
                    source_filename=source_filename,
                    file_id=file_id,
                    page_number=page_number,
                    confidence=0.90,
                )
            )
        logger.info(
            "Germany extractor (summary table): %s (year %s) -> %d records",
            source_filename,
            year,
            len(records),
        )
        return records

    name_hits = _extract_from_name_search(all_text, year_int, source_filename)
    if name_hits:
        for program_code, section_name, section_name_en, line_description, amount in name_hits:
            records.append(
                _build_record(
                    country=country,
                    year=year,
                    program_code=program_code,
                    section_name=section_name,
                    section_name_en=section_name_en,
                    line_description=line_description,
                    amount_local=amount,
                    currency=currency,
                    source_filename=source_filename,
                    file_id=file_id,
                    page_number=1,
                    confidence=0.78,
                )
            )
        logger.info(
            "Germany extractor (name fallback): %s (year %s) -> %d records",
            source_filename,
            year,
            len(records),
        )
        return records

    page_hits = _extract_from_einzelplan_pages(pages, year_int, source_filename)
    if page_hits:
        for program_code, section_name, section_name_en, line_description, amount, page_number in page_hits:
            records.append(
                _build_record(
                    country=country,
                    year=year,
                    program_code=program_code,
                    section_name=section_name,
                    section_name_en=section_name_en,
                    line_description=line_description,
                    amount_local=amount,
                    currency=currency,
                    source_filename=source_filename,
                    file_id=file_id,
                    page_number=page_number,
                    confidence=0.68,
                )
            )
        logger.info(
            "Germany extractor (Einzelplan fallback): %s (year %s) -> %d records",
            source_filename,
            year,
            len(records),
        )
        return records

    logger.debug(
        "Germany extractor: no extractable rows in %s (year %s).",
        source_filename,
        year,
    )
    return []
