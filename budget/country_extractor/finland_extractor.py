"""Finnish Valtion talousarvio (State Budget) extractor.

Finnish science and technology budget items of interest:

- **Luku 88 "Tiede"** under **Pääluokka 29** (Opetusministeriö /
  Ministry of Education)
  Main items:
  - 21. Suomen Akatemian toimintamenot
  - 50. Suomen Akatemian tutkimusmäärärahat
  - 53. Veikkauksen ja raha-arpajaisten voittovarat tieteen edistämiseen /
    tieteen tukemiseen
  - 88. Tiede (chapter total)

- **Tekes / Teknologian kehittämiskeskus** under **Pääluokka 32**
  (Kauppa- ja teollisuusministeriö / Ministry of Trade and Industry)
  Key anchor:
  - Teknologian kehittämiskeskuksen toimintamenot
  Nearby direct R&D lines often include:
  - Tutkimus- ja kehitystoiminta
  - Avustukset teknologiseen tutkimukseen ja kehitykseen
  - Lainat teknologiseen tutkimukseen ja kehitykseen

Document formats handled
------------------------
1. Older structure with explicit chapter heading:
     Pääluokka {nn}
     Luku 88
     88. Tiede i ... {amount}

2. Modern table layout (confirmed in the 2002 local sample):
     Pääluokka 29
     88. Tiede      {eur_amount} {fim_amount}
     21. Suomen Akatemian toimintamenot ...
     50. Suomen Akatemian tutkimusmäärärahat ...

Amounts are full values, not thousands:
- 2002 onward: EUR
- pre-2002: FIM

Some files contain wrapped descriptions across lines; this extractor first builds
"logical lines" by joining continuation rows before matching target items.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

logger = logging.getLogger("innovation_pipeline")

_CONTROL_DIGIT_TRANSLATION = str.maketrans({
    chr(0x13 + digit): str(digit)
    for digit in range(10)
})

# ── Regex constants ────────────────────────────────────────────────────────────

_LOGICAL_LINE_START_RE = re.compile(
    r"^(?:P[aä]äluokka\s+\d{2}\b|Osasto\s+\d{2}\b|Luku\s+\d{1,3}\b|\d{2,3}\.)",
    re.IGNORECASE,
)

_IGNORE_LINE_RE = re.compile(
    r"^(?:"
    r"\d+$"
    r"|N:o\s+\d+"
    r"|\d+/\d{4}"
    r"|Asiakirjayhdistelmä\b"
    r"|Tämä\s+tuloste\s+ei\s+ole\s+virallinen\s+asiakirja"
    r"|Sivu\s+\d+"
    r")",
    re.IGNORECASE,
)

_MINISTRY_HEADER_RE = re.compile(
    r"^(?:P[aä]äluokka\s+\d{2}\b|\d{2}\.\s+.+hallinnonala\b)",
    re.IGNORECASE,
)

_MINISTRY29_RE = re.compile(
    r"(?:P[aä]äluokka\s+29\b"
    r"|29\.\s+Opetusministeri[oö]n\s+hallinnonala\b"
    r"|29\.\s+Undervisnings-\s+och\s+kulturministeriets\s+f[oö]rvaltningsomr[aå]de\b)",
    re.IGNORECASE,
)

_MINISTRY32_RE = re.compile(
    r"(?:P[aä]äluokka\s+32\b"
    r"|32\.\s+Kauppa-\s+ja\s+teollisuusministeri[oö]n\s+hallinnonala\b"
    r"|32\.\s+Ty[oö]-\s+ja\s+elinkeinoministeri[oö]n\s+hallinnonala\b"
    r"|32\.\s+Arbets-\s+och\s+n[aä]ringsministeriets\s+f[oö]rvaltningsomr[aå]de\b)",
    re.IGNORECASE,
)

_SCIENCE_TOTAL_RE = re.compile(r"^(?:88|60)\.\s*Tiede\b", re.IGNORECASE)

_AKATEMIA_OPERATING_RE = re.compile(
    r"^\d{2}\.\s+(?:Suomen\s+Akatemian\s+toimintamenot|Finlands\s+Akademis\s+omkostnader)\b",
    re.IGNORECASE,
)

_AKATEMIA_GRANTS_RE = re.compile(
    r"^\d{2}\.\s+(?:Suomen\s+Akatemian\s+tutkimusm[aä][aä]r[aä]rahat|Finlands\s+Akademis\s+forskningsanslag)\b",
    re.IGNORECASE,
)

_SCIENCE_LOTTERY_RE = re.compile(
    r"^\d{2}\.\s+(?:Veikkauksen\s+ja\s+raha-arpajaisten\s+voittovarat\s+tieteen\s+"
    r"(?:edist[aä]miseen|tukemiseen)|Tippnings-\s+och\s+penninglotterivinstmedel\s+"
    r"f[oö]r\s+fr[aä]mjande\s+av\s+vetenska(?:pen|pen)"
    r"|Avkastning\s+av\s+penningspelsverksamheten\s+f[oö]r\s+fr[aä]mjande\s+av\s+vetenskap)\b",
    re.IGNORECASE,
)

_DUAL_AMOUNT_TAIL_RE = re.compile(
    r"(\d{1,3}(?:[ \xa0\u202f]\d{3}){2,3})\s+(\d{1,3}(?:[ \xa0\u202f]\d{3}){2,3})$"
)
_SINGLE_AMOUNT_TAIL_RE = re.compile(
    r"(\d{1,3}(?:[ \xa0\u202f]\d{3}){1,3}|\d{4,})$"
)
_AMOUNT_ANY_RE = re.compile(r"\d{1,3}(?:[ \xa0\u202f]\d{3}){1,3}")

_TEKES_ANCHOR_RE = re.compile(
    r"Teknologian\s+kehitt[aä]miskesk\w*"
    r"|Innovaatiorahoituskeskus\s+Business\s+Finland\w*"
    r"|Innovationsfinansieringsverket\s+Business\s+Finland"
    r"|(?:^|\b)Tekes\b"
    r"|(?:^|\b)Business\s+Finland\b",
    re.IGNORECASE,
)

_TEKES_RD_LINE_RE = re.compile(
    r"^(?:20\.\s+Julkinen\s+tutkimus-\s+ja\s+kehitystoiminta"
    r"|27\.\s+Tutkimus-\s+ja\s+kehitystoiminta"
    r"|40\.\s+Avustukset\s+teknologiseen\s+tutkimukseen\s+ja\s+kehitykseen"
    r"|40\.\s+Avustukset\s+tutkimukseen,\s+kehitykseen\s+ja\s+innovaatiotoimintaan"
    r"|40\.\s+Tutkimus-,\s+kehitt[aä]mis-\s+ja\s+innovaatiotoiminnan\s+tukeminen"
    r"|83\.\s+Lainat\s+tutkimus-\s+ja\s+innovaatiotoimintaan"
    r"|40\.\s+St[oö]djande\s+av\s+forsknings-,\s+utvecklings-\s+och\s+innovationsverksamhet"
    r"|43\.\s+St[oö]djande\s+av\s+f[oö]retagsdriven\s+forsknings-,\s+utvecklings-\s+och\s+innovationsverksamhet"
    r"|83\.\s+L[aå]n\s+f[oö]r\s+forsknings-\s+och\s+innovationsverksamhet)\b",
    re.IGNORECASE,
)

_AKATEMIA_OPERATING_RAW_RE = re.compile(
    r"Suomen\s+Akatemian\s+toimintamenot|Finlands\s+Akademis\s+omkostnader",
    re.IGNORECASE,
)
_AKATEMIA_GRANTS_RAW_RE = re.compile(
    r"Suomen\s+Akatemian\s+tutkimusm[aä][aä]r[aä]rahat|Finlands\s+Akademis\s+forskningsanslag",
    re.IGNORECASE,
)
_SCIENCE_LOTTERY_RAW_RE = re.compile(
    r"Veikkauksen\s+ja\s+raha-arpajaisten\s+voittovarat\s+tieteen\s+(?:edist[aä]miseen|tukemiseen)"
    r"|Tippnings-\s+och\s+penninglotterivinstmedel\s+f[oö]r\s+fr[aä]mjande\s+av\s+vetenskap(?:en)?"
    r"|Avkastning\s+av\s+penningspelsverksamheten\s+f[oö]r\s+fr[aä]mjande\s+av\s+vetenskap",
    re.IGNORECASE,
)
_TEKES_RAW_RE = re.compile(
    r"Teknologian\s+kehitt[aä]miskesk\w*"
    r"|Innovaatiorahoituskeskus\s+Business\s+Finland\w*"
    r"|Innovationsfinansieringsverket\s+Business\s+Finland"
    r"|Tutkimus-,\s+kehitt[aä]mis-\s+ja\s+innovaatiotoiminnan\s+tukeminen"
    r"|St[oö]djande\s+av\s+forsknings-,\s+utvecklings-\s+och\s+innovationsverksamhet"
    r"|Lainat\s+tutkimus-\s+ja\s+innovaatiotoimintaan"
    r"|L[aå]n\s+f[oö]r\s+forsknings-\s+och\s+innovationsverksamhet",
    re.IGNORECASE,
)

_PROGRAM_METADATA: dict[str, dict[str, str]] = {
    "FI_SCIENCE_TOTAL": {
        "program_description": "Luku 88 Tiede",
        "program_description_en": "Chapter 88 Science",
        "budget_type": "chapter_total",
    },
    "FI_AKATEMIA": {
        "program_description": "Suomen Akatemia",
        "program_description_en": "Academy of Finland",
        "budget_type": "operating_expenditure",
    },
    "FI_AKATEMIA_GRANTS": {
        "program_description": "Suomen Akatemia",
        "program_description_en": "Academy of Finland",
        "budget_type": "research_grants",
    },
    "FI_LOTTERY_SCIENCE": {
        "program_description": "Tieteen edistäminen veikkausvoittovaroin",
        "program_description_en": "Science promotion from lottery proceeds",
        "budget_type": "earmarked_grant",
    },
    "FI_TEKES": {
        "program_description": "Tekes / Business Finland",
        "program_description_en": "Tekes / Business Finland",
        "budget_type": "rdi_support",
    },
}


# ── Amount parsing ─────────────────────────────────────────────────────────────

def _parse_amount(raw: str) -> Optional[float]:
    """Parse a Finnish full-value amount using spaces as thousands separators."""
    raw = raw.translate(_CONTROL_DIGIT_TRANSLATION)
    cleaned = raw.replace("\xa0", " ").replace("\u202f", " ").strip()
    cleaned = re.sub(r"\s+", "", cleaned)
    if not re.fullmatch(r"\d+", cleaned):
        return None
    try:
        value = float(cleaned)
    except ValueError:
        return None
    return value if value > 0 else None


def _extract_amounts(line: str) -> list[float]:
    """Extract the trailing budget amount columns from a logical line."""
    dual = _DUAL_AMOUNT_TAIL_RE.search(line)
    if dual:
        values = [_parse_amount(dual.group(1)), _parse_amount(dual.group(2))]
        return [value for value in values if value is not None]

    single = _SINGLE_AMOUNT_TAIL_RE.search(line)
    if single:
        value = _parse_amount(single.group(1))
        return [value] if value is not None else []

    values: list[float] = []
    for match in _AMOUNT_ANY_RE.finditer(line):
        value = _parse_amount(match.group(0))
        if value is not None:
            values.append(value)
    if values:
        return values

    return []


def _select_line_amount(line: str, year_int: int) -> Optional[float]:
    """Select the relevant currency column from a logical line.

    2002+ files may contain dual columns (EUR first, FIM second). Pre-2002
    files typically contain a single FIM amount.
    """
    values = _extract_amounts(line)
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    return values[0] if year_int >= 2002 else values[-1]


# ── Text normalization ─────────────────────────────────────────────────────────

def _merge_wrapped_line(base: str, continuation: str) -> str:
    """Join a wrapped PDF line to the prior logical line."""
    if not continuation:
        return base
    if base.endswith("-"):
        merged = base[:-1] + continuation.lstrip()
    else:
        merged = f"{base} {continuation.lstrip()}"
    return re.sub(r"\s+", " ", merged).strip()


def _build_logical_lines(sorted_pages) -> list[tuple[int, str]]:
    """Build logical lines by joining wrapped continuation rows."""
    lines: list[tuple[int, str]] = []
    current_page: Optional[int] = None
    current_text: Optional[str] = None

    for row in sorted_pages.itertuples(index=False):
        page_number = int(getattr(row, "page_number", 1))
        page_text = row.text if isinstance(row.text, str) else ""
        page_text = page_text.translate(_CONTROL_DIGIT_TRANSLATION)

        for raw_line in page_text.splitlines():
            line = raw_line.replace("\xa0", " ").replace("\u202f", " ").strip()
            line = re.sub(r"\s+", " ", line)
            if not line:
                continue
            if _IGNORE_LINE_RE.match(line):
                continue

            if _LOGICAL_LINE_START_RE.match(line):
                if current_text is not None:
                    lines.append((current_page or 1, current_text))
                current_page = page_number
                current_text = line
                continue

            if current_text is None:
                current_page = page_number
                current_text = line
                continue

            current_text = _merge_wrapped_line(current_text, line)

    if current_text is not None:
        lines.append((current_page or 1, current_text))

    return lines


def _slice_ministry(lines: list[tuple[int, str]], header_re: re.Pattern) -> list[tuple[int, str]]:
    """Return the logical lines for a ministry block until the next ministry header."""
    paaluokka_hits: list[int] = []
    generic_hits: list[int] = []

    for idx, (_, line) in enumerate(lines):
        if header_re.search(line):
            if line.lower().startswith("pääluokka") or line.lower().startswith("paaluokka"):
                paaluokka_hits.append(idx)
            else:
                generic_hits.append(idx)

    start_idx: Optional[int]
    if paaluokka_hits:
        start_idx = paaluokka_hits[0]
    elif generic_hits:
        # Revenue sections can contain an earlier "... hallinnonala" hit with the
        # same code. Prefer the last generic hit, which is usually in Määrärahat.
        start_idx = generic_hits[-1]
    else:
        start_idx = None

    if start_idx is None:
        return []

    start_line = lines[start_idx][1].lower()
    started_at_paaluokka = start_line.startswith("pääluokka") or start_line.startswith("paaluokka")
    end_idx = len(lines)
    for idx in range(start_idx + 1, len(lines)):
        candidate = lines[idx][1]
        candidate_lower = candidate.lower()
        if started_at_paaluokka:
            if candidate_lower.startswith("pääluokka") or candidate_lower.startswith("paaluokka"):
                end_idx = idx
                break
        elif _MINISTRY_HEADER_RE.match(candidate):
            end_idx = idx
            break

    return lines[start_idx:end_idx]


def _find_matching_line(
    lines: list[tuple[int, str]],
    pattern: re.Pattern,
) -> tuple[Optional[int], Optional[str]]:
    """Return the first line matching a pattern."""
    for page_number, line in lines:
        if pattern.search(line):
            return page_number, line
    return None, None


def _extract_amount_after_match(text: str, pattern: re.Pattern) -> Optional[float]:
    """Fallback extractor: find a named item anywhere in raw text, then take the first amount."""
    match = pattern.search(text)
    if not match:
        return None
    window = text[match.start(): min(len(text), match.start() + 400)]
    for amount_match in _AMOUNT_ANY_RE.finditer(window):
        value = _parse_amount(amount_match.group(0))
        if value is not None:
            return value
    return None


def _find_science_window(lines: list[tuple[int, str]]) -> list[tuple[int, str]]:
    """Return a local window around the Academy / science lines."""
    anchor_idx: Optional[int] = None
    for idx, (_, line) in enumerate(lines):
        if _AKATEMIA_OPERATING_RE.search(line) or _AKATEMIA_GRANTS_RE.search(line):
            anchor_idx = idx
            break

    if anchor_idx is None:
        for idx, (_, line) in enumerate(lines):
            if _SCIENCE_TOTAL_RE.search(line):
                anchor_idx = idx
                break

    if anchor_idx is None:
        return []

    start_idx = max(0, anchor_idx - 6)
    end_idx = min(len(lines), anchor_idx + 14)
    return lines[start_idx:end_idx]


def _find_best_tekes_record(
    lines: list[tuple[int, str]],
    year_int: int,
) -> tuple[Optional[int], Optional[str], Optional[str], Optional[float], float]:
    """Pick the best Tekes-related line from the ministry 32 section.

    Preference:
    1. Direct R&D funding lines close to the Tekes anchor.
    2. The named Teknologian kehittämiskeskus / Tekes line itself.
    """
    anchor_idx: Optional[int] = None
    for idx, (_, line) in enumerate(lines):
        if _TEKES_ANCHOR_RE.search(line):
            anchor_idx = idx
            break

    if anchor_idx is None:
        return None, None, None, None, 0.0

    anchor_page, anchor_line = lines[anchor_idx]
    anchor_amount = _select_line_amount(anchor_line, year_int)

    best_page: Optional[int] = None
    best_line: Optional[str] = None
    best_amount: Optional[float] = None

    for page_number, line in lines:
        if not _TEKES_RD_LINE_RE.search(line):
            continue
        amount = _select_line_amount(line, year_int)
        if amount is None:
            continue
        if best_amount is None or amount > best_amount:
            best_page = page_number
            best_line = line
            best_amount = amount

    if best_amount is not None:
        return best_page, best_line, _translate_tekes_line(best_line), best_amount, 0.88

    if anchor_amount is not None:
        return (
            anchor_page,
            anchor_line,
            "Technology Development Centre operating expenses",
            anchor_amount,
            0.80,
        )

    return None, None, None, None, 0.0


# ── English labels ─────────────────────────────────────────────────────────────

def _translate_tekes_line(line: str) -> str:
    """Translate common Tekes-related line descriptions."""
    if re.search(r"^20\.\s+Julkinen\s+tutkimus-\s+ja\s+kehitystoiminta", line, re.IGNORECASE):
        return "Public research and development activity"
    if re.search(r"^27\.\s+Tutkimus-\s+ja\s+kehitystoiminta", line, re.IGNORECASE):
        return "Research and development activity"
    if re.search(
        r"^40\.\s+Avustukset\s+teknologiseen\s+tutkimukseen\s+ja\s+kehitykseen",
        line,
        re.IGNORECASE,
    ):
        return "Grants for technological research and development"
    if re.search(
        r"^40\.\s+Avustukset\s+tutkimukseen,\s+kehitykseen\s+ja\s+innovaatiotoimintaan",
        line,
        re.IGNORECASE,
    ):
        return "Grants for research, development and innovation activity"
    if re.search(
        r"^40\.\s+Tutkimus-,\s+kehitt[aä]mis-\s+ja\s+innovaatiotoiminnan\s+tukeminen",
        line,
        re.IGNORECASE,
    ):
        return "Support for research, development and innovation activity"
    if re.search(
        r"^83\.\s+Lainat\s+teknologiseen\s+tutkimukseen\s+ja\s+kehitykseen",
        line,
        re.IGNORECASE,
    ):
        return "Loans for technological research and development"
    if re.search(
        r"^83\.\s+Lainat\s+tutkimus-\s+ja\s+innovaatiotoimintaan",
        line,
        re.IGNORECASE,
    ):
        return "Loans for research and innovation activity"
    if re.search(
        r"^40\.\s+St[oö]djande\s+av\s+forsknings-,\s+utvecklings-\s+och\s+innovationsverksamhet",
        line,
        re.IGNORECASE,
    ):
        return "Support for research, development and innovation activity"
    if re.search(
        r"^43\.\s+St[oö]djande\s+av\s+f[oö]retagsdriven\s+forsknings-,\s+utvecklings-\s+och\s+innovationsverksamhet",
        line,
        re.IGNORECASE,
    ):
        return "Support for business-driven research, development and innovation"
    if re.search(
        r"^83\.\s+L[aå]n\s+f[oö]r\s+forsknings-\s+och\s+innovationsverksamhet",
        line,
        re.IGNORECASE,
    ):
        return "Loans for research and innovation activity"
    return "Tekes / Technology Development Centre"


def _make_record(
    *,
    country: str,
    year: str,
    section_code: str,
    section_name: str,
    section_name_en: str,
    program_code: str,
    line_description: str,
    line_description_en: str,
    amount_local: float,
    currency: str,
    source_filename: str,
    file_id: str,
    page_number: int,
    taxonomy_score: float,
    confidence: float,
    decision: str = "include",
) -> dict:
    """Build a standard budget record."""
    metadata = _PROGRAM_METADATA.get(program_code, {})
    return {
        "country": country,
        "year": year,
        "section_code": section_code,
        "section_name": section_name,
        "section_name_en": section_name_en,
        "program_code": program_code,
        "program_description": metadata.get("program_description", line_description),
        "program_description_en": metadata.get("program_description_en", line_description_en),
        "line_description": line_description,
        "line_description_en": line_description_en,
        "budget_type": metadata.get("budget_type", ""),
        "text_snippet": line_description,
        "raw_line": line_description,
        "amount_local": amount_local,
        "currency": currency,
        "unit": currency,
        "rd_category": "direct_rd",
        "taxonomy_score": taxonomy_score,
        "decision": decision,
        "confidence": confidence,
        "source_file": source_filename,
        "file_id": file_id,
        "page_number": page_number,
    }


# ── Public API ─────────────────────────────────────────────────────────────────

def extract_finland_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract Finnish science budget records in the standard schema."""
    year_int = int(year) if str(year).isdigit() else 2003
    currency = "EUR" if year_int >= 2002 else "FIM"
    records: list[dict] = []
    all_text = "\n".join(
        row.text if isinstance(row.text, str) else ""
        for row in sorted_pages.itertuples(index=False)
    )
    all_text = all_text.translate(_CONTROL_DIGIT_TRANSLATION)

    logical_lines = _build_logical_lines(sorted_pages)
    if not logical_lines:
        logger.debug("Finland extractor: empty text in %s (year %s).", source_filename, year)
        return []

    # ── Science block: Pääluokka 29 / Luku 88 ────────────────────────────────
    ministry29_lines = _slice_ministry(logical_lines, _MINISTRY29_RE)
    science_search_lines = ministry29_lines or logical_lines
    science_window = _find_science_window(science_search_lines) if science_search_lines else []

    if science_search_lines:
        page_number, line = _find_matching_line(science_window, _SCIENCE_TOTAL_RE)
        amount = _select_line_amount(line, year_int) if line else None
        if line and amount is not None:
            records.append(
                _make_record(
                    country=country,
                    year=year,
                    section_code="FI_SCIENCE",
                    section_name="Tiede ja tutkimus",
                    section_name_en="Science and Research",
                    program_code="FI_SCIENCE_TOTAL",
                    line_description="88. Tiede",
                    line_description_en="Chapter 88 Science",
                    amount_local=amount,
                    currency=currency,
                    source_filename=source_filename,
                    file_id=file_id,
                    page_number=page_number or 1,
                    taxonomy_score=9.0,
                    confidence=0.92,
                )
            )

        page_number, line = _find_matching_line(science_search_lines, _AKATEMIA_OPERATING_RE)
        amount = _select_line_amount(line, year_int) if line else None
        if line and amount is not None:
            records.append(
                _make_record(
                    country=country,
                    year=year,
                    section_code="FI_SCIENCE",
                    section_name="Tiede ja tutkimus",
                    section_name_en="Science and Research",
                    program_code="FI_AKATEMIA",
                    line_description="Suomen Akatemian toimintamenot",
                    line_description_en="Academy of Finland operating expenses",
                    amount_local=amount,
                    currency=currency,
                    source_filename=source_filename,
                    file_id=file_id,
                    page_number=page_number or 1,
                    taxonomy_score=9.0,
                    confidence=0.90,
                )
            )

        page_number, line = _find_matching_line(science_search_lines, _AKATEMIA_GRANTS_RE)
        amount = _select_line_amount(line, year_int) if line else None
        if line and amount is not None:
            records.append(
                _make_record(
                    country=country,
                    year=year,
                    section_code="FI_SCIENCE",
                    section_name="Tiede ja tutkimus",
                    section_name_en="Science and Research",
                    program_code="FI_AKATEMIA_GRANTS",
                    line_description="Suomen Akatemian tutkimusmäärärahat",
                    line_description_en="Academy of Finland research grants",
                    amount_local=amount,
                    currency=currency,
                    source_filename=source_filename,
                    file_id=file_id,
                    page_number=page_number or 1,
                    taxonomy_score=9.4,
                    confidence=0.92,
                )
            )

        page_number, line = _find_matching_line(science_search_lines, _SCIENCE_LOTTERY_RE)
        amount = _select_line_amount(line, year_int) if line else None
        if line and amount is not None:
            records.append(
                _make_record(
                    country=country,
                    year=year,
                    section_code="FI_SCIENCE",
                    section_name="Tiede ja tutkimus",
                    section_name_en="Science and Research",
                    program_code="FI_LOTTERY_SCIENCE",
                    line_description="Veikkauksen ja raha-arpajaisten voittovarat tieteen tukemiseen",
                    line_description_en="Lottery and pools proceeds for science support",
                    amount_local=amount,
                    currency=currency,
                    source_filename=source_filename,
                    file_id=file_id,
                    page_number=page_number or 1,
                    taxonomy_score=8.6,
                    confidence=0.88,
                )
            )

    # ── Tekes block: Pääluokka 32 ────────────────────────────────────────────
    ministry32_lines = _slice_ministry(logical_lines, _MINISTRY32_RE)
    tekes_search_lines = ministry32_lines or logical_lines
    (
        tekes_page,
        tekes_line,
        tekes_line_en,
        tekes_amount,
        tekes_confidence,
    ) = _find_best_tekes_record(tekes_search_lines, year_int)

    if tekes_line and tekes_amount is not None:
        records.append(
            _make_record(
                country=country,
                year=year,
                section_code="FI_TEKES",
                section_name="Teknologian kehittäminen",
                section_name_en="Technology Development",
                program_code="FI_TEKES",
                line_description=tekes_line,
                line_description_en=tekes_line_en or "Tekes / Technology Development Centre",
                amount_local=tekes_amount,
                currency=currency,
                source_filename=source_filename,
                file_id=file_id,
                page_number=tekes_page or 1,
                taxonomy_score=8.8,
                confidence=tekes_confidence,
            )
        )

    existing_codes = {record["program_code"] for record in records}

    if "FI_AKATEMIA" not in existing_codes:
        amount = _extract_amount_after_match(all_text, _AKATEMIA_OPERATING_RAW_RE)
        if amount is not None:
            records.append(
                _make_record(
                    country=country,
                    year=year,
                    section_code="FI_SCIENCE",
                    section_name="Tiede ja tutkimus",
                    section_name_en="Science and Research",
                    program_code="FI_AKATEMIA",
                    line_description="Suomen Akatemian toimintamenot",
                    line_description_en="Academy of Finland operating expenses",
                    amount_local=amount,
                    currency=currency,
                    source_filename=source_filename,
                    file_id=file_id,
                    page_number=1,
                    taxonomy_score=8.8,
                    confidence=0.72,
                )
            )

    if "FI_AKATEMIA_GRANTS" not in existing_codes:
        amount = _extract_amount_after_match(all_text, _AKATEMIA_GRANTS_RAW_RE)
        if amount is not None:
            records.append(
                _make_record(
                    country=country,
                    year=year,
                    section_code="FI_SCIENCE",
                    section_name="Tiede ja tutkimus",
                    section_name_en="Science and Research",
                    program_code="FI_AKATEMIA_GRANTS",
                    line_description="Suomen Akatemian tutkimusmäärärahat",
                    line_description_en="Academy of Finland research grants",
                    amount_local=amount,
                    currency=currency,
                    source_filename=source_filename,
                    file_id=file_id,
                    page_number=1,
                    taxonomy_score=9.0,
                    confidence=0.72,
                )
            )

    if "FI_LOTTERY_SCIENCE" not in existing_codes:
        amount = _extract_amount_after_match(all_text, _SCIENCE_LOTTERY_RAW_RE)
        if amount is not None:
            records.append(
                _make_record(
                    country=country,
                    year=year,
                    section_code="FI_SCIENCE",
                    section_name="Tiede ja tutkimus",
                    section_name_en="Science and Research",
                    program_code="FI_LOTTERY_SCIENCE",
                    line_description="Veikkauksen ja raha-arpajaisten voittovarat tieteen tukemiseen",
                    line_description_en="Lottery and pools proceeds for science support",
                    amount_local=amount,
                    currency=currency,
                    source_filename=source_filename,
                    file_id=file_id,
                    page_number=1,
                    taxonomy_score=8.2,
                    confidence=0.68,
                )
            )

    if "FI_TEKES" not in existing_codes:
        amount = _extract_amount_after_match(all_text, _TEKES_RAW_RE)
        if amount is not None and amount >= 10_000_000:
            records.append(
                _make_record(
                    country=country,
                    year=year,
                    section_code="FI_TEKES",
                    section_name="Teknologian kehittäminen",
                    section_name_en="Technology Development",
                    program_code="FI_TEKES",
                    line_description="Tekes / Business Finland (fallback extraction)",
                    line_description_en="Tekes / Business Finland (fallback extraction)",
                    amount_local=amount,
                    currency=currency,
                    source_filename=source_filename,
                    file_id=file_id,
                    page_number=1,
                    taxonomy_score=8.2,
                    confidence=0.62,
                    decision="review",
                )
            )

    if records:
        logger.info(
            "Finland extractor: %s (year %s) -> %d records",
            source_filename,
            year,
            len(records),
        )
    else:
        logger.debug(
            "Finland extractor: no extractable science lines in %s (year %s).",
            source_filename,
            year,
        )

    return records
