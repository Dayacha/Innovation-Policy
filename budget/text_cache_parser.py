"""
Text cache parser for budget.

Reads pre-extracted PDF text from Data/output/budget/full_text/{Country}/*.txt.gz
and produces RawRow-compatible records.

File format (from PDF-to-text extraction):
  - Agency blocks separated by ALL-CAPS names
  - Each block: agency name → French translation → vote numbers + descriptions + amounts
  - Two main formats:
      C54-style (single column):  vote amounts then a TOTAL as the last number
      C44-style (two columns):    amounts come in pairs (main estimates, interim)

Detection: if the last number in a block equals the sum of all preceding numbers,
it is a total (C54 style). Otherwise, assume C44 style (take every other number
starting at index 0 as the main estimates and sum them).

Usage:
  from budget.text_cache_parser import parse_text_cache
  rows = parse_text_cache(country="Canada", year_range=(2023, 2024))
"""

from __future__ import annotations

import gzip
import hashlib
import logging
import re
from pathlib import Path
from typing import Optional

from budget import config as cfg
from budget.docx_table_parser import RawRow

logger = logging.getLogger(__name__)

TEXT_CACHE_DIR = Path("Data/output/budget/full_text")
CHILE_SOURCE_PDF_DIR = Path("Data/input/finance_bills/Chile")
PORTUGAL_SOURCE_PDF_DIR = Path("Data/input/finance_bills/Portugal")

# Regex for ALL-CAPS agency name lines (≥3 caps words, may span multiple lines)
_RE_CAPS_LINE = re.compile(r"^[A-Z][A-Z\s\(\)\-'\.&/,]{10,}$")

# Regex for standalone dollar amounts (possibly with commas or dashes)
_RE_AMOUNT = re.compile(r"^–?\s*([\d,]+)\s*$")

# Regex for fiscal year in filename e.g. "2023-24" or "2023-2024"
_RE_FISCAL_YEAR = re.compile(r"(\d{4})-(\d{2,4})")

# Regex for older single-year act filenames like "1987-3" or "1995-2"
# where the suffix is an act/document number, not the second fiscal-year part.
_RE_SINGLE_YEAR_ACT = re.compile(r"(?<!\d)((?:19|20)\d{2})-(\d{1,2})(?!\d)")

# Regex for vote numbers (1, 5, 1b, 5b, 10, etc.)
_RE_VOTE = re.compile(r"^\d+[a-z]?$")
_RE_YEAR_AFTER_HASH = re.compile(r"__([12]\d{3})(?=[_\-])")
_RE_CHILE_PAGE = re.compile(r"^=== Page (\d+)\.0")
_RE_CHILE_AMOUNT = re.compile(r"^(?:M\$\s*)?([0-9][0-9\.\,\s\u00a0]*)$")
_RE_CHILE_INLINE_AMOUNT = re.compile(r"^(?P<entity>.+?)\s+(?P<amount>[0-9][0-9\.\,]{3,})$")
_RE_CHILE_MINISTRY = re.compile(r"^(MINISTERIO DE|MINISTERIO DEL|MINISTERIO DE LA|MINISTERIO DE LAS)\b", re.IGNORECASE)
_RE_CHILE_ENTITY_SIGNAL = re.compile(
    r"anid|conicyt|investig|ciencia|tecnolog|innov|fomento pesquero|agropecuari|"
    r"forestal|informaci[oó]n de recursos naturales|fundaci[oó]n para la innovaci[oó]n agraria|"
    r"ant[aá]rt|nuclear|millenium|milenium|acuicultura",
    re.IGNORECASE,
)
_RE_CHILE_INSTITUTION_WORD = re.compile(
    r"\b(agencia|comisi[oó]n|comit[eé]|instituto|fundaci[oó]n|centro|oficina|subsecretar[ií]a)\b",
    re.IGNORECASE,
)
_RE_CHILE_EXCLUDE_TEXT = re.compile(
    r"^(ingresos|gastos|transferencias|adquisici[oó]n|iniciativas de inversi[oó]n|"
    r"saldo|servicio de la deuda|moneda nacional|sub-|t[ií]tulo|item|asig\.?|"
    r"denominaciones|glosa|partida|cap[ií]tulo|programa|fisco|tesoro p[úu]blico|"
    r"al gobierno central|al sector privado|a otras entidades p[úu]blicas|"
    r"transferencias corrientes|transferencias de capital|gastos en personal|"
    r"bienes y servicios de consumo|pr[eé]stamos|endeudamiento|ley de presupuestos)",
    re.IGNORECASE,
)
_RE_CHILE_NON_RD_ENTITY = re.compile(
    r"polic[ií]a de investigaciones|servicio local de educaci[oó]n|junta nacional de|"
    r"superintendencia de educaci[oó]n|agencia de calidad de la educaci[oó]n|"
    r"subsecretar[ií]a de educaci[oó]n(?: superior| parvularia)?$|"
    r"fondo nacional de salud|subsecretar[ií]a de redes asistenciales|"
    r"ministerio p[úu]blico|instituto nacional de derechos humanos|"
    r"direcci[oó]n de sanidad|subsecretar[ií]a de salud|subsecretar[ií]a de agricultura|"
    r"oficina de estudios y pol[ií]ticas agrarias|comisi[oó]n nacional del medio ambiente|"
    r"subsecretar[ií]a de bienes nacionales|subsecretar[ií]a de vivienda y urbanismo",
    re.IGNORECASE,
)
_RE_CHILE_LEADING_NUMERIC = re.compile(r"^(?:[0-9]{1,3}(?:[.,][0-9]{3})+|[0-9]{1,3}(?:\s+[0-9]{1,3}){1,3})\s+")
_RE_CHILE_LEADING_BULLET = re.compile(r"^(?:[-–—]+\s*)+")
_RE_CHILE_PROGRAM_SUFFIX = re.compile(r"\bprograma\s+\d+\b$", re.IGNORECASE)
_RE_CHILE_KNOWN_RD_ENTITY = re.compile(
    r"agencia nacional de investigaci[oó]n y desarrollo|"
    r"comisi[oó]n nacional de investigaci[oó]n cient[ií]fica y tecnol[oó]gica|"
    r"comisi[oó]n chilena de energ[ií]a nuclear|"
    r"instituto ant[aá]rtico chileno|"
    r"instituto de investigaciones agropecuarias|"
    r"fundaci[oó]n para la innovaci[oó]n agraria|"
    r"instituto de fomento pesquero|"
    r"centro de informaci[oó]n de recursos naturales|"
    r"instituto forestal|"
    r"instituto de salud p[úu]blica de chile|"
    r"comit[eé] innova chile|"
    r"fondo de fomento ciencia y tecnolog[ií]a|"
    r"fondo de investigaci[oó]n pesquera|"
    r"iniciativa cient[ií]fica mille?nnium|"
    r"centro ant[aá]rtico internacional|"
    r"acceso a la informaci[oó]n electr[oó]nica para ciencia y tecnolog[ií]a|"
    r"apoyo innovaci[oó]n educaci[oó]n superior|"
    r"centros tecnol[oó]gicos|centros de excelencia|"
    r"consorcios tecnol[oó]gicos|fomento de la ciencia y la tecnolog[ií]a|"
    r"internacionalizaci[oó]n del esfuerzo innovador|"
    r"fie-innovaci[oó]n e i&d empresarial|"
    r"convocatoria proyectos de innovaci[oó]n agraria",
    re.IGNORECASE,
)
_CHILE_TARGET_MINISTRIES = {
    "ministerio de educacion",
    "ministerio de ciencia tecnologia conocimiento e innovacion",
    "ministerio de economia fomento y turismo",
    "ministerio de economia fomento y reconstruccion",
    "ministerio de agricultura",
    "ministerio de relaciones exteriores",
    "ministerio de energia",
    "ministerio de salud",
}
_RE_PORTUGAL_PAGE = re.compile(r"^=== Page (\d+)(?:\.0)?")
_RE_PORTUGAL_AMOUNT = re.compile(r"^-?\s*\d{1,3}(?:[ .]\d{3})+(?:,\d+)?\s*$")
_RE_PORTUGAL_MAP = re.compile(r"\bMAPA\s+(V|VII)\b", re.IGNORECASE)
_RE_PORTUGAL_SKIP_PAGE = re.compile(
    r"RESPONSABILIDADES CONTRATUAIS PLURIANUAIS|"
    r"MAPA\s+XIV|"
    r"FREGUESIA|"
    r"MUNIC[IÍ]PIO|"
    r"DISTRITO|"
    r"\bRA\b",
    re.IGNORECASE,
)
_RE_PORTUGAL_TARGET_ENTITY = re.compile(
    r"FUNDA[ÇC][AÃ]O PARA A CI[ÊE]NCIA E (?:A )?TECNOLOGIA|"
    r"\bFCT\b|"
    r"JUNTA NACIONAL DE INVESTIGA[ÇC][AÃ]O CIENT[IÍ]FICA E TECNOL[ÓO]GICA|"
    r"\bJNICT\b|"
    r"LABORAT[ÓO]RIO NACIONAL DE ENGENHARIA CIVIL|"
    r"\bLNEC\b|"
    r"AG[ÊE]NCIA NACIONAL DE INOVA[ÇC][AÃ]O|"
    r"\bANI\b",
    re.IGNORECASE,
)
_RE_PORTUGAL_ENTITY_START = re.compile(
    r"^(FUNDA[ÇC][AÃ]O|FUNDO|INSTITUTO|LABORAT[ÓO]RIO|AG[ÊE]NCIA|AGENCIA|"
    r"JUNTA|UNIVERSIDADE|ESCOLA|SAS\b|UL\b|UTL\b|CP\b|METRO\b|AUTORIDADE|"
    r"COMISS[ÃA]O|ENTIDADE|REGI[ÃA]O|TURISMO|ADMINISTRA[ÇC][ÃA]O|OPART\b|"
    r"RADIO\b|TEATRO\b|COA\b|IMAR\b|ISCTE\b|MOBI\.E|POLIS\b|SPGM\b|MARINA\b|"
    r"INSTITUI[ÇC][ÃA]O\b|CENTRO\b|AICEP\b|CINEMATECA\b|COFRE\b|ASSEMBLEIA\b|"
    r"PRESID[ÊE]NCIA\b|TRIBUNAL\b|SERVI[ÇC]O\b)",
    re.IGNORECASE,
)
_PORTUGAL_ENTITY_MIN_AMOUNT = {
    "fct": 1_000_000.0,
    "jnict": 1_000_000.0,
    "lnec": 1_000_000.0,
    "ani": 500_000.0,
}


def _parse_fiscal_year(filename: str) -> Optional[int]:
    """Extract the first calendar year from a fiscal year string like '2023-24'."""
    m_hash = _RE_YEAR_AFTER_HASH.search(filename)
    if m_hash:
        return int(m_hash.group(1))

    m = _RE_FISCAL_YEAR.search(filename)
    if m:
        year = int(m.group(1))
        tail = m.group(2)
        # Distinguish true fiscal-year strings like 2023-24 / 2023-2024 from
        # older Canada cache filenames like 1987-3 where the suffix is just an
        # act number. One- or two-digit tails that are far from year+1 should be
        # treated as single-year act files, not fiscal years.
        if len(tail) <= 2:
            tail_num = int(tail)
            next_two = (year + 1) % 100
            if tail_num in {next_two, year % 100}:
                return year
        elif len(tail) == 4:
            if int(tail) in {year, year + 1}:
                return year

    m_act = _RE_SINGLE_YEAR_ACT.search(filename)
    if m_act:
        return int(m_act.group(1))
    # Try plain 4-digit year
    m2 = re.search(r"\b(19|20)\d{2}\b", filename)
    if m2:
        return int(m2.group(0))
    return None


def _cache_style_file_id(path: Path) -> str:
    return hashlib.md5(str(path).encode()).hexdigest()[:12]


def _render_pages_to_cache_text(pages: list[object]) -> str:
    rendered: list[str] = []
    for pg in pages:
        method = getattr(pg, "method", "unknown")
        page_num = getattr(pg, "page_num", 0)
        rendered.append(f"=== Page {page_num} | method: {method} ===")
        text = str(getattr(pg, "text", "") or "").rstrip()
        if text:
            rendered.append(text)
    return "\n".join(rendered) + "\n"


def _is_caps_agency_name(line: str) -> bool:
    """Return True if the line looks like an ALL-CAPS agency header."""
    stripped = line.strip()
    if len(stripped) < 10:
        return False
    # Must be mostly uppercase letters (allow spaces, hyphens, parens, etc.)
    alpha = [c for c in stripped if c.isalpha()]
    if not alpha:
        return False
    upper_ratio = sum(1 for c in alpha if c.isupper()) / len(alpha)
    return upper_ratio >= 0.85 and stripped[0].isupper()


def _parse_amount(s: str) -> Optional[float]:
    """Parse a comma-formatted number string to float, or None."""
    s = s.strip().lstrip("–").strip()
    s = s.replace(",", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _extract_agency_total(amounts: list[float]) -> Optional[float]:
    """
    Given a list of amounts from an agency block, extract the total.

    If the last number equals the sum of all preceding (within 5%), it IS the total.
    Otherwise assume two-column format (main estimates, interim pairs) —
    take every other number starting at index 0 (main estimates) and sum.
    """
    if not amounts:
        return None
    if len(amounts) == 1:
        return amounts[0]

    last = amounts[-1]
    preceding = amounts[:-1]
    preceding_sum = sum(preceding)

    if preceding_sum > 0 and abs(last - preceding_sum) / preceding_sum < 0.05:
        # Last is a total
        return last

    # Two-column format: main estimates at even indices (0, 2, 4, ...)
    main_estimates = [a for i, a in enumerate(amounts) if i % 2 == 0]
    return sum(main_estimates)


def _split_into_agency_blocks(lines: list[str]) -> list[tuple[str, list[str]]]:
    """
    Split text lines into (agency_name, block_lines) pairs.
    A new block starts when we see an ALL-CAPS agency name.
    """
    blocks: list[tuple[str, list[str]]] = []
    current_name: Optional[str] = None
    current_lines: list[str] = []

    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if _is_caps_agency_name(stripped):
            # Could be multi-line agency name (continued on next line)
            name_parts = [stripped]
            j = i + 1
            while j < len(lines):
                next_stripped = lines[j].strip()
                if (
                    _is_caps_agency_name(next_stripped)
                    and not _is_french_translation(next_stripped)
                    and len(next_stripped) > 5
                    # continuation: doesn't start a completely new context
                    and not re.match(r"^\d", next_stripped)
                ):
                    name_parts.append(next_stripped)
                    j += 1
                else:
                    break

            full_name = " ".join(name_parts)

            if current_name is not None:
                blocks.append((current_name, current_lines))

            current_name = full_name
            current_lines = []
            i = j
        else:
            if current_name is not None:
                current_lines.append(stripped)
            i += 1

    if current_name is not None and current_lines:
        blocks.append((current_name, current_lines))

    return blocks


def _is_french_translation(line: str) -> bool:
    """Heuristic: French agency names often have lowercase accented chars."""
    accented = set("éèêëàâùûîïôœç")
    return any(c in accented for c in line.lower())


def _extract_amounts_from_block(block_lines: list[str]) -> list[float]:
    """Extract all standalone dollar amounts from a block's lines."""
    amounts = []
    for line in block_lines:
        stripped = line.strip()
        # Skip vote numbers, descriptions, page markers
        if not stripped:
            continue
        if stripped.startswith("==="):
            continue
        if stripped.startswith("–") and not re.match(r"^–?\s*[\d,]+\s*$", stripped):
            continue
        if _RE_VOTE.match(stripped):
            continue
        # Try to parse as amount
        amt = _parse_amount(stripped)
        if amt is not None and amt > 0:
            amounts.append(amt)
    return amounts


def _extract_vote_amounts_from_block(block_lines: list[str]) -> list[float]:
    """
    Extract the first appropriation amount after each vote line.

    Canada appropriation schedules often list one vote number followed by a
    description and then multiple numeric columns. The first numeric value after
    each vote is the clearest deterministic proxy for the current appropriation
    amount; later values are often prior/granted/cumulative columns that can
    create huge false totals if summed blindly.
    """
    vote_amounts: list[float] = []
    i = 0
    while i < len(block_lines):
        stripped = block_lines[i].strip()
        if not _RE_VOTE.match(stripped):
            i += 1
            continue

        found = None
        j = i + 1
        while j < len(block_lines):
            nxt = block_lines[j].strip()
            if not nxt:
                j += 1
                continue
            if nxt.startswith("==="):
                break
            if _RE_VOTE.match(nxt):
                break
            amt = _parse_amount(nxt)
            if amt is not None and amt > 0:
                found = amt
                break
            j += 1

        if found is not None:
            vote_amounts.append(found)
            i = j + 1
        else:
            i += 1

    return vote_amounts


def _clean_chile_line(line: str) -> str:
    line = str(line or "").replace("\t", " ").replace("\xa0", " ")
    line = re.sub(r"\s+", " ", line)
    return line.strip(" .")


def _chile_source_pdf_path(year: int) -> Optional[Path]:
    patterns = [
        f"{year}_Ley de presupuestos.pdf",
        f"{year}_Ley de presupuestos.PDF",
    ]
    for pattern in patterns:
        path = CHILE_SOURCE_PDF_DIR / pattern
        if path.exists():
            return path
    return None


def _chile_text_cache_is_effectively_empty(lines: list[str]) -> bool:
    substantive = 0
    for line in lines:
        cleaned = _clean_chile_line(line)
        if not cleaned:
            continue
        if cleaned.startswith("=== Page"):
            continue
        substantive += 1
        if substantive >= 5:
            return False
    return True


def _render_pages_to_chile_cache_text(pages: list[object]) -> str:
    return _render_pages_to_cache_text(pages).replace("=== Page ", "=== Page ").replace(" | method:", ".0 | method:")


def _normalise_ministry_name(name: str) -> str:
    cleaned = _clean_chile_line(name).lower()
    cleaned = (
        cleaned.replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ñ", "n")
    )
    return re.sub(r"[^a-z0-9 ]+", " ", cleaned).strip()


def _parse_chile_amount(text: str) -> Optional[float]:
    cleaned = _clean_chile_line(text)
    if re.fullmatch(r"[0-9]{1,3}(?:,\s*[0-9]{1,3})+", cleaned):
        return None
    m = _RE_CHILE_AMOUNT.match(cleaned)
    if not m:
        return None
    digits = re.sub(r"[^0-9\.,]", "", m.group(1))
    if not digits:
        return None
    if "." not in digits and "," not in digits:
        if len(digits) < 4:
            return None
        return float(digits)
    digits = digits.replace(".", "").replace(",", ".")
    try:
        value = float(digits)
    except ValueError:
        return None
    if value < 10:
        return None
    return value


def _extract_chile_inline_entity_amount(text: str) -> tuple[Optional[str], Optional[float]]:
    cleaned = _clean_chile_line(text)
    match = _RE_CHILE_INLINE_AMOUNT.match(cleaned)
    if not match:
        return None, None
    amount = _parse_chile_amount(match.group("amount"))
    if amount is None:
        return None, None
    entity = _strip_chile_leading_numeric(match.group("entity"))
    entity = re.sub(r"\s*\|\s*", " ", entity).strip(" -|")
    if not entity:
        return None, None
    return entity, amount


def _strip_chile_leading_numeric(text: str) -> str:
    cleaned = _clean_chile_line(text)
    prev = None
    while cleaned and cleaned != prev:
        prev = cleaned
        cleaned = _RE_CHILE_LEADING_BULLET.sub("", cleaned).strip()
        cleaned = _RE_CHILE_LEADING_NUMERIC.sub("", cleaned).strip()
    cleaned = _RE_CHILE_PROGRAM_SUFFIX.sub("", cleaned).strip()
    return cleaned


def _looks_like_chile_metadata(line: str) -> bool:
    stripped = _clean_chile_line(line)
    if not stripped:
        return True
    if stripped.startswith("==="):
        return True
    if re.fullmatch(r"[0-9]{1,3}", stripped):
        return True
    if re.fullmatch(r"[0-9]{1,3}(?:,\s*[0-9]{1,3})+", stripped):
        return True
    if re.fullmatch(r"[0-9]{1,3}(?:,\s*[0-9]{1,3})*\s*(?:[A-Z]{1,3})?", stripped):
        return True
    if stripped in {"Sub-", "Título", "Item", "Ítem", "Asig.", "Denominaciones", "Glosa", "Nº"}:
        return True
    return False


def _is_chile_candidate_text(text: str, current_ministry: str) -> bool:
    cleaned = _strip_chile_leading_numeric(text)
    if not cleaned or len(cleaned) < 8:
        return False
    if _RE_CHILE_EXCLUDE_TEXT.search(cleaned):
        return False
    if _RE_CHILE_NON_RD_ENTITY.search(cleaned):
        return False
    if _RE_CHILE_MINISTRY.match(cleaned):
        return False
    if _RE_CHILE_KNOWN_RD_ENTITY.search(cleaned):
        return True
    if _RE_CHILE_ENTITY_SIGNAL.search(cleaned):
        if _RE_CHILE_INSTITUTION_WORD.search(cleaned):
            return True
        return _normalise_ministry_name(current_ministry) in _CHILE_TARGET_MINISTRIES
    return False


def _parse_chile_lines(lines: list[str], source_file: str, country: str, year: int) -> list[RawRow]:
    rows: list[RawRow] = []
    seen: set[tuple[int, str, float]] = set()
    page_number = 0
    current_ministry = ""
    i = 0

    while i < len(lines):
        raw = lines[i]
        page_match = _RE_CHILE_PAGE.match(raw.strip())
        if page_match:
            page_number = int(page_match.group(1))
            i += 1
            continue

        line = _clean_chile_line(raw)
        if not line:
            i += 1
            continue

        if _RE_CHILE_MINISTRY.match(line) and (line.isupper() or _normalise_ministry_name(line) in _CHILE_TARGET_MINISTRIES):
            ministry_parts = [line]
            j = i + 1
            while j < len(lines):
                nxt = _clean_chile_line(lines[j])
                if not nxt or nxt.startswith("==="):
                    break
                if _looks_like_chile_metadata(nxt):
                    break
                if _parse_chile_amount(nxt) is not None:
                    break
                if nxt.isupper() or nxt[:1].isupper():
                    ministry_parts.append(nxt)
                    j += 1
                    continue
                break
            current_ministry = " ".join(ministry_parts)
            i = j
            continue

        if _parse_chile_amount(line) is not None:
            i += 1
            continue

        if _looks_like_chile_metadata(line):
            i += 1
            continue

        inline_entity, inline_amount = _extract_chile_inline_entity_amount(line)
        if inline_amount is not None:
            entity = inline_entity or ""
            if entity and _is_chile_candidate_text(entity, current_ministry):
                key = (page_number, entity.lower(), float(inline_amount))
                if key not in seen:
                    seen.add(key)
                    rows.append(
                        RawRow(
                            country=country,
                            year=year,
                            source_file=source_file,
                            page_number=page_number,
                            section_name=current_ministry or entity,
                            entity_raw=entity,
                            amount_current=inline_amount,
                            amount_prior=None,
                            is_header_row=False,
                            is_total_row=False,
                            cells_raw=[],
                        )
                    )
            i += 1
            continue

        entity_parts = [line]
        amount = None
        j = i + 1
        while j < len(lines):
            nxt = _clean_chile_line(lines[j])
            if not nxt:
                j += 1
                continue
            if nxt.startswith("==="):
                break
            if _RE_CHILE_MINISTRY.match(nxt):
                break
            maybe_amount = _parse_chile_amount(nxt)
            if maybe_amount is not None:
                amount = maybe_amount
                break
            if _looks_like_chile_metadata(nxt):
                j += 1
                continue
            entity_parts.append(nxt)
            if len(entity_parts) >= 3:
                break
            j += 1

        entity = _strip_chile_leading_numeric(" ".join(entity_parts))
        if amount is not None and _is_chile_candidate_text(entity, current_ministry):
            key = (page_number, entity.lower(), float(amount))
            if key not in seen:
                seen.add(key)
                rows.append(
                    RawRow(
                        country=country,
                        year=year,
                        source_file=source_file,
                        page_number=page_number,
                        section_name=current_ministry or entity,
                        entity_raw=entity,
                        amount_current=amount,
                        amount_prior=None,
                        is_header_row=False,
                        is_total_row=False,
                        cells_raw=[],
                    )
                )
        i += 1

    return rows


def _parse_chile_text_file(file_path: Path, country: str, year: int) -> list[RawRow]:
    try:
        with gzip.open(file_path, "rt", encoding="utf-8", errors="replace") as f:
            lines = f.read().splitlines()
    except Exception as e:
        logger.warning(f"Could not read {file_path}: {e}")
        return []

    source_file = file_path.stem
    rows = _parse_chile_lines(lines, source_file, country, year)
    if rows:
        logger.info(f"[{country} {year}] {file_path.name}: {len(rows)} Chile rows parsed")
        return rows

    if not (2002 <= year <= 2008):
        logger.info(f"[{country} {year}] {file_path.name}: 0 Chile rows parsed")
        return rows

    if not _chile_text_cache_is_effectively_empty(lines):
        logger.info(f"[{country} {year}] {file_path.name}: 0 Chile rows parsed")
        return rows

    pdf_path = _chile_source_pdf_path(year)
    if pdf_path is None:
        logger.info(f"[{country} {year}] {file_path.name}: 0 Chile rows parsed (no source PDF for OCR fallback)")
        return rows

    try:
        from budget.pdf_reader import extract_pages

        logger.info(f"[{country} {year}] {file_path.name}: empty text cache detected; retrying from PDF OCR fallback")
        pages = extract_pages(
            pdf_path,
            cache_dir=TEXT_CACHE_DIR,
            force_reextract=True,
            ocr_zoom=2.0,
            ocr_langs="eng+spa",
        )
        rendered = _render_pages_to_chile_cache_text(pages)
        with gzip.open(file_path, "wt", encoding="utf-8") as f:
            f.write(rendered)
        ocr_lines = rendered.splitlines()
        rows = _parse_chile_lines(ocr_lines, source_file, country, year)
    except Exception as e:
        logger.warning(f"[{country} {year}] OCR fallback failed for {pdf_path.name}: {e}")
        rows = []

    logger.info(f"[{country} {year}] {file_path.name}: {len(rows)} Chile rows parsed")
    return rows


def _ensure_portugal_text_cache(
    year_range: Optional[tuple[int, int]],
    text_cache_dir: Path = TEXT_CACHE_DIR,
) -> Path:
    country_dir = text_cache_dir / "Portugal"
    country_dir.mkdir(parents=True, exist_ok=True)

    if year_range is None:
        return country_dir

    from budget.pdf_reader import extract_pages

    start, end = year_range
    page_cache_dir = country_dir / "_pagecache"
    for pdf_path in sorted(PORTUGAL_SOURCE_PDF_DIR.glob("*.pdf")):
        year = _parse_fiscal_year(pdf_path.name)
        if year is None or not (start <= year <= end):
            continue
        out_name = f"pdf_{_cache_style_file_id(pdf_path)}__{pdf_path.stem}.txt.gz"
        out_path = country_dir / out_name
        if out_path.exists():
            continue
        pages = extract_pages(
            pdf_path,
            cache_dir=page_cache_dir,
            force_reextract=False,
            ocr_langs="por+eng",
        )
        rendered = _render_pages_to_cache_text(pages)
        with gzip.open(out_path, "wt", encoding="utf-8") as f:
            f.write(rendered)
    return country_dir


def _clean_portugal_line(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _parse_portugal_amount(text: str) -> Optional[float]:
    cleaned = _clean_portugal_line(text)
    if not _RE_PORTUGAL_AMOUNT.match(cleaned):
        return None
    digits = cleaned.replace(" ", "").replace(".", "").replace(",", ".")
    try:
        return float(digits)
    except ValueError:
        return None


def _looks_like_portugal_entity_start(text: str) -> bool:
    cleaned = _clean_portugal_line(text)
    return bool(_RE_PORTUGAL_ENTITY_START.match(cleaned))


def _is_portugal_metadata_line(text: str) -> bool:
    cleaned = _clean_portugal_line(text)
    if not cleaned:
        return True
    upper = cleaned.upper()
    if upper.startswith(("DIÁRIO DA REPÚBLICA", "ANO ECONÓMICO", "PÁGINA", "FONTE:", "DESIGNAÇÃO", "IMPORTÂNCIAS EM")):
        return True
    if upper in {"-", "–", "—", "MAPA V", "MAPA VII"}:
        return True
    if re.fullmatch(r"\d{1,2}", cleaned):
        return True
    return False


def _parse_portugal_page(
    page_lines: list[str],
    source_file: str,
    country: str,
    year: int,
    page_number: int,
) -> list[RawRow]:
    page_text = "\n".join(page_lines)
    if _RE_PORTUGAL_SKIP_PAGE.search(page_text):
        return []
    upper_text = page_text.upper()
    if "DESIGNAÇÃO" not in upper_text:
        return []
    if not _RE_PORTUGAL_TARGET_ENTITY.search(page_text):
        return []
    amount_line_count = sum(1 for raw in page_lines if _parse_portugal_amount(raw) is not None)
    is_map_page = bool(_RE_PORTUGAL_MAP.search(page_text))
    if not is_map_page and amount_line_count < 5:
        return []

    amount_started = False
    entity_started = False
    amounts: list[float] = []
    entities: list[str] = []
    current_entity = ""
    section_bits: list[str] = []

    for raw in page_lines:
        line = _clean_portugal_line(raw)
        if not line:
            continue
        if not amount_started:
            if _RE_PORTUGAL_AMOUNT.match(line):
                amount = _parse_portugal_amount(line)
                if amount is not None:
                    amounts.append(amount)
                    amount_started = True
                continue
            if not _is_portugal_metadata_line(line) and (line.isupper() or line.upper() == line):
                section_bits.append(line)
            continue

        if not entity_started:
            amount = _parse_portugal_amount(line)
            if amount is not None:
                amounts.append(amount)
                continue
            entity_started = True

        upper = line.upper()
        if upper.startswith("DESIGNAÇÃO"):
            break
        if _is_portugal_metadata_line(line):
            continue
        if _looks_like_portugal_entity_start(line):
            if current_entity:
                entities.append(current_entity)
            current_entity = line
        elif current_entity:
            current_entity = f"{current_entity} {line}".strip()
        else:
            current_entity = line

    if current_entity:
        entities.append(current_entity)

    if not amounts or not entities:
        return []

    clean_section_bits = [
        bit
        for bit in section_bits
        if not re.fullmatch(r"\d+", bit)
        and bit.upper() not in {"MAPA V", "MAPA VII", "DESIGNAÇÃO", "IMPORTÂNCIAS EM EUROS"}
    ]
    section_name = " | ".join(clean_section_bits[-4:]).strip()
    rows: list[RawRow] = []
    seen: set[tuple[str, float]] = set()
    for entity, amount in zip(entities, amounts):
        if not _RE_PORTUGAL_TARGET_ENTITY.search(entity):
            continue
        entity_upper = entity.upper()
        if "FUNDA" in entity_upper or "FCT" in entity_upper:
            if amount < _PORTUGAL_ENTITY_MIN_AMOUNT["fct"]:
                continue
        elif "JNICT" in entity_upper or "JUNTA NACIONAL DE INVESTIGA" in entity_upper:
            if amount < _PORTUGAL_ENTITY_MIN_AMOUNT["jnict"]:
                continue
        elif "LNEC" in entity_upper or "LABORAT" in entity_upper:
            if amount < _PORTUGAL_ENTITY_MIN_AMOUNT["lnec"]:
                continue
        elif "AGENCIA NACIONAL DE INOVA" in entity_upper or re.search(r"\bANI\b", entity_upper):
            if amount < _PORTUGAL_ENTITY_MIN_AMOUNT["ani"]:
                continue
        key = (entity.lower(), float(amount))
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            RawRow(
                country=country,
                year=year,
                source_file=source_file,
                page_number=page_number,
                section_name=section_name or entity,
                entity_raw=entity,
                amount_current=amount,
                amount_prior=None,
                is_header_row=False,
                is_total_row=False,
                cells_raw=[],
            )
        )
    return rows


def _parse_portugal_text_file(file_path: Path, country: str, year: int) -> list[RawRow]:
    try:
        with gzip.open(file_path, "rt", encoding="utf-8", errors="replace") as f:
            lines = f.read().splitlines()
    except Exception as e:
        logger.warning(f"Could not read {file_path}: {e}")
        return []

    source_file = file_path.stem
    rows: list[RawRow] = []
    page_number = 0
    page_lines: list[str] = []

    def flush_page() -> None:
        nonlocal rows, page_lines, page_number
        if page_number <= 0 or not page_lines:
            return
        rows.extend(_parse_portugal_page(page_lines, source_file, country, year, page_number))

    for raw in lines:
        m = _RE_PORTUGAL_PAGE.match(raw.strip())
        if m:
            flush_page()
            page_number = int(m.group(1))
            page_lines = []
            continue
        page_lines.append(raw)
    flush_page()

    logger.info(f"[{country} {year}] {file_path.name}: {len(rows)} Portugal rows parsed")
    return rows


def parse_text_file(
    file_path: Path,
    country: str,
    year: int,
) -> list[RawRow]:
    """
    Parse a single .txt.gz file and return RawRow records (one per agency).
    """
    if country == "Chile":
        return _parse_chile_text_file(file_path, country, year)
    if country == "Portugal":
        return _parse_portugal_text_file(file_path, country, year)

    try:
        with gzip.open(file_path, "rt", encoding="utf-8", errors="replace") as f:
            text = f.read()
    except Exception as e:
        logger.warning(f"Could not read {file_path}: {e}")
        return []

    lines = text.splitlines()
    blocks = _split_into_agency_blocks(lines)

    source_file = file_path.stem  # filename without .gz

    rows = []
    for agency_name, block_lines in blocks:
        vote_amounts = _extract_vote_amounts_from_block(block_lines) if country == "Canada" else []
        if vote_amounts:
            total = sum(vote_amounts)
        else:
            amounts = _extract_amounts_from_block(block_lines)
            total = _extract_agency_total(amounts)

        if total is None or total <= 0:
            continue

        row = RawRow(
            country=country,
            year=year,
            source_file=source_file,
            page_number=0,
            section_name=agency_name,
            entity_raw=agency_name,
            amount_current=total,
            amount_prior=None,
            is_header_row=False,
            is_total_row=True,
            cells_raw=[],
        )
        rows.append(row)

    logger.info(f"[{country} {year}] {file_path.name}: {len(rows)} agencies parsed")
    return rows


def parse_text_cache(
    country: str,
    year_range: Optional[tuple[int, int]] = None,
    text_cache_dir: Path = TEXT_CACHE_DIR,
) -> list[RawRow]:
    """
    Parse all .txt.gz files for a country from the text cache directory.

    Args:
        country: Country name (e.g. "Canada")
        year_range: Optional (start_year, end_year) inclusive filter
        text_cache_dir: Root directory containing {country}/*.txt.gz files

    Returns:
        List of RawRow records
    """
    if country == "Portugal" and year_range is not None:
        _ensure_portugal_text_cache(year_range=year_range, text_cache_dir=text_cache_dir)

    country_dir = text_cache_dir / country
    if not country_dir.exists():
        logger.warning(f"Text cache directory not found: {country_dir}")
        return []

    all_rows: list[RawRow] = []
    files_found = 0

    for gz_file in sorted(country_dir.glob("*.txt.gz")):
        year = _parse_fiscal_year(gz_file.name)
        if year is None:
            logger.debug(f"Skipping (no year found): {gz_file.name}")
            continue

        if year_range is not None:
            start, end = year_range
            if not (start <= year <= end):
                continue

        files_found += 1
        rows = parse_text_file(gz_file, country, year)
        all_rows.extend(rows)

    logger.info(
        f"[{country}] Text cache: {files_found} files, {len(all_rows)} agency rows parsed"
    )
    return all_rows
