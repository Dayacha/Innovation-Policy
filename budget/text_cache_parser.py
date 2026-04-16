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
import logging
import re
from pathlib import Path
from typing import Optional

from budget import config as cfg
from budget.docx_table_parser import RawRow

logger = logging.getLogger(__name__)

TEXT_CACHE_DIR = Path("Data/output/budget/full_text")

# Regex for ALL-CAPS agency name lines (≥3 caps words, may span multiple lines)
_RE_CAPS_LINE = re.compile(r"^[A-Z][A-Z\s\(\)\-'\.&/,]{10,}$")

# Regex for standalone dollar amounts (possibly with commas or dashes)
_RE_AMOUNT = re.compile(r"^–?\s*([\d,]+)\s*$")

# Regex for fiscal year in filename e.g. "2023-24" or "2023-2024"
_RE_FISCAL_YEAR = re.compile(r"(\d{4})-(\d{2,4})")

# Regex for vote numbers (1, 5, 1b, 5b, 10, etc.)
_RE_VOTE = re.compile(r"^\d+[a-z]?$")


def _parse_fiscal_year(filename: str) -> Optional[int]:
    """Extract the first calendar year from a fiscal year string like '2023-24'."""
    m = _RE_FISCAL_YEAR.search(filename)
    if m:
        return int(m.group(1))
    # Try plain 4-digit year
    m2 = re.search(r"\b(19|20)\d{2}\b", filename)
    if m2:
        return int(m2.group(0))
    return None


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


def parse_text_file(
    file_path: Path,
    country: str,
    year: int,
) -> list[RawRow]:
    """
    Parse a single .txt.gz file and return RawRow records (one per agency).
    """
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
