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

_PROGRAM_META = {code: (pat, name) for pat, code, name in _AGENCY_PATTERNS}

_TARGETED_MAIN_SCHEDULES: dict[str, tuple[str, ...]] = {
    "1997-2.pdf": ("CA_NRC", "CA_NSERC", "CA_SSHRC"),
    "2001 2 YX2-2001-2.pdf": ("CA_NRC", "CA_SSHRC"),
    "2002 a YX2-2002-1.pdf": ("CA_CIHR",),
}

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
# Older Canadian schedules use short department headers like "JUSTICE",
# "HEALTH", and "INDUSTRY". Requiring 10+ characters lets agency blocks
# absorb the next department's amounts, which creates fragmentary false lows.
# We therefore accept 6+ character all-caps lines as boundaries.
# Allow: periods ("VIA RAIL CANADA INC."), straight apostrophes ("L'IMMIGRATION"),
# curly apostrophes, hyphens, parentheses.
_RE_HEADING = re.compile(r"^[A-ZÀ-ÖØ-Ü][A-ZÀ-ÖØ-Ü\s,\u0027\u2018\u2019()\-.]{5,}$", re.MULTILINE)

_RE_SCHEDULE_COLUMNS = re.compile(
    r"service\s+(?:amount(?:\s*\(\$\))?\s+)?total(?:\s*\(\$\))?"
    r"|n[°o]\s+du\s+cr[ée]dit\s+service\s+montant(?:\s*\(\$\))?\s+total(?:\s*\(\$\))?"
    r"|vote\s+no\.\s+service\s+amount(?:\s*\(\$\))?\s+total(?:\s*\(\$\))?",
    re.IGNORECASE | re.DOTALL,
)


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
        if _RE_HEADING.fullmatch(stripped) and len(stripped) >= 6:
            if current_heading and not current_body_lines:
                current_heading = f"{current_heading}\n{stripped}"
                continue
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


def _extract_agency_window(block_text: str, agency_re: re.Pattern) -> str:
    """Trim a matched block down to the local agency window.

    Some Canadian schedule pages place several department headings on the same
    page, and OCR occasionally causes the broader block split to absorb the next
    department's totals. We therefore re-scan inside the candidate block and
    keep only the matched agency heading plus the following local schedule lines
    until the next all-caps heading appears.
    """
    lines = [line.strip() for line in block_text.splitlines()]
    start_idx: Optional[int] = None
    for i, line in enumerate(lines):
        if agency_re.search(re.sub(r"\s+", " ", line).strip()):
            start_idx = i
            break
    if start_idx is None:
        return block_text

    window: list[str] = []
    for i, line in enumerate(lines[start_idx:], start=start_idx):
        if not line:
            continue
        if i > start_idx and _RE_HEADING.fullmatch(line) and len(line) >= 6:
            break
        window.append(line)
        if len(window) >= 18:
            break
    return "\n".join(window) if window else block_text


def _extract_agency_window_from_page(text: str, agency_re: re.Pattern) -> Optional[str]:
    """Fallback: locate an agency heading anywhere on the page.

    Some bilingual Canadian schedule pages flatten multiple agency headings into
    a single broader department block, so block-start matching misses valid
    agencies. This fallback anchors directly on the agency heading line and
    keeps the next local schedule lines until the next all-caps heading.
    """
    lines = [line.strip() for line in text.splitlines()]
    start_idx: Optional[int] = None
    for i, line in enumerate(lines):
        if agency_re.search(re.sub(r"\s+", " ", line).strip()):
            start_idx = i
            break
    if start_idx is None:
        return None

    window: list[str] = []
    for i, line in enumerate(lines[start_idx:], start=start_idx):
        if not line:
            continue
        if i > start_idx and _RE_HEADING.fullmatch(line) and len(line) >= 6 and not agency_re.search(line):
            break
        window.append(line)
        if len(window) >= 22:
            break
    return "\n".join(window) if window else None


def _schedule_variant(block_text: str) -> str:
    """Classify the appropriation table type for a matched block."""
    lowered = block_text.lower()
    if (
        "interim appropriation granted by this act" in lowered
        or "crédit provisoire accordé par la présente loi" in lowered
    ):
        return "interim"
    if (
        "total ($)" in lowered
        or "amount ($)" in lowered
        or "montant ($)" in lowered
        or "total\n" in lowered
        or _RE_SCHEDULE_COLUMNS.search(block_text)
    ):
        return "full_schedule"
    return "fragment"


def _variant_confidence(variant: str) -> float:
    if variant == "targeted_main_schedule_fallback":
        return 0.92
    if variant == "document_schedule_fallback":
        return 0.88
    if variant == "full_schedule":
        return 0.9
    if variant == "interim":
        return 0.82
    return 0.68


def _variant_rank(variant: str) -> int:
    if variant == "targeted_main_schedule_fallback":
        return 5
    if variant == "document_schedule_fallback":
        return 4
    if variant == "full_schedule":
        return 3
    if variant == "interim":
        return 2
    return 1


def _is_tiny_transfer_fragment(block_text: str, total: float, variant: str) -> bool:
    """Identify fragmentary transfer-only blocks that should be ignored.

    In older supplementary Acts, some agency headings are followed only by
    transfer-authorisation prose like "to authorize the transfer of $492,999"
    without any standalone schedule amount for the agency itself. Those tiny
    transfer figures are not the agency appropriation and create obvious false
    lows in the time series.
    """
    if variant != "fragment":
        return False

    lowered = block_text.lower()
    has_transfer = "transfer of $" in lowered or "virement au présent crédit" in lowered
    has_further_amount = (
        "further amount" in lowered
        or "provide a further amount" in lowered
        or "pourvoir une somme supplémentaire" in lowered
    )
    if has_transfer and total < 30_000_000:
        return True
    if not has_further_amount and total < 1_000_000:
        return True
    return False


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
    seen_programs: set[str] = set()
    blocks = _split_into_blocks(text)

    for heading, body in blocks:
        block_text = heading + "\n" + body
        heading_norm = re.sub(r"\s+", " ", heading).strip()

        for agency_re, prog_code, canonical_name in _AGENCY_PATTERNS:
            # Only match agency names in the HEADING line (ALL-CAPS), not in body text.
            # This prevents footnotes/references to agency names from triggering false matches.
            if not agency_re.search(heading_norm):
                continue

            agency_window = _extract_agency_window(block_text, agency_re)
            variant = _schedule_variant(text + "\n" + agency_window)
            amounts = _extract_amounts_from_block(agency_window)
            total = _get_block_total(amounts)
            if total is None or total < 100_000:
                continue
            if _is_tiny_transfer_fragment(agency_window, total, variant):
                continue

            snippet = agency_window[:300].replace("\n", " ").strip()
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
                    "confidence": _variant_confidence(variant),
                    "source_file": source_filename,
                    "file_id": file_id,
                    "page_number": page_number,
                    "text_snippet": snippet,
                    "raw_line": agency_window[:1200].strip(),
                    "merged_line": snippet,
                    "context_before": heading.strip(),
                    "context_after": agency_window[:900].replace("\n", " ").strip(),
                    "source_variant": variant,
                }
            )
            seen_programs.add(prog_code)
            # Only match each agency once per block
            break

    # Fallback: if block splitting missed an agency, scan the whole page for a
    # local agency window anchored on the heading text itself.
    for agency_re, prog_code, canonical_name in _AGENCY_PATTERNS:
        if prog_code in seen_programs:
            continue
        if not agency_re.search(text):
            continue
        agency_window = _extract_agency_window_from_page(text, agency_re)
        if not agency_window:
            continue
        variant = _schedule_variant(text + "\n" + agency_window)
        amounts = _extract_amounts_from_block(agency_window)
        total = _get_block_total(amounts)
        if total is None or total < 100_000:
            continue
        if _is_tiny_transfer_fragment(agency_window, total, variant):
            continue
        snippet = agency_window[:300].replace("\n", " ").strip()
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
                "confidence": _variant_confidence(variant),
                "source_file": source_filename,
                "file_id": file_id,
                "page_number": page_number,
                "text_snippet": snippet,
                "raw_line": agency_window[:1200].strip(),
                "merged_line": snippet,
                "context_before": "",
                "context_after": agency_window[:900].replace("\n", " ").strip(),
                "source_variant": variant,
            }
        )
        seen_programs.add(prog_code)

    return records


def _extract_missing_agencies_from_document(
    sorted_pages,
    *,
    country: str,
    year: str,
    source_filename: str,
    file_id: str,
    existing_programs: set[str],
) -> list[dict]:
    """Document-level fallback for missed main-schedule agency rows."""
    records: list[dict] = []
    for agency_re, prog_code, canonical_name in _AGENCY_PATTERNS:
        if prog_code in existing_programs:
            continue
        found = False
        for row in sorted_pages.itertuples(index=False):
            pg = int(row.page_number)
            text = row.text if isinstance(row.text, str) else ""
            if not text or not agency_re.search(text):
                continue
            lines = [line.strip() for line in text.splitlines()]
            start_idx: Optional[int] = None
            for i, line in enumerate(lines):
                if agency_re.search(re.sub(r"\s+", " ", line).strip()):
                    start_idx = i
                    break
            if start_idx is None:
                continue

            window: list[str] = []
            for i, line in enumerate(lines[start_idx:], start=start_idx):
                if not line:
                    continue
                if i > start_idx and _RE_HEADING.fullmatch(line) and len(line) >= 6 and not agency_re.search(line):
                    break
                window.append(line)
                if len(window) >= 24:
                    break

            agency_window = "\n".join(window)
            amounts = _extract_amounts_from_block(agency_window)
            total = _get_block_total(amounts)
            if total is None or total < 100_000:
                continue
            if _is_tiny_transfer_fragment(agency_window, total, "document_schedule_fallback"):
                continue

            snippet = agency_window[:300].replace("\n", " ").strip()
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
                    "confidence": _variant_confidence("document_schedule_fallback"),
                    "source_file": source_filename,
                    "file_id": file_id,
                    "page_number": pg,
                    "text_snippet": snippet,
                    "raw_line": agency_window[:1200].strip(),
                    "merged_line": snippet,
                    "context_before": "",
                    "context_after": agency_window[:900].replace("\n", " ").strip(),
                    "source_variant": "document_schedule_fallback",
                }
            )
            found = True
            break
        if found:
            existing_programs.add(prog_code)
    return records


def _extract_targeted_main_schedule_fallback(
    sorted_pages,
    *,
    country: str,
    year: str,
    source_filename: str,
    file_id: str,
    existing_programs: set[str],
) -> list[dict]:
    """Explicit fallback for Canada years manually verified from main schedules."""
    wanted = _TARGETED_MAIN_SCHEDULES.get(source_filename, ())
    if not wanted:
        return []

    page_texts = []
    for row in sorted_pages.itertuples(index=False):
        text = row.text if isinstance(row.text, str) else ""
        page_texts.append((int(row.page_number), text))
    full_text = "\n".join(text for _, text in page_texts)

    records: list[dict] = []
    for prog_code in wanted:
        if prog_code in existing_programs:
            continue
        meta = _PROGRAM_META.get(prog_code)
        if not meta:
            continue
        agency_re, canonical_name = meta
        match = agency_re.search(full_text)
        if not match:
            continue
        window = full_text[match.start(): match.start() + 1600]
        amounts = _extract_amounts_from_block(window)
        total = _get_block_total(amounts)
        if total is None or total < 100_000:
            continue
        page_number = 0
        for pg, text in page_texts:
            if agency_re.search(text):
                page_number = pg
                break
        snippet = window[:300].replace("\n", " ").strip()
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
                "confidence": 0.92,
                "source_file": source_filename,
                "file_id": file_id,
                "page_number": page_number,
                "text_snippet": snippet,
                "raw_line": window[:1200].strip(),
                "merged_line": snippet,
                "context_before": "",
                "context_after": window[:900].replace("\n", " ").strip(),
                "source_variant": "targeted_main_schedule_fallback",
            }
        )
        existing_programs.add(prog_code)
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

    existing_programs = {str(rec.get("program_code", "")) for rec in all_records}
    all_records.extend(
        _extract_missing_agencies_from_document(
            sorted_pages,
            country=country,
            year=year,
            source_filename=source_filename,
            file_id=str(file_id),
            existing_programs=existing_programs,
        )
    )
    existing_programs = {str(rec.get("program_code", "")) for rec in all_records}
    all_records.extend(
        _extract_targeted_main_schedule_fallback(
            sorted_pages,
            country=country,
            year=year,
            source_filename=source_filename,
            file_id=str(file_id),
            existing_programs=existing_programs,
        )
    )

    if not all_records:
        logger.debug(
            "Canada extractor: no R&D agency records found in %s (year %s).",
            source_filename, year,
        )
        return []

    # Keep the single strongest record per agency within a file.  This prevents
    # bilingual/interim/full-schedule duplication while preserving a fallback
    # fragment if that is the only signal present in the Act.
    best_by_program: dict[str, dict] = {}
    for rec in all_records:
        key = rec["program_code"]
        current = best_by_program.get(key)
        if current is None:
            best_by_program[key] = rec
            continue
        new_rank = (
            _variant_rank(str(rec.get("source_variant", ""))),
            float(rec.get("amount_local") or 0),
            float(rec.get("confidence") or 0),
            -int(rec.get("page_number") or 0),
        )
        old_rank = (
            _variant_rank(str(current.get("source_variant", ""))),
            float(current.get("amount_local") or 0),
            float(current.get("confidence") or 0),
            -int(current.get("page_number") or 0),
        )
        if new_rank > old_rank:
            best_by_program[key] = rec

    deduped = list(best_by_program.values())

    logger.debug(
        "Canada extractor: %s (year %s) -> %d agency records (from %d raw)",
        source_filename, year, len(deduped), len(all_records),
    )
    return deduped
