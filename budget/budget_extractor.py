"""Budget extraction engine: taxonomy scoring + section-aware parsing.

Pipeline:
  1. Group pages by file to carry § section context across consecutive pages.
  2. For each page, parse all budget lines (section headers, sub-sections,
     line items) using the Danish Finanslov structure.
  3. Score each line using the full context window:
       section_name + line description + neighboring lines
  4. Keep lines where taxonomy score >= REVIEW_THRESHOLD (1).
  5. Return one row per detected budget amount with standardized columns.

Why we process ALL pages (not just keyword candidates):
  A § section header may appear on a page that has no R&D keywords, but
  the sub-items on subsequent pages need that context to score correctly.
  E.g. "§ 20. Undervisningsministeriet" might appear on page 45 (no
  keywords) while "20.31. Universiteter ... 387.000.000" is on page 47.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

import re

from budget.config import DK_SECTION_MAP
from budget.section_parser import (
    parse_page_lines,
    _RE_AMOUNT_TAIL, _RE_MILL_AMOUNT, _RE_STANDALONE_AMOUNT,
    _RE_SUBITEM, _RE_SUBSECTION, _RE_ITEM_HEADER, _RE_PROGRAM_HEADER,
)
from budget.temporal_smoothing import compute_temporal_prior
from budget.taxonomy import INCLUDE_THRESHOLD, REVIEW_THRESHOLD, load_taxonomy, score_text
from budget.translation_utils import translate_to_english_glossary, preclean_text
from budget.spain_extractor import extract_spain_items
from budget.uk_extractor import extract_uk_items
from budget.canada_extractor import extract_canada_items
from budget.australia_extractor import extract_australia_items
from budget.belgium_extractor import extract_belgium_items
from budget.utils import logger

# Countries with dedicated extractors that bypass the Danish pipeline entirely
_COUNTRY_DEDICATED_EXTRACTORS: frozenset[str] = frozenset({"Spain", "United Kingdom", "Canada", "Australia", "Belgium"})

# Countries whose available PDFs are not suitable for R&D extraction
_COUNTRY_SKIP_EXTRACTORS: frozenset[str] = frozenset()

# Country → primary document language (for taxonomy extensions)
_COUNTRY_LANGUAGES: dict[str, tuple[str, ...]] = {
    "Denmark":        ("danish",),
    "France":         ("french",),
    "Germany":        ("german",),
    "Sweden":         ("swedish",),
    "Norway":         ("norwegian",),
    "Finland":        ("swedish",),   # Swedish-language docs common in Finnish archives
    "Netherlands":    (),             # English taxonomy only for now
    "Belgium":        ("french",),
    "United Kingdom": (),             # English taxonomy is already the base
}

# Heuristics / keyword sets for validation
_BUDGET_KEYWORDS = {
    "driftsudgifter", "tilskud", "anlægsudgifter", "anlægstilskud",
    "operating", "grants", "appropriation", "budget", "udgifter", "indtægter",
}
_POS_KEYWORDS = {
    "forskning", "forsknings", "videnskab", "videnskabelige",
    "teknologisk", "rumforskning", "laboratorium", "laboratoriet",
    "institut", "undersøgelser", "research", "science", "technology",
}
_NEG_KEYWORDS = {
    "kursus", "skole", "pension", "bibliotek", "social", "blindesamfund",
    "kultur", "stipendier", "kirke", "bolig", "kørsel",
}
_LEGAL_REF_RE = re.compile(r"^(ændringer i medfør|medfør af|L\\s*\\d{2,3}|\\d{4}\\s*§|§\\s*\\d{2,3})", re.IGNORECASE)
_CODE_RE = re.compile(r"\b\d{1,2}\.\d{2}(?:\.\d{1,2})?\b")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _clean_legal_prefix(text: str) -> tuple[str, bool]:
    """Drop leading legal references before the first valid item code."""
    cleaned = text.strip()
    m = _CODE_RE.search(cleaned)
    if not m:
        return cleaned, False
    prefix = cleaned[:m.start()]
    if prefix and re.search(r"\d{3,4}\s*§|\d{3}\s*\d{4}\s*§|L\s*\d{2,3}", prefix, re.IGNORECASE):
        return cleaned[m.start():].lstrip(" .,:;-"), True
    return cleaned, False


def _extract_budget_fields(desc_raw: str) -> dict:
    """
    Split a raw description into budget_type, program_code, program_description.
    Also flags merged adjacent codes.
    """
    desc = preclean_text(desc_raw)
    desc, cleaned_prefix = _clean_legal_prefix(desc)

    budget_type = ""
    bt_match = re.match(r"(tilskud|driftsudgifter|anlægsudgifter|indtægter)\s+", desc, re.IGNORECASE)
    if bt_match:
        budget_type = bt_match.group(1).lower()
        desc = desc[bt_match.end():].strip()

    code_matches = list(_CODE_RE.finditer(desc))
    program_code = ""
    program_description = desc
    merged_adjacent = False
    if code_matches:
        program_code = code_matches[0].group()
        program_description = desc[code_matches[0].end():].strip(" .,:;-")
        if len(code_matches) > 1:
            merged_adjacent = True

    return {
        "budget_type": budget_type,
        "program_code": program_code,
        "program_description": program_description,
        "cleaned_from_legal_prefix": cleaned_prefix,
        "merged_adjacent": merged_adjacent,
    }


def _quality(decision: str, parse_error: bool) -> str:
    if parse_error:
        return "low"
    if decision == "include":
        return "high"
    return "medium"

def _currency(country: str, year: str = "") -> str:
    """Return ISO 4217 currency code, historically correct.

    Most Euro-area countries adopted EUR on 1 Jan 2002 (coins/notes).
    Finance bills before that year used national currencies.
    """
    try:
        yr = int(year)
    except (ValueError, TypeError):
        yr = 9999  # unknown year → use modern currency

    pre_euro = yr < 2002

    if country == "Denmark":
        return "DKK"          # never joined Eurozone
    if country == "Sweden":
        return "SEK"          # never joined Eurozone
    if country == "Norway":
        return "NOK"          # not EU
    if country == "United Kingdom":
        return "GBP"          # never joined Eurozone
    if country == "France":
        return "FRF" if pre_euro else "EUR"
    if country == "Germany":
        return "DEM" if pre_euro else "EUR"
    if country == "Netherlands":
        return "NLG" if pre_euro else "EUR"
    if country == "Belgium":
        return "BEF" if pre_euro else "EUR"
    if country == "Finland":
        return "FIM" if pre_euro else "EUR"
    if country == "Austria":
        return "ATS" if pre_euro else "EUR"
    if country == "Italy":
        return "ITL" if pre_euro else "EUR"
    if country == "Spain":
        return "ESP" if pre_euro else "EUR"
    if country == "Portugal":
        return "PTE" if pre_euro else "EUR"
    if country == "Ireland":
        return "IEP" if pre_euro else "EUR"
    return "Unknown"


def _file_label(country: str, year: str, filename: str) -> str:
    c = country if country not in ("", "Unknown") else Path(filename).stem
    y = year if year not in ("", "Unknown") else "UnknownYear"
    return f"{c}_{y}"


def _filepath_from_row(row: object, filepath_col: str) -> str:
    """Get filepath from a named tuple row, handling both column names."""
    val = getattr(row, filepath_col, None)
    if val:
        return str(val)
    # Fallback to other common column names
    for attr in ("filepath", "source_filepath"):
        v = getattr(row, attr, None)
        if v:
            return str(v)
    return "unknown.pdf"


def _pillar(rd_category: str, hits: list[str], scoring_text: str) -> str:
    """Return human-readable pillar label (no A–H codes)."""
    s = scoring_text.lower()
    if rd_category == "excluded":
        return "Exclusions"
    if rd_category == "innovation_system":
        return "Innovation"
    if rd_category == "direct_rd":
        return "Direct R&D"
    if rd_category == "institution_funding":
        return "Institutional"
    if rd_category == "sectoral_rd":
        return "Sectoral"
    # Budget / instruments
    if any("instrument" in h or "budget" in h for h in hits):
        return "Budget"
    # Ambiguous bucket
    if any("anchor" in h or "(-context)" in h for h in hits):
        return "Ambiguous"
    # If tech/innovation words appear, treat as Innovation
    if re.search(r"teknolog|innovation|patent", s):
        return "Innovation"
    return "Ambiguous"


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        # Time-series key
        "country", "year",
        # Budget structure
        "section_code", "section_name", "section_name_en",
        "program_code", "program_description", "program_description_en", "budget_type",
        "item_code", "item_description",
        "line_code", "line_description", "line_description_en",
        # Amount
        "amount_local", "currency", "amount_raw",
        # Classification
        "rd_category", "pillar", "rd_label", "taxonomy_score", "taxonomy_hits",
        "decision", "confidence", "rationale",
        "parse_quality", "parse_error_type", "cleaned_from_legal_prefix",
        # Provenance
        "source_file", "page_number", "file_id",
        # Legacy aliases
        "file_label", "source_filename", "keywords_matched",
        "text_snippet", "text_snippet_en",
        "detected_amount_raw", "detected_amount_value", "detected_currency",
        "is_header_total", "is_program_level",
    ])


# ── Main extractor ────────────────────────────────────────────────────────────

def extract_budget_items(
    pages_df: pd.DataFrame,
    prior_results_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Extract R&D-relevant budget line items from page-level text.

    Accepts either:
    - pages_df format (columns: file_id, filepath, country_guess, ...)
    - candidates_df format (columns: file_id, source_filepath, country_guess, ...)

    Processes pages in document order to maintain § section context.
    """
    if pages_df.empty:
        return _empty_df()

    records: list[dict] = []
    seen: set[tuple] = set()

    # Determine which column holds the filepath
    filepath_col = "filepath" if "filepath" in pages_df.columns else "source_filepath"

    for file_id, file_df in pages_df.groupby("file_id", sort=False):
        section_code = ""
        section_name = ""
        current_program_code = ""
        current_program_desc = ""
        sorted_pages = file_df.sort_values("page_number")

        # Detect country once per file to load the right language taxonomy
        first_row = sorted_pages.iloc[0]
        country_for_file = str(first_row.get("country_guess", "Unknown")
                               if hasattr(first_row, "get") else
                               getattr(first_row, "country_guess", "Unknown"))

        # ── Countries to skip (wrong document type) ──────────────────────────
        if country_for_file in _COUNTRY_SKIP_EXTRACTORS:
            filepath_val = _filepath_from_row(first_row, filepath_col)
            logger.debug(
                "Skipping %s (country=%s): PDF type not suitable for R&D extraction.",
                Path(filepath_val).name, country_for_file,
            )
            continue

        # ── Country-specific extractors (bypass Danish pipeline) ──────────────
        if country_for_file in _COUNTRY_DEDICATED_EXTRACTORS:
            year_for_file = str(first_row.get("year_guess", "Unknown")
                                if hasattr(first_row, "get") else
                                getattr(first_row, "year_guess", "Unknown"))
            filepath_val = _filepath_from_row(first_row, filepath_col)
            source_fn = Path(filepath_val).name
            if country_for_file == "Spain":
                spain_records = extract_spain_items(
                    sorted_pages,
                    file_id=str(file_id),
                    country=country_for_file,
                    year=year_for_file,
                    source_filename=source_fn,
                )
                # Deduplicate: if two files have the same year and program code,
                # keep only the first (handles duplicate BOE files like
                # "2023 BOE-A-2022-22128.pdf" vs "BOE-A-2022-22128-consolidado para 2023.pdf")
                existing_keys = {
                    (r.get("year", ""), r.get("program_code", ""))
                    for r in records
                    if r.get("country") == "Spain"
                }
                for rec in spain_records:
                    key = (rec.get("year", ""), rec.get("program_code", ""))
                    if key not in existing_keys:
                        records.append(rec)
                        existing_keys.add(key)

            elif country_for_file == "United Kingdom":
                uk_records = extract_uk_items(
                    sorted_pages,
                    file_id=str(file_id),
                    country=country_for_file,
                    year=year_for_file,
                    source_filename=source_fn,
                )
                # Deduplicate by year + program_code
                existing_keys_uk = {
                    (r.get("year", ""), r.get("program_code", ""))
                    for r in records
                    if r.get("country") == "United Kingdom"
                }
                for rec in uk_records:
                    key = (rec.get("year", ""), rec.get("program_code", ""))
                    if key not in existing_keys_uk:
                        records.append(rec)
                        existing_keys_uk.add(key)

            elif country_for_file == "Canada":
                canada_records = extract_canada_items(
                    sorted_pages,
                    file_id=str(file_id),
                    country=country_for_file,
                    year=year_for_file,
                    source_filename=source_fn,
                )
                # Deduplicate: same (year, program_code, amount_bin) across multiple Acts
                existing_keys_ca = {
                    (r.get("year", ""), r.get("program_code", ""), int(round(r.get("amount_local", 0), -4)))
                    for r in records
                    if r.get("country") == "Canada"
                }
                for rec in canada_records:
                    key = (rec.get("year", ""), rec.get("program_code", ""), int(round(rec.get("amount_local", 0), -4)))
                    if key not in existing_keys_ca:
                        records.append(rec)
                        existing_keys_ca.add(key)

            elif country_for_file == "Australia":
                au_records = extract_australia_items(
                    sorted_pages,
                    file_id=str(file_id),
                    country=country_for_file,
                    year=year_for_file,
                    source_filename=source_fn,
                )
                existing_keys_au = {
                    (r.get("year", ""), r.get("program_code", ""))
                    for r in records
                    if r.get("country") == "Australia"
                }
                for rec in au_records:
                    key = (rec.get("year", ""), rec.get("program_code", ""))
                    if key not in existing_keys_au:
                        records.append(rec)
                        existing_keys_au.add(key)

            elif country_for_file == "Belgium":
                be_records = extract_belgium_items(
                    sorted_pages,
                    file_id=str(file_id),
                    country=country_for_file,
                    year=year_for_file,
                    source_filename=source_fn,
                )
                existing_keys_be = {
                    (r.get("year", ""), r.get("program_code", ""), int(round(r.get("amount_local") or 0, -3)))
                    for r in records
                    if r.get("country") == "Belgium"
                }
                for rec in be_records:
                    key = (rec.get("year", ""), rec.get("program_code", ""), int(round(rec.get("amount_local") or 0, -3)))
                    if key not in existing_keys_be:
                        records.append(rec)
                        existing_keys_be.add(key)

            continue  # skip Danish pipeline for this file

        langs = _COUNTRY_LANGUAGES.get(country_for_file, ())
        tax = load_taxonomy(languages=tuple(langs))

        # Detect if this file uses Mill. kr. amounts (1991+ Danish Finance Bills).
        # Strategy: count standalone amounts in each format and use majority vote.
        #   Old kr. format:    "7.711.455.939" — \d{1,3}([.,]\d{3})+
        #   Mill. kr. format:  "745,0"          — \d{1,4}[,\s]\d{1,2}
        # If mill-format amounts outnumber kr-format amounts, enable mill_kr_mode.
        _re_old_amount = re.compile(
            r"^\s*[÷\-\+]?\s*\d{1,3}(?:[.,]\d{3})+\s*\d?\)?\s*$"
        )
        _re_mill_amount_detect = re.compile(
            r"^\s*[-÷]?\s*\d{1,4}(?:\.\d{3})*[,\s]\d{1,2}\s*\d?\)?\s*$"
        )
        mill_count = 0
        old_count = 0
        for r in sorted_pages.itertuples(index=False):
            for line in str(r.text if isinstance(r.text, str) else "").splitlines():
                if _re_mill_amount_detect.match(line):
                    mill_count += 1
                if _re_old_amount.match(line):
                    old_count += 1
        mill_kr_mode = mill_count > old_count and mill_count > 20

        in_artsoversigt = False  # carried across pages within the same file

        for row in sorted_pages.itertuples(index=False):
            page_text = row.text if isinstance(row.text, str) else ""
            if not page_text.strip():
                continue

            filepath = _filepath_from_row(row, filepath_col)
            country = str(row.country_guess)
            year = str(row.year_guess)
            source_filename = Path(filepath).name
            currency = _currency(country, year)
            file_lbl = _file_label(country, year, source_filename)

            budget_lines, section_code, section_name, in_artsoversigt = parse_page_lines(
                page_text, section_code, section_name,
                mill_kr_mode=mill_kr_mode, in_artsoversigt=in_artsoversigt,
            )

            for bl in budget_lines:
                # Reset program context when a top-level group header is encountered
                # (e.g. "20. Anlægs- og udlånsbevillinger"). Without this, section-level
                # totals on the following lines (Anlægsudgifter 779M, etc.) would be
                # falsely attributed to the last R&D program from the previous page.
                if bl.line_type == "group_header":
                    current_program_code = ""
                    current_program_desc = ""
                    continue

                # Skip items from legal annotation sections ("Tekstanmærkninger").
                # These have section_name like "ad 20.16.12., 20.17.01.,..."
                # and contain legal references to specific budget lines, not
                # actual budget appropriations.
                if re.match(r"^ad\s+\d{1,2}\.\d{2}", preclean_text(bl.section_name), re.IGNORECASE):
                    continue

                # Maintain program context (headers with codes but no amount)
                if bl.amount_value == 0:
                    if bl.line_code and _CODE_RE.match(bl.line_code) and not _LEGAL_REF_RE.match(preclean_text(bl.description)):
                        current_program_code = bl.line_code
                        current_program_desc = bl.description
                    continue  # no extractable amount on this line

                # Merge description with next line only when the current line is very short
                # and context_after is not a standalone amount (old-format or mill-kr).
                # Merging with a bare number (e.g. the secondary column in 1991+ bills)
                # adds spurious digits that confuse later digit-presence filters.
                merged_desc = bl.description
                _ctx_after_is_amount = bool(
                    _RE_AMOUNT_TAIL.search(bl.context_after)
                    or _RE_STANDALONE_AMOUNT.match(bl.context_after)
                    or (mill_kr_mode and _RE_MILL_AMOUNT.match(bl.context_after))
                )
                # Don't merge when context_after starts a new budget line
                _ctx_after_is_new_line = bool(
                    _RE_SUBITEM.match(bl.context_after)
                    or _RE_SUBSECTION.match(bl.context_after)
                    or _RE_ITEM_HEADER.match(bl.context_after)
                    or _RE_PROGRAM_HEADER.match(bl.context_after)
                )
                if (
                    bl.context_after
                    and not _ctx_after_is_amount
                    and not _ctx_after_is_new_line
                    and len(bl.description.split()) < 5
                    and not _LEGAL_REF_RE.match(bl.description)
                ):
                    merged_desc = f"{bl.description} {bl.context_after}".strip()

                # Program inheritance: if this line has a code, refresh program context.
                # Lines without their own code (bl.line_code == "") are either sub-items
                # (inherit parent program_code) or group-level labels (do NOT inherit).
                # Distinguishing heuristic: a line with no code AND a very large amount
                # that appears to be a group subtotal should not inherit program context.
                # We detect this by checking bl.item_code: sub-items have it set to the
                # PARENT subsection code (from the section parser's current_item_code);
                # orphan group labels also have it set. So we use a secondary check:
                # if bl.line_code is empty AND bl.item_code matches current_program_code,
                # treat as a legitimate sub-item; otherwise it may be a group label.
                program_code = current_program_code
                program_desc = current_program_desc
                if bl.line_code and _CODE_RE.match(bl.line_code):
                    program_code = bl.line_code
                    program_desc = merged_desc
                    current_program_code = program_code
                    current_program_desc = program_desc
                elif not bl.line_code and bl.item_code and bl.item_code != current_program_code:
                    # This line's inherited item_code differs from current program —
                    # likely a group label appearing between subsections. Don't inherit.
                    program_code = ""
                    program_desc = ""

                # Budget type detection
                desc_clean = preclean_text(merged_desc)
                desc_lower = merged_desc.lower()
                budget_type = ""
                for kw in ("driftsudgifter", "anlægsudgifter", "anlægstilskud", "tilskud"):
                    if kw in desc_lower:
                        budget_type = kw
                        break

                # In mill_kr_mode, no-code lines whose ORIGINAL description contains no
                # digits and no budget-type keyword are artsoversigt category labels
                # (e.g. "Forskning og universitetsuddannelser", "Støtteordninger").
                # They inherit program_code from the preceding subsection but represent
                # ministry-level category subtotals. Clear inherited context so they
                # fail the has_code filter below.
                # NOTE: use bl.description (original line) not merged_desc to avoid
                # spurious digits from merged context_after amounts.
                # Strip parenthetical footnote refs like "(tekstanm. 184)" before the
                # digit check — these are annotations, not program-code digits.
                if mill_kr_mode and not bl.line_code and not budget_type:
                    orig_desc_clean = preclean_text(bl.description)
                    orig_no_parens = re.sub(r"\([^)]*\)", "", orig_desc_clean).strip()
                    if not any(c.isdigit() for c in orig_no_parens):
                        program_code = ""
                        program_desc = ""

                # Exclude revenue / income lines
                if re.search(r"indtægter|revenue|income", desc_lower):
                    continue

                # Derive program code/description from the line itself if present
                code_in_line = _CODE_RE.search(desc_clean)
                if code_in_line:
                    derived_code = code_in_line.group()
                    derived_desc = desc_clean[code_in_line.end():].strip(" .,:;-")
                    if not program_code:
                        program_code = derived_code
                        current_program_code = derived_code
                    if derived_desc and not program_desc:
                        program_desc = derived_desc
                        current_program_desc = derived_desc

                # Two-level scoring driven by program description when available
                section_clean = preclean_text(bl.section_name)
                desc_clean = preclean_text(merged_desc)
                prog_clean = preclean_text(program_desc)

                # Use item info for alignment
                item_code = bl.item_code or program_code
                item_desc = bl.item_description or program_desc

                scoring_text = prog_clean or desc_clean
                # When prog_clean is a ministry/agency name (e.g. "Teknologistyrelsen")
                # and desc_clean embeds a code+description (e.g. "52.01 Teknologisk
                # Service"), combine both so taxonomy scoring sees the full context.
                if prog_clean and _CODE_RE.search(desc_clean):
                    scoring_text = f"{prog_clean} {desc_clean}".strip()
                # When scoring_text is just a budget type (e.g. "Driftsudgifter")
                # with no R&D signal, supplement with item_desc (e.g.
                # "Forskningssekretariatet") so that the has_rd_word check succeeds
                # and the item is correctly classified as "include".
                _RD_SIGNAL_RE = re.compile(
                    r"forskning|forsøgs|udvikling|research|teknolog|universit|videnskab",
                    re.IGNORECASE,
                )
                item_clean = preclean_text(item_desc)
                if item_clean and not _RD_SIGNAL_RE.search(scoring_text):
                    scoring_text = f"{item_clean} {scoring_text}".strip()
                content = f"{section_clean} {scoring_text}"
                content_score, content_hits, content_cat = score_text(content, tax)
                desc_score, desc_hits, _ = score_text(scoring_text, tax)
                if _RD_SIGNAL_RE.search(scoring_text):
                    desc_score = max(desc_score, INCLUDE_THRESHOLD)

                context = (
                    f"{section_clean} "
                    f"{scoring_text} "
                    f"{bl.context_before} "
                    f"{bl.context_after}"
                )
                context_score, context_hits, context_cat = score_text(context, tax)

                # Use whichever set of hits is richer for reporting
                if context_score > content_score:
                    score, hits, category = context_score, context_hits, context_cat
                else:
                    score, hits, category = content_score, content_hits, content_cat

                # ── Validation filters ─────────────────────────────────
                # Always skip ministry-level aggregate labels (Artsoversigt block)
                _desc_lower_clean = desc_clean.lower()
                if _desc_lower_clean in {
                    "bevilling i alt", "beviling i alt", "aktivitet i alt",
                    "nettostyrede aktiviteter", "udgifter i alt",
                    "artsoversigt", "artsoversigt:",
                    "driftsindtægter", "anlægsindtægter", "overførselsudgifter",
                    "skatter og overførselsindtægter",
                    "overførsler mellem offentlige myndigheder", "finansielle poster",
                }:
                    continue
                if not any(char.isdigit() for char in desc_clean) and not program_code:
                    continue  # likely headers
                if re.fullmatch(r"[0-9 .,÷\-]+", desc_clean.strip()):
                    continue  # pure number (mill-kr or kr amount that became a description)
                if ("......" in merged_desc or "......" in bl.description
                        or "......" in program_desc):
                    continue  # table-of-contents entry (dotted leaders + page number)
                if _LEGAL_REF_RE.match(desc_clean):
                    continue
                if len(desc_clean.split()) < 2 and not program_code:
                    continue
                if program_code and _LEGAL_REF_RE.match(prog_clean):
                    continue
                has_code = bool(program_code or _CODE_RE.search(desc_clean) or _CODE_RE.search(section_clean))
                has_budget_kw = any(k in desc_clean.lower() for k in _BUDGET_KEYWORDS)
                if not (has_code or has_budget_kw):
                    continue
                has_pos = any(k in desc_clean.lower() for k in _POS_KEYWORDS)
                has_neg = any(k in desc_clean.lower() for k in _NEG_KEYWORDS)
                if has_neg and not has_pos:
                    continue
                # "Tilskud under [Ministry]" is a Danish pension/transfer
                # allocation pattern — grants routed through one ministry
                # to pay pension obligations. Not R&D spending.
                # NOTE: use ORIGINAL text (before preclean_text) because preclean_text
                # collapses "Tilskud under Undervisningsministeriet" → "Undervisningsministeriet",
                # making the filter regex fail on the cleaned text.
                _raw_prog = program_desc or bl.item_description or ""
                _raw_desc = bl.description or ""
                _full_text_raw = f"{_raw_prog} {_raw_desc} {merged_desc}"
                if re.search(r"\btilskud under\b.+ministeriet", _full_text_raw, re.IGNORECASE):
                    continue
                # Non-deductible VAT refunds and leisure education — not R&D
                if re.search(r"\bkøbsmoms\b|\bfritidsundervisning\b", desc_clean, re.IGNORECASE):
                    continue
                if desc_clean.lower() in {
                    "tilskud", "driftsudgifter", "anlægsudgifter", "anlægstilskud",
                    "indtægter", "udlån", "kapitalindtægter",
                } and not program_code:
                    continue
                if desc_score == 0 and context_score < INCLUDE_THRESHOLD:
                    continue

                # Pre-filter boost: ensure named innovation bodies are never
                # filtered out even when surrounding context is sparse.
                _boost_match = re.search(r"teknologisk service|teknologirådet|tekniske prøvenævn", scoring_text, re.IGNORECASE)
                if _boost_match:
                    content_score = max(content_score, INCLUDE_THRESHOLD)
                    context_score = max(context_score, REVIEW_THRESHOLD)
                    score = max(score, content_score, context_score)
                    category = "innovation_system"

                temporal_prior = compute_temporal_prior(
                    country=country,
                    year=year,
                    section_code=bl.section_code,
                    program_code=program_code,
                    program_description=program_desc,
                    line_description=display_desc if "display_desc" in locals() else merged_desc,
                    history_df=prior_results_df,
                )
                smoothed_score = score + temporal_prior.boost
                if max(context_score, smoothed_score) < REVIEW_THRESHOLD:
                    continue

                dedup = (source_filename, int(row.page_number), program_code or bl.line_code, budget_type, bl.raw_amount)

                # Skip generic legal adjustments not informative for R&D
                if re.search(r"ændringer i medfør", desc_clean, re.IGNORECASE):
                    continue

                # Fallback: ensure program/item fields filled from description if still empty
                if not program_code:
                    m_pc = _CODE_RE.search(desc_clean)
                    if m_pc:
                        program_code = m_pc.group()
                        if not program_desc:
                            tail = desc_clean[m_pc.end():].strip(" .,:;-")
                            if tail:
                                program_desc = tail
                if not item_code and program_code:
                    item_code = program_code
                if not item_desc and program_desc:
                    item_desc = program_desc

                has_rd_word = bool(re.search(r"forskning|forsøgs|udvikling|research|teknolog", scoring_text, re.IGNORECASE))
                decision = "include" if (
                    smoothed_score >= INCLUDE_THRESHOLD and (desc_score > 0 or has_rd_word)
                ) else "review"

                parse_error = False
                # Validation: if the ORIGINAL description (not merged) has a code
                # different from item_code, flag. Using bl.description avoids
                # false positives when context_after merge adds a code from the
                # next item (that code belongs to the next item, not this one).
                code_in_orig = _CODE_RE.search(preclean_text(bl.description))
                code_in_desc = _CODE_RE.search(desc_clean)
                if (code_in_orig and item_code
                        and code_in_orig.group() != item_code
                        and code_in_orig.group() != bl.line_code):
                    # Only flag if the description's code is neither the item's
                    # own code nor its parent context — avoids false-flagging
                    # correctly parsed program items whose inherited item_code
                    # is the parent subsection.
                    parse_error = True
                # If the ORIGINAL line (before merge) contains a budget keyword
                # immediately followed by a code, the line is likely malformed.
                # Use bl.description (pre-merge) to avoid false positives when the
                # context_after merge attaches a code-bearing header line.
                orig_lower = bl.description.lower()
                if re.search(r"(tilskud|driftsudgifter|anlægsudgifter|indtægter)\s+\d{1,2}\.\d{2}", orig_lower):
                    parse_error = True
                if parse_error:
                    decision = "review"

                # Post-parse_error boost: named innovation bodies always include,
                # even if parse_error was set due to merged context_after codes.
                if _boost_match:
                    decision = "include"

                # Confidence based on multiple signals
                confidence = 0.40 + 0.10 * smoothed_score
                if has_code:
                    confidence += 0.10
                if has_budget_kw:
                    confidence += 0.10
                if has_pos:
                    confidence += 0.05
                if has_neg:
                    confidence -= 0.10
                if temporal_prior.boost:
                    confidence += 0.03
                confidence = round(min(0.99, max(0.05, confidence)), 3)
                # Enhance display description when the line is a generic budget type
                display_desc = f"{program_code} {program_desc}".strip() if program_code else (program_desc or merged_desc)

                snippet_en = translate_to_english_glossary(display_desc)
                merged_en = translate_to_english_glossary(desc_clean)
                program_desc_en = translate_to_english_glossary(program_desc) if program_desc else ""
                section_en = translate_to_english_glossary(section_clean)
                if country.lower() == "denmark":
                    section_en = DK_SECTION_MAP.get(bl.section_code, section_en)
                pillar = _pillar(category, hits, scoring_text)

                rationale = (
                    f"score={score}; "
                    f"hits=[{', '.join(hits[:6])}]; "
                    f"section={bl.section_code}"
                )
                if temporal_prior.boost:
                    rationale = f"{rationale}; prior_boost={temporal_prior.boost}; prior_match={temporal_prior.match_type}; prior_years={temporal_prior.matched_years}"

                records.append({
                    # ── Time-series key ──────────────────────────────────
                    "country": country,
                    "year": year,
                    # ── Budget structure ─────────────────────────────────
                    "section_code": bl.section_code,
                    "section_name": bl.section_name,
                    "section_name_en": section_en,
                    "program_code": program_code,
                    "program_description": program_desc,
                    "program_description_en": program_desc_en,
                    "budget_type": budget_type,
                    "item_code": item_code,
                    "item_description": item_desc,
                    "line_code": bl.line_code,
                    "line_description": display_desc,
                    "line_description_en": snippet_en,
                    "merged_line": merged_desc,
                    "merged_line_en": merged_en,
                    "raw_line": bl.original_line,
                    "context_before": bl.context_before,
                    "context_after": bl.context_after,
                    "line_type": bl.line_type,
                    # ── Amount ───────────────────────────────────────────
                    "amount_local": bl.amount_value,
                    "currency": currency,
                    "amount_raw": bl.raw_amount,
                    # ── Classification ───────────────────────────────────
                    "rd_category": category,
                    "pillar": pillar,
                    "rd_label": pillar if pillar else category,
                    "taxonomy_score": score,
                    "smoothed_taxonomy_score": smoothed_score,
                    "content_score": content_score,
                    "context_score": context_score,
                    "taxonomy_hits": "; ".join(hits[:8]),
                    "decision": decision,        # "include" | "review"
                    "confidence": confidence,
                    "parse_error": parse_error,
                    "temporal_prior_boost": temporal_prior.boost,
                    "temporal_prior_match_type": temporal_prior.match_type,
                    "temporal_prior_years": temporal_prior.matched_years,
                    "rationale": rationale,
                    # ── Provenance ───────────────────────────────────────
                    "source_file": source_filename,
                    "page_number": row.page_number,
                    "file_id": file_id,
                    # ── Legacy aliases (used by reporting/main.py) ───────
                    "file_label": file_lbl,
                    "source_filename": source_filename,
                    "keywords_matched": "; ".join(hits),
                    "text_snippet": bl.description,
                    "text_snippet_en": snippet_en,
                    "detected_amount_raw": bl.raw_amount,
                    "detected_amount_value": bl.amount_value,
                    "detected_currency": currency,
                    "is_header_total": bl.line_type == "section_header",
                    "is_program_level": bl.line_type in ("subsection", "line_item"),
                })

    df = pd.DataFrame(records)
    if not df.empty:
        # Ensure legacy columns exist (dedicated extractors may not set them)
        for _col in ("file_label", "source_filename", "keywords_matched",
                     "text_snippet", "text_snippet_en", "detected_amount_raw",
                     "detected_amount_value", "detected_currency",
                     "is_header_total", "is_program_level"):
            if _col not in df.columns:
                df[_col] = ""
        df = df.sort_values(
            ["taxonomy_score", "amount_local", "file_label", "page_number"],
            ascending=[False, False, True, True],
        ).reset_index(drop=True)

        # Deduplicate: A.Oversigter. summary items and B.Bevillinger. detail items
        # may both appear for the same program code and amount. Pages are in order,
        # so keep the FIRST occurrence (A.Oversigter. comes before B.Bevillinger.).
        # Key: (file_id, program_code, amount_local) — same program + same amount
        # in the same file is almost certainly a duplicate.
        if "file_id" in df.columns and "program_code" in df.columns:
            dup_key = ["file_id", "program_code", "amount_local"]
            has_prog = df["program_code"].notna() & (df["program_code"] != "")
            df_with_prog = df[has_prog].copy()
            df_no_prog = df[~has_prog].copy()
            df_with_prog = (
                df_with_prog.sort_values(["file_id", "program_code", "page_number"])
                .drop_duplicates(subset=dup_key, keep="first")
            )

            # Remove parent subsection aggregates when their children are present.
            # E.g. "20.61 Universiteter" (8.3B) should be dropped if "20.61.01",
            # "20.61.02", etc. also appear — those children sum to ≈ parent total
            # and keeping both inflates totals.
            if not df_with_prog.empty:
                prog = df_with_prog["program_code"].astype(str)
                file_id_col = df_with_prog["file_id"].astype(str)
                # Build set of (file_id, program_code) pairs already present
                child_keys: set[tuple[str, str]] = set(
                    zip(file_id_col, prog)
                )
                # A row is a "parent aggregate" if another row in the same file
                # has a program_code that starts with this code + "."
                def _is_covered_parent(row: "pd.Series") -> bool:
                    prefix = str(row["program_code"]) + "."
                    fid = str(row["file_id"])
                    return any(
                        pc.startswith(prefix) for (f, pc) in child_keys
                        if f == fid and pc != str(row["program_code"])
                    )
                parent_mask = df_with_prog.apply(_is_covered_parent, axis=1)
                df_with_prog = df_with_prog[~parent_mask]

            df = pd.concat([df_with_prog, df_no_prog], ignore_index=True)
            df = df.sort_values(
                ["taxonomy_score", "amount_local", "file_label", "page_number"],
                ascending=[False, False, True, True],
            ).reset_index(drop=True)

    logger.info("Budget items extracted: %s", len(df))
    return df
