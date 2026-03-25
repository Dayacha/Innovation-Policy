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
from budget.section_parser import parse_page_lines, _RE_AMOUNT_TAIL
from budget.taxonomy import INCLUDE_THRESHOLD, REVIEW_THRESHOLD, load_taxonomy, score_text
from budget.translation_utils import translate_to_english_glossary, preclean_text
from budget.utils import logger

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

def extract_budget_items(pages_df: pd.DataFrame) -> pd.DataFrame:
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
        langs = _COUNTRY_LANGUAGES.get(country_for_file, ())
        tax = load_taxonomy(languages=tuple(langs))

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

            budget_lines, section_code, section_name = parse_page_lines(
                page_text, section_code, section_name
            )

            for bl in budget_lines:
                # Maintain program context (headers with codes but no amount)
                if bl.amount_value == 0:
                    if bl.line_code and _CODE_RE.match(bl.line_code) and not _LEGAL_REF_RE.match(preclean_text(bl.description)):
                        current_program_code = bl.line_code
                        current_program_desc = bl.description
                    continue  # no extractable amount on this line

                # Merge description with next line only when the current line is very short
                merged_desc = bl.description
                if (
                    bl.context_after
                    and not _RE_AMOUNT_TAIL.search(bl.context_after)
                    and len(bl.description.split()) < 5
                    and not _LEGAL_REF_RE.match(bl.description)
                ):
                    merged_desc = f"{bl.description} {bl.context_after}".strip()

                # Program inheritance: if this line has a code, refresh program context
                program_code = current_program_code
                program_desc = current_program_desc
                if bl.line_code and _CODE_RE.match(bl.line_code):
                    program_code = bl.line_code
                    program_desc = merged_desc
                    current_program_code = program_code
                    current_program_desc = program_desc

                # Budget type detection
                desc_clean = preclean_text(merged_desc)
                desc_lower = merged_desc.lower()
                budget_type = ""
                for kw in ("tilskud", "driftsudgifter", "anlægsudgifter", "anlægstilskud"):
                    if kw in desc_lower:
                        budget_type = kw
                        break
                if not budget_type and merged_desc:
                    budget_type = merged_desc.split()[0].lower()

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
                content = f"{section_clean} {scoring_text}"
                content_score, content_hits, content_cat = score_text(content, tax)
                desc_score, desc_hits, _ = score_text(scoring_text, tax)
                if re.search(r"forskning|forsøgs|udvikling|research|teknolog", scoring_text, re.IGNORECASE):
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
                if not any(char.isdigit() for char in desc_clean) and not program_code:
                    continue  # likely headers
                if re.fullmatch(r"[0-9 .]+", desc_clean.strip()):
                    continue  # totals
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
                if desc_clean.lower() in {"tilskud", "driftsudgifter", "indtægter"} and not program_code:
                    continue
                if desc_score == 0 and context_score < INCLUDE_THRESHOLD:
                    continue

                if context_score < REVIEW_THRESHOLD:
                    continue

                dedup = (source_filename, int(row.page_number), program_code or bl.line_code, budget_type, bl.raw_amount)

                # Boost for key innovation terms we care about
                if re.search(r"teknologisk service|teknologirådet|tekniske prøvenævn", scoring_text, re.IGNORECASE):
                    content_score = max(content_score, INCLUDE_THRESHOLD)
                    score = max(score, content_score, context_score)
                    category = "innovation_system"
                    decision = "include"

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
                    score >= INCLUDE_THRESHOLD and (desc_score > 0 or has_rd_word)
                ) else "review"

                parse_error = False
                # Validation: if description has a code different from item_code, flag
                code_in_desc = _CODE_RE.search(desc_clean)
                if code_in_desc and item_code and code_in_desc.group() != item_code:
                    parse_error = True
                # If budget_type contains an inline code pattern after keyword
                if re.search(r"(tilskud|driftsudgifter|anlægsudgifter|indtægter)\s+\d{1,2}\.\d{2}", desc_lower):
                    parse_error = True
                if parse_error:
                    decision = "review"

                # Confidence based on multiple signals
                confidence = 0.40 + 0.10 * score
                if has_code:
                    confidence += 0.10
                if has_budget_kw:
                    confidence += 0.10
                if has_pos:
                    confidence += 0.05
                if has_neg:
                    confidence -= 0.10
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
                    "content_score": content_score,
                    "context_score": context_score,
                    "taxonomy_hits": "; ".join(hits[:8]),
                    "decision": decision,        # "include" | "review"
                    "confidence": confidence,
                    "parse_error": parse_error,
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
        df = df.sort_values(
            ["taxonomy_score", "amount_local", "file_label", "page_number"],
            ascending=[False, False, True, True],
        ).reset_index(drop=True)

    logger.info("Budget items extracted: %s", len(df))
    return df
