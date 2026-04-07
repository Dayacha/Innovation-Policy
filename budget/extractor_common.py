"""Shared helpers for budget extraction pipelines."""

from __future__ import annotations

from pathlib import Path
import re

import pandas as pd

from budget.taxonomy import INCLUDE_THRESHOLD, REVIEW_THRESHOLD, score_text
from budget.translation_utils import translate_to_english_glossary, preclean_text

# Generic program code pattern (e.g. "20.31", "3.02.1")
# Used internally by clean_legal_prefix and extract_budget_fields.
_CODE_RE = re.compile(r"\b\d{1,2}\.\d{2}(?:\.\d{1,2})?\b")


def clean_legal_prefix(text: str) -> tuple[str, bool]:
    """Drop leading legal references before the first valid item code."""
    cleaned = text.strip()
    m = _CODE_RE.search(cleaned)
    if not m:
        return cleaned, False
    prefix = cleaned[:m.start()]
    if prefix and re.search(r"\d{3,4}\s*§|\d{3}\s*\d{4}\s*§|L\s*\d{2,3}", prefix, re.IGNORECASE):
        return cleaned[m.start():].lstrip(" .,:;-"), True
    return cleaned, False


def extract_budget_fields(desc_raw: str) -> dict:
    """Split a raw description into budget-type and code fields."""
    desc = preclean_text(desc_raw)
    desc, cleaned_prefix = clean_legal_prefix(desc)

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


def quality(decision: str, parse_error: bool) -> str:
    if parse_error:
        return "low"
    if decision == "include":
        return "high"
    return "medium"


def currency(country: str, year: str = "") -> str:
    """Return ISO 4217 currency code, historically correct."""
    try:
        yr = int(year)
    except (ValueError, TypeError):
        yr = 9999

    pre_euro = yr < 2002

    if country == "Denmark":
        return "DKK"
    if country == "Sweden":
        return "SEK"
    if country == "Norway":
        return "NOK"
    if country == "United Kingdom":
        return "GBP"
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


def file_label(country: str, year: str, filename: str) -> str:
    c = country if country not in ("", "Unknown") else Path(filename).stem
    y = year if year not in ("", "Unknown") else "UnknownYear"
    return f"{c}_{y}"


def filepath_from_row(row: object, filepath_col: str) -> str:
    """Get filepath from a named tuple row, handling both column names."""
    val = getattr(row, filepath_col, None)
    if val:
        return str(val)
    for attr in ("filepath", "source_filepath"):
        v = getattr(row, attr, None)
        if v:
            return str(v)
    return "unknown.pdf"


def pillar(rd_category: str, hits: list[str], scoring_text: str) -> str:
    """Return human-readable pillar label (no A-H codes)."""
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
    if any("instrument" in h or "budget" in h for h in hits):
        return "Budget"
    if any("anchor" in h or "(-context)" in h for h in hits):
        return "Ambiguous"
    if re.search(r"teknolog|innovation|patent", s):
        return "Innovation"
    return "Ambiguous"


def empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "country", "year",
        "section_code", "section_name", "section_name_en",
        "program_code", "program_description", "program_description_en", "budget_type",
        "item_code", "item_description",
        "line_code", "line_description", "line_description_en",
        "amount_local", "currency", "amount_raw",
        "rd_category", "pillar", "rd_label", "taxonomy_score", "taxonomy_hits",
        "decision", "confidence", "rationale",
        "parse_quality", "parse_error_type", "cleaned_from_legal_prefix",
        "source_file", "page_number", "file_id",
        "file_label", "source_filename", "keywords_matched",
        "text_snippet", "text_snippet_en",
        "detected_amount_raw", "detected_amount_value", "detected_currency",
        "is_header_total", "is_program_level",
    ])


def enrich_dedicated_record(rec: dict, tax) -> dict:
    """Normalize dedicated extractor rows into the full budget item schema."""
    row = dict(rec)

    country_name = str(row.get("country", "") or "")
    year = str(row.get("year", "") or "")
    section_name = str(row.get("section_name", "") or "")
    section_name_en = str(
        row.get("section_name_en")
        or translate_to_english_glossary(section_name)
        or section_name
    )
    program_code = str(row.get("program_code", "") or "")
    program_description = str(
        row.get("program_description")
        or row.get("item_description")
        or row.get("line_description")
        or ""
    )
    program_description_en = str(
        row.get("program_description_en")
        or translate_to_english_glossary(program_description)
        or program_description
    )
    line_description = str(row.get("line_description", "") or program_description)
    line_description_en = str(
        row.get("line_description_en")
        or translate_to_english_glossary(line_description)
        or line_description
    )
    budget_type = str(row.get("budget_type", "") or "")
    text_snippet = str(row.get("text_snippet", "") or line_description)
    merged_line = str(row.get("merged_line", "") or line_description)
    raw_line = str(row.get("raw_line", "") or line_description)
    raw_amount = str(row.get("amount_raw", "") or row.get("detected_amount_raw", "") or "")
    amount_local = row.get("amount_local", 0)
    row_currency = str(row.get("currency", "") or currency(country_name, year))
    default_context_before = " | ".join(
        part for part in (
            f"{row.get('section_code', '')} {section_name}".strip(),
            f"{program_code} {program_description}".strip(),
        )
        if part and part.strip()
    )
    default_context_after = " | ".join(
        part for part in (
            raw_amount,
            f"{amount_local} {row_currency}".strip() if amount_local not in ("", None) else "",
            line_description_en,
        )
        if str(part).strip()
    )
    if not merged_line:
        merged_line = line_description
    if not raw_line:
        raw_line = merged_line
    scoring_text = " ".join(
        part for part in (
            section_name,
            section_name_en,
            program_description,
            line_description,
            text_snippet,
        )
        if part
    )
    score, hits, category = score_text(scoring_text, tax)
    existing_score = float(row.get("taxonomy_score") or 0)
    taxonomy_score = max(existing_score, score)
    rd_category = str(row.get("rd_category", "") or category or "direct_rd")
    row_pillar = str(row.get("pillar", "") or pillar(rd_category, hits, scoring_text))
    decision = str(row.get("decision", "") or "")
    if not decision:
        if taxonomy_score >= INCLUDE_THRESHOLD:
            decision = "include"
        elif taxonomy_score >= REVIEW_THRESHOLD:
            decision = "review"
        else:
            decision = "exclude"

    parse_error = bool(row.get("parse_error", False))
    confidence = row.get("confidence")
    if confidence in ("", None):
        confidence = 0.40 + 0.08 * taxonomy_score + (0.05 if hits else 0.0)
    confidence = round(min(0.99, max(0.05, float(confidence))), 3)

    rationale = str(row.get("rationale", "") or "").strip()
    if not rationale:
        rationale = (
            f"Dedicated extractor ({country_name})"
            f"; taxonomy_score={taxonomy_score}"
            f"; hits=[{', '.join(hits[:6])}]"
        )

    file_id = row.get("file_id", "")
    source_file = str(row.get("source_file", "") or row.get("source_filename", "") or "")
    page_number = int(row.get("page_number", 1) or 1)

    row.update({
        "country": country_name,
        "year": year,
        "section_code": str(row.get("section_code", "") or ""),
        "section_name": section_name,
        "section_name_en": section_name_en,
        "program_code": program_code,
        "program_description": program_description,
        "program_description_en": program_description_en,
        "budget_type": budget_type,
        "item_code": str(row.get("item_code", "") or program_code),
        "item_description": str(row.get("item_description", "") or program_description),
        "line_code": str(row.get("line_code", "") or ""),
        "line_description": line_description,
        "line_description_en": line_description_en,
        "merged_line": merged_line,
        "merged_line_en": str(
            row.get("merged_line_en")
            or translate_to_english_glossary(merged_line)
            or line_description_en
        ),
        "raw_line": raw_line,
        "context_before": str(row.get("context_before", "") or default_context_before),
        "context_after": str(row.get("context_after", "") or default_context_after),
        "line_type": str(row.get("line_type", "") or "line_item"),
        "amount_local": amount_local,
        "currency": row_currency,
        "amount_raw": raw_amount,
        "rd_category": rd_category,
        "pillar": row_pillar,
        "rd_label": str(row.get("rd_label", "") or row_pillar or rd_category),
        "taxonomy_score": taxonomy_score,
        "smoothed_taxonomy_score": float(row.get("smoothed_taxonomy_score") or taxonomy_score),
        "content_score": float(row.get("content_score") or taxonomy_score),
        "context_score": float(row.get("context_score") or taxonomy_score),
        "taxonomy_hits": str(row.get("taxonomy_hits", "") or "; ".join(hits[:8])),
        "decision": decision,
        "confidence": confidence,
        "parse_error": parse_error,
        "parse_quality": str(row.get("parse_quality", "") or quality(decision, parse_error)),
        "parse_error_type": str(row.get("parse_error_type", "") or ""),
        "cleaned_from_legal_prefix": bool(row.get("cleaned_from_legal_prefix", False)),
        "temporal_prior_boost": float(row.get("temporal_prior_boost") or 0.0),
        "temporal_prior_match_type": str(row.get("temporal_prior_match_type", "") or ""),
        "temporal_prior_years": str(row.get("temporal_prior_years", "") or ""),
        "rationale": rationale,
        "source_file": source_file,
        "page_number": page_number,
        "file_id": file_id,
        "file_label": str(row.get("file_label", "") or file_label(country_name, year, source_file)),
        "source_filename": str(row.get("source_filename", "") or source_file),
        "keywords_matched": str(row.get("keywords_matched", "") or "; ".join(hits)),
        "text_snippet": text_snippet,
        "text_snippet_en": str(
            row.get("text_snippet_en")
            or translate_to_english_glossary(text_snippet)
            or line_description_en
        ),
        "detected_amount_raw": str(row.get("detected_amount_raw", "") or row.get("amount_raw", "") or amount_local),
        "detected_amount_value": row.get("detected_amount_value", amount_local),
        "detected_currency": str(row.get("detected_currency", "") or row_currency),
        "is_header_total": bool(row.get("is_header_total", False) or program_code.endswith("_TOTAL")),
        "is_program_level": bool(row.get("is_program_level", True)),
    })
    return row


__all__ = [
    "_CODE_RE",
    "clean_legal_prefix",
    "currency",
    "empty_df",
    "enrich_dedicated_record",
    "extract_budget_fields",
    "file_label",
    "filepath_from_row",
    "pillar",
    "quality",
]
