"""Reporting utilities for line-level budget item extraction and outputs."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from src.translation_utils import translate_to_english_glossary
from src.utils import logger, normalize_text

# Generic budget terms that often indicate header/aggregate lines.
HEADER_TOTAL_TERMS = {
    "nettotal",
    "driftsudgifter",
    "indtægter",
    "indtaegter",
    "tilskud",
    "anlægsudgifter",
    "anlaegsudgifter",
    "kapitalindtægter",
    "kapitalindtaegter",
    "udgifter",
    "indtægter",
    "budget",
    "sum",
    "total",
}

# Program/institution terms that suggest line-item level research spending.
PROGRAM_RESEARCH_TERMS = {
    "forskning",
    "forsknings",
    "research",
    "science",
    "teknologi",
    "technology",
    "innovation",
    "universitet",
    "university",
    "institute",
    "institut",
    "raad",
    "råd",
    "council",
    "laboratory",
    "higher education",
}

MINISTRY_SECTION_TERMS = {
    "ministeriet",
    "ministry",
    "departement",
    "section",
    "kapitel",
}


def _file_label(country_guess: str, year_guess: str, source_filename: str) -> str:
    """Build a compact file label for demos, e.g., Denmark_1975."""
    country = country_guess if country_guess and country_guess != "Unknown" else Path(source_filename).stem
    year = year_guess if year_guess and year_guess != "Unknown" else "UnknownYear"
    return f"{country}_{year}"


def _guess_currency(country_guess: str) -> str:
    """Map country to a likely budget currency."""
    if country_guess == "Denmark":
        return "DKK"
    if country_guess in {"France", "Germany"}:
        return "EUR"
    if country_guess == "United Kingdom":
        return "GBP"
    return "Unknown"


def _split_page_into_lines(text: str) -> list[str]:
    """Split extracted page text into non-empty lines."""
    lines = [line.strip() for line in re.split(r"\r?\n", text or "")]
    return [line for line in lines if line]


def _extract_amounts_from_line(line: str) -> list[tuple[str, int]]:
    """Extract numeric amount candidates from one line."""
    if not line:
        return []
    pattern = r"(?<!\d)(\d{1,3}(?:[.,\s]\d{3})+(?:[.,]\d{2})?|\d{4,})(?!\d)"
    matches = re.findall(pattern, line)

    amounts = []
    seen = set()
    for raw in matches:
        cleaned = re.sub(r"[^\d]", "", raw)
        if not cleaned:
            continue
        value = int(cleaned)
        if value < 10000:
            continue
        if 1900 <= value <= 2099:
            # Avoid picking years as amounts.
            continue
        if raw in seen:
            continue
        seen.add(raw)
        amounts.append((raw, value))
    return amounts


def _contains_any(text_normalized: str, terms: set[str]) -> bool:
    """Check whether any term appears in normalized text."""
    return any(term in text_normalized for term in terms)


def is_header_or_total_line(line: str) -> bool:
    """Return True if a line looks like a ministry/section header or aggregate total."""
    norm = normalize_text(line or "").strip()
    if not norm:
        return False

    if norm.startswith("§"):
        return True

    # Section codes/titles with no program cues are usually headers.
    if re.match(r"^\d{1,2}(?:\.\d{2})?\.?\s+[a-z ]+$", norm):
        if not _contains_any(norm, PROGRAM_RESEARCH_TERMS):
            return True

    has_header_term = _contains_any(norm, HEADER_TOTAL_TERMS)
    has_program_term = _contains_any(norm, PROGRAM_RESEARCH_TERMS)
    has_ministry_term = _contains_any(norm, MINISTRY_SECTION_TERMS)
    amounts = _extract_amounts_from_line(line)

    # Ministry/section labels without clear program entities.
    if has_ministry_term and not has_program_term and len(amounts) <= 1:
        return True

    # Budget aggregate pattern: multiple amounts + generic accounting labels.
    if has_header_term and not has_program_term:
        return True
    if len(amounts) >= 3 and not has_program_term:
        return True

    # A line dominated by totals and little text is likely an aggregate header line.
    alpha_tokens = re.findall(r"[a-z]+", norm)
    if len(amounts) >= 2 and len(alpha_tokens) <= 3 and not has_program_term:
        return True

    return False


def is_program_level_research_line(line: str) -> bool:
    """Return True if a line likely refers to a program-level research/innovation body."""
    norm = normalize_text(line or "").strip()
    if not norm:
        return False

    has_program_term = _contains_any(norm, PROGRAM_RESEARCH_TERMS)
    if not has_program_term:
        return False

    # Reject pure section/header lines even if they contain a broad term.
    if norm.startswith("§") and len(re.findall(r"[a-z]+", norm)) <= 4:
        return False

    return True


def _score_line(line: str) -> tuple[float, list[str], list[str]]:
    """Score line quality for research program amount extraction."""
    norm = normalize_text(line or "")
    positive_hits = sorted([term for term in PROGRAM_RESEARCH_TERMS if term in norm])
    negative_hits = sorted([term for term in HEADER_TOTAL_TERMS if term in norm])

    score = 0.0
    score += 1.2 * len(positive_hits)
    score -= 0.8 * len(negative_hits)

    if norm.startswith("§"):
        score -= 1.0
    if _contains_any(norm, MINISTRY_SECTION_TERMS) and len(positive_hits) == 0:
        score -= 1.5

    return score, positive_hits, negative_hits


def _guess_budget_category(matched_keywords: str, matched_line: str) -> str:
    """Infer a simple budget category from keywords and matched line."""
    kw = normalize_text((matched_keywords or "") + " " + (matched_line or ""))
    if any(token in kw for token in ["research", "recherche", "forskning", "forsknings"]):
        return "research funding"
    if any(token in kw for token in ["university", "universite", "universitet", "higher education", "uddannelse"]):
        return "higher education"
    if any(token in kw for token in ["technology", "technologie", "teknologi", "innovation"]):
        return "innovation and technology"
    if any(token in kw for token in ["ministry", "ministere", "ministeriet", "council", "rad"]):
        return "public administration"
    return "general budget item"


def _to_confidence(score: float, same_line_amount: bool, amount_numeric: int) -> float:
    """Map line score to a 0..1 confidence value."""
    confidence = 0.45 + (0.08 * score)
    if same_line_amount:
        confidence += 0.08
    else:
        confidence -= 0.05
    if amount_numeric < 100000:
        confidence -= 0.10
    return round(max(0.05, min(0.99, confidence)), 3)


def detect_budget_items(keyword_hits_df: pd.DataFrame) -> pd.DataFrame:
    """Detect research-related budget amounts using line-level extraction."""
    if keyword_hits_df.empty:
        return pd.DataFrame(
            columns=[
                "file_id",
                "country",
                "year",
                "page_number",
                "matched_line",
                "context_before",
                "context_after",
                "amount_raw",
                "amount_numeric",
                "currency_guess",
                "category_guess",
                "is_header_total",
                "is_program_level",
                "confidence",
                "rationale",
            ]
        )

    records = []
    seen = set()

    for row in keyword_hits_df.itertuples(index=False):
        page_text = row.text if isinstance(row.text, str) else ""
        lines = _split_page_into_lines(page_text)
        if not lines:
            continue

        source_path_value = getattr(row, "source_filepath", None) or getattr(row, "filepath", "")
        source_filename = Path(source_path_value).name if source_path_value else "unknown.pdf"
        file_label = _file_label(row.country_guess, row.year_guess, source_filename)

        for idx, line in enumerate(lines):
            header_flag = is_header_or_total_line(line)
            program_flag = is_program_level_research_line(line)

            if not program_flag or header_flag:
                continue

            line_score, positive_hits, negative_hits = _score_line(line)
            context_before = lines[idx - 1] if idx > 0 else ""
            context_after = lines[idx + 1] if idx < len(lines) - 1 else ""

            candidate_positions = [idx]
            if not _extract_amounts_from_line(line):
                if idx > 0:
                    candidate_positions.append(idx - 1)
                if idx < len(lines) - 1:
                    candidate_positions.append(idx + 1)

            for pos in candidate_positions:
                amount_line = lines[pos]
                if pos != idx and is_header_or_total_line(amount_line):
                    continue

                for amount_raw, amount_numeric in _extract_amounts_from_line(amount_line):
                    dedup_key = (row.file_id, row.page_number, idx, pos, amount_raw)
                    if dedup_key in seen:
                        continue
                    seen.add(dedup_key)

                    same_line_amount = pos == idx
                    confidence = _to_confidence(line_score, same_line_amount, amount_numeric)
                    rationale_parts = [
                        f"program_terms={','.join(positive_hits) if positive_hits else 'none'}",
                        f"header_terms={','.join(negative_hits) if negative_hits else 'none'}",
                        f"amount_source={'same_line' if same_line_amount else 'neighbor_line'}",
                    ]
                    if is_header_or_total_line(amount_line):
                        rationale_parts.append("rejected_header_amount_line")

                    category_guess = _guess_budget_category(str(getattr(row, "matched_keywords", "")), line)

                    records.append(
                        {
                            "file_id": row.file_id,
                            "country": row.country_guess,
                            "year": row.year_guess,
                            "page_number": row.page_number,
                            "matched_line": line,
                            "context_before": context_before,
                            "context_after": context_after,
                            "amount_raw": amount_raw,
                            "amount_numeric": amount_numeric,
                            "currency_guess": _guess_currency(row.country_guess),
                            "category_guess": category_guess,
                            "is_header_total": False,
                            "is_program_level": True,
                            "confidence": confidence,
                            "rationale": "; ".join(rationale_parts),
                            # Backward-compatible fields used by text/json/excel outputs.
                            "file_label": file_label,
                            "source_filename": source_filename,
                            "keywords_matched": getattr(row, "matched_keywords", ""),
                            "text_snippet": line,
                            "text_snippet_en": translate_to_english_glossary(line),
                            "detected_amount_raw": amount_raw,
                            "detected_amount_value": amount_numeric,
                            "detected_currency": _guess_currency(row.country_guess),
                        }
                    )

    budget_df = pd.DataFrame(records)
    if not budget_df.empty:
        budget_df = budget_df.sort_values(
            ["confidence", "detected_amount_value", "file_label", "page_number"],
            ascending=[False, False, True, True],
        ).reset_index(drop=True)

    logger.info("Line-level budget candidates detected: %s", len(budget_df))
    return budget_df


def build_results_text(budget_df: pd.DataFrame) -> str:
    """Build a plain-text results report focused on budget items."""
    lines = ["Candidate budget items detected:", ""]
    if budget_df.empty:
        lines.append("No budget items detected.")
    else:
        for row in budget_df.head(10).itertuples(index=False):
            lines.append(f"File: {row.file_label}")
            lines.append(f"Page: {row.page_number}")
            lines.append("Text snippet:")
            lines.append(f'"{row.text_snippet}"')
            lines.append("Text snippet (EN):")
            lines.append(f'"{row.text_snippet_en}"')
            lines.append(f"Detected amount: {row.detected_amount_raw} {row.detected_currency}")
            lines.append(f"Category guess: {row.category_guess}")
            lines.append("")
    return "\n".join(lines).strip() + "\n"


def build_results_json_records(budget_df: pd.DataFrame) -> list[dict]:
    """Build JSON-serializable records matching the presentation format."""
    if budget_df.empty:
        return []

    records = []
    for row in budget_df.itertuples(index=False):
        records.append(
            {
                "File": row.file_label,
                "Page": int(row.page_number),
                "Text snippet": row.text_snippet,
                "Text snippet (EN)": row.text_snippet_en,
                "Detected amount": f"{row.detected_amount_raw} {row.detected_currency}",
                "Category guess": row.category_guess,
                "Confidence": row.confidence,
                "Rationale": row.rationale,
            }
        )
    return records
