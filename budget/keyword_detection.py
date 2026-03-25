"""Keyword detection stage for identifying innovation-related candidate pages."""

import re
from pathlib import Path

import pandas as pd

from budget.config import KEYWORD_HITS_FILE
from budget.language_utils import infer_language_from_hits, language_keyword_hits
from budget.utils import logger


def _build_snippet(text: str, max_len: int = 280) -> str:
    """Create a compact one-line snippet for manual inspection."""
    compact = re.sub(r"\s+", " ", text or "").strip()
    return compact[:max_len]


def detect_candidate_pages(pages_df: pd.DataFrame) -> pd.DataFrame:
    """Detect candidate pages using multilingual keyword matching."""
    if pages_df.empty:
        logger.warning("No page text available for keyword detection.")
        empty_df = pd.DataFrame(
            columns=[
                "candidate_id",
                "file_id",
                "source_filename",
                "source_filepath",
                "country_guess",
                "year_guess",
                "page_number",
                "detected_language",
                "matched_keywords",
                "policy_keyword_count",
                "ministry_keyword_count",
                "keyword_count",
                "candidate_score",
                "text_snippet",
                "text",
            ]
        )
        empty_df.to_csv(KEYWORD_HITS_FILE, index=False, encoding="utf-8")
        return empty_df

    records = []
    for row in pages_df.itertuples(index=False):
        text = row.text if isinstance(row.text, str) else ""
        if not text.strip():
            continue

        hit_map = language_keyword_hits(text)
        detected_language = infer_language_from_hits(hit_map)
        if detected_language == "unknown":
            continue

        language_data = hit_map[detected_language]
        matched_keywords = language_data["all_matches"]
        policy_matches = language_data["policy_matches"]
        ministry_matches = language_data["ministry_matches"]
        keyword_count = len(matched_keywords)
        if keyword_count == 0:
            continue

        # Simple candidate score: unique keyword count with slight text length adjustment.
        candidate_score = keyword_count + min(len(text) / 2000.0, 1.0)
        source_filepath = str(row.filepath)
        source_filename = Path(source_filepath).name

        record = {
            "candidate_id": f"{row.file_id}_p{int(row.page_number):04d}",
            "file_id": row.file_id,
            "source_filename": source_filename,
            "source_filepath": source_filepath,
            "country_guess": row.country_guess,
            "year_guess": row.year_guess,
            "page_number": row.page_number,
            "detected_language": detected_language,
            "matched_keywords": "; ".join(matched_keywords),
            "policy_keyword_count": len(policy_matches),
            "ministry_keyword_count": len(ministry_matches),
            "keyword_count": keyword_count,
            "candidate_score": round(candidate_score, 3),
            "text_snippet": _build_snippet(text),
            "text": text,
        }
        records.append(record)

    candidates_df = pd.DataFrame(records)
    if not candidates_df.empty:
        candidates_df = candidates_df.sort_values(
            ["candidate_score", "keyword_count", "source_filename", "page_number"],
            ascending=[False, False, True, True],
        ).reset_index(drop=True)
    candidates_df.to_csv(KEYWORD_HITS_FILE, index=False, encoding="utf-8")
    logger.info("Keyword hits saved: %s (rows=%s)", KEYWORD_HITS_FILE, len(candidates_df))
    return candidates_df
