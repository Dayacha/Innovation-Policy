"""Rule-based placeholder classifier for future LLM integration."""

import pandas as pd

from src.utils import normalize_text


def _classify_text_block(text: str, keyword_count: int) -> dict:
    """Return a lightweight relevance classification from text heuristics."""
    normalized = normalize_text(text or "")

    higher_ed_terms = ["university", "universite", "universitet", "higher education", "enseignement superieur"]
    ministry_terms = [
        "ministry",
        "ministere",
        "ministeriet",
        "research council",
        "forskningsrad",
        "conseil de la recherche",
    ]
    science_terms = ["science", "recherche", "forskning", "technology", "technologie", "teknologi", "innovation"]

    has_higher_ed = any(term in normalized for term in [normalize_text(t) for t in higher_ed_terms])
    has_ministry = any(term in normalized for term in [normalize_text(t) for t in ministry_terms])
    has_science = any(term in normalized for term in [normalize_text(t) for t in science_terms])

    if keyword_count >= 5 and (has_science or has_ministry):
        return {
            "innovation_relevant": True,
            "category_guess": "high_confidence_innovation_budget",
            "confidence": 0.9,
            "rationale": "Many innovation keywords with science/ministry context.",
        }
    if keyword_count >= 3 and (has_higher_ed or has_science):
        return {
            "innovation_relevant": True,
            "category_guess": "research_or_higher_education_funding",
            "confidence": 0.75,
            "rationale": "Moderate keyword density and education/science context.",
        }
    if keyword_count >= 2:
        return {
            "innovation_relevant": True,
            "category_guess": "possible_innovation_reference",
            "confidence": 0.6,
            "rationale": "Some innovation keywords present but context is limited.",
        }
    return {
        "innovation_relevant": False,
        "category_guess": "not_innovation_related",
        "confidence": 0.55,
        "rationale": "Too few supporting keywords for innovation relevance.",
    }


def classify_candidates(candidates_df: pd.DataFrame) -> pd.DataFrame:
    """Apply placeholder classification to detected candidate pages."""
    if candidates_df.empty:
        return candidates_df.copy()

    output_df = candidates_df.copy()
    labels = output_df.apply(
        lambda row: _classify_text_block(row.get("text", ""), int(row.get("keyword_count", 0))),
        axis=1,
    )
    label_df = pd.DataFrame(labels.tolist())
    output_df = pd.concat([output_df.reset_index(drop=True), label_df], axis=1)
    output_df["confidence"] = output_df["confidence"].round(3)
    output_df["relevance_label"] = output_df["innovation_relevant"].map(
        {True: "relevant", False: "not_relevant"}
    )

    # Keep final candidate file compact and review-friendly.
    ordered_cols = [
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
        "relevance_label",
        "innovation_relevant",
        "category_guess",
        "confidence",
        "rationale",
        "text_snippet",
    ]
    available_cols = [col for col in ordered_cols if col in output_df.columns]
    output_df = output_df[available_cols].copy()
    return output_df
