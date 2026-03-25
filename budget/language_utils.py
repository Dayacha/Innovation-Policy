"""Language-aware keyword utilities for candidate detection."""

from budget.config import KEYWORDS_BY_LANGUAGE
from budget.utils import normalize_text


def language_keyword_hits(text: str) -> dict:
    """Count matched keywords by language and return detailed match metadata."""
    normalized_text = normalize_text(text)
    result = {}

    for language, groups in KEYWORDS_BY_LANGUAGE.items():
        policy_terms = {normalize_text(k) for k in groups["policy_terms"]}
        ministry_terms = {normalize_text(k) for k in groups["ministry_terms"]}

        policy_matches = sorted([kw for kw in policy_terms if kw in normalized_text])
        ministry_matches = sorted([kw for kw in ministry_terms if kw in normalized_text])
        all_matches = sorted(set(policy_matches + ministry_matches))

        result[language] = {
            "policy_matches": policy_matches,
            "ministry_matches": ministry_matches,
            "all_matches": all_matches,
            "total_matches": len(all_matches),
        }
    return result


def infer_language_from_hits(hit_map: dict) -> str:
    """Infer language using maximum keyword hit count."""
    best_language = "unknown"
    best_score = 0
    for language, metadata in hit_map.items():
        score = metadata["total_matches"]
        if score > best_score:
            best_score = score
            best_language = language
    return best_language

