"""Shared utilities used across pipeline stages."""

import hashlib
import logging
import re
import unicodedata
from pathlib import Path
from typing import Iterable

from src.config import COUNTRY_TOKEN_MAP


logger = logging.getLogger("innovation_pipeline")


def configure_logging() -> None:
    """Configure basic console logging for local runs."""
    if not logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )


def ensure_directories(paths: Iterable[Path]) -> None:
    """Create directories when missing."""
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def normalize_text(value: str) -> str:
    """Normalize text for matching (lowercase and strip accents)."""
    value = value.lower()
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def infer_country_year(path: Path) -> tuple[str, str]:
    """Infer country and year from path tokens with simple heuristics."""
    full_text = normalize_text(str(path).replace("\\", "/"))
    tokens = set(t for t in re.split(r"[^a-z0-9]+", full_text) if t)

    country_guess = "Unknown"
    for token, country in sorted(COUNTRY_TOKEN_MAP.items(), key=lambda x: len(x[0]), reverse=True):
        if token in tokens:
            country_guess = country
            break
        # Only allow substring matching for longer tokens to avoid false positives like "de" in "Desktop".
        if len(token) >= 4 and token in full_text:
            country_guess = country
            break

    year_match = re.search(r"(?<!\d)(19|20)\d{2}(?!\d)", full_text)
    year_guess = year_match.group(0) if year_match else "Unknown"
    return country_guess, year_guess


def build_file_id(path: Path) -> str:
    """Build a deterministic file identifier from a path string."""
    digest = hashlib.md5(str(path).encode("utf-8")).hexdigest()
    return f"pdf_{digest[:12]}"


def text_quality_metrics(text: str) -> tuple[int, float]:
    """Return character count and alphanumeric ratio for text quality checks."""
    if not text:
        return 0, 0.0
    char_count = len(text)
    alnum_count = sum(1 for c in text if c.isalnum())
    return char_count, (alnum_count / char_count if char_count > 0 else 0.0)
