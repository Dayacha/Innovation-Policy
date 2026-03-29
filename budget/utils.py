"""Shared utilities used across pipeline stages."""

import hashlib
import logging
import re
import unicodedata
from pathlib import Path
from typing import Iterable

from budget.config import COUNTRY_TOKEN_MAP


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
    """Infer country and year from path tokens.

    Strategy (in priority order):
    1. Check each path *segment* (directory names win over bare filename tokens)
       because files are organised as  Data/PDF/<Country>/<filename>.pdf.
    2. Match exact tokens against COUNTRY_TOKEN_MAP (longest token first).
    3. Allow substring match only for tokens of length ≥ 5 to avoid
       false positives ("de" in "Desktop", "fr" in "Frankfurt", etc.).
    """
    path_str = str(path).replace("\\", "/")
    segments = [s.lower() for s in path_str.split("/") if s]

    # ── 1. Exact match in directory names (highest priority) ─────────────────
    country_guess = "Unknown"
    sorted_tokens = sorted(COUNTRY_TOKEN_MAP.items(), key=lambda x: len(x[0]), reverse=True)

    for segment in segments[:-1]:  # skip the filename itself
        seg_norm = normalize_text(segment)
        for token, country in sorted_tokens:
            if token == seg_norm or (len(token) >= 4 and token in seg_norm):
                country_guess = country
                break
        if country_guess != "Unknown":
            break

    # ── 2. Fall back to scanning the full (normalised) path ──────────────────
    if country_guess == "Unknown":
        full_text = normalize_text(path_str)
        tokens = set(re.split(r"[^a-z0-9]+", full_text))
        for token, country in sorted_tokens:
            if token in tokens:
                country_guess = country
                break
            if len(token) >= 5 and token in full_text:
                country_guess = country
                break

    # ── Year: look for 4-digit year in range 1945-2030 ───────────────────────
    full_text = normalize_text(path_str)
    # Spanish BOE filenames: "BOE-A-YYYY-NNNNN-consolidado para BUDGET_YEAR.pdf"
    # The "para YYYY" token is the actual budget year; prefer it over the
    # BOE publication year that appears earlier in the filename.
    para_match = re.search(r"\bpara[_ -]?(19[4-9]\d|20[0-2]\d)\b", full_text)
    if para_match:
        year_guess = para_match.group(1)
    else:
        year_match = re.search(r"(?<!\d)(19[4-9]\d|20[0-2]\d)(?!\d)", full_text)
        year_guess = year_match.group(0) if year_match else "Unknown"

    return country_guess, year_guess


def build_file_id(path: Path) -> str:
    """Build a deterministic file identifier from a path string."""
    digest = hashlib.md5(str(path).encode("utf-8")).hexdigest()
    return f"pdf_{digest[:12]}"


def compute_file_hash(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Return a stable MD5 hash of the file bytes."""
    digest = hashlib.md5()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def build_file_id_from_content_hash(content_hash: str) -> str:
    """Build a stable file identifier from a content hash."""
    normalized = str(content_hash).strip().lower()
    return f"pdf_{normalized[:12]}"


def text_quality_metrics(text: str) -> tuple[int, float]:
    """Return character count and alphanumeric ratio for text quality checks."""
    if not text:
        return 0, 0.0
    char_count = len(text)
    alnum_count = sum(1 for c in text if c.isalnum())
    return char_count, (alnum_count / char_count if char_count > 0 else 0.0)
