"""
Source Quote Recovery
=====================
For every reform in reforms_mentions.csv that has a source_quote, fuzzy-match
the quote against the extracted text file to recover:

  source_page_recovered   int   — best-matching page number
  source_quote_verified   str   — verbatim text window that best matches the quote
  source_match_score      float — similarity score 0-1 (≥0.55 = reliable)

Rows that already have source_page_start keep their original value; this script
fills the gaps and adds the verified quote for all rows.

Usage:
  python -m reforms.source_recovery            # updates reforms_mentions.csv in place
  python -m reforms.source_recovery --dry-run  # prints samples, writes nothing
  python -m reforms.source_recovery --min-score 0.4
"""
from __future__ import annotations

import argparse
import re
import sys
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEXT_DIR = PROJECT_ROOT / "Data/output/reforms/extracted_text"
DEFAULT_INPUT = PROJECT_ROOT / "Data/output/reforms/output/reforms_mentions.csv"
DEFAULT_MIN_SCORE = 0.45

_PAGE_RE = re.compile(r"^--- Page (\d+) ---\s*$")


def _load_pages(text_path: Path) -> list[tuple[int, str]]:
    """Return list of (page_number, page_text) from an extracted text file."""
    pages: list[tuple[int, str]] = []
    current_page: int | None = None
    lines: list[str] = []

    with open(text_path, encoding="utf-8", errors="replace") as f:
        for line in f:
            m = _PAGE_RE.match(line)
            if m:
                if current_page is not None:
                    pages.append((current_page, " ".join(lines)))
                current_page = int(m.group(1))
                lines = []
            else:
                stripped = line.strip()
                if stripped:
                    lines.append(stripped)

    if current_page is not None and lines:
        pages.append((current_page, " ".join(lines)))

    return pages


def _normalize(text: str) -> str:
    """Collapse whitespace, strip punctuation noise, lowercase."""
    text = re.sub(r"[_\-–—]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip().lower()


def _word_tokens(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", _normalize(text))


def _anchor_phrases(quote: str, n: int = 4) -> list[str]:
    """Extract up to 5 distinctive n-gram phrases from the quote for fast pre-filtering."""
    words = _word_tokens(quote)
    if len(words) < n:
        return [" ".join(words)]
    step = max(1, (len(words) - n) // 4)
    starts = list(range(0, len(words) - n + 1, step))[:5]
    return [" ".join(words[i : i + n]) for i in starts]


def _page_word_text(page_text: str) -> str:
    """Normalized word-only version of page text for anchor search."""
    return " ".join(_word_tokens(page_text))


def _char_anchors(quote: str, length: int = 18) -> list[str]:
    """Character-level anchors (no spaces) for OCR-fused text."""
    raw = re.sub(r"[^a-z0-9]", "", _normalize(quote))
    if len(raw) < length:
        return [raw]
    step = max(1, (len(raw) - length) // 4)
    starts = list(range(0, len(raw) - length + 1, step))[:5]
    return [raw[i : i + length] for i in starts]


def _page_matches_anchor(page_words: str, anchors: list[str],
                         page_chars: str, char_anchors: list[str]) -> bool:
    if any(a in page_words for a in anchors):
        return True
    return any(a in page_chars for a in char_anchors)


def _sliding_similarity(needle_norm: str, haystack_norm: str, window_chars: int,
                        haystack_raw: str) -> tuple[float, str]:
    """Slide a window over haystack and return (best_score, best_raw_window)."""
    step = max(10, window_chars // 4)
    best_score = 0.0
    best_start = 0

    for start in range(0, max(1, len(haystack_norm) - window_chars + 1), step):
        window = haystack_norm[start : start + window_chars]
        score = SequenceMatcher(None, needle_norm, window, autojunk=False).ratio()
        if score > best_score:
            best_score = score
            best_start = start

    best_window = haystack_raw[best_start : best_start + window_chars]
    return best_score, best_window


def find_best_page(
    quote: str,
    pages: list[tuple[int, str]],
    min_score: float = DEFAULT_MIN_SCORE,
) -> tuple[int | None, str, float]:
    """
    Search all pages for the best fuzzy match of quote.
    Returns (page_number, verified_verbatim_window, score).
    page_number is None if no page exceeds min_score.

    Strategy:
      1. Anchor filter — only fuzzy-match pages containing ≥1 n-gram anchor from quote.
      2. If no anchor match, fall back to top-5 pages ranked by word overlap.
      3. Sliding-window SequenceMatcher on finalists only.
    """
    if not quote or not quote.strip():
        return None, "", 0.0

    needle_norm = _normalize(quote)
    needle_words = set(_word_tokens(quote))
    window_chars = max(len(needle_norm) + 60, int(len(needle_norm) * 1.4))

    anchors = _anchor_phrases(quote, n=4)
    char_anch = _char_anchors(quote, length=18)

    # Pre-compute per-page word strings and char strings
    page_word_texts = [
        (pn, pt, _page_word_text(pt), re.sub(r"[^a-z0-9]", "", _normalize(pt)))
        for pn, pt in pages if pt.strip()
    ]

    # Step 1: anchor filter (word n-gram OR char n-gram for OCR-fused text)
    candidates = [(pn, pt, pw) for pn, pt, pw, pc in page_word_texts
                  if _page_matches_anchor(pw, anchors, pc, char_anch)]

    # Step 2: fallback — rank all pages by word overlap, take top 5
    if not candidates:
        def _overlap(pw: str) -> int:
            pw_words = set(pw.split())
            return len(needle_words & pw_words)
        ranked = sorted(page_word_texts, key=lambda x: _overlap(x[2]), reverse=True)
        candidates = [(pn, pt, pw) for pn, pt, pw, _ in ranked[:5]]

    best_page: int | None = None
    best_text = ""
    best_score = 0.0

    for page_num, page_text, page_words in candidates:
        page_norm = _normalize(page_text)
        score, window_raw = _sliding_similarity(needle_norm, page_norm, window_chars, page_text)
        if score > best_score:
            best_score = score
            best_page = page_num
            best_text = window_raw

    if best_score < min_score:
        return None, "", best_score

    best_text = _trim_to_sentence(best_text)
    return best_page, best_text, best_score


def _trim_to_sentence(text: str, max_words: int = 80) -> str:
    """Trim text to a clean sentence boundary, max ~max_words words."""
    words = text.split()
    if len(words) <= max_words:
        return text.strip()
    truncated = " ".join(words[:max_words])
    # Try to end on sentence boundary
    for sep in (". ", "! ", "? "):
        idx = truncated.rfind(sep)
        if idx > len(truncated) // 2:
            return truncated[: idx + 1].strip()
    return truncated.strip()


# ---------------------------------------------------------------------------
# Page-indexed corpus cache  (key = country_year string)
# ---------------------------------------------------------------------------

_PAGE_CACHE: dict[str, list[tuple[int, str]]] = {}


def _get_pages(country_year: str) -> list[tuple[int, str]]:
    if country_year not in _PAGE_CACHE:
        p = TEXT_DIR / f"{country_year}.txt"
        _PAGE_CACHE[country_year] = _load_pages(p) if p.exists() else []
    return _PAGE_CACHE[country_year]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_source_recovery(
    input_path: Path = DEFAULT_INPUT,
    min_score: float = DEFAULT_MIN_SCORE,
    dry_run: bool = False,
    verbose: bool = True,
) -> pd.DataFrame:
    df = pd.read_csv(input_path, low_memory=False)

    # Initialise output columns if missing
    for col in ("source_page_recovered", "source_quote_verified", "source_match_score"):
        if col not in df.columns:
            df[col] = None

    # Build country-year key from reform_id  e.g. AUS_1987_001 → AUS_1987
    df["_cy_key"] = df["reform_id"].str.extract(r"^([A-Z]{3}_\d{4})")

    n_total = len(df)
    n_has_quote = df["source_quote"].notna().sum()
    n_recovered = 0
    n_failed = 0

    if verbose:
        print(f"Source recovery: {n_has_quote} reforms with quotes out of {n_total} total")

    for idx, row in df.iterrows():
        quote = str(row.get("source_quote", "") or "").strip()
        if not quote or quote.lower() == "nan":
            continue

        cy_key = row.get("_cy_key", "")
        if not cy_key:
            continue

        pages = _get_pages(cy_key)
        if not pages:
            n_failed += 1
            continue

        page_num, verified, score = find_best_page(quote, pages, min_score)

        df.at[idx, "source_match_score"] = round(score, 3)

        if page_num is not None:
            df.at[idx, "source_page_recovered"] = page_num
            df.at[idx, "source_quote_verified"] = verified
            n_recovered += 1
        else:
            n_failed += 1

        if verbose and idx % 200 == 0:
            print(f"  … processed {idx}/{n_total}", end="\r")

    df = df.drop(columns=["_cy_key"])

    if verbose:
        pct = n_recovered / max(n_has_quote, 1) * 100
        print(f"\n  Recovered page: {n_recovered}/{n_has_quote} ({pct:.1f}%)")
        print(f"  Below threshold: {n_failed}")

        # Show a few examples
        sample = df[df["source_page_recovered"].notna()].head(3)
        for _, r in sample.iterrows():
            print(f"\n  [{r['reform_id']}]  page={r['source_page_recovered']}  "
                  f"score={r['source_match_score']}")
            print(f"    ORIGINAL : {str(r['source_quote'])[:120]}")
            print(f"    VERIFIED : {str(r['source_quote_verified'])[:120]}")

    if not dry_run:
        df.to_csv(input_path, index=False)
        if verbose:
            print(f"\n  Saved → {input_path.name}")

    return df


def _parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    p.add_argument("--min-score", type=float, default=DEFAULT_MIN_SCORE,
                   help="Minimum similarity score to accept a match (default: %(default)s)")
    p.add_argument("--dry-run", action="store_true",
                   help="Print samples without writing output")
    p.add_argument("--quiet", action="store_true")
    return p.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    run_source_recovery(
        input_path=args.input,
        min_score=args.min_score,
        dry_run=args.dry_run,
        verbose=not args.quiet,
    )
