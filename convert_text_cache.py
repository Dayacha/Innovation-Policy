"""
One-time conversion: translate full_text/<Country>/pdf_{hash}__name.txt.gz
into full_text/{hash}.json.gz so that budget/pdf_reader.py finds them and
skips re-extraction.

Run once before the overnight LLM extraction:
    python convert_text_cache.py
"""
from __future__ import annotations

import gzip
import json
import re
import sys
from pathlib import Path

DATA_ROOT = Path(__file__).parent / "Data" / "output" / "budget" / "full_text"

# Regex to split on page markers written by the old text-cache format:
# === Page 1.0 | method: ocr_fallback ===
_PAGE_MARKER = re.compile(
    r"=== Page ([\d.]+) \| method: (\S+) ===\n?"
)

COUNTRIES = ["Belgium", "Chile", "Estonia", "Iceland", "Israel", "Colombia", "Costa Rica", "Czech Republic"]


def parse_txt_gz(path: Path) -> list[dict]:
    """Parse a .txt.gz file into a list of {page_num, text, method} dicts."""
    with gzip.open(path, "rt", encoding="utf-8") as f:
        raw = f.read()

    pages = []
    segments = _PAGE_MARKER.split(raw)
    # split() with a capturing group gives: [pre, page_num, method, text, page_num, method, text, ...]
    # segments[0] is text before the first marker (ignore if empty)
    i = 1
    while i + 2 < len(segments):
        page_num_str = segments[i]
        method = segments[i + 1]
        text = segments[i + 2]
        try:
            page_num = int(float(page_num_str))
        except ValueError:
            page_num = len(pages) + 1
        # Normalise method name (ocr_fallback → ocr, direct stays direct)
        if method == "ocr_fallback":
            method = "ocr"
        pages.append({"page_num": page_num, "text": text.rstrip("\n"), "method": method})
        i += 3

    return pages


def convert_country(country: str, dry_run: bool = False) -> tuple[int, int]:
    src_dir = DATA_ROOT / country
    if not src_dir.exists():
        print(f"  [SKIP] {country}: no directory at {src_dir}")
        return 0, 0

    converted = 0
    skipped = 0

    for txt_gz in sorted(src_dir.glob("pdf_*.txt.gz")):
        # Filename: pdf_{hash}__{original_name}.txt.gz
        # Extract the 12-char hash
        stem = txt_gz.stem  # e.g. pdf_609dee4bc59f__1993_08_1.txt
        # strip trailing .txt if present (Path.stem only strips last extension)
        stem = stem.removesuffix(".txt") if stem.endswith(".txt") else stem
        parts = stem.split("__", 1)
        if len(parts) < 2 or not parts[0].startswith("pdf_"):
            print(f"  [WARN] Unexpected filename: {txt_gz.name} — skipping")
            continue

        file_id = parts[0][4:]  # strip "pdf_" prefix
        dest = DATA_ROOT / f"{file_id}.json.gz"

        if dest.exists():
            skipped += 1
            continue

        pages = parse_txt_gz(txt_gz)
        if not pages:
            print(f"  [WARN] No pages parsed from {txt_gz.name}")
            continue

        if not dry_run:
            with gzip.open(dest, "wt", encoding="utf-8") as f:
                json.dump(pages, f, ensure_ascii=False)

        converted += 1
        print(f"  {'[DRY] ' if dry_run else ''}Converted {txt_gz.name} → {dest.name}  ({len(pages)} pages)")

    return converted, skipped


def main():
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("DRY RUN — no files will be written\n")

    total_converted = 0
    total_skipped = 0

    for country in COUNTRIES:
        print(f"\n=== {country} ===")
        c, s = convert_country(country, dry_run=dry_run)
        print(f"  → {c} converted, {s} already existed")
        total_converted += c
        total_skipped += s

    print(f"\nDone. Total converted: {total_converted}, already cached: {total_skipped}")


if __name__ == "__main__":
    main()
