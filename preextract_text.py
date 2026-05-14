"""
Pre-extract PDF text into the full_text cache before running the LLM pipeline.

This populates Data/output/budget/full_text/{hash}.json.gz for every PDF so that
the LLM pipeline (--llm-pipeline) finds everything cached and skips re-extraction.

Usage:
    python preextract_text.py --country Italy
    python preextract_text.py --country Slovenia
    python preextract_text.py --country Italy Slovenia "Czech Republic"
    python preextract_text.py          # all countries in config.yaml
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Add project root to sys.path so budget package is importable
sys.path.insert(0, str(Path(__file__).parent))

from budget.config import PDF_ROOT, PDF_TEXT_CACHE_DIR
from budget.pdf_reader import extract_pages


def discover_pdfs(countries: list[str]) -> list[tuple[str, int, Path]]:
    files = []
    for country in countries:
        country_dir = PDF_ROOT / country
        if not country_dir.exists():
            print(f"[WARN] No input directory for {country}: {country_dir}")
            continue
        for p in sorted(country_dir.iterdir()):
            if p.suffix.lower() not in (".pdf", ".docx"):
                continue
            # Year is the first token of the filename (or parent folder name if nested)
            try:
                year = int(p.name.split()[0])
            except (ValueError, IndexError):
                year = 0
            files.append((country, year, p))
    return files


def main():
    parser = argparse.ArgumentParser(description="Pre-extract PDF text into cache.")
    parser.add_argument(
        "--country", nargs="*", metavar="COUNTRY",
        help="One or more country names (e.g. Italy Slovenia). Default: all in PDF_ROOT.",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-extract even if cache already exists.",
    )
    parser.add_argument(
        "--ocr-langs", default=None,
        help="Tesseract language codes, e.g. 'eng+ita'. Defaults per country if not set.",
    )
    args = parser.parse_args()

    # Country → OCR language hint
    OCR_LANGS = {
        "Italy": "eng+ita",
        "Slovenia": "eng+slv",
        "Czech Republic": "eng+ces",
        "Hungary": "eng+hun",
        "Latvia": "eng+lav",
        "Lithuania": "eng+lit",
        "Korea": "eng+kor",
        "Israel": "eng+heb",
        "Iceland": "eng+isl",
        "Colombia": "eng+spa",
        "Costa Rica": "eng+spa",
        "Chile": "eng+spa",
        "Belgium": "eng+fra+nld",
        "France": "eng+fra",
        "Germany": "eng+deu",
        "Austria": "eng+deu",
        "Switzerland": "eng+deu+fra",
        "Norway": "eng+nor",
        "Denmark": "eng+dan",
        "Sweden": "eng+swe",
        "Slovakia": "eng+slk",
        "Finland": "eng+fin",
        "Netherlands": "eng+nld",
        "Estonia": "eng+est",
        "Poland": "eng+pol",
    }

    if args.country:
        countries = args.country
    else:
        countries = [d.name for d in sorted(PDF_ROOT.iterdir()) if d.is_dir()]

    print(f"Countries: {', '.join(countries)}")
    print(f"Cache dir: {PDF_TEXT_CACHE_DIR}")
    print(f"Force re-extract: {args.force}\n")

    PDF_TEXT_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    all_files = discover_pdfs(countries)
    print(f"Found {len(all_files)} PDF/DOCX files total\n")

    done = 0
    cached = 0
    failed = 0

    for country, year, path in all_files:
        langs = args.ocr_langs or OCR_LANGS.get(country, "eng")
        t0 = time.time()
        try:
            print(f"  [ ... ] {country} {year}: {path.name}", flush=True)
            pages = extract_pages(
                path=path,
                cache_dir=PDF_TEXT_CACHE_DIR,
                force_reextract=args.force,
                ocr_zoom=2.0,
                ocr_langs=langs,
            )
            elapsed = round(time.time() - t0, 1)
            if elapsed < 0.05:
                cached += 1
                print(f"\r  [CACHE] {country} {year}: {path.name} ({len(pages)} pages)", flush=True)
            else:
                done += 1
                print(f"\r  [DONE]  {country} {year}: {path.name} — {len(pages)} pages in {elapsed}s", flush=True)
        except Exception as e:
            failed += 1
            print(f"\r  [ERROR] {country} {year}: {path.name} — {e}", flush=True)

    print(f"\n{'='*60}")
    print(f"Extracted: {done}  |  Already cached: {cached}  |  Errors: {failed}")
    print(f"Total files: {len(all_files)}")


if __name__ == "__main__":
    main()
