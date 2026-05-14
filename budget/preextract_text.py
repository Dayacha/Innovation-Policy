"""
Pre-extract PDF/DOC/DOCX text into the shared budget text cache without using the LLM.

This warms `Data/output/budget/full_text/` so later `budget.pipeline` runs can skip
slow OCR/text extraction and go straight to the LLM stage.

Examples:
    python -m budget.preextract_text --countries Italy
    python -m budget.preextract_text --countries Slovenia --years 2024-2025
    python -m budget.preextract_text --countries Italy Slovenia --fresh
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from budget import config as cfg
from budget.config import get_country_context
from budget.pdf_reader import extract_pages
from budget.pipeline import _discover_files, load_config

logger = logging.getLogger(__name__)

_OCR_LANG_HINTS = {
    "Italy": "eng+ita",
    "Slovenia": "eng+slv",
    "Czech Republic": "eng+ces",
    "Hungary": "eng+hun",
    "Latvia": "eng+lav",
    "Lithuania": "eng+lit",
    "Poland": "eng+pol",
    "Korea": "eng+kor",
    "Israel": "eng+heb",
    "Iceland": "eng+isl",
    "Colombia": "eng+spa",
    "Costa Rica": "eng+spa",
    "Chile": "eng+spa",
    "Luxembourg": "fra+deu+eng",
    "Mexico": "spa+eng",
    "Portugal": "por+eng",
    "Turkey": "tur+eng",
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
}


def _parse_years(value: str | None) -> tuple[int, int] | None:
    if not value:
        return None
    if "-" in value:
        start_str, end_str = value.split("-", 1)
        start, end = int(start_str), int(end_str)
        if end < start:
            raise ValueError(f"Invalid year range: {value}")
        return start, end
    year = int(value)
    return year, year


def run_preextract(
    countries: list[str] | None = None,
    year_range: tuple[int, int] | None = None,
    force_reextract: bool = False,
) -> int:
    config = load_config()
    budget_cfg = config.get("budget", {})

    pdf_root = Path(budget_cfg.get("pdf_root", str(cfg.PDF_ROOT)))
    cache_dir = Path(budget_cfg.get("pdf_text_cache_dir", str(cfg.PDF_TEXT_CACHE_DIR)))
    default_year_cfg = budget_cfg.get("year_range", {})
    if year_range is None:
        year_range = (
            int(default_year_cfg.get("start", 1970)),
            int(default_year_cfg.get("end", 2026)),
        )

    ocr_zoom = float(budget_cfg.get("ocr_zoom", 2.0))
    ocr_langs_cfg = str(budget_cfg.get("ocr_langs", "eng"))
    files = _discover_files(pdf_root, countries=countries, year_range=year_range)

    logger.info("Discovered %s source files", len(files))
    if not files:
        logger.warning("No files matched the requested filters.")
        return 0

    processed = 0
    errors = 0
    for country, year, path in files:
        try:
            country_ctx = get_country_context(country)
            ocr_langs = ocr_langs_cfg if ocr_langs_cfg != "eng" else _OCR_LANG_HINTS.get(country, "eng")
            pages = extract_pages(
                path=path,
                cache_dir=cache_dir,
                force_reextract=force_reextract,
                ocr_zoom=float(country_ctx.get("ocr_zoom", ocr_zoom)),
                ocr_langs=str(country_ctx.get("ocr_langs", ocr_langs)),
                force_ocr=bool(country_ctx.get("force_ocr", False)),
            )
            methods = {}
            for page in pages:
                methods[page.method] = methods.get(page.method, 0) + 1
            methods_summary = ", ".join(f"{k}={v}" for k, v in sorted(methods.items()))
            logger.info(
                "[%s %s] %s -> %s pages (%s)",
                country,
                year,
                path.name,
                len(pages),
                methods_summary or "no text",
            )
            processed += 1
        except Exception as exc:
            errors += 1
            logger.exception("Failed to extract %s: %s", path.name, exc)

    logger.info("Pre-extraction complete: %s processed, %s errors.", processed, errors)
    return 1 if errors else 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Pre-extract budget source text into the shared cache without running the LLM."
    )
    parser.add_argument(
        "--countries",
        nargs="+",
        default=None,
        help="Country directory names under Data/input/finance_bills/",
    )
    parser.add_argument(
        "--years",
        default=None,
        help="Single year or inclusive range, e.g. 2025 or 2024-2025",
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Ignore any existing cached text and re-extract.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )

    args = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    year_range = _parse_years(args.years)
    return run_preextract(
        countries=args.countries,
        year_range=year_range,
        force_reextract=args.fresh,
    )


if __name__ == "__main__":
    raise SystemExit(main())
