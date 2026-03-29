"""
Reform Extraction Pipeline (Stream 2)

Extracts structural reform events from OECD Economic Survey PDFs using an
LLM-powered pipeline.  Outputs a country×year panel dataset of reforms
classified by theme, growth orientation, and importance.

Standalone usage (from project root):
    python -m reforms.pipeline_reforms
    python -m reforms.pipeline_reforms --country FRA --year 2024
    python -m reforms.pipeline_reforms --build-panel-only

Called from the unified entry point:
    python main.py --reforms-only
    python main.py --reforms-country FRA --reforms-year 2024
"""

import logging
import sys
import time
from pathlib import Path

import yaml

from .catalog import SurveyCatalog
from .countries import CODE_TO_NAME, get_country_list
from .downloader import PDFDownloader
from .extractor import extract_text_from_pdf
from .panel_builder import PanelBuilder
from .reform_analyzer import ReformAnalyzer

PROJECT_ROOT = Path(__file__).resolve().parent.parent

logger = logging.getLogger(__name__)


def _cleanup_source_files(entry, reforms_path: Path, cleanup_enabled: bool) -> None:
    """Delete intermediate text/PDF files after a successful JSON save."""
    if not cleanup_enabled or not reforms_path.exists():
        return

    for key in ("text_path", "pdf_path"):
        raw_path = entry.get(key)
        if not raw_path:
            continue
        path = Path(raw_path)
        try:
            if path.exists():
                path.unlink()
                logger.info("Deleted %s after successful JSON save: %s", key, path.name)
        except Exception as exc:
            logger.warning("Could not delete %s for %s_%d: %s", key, entry["country_code"], entry["year"], exc)


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def load_reforms_config(config_path="config.yaml"):
    """Load the reform pipeline configuration from the unified config.yaml.

    Reads the top-level 'reforms' and 'llm' sections and builds a flat config
    dict that the SurveyCatalog / ReformAnalyzer / PanelBuilder expect.
    Resolves relative data paths against the project root.
    """
    config_path = Path(config_path)
    if not config_path.is_absolute():
        config_path = PROJECT_ROOT / config_path

    if not config_path.exists():
        example = PROJECT_ROOT / "config.yaml.example"
        print(
            f"config.yaml not found.  "
            f"Copy {example.name} to config.yaml and add your API key."
        )
        return None

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    llm_section = raw.get("llm", {})
    reforms_section = raw.get("reforms", {})

    if not reforms_section:
        logger.warning("No 'reforms' section found in config.yaml — skipping reform pipeline")
        return None

    # Build the flat config structure that the reform modules expect
    config = {
        "llm": {
            "provider":    llm_section.get("provider", "anthropic"),
            "api_key":     llm_section.get("api_key", ""),
            "model":       llm_section.get("model", "claude-sonnet-4-6"),
            "temperature": llm_section.get("temperature", 0),
            "max_tokens":  llm_section.get("max_tokens", 4096),
        },
        "processing": {
            "chunk_size":       reforms_section.get("chunk_size", 12000),
            "chunk_overlap":    reforms_section.get("chunk_overlap", 500),
            "max_retries":      reforms_section.get("max_retries", 3),
            "api_delay":        reforms_section.get("api_delay", 1.0),
            "skip_existing":    reforms_section.get("skip_existing", True),
            "cleanup_after_success": reforms_section.get("cleanup_after_success", False),
            "dedup_threshold":  reforms_section.get("dedup_threshold", 0.65),
            "include_remaining_sections": reforms_section.get("include_remaining_sections", True),
            "remaining_min_taxonomy_score": reforms_section.get("remaining_min_taxonomy_score", 2.0),
            "remaining_neighbor_pages": reforms_section.get("remaining_neighbor_pages", 0),
        },
        "countries":   reforms_section.get("countries", []),
        "year_range":  reforms_section.get("year_range", {"start": 1995, "end": 2025}),
        "themes":      reforms_section.get("themes", []),
        "panel":       reforms_section.get("panel", {}),
    }

    # Resolve data paths (relative → absolute against project root)
    paths = {
        "raw_pdfs":       reforms_section.get("pdf_dir", "data/input/surveys"),
        "extracted_text": reforms_section.get("extracted_text", "data/output/reforms/extracted_text"),
        "reforms_json":   reforms_section.get("reforms_json",   "data/output/reforms/reforms_json"),
        "output":         reforms_section.get("output",         "data/output/reforms/output"),
        "kappa_catalog":  reforms_section.get("kappa_catalog",  "data/input/surveys/kappa_catalog.json"),
    }
    for key, val in paths.items():
        p = Path(val)
        paths[key] = str(PROJECT_ROOT / p) if not p.is_absolute() else val

    suffix = reforms_section.get("output_suffix", "").strip()
    if suffix:
        paths["reforms_json"] = f"{paths['reforms_json']}_{suffix}"
        paths["output"]       = f"{paths['output']}_{suffix}"

    config["paths"] = paths

    # Kappa API key for auto-downloading Economic Survey PDFs
    config["kappa_api_key"] = (
        reforms_section.get("kappa_api_key", "").strip()
        or ""
    )

    return config


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

def _step_catalog(config, country=None):
    catalog = SurveyCatalog(config)
    pdf_dir = config["paths"]["raw_pdfs"]
    n_local = catalog.build_catalog_from_local_pdfs(pdf_dir)
    logger.info("Reform catalog: %d local PDFs found", n_local)

    countries = [country] if country else None
    year_range = config.get("year_range", {})
    catalog.build_expected_catalog(
        countries=countries,
        start_year=year_range.get("start", 1995),
        end_year=year_range.get("end", 2025),
    )

    summary = catalog.summary()
    logger.info(
        "Catalog summary — total: %d  with PDF: %d  processed: %d",
        summary["total_entries"],
        summary["with_pdf"],
        summary["processed"],
    )
    return catalog


def _step_fetch_kappa_catalog(config, country=None) -> dict:
    """
    Build or incrementally update the survey catalog using the Kappa API.

    - If kappa_catalog.json already exists, only fetches surveys newer than the
      latest publication date in it (incremental — cheap API call).
    - If it does not exist, fetches the full catalog.

    Saves result to data/input/surveys/kappa_catalog.json.
    Returns the catalog dict (iso3 → year → metadata).
    """
    from .kappa_client import KappaClient

    kappa = KappaClient.from_config(config)
    if kappa is None:
        logger.warning(
            "No Kappa API key — cannot update catalog.\n"
            "Set reforms.kappa_api_key in config.yaml or KAPPA_API_KEY env var."
        )
        return {}

    catalog_path = Path(config["paths"]["kappa_catalog"])

    if catalog_path.exists():
        existing = KappaClient.load_catalog(catalog_path)
        total_before = sum(len(v) for v in existing.values())
        logger.info(
            "Existing catalog found (%d entries) — checking for new surveys...",
            total_before,
        )
        catalog = kappa.update_catalog(existing)
    else:
        logger.info("No catalog found — doing full Kappa fetch...")
        year_range = config.get("year_range", {})
        countries = [country] if country else config.get("countries") or None
        catalog = kappa.build_survey_catalog(
            start_year=year_range.get("start", 1995),
            end_year=year_range.get("end", 2025),
            countries=countries,
        )

    KappaClient.save_catalog(catalog, catalog_path)
    return catalog


def _step_download_pdfs(config, country=None, year=None) -> dict:
    """
    Download Economic Survey PDFs.

    If a Kappa API key is configured, uses the Kappa catalog (authenticated
    PDF links, most reliable). Otherwise falls back to trying public OECD
    iLibrary URL patterns.

    PDFs are saved as  data/input/surveys/{ISO3}_{YEAR}.pdf
    """
    from .kappa_client import KappaClient
    from .countries import get_country_list

    downloader = PDFDownloader(config)
    kappa = KappaClient.from_config(config)

    # ── Path A: Kappa API available ──────────────────────────────────────────
    if kappa:
        catalog_path = Path(config["paths"]["kappa_catalog"])
        if catalog_path.exists():
            catalog = KappaClient.load_catalog(catalog_path)
            logger.info("Using saved Kappa catalog from %s", catalog_path)
        else:
            logger.info("Catalog not found — fetching from Kappa API...")
            catalog = _step_fetch_kappa_catalog(config, country=country)

        if catalog:
            stats = downloader.download_from_kappa(catalog, country_code=country, year=year)
            logger.info(
                "Kappa download complete — downloaded: %d  skipped: %d  failed: %d",
                stats["downloaded"], stats["skipped"], stats["failed"],
            )
            return stats

    # ── Path B: No Kappa key — try public OECD URL patterns ──────────────────
    logger.info(
        "No Kappa API key configured — trying public OECD iLibrary URL patterns.\n"
        "  Tip: add kappa_api_key to config.yaml for more reliable downloads."
    )

    year_range = config.get("year_range", {})
    config_countries = config.get("countries") or [c for c, _ in get_country_list()]

    # Build (code, year) pairs to attempt
    if country and year:
        pairs = [(country, year)]
    elif country:
        pairs = [(country, yr) for yr in range(
            year_range.get("start", 2000), year_range.get("end", 2025) + 1, 2
        )]
    elif year:
        pairs = [(c, year) for c in config_countries]
    else:
        pairs = [
            (c, yr)
            for c in config_countries
            for yr in range(year_range.get("start", 2000), year_range.get("end", 2025) + 1, 2)
        ]

    # Load catalog for read_url/pdf_url hints even without a Kappa key
    kappa_catalog_path = Path(config["paths"]["kappa_catalog"])
    kappa_catalog = KappaClient.load_catalog(kappa_catalog_path) if kappa_catalog_path.exists() else {}

    # Also load survey_catalog for manually-curated pdf_url/url fields
    survey_catalog_path = Path(config["paths"]["output"]) / "survey_catalog.json"
    import json as _json
    survey_catalog = {}
    if survey_catalog_path.exists():
        with open(survey_catalog_path) as _f:
            survey_catalog = _json.load(_f)

    reforms_json_dir = Path(config["paths"]["reforms_json"])

    stats = {"downloaded": 0, "skipped": 0, "failed": 0}
    for code, yr in pairs:
        if (reforms_json_dir / f"{code}_{yr}.json").exists():
            logger.info("Skipping download (JSON already exists): %s %d", code, yr)
            stats["skipped"] += 1
            continue
        if downloader.is_downloaded(code, yr):
            stats["skipped"] += 1
            continue
        kappa_meta = kappa_catalog.get(code, {}).get(str(yr), {})
        survey_meta = survey_catalog.get(f"{code}_{yr}", {})
        # Prefer kappa pdf_url, fall back to survey_catalog pdf_url, then read_url
        pdf_url = kappa_meta.get("pdf_url") or survey_meta.get("pdf_url")
        read_url = kappa_meta.get("read_url") or survey_meta.get("url")
        result = downloader.download_survey(
            code, yr,
            url=pdf_url,
            read_url=read_url,
        )
        if result:
            stats["downloaded"] += 1
        else:
            stats["failed"] += 1

    logger.info(
        "URL-pattern download complete — downloaded: %d  skipped: %d  failed: %d",
        stats["downloaded"], stats["skipped"], stats["failed"],
    )
    return stats


def _step_extract_text(config, catalog):
    text_dir = Path(config["paths"]["extracted_text"])
    text_dir.mkdir(parents=True, exist_ok=True)
    skip_existing = config.get("processing", {}).get("skip_existing", True)

    surveys = catalog.get_surveys_with_pdfs()
    if not surveys:
        logger.info("No PDFs available for text extraction")
        return 0

    config_countries = config.get("countries", [])
    if config_countries:
        surveys = [s for s in surveys if s["country_code"] in config_countries]

    year_range = config.get("year_range", {})
    surveys = [
        s for s in surveys
        if year_range.get("start", 1900) <= s["year"] <= year_range.get("end", 2100)
    ]

    extracted = skipped = errors = 0
    for entry in surveys:
        code, year = entry["country_code"], entry["year"]
        text_path = text_dir / f"{code}_{year}.txt"

        if skip_existing and text_path.exists():
            catalog.add_entry(code, year, text_path=str(text_path), status="text_extracted")
            skipped += 1
            continue

        try:
            logger.info("Extracting text: %s %d", CODE_TO_NAME.get(code, code), year)
            extract_text_from_pdf(entry["pdf_path"], output_path=text_path)
            catalog.add_entry(code, year, text_path=str(text_path), status="text_extracted")
            extracted += 1
        except Exception as exc:
            logger.error("Text extraction failed for %s_%d: %s", code, year, exc)
            errors += 1

    catalog.save_catalog()
    logger.info(
        "Text extraction complete — new: %d  skipped: %d  errors: %d",
        extracted, skipped, errors,
    )
    return extracted


def _step_analyze_reforms(config, catalog, country=None, year=None):
    analyzer = ReformAnalyzer(config)
    skip_existing = config.get("processing", {}).get("skip_existing", True)
    cleanup_after_success = config.get("processing", {}).get("cleanup_after_success", False)

    surveys = catalog.get_surveys_with_text()
    if not surveys:
        logger.info("No surveys with extracted text to analyze")
        return 0

    if country:
        surveys = [s for s in surveys if s["country_code"] == country]
    if year:
        surveys = [s for s in surveys if s["year"] == year]

    config_countries = config.get("countries", [])
    if config_countries:
        surveys = [s for s in surveys if s["country_code"] in config_countries]

    year_range = config.get("year_range", {})
    surveys = [
        s for s in surveys
        if year_range.get("start", 1900) <= s["year"] <= year_range.get("end", 2100)
    ]

    analyzed = skipped = errors = 0
    for entry in surveys:
        code, yr = entry["country_code"], entry["year"]

        if skip_existing and analyzer.load_results(code, yr):
            logger.info("Skipping (cached): %s %d", CODE_TO_NAME.get(code, code), yr)
            catalog.add_entry(code, yr, status="reforms_extracted")
            skipped += 1
            continue

        logger.info("Analyzing reforms: %s %d", CODE_TO_NAME.get(code, code), yr)
        try:
            with open(entry["text_path"], encoding="utf-8") as f:
                text = f.read()
            analyzer.llm.set_current_survey(code, yr)
            analyzer.analyze_survey(text, code, yr)
            reforms_path = (
                Path(config["paths"]["reforms_json"]) / f"{code}_{yr}.json"
            )
            catalog.add_entry(code, yr, reforms_path=str(reforms_path), status="reforms_extracted")
            _cleanup_source_files(entry, reforms_path, cleanup_after_success)
            analyzed += 1
        except Exception as exc:
            logger.error("Reform analysis failed for %s_%d: %s", code, yr, exc)
            errors += 1

    catalog.save_catalog()
    analyzer.llm.save_usage()
    analyzer.llm.print_usage_report()
    logger.info(
        "LLM analysis complete — analyzed: %d  skipped: %d  errors: %d",
        analyzed, skipped, errors,
    )
    return analyzed


def _step_build_panel(config):
    builder = PanelBuilder(config)
    datasets = builder.build_all_datasets()
    mentions_df = datasets["mentions"]
    events_df = datasets["events"]
    panel_df = datasets["panel"]
    if not mentions_df.empty:
        logger.info(
            "Reform panel built — mentions: %d  events: %d  panel rows: %d",
            len(mentions_df), len(events_df), len(panel_df),
        )
    else:
        logger.info("No reform data available to build panel")
    return datasets


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_reforms_pipeline(
    config_path="config.yaml",
    country=None,
    year=None,
    build_panel_only=False,
    fetch_catalog=False,
    download_only=False,
):
    """Run the full reform extraction pipeline (or a subset of steps).

    Args:
        config_path:      Path to config.yaml (relative to project root).
        country:          ISO 3166-1 alpha-3 code to filter to one country.
        year:             Survey year to filter to one year.
        build_panel_only: Rebuild panel from cached JSON — no LLM calls.
        fetch_catalog:    Query Kappa API and save kappa_catalog.json, then stop.
        download_only:    Download PDFs from Kappa catalog, then stop.
    """
    config = load_reforms_config(config_path)
    if config is None:
        logger.warning("Reform pipeline skipped — config not found at %s", config_path)
        return

    logger.info("=== Reform Extraction Pipeline ===")
    start = time.time()

    if fetch_catalog:
        _step_fetch_kappa_catalog(config, country=country)

    elif download_only:
        _step_download_pdfs(config, country=country, year=year)

    elif build_panel_only:
        _step_build_panel(config)

    else:
        catalog = _step_catalog(config, country=country)
        _step_extract_text(config, catalog)
        _step_analyze_reforms(config, catalog, country=country, year=year)
        _step_build_panel(config)

    logger.info("Reform pipeline finished in %.1fs", time.time() - start)
    logger.info("Output: %s", config["paths"]["output"])


# ---------------------------------------------------------------------------
# CLI helpers — called from main.py to keep it clean
# ---------------------------------------------------------------------------

def add_arguments(parser):
    """Register all --reforms-* flags onto an argparse parser.

    Called from main.py so the unified entry point stays thin.
    """
    parser.add_argument(
        "--reforms-config", type=Path, default=Path("config.yaml"),
        help="Path to config.yaml (default: config.yaml)",
    )
    parser.add_argument(
        "--reforms-country", type=str, default=None, metavar="ISO3",
        help="Limit reform pipeline to one country (e.g. DNK, FRA)",
    )
    parser.add_argument(
        "--reforms-year", type=int, default=None, metavar="YEAR",
        help="Limit reform pipeline to one survey year",
    )

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--reforms-fetch-catalog", action="store_true",
        help="Check Kappa API for new surveys and update kappa_catalog.json",
    )
    mode.add_argument(
        "--reforms-download", action="store_true",
        help="Download Survey PDFs from catalog (Kappa if key set, else public URLs)",
    )
    mode.add_argument(
        "--reforms-build-panel-only", action="store_true",
        help="Rebuild reform panel from cached JSON without LLM calls (free, instant)",
    )


def run_from_args(args):
    """Execute the reform pipeline based on parsed CLI args.

    Called from main.py after argument parsing.
    """
    run_reforms_pipeline(
        config_path=args.reforms_config,
        country=args.reforms_country,
        year=args.reforms_year,
        build_panel_only=args.reforms_build_panel_only,
        fetch_catalog=args.reforms_fetch_catalog,
        download_only=args.reforms_download,
    )


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Reform Extraction Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("-v", "--verbose", action="store_true")
    add_arguments(parser)
    # Also accept short forms when run standalone
    parser.add_argument("--country", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--year", type=int, default=None, help=argparse.SUPPRESS)

    args = parser.parse_args()

    # Map short forms to the --reforms-* attrs that run_from_args expects
    if not args.reforms_country and args.country:
        args.reforms_country = args.country
    if not args.reforms_year and args.year:
        args.reforms_year = args.year
    if not hasattr(args, "reforms_config") or not args.reforms_config:
        args.reforms_config = Path(args.config)
    else:
        args.reforms_config = Path(args.reforms_config)

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    run_from_args(args)
