"""
budget pipeline — main orchestrator.

Workflow per source file:
  1. Discover source files (PDF / DOCX) under data/input/finance_bills/<Country>/
  2. Infer country + year from filename / directory
  3. Extract text (with cache)
  4. Two-pass LLM extraction (scan → extract)
  5. Validate and normalise
  6. Accumulate results
  7. Write results.csv + results.xlsx + run_log.jsonl + llm_usage.json

Entry point:
    python -m budget.pipeline [--countries UK AU CA NZ] [--years 2000-2020]
    or call run_pipeline(config) from main.py
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers: file discovery and metadata parsing
# ---------------------------------------------------------------------------

_YEAR_PATTERN = re.compile(r"(?<![0-9])(1[89]\d{2}|20[012]\d)(?![0-9])")


def _infer_year(path: Path) -> Optional[int]:
    """Extract the 4-digit year from a filename (first match)."""
    m = _YEAR_PATTERN.search(path.stem)
    if m:
        return int(m.group(1))
    # Try parent directory name too
    m = _YEAR_PATTERN.search(path.parent.name)
    return int(m.group(1)) if m else None


def _discover_files(
    pdf_root: Path,
    countries: Optional[list[str]] = None,
    year_range: Optional[tuple[int, int]] = None,
) -> list[tuple[str, int, Path]]:
    """
    Walk pdf_root and return list of (country, year, path) tuples.

    Args:
        pdf_root:    Root directory, e.g. data/input/finance_bills/
        countries:   If set, only include these country directory names.
        year_range:  (start, end) inclusive. If None, include all years.

    Returns:
        Sorted list of (country_name, year, file_path).
    """
    results: list[tuple[str, int, Path]] = []

    if not pdf_root.exists():
        logger.error(f"PDF root not found: {pdf_root}")
        return results

    for country_dir in sorted(pdf_root.iterdir()):
        if not country_dir.is_dir():
            continue
        country = country_dir.name

        if countries and country not in countries:
            continue

        for f in sorted(country_dir.iterdir()):
            if f.suffix.lower() not in (".pdf", ".docx", ".doc"):
                continue
            year = _infer_year(f)
            if year is None:
                logger.debug(f"Could not infer year from {f.name} — skipping")
                continue
            if year_range and not (year_range[0] <= year <= year_range[1]):
                continue
            results.append((country, year, f))

    results.sort(key=lambda x: (x[0], x[1]))
    return results


# ---------------------------------------------------------------------------
# Run-log helpers
# ---------------------------------------------------------------------------

def _append_run_log(log_file: Path, entry: dict) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _already_processed(log_file: Path, source_file: str) -> bool:
    """Return True if this source_file already has a successful entry in the run log."""
    if not log_file.exists():
        return False
    try:
        with open(log_file, encoding="utf-8") as f:
            for line in f:
                entry = json.loads(line.strip())
                if entry.get("source_file") == source_file and entry.get("status") == "ok":
                    return True
    except Exception:
        pass
    return False


def _row_source_file(row) -> str:
    """Return source_file for either dict-like rows or BudgetRow-like objects."""
    if hasattr(row, "get"):
        try:
            return str(row.get("source_file", ""))
        except Exception:
            pass
    return str(getattr(row, "source_file", ""))


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_config(config_path: Optional[Path] = None) -> dict:
    """Load config.yaml and return as dict, merging budget-specific defaults."""
    from budget import config as cfg

    if config_path is None:
        config_path = cfg.PROJECT_ROOT / "config.yaml"

    raw: dict = {}
    if config_path.exists():
        with open(config_path, encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
    else:
        logger.warning(f"config.yaml not found at {config_path} — using defaults")

    # Inject budget path defaults
    raw.setdefault("budget", {})
    blm = raw["budget"]
    blm.setdefault("pdf_root", str(cfg.PDF_ROOT))
    blm.setdefault("output_dir", str(cfg.OUTPUT_DIR))
    blm.setdefault("llm_cache_dir", str(cfg.LLM_CACHE_DIR))
    blm.setdefault("pdf_text_cache_dir", str(cfg.PDF_TEXT_CACHE_DIR))
    blm.setdefault("chunk_size", cfg.CHUNK_SIZE)
    blm.setdefault("chunk_overlap", cfg.CHUNK_OVERLAP)
    blm.setdefault("use_scan_pass", cfg.USE_SCAN_PASS)
    blm.setdefault("scan_threshold", cfg.SCAN_THRESHOLD)
    blm.setdefault("skip_cached", cfg.SKIP_CACHED)
    blm.setdefault("run_consistency_pass", False)
    blm.setdefault("countries", [])
    blm.setdefault("year_range", {"start": 1970, "end": 2026})
    blm.setdefault("ocr_langs", "eng")
    blm.setdefault("ocr_zoom", 2.0)
    blm.setdefault("limit_files_per_year", None)  # None = no limit; int = cap per country-year

    # ── LLM model: budget.llm block takes precedence over global llm block ──
    # This lets the budget pipeline use a different model than reforms.
    blm_llm = blm.get("llm", {})
    global_llm = raw.get("llm", {})

    # Build effective llm config: start from global, override with budget.llm
    effective_llm = {**global_llm, **blm_llm}
    effective_llm.setdefault("provider", cfg.DEFAULT_LLM_CONFIG["provider"])
    effective_llm.setdefault("model", cfg.EXTRACT_MODEL)
    effective_llm.setdefault("scan_model", cfg.SCAN_MODEL)
    effective_llm.setdefault("max_tokens", 4096)
    effective_llm.setdefault("temperature", 0)
    effective_llm.setdefault("api_delay", 0.5)

    # ── Key resolution: if budget.llm.api_key is explicitly set, prefer it
    # over any OECD institutional key from the global llm block.
    # This ensures the budget pipeline uses the personal api_key rather than
    # the OECD key even when both are present in config.yaml.
    if blm_llm.get("api_key", "").strip():
        effective_llm.pop("oecd_openai_key", None)
        effective_llm.pop("oecd_anthropic_key", None)

    # Store back — extractor and client read from raw["llm"]
    raw["llm"] = effective_llm
    # Keep a copy so extractor can read scan_model separately
    blm["_resolved_llm"] = effective_llm

    return raw


# ---------------------------------------------------------------------------
# Core run function for a single file
# ---------------------------------------------------------------------------

def _process_file(
    country: str,
    year: int,
    path: Path,
    client,
    country_ctx: dict,
    blm_cfg: dict,
    cache_dir: Path,
    pdf_text_cache_dir: Path,
    run_log: Path,
) -> list:
    """Extract rows from one source file. Returns list[BudgetRow]."""
    from budget.pdf_reader import extract_pages
    from budget.extractor import extract_document
    from budget.validator import validate_rows, summarise

    source_file = path.name

    # Track the LLM call context for cost attribution
    client.set_current_survey(country_code=country, survey_year=year)

    # Extract text
    pages = extract_pages(
        path=path,
        cache_dir=pdf_text_cache_dir,
        force_reextract=False,
        ocr_zoom=float(blm_cfg.get("ocr_zoom", 2.0)),
        ocr_langs=blm_cfg.get("ocr_langs", "eng"),
    )

    if not pages:
        logger.warning(f"No pages extracted from {path.name}")
        return []

    logger.info(f"  {path.name}: {len(pages)} pages")

    # LLM extraction — read scan_model from resolved llm config
    scan_model = blm_cfg.get("_resolved_llm", {}).get("scan_model") or None

    rows = extract_document(
        pages=pages,
        client=client,
        country=country,
        year=year,
        source_file=source_file,
        country_ctx=country_ctx,
        cache_dir=cache_dir,
        use_scan_pass=blm_cfg.get("use_scan_pass", True),
        scan_threshold=float(blm_cfg.get("scan_threshold", 0.4)),
        run_consistency_pass=blm_cfg.get("run_consistency_pass", False),
        scan_model=scan_model,
    )

    # Validation
    rows = validate_rows(rows)

    summary = summarise(rows, country, year)
    logger.info(
        f"  {source_file}: {summary['include']} include, "
        f"{summary['review']} review, {summary['skip']} skip"
    )

    return rows


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def _estimate_cost(file_list: list, blm_cfg: dict) -> None:
    """Print a rough cost estimate using the configured model's actual pricing."""
    from reforms.llm_client import PRICING

    n_files = len(file_list)
    avg_pages = 50
    scan_keep_rate = 0.15
    scan_tokens_per_batch = 6_000
    extract_tokens_per_chunk = 12_000

    n_scan_batches = (n_files * avg_pages) / 8
    n_extract_chunks = (n_files * avg_pages * scan_keep_rate) / 10

    resolved = blm_cfg.get("_resolved_llm", {})
    scan_model = resolved.get("scan_model", "gpt-4o-mini")
    extract_model = resolved.get("model", "gpt-4o-mini")

    scan_price = PRICING.get(scan_model, {"input": 0.15})["input"]
    extract_price = PRICING.get(extract_model, {"input": 0.15})["input"]

    scan_cost = (n_scan_batches * scan_tokens_per_batch / 1_000_000) * scan_price
    extract_cost = (n_extract_chunks * extract_tokens_per_chunk / 1_000_000) * extract_price

    logger.info("━" * 60)
    logger.info(f"DRY-RUN ESTIMATE — {n_files} files")
    logger.info(f"  Scan pass  ({scan_model}):  ~{n_scan_batches:.0f} batches  ≈ ${scan_cost:.2f}")
    logger.info(f"  Extract pass ({extract_model}): ~{n_extract_chunks:.0f} chunks ≈ ${extract_cost:.2f}")
    logger.info(f"  TOTAL ESTIMATE:  ≈ ${scan_cost + extract_cost:.2f}  (real cost often 30-50% lower due to cache + scan filtering)")
    logger.info("━" * 60)


def run_pipeline(
    config: Optional[dict] = None,
    config_path: Optional[Path] = None,
    countries: Optional[list[str]] = None,
    year_range: Optional[tuple[int, int]] = None,
    skip_cached: bool = True,
    dry_run: bool = False,
    limit_per_year: Optional[int] = None,
    build_panel: bool = True,
) -> list:
    """
    Run the full budget pipeline.

    Args:
        config:          Pre-loaded config dict (takes precedence over config_path).
        config_path:     Path to config.yaml.
        countries:       Override countries list from config.
        year_range:      Override year range as (start, end).
        skip_cached:     Skip source files already in the run log.
        dry_run:         Print cost estimate and file list, then exit without calling LLM.
        limit_per_year:  Max files to process per (country, year). Useful for cheap tests.
        build_panel:     After extraction, build panel.csv time-series database.

    Returns:
        List of all BudgetRow objects extracted across all files.
    """
    from budget import config as cfg
    from budget.llm_client import BudgetLLMClient
    from budget.config import get_country_context
    from budget.output_schema import rows_to_csv, rows_to_excel, load_csv

    if config is None:
        config = load_config(config_path)

    blm_cfg = config.get("budget", {})

    # Resolve paths
    pdf_root = Path(blm_cfg.get("pdf_root", str(cfg.PDF_ROOT)))
    output_dir = Path(blm_cfg.get("output_dir", str(cfg.OUTPUT_DIR)))
    cache_dir = Path(blm_cfg.get("llm_cache_dir", str(cfg.LLM_CACHE_DIR)))
    pdf_text_cache_dir = Path(blm_cfg.get("pdf_text_cache_dir", str(cfg.PDF_TEXT_CACHE_DIR)))
    run_log = output_dir / "run_log.jsonl"
    results_csv = output_dir / "results.csv"
    results_excel = output_dir / "results.xlsx"

    # Ensure output dirs exist
    output_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Filter settings
    if countries is None:
        countries = blm_cfg.get("countries") or None
    yr_cfg = blm_cfg.get("year_range", {})
    if year_range is None:
        start = yr_cfg.get("start", 1970)
        end = yr_cfg.get("end", 2026)
        year_range = (int(start), int(end))

    limit_per_year = limit_per_year or blm_cfg.get("limit_files_per_year") or None

    # Discover files
    all_files = _discover_files(pdf_root, countries=countries, year_range=year_range)

    # Apply per-year file limit (useful for cheap Australia tests: many No1/No2/... per year)
    if limit_per_year:
        from collections import defaultdict
        year_counts: dict[tuple, int] = defaultdict(int)
        limited: list[tuple] = []
        for c, y, p in all_files:
            key = (c, y)
            if year_counts[key] < limit_per_year:
                limited.append((c, y, p))
                year_counts[key] += 1
        logger.info(
            f"limit_per_year={limit_per_year}: {len(all_files)} → {len(limited)} files"
        )
        all_files = limited

    logger.info(f"Discovered {len(all_files)} source files")

    if not all_files:
        logger.warning("No source files found. Check pdf_root and countries filter.")
        return []

    if dry_run:
        _estimate_cost(all_files, blm_cfg)
        for c, y, p in all_files[:20]:
            logger.info(f"  {c} {y}: {p.name}")
        if len(all_files) > 20:
            logger.info(f"  … and {len(all_files) - 20} more")
        return []

    # Initialise LLM client
    usage_file = output_dir / "llm_usage.json"
    client = BudgetLLMClient.from_config(config, usage_file=usage_file)
    logger.info(
        f"LLM: provider={client.provider}, extract_model={client.model}, "
        f"scan_model={blm_cfg.get('_resolved_llm', {}).get('scan_model', 'same')}"
    )

    # Load existing results.
    # Old rows for each file are dropped exactly when that file is re-processed,
    # so re-running any subset (whole country, single year, single file) always
    # replaces only the rows it actually re-extracts — nothing more, nothing less.
    all_rows = load_csv(results_csv) if results_csv.exists() else []
    initial_count = len(all_rows)

    processed = 0
    skipped = 0
    errors = 0

    for country, year, path in all_files:
        source_file = path.name

        # Skip if already processed
        if skip_cached and _already_processed(run_log, source_file):
            logger.debug(f"Skipping cached: {source_file}")
            skipped += 1
            continue

        logger.info(f"Processing: {country} {year} — {path.name}")
        t0 = time.time()

        country_ctx = get_country_context(country)

        try:
            rows = _process_file(
                country=country,
                year=year,
                path=path,
                client=client,
                country_ctx=country_ctx,
                blm_cfg=blm_cfg,
                cache_dir=cache_dir,
                pdf_text_cache_dir=pdf_text_cache_dir,
                run_log=run_log,
            )

            # Replace: drop any existing rows for this source file before adding new ones.
            # This ensures re-running a file always produces a clean result regardless
            # of whether it's a whole-country run, single-year run, or partial run.
            before = len(all_rows)
            all_rows = [r for r in all_rows if _row_source_file(r) != source_file]
            if len(all_rows) < before:
                logger.debug(f"Replaced {before - len(all_rows)} old rows for {source_file}")
            all_rows.extend(rows)
            processed += 1
            elapsed = round(time.time() - t0, 1)

            _append_run_log(run_log, {
                "status": "ok",
                "country": country,
                "year": year,
                "source_file": source_file,
                "rows_extracted": len(rows),
                "elapsed_seconds": elapsed,
                "timestamp": datetime.now().isoformat(),
            })

        except KeyboardInterrupt:
            logger.warning("Interrupted by user.")
            break
        except Exception as e:
            errors += 1
            logger.error(f"Error processing {source_file}: {e}")
            logger.debug(traceback.format_exc())
            _append_run_log(run_log, {
                "status": "error",
                "country": country,
                "year": year,
                "source_file": source_file,
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
            })
            continue

        # Write interim results every 10 files
        if processed % 10 == 0:
            rows_to_csv(all_rows, results_csv)
            logger.info(f"Interim save: {len(all_rows)} total rows")

    # Apply country-specific cleaners before final write
    if all_rows and all_files:
        try:
            from budget.targeted_recovery import recover_missing_agency_rows

            before = len(all_rows)
            all_rows = recover_missing_agency_rows(
                all_rows=all_rows,
                file_specs=all_files,
                config=config,
                pdf_text_cache_dir=pdf_text_cache_dir,
            )
            added = len(all_rows) - before
            if added:
                logger.info(f"Targeted recovery complete. {added} additional rows found.")
        except Exception as e:
            logger.warning(f"Targeted recovery failed (non-fatal): {e}")

    # Apply country-specific cleaners before final write
    if all_rows:
        try:
            import pandas as pd
            from budget.cleaners import apply_country_cleaner
            results_df = pd.DataFrame(all_rows)
            cleaned_parts = []
            for country_name, country_df in results_df.groupby("country"):
                cleaned_parts.append(apply_country_cleaner(country_df, country=country_name))
            results_df = pd.concat(cleaned_parts, ignore_index=True)
            all_rows = results_df.to_dict("records")
            logger.info(f"Country cleaners applied. Rows after cleaning: {len(all_rows)}")
        except Exception as e:
            logger.warning(f"Country cleaner failed (non-fatal): {e}")

    # Unit normalisation + parent-child dedup + temporal outlier flagging
    if all_rows:
        try:
            from budget.dedup import run_dedup
            results_df = pd.DataFrame(all_rows)
            results_df = run_dedup(results_df)
            all_rows = results_df.to_dict("records")
            redundant = sum(1 for r in all_rows if r.get("aggregation_role") == "redundant")
            logger.info(f"Dedup complete. {redundant} rows marked redundant.")
        except Exception as e:
            logger.warning(f"Dedup failed (non-fatal): {e}")

    # Final write
    rows_to_csv(all_rows, results_csv)
    try:
        rows_to_excel(all_rows, results_excel)
    except ImportError:
        logger.warning("openpyxl not available — skipping Excel export")

    # Save usage
    client.save_usage()

    new_rows = len(all_rows) - initial_count
    logger.info(
        f"Pipeline complete: {processed} files processed, {skipped} skipped, "
        f"{errors} errors. {new_rows} new rows. Total: {len(all_rows)} rows."
    )
    logger.info(f"Results: {results_csv}")
    _print_cost_summary(client)

    # Build cross-year panel
    if build_panel and results_csv.exists():
        try:
            from budget.panel_builder import build_panel as _build_panel
            panel_path, summary_path = _build_panel(results_csv, output_dir)
            logger.info(f"Panel: {panel_path}")
            logger.info(f"Series summary: {summary_path}")
        except Exception as e:
            logger.warning(f"Panel build failed (non-fatal): {e}")

    # Post-processing QA (dedup + consistency checks across full dataset)
    if results_csv.exists():
        try:
            from budget.postprocess import run_postprocess
            clean_path, report_path = run_postprocess(results_csv, output_dir)
            logger.info(f"Clean results: {clean_path}")
            logger.info(f"QA report: {report_path}")
        except Exception as e:
            logger.warning(f"Post-processing failed (non-fatal): {e}")

    return all_rows


def _print_cost_summary(client) -> None:
    """Log a concise cost summary."""
    try:
        total_cost = sum(r.get("cost_usd", 0) for r in client.usage_records)
        total_calls = len(client.usage_records)
        logger.info(
            f"LLM cost summary: {total_calls} calls, "
            f"${total_cost:.4f} total, "
            f"${total_cost/max(total_calls,1):.5f} avg/call"
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _cli() -> None:
    parser = argparse.ArgumentParser(
        description="budget: LLM-based R&D budget extraction pipeline"
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to config.yaml (default: auto-detect from project root)",
    )
    parser.add_argument(
        "--countries",
        nargs="+",
        default=None,
        help="Country directory names to process, e.g. --countries UK Australia Canada",
    )
    parser.add_argument(
        "--years",
        default=None,
        help="Year range: e.g. 2000-2020 or a single year 2010",
    )
    parser.add_argument(
        "--no-scan",
        action="store_true",
        help="Disable pass-1 scan and send all pages to extraction",
    )
    parser.add_argument(
        "--consistency-pass",
        action="store_true",
        help="Enable optional pass-3 consistency/dedup LLM call",
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Ignore cached results and re-process all files",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Estimate cost and list files to process, without calling the LLM",
    )
    parser.add_argument(
        "--limit-per-year",
        type=int,
        default=None,
        metavar="N",
        help="Process at most N files per country-year (useful for cheap tests: "
             "Australia has 6-8 files per year, use --limit-per-year 2 to test cheaply)",
    )
    parser.add_argument(
        "--no-panel",
        action="store_true",
        help="Skip building the cross-year panel after extraction",
    )
    parser.add_argument(
        "--panel-only",
        action="store_true",
        help="Skip extraction and only (re-)build the panel from existing results.csv",
    )
    parser.add_argument(
        "--postprocess-only",
        action="store_true",
        help="Skip extraction and only run QA post-processing on existing results.csv",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Run amount verification pass on existing results.csv (re-reads source docs, uses LLM)",
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

    config_path = Path(args.config) if args.config else None
    config = load_config(config_path)

    # Apply CLI overrides
    blm = config.setdefault("budget", {})
    if args.no_scan:
        blm["use_scan_pass"] = False
    if args.consistency_pass:
        blm["run_consistency_pass"] = True

    year_range = None
    if args.years:
        parts = args.years.split("-")
        if len(parts) == 2:
            year_range = (int(parts[0]), int(parts[1]))
        elif len(parts) == 1:
            year_range = (int(parts[0]), int(parts[0]))

    # --verify: re-read source docs and fix amounts for known confusion patterns
    if args.verify:
        import pandas as pd
        from budget import config as _cfg
        from budget.verify import run_verify
        output_dir = Path(blm.get("output_dir", str(_cfg.OUTPUT_DIR)))
        results_csv = output_dir / "results.csv"
        if not results_csv.exists():
            logger.error("No results.csv found — run extraction first.")
            return
        df = pd.read_csv(results_csv)
        source_dir = Path(config.get("data_dir", "Data/input/finance_bills"))
        country_filter = args.countries[0] if args.countries else None
        df = run_verify(df, config, source_dir=source_dir, country=country_filter)
        df.to_csv(results_csv, index=False)
        logger.info(f"Verify pass complete. Results saved to {results_csv}")
        return

    # --postprocess-only: skip extraction, apply country cleaners + QA on existing results
    if args.postprocess_only:
        import pandas as pd
        from budget import config as _cfg
        from budget.cleaners import apply_country_cleaner
        from budget.postprocess import run_postprocess as _postprocess
        output_dir = Path(blm.get("output_dir", str(_cfg.OUTPUT_DIR)))
        results_csv = output_dir / "results.csv"
        if results_csv.exists():
            from budget.dedup import run_dedup
            df = pd.read_csv(results_csv)
            # Country cleaners
            cleaned_parts = []
            for country_name, country_df in df.groupby("country"):
                cleaned_parts.append(apply_country_cleaner(country_df, country=country_name))
            df = pd.concat(cleaned_parts, ignore_index=True)
            logger.info(f"Country cleaners applied. Rows: {len(df)}")
            # Unit normalisation + dedup
            df = run_dedup(df)
            redundant = (df.get("aggregation_role", pd.Series()) == "redundant").sum()
            logger.info(f"Dedup complete. {redundant} redundant rows flagged.")
            df.to_csv(results_csv, index=False)
        clean_path, report_path = _postprocess(results_csv, output_dir)
        logger.info(f"Clean results: {clean_path}")
        logger.info(f"QA report: {report_path}")
        return

    # --panel-only: skip extraction, just rebuild the panel from existing results
    if args.panel_only:
        from budget import config as _cfg
        from budget.panel_builder import build_panel as _build_panel
        output_dir = Path(blm.get("output_dir", str(_cfg.OUTPUT_DIR)))
        results_csv = output_dir / "results.csv"
        panel_path, summary_path = _build_panel(results_csv, output_dir)
        logger.info(f"Panel: {panel_path}")
        logger.info(f"Series summary: {summary_path}")
        return

    run_pipeline(
        config=config,
        countries=args.countries,
        year_range=year_range,
        skip_cached=not args.fresh,
        dry_run=args.dry_run,
        limit_per_year=args.limit_per_year,
        build_panel=not args.no_panel,
    )


if __name__ == "__main__":
    _cli()
