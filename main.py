"""Unified entry point for the Innovation Policy pipeline.

Budget pipeline (Stream 1):
    Extracts R&D spending time-series from Finance Bills (DOCX / pre-extracted PDF text)
    using deterministic table parsing + LLM agency classification.

    Input:  Data/input/finance_bills/<Country>/*.docx
            Data/output/budget/full_text/<Country>/*.txt.gz  (pre-extracted PDFs)
    Output: Data/output/budget/<Country>/<country>_docx_series.csv  (per-country)
            Data/output/budget/rd_database.csv                       (combined, app-ready)

    Run per country:
        python main.py --budget --country Australia
        python main.py --budget --country Canada --years 2020-2024
        python main.py --budget --country Australia --fill-gaps
        python main.py --budget --build-database   # rebuild combined DB only

    For narrative PDF countries (UK, France, Germany, Japan) that have no
    structured DOCX tables — run LLM extraction first, then compile:
        python main.py --budget --country UK --llm-pipeline
        python main.py --budget --country France --llm-pipeline --years 2000-2024
        python main.py --budget --country Germany --llm-pipeline
        python main.py --budget --country Japan --llm-pipeline

    After the first run, results are cached. Re-running without --llm-pipeline
    just rebuilds the canonical series at no extra LLM cost:
        python main.py --budget --country UK   # uses cached LLM output

Reform pipeline (Stream 2):
    Extracts structural reform events from OECD Economic Survey PDFs using an LLM.

    Input:  Data/surveys/<COUNTRY_CODE>_<YEAR>.pdf
    Output: Data/output/reforms/output/reform_panel.csv  |  reforms_events.csv

    Run:
        python main.py --reforms-only
        python main.py --reforms-country FRA
        python main.py --reforms-year 2019
        python main.py --reforms-extract-text-only
        python main.py --reforms-build-panel-only
        python main.py --reforms-fetch-catalog
        python main.py --reforms-download

Cross-verification (two-model merger):
    Compare two independent extraction runs and produce a merged dataset.
    Requires two completed extraction runs (primary and secondary model).

    Step 1 — primary run (gpt-4o-mini, output_suffix: ""):
        python main.py --reforms-only

    Step 2 — secondary run (claude-sonnet-4, set output_suffix: "anthropic" in config):
        python main.py --reforms-only

    Step 3 — merge and adjudicate:
        python main.py --reforms-cross-verify
        python main.py --reforms-cross-verify --country DNK --year 2021
        python main.py --reforms-cross-verify --consensus-only
        python main.py --reforms-cross-verify --build-panel-only

Full automated pipeline (all three stages in one command):
    Checks Run A status, runs Run B (Anthropic), then cross-verifies.
    No manual config.yaml changes needed — API keys resolved automatically.

        python main.py --reforms-full-pipeline
        python main.py --reforms-full-pipeline --country DNK
        python main.py --reforms-full-pipeline --country DNK --year 2021
        python main.py --reforms-full-pipeline --check-only        # status report only
        python main.py --reforms-full-pipeline --skip-run-a-check  # skip Run A check
        python main.py --reforms-full-pipeline --consensus-only    # no LLM adjudication
"""

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent


def _run_budget(args) -> None:
    """Run the budget compile pipeline."""
    import yaml

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    cfg_path = PROJECT_ROOT / "config.yaml"
    with open(cfg_path) as f:
        config = yaml.safe_load(f)

    from budget.compile import compile_country, build_combined_database

    # --build-database: just rebuild the combined CSV, no extraction
    if args.build_database:
        db = build_combined_database()
        if not db.empty:
            print(f"\nCombined database: {len(db)} rows")
            print(db.groupby("country").agg(
                agencies=("canonical_name", "nunique"),
                years=("year", "nunique"),
                min_year=("year", "min"),
                max_year=("year", "max"),
            ).to_string())
        return

    if not args.country:
        print("Error: --country is required with --budget (or use --build-database)")
        sys.exit(1)

    year_range = None
    if args.years:
        parts = args.years.split("-")
        if len(parts) == 2:
            y_start, y_end = int(parts[0]), int(parts[1])
            if y_end < y_start:
                print(f"Error: --years {args.years} is invalid (end year {y_end} < start year {y_start}). "
                      f"Did you mean --years {y_start}-20{y_end:02d}?")
                sys.exit(1)
            if y_end < 1970 or y_start > 2100:
                print(f"Error: --years {args.years} looks wrong (expected e.g. 2016-2020).")
                sys.exit(1)
            year_range = (y_start, y_end)
        else:
            print(f"Error: --years must be in format START-END (e.g. 2016-2020), got: {args.years}")
            sys.exit(1)

    # --llm-pipeline: run LLM extraction first (for narrative PDF countries such as
    # UK, France, Germany, Japan that have no structured DOCX/text-cache tables).
    # This calls budget/pipeline.py which does the expensive per-page extraction,
    # caching results in results.csv.  compile_country() then reads that cache
    # automatically — no double cost on subsequent runs.
    if getattr(args, "llm_pipeline", False):
        from budget.pipeline import run_pipeline, load_config as load_pipeline_config
        pipeline_config = load_pipeline_config(cfg_path)
        print(f"\nRunning LLM extraction pipeline for {args.country}...")
        run_pipeline(
            config=pipeline_config,
            countries=[args.country],
            year_range=year_range,
            dry_run=args.dry_run,
            build_panel=False,   # compile_country builds the series
        )
        if args.dry_run:
            return

    series = compile_country(
        country=args.country,
        config=config,
        year_range=year_range,
        dry_run=args.dry_run,
        entity_dedup=not args.no_entity_dedup,
        fill_gaps_flag=args.fill_gaps,
        fill_gaps_llm=not args.no_gap_llm,
    )

    if not series.empty:
        print(f"\nSeries summary for {args.country}:")
        print(series.groupby("canonical_name").agg(
            years=("year", "count"),
            min_year=("year", "min"),
            max_year=("year", "max"),
        ).to_string())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Innovation Policy Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # ── Pipeline selection ────────────────────────────────────────────────────
    pipeline_group = parser.add_mutually_exclusive_group()
    pipeline_group.add_argument(
        "--budget", action="store_true",
        help="Run the budget extraction pipeline (budget)",
    )
    pipeline_group.add_argument(
        "--reforms-only", action="store_true",
        help="Run only the OECD Economic Survey reform extraction pipeline",
    )
    pipeline_group.add_argument(
        "--reforms-cross-verify", action="store_true",
        help="Merge two reform extraction runs (cross-verification, Strategy B)",
    )
    pipeline_group.add_argument(
        "--reforms-full-pipeline", action="store_true",
        help="Run full pipeline: check Run A → Run B (Anthropic) → cross-verify",
    )

    # ── Budget pipeline flags ─────────────────────────────────────────────────
    parser.add_argument("--country", help="Country to compile (e.g. Australia, Canada, UK, France, Germany, Japan)")
    parser.add_argument("--years", help="Year range e.g. 2020-2024")
    parser.add_argument("--no-entity-dedup", action="store_true", help="Skip LLM entity deduplication")
    parser.add_argument("--fill-gaps", action="store_true", help="Try to fill missing agency-years from source documents")
    parser.add_argument("--no-gap-llm", action="store_true", help="Gap filling: text search only, skip LLM phase")
    parser.add_argument("--dry-run", action="store_true", help="Parse only, no LLM classification calls")
    parser.add_argument("--build-database", action="store_true", help="Rebuild combined rd_database.csv from all country series")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument(
        "--llm-pipeline", action="store_true",
        help=(
            "Run LLM extraction first before compiling. Required for narrative PDF "
            "countries (UK, France, Germany, Japan) where budget tables are not in "
            "structured DOCX format. Results are cached — re-running without this flag "
            "uses the existing cache at no extra LLM cost."
        ),
    )

    # ── Reform pipeline flags ─────────────────────────────────────────────────
    from reforms.pipeline_reforms import add_arguments as _add_reform_args
    _add_reform_args(parser)

    # ── Cross-verification flags ──────────────────────────────────────────────
    parser.add_argument(
        "--consensus-only", action="store_true",
        help="Cross-verify: keep only reforms found by both models (no LLM adjudication)",
    )
    parser.add_argument(
        "--build-panel-only", action="store_true",
        help="Cross-verify: skip merging, re-build panel from existing merged JSONs",
    )

    # ── Full pipeline flags ───────────────────────────────────────────────────
    parser.add_argument(
        "--check-only", action="store_true",
        help="Full pipeline: report status of all stages without running anything",
    )
    parser.add_argument(
        "--skip-run-a-check", action="store_true",
        help="Full pipeline: skip Run A completeness check (assume it is done)",
    )
    parser.add_argument(
        "--countries", nargs="+", metavar="CODE",
        help="Full pipeline: run for multiple country codes, e.g. --countries CAN FRA DEU",
    )
    parser.add_argument(
        "--g7", action="store_true",
        help="Full pipeline: run all G7 countries (CAN FRA DEU ITA JPN GBR USA)",
    )
    parser.add_argument(
        "--g20", action="store_true",
        help="Full pipeline: run all G20 OECD members",
    )

    args = parser.parse_args()

    if args.budget or args.build_database:
        _run_budget(args)

    elif args.reforms_only:
        from reforms.pipeline_reforms import run_from_args as _run_reforms
        _run_reforms(args)

    elif args.reforms_full_pipeline:
        from reforms.full_pipeline import run_full_pipeline
        from reforms.pipeline_reforms import load_reforms_config

        _G7  = ["CAN", "FRA", "DEU", "ITA", "JPN", "GBR", "USA"]
        _G20_OECD = [
            "AUS", "CAN", "FRA", "DEU", "ITA", "JPN", "KOR",
            "MEX", "TUR", "GBR", "USA",
        ]

        config = load_reforms_config(args.config)
        if config is None:
            sys.exit(1)

        # Resolve country list: --g7 / --g20 / --countries / single --country / all
        if getattr(args, "g7", False):
            fp_countries = _G7
        elif getattr(args, "g20", False):
            fp_countries = _G20_OECD
        elif getattr(args, "countries", None):
            fp_countries = [c.upper() for c in args.countries]
        else:
            single = (
                getattr(args, "reforms_country", None)
                or getattr(args, "country", None)
            )
            fp_countries = [single] if single else [None]  # None = all countries

        fp_year        = getattr(args, "reforms_year", None)
        check_only     = getattr(args, "check_only", False)
        skip_a         = getattr(args, "skip_run_a_check", False)
        consensus_only = getattr(args, "consensus_only", False)

        total = len([c for c in fp_countries if c])
        for i, fp_country in enumerate(fp_countries, 1):
            if total > 1:
                print(f"\n{'#'*60}")
                print(f"# Country {i}/{total}: {fp_country}")
                print(f"{'#'*60}")
            run_full_pipeline(
                config=config,
                country=fp_country,
                year=fp_year,
                check_only=check_only,
                skip_run_a_check=skip_a,
                consensus_only=consensus_only,
            )

    elif args.reforms_cross_verify:
        from reforms.cross_verifier import main as _run_cross_verify
        # Pass through relevant flags as argv so cross_verifier parses them
        cv_argv = ["--config", args.config]
        if getattr(args, "reforms_country", None):
            cv_argv += ["--country", args.reforms_country]
        if getattr(args, "reforms_year", None):
            cv_argv += ["--year", str(args.reforms_year)]
        if getattr(args, "consensus_only", False):
            cv_argv.append("--consensus-only")
        if getattr(args, "build_panel_only", False):
            cv_argv.append("--build-panel-only")
        _run_cross_verify(cv_argv)

    else:
        # Default: print help
        parser.print_help()
