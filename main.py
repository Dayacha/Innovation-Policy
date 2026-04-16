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
            year_range = (int(parts[0]), int(parts[1]))

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

    # ── Budget pipeline flags ─────────────────────────────────────────────────
    parser.add_argument("--country", help="Country to compile (e.g. Australia, Canada)")
    parser.add_argument("--years", help="Year range e.g. 2020-2024")
    parser.add_argument("--no-entity-dedup", action="store_true", help="Skip LLM entity deduplication")
    parser.add_argument("--fill-gaps", action="store_true", help="Try to fill missing agency-years from source documents")
    parser.add_argument("--no-gap-llm", action="store_true", help="Gap filling: text search only, skip LLM phase")
    parser.add_argument("--dry-run", action="store_true", help="Parse only, no LLM classification calls")
    parser.add_argument("--build-database", action="store_true", help="Rebuild combined rd_database.csv from all country series")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")

    # ── Reform pipeline flags ─────────────────────────────────────────────────
    from reforms.pipeline_reforms import add_arguments as _add_reform_args
    _add_reform_args(parser)

    args = parser.parse_args()

    if args.budget or args.build_database:
        _run_budget(args)

    elif args.reforms_only:
        from reforms.pipeline_reforms import run_from_args as _run_reforms
        _run_reforms(args)

    else:
        # Default: print help
        parser.print_help()
