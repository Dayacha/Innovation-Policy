"""
Standalone Reform Cleaning Script
===================================

Runs the full two-pass cleaning workflow on an existing
``reforms_mentions.csv`` — score, LLM adjudication, K/L classification,
intensity score — without re-running the extraction pipeline.

Use this when:
  • You already have reforms_mentions.csv and want to (re-)clean it
  • You changed the taxonomy and want to re-score without re-extracting
  • You want to inspect Pass 1 results before committing to LLM cost
  • You interrupted a previous run and want to resume from checkpoint

What it does
------------
Pass 1 (free, instant):
  Scores every row with the taxonomy and adds:
    tax_score, score_band, filter_decision

Pass 2 (LLM, ~$0.50):
  Adjudicates borderline rows and classifies all kept rows, adding:
    llm_decision, llm_rationale, activity_lens, defence_scope

Then rebuilds:
  reform_intensity_score.csv   Country × year weighted reform intensity
  reform_panel_clean.csv       Country × year × subtheme binary panel
  cleaning_report.json         Full diagnostics

All cleaning columns are written in-place into reforms_mentions.csv.
No intermediate files are created.

Usage
-----
  # Full run (Pass 1 + Pass 2 + panels)
  python -m reforms.run_cleaning

  # Pass 1 only — free, no API needed
  python -m reforms.run_cleaning --skip-llm

  # Resume interrupted LLM pass from checkpoint
  python -m reforms.run_cleaning

  # Custom input file
  python -m reforms.run_cleaning --input Data/output/reforms/output/reforms_mentions.csv

  # Larger batches (faster, slightly higher per-call cost)
  python -m reforms.run_cleaning --batch-size 20
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from reforms.clean_pipeline import run_clean_pipeline  # noqa: E402

DEFAULT_INPUT = PROJECT_ROOT / "Data/output/reforms/output/reforms_mentions.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "Data/output/reforms/output"


def _parse_args(argv=None):
    p = argparse.ArgumentParser(
        description=(
            "Score, LLM-adjudicate, and clean reforms_mentions.csv, "
            "then rebuild the reform intensity score panel."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--input", type=Path, default=DEFAULT_INPUT,
        help="Path to reforms_mentions.csv  (default: %(default)s)",
    )
    p.add_argument(
        "--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR,
        help="Directory for intensity score + panel outputs  (default: %(default)s)",
    )
    p.add_argument(
        "--config", default="config.yaml",
        help="Path to config.yaml  (default: %(default)s)",
    )
    p.add_argument(
        "--skip-llm", action="store_true",
        help="Pass 1 only — taxonomy scoring, no LLM calls (free, instant)",
    )
    p.add_argument(
        "--batch-size", type=int, default=10,
        help="Rows per LLM call in Pass 2  (default: 10)",
    )
    p.add_argument(
        "--quiet", action="store_true",
        help="Suppress verbose output",
    )
    return p.parse_args(argv)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    args = _parse_args()
    run_clean_pipeline(
        input_path=args.input,
        output_dir=args.output_dir,
        config_path=args.config,
        skip_llm=args.skip_llm,
        batch_size=args.batch_size,
        verbose=not args.quiet,
    )
