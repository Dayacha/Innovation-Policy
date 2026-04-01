"""
Pass 1 — Taxonomy Scoring Filter
=================================

Scores every row in ``reforms_mentions.csv`` against the project taxonomy
(search_library.json, pillars A–L) and writes three new columns back into
that same file:

  tax_score       float  — raw taxonomy score for description + source_quote
  score_band      str    — "keep" | "borderline" | "drop"
  filter_decision str    — "keep_rule_based" | "escalate_to_llm" | "drop_rule_based"

No new files are created.  Re-running overwrites the three columns in place.

Decision bands
--------------
  score ≥ 3   →  keep          (R&D terms clearly present)
  score 1–2   →  borderline    (escalate to LLM adjudicator in Pass 2)
  score ≤ 0   →  drop          (no R&D signal)

Usage
-----
  python -m reforms.scoring_filter
  python -m reforms.scoring_filter --input Data/output/reforms/output/reforms_mentions.csv
  python -m reforms.scoring_filter --quiet
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from budget.taxonomy import load_taxonomy, score_text  # noqa: E402

logger = logging.getLogger(__name__)

THRESHOLD_KEEP = 3
THRESHOLD_DROP = 0

DEFAULT_INPUT = PROJECT_ROOT / "Data/output/reforms/output/reforms_mentions.csv"


def _score_row(row: dict, tax) -> float:
    parts = [
        str(row.get("description", "") or ""),
        str(row.get("source_quote", "") or ""),
    ]
    text = " ".join(p for p in parts if p and p.lower() != "nan")
    result = score_text(text, tax)
    return float(result[0]) if isinstance(result, tuple) else float(result)


def _band(score: float) -> str:
    if score >= THRESHOLD_KEEP:
        return "keep"
    if score <= THRESHOLD_DROP:
        return "drop"
    return "borderline"


def run_scoring_filter(
    input_path: Path = DEFAULT_INPUT,
    verbose: bool = True,
) -> pd.DataFrame:
    """Score all rows and write tax_score / score_band / filter_decision
    back into the same CSV file.

    Returns the annotated DataFrame.
    """
    logger.info("Loading taxonomy …")
    tax = load_taxonomy()

    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Input not found: {path}\n"
            "Run the reform extraction pipeline first:\n"
            "  python -m reforms.pipeline_reforms"
        )

    logger.info("Loading %s …", path.name)
    df = pd.read_csv(path, low_memory=False)
    total = len(df)
    logger.info("Loaded %d rows. Scoring …", total)

    df["tax_score"] = df.apply(lambda r: _score_row(r.to_dict(), tax), axis=1)
    df["score_band"] = df["tax_score"].apply(_band)
    df["filter_decision"] = df["score_band"].map({
        "keep": "keep_rule_based",
        "borderline": "escalate_to_llm",
        "drop": "drop_rule_based",
    })

    # Write columns back into the same file
    df.to_csv(path, index=False)
    logger.info("Columns tax_score / score_band / filter_decision written to %s", path.name)

    if verbose:
        kept = (df["score_band"] == "keep").sum()
        border = (df["score_band"] == "borderline").sum()
        dropped = (df["score_band"] == "drop").sum()
        sep = "=" * 62
        print(f"\n{sep}")
        print(" PASS 1 — TAXONOMY SCORING RESULTS")
        print(sep)
        print(f"  Total rows            : {total:>6,}")
        print(f"  KEEP      (score ≥ 3) : {kept:>6,}  ({kept/total*100:5.1f}%)")
        print(f"  BORDERLINE (score 1–2): {border:>6,}  ({border/total*100:5.1f}%)  → LLM")
        print(f"  DROP      (score ≤ 0) : {dropped:>6,}  ({dropped/total*100:5.1f}%)")
        print()
        print("  Mean score by sub-theme:")
        for st, sc in df.groupby("sub_theme")["tax_score"].mean().sort_values(ascending=False).items():
            print(f"    {st:<32s}: {sc:5.2f}")
        print()
        print("  % kept by sub-theme:")
        for st, pct in (
            df.groupby("sub_theme")["score_band"]
            .apply(lambda x: (x == "keep").mean() * 100)
            .sort_values(ascending=False)
            .items()
        ):
            bar = "█" * int(pct / 5)
            print(f"    {st:<32s}: {pct:5.1f}%  {bar}")
        print(sep)

    return df


def _parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Pass 1: Score reform mentions and add tax_score/score_band columns in place.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--input", type=Path, default=DEFAULT_INPUT,
                   help="Path to reforms_mentions.csv  (default: %(default)s)")
    p.add_argument("--quiet", action="store_true", help="Suppress summary table")
    return p.parse_args(argv)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
                        datefmt="%H:%M:%S")
    args = _parse_args()
    run_scoring_filter(input_path=args.input, verbose=not args.quiet)
