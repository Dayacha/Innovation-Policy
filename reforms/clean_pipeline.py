"""
Reform Cleaning Pipeline — Orchestrator
========================================

Runs Pass 1 (taxonomy scoring) and Pass 2 (LLM adjudication + K/L
classification) on ``reforms_mentions.csv``, then builds the reform
intensity score panel.

All cleaning results are written as new columns directly into
``reforms_mentions.csv`` — no extra intermediate CSVs are created.

After running, the file has these additional columns:

  tax_score       float  — taxonomy relevance score
  score_band      str    — "keep" | "borderline" | "drop"
  filter_decision str    — "keep_rule_based" | "escalate_to_llm" | "drop_rule_based"
  llm_decision    str    — "include" | "exclude" | "n/a"
  llm_rationale   str    — LLM explanation (borderline rows and kept rows)
  activity_lens   str    — K1–K8 (type of R&D activity)
  defence_scope   str    — L1–L6 (civilian vs. defence scope)

The "clean" view is simply:
  df[(df["score_band"] == "keep") |
     ((df["score_band"] == "borderline") & (df["llm_decision"] == "include"))]

Two additional output files are produced (these have a genuinely different
shape from the mentions file — they are aggregated panels):

  reform_intensity_score.csv   Country × year reform intensity metrics
  reform_panel_clean.csv       Country × year × subtheme binary panel

Usage
-----
  python -m reforms.clean_pipeline                  # full run
  python -m reforms.clean_pipeline --skip-llm       # Pass 1 only (free, instant)
  python -m reforms.clean_pipeline --batch-size 20
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from reforms.scoring_filter import run_scoring_filter  # noqa: E402
from reforms.adjudicator import run_adjudicator         # noqa: E402

logger = logging.getLogger(__name__)

DEFAULT_INPUT = PROJECT_ROOT / "Data/output/reforms/output/reforms_mentions.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "Data/output/reforms/output"

SUBTHEME_WEIGHTS: dict[str, float] = {
    "rd_funding": 1.0,
    "knowledge_transfer": 0.9,
    "research_infrastructure": 0.8,
    "innovation_instruments": 0.7,
    "sectoral_rd": 0.6,
    "startup_ecosystem": 0.4,
    "human_capital": 0.3,
    "other": 0.1,
}


# ---------------------------------------------------------------------------
# "Clean view" helper
# ---------------------------------------------------------------------------

def clean_view(df: pd.DataFrame) -> pd.DataFrame:
    """Return only rows that passed the cleaning pipeline.

    Kept rows (score_band == "keep") and LLM-rescued borderline rows
    (score_band == "borderline" AND llm_decision == "include").
    Rows with score_band == "drop" or llm_decision == "exclude" are excluded.
    """
    if "score_band" not in df.columns:
        return df  # not yet scored — return everything

    keep_mask = df["score_band"] == "keep"

    if "llm_decision" in df.columns:
        rescued_mask = (df["score_band"] == "borderline") & (df["llm_decision"] == "include")
    else:
        rescued_mask = pd.Series(False, index=df.index)

    return df[keep_mask | rescued_mask].copy()


# ---------------------------------------------------------------------------
# Reform intensity score
# ---------------------------------------------------------------------------

def build_intensity_score(df: pd.DataFrame) -> pd.DataFrame:
    """Build a country × year reform intensity panel from the clean view.

    Expects the clean mentions dataframe (output of ``clean_view()``).
    """
    df = df.copy()
    direction_map = {
        "growth_supporting": 1, "growth_hindering": -1,
        "neutral": 0, "unknown": 0,
    }
    df["direction"] = df["growth_orientation"].map(direction_map).fillna(0).astype(int)
    df["st_weight"] = df["sub_theme"].map(SUBTHEME_WEIGHTS).fillna(0.1)
    df["weighted_positive"] = df.apply(
        lambda r: r["st_weight"] if r["direction"] > 0 else 0.0, axis=1
    )
    df["weighted_signed"] = df["st_weight"] * df["direction"]

    grp = df.groupby(["country_code", "country_name", "survey_year"])
    panel = grp.agg(
        n_reforms_clean=("reform_id", "count"),
        n_positive=("direction", lambda x: (x > 0).sum()),
        n_negative=("direction", lambda x: (x < 0).sum()),
        net_reforms=("direction", "sum"),
        weighted_score=("weighted_positive", "sum"),
        weighted_net_score=("weighted_signed", "sum"),
    ).reset_index()

    # Per sub-theme counts (wide)
    st_counts = (
        df.groupby(["country_code", "survey_year", "sub_theme"])
        .size().unstack(fill_value=0).reset_index()
    )
    st_counts.columns = [
        f"n_{c}" if c not in ("country_code", "survey_year") else c
        for c in st_counts.columns
    ]
    panel = panel.merge(st_counts, on=["country_code", "survey_year"], how="left")

    # K-lens counts (if available)
    if "activity_lens" in df.columns:
        k_counts = (
            df[df["activity_lens"].notna()]
            .groupby(["country_code", "survey_year", "activity_lens"])
            .size().unstack(fill_value=0).reset_index()
        )
        k_counts.columns = [
            f"k_{c.lower()}" if c not in ("country_code", "survey_year") else c
            for c in k_counts.columns
        ]
        panel = panel.merge(k_counts, on=["country_code", "survey_year"], how="left")

    panel = panel.sort_values(["country_code", "survey_year"]).reset_index(drop=True)
    panel[panel.select_dtypes("number").columns] = (
        panel.select_dtypes("number").fillna(0)
    )
    return panel


def build_binary_panel(df: pd.DataFrame) -> pd.DataFrame:
    """Country × year × sub_theme binary panel from the clean view."""
    df = df.copy()
    df["direction"] = (
        df["growth_orientation"]
        .map({"growth_supporting": 1, "growth_hindering": -1, "neutral": 0, "unknown": 0})
        .fillna(0).astype(int)
    )
    panel = (
        df.groupby(["country_code", "country_name", "survey_year", "sub_theme"])
        .agg(
            n_reforms=("reform_id", "count"),
            n_positive=("direction", lambda x: (x > 0).sum()),
            has_positive_reform=("direction", lambda x: int((x > 0).any())),
        )
        .reset_index()
    )
    return panel.sort_values(
        ["country_code", "survey_year", "sub_theme"]
    ).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Cleaning report
# ---------------------------------------------------------------------------

def _build_report(df: pd.DataFrame, intensity: pd.DataFrame, elapsed: float) -> dict:
    n_raw = len(df)

    n_keep = int((df["score_band"] == "keep").sum()) if "score_band" in df.columns else 0
    n_border = int((df["score_band"] == "borderline").sum()) if "score_band" in df.columns else 0
    n_drop = int((df["score_band"] == "drop").sum()) if "score_band" in df.columns else 0

    llm_done = "llm_decision" in df.columns
    n_rescued = int(
        ((df["score_band"] == "borderline") & (df["llm_decision"] == "include")).sum()
    ) if llm_done else 0
    n_excluded = int(
        ((df["score_band"] == "borderline") & (df["llm_decision"] == "exclude")).sum()
    ) if llm_done else 0

    clean = clean_view(df)
    n_final = len(clean)

    report = {
        "generated_at": datetime.now().isoformat(),
        "counts": {
            "raw": n_raw,
            "pass1_keep": n_keep,
            "pass1_borderline": n_border,
            "pass1_drop": n_drop,
            "pass2_rescued": n_rescued,
            "pass2_excluded": n_excluded,
            "final_clean": n_final,
            "retention_pct": round(n_final / n_raw * 100, 1) if n_raw else 0,
        },
        "subtheme_breakdown": {},
        "activity_lens": {},
        "defence_scope": {},
        "coverage": {
            "n_countries": int(clean["country_code"].nunique()),
            "n_country_years": int(clean.groupby(["country_code", "survey_year"]).ngroups),
        },
        "intensity_summary": {
            "mean_weighted_score": round(float(intensity["weighted_score"].mean()), 3),
            "max_weighted_score": round(float(intensity["weighted_score"].max()), 3),
            "mean_net_reforms": round(float(intensity["net_reforms"].mean()), 3),
        },
        "elapsed_seconds": round(elapsed, 1),
    }

    for st, g in clean.groupby("sub_theme"):
        report["subtheme_breakdown"][str(st)] = {
            "n": len(g),
            "pct": round(len(g) / n_final * 100, 1) if n_final else 0,
            "n_positive": int((g["growth_orientation"] == "growth_supporting").sum()),
        }
    if "activity_lens" in clean.columns:
        report["activity_lens"] = {
            str(k): int(v)
            for k, v in clean["activity_lens"].value_counts(dropna=False).items()
        }
    if "defence_scope" in clean.columns:
        report["defence_scope"] = {
            str(k): int(v)
            for k, v in clean["defence_scope"].value_counts(dropna=False).items()
        }
    return report


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def run_clean_pipeline(
    input_path: Path = DEFAULT_INPUT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    config_path: str = "config.yaml",
    skip_llm: bool = False,
    batch_size: int = 10,
    verbose: bool = True,
) -> dict:
    """Run the full two-pass cleaning pipeline.

    Modifies reforms_mentions.csv in place (adds cleaning columns).
    Writes reform_intensity_score.csv and reform_panel_clean.csv to output_dir.

    Returns a dict with keys: "df", "intensity", "binary_panel", "report".
    """
    import time as _time
    t0 = _time.time()

    input_path = Path(input_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if verbose:
        print("\n" + "=" * 62)
        print(" REFORM CLEANING PIPELINE")
        print("=" * 62)
        print(f"  Working on: {input_path.name}")
        print(f"  Panels → : {output_dir}")

    # ------------------------------------------------------------------
    # Pass 1
    # ------------------------------------------------------------------
    if verbose:
        print("\nPASS 1 — Taxonomy scoring filter")
    df = run_scoring_filter(input_path=input_path, verbose=verbose)

    # ------------------------------------------------------------------
    # Pass 2
    # ------------------------------------------------------------------
    if skip_llm:
        if verbose:
            print("\nPASS 2 — Skipped (--skip-llm)")
        # Add placeholder columns for schema consistency
        df = pd.read_csv(input_path, low_memory=False)
        for col, val in [("llm_decision", "n/a"), ("llm_rationale", ""),
                         ("activity_lens", None), ("defence_scope", None)]:
            if col not in df.columns:
                df[col] = val
        df.to_csv(input_path, index=False)
    else:
        if verbose:
            print("\nPASS 2 — LLM adjudication + K/L classification")
        checkpoint_path = output_dir / "adjudicator_checkpoint.json"
        df = run_adjudicator(
            input_path=input_path,
            checkpoint_path=checkpoint_path,
            config_path=config_path,
            batch_size=batch_size,
            adjudicate=True,
            classify=True,
            verbose=verbose,
        )

    # ------------------------------------------------------------------
    # Build aggregated panels
    # ------------------------------------------------------------------
    if verbose:
        print("\nBuilding reform intensity score …")

    clean = clean_view(df)
    intensity_df = build_intensity_score(clean)
    binary_df = build_binary_panel(clean)

    intensity_df.to_csv(output_dir / "reform_intensity_score.csv", index=False)
    binary_df.to_csv(output_dir / "reform_panel_clean.csv", index=False)

    # ------------------------------------------------------------------
    # Report
    # ------------------------------------------------------------------
    elapsed = _time.time() - t0
    report = _build_report(df, intensity_df, elapsed)
    with open(output_dir / "cleaning_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    if verbose:
        c = report["counts"]
        cov = report["coverage"]
        iss = report["intensity_summary"]
        sep = "=" * 62
        print(f"\n{sep}")
        print(" FINAL SUMMARY")
        print(sep)
        print(f"  Raw rows            : {c['raw']:>6,}")
        print(f"  Pass 1 keep         : {c['pass1_keep']:>6,}")
        print(f"  Pass 1 borderline   : {c['pass1_borderline']:>6,}  (sent to LLM)")
        print(f"  Pass 1 drop         : {c['pass1_drop']:>6,}")
        if not skip_llm:
            print(f"  Pass 2 rescued      : {c['pass2_rescued']:>6,}")
            print(f"  Pass 2 excluded     : {c['pass2_excluded']:>6,}")
        print(f"  ── Final clean      : {c['final_clean']:>6,}  ({c['retention_pct']:.1f}%)")
        print()
        print(f"  Countries           : {cov['n_countries']}")
        print(f"  Country × years     : {cov['n_country_years']}")
        print(f"  Mean weighted score : {iss['mean_weighted_score']:.3f}")
        print()
        print("  Written to reforms_mentions.csv (new columns):")
        print("    tax_score, score_band, filter_decision,")
        print("    llm_decision, llm_rationale, activity_lens, defence_scope")
        print()
        print("  Aggregated panels written:")
        print(f"    reform_intensity_score.csv")
        print(f"    reform_panel_clean.csv")
        print(f"    cleaning_report.json")
        print(sep)

    return {
        "df": df,
        "intensity": intensity_df,
        "binary_panel": binary_df,
        "report": report,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args(argv=None):
    p = argparse.ArgumentParser(
        description=(
            "Two-pass reform cleaning: taxonomy filter + LLM adjudication/K/L "
            "classification written in-place to reforms_mentions.csv, then "
            "builds reform intensity score panel."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--input", type=Path, default=DEFAULT_INPUT,
                   help="Path to reforms_mentions.csv  (default: %(default)s)")
    p.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR,
                   help="Dir for intensity score + panel CSVs  (default: %(default)s)")
    p.add_argument("--config", default="config.yaml",
                   help="config.yaml path  (default: %(default)s)")
    p.add_argument("--skip-llm", action="store_true",
                   help="Pass 1 only — no LLM calls (instant, free)")
    p.add_argument("--batch-size", type=int, default=10,
                   help="Rows per LLM call  (default: 10)")
    p.add_argument("--quiet", action="store_true", help="Suppress verbose output")
    return p.parse_args(argv)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
                        datefmt="%H:%M:%S")
    args = _parse_args()
    run_clean_pipeline(
        input_path=args.input,
        output_dir=args.output_dir,
        config_path=args.config,
        skip_llm=args.skip_llm,
        batch_size=args.batch_size,
        verbose=not args.quiet,
    )
