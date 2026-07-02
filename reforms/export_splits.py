"""
Regenerate reforms_kept.csv, reforms_borderline.csv, reforms_dropped.csv,
and reforms_events_clean.csv from the current reforms_mentions.csv.

The four outputs are:
  reforms_kept.csv        — clean_view rows (Pass-1 keep[LLM≠exclude] + rescued borderline)
  reforms_borderline.csv  — borderline rows excluded by LLM (review candidates)
  reforms_dropped.csv     — Pass-1 drops (score ≤ 0)
  reforms_events_clean.csv — reforms_events.csv filtered to reforms_kept IDs only

Added columns on reforms_kept:
  year_flag  — "ok" | "future_impl" | "suspicious" — temporal consistency check

Usage:
  python -m reforms.export_splits
  python -m reforms.export_splits --input path/to/reforms_mentions.csv
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def clean_view(df: pd.DataFrame) -> pd.DataFrame:
    """Pass-1 keeps (LLM not excluded) + LLM-rescued borderline rows."""
    if "llm_decision" in df.columns:
        keep_mask = (df["score_band"] == "keep") & (df["llm_decision"] != "exclude")
        rescued = (df["score_band"] == "borderline") & (df["llm_decision"] == "include")
    else:
        keep_mask = df["score_band"] == "keep"
        rescued = pd.Series(False, index=df.index)
    return df[keep_mask | rescued].copy()


def _add_year_flag(df: pd.DataFrame) -> pd.DataFrame:
    """Add year_flag column to flag temporal anomalies.

    ok            — implementation_year <= survey_year + 2  (or missing)
    future_impl   — implementation_year > survey_year + 2 but plausible (plan/target)
    suspicious    — implementation_year > survey_year + 10 or impl < survey - 30
                    (likely LLM year extraction error)
    """
    df = df.copy()
    sy = pd.to_numeric(df["survey_year"], errors="coerce")
    iy = pd.to_numeric(df["implementation_year"], errors="coerce")
    gap = iy - sy

    flags = pd.Series("ok", index=df.index)
    # Future implementation: plan/target announced but not yet delivered
    flags[(gap > 2) & (gap <= 10)] = "future_impl"
    # Suspicious: >10 years ahead OR reform predates survey by >30 years
    flags[(gap > 10) | (gap < -30)] = "suspicious"
    # No implementation year — leave as "ok"
    flags[iy.isna()] = "ok"

    df["year_flag"] = flags
    return df


DEFAULT_INPUT = PROJECT_ROOT / "Data/output/reforms/output/reforms_mentions.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "Data/output/reforms/output"


def export_splits(
    input_path: Path = DEFAULT_INPUT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    verbose: bool = True,
) -> dict[str, pd.DataFrame]:
    df = pd.read_csv(input_path, low_memory=False)

    kept = clean_view(df)
    kept = _add_year_flag(kept)

    borderline_excl = df[
        (df["score_band"] == "borderline") & (df["llm_decision"] == "exclude")
    ].copy()

    dropped = df[df["score_band"] == "drop"].copy()

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    kept.to_csv(output_dir / "reforms_kept.csv", index=False)
    borderline_excl.to_csv(output_dir / "reforms_borderline.csv", index=False)
    dropped.to_csv(output_dir / "reforms_dropped.csv", index=False)

    # Filter reforms_events to kept IDs only → reforms_events_clean.csv
    events_path = output_dir / "reforms_events.csv"
    events_clean = pd.DataFrame()
    if events_path.exists():
        events = pd.read_csv(events_path, low_memory=False)
        kept_ids = set(kept["reform_id"])
        events_clean = events[events["reform_id"].isin(kept_ids)].copy()
        events_clean.to_csv(output_dir / "reforms_events_clean.csv", index=False)

    if verbose:
        n_future = (kept["year_flag"] == "future_impl").sum()
        n_susp = (kept["year_flag"] == "suspicious").sum()
        sep = "=" * 62
        print(f"\n{sep}")
        print(" REFORM SPLITS — regenerated from reforms_mentions.csv")
        print(sep)
        print(f"  Total rows in mentions   : {len(df):>6,}")
        print(f"  reforms_kept.csv         : {len(kept):>6,}  (Pass-1 keep[LLM≠excl] + rescued)")
        print(f"    ↳ year_flag=ok         : {(kept['year_flag']=='ok').sum():>6,}")
        print(f"    ↳ year_flag=future_impl: {n_future:>6,}  (announced, not yet delivered)")
        print(f"    ↳ year_flag=suspicious : {n_susp:>6,}  ← review before regression")
        print(f"  reforms_borderline.csv   : {len(borderline_excl):>6,}  (borderline → LLM excluded)")
        print(f"  reforms_dropped.csv      : {len(dropped):>6,}  (Pass-1 drop)")
        # keep-band rows overridden by LLM exclude (removed from kept)
        n_llm_overrides = int(
            ((df["score_band"] == "keep") & (df["llm_decision"] == "exclude")).sum()
        )
        if n_llm_overrides:
            print(f"  LLM-overridden keeps     : {n_llm_overrides:>6,}  (score≥3 but LLM=exclude → removed)")
        if not events_clean.empty:
            print(f"  reforms_events_clean.csv : {len(events_clean):>6,}  (events filtered to kept IDs)")
        print(sep)

    return {"kept": kept, "borderline": borderline_excl, "dropped": dropped,
            "events_clean": events_clean}


def _parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    p.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    p.add_argument("--quiet", action="store_true")
    return p.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    export_splits(args.input, args.output_dir, verbose=not args.quiet)
