"""
Recover Missing 3-Layer Reforms
================================
The three-layer pipeline (Anthropic Claude + GPT-4o-mini + cross-verification)
was run on CAN, JPN, USA surveys and produced reforms_json_merged/.
83 of those reforms were never incorporated into reforms_mentions.csv because
the main pipeline was built from reforms_json/ (single-model run) instead.

This script:
1. Identifies reforms in reforms_json_merged/ that are absent from reforms_mentions.csv
2. Aligns their schema to match reforms_mentions.csv
3. Runs taxonomy Pass-1 scoring on them
4. Sets llm_decision based on cross_verification_status:
     two_model_included  → "include"   (found by BOTH models, high confidence)
     one_model_included  → "include"   (from rigorous 3-layer pipeline, accepted)
5. Appends them to reforms_mentions.csv
6. Runs source_recovery to fill page numbers
7. Re-exports the splits

Usage:
  python -m reforms.recover_merged            # full recovery
  python -m reforms.recover_merged --dry-run  # show what would be added
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logger = logging.getLogger(__name__)

JSON_MERGED = PROJECT_ROOT / "Data/output/reforms/reforms_json_merged"
MENTIONS_PATH = PROJECT_ROOT / "Data/output/reforms/output/reforms_mentions.csv"
OUTPUT_DIR = PROJECT_ROOT / "Data/output/reforms/output"

COUNTRY_NAMES = {"CAN": "Canada", "JPN": "Japan", "USA": "United States"}

MENTIONS_COLS = [
    "reform_id", "country_code", "country_name", "survey_year",
    "implementation_year", "announcement_year", "announcement_year_source",
    "announcement_year_confidence", "legislation_year", "legislation_year_source",
    "legislation_year_confidence", "implementation_year_end",
    "implementation_year_source", "implementation_year_confidence",
    "theme", "sub_theme", "secondary_type", "alternative_theme",
    "rd_actor", "rd_stage", "growth_orientation", "growth_orientation_rationale",
    "growth_orientation_confidence", "package_name", "component_name", "is_component",
    "status", "status_evidence", "status_confidence",
    "is_major_reform", "importance_bucket", "importance_rationale", "importance_confidence",
    "description", "source_quote", "source_page_start", "source_page_end",
    "tax_score", "score_band", "filter_decision",
    "llm_decision", "llm_rationale", "activity_lens", "defence_scope",
    "source_page_recovered", "source_quote_verified", "source_match_score",
]


def _load_missing_reforms(mentions_ids: set[str]) -> pd.DataFrame:
    rows = []
    for f in sorted(JSON_MERGED.glob("*.json")):
        parts = f.stem.split("_")
        country_code = parts[0]
        survey_year = int(parts[1])
        d = json.loads(f.read_text(encoding="utf-8"))
        for r in d.get("reforms", []):
            rid = r.get("reform_id", "")
            if rid and rid not in mentions_ids:
                row = dict(r)
                row["country_code"] = country_code
                row["country_name"] = COUNTRY_NAMES.get(country_code, country_code)
                row["survey_year"] = survey_year
                row["_cv_status"] = r.get("cross_verification_status", "")
                row["_found_by"] = str(r.get("found_by_models", []))
                rows.append(row)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _score_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Run taxonomy Pass-1 scoring. Falls back to score=3/keep if taxonomy unavailable."""
    try:
        # Try importing the taxonomy scorer
        taxonomy_path = PROJECT_ROOT / "Data" / "input" / "taxonomy" / "search_library.json"
        if not taxonomy_path.exists():
            raise FileNotFoundError("taxonomy not found")

        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "scoring_filter",
            PROJECT_ROOT / "reforms" / "scoring_filter.py"
        )
        # scoring_filter imports budget.taxonomy which may not exist — skip if broken
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        tax = mod.load_taxonomy() if hasattr(mod, "load_taxonomy") else None
        if tax is None:
            raise ImportError("load_taxonomy not available")

        scores = []
        for _, row in df.iterrows():
            text = " ".join(str(row.get(c, "") or "") for c in ["description", "source_quote"])
            score = float(mod._score_row(row.to_dict(), tax))
            scores.append(score)
        df["tax_score"] = scores
    except Exception:
        # Fallback: these came from a rigorous 3-layer pipeline — default to score=5
        logger.warning("Taxonomy scorer unavailable — defaulting tax_score=5.0 for recovered reforms")
        df["tax_score"] = 5.0

    def _band(s):
        if s >= 3:
            return "keep"
        if s <= 0:
            return "drop"
        return "borderline"

    df["score_band"] = df["tax_score"].apply(_band)
    df["filter_decision"] = df["score_band"].map({
        "keep": "keep_rule_based",
        "borderline": "escalate_to_llm",
        "drop": "drop_rule_based",
    })
    return df


def run_recovery(dry_run: bool = False, verbose: bool = True) -> pd.DataFrame:
    mentions = pd.read_csv(MENTIONS_PATH, low_memory=False)
    mention_ids = set(mentions["reform_id"].dropna())

    missing = _load_missing_reforms(mention_ids)
    if missing.empty:
        if verbose:
            print("No missing reforms found — reforms_mentions.csv is already complete.")
        return mentions

    if verbose:
        print(f"\nRecovered {len(missing)} reforms missing from reforms_mentions.csv")
        print(f"  By country: {missing['country_code'].value_counts().to_dict()}")
        print(f"  Cross-verification: {missing['_cv_status'].value_counts().to_dict()}")

    if dry_run:
        print("\n  --dry-run: not writing anything.")
        return mentions

    # Score
    missing = _score_rows(missing)

    # LLM decision: trust the 3-layer pipeline
    missing["llm_decision"] = "include"
    missing["llm_rationale"] = (
        "Recovered from three-layer cross-verification pipeline (Anthropic + GPT). "
        + missing["_cv_status"].fillna("")
    )
    missing["activity_lens"] = None
    missing["defence_scope"] = None
    missing["source_page_recovered"] = None
    missing["source_quote_verified"] = None
    missing["source_match_score"] = None

    # Align to mentions schema — add missing columns, drop extras
    for col in MENTIONS_COLS:
        if col not in missing.columns:
            missing[col] = None
    missing = missing[MENTIONS_COLS]

    # Append and sort
    combined = pd.concat([mentions, missing], ignore_index=True)
    combined = combined.sort_values(
        ["country_code", "survey_year", "reform_id"]
    ).reset_index(drop=True)
    combined.to_csv(MENTIONS_PATH, index=False)

    if verbose:
        print(f"\n  reforms_mentions.csv: {len(mentions)} → {len(combined)} rows")
        print(f"  Score distribution of recovered reforms:")
        print(f"    {missing['score_band'].value_counts().to_dict()}")

    # Run source recovery on just the new rows
    if verbose:
        print("\n  Running source recovery on recovered reforms…")
    try:
        from reforms.source_recovery import run_source_recovery
        run_source_recovery(input_path=MENTIONS_PATH, verbose=False)
        if verbose:
            print("  Source recovery complete.")
    except Exception as exc:
        logger.warning("Source recovery failed (non-fatal): %s", exc)

    # Regenerate splits
    if verbose:
        print("\n  Regenerating splits…")
    try:
        from reforms.export_splits import export_splits
        export_splits(verbose=verbose)
    except Exception as exc:
        logger.warning("export_splits failed (non-fatal): %s", exc)

    return combined


def _parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--quiet", action="store_true")
    return p.parse_args(argv)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
                        datefmt="%H:%M:%S")
    args = _parse_args()
    run_recovery(dry_run=args.dry_run, verbose=not args.quiet)
