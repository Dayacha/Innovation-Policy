"""
Apply reviewed gap/outlier corrections to country series outputs.

This is a post-review pass for `budget.gap_review`. It reads
`<country>_gap_review.csv`, applies accepted `correct` / `drop` verdicts to the
country detail series, rebuilds the country totals file, and rebuilds the
combined `rd_database.csv`.

The apply step is intentionally conservative:
  - `keep` and `unclear` do nothing
  - `drop` removes all rows for that (canonical_name, year)
  - `correct` replaces all rows for that (canonical_name, year) with one
    reviewed override row

Usage:
  python -m budget.gap_review_apply --country Canada
  python -m budget.gap_review_apply --country Canada --min-confidence 0.8
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

import pandas as pd

from budget import config as cfg
from budget.canonical_series import build_totals_series
from budget.compile import build_combined_database

logger = logging.getLogger(__name__)

_GENERIC_PROGRAM_RE = re.compile(
    r"(?:^| )(program|bureau)(?:$| )",
    flags=re.IGNORECASE,
)


def _amount_matches_existing(existing: pd.DataFrame, amount: float, tol: float = 1.0) -> tuple[bool, str]:
    if existing.empty:
        return False, "no_existing_rows"
    values = pd.to_numeric(existing.get("amount_local"), errors="coerce").dropna().tolist()
    if not values:
        return True, "matched_missing_placeholder"
    if any(abs(float(v) - amount) <= tol for v in values):
        return True, "matched_single_row"
    if abs(sum(float(v) for v in values) - amount) <= tol:
        return True, "matched_row_sum"
    return False, "no_exact_source_match"


def apply_gap_review(
    country: str,
    output_dir: Path = cfg.OUTPUT_DIR,
    min_confidence: float = 0.75,
    include_programs: bool = False,
) -> pd.DataFrame:
    output_dir = Path(output_dir)
    country_dir = output_dir / country
    cname = country.lower().replace(" ", "_")

    review_path = country_dir / f"{cname}_gap_review.csv"
    series_path = country_dir / f"{cname}_docx_series.csv"
    totals_path = country_dir / f"{cname}_docx_totals.csv"
    audit_path = country_dir / f"{cname}_gap_review_applied.csv"

    if not review_path.exists():
        raise FileNotFoundError(review_path)
    if not series_path.exists():
        raise FileNotFoundError(series_path)

    review_df = pd.read_csv(review_path)
    series_df = pd.read_csv(series_path)

    audit_rows = []

    for _, row in review_df.iterrows():
        year = int(row["year"])
        canonical_name = str(row["canonical_name"])
        verdict = str(row["verdict"])
        confidence = float(pd.to_numeric(pd.Series([row.get("confidence")]), errors="coerce").fillna(0).iloc[0])

        mask = (
            (series_df["year"] == year)
            & (series_df["canonical_name"].astype(str) == canonical_name)
        )
        existing = series_df.loc[mask].copy()

        audit = {
            "country": country,
            "year": year,
            "canonical_name": canonical_name,
            "verdict": verdict,
            "old_rows": len(existing),
            "old_amounts": " | ".join(str(v) for v in existing.get("amount_local", pd.Series(dtype=object)).tolist()),
            "review_amount": row.get("correct_amount_local", None),
            "review_unit": row.get("correct_unit", None),
            "preferred_source_files": row.get("preferred_source_files", "[]"),
            "reason": row.get("reason", ""),
            "confidence": confidence,
            "applied": False,
            "skip_reason": "",
            "match_reason": "",
        }

        if verdict not in {"correct", "drop"}:
            audit["skip_reason"] = "verdict_not_actionable"
            audit_rows.append(audit)
            continue
        if confidence < min_confidence:
            audit["skip_reason"] = "below_confidence_threshold"
            audit_rows.append(audit)
            continue
        if not include_programs and _GENERIC_PROGRAM_RE.search(canonical_name):
            audit["skip_reason"] = "generic_program_like_name"
            audit_rows.append(audit)
            continue

        review_amount = row.get("correct_amount_local", None)
        amount_local = None
        if pd.notna(review_amount):
            amount_local = float(review_amount)
            if amount_local <= 0:
                audit["skip_reason"] = "non_positive_review_amount"
                audit_rows.append(audit)
                continue
            matched, match_reason = _amount_matches_existing(existing, amount_local)
            audit["match_reason"] = match_reason
            if not matched:
                audit["skip_reason"] = match_reason
                audit_rows.append(audit)
                continue
        elif verdict == "correct":
            audit["skip_reason"] = "missing_review_amount"
            audit_rows.append(audit)
            continue

        # Remove existing rows for that agency-year only after the case passes filters.
        series_df = series_df.loc[~mask].copy()

        if verdict == "drop":
            audit["applied"] = True
            audit_rows.append(audit)
            continue

        # verdict == "correct" → replace with one authoritative override row
        template = existing.iloc[0].to_dict() if not existing.empty else {}
        preferred = []
        try:
            preferred = json.loads(row.get("preferred_source_files", "[]") or "[]")
        except Exception:
            preferred = []

        source_file = preferred[0] if preferred else template.get("source_file", "gap_review_override")
        unit = row.get("correct_unit") if pd.notna(row.get("correct_unit")) else template.get("unit", "")
        currency = template.get("currency", cfg.COUNTRY_CONTEXT.get(country, {}).get("currency", "LOCAL"))
        category = template.get("category", row.get("category", ""))

        new_row = {
            "country": country,
            "year": year,
            "canonical_name": canonical_name,
            "category": category,
            "amount_local": amount_local,
            "unit": unit,
            "currency": currency,
            "item_type": "section_total",
            "line_description_en": canonical_name,
            "source_file": source_file,
            "page_number": row.get("evidence_page", ""),
            "series_notes": (
                str(template.get("series_notes", "") or "")
                + " [gap_review override]"
            ).strip(),
        }
        series_df = pd.concat([series_df, pd.DataFrame([new_row])], ignore_index=True)
        audit["applied"] = True
        audit_rows.append(audit)

    series_df = series_df.sort_values(["country", "canonical_name", "year", "source_file"]).reset_index(drop=True)
    series_df.to_csv(series_path, index=False)
    logger.info(f"Updated series → {series_path}")

    totals_df = build_totals_series(series_df, country=country)
    if not totals_df.empty:
        totals_df.to_csv(totals_path, index=False)
        logger.info(f"Updated totals → {totals_path}")

    audit_df = pd.DataFrame(audit_rows)
    audit_df.to_csv(audit_path, index=False)
    logger.info(f"Apply audit → {audit_path}")

    build_combined_database(output_dir=output_dir)
    return audit_df


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")

    parser = argparse.ArgumentParser(description="Apply reviewed gap corrections to country series")
    parser.add_argument("--country", required=True)
    parser.add_argument("--min-confidence", type=float, default=0.75)
    parser.add_argument("--include-programs", action="store_true")
    args = parser.parse_args()

    df = apply_gap_review(
        country=args.country,
        min_confidence=args.min_confidence,
        include_programs=args.include_programs,
    )
    if not df.empty:
        print("\n=== Applied corrections ===")
        print(df.loc[df["applied"] == True, ["year", "canonical_name", "verdict", "review_amount", "confidence", "match_reason"]].to_string(index=False))
