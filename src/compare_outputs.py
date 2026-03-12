"""Helpers to compare baseline extraction with AI-enhanced outputs."""

from __future__ import annotations

import pandas as pd


def build_comparison(
    baseline_df: pd.DataFrame,
    ai_df: pd.DataFrame,
    output_csv_path,
    output_jsonl_path=None,
) -> None:
    """Join baseline and AI results for side-by-side review, saving CSV and/or JSONL."""
    if baseline_df.empty or ai_df.empty:
        if output_csv_path:
            output_csv_path.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame().to_csv(output_csv_path, index=False)
        if output_jsonl_path:
            output_jsonl_path.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame().to_json(output_jsonl_path, orient="records", lines=True, force_ascii=False)
        return

    ai_df = ai_df.copy()
    if "record_id" not in ai_df.columns:
        raise ValueError("AI output must include record_id for alignment")

    ai_df = ai_df.rename(
        columns={
            "clean_program_description_da": "ai_clean_program_description_da",
            "clean_program_description_en": "ai_clean_program_description_en",
        }
    )

    comparison_cols = {
        "line_description": "original_line_description",
        "rd_category": "original_rd_category",
        "decision": "original_decision",
        "confidence": "original_confidence",
    }

    baseline_subset = baseline_df.copy()
    baseline_subset = baseline_subset.rename(columns=comparison_cols)

    merged = baseline_subset.merge(ai_df, on="record_id", how="left", suffixes=("_baseline", "_ai"))

    def _lower(value):
        return str(value).lower() if value is not None else ""

    merged["changed_decision"] = merged.apply(
        lambda row: _lower(row.get("original_decision")) != _lower(row.get("ai_decision")),
        axis=1,
    )
    merged["changed_category"] = merged.apply(
        lambda row: _lower(row.get("original_rd_category")) != _lower(row.get("ai_rd_category")),
        axis=1,
    )

    ordered_cols = [
        "record_id",
        "original_line_description",
        "original_rd_category",
        "original_decision",
        "original_confidence",
        "ai_clean_program_description_da",
        "ai_clean_program_description_en",
        "ai_rd_category",
        "ai_decision",
        "ai_confidence",
        "changed_decision",
        "changed_category",
        "parse_issue",
    ]

    for col in ordered_cols:
        if col not in merged.columns:
            merged[col] = None

    merged = merged[ordered_cols]

    if output_csv_path:
        output_csv_path.parent.mkdir(parents=True, exist_ok=True)
        merged.to_csv(output_csv_path, index=False, encoding="utf-8")

    if output_jsonl_path:
        output_jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        merged.to_json(output_jsonl_path, orient="records", lines=True, force_ascii=False)
