from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

import pandas as pd

from budget import config as cfg


QUEUE_CSV = cfg.OUTPUT_DIR / "llm_budget_review_queue.csv"
APPLIED_AUDIT_CSV = cfg.OUTPUT_DIR / "llm_budget_review_applied.csv"

_MANUAL_COLUMNS = [
    "review_status",
    "original_source_checked",
    "review_decision",
    "double_count_check",
    "amount_local_override",
    "unit_override",
    "currency_override",
    "canonical_name_override",
    "category_override",
    "review_notes",
    "reviewed_by",
    "reviewed_at",
]

_QUEUE_COLUMNS = [
    "review_row_id",
    "country",
    "year",
    "source_file",
    "source_path",
    "page_number",
    "section_name_en",
    "line_description_en",
    "amount_local",
    "unit",
    "currency",
    "item_type",
    "rd_category",
    "pipeline_decision",
    "pipeline_confidence",
    "pipeline_rationale",
    "pipeline_notes",
    "pipeline_review_status",
    "llm_model",
    "extraction_pass",
    "source_row_duplicate_count",
    "country_year_name_count",
    "current_final_exact_name_match",
    "current_final_exact_amount_match",
    *_MANUAL_COLUMNS,
]

_TRUTHY = {"1", "true", "yes", "y"}
_ACCEPTED_DOUBLE_COUNT = {"unique", "context_only", "not_double_counted", "no_repeat"}


def _clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _clean_num(value: object) -> str:
    num = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(num):
        return ""
    return f"{float(num):.4f}"


def _row_id(row: pd.Series) -> str:
    payload = "|".join(
        [
            _clean_text(row.get("country")),
            _clean_text(row.get("year")),
            _clean_text(row.get("source_file")),
            _clean_num(row.get("page_number")),
            _clean_text(row.get("line_description_en")).lower(),
            _clean_num(row.get("amount_local")),
            _clean_text(row.get("currency")),
        ]
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


def _merge_key(df: pd.DataFrame) -> pd.Series:
    return (
        df.get("country", pd.Series("", index=df.index)).fillna("").astype(str).str.strip()
        + "|"
        + pd.to_numeric(df.get("year", pd.Series(index=df.index)), errors="coerce").fillna(-1).astype(int).astype(str)
        + "|"
        + df.get("source_file", pd.Series("", index=df.index)).fillna("").astype(str).str.strip()
        + "|"
        + pd.to_numeric(df.get("page_number", pd.Series(index=df.index)), errors="coerce").round(4).fillna(-1).astype(str)
        + "|"
        + df.get("line_description_en", pd.Series("", index=df.index)).fillna("").astype(str).str.strip().str.lower()
        + "|"
        + pd.to_numeric(df.get("amount_local", pd.Series(index=df.index)), errors="coerce").round(4).fillna(-1).astype(str)
        + "|"
        + df.get("currency", pd.Series("", index=df.index)).fillna("").astype(str).str.strip()
    )


def build_review_queue(
    output_dir: Path = cfg.OUTPUT_DIR,
    queue_csv: Path = QUEUE_CSV,
) -> pd.DataFrame:
    output_dir = Path(output_dir)
    status_path = output_dir / "results_review_status.csv"
    results_path = output_dir / "results.csv"
    rd_path = output_dir / "rd_database.csv"

    if not status_path.exists():
        raise FileNotFoundError(status_path)

    review_df = pd.read_csv(status_path)
    for col in ("year", "page_number", "amount_local", "confidence"):
        if col in review_df.columns:
            review_df[col] = pd.to_numeric(review_df[col], errors="coerce")
    review_df = review_df.rename(
        columns={
            "decision": "pipeline_decision",
            "confidence": "pipeline_confidence",
            "rationale": "pipeline_rationale",
            "review_status": "pipeline_review_status",
        }
    )
    review_df["review_row_id"] = review_df.apply(_row_id, axis=1)
    review_df["source_path"] = review_df.apply(
        lambda row: str(Path("Data/input/finance_bills") / str(row["country"]) / str(row["source_file"])),
        axis=1,
    )

    if results_path.exists():
        results_df = pd.read_csv(results_path, low_memory=False)
        for col in ("year", "page_number", "amount_local", "confidence"):
            if col in results_df.columns:
                results_df[col] = pd.to_numeric(results_df[col], errors="coerce")
        results_df["_merge_key"] = _merge_key(results_df)
        extra_cols = [
            "_merge_key",
            "unit",
            "item_type",
            "notes",
            "llm_model",
            "extraction_pass",
        ]
        review_df["_merge_key"] = _merge_key(review_df)
        review_df = review_df.merge(
            results_df[extra_cols].drop_duplicates("_merge_key"),
            on="_merge_key",
            how="left",
        )
        review_df = review_df.rename(columns={"notes": "pipeline_notes"})
        review_df = review_df.drop(columns=["_merge_key"])
    else:
        for col in ("unit", "item_type", "pipeline_notes", "llm_model", "extraction_pass"):
            review_df[col] = ""

    dup_key = [
        "country",
        "year",
        "source_file",
        "page_number",
        "line_description_en",
        "amount_local",
    ]
    review_df["source_row_duplicate_count"] = review_df.groupby(dup_key, dropna=False)["review_row_id"].transform("size")
    review_df["country_year_name_count"] = review_df.groupby(
        ["country", "year", "line_description_en"],
        dropna=False,
    )["review_row_id"].transform("size")

    if rd_path.exists():
        rd_df = pd.read_csv(rd_path, low_memory=False)
        rd_df["year"] = pd.to_numeric(rd_df.get("year"), errors="coerce")
        rd_df["amount_local"] = pd.to_numeric(rd_df.get("amount_local"), errors="coerce")
        rd_df["canonical_name_norm"] = rd_df.get("canonical_name", pd.Series("", index=rd_df.index)).fillna("").astype(str).str.strip().str.lower()
        exact_name_hits = {
            (str(row.country), int(row.year), str(row.canonical_name_norm))
            for row in rd_df.dropna(subset=["year"]).itertuples()
        }
        exact_amount_hits = {
            (str(row.country), int(row.year), _clean_num(row.amount_local))
            for row in rd_df.dropna(subset=["year"]).itertuples()
            if _clean_num(row.amount_local)
        }
        review_df["current_final_exact_name_match"] = review_df.apply(
            lambda row: (
                (str(row["country"]), int(row["year"]), _clean_text(row["line_description_en"]).lower()) in exact_name_hits
                if pd.notna(row["year"])
                else False
            ),
            axis=1,
        )
        review_df["current_final_exact_amount_match"] = review_df.apply(
            lambda row: (
                (str(row["country"]), int(row["year"]), _clean_num(row["amount_local"])) in exact_amount_hits
                if pd.notna(row["year"])
                else False
            ),
            axis=1,
        )
    else:
        review_df["current_final_exact_name_match"] = False
        review_df["current_final_exact_amount_match"] = False

    existing_manual = pd.DataFrame(columns=["review_row_id", *_MANUAL_COLUMNS])
    if queue_csv.exists():
        existing_manual = pd.read_csv(queue_csv, low_memory=False)
        keep_cols = [col for col in ["review_row_id", *_MANUAL_COLUMNS] if col in existing_manual.columns]
        existing_manual = existing_manual[keep_cols].drop_duplicates("review_row_id")

    review_df = review_df.merge(existing_manual, on="review_row_id", how="left", suffixes=("", "_existing"))
    for col in _MANUAL_COLUMNS:
        if col not in review_df.columns:
            review_df[col] = ""
        review_df[col] = review_df[col].fillna("")
    review_df["review_status"] = review_df["review_status"].replace("", pd.NA).fillna("pending_original_source_review")

    for col in _QUEUE_COLUMNS:
        if col not in review_df.columns:
            review_df[col] = ""

    out = review_df[_QUEUE_COLUMNS].sort_values(
        ["country", "year", "source_file", "page_number", "line_description_en"],
        kind="stable",
    ).reset_index(drop=True)
    queue_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(queue_csv, index=False)
    return out


def load_accepted_review_rows(
    output_dir: Path = cfg.OUTPUT_DIR,
    queue_csv: Path = QUEUE_CSV,
) -> pd.DataFrame:
    if not Path(queue_csv).exists():
        return pd.DataFrame()

    df = pd.read_csv(queue_csv, low_memory=False)
    if df.empty:
        return pd.DataFrame()

    decision = df.get("review_decision", pd.Series("", index=df.index)).fillna("").astype(str).str.strip().str.lower()
    checked = df.get("original_source_checked", pd.Series("", index=df.index)).fillna("").astype(str).str.strip().str.lower()
    double_count = df.get("double_count_check", pd.Series("", index=df.index)).fillna("").astype(str).str.strip().str.lower()

    accepted = df[
        decision.eq("include")
        & checked.isin(_TRUTHY)
        & double_count.isin(_ACCEPTED_DOUBLE_COUNT)
    ].copy()
    if accepted.empty:
        return pd.DataFrame()

    accepted["canonical_name"] = accepted["canonical_name_override"].fillna("").astype(str).str.strip()
    accepted.loc[accepted["canonical_name"].eq(""), "canonical_name"] = (
        accepted["line_description_en"].fillna("").astype(str).str.strip()
    )
    accepted["category"] = accepted["category_override"].fillna("").astype(str).str.strip()
    accepted.loc[accepted["category"].eq(""), "category"] = (
        accepted["rd_category"].fillna("").astype(str).str.strip()
    )
    accepted["amount_local"] = pd.to_numeric(
        accepted["amount_local_override"].where(
            accepted["amount_local_override"].fillna("").astype(str).str.strip().ne(""),
            accepted["amount_local"],
        ),
        errors="coerce",
    )
    accepted["unit"] = accepted["unit_override"].where(
        accepted["unit_override"].fillna("").astype(str).str.strip().ne(""),
        accepted["unit"],
    )
    accepted["currency"] = accepted["currency_override"].where(
        accepted["currency_override"].fillna("").astype(str).str.strip().ne(""),
        accepted["currency"],
    )
    accepted["item_type"] = "verified_override"
    accepted["series_notes"] = accepted.apply(
        lambda row: "; ".join(
            part
            for part in [
                "accepted from llm budget review queue after original-source check",
                _clean_text(row.get("review_notes")),
            ]
            if part
        ),
        axis=1,
    )

    preferred = [
        "country",
        "year",
        "canonical_name",
        "category",
        "amount_local",
        "unit",
        "currency",
        "item_type",
        "line_description_en",
        "source_file",
        "page_number",
        "series_notes",
        "review_row_id",
    ]
    for col in preferred:
        if col not in accepted.columns:
            accepted[col] = ""
    return accepted[preferred].reset_index(drop=True)


def write_applied_audit(df: pd.DataFrame, audit_csv: Path = APPLIED_AUDIT_CSV) -> None:
    audit_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(audit_csv, index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build/apply the LLM budget review queue")
    parser.add_argument("--build-queue", action="store_true")
    parser.add_argument("--show-accepted", action="store_true")
    args = parser.parse_args()

    if args.build_queue:
        df = build_review_queue()
        print(f"Wrote {len(df)} review rows to {QUEUE_CSV}")

    if args.show_accepted:
        df = load_accepted_review_rows()
        print(df.to_string(index=False) if not df.empty else "No accepted review rows found.")


if __name__ == "__main__":
    main()
