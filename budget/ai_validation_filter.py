from __future__ import annotations

from pathlib import Path
import pandas as pd

# Columns to keep in the final clean output — everything else is internal processing noise
_OUTPUT_COLS = [
    "country", "year",
    "section_code", "section_name", "section_name_en",
    "program_code", "clean_program_description_en", "line_description", "line_description_en",
    "amount_local", "currency",
    "rd_category", "ai_rd_category", "ai_pillar",
    "ai_decision", "ai_confidence", "ai_rationale",
    "taxonomy_score", "confidence", "decision",
    "source_file", "page_number",
]

NOISE_PATTERNS = [
    r"\bsvømme\w*",           # swimming pools/baths
    r"swimming pool",
    r"\bMindre byggearbejder\b",
    r"\bVedligeholdelse\b",
    r"\bAfdrag\b",            # debt repayments
    r"\bDepartementet\b",
    r"\bbygningstjeneste\b",
]


def filter_ai_validated(root: Path) -> list[Path]:
    """
    Post-process ai_validated_candidates_clean.csv files under root.

    Rules:
      - keep == True
      - ai_decision != 'exclude'
      - drop rows where program_description or line_description matches NOISE_PATTERNS
      - dedup by (program_code, validated_amount_local, page_number) keeping shortest program_description
    Returns list of written file paths.
    """
    written: list[Path] = []
    if not root.exists():
        return written

    for csv_path in root.rglob("ai_validated_candidates_clean.csv"):
        try:
            df = pd.read_csv(csv_path)
        except Exception:
            continue
        if df.empty:
            continue

        # Ensure required columns exist
        if "program_description" not in df.columns:
            df["program_description"] = ""
        if "line_description" not in df.columns:
            df["line_description"] = ""

        filt = df["keep"] == True
        filt &= df["ai_decision"].str.lower() != "exclude"
        pat = "|".join(NOISE_PATTERNS)
        filt &= ~df["program_description"].str.contains(pat, case=False, na=False)
        filt &= ~df["line_description"].str.contains(pat, case=False, na=False)
        out_df = df[filt].copy()
        out_df["__len"] = out_df["program_description"].astype(str).str.len()
        subset_cols = [c for c in ["program_code", "validated_amount_local", "page_number"] if c in out_df.columns]
        out_df = out_df.sort_values("__len")
        if subset_cols:
            out_df = out_df.drop_duplicates(subset=subset_cols, keep="first")
        out_df = out_df.drop(columns="__len")

        # Keep only clean output columns — drop internal processing fields
        keep_cols = [c for c in _OUTPUT_COLS if c in out_df.columns]
        out_df = out_df[keep_cols].copy()

        # Write amounts as plain integers (no scientific notation)
        if "amount_local" in out_df.columns:
            out_df["amount_local"] = (
                pd.to_numeric(out_df["amount_local"], errors="coerce")
                .round(0)
                .astype("Int64")
            )

        out_file = csv_path.with_name("ai_ready_for_verification.csv")
        out_df.to_csv(out_file, index=False)
        written.append(out_file)

    return written
