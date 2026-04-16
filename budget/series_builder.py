"""
Traceable series builder for budget.

Builds the final R&D time series with full audit trail — every number
in the output links back to an exact row in results.csv, which links
to a source_file + page_number in the original document.

Outputs two files:
  series.csv          — one row per (country, year, canonical_agency)
                        with amount + source_file + page_number
  series_audit.csv    — one row per CONTRIBUTING ROW from results.csv
                        so you can verify every number against the source

The audit file is the key: you can open it, find any number, and it tells
you exactly which file and page to open to verify it.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

__all__ = ["build_series", "SERIES_COLS", "AUDIT_COLS"]

SERIES_COLS = [
    "country", "year", "canonical_name", "agency_type",
    "amount_local", "unit", "currency",
    "n_source_rows",        # how many results.csv rows contributed
    "source_files",         # comma-separated list of source files used
    "pages",                # comma-separated page numbers
    "item_types_used",      # which item_types were picked
    "results_row_ids",      # index positions in results.csv for full traceability
    "selection_rule",       # why this row was chosen (audit note)
]

AUDIT_COLS = [
    "country", "year", "canonical_name",
    "results_row_id",       # exact index in results.csv
    "amount_local", "unit", "currency",
    "item_type", "section_name_en", "line_description_en",
    "source_file", "page_number",
    "decision", "confidence",
    "role",                 # "selected" | "duplicate_skipped" | "parent_skipped"
    "skip_reason",
]


# ---------------------------------------------------------------------------
# Core selection logic — picks ONE best row per (agency, year)
# ---------------------------------------------------------------------------

_TYPE_RANK = {"section_total": 0, "program_total": 1, "line_item": 2}


def _select_best_row(
    matches: pd.DataFrame,
    agency_def: dict,
) -> tuple[pd.Series | None, pd.DataFrame]:
    """
    From matching rows for one agency in one year, pick the single
    best representative row. Returns (selected_row, all_matches_with_roles).

    Selection logic:
      1. Prefer preferred_item_type in order
      2. Among same type, prefer the row from the lowest Act number
         (main Appropriation Act = No1, not supplementary)
      3. Among same type + same Act, take largest amount
         (most complete appropriation, not a sub-item)

    All non-selected rows get role='duplicate_skipped' with a reason.
    """
    if matches.empty:
        return None, matches

    matches = matches.copy()
    matches["_role"] = "duplicate_skipped"
    matches["_skip_reason"] = ""

    # Extract Act number for priority
    import re
    def act_num(fname):
        m = re.search(r"\bNo\.?\s*(\d+)\b", str(fname), re.IGNORECASE)
        return int(m.group(1)) if m else 999

    matches["_act_no"] = matches["source_file"].apply(act_num)
    matches["_type_rank"] = matches["item_type"].map(_TYPE_RANK).fillna(9)

    preferred = agency_def.get("preferred_item_type", ["section_total", "program_total"])

    selected_idx = None
    for itype in preferred:
        subset = matches[matches["item_type"] == itype]
        if subset.empty:
            continue
        # Lowest Act number first, then largest amount
        subset = subset.sort_values(["_act_no", "amount_local"], ascending=[True, False])
        selected_idx = subset.index[0]
        break

    if selected_idx is None:
        # Fallback: lowest act, largest amount
        sorted_m = matches.sort_values(["_act_no", "amount_local"], ascending=[True, False])
        selected_idx = sorted_m.index[0]

    matches.at[selected_idx, "_role"] = "selected"

    # Label why others were skipped
    selected_row = matches.loc[selected_idx]
    for idx in matches.index:
        if idx == selected_idx:
            continue
        row = matches.loc[idx]
        if row["source_file"] != selected_row["source_file"] and abs(
            float(row["amount_local"] or 0) - float(selected_row["amount_local"] or 0)
        ) < 1000:
            matches.at[idx, "_skip_reason"] = (
                f"same amount in different file (Act No.{int(row['_act_no'])})"
            )
        elif row["item_type"] != selected_row["item_type"]:
            matches.at[idx, "_skip_reason"] = (
                f"lower priority item_type ({row['item_type']})"
            )
        else:
            matches.at[idx, "_skip_reason"] = "duplicate amount"

    return matches.loc[selected_idx], matches


# ---------------------------------------------------------------------------
# Agency matching
# ---------------------------------------------------------------------------

def _matches_agency(row: pd.Series, agency_def: dict) -> bool:
    desc = str(row.get("line_description_en", "")).lower()
    section = str(row.get("section_name_en", "")).lower()
    combined = f"{desc} {section}"
    return any(v.lower() in combined for v in agency_def["name_variants"])


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_series(
    results_df: pd.DataFrame,
    registry: pd.DataFrame,
    country: str,
    output_dir: Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build the traceable R&D series for a country.

    Parameters
    ----------
    results_df : full results DataFrame (already cleaned + deduped)
    registry   : agency_registry DataFrame (from agency_classifier)
    country    : country to process
    output_dir : if set, writes series.csv and series_audit.csv

    Returns
    -------
    (series_df, audit_df)
    """
    include_agencies = registry[
        (registry["country"] == country)
        & (registry["include_in_series"].astype(str).str.lower().isin(["true", "1", "yes"]))
    ].to_dict("records")

    if not include_agencies:
        logger.warning(f"No include_in_series=True agencies for {country} in registry")
        return pd.DataFrame(), pd.DataFrame()

    df = results_df[
        (results_df["country"] == country)
        & (results_df["decision"] == "include")
    ].copy()
    df["amount_local"] = pd.to_numeric(df["amount_local"], errors="coerce")
    df = df.dropna(subset=["amount_local"])
    df = df.reset_index(drop=True)  # ensure clean integer index
    df["_original_idx"] = df.index

    series_rows = []
    audit_rows = []

    for agency in include_agencies:
        canonical_name = agency.get("canonical_name", agency["agency_name"])
        agency_def = {
            "name_variants": [agency["agency_name"]],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
        }

        for year, year_df in df.groupby("year"):
            # Match rows for this agency
            matches = year_df[
                year_df.apply(lambda r: _matches_agency(r, agency_def), axis=1)
                & year_df["item_type"].isin(["section_total", "program_total", "line_item"])
            ].copy()

            if matches.empty:
                # Gap — record in audit
                audit_rows.append({
                    "country": country, "year": year,
                    "canonical_name": canonical_name,
                    "results_row_id": None,
                    "amount_local": None, "unit": None, "currency": None,
                    "item_type": None, "section_name_en": None,
                    "line_description_en": None,
                    "source_file": None, "page_number": None,
                    "decision": None, "confidence": None,
                    "role": "gap", "skip_reason": "no matching rows",
                })
                continue

            selected, all_matches = _select_best_row(matches, agency_def)

            if selected is None:
                continue

            # Series row
            series_rows.append({
                "country": country,
                "year": year,
                "canonical_name": canonical_name,
                "agency_type": agency.get("agency_type", ""),
                "amount_local": float(selected["amount_local"]),
                "unit": selected.get("unit"),
                "currency": selected.get("currency"),
                "n_source_rows": len(all_matches),
                "source_files": selected.get("source_file", ""),
                "pages": str(int(selected.get("page_number", 0) or 0)),
                "item_types_used": selected.get("item_type", ""),
                "results_row_ids": str(int(selected.get("_original_idx", -1))),
                "selection_rule": (
                    f"Preferred {selected.get('item_type')} from "
                    f"{str(selected.get('source_file',''))[-25:]} "
                    f"p.{int(selected.get('page_number',0) or 0)}"
                ),
            })

            # Audit rows — ALL matching rows with roles
            for idx, row in all_matches.iterrows():
                audit_rows.append({
                    "country": country,
                    "year": year,
                    "canonical_name": canonical_name,
                    "results_row_id": int(row.get("_original_idx", -1)),
                    "amount_local": float(row["amount_local"]),
                    "unit": row.get("unit"),
                    "currency": row.get("currency"),
                    "item_type": row.get("item_type"),
                    "section_name_en": row.get("section_name_en"),
                    "line_description_en": row.get("line_description_en"),
                    "source_file": row.get("source_file"),
                    "page_number": row.get("page_number"),
                    "decision": row.get("decision"),
                    "confidence": row.get("confidence"),
                    "role": row.get("_role", ""),
                    "skip_reason": row.get("_skip_reason", ""),
                })

    series_df = pd.DataFrame(series_rows).sort_values(["canonical_name", "year"])
    audit_df = pd.DataFrame(audit_rows).sort_values(["canonical_name", "year"])

    logger.info(
        f"Series [{country}]: {len(include_agencies)} agencies, "
        f"{series_df['year'].nunique() if not series_df.empty else 0} years with data, "
        f"{len(series_df)} data points, "
        f"{len(audit_df[audit_df['role']=='gap'])} gaps"
    )

    if output_dir:
        output_dir = Path(output_dir)
        s_path = output_dir / f"{country.lower().replace(' ','_')}_series.csv"
        a_path = output_dir / f"{country.lower().replace(' ','_')}_series_audit.csv"
        series_df.to_csv(s_path, index=False)
        audit_df.to_csv(a_path, index=False)
        logger.info(f"Series → {s_path}")
        logger.info(f"Audit  → {a_path}")

    return series_df, audit_df
