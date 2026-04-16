"""
Post-extraction deduplication and unit normalisation for budget.

Handles three systematic issues found in multi-file government budget data:

1. Unit normalisation
   Documents in the same year may report amounts in different units
   (e.g. "dollar" vs "thousand"). Normalise everything to a single
   unit (AUD thousands for Australia) before any aggregation.

2. Parent-child double counting
   A section_total that equals (or closely matches) the sum of its
   children line_items / program_totals is redundant. Keep the children,
   mark the parent as aggregation_role='redundant'.

3. Cross-file duplicate amounts
   The same agency amount appears in multiple Appropriation Acts for the
   same year (No1 main bill + No5 supplementary). Keep only the main bill
   (lowest Act number) occurrence.

Run order:
    normalise_units → deduplicate_within_year → flag_temporal_outliers

Usage:
    from budget.dedup import run_dedup
    df = run_dedup(df, country="Australia")
"""

from __future__ import annotations

import logging
import re
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

__all__ = ["run_dedup"]

# ---------------------------------------------------------------------------
# Unit normalisation
# ---------------------------------------------------------------------------

# How to convert each unit label to AUD thousands (multiply by this factor).
# "thousand" → already in thousands → factor 1.
# "dollar"   → full dollars → divide by 1000 → factor 0.001.
# "million"  → millions → multiply by 1000 → factor 1000.
# "billion"  → billions → multiply by 1,000,000 → factor 1,000,000.
_UNIT_TO_THOUSANDS: dict[str, float] = {
    "thousand": 1.0,
    "thousands": 1.0,
    "dollar": 0.001,
    "dollars": 0.001,
    "aud": 0.001,
    "million": 1_000.0,
    "millions": 1_000.0,
    "billion": 1_000_000.0,
    "billions": 1_000_000.0,
    # GBP / CAD etc — same numeric scales
    "gbp": 0.001,
    "cad": 0.001,
    "nzd": 0.001,
    "dkk": 0.001,
    "nok": 0.001,
    "sek": 0.001,
    "eur": 0.001,
}


def _normalise_unit(amount: float, unit: str) -> tuple[float, str]:
    """Convert amount to thousands. Returns (normalised_amount, 'thousand')."""
    factor = _UNIT_TO_THOUSANDS.get(str(unit).strip().lower(), 1.0)
    return round(amount * factor, 3), "thousand"


def normalise_units(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise all amounts to the same unit (thousands of local currency)."""
    df = df.copy()
    df["amount_local"] = pd.to_numeric(df["amount_local"], errors="coerce")

    normalised_amounts = []
    normalised_units = []

    for _, row in df.iterrows():
        amt = row["amount_local"]
        unit = str(row.get("unit", "thousand")).lower().strip()
        if pd.isna(amt):
            normalised_amounts.append(None)
            normalised_units.append(unit)
        else:
            na, nu = _normalise_unit(float(amt), unit)
            normalised_amounts.append(na)
            normalised_units.append(nu)

    df["amount_local"] = normalised_amounts
    df["unit"] = normalised_units
    return df


# ---------------------------------------------------------------------------
# Act number extraction (for cross-file dedup priority)
# ---------------------------------------------------------------------------

_RE_ACT_NO = re.compile(r"\bNo\.?\s*(\d+)\b", re.IGNORECASE)


def _act_number(source_file: str) -> int:
    """Extract the Appropriation Act number from filename. Returns 999 if not found."""
    m = _RE_ACT_NO.search(str(source_file))
    return int(m.group(1)) if m else 999


# ---------------------------------------------------------------------------
# Within-year parent-child deduplication
# ---------------------------------------------------------------------------

def _dedup_year_group(group: pd.DataFrame) -> pd.DataFrame:
    """
    Dedup a single (country, year) group:

    1. Cross-file: for each (section_name_en, line_description_en, amount_local)
       triple that appears in multiple source files, keep only the row from
       the lowest Act number.

    2. Parent-child: for each section, if section_total ≈ sum(children),
       mark section_total as aggregation_role='redundant'.

    3. Exact-value duplicate within same file: if section_total == program_total
       == line_item for the same amount (same number extracted 3 ways from same
       page), keep only the most granular (line_item > program_total > section_total).
    """
    group = group.copy()
    if "aggregation_role" not in group.columns:
        group["aggregation_role"] = "count"
    if "dedup_notes" not in group.columns:
        group["dedup_notes"] = ""

    group["_act_no"] = group["source_file"].apply(_act_number)

    # ── Step 1: Cross-file dedup (same description + same amount, multiple files) ──
    key_cols = ["line_description_en", "amount_local"]
    seen_keys: set = set()
    for idx in group.sort_values("_act_no").index:
        row = group.loc[idx]
        amt = row["amount_local"]
        if pd.isna(amt):
            continue
        key = (str(row["line_description_en"]), round(float(amt), -2))
        if key in seen_keys:
            group.at[idx, "aggregation_role"] = "redundant"
            group.at[idx, "dedup_notes"] = (
                f"Cross-file dup: same amount in Act No.{int(row['_act_no'])}"
            )
        else:
            seen_keys.add(key)

    # ── Step 2: Exact-value within-file triple counting ──
    # (section_total = program_total = line_item all same amount)
    _TYPE_PRIORITY = {"line_item": 0, "program_total": 1, "section_total": 2}
    amount_to_types: dict = {}
    for idx, row in group[group["aggregation_role"] == "count"].iterrows():
        amt = row["amount_local"]
        if pd.isna(amt) or float(amt) < 100:
            continue
        key = (str(row["source_file"]), round(float(amt), -2))
        if key not in amount_to_types:
            amount_to_types[key] = []
        amount_to_types[key].append((idx, row["item_type"]))

    for key, entries in amount_to_types.items():
        if len(entries) <= 1:
            continue
        # Multiple item_types with same amount in same file → keep most granular
        best_priority = min(_TYPE_PRIORITY.get(t, 9) for _, t in entries)
        for idx, itype in entries:
            if _TYPE_PRIORITY.get(itype, 9) > best_priority:
                group.at[idx, "aggregation_role"] = "redundant"
                group.at[idx, "dedup_notes"] = (
                    f"Exact-value dup: {itype} same as more granular row"
                )

    # ── Step 3: Parent ≈ sum(children) ──
    active = group[group["aggregation_role"] == "count"].copy()
    section_totals = active[active["item_type"] == "section_total"]
    children = active[active["item_type"].isin(["line_item", "program_total"])]

    for idx, st_row in section_totals.iterrows():
        st_amt = st_row["amount_local"]
        if pd.isna(st_amt) or float(st_amt) < 100:
            continue
        st_section = str(st_row.get("section_name_en", "")).lower()

        # Match children by section_name proximity
        child_mask = children["section_name_en"].str.lower().str.contains(
            re.escape(st_section[:30]), na=False
        ) if st_section else pd.Series(False, index=children.index)

        child_sum = children.loc[child_mask, "amount_local"].sum()
        if child_sum == 0:
            continue

        ratio = float(st_amt) / float(child_sum) if child_sum else 999
        if 0.85 <= ratio <= 1.15:
            # Section total ≈ sum of children → redundant
            group.at[idx, "aggregation_role"] = "redundant"
            group.at[idx, "dedup_notes"] = (
                f"Parent≈children: section_total {st_amt:,.0f} ≈ child_sum {child_sum:,.0f}"
            )
        elif ratio < 0.85:
            # Children are just the R&D subset → section_total is broader context
            group.at[idx, "aggregation_role"] = "context"
            group.at[idx, "dedup_notes"] = (
                f"Parent>children: section_total {st_amt:,.0f} >> R&D subset {child_sum:,.0f}"
            )

    group = group.drop(columns=["_act_no"])
    return group


# ---------------------------------------------------------------------------
# Temporal outlier flagging
# ---------------------------------------------------------------------------

def flag_temporal_outliers(
    df: pd.DataFrame,
    iqr_multiplier: float = 3.0,
    min_years: int = 5,
) -> pd.DataFrame:
    """
    Flag rows in years where the total 'count' amount is an extreme outlier.

    Only flags — does not remove. Adds 'temporal_flag' column with reason.
    Operates per (country, item_type) to avoid mixing totals with line items.
    """
    df = df.copy()
    if "temporal_flag" not in df.columns:
        df["temporal_flag"] = ""

    count_rows = df[df.get("aggregation_role", pd.Series("count", index=df.index)) != "redundant"]

    for country, cdf in count_rows.groupby("country"):
        yearly = (
            cdf[cdf["decision"] == "include"]
            .groupby("year")["amount_local"]
            .sum()
            .sort_index()
        )
        if len(yearly) < min_years:
            continue

        q1, q3 = yearly.quantile(0.25), yearly.quantile(0.75)
        iqr = q3 - q1
        lo = q1 - iqr_multiplier * iqr
        hi = q3 + iqr_multiplier * iqr

        outlier_years = yearly[(yearly < lo) | (yearly > hi)].index.tolist()
        for yr in outlier_years:
            mask = (df["country"] == country) & (df["year"].astype(str) == str(yr))
            yr_total = yearly.get(yr, 0)
            df.loc[mask, "temporal_flag"] = (
                f"Outlier: year total {yr_total:,.0f} outside [{lo:,.0f}, {hi:,.0f}]"
            )
            logger.warning(
                f"[{country}] {yr}: total {yr_total:,.0f} is a temporal outlier "
                f"(IQR range [{lo:,.0f}, {hi:,.0f}]) — check for unit error or double count"
            )

    return df


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_dedup(df: pd.DataFrame, country: Optional[str] = None) -> pd.DataFrame:
    """
    Run the full dedup pipeline on results DataFrame.

    Steps:
      1. Normalise units → all amounts in thousands of local currency
      2. Within-year parent-child + cross-file dedup (per country)
      3. Flag temporal outliers

    Parameters
    ----------
    df      : full results DataFrame
    country : if set, only process rows for this country

    Returns
    -------
    Modified DataFrame with new columns:
      aggregation_role : 'count' | 'redundant' | 'context'
      dedup_notes      : reason for any aggregation_role change
      temporal_flag    : non-empty string if the year is a temporal outlier
    """
    if country:
        mask = df["country"] == country
        df_target = df[mask].copy()
        df_rest = df[~mask].copy()
    else:
        df_target = df.copy()
        df_rest = pd.DataFrame()

    before = len(df_target)

    # Step 1: unit normalisation
    df_target = normalise_units(df_target)

    # Step 2: within-year dedup
    parts = []
    for (ctry, yr), grp in df_target.groupby(["country", "year"]):
        parts.append(_dedup_year_group(grp))
    df_target = pd.concat(parts, ignore_index=True) if parts else df_target

    redundant = (df_target.get("aggregation_role", pd.Series()) == "redundant").sum()
    logger.info(
        f"Dedup: {redundant} rows marked redundant out of {before} "
        f"({'all' if not country else country})"
    )

    # Step 3: temporal outlier flagging
    df_target = flag_temporal_outliers(df_target)

    if not df_rest.empty:
        df_out = pd.concat([df_target, df_rest], ignore_index=True)
    else:
        df_out = df_target

    return df_out
