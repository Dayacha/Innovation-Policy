"""Temporal smoothing helpers for budget extraction.

Stage 1 (during extraction): compute_temporal_prior()
  - Called per budget line in budget_extractor.py
  - Checks T-1 and T+1 years for the same program with 'include' decision
  - Returns a score boost that nudges borderline 'review' lines to 'include'
  - No-op if prior_results_df is None (e.g. first year processed)

Stage 2 (post-processing on results.csv): apply_temporal_smoother()
  - Runs on the full results table after all years are extracted
  - Pass 1 LOCAL:  upgrade 'review' if ≥1 adjacent year (T±1) is 'include'
  - Pass 2 GLOBAL: upgrade ALL remaining 'review' rows for programs that have
                   ≥ min_global_include_to_upgrade 'include' years in the series;
                   downgrade to 'skip' programs that never reach 'include' and
                   appear as 'review' for ≥ min_review_to_downgrade years.
"""

from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher

import pandas as pd


# Stage 1 tunable
TEMPORAL_PRIOR_BOOST: float = 1.5
FUZZY_MATCH_THRESHOLD: float = 0.85

# Stage 2 tunable
MIN_NEIGHBORS_TO_UPGRADE: int = 2       # ≥N include neighbors (T±1) → upgrade review
                                        # Require BOTH T-1 and T+1 to be include before
                                        # auto-upgrading a review row (was 1 — too aggressive)
MIN_GLOBAL_INCLUDE_TO_UPGRADE: int = 3  # ≥N include years in series → upgrade remaining reviews
                                        # Raised from 2: need at least 3 confirmed years before
                                        # pulling sparse review rows up automatically
MIN_REVIEW_TO_DOWNGRADE: int = 3        # 0 include + ≥N review years → downgrade to skip

# Stage 2 Pass 3: spike / low-budget anomaly detection
SPIKE_RATIO_THRESHOLD: float = 3.0      # flag if amount > N × median (was 5.0 — missed 3× spikes)
DROP_RATIO_THRESHOLD: float = 0.20      # flag if amount < N × median (was 0.10 — missed 80% drops)


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class TemporalPrior:
    boost: float = 0.0
    match_type: str = ""
    matched_years: str = ""
    rationale: str = ""


# ── Shared utilities ──────────────────────────────────────────────────────────

def _normalize_text(value) -> str:
    if pd.isna(value):
        return ""
    return " ".join(str(value).strip().lower().split())


def _description_for_match(row) -> str:
    """Best available description string for matching, normalized.

    Works with both pd.Series (row from iterrows/apply) and plain dict.
    """
    get = row.get if isinstance(row, (dict, pd.Series)) else lambda k, d="": getattr(row, k, d)
    for col in ("program_description", "line_description", "program_description_en", "line_description_en"):
        val = _normalize_text(get(col, ""))
        if val:
            return val
    return ""


def _similarity(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    return SequenceMatcher(None, left, right).ratio()


def _safe_col(df: pd.DataFrame, col: str) -> pd.Series:
    """Return df[col] if it exists, else empty-string Series of same length."""
    if col in df.columns:
        return df[col].fillna("").astype(str)
    return pd.Series("", index=df.index)


# ── Stage 1: compute_temporal_prior (called during extraction) ────────────────

def _prepare_history(history_df: pd.DataFrame | None) -> pd.DataFrame:
    """Normalize the prior-year results df for fast lookup."""
    if history_df is None or history_df.empty:
        return pd.DataFrame()
    df = history_df.copy()
    df["year_int"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year_int", "country", "section_code"]).copy()
    df["year_int"] = df["year_int"].astype(int)
    df["program_code_norm"] = _safe_col(df, "program_code").str.strip()
    df["match_description"] = df.apply(_description_for_match, axis=1)
    df["decision_norm"] = _safe_col(df, "decision").str.lower()
    return df


def compute_temporal_prior(
    *,
    country: str,
    year: str | int,
    section_code: str,
    program_code: str,
    program_description: str,
    line_description: str,
    history_df: pd.DataFrame | None,
) -> TemporalPrior:
    """Return a score boost from T-1/T+1 existing results (Stage 1).

    Priority:
      1. Exact program_code match in T±1 with decision='include' → full boost
      2. Fuzzy description match (≥ FUZZY_MATCH_THRESHOLD) in T±1 with 'include' → full boost
      3. No match → zero boost (no penalty)

    Note: during sequential forward extraction T+1 will almost always be
    unavailable; the function handles this gracefully.
    """
    history = _prepare_history(history_df)
    if history.empty:
        return TemporalPrior()

    try:
        year_int = int(year)
    except (TypeError, ValueError):
        return TemporalPrior()

    desc = _normalize_text(program_description or line_description)
    prog_code_norm = _normalize_text(program_code)

    candidates = history[
        (history["country"].astype(str) == str(country))
        & (history["section_code"].astype(str) == str(section_code))
        & (history["year_int"].isin([year_int - 1, year_int + 1]))
    ]
    if candidates.empty:
        return TemporalPrior()

    # 1. Exact code match
    if prog_code_norm:
        exact_include = candidates[
            (candidates["program_code_norm"].map(_normalize_text) == prog_code_norm)
            & (candidates["decision_norm"] == "include")
        ]
        if not exact_include.empty:
            years = sorted(exact_include["year_int"].unique().tolist())
            return TemporalPrior(
                boost=TEMPORAL_PRIOR_BOOST,
                match_type="exact_code",
                matched_years=",".join(str(y) for y in years),
                rationale=f"temporal prior boost from exact code match in year(s) {years}",
            )

    # 2. Fuzzy description match
    if not desc:
        return TemporalPrior()
    fuzzy_include = candidates[
        candidates["match_description"].map(lambda other: _similarity(desc, other) >= FUZZY_MATCH_THRESHOLD)
        & (candidates["decision_norm"] == "include")
    ]
    if not fuzzy_include.empty:
        years = sorted(fuzzy_include["year_int"].unique().tolist())
        return TemporalPrior(
            boost=TEMPORAL_PRIOR_BOOST,
            match_type="fuzzy_description",
            matched_years=",".join(str(y) for y in years),
            rationale=f"temporal prior boost from fuzzy description match in year(s) {years}",
        )

    return TemporalPrior()


# ── Stage 2: apply_temporal_smoother (post-processing on results.csv) ─────────

def _build_program_key(df: pd.DataFrame) -> pd.Series:
    """Build a stable grouping key: 'country|section_code|program_code_or_description'.

    Falls back to normalized description when program_code is blank so that
    programs without codes can still be grouped across years.
    Rows where BOTH code and description are empty get key suffix '' and are
    excluded from global rules (not enough info to group reliably).
    """
    code = _safe_col(df, "program_code").str.strip().map(_normalize_text)
    has_code = code.ne("")
    key_body = code.where(has_code, df["match_description"])
    return df["country"].astype(str) + "|" + df["section_code"].astype(str) + "|" + key_body


def _append_reason(df: pd.DataFrame, idx, reason: str) -> None:
    """Append temporal smoothing reason to the rationale column in-place."""
    df.at[idx, "temporal_smoothing_reason"] = reason
    if "rationale" in df.columns:
        base = str(df.at[idx, "rationale"]) if pd.notna(df.at[idx, "rationale"]) else ""
        df.at[idx, "rationale"] = (base.rstrip("; ") + "; " + reason).lstrip("; ")


def apply_temporal_smoother(
    results_df: pd.DataFrame,
    min_neighbors_to_upgrade: int = MIN_NEIGHBORS_TO_UPGRADE,
    min_global_include_to_upgrade: int = MIN_GLOBAL_INCLUDE_TO_UPGRADE,
    min_review_to_downgrade: int = MIN_REVIEW_TO_DOWNGRADE,
) -> pd.DataFrame:
    """Apply temporal consistency smoothing over the full results table.

    Pass 1 LOCAL
        For each 'review' row, count how many of its T-1 / T+1 neighbors
        (same country, section, program) have decision='include'.
        If count ≥ min_neighbors_to_upgrade → upgrade to 'include'.
        Uses a dict lookup — O(n) not O(n²).

    Pass 2 GLOBAL
        For each unique program (grouped by country|section|program_code):
          • If ≥ min_global_include_to_upgrade years are 'include' and there
            are still 'review' rows → upgrade those reviews to 'include'.
          • If 0 years are 'include' and ≥ min_review_to_downgrade years are
            'review' → downgrade those reviews to 'skip'.

    Args:
        results_df:                  Full results table (all years, all countries).
        min_neighbors_to_upgrade:    Default 1 — enough to upgrade a borderline row.
        min_global_include_to_upgrade: Default 2 — need at least two confirmed years
                                       before upgrading all reviews for a program.
        min_review_to_downgrade:     Default 3 — persistent review with no include.

    Returns:
        Copy of results_df with updated 'decision' and 'temporal_smoothing_reason'.
    """
    if results_df.empty:
        return results_df

    df = results_df.copy()

    # Prep working columns
    df["year_int"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year_int"]).copy()
    df["year_int"] = df["year_int"].astype(int)
    df["program_code_norm"] = _safe_col(df, "program_code").str.strip()
    df["match_description"] = df.apply(_description_for_match, axis=1)
    df["decision"] = _safe_col(df, "decision")
    df["temporal_smoothing_reason"] = ""

    df["_prog_key"] = _build_program_key(df)

    # ── Pass 0: SPIKE / ANOMALY DETECTION (runs FIRST) ───────────────────────
    # Detect anomalous amounts BEFORE the upgrade passes so that spike rows
    # are never silently promoted to 'include' by a single-neighbor rule.
    # For each program series with ≥3 'include' years, flag rows whose amount
    # deviates more than SPIKE_RATIO_THRESHOLD above or DROP_RATIO_THRESHOLD
    # below the series median.
    if "amount_local" in df.columns:
        df["_amount_num"] = pd.to_numeric(df["amount_local"], errors="coerce")
        for prog_key, grp in df.groupby("_prog_key", sort=False):
            key_body = str(prog_key).rsplit("|", 1)[-1]
            if not key_body:
                continue
            inc_idx = grp.index[grp["decision"].str.lower() == "include"]
            if len(inc_idx) < 3:
                continue  # not enough data for reliable spike detection
            amounts = df.loc[inc_idx, "_amount_num"].dropna()
            if amounts.empty or amounts.median() == 0:
                continue
            series_median = amounts.median()
            # Check all rows (include + review) so previously-flagged reviews
            # that happen to be anomalous are also caught.
            for idx in grp.index:
                current_decision = df.at[idx, "decision"].lower()
                if current_decision == "skip":
                    continue
                amt = df.at[idx, "_amount_num"]
                if pd.isna(amt):
                    continue
                ratio = amt / series_median
                if ratio > SPIKE_RATIO_THRESHOLD:
                    df.at[idx, "decision"] = "review"
                    _append_reason(
                        df, idx,
                        f"spike detected: amount={amt:.0f} is {ratio:.1f}× series median={series_median:.0f}; needs AI verification",
                    )
                elif ratio < DROP_RATIO_THRESHOLD:
                    df.at[idx, "decision"] = "review"
                    _append_reason(
                        df, idx,
                        f"low-budget anomaly: amount={amt:.0f} is {ratio:.3f}× series median={series_median:.0f}; needs AI verification",
                    )
        df = df.drop(columns=["_amount_num"], errors="ignore")

    # ── Pass 1: LOCAL ─────────────────────────────────────────────────────────
    # Build (prog_key, year_int) → decision dict for O(1) neighbor lookup.
    # Only upgrade 'review' rows — never override a spike/anomaly 'review'.
    key_year_decision: dict[tuple, str] = {
        (k, y): d.lower()
        for k, y, d in zip(df["_prog_key"], df["year_int"], df["decision"])
    }

    review_rows = df.index[df["decision"].str.lower() == "review"]
    for idx in review_rows:
        row = df.loc[idx]
        key, yr = row["_prog_key"], row["year_int"]
        # Skip if this row was just flagged as an anomaly
        if "spike detected" in str(row.get("temporal_smoothing_reason", "")) or \
           "low-budget anomaly" in str(row.get("temporal_smoothing_reason", "")):
            continue
        n_include_neighbors = sum(
            1 for delta in (-1, 1)
            if key_year_decision.get((key, yr + delta)) == "include"
        )
        if n_include_neighbors >= min_neighbors_to_upgrade:
            df.at[idx, "decision"] = "include"
            _append_reason(
                df, idx,
                f"local upgrade: {n_include_neighbors} include neighbor(s) in T±1",
            )

    # Rebuild lookup after local upgrades
    key_year_decision = {
        (k, y): d.lower()
        for k, y, d in zip(df["_prog_key"], df["year_int"], df["decision"])
    }

    # ── Pass 2: GLOBAL ────────────────────────────────────────────────────────
    for prog_key, grp in df.groupby("_prog_key", sort=False):
        # Skip programs without a meaningful key (empty code + empty description)
        key_body = str(prog_key).rsplit("|", 1)[-1]
        if not key_body:
            continue

        decisions = grp["decision"].str.lower()
        n_include = (decisions == "include").sum()
        n_review = (decisions == "review").sum()

        if n_review == 0:
            continue  # nothing to change

        if n_include >= min_global_include_to_upgrade:
            # Upgrade remaining reviews — but not anomaly-flagged rows
            review_idx = grp.index[decisions == "review"]
            for idx in review_idx:
                reason = str(df.at[idx, "temporal_smoothing_reason"])
                if "spike detected" in reason or "low-budget anomaly" in reason:
                    continue
                df.at[idx, "decision"] = "include"
                _append_reason(
                    df, idx,
                    f"global upgrade: program has {n_include} include year(s) across full series",
                )
        elif n_include == 0 and n_review >= min_review_to_downgrade:
            # Downgrade persistent review-only programs
            review_idx = grp.index[decisions == "review"]
            for idx in review_idx:
                df.at[idx, "decision"] = "skip"
                _append_reason(
                    df, idx,
                    f"global downgrade: 0 include years, {n_review} review-only year(s) in series",
                )

    return df.drop(
        columns=["year_int", "program_code_norm", "match_description", "_prog_key"],
        errors="ignore",
    )
