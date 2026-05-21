"""Data loader and color/label constants for the Innovation Policy Dashboard."""

import json
import os
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent

BUDGET_DATABASE          = PROJECT_ROOT / "Data/output/budget/rd_database.csv"
BUDGET_OUTPUT_DIR        = PROJECT_ROOT / "Data/output/budget"
FINANCE_BILLS_DIR        = PROJECT_ROOT / "Data/input/finance_bills"
KOREA_THEME_PANEL        = PROJECT_ROOT / "Data/output/budget/Korea/korea_theme_panel.csv"
BUDGET_GAP_DEEPDIVE_SUMMARY = PROJECT_ROOT / "Data/output/budget/country_gap_deepdive_summary.csv"
BUDGET_GAP_DEEPDIVE_DETAIL  = PROJECT_ROOT / "Data/output/budget/country_gap_deepdive_detail.csv"
# Legacy paths (old rule-based pipeline — kept for reference only)
BUDGET_RESULTS_AI            = PROJECT_ROOT / "Data/output/budget/results_ai_verified.csv"
BUDGET_RESULTS_REVIEW_STATUS = PROJECT_ROOT / "Data/output/budget/results_review_status.csv"
BUDGET_RESULTS               = PROJECT_ROOT / "Data/output/budget/results.csv"
REFORMS_EVENTS         = PROJECT_ROOT / "Data/output/reforms/output/reforms_events.csv"
REFORMS_MENTIONS       = PROJECT_ROOT / "Data/output/reforms/output/reforms_mentions.csv"
REFORM_PANEL           = PROJECT_ROOT / "Data/output/reforms/output/reform_panel.csv"
REFORM_PANEL_SUBTHEME  = PROJECT_ROOT / "Data/output/reforms/output/reform_panel_subtheme.csv"
REFORM_PANEL_CLEAN     = PROJECT_ROOT / "Data/output/reforms/output/reform_panel_clean.csv"
REFORM_INTENSITY       = PROJECT_ROOT / "Data/output/reforms/output/reform_intensity_score.csv"

# ── Multi-stage paths (new cross-verification pipeline) ───────────────────────
STAGE_PATHS = {
    "stage1": {
        "label":    "Stage 1 — GPT-4o-mini",
        "database": PROJECT_ROOT / "Data/output/reforms/output/reforms_events.csv",
        "panel":    PROJECT_ROOT / "Data/output/reforms/output/reform_panel.csv",
    },
    "stage2": {
        "label":    "Stage 2 — Claude Haiku",
        "database": PROJECT_ROOT / "Data/output/reforms/output_anthropic/reforms_database.csv",
        "panel":    PROJECT_ROOT / "Data/output/reforms/output_anthropic/reform_panel.csv",
    },
    "stage3": {
        "label":    "Stage 3 — GPT-4o-mini (2nd run)",
        "database": PROJECT_ROOT / "Data/output/reforms/output_gpt_personal/reforms_database.csv",
        "panel":    PROJECT_ROOT / "Data/output/reforms/output_gpt_personal/reform_panel.csv",
    },
    "merged": {
        "label":    "Stage 4 — Cross-verified (Merged)",
        "database": PROJECT_ROOT / "Data/output/reforms/output_merged/reforms_database.csv",
        "panel":    PROJECT_ROOT / "Data/output/reforms/output_merged/reform_panel.csv",
    },
}

MERGED_REFORMS_JSON_DIR = PROJECT_ROOT / "Data/output/reforms/reforms_json_merged"


def _parse_survey_year_list(value) -> list[int]:
    """Parse a comma-separated survey-year list into sorted unique ints."""
    if pd.isna(value):
        return []
    years: list[int] = []
    for part in str(value).split(","):
        token = part.strip()
        if not token:
            continue
        try:
            years.append(int(float(token)))
        except Exception:
            continue
    return sorted(set(years))


def _count_found_models(value) -> int:
    """Return the number of model runs that found a reform."""
    if isinstance(value, list):
        return len([v for v in value if str(v).strip()])
    if pd.isna(value):
        return 0
    parts = [p.strip() for p in str(value).split("|") if p.strip()]
    return len(parts)


def _verification_bucket(status: str, found_by_models=None) -> str:
    """Map raw CV metadata to a stable display/filter bucket."""
    status = str(status or "").strip()
    n_found = _count_found_models(found_by_models)

    explicit = {
        "three_model_confirmed": "All 3 models agreed",
        "two_model_included": "2 of 3 models",
        "one_model_included": "1 model only",
        "three_model_rejected": "Excluded — all 3 models agreed",
        "two_model_excluded": "Excluded — 2 of 3 models",
        "one_model_excluded": "Excluded — 1 model only",
        "consensus_confirmed": "Both models agreed",
        "disputed_included": "1 model only",
        "consensus_rejected": "Excluded — both models agreed",
        "disputed_excluded": "Excluded — 1 model only",
        "run_a_only": "1 model only",
        "run_b_only": "1 model only",
        "run_c_only": "1 model only",
    }
    if status in explicit:
        return explicit[status]

    # Backward compatibility for older merged outputs that only wrote "consensus".
    if status == "consensus":
        if n_found >= 3:
            return "All 3 models agreed"
        if n_found == 2:
            return "Both models agreed"
        if n_found == 1:
            return "1 model only"

    if n_found >= 3:
        return "All 3 models agreed"
    if n_found == 2:
        return "Both models agreed"
    if n_found == 1:
        return "1 model only"
    return ""


def _load_merged_reforms_from_json() -> pd.DataFrame:
    """Fallback loader for merged data when the CSV is stale or incomplete."""
    if not MERGED_REFORMS_JSON_DIR.exists():
        return pd.DataFrame()

    rows: list[dict] = []
    excluded_statuses = {
        "three_model_rejected",
        "two_model_excluded",
        "one_model_excluded",
        "consensus_rejected",
        "disputed_excluded",
    }

    for json_file in sorted(MERGED_REFORMS_JSON_DIR.glob("*.json")):
        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
        except Exception:
            continue

        country_code = data.get("country_code", "")
        country_name = data.get("country_name", "")
        survey_year = data.get("survey_year")
        reforms = data.get("all_reforms_including_excluded") or data.get("reforms") or []

        for reform in reforms:
            row = dict(reform)
            row["country_code"] = country_code
            row["country_name"] = country_name
            row["survey_year"] = survey_year
            status = str(row.get("cross_verification_status") or "")
            row["cv_included"] = status not in excluded_statuses if status else True
            found_by = row.get("found_by_models", [])
            if isinstance(found_by, list):
                row["found_by_models"] = " | ".join(str(v) for v in found_by if str(v).strip())
            rows.append(row)

    return pd.DataFrame(rows)


def _apply_clean_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Return only clean rows when cleaning columns exist.

    If scoring_filter (Pass 1) and adjudicator (Pass 2) have run, the
    mentions file contains score_band and llm_decision columns.  The clean
    view keeps:
      • rows with score_band == "keep"               (clear R&D content)
      • rows with score_band == "borderline" AND
                  llm_decision == "include"           (rescued by LLM)
    Falls back to the full dataset when those columns are absent so the app
    still works before cleaning has been run.
    """
    if "score_band" not in df.columns:
        return df  # cleaning hasn't run yet — show everything

    keep_mask = df["score_band"] == "keep"

    if "llm_decision" in df.columns:
        rescued = (df["score_band"] == "borderline") & (df["llm_decision"] == "include")
    else:
        # Pass 1 done but Pass 2 not yet — keep only the high-confidence band
        rescued = pd.Series(False, index=df.index)

    return df[keep_mask | rescued].copy()

# ── Labels ────────────────────────────────────────────────────────────────────

SUBTHEME_LABELS = {
    "rd_funding":              "Public R&D Funding",
    "innovation_instruments":  "Innovation Instruments",
    "research_infrastructure": "Research Infrastructure",
    "knowledge_transfer":      "Knowledge Transfer",
    "startup_ecosystem":       "Startup Ecosystem",
    "human_capital":           "Human Capital",
    "sectoral_rd":             "Sectoral / Mission R&D",
    "other":                   "Other",
}

ACTOR_LABELS = {
    "public":         "Public sector",
    "private":        "Private sector",
    "public_private": "Public–Private",
    "unknown":        "Unknown",
}

STAGE_LABELS = {
    "basic":             "Basic research",
    "applied":           "Applied research",
    "commercialization": "Commercialisation",
    "adoption":          "Adoption & diffusion",
    "unknown":           "Unknown",
}

STATUS_LABELS = {
    "implemented": "Implemented",
    "legislated":  "Legislated",
    "announced":   "Announced",
}

# ── Color palettes ────────────────────────────────────────────────────────────
# High-contrast, OECD-publication-quality categorical palettes.
# Each color is tested for readability on a white background.

# 8 innovation sub-types — ordered from warm to cool, all distinguishable
SUBTHEME_COLORS = {
    "rd_funding":              "#003189",   # OECD navy
    "innovation_instruments":  "#009FDA",   # OECD sky blue
    "research_infrastructure": "#00A389",   # teal
    "knowledge_transfer":      "#3D9349",   # green
    "human_capital":           "#8DC63F",   # lime green
    "startup_ecosystem":       "#F0A500",   # amber
    "sectoral_rd":             "#E86B33",   # OECD orange
    "other":                   "#9B9B9B",   # neutral grey
}

# Shorter display names for legends/axes
SUBTHEME_SHORT = {
    "rd_funding":              "R&D Funding",
    "innovation_instruments":  "Instruments",
    "research_infrastructure": "Infrastructure",
    "knowledge_transfer":      "Knowledge Transfer",
    "human_capital":           "Human Capital",
    "startup_ecosystem":       "Startups",
    "sectoral_rd":             "Sectoral R&D",
    "other":                   "Other",
}

# Budget R&D categories (values from budget pipeline)
RD_CATEGORY_COLORS = {
    "science_agency":          "#003189",   # navy
    "direct_rd":               "#003189",   # navy (legacy)
    "research_infrastructure": "#009FDA",   # sky blue
    "possible_rd":             "#009FDA",   # sky blue (legacy)
    "innovation_instruments":  "#3D9349",   # green
    "innovation_system":       "#3D9349",   # green (legacy)
    "institution_funding":     "#E86B33",   # orange (legacy)
    "unclear":                 "#9B9B9B",   # grey
    "other":                   "#9B9B9B",   # grey
}

RD_CATEGORY_LABELS = {
    "science_agency":          "Science Agency",
    "direct_rd":               "Direct R&D",
    "research_infrastructure": "Research Infrastructure",
    "higher_education":        "Higher Education",
    "possible_rd":             "Possible R&D",
    "innovation_instruments":  "Innovation Instruments",
    "innovation_system":       "Innovation System",
    "institution_funding":     "Institutional Funding",
    "unclear":                 "Unclassified",
    "other":                   "Other",
}

# Growth orientation — semantic colors, dark enough for chart labels
ORIENTATION_COLORS = {
    "growth_supporting":  "#3D9349",   # green
    "growth_hindering":   "#C1272D",   # red
    "mixed":              "#E86B33",   # orange
    "unclear_or_neutral": "#9B9B9B",   # grey
}

ORIENTATION_LABELS = {
    "growth_supporting":  "Growth-supporting",
    "growth_hindering":   "Growth-hindering",
    "mixed":              "Mixed",
    "unclear_or_neutral": "Unclear / Neutral",
}


# ── Data loaders ──────────────────────────────────────────────────────────────

def _budget_database_mtime() -> float | None:
    if not BUDGET_DATABASE.exists():
        return None
    return BUDGET_DATABASE.stat().st_mtime


@st.cache_data
def _load_budget_cached(_mtime: float | None):
    """Load the R&D budget database from the budget pipeline."""
    if _mtime is None or not BUDGET_DATABASE.exists():
        return pd.DataFrame()

    df = pd.read_csv(BUDGET_DATABASE)

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["amount_local"] = pd.to_numeric(df["amount_local"], errors="coerce")
    df = df.dropna(subset=["year", "amount_local"])
    df["year"] = df["year"].astype(int)

    def _currency_era(row) -> str:
        country = str(row.get("country", "") or "")
        currency = str(row.get("currency", "") or "").upper()
        year = row.get("year")
        if country == "Slovakia":
            if currency == "SKK":
                return "Pre-2009 SKK era"
            if currency == "EUR":
                return "2009+ EUR era"
        if country == "Finland":
            if currency == "FIM":
                return "Pre-2002 FIM era"
            if currency == "EUR":
                return "2002+ EUR era"
        if country == "France":
            if currency == "FRF":
                return "Pre-2002 FRF era"
            if currency == "EUR":
                return "2002+ EUR era"
        if country == "Netherlands":
            if currency == "NLG":
                return "Pre-2002 NLG era"
            if currency == "EUR":
                return "2002+ EUR era"
        if country == "Lithuania":
            if currency == "TAL":
                return "1993 talonas era"
            if currency == "LTL":
                return "1994-2014 litas era"
            if currency == "EUR":
                return "2015+ EUR era"
        if currency:
            return currency
        return ""

    df["currency_era"] = df.apply(_currency_era, axis=1)

    # Map category → rd_category for chart compatibility
    df["rd_category"] = df.get("category", pd.Series("other", index=df.index)).fillna("other")
    df["rd_category_label"] = df["rd_category"].map(lambda x: RD_CATEGORY_LABELS.get(x, x))

    # App display fields
    df["ministry_display"] = df["canonical_name"]
    df["budget_line_display"] = df["line_description_en"].fillna(df["canonical_name"]) if "line_description_en" in df.columns else df["canonical_name"]
    df["budget_category"] = df["rd_category"]
    df["budget_category_label"] = df["rd_category_label"]

    # Columns the app references that may not exist in the new pipeline output
    for col in ["confidence", "decision", "ai_decision", "ai_rationale"]:
        if col not in df.columns:
            df[col] = None

    return df


def load_budget():
    return _load_budget_cached(_budget_database_mtime())


@st.cache_data
def load_korea_theme_panel():
    """Load the Korea-only thematic R&D panel when available."""
    if not KOREA_THEME_PANEL.exists():
        return pd.DataFrame()

    df = pd.read_csv(KOREA_THEME_PANEL)
    for col in ("year", "page_number"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "amount_local" in df.columns:
        df["amount_local"] = pd.to_numeric(df["amount_local"], errors="coerce")
    if "year" in df.columns:
        df = df.dropna(subset=["year"]).copy()
        df["year"] = df["year"].astype(int)
    return df


@st.cache_data
def load_budget_gap_deepdive_summary():
    if not BUDGET_GAP_DEEPDIVE_SUMMARY.exists():
        return pd.DataFrame()
    df = pd.read_csv(BUDGET_GAP_DEEPDIVE_SUMMARY)
    for col in (
        "criticality_rank",
        "problem_years",
        "missing_agency_years",
        "outlier_agency_years",
        "missing_years",
        "outlier_years",
        "missing_years_with_run_logs",
        "missing_years_with_zero_row_docs",
        "missing_years_without_run_logs",
    ):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "criticality_score" in df.columns:
        df["criticality_score"] = pd.to_numeric(df["criticality_score"], errors="coerce")
    return df


@st.cache_data
def load_budget_gap_deepdive_detail():
    if not BUDGET_GAP_DEEPDIVE_DETAIL.exists():
        return pd.DataFrame()
    df = pd.read_csv(BUDGET_GAP_DEEPDIVE_DETAIL)
    for col in (
        "criticality_rank",
        "year",
        "missing_agency_years",
        "outlier_agency_years",
        "run_docs_for_year",
        "zero_row_docs_for_year",
    ):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "criticality_score" in df.columns:
        df["criticality_score"] = pd.to_numeric(df["criticality_score"], errors="coerce")
    return df


def _country_budget_output_dir(country: str) -> Path:
    return BUDGET_OUTPUT_DIR / str(country)


@st.cache_data
def load_budget_run_log() -> pd.DataFrame:
    path = BUDGET_OUTPUT_DIR / "run_log.jsonl"
    if not path.exists():
        return pd.DataFrame()
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except Exception:
                    pass
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce")
    return df


def _country_finance_bills_dir(country: str) -> Path:
    return FINANCE_BILLS_DIR / str(country)


@st.cache_data
def load_budget_country_gap_report(country: str) -> pd.DataFrame:
    country_dir = _country_budget_output_dir(country)
    if not country_dir.exists():
        return pd.DataFrame()

    matches = sorted(country_dir.glob("*_gap_report.csv"))
    if not matches:
        return pd.DataFrame()

    df = pd.read_csv(matches[0])
    for col in ("year",):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ("raw_row_amount", "series_amount", "prev_amount", "next_amount"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


@st.cache_data
def load_budget_country_gap_review_table(country: str) -> pd.DataFrame:
    country_dir = _country_budget_output_dir(country)
    if not country_dir.exists():
        return pd.DataFrame()

    matches = sorted(country_dir.glob("*_country_gap_review_table.csv"))
    if not matches:
        return pd.DataFrame()

    df = pd.read_csv(matches[0])
    for col in ("year", "run_log_rows_extracted", "docx_results_rows", "docx_audit_in_series_rows"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


@st.cache_data
def load_budget_country_notes(country: str) -> dict[str, str]:
    notes_dir = _country_finance_bills_dir(country)
    notes = {"source_notes": "", "quality_note": ""}
    if not notes_dir.exists():
        return notes

    source_path = notes_dir / "SOURCE_NOTES.md"
    quality_path = notes_dir / "QUALITY_NOTE.md"
    if source_path.exists():
        notes["source_notes"] = source_path.read_text(encoding="utf-8")
    if quality_path.exists():
        notes["quality_note"] = quality_path.read_text(encoding="utf-8")
    return notes


@st.cache_data
def load_reforms():
    if not REFORMS_EVENTS.exists():
        return pd.DataFrame()
    df = pd.read_csv(REFORMS_EVENTS)
    for col in ("implementation_year", "announcement_year", "legislation_year", "survey_year"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "sub_theme" in df.columns:
        df["sub_theme"]       = df["sub_theme"].fillna("other")
        df["sub_theme_label"] = df["sub_theme"].map(lambda x: SUBTHEME_LABELS.get(x, x))
        df["sub_theme_short"] = df["sub_theme"].map(lambda x: SUBTHEME_SHORT.get(x, x))
    if "rd_actor" in df.columns:
        df["rd_actor"] = df["rd_actor"].fillna("unknown")
        df["rd_actor_label"] = df["rd_actor"].map(lambda x: ACTOR_LABELS.get(x, x))
    if "rd_stage" in df.columns:
        df["rd_stage"] = df["rd_stage"].fillna("unknown")
        df["rd_stage_label"] = df["rd_stage"].map(lambda x: STAGE_LABELS.get(x, x))
    if "growth_orientation" in df.columns:
        df["growth_orientation"] = df["growth_orientation"].fillna("unclear_or_neutral")
        df["orientation_label"]  = df["growth_orientation"].map(
            lambda x: ORIENTATION_LABELS.get(x, x)
        )
    if "status" in df.columns:
        df["status_label"] = df["status"].map(lambda x: STATUS_LABELS.get(x, x))
    if "is_major_reform" in df.columns:
        df["is_major_reform"] = df["is_major_reform"].astype(bool)

    if "mention_survey_years" in df.columns or "survey_year" in df.columns:
        parsed_lists: list[list[int]] = []
        for _, row in df.iterrows():
            years = _parse_survey_year_list(row.get("mention_survey_years"))
            survey_year = row.get("survey_year")
            if pd.notna(survey_year):
                years = sorted(set(years + [int(float(survey_year))]))
            parsed_lists.append(years)

        df["all_seen_survey_years_list"] = parsed_lists
        df["all_seen_survey_years"] = [
            ", ".join(str(y) for y in years) if years else ""
            for years in parsed_lists
        ]
        df["first_seen_survey_year"] = [
            years[0] if years else pd.NA
            for years in parsed_lists
        ]
        df["last_seen_survey_year"] = [
            years[-1] if years else pd.NA
            for years in parsed_lists
        ]
        df["first_seen_survey_year"] = pd.to_numeric(
            df["first_seen_survey_year"], errors="coerce"
        ).astype("Int64")
        df["last_seen_survey_year"] = pd.to_numeric(
            df["last_seen_survey_year"], errors="coerce"
        ).astype("Int64")

    # Display year for app charts: prefer enacted timing, then survey timing.
    display_year = pd.Series(pd.NA, index=df.index, dtype="Int64")
    for col in (
        "implementation_year",
        "announcement_year",
        "first_seen_survey_year",
        "survey_year",
    ):
        if col not in df.columns:
            continue
        candidate = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        display_year = display_year.fillna(candidate)
    df["display_year"] = display_year
    return df


@st.cache_data
def load_reform_panel():
    """Load the reform panel. Prefers the clean panel when available."""
    path = REFORM_PANEL_CLEAN if REFORM_PANEL_CLEAN.exists() else REFORM_PANEL
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    for col in ("year", "survey_year"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


@st.cache_data
def load_reform_mentions():
    """Load reform mentions, applying the clean filter if cleaning has run.

    When score_band / llm_decision columns are present (added by the
    two-pass cleaning pipeline), only clean rows are returned.  Before
    cleaning runs, all rows are returned so the app still works.
    """
    if not REFORMS_MENTIONS.exists():
        return pd.DataFrame()
    df = pd.read_csv(REFORMS_MENTIONS)
    df = _apply_clean_filter(df)
    for col in ("implementation_year", "announcement_year", "survey_year"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "sub_theme" in df.columns:
        df["sub_theme"] = df["sub_theme"].fillna("other")
        df["sub_theme_label"] = df["sub_theme"].map(lambda x: SUBTHEME_LABELS.get(x, x))
    if "status" in df.columns:
        df["status_label"] = df["status"].map(lambda x: STATUS_LABELS.get(x, x))
    return df


@st.cache_data
def load_reform_panel_subtheme():
    if not REFORM_PANEL_SUBTHEME.exists():
        return pd.DataFrame()
    df = pd.read_csv(REFORM_PANEL_SUBTHEME)
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    return df


@st.cache_data
def load_reform_intensity():
    """Load the reform intensity score panel (country × year).

    Built by the two-pass cleaning pipeline.  Returns an empty DataFrame
    before cleaning has been run.
    """
    if not REFORM_INTENSITY.exists():
        return pd.DataFrame()
    df = pd.read_csv(REFORM_INTENSITY)
    df["survey_year"] = pd.to_numeric(df["survey_year"], errors="coerce").astype("Int64")
    return df


def budget_available():
    return BUDGET_DATABASE.exists()

def reforms_available():
    return REFORMS_EVENTS.exists() or any(
        p["database"].exists() for p in STAGE_PATHS.values()
    )

def available_reform_stages() -> dict:
    """Return {stage_key: label} for stages that have data on disk."""
    return {
        key: info["label"]
        for key, info in STAGE_PATHS.items()
        if info["database"].exists()
    }


def _enrich_reforms(df: pd.DataFrame) -> pd.DataFrame:
    """Apply labels, type coercions, and display columns to a reforms dataframe."""
    for col in ("implementation_year", "announcement_year", "legislation_year", "survey_year"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "sub_theme" in df.columns:
        df["sub_theme"]       = df["sub_theme"].fillna("other")
        df["sub_theme_label"] = df["sub_theme"].map(lambda x: SUBTHEME_LABELS.get(x, x))
        df["sub_theme_short"] = df["sub_theme"].map(lambda x: SUBTHEME_SHORT.get(x, x))
    if "rd_actor" in df.columns:
        df["rd_actor"]       = df["rd_actor"].fillna("unknown")
        df["rd_actor_label"] = df["rd_actor"].map(lambda x: ACTOR_LABELS.get(x, x))
    if "rd_stage" in df.columns:
        df["rd_stage"]       = df["rd_stage"].fillna("unknown")
        df["rd_stage_label"] = df["rd_stage"].map(lambda x: STAGE_LABELS.get(x, x))
    if "growth_orientation" in df.columns:
        df["growth_orientation"] = df["growth_orientation"].fillna("unclear_or_neutral")
        df["orientation_label"]  = df["growth_orientation"].map(
            lambda x: ORIENTATION_LABELS.get(x, x)
        )
    if "status" in df.columns:
        df["status_label"] = df["status"].map(lambda x: STATUS_LABELS.get(x, x))
    if "is_major_reform" in df.columns:
        df["is_major_reform"] = df["is_major_reform"].astype(bool)

    # Display year: prefer implementation → announcement → survey_year
    display_year = pd.Series(pd.NA, index=df.index, dtype="Int64")
    for col in ("implementation_year", "announcement_year", "survey_year"):
        if col not in df.columns:
            continue
        candidate = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        display_year = display_year.fillna(candidate)
    df["display_year"] = display_year

    # Normalise found_by_models (stored as pipe-separated model IDs)
    if "found_by_models" in df.columns:
        df["found_by_models"] = df["found_by_models"].fillna("").astype(str)
        # Build a short human-readable version: "GPT-4o-mini | Claude Haiku | GPT-4o-mini (2nd run)"
        _model_short = {
            "gpt-4o-mini":              "GPT-4o-mini",
            "gpt-4o":                   "GPT-4o",
            "claude-haiku-4-5-20251001":"Claude Haiku",
            "claude-3-5-haiku-20241022":"Claude Haiku",
            "claude-sonnet-4-20250514": "Claude Sonnet",
            "claude-sonnet-4-6":        "Claude Sonnet",
        }
        def _fmt_found_by(s):
            if not s:
                return ""
            parts = [p.strip() for p in s.split("|")]
            # Deduplicate while preserving order — two GPT runs differ by key, same model name
            seen, out = set(), []
            for p in parts:
                short = _model_short.get(p.lower(), p)
                if short not in seen:
                    seen.add(short)
                    out.append(short)
                else:
                    # second occurrence of same model name = cross-verification rerun
                    out.append(f"{short} (2nd run)")
            return " + ".join(out)
        df["found_by_display"] = df["found_by_models"].apply(_fmt_found_by)

    if "cross_verification_status" in df.columns or "found_by_models" in df.columns:
        statuses = df["cross_verification_status"] if "cross_verification_status" in df.columns else ""
        found_by = df["found_by_models"] if "found_by_models" in df.columns else ""
        if not isinstance(statuses, pd.Series):
            statuses = pd.Series([""] * len(df), index=df.index)
        if not isinstance(found_by, pd.Series):
            found_by = pd.Series([""] * len(df), index=df.index)
        df["verification_bucket"] = [
            _verification_bucket(status, models)
            for status, models in zip(statuses, found_by)
        ]

    # Normalise cv_included — safe bool coerce (CSV round-trip may yield string "True"/"False")
    if "cv_included" in df.columns:
        df["cv_included"] = df["cv_included"].astype(str).str.lower().map(
            {"true": True, "false": False, "1": True, "0": False}
        ).fillna(False)

    return df


@st.cache_data
def load_reforms_stage(stage: str, included_only: bool = True) -> pd.DataFrame:
    """Load reforms database for the given stage key.

    stage        : "stage1" | "stage2" | "stage3" | "merged"
    included_only: if True (default) and cv_included column exists, filter to
                   only included reforms. Pass False to get all incl. excluded.
    """
    info = STAGE_PATHS.get(stage)
    if info is None or not info["database"].exists():
        return pd.DataFrame()

    df = pd.read_csv(info["database"])

    # Backward compatibility: older merged CSVs may omit CV metadata even though
    # the underlying survey JSONs contain it. Rebuild from JSONs in that case.
    if stage == "merged":
        required_cv_cols = {"cross_verification_status", "found_by_models", "cv_included"}
        if not required_cv_cols.issubset(df.columns):
            fallback_df = _load_merged_reforms_from_json()
            if not fallback_df.empty:
                df = fallback_df

    # For merged stage, optionally filter to included reforms only
    # _enrich_reforms guarantees cv_included is bool dtype
    df = _enrich_reforms(df)
    if included_only and "cv_included" in df.columns:
        df = df[df["cv_included"] == True].copy()  # noqa: E712

    return df


@st.cache_data
def load_reform_panel_stage(stage: str) -> pd.DataFrame:
    """Load reform panel for the given stage key."""
    info = STAGE_PATHS.get(stage)
    if info is None or not info["panel"].exists():
        return pd.DataFrame()
    df = pd.read_csv(info["panel"])
    for col in ("year", "survey_year"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def get_app_password() -> str:
    try:
        secret_password = st.secrets.get("app_password", "")
        if secret_password:
            return str(secret_password)
    except Exception:
        pass
    return os.getenv("APP_PASSWORD", "innovationextract26")
