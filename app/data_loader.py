"""Data loader and color/label constants for the Innovation Policy Dashboard."""

import os
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent

BUDGET_RESULTS_AI      = PROJECT_ROOT / "Data/output/budget/results_ai_verified.csv"
BUDGET_RESULTS         = PROJECT_ROOT / "Data/output/budget/results.csv"
REFORMS_EVENTS         = PROJECT_ROOT / "Data/output/reforms/output/reforms_events.csv"
REFORMS_MENTIONS       = PROJECT_ROOT / "Data/output/reforms/output/reforms_mentions.csv"
REFORM_PANEL           = PROJECT_ROOT / "Data/output/reforms/output/reform_panel.csv"
REFORM_PANEL_SUBTHEME  = PROJECT_ROOT / "Data/output/reforms/output/reform_panel_subtheme.csv"
REFORM_PANEL_CLEAN     = PROJECT_ROOT / "Data/output/reforms/output/reform_panel_clean.csv"
REFORM_INTENSITY       = PROJECT_ROOT / "Data/output/reforms/output/reform_intensity_score.csv"


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

# Budget R&D categories (actual values from pipeline output)
RD_CATEGORY_COLORS = {
    "direct_rd":           "#003189",   # navy
    "possible_rd":         "#009FDA",   # sky blue
    "innovation_system":   "#3D9349",   # green
    "institution_funding": "#E86B33",   # orange
    "other":               "#9B9B9B",   # grey
}

RD_CATEGORY_LABELS = {
    "direct_rd":           "Direct R&D",
    "possible_rd":         "Possible R&D",
    "innovation_system":   "Innovation System",
    "institution_funding": "Institutional Funding",
    "other":               "Other",
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

@st.cache_data
def load_budget():
    budget_path = BUDGET_RESULTS_AI if BUDGET_RESULTS_AI.exists() else BUDGET_RESULTS
    if not budget_path.exists():
        return pd.DataFrame()
    df = pd.read_csv(budget_path)

    # Normalize AI-verified schema to the baseline budget schema expected by the app.
    if "validated_amount_local" in df.columns:
        df["amount_local"] = pd.to_numeric(df["validated_amount_local"], errors="coerce").fillna(
            pd.to_numeric(df.get("amount_local"), errors="coerce")
        )
    if "ai_rd_category" in df.columns:
        df["rd_category"] = df["ai_rd_category"].fillna(df.get("rd_category"))
    if "ai_decision" in df.columns:
        df["decision"] = df["ai_decision"].fillna(df.get("decision"))
    if "ai_confidence" in df.columns:
        df["confidence"] = df["ai_confidence"].fillna(df.get("confidence"))
    if "ai_pillar" in df.columns:
        df["pillar"] = df["ai_pillar"].fillna(df.get("pillar"))
    if "currency" not in df.columns and "currency_baseline" in df.columns:
        df["currency"] = df["currency_baseline"]
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["amount_local"] = pd.to_numeric(df["amount_local"], errors="coerce")
    df = df.dropna(subset=["year", "amount_local"])
    df["year"] = df["year"].astype(int)
    if "rd_category" in df.columns:
        df["rd_category"] = df["rd_category"].str.lower().fillna("other")
        df["rd_category_label"] = df["rd_category"].map(
            lambda x: RD_CATEGORY_LABELS.get(x, x)
        )
    if "decision" in df.columns:
        df = df[df["decision"].isin(["include", "review"])]

    # Display-safe English fields for the app UI.
    def _coalesce(cols):
        existing = [c for c in cols if c in df.columns]
        if not existing:
            return pd.Series("", index=df.index, dtype="object")
        out = df[existing[0]].copy()
        for col in existing[1:]:
            mask = out.isna() | (out.astype(str).str.strip() == "")
            out = out.where(~mask, df[col])
        return out

    df["ministry_display"] = _coalesce(["section_name_en", "section_name"])
    df["program_display"] = _coalesce(
        ["clean_program_description_en", "program_description_en", "program_description"]
    )
    df["budget_line_display"] = _coalesce(
        ["clean_program_description_en", "line_description_en", "line_description", "program_description_en", "program_description"]
    )
    df["budget_category"] = _coalesce(["ai_pillar", "pillar", "rd_category"])
    df["budget_category_label"] = df["budget_category"]
    return df


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
    return BUDGET_RESULTS_AI.exists() or BUDGET_RESULTS.exists()

def reforms_available():
    return REFORMS_EVENTS.exists()


def get_app_password() -> str:
    try:
        secret_password = st.secrets.get("app_password", "")
        if secret_password:
            return str(secret_password)
    except Exception:
        pass
    return os.getenv("APP_PASSWORD", "innovationextract26")
