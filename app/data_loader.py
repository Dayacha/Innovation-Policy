"""
Data loader for the Innovation Policy Dashboard.

Reads both pipeline outputs and caches them in Streamlit's session cache.
"""

from pathlib import Path

import pandas as pd
import streamlit as st

# Resolve project root from this file's location (app/ → project root)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

BUDGET_RESULTS   = PROJECT_ROOT / "Data/output/budget/results.csv"
REFORMS_EVENTS   = PROJECT_ROOT / "Data/output/reforms/output/reforms_events.csv"
REFORMS_MENTIONS = PROJECT_ROOT / "Data/output/reforms/output/reforms_mentions.csv"
REFORM_PANEL     = PROJECT_ROOT / "Data/output/reforms/output/reform_panel.csv"

# ── Labels ──────────────────────────────────────────────────────────────────

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
    "public":         "Public (universities, councils, PROs)",
    "private":        "Private (firms)",
    "public_private": "Public–Private (collaborative)",
    "unknown":        "Unknown",
}

STAGE_LABELS = {
    "basic":             "Basic research",
    "applied":           "Applied research",
    "commercialization": "Commercialisation",
    "adoption":          "Diffusion & adoption",
    "unknown":           "Unknown",
}

STATUS_LABELS = {
    "implemented": "Implemented",
    "legislated":  "Legislated",
    "announced":   "Announced",
    "recommended": "OECD Recommended",
}

ORIENTATION_COLORS = {
    "growth_supporting":  "#2ecc71",
    "growth_hindering":   "#e74c3c",
    "mixed":              "#f39c12",
    "unclear_or_neutral": "#95a5a6",
}

SUBTHEME_COLORS = {
    "rd_funding":              "#1f77b4",
    "innovation_instruments":  "#ff7f0e",
    "research_infrastructure": "#2ca02c",
    "knowledge_transfer":      "#d62728",
    "startup_ecosystem":       "#9467bd",
    "human_capital":           "#8c564b",
    "sectoral_rd":             "#e377c2",
    "other":                   "#7f7f7f",
}

RD_CATEGORY_COLORS = {
    "direct_rd":          "#1f77b4",
    "innovation":         "#ff7f0e",
    "institutional":      "#2ca02c",
    "sectoral_rd":        "#d62728",
    "budget_instruments": "#9467bd",
    "other":              "#7f7f7f",
}


# ── Loaders ─────────────────────────────────────────────────────────────────

@st.cache_data
def load_budget():
    """Load Stream 1 — budget line items from Finance Bills."""
    if not BUDGET_RESULTS.exists():
        return pd.DataFrame()

    df = pd.read_csv(BUDGET_RESULTS)
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["amount_local"] = pd.to_numeric(df["amount_local"], errors="coerce")
    df = df.dropna(subset=["year", "amount_local"])
    df["year"] = df["year"].astype(int)

    # Normalise rd_category to lowercase
    if "rd_category" in df.columns:
        df["rd_category"] = df["rd_category"].str.lower().fillna("other")

    # Keep only include + review decisions
    if "decision" in df.columns:
        df = df[df["decision"].isin(["include", "review"])]

    return df


@st.cache_data
def load_reforms():
    """Load Stream 2 — deduplicated innovation reform events."""
    if not REFORMS_EVENTS.exists():
        return pd.DataFrame()

    df = pd.read_csv(REFORMS_EVENTS)

    for col in ("implementation_year", "announcement_year", "legislation_year",
                "survey_year"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "sub_theme" in df.columns:
        df["sub_theme"] = df["sub_theme"].fillna("other")
    if "rd_actor" in df.columns:
        df["rd_actor"] = df["rd_actor"].fillna("unknown")
    if "rd_stage" in df.columns:
        df["rd_stage"] = df["rd_stage"].fillna("unknown")
    if "growth_orientation" in df.columns:
        df["growth_orientation"] = df["growth_orientation"].fillna("unclear_or_neutral")
    if "is_major_reform" in df.columns:
        df["is_major_reform"] = df["is_major_reform"].astype(bool)

    return df


@st.cache_data
def load_reform_panel():
    """Load the country×year reform panel."""
    if not REFORM_PANEL.exists():
        return pd.DataFrame()
    df = pd.read_csv(REFORM_PANEL)
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    return df


def budget_available():
    return BUDGET_RESULTS.exists()


def reforms_available():
    return REFORMS_EVENTS.exists()
