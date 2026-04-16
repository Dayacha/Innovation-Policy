"""
Australia-specific post-extraction cleaner.

Rules here are based on manual audit of extracted results vs source documents.
Each rule has a comment explaining WHY it's a false positive (or correction).

Audit history:
  - 1975–1978: audited against source DOCX files (April 2025)
    → identified 9 false positive patterns, added PATTERN 1-9 to prompt
  - Full 1975–2026 run: 13 residual FPs identified after prompt update
    → added as deterministic rules below
"""

from __future__ import annotations

import re
import pandas as pd

__all__ = ["clean"]

# ---------------------------------------------------------------------------
# Confirmed false positive line descriptions (exact or startswith match).
# These are lines the LLM consistently misclassifies as 'include' despite
# prompt instructions. Deterministic matching is more reliable here.
# ---------------------------------------------------------------------------

# Exact-match false positives (case-insensitive strip)
_EXACT_FP: list[str] = [
    "search for oil—subsidy",
    "road safety promotion and research",
    "queen elizabeth ii fellowship scheme",
]

# Startswith false positives (case-insensitive) — division headers
_STARTSWITH_FP: list[tuple[str, str]] = [
    # (prefix, reason)
    ("division 927", "Capital Works and Services division header — not R&D"),
    ("division 928", "Payments to States division header — not R&D"),
    ("capital works and services", "Capital division header — not R&D"),
    ("payments to or for the states", "Inter-government transfer — not R&D"),
]

# Section names whose rows should be dropped regardless of line content.
# Even if an agency name appears in the description, these sections are
# procurement/admin classifications, not R&D appropriations.
_SECTION_DROP_PATTERNS: list[str] = [
    "furniture and fittings",
    "administrative expenses—",
    "land, buildings and plant",
    "capital works and services",
    "division 290",   # FURNITURE AND FITTINGS division number
    "division 291",   # LAND, BUILDINGS AND PLANT
    "division 920",   # OLD CAPITAL WORKS heading
]

# Contains-match false positives — only when combined with other signals
_CONTAINS_FP: list[tuple[str, str]] = [
    # (substring, reason)
    ("repairs and maintenance", "Facility maintenance — not R&D spending"),
    ("instruments and apparatus", "Equipment procurement — not R&D (unless research lab)"),
]

# Section names whose section_total rows are never valid 'include'.
# These are broad ministries/portfolios where the TOTAL is not a pure R&D figure.
# We do extract R&D LINE ITEMS from within these, just not the portfolio total.
# Use partial strings — checked with `if name in section_name_lower`.
_MIXED_SECTION_TOTALS: list[str] = [
    # Health/Social
    "hospitals and health services commission",
    "commission on advanced education",
    "commission on technical and further education",
    "department of health",
    "department of community services",
    "department of education",
    "department of employment",
    "department of social security",
    "department of housing",
    "department of veterans",
    # Transport/Infrastructure
    "department of transport",
    "department of civil aviation",
    "air transport group",
    # Primary Industries (not pure R&D)
    "department of primary industry",
    "department of primary industries",
    "department of agriculture",
    # Industry / Science portfolios (broad — R&D is a subset)
    # NOTE: Do NOT add "department of science" alone — that was a pure R&D ministry in 1975-1986
    "department of industry",          # catches all variants: Industry, Technology and Commerce;
                                        # Industry, Science and Resources; Industry, Science, Energy and Resources
    "industry, innovation and science portfolio",
    "industry, science and resources portfolio",
    "industry portfolio",
    "industry and science portfolio",
    "industry and science",            # post-2014 portfolio name
    "innovation, industry, science",   # 2009-2013 era
    # Environment (has R&D line items but total is not R&D)
    "department of the environment",
    "department of environment",
    "department of climate change",
    "department of sustainability",
    # Foreign Affairs / Defence
    "department of foreign affairs",
    "department of defence",
    "department of immigration",
    # Finance/Administrative
    "department of finance",
    "department of administrative services",
    "department of the prime minister",
    "department of the treasury",
]

# ---------------------------------------------------------------------------
# Confirmed legitimate items that look suspicious but are genuine R&D
# (used as safeguard — don't accidentally drop these)
# ---------------------------------------------------------------------------
_PROTECTED_CONTAINS: list[str] = [
    "serum laboratories",
    "medical research endowment",
    "nhmrc",
    "coal research",
    "water resources research",
    "barley research",
    "wine research",
    "wool research",
    "wheat research",
]


def _is_protected(desc: str) -> bool:
    desc_lower = desc.lower()
    return any(p in desc_lower for p in _PROTECTED_CONTAINS)


# ---------------------------------------------------------------------------
# Unit correction: pre-2000 Australian Appropriation Acts use FULL DOLLARS
# (not thousands). The LLM defaults to unit="thousand" due to unit_hint.
# Threshold: if year <= 1999 AND unit==thousand AND amount > 500_000,
# the amount is almost certainly in dollars → correct to dollar.
# (A genuine thousand-denomination amount > 500,000 would be $500M+ for one
# line in 1975-1999 dollars, which is implausible for any single R&D line.)
# ---------------------------------------------------------------------------
_DOLLAR_ERA_CUTOFF = 1999  # inclusive


def _fix_units(df: pd.DataFrame) -> pd.DataFrame:
    """Correct unit=thousand → dollar for pre-2000 Australian Appropriation Acts."""
    df = df.copy()
    df["amount_local"] = pd.to_numeric(df["amount_local"], errors="coerce")

    mask = (
        (df["year"] <= _DOLLAR_ERA_CUTOFF)
        & (df["unit"].str.lower().str.strip() == "thousand")
        & (df["amount_local"] > 500_000)
    )
    n_fixed = mask.sum()
    if n_fixed:
        df.loc[mask, "unit"] = "dollar"
        # Log is not available here; note is added to rows
        for idx in df[mask].index:
            existing = str(df.at[idx, "notes"] or "")
            df.at[idx, "notes"] = (
                (existing + "; " if existing else "") +
                "AU cleaner: unit corrected thousand→dollar (pre-2000 dollar-denomination act)"
            )
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Australia-specific cleaning rules to the results DataFrame."""

    # Fix unit denomination for old acts first
    df = _fix_units(df)

    # Sanity-check: flag impossibly large amounts for known agencies
    # (likely prior-year or total-fund amount picked up by mistake)
    _AGENCY_MAX_THOUSANDS: dict[str, float] = {
        "nhmrc": 3_000_000,       # NHMRC > $3B is impossible
        "national health and medical research": 3_000_000,
        "csiro": 3_000_000,
        "commonwealth scientific": 3_000_000,
        "arc": 2_000_000,
        "australian research council": 2_000_000,
        "ansto": 1_000_000,
        "geoscience australia": 500_000,
    }

    rows_to_drop: list[int] = []
    notes: dict[int, str] = {}

    df["amount_local"] = pd.to_numeric(df["amount_local"], errors="coerce")

    for idx, row in df.iterrows():
        desc = str(row.get("line_description_en", "")).strip()
        desc_lower = desc.lower()
        section = str(row.get("section_name_en", "")).strip().lower()
        decision = str(row.get("decision", ""))
        item_type = str(row.get("item_type", ""))
        amount = row.get("amount_local")
        unit = str(row.get("unit", "")).lower().strip()

        if _is_protected(desc_lower):
            continue

        # 0a. Drop rows from procurement/admin sections regardless of line content
        for sec_pat in _SECTION_DROP_PATTERNS:
            if sec_pat in section:
                rows_to_drop.append(idx)
                notes[idx] = f"AU cleaner: procurement/admin section '{sec_pat}'"
                break
        if idx in rows_to_drop:
            continue

        # 0b. Sanity-check: impossibly large amounts for known dedicated agencies
        if decision == "include" and amount and not pd.isna(amount):
            # Convert to thousands for comparison
            from budget.dedup import _UNIT_TO_THOUSANDS
            factor = _UNIT_TO_THOUSANDS.get(unit, 1.0)
            amount_thousands = float(amount) * factor
            for agency_key, max_thousands in _AGENCY_MAX_THOUSANDS.items():
                if agency_key in section or agency_key in desc_lower:
                    if amount_thousands > max_thousands:
                        df.at[idx, "decision"] = "review"
                        notes[idx] = (
                            f"AU cleaner: amount {amount_thousands:,.0f} K exceeds "
                            f"plausible max for {agency_key} ({max_thousands:,.0f} K) "
                            f"— likely prior-year or administered fund total"
                        )
                        break

        # 1. Exact-match false positives → drop
        for fp in _EXACT_FP:
            if desc_lower == fp:
                rows_to_drop.append(idx)
                notes[idx] = f"AU cleaner: exact FP match '{fp}'"
                break
        if idx in rows_to_drop:
            continue

        # 2. Startswith false positives → drop
        for prefix, reason in _STARTSWITH_FP:
            if desc_lower.startswith(prefix):
                rows_to_drop.append(idx)
                notes[idx] = f"AU cleaner: {reason}"
                break
        if idx in rows_to_drop:
            continue

        # 3. Contains-match false positives (only if decision == 'include')
        if decision == "include":
            for substr, reason in _CONTAINS_FP:
                if substr in desc_lower:
                    # Downgrade to review rather than drop — less certain
                    df.at[idx, "decision"] = "review"
                    notes[idx] = f"AU cleaner: downgraded — {reason}"
                    break

        # 4a. "Outcome X" program_totals from broad ministries → downgrade
        # Modern Australian PBS documents have Outcome-level totals that bundle
        # R&D + non-R&D spending. Only the payment-to-agency line items are clean R&D.
        if item_type == "program_total" and decision == "include":
            if (
                desc_lower.startswith("outcome ")
                or "outcome 1 -" in desc_lower
                or "outcome 2 -" in desc_lower
                or "outcome 3 -" in desc_lower
            ):
                # Only downgrade if it's a broad ministry section
                broad_section = any(m in section for m in [
                    "department of industry", "industry portfolio",
                    "innovation, industry", "industry, innovation",
                ])
                if broad_section:
                    df.at[idx, "decision"] = "review"
                    notes[idx] = f"AU cleaner: Outcome program_total for broad ministry"
                    continue

        # 4b. Section totals for mixed-purpose ministries → downgrade include→review
        # Protect dedicated R&D agencies even when nested under a broad portfolio
        # Check BOTH section_name and line_description for the agency name
        _PROTECTED_AGENCIES = [
            "australian research council", "arc",
            "csiro", "commonwealth scientific and industrial",
            "nhmrc", "national health and medical research",
            "australian nuclear science", "ansto",
            "australian institute of marine science", "aims",
            "geoscience australia",
            "bureau of meteorology",
            "australian atomic energy",
        ]
        if item_type in ("section_total", "program_total") and decision == "include":
            is_dedicated = (
                any(p in section for p in _PROTECTED_AGENCIES)
                or any(p in desc_lower for p in _PROTECTED_AGENCIES)
            )
            if not is_dedicated:
                for mixed in _MIXED_SECTION_TOTALS:
                    if mixed in section:
                        df.at[idx, "decision"] = "review"
                        notes[idx] = (
                            f"AU cleaner: {item_type} for mixed ministry '{section[:50]}'"
                        )
                        break

    # Apply notes
    for idx, note in notes.items():
        df.at[idx, "cleaning_notes"] = note

    # Drop confirmed false positives
    df = df.drop(index=rows_to_drop).reset_index(drop=True)

    return df
