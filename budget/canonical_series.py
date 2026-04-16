"""
Canonical R&D agency series builder for budget.

The raw LLM extraction captures everything plausibly R&D-related, including
portfolio totals for broad ministries (Health, Industry, Education) that are
NOT pure R&D. Summing those gives a meaningless number.

This module defines, per country, the SPECIFIC AGENCIES to track for a
reliable, year-comparable R&D time series. For each agency we:

  1. Match rows in the results DataFrame by name variants
  2. Select the best single amount per year (agency-level total, preferring
     dedicated R&D agency totals over portfolio or line-item level)
  3. Flag gaps (years with no data) rather than interpolating
  4. Build a clean panel: one row per (country, agency, year)

Design principle:
  Track the same entities over time. When an agency renames itself
  (e.g. AAEC → ANSTO, Dept of Science → absorbed into Industry) the
  canonical series handles that via name_variants.

Adding a new country:
  Add a block to CANONICAL_AGENCIES. Each agency needs:
    - canonical_name  : stable name used in the output
    - category        : rd_category for this series
    - name_variants   : list of partial strings to match in line_description_en
      (case-insensitive, OR logic — first match wins)
    - preferred_item_type : which item_type to prefer (section_total or program_total)
    - active_years    : (start, end) inclusive — None means open
"""

from __future__ import annotations

import logging
import re
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

__all__ = ["build_canonical_series", "CANONICAL_AGENCIES"]

# ---------------------------------------------------------------------------
# Agency definitions
# ---------------------------------------------------------------------------

CANONICAL_AGENCIES: dict[str, list[dict]] = {

    # -----------------------------------------------------------------------
    # AUSTRALIA
    # Sourced from: ABS Cat. 8104 (R&D by funding source), Appropriation Acts
    # Audited: April 2025 against LLM extraction results 1975-2026
    # -----------------------------------------------------------------------
    "Australia": [
        {
            "canonical_name": "CSIRO",
            "category": "science_agency",
            "name_variants": [
                "commonwealth scientific and industrial research",
                "commonwealth scientific and industrial",  # catches truncated cell text
                "science and industry research act",
                "csiro",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1926, 2099),
            "notes": "Core public research agency. Pre-1949: Advisory Council of Science. "
                     "Continuously tracked since 1975 in these files.",
        },
        {
            "canonical_name": "Australian Research Council (ARC)",
            "category": "science_agency",
            "name_variants": [
                "australian research council",
                "arc —",
                "arc—",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1988, 2099),
            "notes": "Created 1988. Before 1988 equivalent grants appear as "
                     "'Research grants' under Dept of Science.",
        },
        {
            "canonical_name": "NHMRC / Medical Research Fund",
            "category": "science_agency",
            "name_variants": [
                "national health and medical research",
                "nhmrc",
                "medical research endowment fund",
                "medical research (for payment",
                "health research (including payments to the med",
                "health research (including payments",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1936, 2099),
            "notes": "NHMRC grants and endowment fund payments. "
                     "Early years appear as line items under Health.",
        },
        {
            "canonical_name": "ANSTO / Atomic Energy Commission",
            "category": "science_agency",
            "name_variants": [
                "australian nuclear science and technology",
                "ansto",
                "australian atomic energy",
                "atomic energy act",
                "atomic energy commission",
                "for expenditure under the australian nuclear science",
                "expenditure under the australian nuclear science",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1953, 2099),
            "notes": "AAEC renamed ANSTO in 1987.",
        },
        {
            "canonical_name": "Australian Institute of Marine Science (AIMS)",
            "category": "science_agency",
            "name_variants": [
                "australian institute of marine science",
                "aims",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1972, 2099),
        },
        {
            "canonical_name": "Industrial R&D Grants",
            "category": "innovation_instruments",
            "name_variants": [
                "industrial research and development",
                "australian industrial research and development",
                "industry research and development",
                "industry innovation program (including payment",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1967, 2010),
            "notes": "Pre-AusIndustry era industrial R&D grants scheme.",
        },
        {
            "canonical_name": "Research Grants (Dept of Science)",
            "category": "direct_rd",
            "name_variants": [
                "research grants—support for research projects",
                "research grants — support",
                "research grants (general)",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "active_years": (1965, 1995),
            "notes": "General competitive research grants before ARC took over.",
        },
        {
            "canonical_name": "Geoscience Australia / Bureau of Mineral Resources",
            "category": "science_agency",
            "name_variants": [
                "geoscience australia",
                "bureau of mineral resources",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1946, 2099),
            "notes": "BMR renamed Geoscience Australia in 2001.",
        },
        {
            "canonical_name": "Bureau of Meteorology (Research)",
            "category": "science_agency",
            "name_variants": [
                "commonwealth bureau of meteorology",
                "bureau of meteorology",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1906, 2099),
            "notes": "Include only if extracted as R&D — BoM has a large operational budget.",
        },
    ],

    # -----------------------------------------------------------------------
    # DENMARK — to be populated after audit
    # -----------------------------------------------------------------------
    "Denmark": [
        {
            "canonical_name": "Statens teknisk-videnskabelige Forskningsfond",
            "category": "science_agency",
            "name_variants": ["teknisk-videnskabelige", "forskningsfond"],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1946, 2099),
        },
        {
            "canonical_name": "Statens naturvidenskabelige Forskningsrad",
            "category": "science_agency",
            "name_variants": ["naturvidenskabelige forskningsrad", "naturvidenskabelige Forskningsråd"],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1946, 2099),
        },
        {
            "canonical_name": "Atomenergikommissionen",
            "category": "science_agency",
            "name_variants": ["atomenergikommissionen", "atomic energy commission"],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1955, 2099),
        },
        {
            "canonical_name": "Danmarks tekniske Hojskole (DTH)",
            "category": "higher_education",
            "name_variants": ["danmarks tekniske", "dth"],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1829, 2099),
        },
    ],

    # -----------------------------------------------------------------------
    # CANADA
    # Source: Main Estimates / Supplementary Estimates (Mains, Supps A/B/C)
    # Key R&D agencies: granting councils (NSERC, SSHRC, CIHR), NRC, CFI,
    # AECL/CNL, DRDC, IRAP
    # -----------------------------------------------------------------------
    "Canada": [
        {
            "canonical_name": "NSERC",
            "category": "science_agency",
            "name_variants": [
                "natural sciences and engineering research council",
                "nserc",
                "conseil de recherches en sciences naturelles",
                "crsng",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1978, 2099),
            "notes": "Created 1978 from NRC grants function.",
        },
        {
            "canonical_name": "SSHRC",
            "category": "science_agency",
            "name_variants": [
                "social sciences and humanities research council",
                "sshrc",
                "conseil de recherches en sciences humaines",
                "crsh",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1977, 2099),
            "notes": "Created 1977.",
        },
        {
            "canonical_name": "CIHR",
            "category": "science_agency",
            "name_variants": [
                "canadian institutes of health research",
                "cihr",
                "instituts de recherche en santé du canada",
                "irsc",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (2000, 2099),
            "notes": "Replaced Medical Research Council in 2000.",
        },
        {
            "canonical_name": "National Research Council (NRC)",
            "category": "science_agency",
            "name_variants": [
                "national research council",
                "conseil national de recherches",
                "nrc canada",
                "cnrc",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1916, 2099),
            "notes": "Includes IRAP (Industrial Research Assistance Program).",
        },
        {
            "canonical_name": "Canada Foundation for Innovation (CFI)",
            "category": "research_infrastructure",
            "name_variants": [
                "canada foundation for innovation",
                "fondation canadienne pour l'innovation",
                "cfi",
                "fci",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1997, 2099),
            "notes": "Funds research infrastructure at universities and hospitals.",
        },
        {
            "canonical_name": "AECL / Canadian Nuclear Laboratories",
            "category": "science_agency",
            "name_variants": [
                "atomic energy of canada",
                "énergie atomique du canada",
                "aecl",
                "eacl",
                "canadian nuclear laboratories",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1952, 2099),
            "notes": "AECL privatised operations to CNL in 2015; federal appropriation continues.",
        },
        {
            "canonical_name": "Defence Research and Development Canada (DRDC)",
            "category": "science_agency",
            "name_variants": [
                "defence research and development canada",
                "recherche et développement pour la défense canada",
                "drdc",
                "rddc",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1947, 2099),
        },
        {
            "canonical_name": "Medical Research Council (MRC Canada)",
            "category": "science_agency",
            "name_variants": [
                "medical research council of canada",
                "conseil de recherches médicales",
                "mrc canada",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1960, 2000),
            "notes": "Replaced by CIHR in 2000.",
        },
    ],

    # -----------------------------------------------------------------------
    # UK — skeleton, populate after first run
    # -----------------------------------------------------------------------
    "UK": [
        {
            "canonical_name": "Research Councils (total)",
            "category": "science_agency",
            "name_variants": ["research councils uk", "ukri", "rcuk"],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1965, 2099),
        },
        {
            "canonical_name": "Medical Research Council (MRC)",
            "category": "science_agency",
            "name_variants": ["medical research council", "mrc"],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1913, 2099),
        },
    ],

    # -----------------------------------------------------------------------
    # NEW ZEALAND — skeleton
    # -----------------------------------------------------------------------
    "New Zealand": [
        {
            "canonical_name": "DSIR / Crown Research Institutes",
            "category": "science_agency",
            "name_variants": [
                "department of scientific and industrial research",
                "dsir",
                "crown research institute",
                "foundation for research, science",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1926, 2099),
        },
    ],
}


# ---------------------------------------------------------------------------
# Matching and series building
# ---------------------------------------------------------------------------

def _match_agency(desc: str, section: str, agency: dict, desc_raw: str = "") -> bool:
    """Return True if the row matches any of the agency's name variants.

    Checks line_description_en, section_name_en, and the raw entity text
    (desc_raw) so that even if line_description_en is corrupted by a bad
    registry match, the raw entity name still triggers a match.

    Short variants (≤4 chars, e.g. 'ORE', 'ARC', 'AIMS') use word-boundary
    matching to prevent false matches like 'ORE' inside 'fOREign'.
    Longer variants use plain substring matching.
    """
    combined = f"{desc} {section} {desc_raw}".lower()
    for variant in agency["name_variants"]:
        v = variant.lower()
        if len(v) <= 4:
            if re.search(r"(?<![a-z])" + re.escape(v) + r"(?![a-z])", combined):
                return True
        else:
            if v in combined:
                return True
    return False


def _section_match_score(desc: str, section: str, agency: dict) -> int:
    """
    Return a score indicating how directly this row belongs to the agency:
      2 = agency name appears in the section_name (it's the agency's own section)
      1 = agency name appears only in the line description
      0 = no match (should not happen if _match_agency returned True)

    Used to prefer the agency's own appropriation line over cross-references
    from other departments (e.g. furniture purchases coded under dept X for CSIRO).
    """
    sec_lower = section.lower()
    desc_lower = desc.lower()
    for variant in agency["name_variants"]:
        v = variant.lower()
        if v in sec_lower:
            return 2
    for variant in agency["name_variants"]:
        v = variant.lower()
        if v in desc_lower:
            return 1
    return 0


def _best_amount_for_agency(
    matches: pd.DataFrame,
    preferred_types: list[str],
    agency: dict,
) -> Optional[pd.Series]:
    """
    From a set of matching rows for one agency in one year, pick the single
    best row to represent that agency's total for the year.

    Priority (in order):
      1. Prefer rows where the SECTION name contains the agency name variant
         (score=2) over rows where only the line description matches (score=1).
         This avoids picking cross-departmental references (e.g. CSIRO's
         furniture allocation under Dept of Science).
      2. Within same section-match score, prefer the highest-priority item_type.
      3. Within same type, pick the largest amount — but only if it is at least
         20% of the global maximum (avoids picking tiny sub-components).
      4. Fallback: return the globally largest match.
    """
    if matches.empty:
        return None

    matches = matches.copy()
    matches["_sec_score"] = matches.apply(
        lambda r: _section_match_score(
            str(r.get("line_description_en", "")),
            str(r.get("section_name_en", "")),
            agency,
        ),
        axis=1,
    )

    # Global maximum across all matches (across all score tiers and types)
    global_max = float(matches["amount_local"].max())

    # Try section-name matches first (score=2), then description-only (score=1)
    for score in [2, 1, 0]:
        pool = matches[matches["_sec_score"] == score]
        if pool.empty:
            continue

        pool_max = float(pool["amount_local"].max())

        # Only use this tier if its best amount is at least 20% of the global max.
        # This avoids picking a tiny supplementary-act CSIRO section_total (score=2)
        # over the main-act appropriation that happens to sit under a portfolio
        # section (score=1).
        if pool_max < global_max * 0.20:
            continue

        for itype in preferred_types:
            subset = pool[pool["item_type"] == itype]
            if subset.empty:
                continue
            best_in_type = subset.loc[subset["amount_local"].idxmax()]
            type_max = float(best_in_type["amount_local"])
            # Accept if it's at least 20% of the pool's global max
            if type_max >= pool_max * 0.20:
                return best_in_type

        # Fallback within this score tier: largest overall
        return pool.loc[pool["amount_local"].idxmax()]

    # Should never reach here, but be safe
    return matches.loc[matches["amount_local"].idxmax()]


def _get_agencies_for_country(country: str) -> list[dict]:
    """
    Return the merged list of canonical agencies for a country:
    hardcoded CANONICAL_AGENCIES + auto-discovered agencies from
    discovered_agencies.json (produced by agency_discovery.py).

    Discovered agencies that duplicate an existing canonical_name are skipped.
    """
    hardcoded = CANONICAL_AGENCIES.get(country, [])
    existing_names = {a["canonical_name"].lower() for a in hardcoded}

    try:
        from budget.agency_discovery import load_discovered_agencies
        discovered = load_discovered_agencies(country)
    except Exception:
        discovered = []

    merged = list(hardcoded)
    added = 0
    for agency in discovered:
        if agency.get("canonical_name", "").lower() not in existing_names:
            # Ensure required fields exist
            agency.setdefault("preferred_item_type", ["section_total", "program_total", "line_item"])
            agency.setdefault("active_years", (1900, 2099))
            # Always ensure canonical_name and source_entity are in name_variants
            # so _match_agency can find the raw text (e.g. "CANADIAN SPACE AGENCY")
            variants = agency.setdefault("name_variants", [])
            canonical_lc = agency["canonical_name"].lower()
            for v in [agency["canonical_name"], agency.get("source_entity", "")]:
                if v and v.lower() not in [x.lower() for x in variants]:
                    variants.append(v)
            merged.append(agency)
            existing_names.add(canonical_lc)
            added += 1

    if added:
        logger.info(f"[{country}] Merged {len(hardcoded)} hardcoded + {added} discovered agencies")

    return merged


def build_canonical_series(
    df: pd.DataFrame,
    country: str,
    decision_filter: Optional[list[str]] = None,
) -> pd.DataFrame:
    """
    Build the canonical R&D time series for a country.

    Parameters
    ----------
    df               : full results DataFrame (already cleaned + deduped)
    country          : country name (must match CANONICAL_AGENCIES key)
    decision_filter  : list of decisions to include (default: ['include', 'review'])
                       Includes 'review' so that agency-matched rows are captured
                       even if the registry hasn't classified them yet.

    Returns
    -------
    DataFrame with columns:
        country, year, canonical_name, category,
        amount_local, unit, currency,
        item_type, line_description_en, source_file, page_number,
        series_notes
    """
    if decision_filter is None:
        # Include both "include" and "review" — name_variants act as the quality gate.
        # A row matched by a specific agency name_variant is almost certainly that agency
        # regardless of whether the registry has classified it yet.
        decision_filter = ["include", "review"]

    agencies = _get_agencies_for_country(country)
    if not agencies:
        logger.warning(f"No canonical agencies defined for '{country}'")
        return pd.DataFrame()

    subset = df[
        (df["country"] == country)
        & (df["decision"].isin(decision_filter))
    ].copy()

    subset["amount_local"] = pd.to_numeric(subset["amount_local"], errors="coerce")
    subset = subset.dropna(subset=["amount_local"])

    records = []

    for agency in agencies:
        canonical_name = agency["canonical_name"]
        active_start, active_end = agency.get("active_years", (1800, 2099))

        for year, year_df in subset.groupby("year"):
            try:
                yr_int = int(str(year))
            except ValueError:
                continue

            if not (active_start <= yr_int <= active_end):
                continue

            # Find matching rows
            matches = year_df[
                year_df.apply(
                    lambda r: _match_agency(
                        str(r.get("line_description_en", "")),
                        str(r.get("section_name_en", "")),
                        agency,
                        str(r.get("line_description", "")),  # raw fallback
                    ),
                    axis=1,
                )
            ]

            if matches.empty:
                # Gap year for this agency
                records.append({
                    "country": country,
                    "year": yr_int,
                    "canonical_name": canonical_name,
                    "category": agency["category"],
                    "amount_local": None,
                    "unit": None,
                    "currency": None,
                    "item_type": None,
                    "line_description_en": None,
                    "source_file": None,
                    "page_number": None,
                    "series_notes": "gap: no matching rows in this year",
                })
                continue

            # One row per source file — keeps separate Act amounts so the
            # detail series shows e.g. CSIRO 587,072 (No1) AND 12,224 (No2)
            # for the same year. build_totals_series handles aggregation.
            emitted = 0
            for source_file, file_matches in matches.groupby("source_file"):
                best = _best_amount_for_agency(file_matches, agency["preferred_item_type"], agency)
                if best is None:
                    continue
                records.append({
                    "country": country,
                    "year": yr_int,
                    "canonical_name": canonical_name,
                    "category": agency["category"],
                    "amount_local": float(best["amount_local"]),
                    "unit": best.get("unit"),
                    "currency": best.get("currency"),
                    "item_type": best.get("item_type"),
                    "line_description_en": best.get("line_description_en"),
                    "source_file": source_file,
                    "page_number": best.get("page_number"),
                    "series_notes": agency.get("notes", ""),
                })
                emitted += 1

            if emitted == 0:
                # Fallback gap
                records.append({
                    "country": country,
                    "year": yr_int,
                    "canonical_name": canonical_name,
                    "category": agency["category"],
                    "amount_local": None,
                    "unit": None,
                    "currency": None,
                    "item_type": None,
                    "line_description_en": None,
                    "source_file": None,
                    "page_number": None,
                    "series_notes": "gap: no matching rows in this year",
                })

    out = pd.DataFrame(records)
    if out.empty:
        return out

    out = out.sort_values(["canonical_name", "year"]).reset_index(drop=True)

    logger.info(
        f"Canonical series [{country}]: {len(agencies)} agencies, "
        f"{out['year'].nunique()} years, "
        f"{out['amount_local'].notna().sum()} data points, "
        f"{out['amount_local'].isna().sum()} gaps."
    )
    return out


def build_totals_series(
    detail_df: pd.DataFrame,
    country: str,
) -> pd.DataFrame:
    """
    Build an aggregated totals series from the detail series.

    For each (canonical_name, year), sum all amounts across Acts, but flag
    potential restatements (where two Acts have the exact same amount for
    the same agency — likely the same money re-appropriated, not additional).

    Returns a DataFrame with one row per (canonical_name, year):
        canonical_name, year, amount_total, amount_primary, n_acts,
        acts, additive_flag, currency, unit, category

    additive_flag values:
        "additive"      — amounts from different Acts appear genuinely additional
        "restatement"   — at least two Acts share the same amount (possible double-count)
        "single"        — only one Act contributed (no ambiguity)
        "gap"           — no data for this agency-year
    """
    rows = detail_df[
        (detail_df["country"] == country)
        & detail_df["amount_local"].notna()
    ].copy()

    if rows.empty:
        return pd.DataFrame()

    records = []
    for (canonical_name, year), grp in rows.groupby(["canonical_name", "year"]):
        amounts = grp["amount_local"].tolist()
        acts = grp["source_file"].tolist()
        currency = grp["currency"].iloc[0] if "currency" in grp.columns else None
        unit = grp["unit"].iloc[0] if "unit" in grp.columns else None
        category = grp["category"].iloc[0] if "category" in grp.columns else None

        # Primary amount = the row from the lowest Act number
        primary_row = grp.iloc[0]  # already sorted by source_file in detail series
        amount_primary = float(primary_row["amount_local"])

        # Detect restatements: any two rows share the same amount
        has_restatement = len(amounts) > 1 and len(set(round(a, 0) for a in amounts)) < len(amounts)

        if len(amounts) == 1:
            additive_flag = "single"
            amount_total = amounts[0]
        elif has_restatement:
            additive_flag = "restatement"
            # Use primary amount only — do not sum to avoid double-count
            amount_total = amount_primary
        else:
            additive_flag = "additive"
            amount_total = sum(amounts)

        records.append({
            "country": country,
            "canonical_name": canonical_name,
            "year": year,
            "amount_total": float(amount_total),
            "amount_primary": float(amount_primary),
            "n_acts": len(amounts),
            "acts": " | ".join(str(a) for a in acts),
            "additive_flag": additive_flag,
            "currency": currency,
            "unit": unit,
            "category": category,
        })

    # Also add gap rows from detail_df (where amount_local is None)
    gap_rows = detail_df[
        (detail_df["country"] == country)
        & detail_df["amount_local"].isna()
    ]
    for _, row in gap_rows.iterrows():
        records.append({
            "country": country,
            "canonical_name": row["canonical_name"],
            "year": row["year"],
            "amount_total": None,
            "amount_primary": None,
            "n_acts": 0,
            "acts": "",
            "additive_flag": "gap",
            "currency": row.get("currency"),
            "unit": row.get("unit"),
            "category": row.get("category"),
        })

    out = pd.DataFrame(records).sort_values(["canonical_name", "year"]).reset_index(drop=True)

    n_additive = (out["additive_flag"] == "additive").sum()
    n_restatement = (out["additive_flag"] == "restatement").sum()
    if n_restatement:
        logger.warning(
            f"[{country}] {n_restatement} agency-years have possible restatements "
            f"(same amount in multiple Acts) — check 'restatement' rows before summing."
        )
    logger.info(
        f"Totals series [{country}]: {n_additive} additive, "
        f"{n_restatement} restatement, "
        f"{(out['additive_flag']=='single').sum()} single-act rows"
    )
    return out
