"""
Agency classifier for budget.

After LLM extraction, we have many unique agency/section names. Some are
dedicated R&D agencies (CSIRO, ARC, ANSTO). Others are broad ministries
(Department of Health, Education Portfolio) whose totals include mostly
non-R&D spending. Still others are programmes within ministries.

This module:
  1. Collects all unique agency names found in the extracted results
  2. Classifies each one with a single cheap LLM call (cached permanently)
  3. Writes classifications to agency_registry.csv — the living database
     of every agency we've ever seen, with its type and R&D relevance

Classification types:
  dedicated_rd     : agency exists solely for R&D (CSIRO, ARC, NHMRC, DFG)
  mixed_ministry   : broad department where R&D is a subset (Health, Defence)
  rd_programme     : a specific R&D fund/scheme within a ministry
  unclear          : not enough information to classify

The registry is the source of truth for building the canonical time series.
New agencies are automatically discovered and classified — no manual list needed.

Usage:
  python -m budget.agency_classifier --country Australia
  python -m budget.agency_classifier --all
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

REGISTRY_FILE = Path("Data/output/budget/agency_registry.csv")
BATCH_SIZE = 25

REGISTRY_COLUMNS = [
    "country",
    "agency_name",          # as extracted (section_name_en or line_description_en)
    "agency_type",          # dedicated_rd | mixed_ministry | rd_programme | unclear
    "rd_fraction",          # estimated % of agency budget that is R&D (0-100)
    "canonical_name",       # standardised name (handles renames/variants)
    "active_years",         # "1975-1987" or "1975-" (open-ended)
    "include_in_series",    # True/False — whether to use in canonical time series
    "notes",
    "classified_by",        # "llm" or "manual"
]

# ---------------------------------------------------------------------------
# Classification prompt
# ---------------------------------------------------------------------------

_CLASSIFY_SYSTEM = """\
You are classifying government budget agencies to determine whether they are
dedicated R&D agencies or broad ministries.

For each agency name provided, return a JSON object with:
{
  "agency_type": "dedicated_rd" | "mixed_ministry" | "rd_programme" | "unclear",
  "rd_fraction": <integer 0-100, estimated % of total budget that is R&D>,
  "canonical_name": "<standardised English name>",
  "include_in_series": <true|false>,
  "notes": "<brief reason>"
}

Definitions:
  dedicated_rd   : agency exists primarily or solely to conduct/fund R&D.
                   Examples: CSIRO, ARC, NHMRC, DFG, CNRS, MRC, NSERC.
                   rd_fraction typically 80-100. include_in_series: true.

  mixed_ministry : broad government department where R&D is a small component.
                   Examples: Department of Health, Ministry of Defence,
                   Department of Education, Department of Transport.
                   rd_fraction typically 1-20. include_in_series: false.
                   (We extract the R&D LINE ITEMS from these, not the total.)

  rd_programme   : a specific R&D scheme or fund within a ministry, not a
                   standalone agency. Examples: Industrial R&D Grants Board,
                   National Energy R&D Programme, Medical Research Endowment Fund.
                   rd_fraction: 100. include_in_series: true.

  unclear        : cannot determine from the name alone.
                   include_in_series: false (flag for manual review).

Return ONLY the JSON object. No prose.
"""

_BATCH_CLASSIFY_SYSTEM = """\
You are classifying government budget agencies.

You will receive a country and a list of agency names. Return ONLY JSON in this shape:
{
  "results": [
    {
      "input_name": "<exact input agency name>",
      "agency_type": "dedicated_rd" | "mixed_ministry" | "rd_programme" | "unclear",
      "rd_fraction": <integer 0-100>,
      "canonical_name": "<standardised English name>",
      "include_in_series": <true|false>,
      "notes": "<brief reason>"
    }
  ]
}

Rules:
  - input_name must exactly match one provided agency name.
  - Return one result for every provided agency name.
  - Return ONLY JSON, no prose.
"""


def _classify_prompt(agency_name: str, country: str) -> str:
    return f"Country: {country}\nAgency name: {agency_name}"


def _batch_classify_prompt(batch: list[tuple[str, str]]) -> str:
    country = batch[0][0] if batch else ""
    lines = [f"Country: {country}", "Agency names:"]
    for i, (_, agency_name) in enumerate(batch, start=1):
        lines.append(f'{i}. "{agency_name}"')
    return "\n".join(lines)


def _iter_batches(items: list[tuple[str, str]], size: int) -> list[list[tuple[str, str]]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


# ---------------------------------------------------------------------------
# Registry management
# ---------------------------------------------------------------------------

def load_registry(registry_file: Path = REGISTRY_FILE) -> pd.DataFrame:
    if registry_file.exists():
        return pd.read_csv(registry_file)
    return pd.DataFrame(columns=REGISTRY_COLUMNS)


def save_registry(df: pd.DataFrame, registry_file: Path = REGISTRY_FILE) -> None:
    registry_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(registry_file, index=False)
    logger.info(f"Registry saved: {len(df)} agencies → {registry_file}")


def _known_agency(registry: pd.DataFrame, country: str, name: str) -> bool:
    if registry.empty:
        return False
    return (
        (registry["country"] == country) & (registry["agency_name"] == name)
    ).any()


# ---------------------------------------------------------------------------
# Main classification logic
# ---------------------------------------------------------------------------

def classify_agencies(
    results_df: pd.DataFrame,
    config: dict,
    country: str | None = None,
    registry_file: Path = REGISTRY_FILE,
    dry_run: bool = False,
) -> pd.DataFrame:
    """
    Classify all unique agency names found in results_df that are not yet
    in the registry.

    Parameters
    ----------
    results_df    : extracted results (all decisions)
    config        : pipeline config (for LLM client)
    country       : if set, only classify agencies for this country
    registry_file : path to agency_registry.csv
    dry_run       : if True, print what would be classified but don't call LLM

    Returns
    -------
    Updated registry DataFrame.
    """
    registry = load_registry(registry_file)

    # Collect unique (country, agency_name) pairs from section_name_en
    df = results_df.copy()
    if country:
        df = df[df["country"] == country]

    # Gather agency candidates: section_name_en where item_type=section_total
    candidates = (
        df[df["item_type"] == "section_total"]
        [["country", "section_name_en"]]
        .rename(columns={"section_name_en": "agency_name"})
        .dropna()
        .drop_duplicates()
    )

    # Also capture line_description_en for section_totals that are actually
    # named agencies (e.g. "Total: Australian Research Council")
    line_agencies = (
        df[df["item_type"].isin(["section_total", "program_total"])]
        .assign(agency_name=lambda x: x["line_description_en"].str.replace(
            r"^Total[:,\s]*", "", regex=True
        ).str.strip())
        [["country", "agency_name"]]
        .dropna()
        .drop_duplicates()
    )

    all_candidates = (
        pd.concat([candidates, line_agencies])
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # Filter to unclassified
    new_agencies = [
        (row["country"], row["agency_name"])
        for _, row in all_candidates.iterrows()
        if not _known_agency(registry, row["country"], row["agency_name"])
        and len(str(row["agency_name"]).strip()) > 3
    ]

    logger.info(
        f"Agency classifier: {len(all_candidates)} total candidates, "
        f"{len(new_agencies)} unclassified."
    )

    if dry_run:
        print(f"\nWould classify {len(new_agencies)} new agencies:")
        for ctry, name in new_agencies[:20]:
            print(f"  [{ctry}] {name}")
        if len(new_agencies) > 20:
            print(f"  ... and {len(new_agencies)-20} more")
        return registry

    from budget.llm_client import BudgetLLMClient
    client = BudgetLLMClient.from_config(config)

    if new_agencies:
        logger.info(
            f"Agency classifier LLM batches: {len(new_agencies)} uncached labels in "
            f"{(len(new_agencies) + BATCH_SIZE - 1) // BATCH_SIZE} batches"
        )

    new_rows = []
    for batch in _iter_batches(new_agencies, BATCH_SIZE):
        batch_result = client.call_json(
            system_prompt=_BATCH_CLASSIFY_SYSTEM,
            user_prompt=_batch_classify_prompt(batch),
            max_tokens=4000,
            operation=client.OP_OTHER,
        )

        parsed_results: dict[str, dict] = {}
        if "_parse_error" not in batch_result and isinstance(batch_result.get("results"), list):
            for item in batch_result["results"]:
                input_name = str(item.get("input_name", "")).strip()
                if input_name:
                    parsed_results[input_name] = item

        if len(parsed_results) != len(batch):
            logger.warning(
                f"Agency classifier batch fallback: parsed {len(parsed_results)}/{len(batch)} "
                "results; retrying missing labels individually"
            )

        for ctry, agency_name in batch:
            result = parsed_results.get(agency_name)
            if result is None:
                result = client.call_json(
                    system_prompt=_CLASSIFY_SYSTEM,
                    user_prompt=_classify_prompt(agency_name, ctry),
                    max_tokens=200,
                    operation=client.OP_OTHER,
                )

            if "_parse_error" in result:
                agency_type = "unclear"
                rd_fraction = 0
                canonical_name = agency_name
                include = False
                notes = f"parse error: {result['_parse_error'][:80]}"
            else:
                agency_type = result.get("agency_type", "unclear")
                rd_fraction = result.get("rd_fraction", 0)
                canonical_name = result.get("canonical_name", agency_name)
                include = result.get("include_in_series", False)
                notes = result.get("notes", "")

            new_rows.append({
                "country": ctry,
                "agency_name": agency_name,
                "agency_type": agency_type,
                "rd_fraction": rd_fraction,
                "canonical_name": canonical_name,
                "active_years": "",
                "include_in_series": include,
                "notes": notes,
                "classified_by": "llm",
            })

            logger.debug(
                f"[{ctry}] {agency_name[:50]} → {agency_type} "
                f"(rd_fraction={rd_fraction}%, include={include})"
            )

    if new_rows:
        registry = pd.concat(
            [registry, pd.DataFrame(new_rows)],
            ignore_index=True,
        )
        save_registry(registry, registry_file)
        client.save_usage()

    return registry


# ---------------------------------------------------------------------------
# Build time series from registry
# ---------------------------------------------------------------------------

def build_series_from_registry(
    results_df: pd.DataFrame,
    registry: pd.DataFrame,
    country: str,
) -> pd.DataFrame:
    """
    Build the R&D time series using the agency registry to decide what to include.

    For each year:
      - Find all section_total / program_total rows whose agency is classified
        as include_in_series=True in the registry
      - Remove parents when children are present (avoid double counting)
      - Sum the remaining rows → total R&D for that year

    Returns a DataFrame with one row per (country, year, canonical_agency).
    This naturally handles new agencies: when a new dedicated_rd agency appears
    in a future year, it gets classified and automatically enters the series.
    """
    include_agencies = registry[
        (registry["country"] == country)
        & (registry["include_in_series"] == True)  # noqa: E712
    ]["agency_name"].tolist()

    if not include_agencies:
        logger.warning(f"No agencies marked include_in_series for {country}")
        return pd.DataFrame()

    df = results_df[
        (results_df["country"] == country)
        & (results_df["decision"] == "include")
    ].copy()
    df["amount_local"] = pd.to_numeric(df["amount_local"], errors="coerce")

    records = []
    for year, year_df in df.groupby("year"):
        for agency_name in include_agencies:
            # Match by section_name_en or by line_description_en (cleaned)
            mask = (
                year_df["section_name_en"].str.lower().str.contains(
                    agency_name.lower()[:30], na=False
                )
                | year_df["line_description_en"].str.lower().str.contains(
                    agency_name.lower()[:30], na=False
                )
            ) & year_df["item_type"].isin(["section_total", "program_total"])

            matches = year_df[mask].dropna(subset=["amount_local"])
            if matches.empty:
                continue

            # Prefer the most specific (smallest scope = lowest amount that
            # is still a section_total for this agency, not a parent portfolio)
            best_idx = matches["amount_local"].idxmax()
            row = matches.loc[best_idx]

            canonical = registry[
                (registry["country"] == country)
                & (registry["agency_name"] == agency_name)
            ]["canonical_name"].iloc[0] if not registry[
                (registry["country"] == country)
                & (registry["agency_name"] == agency_name)
            ].empty else agency_name

            records.append({
                "country": country,
                "year": year,
                "canonical_name": canonical,
                "agency_name": agency_name,
                "amount_local": float(row["amount_local"]),
                "unit": row.get("unit"),
                "currency": row.get("currency"),
                "item_type": row.get("item_type"),
                "source_file": row.get("source_file"),
            })

    return pd.DataFrame(records).sort_values(["canonical_name", "year"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import yaml
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")

    parser = argparse.ArgumentParser(description="Classify agencies from extracted results")
    parser.add_argument("--country", help="Classify only this country")
    parser.add_argument("--all", action="store_true", help="Classify all countries")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--results", default="Data/output/budget/results.csv")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    results_df = pd.read_csv(args.results)
    country_filter = args.country if not args.all else None

    registry = classify_agencies(
        results_df,
        config=config,
        country=country_filter,
        dry_run=args.dry_run,
    )

    print(f"\nRegistry summary:")
    if not registry.empty:
        summary = registry.groupby(["country", "agency_type"]).size().unstack(fill_value=0)
        print(summary.to_string())
        n_include = registry["include_in_series"].sum()
        print(f"\nTotal include_in_series=True: {n_include}")
