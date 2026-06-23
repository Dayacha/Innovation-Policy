"""
budget panel builder — cross-year time-series consolidation.

This runs AFTER all individual documents have been extracted.
It answers the user's question: "does the consistency pass take all the
extracted years and make the final database?"

Yes — this is that step. It takes the full results.csv and:
  1. Normalises amounts to a consistent unit (millions of local currency).
  2. Fuzzy-matches section/programme names across years so "CSIRO" in 1990
     and "Commonwealth Scientific and Industrial Research Organisation" in 2005
     map to the same series.
  3. Drops duplicate totals vs components (section_total already covered by its
     program_total children are flagged to avoid double-counting).
  4. Produces panel.csv — one row per (country, year, series_id) — which is the
     final time-series database.
  5. Produces summary_by_series.csv — one row per identified series with
     coverage and trend metadata.
"""

from __future__ import annotations

import csv
import logging
import re
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Amount normalisation to millions of local currency
# ---------------------------------------------------------------------------

_TO_MILLIONS: dict[str, float] = {
    "billion":    1_000.0,
    "milliard":   1_000.0,
    "millions":   1.0,
    "million":    1.0,
    "m":          1.0,
    "thousand":   0.001,
    "thousands":  0.001,
    "k":          0.001,
    "000s":       0.001,
    "as_printed": 1.0,   # assume millions when unit is unknown
    "dollar":     1.0,   # assume reported in millions already
    "kr":         1.0,
    "":           1.0,
}


def _to_millions(amount: Optional[float], unit: str) -> Optional[float]:
    """Convert amount to millions of local currency. Returns None if unknown."""
    if amount is None:
        return None
    unit_clean = unit.strip().lower().replace(",", "")
    mult = _TO_MILLIONS.get(unit_clean)
    if mult is None:
        logger.debug(f"Unknown unit '{unit}' — assuming millions")
        mult = 1.0
    return round(amount * mult, 4)


# ---------------------------------------------------------------------------
# Name normalisation for series matching
# ---------------------------------------------------------------------------

_STOP_WORDS = {
    "the", "of", "for", "and", "in", "a", "an", "to", "de", "du", "des",
    "department", "dept", "ministry", "bureau", "office", "agency", "authority",
    "commonwealth", "australian", "australia", "government", "national",
    "canada", "canadian", "new", "zealand",
}

_ABBREVS = {
    "csiro": "commonwealth scientific and industrial research organisation",
    "nserc": "natural sciences and engineering research council",
    "sshrc": "social sciences and humanities research council",
    "cihr":  "canadian institutes of health research",
    "cfi":   "canada foundation for innovation",
    "nrc":   "national research council",
    "arc":   "australian research council",
    "nhmrc": "national health and medical research council",
    "ansto": "australian nuclear science and technology organisation",
    "mbie":  "ministry of business innovation and employment",
    "frst":  "foundation for research science and technology",
    "dsir":  "department of scientific and industrial research",
    "epsrc": "engineering and physical sciences research council",
    "bbsrc": "biotechnology and biological sciences research council",
    "esrc":  "economic and social research council",
    "ahrc":  "arts and humanities research council",
    "nerc":  "natural environment research council",
    "mrc":   "medical research council",
    "ukri":  "uk research and innovation",
    "hefce": "higher education funding council for england",
    "bis":   "department for business innovation and skills",
    "beis":  "department for business energy and industrial strategy",
    "dti":   "department of trade and industry",
    "dius":  "department for innovation universities and skills",
    "ost":   "office of science and technology",
}


def _normalise_name(name: str) -> str:
    """Lower-case, expand known abbreviations, strip stop words and punctuation."""
    text = name.lower().strip()
    # Replace common abbreviations
    for abbr, full in _ABBREVS.items():
        text = re.sub(rf"\b{re.escape(abbr)}\b", full, text)
    # Remove punctuation except spaces
    text = re.sub(r"[^\w\s]", " ", text)
    # Remove stop words
    words = [w for w in text.split() if w not in _STOP_WORDS and len(w) > 1]
    return " ".join(words)


def _similarity(a: str, b: str) -> float:
    """Quick token-overlap Jaccard similarity for name matching."""
    sa = set(_normalise_name(a).split())
    sb = set(_normalise_name(b).split())
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


# ---------------------------------------------------------------------------
# Series assignment
# ---------------------------------------------------------------------------

class _Series:
    """A named R&D spending series tracked across years."""
    _counter = 0

    def __init__(self, canonical_name: str, country: str, rd_category: str):
        _Series._counter += 1
        self.series_id = f"S{_Series._counter:04d}"
        self.canonical_name = canonical_name
        self.country = country
        self.rd_category = rd_category
        self.aliases: set[str] = {canonical_name}


def _assign_series(
    rows: list[dict],
    similarity_threshold: float = 0.50,
) -> list[dict]:
    """
    Assign a series_id to every row, merging rows whose programme names are
    similar across years.

    Works per-country. Returns the enriched rows list.
    """
    from collections import defaultdict

    series_by_country: dict[str, list[_Series]] = defaultdict(list)
    _Series._counter = 0  # reset for reproducible IDs

    for row in rows:
        country = row.get("country", "")
        # Use english name for matching; fall back to original
        name = row.get("line_description_en") or row.get("line_description") or ""
        rd_cat = row.get("rd_category", "unclear")

        best_series: Optional[_Series] = None
        best_score = similarity_threshold

        for series in series_by_country[country]:
            for alias in series.aliases:
                score = _similarity(name, alias)
                if score > best_score:
                    best_score = score
                    best_series = series

        if best_series is None:
            best_series = _Series(name, country, rd_cat)
            series_by_country[country].append(best_series)
        else:
            best_series.aliases.add(name)

        row["series_id"] = best_series.series_id
        row["series_name"] = best_series.canonical_name

    return rows


# ---------------------------------------------------------------------------
# Double-count guard: three-way classification of totals vs children
# ---------------------------------------------------------------------------

def _flag_double_counts(rows: list[dict]) -> list[dict]:
    """
    For every (country, year, section_name) group that has BOTH a section_total
    AND child rows (program_total or line_item), classify the relationship and
    set aggregation_role on each row:

      'count'      — use this row when summing R&D spend (never double-count)
      'redundant'  — this row is already covered by its children; exclude from sum
      'context'    — informational total for a mixed ministry; children are the R&D subset

    Three situations (based on ratio of line_sum / section_total):
      ≈ 1.0  (±15%)  → children fully cover total: section_total = 'redundant'
      < 0.85         → children are R&D subset of mixed ministry:
                         section_total = 'context', children = 'count'
      > 1.15         → likely from different documents / data conflict:
                         all rows = 'review', detail added to notes
    """
    from collections import defaultdict

    # Group by (country, year, section_name normalised)
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for row in rows:
        sect = (row.get("section_name_en") or row.get("section_name") or "").strip().lower()[:60]
        key = (row.get("country"), row.get("year"), sect)
        groups[key].append(row)

    for group in groups.values():
        section_totals = [r for r in group if r.get("item_type") == "section_total"]
        children = [r for r in group if r.get("item_type") in ("program_total", "line_item")]

        # No overlap — just mark every row as 'count' and move on
        if not section_totals or not children:
            for r in group:
                r.setdefault("aggregation_role", "count")
                r.setdefault("double_count", False)
            continue

        child_sum = sum(
            float(r.get("amount_millions") or 0) for r in children
            if r.get("amount_millions") not in (None, "", "None")
        )

        for st in section_totals:
            st_amt = float(st.get("amount_millions") or 0)
            if st_amt <= 0:
                st["aggregation_role"] = "count"
                st["double_count"] = False
                continue

            ratio = child_sum / st_amt

            if 0.85 <= ratio <= 1.15:
                # Children ≈ total → section_total is redundant
                st["aggregation_role"] = "redundant"
                st["double_count"] = True
                note = f"redundant: children sum {child_sum:.2f}M ≈ total {st_amt:.2f}M"
                st["notes"] = (st.get("notes", "") + " | " + note).strip(" |")
                for ch in children:
                    ch.setdefault("aggregation_role", "count")
                    ch["double_count"] = False

            elif ratio < 0.85:
                # Children are only the R&D subset of a larger mixed ministry
                st["aggregation_role"] = "context"
                st["double_count"] = False
                note = (f"context: children sum {child_sum:.2f}M is R&D subset "
                        f"of total {st_amt:.2f}M ({ratio*100:.0f}%)")
                st["notes"] = (st.get("notes", "") + " | " + note).strip(" |")
                for ch in children:
                    ch.setdefault("aggregation_role", "count")
                    ch["double_count"] = False

            else:
                # Children sum > total — data conflict (different acts/docs mixed)
                st["aggregation_role"] = "review"
                st["double_count"] = False
                for ch in children:
                    ch["aggregation_role"] = "review"
                    ch["double_count"] = False
                note = (f"data_conflict: children sum {child_sum:.2f}M > "
                        f"total {st_amt:.2f}M — check source documents")
                for r in group:
                    r["notes"] = (r.get("notes", "") + " | " + note).strip(" |")

    # Default for rows not in any mixed group
    for row in rows:
        row.setdefault("aggregation_role", "count")
        row.setdefault("double_count", False)

    return rows


# ---------------------------------------------------------------------------
# Main build function
# ---------------------------------------------------------------------------

PANEL_COLUMNS = [
    "country", "year", "series_id", "series_name",
    "item_type", "aggregation_role", "section_code", "section_name_en",
    "line_description_en", "amount_millions", "currency",
    "rd_category", "decision", "confidence",
    "double_count", "source_file", "page_number", "notes",
]

SUMMARY_COLUMNS = [
    "country", "series_id", "series_name", "rd_category",
    "first_year", "last_year", "n_years",
    "min_amount_millions", "max_amount_millions", "mean_amount_millions",
    "currency", "decision_mode",
]


def build_panel(
    results_csv: Path,
    output_dir: Path,
    similarity_threshold: float = 0.50,
    include_review: bool = True,
) -> tuple[Path, Path]:
    """
    Build panel.csv and summary_by_series.csv from results.csv.

    Args:
        results_csv:          Path to the raw extraction results.
        output_dir:           Where to write panel.csv and summary_by_series.csv.
        similarity_threshold: Jaccard threshold for series name matching.
        include_review:       Include 'review' rows in the panel (alongside 'include').

    Returns:
        (panel_path, summary_path)
    """
    if not results_csv.exists():
        logger.error(f"results.csv not found: {results_csv}")
        raise FileNotFoundError(results_csv)

    # Load results
    with open(results_csv, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    logger.info(f"Panel builder: {len(rows)} raw rows from {results_csv.name}")

    # Filter to include/review only
    valid_decisions = {"include", "review"} if include_review else {"include"}
    rows = [r for r in rows if r.get("decision", "").strip() in valid_decisions]
    logger.info(f"  After decision filter: {len(rows)} rows")

    # Drop rows with no amount
    rows = [r for r in rows if r.get("amount_local", "").strip() not in ("", "None")]
    logger.info(f"  After amount filter: {len(rows)} rows")

    # Normalise amounts to millions
    for row in rows:
        try:
            amount_local = float(row.get("amount_local", "") or 0)
            unit = row.get("unit", "") or ""
            row["amount_millions"] = _to_millions(amount_local, unit)
        except (ValueError, TypeError):
            row["amount_millions"] = None

    # Drop rows with no normalised amount
    rows = [r for r in rows if r.get("amount_millions") is not None]

    # Series assignment (fuzzy name matching across years)
    rows = _assign_series(rows, similarity_threshold=similarity_threshold)

    # Double-count flagging
    rows = _flag_double_counts(rows)

    # Write panel.csv
    output_dir.mkdir(parents=True, exist_ok=True)
    panel_path = output_dir / "panel.csv"
    with open(panel_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=PANEL_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Panel written: {panel_path} ({len(rows)} rows)")

    # Build summary_by_series
    summary = _build_series_summary(rows)
    summary_path = output_dir / "summary_by_series.csv"
    with open(summary_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SUMMARY_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(summary)
    logger.info(f"Series summary written: {summary_path} ({len(summary)} series)")

    return panel_path, summary_path


def _build_series_summary(rows: list[dict]) -> list[dict]:
    """
    Aggregate rows into one summary row per (country, series_id).
    Only rows with aggregation_role='count' contribute to amount totals
    to avoid double-counting section_totals alongside their children.
    """
    from collections import defaultdict

    groups: dict[tuple, list[dict]] = defaultdict(list)
    for row in rows:
        key = (row.get("country"), row.get("series_id"))
        groups[key].append(row)

    summaries: list[dict] = []
    for (country, series_id), group in sorted(groups.items()):
        years = []
        amounts = []       # only from aggregation_role='count' rows
        currencies = []
        decisions = []
        for r in group:
            try:
                years.append(int(r.get("year", 0)))
            except (ValueError, TypeError):
                pass
            # Only count non-redundant rows toward the amount
            if r.get("aggregation_role", "count") == "count":
                amt = r.get("amount_millions")
                if amt is not None:
                    try:
                        amounts.append(float(amt))
                    except (ValueError, TypeError):
                        pass
            currencies.append(r.get("currency", ""))
            decisions.append(r.get("decision", ""))

        if not years:
            continue

        primary_currency = max(set(currencies), key=currencies.count) if currencies else ""
        decision_mode = max(set(decisions), key=decisions.count) if decisions else ""
        canonical = group[0].get("series_name", "")
        rd_cat = group[0].get("rd_category", "unclear")

        summaries.append({
            "country": country,
            "series_id": series_id,
            "series_name": canonical,
            "rd_category": rd_cat,
            "first_year": min(years) if years else "",
            "last_year": max(years) if years else "",
            "n_years": len(set(years)),
            "min_amount_millions": round(min(amounts), 3) if amounts else "",
            "max_amount_millions": round(max(amounts), 3) if amounts else "",
            "mean_amount_millions": round(sum(amounts) / len(amounts), 3) if amounts else "",
            "currency": primary_currency,
            "decision_mode": decision_mode,
        })

    return summaries
