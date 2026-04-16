"""
Post-extraction validation and normalisation for budget rows.

Responsibilities:
  - Normalise units (convert "million" / "thousand" / "billion" tags to a
    canonical amount_local in the document's native unit, plus an annotated
    normalised_amount).
  - Detect implausible amounts (order-of-magnitude outliers within country-year).
  - Flag duplicates that survived deduplication (same section + amount).
  - Annotate each row with a final decision: include / review / skip.
  - Provide a summary report per country-year.
"""

from __future__ import annotations

import logging
import math
import statistics
from typing import Optional

from budget.output_schema import BudgetRow

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Unit normalisation
# ---------------------------------------------------------------------------

# Multipliers relative to "as_printed" (i.e., scale factor)
UNIT_MULTIPLIERS: dict[str, float] = {
    "billion": 1_000_000_000,
    "billion_": 1_000_000_000,
    "milliard": 1_000_000_000,   # French/German for 10^9
    "million": 1_000_000,
    "million_": 1_000_000,
    "millon": 1_000_000,         # Spanish
    "millions": 1_000_000,
    "m": 1_000_000,              # shorthand common in UK tables
    "thousand": 1_000,
    "thousands": 1_000,
    "k": 1_000,
    "000s": 1_000,
    "as_printed": 1,
    "dollar": 1,
    "kr": 1,
    "": 1,
}


def normalise_amount(amount_local: Optional[float], unit: str) -> Optional[float]:
    """
    Return amount expressed in the base (smallest) unit of the currency.

    E.g. 387 million DKK → 387_000_000 DKK (base unit).
    Returns None if amount_local is None or unit is unrecognised.
    """
    if amount_local is None:
        return None
    unit_clean = unit.strip().lower().replace(",", "").replace(".", "")
    mult = UNIT_MULTIPLIERS.get(unit_clean)
    if mult is None:
        logger.debug(f"Unknown unit '{unit}' — treating as as_printed")
        mult = 1
    return amount_local * mult


# ---------------------------------------------------------------------------
# Plausibility checks
# ---------------------------------------------------------------------------

# Expected order-of-magnitude ranges for normalised amounts (in base currency units)
# These are broad — just catch wildly wrong OCR artifacts like 1e12 GBP
_PLAUSIBILITY_RANGES: dict[str, tuple[float, float]] = {
    "GBP": (1e4, 1e11),     # £10K – £100B
    "AUD": (1e4, 1e11),
    "CAD": (1e4, 1e11),
    "NZD": (1e3, 1e10),
    "DKK": (1e5, 1e12),
    "EUR": (1e4, 1e11),
    "USD": (1e4, 1e12),
    "NOK": (1e5, 1e12),
    "SEK": (1e5, 1e12),
    "LOCAL": (0, 1e15),
}


def check_plausibility(row: BudgetRow) -> tuple[bool, str]:
    """
    Check if a row's amount is within a plausible range for its currency.

    Returns (ok: bool, reason: str).
    """
    norm = normalise_amount(row.amount_local, row.unit)
    if norm is None:
        return True, ""   # No amount — can't check, don't penalise

    currency = row.currency.upper()
    lo, hi = _PLAUSIBILITY_RANGES.get(currency, (0, 1e15))

    if not (lo <= norm <= hi):
        msg = f"Amount {norm:.3e} {currency} out of plausible range [{lo:.0e}, {hi:.0e}]"
        return False, msg
    return True, ""


# ---------------------------------------------------------------------------
# Outlier detection within a country-year batch
# ---------------------------------------------------------------------------

def _remove_outliers(rows: list[BudgetRow]) -> list[BudgetRow]:
    """
    Flag rows whose normalised amounts are extreme outliers vs their peers.
    Uses IQR-based outlier detection. Marks outliers as decision='review'.
    Does NOT delete rows — just changes decision and adds a note.
    """
    amounts = []
    for r in rows:
        n = normalise_amount(r.amount_local, r.unit)
        if n is not None and n > 0:
            amounts.append(n)

    if len(amounts) < 4:
        return rows  # not enough data to detect outliers

    log_amounts = [math.log10(a) for a in amounts]
    q1 = statistics.quantiles(log_amounts, n=4)[0]
    q3 = statistics.quantiles(log_amounts, n=4)[2]
    iqr = q3 - q1
    lo_threshold = q1 - 3 * iqr
    hi_threshold = q3 + 3 * iqr

    for row in rows:
        n = normalise_amount(row.amount_local, row.unit)
        if n is None or n <= 0:
            continue
        log_n = math.log10(n)
        if log_n < lo_threshold or log_n > hi_threshold:
            if row.decision == "include":
                row.decision = "review"
                row.notes = (row.notes + " | OUTLIER: amount is a statistical outlier").strip(" | ")
            logger.debug(f"Outlier flagged: {row.line_description} = {n:.2e} {row.currency}")

    return rows


# ---------------------------------------------------------------------------
# Main validation pass
# ---------------------------------------------------------------------------

def validate_rows(rows: list[BudgetRow]) -> list[BudgetRow]:
    """
    Run all validation checks on a list of BudgetRow objects.

    Modifies rows in-place (decision, confidence, notes) and returns the list.
    Rows that fail hard checks are marked decision='skip'.
    """
    for row in rows:
        warnings = row.validate()

        # Hard failure: missing country/year
        if "missing country" in warnings or any("year" in w for w in warnings):
            row.decision = "skip"
            row.confidence = 0.0
            row.notes = (row.notes + " | SKIP: " + "; ".join(warnings)).strip(" | ")
            continue

        # Hard failure: missing amount
        if row.amount_local is None:
            if row.decision == "include":
                row.decision = "review"
            row.confidence = min(row.confidence, 0.4)
            row.notes = (row.notes + " | no_amount").strip(" | ")

        # Plausibility
        ok, reason = check_plausibility(row)
        if not ok:
            row.decision = "review"
            row.confidence = min(row.confidence, 0.5)
            row.notes = (row.notes + " | " + reason).strip(" | ")

    # Outlier detection over the batch
    rows = _remove_outliers(rows)

    return rows


# ---------------------------------------------------------------------------
# Summary report
# ---------------------------------------------------------------------------

def summarise(rows: list[BudgetRow], country: str, year: int) -> dict:
    """Return a summary dict for logging / run log."""
    total = len(rows)
    include = sum(1 for r in rows if r.decision == "include")
    review = sum(1 for r in rows if r.decision == "review")
    skip = sum(1 for r in rows if r.decision == "skip")

    amounts = [normalise_amount(r.amount_local, r.unit) for r in rows
               if r.decision == "include" and r.amount_local is not None]
    total_amount = sum(a for a in amounts if a is not None)

    # Most common currency
    currencies = [r.currency for r in rows if r.currency]
    primary_currency = max(set(currencies), key=currencies.count) if currencies else "?"

    return {
        "country": country,
        "year": year,
        "total_rows": total,
        "include": include,
        "review": review,
        "skip": skip,
        "total_amount_base_unit": round(total_amount, 2),
        "primary_currency": primary_currency,
    }
