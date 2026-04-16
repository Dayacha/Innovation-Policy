"""
Post-extraction amount verification for budget.

Some document formats (notably modern Australian Appropriation Acts) contain
two rows of amounts per agency: current-year (plain) and prior-year (italic).
The LLM sometimes picks the prior-year figure.

This module re-reads the source page and asks the LLM to confirm or correct
the amount for flagged rows, using a targeted single-question prompt.

Usage:
    from budget.verify import run_verify
    df = run_verify(df, config, source_dir)

Or via CLI:
    python -m budget.pipeline --verify --countries Australia
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

__all__ = ["run_verify"]

# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

_VERIFY_SYSTEM = """\
You are verifying a single budget amount extracted from a government document.

The document may show TWO rows of amounts for the same agency:
  Row 1: plain figures  = CURRENT YEAR appropriation  ← the correct one
  Row 2: italic figures = PRIOR YEAR actual           ← do NOT use this

Your task: given the page text below and the agency name and extracted amount,
determine whether the correct CURRENT YEAR amount was extracted.

If yes → return {"correct": true, "verified_amount": <same number>}
If no  → return {"correct": false, "verified_amount": <correct current year amount>}

Rules:
- Only return a JSON object, no prose.
- verified_amount must be a plain number (no commas, no currency symbols).
- If you cannot determine the correct amount from the text, return {"correct": true, "verified_amount": null}.
"""


def _verify_prompt(agency: str, extracted: float, unit: str, page_text: str) -> str:
    return (
        f"Agency: {agency}\n"
        f"Extracted amount: {extracted:,.0f} ({unit})\n\n"
        f"--- PAGE TEXT ---\n{page_text[:4000]}\n--- END ---\n\n"
        f"Is {extracted:,.0f} the CURRENT YEAR appropriation for {agency}? "
        f"If not, what is the correct current-year amount?"
    )


# ---------------------------------------------------------------------------
# Row selection — which rows to verify
# ---------------------------------------------------------------------------

# Only verify rows from modern Australian docs (2000+) where two-row confusion occurs.
# Extend this per country as needed.
_SHOULD_VERIFY: dict[str, callable] = {
    "Australia": lambda row: (
        row.get("decision") == "include"
        and int(str(row.get("year", 0))) >= 2000
        and float(row.get("amount_local") or 0) > 0
    ),
}


# ---------------------------------------------------------------------------
# Core verification logic
# ---------------------------------------------------------------------------

def _get_page_text(source_file: str, page_number: int, source_dir: Path) -> str | None:
    """Re-read a specific page from the source document."""
    try:
        from budget.pdf_reader import extract_pages
        candidates = list(source_dir.glob(f"**/{source_file}"))
        if not candidates:
            return None
        pages = list(extract_pages(candidates[0]))
        matched = [p for p in pages if p.page_num == page_number]
        return matched[0].text if matched else None
    except Exception as e:
        logger.debug(f"Could not read {source_file} page {page_number}: {e}")
        return None


def run_verify(
    df: pd.DataFrame,
    config: dict,
    source_dir: Path,
    country: str | None = None,
) -> pd.DataFrame:
    """
    Verify and correct amounts for rows likely to have current/prior year confusion.

    Parameters
    ----------
    df         : results DataFrame (will be modified in place copy)
    config     : pipeline config dict (for LLM client)
    source_dir : root directory containing source documents
    country    : if set, only verify rows for this country

    Returns
    -------
    Modified DataFrame with corrected amount_local values and verify_notes column.
    """
    from budget.llm_client import BudgetLLMClient

    df = df.copy()
    if "verify_notes" not in df.columns:
        df["verify_notes"] = ""

    client = BudgetLLMClient.from_config(config)

    countries_to_check = [country] if country else list(_SHOULD_VERIFY.keys())
    total_checked = total_corrected = 0

    for ctry in countries_to_check:
        should_verify = _SHOULD_VERIFY.get(ctry)
        if not should_verify:
            continue

        mask = df["country"] == ctry
        subset = df[mask]

        for idx, row in subset.iterrows():
            if not should_verify(row.to_dict()):
                continue

            page_text = _get_page_text(
                str(row.get("source_file", "")),
                int(row.get("page_number") or 0),
                source_dir,
            )
            if not page_text:
                continue

            agency = str(row.get("line_description_en") or row.get("section_name_en") or "")
            extracted = float(row.get("amount_local") or 0)
            unit = str(row.get("unit", ""))

            total_checked += 1

            user_prompt = _verify_prompt(agency, extracted, unit, page_text)
            result = client.call_json(
                system_prompt=_VERIFY_SYSTEM,
                user_prompt=user_prompt,
                max_tokens=128,
                operation=client.OP_OTHER,
            )

            if "_parse_error" in result:
                logger.debug(f"Verify parse error for row {idx}: {result['_parse_error']}")
                continue

            correct = result.get("correct", True)
            verified_amount = result.get("verified_amount")

            if not correct and verified_amount is not None:
                try:
                    new_amount = float(str(verified_amount).replace(",", ""))
                    if new_amount > 0 and abs(new_amount - extracted) / max(extracted, 1) > 0.01:
                        old = extracted
                        df.at[idx, "amount_local"] = new_amount
                        df.at[idx, "verify_notes"] = (
                            f"Amount corrected by verify pass: {old:,.0f} → {new_amount:,.0f} {unit}"
                        )
                        total_corrected += 1
                        logger.info(
                            f"[{ctry} {row.get('year')}] {agency[:40]}: "
                            f"{old:,.0f} → {new_amount:,.0f} {unit}"
                        )
                except (ValueError, TypeError):
                    pass

    logger.info(
        f"Verify pass complete: {total_checked} rows checked, "
        f"{total_corrected} amounts corrected."
    )
    client.save_usage()
    return df
