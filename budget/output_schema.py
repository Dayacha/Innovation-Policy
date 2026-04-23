"""
Output schema for the budget pipeline.

BudgetRow is the canonical data object — one row in results.csv.
Provides serialisation helpers and compatibility with the budget/ pipeline schema.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

from budget.config import OUTPUT_COLUMNS, RD_CATEGORIES


@dataclass
class BudgetRow:
    """One extracted R&D budget line item."""

    # Source identification
    country: str = ""
    year: int = 0
    source_file: str = ""
    page_number: str = ""          # may be a range "12-15"

    # Budget classification
    section_code: str = ""         # e.g. "20", "§20", "Vote:Science"
    section_name: str = ""         # original language
    section_name_en: str = ""      # English translation / original if English

    line_code: str = ""            # sub-line code if available
    line_description: str = ""     # original language
    line_description_en: str = ""  # English

    # Financial data
    amount_local: Optional[float] = None   # numeric amount in local currency
    unit: str = ""                 # "million", "thousand", "billion", "dollar", etc.
    currency: str = ""             # ISO 4217 e.g. "GBP", "DKK"

    # Classification
    rd_category: str = "unclear"   # one of RD_CATEGORIES keys
    decision: str = "include"      # include | review | skip
    confidence: float = 0.8        # 0.0–1.0

    # Item type hierarchy
    item_type: str = "line_item"   # section_total | program_total | line_item

    # Provenance
    llm_model: str = ""            # model that produced this row
    extraction_pass: str = ""      # "scan" | "extract" | "direct"
    notes: str = ""                # free-form LLM notes / caveats

    def validate(self) -> list[str]:
        """Return list of validation warnings (empty = OK)."""
        warnings: list[str] = []
        if not self.country:
            warnings.append("missing country")
        if not self.year or self.year < 1900 or self.year > 2100:
            warnings.append(f"suspicious year: {self.year}")
        if self.amount_local is None:
            warnings.append("amount_local is None")
        elif self.amount_local < 0:
            warnings.append(f"negative amount: {self.amount_local}")
        if self.rd_category not in RD_CATEGORIES and self.rd_category != "unclear":
            warnings.append(f"unknown rd_category: {self.rd_category}")
        if not (0.0 <= self.confidence <= 1.0):
            warnings.append(f"confidence out of range: {self.confidence}")
        if self.decision not in ("include", "review", "skip"):
            warnings.append(f"unknown decision: {self.decision}")
        return warnings

    def get(self, key: str, default=None):
        """Dict-compatible .get() so BudgetRow can be used interchangeably with dict."""
        return getattr(self, key, default)

    def to_dict(self) -> dict:
        d = asdict(self)
        # Ensure all OUTPUT_COLUMNS are present
        for col in OUTPUT_COLUMNS:
            if col not in d:
                d[col] = ""
        return {col: d.get(col, "") for col in OUTPUT_COLUMNS}

    @classmethod
    def from_llm_json(
        cls,
        item: dict,
        *,
        country: str,
        year: int,
        source_file: str,
        llm_model: str,
        extraction_pass: str = "extract",
    ) -> "BudgetRow":
        """Construct a BudgetRow from a raw LLM-extracted JSON item dict."""

        def _f(key: str, default="") -> str:
            return str(item.get(key, default) or default).strip()

        def _float(key: str) -> Optional[float]:
            val = item.get(key)
            if val is None:
                return None
            try:
                return float(str(val).replace(",", "").strip())
            except (ValueError, TypeError):
                return None

        row = cls(
            country=country,
            year=year,
            source_file=source_file,
            page_number=_f("page_number"),
            item_type=_f("item_type") or "line_item",
            section_code=_f("section_code"),
            section_name=_f("section_name"),
            section_name_en=_f("section_name_en") or _f("section_name"),
            line_code=_f("line_code"),
            line_description=_f("line_description"),
            line_description_en=_f("line_description_en") or _f("line_description"),
            amount_local=_float("amount_local"),
            unit=_f("unit"),
            currency=_f("currency"),
            rd_category=_f("rd_category") or "unclear",
            decision=_f("decision") or "include",
            confidence=float(item.get("confidence", 0.8) or 0.8),
            llm_model=llm_model,
            extraction_pass=extraction_pass,
            notes=_f("notes"),
        )
        return row


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def rows_to_csv(rows: list, path: Path) -> None:
    """Write rows to CSV. Accepts BudgetRow objects or plain dicts."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            d = row.to_dict() if hasattr(row, "to_dict") else row
            writer.writerow({k: d.get(k, "") for k in OUTPUT_COLUMNS})


def rows_to_excel(rows: list, path: Path) -> None:
    """Write rows to Excel. Accepts BudgetRow objects or plain dicts."""
    import openpyxl
    path.parent.mkdir(parents=True, exist_ok=True)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Budget LLM Results"
    ws.append(OUTPUT_COLUMNS)
    for row in rows:
        d = row.to_dict() if hasattr(row, "to_dict") else row
        ws.append([d.get(col, "") for col in OUTPUT_COLUMNS])
    wb.save(path)


def load_csv(path: Path) -> list[BudgetRow]:
    """Load existing results CSV back into BudgetRow objects."""
    if not path.exists():
        return []
    rows: list[BudgetRow] = []
    with open(path, newline="", encoding="utf-8") as f:
        for item in csv.DictReader(f):
            row = BudgetRow()
            for col in OUTPUT_COLUMNS:
                val = item.get(col, "")
                if col in ("year",):
                    try:
                        val = int(val)
                    except (ValueError, TypeError):
                        val = 0
                elif col in ("amount_local", "amount_usd_approx", "confidence"):
                    try:
                        val = float(val) if val else None
                    except (ValueError, TypeError):
                        val = None
                setattr(row, col, val)
            rows.append(row)
    return rows
