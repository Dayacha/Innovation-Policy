"""UK Budget Red Book / DEL table extractor.

Two complementary extraction approaches:

1. **DEL table parsing** (2008+, primary method)
   Modern UK Budget documents include Departmental Expenditure Limits (DEL) tables
   with rows for science/R&D-related departments. We extract:

   - 2023+: "Science, Innovation and Technology" (DSIT) — dedicated R&D dept.
     Capital DEL is primarily UKRI + Innovate UK; high confidence GBARD proxy.
   - 2016-2022: "Business, Energy and Industrial Strategy" (BEIS) — includes UKRI
     and science budget; Capital DEL is primary R&D vehicle under ESA10.
   - 2010-2016: "Business, Innovation and Skills" (BIS) — science budget was a core
     component; Capital DEL includes research council grants.
   - 2008-2010: "Innovation, Universities and Skills" (DIUS) — dedicated science
     department that existed before BIS was created.

   DEL tables appear in both tab-separated (older) and newline-per-value (newer)
   formats.  The parser handles both.

2. **Narrative science budget figures** (all years, secondary method)
   Some Red Books explicitly state the total UK science budget or government R&D
   spending as a single aggregate figure. These are extracted as high-confidence
   "include" records where they match the current budget year, not future targets.

Currency: GBP; DEL table amounts are in £ billion → converted to full pounds.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

logger = logging.getLogger("innovation_pipeline")


# ── Department registry ────────────────────────────────────────────────────────
# (name_substring, program_code, canonical_name, decision, confidence)
_DEPT_REGISTRY: list[tuple[str, str, str, str, float]] = [
    (
        "Department of Trade and Industry",
        "UK_SCIMINISTRY",
        "UK Science Ministry (BEIS / BIS / DIUS / DTI)",
        "review",
        0.65,
    ),
    (
        "Trade and Industry",
        "UK_SCIMINISTRY",
        "UK Science Ministry (BEIS / BIS / DIUS / DTI)",
        "review",
        0.62,
    ),
    (
        "Science, Innovation and Technology",
        "UK_DSIT",
        "Dept for Science, Innovation and Technology",
        "include",
        0.92,
    ),
    (
        "Business, Energy and Industrial Strategy",
        # Consolidated code: BEIS was the UK science ministry 2016-2023.
        # Capital DEL is the primary R&D vehicle (UKRI + Innovate UK grants).
        "UK_SCIMINISTRY",
        "UK Science Ministry (BEIS / BIS / DIUS)",
        "include",
        0.78,
    ),
    (
        "Business, Innovation and Skills",
        "UK_SCIMINISTRY",
        "UK Science Ministry (BEIS / BIS / DIUS)",
        "include",
        0.75,
    ),
    (
        "Innovation, Universities and Skills",
        "UK_SCIMINISTRY",
        "UK Science Ministry (BEIS / BIS / DIUS)",
        "include",
        0.78,
    ),
]

# ── Regex helpers ──────────────────────────────────────────────────────────────

# Fiscal year label: 2011-12, 2023-24 etc.
_FY_LABEL_RE = re.compile(r"\b(\d{4})-(\d{2})\b")

# A line that is purely a fiscal year (possibly with trailing footnote digits)
_FY_LINE_RE = re.compile(r"^(\d{4})-(\d{2})\d*$")

# A line that is a numeric DEL amount (£bn): 0.0, 17.2, 116.1, -0.8 etc.
# Also matches plain dash ("-") for N/A
_AMOUNT_LINE_RE = re.compile(r"^-?\s*\d[\d,. ]*\d?\s*$|^-$")

# Table page detection
_DEL_TABLE_RE = re.compile(
    r"Departmental\s+(?:Expenditure\s+Limits|(?:Resource|Capital)\s+Budgets)",
    re.IGNORECASE,
)
_GBP_BN_HEADER_RE = re.compile(r"£\s*billion", re.IGNORECASE)

# Capital DEL section marker
_CAPITAL_DEL_RE = re.compile(r"\bCapital DEL\b", re.IGNORECASE)
_RESOURCE_DEL_RE = re.compile(r"\bResource DEL\b", re.IGNORECASE)

# Narrative science budget patterns
_NARRATIVE_PATTERNS: list[tuple[re.Pattern, str, str, float]] = [
    (
        re.compile(
            r"total\s+UK\s+science\s+spending.{0,60}£\s*([\d,.]+)\s*(billion|bn)",
            re.IGNORECASE | re.DOTALL,
        ),
        "UK_SCIENCE_TOTAL",
        "Total UK science spending",
        0.88,
    ),
    (
        re.compile(
            r"ring.fenced\s+science\s+budget.{0,80}£\s*([\d,.]+)\s*(billion|bn)",
            re.IGNORECASE | re.DOTALL,
        ),
        "UK_SCIENCE_BUDGET",
        "Ring-fenced science budget",
        0.90,
    ),
    (
        re.compile(
            r"(?:government|public)\s+(?:investment\s+in|spending\s+on)\s+"
            r"science.{0,80}£\s*([\d,.]+)\s*(billion|bn)",
            re.IGNORECASE | re.DOTALL,
        ),
        "UK_SCIENCE_INVEST",
        "Government investment in science",
        0.85,
    ),
]

# Exclude context: these indicate the amount is private-sector, cumulative, or
# a far-future target — not current government budget
_EXCLUDE_NARRATIVE_RE = re.compile(
    r"(tax\s+relief|r&d\s+tax|private\s+sector|businesses?\s+spend|"
    r"over\s+(?:the\s+)?(?:next\s+)?\d+\s*years?|by\s+20[2-9]\d|"
    r"catapult|satellite|quantum\s+computing|ai\s+for\s+science|"
    r"metascience|cumulative)",
    re.IGNORECASE,
)


# ── DEL table parsing ──────────────────────────────────────────────────────────


def _is_del_page(text: str) -> bool:
    return bool(_DEL_TABLE_RE.search(text) and _GBP_BN_HEADER_RE.search(text))


def _extract_fiscal_years(lines: list[str]) -> list[str]:
    """Return ordered list of fiscal year labels found in the table header."""
    fiscal_years: list[str] = []
    seen: set[str] = set()
    for line in lines[:60]:  # headers are always near the top
        # Single year on a line (strip trailing footnote digits)
        m = _FY_LINE_RE.match(line)
        if m:
            fy = f"{m.group(1)}-{m.group(2)}"
            if fy not in seen:
                fiscal_years.append(fy)
                seen.add(fy)
            continue
        # Multiple years on one line (tab- or space-separated)
        for m2 in _FY_LABEL_RE.finditer(line):
            fy = f"{m2.group(1)}-{m2.group(2)}"
            if fy not in seen:
                fiscal_years.append(fy)
                seen.add(fy)
    return fiscal_years


def _normalize_ocr_amount_tokens(tokens: list[str]) -> list[str]:
    """Repair common DEL OCR collapses like 163 -> 16.3 or 12 -> 1.2.

    UK DEL rows often contain values in a narrow range of £bn amounts. When OCR
    drops decimal points, sequences such as:
      163 15.9 149 13.9  -> 16.3 15.9 14.9 13.9
      12 13 1.0 12       -> 1.2  1.3  1.0  1.2
    become tractable again. Very large integers (e.g. "2021" from FY labels)
    are left untouched here and later filtered out as implausible.
    """
    normalized: list[str] = []
    for tok in tokens:
        clean = tok.replace(",", "").replace(" ", "").strip()
        if not clean:
            normalized.append(tok)
            continue
        if "." in clean or clean.startswith("-"):
            normalized.append(clean)
            continue
        if clean.isdigit():
            if len(clean) == 2:
                normalized.append(f"{clean[0]}.{clean[1]}")
                continue
            if len(clean) == 3:
                normalized.append(f"{clean[:2]}.{clean[2]}")
                continue
        normalized.append(clean)
    return normalized


def _parse_del_page(
    page_text: str,
) -> list[dict]:
    """
    Parse a DEL table page and return raw extraction records.
    Each record: {dept_code, fiscal_year, budget_year, del_type, amount_bn, decision, confidence}
    """
    lines = [line.strip() for line in page_text.split("\n")]
    fiscal_years = _extract_fiscal_years(lines)
    if not fiscal_years:
        return []

    n_years = len(fiscal_years)
    results: list[dict] = []
    # Detect page-level DEL type from table title
    if re.search(r"Capital\s+Departmental\s+Expenditure", page_text, re.IGNORECASE):
        current_del_type = "Capital DEL"
    elif re.search(r"Total\s+Departmental\s+Expenditure", page_text, re.IGNORECASE):
        current_del_type = "Total DEL"
    else:
        current_del_type = "Resource DEL"

    i = 0
    while i < len(lines):
        raw_line = lines[i]

        # Track section type
        if _CAPITAL_DEL_RE.search(raw_line):
            current_del_type = "Capital DEL"
        elif _RESOURCE_DEL_RE.search(raw_line):
            current_del_type = "Resource DEL"

        # Skip "of which:" sub-rows
        if raw_line.lower().startswith("of which"):
            i += 1
            continue

        # Try to match a target department
        # Strip trailing footnote superscripts (1-2 digits)
        clean_line = re.sub(r"\d{1,2}$", "", raw_line).strip()

        matched = False
        for dept_name, dept_code, canonical, decision, confidence in _DEPT_REGISTRY:
            if dept_name not in clean_line:
                continue

            matched = True
            format_c_used = False
            # Found department row — collect amounts
            # Format A: tab-separated values on same line
            parts = re.split(r"\t+", raw_line)
            if len(parts) >= n_years + 1:
                amounts_raw = parts[1 : n_years + 1]
            else:
                # Format B: values on subsequent lines (direct_text extraction)
                amounts_raw = []
                j = i + 1
                while j < len(lines) and len(amounts_raw) < n_years:
                    if _AMOUNT_LINE_RE.match(lines[j]):
                        amounts_raw.append(lines[j])
                        j += 1
                    elif not lines[j]:
                        j += 1  # skip blank lines
                    else:
                        break  # stop at next non-numeric line

            # Format C: space-separated amounts on the same line (OCR fallback).
            # E.g. "Business, Innovation and Skills 163 15.9 149 13.9"
            # Used when OCR collapses multi-column rows onto a single line.
            if not amounts_raw:
                name_idx = raw_line.find(dept_name)
                if name_idx >= 0:
                    suffix = raw_line[name_idx + len(dept_name):]
                else:
                    suffix = re.sub(re.escape(dept_name), "", clean_line, count=1)
                tokens = re.findall(r"-?\d[\d,.]*", suffix.strip())
                if len(tokens) >= n_years:
                    amounts_raw = _normalize_ocr_amount_tokens(tokens[:n_years])
                    format_c_used = True
                elif tokens:
                    amounts_raw = _normalize_ocr_amount_tokens(tokens)
                    format_c_used = True

            for fy, amt_str in zip(fiscal_years, amounts_raw):
                amt_str = str(amt_str).strip()
                if not amt_str or amt_str == "-":
                    continue
                try:
                    amount_bn = float(amt_str.replace(",", "").replace(" ", ""))
                except ValueError:
                    continue
                if format_c_used and amount_bn > 100:
                    continue  # likely a fiscal-year token or missing decimal OCR
                if amount_bn < 0.5:
                    continue  # filter out tiny/zero amounts
                # Fiscal year YYYY-YY → calendar year YYYY
                budget_year = str(int(fy[:4]))
                # Format C (OCR) values may have dropped decimal points;
                # mark as 'review' with lower confidence so anomaly detection
                # and AI validation can catch bad OCR (e.g. "13" for "1.3").
                row_decision = decision if not format_c_used else "review"
                row_confidence = confidence if not format_c_used else round(confidence * 0.7, 2)
                results.append(
                    {
                        "dept_code": dept_code,
                        "canonical": canonical,
                        "fiscal_year": fy,
                        "budget_year": budget_year,
                        "del_type": current_del_type,
                        "amount_bn": amount_bn,
                        "decision": row_decision,
                        "confidence": row_confidence,
                    }
                )
            break  # matched a department; stop checking others

        i += 1

    return results


# ── Narrative science total patterns ───────────────────────────────────────────


def _parse_narrative(text: str, doc_year: str) -> list[dict]:
    """Extract aggregate science/R&D totals from narrative text."""
    results: list[dict] = []
    for pattern, prog_code, label, confidence in _NARRATIVE_PATTERNS:
        for m in pattern.finditer(text):
            snippet = " ".join(m.group(0).split())
            if _EXCLUDE_NARRATIVE_RE.search(snippet):
                continue
            # Parse amount
            raw_num = m.group(1).replace(",", "")
            try:
                value = float(raw_num)
            except ValueError:
                continue
            unit = m.group(2).lower()
            amount = value * 1e9 if unit in ("billion", "bn") else value * 1e6
            if amount < 1e9:
                continue  # require ≥ £1 billion
            results.append(
                {
                    "prog_code": prog_code,
                    "label": label,
                    "amount": amount,
                    "confidence": confidence,
                    "snippet": snippet[:240],
                }
            )
    return results


# ── Record builder ─────────────────────────────────────────────────────────────


def _build_record(
    *,
    country: str,
    year: str,
    source_filename: str,
    file_id: str,
    page_number: int,
    program_code: str,
    label: str,
    amount: float,
    confidence: float,
    decision: str,
    line_description: str,
) -> dict:
    return {
        "country": country,
        "year": year,
        "section_code": "UK_SCIENCE",
        "section_name": "Science and innovation",
        "section_name_en": "Science and innovation",
        "program_code": program_code,
        "line_description": line_description,
        "line_description_en": line_description,
        "amount_local": amount,
        "currency": "GBP",
        "unit": "GBP",
        "rd_category": "direct_rd",
        "taxonomy_score": 8.5,
        "decision": decision,
        "confidence": confidence,
        "source_file": source_filename,
        "file_id": file_id,
        "page_number": page_number,
        "program_description": label,
        "program_description_en": label,
    }


# ── Main extractor ─────────────────────────────────────────────────────────────


def extract_uk_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract R&D budget data from UK Red Book PDFs."""
    records: list[dict] = []
    # Deduplicate: (program_code, budget_year, del_type)
    seen_keys: set[tuple] = set()

    for row in sorted_pages.itertuples(index=False):
        page_number = int(row.page_number)
        text = row.text if isinstance(row.text, str) else ""
        if not text.strip():
            continue

        # 1. DEL table extraction
        if _is_del_page(text):
            del_records = _parse_del_page(text)
            for r in del_records:
                # Only keep the row whose fiscal year matches the document year.
                # UK DEL tables span multiple years (outturn + plans); data for
                # year X should come from the X budget document, not a later one
                # that shows X as a historical comparison column.
                if r["budget_year"] != str(year):
                    continue
                prog_code = f"{r['dept_code']}_{r['del_type'].split()[0].upper()}"
                dedup_key = (r["dept_code"], r["budget_year"], r["del_type"])
                if dedup_key in seen_keys:
                    continue
                seen_keys.add(dedup_key)
                description = f"{r['canonical']} — {r['del_type']} (FY {r['fiscal_year']})"
                records.append(
                    _build_record(
                        country=country,
                        year=r["budget_year"],
                        source_filename=source_filename,
                        file_id=file_id,
                        page_number=page_number,
                        program_code=prog_code,
                        label=r["canonical"],
                        amount=r["amount_bn"] * 1e9,
                        confidence=r["confidence"],
                        decision=r["decision"],
                        line_description=description,
                    )
                )

        # 2. Narrative science total extraction
        narrative_records = _parse_narrative(text, year)
        for r in narrative_records:
            dedup_key = (r["prog_code"], year)
            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)
            records.append(
                _build_record(
                    country=country,
                    year=year,
                    source_filename=source_filename,
                    file_id=file_id,
                    page_number=page_number,
                    program_code=r["prog_code"],
                    label=r["label"],
                    amount=r["amount"],
                    confidence=r["confidence"],
                    decision="include",
                    line_description=r["snippet"],
                )
            )

    if not records:
        logger.debug(
            "UK extractor: no science/R&D data found in %s (year %s).",
            source_filename,
            year,
        )

    return records
