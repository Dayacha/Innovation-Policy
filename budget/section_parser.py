"""Structure-aware parser for Danish Finanslov (Finance Bill) documents.

Danish budget documents follow a hierarchical structure:
  § 20. Undervisningsministeriet ............ 7.711.455.939   ← ministry section
    20.31. Universiteter og højere læreranstalter:            ← sub-section
      01.01. Driftsbudget:                                    ← line-item group
        Driftsudgifter ........................... 387.000.000 ← spending line
        Tilskud .....................................  2.500.000 ← grant line

Number format: Danish uses '.' as thousands separator → 7.711.455.939 = 7,711,455,939
Negative amounts use '÷' or '-' prefix.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass
class BudgetLine:
    """One parsed line from a budget document."""
    line_code: str       # "§20", "20.31", "01.01", program code, or "" for plain lines
    description: str     # text label of the line
    raw_amount: str      # as found in text, e.g. "387.000.000"
    amount_value: int    # parsed integer (0 when absent)
    section_code: str    # nearest § ancestor, e.g. "§20"
    section_name: str    # ministry name, e.g. "Undervisningsministeriet"
    line_type: str       # "section_header" | "subsection" | "program_header" | "line_item"
    original_line: str
    item_code: str = ""          # sub-item code like 2.11, 62.01
    item_description: str = ""    # header text for that item
    context_before: str = field(default="")
    context_after: str = field(default="")


# ── Amount regex ─────────────────────────────────────────────────────────────
# Matches Danish-formatted numbers at the right of a line:
#   "Driftsudgifter ........... 3.549.062.600"
#   "§ 20. Undervisningsministeriet .... 7.711.455.939"
#   "÷ 25.800.000.000"  (negative)
_RE_AMOUNT_TAIL = re.compile(
    r"^(.+?)[\.\-_\s]{3,}\s*"           # description + separator (dots / spaces)
    r"([÷\-\+]?\s*\d{1,3}"             # sign + first 1-3 digits
    r"(?:[.,\s]\d{3})*"                 # thousands groups
    r"(?:[.,]\d{1,2})?)"                # optional cents
    r"\s*\d?\)?\s*$",                   # optional footnote marker at end
    re.UNICODE,
)

# § section header: "§ 20. Undervisningsministeriet" or "& 20." (OCR artifact)
# Allow optional leading noise chars (e.g. ": § 20." from PDF extraction artifacts)
_RE_SECTION = re.compile(
    r"^[^§&8\d]*[§&8]\s*(\d+(?:\.\d+)?)\.\s+(.+)",
    re.UNICODE,
)

# Sub-section code: "20.31.", "01.01.", "3.04.94." etc.
_RE_SUBSECTION = re.compile(
    r"^(\d{1,2}\.\d{2}(?:\.\d{2})?)\.\s+(.+)",
)

# Item header like "2.11. Undersøgelsesskibene:" (colon optional)
_RE_ITEM_HEADER = re.compile(
    r"^(\d{1,2}\.\d{2})\.\s+(.+?):?\s*$",
)

# Budget line with code + budget keyword on same line
_RE_CODE_BUDGET = re.compile(
    r"^(driftsudgifter|tilskud|indtægter|anlægsudgifter)\s+(\d{1,2}\.\d{2})\.\s+(.+)",
    re.IGNORECASE,
)

# Program header without trailing dot and without inline amount
_RE_PROGRAM_HEADER = re.compile(
    r"^(\d{1,2}\.\d{2}(?:\.\d{2})?)\s+(.+)",
)

# Year pattern to reject years masquerading as amounts
_RE_YEAR = re.compile(r"^(19|20)\d{2}$")

# Standalone amount line: just a number (possibly with sign), no description text
# Matches: "2.450.000", "÷ 25.800.000", "300.000", "1.533.000 1)" etc.
_RE_STANDALONE_AMOUNT = re.compile(
    r"^\s*[÷\-\+]?\s*\d{1,3}(?:[.,]\d{3})+\s*\d?\)?\s*$",
    re.UNICODE,
)


def _parse_danish_amount(raw: str) -> int | None:
    """Parse a Danish-format number string into an integer.

    '7.711.455.939' → 7711455939
    '÷ 25.800.000.000' → -25800000000
    Returns None for values that look like years or are too small.
    """
    if not raw:
        return None
    s = raw.strip()
    negative = s.startswith(("÷", "−", "-"))
    digits = re.sub(r"[^\d]", "", s)
    if not digits:
        return None
    if _RE_YEAR.match(digits):
        return None
    val = int(digits)
    if val < 10_000:
        return None  # too small to be a budget amount
    return -val if negative else val


def _extract_tail_amount(line: str) -> tuple[str, int] | None:
    """Extract the right-aligned amount from a line. Returns (raw, value) or None."""
    m = _RE_AMOUNT_TAIL.match(line.strip())
    if not m:
        return None
    raw = m.group(2).strip()
    val = _parse_danish_amount(raw)
    if val is None:
        return None
    return raw, val


def parse_page_lines(
    page_text: str,
    section_code: str = "",
    section_name: str = "",
) -> tuple[list[BudgetLine], str, str]:
    """Parse one page of budget text into structured BudgetLine records.

    Carries section context (section_code, section_name) in and out so
    callers can thread context across consecutive pages of the same file.

    Returns:
        (budget_lines, updated_section_code, updated_section_name)
    """
    raw = [ln.strip() for ln in re.split(r"\r?\n", page_text or "") if ln.strip()]
    result: list[BudgetLine] = []
    skip_next = False
    current_item_code = ""
    current_item_desc = ""

    for i, line in enumerate(raw):
        if skip_next:
            skip_next = False
            continue

        before = raw[i - 1] if i > 0 else ""
        next_line = raw[i + 1] if i < len(raw) - 1 else ""
        # Look two lines ahead for multi-line descriptions ("text continues\namount")
        after = raw[i + 2] if i < len(raw) - 2 else next_line

        # Helper: if current line has no inline amount, check if next line is a
        # standalone amount (common pattern in 1970s Danish scanned Finanslov PDFs)
        def _next_line_amount() -> tuple[str, int] | None:
            if next_line and _RE_STANDALONE_AMOUNT.match(next_line):
                val = _parse_danish_amount(next_line)
                if val is not None:
                    return next_line.strip(), val
            return None

        # ── § section header ──────────────────────────────────────────────
        m = _RE_SECTION.match(line)
        if m:
            sec_num = m.group(1)
            rest = m.group(2).rstrip(". ")
            tail = _extract_tail_amount(line)
            desc = rest
            if tail:
                am = _RE_AMOUNT_TAIL.match(line)
                if am:
                    desc = am.group(1).lstrip("§&8 0123456789.").strip()
            elif (nla := _next_line_amount()):
                tail = nla
                skip_next = True

            section_code = f"§{sec_num}"
            section_name = desc

            result.append(BudgetLine(
                line_code=section_code,
                description=desc,
                raw_amount=tail[0] if tail else "",
                amount_value=tail[1] if tail else 0,
                section_code=section_code,
                section_name=section_name,
                line_type="section_header",
                original_line=line,
                context_before=before,
                context_after=after,
            ))
            continue

        # ── Sub-section code: "20.31. Universiteter ..." ─────────────────
        m2 = _RE_SUBSECTION.match(line)
        if m2:
            code = m2.group(1)
            rest2 = m2.group(2).rstrip(". :")
            tail2 = _extract_tail_amount(line)
            desc2 = rest2
            if tail2:
                am2 = _RE_AMOUNT_TAIL.match(line)
                if am2:
                    desc2 = am2.group(1).strip().rstrip(". :")
            elif (nla2 := _next_line_amount()):
                tail2 = nla2
                skip_next = True

            result.append(BudgetLine(
                line_code=code,
                description=desc2,
                raw_amount=tail2[0] if tail2 else "",
                amount_value=tail2[1] if tail2 else 0,
                section_code=section_code,
                section_name=section_name,
                line_type="subsection",
                original_line=line,
                context_before=before,
                context_after=after,
            ))
            continue

        # ── Item header (e.g. "2.22. Fiskeriministeriets forsøgslaboratorium:") ──
        m_item = _RE_ITEM_HEADER.match(line)
        if m_item and not _extract_tail_amount(line):
            current_item_code = m_item.group(1)
            current_item_desc = m_item.group(2).strip()
            result.append(BudgetLine(
                line_code=current_item_code,
                description=current_item_desc,
                raw_amount="",
                amount_value=0,
                section_code=section_code,
                section_name=section_name,
                line_type="program_header",
                original_line=line,
                item_code=current_item_code,
                item_description=current_item_desc,
                context_before=before,
                context_after=after,
            ))
            continue

        # ── Program header without inline amount (e.g. "62.02 Bidrag …") ──
        m_prog = _RE_PROGRAM_HEADER.match(line)
        if m_prog and not _extract_tail_amount(line) and not _RE_STANDALONE_AMOUNT.match(next_line or ""):
            code = m_prog.group(1)
            desc_prog = m_prog.group(2).strip().rstrip(".: ")
            result.append(BudgetLine(
                line_code=code,
                description=desc_prog,
                raw_amount="",
                amount_value=0,
                section_code=section_code,
                section_name=section_name,
                line_type="program_header",
                original_line=line,
                context_before=before,
                context_after=after,
            ))
            continue

        # ── Line with budget keyword + code on same line ─────────────────
        m_code_budget = _RE_CODE_BUDGET.match(line)
        tail_code_budget = _extract_tail_amount(line)
        if m_code_budget and tail_code_budget:
            budget_kw = m_code_budget.group(1).strip()
            current_item_code = m_code_budget.group(2)
            current_item_desc = m_code_budget.group(3).strip().rstrip(":")
            result.append(BudgetLine(
                line_code=current_item_code,
                description=budget_kw,
                raw_amount=tail_code_budget[0],
                amount_value=tail_code_budget[1],
                section_code=section_code,
                section_name=section_name,
                line_type="line_item",
                original_line=line,
                item_code=current_item_code,
                item_description=current_item_desc,
                context_before=before,
                context_after=after,
            ))
            continue

        # ── Generic line with right-aligned amount ───────────────────────
        tail3 = _extract_tail_amount(line)
        if tail3:
            am3 = _RE_AMOUNT_TAIL.match(line.strip())
            desc3 = am3.group(1).rstrip(". ") if am3 else line
            item_code = current_item_code
            item_desc = current_item_desc
            # If desc3 itself starts with a code, reset item context
            m_inline = _RE_ITEM_HEADER.match(desc3 + ":")
            if m_inline:
                item_code = m_inline.group(1)
                item_desc = m_inline.group(2).strip()
                current_item_code = item_code
                current_item_desc = item_desc
                # If the rest after budget keyword exists, reduce desc3 to budget type only
                desc3 = desc3
            result.append(BudgetLine(
                line_code=item_code,
                description=desc3,
                raw_amount=tail3[0],
                amount_value=tail3[1],
                section_code=section_code,
                section_name=section_name,
                line_type="line_item",
                original_line=line,
                item_code=item_code,
                item_description=item_desc,
                context_before=before,
                context_after=after,
            ))
            continue

        # ── Generic description line with amount on next line ─────────────
        # e.g. "Driftsudgifter...,,,; . . . ," followed by "1.522.000"
        # Must have some text and not be a pure noise line
        if len(line) > 3 and (nla3 := _next_line_amount()):
            desc4 = re.sub(r"[\.\,\;\s]+$", "", line).strip()
            if desc4:
                skip_next = True
                result.append(BudgetLine(
                    line_code="",
                    description=desc4,
                    raw_amount=nla3[0],
                    amount_value=nla3[1],
                    section_code=section_code,
                    section_name=section_name,
                    line_type="line_item",
                    original_line=line,
                    item_code=current_item_code,
                    item_description=current_item_desc,
                    context_before=before,
                    context_after=after,
                ))

    return result, section_code, section_name
