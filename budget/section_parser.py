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
# Allow only short leading noise (≤8 chars) to avoid matching § references deep
# inside legal annotation lines like "...tekstanmærkning til § 16. Sundhedsmini-".
_RE_SECTION = re.compile(
    r"^[^§&8\d]{0,8}[§&8]\s*(\d\s*\d?)\s*\.\s*([A-ZÆØÅÜÖ].{2,})",
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

# Group header: "09. Særlige forskningsinstitutioner" or "20. Anlægs- og udlånsbevillinger"
# These are top-level dividers inside a § section (single digit-group code).
# Detecting them lets the parser reset sub-item context so that following
# section-level totals (Anlægsudgifter, Tilskud, …) are NOT attributed to
# the last program code carried over from the previous page.
_RE_GROUP_HEADER = re.compile(
    r"^(\d{1,2})\.\s+([A-ZÆØÅÖ].{8,})",
    re.UNICODE,
)

# Program header without trailing dot and without inline amount
_RE_PROGRAM_HEADER = re.compile(
    r"^(\d{1,2}\.\d{2}(?:\.\d{2})?)\s+(.+)",
)

# Sub-item in 1991+ Mill. kr. format: "01. Departementets afdelinger (Driftsbev.)"
# Two-digit code followed by description (distinct from subsection NN.MM. codes).
_RE_SUBITEM = re.compile(
    r"^(\d{2})\.\s+(.+)",
)

# Year pattern to reject years masquerading as amounts
_RE_YEAR = re.compile(r"^(19|20)\d{2}$")

# Standalone amount line: just a number (possibly with sign), no description text
# Matches: "2.450.000", "÷ 25.800.000", "300.000", "1.533.000 1)" etc.
_RE_STANDALONE_AMOUNT = re.compile(
    r"^\s*[÷\-\+]?\s*\d{1,3}(?:[.,]\d{3})+\s*\d?\)?\s*$",
    re.UNICODE,
)

# Mill. kr. standalone amount: "89,9", "1.161,1", "-60,0", "745,0"
# Used for 1991+ Danish Finance Bills where amounts are in millions of kroner.
_RE_MILL_AMOUNT = re.compile(
    r"^\s*[÷\-]?\s*\d{1,4}(?:\.\d{3})*[,\s]\d{1,2}\s*\d?\)?\s*$",
    re.UNICODE,
)

# Ministry-level aggregate labels in the "Artsoversigt" block.
# When encountered, reset current item context — subsequent amounts are
# ministry totals, NOT program-level R&D line items.
_ARTSOVERSIGT_LABELS: frozenset[str] = frozenset({
    "artsoversigt", "artsoversigt:", "bevilling i alt", "beviling i alt",
    "nettostyrede aktiviteter", "aktivitet i alt", "udgifter i alt",
    "driftsindtægter", "anlægsindtægter", "overførselsudgifter",
    "skatter og overførselsindtægter",
    "overførsler mellem offentlige myndigheder",
    "finansielle poster",
})

# Labels that mark the START of the detailed appropriations section.
# In newer Finance Bills (2014+), the artsoversigt appears BEFORE the
# program items ("A. Oversigter." → artsoversigt → "B. Bevillinger." →
# detailed items). We must reset in_artsoversigt when we enter section B.
_ARTSOVERSIGT_RESET_LABELS: frozenset[str] = frozenset({
    "b. bevillinger",
    "b. specifikationer",
})


def _despace_ocr(text: str) -> str:
    """Collapse character-spaced OCR text: 'U d e n r i g s' → 'Udenrigs'.

    Only collapses when every space-separated token is a single character
    (a hallmark of character-spaced scanned text). Mixed-width tokens are
    left unchanged.
    """
    tokens = text.strip().split()
    if len(tokens) >= 3 and all(len(t) == 1 for t in tokens):
        return "".join(tokens)
    return text.strip()


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


def _parse_mill_kr_amount(raw: str) -> int | None:
    """Parse a Mill. kr. amount string to integer kroner.

    '89,9'    →  89_900_000
    '1.161,1' → 1_161_100_000
    '745,0'   → 745_000_000
    '-60,0'   → -60_000_000
    Returns None for values that look like years or too small.
    """
    if not raw:
        return None
    s = raw.strip()
    negative = s.startswith(("÷", "−", "-"))
    s = re.sub(r"^[÷−\-\s]+", "", s).strip()

    # Normalise OCR space-as-decimal-separator: "225 6" → "225,6"
    if " " in s and "," not in s:
        parts = s.split()
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            s = parts[0] + "," + parts[1]

    if "," in s:
        int_part, dec_part = s.rsplit(",", 1)
    else:
        int_part, dec_part = s, "0"

    # Integer part: remove thousand-separator dots
    int_digits = re.sub(r"[^\d]", "", int_part)
    dec_digits = re.sub(r"[^\d]", "", dec_part)
    if not int_digits:
        return None
    int_val = int(int_digits)
    dec_str = dec_digits[:2] if len(dec_digits) >= 2 else dec_digits.ljust(1, "0")
    # 1 decimal place: 0.X mill kr = X * 100,000 kr
    # 2 decimal places: 0.XY mill kr = XY * 10,000 kr
    if len(dec_str) == 1:
        kr_val = int_val * 1_000_000 + int(dec_str) * 100_000
    else:
        kr_val = int_val * 1_000_000 + int(dec_str) * 10_000

    if kr_val == 0:
        return None
    return -kr_val if negative else kr_val


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
    mill_kr_mode: bool = False,
    in_artsoversigt: bool = False,
) -> tuple[list[BudgetLine], str, str, bool]:
    """Parse one page of budget text into structured BudgetLine records.

    Carries section context (section_code, section_name, in_artsoversigt) in
    and out so callers can thread context across consecutive pages of the same file.

    Args:
        mill_kr_mode: When True, treat standalone numbers like "89,9" as
                      Mill. kr. amounts (1991+ Danish Finance Bill format).
        in_artsoversigt: When True, we are inside a ministry-level aggregate
                         block (Artsoversigt). All lines are skipped until the
                         next § section header resets the flag.

    Returns:
        (budget_lines, updated_section_code, updated_section_name, in_artsoversigt)
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
        # In mill_kr_mode, also matches "89,9", "745,0" etc. (Mill. kr. format).
        def _next_line_amount() -> tuple[str, int] | None:
            if not next_line:
                return None
            if _RE_STANDALONE_AMOUNT.match(next_line):
                val = _parse_danish_amount(next_line)
                if val is not None:
                    return next_line.strip(), val
            if mill_kr_mode and _RE_MILL_AMOUNT.match(next_line):
                val = _parse_mill_kr_amount(next_line)
                if val is not None:
                    return next_line.strip(), val
            return None

        # ── Skip artsoversigt block content ───────────────────────────────
        # After an artsoversigt label is detected, all lines are ministry-level
        # aggregate totals (not program-level R&D items) and must be skipped.
        # Resets on:
        #   • a new § section header (always)
        #   • "B. Bevillinger." and similar labels that mark the start of the
        #     detailed appropriations section in newer Finance Bills (2014+
        #     structure: artsoversigt appears before program items, not after).
        if in_artsoversigt:
            if _RE_SECTION.match(line):
                in_artsoversigt = False  # fall through to § section processing
            elif line.strip().lower().rstrip(" .") in _ARTSOVERSIGT_RESET_LABELS:
                in_artsoversigt = False  # fall through to normal processing
            elif _RE_SUBSECTION.match(line) or _RE_ITEM_HEADER.match(line):
                # A subsection code (NN.MM. or NN.MM.XX.) appearing while in
                # artsoversigt mode means we've entered the B.Bevillinger.
                # detail section without an explicit "B. Bevillinger." header.
                # This happens in 1991–1997 Finance Bills when the artsoversigt
                # fits on one page and the detail items begin on the next page
                # without a labelled header.
                in_artsoversigt = False  # fall through to normal processing
            else:
                continue  # skip all other artsoversigt content

        # ── Artsoversigt / ministry aggregate block ───────────────────────
        # When an aggregate label appears, emit a group_header to signal
        # budget_extractor to reset program context, set in_artsoversigt so
        # subsequent lines in the block are skipped.
        if line.strip().lower() in _ARTSOVERSIGT_LABELS:
            in_artsoversigt = True
            current_item_code = ""
            current_item_desc = ""
            result.append(BudgetLine(
                line_code="", description=line.strip(),
                raw_amount="", amount_value=0,
                section_code=section_code, section_name=section_name,
                line_type="group_header", original_line=line,
                context_before=before, context_after=after,
            ))
            continue

        # ── § section header ──────────────────────────────────────────────
        m = _RE_SECTION.match(line)
        if m:
            sec_num = re.sub(r"\s", "", m.group(1))  # strip spaces from "6 . 1" → "6.1"
            rest = _despace_ocr(m.group(2).rstrip(". "))
            tail = _extract_tail_amount(line)
            desc = rest
            if tail:
                am = _RE_AMOUNT_TAIL.match(line)
                if am:
                    desc = _despace_ocr(am.group(1).lstrip("§&8 0123456789.").strip())
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

            # Keep item context in sync: otherwise generic lines that follow
            # this code (e.g. "Driftsudgifter ... 109M") inherit the PREVIOUS
            # item's code instead of this one.
            current_item_code = code
            current_item_desc = desc2

            result.append(BudgetLine(
                line_code=code,
                description=desc2,
                raw_amount=tail2[0] if tail2 else "",
                amount_value=tail2[1] if tail2 else 0,
                section_code=section_code,
                section_name=section_name,
                line_type="subsection",
                original_line=line,
                item_code=code,
                item_description=desc2,
                context_before=before,
                context_after=after,
            ))
            continue

        # ── Group header (e.g. "09. Særlige forskningsinstitutioner") ──────────
        # Single-digit-group code: marks a top-level block inside a § ministry.
        # Reset item context so that the block's summary totals (Anlægsudgifter,
        # Tilskud, …) are not inherited by the next R&D program on a later page.
        # In mill_kr_mode, single-digit-group codes like "01. Description" are
        # sub-items (under a parent subsection), not section-level group headers.
        # We detect this by checking whether a parent item context is active.
        m_grp = _RE_GROUP_HEADER.match(line)
        if m_grp and not _extract_tail_amount(line):
            is_subitem_in_mill_mode = mill_kr_mode and current_item_code != ""
            if not is_subitem_in_mill_mode:
                current_item_code = ""
                current_item_desc = ""
                result.append(BudgetLine(
                    line_code=m_grp.group(1),
                    description=m_grp.group(2).strip(),
                    raw_amount="",
                    amount_value=0,
                    section_code=section_code,
                    section_name=section_name,
                    line_type="group_header",
                    original_line=line,
                    context_before=before,
                    context_after=after,
                ))
                continue
            # else: fall through to handle as sub-item below

        # ── Sub-item in 1991+ Mill. kr. format ───────────────────────────
        # "01. Departementets afdelinger" — two-digit code under active subsection.
        # Only active in mill_kr_mode when a parent subsection context is set.
        # Gives sub-items their own composite line_code (e.g. "20.22.01") so they
        # can be distinguished from bare text group labels in budget_extractor.
        if mill_kr_mode and current_item_code:
            m_sub = _RE_SUBITEM.match(line)
            if m_sub:
                sub_code = m_sub.group(1)
                sub_desc = m_sub.group(2).strip()
                full_code = f"{current_item_code}.{sub_code}"
                tail_sub = _extract_tail_amount(line)
                if tail_sub:
                    result.append(BudgetLine(
                        line_code=full_code,
                        description=sub_desc,
                        raw_amount=tail_sub[0],
                        amount_value=tail_sub[1],
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
                nla_sub = _next_line_amount()
                if nla_sub:
                    skip_next = True
                    result.append(BudgetLine(
                        line_code=full_code,
                        description=sub_desc,
                        raw_amount=nla_sub[0],
                        amount_value=nla_sub[1],
                        section_code=section_code,
                        section_name=section_name,
                        line_type="line_item",
                        original_line=line,
                        item_code=current_item_code,
                        item_description=current_item_desc,
                        context_before=before,
                        context_after=after,
                    ))
                else:
                    result.append(BudgetLine(
                        line_code=full_code,
                        description=sub_desc,
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

    return result, section_code, section_name, in_artsoversigt
