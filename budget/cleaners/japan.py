"""
Japan-specific post-extraction cleaner.

Audit findings (2019–2020, April 2026):

UNIT: The country profile instructs the LLM to return amounts exactly as
printed (千円 = thousands of yen), unit='thousand'. This is numerically
correct — dedup.normalise_units() converts to a common base.
NOTE: 2019-2020 data was manually corrected to unit='million' before this
cleaner existed; those rows have unit='million' and are left unchanged.
No automatic unit conversion applied here.

DUPLICATES: Each agency appears twice per year:
  - 運営費 (operating budget line in ministry)
  - 運営費交付金 (operating grant disbursed to agency)
Same amount, different line name. Keep 交付金 (disbursement side), drop 運営費.

MINISTRY TOTALS: 経済産業省所管合計, 文部科学本省計, 合計 etc. are
broad aggregates that include non-R&D spending. Mark as aggregation_role='redundant'.

ZERO AMOUNTS: OCR failures — drop.
"""

from __future__ import annotations

import re
import pandas as pd

__all__ = ["clean"]

# ---------------------------------------------------------------------------
# Ministry-level totals to mark redundant (too broad for R&D time series).
# These are 合計 / 所管合計 / 本省計 rows at ministry level.
# ---------------------------------------------------------------------------
_TOTAL_DESC_PATTERNS: list[re.Pattern] = [
    # Japanese patterns (2019-2020 data — line_description is Japanese text)
    re.compile(r"所管合計"),          # e.g. 経済産業省所管合計
    re.compile(r"本省計"),             # e.g. 文部科学本省計
    re.compile(r"^合計$"),             # bare 合計 (total)
    re.compile(r"^小計$"),             # 小計 (subtotal)
    re.compile(r"教育振興助成費"),      # Education Promotion Assistance Expenses — ministry-wide aggregate
    # English patterns (2016-2018 — LLM returns English in line_description_en)
    re.compile(r"^Total\b", re.IGNORECASE),               # "Total" bare or "Total for Ministry..."
    re.compile(r"\bPromotion\s+Assistance\s+Expenses\b", re.IGNORECASE),  # Education Promotion Assistance
    re.compile(r"^Common\s+expenses\s+of\s+the\s+Ministry\b", re.IGNORECASE),
    re.compile(r"\bMinistry\s+jurisdiction\s+total\b", re.IGNORECASE),
    re.compile(r"\bFY\s+\d{4}\s+Budget\s+of\s+the\s+Ministry\b", re.IGNORECASE),
    re.compile(r"\bExpenses\s+necessary\s+for\s+the\s+operating\s+expenses\s+of\s+national\s+university\b", re.IGNORECASE),
    # Broad programme-category labels (appear at section level as S&T budget buckets,
    # not as individual agency grants — explicit agency grants use the agency name)
    re.compile(r"^Science\s+and\s+Technology\s+Promotion(\s+Expenses)?$", re.IGNORECASE),   # 科学技術振興費/科学技術振興 MEXT bucket
    re.compile(r"^Operating\s+Expenses?\s+(Subsidy\s+for\s+|for\s+)?National\s+University\s+Corporations?$", re.IGNORECASE),  # all-universities aggregate
    re.compile(r"^Research\s+Promotion(\s+Expenses)?$", re.IGNORECASE),   # 研究振興費 sub-programme bucket
    # Bureau/policy office totals — not individual agency grants
    re.compile(r"\bIndustrial\s+Science\s+and\s+Technology\s+Policy\s+Bureau\b", re.IGNORECASE),  # 産業技術環境局 — METI bureau total
    re.compile(r"\bEducation\s+and\s+Science\s+Promotion(\s+Expenses)?\b", re.IGNORECASE),   # 文教及び科学振興費 — broad MEXT bucket
    re.compile(r"\bjurisdiction\s+total\b", re.IGNORECASE),   # any "X jurisdiction total"
    re.compile(r"\bMinistry\s+subtotal\b", re.IGNORECASE),    # 本省計 English form
    re.compile(r"^Cabinet\s+Office\s+Total\b", re.IGNORECASE),
    # Japanese patterns for bureau/policy totals
    re.compile(r"産業技術環境局"),     # METI Industrial S&T Policy Bureau
    re.compile(r"文教及び科学振興費"), # Education & Science Promotion budget bucket
    re.compile(r"科学技術振興費"),     # Science & Technology Promotion — broad MEXT bucket
    re.compile(r"研究振興費"),         # Research Promotion — sub-bucket of MEXT
    # Fiscal-year total labels — these are section totals labeled with the budget year
    re.compile(r"^(令和|平成)\d+年度予算額$"),   # "FY20XX budget amount" — section total
    re.compile(r"^(令和|平成)\d+年度当初予算額$"), # "FY20XX initial budget"
    re.compile(r"^FY\s*\d{4}\s*(Budget|Approved\s+Budget)$", re.IGNORECASE),
    # Generic R&D label used as section total (not a specific programme line)
    re.compile(r"^研究開発費$"),   # bare "R&D expenses" — too generic, usually a section sub-total
]


# Broad section names that indicate a budget bucket (not an agency grant)
# Applied when line_description is empty/NaN
_BROAD_SECTION_PATTERNS: list[re.Pattern] = [
    re.compile(r"科学技術振興費"),     # Science & Technology Promotion bucket
    re.compile(r"研究振興費"),         # Research Promotion bucket
    re.compile(r"文教及び科学振興費"), # Education & Science Promotion bucket
    re.compile(r"所管合計"),           # Ministry jurisdiction total
    re.compile(r"本省計"),             # Ministry subtotal
    re.compile(r"^合計$"),
]

_GENERIC_LABEL_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"^Research and Development( Promotion| Expenses)?$", re.IGNORECASE), "generic English budget bucket"),
    (re.compile(r"^Promotion of Science and Technology and Academic Policy$", re.IGNORECASE), "generic policy bucket"),
    (re.compile(r"^Expenses (necessary for )?promoting science and technology innovation$", re.IGNORECASE), "generic innovation bucket"),
    (re.compile(r"^Research and Testing Expenses$", re.IGNORECASE), "generic spending bucket"),
    (re.compile(r"^研究開発(推進|費)?$"), "generic Japanese budget bucket"),
]


def _is_broad_section(section_name: str) -> bool:
    if not isinstance(section_name, str) or not section_name.strip():
        return False
    for pat in _BROAD_SECTION_PATTERNS:
        if pat.search(section_name):
            return True
    return False


def _is_ministry_total(desc_jp: str, desc_en: str = "") -> bool:
    """True if either the Japanese or English description is a broad ministry-level aggregate."""
    for text in (desc_jp, desc_en):
        if not isinstance(text, str) or not text.strip():
            continue
        for pat in _TOTAL_DESC_PATTERNS:
            if pat.search(text):
                return True
    return False


# ---------------------------------------------------------------------------
# 運営費 / 運営費交付金 dedup helpers
# ---------------------------------------------------------------------------
_RE_UNEIHI = re.compile(r"運営費$")          # ends with 運営費 (budget line)
_RE_KOUFU  = re.compile(r"運営費交付金")      # contains 運営費交付金 (grant)


def _base_agency_name(desc: str) -> str:
    """Strip 運営費 / 運営費交付金 suffix and normalise common prefixes."""
    if not isinstance(desc, str):
        return desc
    s = desc.strip()
    # Strip 交付金 and anything trailing (e.g. "...交付金に必要な経費")
    s = re.sub(r"運営費交付金.*", "", s)
    s = re.sub(r"運営費$", "", s)
    # Normalise 国立研究開発法人X → X (so both forms key to the same agency)
    s = re.sub(r"^国立研究開発法人", "", s)
    s = re.sub(r"^独立行政法人", "", s)
    return s.strip()


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Japan-specific corrections. Returns cleaned copy."""
    df = df.copy()

    if "cleaning_notes" not in df.columns:
        df["cleaning_notes"] = ""

    # ------------------------------------------------------------------
    # 1. Drop zero-amount rows (OCR failures)
    # ------------------------------------------------------------------
    zero_mask = df["amount_local"].notna() & (df["amount_local"] == 0.0)
    if zero_mask.any():
        df = df[~zero_mask].copy()

    # ------------------------------------------------------------------
    # 3. Mark ministry-level totals as aggregation_role='redundant'
    #    (too broad: include non-R&D ministry spend)
    # ------------------------------------------------------------------
    if "aggregation_role" not in df.columns:
        df["aggregation_role"] = ""

    desc_en = df.get("line_description_en", pd.Series("", index=df.index)).fillna("")
    total_mask = pd.Series(
        [_is_ministry_total(str(jp), str(en))
         for jp, en in zip(df["line_description"], desc_en)],
        index=df.index,
    )
    if total_mask.any():
        df.loc[total_mask, "aggregation_role"] = "redundant"
        df.loc[total_mask, "decision"] = "review"
        df.loc[total_mask, "cleaning_notes"] = (
            df.loc[total_mask, "cleaning_notes"]
            + "[ministry total: too broad for R&D time series]"
        )

    # ------------------------------------------------------------------
    # 3a. Mark recurring generic labels that still slip through the broad
    #     section/ministry-total rules.
    # ------------------------------------------------------------------
    for pat, reason in _GENERIC_LABEL_PATTERNS:
        mask = df["line_description"].apply(
            lambda d: bool(isinstance(d, str) and pat.search(d.strip()))
        )
        if "line_description_en" in df.columns:
            mask = mask | df["line_description_en"].apply(
                lambda d: bool(isinstance(d, str) and pat.search(d.strip()))
            )
        if mask.any():
            df.loc[mask, "aggregation_role"] = "redundant"
            df.loc[mask, "decision"] = "review"
            df.loc[mask, "cleaning_notes"] = (
                df.loc[mask, "cleaning_notes"]
                + f"[generic_label: {reason}]"
            )

    # ------------------------------------------------------------------
    # 3b. Mark rows where line_description is empty/NaN and section_name
    #     is a broad budget bucket (not a specific agency).
    # ------------------------------------------------------------------
    empty_desc = df["line_description"].isna() | (df["line_description"].astype(str).str.strip() == "")
    section_col = df.get("section_name", pd.Series("", index=df.index)).fillna("")
    broad_section_mask = empty_desc & section_col.apply(_is_broad_section)
    if broad_section_mask.any():
        df.loc[broad_section_mask, "aggregation_role"] = "redundant"
        df.loc[broad_section_mask, "decision"] = "review"
        df.loc[broad_section_mask, "cleaning_notes"] = (
            df.loc[broad_section_mask, "cleaning_notes"]
            + "[broad_section: budget bucket, not specific agency grant]"
        )

    # ------------------------------------------------------------------
    # 4a. Force section_total item_type rows to decision='review'
    #     The pipeline sometimes classifies section totals as 'include' when
    #     the description is R&D-relevant. But section totals are aggregates
    #     that double-count their children.
    # ------------------------------------------------------------------
    if "item_type" in df.columns:
        st_mask = df["item_type"] == "section_total"
        if st_mask.any():
            df.loc[st_mask, "decision"] = "review"
            df.loc[st_mask, "aggregation_role"] = "redundant"
            df.loc[st_mask, "cleaning_notes"] = (
                df.loc[st_mask, "cleaning_notes"]
                + "[section_total: aggregate, not individual agency]"
            )

    # ------------------------------------------------------------------
    # 4b. Deduplicate same-amount rows within a year.
    #
    #     Japan budgets list each agency's appropriation twice:
    #       (a) Under the parent ministry (文部科学省) as 運営費 or 交付金
    #       (b) Under the agency's own section (同機構運営費交付金)
    #     Both forms carry the same yen amount but with different description
    #     text, so simple (year, description, amount) dedup misses them.
    #
    #     Strategy: sort by item_type priority (line_item first), then
    #     deduplicate by (year, amount_local). The ministry-section entry
    #     (often program_total) is marked redundant; the agency-section
    #     line_item is kept as the canonical row.
    #
    #     Guard: only apply to amounts > 1 billion yen (1,000,000 thousand)
    #     to avoid discarding genuinely coincident small amounts.
    # ------------------------------------------------------------------
    if len(df) > 1 and "amount_local" in df.columns:
        # Pass A: exact (year, description, amount) — catches translation pairs
        dup_mask_a = df.duplicated(
            subset=["year", "line_description", "amount_local"], keep="first"
        )
        if dup_mask_a.any():
            df.loc[dup_mask_a, "decision"] = "review"
            df.loc[dup_mask_a, "aggregation_role"] = "redundant"
            df.loc[dup_mask_a, "cleaning_notes"] = (
                df.loc[dup_mask_a, "cleaning_notes"]
                + "[duplicate: same year/description/amount]"
            )

        # Pass B: (year, amount) for large amounts — catches ministry vs agency section pairs.
        # Sort so line_item comes before program_total (keep the more specific row).
        _type_rank = {"line_item": 0, "program_total": 1, "section_total": 2}
        if "item_type" in df.columns:
            df["_rank"] = df["item_type"].map(lambda t: _type_rank.get(str(t), 1))
        else:
            df["_rank"] = 1
        df = df.sort_values("_rank", kind="stable").reset_index(drop=True)

        large_mask = df["amount_local"].fillna(0) >= 1_000_000  # >= 1B yen (in thousands)
        already_review = df["decision"] == "review"
        dup_mask_b = (
            df.duplicated(subset=["year", "amount_local"], keep="first")
            & large_mask
            & ~already_review
        )
        if dup_mask_b.any():
            df.loc[dup_mask_b, "decision"] = "review"
            df.loc[dup_mask_b, "aggregation_role"] = "redundant"
            df.loc[dup_mask_b, "cleaning_notes"] = (
                df.loc[dup_mask_b, "cleaning_notes"]
                + "[duplicate: same year/amount — ministry section vs agency section]"
            )
        df = df.drop(columns=["_rank"])

    # ------------------------------------------------------------------
    # 5. Deduplicate 運営費 / 運営費交付金 pairs
    #    For each (year, section_name, agency_base, amount), if both
    #    運営費 and 運営費交付金 exist, drop the 運営費 row (keep 交付金).
    # ------------------------------------------------------------------
    if len(df) == 0:
        return df

    uneihi_mask = df["line_description"].apply(
        lambda d: bool(isinstance(d, str) and _RE_UNEIHI.search(d)
                       and not _RE_KOUFU.search(d))
    )
    koufu_mask = df["line_description"].apply(
        lambda d: bool(isinstance(d, str) and _RE_KOUFU.search(d))
    )

    if uneihi_mask.any() and koufu_mask.any():
        # Build a set of (year, agency_base, amount) for 交付金 rows.
        # Note: section_name is intentionally excluded — the same agency can appear
        # under both its own named section and its parent ministry section, so
        # keying by section would miss valid duplicates.
        koufu_keys: set[tuple] = set()
        for _, row in df[koufu_mask].iterrows():
            base = _base_agency_name(row["line_description"])
            key = (row.get("year"), base,
                   round(float(row["amount_local"]), 0) if pd.notna(row["amount_local"]) else None)
            koufu_keys.add(key)

        drop_indices = []
        for idx, row in df[uneihi_mask].iterrows():
            base = _base_agency_name(row["line_description"])
            key = (row.get("year"), base,
                   round(float(row["amount_local"]), 0) if pd.notna(row["amount_local"]) else None)
            if key in koufu_keys:
                drop_indices.append(idx)

        if drop_indices:
            df = df.drop(index=drop_indices).copy()

    return df.reset_index(drop=True)
