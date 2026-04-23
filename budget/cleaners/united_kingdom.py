"""
UK-specific post-extraction cleaner.

Audit findings (HM Treasury budgets / spending reviews 2010–2025, April 2026):

DOCUMENT TYPE: UK source files are a mix of:
  - Annual Supply Estimates (DEL tables, £ thousands) — amounts are reliable
  - Budget documents / spending reviews (narrative prose, £ billion/million)
    → amounts are MULTI-YEAR COMMITMENTS, not single-year appropriations
  - Autumn Statements / Spring Statements (similar to spending reviews)

UNIT: Budget narrative prose announces in £ billion / £ million. The LLM
  stores the raw amount in full GBP, labels it 'thousand'. The existing
  correction (divide by 1e6 for amounts >= £1M labeled 'thousand') handles
  this; we preserve it here.

KEY ISSUES IDENTIFIED:

1. Section/department totals in include:
   "Total resource DEL", "Total Capital DEL", "Total investment in science",
   "Total budget for R&D", "Total Resource DEL for UKRI" — these are
   ministry-wide aggregates, not individual programme grants.

2. Multi-year commitment narratives:
   Lines like "UKRI directing £9 billion over four years to IS-8 sectors" or
   "£1.6 billion of funding to support … over seven years" are political
   announcements from spending reviews, NOT annual budget appropriations.
   These massively inflate the year total if taken at face value.

3. Broad department totals:
   "Total budget for the Department for Business, Innovation and Skills",
   "Total budget for BEIS" — include non-R&D spending.

4. Non-R&D items from BEIS/HMT context:
   EV purchase subsidies, climate funds, biodiversity grants, tax instruments.

5. Duplicate entries: same programme announced in multiple documents
   (Budget + Spending Review for the same year).
"""

from __future__ import annotations

import re
import pandas as pd

__all__ = ["clean"]

# ---------------------------------------------------------------------------
# Multi-year spending commitment patterns (spending review prose, not annual)
# Lines containing these patterns are MULTI-YEAR TOTALS, not annual amounts.
# ---------------------------------------------------------------------------
_MULTI_YEAR_PATTERNS: list[re.Pattern] = [
    re.compile(r"\bover\s+(the\s+)?(next\s+)?(\d+|two|three|four|five|six|seven|eight|nine|ten)\s+years?\b", re.IGNORECASE),
    re.compile(r"\bfrom\s+\d{4}\s+to\s+\d{4}\b", re.IGNORECASE),
    re.compile(r"\b\d{4}[–-]\d{2,4}\b"),            # "2022-25", "2022–2025" etc.
    re.compile(r"\bover\s+the\s+(spending\s+review|SR)\s+period\b", re.IGNORECASE),
    re.compile(r"\bper\s+year\b", re.IGNORECASE),    # "£X million per year" — keep but flag
    re.compile(r"\bby\s+\d{4}\b", re.IGNORECASE),   # "by 2030" — future pledge, not annual
    re.compile(r"\bmulti[\s\-]year\b", re.IGNORECASE),  # "multi-year programme"
]

# Descriptions that START with "£X" are usually spending-review narratives
_STARTS_WITH_AMOUNT = re.compile(r"^\s*£\s*[\d,\.]+\s*(billion|million|bn|m)\b", re.IGNORECASE)

# ---------------------------------------------------------------------------
# Budget narrative prose patterns
# UK Budget documents contain policy announcements written as sentences.
# These are NOT formal DEL appropriation entries — they describe spending
# intentions, not voted amounts in Supply Estimates tables.
# Examples: "invest £100 million in science capital development"
#           "Investment in digital infrastructure"
#           "Over £1 billion of public and private investment..."
#           "Research Councils have invested in research capital"
# ---------------------------------------------------------------------------
_NARRATIVE_PATTERNS: list[re.Pattern] = [
    # Starts with a monetary verb — clear narrative announcement
    re.compile(r"^\s*invest\s", re.IGNORECASE),             # "invest £X in..."
    re.compile(r"^\s*investing\s", re.IGNORECASE),          # "investing £X to..."
    re.compile(r"^\s*Investment\s+(in|for|to|of)\b", re.IGNORECASE),  # "Investment in X"
    re.compile(r"^\s*Initial\s+investment\b", re.IGNORECASE),
    re.compile(r"^\s*Additional\s+investment\b", re.IGNORECASE),
    # "Over £X" / "Up to £X" prefix — policy pledge, not a DEL entry
    re.compile(r"^\s*Over\s+£", re.IGNORECASE),
    re.compile(r"^\s*Up\s+to\s+£", re.IGNORECASE),    # "Up to £121 million for Made Smarter..."
    # Government subject — full-sentence narrative ("The government will invest £X...")
    re.compile(r"^\s*The\s+government\s+(will|is|has|are)\b", re.IGNORECASE),
    re.compile(r"^\s*government\s+will\b", re.IGNORECASE),  # "government will allocate at least £X"
    re.compile(r"^\s*Budget\s+announces\b", re.IGNORECASE), # "Budget announces £X million in R&D"
    re.compile(r"Building\s+on\s+the\s+government", re.IGNORECASE),  # "X – Building on the government's..."
    re.compile(r"the\s+government\s+is\s+announcing\b", re.IGNORECASE),  # "...the government is announcing..."
    re.compile(r"the\s+government\s+is\s+(investing|confirming|committing)\b", re.IGNORECASE),
    re.compile(r"the\s+government\s+has\s+(asked|confirmed|committed)\b", re.IGNORECASE),
    re.compile(r"[–—]\s*[Tt]he\s+government\s+(will|is|has)\b"),  # "Programme – The government will..."
    re.compile(r"In\s+line\s+with\s+the\s+commitment\b", re.IGNORECASE),  # "In line with the commitment to..."
    # Narrative sentence openers
    re.compile(r"^\s*Support\s+for\s+.{0,60}(start.?up|compan|invest|innovat)", re.IGNORECASE),
    re.compile(r"\bhave\s+invested\b", re.IGNORECASE),      # "Research Councils have invested"
    re.compile(r"^\s*Challenge\s+Fund\s+for\b", re.IGNORECASE),
    re.compile(r"^\s*Funding\s+(to|for)\s+.{0,40}(develop|build|create|expand)", re.IGNORECASE),
    # Verb phrases as description openers — clearly narrative actions, not programme names
    re.compile(r"^\s*establish\s+(the\s+)?new\b", re.IGNORECASE),  # "establish the new ARIA"
    re.compile(r"^\s*commitment\s+to\s+(fund|support|provide|increase)\b", re.IGNORECASE),  # "commitment to fund/increase..."
    re.compile(r"^\s*fund\s+(an?\s+)?(increase|expansion|additional)\b", re.IGNORECASE),  # "fund an increase..."
    re.compile(r"^\s*Reinvesting\b", re.IGNORECASE),                # "Reinvesting up to £Xm from..."
    re.compile(r"\bincreasing\s+contract\s+value\b", re.IGNORECASE),  # "SBRI increasing contract value..."
    re.compile(r"\bto\s+support\s+the\s+Government'?s?\b", re.IGNORECASE),  # "Funding to support the Government's..."
    re.compile(r"^\s*Innovate\s+UK\s+will\b", re.IGNORECASE),       # "Innovate UK will launch a new..."
    # Multi-year strategy labels ("ten-year £X billion programme")
    re.compile(r"\b(ten|five|four|three|two|seven|eight|nine|six|10|5|4|3|7|8|9|6)\s*[–-]?\s*year\b.*£", re.IGNORECASE),
    # Regional narrative ("The Midlands will also receive...")
    re.compile(r"^\s*The\s+\w+\s+will\s+(also\s+)?(receive|get|benefit)\b", re.IGNORECASE),
]

# ---------------------------------------------------------------------------
# Broad department / aggregate total patterns
# ---------------------------------------------------------------------------
_DEPT_TOTAL_PATTERNS: list[re.Pattern] = [
    re.compile(r"^Total (resource|capital)\s+DEL\b", re.IGNORECASE),
    re.compile(r"\bTotal\s+Resource\s+DEL\b", re.IGNORECASE),
    re.compile(r"\bTotal\s+Capital\s+DEL\b", re.IGNORECASE),
    re.compile(r"^Total\s+Capital\s+Budget\s+DEL\b", re.IGNORECASE),    # "Total Capital Budget DEL"
    re.compile(r"^Total budget for the Department\b", re.IGNORECASE),
    re.compile(r"^Total budget for BEIS\b", re.IGNORECASE),
    re.compile(r"^Total budget for BIS\b", re.IGNORECASE),
    re.compile(r"^Total (investment|budget) in science\b", re.IGNORECASE),
    re.compile(r"^Total budget for (R&D|Research and Development|UK Research)\b", re.IGNORECASE),
    re.compile(r"\bTotal budget for UK Research and Innovation\b", re.IGNORECASE),
    # Bare DEL lines (component subtotals, not full programme descriptions)
    re.compile(r"^Capital DEL$", re.IGNORECASE),
    re.compile(r"^Resource DEL$", re.IGNORECASE),
    re.compile(r"^Total including\b", re.IGNORECASE),   # "Total including COVID-19" etc.
    # "of which:" lines are sub-components of a larger total — double-counting
    re.compile(r"^\s*of which\s*:", re.IGNORECASE),
    # Pure accounting/admin lines — not science commitments
    re.compile(r"\bUK\s+science\s+spend\s+as\s+a\s+%", re.IGNORECASE),   # ratio, not amount
    re.compile(r"^Departmental\s+Expenditure\s+Limits?\b", re.IGNORECASE),  # bare DEL header
    re.compile(r"^Total\s+Capital\s+DEL\s+expenditure\b", re.IGNORECASE),
    # Generic dept-name-only labels (no amount context)
    re.compile(r"^Science,?\s+Innovation\s+and\s+Technology$", re.IGNORECASE),  # bare dept name
    re.compile(r"^R&D\s+budget$", re.IGNORECASE),
    # NOTE: "Total UK science spending", "DTI Science Budget DEL", "Science budget allocation",
    # "Science budget" are intentionally NOT filtered — these are the headline government R&D
    # commitment figures that Budget documents announce, and are exactly what this database
    # is designed to capture.
]

# ---------------------------------------------------------------------------
# Non-R&D false positives from BEIS/HMT budget context
# ---------------------------------------------------------------------------
_NON_RD_SUBSTRINGS: list[tuple[str, str]] = [
    ("plug-in car grant",           "EV purchase subsidy — not R&D"),
    ("plug in car",                 "EV purchase subsidy — not R&D"),
    ("charging network",            "EV infrastructure deployment — not R&D"),
    ("nature for climate",          "Conservation fund — not R&D"),
    ("nature recovery network",     "Biodiversity conservation — not R&D"),
    ("darwin plus",                 "Overseas biodiversity conservation — not R&D"),
    ("biodiversity conservation",   "Biodiversity conservation — not R&D"),
    ("fly-tipping",                 "Waste enforcement — not R&D"),
    ("fly tipping",                 "Waste enforcement — not R&D"),
    ("natural environment impact",  "Environmental policy fund — not R&D"),
    ("climate change levy",         "Tax instrument — not R&D appropriation"),
    ("digital waste tracking",      "Operational IT system — not R&D"),
    ("brownfield land",             "Land remediation — not R&D"),
    ("cardiff parkway",             "Infrastructure — not R&D"),
    ("talent pipeline for the defence industry",  "Skills/training — not direct R&D"),
    ("techfirst digital skills",    "Skills training — not direct R&D"),
    # Energy/infrastructure (not R&D appropriations)
    ("carbon capture and storage infrastructure", "CCS infrastructure deployment — not R&D"),
    ("green heat network",          "Heat network infrastructure — not R&D"),
    ("heat network",                "Heat infrastructure — not R&D"),
    ("full-fibre broadband",        "Telecoms infrastructure — not R&D"),
    ("full fibre broadband",        "Telecoms infrastructure — not R&D"),
    ("5g testbed",                  "Telecoms infrastructure — not R&D"),
    ("low emission vehicle",        "EV deployment — not direct R&D"),
    ("low-emission vehicle",        "EV deployment — not direct R&D"),
    ("hydrogen hub",                "H2 infrastructure — not direct R&D"),
    ("satellite communications",    "Telecoms infrastructure — not R&D"),
    ("energy transition zone",      "Industrial transition support — not R&D"),
    ("north sea transition",        "Fossil fuel transition — not R&D"),
    ("floating offshore wind",      "Energy deployment — not R&D"),
    ("offshore wind",               "Energy deployment — not R&D"),
    ("biomass feedstock",           "Biomass energy deployment — not R&D"),
    ("energy storage prototype",    "Energy storage deployment — not R&D"),
    ("antiviral treatment",         "Health procurement — not R&D"),
    # Tax instruments — fiscal cost of reliefs, not budget appropriations
    ("r&d tax credit",              "Tax relief — fiscal cost, not a budget appropriation"),
    ("research and development tax credit", "Tax relief — fiscal cost, not a budget appropriation"),
    ("r&d expenditure credit",      "Tax relief instrument — not a budget appropriation"),
    ("above the line credit",       "Tax relief instrument — not a budget appropriation"),
    # Training programmes (not R&D)
    ("employer training pilot",     "Workforce training — not R&D"),
    ("employer training",           "Workforce training — not R&D"),
    ("skills bootcamp",             "Skills training — not R&D"),
    # NHS admin / clinical procurement (not R&D appropriations)
    ("nhs funding for",             "NHS service delivery — not direct R&D appropriation"),
    ("diagnostic testing",          "Health service delivery — not R&D"),
]

_GENERIC_PROGRAMME_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"^Science budget$", re.IGNORECASE), "broad headline budget label"),
    (re.compile(r"^Research and Development funding$", re.IGNORECASE), "generic funding label"),
    (re.compile(r"^Investment in research and development", re.IGNORECASE), "generic funding label"),
    (re.compile(r"^Funding for\b", re.IGNORECASE), "generic funding label"),
    (re.compile(r"^Investment in\b", re.IGNORECASE), "generic investment label"),
    (re.compile(r"^Additional spending in R&D$", re.IGNORECASE), "generic funding label"),
    (re.compile(r"^Long-term support for research and development$", re.IGNORECASE), "generic funding label"),
    (re.compile(r"^National Productivity Investment Fund", re.IGNORECASE), "cross-cutting investment fund"),
]

# ---------------------------------------------------------------------------
# Unit correction constants
# ---------------------------------------------------------------------------
_GBP_TO_MILLION = 1_000_000.0


def _is_dept_total(desc: str) -> bool:
    if not isinstance(desc, str):
        return False
    for pat in _DEPT_TOTAL_PATTERNS:
        if pat.search(desc.strip()):
            return True
    return False


def _is_multi_year(desc: str) -> bool:
    if not isinstance(desc, str):
        return False
    for pat in _MULTI_YEAR_PATTERNS:
        if pat.search(desc):
            return True
    if _STARTS_WITH_AMOUNT.match(desc):
        return True
    return False


def _is_non_rd(desc: str) -> tuple[bool, str]:
    if not isinstance(desc, str):
        return False, ""
    d = desc.lower()
    for substring, reason in _NON_RD_SUBSTRINGS:
        if substring in d:
            return True, reason
    return False, ""


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply UK-specific corrections. Returns cleaned copy."""
    df = df.copy()

    if "cleaning_notes" not in df.columns:
        df["cleaning_notes"] = ""
    if "aggregation_role" not in df.columns:
        df["aggregation_role"] = ""

    # ------------------------------------------------------------------
    # 1. Unit correction: amounts stored as raw GBP labeled 'thousand'.
    #    Divide by 1e6 to get millions GBP.
    # ------------------------------------------------------------------
    needs_fix = (
        (df["currency"].str.upper().str.strip() == "GBP")
        & (df["unit"].str.lower().str.strip() == "thousand")
        & df["amount_local"].notna()
        & (df["amount_local"] >= 1_000_000)
    )
    if needs_fix.any():
        df.loc[needs_fix, "amount_local"] = df.loc[needs_fix, "amount_local"] / _GBP_TO_MILLION
        df.loc[needs_fix, "unit"] = "million"
        df.loc[needs_fix, "cleaning_notes"] += "[unit corrected: raw GBP → million GBP]"

    # ------------------------------------------------------------------
    # 2. Force section_total item_type rows to review/redundant.
    # ------------------------------------------------------------------
    if "item_type" in df.columns:
        st_mask = df["item_type"] == "section_total"
        if st_mask.any():
            df.loc[st_mask, "aggregation_role"] = "redundant"
            df.loc[st_mask, "decision"] = "review"
            df.loc[st_mask, "cleaning_notes"] += "[section_total: aggregate, not individual programme]"

    # ------------------------------------------------------------------
    # 3. Mark broad department / DEL totals as redundant.
    # ------------------------------------------------------------------
    dept_mask = df["line_description"].apply(_is_dept_total)
    if dept_mask.any():
        df.loc[dept_mask, "aggregation_role"] = "redundant"
        df.loc[dept_mask, "decision"] = "review"
        df.loc[dept_mask, "cleaning_notes"] += "[dept_total: ministry-wide aggregate, includes non-R&D]"

    # ------------------------------------------------------------------
    # 3b. Mark recurring generic funding labels that should not become
    #     canonical UK "agencies" in the final series.
    # ------------------------------------------------------------------
    for pat, reason in _GENERIC_PROGRAMME_PATTERNS:
        mask = df["line_description"].apply(
            lambda d: bool(isinstance(d, str) and pat.search(d.strip()))
        )
        if mask.any():
            df.loc[mask, "aggregation_role"] = "redundant"
            df.loc[mask, "decision"] = "review"
            df.loc[mask, "cleaning_notes"] += f"[generic_programme: {reason}]"

    # ------------------------------------------------------------------
    # 4. Flag multi-year spending review commitments.
    #    These are policy announcements (total over multiple years),
    #    not single-year budget appropriations.
    # ------------------------------------------------------------------
    multi_mask = df["line_description"].apply(_is_multi_year)
    if multi_mask.any():
        df.loc[multi_mask, "aggregation_role"] = "redundant"
        df.loc[multi_mask, "decision"] = "review"
        df.loc[multi_mask, "cleaning_notes"] += "[multi_year_commitment: total over multiple years, not annual appropriation]"

    # ------------------------------------------------------------------
    # 5. Flag Budget narrative prose lines.
    #    UK Budget documents contain policy announcements written as
    #    sentences ("invest £100M in science capital"). These are not
    #    formal DEL appropriation entries from Supply Estimates tables.
    # ------------------------------------------------------------------
    def _is_narrative(desc: str) -> bool:
        if not isinstance(desc, str):
            return False
        for pat in _NARRATIVE_PATTERNS:
            if pat.search(desc):
                return True
        return False

    narr_mask = df["line_description"].apply(_is_narrative)
    if narr_mask.any():
        df.loc[narr_mask, "aggregation_role"] = "redundant"
        df.loc[narr_mask, "decision"] = "review"
        df.loc[narr_mask, "cleaning_notes"] += (
            "[narrative_prose: Budget announcement text, not a DEL appropriation entry]"
        )

    # ------------------------------------------------------------------
    # 6. Mark non-R&D false positives as review.
    # ------------------------------------------------------------------
    for idx, row in df.iterrows():
        desc = str(row.get("line_description", ""))
        is_fp, reason = _is_non_rd(desc)
        if is_fp and row.get("decision") == "include":
            df.at[idx, "decision"] = "review"
            df.at[idx, "confidence"] = 0.2
            df.at[idx, "rd_category"] = "unclear"
            df.at[idx, "cleaning_notes"] = str(df.at[idx, "cleaning_notes"]) + f"[non-R&D: {reason}]"

    # ------------------------------------------------------------------
    # 7. Deduplicate exact (year, description, amount) matches.
    # ------------------------------------------------------------------
    if len(df) > 1 and "amount_local" in df.columns:
        dup_mask = df.duplicated(
            subset=["year", "line_description", "amount_local"], keep="first"
        )
        if dup_mask.any():
            df.loc[dup_mask, "aggregation_role"] = "redundant"
            df.loc[dup_mask, "decision"] = "review"
            df.loc[dup_mask, "cleaning_notes"] += "[duplicate: same year/description/amount]"

    # ------------------------------------------------------------------
    # 8. Within-file dedup: same (year, source_file, amount) pairs.
    #
    #    UK Budget PDFs describe each measure TWICE in the same document:
    #    once as a short name ("Birmingham STEAMhouse £14M") and again as
    #    a full sentence ("The government will invest £14 million in
    #    STEAMhouse..."). Both pass as include because the sentence form
    #    wasn't caught by narrative patterns.
    #
    #    Approach: work only on include rows. Sort by description length
    #    (shorter = canonical), then mark the longer duplicate as redundant.
    #    Guard: amounts >= £1M only (avoid coincidental small-amount matches).
    # ------------------------------------------------------------------
    if len(df) > 1 and "amount_local" in df.columns:
        # Isolate include rows for dedup — leave review/skip rows untouched
        inc_mask = df["decision"] == "include"
        large_mask = df["amount_local"].fillna(0) >= 1_000   # >= £1M in thousands
        candidates = df[inc_mask & large_mask].copy()

        if len(candidates) > 1:
            candidates["_desc_len"] = candidates["line_description"].str.len().fillna(999)
            # Sort: shorter description first → that's the canonical short name
            candidates = candidates.sort_values("_desc_len", kind="stable")
            dup_mask_inner = candidates.duplicated(
                subset=["year", "source_file", "amount_local"], keep="first"
            )
            dup_idx = candidates[dup_mask_inner].index

            if len(dup_idx) > 0:
                df.loc[dup_idx, "aggregation_role"] = "redundant"
                df.loc[dup_idx, "decision"] = "review"
                df.loc[dup_idx, "cleaning_notes"] += (
                    "[duplicate: same year/file/amount — Budget doc describes measure twice]"
                )

    # ------------------------------------------------------------------
    # 9. Year-gated programme filter.
    #
    #    Some well-known UK R&D funds are referenced by name in documents
    #    from BEFORE they were created — the LLM projects known fund names
    #    onto unrelated text.  Filter entries that appear in years when the
    #    programme did not yet exist.
    #
    #    ISCF (Industrial Strategy Challenge Fund): announced Oct 2017.
    #    Strength in Places Fund: announced Nov 2017, launched 2018.
    # ------------------------------------------------------------------
    _YEAR_GATED: list[tuple[re.Pattern, int, str]] = [
        # (pattern, first_valid_year, reason)
        (re.compile(r"industrial strategy challenge fund", re.IGNORECASE), 2017,
         "ISCF did not exist before 2017 — hallucinated fund name"),
        (re.compile(r"strength in places fund", re.IGNORECASE), 2018,
         "Strength in Places Fund did not exist before 2018"),
        (re.compile(r"advanced research and invention agency|ARIA\b", re.IGNORECASE), 2021,
         "ARIA did not exist before 2021"),
        (re.compile(r"UK Research and Innovation|UKRI\b", re.IGNORECASE), 2018,
         "UKRI did not exist before 2018"),
        (re.compile(r"faraday institution|faraday battery challenge", re.IGNORECASE), 2017,
         "Faraday Institution/Battery Challenge launched 2017"),
    ]

    if "year" in df.columns:
        for pat, first_year, reason in _YEAR_GATED:
            desc_match = df["line_description"].apply(
                lambda d: bool(isinstance(d, str) and pat.search(d))
            )
            year_too_early = df["year"].apply(
                lambda y: isinstance(y, (int, float)) and int(y) < first_year
            )
            anachronism_mask = desc_match & year_too_early & (df["decision"] == "include")
            if anachronism_mask.any():
                df.loc[anachronism_mask, "decision"] = "review"
                df.loc[anachronism_mask, "aggregation_role"] = "redundant"
                df.loc[anachronism_mask, "cleaning_notes"] += (
                    f"[anachronism: {reason}]"
                )

    # ------------------------------------------------------------------
    # 10. Repeated-description dedup within same (year, source_file).
    #
    #     UK Budget PDFs contain multiple chapters/tables that all use the
    #     same generic label ("Research and Development funding", "Science
    #     budget") for different numbers.  When the same description appears
    #     3+ times in the same document-year with different amounts, ALL
    #     occurrences are marked review — the generic label offers no
    #     disambiguation and the amounts can't be independently verified.
    #
    #     Guard: only affects include rows with amounts >= £1M (1000 in
    #     thousands), so legitimate small programmes with shared names are
    #     not touched.
    # ------------------------------------------------------------------
    if len(df) > 1 and "amount_local" in df.columns:
        inc_large = (df["decision"] == "include") & (df["amount_local"].fillna(0) >= 1_000)
        counts = (
            df[inc_large]
            .groupby(["year", "source_file", "line_description"])
            .size()
        )
        repeated = counts[counts >= 3].reset_index()[["year", "source_file", "line_description"]]
        if len(repeated) > 0:
            repeated["_key"] = list(zip(repeated["year"], repeated["source_file"], repeated["line_description"]))
            repeated_keys = set(repeated["_key"])
            rep_mask = df.apply(
                lambda r: (r["decision"] == "include"
                           and inc_large.loc[r.name]
                           and (r["year"], r["source_file"], r["line_description"]) in repeated_keys),
                axis=1,
            )
            if rep_mask.any():
                df.loc[rep_mask, "decision"] = "review"
                df.loc[rep_mask, "aggregation_role"] = "redundant"
                df.loc[rep_mask, "cleaning_notes"] += (
                    "[repeated_generic_label: same description ≥3× in same doc — ambiguous]"
                )

    return df.reset_index(drop=True)
