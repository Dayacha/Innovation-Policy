"""Lightweight AI client abstraction for post-processing validation batches.

Architecture
------------
Three distinct prompt modes are used, each targeting a different task:

1. INCLUDE mode  (build_messages_include)
   Records already classified as "include" by the taxonomy.
   AI task: validate the amount, classify by Frascati type, flag double-counting risk.
   NOT re-deciding R&D status — the taxonomy already decided that.

2. REVIEW mode  (build_messages_review)
   Borderline records the taxonomy scored 1–2 (insufficient for automatic include).
   AI task: make a binary include/exclude decision based strictly on OECD taxonomy
   definitions provided in the prompt. Must give an explicit rationale.

3. AGGREGATION mode  (build_aggregation_messages)
   One call per (country, year) after individual records are validated.
   AI task: detect double-counting across records, produce a total R&D estimate.

4. ANOMALY mode  (build_anomaly_messages)
   One call per country after the full time series is assembled.
   AI task: flag year-over-year anomalies (unit errors, spikes, gaps).

All prompts share:
- Taxonomy grounding loaded from search_library.json (OECD researchers' definitions)
- Strict anti-hallucination rules (never invent data not present in the input)
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - optional dependency
    OpenAI = None  # type: ignore


class MissingAPIKeyError(RuntimeError):
    """Raised when the API key is missing."""


class MissingOpenAIDependencyError(RuntimeError):
    """Raised when openai package is not installed."""


@dataclass
class AIClientConfig:
    model: str = "gpt-4o-mini"
    temperature: float = 0
    max_output_tokens: int = 2000
    api_key_env: str = "OPENAI_API_KEY"


# ── Taxonomy grounding ────────────────────────────────────────────────────────

def _load_taxonomy_grounding() -> dict:
    """Load the full OECD taxonomy from search_library.json.

    Returns a structured dict with keyword lists AND decision rules so the AI
    uses the researchers' own definitions — not its generic intuition.
    """
    json_file = (
        Path(__file__).resolve().parent.parent
        / "Data" / "input" / "taxonomy" / "search_library.json"
    )
    try:
        data = json.loads(json_file.read_text(encoding="utf-8"))

        # Ambiguous section: "terms" is a dict {term: {require_anchor, ...}}
        raw_ambiguous = data.get("ambiguous", {}).get("terms", {})
        if isinstance(raw_ambiguous, dict):
            ambiguous_terms = list(raw_ambiguous.keys())[:20]
        elif isinstance(raw_ambiguous, list):
            ambiguous_terms = []
            for kw in raw_ambiguous[:20]:
                if isinstance(kw, dict):
                    ambiguous_terms.append(str(kw.get("term", kw.get("keyword", ""))))
                else:
                    ambiguous_terms.append(str(kw))
        else:
            ambiguous_terms = []
        ambiguous_terms = [t for t in ambiguous_terms if t]

        # Activity lens codes (K1-K8) for GBARD classification
        activity_lenses = {}
        for code, lens in data.get("activity_lens", {}).get("lenses", {}).items():
            activity_lenses[code] = {
                "class": lens.get("class"),
                "keywords": lens.get("keywords", [])[:6],
                "notes": lens.get("notes", ""),
            }

        # Decision rules from the taxonomy
        decision_rules = data.get("decision_rules", {})

        return {
            "core_rd": data.get("auto_include", {}).get("keywords", []),
            "institutions": data.get("institutions", {}).get("keywords", []),
            "sectoral_rd": data.get("sectoral_rd", {}).get("keywords", []),
            "budget_terms": data.get("budget_terms", {}).get("keywords", []),
            "exclusions": data.get("exclusions", {}).get("keywords", []),
            "ambiguous": ambiguous_terms,
            "decision_rules": decision_rules,
            "activity_lenses": activity_lenses,
        }
    except Exception:
        return {
            "core_rd": ["research and development", "R&D", "scientific research",
                        "basic research", "applied research", "experimental development"],
            "institutions": ["research council", "national laboratory", "research institute"],
            "sectoral_rd": [],
            "budget_terms": [],
            "exclusions": ["market research", "regional development", "vocational training"],
            "ambiguous": ["development", "programme", "innovation"],
            "decision_rules": {},
            "activity_lenses": {},
        }


_TAXONOMY = _load_taxonomy_grounding()


# ── Shared prompt blocks ──────────────────────────────────────────────────────

_ANTI_HALLUCINATION_RULES = """\
STRICT RULES — violations will invalidate results:
1. Use ONLY data present in the record fields provided. Never invent program codes,
   agency names, amounts, years, descriptions, or classifications.
2. If a field is missing or ambiguous, return null for that field — do not guess.
3. Do not use general world knowledge to fill in amounts or names.
   Your sole source of truth is the record fields you receive.
4. validated_amount_local MUST equal amount_local from the input UNLESS the budget
   context window (previous_lines / next_lines / neighbor_amounts) explicitly shows
   the line is a sub-total, aggregate, or that the unit header says "$'000" or similar.
5. If you cannot make a confident determination, set ai_decision to "review" and
   describe the uncertainty clearly in ai_rationale. Never force a decision.
6. Do NOT wrap your JSON in markdown fences or add any prose outside the JSON."""

def _build_taxonomy_grounding() -> str:
    """Build the taxonomy grounding block from the loaded JSON taxonomy."""
    t = _TAXONOMY
    dr = t.get("decision_rules", {})

    # Summarise decision rules from the taxonomy JSON
    auto_include_rules = "; ".join(
        r.get("rule", "") for r in dr.get("auto_include", [])
    )
    exclude_rules = "; ".join(
        r.get("rule", "") for r in dr.get("exclude", [])
    )
    review_rules = "; ".join(
        r.get("rule", "") for r in dr.get("review_needed", [])
    )

    # Activity lens codes
    lens_lines = []
    for code, lens in t.get("activity_lenses", {}).items():
        kws = ", ".join(lens.get("keywords", [])[:4])
        lens_lines.append(f"  {code} {lens.get('class','')}: {kws}")
    lens_block = "\n".join(lens_lines) if lens_lines else "  (see Frascati Manual Ch. 12)"

    return f"""\
OECD TAXONOMY (from project search_library.json) — base ALL decisions on this:

HIGH-CONFIDENCE R&D keywords (auto-include when present):
{", ".join(t["core_rd"][:35])}

KNOWN R&D INSTITUTION keywords (funding these = R&D):
{", ".join(t["institutions"][:25])}

SECTOR-SPECIFIC R&D keywords:
{", ".join(t["sectoral_rd"][:20]) if t["sectoral_rd"] else "(see core_rd)"}

BUDGET INSTRUMENT keywords (grants, appropriations, transfers to R&D bodies):
{", ".join(t["budget_terms"][:15]) if t.get("budget_terms") else "(appropriation, grant, transfer, allocation)"}

EXCLUSION keywords — these are DEFINITIVELY NOT R&D:
{", ".join(t["exclusions"][:25])}

AMBIGUOUS terms — only count as R&D when an explicit R&D anchor appears nearby:
{", ".join(t["ambiguous"][:20])}

DECISION RULES (from taxonomy):
- AUTO-INCLUDE when: {auto_include_rules or 'a core R&D phrase or institution keyword + budget instrument appears'}
- EXCLUDE when: {exclude_rules or 'exclusion keyword present and no R&D override'}
- REVIEW when uncertain: {review_rules or 'only ambiguous terms present with no R&D anchor'}

ACTIVITY LENS (GBARD R&D stage codes — use for frascati_type):
{lens_block}

FRASCATI BUDGET TYPES (map to frascati_type field):
- intramural_rd       : R&D performed inside a government agency or national lab
- extramural_grants   : Grants to universities / firms / research institutes for R&D
- rd_coordination     : Research councils, science academies allocating R&D funds
- rd_infrastructure   : Large research facilities, observatories, supercomputing, databases
- higher_ed_rd        : Block grants to universities where R&D share is inseparable from teaching
- not_rd              : Education, admin, social transfers, infrastructure maintenance, market research"""


_TAXONOMY_GROUNDING = _build_taxonomy_grounding()

_OUTPUT_SCHEMA_INCLUDE = """\
Required JSON keys per record (include mode):
  record_id                    : string  — echo back unchanged
  keep                         : bool    — true if this is a valid R&D budget line
  clean_program_code           : string | null
  clean_program_description_da : string | null — cleaned original-language description
  clean_program_description_en : string | null — English translation of cleaned description
  clean_budget_type_da         : string | null
  clean_budget_type_en         : string | null
  validated_amount_local       : number | null — corrected amount (see Rule 4 above)
  currency                     : string | null
  frascati_type                : one of [intramural_rd, extramural_grants, rd_coordination,
                                          rd_infrastructure, higher_ed_rd, not_rd]
  ai_rd_category               : one of [direct_rd, innovation_system, possible_rd, not_rd]
  ai_pillar                    : one of [Direct R&D, Innovation, Ambiguous, Exclude]
  ai_confidence                : float 0–1
  ai_decision                  : one of [include, review, exclude]
  ai_rationale                 : string — one sentence citing which taxonomy term matched
  parse_issue                  : one of [none, legal_reference_noise, merged_adjacent_items,
                                          malformed_budget_type, missing_program_code,
                                          amount_alignment_uncertain, duplicate_candidate,
                                          unit_conversion_applied, other]
  double_counting_risk         : bool — true if this record may duplicate another line"""

_OUTPUT_SCHEMA_REVIEW = """\
Required JSON keys per record (review mode):
  record_id                    : string  — echo back unchanged
  keep                         : bool    — true only if clearly R&D per taxonomy
  clean_program_description_en : string | null — English translation only if keep=true
  validated_amount_local       : number | null — null if keep=false
  currency                     : string | null
  frascati_type                : one of [intramural_rd, extramural_grants, rd_coordination,
                                          rd_infrastructure, higher_ed_rd, not_rd]
  ai_rd_category               : one of [direct_rd, innovation_system, possible_rd, not_rd]
  ai_pillar                    : one of [Direct R&D, Innovation, Ambiguous, Exclude]
  ai_confidence                : float 0–1
  ai_decision                  : one of [include, exclude]  — NO "review" allowed here;
                                  you MUST make a binary decision
  ai_rationale                 : string — cite the specific taxonomy term (include list or
                                  exclusion list) that drove your decision. If neither
                                  list applies, classify as exclude.
  parse_issue                  : one of [none, legal_reference_noise, merged_adjacent_items,
                                          malformed_budget_type, missing_program_code,
                                          amount_alignment_uncertain, other]"""

_OUTPUT_SCHEMA_UNIFIED = """\
Required JSON keys per record (unified row-validation mode):
  record_id                    : string  — echo back unchanged
  keep                         : bool    — true if this is a valid R&D budget line
  clean_program_code           : string | null
  clean_program_description_da : string | null — cleaned original-language description
  clean_program_description_en : string | null — English translation of cleaned description
  clean_budget_type_da         : string | null
  clean_budget_type_en         : string | null
  validated_amount_local       : number | null — corrected amount only if page/budget context
                                  explicitly shows a unit or alignment error
  currency                     : string | null
  frascati_type                : one of [intramural_rd, extramural_grants, rd_coordination,
                                          rd_infrastructure, higher_ed_rd, not_rd]
  ai_rd_category               : one of [direct_rd, innovation_system, possible_rd, not_rd]
  ai_pillar                    : one of [Direct R&D, Innovation, Ambiguous, Exclude]
  ai_confidence                : float 0–1
  ai_decision                  : one of [include, review, exclude]
  ai_rationale                 : string — cite the specific taxonomy term or exclusion that drove the decision
  parse_issue                  : one of [none, legal_reference_noise, merged_adjacent_items,
                                          malformed_budget_type, missing_program_code,
                                          amount_alignment_uncertain, duplicate_candidate,
                                          unit_conversion_applied, other]
  double_counting_risk         : bool — true if this record may duplicate another line on the same page/batch"""


# ── AI Client ─────────────────────────────────────────────────────────────────

class AIClient:
    """Wrapper around an OpenAI-style chat/completions API."""

    def __init__(self, config: AIClientConfig | None = None):
        self.config = config or AIClientConfig()

        if OpenAI is None:
            raise MissingOpenAIDependencyError(
                "openai package is not installed. Add it to requirements."
            )

        api_key = os.getenv(self.config.api_key_env)
        if not api_key:
            raise MissingAPIKeyError(
                f"API key missing. Set {self.config.api_key_env} in your environment."
            )

        self.client = OpenAI(api_key=api_key)

    # ── Include-mode prompt ───────────────────────────────────────────────────

    def build_messages_include(self, batch: List[dict]) -> list[dict]:
        """Prompt for high-confidence "include" records.

        These records were already classified as R&D by the taxonomy keyword
        scoring. The AI's job is NOT to re-evaluate R&D status but to:
        - Validate and correct the amount if context shows a unit issue
        - Classify by Frascati budget type
        - Flag potential double-counting within the batch
        - Clean and translate descriptions
        """
        system = "\n\n".join([
            (
                "You are a specialist in OECD government R&D budget statistics. "
                "You are reviewing budget line items that have ALREADY been identified as "
                "R&D-relevant by a keyword taxonomy. Your task is to re-decide if they "
                "are R&D — assume they are. The extractionare from finance bills documents"
                " Your tasks are:\n"
                "1. Validate and correct the reported amount if the surrounding budget "
                "context (previous_lines, next_lines, neighbor_amounts) clearly shows a "
                "unit conversion issue (e.g. heading says '$000' but amount looks like full dollars).\n"
                "2. Classify each item by Frascati budget type.\n"
                "3. Clean the original-language description and provide an English translation.\n"
                "4. Flag double_counting_risk=true if this record appears to be a subtotal "
                "or aggregate of another record in the same batch."
            ),
            _ANTI_HALLUCINATION_RULES,
            _TAXONOMY_GROUNDING,
            _OUTPUT_SCHEMA_INCLUDE,
        ])

        # If the batch was grouped by page, the first record carries a shared
        # page_context field with the full page text — sent once for all records
        # in this batch instead of repeated as a trimmed excerpt per record.
        page_context = None
        clean_batch = []
        for rec in batch:
            rec = dict(rec)
            if "page_context" in rec:
                page_context = rec.pop("page_context")
            clean_batch.append(rec)

        user_content = {
            "task": "validate_include_records",
            "mode": "include",
            "items": clean_batch,
            "requirements": {
                "return_format": "JSON array, one object per input record, same order",
                "language_notes": (
                    "Input may be multilingual. Provide English translation in "
                    "clean_program_description_en only. Do not translate if input is "
                    "already English."
                ),
                "amount_rule": (
                    "Only change validated_amount_local if the budget_window context "
                    "or page_context contains explicit evidence of a unit error "
                    "(e.g. '$000' or \"$'000\" in a column header). "
                    "Document the change in parse_issue=unit_conversion_applied."
                ),
            },
        }
        if page_context:
            user_content["page_context"] = (
                "The following is the full text of the budget page from which ALL "
                "records in this batch were extracted. Use it to understand the "
                "surrounding structure and detect subtotals or aggregates:\n\n"
                + page_context
            )

        return [
            {"role": "system", "content": system},
            {
                "role": "user",
                "content": (
                    "Process these pre-validated R&D items. Return only a JSON array "
                    "with no prose.\n" + json.dumps(user_content)
                ),
            },
        ]

    # ── Review-mode prompt ────────────────────────────────────────────────────

    def build_messages_review(self, batch: List[dict]) -> list[dict]:
        """Prompt for borderline "review" records.

        These records scored 1–2 on the taxonomy (below the include threshold of 3).
        The AI must make a BINARY include/exclude decision based strictly on the
        OECD taxonomy definitions in the prompt. "review" is not a valid output.
        """
        system = "\n\n".join([
            (
                "You are a specialist in OECD government R&D budget statistics (GBARD). "
                "You are reviewing borderline budget line items that scored just below the "
                "R&D include threshold in a keyword taxonomy. For each record you must make "
                "a BINARY decision: include (it is R&D per the OECD taxonomy) or exclude "
                "(it is not). You may NOT return 'review' — a decision is required.\n\n"
                "Decision rules:\n"
                "- If the line description or surrounding context contains a HIGH-CONFIDENCE "
                "R&D keyword from the taxonomy → include.\n"
                "- If it contains an EXCLUSION term from the taxonomy → exclude.\n"
                "- If it contains only AMBIGUOUS terms with NO explicit R&D anchor in "
                "the surrounding context → exclude.\n"
                "- If the record is a ministry-wide aggregate, revenue line, or administrative "
                "overhead with no R&D qualifier → exclude.\n"
                "- When genuinely uncertain after applying the above rules → exclude "
                "(conservative default) and explain in ai_rationale."
            ),
            _ANTI_HALLUCINATION_RULES,
            _TAXONOMY_GROUNDING,
            _OUTPUT_SCHEMA_REVIEW,
        ])

        page_context = None
        clean_batch = []
        for rec in batch:
            rec = dict(rec)
            if "page_context" in rec:
                page_context = rec.pop("page_context")
            clean_batch.append(rec)

        user_content = {
            "task": "classify_borderline_records",
            "mode": "review",
            "items": clean_batch,
            "requirements": {
                "return_format": "JSON array, one object per input record, same order",
                "decision_rule": (
                    "ai_decision must be 'include' or 'exclude' — never 'review'. "
                    "ai_rationale must cite the specific taxonomy term that drove the decision."
                ),
                "conservative_default": (
                    "When evidence is absent or ambiguous, default to exclude."
                ),
            },
        }
        if page_context:
            user_content["page_context"] = (
                "The following is the full text of the budget page from which ALL "
                "records in this batch were extracted. Use it as context when "
                "deciding whether borderline lines have an R&D anchor nearby:\n\n"
                + page_context
            )

        return [
            {"role": "system", "content": system},
            {
                "role": "user",
                "content": (
                    "Classify each borderline record. Return only a JSON array "
                    "with no prose.\n" + json.dumps(user_content)
                ),
            },
        ]

    def build_messages_unified(self, batch: List[dict]) -> list[dict]:
        """Prompt for a single row-validation pass over all candidate records.

        Unlike the old include/review split, this prompt applies the same
        OECD taxonomy, amount-validation, and duplicate-risk logic to every row.
        """
        system = "\n\n".join([
            (
                "You are a specialist in OECD government R&D budget statistics (GBARD). "
                "You are validating extracted budget line items from finance bills. "
                "Treat every record with the same standard.\n\n"
                "Your tasks:\n"
                "1. Decide whether each record should be include, review, or exclude.\n"
                "2. Validate the amount and correct it ONLY if the surrounding budget "
                "context explicitly shows a unit or alignment error.\n"
                "3. Classify each kept record by Frascati budget type.\n"
                "4. Clean the original-language description and provide an English translation.\n"
                "5. Flag double_counting_risk=true if this record appears to be a subtotal "
                "or aggregate of another record in the same batch."
            ),
            _ANTI_HALLUCINATION_RULES,
            _TAXONOMY_GROUNDING,
            (
                "Decision rules:\n"
                "- include: clear R&D line, institution, or appropriation per the taxonomy.\n"
                "- exclude: clear exclusion, non-R&D aggregate, revenue line, or administrative line.\n"
                "- review: likely relevant but amount alignment, aggregation status, or R&D scope remains uncertain.\n"
                "- Be conservative with amount corrections; use review rather than guessing."
            ),
            _OUTPUT_SCHEMA_UNIFIED,
        ])

        page_context = None
        clean_batch = []
        for rec in batch:
            rec = dict(rec)
            if "page_context" in rec:
                page_context = rec.pop("page_context")
            clean_batch.append(rec)

        user_content = {
            "task": "validate_budget_records",
            "mode": "unified",
            "items": clean_batch,
            "requirements": {
                "return_format": "JSON array, one object per input record, same order",
                "decision_rule": (
                    "ai_decision must be one of include, review, exclude. "
                    "Use review only when the line may be R&D but uncertainty remains."
                ),
                "amount_rule": (
                    "Only change validated_amount_local when explicit page context "
                    "shows a unit or line-alignment problem."
                ),
            },
        }
        if page_context:
            user_content["page_context"] = (
                "The following is the full text of the budget page from which ALL "
                "records in this batch were extracted. Use it to understand the "
                "page structure, subtotals, and nearby R&D anchors:\n\n"
                + page_context
            )

        return [
            {"role": "system", "content": system},
            {
                "role": "user",
                "content": (
                    "Validate these extracted budget records. Return only a JSON array "
                    "with no prose.\n" + json.dumps(user_content)
                ),
            },
        ]

    # ── Aggregation-mode prompt ───────────────────────────────────────────────

    def build_aggregation_messages(
        self,
        records: list[dict],
        country: str,
        year: str | int,
    ) -> list[dict]:
        """Prompt for the country-year aggregation pass.

        Receives ALL validated records for one (country, year). Checks for
        double-counting and produces a total R&D estimate with confidence.
        """
        system = "\n\n".join([
            (
                "You are a specialist in OECD government R&D budget statistics (GBARD). "
                "You are reviewing ALL validated R&D budget line items for a single "
                f"country-year: {country} {year}.\n\n"
                "Your tasks:\n"
                "1. Identify double-counting: flag pairs of records where one appears "
                "to be a subtotal or aggregate of another (e.g. a department total that "
                "includes a specific agency already listed separately).\n"
                "2. Estimate the total government R&D appropriation for this country-year "
                "by summing non-duplicated records. State which records you excluded from "
                "the sum and why.\n"
                "3. Rate your confidence in the total (0–1).\n"
                "4. Note any data quality issues that limit comparability "
                "(e.g. partial coverage, mixed budget types, unclear units)."
            ),
            _ANTI_HALLUCINATION_RULES,
            (
                "ADDITIONAL RULE for aggregation:\n"
                "Do not add records to the total that are not present in the input. "
                "Do not subtract records unless you have explicit evidence they are "
                "duplicates of another listed record. State your reasoning for every "
                "exclusion from the sum."
            ),
        ])

        output_schema = """\
Return a single JSON object (not an array) with these keys:
  country                  : string
  year                     : string or int
  double_counting_flags    : array of objects, each with:
      record_ids           : array of record_id strings that overlap
      reason               : string — why they overlap
      recommended_action   : "keep_first" | "keep_largest" | "manual_review"
  included_record_ids      : array of record_id strings used in the total
  excluded_record_ids      : array of record_id strings excluded (duplicates)
  estimated_total_rd       : number | null — sum in local currency
  currency                 : string
  confidence               : float 0–1
  coverage_notes           : string — data quality caveats (null if none)"""

        user_content = {
            "task": "aggregate_country_year_rd",
            "country": country,
            "year": year,
            "validated_records": records,
            "output_schema": output_schema,
        }

        return [
            {"role": "system", "content": system},
            {
                "role": "user",
                "content": (
                    f"Aggregate R&D records for {country} {year}. "
                    "Return only the JSON object, no prose.\n"
                    + json.dumps(user_content)
                ),
            },
        ]

    # ── Anomaly-mode prompt ───────────────────────────────────────────────────

    def build_anomaly_messages(
        self,
        timeseries_data: list[dict],
        country: str,
    ) -> list[dict]:
        """Prompt for the time-series anomaly detection pass.

        Receives per-program time series for one country. Flags year-over-year
        anomalies: unit errors, implausible spikes/drops, missing years.
        """
        system = "\n\n".join([
            (
                "You are a specialist in OECD government R&D budget statistics (GBARD). "
                f"You are reviewing the extracted R&D time series for {country}.\n\n"
                "Your tasks:\n"
                "1. For each program/agency series, assess whether all values are "
                "plausible and internally consistent.\n"
                "2. Flag any year where the amount is anomalous relative to neighbors "
                "(e.g. 10× or 0.1× the surrounding years — likely a unit error).\n"
                "3. Identify gaps (years with no data between years that have data).\n"
                "4. Where you flag an anomaly, suggest a corrected amount ONLY if the "
                "evidence is strong (e.g. explicit '$000' header on that year's extract). "
                "Otherwise leave suggested_amount as null.\n"
                "5. Rate confidence in each flag (0–1)."
            ),
            _ANTI_HALLUCINATION_RULES,
            (
                "ADDITIONAL RULE for anomaly detection:\n"
                "Do not flag a year as anomalous solely because the amount is large. "
                "Government R&D budgets do grow. Only flag when the year-over-year ratio "
                "is implausible (>5× or <0.1×) AND there is no obvious programmatic "
                "explanation visible in the data provided. When in doubt, do NOT flag."
            ),
        ])

        output_schema = """\
Return a JSON array (one object per anomalous program-year). Each object:
  program_code      : string
  program_name      : string | null
  country           : string
  anomaly_year      : string or int
  anomaly_type      : one of [spike, drop, unit_error, gap, other]
  neighboring_years : object mapping year → amount for context (from input only)
  suspected_cause   : string — one sentence; null if unknown
  suggested_amount  : number | null — corrected amount in local currency; null if uncertain
  currency          : string
  confidence        : float 0–1
If no anomalies are found, return an empty array []."""

        user_content = {
            "task": "detect_timeseries_anomalies",
            "country": country,
            "timeseries": timeseries_data,
            "output_schema": output_schema,
        }

        return [
            {"role": "system", "content": system},
            {
                "role": "user",
                "content": (
                    f"Detect anomalies in the {country} R&D time series. "
                    "Return only the JSON array, no prose.\n"
                    + json.dumps(user_content)
                ),
            },
        ]

    # ── Shared batch runner ───────────────────────────────────────────────────

    def _call(self, messages: list[dict]) -> str:
        """Send messages and return raw response content string."""
        response = self.client.chat.completions.create(
            model=self.config.model,
            messages=messages,
            temperature=self.config.temperature,
            max_tokens=self.config.max_output_tokens,
        )
        content = response.choices[0].message.content if response.choices else ""
        if not content:
            raise ValueError("Empty response from AI model")
        return content

    @staticmethod
    def _extract_json(txt: str) -> str:
        """Strip markdown fences and return the raw JSON string."""
        txt = txt.strip()
        if "```" in txt:
            parts = txt.split("```")
            if len(parts) >= 3:
                candidate = parts[1]
                candidate = re.sub(r"^\s*json\s*", "", candidate, flags=re.IGNORECASE)
                return candidate.strip()
        for opener in ["[", "{"]:
            idx = txt.find(opener)
            if idx != -1:
                return txt[idx:]
        return txt

    @staticmethod
    def _parse_json_loose(txt: str):
        """Parse the first valid JSON value from a model response.

        This is more tolerant than json.loads() on the whole string and handles
        common model behaviors such as:
        - explanatory prose before JSON
        - valid JSON followed by stray text
        - fenced JSON blocks with trailing commentary
        """
        raw = AIClient._extract_json(txt)
        decoder = json.JSONDecoder()
        raw = raw.strip()
        for opener in ("{", "["):
            idx = raw.find(opener)
            if idx == -1:
                continue
            candidate = raw[idx:].lstrip()
            try:
                obj, _end = decoder.raw_decode(candidate)
                return obj
            except json.JSONDecodeError:
                continue
        return json.loads(raw)

    def validate_batch(self, batch: List[dict], mode: str = "unified") -> list[dict]:
        """Send a batch to the model and return a parsed JSON list.

        Parameters
        ----------
        batch : list of record dicts
        mode  : "unified" | "include" | "review"
        """
        if mode == "review":
            messages = self.build_messages_review(batch)
        elif mode == "include":
            messages = self.build_messages_include(batch)
        else:
            messages = self.build_messages_unified(batch)

        content = self._call(messages)
        result = self._parse_json_loose(content)
        if isinstance(result, dict) and "items" in result:
            return result["items"]
        if isinstance(result, list):
            return result
        return []

    def run_aggregation(
        self,
        records: list[dict],
        country: str,
        year: str | int,
    ) -> dict:
        """Run the country-year aggregation pass. Returns a single dict."""
        messages = self.build_aggregation_messages(records, country, year)
        content = self._call(messages)
        result = self._parse_json_loose(content)
        if isinstance(result, list) and result:
            return result[0]
        if isinstance(result, dict):
            return result
        return {}

    def run_anomaly_detection(
        self,
        timeseries_data: list[dict],
        country: str,
    ) -> list[dict]:
        """Run the time-series anomaly detection pass. Returns a list of flags."""
        messages = self.build_anomaly_messages(timeseries_data, country)
        content = self._call(messages)
        result = self._parse_json_loose(content)
        if isinstance(result, list):
            return result
        return []

    # ── Backward-compat shim ──────────────────────────────────────────────────

    def build_messages(self, batch: List[dict]) -> list[dict]:
        """Legacy entry point — routes to unified row-validation prompt."""
        return self.build_messages_unified(batch)
