"""Optional AI validation layer that post-processes extracted candidate records.

Pipeline stages (in order)
---------------------------
1. Individual validation — split by decision tier:
     a. INCLUDE records  → build_messages_include()  (amount validation, Frascati type,
                           double-counting flag, translation)
     b. REVIEW records   → build_messages_review()   (binary include/exclude decision
                           based strictly on OECD taxonomy definitions)

2. Country-year aggregation pass (run_country_year_aggregation):
     After individual validation, one call per (country, year).
     Detects double-counting across records, produces a deduplicated total.
     Results written to aggregation_results.csv under the run directory.

3. Time-series anomaly detection pass (run_timeseries_anomaly_detection):
     After all years are assembled, one call per country.
     Flags implausible year-over-year changes (unit errors, spikes, gaps).
     Results written to anomaly_flags.csv under the run directory.

Cache strategy
--------------
Cache key = MD5(year | section_code | line_description | amount_local)
Year is included so that the same agency in a different year gets its own
cache slot — important when descriptions or amounts evolve year-to-year.
Aggregation and anomaly calls have separate cache files.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd

from budget.ai_batch_runner import run_batches
from budget.ai_client import AIClient, AIClientConfig, MissingAPIKeyError, MissingOpenAIDependencyError
from budget.compare_outputs import build_comparison
from budget.config import (
    BUDGET_ITEMS_FILE,
    CANDIDATES_FILE,
    PROCESSED_DIR,
    RESULTS_AI_VERIFIED_FILE,
    RESULTS_CSV_FILE,
    RESULTS_JSON_FILE,
)
from budget.utils import configure_logging, ensure_directories, logger

BASE_AI_VALIDATION_DIR = PROCESSED_DIR / "ai_validation"

FILTER_DECISIONS = {"include", "review"}

# ── Known R&D agency auto-classification ──────────────────────────────────────
# Program codes whose R&D status is unambiguous (specialist extractors).
# For "include" rows matching these codes AND from English-language countries,
# we skip the AI validation call — saving tokens where no translation is needed.
# Non-English countries always go to AI so descriptions get translated to English.
# Format: program_code_prefix → (frascati_type, ai_rd_category, rationale)
#
# ENGLISH-LANGUAGE COUNTRIES: descriptions already in English, no translation needed.
_ENGLISH_LANGUAGE_COUNTRIES: frozenset[str] = frozenset({
    "Australia", "Canada", "New Zealand", "United Kingdom", "United States",
    "Ireland", "South Africa",
})
_KNOWN_RD_AGENCIES: dict[str, tuple[str, str, str]] = {
    # Australia
    "AU_CSIRO":       ("intramural_rd",    "direct_rd",        "CSIRO: Australia's national science agency — intramural R&D"),
    "AU_ANSTO":       ("intramural_rd",    "direct_rd",        "ANSTO: Australian nuclear science and technology — intramural R&D"),
    "AU_AIMS":        ("intramural_rd",    "direct_rd",        "AIMS: Australian Institute of Marine Science — intramural R&D"),
    "AU_ARC":         ("extramural_grants","direct_rd",        "ARC: Australian Research Council — competitive grants to universities"),
    "AU_NHMRC":       ("extramural_grants","direct_rd",        "NHMRC: National Health & Medical Research Council — health R&D grants"),
    "AU_DEPT_SCIENCE":("rd_coordination",  "direct_rd",        "Department of Science: portfolio coordination of R&D agencies"),
    # Canada
    "CA_NRC":         ("intramural_rd",    "direct_rd",        "NRC: National Research Council Canada — intramural R&D labs"),
    "CA_NSERC":       ("extramural_grants","direct_rd",        "NSERC: Natural Sciences & Engineering Research Council — university grants"),
    "CA_SSHRC":       ("extramural_grants","direct_rd",        "SSHRC: Social Sciences & Humanities Research Council — university grants"),
    "CA_CIHR":        ("extramural_grants","direct_rd",        "CIHR: Canadian Institutes of Health Research — health R&D grants"),
    "CA_CHARS":       ("intramural_rd",    "direct_rd",        "CHARS: Canadian High Arctic Research Station — intramural R&D"),
    "CA_AECL":        ("intramural_rd",    "direct_rd",        "AECL: Atomic Energy of Canada Limited — nuclear R&D"),
    "CA_CFI":         ("rd_infrastructure","innovation_system", "CFI: Canada Foundation for Innovation — research infrastructure"),
    "CA_SCITECH_MINISTRY": ("rd_coordination","direct_rd",     "Ministry of State for Science & Technology — R&D coordination"),
    # New Zealand
    "NZ_":            ("extramural_grants","direct_rd",        "NZ R&D agency — government R&D funding"),
    # United Kingdom
    "UK_DSIT":        ("rd_coordination",  "direct_rd",        "DSIT: Dept for Science, Innovation & Technology — R&D ministry capital (UKRI, Innovate UK)"),
    "UK_SCIMINISTRY": ("rd_coordination",  "direct_rd",        "UK Science Ministry (BEIS/BIS/DIUS): Capital DEL includes research council grants"),
    "UK_SCIENCE_":    ("rd_coordination",  "direct_rd",        "UK government science budget aggregate"),
}


def _auto_classify_row(program_code: str | None) -> dict | None:
    """Return pre-filled AI fields for a known R&D agency, or None if unknown."""
    if not program_code:
        return None
    pc = str(program_code).strip()
    for prefix, (frascati, rd_cat, rationale) in _KNOWN_RD_AGENCIES.items():
        if pc == prefix or pc.startswith(prefix):
            return {
                "keep": True,
                "clean_program_code": pc,
                "clean_program_description_en": None,
                "validated_amount_local": None,  # trust extractor amount
                "frascati_type": frascati,
                "ai_rd_category": rd_cat,
                "ai_pillar": "Direct R&D",
                "ai_confidence": 0.95,
                "ai_decision": "include",
                "ai_rationale": rationale,
                "parse_issue": "none",
                "double_counting_risk": False,
                "_auto_classified": True,
            }
    return None


def _split_auto_classify(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split into (auto-classified include rows, rows that need AI validation).

    Auto-classify only when ALL conditions hold:
      1. decision == "include" (review rows always go to AI for judgment)
      2. program_code matches a known R&D agency
      3. country is English-language (descriptions already in English —
         non-English countries need AI for translation regardless)

    All other rows (review, unknown programs, non-English countries) → AI.
    """
    if df.empty:
        return df.copy(), df.copy()

    country_col = df.get("country", pd.Series("", index=df.index)).fillna("").astype(str)

    auto_mask = (
        (df["decision"].str.lower() == "include")
        & country_col.isin(_ENGLISH_LANGUAGE_COUNTRIES)
        & df["program_code"].fillna("").apply(lambda pc: _auto_classify_row(pc) is not None)
    )

    auto_df = df[auto_mask].copy()
    ai_df = df[~auto_mask].copy()
    return auto_df, ai_df


def _apply_auto_classification(df: pd.DataFrame) -> pd.DataFrame:
    """Fill AI output columns for auto-classified rows."""
    if df.empty:
        return df

    rows = []
    for _, row in df.iterrows():
        fields = _auto_classify_row(row.get("program_code")) or {}
        combined = row.to_dict()
        combined.update(fields)
        rows.append(combined)
    return pd.DataFrame(rows, columns=list(df.columns) + [
        c for c in ["keep", "clean_program_code", "clean_program_description_en",
                    "validated_amount_local", "frascati_type", "ai_rd_category",
                    "ai_pillar", "ai_confidence", "ai_decision", "ai_rationale",
                    "parse_issue", "double_counting_risk", "_auto_classified"]
        if c not in df.columns
    ])


@dataclass
class AIValidationConfig:
    input_file: Path = RESULTS_CSV_FILE
    max_records_to_send: int | None = None
    min_amount_threshold: float | None = None
    include_review_only: bool = False
    batch_size: int = 10
    cache_file: Path | None = None
    raw_output_file: Path | None = None
    clean_output_file: Path | None = None
    comparison_file: Path | None = None
    comparison_jsonl_file: Path | None = None
    failed_batches_file: Path | None = None
    summary_file: Path | None = None
    model: str = "gpt-4o-mini"
    temperature: float = 0.1
    output_format: str = "both"           # csv | json | both
    group_by_page: bool = False
    include_context: bool = False
    run_name: str = "default"
    filter_country: str | None = None
    filter_year: str | int | None = None
    skip_verified_records: bool = False
    verified_results_file: Path = RESULTS_AI_VERIFIED_FILE
    # New passes
    run_aggregation_pass: bool = True     # country-year double-counting check
    run_anomaly_pass: bool = True         # time-series anomaly detection


# ── Review-match columns (stable content key for dedup / reconcile) ───────────

_REVIEW_MATCH_COLUMNS = [
    "country",
    "year",
    "section_code",
    "program_code",
    "program_description",
    "line_description",
    "amount_local",
    "page_number",
    "source_file",
]

_CONTEXT_MERGE_COLUMNS = [
    "section_name",
    "item_code",
    "item_description",
    "line_code",
    "line_type",
    "raw_line",
    "merged_line",
    "context_before",
    "context_after",
    "amount_raw",
]


# ── Cache helpers ─────────────────────────────────────────────────────────────

def _load_cache(cache_path: Path) -> Dict[str, dict]:
    cache: Dict[str, dict] = {}
    if cache_path.exists():
        with cache_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                try:
                    entry = json.loads(line)
                    key = entry.get("cache_key")
                    payload = entry.get("result")
                    if key and payload:
                        cache[key] = payload
                except json.JSONDecodeError:
                    continue
    return cache


def _append_cache_entries(cache_path: Path, entries: List[dict]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("a", encoding="utf-8") as fh:
        for entry in entries:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")


# ── Key / hash helpers ────────────────────────────────────────────────────────

def _normalize_text(value: str) -> str:
    value = str(value).strip()
    value = re.sub(r"\s+", " ", value)
    value = re.sub(r"\.{2,}", ".", value)
    return value


def _cache_key(
    year: str,
    section_code: str,
    line_description: str,
    amount_local,
) -> str:
    """Stable cache key that includes year so per-year evolution is captured."""
    if pd.isna(amount_local):
        amount_local = ""
    normalized = (
        f"{year or ''}|"
        f"{section_code or ''}|"
        f"{_normalize_text(line_description or '')}|"
        f"{amount_local or ''}"
    )
    return hashlib.md5(normalized.lower().encode("utf-8")).hexdigest()


def _review_match_key(row: pd.Series) -> str:
    parts: list[str] = []
    for col in _REVIEW_MATCH_COLUMNS:
        value = row[col] if col in row.index else ""
        if pd.isna(value):
            value = ""
        if col == "amount_local":
            try:
                value = f"{float(value):.6f}" if value != "" else ""
            except (TypeError, ValueError):
                value = str(value)
        parts.append(_normalize_text(str(value)))
    return hashlib.md5("|".join(parts).encode("utf-8")).hexdigest()


def _aggregation_cache_key(country: str, year: str) -> str:
    normalized = f"agg|{country.lower().strip()}|{str(year).strip()}"
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()


def _anomaly_cache_key(country: str, program_code: str) -> str:
    normalized = f"anomaly|{country.lower().strip()}|{program_code.lower().strip()}"
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()


# ── Data loading helpers ──────────────────────────────────────────────────────

def _load_baseline(path: Path) -> pd.DataFrame:
    """Load baseline candidates from CSV or JSON."""
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list) and data and "items" in data[0]:
            rows: list[dict] = []
            for doc in data:
                common = {k: v for k, v in doc.items() if k != "items"}
                for item in doc.get("items", []):
                    rows.append({**common, **item})
            return pd.DataFrame(rows)
        return pd.DataFrame(data if isinstance(data, list) else [])

    df = pd.read_csv(path)

    # Harmonize field names from older pipeline versions
    if "line_description" not in df.columns:
        if "text_snippet" in df.columns:
            df = df.rename(columns={"text_snippet": "line_description"})
        elif "text" in df.columns:
            df = df.rename(columns={"text": "line_description"})
    if "amount_local" not in df.columns:
        if "amount" in df.columns:
            df = df.rename(columns={"amount": "amount_local"})
        elif "amount_dkk" in df.columns:
            df = df.rename(columns={"amount_dkk": "amount_local"})
    if "currency" not in df.columns and "currency_guess" in df.columns:
        df = df.rename(columns={"currency_guess": "currency"})

    return df


def _merge_budget_context(df: pd.DataFrame, context_file: Path = BUDGET_ITEMS_FILE) -> pd.DataFrame:
    """Backfill richer context fields from budget_items_detected.csv when available."""
    if df.empty or not context_file.exists():
        return df

    needed = [col for col in _CONTEXT_MERGE_COLUMNS if col not in df.columns]
    if not needed:
        return df

    try:
        context_df = pd.read_csv(context_file)
    except Exception as exc:
        logger.warning("Could not read budget context file %s: %s", context_file, exc)
        return df

    if context_df.empty:
        return df

    available = [col for col in _CONTEXT_MERGE_COLUMNS if col in context_df.columns]
    if not available:
        return df

    working = df.copy()
    if "__review_match_key" not in working.columns:
        working["__review_match_key"] = working.apply(_review_match_key, axis=1)

    context_working = context_df.copy()
    context_working["__review_match_key"] = context_working.apply(_review_match_key, axis=1)
    context_working = context_working.drop_duplicates(subset=["__review_match_key"], keep="first")

    merged = working.merge(
        context_working[["__review_match_key", *available]],
        on="__review_match_key",
        how="left",
        suffixes=("", "__ctx"),
    )

    for col in available:
        ctx_col = f"{col}__ctx"
        if col in merged.columns and ctx_col in merged.columns:
            merged[col] = merged[col].where(merged[col].notna(), merged[ctx_col])
            merged = merged.drop(columns=[ctx_col])
        elif ctx_col in merged.columns:
            merged = merged.rename(columns={ctx_col: col})

    return merged.drop(columns=["__review_match_key"], errors="ignore")


# ── Candidate filtering ───────────────────────────────────────────────────────

def _filter_candidates(df: pd.DataFrame, config: AIValidationConfig) -> pd.DataFrame:
    df = df.copy()

    if "line_description" in df.columns:
        df["line_description"] = df["line_description"].fillna("").astype(str)
    if "amount_local" in df.columns:
        df["amount_local"] = pd.to_numeric(df["amount_local"], errors="coerce")
    else:
        df["amount_local"] = pd.NA

    mask_valid_desc = df["line_description"].str.strip() != ""
    mask_valid_amount = df["amount_local"].notnull() if df["amount_local"].notnull().any() else True
    decision_col = df.get("decision")

    if config.include_review_only and decision_col is not None:
        mask_decision = decision_col.str.lower().isin({"review"})
    else:
        mask_decision = decision_col.str.lower().isin(FILTER_DECISIONS) if decision_col is not None else True

    mask_min_amount = True
    if config.min_amount_threshold is not None:
        mask_min_amount = df["amount_local"] >= config.min_amount_threshold

    filtered = df[mask_valid_desc & mask_valid_amount & mask_decision & mask_min_amount]

    if config.max_records_to_send:
        filtered = filtered.head(config.max_records_to_send)

    return filtered.reset_index(drop=True)


def exclude_verified_candidates(
    baseline_df: pd.DataFrame,
    verified_results_file: Path,
) -> tuple[pd.DataFrame, int]:
    """Remove rows already present in results_ai_verified.csv."""
    if baseline_df.empty or not verified_results_file.exists():
        return baseline_df.copy(), 0

    try:
        verified_df = pd.read_csv(verified_results_file)
    except Exception as exc:
        logger.warning("Could not read verified results file %s: %s", verified_results_file, exc)
        return baseline_df.copy(), 0

    if verified_df.empty:
        return baseline_df.copy(), 0

    baseline = baseline_df.copy()
    baseline["__review_match_key"] = baseline.apply(_review_match_key, axis=1)
    verified_df["__review_match_key"] = verified_df.apply(_review_match_key, axis=1)
    reviewed_keys = set(verified_df["__review_match_key"].dropna().astype(str))
    filtered = baseline[~baseline["__review_match_key"].isin(reviewed_keys)].copy()
    skipped_count = len(baseline) - len(filtered)
    return filtered.drop(columns="__review_match_key"), skipped_count


# ── Record preparation ────────────────────────────────────────────────────────

def _split_context_lines(text: str | None, max_lines: int = 4) -> list[str]:
    if not text or pd.isna(text):
        return []
    raw = str(text).strip()
    if not raw:
        return []
    parts = [seg.strip(" .;:-") for seg in re.split(r"\s*\|\s*|\s{2,}|\n+", raw) if seg and seg.strip(" .;:-")]
    if not parts:
        parts = [_normalize_text(raw)]
    return parts[:max_lines]


def _extract_amount_tokens(lines: list[str]) -> list[str]:
    amounts: list[str] = []
    seen: set[str] = set()
    for line in lines:
        for match in re.finditer(r"[÷\-\+]?\s*\d{1,3}(?:[.,\s]\d{3})*(?:[.,]\d{1,2})?", line):
            token = _normalize_text(match.group(0))
            digits = re.sub(r"[^\d]", "", token)
            if len(digits) < 4:
                continue
            if token not in seen:
                seen.add(token)
                amounts.append(token)
    return amounts[:8]


def _build_budget_window(record: dict) -> dict:
    previous_lines = _split_context_lines(record.get("context_before"))
    next_lines = _split_context_lines(record.get("context_after"))
    current_line = _normalize_text(
        str(
            record.get("raw_line")
            or record.get("merged_line")
            or record.get("line_description")
            or ""
        )
    )
    return {
        "section": {
            "code": record.get("section_code"),
            "name": record.get("section_name") or record.get("section_name_en"),
        },
        "program": {
            "code": record.get("program_code"),
            "description": record.get("program_description"),
        },
        "item": {
            "code": record.get("item_code"),
            "description": record.get("item_description"),
        },
        "current_line": {
            "line_code": record.get("line_code"),
            "line_type": record.get("line_type"),
            "text": current_line,
            "amount_raw": record.get("amount_raw"),
            "amount_local": record.get("amount_local"),
            "currency": record.get("currency"),
        },
        "previous_lines": previous_lines,
        "next_lines": next_lines,
        "neighbor_amounts": _extract_amount_tokens(previous_lines + next_lines),
    }


def _prepare_records(df: pd.DataFrame, include_context: bool) -> List[dict]:
    optional_fields = {
        "section_name", "item_code", "item_description", "line_code",
        "line_type", "raw_line", "merged_line", "amount_raw",
    }
    if include_context:
        optional_fields |= {"context_before", "context_after", "raw_page_text_excerpt"}

    def _safe(val):
        if pd.isna(val):
            return None
        return val

    def _trim_context(text: str, anchor: str, before: int = 150, after: int = 100) -> str:
        if not text:
            return ""
        raw = _normalize_text(text)
        anch = _normalize_text(anchor)[:80]
        pos = raw.find(anch) if anch else -1
        if pos == -1:
            return raw[: before + after]
        start = max(0, pos - before)
        end = min(len(raw), pos + len(anch) + after)
        return raw[start:end]

    records: List[dict] = []
    for row in df.itertuples(index=False):
        record = {
            "record_id": _safe(getattr(row, "record_id", None)),
            "section_code": _safe(getattr(row, "section_code", "")),
            "section_name_en": _safe(getattr(row, "section_name_en", "")),
            "page_number": _safe(getattr(row, "page_number", None)),
            "budget_type": _safe(getattr(row, "budget_type", "")),
            "line_code": _safe(getattr(row, "line_code", "")),
            "line_description": _normalize_text(str(_safe(getattr(row, "line_description", "")) or "")),
            "amount_local": _safe(getattr(row, "amount_local", None)),
            "currency": _safe(getattr(row, "currency", "")),
            "rd_category": _safe(getattr(row, "rd_category", "")),
            "pillar": _safe(getattr(row, "pillar", "")),
            "decision": _safe(getattr(row, "decision", "")),
            "program_code": _safe(getattr(row, "program_code", "")),
            "program_description": _safe(getattr(row, "program_description", "")),
            "country": _safe(getattr(row, "country", "")),
            "year": _safe(getattr(row, "year", "")),
            "source_file": _safe(getattr(row, "source_file", "")),
        }
        for f in optional_fields:
            if f in df.columns:
                record[f] = _safe(getattr(row, f, ""))
        if include_context and "raw_page_text_excerpt" not in record and "text_snippet" in df.columns:
            record["raw_page_text_excerpt"] = _safe(getattr(row, "text_snippet", ""))

        if include_context:
            ctx_source = record.get("raw_page_text_excerpt") or ""
            if not ctx_source:
                ctx_source = " ".join(
                    str(record.get(k, "") or "")
                    for k in ("context_before", "line_description", "context_after")
                )
            record["raw_page_text_excerpt"] = _trim_context(ctx_source, record["line_description"])
            record["budget_window"] = _build_budget_window(record)
        records.append(record)
    return records


# ── Stage 2: Country-year aggregation pass ────────────────────────────────────

def run_country_year_aggregation(
    validated_df: pd.DataFrame,
    client: AIClient,
    cache_file: Path,
    failed_batches_file: Path,
) -> pd.DataFrame:
    """One AI call per (country, year) to check double-counting and produce totals.

    Input: the merged validated DataFrame (include + review decisions resolved).
    Output: a DataFrame with one row per (country, year) containing:
      - double_counting_flags (JSON string)
      - estimated_total_rd
      - included_record_ids / excluded_record_ids (JSON strings)
      - confidence
      - coverage_notes
    Written to aggregation_results.csv in the run directory.
    """
    if validated_df.empty:
        return pd.DataFrame()

    # Only aggregate records the AI decided to keep (include) or that were
    # pre-classified as include before the AI pass.
    agg_df = validated_df.copy()
    if "ai_decision" in agg_df.columns:
        keep_mask = agg_df["ai_decision"].fillna("").str.lower().isin({"include", ""})
        agg_df = agg_df[keep_mask].copy()
    elif "decision" in agg_df.columns:
        keep_mask = agg_df["decision"].fillna("").str.lower() == "include"
        agg_df = agg_df[keep_mask].copy()

    if agg_df.empty:
        logger.info("Aggregation pass: no included records to aggregate.")
        return pd.DataFrame()

    cache = _load_cache(cache_file)
    cache_entries: list[dict] = []
    results: list[dict] = []

    required_cols = {"country", "year", "record_id", "program_code",
                     "line_description", "amount_local", "currency"}
    missing = required_cols - set(agg_df.columns)
    for col in missing:
        agg_df[col] = ""

    groups = agg_df.groupby(["country", "year"], dropna=False, sort=True)
    total_groups = len(groups)
    logger.info("Aggregation pass: %d country-year groups to process.", total_groups)

    for (country, year), grp in groups:
        country = str(country)
        year = str(year)
        ck = _aggregation_cache_key(country, year)

        if ck in cache:
            result = dict(cache[ck])
            result["country"] = country
            result["year"] = year
            results.append(result)
            logger.debug("Aggregation cache hit: %s %s", country, year)
            continue

        records_payload = grp[[
            c for c in [
                "record_id", "program_code", "line_description",
                "line_description_en", "amount_local", "currency",
                "section_code", "section_name", "frascati_type",
                "ai_rd_category", "taxonomy_score",
            ] if c in grp.columns
        ]].to_dict(orient="records")

        try:
            agg_result = client.run_aggregation(records_payload, country, year)
        except Exception as exc:
            logger.warning(
                "Aggregation call failed for %s %s: %s", country, year, exc
            )
            with failed_batches_file.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps({
                    "task": "aggregation",
                    "country": country,
                    "year": year,
                    "error": str(exc),
                }) + "\n")
            continue

        agg_result["country"] = country
        agg_result["year"] = year
        results.append(agg_result)
        cache_entries.append({"cache_key": ck, "result": agg_result})

    if cache_entries:
        _append_cache_entries(cache_file, cache_entries)

    if not results:
        return pd.DataFrame()

    out_df = pd.DataFrame(results)
    # Serialize list-type columns to JSON strings for CSV storage
    for col in ("double_counting_flags", "included_record_ids", "excluded_record_ids"):
        if col in out_df.columns:
            out_df[col] = out_df[col].apply(
                lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v
            )

    logger.info(
        "Aggregation pass complete: %d country-years processed, %d results.",
        total_groups, len(out_df),
    )
    return out_df


# ── Stage 3: Time-series anomaly detection pass ───────────────────────────────

def run_timeseries_anomaly_detection(
    results_df: pd.DataFrame,
    client: AIClient,
    cache_file: Path,
    failed_batches_file: Path,
) -> pd.DataFrame:
    """One AI call per country to flag implausible year-over-year changes.

    Builds a per-program time series from results_df (include decisions only)
    and asks the AI to flag:
      - Unit errors (10× or 0.1× the surrounding years)
      - Implausible spikes or drops (>5× or <0.1× median of neighbors)
      - Gaps (missing years in an otherwise continuous series)

    Output: a DataFrame of anomaly flags, written to anomaly_flags.csv.
    The AI may suggest a corrected amount but ONLY when the evidence is strong
    (e.g. explicit unit header in the data). Otherwise suggested_amount is null.
    """
    if results_df.empty:
        return pd.DataFrame()

    df = results_df.copy()

    # Work on include decisions only
    if "decision" in df.columns:
        df = df[df["decision"].fillna("").str.lower() == "include"].copy()
    if df.empty:
        logger.info("Anomaly pass: no include-decision rows to analyze.")
        return pd.DataFrame()

    required = {"country", "year", "program_code", "amount_local", "currency"}
    if not required.issubset(df.columns):
        logger.warning("Anomaly pass: missing required columns, skipping.")
        return pd.DataFrame()

    df["year_int"] = pd.to_numeric(df["year"], errors="coerce")
    df["amount_num"] = pd.to_numeric(df["amount_local"], errors="coerce")

    cache = _load_cache(cache_file)
    cache_entries: list[dict] = []
    all_flags: list[dict] = []

    for country, country_df in df.groupby("country", sort=True):
        country = str(country)

        # Build one time series object per program_code
        timeseries_data: list[dict] = []
        for program_code, prog_df in country_df.groupby("program_code", sort=True):
            program_code = str(program_code)
            ck = _anomaly_cache_key(country, program_code)

            prog_sorted = prog_df.dropna(subset=["year_int", "amount_num"]).sort_values("year_int")
            if len(prog_sorted) < 2:
                # Need at least 2 data points to detect anomalies
                continue

            series = {
                str(int(row["year_int"])): float(row["amount_num"])
                for _, row in prog_sorted.iterrows()
            }
            currency = prog_sorted["currency"].iloc[0] if "currency" in prog_sorted.columns else "Unknown"
            desc = ""
            for col in ("line_description_en", "program_description_en",
                        "line_description", "program_description"):
                if col in prog_sorted.columns:
                    val = prog_sorted[col].dropna()
                    if not val.empty:
                        desc = str(val.iloc[0])
                        break

            if ck in cache:
                cached_flags = cache[ck]
                if isinstance(cached_flags, list):
                    all_flags.extend(cached_flags)
                logger.debug("Anomaly cache hit: %s / %s", country, program_code)
                continue

            timeseries_data.append({
                "program_code": program_code,
                "program_name": desc or None,
                "currency": currency,
                "years": series,
            })

        if not timeseries_data:
            continue

        try:
            flags = client.run_anomaly_detection(timeseries_data, country)
        except Exception as exc:
            logger.warning("Anomaly call failed for %s: %s", country, exc)
            with failed_batches_file.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps({
                    "task": "anomaly",
                    "country": country,
                    "error": str(exc),
                }) + "\n")
            continue

        for flag in flags:
            flag["country"] = country

        all_flags.extend(flags)

        # Cache per program_code within this country
        for entry in timeseries_data:
            prog_flags = [f for f in flags if f.get("program_code") == entry["program_code"]]
            ck = _anomaly_cache_key(country, entry["program_code"])
            cache_entries.append({"cache_key": ck, "result": prog_flags})

    if cache_entries:
        _append_cache_entries(cache_file, cache_entries)

    if not all_flags:
        logger.info("Anomaly pass: no anomalies flagged.")
        return pd.DataFrame()

    out_df = pd.DataFrame(all_flags)
    if "neighboring_years" in out_df.columns:
        out_df["neighboring_years"] = out_df["neighboring_years"].apply(
            lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, dict) else v
        )

    logger.info("Anomaly pass complete: %d flags across all countries.", len(out_df))
    return out_df


# ── Path resolution ───────────────────────────────────────────────────────────

def _resolve_paths(config: AIValidationConfig) -> AIValidationConfig:
    base_dir = BASE_AI_VALIDATION_DIR / config.run_name
    base_dir.mkdir(parents=True, exist_ok=True)

    def default(name: str) -> Path:
        return base_dir / name

    config.cache_file = config.cache_file or default("ai_cache.jsonl")
    config.raw_output_file = config.raw_output_file or default("ai_validated_candidates_raw.csv")
    config.clean_output_file = config.clean_output_file or default("ai_validated_candidates_clean.csv")
    config.comparison_file = config.comparison_file or default("baseline_vs_ai_comparison.csv")
    config.comparison_jsonl_file = config.comparison_jsonl_file or default("baseline_vs_ai_comparison.jsonl")
    config.failed_batches_file = config.failed_batches_file or default("failed_batches.jsonl")
    config.summary_file = config.summary_file or default("ai_validation_run_summary.json")
    return config


# ── Main entry point ──────────────────────────────────────────────────────────

def run_ai_validation(config: AIValidationConfig) -> bool:
    """Run the full AI validation pipeline.

    Stage 1: Unified individual record validation for all candidate rows
    Stage 2: Country-year aggregation (double-counting check, total estimate)
    Stage 3: Time-series anomaly detection (unit errors, spikes, gaps)
    """
    configure_logging()
    _resolve_paths(config)

    start_time = datetime.utcnow()
    logger.info("AI validation started — run='%s' model='%s'", config.run_name, config.model)

    # ── Load and prepare baseline ─────────────────────────────────────────────
    if not config.input_file.exists():
        logger.error("Input file not found: %s", config.input_file)
        return False

    baseline_df = _load_baseline(config.input_file)
    baseline_df = _merge_budget_context(baseline_df)

    if config.filter_country:
        baseline_df = baseline_df[
            baseline_df["country"].astype(str).str.lower() == str(config.filter_country).lower()
        ]
    if config.filter_year:
        baseline_df = baseline_df[
            baseline_df["year"].astype(str) == str(config.filter_year)
        ]

    skipped_verified_count = 0
    if config.skip_verified_records:
        baseline_df, skipped_verified_count = exclude_verified_candidates(
            baseline_df=baseline_df,
            verified_results_file=config.verified_results_file,
        )
        if skipped_verified_count:
            logger.info("Skipped %d already-verified records.", skipped_verified_count)

    # Stable MD5 review key — used for dedup and reconcile across runs
    baseline_df["record_id"] = baseline_df.apply(_review_match_key, axis=1)

    filtered_df = _filter_candidates(baseline_df, config)
    if filtered_df.empty:
        logger.warning("No records passed the filter; exiting without API calls.")
        return False

    # ── Auto-classify known R&D agencies (no AI call needed) ─────────────────
    # Split: known agencies with "include" decision → auto-classify immediately.
    # All "review" rows + unknown include rows → send to AI.
    auto_df, ai_needed_df = _split_auto_classify(filtered_df)
    auto_classified_df = _apply_auto_classification(auto_df)

    n_auto = len(auto_df)
    n_ai = len(ai_needed_df)
    n_include_ai = int((ai_needed_df.get("decision", pd.Series(dtype=str)).str.lower() == "include").sum())
    n_review_ai = int((ai_needed_df.get("decision", pd.Series(dtype=str)).str.lower() == "review").sum())
    logger.info(
        "Auto-classified %d known-agency include rows from English-language countries "
        "(no API call needed — R&D status unambiguous, descriptions already in English). "
        "Sending %d records to AI (include=%d needing translation/Frascati, review=%d).",
        n_auto, n_ai, n_include_ai, n_review_ai,
    )

    # ── Cache setup ───────────────────────────────────────────────────────────
    cache = _load_cache(config.cache_file)
    cache_hits_unified = 0
    cache_entries_to_append: List[dict] = []

    # ── Build AI client ───────────────────────────────────────────────────────
    client_config = AIClientConfig(model=config.model, temperature=config.temperature)
    try:
        client = AIClient(client_config)
    except (MissingAPIKeyError, MissingOpenAIDependencyError) as exc:
        logger.error("AI validation unavailable: %s", exc)
        return False

    # ── Stage 1: Unified row-level validation ────────────────────────────────
    all_results_map: Dict[str, dict] = {}

    def _process_tier(
        tier_df: pd.DataFrame,
        mode: str,
    ) -> tuple[int, int]:
        """Process one validation tier. Returns (cache_hits, api_calls).

        When config.group_by_page=True, records from the same (source_file,
        page_number) are sent in a single batch so the AI can see all lines
        from the same budget page together. This improves subtotal detection
        and double-counting flags without extra cost — batches just become
        page-coherent instead of arbitrary 10-record chunks.

        Page text is included once per batch as a shared 'page_context' field
        on the first record, rather than repeated as a trimmed excerpt on every
        record — saving tokens and giving the AI full context.
        """
        if tier_df.empty:
            return 0, 0

        records = _prepare_records(tier_df, include_context=config.include_context)
        context_map = (
            {r["record_id"]: r.get("raw_page_text_excerpt") for r in records}
            if config.include_context else {}
        )

        record_key_map: Dict[str, str] = {}
        pending: List[dict] = []
        hits = 0

        for record in records:
            ck = _cache_key(
                str(record.get("year") or ""),
                str(record.get("section_code") or ""),
                str(record.get("line_description") or ""),
                record.get("amount_local"),
            )
            record_key_map[record["record_id"]] = ck
            if ck in cache:
                cached_copy = dict(cache[ck])
                cached_copy["record_id"] = record["record_id"]
                all_results_map[record["record_id"]] = cached_copy
                hits += 1
            else:
                pending.append(record)

        api_calls = 0
        if pending:
            # Build page-coherent batches when group_by_page is enabled.
            # Records on the same (source_file, page_number) go into one batch.
            # The page text is attached as a shared 'page_context' on each batch's
            # first record (not repeated per record) to save tokens.
            # Batches larger than batch_size*2 are split to avoid token limits.
            if config.group_by_page:
                page_groups: Dict[str, List[dict]] = {}
                for rec in pending:
                    page_key = (
                        f"{rec.get('source_file', '')}|"
                        f"{rec.get('page_number', 'none')}"
                    )
                    page_groups.setdefault(page_key, []).append(rec)

                precomputed_batches: List[List[dict]] = []
                max_batch = config.batch_size * 2  # cap to avoid token limits
                for page_key, page_recs in page_groups.items():
                    # Attach page context once on the first record of each batch
                    if config.include_context and page_recs:
                        shared_ctx = context_map.get(page_recs[0]["record_id"], "")
                        if shared_ctx:
                            page_recs[0]["page_context"] = shared_ctx
                            # Remove per-record excerpts to avoid repetition
                            for rec in page_recs:
                                rec.pop("raw_page_text_excerpt", None)
                    # Split oversized page groups
                    for i in range(0, len(page_recs), max_batch):
                        precomputed_batches.append(page_recs[i : i + max_batch])

                logger.info(
                    "group_by_page=True: %d pending records → %d page-coherent batches "
                    "(mode=%s).",
                    len(pending), len(precomputed_batches), mode,
                )
            else:
                precomputed_batches = None

            pending_results = run_batches(
                client=client,
                pending_records=pending,
                batch_size=config.batch_size,
                failed_batches_file=config.failed_batches_file,
                mode=mode,
                precomputed_batches=precomputed_batches,
            )
            api_calls = len(pending)
            for result in pending_results:
                rid = result.get("record_id")
                if not rid:
                    continue
                all_results_map[rid] = result
                ck = record_key_map.get(rid)
                if ck:
                    cache_entries_to_append.append({"cache_key": ck, "result": result})
                    cache[ck] = result

        if config.include_context and context_map and not config.group_by_page:
            # In non-page-grouped mode, attach the trimmed excerpt to each result
            for rid, ctx in context_map.items():
                if rid in all_results_map:
                    all_results_map[rid]["raw_page_text_excerpt"] = ctx

        return hits, api_calls

    # Only send the rows that actually need AI judgment
    hits_unified, calls_unified = _process_tier(ai_needed_df, mode="unified")
    cache_hits_unified += hits_unified

    if cache_entries_to_append:
        _append_cache_entries(config.cache_file, cache_entries_to_append)

    logger.info(
        "Stage 1 complete: unified row-validation cache_hits=%d api_calls=%d "
        "(auto-classified=%d skipped AI).",
        cache_hits_unified, calls_unified, n_auto,
    )

    # ── Assemble ordered results (AI results + auto-classified) ───────────────
    ordered_results: List[dict] = []
    for rid in ai_needed_df["record_id"]:
        if rid in all_results_map:
            ordered_results.append(all_results_map[rid])

    ai_raw_df = pd.DataFrame(ordered_results) if ordered_results else pd.DataFrame()

    # Merge auto-classified rows into the result set
    auto_result_cols = [
        "record_id", "keep", "clean_program_code", "clean_program_description_en",
        "validated_amount_local", "frascati_type", "ai_rd_category", "ai_pillar",
        "ai_confidence", "ai_decision", "ai_rationale", "parse_issue",
        "double_counting_risk", "_auto_classified",
    ]
    if not auto_classified_df.empty:
        auto_result_rows = []
        for _, row in auto_classified_df.iterrows():
            entry = {"record_id": row["record_id"]}
            for col in auto_result_cols[1:]:
                entry[col] = row.get(col)
            auto_result_rows.append(entry)
        auto_raw_df = pd.DataFrame(auto_result_rows)
    else:
        auto_raw_df = pd.DataFrame()

    raw_df = pd.concat([ai_raw_df, auto_raw_df], ignore_index=True) if not auto_raw_df.empty else ai_raw_df

    if raw_df.empty:
        logger.warning("AI validation produced no results.")
        return False

    raw_df.to_csv(config.raw_output_file, index=False, encoding="utf-8")

    clean_df = filtered_df.merge(raw_df, on="record_id", how="left", suffixes=("_baseline", ""))
    clean_df.to_csv(config.clean_output_file, index=False, encoding="utf-8")

    # Comparison outputs
    build_comparison(
        baseline_df=baseline_df,
        ai_df=raw_df,
        output_csv_path=config.comparison_file if config.output_format in {"csv", "both"} else None,
        output_jsonl_path=config.comparison_jsonl_file if config.output_format in {"json", "both"} else None,
    )

    # ── Stage 2: Country-year aggregation pass ────────────────────────────────
    agg_output_file = config.cache_file.parent / "aggregation_results.csv"
    agg_cache_file = config.cache_file.parent / "aggregation_cache.jsonl"
    agg_df = pd.DataFrame()

    if config.run_aggregation_pass:
        logger.info("Stage 2: running country-year aggregation pass...")
        agg_df = run_country_year_aggregation(
            validated_df=clean_df,
            client=client,
            cache_file=agg_cache_file,
            failed_batches_file=config.failed_batches_file,
        )
        if not agg_df.empty:
            agg_df.to_csv(agg_output_file, index=False, encoding="utf-8")
            logger.info(
                "Aggregation results written: %d rows → %s",
                len(agg_df), agg_output_file,
            )
    else:
        logger.info("Stage 2 (aggregation pass) skipped — run_aggregation_pass=False.")

    # ── Stage 3: Time-series anomaly detection pass ───────────────────────────
    anomaly_output_file = config.cache_file.parent / "anomaly_flags.csv"
    anomaly_cache_file = config.cache_file.parent / "anomaly_cache.jsonl"
    anomaly_df = pd.DataFrame()

    if config.run_anomaly_pass:
        logger.info("Stage 3: running time-series anomaly detection pass...")
        # Use the full results file so the anomaly detector sees all years,
        # not just those processed in this run.
        full_results_df = pd.DataFrame()
        if RESULTS_CSV_FILE.exists():
            try:
                full_results_df = pd.read_csv(RESULTS_CSV_FILE)
            except Exception as exc:
                logger.warning("Could not read results.csv for anomaly pass: %s", exc)

        anomaly_source = full_results_df if not full_results_df.empty else clean_df
        if config.filter_country and "country" in anomaly_source.columns:
            anomaly_source = anomaly_source[
                anomaly_source["country"].astype(str).str.lower()
                == str(config.filter_country).lower()
            ]

        anomaly_df = run_timeseries_anomaly_detection(
            results_df=anomaly_source,
            client=client,
            cache_file=anomaly_cache_file,
            failed_batches_file=config.failed_batches_file,
        )
        if not anomaly_df.empty:
            anomaly_df.to_csv(anomaly_output_file, index=False, encoding="utf-8")
            logger.info(
                "Anomaly flags written: %d flags → %s",
                len(anomaly_df), anomaly_output_file,
            )
    else:
        logger.info("Stage 3 (anomaly pass) skipped — run_anomaly_pass=False.")

    # ── Summary ───────────────────────────────────────────────────────────────
    _baseline_countries = sorted(baseline_df["country"].dropna().astype(str).unique().tolist()) if "country" in baseline_df.columns else []
    _baseline_years = sorted(baseline_df["year"].dropna().astype(int).unique().tolist()) if "year" in baseline_df.columns else []
    summary = {
        "input_file": str(config.input_file),
        "total_baseline_rows": len(baseline_df),
        "_baseline_countries": _baseline_countries,  # used by stale-cache guard
        "_baseline_year_range": [min(_baseline_years), max(_baseline_years)] if _baseline_years else [],
        "filtered_rows": len(filtered_df),
        "include_tier_records": int((filtered_df.get("decision", pd.Series(dtype=str)).str.lower() == "include").sum()),
        "review_tier_records": int((filtered_df.get("decision", pd.Series(dtype=str)).str.lower() == "review").sum()),
        "unified_row_validation_cache_hits": cache_hits_unified,
        "unified_row_validation_api_calls": calls_unified,
        "skipped_verified_rows": skipped_verified_count,
        "aggregation_pass_ran": config.run_aggregation_pass,
        "aggregation_country_years": len(agg_df) if not agg_df.empty else 0,
        "anomaly_pass_ran": config.run_anomaly_pass,
        "anomaly_flags": len(anomaly_df) if not anomaly_df.empty else 0,
        "model": config.model,
        "batch_size": config.batch_size,
        "raw_output_file": str(config.raw_output_file),
        "clean_output_file": str(config.clean_output_file),
        "aggregation_output_file": str(agg_output_file) if config.run_aggregation_pass else None,
        "anomaly_output_file": str(anomaly_output_file) if config.run_anomaly_pass else None,
        "cache_file": str(config.cache_file),
        "run_name": config.run_name,
        "started_at_utc": start_time.isoformat(),
        "ended_at_utc": datetime.utcnow().isoformat(),
    }

    config.summary_file.parent.mkdir(parents=True, exist_ok=True)
    config.summary_file.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    logger.info("AI validation complete. Summary: %s", config.summary_file)
    return True
