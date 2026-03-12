"""Optional AI validation layer that post-processes extracted candidate records."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd

from src.ai_batch_runner import run_batches
from src.ai_client import AIClient, AIClientConfig, MissingAPIKeyError, MissingOpenAIDependencyError
from src.compare_outputs import build_comparison
from src.config import CANDIDATES_FILE, PROCESSED_DIR, RESULTS_CSV_FILE, RESULTS_JSON_FILE
from src.utils import configure_logging, ensure_directories, logger

BASE_AI_VALIDATION_DIR = PROCESSED_DIR / "ai_validation"

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
    output_format: str = "both"  # csv | json | both
    group_by_page: bool = False
    include_context: bool = False
    run_name: str = "default"
    filter_country: str | None = None
    filter_year: str | int | None = None


FILTER_DECISIONS = {"include", "review"}


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


def _resolve_paths(config: AIValidationConfig):
    """Populate output paths based on run_name if not provided."""
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


def _load_baseline(path: Path) -> pd.DataFrame:
    """Load baseline candidates from CSV or JSON (flattening results.json structure)."""
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

    # Harmonize field names when coming from innovation_candidates.csv
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


def _append_cache_entries(cache_path: Path, entries: List[dict]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("a", encoding="utf-8") as fh:
        for entry in entries:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _normalize_text(value: str) -> str:
    value = value.strip()
    value = re.sub(r"\s+", " ", value)
    value = re.sub(r"\.{2,}", ".", value)
    return value


def _cache_key(section_code: str, line_description: str, amount_local) -> str:
    if pd.isna(amount_local):
        amount_local = ""
    normalized = f"{section_code or ''}|{_normalize_text(line_description or '')}|{amount_local or ''}"
    digest = hashlib.md5(normalized.lower().encode("utf-8")).hexdigest()
    return digest


def _filter_candidates(df: pd.DataFrame, config: AIValidationConfig) -> pd.DataFrame:
    df = df.copy()

    if "line_description" in df.columns:
        df["line_description"] = df["line_description"].fillna("").astype(str)
    if "amount_local" in df.columns:
        df["amount_local"] = pd.to_numeric(df["amount_local"], errors="coerce")
    else:
        df["amount_local"] = pd.NA

    mask_valid_desc = df["line_description"].str.strip() != ""
    if df["amount_local"].notnull().any():
        mask_valid_amount = df["amount_local"].notnull()
    else:
        mask_valid_amount = True
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


def _prepare_records(df: pd.DataFrame, include_context: bool) -> List[dict]:
    records: List[dict] = []
    optional_fields = {"context_before", "context_after", "raw_page_text_excerpt"} if include_context else set()

    def _safe(val):
        if pd.isna(val):
            return None
        return val

    def _trim_context(text: str, anchor: str, before: int = 150, after: int = 100) -> str:
        if not text:
            return ""
        raw = _normalize_text(text)
        anch = _normalize_text(anchor)[:80]  # anchor head
        pos = raw.find(anch) if anch else -1
        if pos == -1:
            # fallback: just return first slice of raw to keep context short
            return raw[: before + after]
        start = max(0, pos - before)
        end = min(len(raw), pos + len(anch) + after)
        return raw[start:end]

    for row in df.itertuples(index=False):
        record = {
            "record_id": _safe(getattr(row, "record_id")),
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
        for field in optional_fields:
            if field in df.columns:
                record[field] = _safe(getattr(row, field, ""))
        # If using innovation_candidates.csv, map text_snippet to context excerpt when context requested
        if include_context and "raw_page_text_excerpt" not in record and "text_snippet" in df.columns:
            record["raw_page_text_excerpt"] = _safe(getattr(row, "text_snippet", ""))

        if include_context:
            # Build a short, focused context around the line_description
            ctx_source = record.get("raw_page_text_excerpt") or ""
            if not ctx_source:
                ctx_source = " ".join(str(record.get(k, "") or "") for k in ("context_before", "line_description", "context_after"))
            record["raw_page_text_excerpt"] = _trim_context(ctx_source, record["line_description"], before=150, after=100)
            # Drop the other noisy context fields to avoid sending long blobs
            record.pop("context_before", None)
            record.pop("context_after", None)
        records.append(record)
    return records


def run_ai_validation(config: AIValidationConfig) -> None:
    configure_logging()
    _resolve_paths(config)

    start_time = datetime.utcnow()
    logger.info("AI validation started with config: %s", asdict(config))

    input_path = config.input_file
    if not input_path.exists():
        logger.error("Input file not found: %s", input_path)
        return

    baseline_df = _load_baseline(input_path)
    if config.filter_country:
        baseline_df = baseline_df[baseline_df["country"].astype(str).str.lower() == str(config.filter_country).lower()]
    if config.filter_year:
        baseline_df = baseline_df[baseline_df["year"].astype(str) == str(config.filter_year)]
    if "record_id" not in baseline_df.columns:
        baseline_df["record_id"] = [
            f"rec_{i:06d}" for i in range(len(baseline_df))
        ]

    filtered_df = _filter_candidates(baseline_df, config)
    if filtered_df.empty:
        logger.warning("No records passed the AI filter; exiting without API calls.")
        return

    cache = _load_cache(config.cache_file)
    cache_hits = 0
    cache_entries_to_append: List[dict] = []

    records = _prepare_records(filtered_df, include_context=config.include_context)
    context_map = {r["record_id"]: r.get("raw_page_text_excerpt") for r in records} if config.include_context else {}

    record_key_map: Dict[str, str] = {}
    pending_records: List[dict] = []
    cached_results: List[dict] = []

    for record in records:
        key = _cache_key(record.get("section_code", ""), record.get("line_description", ""), record.get("amount_local"))
        record_key_map[record["record_id"]] = key
        if key in cache:
            cached_copy = dict(cache[key])
            cached_copy["record_id"] = record["record_id"]
            cached_results.append(cached_copy)
            cache_hits += 1
        else:
            pending_records.append(record)

    ai_results: List[dict] = []
    if pending_records:
        client_config = AIClientConfig(model=config.model, temperature=config.temperature)
        try:
            client = AIClient(client_config)
        except (MissingAPIKeyError, MissingOpenAIDependencyError) as exc:
            logger.error("AI validation unavailable: %s", exc)
            return

        precomputed_batches = None
        if config.group_by_page:
            grouped: Dict[str, List[dict]] = {}
            for rec in pending_records:
                page_key = str(rec.get("page_number")) if rec.get("page_number") is not None else "none"
                grouped.setdefault(page_key, []).append(rec)
            precomputed_batches = list(grouped.values())

        ai_results = run_batches(
            client=client,
            pending_records=pending_records,
            batch_size=config.batch_size,
            failed_batches_file=config.failed_batches_file,
            precomputed_batches=precomputed_batches,
        )

        for result in ai_results:
            rid = result.get("record_id")
            if not rid:
                continue
            key = record_key_map.get(rid)
            if key:
                cache_entries_to_append.append({"cache_key": key, "result": result})
                cache[key] = result

    if cache_entries_to_append:
        _append_cache_entries(config.cache_file, cache_entries_to_append)

    all_results_map: Dict[str, dict] = {res["record_id"]: res for res in cached_results}
    for res in ai_results:
        rid = res.get("record_id")
        if rid:
            all_results_map[rid] = res

    ordered_results: List[dict] = []
    for rid in filtered_df["record_id"]:
        if rid in all_results_map:
            ordered_results.append(all_results_map[rid])

    if not ordered_results:
        logger.warning("AI validation produced no results.")
        return

    raw_df = pd.DataFrame(ordered_results)
    if config.include_context and context_map:
        raw_df["raw_page_text_excerpt"] = raw_df["record_id"].map(context_map)
    raw_df.to_csv(config.raw_output_file, index=False, encoding="utf-8")

    # Preserve baseline structure while appending AI enhancements for side-by-side review
    clean_df = filtered_df.merge(raw_df, on="record_id", how="left", suffixes=("_baseline", ""))
    clean_df.to_csv(config.clean_output_file, index=False, encoding="utf-8")

    # Save comparison in requested formats
    if config.output_format in {"csv", "both"}:
        comparison_csv = config.comparison_file
    else:
        comparison_csv = None

    comparison_jsonl = None
    if config.output_format in {"json", "both"}:
        comparison_jsonl = config.comparison_jsonl_file

    build_comparison(
        baseline_df=baseline_df,
        ai_df=raw_df,
        output_csv_path=comparison_csv,
        output_jsonl_path=comparison_jsonl,
    )

    summary = {
        "input_file": str(config.input_file),
        "total_baseline_rows": len(baseline_df),
        "filtered_rows": len(filtered_df),
        "sent_to_api": len(pending_records),
        "cache_hits": cache_hits,
        "model": config.model,
        "batch_size": config.batch_size,
        "group_by_page": config.group_by_page,
        "raw_output_file": str(config.raw_output_file),
        "clean_output_file": str(config.clean_output_file),
        "comparison_file_csv": str(config.comparison_file) if comparison_csv else None,
        "comparison_file_jsonl": str(config.comparison_jsonl_file) if comparison_jsonl else None,
        "failed_batches_file": str(config.failed_batches_file),
        "cache_file": str(config.cache_file),
        "output_format": config.output_format,
        "include_context": config.include_context,
        "run_name": config.run_name,
        "started_at_utc": start_time.isoformat(),
        "ended_at_utc": datetime.utcnow().isoformat(),
    }

    config.summary_file.parent.mkdir(parents=True, exist_ok=True)
    config.summary_file.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    logger.info("AI validation complete. Raw results: %s", config.raw_output_file)
