"""Run isolated reform-extraction benchmarks across multiple LLM models.

This module is intentionally separate from the main reform pipeline so model
tests do not overwrite the canonical outputs in ``Data/output/reforms/output``.

Example:
    python -m reforms.model_benchmark \
      --run-name pilot_20260330 \
      --model openai:gpt-4o-mini \
      --model openai:gpt-4o \
      --model openai:gpt-5-mini \
      --model openai:gpt-5 \
      --model anthropic:claude-sonnet-4-20250514 \
      --model anthropic:claude-opus-4-20250514
"""

from __future__ import annotations

import argparse
import copy
import json
import logging
import re
from pathlib import Path

import pandas as pd

from reforms.pipeline_reforms import (
    _step_analyze_reforms,
    _step_build_panel,
    _step_catalog,
    _step_extract_text,
    load_reforms_config,
)
from reforms.reform_analyzer import ReformAnalyzer


PROJECT_ROOT = Path(__file__).resolve().parent.parent
BENCHMARK_ROOT = PROJECT_ROOT / "Data" / "output" / "reforms" / "benchmarks"
logger = logging.getLogger(__name__)

MODEL_PRESETS: dict[str, tuple[str, str]] = {
    "gpt4o-mini": ("openai", "gpt-4o-mini"),
    "gpt4o": ("openai", "gpt-4o"),
    "gpt5-mini": ("openai", "gpt-5-mini"),
    "gpt5": ("openai", "gpt-5"),
    "claude-sonnet-4": ("anthropic", "claude-sonnet-4-20250514"),
    "claude-opus-4": ("anthropic", "claude-opus-4-20250514"),
}


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9._-]+", "-", value.strip())
    return slug.strip("-").lower()


def _resolve_model_spec(raw: str) -> tuple[str, str]:
    preset = MODEL_PRESETS.get(raw.strip().lower())
    if preset:
        return preset
    if ":" not in raw:
        raise ValueError(
            f"Invalid model spec '{raw}'. Use provider:model or a preset like gpt4o-mini."
        )
    provider, model = raw.split(":", 1)
    provider = provider.strip().lower()
    model = model.strip()
    if provider not in {"openai", "anthropic"}:
        raise ValueError(f"Unsupported provider '{provider}' in model spec '{raw}'.")
    if not model:
        raise ValueError(f"Missing model name in model spec '{raw}'.")
    return provider, model


def _build_benchmark_config(base_config: dict, *, provider: str, model: str, run_dir: Path) -> dict:
    cfg = copy.deepcopy(base_config)
    cfg["llm"]["provider"] = provider
    cfg["llm"]["model"] = model
    cfg["paths"]["output"] = str(run_dir / "output")
    cfg["paths"]["reforms_json"] = str(run_dir / "reforms_json")
    Path(cfg["paths"]["output"]).mkdir(parents=True, exist_ok=True)
    Path(cfg["paths"]["reforms_json"]).mkdir(parents=True, exist_ok=True)
    return cfg


def _apply_target_filters(config: dict, *, country: str | None, year: int | None) -> None:
    if country:
        config["countries"] = [country]
    if year is not None:
        config["year_range"] = {"start": int(year), "end": int(year)}


def _collect_run_summary(run_dir: Path, *, provider: str, model: str) -> dict:
    output_dir = run_dir / "output"
    json_dir = run_dir / "reforms_json"
    mentions_path = output_dir / "reforms_mentions.csv"
    events_path = output_dir / "reforms_events.csv"
    panel_path = output_dir / "reform_panel.csv"
    usage_path = output_dir / "llm_usage.json"

    mentions_rows = events_rows = panel_rows = json_files = 0
    total_input_tokens = total_output_tokens = total_tokens = total_calls = 0
    total_cost = 0.0

    if mentions_path.exists():
        mentions_rows = len(pd.read_csv(mentions_path))
    if events_path.exists():
        events_rows = len(pd.read_csv(events_path))
    if panel_path.exists():
        panel_rows = len(pd.read_csv(panel_path))
    if json_dir.exists():
        json_files = len(list(json_dir.glob("*.json")))
    if usage_path.exists():
        payload = json.loads(usage_path.read_text(encoding="utf-8"))
        records = payload.get("records", [])
        total_calls = len(records)
        total_input_tokens = sum(int(r.get("input_tokens", 0) or 0) for r in records)
        total_output_tokens = sum(int(r.get("output_tokens", 0) or 0) for r in records)
        total_tokens = total_input_tokens + total_output_tokens
        total_cost = round(sum(float(r.get("cost_usd", 0) or 0) for r in records), 6)

    return {
        "provider": provider,
        "model": model,
        "run_dir": str(run_dir),
        "json_files": json_files,
        "mentions_rows": mentions_rows,
        "events_rows": events_rows,
        "panel_rows": panel_rows,
        "llm_calls": total_calls,
        "input_tokens": total_input_tokens,
        "output_tokens": total_output_tokens,
        "total_tokens": total_tokens,
        "cost_usd": total_cost,
    }


def _resolve_existing_text_path(config: dict, *, country: str | None, year: int | None) -> Path | None:
    if not country or year is None:
        return None
    text_dir = Path(config["paths"]["extracted_text"])
    candidate = text_dir / f"{country}_{year}.txt"
    return candidate if candidate.exists() else None


def _run_from_existing_text(config: dict, *, country: str, year: int) -> None:
    text_path = _resolve_existing_text_path(config, country=country, year=year)
    if text_path is None:
        raise RuntimeError(
            f"No existing extracted text found for benchmark target {country}_{year}. "
            f"Expected: {Path(config['paths']['extracted_text']) / f'{country}_{year}.txt'}"
        )
    logger.info("Using existing extracted text: %s", text_path)
    text = text_path.read_text(encoding="utf-8", errors="ignore")
    analyzer = ReformAnalyzer(config)
    analyzer.analyze_survey(text, country, year)
    analyzer.llm.save_usage()
    analyzer.llm.print_usage_report()
    _step_build_panel(config)


def run_benchmark(
    *,
    config_path: str,
    run_name: str,
    models: list[str],
    country: str | None = None,
    year: int | None = None,
) -> Path:
    base_config = load_reforms_config(config_path)
    if base_config is None:
        raise ValueError(f"Could not load reforms config from {config_path}")

    benchmark_dir = BENCHMARK_ROOT / _slugify(run_name)
    benchmark_dir.mkdir(parents=True, exist_ok=True)
    summary_rows: list[dict] = []

    for raw_model in models:
        provider, model = _resolve_model_spec(raw_model)
        model_slug = _slugify(f"{provider}_{model}")
        run_dir = benchmark_dir / model_slug
        logger.info("=== Benchmark run: %s / %s ===", provider, model)
        config = _build_benchmark_config(base_config, provider=provider, model=model, run_dir=run_dir)
        _apply_target_filters(config, country=country, year=year)

        manifest = {
            "provider": provider,
            "model": model,
            "country": country,
            "year": year,
            "paths": config["paths"],
        }
        (run_dir / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        existing_text = _resolve_existing_text_path(config, country=country, year=year)
        if existing_text is not None:
            _run_from_existing_text(config, country=country, year=int(year))
        else:
            catalog = _step_catalog(config, country=country)
            candidate_surveys = catalog.get_surveys_with_pdfs()
            if country:
                candidate_surveys = [s for s in candidate_surveys if s["country_code"] == country]
            if year is not None:
                candidate_surveys = [s for s in candidate_surveys if s["year"] == int(year)]
            if not candidate_surveys:
                raise RuntimeError(
                    f"No matching survey with PDF or extracted text found for benchmark target country={country!r}, year={year!r}"
                )
            _step_extract_text(config, catalog)
            _step_analyze_reforms(config, catalog, country=country, year=year)
            _step_build_panel(config)
        summary_rows.append(_collect_run_summary(run_dir, provider=provider, model=model))

    summary_df = pd.DataFrame(summary_rows).sort_values(["provider", "model"]).reset_index(drop=True)
    summary_path = benchmark_dir / "benchmark_summary.csv"
    summary_df.to_csv(summary_path, index=False, encoding="utf-8")
    logger.info("Benchmark summary saved: %s", summary_path)
    return summary_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Run isolated reform-extraction model benchmarks")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--run-name", required=True, help="Benchmark run folder name")
    parser.add_argument(
        "--model",
        action="append",
        required=True,
        help="Model spec as provider:model or preset (repeat flag for multiple models)",
    )
    parser.add_argument("--country", default=None, help="Optional ISO3 country filter")
    parser.add_argument("--year", type=int, default=None, help="Optional survey year filter")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    summary_path = run_benchmark(
        config_path=args.config,
        run_name=args.run_name,
        models=args.model,
        country=args.country,
        year=args.year,
    )
    print(summary_path)


if __name__ == "__main__":
    main()
