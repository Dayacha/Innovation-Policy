"""
Full cross-verification pipeline — runs all three stages in one command.

Stage 1  Check Run A (gpt-4o-mini) — report which surveys are done/missing.
         Optionally run any missing surveys before continuing.
Stage 2  Run B (claude-sonnet-4) extraction — uses oecd_anthropic_key from
         config.yaml, writes to reforms_json_anthropic/. Skips surveys already
         extracted. No changes to config.yaml needed.
Stage 3  Cross-verification — matches reforms between both runs, adjudicates
         disputes with gpt-4o-mini, builds merged panel in output_merged/.

Usage:
    python main.py --reforms-full-pipeline
    python main.py --reforms-full-pipeline --country DNK
    python main.py --reforms-full-pipeline --country DNK --year 2021
    python main.py --reforms-full-pipeline --check-only        # just report status
    python main.py --reforms-full-pipeline --skip-run-a-check  # skip Run A check
    python main.py --reforms-full-pipeline --consensus-only    # no LLM adjudication
"""

from __future__ import annotations

import copy
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

logger = logging.getLogger(__name__)

# Internal pipeline steps — imported lazily to avoid circular imports
def _get_steps():
    from reforms.pipeline_reforms import (
        _step_catalog,
        _step_extract_text,
        _step_analyze_reforms,
        _step_build_panel,
    )
    return _step_catalog, _step_extract_text, _step_analyze_reforms, _step_build_panel


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _count_jsons(directory: Path, country: str | None, year: int | None) -> int:
    """Count JSON files in directory, optionally filtered by country/year prefix."""
    if not directory.exists():
        return 0
    total = 0
    for p in directory.glob("*.json"):
        parts = p.stem.split("_")
        if country and parts[0].upper() != country.upper():
            continue
        if year:
            try:
                if int(parts[1]) != year:
                    continue
            except (IndexError, ValueError):
                continue
        total += 1
    return total


# ---------------------------------------------------------------------------
# Stage 1 — Run A status check
# ---------------------------------------------------------------------------

def check_run_a(config: dict, country: str | None = None, year: int | None = None) -> dict:
    """Check which surveys have a Run A JSON.

    Considers both PDFs (Data/input/surveys/) and pre-extracted text files
    (Data/output/reforms/extracted_text/) as valid survey sources — PDFs may
    have been deleted after text extraction.

    Returns a dict with keys: done, missing, total, missing_files.
    """
    surveys_dir  = Path(config["paths"]["raw_pdfs"])
    text_dir     = Path(config["paths"]["extracted_text"])
    reforms_json = Path(config["paths"]["reforms_json"])

    # Collect all known stems from PDFs and text files
    stems: set[str] = set()
    for p in surveys_dir.glob("*.pdf"):
        stems.add(p.stem)
    if text_dir.exists():
        for p in text_dir.glob("*.txt"):
            stems.add(p.stem)

    # Apply country/year filter
    def _keep(stem: str) -> bool:
        parts = stem.split("_")
        if country and parts[0].upper() != country.upper():
            return False
        if year:
            try:
                return int(parts[1]) == year
            except (IndexError, ValueError):
                return False
        return True

    stems = {s for s in stems if _keep(s)}

    done, missing = [], []
    for stem in sorted(stems):
        if (reforms_json / f"{stem}.json").exists():
            done.append(stem)
        else:
            missing.append(stem)

    return {
        "total":         len(stems),
        "done":          len(done),
        "missing":       len(missing),
        "missing_files": missing,
    }


def _print_run_a_status(status: dict, model: str) -> None:
    print(f"\n── Stage 1: Run A status ({model}) ────────────────────────")
    print(f"  Surveys with PDF:     {status['total']}")
    print(f"  Already extracted:    {status['done']}")
    print(f"  Missing:              {status['missing']}")
    if status["missing_files"]:
        if len(status["missing_files"]) <= 10:
            for f in status["missing_files"]:
                print(f"    - {f}")
        else:
            for f in status["missing_files"][:5]:
                print(f"    - {f}")
            print(f"    ... and {len(status['missing_files']) - 5} more")


# ---------------------------------------------------------------------------
# Stage 2 — Run B config builder
# ---------------------------------------------------------------------------

def _build_run_b_config(config: dict) -> dict:
    """Return a deep copy of config re-targeted at Claude Haiku / Anthropic.

    Uses oecd_anthropic_key from config.yaml. Paths point to the _anthropic suffix.
    No changes are made to config.yaml on disk.
    """
    llm_cfg = config.get("llm", {})
    cv_cfg  = config.get("cross_verification", {})

    # Resolve the Anthropic key: prefer oecd_anthropic_key, fall back to api_key
    anthropic_key = (
        llm_cfg.get("oecd_anthropic_key", "").strip()
        or llm_cfg.get("api_key", "").strip()
    )
    run_b_model = cv_cfg.get("model_b", "claude-haiku-4-5-20251001")
    suffix_b    = cv_cfg.get("run_b_suffix", "anthropic")

    config_b = copy.deepcopy(config)
    config_b["llm"]["provider"]    = "anthropic"
    config_b["llm"]["model"]       = run_b_model
    config_b["llm"]["api_key"]     = anthropic_key
    config_b["llm"]["temperature"] = 0

    base_json   = config["paths"]["reforms_json"]
    base_output = config["paths"]["output"]
    for suf in [f"_{suffix_b}", "_merged"]:
        if base_json.endswith(suf):
            base_json = base_json[: -len(suf)]
        if base_output.endswith(suf):
            base_output = base_output[: -len(suf)]

    config_b["paths"]["reforms_json"] = f"{base_json}_{suffix_b}"
    config_b["paths"]["output"]       = f"{base_output}_{suffix_b}"

    return config_b


# ---------------------------------------------------------------------------
# Stage 2b — Run C config builder (personal GPT key)
# ---------------------------------------------------------------------------

def _build_run_c_config(config: dict) -> dict:
    """Return a deep copy of config re-targeted at GPT-4o-mini using the personal api_key.

    Uses llm.api_key (not oecd_openai_key) — this is the user's personal key.
    Paths point to the _gpt_personal suffix.
    No changes are made to config.yaml on disk.
    """
    llm_cfg = config.get("llm", {})
    cv_cfg  = config.get("cross_verification", {})

    # Use personal api_key — not the OECD shared key
    personal_key = llm_cfg.get("api_key", "").strip()
    run_c_model  = cv_cfg.get("model_c", "gpt-4o-mini")
    suffix_c     = cv_cfg.get("run_c_suffix", "gpt_personal")

    config_c = copy.deepcopy(config)
    config_c["llm"]["provider"]    = "openai"
    config_c["llm"]["model"]       = run_c_model
    config_c["llm"]["api_key"]     = personal_key
    config_c["llm"]["temperature"] = 0

    base_json   = config["paths"]["reforms_json"]
    base_output = config["paths"]["output"]
    for suf in [f"_{suffix_c}", "_merged", f"_{cv_cfg.get('run_b_suffix', 'anthropic')}"]:
        if base_json.endswith(suf):
            base_json = base_json[: -len(suf)]
        if base_output.endswith(suf):
            base_output = base_output[: -len(suf)]

    config_c["paths"]["reforms_json"] = f"{base_json}_{suffix_c}"
    config_c["paths"]["output"]       = f"{base_output}_{suffix_c}"

    return config_c


# ---------------------------------------------------------------------------
# Stage 3 — Cross-verification config builder
# ---------------------------------------------------------------------------

def _build_cross_verify_config(config: dict) -> dict:
    """Return config pointing at merged output, using gpt-4o-mini adjudicator."""
    cv_cfg       = config.get("cross_verification", {})
    suffix_m     = cv_cfg.get("merged_suffix", "merged")
    adj_model    = cv_cfg.get("adjudicator_model", "gpt-4o-mini")
    adj_provider = cv_cfg.get("adjudicator_provider", "openai")
    llm_cfg      = config.get("llm", {})

    openai_key = (
        llm_cfg.get("oecd_openai_key", "").strip()
        or llm_cfg.get("api_key", "").strip()
    )

    config_cv = copy.deepcopy(config)
    config_cv["llm"]["provider"]    = adj_provider
    config_cv["llm"]["model"]       = adj_model
    config_cv["llm"]["api_key"]     = openai_key
    config_cv["llm"]["temperature"] = 0   # always deterministic

    base_output = config["paths"]["output"]
    for suf in [f"_{cv_cfg.get('run_b_suffix','anthropic')}", "_merged"]:
        if base_output.endswith(suf):
            base_output = base_output[: -len(suf)]

    config_cv["paths"]["output"] = f"{base_output}_{suffix_m}"
    return config_cv


# ---------------------------------------------------------------------------
# Cost estimate helpers
# ---------------------------------------------------------------------------

_PRICING = {
    "claude-sonnet-4-20250514":      {"input": 3.00,  "output": 15.00},
    "claude-sonnet-4-5-20250929":    {"input": 3.00,  "output": 15.00},
    "claude-haiku-4-5-20251001":     {"input": 0.80,  "output": 4.00},
    "claude-3-5-haiku-20241022":     {"input": 0.80,  "output": 4.00},
    "claude-3-haiku-20240307":       {"input": 0.25,  "output": 1.25},
    "gpt-4o-mini":                   {"input": 0.15,  "output": 0.60},
    "gpt-4o":                        {"input": 2.50,  "output": 10.00},
}

# Approximate tokens per survey (from benchmark data)
_AVG_INPUT_TOKENS  = 6_100
_AVG_OUTPUT_TOKENS = 580
_AVG_CHUNKS        = 25   # extraction chunks per survey


def _estimate_cost(model: str, n_surveys: int) -> float:
    pricing = _PRICING.get(model, {"input": 3.00, "output": 15.00})
    calls   = n_surveys * _AVG_CHUNKS
    cost    = calls * (
        _AVG_INPUT_TOKENS  * pricing["input"]  / 1_000_000 +
        _AVG_OUTPUT_TOKENS * pricing["output"] / 1_000_000
    )
    return cost


# ---------------------------------------------------------------------------
# Parallel extraction helper
# ---------------------------------------------------------------------------

def _run_extraction(
    config_x: dict,
    country: str | None,
    year: int | None,
    label: str,
    steps: tuple,
) -> None:
    """Run the full extraction pipeline for one model config (B or C).

    Designed to be called from a thread — output from parallel runs will
    interleave, which is expected and harmless.
    """
    _step_catalog, _step_extract_text, _step_analyze_reforms, _step_build_panel = steps
    print(f"\n  [{label}] Starting extraction ...")
    catalog_x = _step_catalog(config_x, country=country)
    _step_extract_text(config_x, catalog_x)
    _step_analyze_reforms(config_x, catalog_x, country=country, year=year)
    _step_build_panel(config_x, country=country)
    print(f"\n  [{label}] Extraction complete.")


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def run_full_pipeline(
    config: dict,
    country:          str | None = None,
    year:             int | None = None,
    check_only:       bool = False,
    skip_run_a_check: bool = False,
    consensus_only:   bool = False,
) -> None:
    """Run all stages: Run A check → Run B (Haiku) → Run C (personal GPT) → cross-verify.

    Stage layout:
      Stage 1  — Run A: gpt-4o-mini via OECD key (skip if already done)
      Stage 2  — Run B: claude-haiku-4-5 via OECD Anthropic key
      Stage 2b — Run C: gpt-4o-mini via personal api_key
      Stage 3  — Three-model cross-verification + panel build
    """
    _step_catalog, _step_extract_text, _step_analyze_reforms, _step_build_panel = _get_steps()

    from reforms.cross_verifier import run_cross_verification, _print_summary
    from reforms.llm_client import LLMClient

    cv_cfg   = config.get("cross_verification", {})
    model_a  = cv_cfg.get("model_a", "gpt-4o-mini")
    model_b  = cv_cfg.get("model_b", "claude-haiku-4-5-20251001")
    model_c  = cv_cfg.get("model_c", "gpt-4o-mini")
    suffix_b = cv_cfg.get("run_b_suffix", "anthropic")
    suffix_c = cv_cfg.get("run_c_suffix", "gpt_personal")
    suffix_m = cv_cfg.get("merged_suffix", "merged")

    # Strip any active suffix from the base paths to get the clean root
    base_json   = config["paths"]["reforms_json"]
    base_output = config["paths"]["output"]
    for suf in [f"_{suffix_b}", f"_{suffix_c}", f"_{suffix_m}"]:
        if base_json.endswith(suf):
            base_json = base_json[: -len(suf)]
        if base_output.endswith(suf):
            base_output = base_output[: -len(suf)]

    dir_a = Path(base_json)
    dir_b = Path(f"{base_json}_{suffix_b}")
    dir_c = Path(f"{base_json}_{suffix_c}")
    dir_m = Path(f"{base_json}_{suffix_m}")
    out_m = Path(f"{base_output}_{suffix_m}")

    # ── Stage 1: Run A status ────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("FULL CROSS-VERIFICATION PIPELINE  (3-model mode)")
    print("=" * 60)

    if not skip_run_a_check:
        status_a = check_run_a(config, country=country, year=year)
        _print_run_a_status(status_a, model_a)

        if status_a["missing"] > 0:
            logger.info("%d surveys missing from Run A — running extraction now ...", status_a["missing"])
            print(f"\n  Running Run A extraction for {status_a['missing']} missing surveys ...")
            catalog_a = _step_catalog(config, country=country)
            _step_extract_text(config, catalog_a)
            _step_analyze_reforms(config, catalog_a, country=country, year=year)
            _step_build_panel(config, country=country)
            status_a = check_run_a(config, country=country, year=year)
            print(f"  Run A complete — {status_a['done']} surveys in {dir_a.name}/")

        if status_a["done"] == 0:
            print("\n  Run A has no extractions after attempting to run — check logs.")
            return
    else:
        status_a = {"done": _count_jsons(dir_a, country, year), "missing": 0}
        print(f"\n── Stage 1: Run A ({model_a}) ─────────────────────────────")
        print(f"  Found {status_a['done']} JSON files in {dir_a.name}/  (check skipped)")

    total_a = status_a["done"]

    if check_only:
        done_b = _count_jsons(dir_b, country, year)
        done_c = _count_jsons(dir_c, country, year)
        done_m = _count_jsons(dir_m, country, year)
        print(f"\n── Stage 2: Run B ({model_b}) ─────────────────────")
        print(f"  Already extracted:  {done_b} / {total_a}  (~${_estimate_cost(model_b, max(0,total_a-done_b)):.2f} remaining)")
        print(f"\n── Stage 2b: Run C ({model_c}, personal key) ──────────────")
        print(f"  Already extracted:  {done_c} / {total_a}  (~${_estimate_cost(model_c, max(0,total_a-done_c)):.2f} remaining)")
        print(f"\n── Stage 3: Cross-verification (3-model) ───────────────────")
        print(f"  Merged JSONs done:  {done_m}")
        print()
        return

    # ── Stages 2 + 2b: Run B and Run C extraction ───────────────────────────
    config_b    = _build_run_b_config(config)
    config_c    = _build_run_c_config(config)
    done_b_pre  = _count_jsons(dir_b, country, year)
    done_c_pre  = _count_jsons(dir_c, country, year)
    remaining_b = max(0, total_a - done_b_pre)
    remaining_c = max(0, total_a - done_c_pre)

    print(f"\n── Stage 2: Run B ({model_b}) ─────────────")
    print(f"  Output dir:         {dir_b.name}/")
    print(f"  Already extracted:  {done_b_pre} / {total_a}  (~${_estimate_cost(model_b, remaining_b):.2f} remaining)")
    print(f"\n── Stage 2b: Run C ({model_c}, personal key) ──────────────")
    print(f"  Output dir:         {dir_c.name}/")
    print(f"  Already extracted:  {done_c_pre} / {total_a}  (~${_estimate_cost(model_c, remaining_c):.2f} remaining)")

    steps = (_step_catalog, _step_extract_text, _step_analyze_reforms, _step_build_panel)

    # Build the work queue — only runs that still need extraction
    pending = {}
    if remaining_b > 0:
        pending["Run B"] = (config_b, remaining_b)
    if remaining_c > 0:
        pending["Run C"] = (config_c, remaining_c)

    if not pending:
        print("\n  Both Run B and Run C already complete — skipping extraction.")
    elif len(pending) == 1:
        # Only one run needed — execute in the main thread (simpler output)
        label, (cfg, _) = next(iter(pending.items()))
        print(f"\n  Running {label} (sequential — other run already done) ...")
        _run_extraction(cfg, country, year, label, steps)
    else:
        # Both needed — run in parallel (output will interleave, that's expected)
        print(f"\n  Running Run B and Run C in parallel ...")
        print(f"  (output from both models will interleave — this is normal)\n")
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(_run_extraction, cfg, country, year, lbl, steps): lbl
                for lbl, (cfg, _) in pending.items()
            }
            for future in as_completed(futures):
                lbl = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    print(f"\n  [{lbl}] FAILED: {exc}")
                    logger.error("%s extraction failed: %s", lbl, exc)

    print(f"\n  Run B:  {_count_jsons(dir_b, country, year)} surveys in {dir_b.name}/")
    print(f"  Run C:  {_count_jsons(dir_c, country, year)} surveys in {dir_c.name}/")

    # ── Stage 3: Three-model cross-verification ──────────────────────────────
    config_cv = _build_cross_verify_config(config)
    adj_model = cv_cfg.get("adjudicator_model", "gpt-4o-mini")
    threshold = float(cv_cfg.get("similarity_threshold", 0.55))

    # Decide two-model vs three-model based on whether Run C produced output
    use_three_model = dir_c.exists() and any(dir_c.glob("*.json"))

    print(f"\n── Stage 3: Cross-verification ({'3-model' if use_three_model else '2-model'}) ──────────")
    print(f"  Run A:              {dir_a.name}/  ({model_a})")
    print(f"  Run B:              {dir_b.name}/  ({model_b})")
    if use_three_model:
        print(f"  Run C:              {dir_c.name}/  ({model_c})")
    print(f"  Adjudicator:        {adj_model}")
    print(f"  Similarity threshold: {threshold}")
    print(f"  Output:             {dir_m.name}/")

    if not dir_b.exists() or not any(dir_b.glob("*.json")):
        print("  Run B directory is empty — cannot cross-verify.")
        return

    adj_client = None
    if not consensus_only:
        adj_client = LLMClient(
            config_cv,
            usage_file=out_m / "cross_verifier_llm_usage.json",
        )

    summary = run_cross_verification(
        dir_a=dir_a,
        dir_b=dir_b,
        output_dir=dir_m,
        model_a=model_a,
        model_b=model_b,
        threshold=threshold,
        client=adj_client,
        consensus_only=consensus_only,
        country=country,
        year=year,
        dir_c=dir_c if use_three_model else None,
        model_c=model_c if use_three_model else None,
    )

    if adj_client:
        adj_client.save_usage()

    _print_summary(summary)

    logger.info("Building merged panel ...")
    _step_build_panel(config_cv, country=country)

    print(f"\n  Merged panel written to: {out_m}/")
    print("=" * 60 + "\n")
