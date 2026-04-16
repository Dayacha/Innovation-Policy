"""
Cross-Verification — Strategy B: Two-Model Merger
==================================================

Compares reform extractions from two independent model runs (e.g. gpt-4o-mini
and claude-sonnet-4), matches reforms that both models found, adjudicates
reforms found by only one model, and writes a merged output ready for the
panel builder.

Flow
----
Run A (primary)   reforms_json/           e.g. gpt-4o-mini
Run B (secondary) reforms_json_anthropic/ e.g. claude-sonnet-4
                         ↓
              cross_verifier.py
                         ↓
  Per survey:
    consensus   — both models found the reform → keep, merge fields
    run_a_only  — only primary found it        → LLM adjudicates
    run_b_only  — only secondary found it      → LLM adjudicates
                         ↓
              reforms_json_merged/   (merged JSONs, drop-in for panel builder)
              output_merged/         (final CSVs via panel builder)

Each merged reform gets three extra fields:
  cross_verification_status  "consensus" | "disputed_included" | "disputed_excluded"
  found_by_models            list of model names that extracted the reform
  cross_verification_note    LLM rationale for disputed decisions (empty for consensus)

Usage
-----
  python -m reforms.cross_verifier
  python -m reforms.cross_verifier --config config.yaml
  python -m reforms.cross_verifier --country DNK --year 2021
  python -m reforms.cross_verifier --consensus-only   # skip adjudication, keep only consensus
  python -m reforms.cross_verifier --build-panel-only # re-run panel builder on existing merged JSONs
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from difflib import SequenceMatcher
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from reforms.llm_client import LLMClient                      # noqa: E402
from reforms.panel_builder import PanelBuilder                 # noqa: E402
from reforms.pipeline_reforms import load_reforms_config       # noqa: E402

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Similarity helper (shared with adjudicator / panel_builder)
# ---------------------------------------------------------------------------

def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


# ---------------------------------------------------------------------------
# Reform matching
# ---------------------------------------------------------------------------

def _match_reforms(
    reforms_a: list[dict],
    reforms_b: list[dict],
    threshold: float = 0.55,
) -> tuple[list[tuple[dict, dict]], list[dict], list[dict]]:
    """Match reforms from two model runs by description similarity.

    Returns
    -------
    consensus  : list of (reform_a, reform_b) pairs above threshold
    a_only     : reforms from run A with no match in run B
    b_only     : reforms from run B with no match in run A
    """
    used_b = set()
    consensus = []
    a_only = []

    for ra in reforms_a:
        desc_a = str(ra.get("description", "") or "")
        best_score = 0.0
        best_j = -1
        for j, rb in enumerate(reforms_b):
            if j in used_b:
                continue
            desc_b = str(rb.get("description", "") or "")
            score = _similarity(desc_a, desc_b)
            if score > best_score:
                best_score = score
                best_j = j
        if best_score >= threshold and best_j >= 0:
            consensus.append((ra, reforms_b[best_j]))
            used_b.add(best_j)
        else:
            a_only.append(ra)

    b_only = [rb for j, rb in enumerate(reforms_b) if j not in used_b]
    return consensus, a_only, b_only


def _merge_pair(ra: dict, rb: dict, model_a: str, model_b: str) -> dict:
    """Merge two matched reforms, preferring non-null fields; prefer A for ties."""
    merged = dict(ra)
    # Fill nulls in A with values from B
    for key, val in rb.items():
        if key in ("description", "source_quote"):
            # Keep A's description; use longer source_quote
            if len(str(val or "")) > len(str(merged.get(key) or "")):
                merged[key] = val
        elif merged.get(key) is None and val is not None:
            merged[key] = val
    merged["cross_verification_status"] = "consensus"
    merged["found_by_models"] = [model_a, model_b]
    merged["cross_verification_note"] = ""
    return merged


def _tag_disputed(reform: dict, model_name: str, status: str, note: str) -> dict:
    """Attach cross-verification metadata to a disputed reform."""
    r = dict(reform)
    r["cross_verification_status"] = status
    r["found_by_models"] = [model_name]
    r["cross_verification_note"] = note
    return r


# ---------------------------------------------------------------------------
# LLM adjudication of disputed reforms
# ---------------------------------------------------------------------------

_ADJUDICATION_SYSTEM_PROMPT = """\
You are a senior OECD science and technology policy analyst performing a
cross-verification step. You are given reform descriptions that were extracted
by one LLM model but NOT by a second model running on the same survey text.

Your task: decide whether each reform should be INCLUDED in or EXCLUDED from
the final dataset.

INCLUDE when the reform's primary purpose is one of:
• Direct public R&D funding (grants, research councils, competitive programmes)
• Innovation instruments for firms (R&D tax credits, grants, vouchers)
• Research infrastructure (labs, science parks, HPC, research data centres)
• Knowledge transfer (TTOs, spinoffs, university-industry links, patents)
• Human capital for R&D (doctoral programmes, researcher mobility, fellowships)
• Startup/venture ecosystem specifically for deep-tech / R&D-intensive firms
• Sectoral / mission R&D (health, energy, climate, AI, space, quantum, defence)

EXCLUDE when:
• The primary mechanism is capital deployment, infrastructure rollout, or green
  finance — not R&D funding or knowledge transfer.
• The text only contains an OECD recommendation ("should", "could") with no
  government adoption signal.
• The reform concerns general digitalisation, VET, SME finance, or physical
  infrastructure unrelated to research.
• The description and source_quote are too vague to confirm R&D relevance.

Be conservative: when genuinely uncertain, exclude.

Return a JSON array — one object per reform, in input order:
[
  {
    "reform_id": "<id>",
    "decision": "include" | "exclude",
    "rationale": "<1-2 sentences citing specific words from description/quote>"
  },
  ...
]
Return valid JSON only — no markdown, no text outside the array.
"""


def _build_adjudication_prompt(disputes: list[dict]) -> str:
    rows = [
        {
            "reform_id":   r.get("_dispute_id", str(i)),
            "found_by":    r.get("_found_by", "unknown"),
            "sub_theme":   r.get("sub_theme", ""),
            "description": str(r.get("description", "") or "")[:600],
            "source_quote": str(r.get("source_quote", "") or "")[:400],
        }
        for i, r in enumerate(disputes)
    ]
    return (
        f"Adjudicate these {len(rows)} disputed reforms "
        f"(found by one model only):\n\n"
        + json.dumps(rows, ensure_ascii=False, indent=2)
    )


def _parse_adjudication_response(text: str, n: int) -> list[dict]:
    """Parse the LLM adjudication response; fall back to 'exclude' on error."""
    safe = [{"decision": "exclude", "rationale": "parse_error"} for _ in range(n)]
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        import re
        m = re.search(r"\[[\s\S]*\]", text)
        if m:
            try:
                parsed = json.loads(m.group(0))
            except json.JSONDecodeError:
                return safe
        else:
            return safe
    if not isinstance(parsed, list):
        return safe
    results = []
    for item in parsed[:n]:
        results.append({
            "decision":  item.get("decision", "exclude"),
            "rationale": item.get("rationale", ""),
        })
    # Pad if short
    while len(results) < n:
        results.append({"decision": "exclude", "rationale": "missing_from_response"})
    return results


def _adjudicate_disputes(
    disputes: list[dict],
    client: LLMClient,
    batch_size: int = 15,
) -> list[dict]:
    """Run LLM adjudication in batches. Returns list of reform dicts with
    cross_verification_status set to 'disputed_included' or 'disputed_excluded'."""
    if not disputes:
        return []

    results = []
    for i in range(0, len(disputes), batch_size):
        batch = disputes[i : i + batch_size]
        prompt = _build_adjudication_prompt(batch)
        try:
            response = client.call(
                system_prompt=_ADJUDICATION_SYSTEM_PROMPT,
                user_prompt=prompt,
                operation="cross_verification_adjudication",
            )
            decisions = _parse_adjudication_response(response, len(batch))
        except Exception as exc:
            logger.warning("Adjudication batch failed: %s — defaulting to exclude", exc)
            decisions = [{"decision": "exclude", "rationale": "api_error"} for _ in batch]

        for reform, dec in zip(batch, decisions):
            r = dict(reform)
            # Remove internal bookkeeping keys
            r.pop("_dispute_id", None)
            r.pop("_found_by", None)
            if dec["decision"] == "include":
                r["cross_verification_status"] = "disputed_included"
            else:
                r["cross_verification_status"] = "disputed_excluded"
            r["cross_verification_note"] = dec["rationale"]
            results.append(r)

    return results


# ---------------------------------------------------------------------------
# Per-survey merge
# ---------------------------------------------------------------------------

def _merge_survey(
    json_a: dict,
    json_b: dict,
    model_a: str,
    model_b: str,
    threshold: float,
    client: LLMClient | None,
    consensus_only: bool,
) -> dict:
    """Merge two survey JSONs into one. Returns a merged JSON dict."""
    reforms_a = json_a.get("reforms", []) or []
    reforms_b = json_b.get("reforms", []) or []

    consensus_pairs, a_only, b_only = _match_reforms(reforms_a, reforms_b, threshold)

    # Consensus reforms
    merged_reforms = [_merge_pair(ra, rb, model_a, model_b) for ra, rb in consensus_pairs]

    if not consensus_only and client is not None:
        # Tag disputed reforms with internal bookkeeping keys
        disputes = []
        for i, r in enumerate(a_only):
            r2 = dict(r)
            r2["_dispute_id"] = f"a_{i}"
            r2["_found_by"] = model_a
            disputes.append(r2)
        for i, r in enumerate(b_only):
            r2 = dict(r)
            r2["_dispute_id"] = f"b_{i}"
            r2["_found_by"] = model_b
            disputes.append(r2)

        adjudicated = _adjudicate_disputes(disputes, client)
        merged_reforms.extend(adjudicated)
    else:
        # Consensus-only: tag unmatched as excluded without LLM
        for r in a_only:
            merged_reforms.append(_tag_disputed(r, model_a, "disputed_excluded",
                                                "consensus_only_mode"))
        for r in b_only:
            merged_reforms.append(_tag_disputed(r, model_b, "disputed_excluded",
                                                "consensus_only_mode"))

    # Build final JSON, preserving metadata from run A
    result = {
        "country_code": json_a.get("country_code"),
        "country_name": json_a.get("country_name"),
        "survey_year":  json_a.get("survey_year"),
        "cross_verification": {
            "model_a":     model_a,
            "model_b":     model_b,
            "total_a":     len(reforms_a),
            "total_b":     len(reforms_b),
            "consensus":   len(consensus_pairs),
            "a_only":      len(a_only),
            "b_only":      len(b_only),
            "threshold":   threshold,
        },
        "reforms": [r for r in merged_reforms
                    if r.get("cross_verification_status") != "disputed_excluded"],
        "all_reforms_including_excluded": merged_reforms,
    }
    return result


# ---------------------------------------------------------------------------
# Directory-level runner
# ---------------------------------------------------------------------------

def run_cross_verification(
    dir_a: Path,
    dir_b: Path,
    output_dir: Path,
    model_a: str,
    model_b: str,
    threshold: float,
    client: LLMClient | None,
    consensus_only: bool,
    country: str | None = None,
    year: int | None = None,
) -> dict:
    """Process all matching survey JSONs between two directories.

    Returns a summary dict with counts.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    files_a = {f.name: f for f in dir_a.glob("*.json")}
    files_b = {f.name: f for f in dir_b.glob("*.json")}
    common = sorted(set(files_a) & set(files_b))
    only_a = sorted(set(files_a) - set(files_b))
    only_b = sorted(set(files_b) - set(files_a))

    # Filter by country / year if requested
    def _matches_filter(fname: str) -> bool:
        stem = fname.replace(".json", "")
        parts = stem.split("_")
        if country and parts[0].upper() != country.upper():
            return False
        if year:
            try:
                if int(parts[1]) != year:
                    return False
            except (IndexError, ValueError):
                return False
        return True

    common = [f for f in common if _matches_filter(f)]

    summary = {
        "surveys_both": len(common),
        "surveys_a_only": len(only_a),
        "surveys_b_only": len(only_b),
        "total_consensus": 0,
        "total_a_only": 0,
        "total_b_only": 0,
        "total_disputed_included": 0,
        "total_disputes": 0,
    }

    logger.info(
        "Cross-verification: %d surveys in both runs, %d only in A, %d only in B",
        len(common), len(only_a), len(only_b),
    )

    for fname in common:
        try:
            with open(files_a[fname], encoding="utf-8") as f:
                json_a = json.load(f)
            with open(files_b[fname], encoding="utf-8") as f:
                json_b = json.load(f)
        except Exception as exc:
            logger.warning("Could not load %s: %s", fname, exc)
            continue

        logger.info("Merging %s ...", fname)
        merged = _merge_survey(
            json_a, json_b, model_a, model_b,
            threshold, client, consensus_only,
        )

        cv = merged.get("cross_verification", {})
        summary["total_consensus"]  += cv.get("consensus", 0)
        summary["total_a_only"]     += cv.get("a_only", 0)
        summary["total_b_only"]     += cv.get("b_only", 0)
        disputes = cv.get("a_only", 0) + cv.get("b_only", 0)
        summary["total_disputes"]   += disputes

        included = sum(
            1 for r in merged.get("all_reforms_including_excluded", [])
            if r.get("cross_verification_status") == "disputed_included"
        )
        summary["total_disputed_included"] += included

        out_path = output_dir / fname
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False, indent=2)

    # Copy surveys only present in run A (no match in B) — pass through as-is
    for fname in only_a:
        if not _matches_filter(fname):
            continue
        try:
            with open(files_a[fname], encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        data["cross_verification"] = {
            "model_a": model_a, "model_b": None,
            "note": "only_in_run_a — no corresponding run B file",
        }
        for r in data.get("reforms", []):
            r.setdefault("cross_verification_status", "run_a_only")
            r.setdefault("found_by_models", [model_a])
            r.setdefault("cross_verification_note", "no_run_b_file")
        with open(output_dir / fname, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _print_summary(summary: dict) -> None:
    print("\n" + "=" * 60)
    print("CROSS-VERIFICATION SUMMARY")
    print("=" * 60)
    print(f"Surveys processed (both runs):  {summary['surveys_both']}")
    print(f"Surveys only in run A:          {summary['surveys_a_only']}")
    print(f"Surveys only in run B:          {summary['surveys_b_only']}")
    print()
    print(f"Reforms in consensus:           {summary['total_consensus']}")
    print(f"Disputed (run A only):          {summary['total_a_only']}")
    print(f"Disputed (run B only):          {summary['total_b_only']}")
    print(f"  → adjudicated as included:   {summary['total_disputed_included']}")
    print(f"  → adjudicated as excluded:   "
          f"{summary['total_disputes'] - summary['total_disputed_included']}")
    print("=" * 60 + "\n")


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Cross-verify two reform extraction runs and produce a merged dataset."
    )
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--country", help="Process only this country code (e.g. DNK)")
    parser.add_argument("--year", type=int, help="Process only this survey year")
    parser.add_argument(
        "--consensus-only", action="store_true",
        help="Skip LLM adjudication — only keep reforms found by both models",
    )
    parser.add_argument(
        "--build-panel-only", action="store_true",
        help="Skip merging — re-run panel builder on existing merged JSONs",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    config = load_reforms_config(args.config)
    if config is None:
        sys.exit("Could not load config.yaml")

    cv_cfg = config.get("cross_verification", {})

    # Resolve directories
    base_json  = Path(config["paths"]["reforms_json"])
    suffix_b   = cv_cfg.get("run_b_suffix", "anthropic").strip()
    suffix_m   = cv_cfg.get("merged_suffix", "merged").strip()

    # Strip any existing suffix from base_json to get the true base path
    # (in case load_reforms_config already applied output_suffix)
    base_str = str(base_json)
    for suf in [f"_{suffix_b}", f"_{suffix_m}"]:
        if base_str.endswith(suf):
            base_str = base_str[: -len(suf)]
    base_json = Path(base_str)

    dir_a   = base_json
    dir_b   = Path(f"{base_json}_{suffix_b}")
    dir_m   = Path(f"{base_json}_{suffix_m}")
    out_dir = Path(f"{config['paths']['output']}_{suffix_m}")

    model_a    = cv_cfg.get("model_a", "gpt-4o-mini")
    model_b    = cv_cfg.get("model_b", "claude-sonnet-4-20250514")
    threshold  = float(cv_cfg.get("similarity_threshold", 0.55))
    adj_model  = cv_cfg.get("adjudicator_model", "gpt-4o-mini")

    if not dir_a.exists():
        sys.exit(f"Run A directory not found: {dir_a}")
    if not dir_b.exists() and not args.build_panel_only:
        sys.exit(
            f"Run B directory not found: {dir_b}\n"
            f"Run the second extraction first with output_suffix: \"{suffix_b}\" in config.yaml"
        )

    # ------------------------------------------------------------------
    # Build panel only
    # ------------------------------------------------------------------
    if args.build_panel_only:
        logger.info("Building panel from merged JSONs at %s ...", dir_m)
        panel_config = dict(config)
        panel_config["paths"] = dict(config["paths"])
        panel_config["paths"]["reforms_json"] = str(dir_m)
        panel_config["paths"]["output"]       = str(out_dir)
        builder = PanelBuilder(panel_config)
        builder.build_panel()
        logger.info("Panel written to %s", out_dir)
        return

    # ------------------------------------------------------------------
    # Set up adjudicator LLM client
    # ------------------------------------------------------------------
    client = None
    if not args.consensus_only:
        adj_config = dict(config)
        adj_config["llm"] = dict(config["llm"])
        adj_config["llm"]["model"] = adj_model
        # Adjudicator uses whichever provider supports the adjudicator model
        # (defaults to the same provider; override via cross_verification.adjudicator_provider)
        adj_provider = cv_cfg.get("adjudicator_provider", "").strip()
        if adj_provider:
            adj_config["llm"]["provider"] = adj_provider
        adj_config["paths"] = dict(config["paths"])
        adj_config["paths"]["output"] = str(out_dir)
        client = LLMClient(
            adj_config,
            usage_file=out_dir / "cross_verifier_llm_usage.json",
        )

    # ------------------------------------------------------------------
    # Run cross-verification
    # ------------------------------------------------------------------
    logger.info("Run A: %s  (%s)", dir_a, model_a)
    logger.info("Run B: %s  (%s)", dir_b, model_b)
    logger.info("Output: %s", dir_m)
    logger.info("Similarity threshold: %.2f", threshold)
    logger.info("Mode: %s", "consensus-only" if args.consensus_only else "full (with adjudication)")

    summary = run_cross_verification(
        dir_a=dir_a,
        dir_b=dir_b,
        output_dir=dir_m,
        model_a=model_a,
        model_b=model_b,
        threshold=threshold,
        client=client,
        consensus_only=args.consensus_only,
        country=args.country,
        year=args.year,
    )

    _print_summary(summary)

    if client is not None:
        client.save_usage()

    # ------------------------------------------------------------------
    # Build panel from merged JSONs
    # ------------------------------------------------------------------
    logger.info("Building panel from merged JSONs ...")
    panel_config = dict(config)
    panel_config["paths"] = dict(config["paths"])
    panel_config["paths"]["reforms_json"] = str(dir_m)
    panel_config["paths"]["output"]       = str(out_dir)
    builder = PanelBuilder(panel_config)
    builder.build_panel()
    logger.info("Panel written to %s", out_dir)


if __name__ == "__main__":
    main()