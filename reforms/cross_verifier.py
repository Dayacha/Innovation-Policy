"""
Cross-Verification — Three-Model Merger
========================================

Compares reform extractions from two or three independent model runs and
merges them into a single high-quality dataset reviewed by an LLM adjudicator.

Flow (three-model mode)
-----------------------
Run A (primary)    reforms_json/                e.g. gpt-4o-mini (OECD key)
Run B (secondary)  reforms_json_anthropic/      e.g. claude-haiku-4-5
Run C (tertiary)   reforms_json_gpt_personal/   e.g. gpt-4o-mini (personal key)
                              ↓
                   cross_verifier.py
                              ↓
  Per survey, reforms grouped by how many models found them:
    all_three_models   — all 3 found it → very strong signal
    two_of_three_models — 2 of 3 found it → moderate signal
    one_of_three_models — only 1 found it → conservative, lean to exclude
                              ↓
  All reforms sent to LLM adjudicator in batches (with agreement field)
                              ↓
              reforms_json_merged/   (merged JSONs, drop-in for panel builder)
              output_merged/         (final CSVs via panel builder)

Cross-verification statuses in output:
  three_model_confirmed  | three_model_rejected
  two_model_included     | two_model_excluded
  one_model_included     | one_model_excluded

Falls back to two-model mode automatically if Run C directory is absent.
Two-model statuses (backward compat): consensus_confirmed | consensus_rejected |
                                      disputed_included   | disputed_excluded

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


def _fill_nulls(base: dict, donor: dict) -> dict:
    """Fill null fields in base from donor; use longer text for description/quote."""
    for key, val in donor.items():
        if key in ("description", "source_quote"):
            if len(str(val or "")) > len(str(base.get(key) or "")):
                base[key] = val
        elif base.get(key) is None and val is not None:
            base[key] = val
    return base


def _merge_pair(ra: dict, rb: dict, model_a: str, model_b: str) -> dict:
    """Merge two matched reforms, preferring non-null fields; prefer A for ties."""
    merged = _fill_nulls(dict(ra), rb)
    merged["cross_verification_status"] = "consensus"
    merged["found_by_models"] = [model_a, model_b]
    merged["cross_verification_note"] = ""
    return merged


def _merge_triple(ra: dict, rb: dict, rc: dict,
                  model_a: str, model_b: str, model_c: str) -> dict:
    """Merge three matched reforms, filling nulls in priority order A→B→C."""
    merged = _fill_nulls(_fill_nulls(dict(ra), rb), rc)
    merged["cross_verification_status"] = "consensus"
    merged["found_by_models"] = [model_a, model_b, model_c]
    merged["cross_verification_note"] = ""
    return merged


def _tag_disputed(reform: dict, model_name: str, status: str, note: str) -> dict:
    """Attach cross-verification metadata to a single-model reform."""
    r = dict(reform)
    r["cross_verification_status"] = status
    r["found_by_models"] = [model_name]
    r["cross_verification_note"] = note
    return r


# ---------------------------------------------------------------------------
# Three-way matching
# ---------------------------------------------------------------------------

def _match_reforms_three(
    reforms_a: list[dict],
    reforms_b: list[dict],
    reforms_c: list[dict],
    threshold: float = 0.55,
) -> tuple:
    """Greedy three-way matching by description similarity.

    Returns (all_three, ab_only, ac_only, bc_only, a_only, b_only, c_only)
      all_three : list of (ra, rb, rc)  — found by A, B, and C
      ab_only   : list of (ra, rb)      — found by A and B only
      ac_only   : list of (ra, rc)      — found by A and C only
      bc_only   : list of (rb, rc)      — found by B and C only
      a_only    : list of ra            — found only by A
      b_only    : list of rb            — found only by B
      c_only    : list of rc            — found only by C
    """
    # Step 1: Match A vs B
    ab_pairs, a_rem, b_rem = _match_reforms(reforms_a, reforms_b, threshold)

    used_c: set[int] = set()

    # Step 2: For each A-B pair, try to find a matching C
    all_three: list[tuple] = []
    ab_only:   list[tuple] = []

    for (ra, rb) in ab_pairs:
        desc_ab = str(ra.get("description", "") or "")
        best_score, best_j = 0.0, -1
        for j, rc in enumerate(reforms_c):
            if j in used_c:
                continue
            score = _similarity(desc_ab, str(rc.get("description", "") or ""))
            if score > best_score:
                best_score, best_j = score, j
        if best_score >= threshold and best_j >= 0:
            all_three.append((ra, rb, reforms_c[best_j]))
            used_c.add(best_j)
        else:
            ab_only.append((ra, rb))

    # Step 3: For unmatched A, try to find a matching C
    ac_only: list[tuple] = []
    a_only:  list[dict]  = []

    for ra in a_rem:
        desc_a = str(ra.get("description", "") or "")
        best_score, best_j = 0.0, -1
        for j, rc in enumerate(reforms_c):
            if j in used_c:
                continue
            score = _similarity(desc_a, str(rc.get("description", "") or ""))
            if score > best_score:
                best_score, best_j = score, j
        if best_score >= threshold and best_j >= 0:
            ac_only.append((ra, reforms_c[best_j]))
            used_c.add(best_j)
        else:
            a_only.append(ra)

    # Step 4: For unmatched B, try to find a matching C
    bc_only: list[tuple] = []
    b_only:  list[dict]  = []

    for rb in b_rem:
        desc_b = str(rb.get("description", "") or "")
        best_score, best_j = 0.0, -1
        for j, rc in enumerate(reforms_c):
            if j in used_c:
                continue
            score = _similarity(desc_b, str(rc.get("description", "") or ""))
            if score > best_score:
                best_score, best_j = score, j
        if best_score >= threshold and best_j >= 0:
            bc_only.append((rb, reforms_c[best_j]))
            used_c.add(best_j)
        else:
            b_only.append(rb)

    # Step 5: C reforms not matched anywhere
    c_only = [rc for j, rc in enumerate(reforms_c) if j not in used_c]

    return all_three, ab_only, ac_only, bc_only, a_only, b_only, c_only


# ---------------------------------------------------------------------------
# LLM adjudication of disputed reforms
# ---------------------------------------------------------------------------

_REVIEW_SYSTEM_PROMPT = """\
You are a senior OECD science and technology policy analyst performing a
cross-verification quality review. You are given reform descriptions extracted
from OECD Economic Survey PDFs. Each reform has an "agreement" field:

  "all_three_models"    — three independent LLM models all extracted this reform.
                         Very strong signal. Include unless clearly wrong.
  "two_of_three_models" — two of three models extracted this reform.
                         Good signal. Include if it clearly qualifies.
  "one_of_three_models" — only one of three models extracted this reform.
                         Be conservative and lean toward exclusion when uncertain.

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
• For "one_model" reforms: when genuinely uncertain, exclude.

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


def _build_review_prompt(reforms: list[dict]) -> str:
    rows = [
        {
            "reform_id":    r.get("_review_id", str(i)),
            "agreement":    r.get("_agreement", "one_model"),
            "found_by":     r.get("_found_by", "unknown"),
            "sub_theme":    r.get("sub_theme", ""),
            "description":  str(r.get("description", "") or "")[:600],
            "source_quote": str(r.get("source_quote", "") or "")[:400],
        }
        for i, r in enumerate(reforms)
    ]
    counts = {}
    for r in reforms:
        a = r.get("_agreement", "one_of_three_models")
        counts[a] = counts.get(a, 0) + 1
    count_parts = [f"{v} {k}" for k, v in counts.items()]
    return (
        f"Review these {len(rows)} reforms ({', '.join(count_parts)}):\n\n"
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


def _llm_review(
    reforms: list[dict],
    client: LLMClient,
    batch_size: int = 15,
) -> list[dict]:
    """Send ALL reforms (consensus + disputed) through LLM review in batches.

    Each reform must have bookkeeping keys:
      _review_id   unique id for this review batch
      _agreement   "both_models" | "one_model"
      _found_by    model name(s) that found it

    Returns reformed dicts with cross_verification_status set.
    """
    if not reforms:
        return []

    results = []
    for i in range(0, len(reforms), batch_size):
        batch = reforms[i : i + batch_size]
        prompt = _build_review_prompt(batch)
        try:
            response = client.call(
                system_prompt=_REVIEW_SYSTEM_PROMPT,
                user_prompt=prompt,
                operation="cross_verification_review",
            )
            decisions = _parse_adjudication_response(response, len(batch))
        except Exception as exc:
            logger.warning("Review batch failed: %s — defaulting to exclude", exc)
            decisions = [{"decision": "exclude", "rationale": "api_error"} for _ in batch]

        for reform, dec in zip(batch, decisions):
            r = dict(reform)
            agreement = r.pop("_agreement", "one_of_three_models")
            r.pop("_review_id", None)
            r.pop("_found_by", None)

            included = dec["decision"] == "include"
            if agreement in ("all_three_models", "both_models"):
                r["cross_verification_status"] = (
                    "three_model_confirmed" if included else "three_model_rejected"
                )
            elif agreement == "two_of_three_models":
                r["cross_verification_status"] = (
                    "two_model_included" if included else "two_model_excluded"
                )
            else:  # one_of_three_models / one_model
                r["cross_verification_status"] = (
                    "one_model_included" if included else "one_model_excluded"
                )
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
    json_c: dict | None = None,
    model_c: str | None = None,
) -> dict:
    """Merge two or three survey JSONs. Returns a merged JSON dict.

    If json_c / model_c are provided, runs three-way matching.
    Otherwise falls back to two-model matching (backward compatible).
    """
    reforms_a = json_a.get("reforms", []) or []
    reforms_b = json_b.get("reforms", []) or []
    reforms_c = (json_c.get("reforms", []) or []) if json_c is not None else []
    three_model = json_c is not None and model_c is not None

    if three_model:
        (all_three, ab_only, ac_only, bc_only,
         a_only, b_only, c_only) = _match_reforms_three(reforms_a, reforms_b, reforms_c, threshold)
    else:
        ab_pairs, a_only, b_only = _match_reforms(reforms_a, reforms_b, threshold)
        all_three = []
        ab_only   = ab_pairs
        ac_only   = []
        bc_only   = []
        c_only    = []

    review_queue: list[dict] = []

    if three_model:
        # --- Three-model groups ---
        for i, (ra, rb, rc) in enumerate(all_three):
            r = _merge_triple(ra, rb, rc, model_a, model_b, model_c)
            r["_review_id"] = f"abc_{i}"
            r["_agreement"] = "all_three_models"
            r["_found_by"]  = f"{model_a} + {model_b} + {model_c}"
            review_queue.append(r)

        for i, (ra, rb) in enumerate(ab_only):
            r = _merge_pair(ra, rb, model_a, model_b)
            r["_review_id"] = f"ab_{i}"
            r["_agreement"] = "two_of_three_models"
            r["_found_by"]  = f"{model_a} + {model_b}"
            review_queue.append(r)

        for i, (ra, rc) in enumerate(ac_only):
            r = _fill_nulls(dict(ra), rc)
            r["found_by_models"] = [model_a, model_c]
            r["_review_id"] = f"ac_{i}"
            r["_agreement"] = "two_of_three_models"
            r["_found_by"]  = f"{model_a} + {model_c}"
            review_queue.append(r)

        for i, (rb, rc) in enumerate(bc_only):
            r = _fill_nulls(dict(rb), rc)
            r["found_by_models"] = [model_b, model_c]
            r["_review_id"] = f"bc_{i}"
            r["_agreement"] = "two_of_three_models"
            r["_found_by"]  = f"{model_b} + {model_c}"
            review_queue.append(r)

        for i, ra in enumerate(a_only):
            r2 = dict(ra)
            r2["found_by_models"] = [model_a]
            r2["_review_id"] = f"a_{i}"
            r2["_agreement"] = "one_of_three_models"
            r2["_found_by"]  = model_a
            review_queue.append(r2)

        for i, rb in enumerate(b_only):
            r2 = dict(rb)
            r2["found_by_models"] = [model_b]
            r2["_review_id"] = f"b_{i}"
            r2["_agreement"] = "one_of_three_models"
            r2["_found_by"]  = model_b
            review_queue.append(r2)

        for i, rc in enumerate(c_only):
            r2 = dict(rc)
            r2["found_by_models"] = [model_c]
            r2["_review_id"] = f"c_{i}"
            r2["_agreement"] = "one_of_three_models"
            r2["_found_by"]  = model_c
            review_queue.append(r2)

    else:
        # --- Two-model groups (backward compat) ---
        for i, (ra, rb) in enumerate(ab_only):  # ab_only == ab_pairs in 2-model mode
            r = _merge_pair(ra, rb, model_a, model_b)
            r["_review_id"] = f"c_{i}"
            r["_agreement"] = "both_models"
            r["_found_by"]  = f"{model_a} + {model_b}"
            review_queue.append(r)

        for i, ra in enumerate(a_only):
            r2 = dict(ra)
            r2["found_by_models"] = [model_a]
            r2["_review_id"] = f"a_{i}"
            r2["_agreement"] = "one_model"
            r2["_found_by"]  = model_a
            review_queue.append(r2)

        for i, rb in enumerate(b_only):
            r2 = dict(rb)
            r2["found_by_models"] = [model_b]
            r2["_review_id"] = f"b_{i}"
            r2["_agreement"] = "one_model"
            r2["_found_by"]  = model_b
            review_queue.append(r2)

    # --- LLM review or consensus-only mode ---
    if not consensus_only and client is not None:
        merged_reforms = _llm_review(review_queue, client)
    else:
        # Consensus-only: confirmed groups stay, singles excluded, no LLM cost
        merged_reforms = []
        for r in review_queue:
            agreement = r.pop("_agreement", "one_of_three_models")
            r.pop("_review_id", None)
            r.pop("_found_by",  None)
            if agreement in ("all_three_models", "both_models"):
                r["cross_verification_status"] = "three_model_confirmed"
                r["cross_verification_note"]   = "consensus_only_mode"
            elif agreement == "two_of_three_models":
                r["cross_verification_status"] = "two_model_excluded"
                r["cross_verification_note"]   = "consensus_only_mode"
            else:
                r["cross_verification_status"] = "one_model_excluded"
                r["cross_verification_note"]   = "consensus_only_mode"
            merged_reforms.append(r)

    # Statuses that are excluded from the "reforms" (active) list
    _excluded_statuses = {
        "three_model_rejected",
        "two_model_excluded",
        "one_model_excluded",
        # backward compat two-model statuses
        "consensus_rejected",
        "disputed_excluded",
    }

    # Tally counts for the cross_verification metadata block
    n_all3  = len(all_three) if three_model else 0
    n_ab    = len(ab_only)
    n_ac    = len(ac_only) if three_model else 0
    n_bc    = len(bc_only) if three_model else 0
    n_a     = len(a_only)
    n_b     = len(b_only)
    n_c     = len(c_only) if three_model else 0

    result = {
        "country_code": json_a.get("country_code"),
        "country_name": json_a.get("country_name"),
        "survey_year":  json_a.get("survey_year"),
        "cross_verification": {
            "model_a":          model_a,
            "model_b":          model_b,
            "model_c":          model_c,
            "three_model_mode": three_model,
            "total_a":          len(reforms_a),
            "total_b":          len(reforms_b),
            "total_c":          len(reforms_c),
            "all_three":        n_all3,
            "ab_only":          n_ab,
            "ac_only":          n_ac,
            "bc_only":          n_bc,
            "a_only":           n_a,
            "b_only":           n_b,
            "c_only":           n_c,
            "threshold":        threshold,
        },
        "reforms": [r for r in merged_reforms
                    if r.get("cross_verification_status") not in _excluded_statuses],
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
    dir_c: Path | None = None,
    model_c: str | None = None,
) -> dict:
    """Process all matching survey JSONs across two or three directories.

    If dir_c and model_c are provided, runs three-model matching.
    Surveys present in run A but missing from run B (and optionally C) are
    passed through as-is with a 'run_a_only' status.

    Returns a summary dict with counts.
    """
    three_model = dir_c is not None and dir_c.exists() and model_c is not None
    output_dir.mkdir(parents=True, exist_ok=True)

    files_a = {f.name: f for f in dir_a.glob("*.json")}
    files_b = {f.name: f for f in dir_b.glob("*.json")}
    files_c = {f.name: f for f in dir_c.glob("*.json")} if three_model else {}

    if three_model:
        common = sorted(set(files_a) & set(files_b) & set(files_c))
        only_a = sorted(set(files_a) - set(files_b) - set(files_c))
    else:
        common = sorted(set(files_a) & set(files_b))
        only_a = sorted(set(files_a) - set(files_b))

    def _matches_filter(fname: str) -> bool:
        stem = fname.replace(".json", "")
        parts = stem.split("_")
        if country and parts[0].upper() != country.upper():
            return False
        if year:
            try:
                return int(parts[1]) == year
            except (IndexError, ValueError):
                return False
        return True

    common = [f for f in common if _matches_filter(f)]

    summary = {
        "three_model_mode":                  three_model,
        "surveys_processed":                 len(common),
        "surveys_a_only":                    len(only_a),
        "total_three_model_confirmed":       0,
        "total_three_model_rejected":        0,
        "total_two_model_included":          0,
        "total_two_model_excluded":          0,
        "total_one_model_included":          0,
        "total_one_model_excluded":          0,
        # backward-compat two-model keys
        "total_consensus_confirmed":         0,
        "total_consensus_rejected":          0,
        "total_disputed_included":           0,
    }

    mode_label = "three-model" if three_model else "two-model"
    logger.info(
        "Cross-verification (%s): %d surveys to process",
        mode_label, len(common),
    )

    for fname in common:
        try:
            with open(files_a[fname], encoding="utf-8") as f:
                json_a = json.load(f)
            with open(files_b[fname], encoding="utf-8") as f:
                json_b = json.load(f)
            json_c_data = None
            if three_model:
                with open(files_c[fname], encoding="utf-8") as f:
                    json_c_data = json.load(f)
        except Exception as exc:
            logger.warning("Could not load %s: %s", fname, exc)
            continue

        logger.info("Merging %s (%s)...", fname, mode_label)
        merged = _merge_survey(
            json_a, json_b, model_a, model_b,
            threshold, client, consensus_only,
            json_c=json_c_data, model_c=model_c if three_model else None,
        )

        all_reforms = merged.get("all_reforms_including_excluded", [])
        statuses = [r.get("cross_verification_status", "") for r in all_reforms]

        for key in (
            "three_model_confirmed", "three_model_rejected",
            "two_model_included",    "two_model_excluded",
            "one_model_included",    "one_model_excluded",
            "consensus_confirmed",   "consensus_rejected",
            "disputed_included",
        ):
            summary[f"total_{key}"] += statuses.count(key)

        out_path = output_dir / fname
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False, indent=2)

    # Pass-through surveys only in run A
    for fname in only_a:
        if not _matches_filter(fname):
            continue
        try:
            with open(files_a[fname], encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        data["cross_verification"] = {
            "model_a": model_a, "model_b": None, "model_c": None,
            "note": "only_in_run_a — no corresponding run B/C file",
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
    three_model = summary.get("three_model_mode", False)

    print("\n" + "=" * 60)
    print("CROSS-VERIFICATION SUMMARY")
    print("=" * 60)
    print(f"Surveys processed:              {summary.get('surveys_processed', summary.get('surveys_both', 0))}")
    print(f"Surveys only in run A:          {summary['surveys_a_only']}")
    print()

    if three_model:
        n3c  = summary["total_three_model_confirmed"]
        n3r  = summary["total_three_model_rejected"]
        n2i  = summary["total_two_model_included"]
        n2e  = summary["total_two_model_excluded"]
        n1i  = summary["total_one_model_included"]
        n1e  = summary["total_one_model_excluded"]
        n_final = n3c + n2i + n1i
        print(f"All three models agreed ({n3c + n3r} reforms):")
        print(f"  → LLM confirmed as valid:    {n3c}")
        print(f"  → LLM rejected (false pos.): {n3r}")
        print()
        print(f"Two of three models agreed ({n2i + n2e} reforms):")
        print(f"  → LLM included:              {n2i}")
        print(f"  → LLM excluded:              {n2e}")
        print()
        print(f"Only one model found ({n1i + n1e} reforms):")
        print(f"  → LLM included:              {n1i}")
        print(f"  → LLM excluded:              {n1e}")
    else:
        n_confirmed = summary["total_consensus_confirmed"]
        n_rejected  = summary["total_consensus_rejected"]
        n_disp_incl = summary["total_disputed_included"]
        n_disp_excl = (
            summary.get("total_one_model_excluded", 0) +
            summary.get("total_two_model_excluded", 0)
        )
        n_final = n_confirmed + n_disp_incl
        print(f"Both models agreed ({n_confirmed + n_rejected} reforms):")
        print(f"  → LLM confirmed as valid:    {n_confirmed}")
        print(f"  → LLM rejected (false pos.): {n_rejected}")
        print()
        print(f"Only one model found reforms:")
        print(f"  → LLM included:              {n_disp_incl}")
        print(f"  → LLM excluded:              {n_disp_excl}")
        n_final = n_confirmed + n_disp_incl

    print()
    print(f"Final reforms in dataset:       {n_final}")
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