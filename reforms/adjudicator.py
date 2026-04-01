"""
Pass 2 — LLM Adjudicator + K/L Lens Classification
=====================================================

Reads ``reforms_mentions.csv`` (which must already have ``score_band`` from
Pass 1) and adds four new columns back into that same file:

  llm_decision    str   — "include" | "exclude" | "n/a"
  llm_rationale   str   — 1–2 sentence LLM explanation
  activity_lens   str   — K1–K8 or null  (type of R&D activity)
  defence_scope   str   — L1–L6 or null  (civilian vs. defence scope)

No new files are created.  Re-running overwrites those four columns.

Two tasks
---------
Task A — Adjudication (borderline rows only, score_band == "borderline")
  The LLM reads description + source_quote with the full taxonomy as context
  and decides: include or exclude.  Rescues ~200–300 genuinely relevant reforms
  that the keyword rules alone cannot confirm.

Task B — K/L classification (all rows with score_band == "keep" or LLM-included)
  Assigns the K-pillar (what type of R&D?) and L-pillar (defence scope?) to
  every clean reform.  These dimensions are used for heterogeneity analysis
  in the research paper.

Rows with score_band == "drop" are skipped entirely — no LLM cost is wasted
on clearly contaminated rows.

Batching and checkpointing
--------------------------
  Rows are processed in batches of BATCH_SIZE (default 10).  After every batch
  a checkpoint JSON is saved so a run can be interrupted and resumed without
  reprocessing completed rows.

Cost estimate (gpt-4o-mini, ~800 tokens/call):
  ~290 calls for ~2 900 rows  ≈  $0.40–0.60 total

Usage
-----
  python -m reforms.adjudicator
  python -m reforms.adjudicator --adjudicate-only
  python -m reforms.adjudicator --classify-only
  python -m reforms.adjudicator --batch-size 20 --config config.yaml
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from reforms.llm_client import LLMClient           # noqa: E402
from reforms.pipeline_reforms import load_reforms_config  # noqa: E402

logger = logging.getLogger(__name__)

BATCH_SIZE = 10
VALID_K = {"K1", "K2", "K3", "K4", "K5", "K6", "K7", "K8"}
VALID_L = {"L1", "L2", "L3", "L4", "L5", "L6"}

DEFAULT_INPUT = PROJECT_ROOT / "Data/output/reforms/output/reforms_mentions.csv"
DEFAULT_CHECKPOINT = PROJECT_ROOT / "Data/output/reforms/output/adjudicator_checkpoint.json"

OP_ADJUDICATION = "adjudication"
OP_CLASSIFICATION = "kl_classification"


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

def _build_system_prompt(taxonomy_path: Path | None = None) -> str:
    if taxonomy_path is None:
        taxonomy_path = PROJECT_ROOT / "Data/input/taxonomy/search_library.json"

    k_block = l_block = ""
    core_rd = exclusions = ""

    try:
        with open(taxonomy_path, encoding="utf-8") as f:
            lib = json.load(f)

        k_lines = []
        for code, lens in lib.get("activity_lens", {}).get("lenses", {}).items():
            kws = ", ".join(lens.get("keywords", [])[:5])
            k_lines.append(f"  {code}  {lens.get('class', code)}: {kws}")
        k_block = "\n".join(k_lines)

        l_lines = []
        for code, lens in lib.get("defence_lens", {}).get("lenses", {}).items():
            kws = ", ".join(lens.get("keywords", [])[:4])
            l_lines.append(
                f"  {code}  {lens.get('scope', code)} — {lens.get('code', '')}: {kws}"
            )
        l_block = "\n".join(l_lines)

        core_rd = ", ".join(lib.get("auto_include", {}).get("keywords", [])[:20])
        exclusions = ", ".join(lib.get("exclusions", {}).get("keywords", [])[:20])

    except (FileNotFoundError, json.JSONDecodeError, KeyError) as exc:
        logger.warning("Could not load taxonomy for system prompt: %s", exc)

    return f"""You are a senior OECD science and technology policy analyst.
You review reform events extracted from OECD Economic Surveys and:
  (A) ADJUDICATE borderline reforms: include or exclude from the R&D dataset?
  (B) CLASSIFY all reforms on two analytical dimensions (K-pillar, L-pillar).

══ INCLUDE when the reform's primary purpose is ══════════════════════════════
• Direct public R&D funding (grants, research councils, competitive programmes)
• Innovation instruments for firms (R&D tax credits, innovation grants, vouchers)
• Research infrastructure (laboratories, science parks, supercomputing, HPC)
• Knowledge transfer (TTOs, spinoffs, university–industry collaboration, patents)
• Human capital for R&D (doctoral programmes, researcher mobility, fellowships)
• Startup/venture ecosystem specifically for deep-tech / R&D-intensive firms
• Sectoral / mission R&D (health, energy, climate, AI, space, quantum, defence)

Core R&D signal words: {core_rd}

══ EXCLUDE ══════════════════════════════════════════════════════════════════
{exclusions}
Also exclude: general VET/skills training; general SME finance; e-government
digital transformation; renewable energy deployment (feed-in tariffs, subsidies)
unless specifically funding energy R&D; housing/labour/social welfare; physical
infrastructure (roads, EV charging) unless for research purposes.

BORDERLINE RULE: If "research" or "innovation" is incidental to a broader
non-R&D reform → EXCLUDE.  Ask: "Would OECD STI Outlook track this reform?"

══ ACTIVITY LENS (K-pillar) ════════════════════════════════════════════════
{k_block}
null — cannot be determined / reform is being excluded

══ DEFENCE LENS (L-pillar) ═════════════════════════════════════════════════
{l_block}
null — no defence dimension mentioned (leave null; do NOT assume L4/L5)

══ CRITICAL ACCURACY RULES ══════════════════════════════════════════════════
- Base your decision only on the description and source_quote provided — do not
  invent or assume information not present in the input.
- If the description and source_quote are insufficient to make a confident
  determination, set llm_decision = "exclude" and explain in llm_rationale.
- Do not hallucinate K/L values: use null when the dimension is genuinely unclear.

══ OUTPUT ══════════════════════════════════════════════════════════════════
Return a JSON array, one object per reform, in input order:

[
  {{
    "reform_id": "<id>",
    "llm_decision": "include" | "exclude",
    "llm_rationale": "<1–2 sentences; cite specific words from description>",
    "activity_lens": "K1"|"K2"|"K3"|"K4"|"K5"|"K6"|"K7"|"K8"|null,
    "defence_scope": "L1"|"L2"|"L3"|"L4"|"L5"|"L6"|null
  }},
  ...
]

For classify_only tasks set llm_decision = "include" always.
Return valid JSON only — no markdown, no text outside the array.
"""


# ---------------------------------------------------------------------------
# User prompt
# ---------------------------------------------------------------------------

def _build_user_prompt(batch: list[dict], task: str) -> str:
    task_label = (
        "ADJUDICATE (include/exclude) AND CLASSIFY (K/L lenses)"
        if task == "adjudicate+classify"
        else "CLASSIFY K/L lenses only — these are confirmed R&D reforms"
    )
    rows = [
        {
            "reform_id": r.get("reform_id", ""),
            "sub_theme": r.get("sub_theme", ""),
            "description": str(r.get("description", "") or "")[:600],
            "source_quote": str(r.get("source_quote", "") or "")[:400],
        }
        for r in batch
    ]
    return (
        f"Task: {task_label}\n\n"
        f"Return a JSON array for these {len(rows)} reforms:\n\n"
        + json.dumps(rows, ensure_ascii=False, indent=2)
    )


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

def _parse_response(text: str, batch_ids: list[str]) -> list[dict]:
    safe = [
        {"reform_id": rid, "llm_decision": "include",
         "llm_rationale": "parse_error_defaulted_include",
         "activity_lens": None, "defence_scope": None}
        for rid in batch_ids
    ]

    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

    import re
    parsed = None

    # Strategy 1: direct JSON parse
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        pass

    # Strategy 2: regex extraction of first JSON array
    if parsed is None:
        m = re.search(r"\[[\s\S]*\]", text)
        if m:
            try:
                parsed = json.loads(m.group(0))
            except json.JSONDecodeError:
                pass

    if parsed is None:
        logger.warning(
            "No parseable JSON in LLM response; using safe defaults.\n"
            "  Response (first 500 chars): %s", text[:500]
        )
        return safe

    # Strategy 3: unwrap dict — model sometimes returns {"reforms": [...]}
    # or a single reform object directly when given a 1-row batch
    if isinstance(parsed, dict):
        # Check if it's a wrapper with a list value
        for v in parsed.values():
            if isinstance(v, list):
                parsed = v
                break
        else:
            # Single reform object — wrap in list
            if "reform_id" in parsed or "llm_decision" in parsed:
                parsed = [parsed]
            else:
                logger.warning(
                    "LLM returned a JSON object with no list value; using safe defaults.\n"
                    "  Keys found: %s\n  Response (first 300 chars): %s",
                    list(parsed.keys()), text[:300],
                )
                return safe

    if not isinstance(parsed, list):
        logger.warning(
            "Unexpected JSON type %s from LLM; using safe defaults.", type(parsed).__name__
        )
        return safe

    by_id = {}
    for item in parsed:
        if not isinstance(item, dict):
            continue
        decision = str(item.get("llm_decision", "include")).lower()
        if decision not in {"include", "exclude"}:
            decision = "include"
        activity = item.get("activity_lens")
        if activity not in VALID_K:
            activity = None
        defence = item.get("defence_scope")
        if defence not in VALID_L:
            defence = None
        by_id[str(item.get("reform_id", ""))] = {
            "reform_id": str(item.get("reform_id", "")),
            "llm_decision": decision,
            "llm_rationale": str(item.get("llm_rationale", "") or "")[:500],
            "activity_lens": activity,
            "defence_scope": defence,
        }

    return [by_id.get(rid, safe[i]) for i, rid in enumerate(batch_ids)]


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def _load_checkpoint(path: Path) -> dict[str, dict]:
    if path.exists():
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            logger.info("Checkpoint: %d rows already done.", len(data))
            return data
        except Exception as exc:
            logger.warning("Could not read checkpoint: %s", exc)
    return {}


def _save_checkpoint(completed: dict[str, dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(completed, f, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Main function
# ---------------------------------------------------------------------------

def run_adjudicator(
    input_path: Path = DEFAULT_INPUT,
    checkpoint_path: Path = DEFAULT_CHECKPOINT,
    config_path: str = "config.yaml",
    batch_size: int = BATCH_SIZE,
    adjudicate: bool = True,
    classify: bool = True,
    verbose: bool = True,
) -> pd.DataFrame:
    """Add llm_decision / llm_rationale / activity_lens / defence_scope
    columns to reforms_mentions.csv in place.

    Pass 1 (scoring_filter) must have run first so that score_band exists.

    Returns the annotated DataFrame.
    """
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Input not found: {path}")

    df = pd.read_csv(path, low_memory=False)

    if "score_band" not in df.columns:
        raise ValueError(
            "Column 'score_band' missing — run scoring_filter (Pass 1) first:\n"
            "  python -m reforms.scoring_filter"
        )

    # Initialise output columns if they don't exist yet
    for col, default in [
        ("llm_decision", "n/a"),
        ("llm_rationale", ""),
        ("activity_lens", None),
        ("defence_scope", None),
    ]:
        if col not in df.columns:
            df[col] = default

    # ------------------------------------------------------------------
    # Determine which rows to process
    # ------------------------------------------------------------------
    if adjudicate and classify:
        mask = df["score_band"].isin({"keep", "borderline"})
    elif adjudicate:
        mask = df["score_band"] == "borderline"
    else:  # classify only
        mask = df["score_band"] == "keep"

    todo_df = df[mask].copy()
    if len(todo_df) == 0:
        logger.info("No rows to process.")
        return df

    # ------------------------------------------------------------------
    # LLM client
    # ------------------------------------------------------------------
    config = load_reforms_config(config_path)
    if config is None:
        raise RuntimeError(
            "Could not load config.yaml — copy config.yaml.example and add your API key."
        )
    output_dir = path.parent
    config["paths"]["output"] = str(output_dir)

    # Allow the adjudicator to use a different model (cross-model validation).
    reforms_cfg = config.get("reforms", {})
    adj_model = reforms_cfg.get("adjudicator_model", "").strip()
    adj_provider = reforms_cfg.get("adjudicator_provider", "").strip()
    if adj_model:
        config["llm"] = dict(config.get("llm", {}))  # shallow copy — don't mutate original
        config["llm"]["model"] = adj_model
        logger.info("Adjudicator using model override: %s", adj_model)
    if adj_provider:
        config["llm"]["provider"] = adj_provider
        logger.info("Adjudicator using provider override: %s", adj_provider)

    client = LLMClient(
        config,
        usage_file=output_dir / "adjudicator_llm_usage.json",
    )

    system_prompt = _build_system_prompt()
    checkpoint = _load_checkpoint(checkpoint_path)

    rows = todo_df.to_dict(orient="records")
    todo = [r for r in rows if str(r.get("reform_id", "")) not in checkpoint]
    logger.info(
        "%d rows to process (%d already in checkpoint, skipped).",
        len(todo),
        len(rows) - len(todo),
    )

    # ------------------------------------------------------------------
    # Batch loop
    # ------------------------------------------------------------------
    for batch_start in range(0, len(todo), batch_size):
        batch = todo[batch_start: batch_start + batch_size]
        batch_ids = [str(r.get("reform_id", "")) for r in batch]
        n_batches = (len(todo) + batch_size - 1) // batch_size
        batch_num = batch_start // batch_size + 1

        # Task: keep rows are classify_only; borderline rows are adjudicate+classify
        tasks_in_batch = set(
            "adjudicate+classify"
            if r.get("score_band") == "borderline"
            else "classify_only"
            for r in batch
        )
        task = "adjudicate+classify" if "adjudicate+classify" in tasks_in_batch else "classify_only"

        logger.info(
            "  Batch %d/%d (%d rows, task=%s)", batch_num, n_batches, len(batch), task
        )

        user_prompt = _build_user_prompt(batch, task)
        op = OP_ADJUDICATION if task == "adjudicate+classify" else OP_CLASSIFICATION

        try:
            response = client.call(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                max_tokens=1536,
                operation=op,
                json_mode=(client.provider == "openai"),
            )
        except Exception as exc:
            logger.error("LLM call failed (batch %d): %s — safe defaults applied.", batch_num, exc)
            response = json.dumps([
                {"reform_id": rid, "llm_decision": "include",
                 "llm_rationale": f"api_error: {exc}",
                 "activity_lens": None, "defence_scope": None}
                for rid in batch_ids
            ])

        for entry in _parse_response(response, batch_ids):
            checkpoint[entry["reform_id"]] = entry

        _save_checkpoint(checkpoint, checkpoint_path)

    # ------------------------------------------------------------------
    # Write results back into the DataFrame
    # ------------------------------------------------------------------
    for rid, entry in checkpoint.items():
        idx = df.index[df["reform_id"].astype(str) == str(rid)]
        if len(idx) == 0:
            continue
        df.loc[idx, "llm_decision"] = entry.get("llm_decision", "n/a")
        df.loc[idx, "llm_rationale"] = entry.get("llm_rationale", "")
        df.loc[idx, "activity_lens"] = entry.get("activity_lens")
        df.loc[idx, "defence_scope"] = entry.get("defence_scope")

    # Save back to the same file
    df.to_csv(path, index=False)
    logger.info(
        "Columns llm_decision / llm_rationale / activity_lens / defence_scope "
        "written to %s",
        path.name,
    )

    client.save_usage()

    if verbose:
        border_done = df[df["score_band"] == "borderline"]
        kept_done = df[df["score_band"] == "keep"]
        if adjudicate and len(border_done) > 0:
            n_inc = (border_done["llm_decision"] == "include").sum()
            n_exc = (border_done["llm_decision"] == "exclude").sum()
            print(f"\nTask A — Adjudication ({len(border_done)} borderline rows):")
            print(f"  LLM INCLUDE : {n_inc:>5,}  ({n_inc/len(border_done)*100:.1f}%)")
            print(f"  LLM EXCLUDE : {n_exc:>5,}  ({n_exc/len(border_done)*100:.1f}%)")
        if classify and len(kept_done) > 0:
            print(f"\nTask B — K/L classification ({len(kept_done)} kept rows):")
            print("  Activity lens distribution:")
            print(kept_done["activity_lens"].value_counts(dropna=False).to_string())
        client.print_usage_report()

    return df


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args(argv=None):
    p = argparse.ArgumentParser(
        description=(
            "Pass 2: LLM adjudication of borderline reforms + K/L lens "
            "classification — adds columns to reforms_mentions.csv in place."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--input", type=Path, default=DEFAULT_INPUT,
                   help="Path to reforms_mentions.csv  (default: %(default)s)")
    p.add_argument("--checkpoint", type=Path, default=DEFAULT_CHECKPOINT,
                   help="Checkpoint JSON path  (default: %(default)s)")
    p.add_argument("--config", default="config.yaml",
                   help="config.yaml path  (default: %(default)s)")
    p.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                   help=f"Rows per LLM call  (default: {BATCH_SIZE})")
    p.add_argument("--adjudicate-only", action="store_true",
                   help="Task A only (skip K/L classification)")
    p.add_argument("--classify-only", action="store_true",
                   help="Task B only (skip adjudication)")
    p.add_argument("--quiet", action="store_true", help="Suppress verbose output")
    return p.parse_args(argv)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
                        datefmt="%H:%M:%S")
    args = _parse_args()
    run_adjudicator(
        input_path=args.input,
        checkpoint_path=args.checkpoint,
        config_path=args.config,
        batch_size=args.batch_size,
        adjudicate=not args.classify_only,
        classify=not args.adjudicate_only,
        verbose=not args.quiet,
    )
