"""
K-lens Classification for Borderline-Rescued Reforms
=====================================================
The main adjudicator assigns activity_lens (K1-K8) only to score_band=="keep"
rows. Borderline rows that the LLM rescued (llm_decision=="include") are left
without a K-lens. This script fills that gap.

Targets: score_band=="borderline" AND llm_decision=="include" AND activity_lens is null.

Usage:
  python -m reforms.classify_rescued                   # full run
  python -m reforms.classify_rescued --dry-run         # count targets, no LLM calls
  python -m reforms.classify_rescued --batch-size 15
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

from reforms.adjudicator import (   # noqa: E402
    _build_system_prompt,
    _build_user_prompt,
    _parse_response,
    _load_checkpoint,
    _save_checkpoint,
    VALID_K,
    OP_CLASSIFICATION,
    BATCH_SIZE,
)
from reforms.llm_client import LLMClient                    # noqa: E402
from reforms.pipeline_reforms import load_reforms_config    # noqa: E402

logger = logging.getLogger(__name__)

DEFAULT_INPUT = PROJECT_ROOT / "Data/output/reforms/output/reforms_mentions.csv"
DEFAULT_CHECKPOINT = PROJECT_ROOT / "Data/output/reforms/output/rescued_classify_checkpoint.json"
DEFAULT_CONFIG = PROJECT_ROOT / "config.yaml"


def run_classify_rescued(
    input_path: Path = DEFAULT_INPUT,
    checkpoint_path: Path = DEFAULT_CHECKPOINT,
    config_path: Path = DEFAULT_CONFIG,
    batch_size: int = BATCH_SIZE,
    dry_run: bool = False,
    verbose: bool = True,
) -> pd.DataFrame:
    """Classify K/L lens for borderline-rescued rows that have no lens yet."""
    df = pd.read_csv(input_path, low_memory=False)

    target_mask = (
        (df["score_band"] == "borderline")
        & (df["llm_decision"] == "include")
        & (df["activity_lens"].isna())
    )
    targets = df[target_mask].copy()

    if verbose:
        print(f"\nClassify rescued: {len(targets)} borderline-rescued rows without K-lens")

    if len(targets) == 0:
        if verbose:
            print("  Nothing to do.")
        return df

    if dry_run:
        if verbose:
            print("  --dry-run: no LLM calls made.")
            print(targets[["reform_id", "sub_theme", "tax_score"]].head(10).to_string())
        return df

    config = load_reforms_config(config_path)
    output_dir = Path(config["paths"]["output"])
    client = LLMClient(config, usage_file=output_dir / "rescued_classify_llm_usage.json")
    system_prompt = _build_system_prompt()
    checkpoint = _load_checkpoint(checkpoint_path)

    rows = targets.to_dict(orient="records")
    todo = [r for r in rows if str(r.get("reform_id", "")) not in checkpoint]

    if verbose:
        print(f"  Already in checkpoint: {len(rows) - len(todo)}")
        print(f"  To classify now      : {len(todo)}")

    n_batches = (len(todo) + batch_size - 1) // batch_size
    for batch_start in range(0, len(todo), batch_size):
        batch = todo[batch_start: batch_start + batch_size]
        batch_ids = [str(r.get("reform_id", "")) for r in batch]
        batch_num = batch_start // batch_size + 1

        if verbose:
            print(f"  Batch {batch_num}/{n_batches} ({len(batch)} rows) …", end=" ")

        # Force classify_only task — llm_decision already set to "include"
        user_prompt = _build_user_prompt(batch, "classify_only")

        try:
            response = client.call(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                max_tokens=1024,
                operation=OP_CLASSIFICATION,
                json_mode=(client.provider == "openai"),
            )
        except Exception as exc:
            logger.error("LLM call failed (batch %d): %s", batch_num, exc)
            response = json.dumps([
                {"reform_id": rid, "llm_decision": "include",
                 "llm_rationale": f"api_error: {exc}",
                 "activity_lens": None, "defence_scope": None}
                for rid in batch_ids
            ])

        parsed = _parse_response(response, batch_ids)
        for entry in parsed:
            checkpoint[entry["reform_id"]] = entry
        _save_checkpoint(checkpoint, checkpoint_path)

        if verbose:
            k_vals = [e.get("activity_lens") for e in parsed]
            print(f"K-lens assigned: {[k for k in k_vals if k]}")

    # Write K-lens back — do NOT touch llm_decision (already correct)
    for rid, entry in checkpoint.items():
        idx = df.index[df["reform_id"].astype(str) == str(rid)]
        if len(idx) == 0:
            continue
        k = entry.get("activity_lens")
        if k and k in VALID_K:
            df.loc[idx, "activity_lens"] = k
        l_val = entry.get("defence_scope")
        if l_val:
            df.loc[idx, "defence_scope"] = l_val

    df.to_csv(input_path, index=False)

    if verbose:
        filled = df[target_mask & df["activity_lens"].notna()]
        print(f"\n  K-lens filled for {len(filled)}/{len(targets)} rescued rows")
        print(df.loc[df["reform_id"].isin(targets["reform_id"]), "activity_lens"]
              .value_counts(dropna=False).to_string())

    client.save_usage()
    return df


def _parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    p.add_argument("--checkpoint", type=Path, default=DEFAULT_CHECKPOINT)
    p.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    p.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--quiet", action="store_true")
    return p.parse_args(argv)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
                        datefmt="%H:%M:%S")
    args = _parse_args()
    run_classify_rescued(
        input_path=args.input,
        checkpoint_path=args.checkpoint,
        config_path=args.config,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        verbose=not args.quiet,
    )
