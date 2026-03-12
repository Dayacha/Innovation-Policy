"""Batching helpers for the AI validation layer."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Iterable, List, Optional

from src.ai_client import AIClient


def chunk_records(records: List[dict], batch_size: int) -> Iterable[list[dict]]:
    """Yield successive batches of size batch_size."""
    for i in range(0, len(records), batch_size):
        yield records[i : i + batch_size]


def run_batches(
    client: AIClient,
    pending_records: List[dict],
    batch_size: int,
    failed_batches_file: Path,
    retry_delay: float = 1.0,
    max_retries: int = 1,
    precomputed_batches: Optional[List[List[dict]]] = None,
) -> list[dict]:
    """Send records in batches with a single retry per failed batch.

    Failed batches are appended to failed_batches_file as JSON lines for later debugging.
    """
    results: list[dict] = []
    failed_batches_file.parent.mkdir(parents=True, exist_ok=True)

    batches = precomputed_batches if precomputed_batches is not None else list(chunk_records(pending_records, batch_size))

    for batch in batches:
        attempt = 0
        while True:
            try:
                batch_result = client.validate_batch(batch)
                results.extend(batch_result)
                break
            except Exception as exc:  # pragma: no cover - runtime behavior
                attempt += 1
                if attempt > max_retries:
                    failed_entry = {
                        "error": str(exc),
                        "records": batch,
                    }
                    with failed_batches_file.open("a", encoding="utf-8") as fh:
                        fh.write(json.dumps(failed_entry, ensure_ascii=False, default=str) + "\n")
                    break
                time.sleep(retry_delay)
    return results
