from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Iterable

import pandas as pd


def _normalize(text: str) -> str:
    return re.sub(r"\\s+", " ", text.lower()).strip()


def build_hints(df: pd.DataFrame, search_library_path: Path, max_hints: int = 3) -> pd.Series:
    """Return a Series of semicolon-joined hints derived from a search library."""
    if not search_library_path.exists():
        return pd.Series([""] * len(df), index=df.index)

    # Load search library; assume JSON list of dicts with 'label' and optional 'keywords'
    try:
        lib = json.loads(search_library_path.read_text(encoding="utf-8"))
    except Exception:
        return pd.Series([""] * len(df), index=df.index)

    entries: list[tuple[str, list[str]]] = []
    for item in lib:
        label = item.get("label") or item.get("name") or ""
        kws = item.get("keywords") or []
        if isinstance(kws, str):
            kws = [kws]
        kws = [kw.lower() for kw in kws if kw]
        if label:
            entries.append((label, kws))

    def hints_for_row(row) -> str:
        text = _normalize(str(row.get("line_description", "")) + " " + str(row.get("program_description", "")))
        scored = []
        for label, kws in entries:
            score = sum(1 for kw in kws if kw in text)
            if score > 0 or (kws == [] and label.lower() in text):
                scored.append((score, label))
        scored.sort(key=lambda x: (-x[0], x[1]))
        top = [label for _, label in scored[:max_hints]]
        return "; ".join(top)

    return df.apply(hints_for_row, axis=1)


def attach_hints(input_csv: Path, search_library_path: Path, output_csv: Path, max_hints: int = 3) -> Path:
    df = pd.read_csv(input_csv)
    df["taxonomy_hints"] = build_hints(df, search_library_path, max_hints)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False)
    return output_csv
