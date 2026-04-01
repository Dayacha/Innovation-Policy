from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd


def _sheet_name(model_dir_name: str) -> str:
    name = model_dir_name.replace("anthropic_", "claude_").replace("openai_", "")
    name = re.sub(r"[^A-Za-z0-9 _-]+", "_", name)
    return name[:31]


def _usage_metrics(usage_path: Path) -> dict:
    if not usage_path.exists():
        return {
            "llm_calls": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "cost_usd": 0.0,
        }
    payload = json.loads(usage_path.read_text(encoding="utf-8"))
    records = payload.get("records", [])
    input_tokens = sum(int(r.get("input_tokens", 0) or 0) for r in records)
    output_tokens = sum(int(r.get("output_tokens", 0) or 0) for r in records)
    return {
        "llm_calls": len(records),
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": input_tokens + output_tokens,
        "cost_usd": round(sum(float(r.get("cost_usd", 0) or 0) for r in records), 6),
    }


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _model_summary(model_dir: Path) -> dict:
    output_dir = model_dir / "output"
    usage = _usage_metrics(output_dir / "llm_usage.json")
    mentions = _read_csv(output_dir / "reforms_mentions.csv")
    events = _read_csv(output_dir / "reforms_events.csv")
    panel = _read_csv(output_dir / "reform_panel.csv")
    return {
        "provider": model_dir.name.split("_", 1)[0],
        "model_dir": model_dir.name,
        "model": model_dir.name.split("_", 1)[1] if "_" in model_dir.name else model_dir.name,
        "mentions_rows": len(mentions),
        "events_rows": len(events),
        "panel_rows": len(panel),
        **usage,
    }


def build_workbook(run_dir: Path, output_path: Path | None = None) -> Path:
    model_dirs = sorted(
        p for p in run_dir.iterdir() if p.is_dir() and (p / "output").exists()
    )
    if not model_dirs:
        raise RuntimeError(f"No model output folders found in {run_dir}")

    output_path = output_path or (run_dir / "benchmark_comparison.xlsx")
    comparison = pd.DataFrame([_model_summary(d) for d in model_dirs]).sort_values(
        ["cost_usd", "provider", "model"]
    )

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        comparison.to_excel(writer, sheet_name="comparison", index=False)

        for model_dir in model_dirs:
            output_dir = model_dir / "output"
            sheet = _sheet_name(model_dir.name)
            summary = pd.DataFrame([_model_summary(model_dir)])
            mentions = _read_csv(output_dir / "reforms_mentions.csv")
            events = _read_csv(output_dir / "reforms_events.csv")
            panel = _read_csv(output_dir / "reform_panel.csv")
            summary_text = ""
            stats_path = output_dir / "summary_statistics.txt"
            if stats_path.exists():
                summary_text = stats_path.read_text(encoding="utf-8", errors="ignore")

            start = 0
            summary.to_excel(writer, sheet_name=sheet, index=False, startrow=start)
            start += len(summary) + 3

            if summary_text:
                summary_lines = pd.DataFrame({"summary_statistics": summary_text.splitlines()})
                summary_lines.to_excel(writer, sheet_name=sheet, index=False, startrow=start)
                start += len(summary_lines) + 3

            if not events.empty:
                events.to_excel(writer, sheet_name=sheet, index=False, startrow=start)
                start += len(events) + 3

            if not mentions.empty:
                mentions.to_excel(writer, sheet_name=sheet, index=False, startrow=start)
                start += len(mentions) + 3

            if not panel.empty:
                panel.to_excel(writer, sheet_name=sheet, index=False, startrow=start)

    return output_path


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    path = build_workbook(Path(args.run_dir), Path(args.output) if args.output else None)
    print(path)
