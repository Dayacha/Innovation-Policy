from __future__ import annotations

import gzip
import re
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
COUNTRY = "New Zealand"
COUNTRY_SLUG = "new_zealand"
COUNTRY_DIR = ROOT / "Data" / "output" / "budget" / COUNTRY
FULL_TEXT_DIR = ROOT / "Data" / "output" / "budget" / "full_text" / COUNTRY
INPUT_PDF_DIR = ROOT / "Data" / "input" / "finance_bills" / COUNTRY


def _norm(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(text).lower()).strip("_")


def _norm_search(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(text).lower()).strip()


def _cache_key(path: Path) -> str:
    name = path.name
    if "__" in name:
        tail = name.split("__", 1)[1]
    else:
        tail = path.stem
    tail = tail.removesuffix(".txt.gz").removesuffix(".txt")
    return _norm(tail)


def _build_cache_map() -> dict[str, Path]:
    out: dict[str, Path] = {}
    for path in FULL_TEXT_DIR.glob("*.txt.gz"):
        out[_cache_key(path)] = path
    return out


def _find_cache_path(source_file: str, cache_map: dict[str, Path]) -> Path | None:
    if not source_file or pd.isna(source_file):
        return None
    key = _norm(str(source_file).removesuffix(".pdf"))
    return cache_map.get(key)


def _search_terms(row: pd.Series) -> list[str]:
    terms: list[str] = []
    line_desc = str(row.get("line_description_en") or "").strip()
    canonical = str(row.get("canonical_name") or "").strip()
    if line_desc:
        terms.append(line_desc)
        if ":" in line_desc:
            terms.append(line_desc.split(":", 1)[1].strip())
    if canonical:
        terms.append(canonical)
        canonical_base = canonical.replace(" (New Zealand)", "").strip()
        if canonical_base != canonical:
            terms.append(canonical_base)
    if canonical == "Callaghan Innovation":
        terms.extend(["Callaghan Innovation - Operations", "Callaghan Innovation"])
    if canonical == "DSIR (New Zealand)":
        terms.extend(["Total for Scientific and Industrial Research", "Scientific and Industrial Research"])
    if canonical == "Research, Science and Technology Vote (New Zealand)":
        terms.extend(
            [
                "Total for Research, Science and Technology",
                "Total for Vote Science and Innovation",
                "Research, Science and Technology",
                "Research Science and Technology",
                "Research, Science",
                "Research Science",
                "Vote Science and Innovation",
                "Science and Innovation",
                "Public Good Science and Technology",
                "Non-Specific Output Funding for Public Good Science and Technology",
            ]
        )
    if canonical == "Crown Research Institutes (New Zealand)":
        terms.append("Crown Research Institute Core Funding")
    return [t for i, t in enumerate(terms) if t and t not in terms[:i]]


def _extract_excerpt(cache_path: Path, row: pd.Series) -> str:
    try:
        with gzip.open(cache_path, "rt", errors="ignore") as fh:
            lines = fh.read().splitlines()
    except OSError:
        return ""

    terms = _search_terms(row)
    for term in terms:
        term_lower = term.lower()
        for idx, line in enumerate(lines):
            if term_lower in line.lower():
                start = max(0, idx - 2)
                end = min(len(lines), idx + 4)
                snippet = [ln.strip() for ln in lines[start:end] if ln.strip()]
                return " | ".join(snippet)
        for idx in range(len(lines) - 2):
            window = " ".join(part.strip() for part in lines[idx:idx + 6] if part.strip())
            if term_lower in window.lower():
                start = max(0, idx - 2)
                end = min(len(lines), idx + 8)
                snippet = [ln.strip() for ln in lines[start:end] if ln.strip()]
                return " | ".join(snippet)
        term_norm = _norm_search(term)
        for idx in range(len(lines) - 2):
            window = " ".join(part.strip() for part in lines[idx:idx + 6] if part.strip())
            if term_norm and term_norm in _norm_search(window):
                start = max(0, idx - 2)
                end = min(len(lines), idx + 8)
                snippet = [ln.strip() for ln in lines[start:end] if ln.strip()]
                return " | ".join(snippet)
    return ""


def main() -> None:
    series_path = COUNTRY_DIR / f"{COUNTRY_SLUG}_docx_series.csv"
    trace_path = COUNTRY_DIR / f"{COUNTRY_SLUG}_series_traceability.csv"
    series = pd.read_csv(series_path)
    cache_map = _build_cache_map()

    trace = series.copy()
    trace["pdf_path"] = trace["source_file"].apply(
        lambda value: str(INPUT_PDF_DIR / str(value)) if pd.notna(value) and str(value).strip() else ""
    )
    trace["full_text_cache"] = trace["source_file"].apply(
        lambda value: str(_find_cache_path(value, cache_map)) if _find_cache_path(value, cache_map) else ""
    )
    trace["traceability_status"] = trace["amount_local"].notna().map(
        {True: "verified_against_compiled_series", False: "gap_in_final_series"}
    )
    trace["trace_excerpt"] = trace.apply(
        lambda row: _extract_excerpt(Path(row["full_text_cache"]), row)
        if row["full_text_cache"] and pd.notna(row["amount_local"])
        else "",
        axis=1,
    )

    ordered_cols = [
        "year",
        "canonical_name",
        "category",
        "amount_local",
        "unit",
        "currency",
        "item_type",
        "source_file",
        "page_number",
        "pdf_path",
        "full_text_cache",
        "line_description_en",
        "traceability_status",
        "trace_excerpt",
        "series_notes",
    ]
    trace = trace[ordered_cols].sort_values(["canonical_name", "year"], kind="stable")
    trace.to_csv(trace_path, index=False)
    print(f"Wrote {trace_path} ({len(trace)} rows)")


if __name__ == "__main__":
    main()
