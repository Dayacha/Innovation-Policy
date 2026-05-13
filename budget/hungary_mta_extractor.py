from __future__ import annotations

import argparse
import csv
import gzip
import re
import subprocess
from pathlib import Path
from typing import Optional

from budget import config as cfg

FULL_TEXT_DIR = cfg.OUTPUT_DIR / "full_text" / "Hungary"
PDF_DIR = cfg.PDF_ROOT / "Hungary"
OUT_DIR = cfg.OUTPUT_DIR / "Hungary"
OUT_CSV = OUT_DIR / "hungary_mta_extractor_audit.csv"

MTA_CANONICAL = "Hungarian Academy of Sciences (MTA)"
TARGET_YEARS = [1991, 2000, 2003, 2004, 2006, 2008, 2009]

_MTA_HEADING_RE = re.compile(r"XXXIII\.\s+MAGYAR\s+TUDOM", re.IGNORECASE)
_MTA_TEXT_RE = re.compile(r"magyar tudom[aá]nyos akad|\bMTA\b", re.IGNORECASE)
_PAGE_RE = re.compile(r"^=== Page (\d+)\.0")
_TOTAL_LINE_RE = re.compile(r"XXXIII\.\s+fejezet\s+összesen:\s*(.*)$", re.IGNORECASE)


def _read_text_gz(path: Path) -> str:
    with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as fh:
        return fh.read()


def _find_page_for_heading(text: str) -> Optional[int]:
    current_page: Optional[int] = None
    for line in text.splitlines():
        page_match = _PAGE_RE.match(line)
        if page_match:
            current_page = int(page_match.group(1))
            continue
        if _MTA_HEADING_RE.search(line):
            return current_page
    return None


def _has_recoverable_total(text: str) -> tuple[Optional[str], Optional[float]]:
    for line in text.splitlines():
        match = _TOTAL_LINE_RE.search(line)
        if not match:
            continue
        tail = match.group(1).strip()
        if not tail:
            return line.strip(), None
        # Treat bare stubs like "48" or fragmented text like "50 8" as
        # non-recoverable. For Hungary chapter totals we only accept numbers
        # that preserve either grouped thousands or an explicit decimal comma.
        if "," not in tail and "." not in tail:
            return line.strip(), None
        if re.fullmatch(r"\d{1,3}(?:[ .]\d{3})*(?:,\d+)?", tail):
            normalised = tail.replace(" ", "").replace(".", "").replace(",", ".")
            try:
                return line.strip(), float(normalised)
            except ValueError:
                return line.strip(), None
        return line.strip(), None
    return None, None


def _render_pdf_layout(pdf_path: Path, start_page: int, end_page: int) -> str:
    cmd = [
        "pdftotext",
        "-layout",
        "-f",
        str(start_page),
        "-l",
        str(end_page),
        str(pdf_path),
        "-",
    ]
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return proc.stdout


def _candidate_full_text_files(year: int) -> list[Path]:
    return sorted(FULL_TEXT_DIR.glob(f"*__{year}_*.txt.gz"))


def _choose_full_text_file(year: int) -> Optional[Path]:
    candidates = _candidate_full_text_files(year)
    if not candidates:
        return None
    scored: list[tuple[int, Path]] = []
    for path in candidates:
        text = _read_text_gz(path)
        score = 0
        if _MTA_HEADING_RE.search(text):
            score += 10
        if _MTA_TEXT_RE.search(text):
            score += 1
        scored.append((score, path))
    scored.sort(key=lambda item: (item[0], item[1].name))
    return scored[-1][1]


def _choose_pdf_file(year: int) -> Optional[Path]:
    candidates = sorted(PDF_DIR.glob(f"{year}*.pdf"))
    return candidates[0] if candidates else None


def _classify_record(
    year: int,
    full_text_file: Optional[Path],
    pdf_file: Optional[Path],
) -> dict[str, object]:
    record: dict[str, object] = {
        "year": year,
        "canonical_name": MTA_CANONICAL,
        "full_text_file": full_text_file.name if full_text_file else "",
        "pdf_file": pdf_file.name if pdf_file else "",
        "heading_page_from_full_text": "",
        "has_mta_heading_in_full_text": "no",
        "has_total_line_in_full_text": "no",
        "full_text_total_line": "",
        "full_text_total_value": "",
        "layout_total_line": "",
        "layout_total_value": "",
        "extractor_status": "missing_source",
        "diagnosis": "",
    }

    if full_text_file is None:
        record["diagnosis"] = "No full-text cache file found for this year."
        return record

    text = _read_text_gz(full_text_file)
    has_heading = bool(_MTA_HEADING_RE.search(text) or _MTA_TEXT_RE.search(text))
    record["has_mta_heading_in_full_text"] = "yes" if has_heading else "no"
    page = _find_page_for_heading(text)
    record["heading_page_from_full_text"] = page or ""

    total_line, total_value = _has_recoverable_total(text)
    if total_line:
        record["has_total_line_in_full_text"] = "yes"
        record["full_text_total_line"] = total_line
        record["full_text_total_value"] = total_value if total_value is not None else ""

    if pdf_file is not None and page is not None:
        try:
            layout_text = _render_pdf_layout(pdf_file, page, min(page + 4, page + 4))
        except Exception as exc:  # pragma: no cover
            record["diagnosis"] = f"pdftotext layout extraction failed: {exc}"
            record["extractor_status"] = "layout_failed"
            return record
        layout_line, layout_value = _has_recoverable_total(layout_text)
        if layout_line:
            record["layout_total_line"] = layout_line
            record["layout_total_value"] = layout_value if layout_value is not None else ""

    if not has_heading:
        record["extractor_status"] = "source_text_missing_mta"
        record["diagnosis"] = (
            "Original cached text does not contain a detectable MTA chapter heading."
        )
        return record

    if total_value is not None:
        record["extractor_status"] = "recoverable_from_full_text"
        record["diagnosis"] = "Full-text cache contains a recoverable chapter total."
        return record

    if record["layout_total_value"] not in {"", None}:
        record["extractor_status"] = "recoverable_from_layout"
        record["diagnosis"] = "Layout extraction contains a recoverable chapter total."
        return record

    if record["layout_total_line"]:
        record["extractor_status"] = "total_truncated_in_layout"
        record["diagnosis"] = (
            "Chapter heading exists, but the total line is truncated in layout extraction and does not expose a defendable numeric total."
        )
        return record

    if total_line:
        record["extractor_status"] = "total_truncated_in_full_text"
        record["diagnosis"] = (
            "Full-text cache contains the total label, but not a complete numeric total."
        )
        return record

    record["extractor_status"] = "parser_inference_needed"
    record["diagnosis"] = (
        "MTA heading exists, but no explicit recoverable chapter-total line was found in cached text."
    )
    return record


def build_audit(years: list[int]) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for year in years:
        full_text_file = _choose_full_text_file(year)
        pdf_file = _choose_pdf_file(year)
        records.append(_classify_record(year, full_text_file, pdf_file))
    return records


def write_audit(records: list[dict[str, object]], out_csv: Path = OUT_CSV) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "year",
        "canonical_name",
        "full_text_file",
        "pdf_file",
        "heading_page_from_full_text",
        "has_mta_heading_in_full_text",
        "has_total_line_in_full_text",
        "full_text_total_line",
        "full_text_total_value",
        "layout_total_line",
        "layout_total_value",
        "extractor_status",
        "diagnosis",
    ]
    with out_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit Hungary MTA chapter extraction")
    parser.add_argument("--years", nargs="*", type=int, default=TARGET_YEARS)
    args = parser.parse_args()

    records = build_audit(sorted(set(args.years)))
    write_audit(records)
    print(f"Wrote {OUT_CSV}")
    for row in records:
        print(
            row["year"],
            row["extractor_status"],
            row["full_text_file"],
            row["layout_total_line"] or row["full_text_total_line"] or "",
        )


if __name__ == "__main__":
    main()
