"""
Build a cross-country deep-dive table for unresolved budget data gaps.

This consolidates:
  - per-country gap reports
  - re-extraction queues
  - run_log extraction outcomes
  - country source/quality notes

Outputs:
  - Data/output/budget/country_gap_deepdive_summary.csv
  - Data/output/budget/country_gap_deepdive_detail.csv
  - Data/output/budget/country_gap_deepdive_summary.md

Usage:
  ./venv/bin/python -m budget.country_gap_deepdive
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd
from pandas.errors import EmptyDataError

from budget import config as cfg

SUMMARY_CSV = cfg.OUTPUT_DIR / "country_gap_deepdive_summary.csv"
DETAIL_CSV = cfg.OUTPUT_DIR / "country_gap_deepdive_detail.csv"
SOURCE_AUDIT_CSV = cfg.OUTPUT_DIR / "country_gap_source_audit.csv"
SUMMARY_MD = cfg.OUTPUT_DIR / "country_gap_deepdive_summary.md"

_NOTE_FILES = ("QUALITY_NOTE.md", "SOURCE_NOTES.md")
_ISSUE_PRIORITY = [
    "document_changed_or_not_comparable",
    "unsupported_or_missing_source",
    "parsed_but_zero_rows",
    "aggregate_or_summary_only_source",
    "ocr_or_text_layer_problem",
    "outlier_or_unit_break",
    "raw_rows_found_needs_reclassification",
    "general_reextract_needed",
]


@dataclass(frozen=True)
class NoteSignals:
    document_changed: bool = False
    aggregate_source: bool = False
    ocr_issue: bool = False
    unsupported_format: bool = False
    currency_break: bool = False
    source_gap: bool = False
    note_excerpt: str = ""
    note_path: str = ""


def _compress_years(years: Iterable[int]) -> str:
    vals = sorted({int(y) for y in years if pd.notna(y)})
    if not vals:
        return ""
    ranges: list[str] = []
    start = prev = vals[0]
    for year in vals[1:]:
        if year == prev + 1:
            prev = year
            continue
        ranges.append(f"{start}-{prev}" if start != prev else str(start))
        start = prev = year
    ranges.append(f"{start}-{prev}" if start != prev else str(start))
    return ", ".join(ranges)


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except EmptyDataError:
        return pd.DataFrame()


def _load_country_output(country_dir: Path, suffix: str) -> pd.DataFrame:
    files = list(country_dir.glob(f"*_{suffix}.csv"))
    if not files:
        return pd.DataFrame()
    return _safe_read_csv(files[0])


def _load_run_log() -> pd.DataFrame:
    rows: list[dict] = []
    if not cfg.RUN_LOG_FILE.exists():
        return pd.DataFrame()
    for line in cfg.RUN_LOG_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce")
    if "rows_extracted" in df.columns:
        df["rows_extracted"] = pd.to_numeric(df["rows_extracted"], errors="coerce")
    return df


def _extract_note_excerpt(text: str) -> str:
    bullets: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            bullets.append(stripped[2:].strip())
        if len(bullets) == 2:
            break
    if bullets:
        return " | ".join(bullets)
    cleaned = " ".join(text.split())
    return cleaned[:220]


def _load_note_signals(country: str) -> NoteSignals:
    country_dir = cfg.PDF_ROOT / country
    for name in _NOTE_FILES:
        note_path = country_dir / name
        if not note_path.exists():
            continue
        text = note_path.read_text(encoding="utf-8")
        lower = text.lower()
        return NoteSignals(
            document_changed=any(
                key in lower
                for key in (
                    "change the object being measured",
                    "changes structure across time",
                    "not comparable",
                    "comparability problem",
                    "legal-wrapper",
                    "legal wrapper",
                )
            ),
            aggregate_source=any(
                key in lower
                for key in (
                    "summary",
                    "aggregate",
                    "brief",
                    "infographic",
                    "macro totals",
                    "not a clean institutional budget panel",
                )
            ),
            ocr_issue=any(
                key in lower
                for key in (
                    "ocr",
                    "text layer",
                    "empty text",
                    "python-docx",
                    "scan-heavy",
                    "scanned",
                )
            ),
            unsupported_format=any(
                key in lower for key in (".hwp", "unsupported", "not yet part of the extraction path")
            ),
            currency_break=any(
                key in lower
                for key in (
                    "currency",
                    "unit",
                    "eur",
                    "sit",
                    "skk",
                    "frf",
                    "litas",
                    "euro era",
                )
            ),
            source_gap=any(
                key in lower
                for key in (
                    "no recoverable",
                    "no long pre-",
                    "source limitation",
                    "misfiled",
                    "folder coverage",
                    "find better annex source",
                    "replace source",
                )
            ),
            note_excerpt=_extract_note_excerpt(text),
            note_path=str(note_path.relative_to(cfg.PROJECT_ROOT)),
        )
    return NoteSignals()


def _normalize_gap_df(gap_df: pd.DataFrame) -> pd.DataFrame:
    df = gap_df.copy()
    for col in ("year", "raw_row_amount", "series_amount", "prev_amount", "next_amount"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ("gap_type", "gap_subtype", "action", "diagnosis", "canonical_name", "raw_row_file"):
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str)
    return df


def _pick_primary_issue(
    counts: dict[str, int],
    note: NoteSignals,
    missing_years_with_runs: int,
    missing_agency_years: int,
    outlier_agency_years: int,
) -> str:
    if missing_agency_years == 0 and outlier_agency_years == 0:
        return "stable_or_resolved"
    if missing_agency_years == 0 and outlier_agency_years > 0:
        return "outlier_or_unit_break"
    if counts["document_changed_or_not_comparable"] > 0 or note.document_changed:
        return "document_changed_or_not_comparable"
    if counts["unsupported_or_missing_source"] > 0 or (note.source_gap and missing_years_with_runs == 0):
        return "unsupported_or_missing_source"
    if counts["parsed_but_zero_rows"] > 0:
        return "parsed_but_zero_rows"
    if counts["aggregate_or_summary_only_source"] > 0 or note.aggregate_source:
        return "aggregate_or_summary_only_source"
    if counts["ocr_or_text_layer_problem"] > 0 or note.ocr_issue:
        return "ocr_or_text_layer_problem"
    if counts["outlier_or_unit_break"] > 0:
        return "outlier_or_unit_break"
    if counts["raw_rows_found_needs_reclassification"] > 0:
        return "raw_rows_found_needs_reclassification"
    if counts["general_reextract_needed"] > 0:
        return "general_reextract_needed"
    return "stable_or_resolved"


def _issue_label(issue: str) -> str:
    labels = {
        "document_changed_or_not_comparable": "Document changed / not comparable",
        "unsupported_or_missing_source": "Missing or unsupported source",
        "parsed_but_zero_rows": "Document ran but returned zero rows",
        "aggregate_or_summary_only_source": "Only aggregate/summary source available",
        "ocr_or_text_layer_problem": "OCR / text-layer problem",
        "outlier_or_unit_break": "Outlier or unit/currency break",
        "raw_rows_found_needs_reclassification": "Raw rows exist but need reclassification",
        "general_reextract_needed": "Needs targeted re-extraction",
        "stable_or_resolved": "No major unresolved issue",
    }
    return labels.get(issue, issue.replace("_", " ").title())


def _score_summary(row: pd.Series) -> float:
    return float(
        row.get("missing_agency_years", 0) * 1.0
        + row.get("missing_years", 0) * 4.0
        + row.get("outlier_agency_years", 0) * 0.35
        + row.get("problem_years", 0) * 2.0
        + row.get("missing_years_with_zero_row_docs", 0) * 5.0
        + row.get("documented_not_comparable", 0) * 2.0
        + row.get("raw_rows_found_not_classified", 0) * 1.5
    )


def _severity_bucket(score: float) -> str:
    if score >= 500:
        return "critical"
    if score >= 150:
        return "high"
    if score >= 40:
        return "moderate"
    if score > 0:
        return "watch"
    return "stable"


def build_country_gap_deepdive() -> tuple[pd.DataFrame, pd.DataFrame]:
    run_df = _load_run_log()
    detail_rows: list[dict] = []
    summary_rows: list[dict] = []
    source_rows: list[dict] = []

    for country_dir in sorted(p for p in cfg.OUTPUT_DIR.iterdir() if p.is_dir()):
        gap_files = list(country_dir.glob("*_gap_report.csv"))
        if not gap_files:
            continue

        country = country_dir.name
        gap_df = _normalize_gap_df(_safe_read_csv(gap_files[0]))
        if gap_df.empty:
            continue

        queue_df = _load_country_output(country_dir, "reextract_queue")
        audit_df = _load_country_output(country_dir, "docx_audit")
        results_df = _load_country_output(country_dir, "docx_results")
        note = _load_note_signals(country)

        country_run = run_df[run_df.get("country", pd.Series(dtype=object)).astype(str) == country].copy() if not run_df.empty else pd.DataFrame()
        if not country_run.empty:
            country_run["year"] = pd.to_numeric(country_run["year"], errors="coerce")
            country_run["rows_extracted"] = pd.to_numeric(country_run["rows_extracted"], errors="coerce")
            country_run["source_file"] = country_run.get("source_file", pd.Series(dtype=object)).fillna("").astype(str)

        if not audit_df.empty:
            audit_df["year"] = pd.to_numeric(audit_df.get("year"), errors="coerce")
            audit_df["source_file"] = audit_df.get("source_file", pd.Series(dtype=object)).fillna("").astype(str)
            audit_df["canonical_name"] = audit_df.get("canonical_name", pd.Series(dtype=object)).fillna("").astype(str)
            if "in_series" in audit_df.columns:
                audit_df["in_series"] = audit_df["in_series"].fillna(False).astype(bool)
        if not results_df.empty:
            results_df["year"] = pd.to_numeric(results_df.get("year"), errors="coerce")
            results_df["source_file"] = results_df.get("source_file", pd.Series(dtype=object)).fillna("").astype(str)
            results_df["decision"] = results_df.get("decision", pd.Series(dtype=object)).fillna("").astype(str)
            for col in ("line_description_en", "line_description", "section_name_en", "section_name"):
                if col in results_df.columns:
                    results_df[col] = results_df[col].fillna("").astype(str)

        problems = gap_df[gap_df["gap_type"].ne("ok")].copy()
        if problems.empty and country_run.empty:
            continue

        missing_df = problems[problems["gap_type"].eq("missing")].copy()
        outlier_df = problems[problems["gap_type"].eq("outlier")].copy()
        missing_years = sorted({int(y) for y in missing_df["year"].dropna().tolist()})
        outlier_years = sorted({int(y) for y in outlier_df["year"].dropna().tolist()})

        year_issue_counts = {key: 0 for key in _ISSUE_PRIORITY}
        problem_years = sorted({int(y) for y in problems["year"].dropna().tolist()})
        missing_years_with_runs = 0
        missing_years_with_zero_docs = 0
        missing_years_without_runs = 0

        for year in problem_years:
            year_problems = problems[problems["year"].eq(year)].copy()
            year_run = country_run[country_run["year"].eq(year)].copy() if not country_run.empty else pd.DataFrame()
            zero_row_docs = int((year_run.get("rows_extracted", pd.Series(dtype=float)).fillna(-1) == 0).sum()) if not year_run.empty else 0
            any_runs = not year_run.empty
            if year in missing_years:
                if any_runs:
                    missing_years_with_runs += 1
                else:
                    missing_years_without_runs += 1
                if zero_row_docs > 0:
                    missing_years_with_zero_docs += 1

            diagnoses = " ".join(year_problems["diagnosis"].astype(str).tolist()).lower()
            actions = set(year_problems["action"].astype(str))
            subtypes = set(year_problems["gap_subtype"].astype(str))

            if "documented_but_not_comparable" in subtypes or "not comparable" in diagnoses:
                issue = "document_changed_or_not_comparable"
            elif any(key in diagnoses for key in ("unsupported", "missing source", "replace source", "find better annex source")):
                issue = "unsupported_or_missing_source"
            elif zero_row_docs > 0:
                issue = "parsed_but_zero_rows"
            elif any(key in diagnoses for key in ("summary", "aggregate", "legal text", "legal wrapper", "brief")):
                issue = "aggregate_or_summary_only_source"
            elif any(key in diagnoses for key in ("ocr", "text layer", "damaged")):
                issue = "ocr_or_text_layer_problem"
            elif "verify" in actions or year_problems["gap_type"].eq("outlier").any():
                issue = "outlier_or_unit_break"
            elif "reclassify" in actions:
                issue = "raw_rows_found_needs_reclassification"
            else:
                issue = "general_reextract_needed"

            year_issue_counts[issue] += 1

            queue_sources = []
            if not queue_df.empty and "year" in queue_df.columns:
                queue_slice = queue_df[pd.to_numeric(queue_df["year"], errors="coerce").eq(year)]
                if "source_file" in queue_slice.columns:
                    queue_sources = sorted({str(v) for v in queue_slice["source_file"].dropna().tolist()})

            run_sources = sorted({str(v) for v in year_run.get("source_file", pd.Series(dtype=object)).dropna().tolist()}) if any_runs else []
            agencies = sorted({str(v) for v in year_problems["canonical_name"].dropna().tolist()})

            detail_rows.append(
                {
                    "country": country,
                    "year": year,
                    "issue_type": issue,
                    "issue_label": _issue_label(issue),
                    "missing_agency_years": int(year_problems["gap_type"].eq("missing").sum()),
                    "outlier_agency_years": int(year_problems["gap_type"].eq("outlier").sum()),
                    "actions": ", ".join(sorted({a for a in actions if a and a != "none"})),
                    "gap_subtypes": ", ".join(sorted({s for s in subtypes if s})),
                    "agencies": " | ".join(agencies[:6]),
                    "run_docs_for_year": int(len(year_run)),
                    "zero_row_docs_for_year": zero_row_docs,
                    "queue_source_files": " | ".join(queue_sources[:10]),
                    "run_source_files": " | ".join(run_sources[:10]),
                    "diagnosis_excerpt": " | ".join(
                        list(dict.fromkeys([str(v).strip() for v in year_problems["diagnosis"].tolist() if str(v).strip()]))[:3]
                    ),
                }
            )

        year_issue_map = {
            int(row["year"]): row["issue_type"]
            for row in detail_rows
            if row["country"] == country and pd.notna(row.get("year"))
        }
        year_diag_map = {
            int(row["year"]): row["diagnosis_excerpt"]
            for row in detail_rows
            if row["country"] == country and pd.notna(row.get("year"))
        }
        queue_grouped = {}
        if not queue_df.empty:
            queue_df["year"] = pd.to_numeric(queue_df.get("year"), errors="coerce")
            queue_df["source_file"] = queue_df.get("source_file", pd.Series(dtype=object)).fillna("").astype(str)
            queue_df["missing_agencies"] = queue_df.get("missing_agencies", pd.Series(dtype=object)).fillna("").astype(str)
            for (year, source_file), grp in queue_df.groupby(["year", "source_file"], dropna=False):
                if pd.isna(year) or not str(source_file).strip():
                    continue
                queue_grouped[(int(year), str(source_file))] = {
                    "missing_agencies": " | ".join(sorted({v for v in grp["missing_agencies"].tolist() if v})),
                    "queue_rows": int(len(grp)),
                }

        source_keys: set[tuple[int, str]] = set()
        if not country_run.empty:
            source_keys.update(
                {
                    (int(year), source)
                    for year, source in zip(country_run["year"], country_run["source_file"])
                    if pd.notna(year) and str(source).strip()
                }
            )
        if not queue_df.empty:
            source_keys.update(
                {
                    (int(year), source)
                    for year, source in zip(queue_df["year"], queue_df["source_file"])
                    if pd.notna(year) and str(source).strip()
                }
            )
        if not audit_df.empty:
            source_keys.update(
                {
                    (int(year), source)
                    for year, source in zip(audit_df["year"], audit_df["source_file"])
                    if pd.notna(year) and str(source).strip()
                }
            )
        if not results_df.empty:
            source_keys.update(
                {
                    (int(year), source)
                    for year, source in zip(results_df["year"], results_df["source_file"])
                    if pd.notna(year) and str(source).strip()
                }
            )

        currencies = sorted(
            {
                str(v).strip()
                for v in gap_df.get("series_currency", pd.Series(dtype=object)).dropna().tolist()
                if str(v).strip()
            }
        )
        run_docs = int(len(country_run))
        zero_row_docs = int((country_run.get("rows_extracted", pd.Series(dtype=float)).fillna(-1) == 0).sum()) if not country_run.empty else 0
        zero_row_years = (
            int(country_run.loc[country_run["rows_extracted"].fillna(-1).eq(0), "year"].dropna().nunique()) if not country_run.empty else 0
        )

        primary_issue = _pick_primary_issue(
            year_issue_counts,
            note,
            missing_years_with_runs,
            missing_agency_years=int(len(missing_df)),
            outlier_agency_years=int(len(outlier_df)),
        )
        summary_rows.append(
            {
                "country": country,
                "problem_years": len(problem_years),
                "missing_agency_years": int(len(missing_df)),
                "outlier_agency_years": int(len(outlier_df)),
                "missing_years": len(missing_years),
                "outlier_years": len(outlier_years),
                "documented_not_comparable": int(missing_df["gap_subtype"].eq("documented_but_not_comparable").sum()),
                "reextract_cases": int(problems["action"].eq("reextract").sum()),
                "verify_cases": int(problems["action"].eq("verify").sum()),
                "raw_rows_found_not_classified": int(problems["action"].eq("reclassify").sum()),
                "run_docs": run_docs,
                "zero_row_docs": zero_row_docs,
                "zero_row_years": zero_row_years,
                "missing_years_with_run_logs": missing_years_with_runs,
                "missing_years_with_zero_row_docs": missing_years_with_zero_docs,
                "missing_years_without_run_logs": missing_years_without_runs,
                "zero_row_doc_share": round((zero_row_docs / run_docs), 4) if run_docs else 0.0,
                "missing_year_ranges": _compress_years(missing_years),
                "outlier_year_ranges": _compress_years(outlier_years),
                "currencies_seen": ", ".join(currencies),
                "document_change_signal": note.document_changed,
                "aggregate_source_signal": note.aggregate_source,
                "ocr_signal": note.ocr_issue,
                "unsupported_format_signal": note.unsupported_format,
                "currency_or_unit_break_signal": note.currency_break or len(currencies) > 1,
                "source_inventory_gap_signal": note.source_gap,
                "primary_issue_type": primary_issue,
                "primary_issue_label": _issue_label(primary_issue),
                "note_excerpt": note.note_excerpt,
                "note_path": note.note_path,
            }
        )

        for year, source_file in sorted(source_keys):
            run_slice = country_run[country_run["year"].eq(year) & country_run["source_file"].eq(source_file)] if not country_run.empty else pd.DataFrame()
            audit_slice = audit_df[audit_df["year"].eq(year) & audit_df["source_file"].eq(source_file)] if not audit_df.empty else pd.DataFrame()
            results_slice = results_df[results_df["year"].eq(year) & results_df["source_file"].eq(source_file)] if not results_df.empty else pd.DataFrame()
            queue_info = queue_grouped.get((year, source_file), {})

            run_rows = int(run_slice["rows_extracted"].fillna(0).sum()) if not run_slice.empty else 0
            zero_row_docs = int((run_slice.get("rows_extracted", pd.Series(dtype=float)).fillna(-1) == 0).sum()) if not run_slice.empty else 0
            if not run_slice.empty:
                if run_rows > 0:
                    run_status = "ran_with_rows"
                elif zero_row_docs > 0:
                    run_status = "ran_zero_rows"
                else:
                    run_status = "ran_no_rows_signal"
            else:
                run_status = "not_in_run_log"

            audit_rows = int(len(audit_slice))
            audit_in_series = int(audit_slice["in_series"].sum()) if not audit_slice.empty and "in_series" in audit_slice.columns else 0
            audit_entities = (
                " | ".join(sorted({v for v in audit_slice["canonical_name"].tolist() if v})[:8])
                if not audit_slice.empty else ""
            )
            result_rows = int(len(results_slice))
            result_include_rows = int(results_slice["decision"].eq("include").sum()) if not results_slice.empty else 0
            result_labels = ""
            if not results_slice.empty:
                label_series = (
                    results_slice.get("line_description_en", pd.Series(dtype=object)).replace("", pd.NA)
                    .fillna(results_slice.get("line_description", pd.Series(dtype=object)).replace("", pd.NA))
                    .fillna(results_slice.get("section_name_en", pd.Series(dtype=object)).replace("", pd.NA))
                    .fillna(results_slice.get("section_name", pd.Series(dtype=object)).replace("", pd.NA))
                )
                result_labels = " | ".join(sorted({str(v) for v in label_series.dropna().tolist() if str(v).strip()})[:8])

            issue_type = year_issue_map.get(year, "stable_or_resolved")
            year_doc_change_flag = bool(issue_type == "document_changed_or_not_comparable")
            source_rows.append(
                {
                    "country": country,
                    "year": year,
                    "source_file": source_file,
                    "run_status": run_status,
                    "run_log_docs": int(len(run_slice)),
                    "run_log_rows_extracted": run_rows,
                    "run_log_zero_row_docs": zero_row_docs,
                    "docx_results_rows": result_rows,
                    "docx_results_include_rows": result_include_rows,
                    "docx_audit_rows": audit_rows,
                    "docx_audit_in_series_rows": audit_in_series,
                    "extracted_entities": audit_entities or result_labels,
                    "missing_agencies_from_queue": queue_info.get("missing_agencies", ""),
                    "queue_rows": int(queue_info.get("queue_rows", 0)),
                    "year_issue_type": issue_type,
                    "year_issue_label": _issue_label(issue_type),
                    "year_document_change_flag": year_doc_change_flag,
                    "country_document_change_signal": bool(note.document_changed),
                    "document_change_note": note.note_excerpt if (year_doc_change_flag or note.document_changed) else "",
                    "diagnosis_excerpt": year_diag_map.get(year, ""),
                    "note_path": note.note_path,
                }
            )

    summary_df = pd.DataFrame(summary_rows)
    if summary_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    summary_df["criticality_score"] = summary_df.apply(_score_summary, axis=1)
    summary_df["severity"] = summary_df["criticality_score"].map(_severity_bucket)
    summary_df = summary_df.sort_values(
        ["criticality_score", "missing_agency_years", "outlier_agency_years"],
        ascending=[False, False, False],
    ).reset_index(drop=True)
    summary_df["criticality_rank"] = summary_df.index + 1

    detail_df = pd.DataFrame(detail_rows)
    if not detail_df.empty:
        detail_df = detail_df.merge(
            summary_df[["country", "criticality_rank", "criticality_score", "severity"]],
            on="country",
            how="left",
        ).sort_values(["criticality_rank", "year", "issue_type"]).reset_index(drop=True)

    source_df = pd.DataFrame(source_rows)
    if not source_df.empty:
        source_df = source_df.merge(
            summary_df[["country", "criticality_rank", "criticality_score", "severity"]],
            on="country",
            how="left",
        ).sort_values(["criticality_rank", "year", "source_file"]).reset_index(drop=True)
        source_df.to_csv(SOURCE_AUDIT_CSV, index=False)

    return summary_df, detail_df


def _write_markdown(summary_df: pd.DataFrame) -> None:
    lines = [
        "# Country Gap Deep Dive",
        "",
        "Generated from `gap_report`, `reextract_queue`, `run_log`, and source notes.",
        "",
        "## Highest-priority countries",
        "",
        "| Rank | Country | Severity | Score | Missing agency-years | Outliers | Primary issue | Missing years |",
        "| --- | --- | --- | ---: | ---: | ---: | --- | --- |",
    ]
    top = summary_df.head(15)
    for _, row in top.iterrows():
        lines.append(
            "| "
            + " | ".join(
                [
                    str(int(row["criticality_rank"])),
                    str(row["country"]),
                    str(row["severity"]),
                    f"{row['criticality_score']:.1f}",
                    str(int(row["missing_agency_years"])),
                    str(int(row["outlier_agency_years"])),
                    str(row["primary_issue_label"]),
                    str(row["missing_year_ranges"] or "—"),
                ]
            )
            + " |"
        )
    lines.extend(["", "## Files", "", f"- Summary CSV: `{SUMMARY_CSV.relative_to(cfg.PROJECT_ROOT)}`", f"- Detail CSV: `{DETAIL_CSV.relative_to(cfg.PROJECT_ROOT)}`"])
    lines.append(f"- Source audit CSV: `{SOURCE_AUDIT_CSV.relative_to(cfg.PROJECT_ROOT)}`")
    SUMMARY_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    summary_df, detail_df = build_country_gap_deepdive()
    SUMMARY_CSV.parent.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(SUMMARY_CSV, index=False)
    detail_df.to_csv(DETAIL_CSV, index=False)
    _write_markdown(summary_df)
    print(f"Wrote {SUMMARY_CSV}")
    print(f"Wrote {DETAIL_CSV}")
    print(f"Wrote {SOURCE_AUDIT_CSV}")
    print(f"Wrote {SUMMARY_MD}")


if __name__ == "__main__":
    main()
