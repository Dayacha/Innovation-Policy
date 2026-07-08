from __future__ import annotations

import argparse
import html
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REPORT_HTML = ROOT / "Data/output/budget/gap_investigation_report.html"
OUTPUT_CSV = ROOT / "Data/output/budget/gap_manual_review_queue.csv"
OUTPUT_MD = ROOT / "Data/output/budget/gap_manual_review_queue.md"
COUNTRY_OUTPUT_DIR = ROOT / "Data/output/budget"


@dataclass(frozen=True)
class NoteOverride:
    status: str
    basis: str


MANUAL_OVERRIDES: dict[tuple[str, int], NoteOverride] = {
    ("Australia", 2000): NoteOverride(
        "No",
        "Direct inspection of the available 2000 Australia .docx files found no CSIRO, ARC, NHMRC, ANSTO, or AIMS hits; the local source set looks like appropriation acts/supplementary volumes rather than the needed science portfolio detail.",
    ),
    ("Australia", 2001): NoteOverride(
        "No",
        "Direct inspection of the available 2001 Australia .docx files found no CSIRO, ARC, NHMRC, ANSTO, or AIMS hits; the local source set looks like appropriation acts/supplementary volumes rather than the needed science portfolio detail.",
    ),
    ("Colombia", 2003): NoteOverride(
        "No",
        "Colombia source notes and direct PDF inspection show the 2003 Ley 780 de 2002 file behaves like weak wrapper/legal text and does not preserve a usable institutional annex.",
    ),
    ("Colombia", 2007): NoteOverride(
        "No",
        "Colombia source notes and direct PDF inspection show the 2007 Ley 1110 de 2006 source only preserves SENA-to-COLCIENCIAS transfer language without a traceable institutional appropriation amount.",
    ),
    ("Netherlands", 1996): NoteOverride(
        "No",
        "Netherlands 1996 review table says the file runs with rows but only exposes aggregate OCW Art. 16 evidence, not a defendable NWO/KNAW split; the quality note flags this as a comparability/document-structure issue.",
    ),
    ("Slovenia", 2014): NoteOverride(
        "No",
        "Manual Slovenia review says remaining 2001 and 2003-2015 Programme 0503 gaps should be kept unless a defendable 050302 annual amount is verified from the original PDF.",
    ),
    ("New Zealand", 1977): NoteOverride(
        "No",
        "New Zealand audit marks 1977 DSIR as a locked conservative gap: original evidence was absent, ambiguous, or not clean enough to defend.",
    ),
    ("New Zealand", 1984): NoteOverride(
        "No",
        "New Zealand audit marks 1984 DSIR as a locked conservative gap: original evidence was absent, ambiguous, or not clean enough to defend.",
    ),
    ("New Zealand", 1995): NoteOverride(
        "No",
        "New Zealand audit marks 1995 Research, Science and Technology Vote as a locked conservative gap.",
    ),
    ("UK", 2000): NoteOverride(
        "No",
        "UK audit found no qualifying single-year R&D appropriation in the 2000 Red Book after full review; remaining figures are general funds, tax measures, or excluded categories.",
    ),
    ("UK", 2001): NoteOverride(
        "No",
        "UK audit confirms 2001 is a genuine content gap: R&D mentions are tax-credit policy discussion, not a named single-year appropriation.",
    ),
    ("UK", 2002): NoteOverride(
        "No",
        "UK audit confirms 2002 is a source-corpus gap: the Budget 2002 document is not present in the local archive.",
    ),
    ("France", 2018): NoteOverride(
        "No",
        "France 2018 JORF programme totals have already been manually recovered from the original budget law; the remaining year-level gaps are agency rows that do not appear in the local JORF corpus and would require PAP annexes or companion budget documents.",
    ),
    ("France", 2019): NoteOverride(
        "No",
        "France 2019 JORF programme totals have already been manually recovered from the original budget law; the remaining year-level gaps are agency rows that do not appear in the local JORF corpus and would require PAP annexes or companion budget documents.",
    ),
    ("France", 2020): NoteOverride(
        "No",
        "France 2020 JORF programme totals have already been manually recovered from the original budget law; the remaining year-level gaps are agency rows that do not appear in the local JORF corpus and would require PAP annexes or companion budget documents.",
    ),
    ("France", 2021): NoteOverride(
        "No",
        "France 2021 direct JORF review still leaves the remaining canonicals blocked by missing annex-dependent agency rows; even the last checked programme line (`Cultural Research and Scientific Culture`) does not surface as a defendable row in the local source set.",
    ),
    ("France", 2022): NoteOverride(
        "No",
        "France 2022 direct JORF review still leaves the remaining canonicals blocked by missing annex-dependent agency rows; even the last checked programme line (`Cultural Research and Scientific Culture`) does not surface as a defendable row in the local source set.",
    ),
    ("France", 2023): NoteOverride(
        "No",
        "France 2023 current JORF-only review still leaves a large block of missing agency canonicals, indicating the unresolved rows depend on PAP annexes or companion budget documents rather than recoverable JORF mission lines.",
    ),
    ("France", 2024): NoteOverride(
        "No",
        "France 2024 current JORF-only review still leaves a large block of missing agency canonicals, indicating the unresolved rows depend on PAP annexes or companion budget documents rather than recoverable JORF mission lines.",
    ),
    ("France", 2025): NoteOverride(
        "No",
        "France 2025 current JORF-only review still leaves a large block of missing agency canonicals; unresolved rows are annex-dependent and the one remaining outlier/reclassify issue does not change the year-level conclusion.",
    ),
    ("Hungary", 2000): NoteOverride(
        "No",
        "Original Hungary PDF review confirms the MTA chapter heading is present but the chapter-total amount is truncated in the text layer; candidate snippets are inconsistent sub-lines rather than a defendable annual institutional total.",
    ),
    ("Hungary", 2003): NoteOverride(
        "No",
        "Original Hungary PDF review confirms the MTA chapter total is only preserved as a truncated layout fragment ('XXXIII. fejezet osszesen: 50 8'), while candidate extracted rows are inconsistent and not comparable to the audited chapter-total series.",
    ),
    ("Hungary", 2004): NoteOverride(
        "No",
        "Original Hungary PDF review shows the MTA chapter is present but the chapter-total amount is not recoverable from the current text layer; the surviving 'MTA mukodesi koltsegvetese' snippet is a partial operating-budget line, not a defendable replacement for the institutional chapter total used elsewhere in the series.",
    ),
    ("Hungary", 2006): NoteOverride(
        "No",
        "Original Hungary PDF review confirms the MTA chapter total is truncated to a partial '48' in the layout text, and the surviving candidate row is only a sub-line for the Office of Supported Research Places, not the audited MTA chapter total.",
    ),
    ("Hungary", 2008): NoteOverride(
        "No",
        "Original Hungary PDF review confirms the MTA chapter total is truncated to a partial '50' in the layout text; no exact institutional total is recoverable from the current source text layer.",
    ),
    ("Hungary", 2009): NoteOverride(
        "No",
        "Original Hungary PDF review confirms the MTA chapter total is truncated to a partial '49' in the layout text; the current source only supports a rounded chapter-level reading, not a reproducible exact annual institutional total.",
    ),
}


def _strip_tags(value: str) -> str:
    value = re.sub(r"<[^>]+>", " ", value)
    value = html.unescape(value)
    return re.sub(r"\s+", " ", value).strip()


def _slug(country: str) -> str:
    return country.lower().replace(" ", "_")


def parse_report_rows(report_html: Path) -> list[dict[str, str]]:
    text = report_html.read_text(encoding="utf-8")
    rows: list[dict[str, str]] = []
    row_pattern = re.compile(
        r'<tr class="gap-row" data-country="(?P<country>[^"]+)" data-fixable="(?P<fixable>[^"]+)">(?P<body>.*?)</tr>',
        re.S,
    )
    for match in row_pattern.finditer(text):
        body = match.group("body")
        country = match.group("country")
        year_match = re.search(r'<td class="td-year">(\d+)</td>', body)
        if not year_match:
            continue
        category_match = re.search(r'<td class="td-category">(.*?)</td>', body, re.S)
        tried_match = re.search(r'<td class="td-tried">(.*?)</td>', body, re.S)
        conclusion_match = re.search(r'<td class="td-conclusion">(.*?)</td>', body, re.S)
        file_matches = re.findall(r"<li>(.*?)</li>", body, re.S)
        rows.append(
            {
                "country": country,
                "year": int(year_match.group(1)),
                "report_fixable": match.group("fixable"),
                "report_category": _strip_tags(category_match.group(1)) if category_match else "",
                "report_tried": _strip_tags(tried_match.group(1)) if tried_match else "",
                "report_conclusion": _strip_tags(conclusion_match.group(1)) if conclusion_match else "",
                "report_source_files": " | ".join(_strip_tags(v) for v in file_matches),
            }
        )
    return rows


def load_current_gap_years(country: str) -> dict[int, dict[str, str]]:
    path = COUNTRY_OUTPUT_DIR / country / f"{_slug(country)}_gap_report.csv"
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    if "gap_type" not in df.columns or "year" not in df.columns:
        return {}
    df = df[df["gap_type"].fillna("ok") != "ok"].copy()
    if df.empty:
        return {}
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    out: dict[int, dict[str, str]] = {}
    for year, grp in df.groupby(df["year"].astype(int)):
        actions = sorted({str(v).strip() for v in grp.get("action", pd.Series(dtype=str)).dropna() if str(v).strip()})
        canonicals = sorted({str(v).strip() for v in grp.get("canonical_name", pd.Series(dtype=str)).dropna() if str(v).strip()})
        out[int(year)] = {
            "current_gap_report_has_gap": "Yes",
            "current_gap_report_actions": " | ".join(actions),
            "current_gap_report_canonicals": " | ".join(canonicals[:8]),
            "current_gap_report_row_count": str(len(grp)),
        }
    return out


def load_review_table(country: str) -> dict[int, dict[str, str]]:
    matches = sorted((COUNTRY_OUTPUT_DIR / country).glob("*_country_gap_review_table.csv"))
    if not matches:
        return {}
    df = pd.read_csv(matches[0])
    if "year" not in df.columns:
        return {}
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    out: dict[int, dict[str, str]] = {}
    for year, grp in df.groupby(df["year"].astype(int)):
        labels = sorted({str(v).strip() for v in grp.get("year_issue_label", pd.Series(dtype=str)).dropna() if str(v).strip()})
        actions = sorted({str(v).strip() for v in grp.get("recommended_action", pd.Series(dtype=str)).dropna() if str(v).strip()})
        diags = sorted({str(v).strip() for v in grp.get("diagnosis_excerpt", pd.Series(dtype=str)).dropna() if str(v).strip()})
        out[int(year)] = {
            "review_table_issue_labels": " | ".join(labels),
            "review_table_actions": " | ".join(actions),
            "review_table_diagnosis": " | ".join(diags[:3]),
            "review_table_row_count": str(len(grp)),
        }
    return out


def infer_status(row: dict[str, str]) -> tuple[str, str]:
    key = (row["country"], row["year"])
    if key in MANUAL_OVERRIDES:
        override = MANUAL_OVERRIDES[key]
        return override.status, override.basis

    if row["current_gap_report_has_gap"] == "No":
        return "Resolved/Stale", "Row appears in the static HTML report but is not present in the current country gap_report.csv."

    review_actions = row["review_table_actions"].lower()
    review_diag = row["review_table_diagnosis"].lower()
    if "no action needed" in review_actions:
        return "Resolved", "Country review table says no action needed."
    if "keep the gap" in review_actions or "manual audit only" in review_actions or "do not use this source" in review_actions:
        return "No", "Country review table says the gap should be kept with current evidence."
    if "unsupported format" in row["review_table_issue_labels"].lower():
        return "No", "Country review marks the source as unsupported or misfiled."
    if "reclassification" in review_diag or "already contain a matching row" in review_diag:
        return "Potentially", "Current gap report indicates downstream reclassification rather than a missing source."
    if row["current_gap_report_has_gap"] == "Yes":
        return row["report_fixable"], "Still present in the current gap report; no stronger audited override found."
    return row["report_fixable"], "No stronger evidence found."


def build_queue() -> pd.DataFrame:
    report_rows = parse_report_rows(REPORT_HTML)
    countries = sorted({row["country"] for row in report_rows})
    gap_maps = {country: load_current_gap_years(country) for country in countries}
    review_maps = {country: load_review_table(country) for country in countries}

    enriched: list[dict[str, str]] = []
    for row in report_rows:
        current = gap_maps[row["country"]].get(row["year"], {})
        review = review_maps[row["country"]].get(row["year"], {})
        merged = {
            **row,
            "current_gap_report_has_gap": current.get("current_gap_report_has_gap", "No"),
            "current_gap_report_actions": current.get("current_gap_report_actions", ""),
            "current_gap_report_canonicals": current.get("current_gap_report_canonicals", ""),
            "current_gap_report_row_count": current.get("current_gap_report_row_count", "0"),
            "review_table_issue_labels": review.get("review_table_issue_labels", ""),
            "review_table_actions": review.get("review_table_actions", ""),
            "review_table_diagnosis": review.get("review_table_diagnosis", ""),
            "review_table_row_count": review.get("review_table_row_count", "0"),
        }
        suggested_status, review_basis = infer_status(merged)
        merged["suggested_status"] = suggested_status
        merged["review_basis"] = review_basis
        enriched.append(merged)

    df = pd.DataFrame(enriched).sort_values(["country", "year"]).reset_index(drop=True)
    return df


def write_markdown(df: pd.DataFrame, out_path: Path) -> None:
    lines = [
        "# Gap Manual Review Queue",
        "",
        "Generated from the static HTML gap report, the current country gap reports, and audited country review outputs.",
        "",
        "## Status counts",
        "",
    ]
    counts = df["suggested_status"].value_counts().sort_index()
    for status, count in counts.items():
        lines.append(f"- `{status}`: {count}")
    lines.extend(["", "## Sample rows", "", "| Country | Year | Report | Current gap? | Suggested | Basis |", "| --- | ---: | --- | --- | --- | --- |"])
    for _, row in df.head(20).iterrows():
        basis = str(row["review_basis"]).replace("|", "/")
        lines.append(
            f"| {row['country']} | {int(row['year'])} | {row['report_fixable']} | {row['current_gap_report_has_gap']} | {row['suggested_status']} | {basis} |"
        )
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-csv", type=Path, default=OUTPUT_CSV)
    parser.add_argument("--output-md", type=Path, default=OUTPUT_MD)
    args = parser.parse_args()

    df = build_queue()
    df.to_csv(args.output_csv, index=False)
    write_markdown(df, args.output_md)
    print(f"Wrote {len(df)} rows to {args.output_csv}")
    print(df["suggested_status"].value_counts().sort_index().to_string())


if __name__ == "__main__":
    main()
