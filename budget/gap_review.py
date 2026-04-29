"""
LLM review pass for flagged gap/outlier cases.

This module reviews rows from a country gap report (typically action='verify')
by sending the model only:
  - the flagged agency-year
  - neighbouring series context
  - the candidate source-document rows for that year
  - small text snippets from those source documents around the agency name

It does NOT rerun full extraction. It is designed as a cheap second-opinion pass
on already-flagged cases.

Usage:
  python -m budget.gap_review --country Canada
  python -m budget.gap_review --country Canada --limit 10
  python -m budget.gap_review --country Canada --fresh
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from budget import config as cfg
from budget.agency_text_utils import (
    agency_variants,
    extract_snippets_from_text,
    load_gzip_text,
    load_lookup_cache,
    save_lookup_cache,
)
from budget.canonical_series import _get_agencies_for_country
from budget.llm_client import BudgetLLMClient
from budget.pdf_reader import extract_pages
from budget.pipeline import load_config

logger = logging.getLogger(__name__)

_CACHE_NAMESPACE = "gap_review"

_SYSTEM_PROMPT = """You are reviewing a flagged government R&D budget series value.

You will receive:
  - one country, year, and canonical agency
  - the currently compiled annual total
  - neighbouring years for context
  - the component source-document rows that make up that year
  - small snippets from the relevant documents around the agency name

Your task:
  - decide whether the current annual total is correct
  - if not, propose the corrected annual total using ONLY the evidence shown

Return ONLY valid JSON:
{
  "verdict": "keep" | "correct" | "drop" | "unclear",
  "correct_amount_local": <number or null>,
  "correct_unit": "<unit string or null>",
  "preferred_source_files": ["<source_file>", "..."],
  "evidence_page": "<page number or empty string>",
  "confidence": <0.0-1.0>,
  "reason": "<brief explanation>"
}

Rules:
  - Use only the provided evidence.
  - If the current value is supported, return verdict="keep" and repeat the amount.
  - If a single source-document row is clearly wrong, propose the corrected annual total
    implied by the visible valid document rows.
  - Prefer rows whose totals are internally consistent with visible component lines.
  - If the evidence is insufficient, return verdict="unclear".
  - Do not invent source files, pages, or amounts.
"""


def _cache_key(country: str, year: int, canonical_name: str) -> list[str]:
    return [country, str(year), canonical_name.lower()]


def _load_review_cache(country: str, year: int, canonical_name: str) -> Optional[dict]:
    return load_lookup_cache(_CACHE_NAMESPACE, _cache_key(country, year, canonical_name))


def _save_review_cache(country: str, year: int, canonical_name: str, data: dict) -> None:
    save_lookup_cache(_CACHE_NAMESPACE, _cache_key(country, year, canonical_name), data)


def _agency_for_country(country: str, canonical_name: str) -> dict:
    agencies = _get_agencies_for_country(country)
    for agency in agencies:
        if agency.get("canonical_name") == canonical_name:
            return agency
    return {"canonical_name": canonical_name, "name_variants": [canonical_name]}


def _resolve_source_text(country: str, source_file: str, config: dict) -> str:
    budget_cfg = config.get("budget", {})
    pdf_root = Path(budget_cfg.get("pdf_root", str(cfg.PDF_ROOT)))
    text_cache_dir = Path(budget_cfg.get("pdf_text_cache_dir", str(cfg.PDF_TEXT_CACHE_DIR)))

    # Cached text-cache file already named in outputs
    source_path = Path(source_file)
    if source_path.suffix.lower() in {".txt", ".gz"}:
        country_cache = text_cache_dir / country
        candidates = [country_cache / source_path.name]
        if source_path.suffix.lower() == ".txt":
            candidates.append(country_cache / f"{source_path.name}.gz")
        for p in candidates:
            if p.exists():
                return load_gzip_text(p)

    # Original source PDF/DOCX
    doc_path = pdf_root / country / source_file
    if doc_path.exists():
        try:
            pages = extract_pages(doc_path, cache_dir=text_cache_dir / country, force_reextract=False)
            chunks = [f"=== Page {pg.page_num} ===\n{pg.text}" for pg in pages if pg.text]
            return "\n\n".join(chunks)
        except Exception as e:
            logger.warning(f"Could not extract text for {source_file}: {e}")
            return ""

    return ""


def _snippets_for_source(country: str, source_file: str, agency: dict, config: dict, max_snippets: int = 2) -> list[dict]:
    text = _resolve_source_text(country, source_file, config)
    if not text:
        return []
    variants = agency_variants(agency)
    snippets = extract_snippets_from_text(text, variants, max_snippets=max_snippets, before=20, after=1400)
    out = []
    for snip in snippets:
        out.append({
            "source_file": source_file,
            "page_number": snip.get("page_number", ""),
            "text": snip.get("text", ""),
        })
    return out


def _build_case_prompt(
    gap_row: pd.Series,
    detail_rows: pd.DataFrame,
    agency: dict,
    config: dict,
) -> str:
    country = str(gap_row["country"])
    year = int(gap_row["year"])
    canonical_name = str(gap_row["canonical_name"])

    detail_lines = []
    snippets = []
    for _, row in detail_rows.iterrows():
        detail_lines.append(
            json.dumps(
                {
                    "source_file": row.get("source_file", ""),
                    "amount_local": row.get("amount_local", None),
                    "unit": row.get("unit", ""),
                    "currency": row.get("currency", ""),
                    "item_type": row.get("item_type", ""),
                    "line_description_en": row.get("line_description_en", ""),
                    "page_number": row.get("page_number", ""),
                },
                ensure_ascii=False,
            )
        )
        snippets.extend(
            _snippets_for_source(country, str(row.get("source_file", "")), agency, config)
        )

    # De-duplicate snippets by (source_file, page_number)
    seen = set()
    unique_snippets = []
    for snip in snippets:
        key = (snip["source_file"], str(snip["page_number"]))
        if key in seen:
            continue
        seen.add(key)
        unique_snippets.append(snip)

    snippet_text = "\n\n".join(
        f"[SOURCE {s['source_file']} | PAGE {s['page_number']}]\n{s['text']}"
        for s in unique_snippets[:8]
    )

    return (
        f"Country: {country}\n"
        f"Year: {year}\n"
        f"Canonical agency: {canonical_name}\n"
        f"Category: {gap_row.get('category', '')}\n"
        f"Gap type: {gap_row.get('gap_type', '')}\n"
        f"Current annual total: {gap_row.get('series_amount', None)} {detail_rows['unit'].dropna().astype(str).iloc[0] if not detail_rows.empty else ''}\n"
        f"Previous year: {gap_row.get('prev_amount', None)}\n"
        f"Next year: {gap_row.get('next_amount', None)}\n"
        f"Diagnosis: {gap_row.get('diagnosis', '')}\n\n"
        f"Candidate source-document rows for this year:\n" + "\n".join(detail_lines) + "\n\n"
        f"Document snippets around the agency name:\n{snippet_text}"
    )


def review_gaps(
    country: str,
    config: dict,
    output_dir: Path = cfg.OUTPUT_DIR,
    only_action: str = "verify",
    limit: Optional[int] = None,
    use_cache: bool = True,
) -> pd.DataFrame:
    output_dir = Path(output_dir)
    country_dir = output_dir / country
    gap_path = country_dir / f"{country.lower().replace(' ','_')}_gap_report.csv"
    series_path = country_dir / f"{country.lower().replace(' ','_')}_docx_series.csv"

    if not gap_path.exists():
        raise FileNotFoundError(gap_path)
    if not series_path.exists():
        raise FileNotFoundError(series_path)

    gap_df = pd.read_csv(gap_path)
    series_df = pd.read_csv(series_path)

    review_df = gap_df.copy()
    if only_action:
        review_df = review_df[review_df["action"] == only_action].copy()
    if limit:
        review_df = review_df.head(limit).copy()

    if review_df.empty:
        logger.warning(f"No gap rows to review for {country}")
        return pd.DataFrame()

    client = BudgetLLMClient.from_config(config)
    agencies = {a.get("canonical_name"): a for a in _get_agencies_for_country(country)}
    out_rows: list[dict] = []

    logger.info(f"Gap review: {country} ({len(review_df)} cases)")

    for _, gap_row in review_df.iterrows():
        canonical_name = str(gap_row["canonical_name"])
        year = int(gap_row["year"])
        cached = _load_review_cache(country, year, canonical_name) if use_cache else None
        if cached is None:
            detail_rows = series_df[
                (series_df["year"] == year)
                & (series_df["canonical_name"] == canonical_name)
            ].copy()
            agency = agencies.get(canonical_name, _agency_for_country(country, canonical_name))
            prompt = _build_case_prompt(gap_row, detail_rows, agency, config)
            cached = client.call_json(
                system_prompt=_SYSTEM_PROMPT,
                user_prompt=prompt,
                max_tokens=500,
                operation=client.OP_OTHER,
            )
            if "_parse_error" not in cached:
                _save_review_cache(country, year, canonical_name, cached)

        out_rows.append({
            "country": country,
            "year": year,
            "canonical_name": canonical_name,
            "gap_type": gap_row.get("gap_type", ""),
            "action": gap_row.get("action", ""),
            "series_amount": gap_row.get("series_amount", None),
            "prev_amount": gap_row.get("prev_amount", None),
            "next_amount": gap_row.get("next_amount", None),
            "diagnosis": gap_row.get("diagnosis", ""),
            "verdict": cached.get("verdict", "unclear") if isinstance(cached, dict) else "unclear",
            "correct_amount_local": cached.get("correct_amount_local", None) if isinstance(cached, dict) else None,
            "correct_unit": cached.get("correct_unit", None) if isinstance(cached, dict) else None,
            "preferred_source_files": json.dumps(cached.get("preferred_source_files", []), ensure_ascii=False) if isinstance(cached, dict) else "[]",
            "evidence_page": cached.get("evidence_page", "") if isinstance(cached, dict) else "",
            "confidence": cached.get("confidence", None) if isinstance(cached, dict) else None,
            "reason": cached.get("reason", "") if isinstance(cached, dict) else "",
        })

    review_out = pd.DataFrame(out_rows)
    out_path = country_dir / f"{country.lower().replace(' ','_')}_gap_review.csv"
    review_out.to_csv(out_path, index=False)
    client.save_usage()

    logger.info(f"Gap review → {out_path} ({len(review_out)} rows)")
    return review_out


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")

    parser = argparse.ArgumentParser(description="LLM review pass for flagged budget gaps/outliers")
    parser.add_argument("--country", required=True)
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    parser.add_argument("--action", default="verify", help="Only review rows with this action (default: verify)")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--fresh", action="store_true", help="Ignore cached review results")
    args = parser.parse_args()

    config = load_config(Path(args.config) if args.config else None)
    df = review_gaps(
        country=args.country,
        config=config,
        only_action=args.action,
        limit=args.limit,
        use_cache=not args.fresh,
    )

    if not df.empty:
        print("\n=== Gap review summary ===")
        print(df[["year", "canonical_name", "verdict", "correct_amount_local", "confidence", "reason"]].to_string(index=False))
