"""
Targeted agency recovery for budget extraction.

After the expensive document-level extraction pass, this module looks for a
specific failure mode:

  - a known agency name appears in the cached document text
  - but no extracted row for that source file mentions the agency

When that happens, it sends only a small snippet around the agency mention to
the LLM and asks for the missing appropriation lines. This is much cheaper than
re-running extraction for the full document and is designed to run before the
cleaners/dedup step so any recovered rows flow through the normal pipeline.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Optional

from budget import config as cfg
from budget.agency_text_utils import (
    agency_variants,
    extract_snippets_from_text,
    load_shared_agency_lookup_cache,
    save_shared_agency_lookup_cache,
    text_mentions,
)
from budget.canonical_series import _get_agencies_for_country
from budget.llm_client import BudgetLLMClient
from budget.pdf_reader import extract_pages

logger = logging.getLogger(__name__)

# Limit scope to institutions / research bodies; generic programmes are better
# handled by the main extraction logic and ordinary dedup.
_ELIGIBLE_CATEGORIES = {"science_agency", "research_infrastructure"}

_SYSTEM_PROMPT = """You are a government budget extraction specialist.

You will be given:
  - one source document name
  - one target agency
  - a few text snippets from that document around the agency name

Your task is to recover ONLY the appropriation lines that clearly belong to the
target agency in those snippets.

Return ONLY valid JSON:
{
  "found": true or false,
  "items": [
    {
      "page_number": <integer>,
      "item_type": "line_item" | "program_total" | "section_total",
      "line_description_en": "<short English label>",
      "amount_local": <number>,
      "unit": "dollar" | "thousand" | "million" | "billion" | "unknown",
      "decision": "include" | "review",
      "confidence": <0.0-1.0>,
      "notes": "<short note>"
    }
  ]
}

Rules:
  - Use only the provided snippets.
  - If the agency is mentioned but no amount is actually shown, return found=false.
  - If both component lines and a total are shown, include all distinct lines.
  - The same agency may appear more than once in the document (for example English and French schedules, or repeated references). Reconcile all snippets before answering.
  - Treat the agency heading as the start of the relevant block. Do not extract any amount or line item that appears before the agency heading in a snippet.
  - Prefer amounts that appear inside the target agency's own block, after the agency heading. Do not use a number that belongs to the previous block just because it appears immediately before the heading.
  - If operating/capital/grants/contributions lines and a total are shown for the same agency block, prefer the total that matches the visible component lines.
  - If two occurrences disagree, prefer the occurrence where the total is internally consistent with the visible component lines; otherwise return the disputed line with decision="review" and explain briefly in notes.
  - Keep item labels short and literal.
  - Amounts must be exactly as printed in the snippet, not converted.
"""

def _load_cache(country: str, source_file: str, canonical_name: str) -> Optional[dict]:
    return load_shared_agency_lookup_cache(country, source_file, canonical_name)


def _save_cache(country: str, source_file: str, canonical_name: str, data: dict) -> None:
    save_shared_agency_lookup_cache(country, source_file, canonical_name, data)


def _variants(agency: dict) -> list[str]:
    return agency_variants(agency)


def _text_mentions(text: str, variants: list[str]) -> bool:
    low = text.lower()
    for v in variants:
        q = v.lower()
        if len(q) <= 4:
            if re.search(r"(?<![a-z])" + re.escape(q) + r"(?![a-z])", low):
                return True
        elif q in low:
            return True
    return False


def _row_mentions(row: dict, variants: list[str]) -> bool:
    blob = " ".join(
        str(row.get(c, "") or "")
        for c in ("section_name", "section_name_en", "line_description", "line_description_en")
    )
    return _text_mentions(blob, variants)


def _recovery_currency(country: str, year: int) -> str:
    # Belgium switched from BEF to EUR in 2002. Using the modern country
    # default during targeted recovery made the LLM hallucinate EUR rows for
    # pre-2002 snippets, which then could not enter the canonical series.
    if country == "Belgium" and year < 2002:
        return "BEF"
    return cfg.COUNTRY_CONTEXT.get(country, cfg.DEFAULT_COUNTRY_CONTEXT).get("currency", "LOCAL")


def _agency_candidates(country: str, year: int) -> list[dict]:
    agencies = []
    for agency in _get_agencies_for_country(country):
        if agency.get("category") not in _ELIGIBLE_CATEGORIES:
            continue
        active_start, active_end = agency.get("active_years", (1800, 2099))
        if year < int(active_start) or year > int(active_end):
            continue
        agencies.append(agency)
    return agencies


def _snippet_for_agency(path: Path, agency: dict, cache_dir: Path, max_snippets: int = 6) -> list[dict]:
    variants = _variants(agency)
    pages = extract_pages(path, cache_dir=cache_dir, force_reextract=False)
    snippets: list[dict] = []
    for pg in pages:
        text = pg.text or ""
        if not text_mentions(text, variants):
            continue
        # Make snippets strongly forward-looking from the agency heading so we
        # avoid capturing the previous block's totals or labels.
        for snip in extract_snippets_from_text(text, variants, max_snippets=2, before=20, after=1500):
            snippets.append({"page_number": pg.page_num, "text": snip["text"]})
            if len(snippets) >= max_snippets:
                return snippets
    return snippets


def _filter_recovered_rows(rows: list[dict]) -> list[dict]:
    """
    Keep only the coherent agency block when recovery returns a mixed set.

    In Canada appropriation tables, once a block has explicit components plus a
    total, a stray 'Program expenditures' line is usually bleed from the
    previous block and should be dropped.
    """
    if not rows:
        return rows

    labels = {
        str(r.get("line_description", "") or "").strip().lower(): r
        for r in rows
    }
    has_total = any("total" in label for label in labels)
    has_operating = "operating expenditures" in labels
    has_capital = "capital expenditures" in labels

    if has_total and (has_operating or has_capital):
        rows = [
            r for r in rows
            if str(r.get("line_description", "") or "").strip().lower() != "program expenditures"
        ]

    return rows


def _recover_for_agency(
    country: str,
    year: int,
    source_file: str,
    path: Path,
    agency: dict,
    file_rows: list[dict],
    client: BudgetLLMClient,
    cache_dir: Path,
    currency: str,
    use_cache: bool = True,
) -> list[dict]:
    variants = _variants(agency)
    if any(_row_mentions(r, variants) for r in file_rows):
        return []

    snippets = _snippet_for_agency(path, agency, cache_dir=cache_dir)
    if not snippets:
        return []

    cached = _load_cache(country, source_file, agency["canonical_name"]) if use_cache else None
    if cached is None:
        context = "\n\n".join(
            f"[PAGE {s['page_number']}]\n{s['text']}" for s in snippets
        )
        user_prompt = (
            f"Country: {country}\n"
            f"Year: {year}\n"
            f"Document: {source_file}\n"
            f"Target agency: {agency['canonical_name']}\n"
            f"Known variants: {', '.join(variants[:8])}\n"
            f"Currency: {currency}\n\n"
            f"The agency may appear more than once in this document. Reconcile all snippets and return the correct rows for the same agency block only.\n\n"
            f"Relevant snippets:\n{context}"
        )
        try:
            cached = client.call_json(
                system_prompt=_SYSTEM_PROMPT,
                user_prompt=user_prompt,
                max_tokens=700,
                operation=client.OP_EXTRACT,
            )
        except Exception as e:
            logger.warning(
                f"[{country}] Targeted recovery LLM failed for {source_file} / "
                f"{agency['canonical_name']}: {e}"
            )
            return []
        if "_parse_error" not in cached:
            _save_cache(country, source_file, agency["canonical_name"], cached)

    if "_parse_error" in cached or not cached.get("found"):
        return []

    rows: list[dict] = []
    for item in cached.get("items", [])[:6]:
        try:
            amount = float(item.get("amount_local"))
        except (TypeError, ValueError):
            continue
        if amount <= 0:
            continue
        page_number = item.get("page_number")
        rows.append({
            "country": country,
            "year": year,
            "item_type": item.get("item_type", "line_item"),
            "section_code": "",
            "section_name": agency["canonical_name"],
            "section_name_en": agency["canonical_name"],
            "line_code": "",
            "line_description": item.get("line_description_en", ""),
            "line_description_en": item.get("line_description_en", ""),
            "amount_local": amount,
            "unit": item.get("unit", "unknown"),
            "currency": currency,
            "rd_category": agency.get("category", "unclear"),
            "decision": item.get("decision", "review"),
            "confidence": float(item.get("confidence", 0.7)),
            "source_file": source_file,
            "page_number": str(page_number) if page_number is not None else "",
            "llm_model": client.model,
            "extraction_pass": "targeted_recovery",
            "notes": item.get("notes", f"Recovered for missing agency: {agency['canonical_name']}"),
        })
    rows = _filter_recovered_rows(rows)
    if rows:
        logger.info(
            f"[{country}] Targeted recovery: {source_file} -> {agency['canonical_name']} "
            f"({len(rows)} rows)"
        )
    return rows


def recover_missing_agency_rows(
    all_rows: list[dict],
    file_specs: list[tuple[str, int, Path]],
    config: dict,
    pdf_text_cache_dir: Path,
    use_cache: bool = True,
) -> list[dict]:
    """
    Recover likely missed agency rows from cached document text.

    Runs after the main extraction pass and before cleaners/dedup. Only sends
    agency-local snippets to the LLM, and caches the responses by
    (country, source_file, canonical agency).
    """
    if not file_specs:
        return all_rows

    rows_by_file: dict[tuple[str, str], list[dict]] = {}
    for row in all_rows:
        key = (str(row.get("country", "")), str(row.get("source_file", "")))
        rows_by_file.setdefault(key, []).append(row)

    client = BudgetLLMClient.from_config(config)
    additions: list[dict] = []
    additions_by_file: dict[tuple[str, str], list[dict]] = {}

    for country, year, path in file_specs:
        key = (country, path.name)
        file_rows = rows_by_file.get(key, [])
        agencies = _agency_candidates(country, year)
        for agency in agencies:
            recovered = _recover_for_agency(
                country=country,
                year=year,
                source_file=path.name,
                path=path,
                agency=agency,
                file_rows=file_rows + additions_by_file.get(key, []),
                client=client,
                cache_dir=pdf_text_cache_dir,
                currency=_recovery_currency(country, year),
                use_cache=use_cache,
            )
            if recovered:
                additions.extend(recovered)
                additions_by_file.setdefault(key, []).extend(recovered)

    if additions:
        logger.info(f"Targeted recovery added {len(additions)} rows")
    client.save_usage()
    return all_rows + additions
