"""
Two-pass LLM extraction engine.

Pass 1 — SCAN (cheap, batched)
    Send groups of pages to a fast model and ask: is this page relevant?
    Pages scoring below SCAN_THRESHOLD are dropped.

Pass 2 — EXTRACT (quality, chunked)
    Send windows of relevant pages to the extraction model.
    Parse structured JSON → list[BudgetRow].

The JSON cache (LLM_CACHE_DIR) stores responses keyed by
(source_file_hash + chunk_hash) so no chunk is ever sent to the API twice.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from pathlib import Path
from typing import Optional

from budget import config as cfg
from budget.llm_client import BudgetLLMClient
from budget.output_schema import BudgetRow
from budget.pdf_reader import PageText, chunk_pages, get_page_range
from budget.prompts import (
    BATCH_SCAN_SYSTEM_PROMPT,
    EXTRACT_SYSTEM_PROMPT,
    CONSISTENCY_SYSTEM_PROMPT,
    build_batch_scan_user_prompt,
    build_extract_user_prompt,
    build_consistency_user_prompt,
)

logger = logging.getLogger(__name__)

_PORTUGAL_SKIP_SCAN_RE = re.compile(
    r"freguesia\s*/\s*munic|total\s+munic|adicional\s+total\s+transfer",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# LLM response cache (per chunk)
# ---------------------------------------------------------------------------

def _chunk_cache_key(source_file: str, chunk_text: str) -> str:
    content = f"{source_file}|{chunk_text}"
    return hashlib.md5(content.encode()).hexdigest()


def _load_chunk_cache(cache_dir: Path, key: str) -> Optional[dict]:
    p = cache_dir / f"{key}.json"
    if not p.exists():
        return None
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _save_chunk_cache(cache_dir: Path, key: str, data: dict) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    p = cache_dir / f"{key}.json"
    try:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"Failed to write chunk cache {key}: {e}")


# ---------------------------------------------------------------------------
# Pass 1 — Scan (batch relevance filter)
# ---------------------------------------------------------------------------

# How many pages per batch scan call
_SCAN_BATCH_SIZE = 8


def _scan_batch(
    batch: list[PageText],
    client: BudgetLLMClient,
    country: str,
    year: int,
    doc_hint: str,
    cache_dir: Optional[Path],
    source_file: str,
) -> dict:
    page_pairs = [(str(pg.page_num), pg.text) for pg in batch]
    user_prompt = build_batch_scan_user_prompt(page_pairs, country, year, doc_hint)
    cache_key = _chunk_cache_key(source_file + "_scan", user_prompt)
    cached = _load_chunk_cache(cache_dir, cache_key) if cache_dir else None

    if cached is not None:
        logger.debug(f"Scan cache hit: {cache_key[:8]}…")
        return cached

    result = client.call_json(
        system_prompt=BATCH_SCAN_SYSTEM_PROMPT,
        user_prompt=user_prompt,
        max_tokens=1024,
        operation=BudgetLLMClient.OP_SCAN,
    )
    if cache_dir:
        _save_chunk_cache(cache_dir, cache_key, result)
    return result


def scan_pages(
    pages: list[PageText],
    client: BudgetLLMClient,
    country: str,
    year: int,
    doc_hint: str = "",
    threshold: float = cfg.SCAN_THRESHOLD,
    cache_dir: Optional[Path] = None,
    source_file: str = "",
    scan_model: Optional[str] = None,
) -> list[PageText]:
    """
    Run pass-1 scan on all pages, return only those above the relevance threshold.

    If cache_dir is set, scan results are cached per-batch so re-runs are free.
    scan_model: override the model used for this pass (default: cfg.SCAN_MODEL).
    """
    if not pages:
        return []

    relevant: list[PageText] = []
    candidate_pages: list[PageText] = []
    for pg in pages:
        if country == "Portugal" and _PORTUGAL_SKIP_SCAN_RE.search(pg.text):
            logger.debug(f"  p{pg.page_num}: deterministically skipped (Portugal municipal transfer table)")
            continue
        candidate_pages.append(pg)

    batches = [candidate_pages[i : i + _SCAN_BATCH_SIZE] for i in range(0, len(candidate_pages), _SCAN_BATCH_SIZE)]

    original_model = client.model
    scan_model = scan_model or cfg.SCAN_MODEL
    if client.model != scan_model:
        client.switch_model(scan_model)

    try:
        for batch in batches:
            result = _scan_batch(
                batch=batch,
                client=client,
                country=country,
                year=year,
                doc_hint=doc_hint,
                cache_dir=cache_dir,
                source_file=source_file,
            )

            if "_parse_error" in result:
                logger.warning(f"Scan JSON parse error for batch starting p{batch[0].page_num}: {result.get('_parse_error')}")
                # Retry page-by-page so one malformed batch does not force an
                # entire block of irrelevant legal text into extraction.
                for pg in batch:
                    page_result = _scan_batch(
                        batch=[pg],
                        client=client,
                        country=country,
                        year=year,
                        doc_hint=doc_hint,
                        cache_dir=cache_dir,
                        source_file=source_file,
                    )
                    if "_parse_error" in page_result:
                        logger.warning(
                            f"Scan JSON parse error on single page p{pg.page_num}: "
                            f"{page_result.get('_parse_error')}"
                        )
                        continue
                    entry = next(
                        (item for item in page_result.get("pages", []) if str(item.get("page_id", "")) == str(pg.page_num)),
                        None,
                    )
                    if not entry:
                        continue
                    if entry.get("relevant", False) and float(entry.get("confidence", 0.0)) >= threshold:
                        relevant.append(pg)
                continue

            # Build lookup: page_id → confidence
            page_scores: dict[str, float] = {}
            for entry in result.get("pages", []):
                pid = str(entry.get("page_id", ""))
                rel = entry.get("relevant", False)
                conf = float(entry.get("confidence", 0.5))
                page_scores[pid] = conf if rel else 0.0

            for pg in batch:
                score = page_scores.get(str(pg.page_num), 0.0)
                if score >= threshold:
                    relevant.append(pg)
                    logger.debug(f"  p{pg.page_num}: relevant (score={score:.2f})")
                else:
                    logger.debug(f"  p{pg.page_num}: skipped (score={score:.2f})")

    finally:
        # Restore model
        if client.model != original_model:
            client.switch_model(original_model)

    logger.info(f"Scan pass: {len(relevant)}/{len(pages)} pages kept for extraction")
    return relevant


# ---------------------------------------------------------------------------
# Pass 2 — Extract (structured JSON extraction)
# ---------------------------------------------------------------------------

def extract_chunks(
    pages: list[PageText],
    client: BudgetLLMClient,
    country: str,
    year: int,
    source_file: str,
    country_ctx: dict,
    cache_dir: Optional[Path] = None,
    run_consistency_pass: bool = False,
) -> list[BudgetRow]:
    """
    Run pass-2 extraction on the given pages.

    Chunks pages into LLM-sized windows, extracts JSON items from each chunk,
    constructs BudgetRow objects, deduplicates, and returns the final list.
    """
    chunks = chunk_pages(
        pages,
        chunk_size=cfg.CHUNK_SIZE,
        overlap=cfg.CHUNK_OVERLAP,
        max_pages=cfg.MAX_PAGES_PER_CHUNK,
    )

    currency = country_ctx.get("currency", "LOCAL")
    unit_hint = country_ctx.get("unit_hint", "unknown")
    doc_hint = country_ctx.get("doc_type_hint", "")
    known_agencies = country_ctx.get("known_agencies", [])
    mixed_ministries = country_ctx.get("mixed_ministries", [])

    all_items: list[dict] = []

    # Include a short hash of the country addendum in cache keys so that
    # profile changes (unit instructions, skip rules, year-specific notes, etc.)
    # invalidate old cache entries automatically.
    from budget.country_profiles import build_country_addendum as _bca
    _profile_sig = hashlib.md5(_bca(country, year=year).encode()).hexdigest()[:8]

    for chunk_pages_list, chunk_text in chunks:
        page_range = get_page_range(chunk_pages_list)

        # Cache check (include profile signature so profile edits bust the cache)
        cache_key = _chunk_cache_key(f"{source_file}|{_profile_sig}", chunk_text)
        cached = _load_chunk_cache(cache_dir, cache_key) if cache_dir else None

        if cached is not None and cfg.SKIP_CACHED:
            logger.debug(f"Extract cache hit: pages {page_range}")
            chunk_items = cached.get("items", [])
        else:
            user_prompt = build_extract_user_prompt(
                pages_text=chunk_text,
                country=country,
                year=year,
                currency=currency,
                unit_hint=unit_hint,
                doc_hint=doc_hint,
                known_agencies=known_agencies,
                mixed_ministries=mixed_ministries,
                page_range=page_range,
            )

            result = client.call_json(
                system_prompt=EXTRACT_SYSTEM_PROMPT,
                user_prompt=user_prompt,
                max_tokens=cfg.DEFAULT_LLM_CONFIG["max_tokens"],
                operation=BudgetLLMClient.OP_EXTRACT,
            )

            if "_parse_error" in result:
                logger.warning(
                    f"Extract JSON parse error for {source_file} pages {page_range}: "
                    f"{result.get('_parse_error')}"
                )
                chunk_items = []
            else:
                chunk_items = result.get("items", [])
                # Save to cache (include page_range in stored data)
                result["_page_range"] = page_range
                if cache_dir:
                    _save_chunk_cache(cache_dir, cache_key, result)

        # Guard: LLM occasionally returns a string instead of a list of dicts.
        # Iterating a string yields characters which then fail on .get().
        if not isinstance(chunk_items, list):
            logger.warning(
                f"LLM returned non-list items for {source_file} pages {page_range} "
                f"(type={type(chunk_items).__name__}) — skipping chunk"
            )
            chunk_items = []
        # Filter out any non-dict elements inside the list
        chunk_items = [it for it in chunk_items if isinstance(it, dict)]

        # Tag each item with page range info
        for item in chunk_items:
            if not item.get("page_number"):
                item["page_number"] = page_range

        all_items.extend(chunk_items)
        logger.info(f"  pages {page_range}: {len(chunk_items)} items extracted")

    # Optional consistency pass (3rd LLM call to deduplicate and sanity-check)
    if run_consistency_pass and all_items:
        all_items = _run_consistency_pass(all_items, client, country, year)

    # Convert to BudgetRow objects
    rows: list[BudgetRow] = []
    for item in all_items:
        row = BudgetRow.from_llm_json(
            item,
            country=country,
            year=year,
            source_file=source_file,
            llm_model=client.model,
            extraction_pass="extract",
        )
        rows.append(row)

    # Deduplicate by (section_code + line_description + amount_local)
    rows = _dedup_rows(rows)

    logger.info(f"Extracted {len(rows)} rows for {country} {year}")
    return rows


def _run_consistency_pass(
    items: list[dict],
    client: BudgetLLMClient,
    country: str,
    year: int,
) -> list[dict]:
    """Optional pass-3: ask LLM to deduplicate and sanity-check extracted items."""
    if not items:
        return items
    user_prompt = build_consistency_user_prompt(items, country, year)
    result = client.call_json(
        system_prompt=CONSISTENCY_SYSTEM_PROMPT,
        user_prompt=user_prompt,
        max_tokens=cfg.DEFAULT_LLM_CONFIG["max_tokens"],
        operation=BudgetLLMClient.OP_CONSISTENCY,
    )
    if "_parse_error" in result:
        logger.warning(f"Consistency pass JSON parse error: {result.get('_parse_error')}")
        return items
    corrected = result.get("items", items)
    logger.info(f"Consistency pass: {len(items)} → {len(corrected)} items")
    return corrected


def _dedup_rows(rows: list[BudgetRow]) -> list[BudgetRow]:
    """
    Remove duplicate rows based on (section_code, line_description, amount_local).
    Keeps the row with higher confidence when duplicates exist.
    """
    seen: dict[tuple, BudgetRow] = {}
    for row in rows:
        key = (
            row.section_code.strip().lower(),
            row.line_description.strip().lower()[:80],
            row.amount_local,
        )
        if key in seen:
            existing = seen[key]
            if row.confidence > existing.confidence:
                seen[key] = row
        else:
            seen[key] = row
    return list(seen.values())


# ---------------------------------------------------------------------------
# Full document extraction (combines pass 1 + pass 2)
# ---------------------------------------------------------------------------

def extract_document(
    pages: list[PageText],
    client: BudgetLLMClient,
    country: str,
    year: int,
    source_file: str,
    country_ctx: dict,
    cache_dir: Optional[Path] = None,
    use_scan_pass: bool = cfg.USE_SCAN_PASS,
    scan_threshold: float = cfg.SCAN_THRESHOLD,
    run_consistency_pass: bool = False,
    scan_model: Optional[str] = None,
) -> list[BudgetRow]:
    """
    Full two-pass extraction for one source document.

    1. Scan pass: filter to relevant pages (unless use_scan_pass=False).
    2. Extract pass: structured JSON extraction from relevant pages.
    3. Optional consistency pass.

    scan_model: model used for the cheap scan pass (defaults to cfg.SCAN_MODEL).
    """
    if not pages:
        logger.warning(f"No pages for {source_file}")
        return []

    doc_hint = country_ctx.get("doc_type_hint", "")

    if use_scan_pass:
        relevant_pages = scan_pages(
            pages=pages,
            client=client,
            country=country,
            year=year,
            doc_hint=doc_hint,
            threshold=scan_threshold,
            cache_dir=cache_dir,
            source_file=source_file,
            scan_model=scan_model,
        )
        if not relevant_pages:
            logger.warning(f"Scan pass found 0 relevant pages in {source_file}")
            return []
    else:
        relevant_pages = pages

    rows = extract_chunks(
        pages=relevant_pages,
        client=client,
        country=country,
        year=year,
        source_file=source_file,
        country_ctx=country_ctx,
        cache_dir=cache_dir,
        run_consistency_pass=run_consistency_pass,
    )

    return rows
