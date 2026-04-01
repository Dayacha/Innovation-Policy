"""Dedicated country-extractor dispatch for budget extraction."""

from __future__ import annotations

from pathlib import Path
import re

from budget.country_extractor import (
    extract_australia_items,
    extract_belgium_items,
    extract_canada_items,
    extract_chile_items,
    extract_colombia_items,
    extract_costa_rica_items,
    extract_czech_items,
    extract_denmark_items,
    extract_estonia_items,
    extract_finland_items,
    extract_france_items,
    extract_germany_items,
    extract_hungary_items,
    extract_iceland_items,
    extract_israel_items,
    extract_japan_items,
    extract_korea_items,
    extract_latvia_items,
    extract_lithuania_items,
    extract_new_zealand_items,
    extract_netherlands_items,
    extract_norway_items,
    extract_spain_items,
    extract_switzerland_items,
    extract_uk_items,
)
from budget.extractor_common import enrich_dedicated_record, filepath_from_row


COUNTRY_DEDICATED_EXTRACTORS: frozenset[str] = frozenset({
    "Denmark", "Spain", "United Kingdom", "Canada", "Australia", "Belgium",
    "Finland", "France", "Germany", "Japan", "Colombia", "Chile", "Czech Republic",
    "Israel", "Estonia", "Korea", "Costa Rica",
    "Hungary", "Iceland", "Netherlands", "Norway", "Latvia", "Lithuania", "New Zealand", "Switzerland",
})

COUNTRY_SKIP_EXTRACTORS: frozenset[str] = frozenset()


def handle_dedicated_country(
    *,
    records: list[dict],
    sorted_pages,
    file_id: str,
    country_for_file: str,
    year_for_file: str,
    filepath_col: str,
    tax,
    prior_results_df=None,
) -> bool:
    """Run a dedicated extractor for one file if available.

    Returns True when the file was fully handled and the caller should skip
    the generic parser path.
    """
    if country_for_file not in COUNTRY_DEDICATED_EXTRACTORS:
        return False

    first_row = sorted_pages.iloc[0]
    filepath_val = filepath_from_row(first_row, filepath_col)
    source_fn = Path(filepath_val).name

    if country_for_file == "Denmark":
        records.extend(
            extract_denmark_items(
                sorted_pages,
                file_id=str(file_id),
                filepath_col=filepath_col,
                tax=tax,
                prior_results_df=prior_results_df,
            )
        )
        return True

    extractor_map = {
        "Spain": extract_spain_items,
        "United Kingdom": extract_uk_items,
        "Canada": extract_canada_items,
        "Australia": extract_australia_items,
        "Belgium": extract_belgium_items,
        "Finland": extract_finland_items,
        "France": extract_france_items,
        "Germany": extract_germany_items,
        "Japan": extract_japan_items,
        "Colombia": extract_colombia_items,
        "Chile": extract_chile_items,
        "Czech Republic": extract_czech_items,
        "Israel": extract_israel_items,
        "Estonia": extract_estonia_items,
        "Korea": extract_korea_items,
        "Costa Rica": extract_costa_rica_items,
        "Hungary": extract_hungary_items,
        "Iceland": extract_iceland_items,
        "Netherlands": extract_netherlands_items,
        "Norway": extract_norway_items,
        "Latvia": extract_latvia_items,
        "Lithuania": extract_lithuania_items,
        "New Zealand": extract_new_zealand_items,
        "Switzerland": extract_switzerland_items,
    }

    dedupe_rounding = {
        "Canada": -4,
        "Belgium": -3,
        "Finland": -3,
        "Estonia": -3,
        "Costa Rica": -3,
    }

    extractor = extractor_map.get(country_for_file)
    if extractor is None:
        return False

    raw_records = extractor(
        sorted_pages,
        file_id=str(file_id),
        country=country_for_file,
        year=year_for_file,
        source_filename=source_fn,
    )

    existing_by_key = {
        _dedupe_key(existing, country_for_file, dedupe_rounding.get(country_for_file)): idx
        for idx, existing in enumerate(records)
        if existing.get("country") == country_for_file
    }
    for rec in raw_records:
        enriched = enrich_dedicated_record(rec, tax)
        key = _dedupe_key(enriched, country_for_file, dedupe_rounding.get(country_for_file))
        existing_idx = existing_by_key.get(key)
        if existing_idx is None:
            records.append(enriched)
            existing_by_key[key] = len(records) - 1
            continue

        current = records[existing_idx]
        if _record_priority(enriched, country_for_file) > _record_priority(current, country_for_file):
            records[existing_idx] = enriched
    return True


def _dedupe_key(rec: dict, country: str, rounding_digits: int | None) -> tuple:
    year = rec.get("year", "")
    program_code = rec.get("program_code", "")
    if country in {"Germany", "Canada", "Belgium", "Costa Rica", "Czech Republic", "Estonia", "Israel", "Korea", "Spain"}:
        return year, program_code
    if rounding_digits is None:
        return year, program_code
    amount = int(round(rec.get("amount_local") or 0, rounding_digits))
    return year, program_code, amount


def _record_priority(rec: dict, country: str) -> tuple:
    confidence = float(rec.get("confidence") or 0)
    source = str(rec.get("source_file", "") or "").lower()
    page_number = int(rec.get("page_number") or 0)
    amount = float(rec.get("amount_local") or 0)

    if country == "Australia":
        source_score = 0
        if "no1" in source:
            source_score = 4
        elif "dept" in source:
            source_score = 3
        elif "no2" in source:
            source_score = 2
        elif "no3" in source or "no4" in source or "no5" in source or "no6" in source:
            source_score = 1
        return source_score, amount, confidence, page_number

    if country == "France":
        variant = str(rec.get("source_variant", "") or "")
        source_score = 2 if variant.startswith("mission_total") else 1
        return source_score, confidence, amount, -page_number

    if country == "Canada":
        variant = str(rec.get("source_variant", "") or "")
        source_score = 0
        if variant == "full_schedule":
            source_score = 3
        elif variant == "interim":
            source_score = 2
        elif variant == "fragment":
            source_score = 1

        if re.search(r"(?:^|[^0-9])1(?:\.pdf)?$", source) or re.search(r"(?:^|[^a-z])a(?:\.pdf)?$", source):
            source_score += 1

        return source_score, confidence, amount, -page_number

    if country == "Belgium":
        has_amount = 1 if rec.get("amount_local") not in (None, "", 0) else 0
        raw_len = len(str(rec.get("raw_line", "") or ""))
        return has_amount, confidence, raw_len, -page_number

    if country == "Colombia":
        variant = str(rec.get("source_variant", "") or "")
        source_score = 0
        if variant == "decree_annex":
            source_score = 4
        elif variant == "decree":
            source_score = 3
        elif variant == "law":
            source_score = 2
        elif "decreto" in source:
            source_score = 1
        return source_score, confidence, amount, -page_number

    if country == "Costa Rica":
        variant = str(rec.get("source_variant", "") or "")
        source_score = 0
        if variant == "title_page":
            source_score = 4
        elif variant == "program_transfer_block":
            source_score = 3
        elif variant == "program_total":
            source_score = 2
        elif variant == "summary_annex":
            source_score = 1
        return source_score, confidence, amount, -page_number

    if country == "Czech Republic":
        variant = str(rec.get("source_variant", "") or "")
        source_score = 0
        if variant == "chapter_page":
            source_score = 4
        elif variant == "appendix3_row":
            source_score = 3
        elif variant == "chapter_table":
            source_score = 2
        elif variant == "investment_fallback":
            source_score = 1
        return source_score, confidence, amount, -page_number

    if country == "Estonia":
        variant = str(rec.get("source_variant", "") or "")
        source_score = 0
        if variant == "modern_program_page":
            source_score = 5
        elif variant == "legacy_science_line":
            source_score = 4
        elif variant == "legacy_science_section":
            source_score = 3
        elif variant == "ministry_spending_page":
            source_score = 3
        elif variant == "ministry_agency_page":
            source_score = 2
        elif variant == "legacy_line_item":
            source_score = 2
        elif variant == "ministry_header_total":
            source_score = 1
        return source_score, confidence, amount, -page_number

    if country == "Israel":
        variant = str(rec.get("source_variant", "") or "")
        source_score = 0
        if variant == "summary_row":
            source_score = 4
        elif variant == "legacy_summary_row":
            source_score = 3
        elif variant == "detail_section_page":
            source_score = 2
        law_score = 0 if "bill" in source else 1
        return source_score, law_score, confidence, amount, -page_number

    if country == "Korea":
        variant = str(rec.get("source_variant", "") or "")
        source_score = 0
        if variant == "annual_budget_table":
            source_score = 4
        elif variant == "rd_total_line":
            source_score = 3
        elif variant == "fiscal_plan_table":
            source_score = 2
        elif variant == "ministry_total_page":
            source_score = 1

        file_score = 0
        if "예산안" in str(rec.get("source_file", "")):
            file_score = 2
        elif "홍보자료" in str(rec.get("source_file", "")) or "개요" in str(rec.get("source_file", "")):
            file_score = 1
        elif "국가재정운용계획" in str(rec.get("source_file", "")):
            file_score = 0
        elif "인포그래픽" in str(rec.get("source_file", "")) or "핵심과제" in str(rec.get("source_file", "")):
            file_score = -1

        return source_score, file_score, confidence, amount, -page_number

    if country == "Spain":
        variant = str(rec.get("source_variant", "") or "")
        source_score = 0
        if variant == "program_block":
            source_score = 4
        elif variant == "program_name_block":
            source_score = 3
        elif variant == "function_total_summary":
            source_score = 2

        file_score = 0
        source_name = str(rec.get("source_file", "") or "").lower()
        if "consolidado" in source_name and "boe-a" in source_name:
            file_score = 3
        elif "boe-a" in source_name:
            file_score = 2
        elif "draft" in source_name or "bocg" in source_name:
            file_score = 0
        return source_score, file_score, confidence, amount, -page_number

    if country != "Germany":
        return confidence, page_number

    source_score = 0
    if "gesamtplan" in source or "uebersichten" in source or "übersichten" in source:
        source_score = 3
    elif re.match(r"^\d{4}\s+\d{6,7}\.pdf$", str(rec.get("source_file", ""))):
        source_score = 2
    elif "bgbl" in source:
        source_score = 1
    return confidence, amount, source_score, page_number


__all__ = [
    "COUNTRY_DEDICATED_EXTRACTORS",
    "COUNTRY_SKIP_EXTRACTORS",
    "handle_dedicated_country",
]
