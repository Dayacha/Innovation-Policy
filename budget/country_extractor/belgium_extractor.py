"""Belgium federal science budget extractor.

Belgium's federal science policy is organised in Section 46
(`POD Wetenschapsbeleid` / `SPP Politique scientifique` / Belspo).

What is consistently present in the available archive
-----------------------------------------------------
1. Section 46 programme structure pages listing the policy content of:
   - Programme 60/1: national R&D
   - Programme 60/2: international R&D / ESA / space
   - Programme 60/3: federal scientific institutions
2. Explicit Section-46 operational amounts in some years:
   - advance / petty-cash maxima for Section 46 accountants
   - redistribution caps between Section-46 base allocations
   - specific programme-linked EUR / BEF caps or ceilings

What is often *not* present in extractable text
-----------------------------------------------
The full appropriation tables for the programme totals are frequently rendered as
images or degraded OCR. For those years we still emit structured review records for
the Section-46 programmes, with rich context but no numeric amount, so the rows can
flow through taxonomy scoring and later AI validation.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

logger = logging.getLogger("innovation_pipeline")


_SECTION_46_RE = re.compile(
    r"(sectie\s*46|section\s*46)\s*[.\-—:]*\s*"
    r"(?:pod\s+wetenschapsbeleid|spp\s+politique\s+scientifique)?",
    re.IGNORECASE,
)

_SCIENCE_ANCHOR_RE = re.compile(
    r"(belspo|pod\s+wetenschapsbeleid|spp\s+politique\s+scientifique|"
    r"diensten?\s+voor\s+programmatie\s+van\s+het\s+wetenschapsbeleid|"
    r"services?\s+de\s+programmation\s+de\s+la\s+politique\s+scientifique|"
    r"\bd\.?p\.?w\.?b\.?\b|\bs\.?p\.?p\.?s\.?\b|"
    r"\bminister\s+van\s+wetenschapsbeleid\b|"
    r"\bministre\s+de\s+la\s+politique\s+scientifique\b|"
    r"\bart\.?\s*2\.(?:46|11)\b)",
    re.IGNORECASE,
)

_SCIENCE_AMOUNT_ANCHOR_RE = re.compile(
    r"(pod\s+wetenschapsbeleid|spp\s+politique\s+scientifique|"
    r"diensten?\s+voor\s+programmatie\s+van\s+het\s+wetenschapsbeleid|"
    r"services?\s+de\s+programmation\s+de\s+la\s+politique\s+scientifique|"
    r"\bd\.?p\.?w\.?b\.?\b|\bs\.?p\.?p\.?s\.?\b|"
    r"\bd\.?w\.?t\.?c\.?\b|\bs\.?s\.?t\.?c\.?\b|"
    r"\bminister\s+van\s+wetenschapsbeleid\b|"
    r"\bministre\s+de\s+la\s+politique\s+scientifique\b|"
    r"\bart\.?\s*2\.(?:46|11)\b|"
    r"\bsectie\s*46\b|\bsection\s*46\b|"
    r"\bfwi\b|\besf\b)",
    re.IGNORECASE,
)

_PROGRAM_PATTERNS: list[tuple[str, re.Pattern, str, str, str]] = [
    (
        "BE_RD_NATIONAL",
        re.compile(
            r"(programma|programme)\s*60/1\s*[–—-]?\s*"
            r"(onderzoek\s+en\s+ontwikkeling\s+op\s+nationaal\s+vlak"
            r"|r-?d\s+dans\s+le\s+cadre\s+national"
            r"|recherche\s+et\s+developpement\s+dans\s+le\s+cadre\s+national)",
            re.IGNORECASE,
        ),
        "Onderzoek en ontwikkeling op nationaal vlak",
        "Research and Development at National Level",
        "Federal science policy: national R&D programmes",
    ),
    (
        "BE_RD_INTERNATIONAL",
        re.compile(
            r"(programma|programme)\s*60/2\s*[–—-]?\s*"
            r"(onderzoek\s+en\s+ontwikkeling\s+op\s+internationaal\s+vlak"
            r"|r-?d\s+dans\s+le\s+cadre\s+international"
            r"|recherche\s+et\s+developpement\s+dans\s+le\s+cadre\s+international)",
            re.IGNORECASE,
        ),
        "Onderzoek en ontwikkeling op internationaal vlak",
        "Research and Development at International Level",
        "Federal science policy: international R&D and ESA participation",
    ),
    (
        "BE_FED_SCI_INST",
        re.compile(
            r"(programma|programme)\s*60/3\s*[–—-]?\s*"
            r"(federale\s+wetenschappelijke\s+instellingen"
            r"|etablissements\s+scientifiques\s+federaux)",
            re.IGNORECASE,
        ),
        "Federale wetenschappelijke instellingen",
        "Federal Scientific Institutions",
        "Federal science policy: federal scientific institutions",
    ),
]

_AMOUNT_LINE_RE = re.compile(
    r"(?P<label>[^\n]{0,220}?)"
    r"(?P<amount>\d{1,3}(?:[.\s\xa0]\d{3})+(?:,\d+)?)\s*"
    r"(?P<currency>EUR|euro|frank|francs?)\b",
    re.IGNORECASE,
)

_AMOUNT_CUE_RE = re.compile(
    r"(maximumbedrag|montant\s+maximum|maximum\s+de|tot\s+een\s+maximumbedrag|"
    r"voorschotten|avances\s+de\s+fonds|beperkt\s+tot|limite[eé]?\s+[àa]|"
    r"debetpositie|debetpositie.*niet\s+mag\s+overschrijden|"
    r"toegekend|subsidie|subvention|toelage|herverdeling|overgedragen)",
    re.IGNORECASE,
)

_NON_SCIENCE_AMOUNT_RE = re.compile(
    r"(fonds\s+voor\s+europese\s+steun\s+aan\s+de\s+meest\s+behoeftigen|"
    r"fonds\s+europ[ée]en\s+d['’]aide\s+aux\s+plus\s+d[ée]munis|"
    r"programma\s+56/6|programme\s+56/6|"
    r"maatschappelijke\s+integratie|int[ée]gration\s+sociale|"
    r"armoedebestrijding|lutte\s+contre\s+la\s+pauvret[ée]|"
    r"\bsectie\s*44\b|\bsection\s*44\b)",
    re.IGNORECASE,
)

_BASE_ALLOCATION_RE = re.compile(r"\b(?:46\.)?60\.\d{2}\.\d{2}\.\d{2}\.\d{2}(?:\.\d{2})?\b")
_SECTION_MARKER_RE = re.compile(r"\b(?:sectie|section)\s+(\d{1,2})\b", re.IGNORECASE)
_ARTICLE_MARKER_RE = re.compile(r"\bart\.?\s*2\.(\d{1,2})\b", re.IGNORECASE)
_PROGRAM_TOTAL_HEADER_RE = re.compile(
    r"(?:Totalen\s+voor\s+het\s+programma|Totaux\s+pour\s+le\s+programme)\s+11\.60\.(?P<prog>[1-3])",
    re.IGNORECASE,
)
_TOTAL_AFTER_RE = re.compile(
    r"\btot\b[\s:.-]*(?P<amount>\d{1,3}(?:[.\s\xa0]\d{3})+)",
    re.IGNORECASE,
)
_TOTAL_BEFORE_RE = re.compile(
    r"(?P<amount>\d{1,3}(?:[.\s\xa0]\d{3})+)[\s:.-]*\btot\b",
    re.IGNORECASE,
)
_NON_APPROPRIATION_CUE_RE = re.compile(
    r"(maximumbedrag|montant\s+maximum|maximum\s+de|voorschotten|avances\s+de\s+fonds|debetpositie)",
    re.IGNORECASE,
)


def _parse_amount(raw: str) -> Optional[float]:
    cleaned = (
        raw.replace("\xa0", "")
        .replace(" ", "")
        .replace(".", "")
        .replace(",", ".")
        .strip()
    )
    try:
        return float(cleaned)
    except ValueError:
        return None


def _currency_for_year(year: str) -> str:
    try:
        return "BEF" if int(year) < 2002 else "EUR"
    except ValueError:
        return "EUR"


def _program_metadata(program_suffix: str) -> tuple[str, str, str, str]:
    if program_suffix == "1":
        return (
            "BE_RD_NATIONAL",
            "Onderzoek en ontwikkeling op nationaal vlak",
            "Research and Development at National Level",
            "Federal science policy: national R&D programmes",
        )
    if program_suffix == "2":
        return (
            "BE_RD_INTERNATIONAL",
            "Onderzoek en ontwikkeling op internationaal vlak",
            "Research and Development at International Level",
            "Federal science policy: international R&D and ESA participation",
        )
    if program_suffix == "3":
        return (
            "BE_FED_SCI_INST",
            "Federale wetenschappelijke instellingen",
            "Federal Scientific Institutions",
            "Federal science policy: federal scientific institutions",
        )
    raise ValueError(f"Unexpected Section 46 program suffix: {program_suffix}")


def _science_scope_ok(prefix: str) -> bool:
    """Keep amounts only when the most recent section/article marker is science-policy."""
    last_section = None
    for m in _SECTION_MARKER_RE.finditer(prefix):
        last_section = m.group(1)

    last_article = None
    for m in _ARTICLE_MARKER_RE.finditer(prefix):
        last_article = m.group(1)

    if last_section is not None and last_section not in {"11", "46"}:
        return False
    if last_article is not None and last_article not in {"11", "46"}:
        return False
    return True


def _program_records_from_page(
    text: str,
    year: str,
    source_filename: str,
    page_number: int,
    file_id: str,
    country: str,
) -> list[dict]:
    if not (_SECTION_46_RE.search(text) or _SCIENCE_ANCHOR_RE.search(text)):
        return []

    records: list[dict] = []
    seen_programs: set[str] = set()

    for program_code, pat, line_description, line_description_en, summary in _PROGRAM_PATTERNS:
        if not pat.search(text):
            continue
        seen_programs.add(program_code)
        records.append(
            {
                "country": country,
                "year": year,
                "section_code": "BE_SCIENCE",
                "section_name": "POD Wetenschapsbeleid / SPP Politique scientifique",
                "section_name_en": "Federal Science Policy",
                "program_code": program_code,
                "line_description": line_description,
                "line_description_en": line_description_en,
                "program_description": summary,
                "program_description_en": summary,
                "amount_local": None,
                "currency": _currency_for_year(year),
                "unit": _currency_for_year(year),
                "rd_category": "direct_rd",
                "taxonomy_score": 7.5,
                "decision": "review",
                "confidence": 0.7,
                "source_file": source_filename,
                "file_id": file_id,
                "page_number": page_number,
                "text_snippet": text[:700].replace("\n", " ").strip(),
                "raw_line": text[:1800].strip(),
                "merged_line": summary,
                "context_before": "Section 46 federal science policy",
                "context_after": text[:1200].replace("\n", " ").strip(),
                "parse_quality": "medium",
            }
        )

    # If a Section-46 page does not repeat programme headers but carries explicit
    # Section-46 base-allocation codes, emit a general science-policy review row.
    if not seen_programs and (_SCIENCE_ANCHOR_RE.search(text) or _BASE_ALLOCATION_RE.search(text)):
        records.append(
            {
                "country": country,
                "year": year,
                "section_code": "BE_SCIENCE",
                "section_name": "POD Wetenschapsbeleid / SPP Politique scientifique",
                "section_name_en": "Federal Science Policy",
                "program_code": "BE_SCIENCE_POLICY",
                "line_description": "Sectie 46 / Section 46 structure",
                "line_description_en": "Section 46 structure",
                "program_description": "Belgian federal science policy section structure",
                "program_description_en": "Belgian federal science policy section structure",
                "amount_local": None,
                "currency": _currency_for_year(year),
                "unit": _currency_for_year(year),
                "rd_category": "direct_rd",
                "taxonomy_score": 7.0,
                "decision": "review",
                "confidence": 0.55,
                "source_file": source_filename,
                "file_id": file_id,
                "page_number": page_number,
                "text_snippet": text[:700].replace("\n", " ").strip(),
                "raw_line": text[:1800].strip(),
                "merged_line": "Belgian federal science policy section structure",
                "context_before": "Section 46 federal science policy",
                "context_after": text[:1200].replace("\n", " ").strip(),
                "parse_quality": "medium",
            }
        )

    return records


def _programme_total_records_from_page(
    text: str,
    year: str,
    source_filename: str,
    page_number: int,
    file_id: str,
    country: str,
) -> list[dict]:
    if not (_SECTION_46_RE.search(text) or _SCIENCE_ANCHOR_RE.search(text) or "11.60." in text):
        return []

    records: list[dict] = []
    seen: set[str] = set()

    lines = text.splitlines()
    for idx, raw_line in enumerate(lines):
        match = _PROGRAM_TOTAL_HEADER_RE.search(raw_line)
        if not match:
            continue
        program_suffix = match.group("prog")

        total_match = None
        total_line = raw_line
        for candidate in lines[idx: min(len(lines), idx + 5)]:
            total_match = _TOTAL_AFTER_RE.search(candidate)
            if total_match is None:
                total_match = _TOTAL_BEFORE_RE.search(candidate)
            if total_match is not None:
                total_line = candidate
                break
        if total_match is None:
            continue

        amount = _parse_amount(total_match.group("amount"))
        if amount is None or amount <= 0:
            continue
        amount *= 1000.0

        program_code, line_description, line_description_en, summary = _program_metadata(program_suffix)
        if program_code in seen:
            continue
        seen.add(program_code)

        joined = "\n".join(lines[max(0, idx - 4): min(len(lines), idx + 6)])
        context = joined
        records.append(
            {
                "country": country,
                "year": year,
                "section_code": "BE_SCIENCE",
                "section_name": "POD Wetenschapsbeleid / SPP Politique scientifique",
                "section_name_en": "Federal Science Policy",
                "program_code": program_code,
                "line_description": line_description,
                "line_description_en": line_description_en,
                "program_description": summary,
                "program_description_en": summary,
                "amount_local": amount,
                "currency": _currency_for_year(year),
                "unit": _currency_for_year(year),
                "rd_category": "direct_rd",
                "taxonomy_score": 8.8,
                "decision": "include",
                "confidence": 0.86,
                "source_file": source_filename,
                "file_id": file_id,
                "page_number": page_number,
                "text_snippet": context[:700].replace("\n", " ").strip(),
                "raw_line": context[:1800].strip(),
                "merged_line": total_line.strip(),
                "context_before": context[:500].replace("\n", " ").strip(),
                "context_after": context[500:1200].replace("\n", " ").strip(),
                "amount_raw": total_match.group("amount"),
                "parse_quality": "medium",
            }
        )

    return records


def _amount_records_from_page(
    text: str,
    year: str,
    source_filename: str,
    page_number: int,
    file_id: str,
    country: str,
) -> list[dict]:
    if not (_SECTION_46_RE.search(text) or _SCIENCE_ANCHOR_RE.search(text) or _BASE_ALLOCATION_RE.search(text)):
        return []

    records: list[dict] = []
    seen: set[tuple[str, int]] = set()
    default_currency = _currency_for_year(year)

    for m in _AMOUNT_LINE_RE.finditer(text):
        label = re.sub(r"\s+", " ", m.group("label")).strip(" .:-")
        amount = _parse_amount(m.group("amount"))
        currency_label = m.group("currency").lower()
        currency = "BEF" if "frank" in currency_label or "franc" in currency_label else "EUR"
        if amount is None or amount < 10_000:
            continue
        if currency == "BEF" and amount < 1_000_000:
            continue
        if currency == "EUR" and amount < 250_000:
            continue
        if currency == "EUR" and amount > 2_000_000_000:
            continue
        if currency == "BEF" and amount > 100_000_000_000:
            continue

        context = text[max(0, m.start() - 500): min(len(text), m.end() + 500)]
        if not (_SECTION_46_RE.search(context) or _SCIENCE_ANCHOR_RE.search(context) or _BASE_ALLOCATION_RE.search(context)):
            continue
        if _NON_SCIENCE_AMOUNT_RE.search(context):
            continue
        if not _AMOUNT_CUE_RE.search(context):
            continue
        prefix = text[max(0, m.start() - 260): m.start()]
        if not _science_scope_ok(prefix):
            continue
        if not _SCIENCE_AMOUNT_ANCHOR_RE.search(prefix):
            continue
        if _NON_APPROPRIATION_CUE_RE.search(context):
            continue

        if re.search(r"(agentschap|agence\s+spatiale|\bESA\b|ruimtevaart|spatiale)", context, re.IGNORECASE):
            program_code = "BE_RD_INTERNATIONAL"
            line_description = "Belgian international R&D / ESA amount"
            line_description_en = line_description
        elif re.search(r"(federale\s+wetenschappelijke\s+instellingen|etablissements\s+scientifiques\s+federaux|fwi)", context, re.IGNORECASE):
            program_code = "BE_FED_SCI_INST"
            line_description = "Federal scientific institutions amount"
            line_description_en = line_description
        elif re.search(r"(nationaal\s+vlak|cadre\s+national|interuniversitaire attractiepolen|airbus|myrrha)", context, re.IGNORECASE):
            program_code = "BE_RD_NATIONAL"
            line_description = "Belgian national R&D amount"
            line_description_en = line_description
        else:
            program_code = "BE_SCIENCE_POLICY"
            line_description = label or "Federal science policy amount"
            line_description_en = line_description

        key = (program_code, int(round(amount, -3)))
        if key in seen:
            continue
        seen.add(key)

        records.append(
            {
                "country": country,
                "year": year,
                "section_code": "BE_SCIENCE",
                "section_name": "POD Wetenschapsbeleid / SPP Politique scientifique",
                "section_name_en": "Federal Science Policy",
                "program_code": program_code,
                "line_description": label or line_description,
                "line_description_en": line_description_en,
                "program_description": "Explicit Section 46 amount stated in text",
                "program_description_en": "Explicit Section 46 amount stated in text",
                "amount_local": amount,
                "currency": currency or default_currency,
                "unit": currency or default_currency,
                "rd_category": "direct_rd",
                "taxonomy_score": 8.0,
                "decision": "review",
                "confidence": 0.62,
                "source_file": source_filename,
                "file_id": file_id,
                "page_number": page_number,
                "text_snippet": context[:700].replace("\n", " ").strip(),
                "raw_line": context[:1800].strip(),
                "merged_line": label or line_description,
                "context_before": context[:500].replace("\n", " ").strip(),
                "context_after": context[500:1200].replace("\n", " ").strip(),
                "amount_raw": m.group("amount"),
                "parse_quality": "medium",
            }
        )

    return records


def extract_belgium_items(
    sorted_pages,
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract Belgium federal science-policy structure and explicit amounts."""
    all_records: list[dict] = []

    for row in sorted_pages.itertuples(index=False):
        pg = int(row.page_number)
        text = row.text if isinstance(row.text, str) else ""
        if not text.strip():
            continue
        all_records.extend(
            _programme_total_records_from_page(
                text=text,
                year=year,
                source_filename=source_filename,
                page_number=pg,
                file_id=file_id,
                country=country,
            )
        )
        all_records.extend(
            _program_records_from_page(
                text=text,
                year=year,
                source_filename=source_filename,
                page_number=pg,
                file_id=file_id,
                country=country,
            )
        )
        all_records.extend(
            _amount_records_from_page(
                text=text,
                year=year,
                source_filename=source_filename,
                page_number=pg,
                file_id=file_id,
                country=country,
            )
        )

    if not all_records:
        logger.debug("Belgium extractor: no Section 46 science-policy content found in %s (%s)", source_filename, year)
        return []

    # Keep one best review row per year/program, plus one best numeric row per year/program.
    best_by_key: dict[tuple[str, str], dict] = {}
    for rec in all_records:
        amount = rec.get("amount_local")
        key = (
            rec["program_code"],
            "amount" if amount not in (None, "") else "structure",
        )
        current = best_by_key.get(key)
        if current is None:
            best_by_key[key] = rec
            continue

        cur_has_amount = current.get("amount_local") not in (None, "")
        new_has_amount = amount not in (None, "")
        current_rank = (
            1 if cur_has_amount else 0,
            float(current.get("confidence") or 0),
            len(str(current.get("raw_line", ""))),
            -int(current.get("page_number") or 0),
        )
        new_rank = (
            1 if new_has_amount else 0,
            float(rec.get("confidence") or 0),
            len(str(rec.get("raw_line", ""))),
            -int(rec.get("page_number") or 0),
        )
        if new_rank > current_rank:
            best_by_key[key] = rec

    records = list(best_by_key.values())
    logger.info("Belgium extractor: %s (year %s) → %d records", source_filename, year, len(records))
    return records
