"""Belgium federal science budget extractor.

Belgian R&D spending at the federal level is managed by:
- **POD Wetenschapsbeleid / SPP Politique scientifique** (Belspo), Sectie/Section 46
  - Programme 60/1: R&D at national level (government initiatives, IAP poles, federal labs)
  - Programme 60/2: International R&D (ESA, bilateral space projects)
  - Programme 60/3: Federal Scientific Institutions (museums, observatories, etc.)

Document structure
------------------
The Belgian "Loi de finances" / "Financiewet" (Finance Act) published in the
Moniteur Belge / Belgisch Staatsblad is the **authorisation act** that lists
which programs can spend money. The actual EUR amounts are in annexes that are
often in image/scan format and not directly extractable.

What CAN be extracted:
1. Specific advance/provisional appropriation amounts mentioned in the text
   (e.g. "maximumbedrag van X EUR" or "voor een bedrag van Y EUR")
2. ESA contribution amounts sometimes stated explicitly
3. Fixed grants to named research institutions (e.g. Antarctica program)

What CANNOT be extracted from these PDFs:
- The main programme appropriation tables (scanned images in annexes)
- Annual R&D programme totals for Programme 60/1, 60/2

Strategy
--------
Rather than guessing amounts, this extractor marks Belgium records with
decision="review" and amount=None when only programme names are found (no
explicit EUR amount). When explicit EUR amounts ARE mentioned in the context
of Sectie 46 / Wetenschapsbeleid / Politique scientifique, it captures them.
"""

from __future__ import annotations

import re
import logging
from typing import Optional

logger = logging.getLogger("innovation_pipeline")

# ── Science programme patterns ─────────────────────────────────────────────────

# Indicators that the current section is about federal science policy
_SCIENCE_SECTION_RE = re.compile(
    r"(sectie\s*46|section\s*46|pod\s+wetenschapsbeleid|spp\s+politique\s+scientifique"
    r"|programme\s*60/[123]|programma\s*60/[123]"
    r"|belspo|politique\s+scientifique\s+f[eé]d[eé]rale"
    r"|federaal\s+wetenschapsbeleid"
    r"|recherche\s+et\s+d[eé]veloppement\s+(?:au\s+niveau\s+national|à\s+l'échelon)"
    r"|onderzoek\s+en\s+ontwikkeling\s+op\s+(?:nationaal|internationaal)\s+vlak)",
    re.IGNORECASE,
)

# ESA contribution
_ESA_RE = re.compile(
    r"europees?\s+ruimtevaart(?:agentschap)?|agence\s+spatiale\s+europ[eé]enne|\bESA\b",
    re.IGNORECASE,
)

# EUR amount patterns (in text, not tables)
# e.g. "5 500 EUR", "370.000 EUR", "62 500 000 EUR", "maximumbedrag van 5.500 EUR"
_RE_EUR_AMOUNT = re.compile(
    r"(?:bedrag\s+van\s+|maximumbedrag\s+van\s+|plafond\s+de\s+|montant\s+de\s+)"
    r"([\d\s\xa0\.]+(?:,\d+)?)\s*EUR\b",
    re.IGNORECASE,
)

# Any standalone EUR amount on a line
_RE_EUR_STANDALONE = re.compile(
    r"\b([\d]{1,3}(?:[.\s\xa0]\d{3})*(?:,\d+)?)\s*EUR\b",
    re.IGNORECASE,
)


def _parse_eur_amount(raw: str) -> Optional[float]:
    """Parse a Belgian EUR amount string (uses . or space as thousands separator)."""
    cleaned = raw.replace(".", "").replace(",", ".").replace("\xa0", "").replace(" ", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


# ── Page-level extraction ──────────────────────────────────────────────────────

def _extract_from_page(text: str) -> list[tuple[str, str, float]]:
    """Extract (program_code, description, amount_eur) from a science policy page.

    Returns an empty list if no explicit amounts can be found.
    """
    if not _SCIENCE_SECTION_RE.search(text):
        return []

    results: list[tuple[str, str, float]] = []
    seen_amounts: set[float] = set()

    # Look for explicit EUR amounts in the science section context
    for m in _RE_EUR_AMOUNT.finditer(text):
        raw = m.group(1)
        amount = _parse_eur_amount(raw)
        if amount is None or amount < 10_000:
            continue
        # Max cap: federal science budget never exceeded €2B
        if amount > 2_000_000_000:
            continue
        rounded = round(amount, -3)
        if rounded in seen_amounts:
            continue
        seen_amounts.add(rounded)
        snippet = text[max(0, m.start()-50):m.end()+50].replace("\n", " ").strip()
        # Determine programme code from context
        ctx = text[max(0, m.start()-200):m.end()]
        if re.search(r"60/2|international|esa\b|ruimtevaart|spatial", ctx, re.IGNORECASE):
            prog_code = "BE_RD_INTERNATIONAL"
            desc = "R&D International / ESA"
        elif re.search(r"60/3|wetenschappelijke\s+inrichting|fédér.*scientifique.*établissement", ctx, re.IGNORECASE):
            prog_code = "BE_FED_SCI_INST"
            desc = "Federal Scientific Institutions"
        elif re.search(r"60/1|nationaal|national", ctx, re.IGNORECASE):
            prog_code = "BE_RD_NATIONAL"
            desc = "R&D National (Programme 60/1)"
        else:
            prog_code = "BE_SCIENCE_POLICY"
            desc = "Federal Science Policy"
        results.append((prog_code, desc, amount))

    return results


# ── Public API ─────────────────────────────────────────────────────────────────

def extract_belgium_items(
    sorted_pages,   # DataFrame with page_number, text columns
    file_id: str,
    country: str,
    year: str,
    source_filename: str,
) -> list[dict]:
    """Extract federal science policy records from a Belgian Finance Act PDF.

    Most Belgian Finance Act PDFs do NOT contain the main appropriation amounts
    (they are in annexed budget tables published separately). This extractor
    captures only what is available in the text: specific grant caps, advance
    payment limits, and other explicit EUR amounts in the Sectie 46 context.

    For years where no amounts are extractable, returns an empty list. The
    generic taxonomy pipeline will still create "review" records for pages
    mentioning science policy keywords.
    """
    records: list[dict] = []
    seen_amounts: set[float] = set()

    for row in sorted_pages.itertuples(index=False):
        pg = int(row.page_number)
        text = row.text if isinstance(row.text, str) else ""
        if not text.strip():
            continue

        items = _extract_from_page(text)
        for prog_code, desc, amount in items:
            rounded = round(amount, -3)
            if rounded in seen_amounts:
                continue
            seen_amounts.add(rounded)
            snippet = text[:200].replace("\n", " ").strip()
            records.append({
                "country": country,
                "year": year,
                "section_code": "BE_SCIENCE",
                "section_name": "Politique scientifique fédérale / Federaal Wetenschapsbeleid",
                "section_name_en": "Federal Science Policy",
                "program_code": prog_code,
                "line_description": desc,
                "line_description_en": desc,
                "amount_local": amount,
                "currency": "EUR",
                "unit": "EUR",
                "rd_category": "direct_rd",
                "taxonomy_score": 7.0,
                "decision": "review",  # Explicit text amounts are advance caps / admin budgets
                "confidence": 0.50,
                "source_file": source_filename,
                "file_id": file_id,
                "page_number": pg,
            })

    if records:
        logger.info(
            "Belgium extractor: %s (year %s) → %d records",
            source_filename, year, len(records),
        )
    else:
        logger.debug(
            "Belgium extractor: no explicit amounts found in %s (year %s). "
            "Main budget amounts are in annexed tables not available in text form.",
            source_filename, year,
        )

    return records
