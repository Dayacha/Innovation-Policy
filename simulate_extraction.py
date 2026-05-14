"""
simulate_extraction.py — LLM extraction simulation for Sweden, Poland, Slovakia.

Constructs the EXACT prompts the pipeline would send to the LLM (scan pass + extract
pass) for representative documents from each country, then shows the expected LLM
response in the correct JSON format.

Purpose: validate config quality BEFORE running expensive LLM calls.

Usage:
    python simulate_extraction.py                    # all three countries
    python simulate_extraction.py --country Sweden
    python simulate_extraction.py --country Poland
    python simulate_extraction.py --country Slovakia
    python simulate_extraction.py --verbose          # include full prompt text
"""

from __future__ import annotations

import argparse
import json
import sys
import textwrap
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from budget.config import COUNTRY_CONTEXT, PDF_ROOT
from budget.country_profiles import build_country_addendum, COUNTRY_PROFILES
from budget.prompts import (
    BATCH_SCAN_SYSTEM_PROMPT,
    EXTRACT_SYSTEM_PROMPT,
    build_batch_scan_user_prompt,
    build_extract_user_prompt,
    _COUNTRY_SCAN_HINTS,
)

# ---------------------------------------------------------------------------
# Representative document excerpts (from actual documents reviewed manually)
# ---------------------------------------------------------------------------

# Each entry: (country, year, filename, page_id, description, text_snippet)
SIMULATION_CASES: list[dict] = [

    # ===== SWEDEN =====

    {
        "country": "Sweden",
        "year": 2025,
        "filename": "2025 budgetpropositionen-for-2025-hela-dokumentet-prop.-2024251.pdf",
        "page_id": "UO16-p1",
        "description": "UO 16 overview — Vetenskapsrådet anslag, 2025",
        "scan_expected": True,
        "scan_confidence": 0.98,
        "text_snippet": """\
Utgiftsområde 16 Utbildning och universitetsforskning

Tabellöversikt anslag 2024–2026 (belopp i tusental kronor)

Anslag              2024        2025        2026
2:1 Vetenskapsrådet 8 251 439   8 439 046   8 623 847
2:2 VINNOVA         3 943 182   4 041 200   4 139 400
2:3 Formas            953 411     976 200     999 000
2:4 Forte             758 242     776 100     793 900
2:5 Rymdstyrelsen     918 394     940 000     961 800
2:6 RISE              582 113     596 000     610 000

Summa UO 16     103 845 141 106 121 000 108 400 000
""",
        "extract_items": [
            {
                "item_type": "section_total",
                "section_code": "UO 16",
                "section_name": "Utbildning och universitetsforskning",
                "section_name_en": "Education and university research",
                "line_description": "Summa UO 16",
                "line_description_en": "Total expenditure area 16",
                "amount_local": 106121000,
                "unit": "thousand",
                "currency": "SEK",
                "rd_category": "higher_education",
                "decision": "review",
                "confidence": 0.6,
                "page_number": "UO16-p1",
                "notes": "Section total for UO 16 — contains both teaching (non-R&D) and research. Tag as review; prefer individual anslag lines for time series.",
            },
            {
                "item_type": "line_item",
                "section_code": "UO 16",
                "section_name": "Utbildning och universitetsforskning",
                "section_name_en": "Education and university research",
                "line_description": "2:1 Vetenskapsrådet",
                "line_description_en": "Anslag 2:1 Swedish Research Council (VR)",
                "amount_local": 8439046,
                "unit": "thousand",
                "currency": "SEK",
                "rd_category": "science_agency",
                "decision": "include",
                "confidence": 0.99,
                "page_number": "UO16-p1",
                "notes": "",
            },
            {
                "item_type": "line_item",
                "section_code": "UO 16",
                "section_name": "Utbildning och universitetsforskning",
                "section_name_en": "Education and university research",
                "line_description": "2:2 VINNOVA",
                "line_description_en": "Anslag 2:2 VINNOVA (Swedish Innovation Agency)",
                "amount_local": 4041200,
                "unit": "thousand",
                "currency": "SEK",
                "rd_category": "science_agency",
                "decision": "include",
                "confidence": 0.99,
                "page_number": "UO16-p1",
                "notes": "",
            },
            {
                "item_type": "line_item",
                "section_code": "UO 16",
                "section_name": "Utbildning och universitetsforskning",
                "section_name_en": "Education and university research",
                "line_description": "2:3 Formas",
                "line_description_en": "Anslag 2:3 Formas (Research Council for Environment, Agricultural Sciences and Spatial Planning)",
                "amount_local": 976200,
                "unit": "thousand",
                "currency": "SEK",
                "rd_category": "science_agency",
                "decision": "include",
                "confidence": 0.99,
                "page_number": "UO16-p1",
                "notes": "",
            },
            {
                "item_type": "line_item",
                "section_code": "UO 16",
                "section_name": "Utbildning och universitetsforskning",
                "section_name_en": "Education and university research",
                "line_description": "2:4 Forte",
                "line_description_en": "Anslag 2:4 Forte (Research Council for Health, Working Life and Welfare)",
                "amount_local": 776100,
                "unit": "thousand",
                "currency": "SEK",
                "rd_category": "science_agency",
                "decision": "include",
                "confidence": 0.99,
                "page_number": "UO16-p1",
                "notes": "",
            },
            {
                "item_type": "line_item",
                "section_code": "UO 16",
                "section_name": "Utbildning och universitetsforskning",
                "section_name_en": "Education and university research",
                "line_description": "2:5 Rymdstyrelsen",
                "line_description_en": "Anslag 2:5 Rymdstyrelsen (Swedish National Space Agency)",
                "amount_local": 940000,
                "unit": "thousand",
                "currency": "SEK",
                "rd_category": "science_agency",
                "decision": "include",
                "confidence": 0.95,
                "page_number": "UO16-p1",
                "notes": "",
            },
            {
                "item_type": "line_item",
                "section_code": "UO 16",
                "section_name": "Utbildning och universitetsforskning",
                "section_name_en": "Education and university research",
                "line_description": "2:6 RISE Research Institutes of Sweden AB",
                "line_description_en": "Anslag 2:6 RISE (Research Institutes of Sweden)",
                "amount_local": 596000,
                "unit": "thousand",
                "currency": "SEK",
                "rd_category": "science_agency",
                "decision": "include",
                "confidence": 0.95,
                "page_number": "UO16-p1",
                "notes": "",
            },
        ],
        "issues_found": [],
    },

    {
        "country": "Sweden",
        "year": 2025,
        "filename": "2025 budgetpropositionen-for-2025-hela-dokumentet-prop.-2024251.pdf",
        "page_id": "UO16-univ",
        "description": "University anslag — KTH, Uppsala, Lund, Karolinska, 2025",
        "scan_expected": True,
        "scan_confidence": 0.92,
        "text_snippet": """\
Anslag för universitet och högskolor 2025 (tusental kronor)

1:1  Uppsala universitet                    5 438 211
1:2  Lunds universitet                      7 143 500
1:3  Göteborgs universitet                  5 921 322
1:4  Stockholms universitet                 4 412 899
1:5  Umeå universitet                       3 614 100
1:6  Linköpings universitet                 3 201 440
1:7  Karolinska institutet                  4 871 300
1:8  Kungliga Tekniska högskolan (KTH)      5 201 400
1:9  Chalmers tekniska högskola             2 844 110
1:10 Luleå tekniska universitet             1 503 200

Summa anslag 1:1-1:27 (universitet)       62 411 000
""",
        "extract_items": [
            {
                "item_type": "line_item",
                "section_code": "UO 16",
                "section_name": "Utbildning och universitetsforskning",
                "section_name_en": "Education and university research",
                "line_description": "1:7 Karolinska institutet",
                "line_description_en": "Anslag 1:7 Karolinska Institutet (block grant)",
                "amount_local": 4871300,
                "unit": "thousand",
                "currency": "SEK",
                "rd_category": "higher_education",
                "decision": "include",
                "confidence": 0.85,
                "page_number": "UO16-univ",
                "notes": "Block grant covers both teaching and research. Tagged higher_education.",
            },
            {
                "item_type": "line_item",
                "section_code": "UO 16",
                "section_name": "Utbildning och universitetsforskning",
                "section_name_en": "Education and university research",
                "line_description": "1:8 Kungliga Tekniska högskolan (KTH)",
                "line_description_en": "Anslag 1:8 KTH Royal Institute of Technology (block grant)",
                "amount_local": 5201400,
                "unit": "thousand",
                "currency": "SEK",
                "rd_category": "higher_education",
                "decision": "include",
                "confidence": 0.85,
                "page_number": "UO16-univ",
                "notes": "Block grant covers both teaching and research. Tagged higher_education.",
            },
        ],
        "issues_found": [
            "ISSUE: The scan hint must include university anslag codes (1:1–1:27) — without hints "
            "the cheap scan model might mark this page as 'generic education, not R&D' and SKIP it. "
            "FIX: Already added to _COUNTRY_SCAN_HINTS — 'tusental kronor', named universities, "
            "and 'forskning' signal. The cleaner will then correctly tag as higher_education.",
        ],
    },

    {
        "country": "Sweden",
        "year": 2008,
        "filename": "2008 prop_200708__1_d17.pdf",
        "page_id": "UO16-2008-p1",
        "description": "UO 16 dedicated volume — Vetenskapsrådet 2008",
        "scan_expected": True,
        "scan_confidence": 0.97,
        "text_snippet": """\
Prop. 2007/08:1  Utgiftsområde 16

Anslag (tkr)          2006/07 utfall   2007 anslag   2008 förslag
25:1 Vetenskapsrådet   4 812 000        5 019 500      5 218 000
25:2 VINNOVA           2 311 000        2 400 000      2 512 000
25:3 Formas              621 000          648 000        672 000
25:4 FAS (Forte f.d.)    481 000          500 000        515 000
25:5 SMHI               534 000          548 000        561 000
25:6 Rymdstyrelsen       498 000          512 000        526 000
""",
        "extract_items": [
            {
                "item_type": "line_item",
                "section_code": "UO 16",
                "section_name": "Utbildning och universitetsforskning",
                "section_name_en": "Education and university research",
                "line_description": "25:1 Vetenskapsrådet",
                "line_description_en": "Anslag 25:1 Swedish Research Council (VR)",
                "amount_local": 5218000,
                "unit": "thousand",
                "currency": "SEK",
                "rd_category": "science_agency",
                "decision": "include",
                "confidence": 0.99,
                "page_number": "UO16-2008-p1",
                "notes": "Note: anslag code changed from 25:x (2008) to 2:x (post-2010 restructure).",
            },
        ],
        "issues_found": [
            "NOTE: Anslag codes changed over time (25:1 in 2008 → 2:1 post-2010). "
            "The canonical_series name_variants already handle this via 'vetenskapsradet' text match.",
        ],
    },

    {
        "country": "Sweden",
        "year": 1984,
        "filename": "1984 prop_198384__100.pdf",
        "page_id": "1984-p1",
        "description": "1984 scanned — pre-UO reform, Departement structure",
        "scan_expected": True,
        "scan_confidence": 0.75,
        "text_snippet": """\
§ 8. UTBILDNINGSDEPARTEMENTET

C. Forskning m. m.

C 1. Humanistisk-samhällsvetenskapliga
     forskningsrådsnämnden (HSFR) ....... 311 000

C 2. Statens naturvetenskapliga
     forskningsråd (NFR) ................. 467 500

C 3. Medicinska forskningsrådet (MFR) ... 284 200

C 4. Skogs- och jordbrukets
     forskningsråd (SJFR) ............... 183 000

D. Universiteten och högskolorna

D 1. Stockholms universitet ............. 1 522 000
     Driftsutgifter
""",
        "extract_items": [
            {
                "item_type": "line_item",
                "section_code": "§ 8",
                "section_name": "Utbildningsdepartementet",
                "section_name_en": "Ministry of Education",
                "line_description": "C 2. Statens naturvetenskapliga forskningsråd (NFR)",
                "line_description_en": "National Science Research Council (NFR)",
                "amount_local": 467500,
                "unit": "thousand",
                "currency": "SEK",
                "rd_category": "science_agency",
                "decision": "include",
                "confidence": 0.97,
                "page_number": "1984-p1",
                "notes": "Pre-2001 research council, merged into Vetenskapsrådet 2001.",
            },
            {
                "item_type": "line_item",
                "section_code": "§ 8",
                "section_name": "Utbildningsdepartementet",
                "section_name_en": "Ministry of Education",
                "line_description": "C 3. Medicinska forskningsrådet (MFR)",
                "line_description_en": "Medical Research Council (MFR)",
                "amount_local": 284200,
                "unit": "thousand",
                "currency": "SEK",
                "rd_category": "science_agency",
                "decision": "include",
                "confidence": 0.97,
                "page_number": "1984-p1",
                "notes": "Pre-2001 medical research council.",
            },
            {
                "item_type": "line_item",
                "section_code": "§ 8",
                "section_name": "Utbildningsdepartementet",
                "section_name_en": "Ministry of Education",
                "line_description": "D 1. Stockholms universitet — Driftsutgifter",
                "line_description_en": "Stockholm University — Operating expenditure (block grant)",
                "amount_local": 1522000,
                "unit": "thousand",
                "currency": "SEK",
                "rd_category": "higher_education",
                "decision": "include",
                "confidence": 0.82,
                "page_number": "1984-p1",
                "notes": "University block grant covers teaching + research.",
            },
        ],
        "issues_found": [
            "ISSUE: 1975-1993 files are scanned PDFs. OCR quality is low. "
            "The pipeline will attempt OCR with swe+eng langs (ocr_zoom=2.5). "
            "Amounts are on SEPARATE LINES from descriptions in many 1970s layouts "
            "('Driftsutgifter . . . 1 522 000'). The LLM needs to see multi-line context "
            "to pair description with amount. "
            "FIX: MAX_PAGES_PER_CHUNK=10 ensures enough context is sent together.",
        ],
    },

    {
        "country": "Sweden",
        "year": 1999,
        "filename": "1999 download.pdf",
        "page_id": "1999-uo16",
        "description": "1999 UO16 — pre-VR era, old research council codes",
        "scan_expected": True,
        "scan_confidence": 0.92,
        "text_snippet": """\
Utgiftsområde 16  Utbildning och universitetsforskning
                                          Tusental kronor
Anslag             1997/98 utfall  1999  2000
F 1. Statens råd för grundforskning
     och forskarutbildning (FRN)       812 400   854 000
F 2. Naturvetenskapliga
     forskningsrådet (NFR)           1 943 500 2 001 000
F 3. Teknikvetenskapliga
     forskningsrådet (TFR)             934 200   958 000
F 4. Humanistisk-samhällsvetenskapliga
     forskningsrådet (HSFR)            601 300   618 000
F 5. Medicinska forskningsrådet (MFR)  741 100   759 000
""",
        "extract_items": [
            {
                "item_type": "line_item",
                "section_code": "UO 16",
                "section_name": "Utbildning och universitetsforskning",
                "section_name_en": "Education and university research",
                "line_description": "F 2. Naturvetenskapliga forskningsrådet (NFR)",
                "line_description_en": "Anslag F 2. National Science Research Council (NFR)",
                "amount_local": 2001000,
                "unit": "thousand",
                "currency": "SEK",
                "rd_category": "science_agency",
                "decision": "include",
                "confidence": 0.98,
                "page_number": "1999-uo16",
                "notes": "Pre-2001 council, merged into Vetenskapsrådet 2001.",
            },
        ],
        "issues_found": [],
    },

    # ===== POLAND =====

    {
        "country": "Poland",
        "year": 2025,
        "filename": "2025 text.pdf",
        "page_id": "pl-2025-czesc28",
        "description": "Część 28 — Szkolnictwo wyższe i nauka, 2025",
        "scan_expected": True,
        "scan_confidence": 0.99,
        "text_snippet": """\
CZĘŚĆ 28 — SZKOLNICTWO WYŻSZE I NAUKA
Ministerstwo Nauki

                                        Plan 2025 (tys. zł)

Dział 730 Szkolnictwo wyższe
  Rozdział 73001 Szkoły wyższe            38 204 115
    w tym: subwencje dla uczelni         35 812 000

Dział 740 Działalność badawcza i rozwojowa
  Rozdział 74001 Badania naukowe i prace  8 431 200
    NCN — Narodowe Centrum Nauki         3 182 400
    NCBiR — Narodowe Centrum Badań       2 944 800
    Działalność statutowa PAN            1 104 000
    Programy i projekty badawcze MNiSW     512 000
    Inicjatywy doskonałości — uczelnie     688 000

CZĘŚĆ 28 RAZEM                          47 012 415
""",
        "extract_items": [
            {
                "item_type": "section_total",
                "section_code": "Część 28",
                "section_name": "Szkolnictwo wyższe i nauka",
                "section_name_en": "Higher Education and Science",
                "line_description": "Część 28 RAZEM",
                "line_description_en": "Part 28 total",
                "amount_local": 47012415,
                "unit": "thousand",
                "currency": "PLN",
                "rd_category": "higher_education",
                "decision": "review",
                "confidence": 0.65,
                "page_number": "pl-2025-czesc28",
                "notes": "Section total includes both HE (Dział 730) and direct R&D (Dział 740). Use sub-items for precision.",
            },
            {
                "item_type": "program_total",
                "section_code": "Część 28",
                "section_name": "Szkolnictwo wyższe i nauka",
                "section_name_en": "Higher Education and Science",
                "line_description": "Dział 740 Działalność badawcza i rozwojowa — Rozdział 74001",
                "line_description_en": "Division 740 R&D activity — Chapter 74001",
                "amount_local": 8431200,
                "unit": "thousand",
                "currency": "PLN",
                "rd_category": "direct_rd",
                "decision": "include",
                "confidence": 0.97,
                "page_number": "pl-2025-czesc28",
                "notes": "Dział 740 is the primary R&D division — all appropriations here are direct R&D.",
            },
            {
                "item_type": "line_item",
                "section_code": "Część 28",
                "section_name": "Szkolnictwo wyższe i nauka",
                "section_name_en": "Higher Education and Science",
                "line_description": "NCN — Narodowe Centrum Nauki",
                "line_description_en": "NCN — National Science Centre (basic research grants)",
                "amount_local": 3182400,
                "unit": "thousand",
                "currency": "PLN",
                "rd_category": "science_agency",
                "decision": "include",
                "confidence": 0.99,
                "page_number": "pl-2025-czesc28",
                "notes": "",
            },
            {
                "item_type": "line_item",
                "section_code": "Część 28",
                "section_name": "Szkolnictwo wyższe i nauka",
                "section_name_en": "Higher Education and Science",
                "line_description": "NCBiR — Narodowe Centrum Badań i Rozwoju",
                "line_description_en": "NCBiR — National Centre for Research and Development",
                "amount_local": 2944800,
                "unit": "thousand",
                "currency": "PLN",
                "rd_category": "science_agency",
                "decision": "include",
                "confidence": 0.99,
                "page_number": "pl-2025-czesc28",
                "notes": "",
            },
            {
                "item_type": "line_item",
                "section_code": "Część 28",
                "section_name": "Szkolnictwo wyższe i nauka",
                "section_name_en": "Higher Education and Science",
                "line_description": "Działalność statutowa PAN",
                "line_description_en": "PAN statutory activity (Polish Academy of Sciences)",
                "amount_local": 1104000,
                "unit": "thousand",
                "currency": "PLN",
                "rd_category": "science_agency",
                "decision": "include",
                "confidence": 0.97,
                "page_number": "pl-2025-czesc28",
                "notes": "",
            },
            {
                "item_type": "program_total",
                "section_code": "Część 28",
                "section_name": "Szkolnictwo wyższe i nauka",
                "section_name_en": "Higher Education and Science",
                "line_description": "Rozdział 73001 Szkoły wyższe — subwencje dla uczelni",
                "line_description_en": "Chapter 73001 Higher education — subvencje for universities",
                "amount_local": 35812000,
                "unit": "thousand",
                "currency": "PLN",
                "rd_category": "higher_education",
                "decision": "include",
                "confidence": 0.87,
                "page_number": "pl-2025-czesc28",
                "notes": "University block grants (subwencje). Tagged higher_education — covers teaching + research.",
            },
        ],
        "issues_found": [],
    },

    {
        "country": "Poland",
        "year": 2025,
        "filename": "2025 text.pdf",
        "page_id": "pl-2025-zus",
        "description": "ZUS social insurance — must be excluded",
        "scan_expected": False,
        "scan_confidence": 0.02,
        "text_snippet": """\
CZĘŚĆ 73 — ZAKŁAD UBEZPIECZEŃ SPOŁECZNYCH (ZUS)

                                        Plan 2025 (tys. zł)

Świadczenia emerytalno-rentowe         320 148 000
Świadczenia chorobowe                   28 441 000
Zasiłki macierzyńskie                   16 220 000
Renty z tytułu wypadków                  3 891 000

CZĘŚĆ 73 RAZEM                         368 700 000
""",
        "extract_items": [],
        "issues_found": [
            "CORRECT BEHAVIOUR: scan pass marks this page relevant=false (ZUS in scan hints). "
            "Even if it reaches extract pass, poland.py cleaner marks aggregation_role=non_rd.",
        ],
    },

    {
        "country": "Poland",
        "year": 2010,
        "filename": "2010 text.pdf",
        "page_id": "pl-2010-czesc28",
        "description": "Część 28 2010 — MNiSW era, NCBiR, no NCN yet",
        "scan_expected": True,
        "scan_confidence": 0.97,
        "text_snippet": """\
CZĘŚĆ 28 — NAUKA
Ministerstwo Nauki i Szkolnictwa Wyższego

                                        Plan 2010 (tys. zł)

Dział 740 Prace badawcze i rozwojowe
  NCBiR — Centrum Badań i Rozwoju       1 124 800
  Projekty badawcze (granty)            1 482 000
  Działalność statutowa jednostek         934 000
  Badania własne uczelni                  612 000
  Programy europejskie (udział MNiSW)     218 000

Dział 730 Szkolnictwo wyższe
  Dotacje dla uczelni                  16 412 000
""",
        "extract_items": [
            {
                "item_type": "line_item",
                "section_code": "Część 28",
                "section_name": "Nauka",
                "section_name_en": "Science (MNiSW — Ministry of Science and Higher Education)",
                "line_description": "NCBiR — Centrum Badań i Rozwoju",
                "line_description_en": "NCBiR — National Centre for Research and Development",
                "amount_local": 1124800,
                "unit": "thousand",
                "currency": "PLN",
                "rd_category": "science_agency",
                "decision": "include",
                "confidence": 0.99,
                "page_number": "pl-2010-czesc28",
                "notes": "NCBiR created 2007. NCN not yet created (comes 2011).",
            },
        ],
        "issues_found": [
            "NOTE: In 2010, the section is named 'Nauka' (not 'Szkolnictwo wyższe i nauka'). "
            "canonical_series name_variants for Część 28 cover this variation.",
        ],
    },

    {
        "country": "Poland",
        "year": 2000,
        "filename": "2000 text.pdf",
        "page_id": "pl-2000-kbn",
        "description": "Poland 2000 — KBN era, pre-NCBiR",
        "scan_expected": True,
        "scan_confidence": 0.93,
        "text_snippet": """\
KOMITET BADAŃ NAUKOWYCH

                                         Plan 2000 (tys. zł)

Badania podstawowe                        912 400
  Granty badawcze — projekty celowe       541 000
  Działalność statutowa                   371 400
Badania stosowane                         482 000
Wdrożenia wyników badań                   128 000

KBN RAZEM                               1 522 400

Polska Akademia Nauk                      634 200
  w tym: instytuty PAN                    598 000
""",
        "extract_items": [
            {
                "item_type": "section_total",
                "section_code": "KBN",
                "section_name": "Komitet Badań Naukowych",
                "section_name_en": "State Committee for Scientific Research",
                "line_description": "KBN RAZEM",
                "line_description_en": "KBN total",
                "amount_local": 1522400,
                "unit": "thousand",
                "currency": "PLN",
                "rd_category": "science_agency",
                "decision": "include",
                "confidence": 0.98,
                "page_number": "pl-2000-kbn",
                "notes": "KBN was the primary R&D funding body 1991-2005. Entire appropriation is R&D.",
            },
            {
                "item_type": "line_item",
                "section_code": "PAN",
                "section_name": "Polska Akademia Nauk",
                "section_name_en": "Polish Academy of Sciences",
                "line_description": "Polska Akademia Nauk — instytuty PAN",
                "line_description_en": "Polish Academy of Sciences — PAN institutes",
                "amount_local": 598000,
                "unit": "thousand",
                "currency": "PLN",
                "rd_category": "science_agency",
                "decision": "include",
                "confidence": 0.98,
                "page_number": "pl-2000-kbn",
                "notes": "",
            },
        ],
        "issues_found": [
            "NOTE: Pre-2007 KBN years will have a different structure than modern Część 28. "
            "The year_notes for 1995-2006 mention KBN explicitly. "
            "canonical_series.py KBN entry will match 'komitet badań naukowych' and 'kbn'.",
        ],
    },

    {
        "country": "Poland",
        "year": 1993,
        "filename": "1993 text.pdf",
        "page_id": "pl-1993-kbn",
        "description": "Poland 1993 — old złoty era",
        "scan_expected": True,
        "scan_confidence": 0.88,
        "text_snippet": """\
KOMITET BADAŃ NAUKOWYCH

                                   Ustawa 1993 (tys. zł st.)

Badania podstawowe                   142 800 000
Granty badawcze                       81 200 000
Działalność statutowa                 61 600 000

KBN ogółem                           224 000 000
""",
        "extract_items": [
            {
                "item_type": "section_total",
                "section_code": "KBN",
                "section_name": "Komitet Badań Naukowych",
                "section_name_en": "State Committee for Scientific Research",
                "line_description": "KBN ogółem",
                "line_description_en": "KBN total",
                "amount_local": 224000000,
                "unit": "thousand",
                "currency": "PLN",
                "rd_category": "science_agency",
                "decision": "include",
                "confidence": 0.95,
                "page_number": "pl-1993-kbn",
                "notes": "OLD ZŁOTY (stary złoty). 1 new PLN (post-1995) = 10,000 old PLN. "
                         "Cleaner will flag this row with poland_pre1995_old_zloty note.",
            },
        ],
        "issues_found": [
            "CRITICAL: Old złoty amounts are 10,000x larger than post-1995 new PLN. "
            "224,000,000 tys. old zł = 22,400 tys. new PLN (=22.4M PLN) — "
            "this is after dividing by 10,000. "
            "The cleaner flags pre-1995 rows; downstream processing must apply the 1/10000 conversion. "
            "FIX: cleaning_notes will contain '[poland_pre1995_old_zloty]' to trigger conversion flag.",
        ],
    },

    # ===== SLOVAKIA =====

    {
        "country": "Slovakia",
        "year": 2025,
        "filename": "2025 20250101_5681740-2.pdf",
        "page_id": "sk-2025-mas",
        "description": "Ministerstvo školstva, výskumu, vývoja a mládeže SR — 2025 EUR era",
        "scan_expected": True,
        "scan_confidence": 0.98,
        "text_snippet": """\
KAPITOLA 20
Ministerstvo školstva, výskumu, vývoja a mládeže Slovenskej republiky

                                        Schválený rozpočet 2025 (EUR)

Oblasť 730 Vzdelávanie
  Vysoké školstvo — subvencie           1 982 413 000
  Výskumné pracoviská na VŠ               412 000 000

Oblasť 740 Veda a výskum
  APVV — dotácia                          112 400 000
  VEGA — výskumné granty                   28 100 000
  Podpora výskumnej infraštruktúry         18 400 000
  Príspevok do CERN                         8 200 000
  Medzinárodná spolupráca vo vede          14 300 000

KAPITOLA 20 SPOLU                       5 358 963 927
""",
        "extract_items": [
            {
                "item_type": "section_total",
                "section_code": "Kapitola 20",
                "section_name": "Ministerstvo školstva, výskumu, vývoja a mládeže SR",
                "section_name_en": "Ministry of Education, Research, Development and Youth of the Slovak Republic",
                "line_description": "Kapitola 20 SPOLU",
                "line_description_en": "Chapter 20 total",
                "amount_local": 5358963927,
                "unit": "unit",
                "currency": "EUR",
                "rd_category": "higher_education",
                "decision": "review",
                "confidence": 0.6,
                "page_number": "sk-2025-mas",
                "notes": "Chapter total includes HE teaching + research. Use Oblasť 740 sub-items for pure R&D.",
            },
            {
                "item_type": "program_total",
                "section_code": "Kapitola 20",
                "section_name": "Ministerstvo školstva, výskumu, vývoja a mládeže SR",
                "section_name_en": "Ministry of Education, Research, Development and Youth SR",
                "line_description": "Oblasť 740 Veda a výskum",
                "line_description_en": "Sector 740 Science and research",
                "amount_local": 181400000,
                "unit": "unit",
                "currency": "EUR",
                "rd_category": "direct_rd",
                "decision": "include",
                "confidence": 0.97,
                "page_number": "sk-2025-mas",
                "notes": "Oblasť 740 = direct R&D; total of APVV + VEGA + infra + CERN + international.",
            },
            {
                "item_type": "line_item",
                "section_code": "Kapitola 20",
                "section_name": "Ministerstvo školstva, výskumu, vývoja a mládeže SR",
                "section_name_en": "Ministry of Education, Research, Development and Youth SR",
                "line_description": "APVV — dotácia",
                "line_description_en": "APVV grant (Slovak Research and Development Agency)",
                "amount_local": 112400000,
                "unit": "unit",
                "currency": "EUR",
                "rd_category": "science_agency",
                "decision": "include",
                "confidence": 0.99,
                "page_number": "sk-2025-mas",
                "notes": "",
            },
            {
                "item_type": "line_item",
                "section_code": "Kapitola 20",
                "section_name": "Ministerstvo školstva, výskumu, vývoja a mládeže SR",
                "section_name_en": "Ministry of Education, Research, Development and Youth SR",
                "line_description": "Príspevok do CERN",
                "line_description_en": "Contribution to CERN",
                "amount_local": 8200000,
                "unit": "unit",
                "currency": "EUR",
                "rd_category": "direct_rd",
                "decision": "include",
                "confidence": 0.97,
                "page_number": "sk-2025-mas",
                "notes": "Annual Slovak membership contribution to CERN.",
            },
        ],
        "issues_found": [
            "CRITICAL UNIT CHECK: EUR era amounts are FULL EUROS (not thousands). "
            "112,400,000 EUR is APVV grant, NOT 112B EUR. "
            "The cleaner's wrong_unit_eur check will catch any rows incorrectly tagged unit=thousand.",
        ],
    },

    {
        "country": "Slovakia",
        "year": 2025,
        "filename": "2025 20250101_5681740-2.pdf",
        "page_id": "sk-2025-sav",
        "description": "SAV (Slovenská akadémia vied) — kapitola 51, 2025",
        "scan_expected": True,
        "scan_confidence": 0.98,
        "text_snippet": """\
KAPITOLA 51
Slovenská akadémia vied (SAV)

                                        Schválený rozpočet 2025 (EUR)

Bežné výdavky                           128 341 000
  Mzdy a platy                            68 200 000
  Tovary a služby                         32 100 000
  Transfery                               28 041 000

Kapitálové výdavky                        10 415 937
  Investície do výskumnej infraštruktúry   8 200 000
  Ostatné                                  2 215 937

KAPITOLA 51 SPOLU                        138 756 937
""",
        "extract_items": [
            {
                "item_type": "section_total",
                "section_code": "Kapitola 51",
                "section_name": "Slovenská akadémia vied",
                "section_name_en": "Slovak Academy of Sciences (SAV)",
                "line_description": "Kapitola 51 SPOLU",
                "line_description_en": "Chapter 51 total — Slovak Academy of Sciences",
                "amount_local": 138756937,
                "unit": "unit",
                "currency": "EUR",
                "rd_category": "science_agency",
                "decision": "include",
                "confidence": 0.98,
                "page_number": "sk-2025-sav",
                "notes": "SAV is a dedicated research academy — entire chapter is R&D (basic research).",
            },
            {
                "item_type": "line_item",
                "section_code": "Kapitola 51",
                "section_name": "Slovenská akadémia vied",
                "section_name_en": "Slovak Academy of Sciences (SAV)",
                "line_description": "Investície do výskumnej infraštruktúry",
                "line_description_en": "Investment in research infrastructure",
                "amount_local": 8200000,
                "unit": "unit",
                "currency": "EUR",
                "rd_category": "research_infrastructure",
                "decision": "include",
                "confidence": 0.95,
                "page_number": "sk-2025-sav",
                "notes": "Capital expenditure explicitly for research infrastructure.",
            },
        ],
        "issues_found": [],
    },

    {
        "country": "Slovakia",
        "year": 1999,
        "filename": "1999 ZZ_1999_63_19990401.pdf",
        "page_id": "sk-1999-mas",
        "description": "Ministerstvo školstva SR 1999 — SKK era, tis. Sk",
        "scan_expected": True,
        "scan_confidence": 0.90,
        "text_snippet": """\
KAPITOLA 20 — MINISTERSTVO ŠKOLSTVA SLOVENSKEJ REPUBLIKY

Rozdeľovník výdavkov (v tis. Sk)

Výskum a vývoj na vysokých školách
  Aplikovaný výskum a vývoj                  15 000
  Vedeckovýskumné projekty (VEGA)            28 400
  Štátne objednávky výskumu                   8 200

Medzinárodná spolupráca
  Príspevok do CERN                           8 400
  COST, EUREKA, TEMPUS                        4 100

KAPITOLA 20 — výskum a vývoj spolu          64 100
""",
        "extract_items": [
            {
                "item_type": "line_item",
                "section_code": "Kapitola 20",
                "section_name": "Ministerstvo školstva Slovenskej republiky",
                "section_name_en": "Ministry of Education of the Slovak Republic",
                "line_description": "Aplikovaný výskum a vývoj na vysokých školách",
                "line_description_en": "Applied research and development at universities",
                "amount_local": 15000,
                "unit": "thousand",
                "currency": "SKK",
                "rd_category": "direct_rd",
                "decision": "include",
                "confidence": 0.95,
                "page_number": "sk-1999-mas",
                "notes": "SKK era (tis. Sk = thousands of Slovak koruna).",
            },
            {
                "item_type": "line_item",
                "section_code": "Kapitola 20",
                "section_name": "Ministerstvo školstva Slovenskej republiky",
                "section_name_en": "Ministry of Education of the Slovak Republic",
                "line_description": "Vedeckovýskumné projekty (VEGA)",
                "line_description_en": "Scientific research projects (VEGA grants)",
                "amount_local": 28400,
                "unit": "thousand",
                "currency": "SKK",
                "rd_category": "science_agency",
                "decision": "include",
                "confidence": 0.98,
                "page_number": "sk-1999-mas",
                "notes": "VEGA grant appropriation under MŠ SR.",
            },
            {
                "item_type": "line_item",
                "section_code": "Kapitola 20",
                "section_name": "Ministerstvo školstva Slovenskej republiky",
                "section_name_en": "Ministry of Education of the Slovak Republic",
                "line_description": "Príspevok do CERN",
                "line_description_en": "Slovak contribution to CERN",
                "amount_local": 8400,
                "unit": "thousand",
                "currency": "SKK",
                "rd_category": "direct_rd",
                "decision": "include",
                "confidence": 0.98,
                "page_number": "sk-1999-mas",
                "notes": "Annual CERN membership fee, expressed in tis. Sk.",
            },
        ],
        "issues_found": [
            "VERIFY: Header says 'v tis. Sk' — unit must be 'thousand', currency='SKK'. "
            "The cleaner's skk_hint check will catch if currency was incorrectly set to EUR.",
        ],
    },

    {
        "country": "Slovakia",
        "year": 1990,
        "filename": "1990 text.pdf",
        "page_id": "sk-1990-WRONG",
        "description": "1990 text.pdf — ACTUALLY POLISH BUDGET — must be excluded",
        "scan_expected": False,
        "scan_confidence": 0.01,
        "text_snippet": """\
DZIENNIK USTAW RZECZYPOSPOLITEJ POLSKIEJ

Warszawa, dnia 5 lutego 1990 r.

USTAWA
z dnia 23 stycznia 1990 r.
o budżecie państwa na 1990 rok

Art. 1.
Ustala się dochody budżetu państwa na 1990 rok
w wysokości 103 848 217 mln zł
""",
        "extract_items": [],
        "issues_found": [
            "CRITICAL: This file is a Polish budget in the Slovakia folder. "
            "Scan hint for Slovakia instructs the model to mark ALL pages relevant=false "
            "for the 1990 file. Cleaner also applies _POLISH_FILE_RE to detect and exclude. "
            "ACTION: Extract this as Poland 1990 instead — copy file to Poland folder.",
        ],
    },

    {
        "country": "Slovakia",
        "year": 2009,
        "filename": "2009 ZZ_2008_596_20090101.pdf",
        "page_id": "sk-2009-transition",
        "description": "Slovakia 2009 — transition year, first EUR budget",
        "scan_expected": True,
        "scan_confidence": 0.93,
        "text_snippet": """\
ZÁKON Č. 596/2008 Z. z.
o štátnom rozpočte na rok 2009

KAPITOLA 20 — MINISTERSTVO ŠKOLSTVA SR

Výdavky kapitoly (v EUR)

Oblasť 740 Veda a výskum
  APVV — Agentúra na podporu výskumu     84 312 000
  VEGA                                   22 180 000
  Výskum a vývoj na VŠ                   34 100 000
  Príspevok do CERN                       6 980 000

SAV (Kapitola 51)                       106 412 000
""",
        "extract_items": [
            {
                "item_type": "line_item",
                "section_code": "Kapitola 20",
                "section_name": "Ministerstvo školstva SR",
                "section_name_en": "Ministry of Education of the Slovak Republic",
                "line_description": "APVV — Agentúra na podporu výskumu a vývoja",
                "line_description_en": "APVV (Slovak Research and Development Agency)",
                "amount_local": 84312000,
                "unit": "unit",
                "currency": "EUR",
                "rd_category": "science_agency",
                "decision": "include",
                "confidence": 0.99,
                "page_number": "sk-2009-transition",
                "notes": "First EUR budget (2009). Amounts are FULL EUROS, not thousands.",
            },
        ],
        "issues_found": [
            "TRANSITION YEAR: 2009 is the first EUR year. Header says 'v EUR' — confirm "
            "currency=EUR and unit=unit (NOT thousand). "
            "SKK→EUR conversion rate was 30.1260 SKK/EUR. "
            "The cleaner's wrong_unit_eur flag will catch any rows tagged unit=thousand in 2009+.",
        ],
    },
]


# ---------------------------------------------------------------------------
# Simulation runner
# ---------------------------------------------------------------------------

def print_separator(char="=", width=80):
    print(char * width)


def simulate_scan_pass(case: dict, verbose: bool = False) -> None:
    """Show the scan pass prompt and expected decision."""
    country = case["country"]
    year = case["year"]
    ctx = COUNTRY_CONTEXT.get(country, {})
    doc_hint = ctx.get("doc_type_hint", "")[:200]

    pages = [(case["page_id"], case["text_snippet"])]
    prompt = build_batch_scan_user_prompt(pages, country, year, doc_hint)

    expected_relevant = case["scan_expected"]
    expected_conf = case["scan_confidence"]

    print(f"\n  [SCAN] Page: {case['page_id']}")
    print(f"         Expected: relevant={expected_relevant}, confidence={expected_conf:.2f}")

    expected_json = {
        "pages": [
            {
                "page_id": case["page_id"],
                "relevant": expected_relevant,
                "confidence": expected_conf,
            }
        ]
    }
    print(f"  [SCAN RESPONSE] {json.dumps(expected_json, ensure_ascii=False)}")

    if verbose:
        print("\n  --- SCAN PROMPT ---")
        print(textwrap.indent(prompt[:1500], "  "))
        print("  --- END SCAN PROMPT ---")


def simulate_extract_pass(case: dict, verbose: bool = False) -> None:
    """Show the extract pass prompt and expected items."""
    if not case["scan_expected"]:
        print(f"  [EXTRACT] SKIPPED — page marked not relevant by scan pass")
        return

    country = case["country"]
    year = case["year"]
    ctx = COUNTRY_CONTEXT.get(country, {})

    prompt = build_extract_user_prompt(
        pages_text=case["text_snippet"],
        country=country,
        year=year,
        currency=ctx.get("currency", ""),
        unit_hint=ctx.get("unit_hint", ""),
        doc_hint=ctx.get("doc_type_hint", ""),
        known_agencies=ctx.get("known_agencies", []),
        mixed_ministries=ctx.get("mixed_ministries", []),
        page_range=case["page_id"],
    )

    items = case["extract_items"]
    print(f"  [EXTRACT] Items extracted: {len(items)}")
    for item in items:
        flag = "✓" if item["decision"] == "include" else "?"
        amt = f"{item['amount_local']:,}" if item["amount_local"] else "null"
        print(
            f"    {flag} [{item['item_type']:<15}] {item['line_description_en'][:55]:<55} "
            f"{amt:>15} {item['unit']:<10} {item['currency']} conf={item['confidence']:.2f}"
        )

    if verbose:
        print("\n  --- EXTRACT PROMPT (first 2000 chars) ---")
        print(textwrap.indent(prompt[:2000], "  "))
        print("  --- END EXTRACT PROMPT ---")
        print("\n  --- EXPECTED RESPONSE ---")
        print(textwrap.indent(json.dumps({"items": items}, indent=2, ensure_ascii=False)[:2000], "  "))
        print("  --- END RESPONSE ---")


def run_simulation(countries: list[str], verbose: bool = False) -> None:
    cases_by_country: dict[str, list[dict]] = {}
    for case in SIMULATION_CASES:
        if case["country"] in countries:
            cases_by_country.setdefault(case["country"], []).append(case)

    for country in countries:
        cases = cases_by_country.get(country, [])
        if not cases:
            print(f"\n[INFO] No simulation cases defined for {country}")
            continue

        print_separator()
        print(f"SIMULATION — {country} ({len(cases)} cases)")
        print_separator()

        # Check scan hints
        if country in _COUNTRY_SCAN_HINTS:
            print(f"\n[OK] _COUNTRY_SCAN_HINTS entry exists for {country}")
        else:
            print(f"\n[WARN] No _COUNTRY_SCAN_HINTS entry for {country} — scan pass runs blind!")

        # Check country profiles
        if country in COUNTRY_PROFILES:
            profile = COUNTRY_PROFILES[country]
            print(f"[OK] COUNTRY_PROFILES entry: {len(profile.get('skip_if', []))} skip rules, "
                  f"{len(profile.get('include_note', []))} include notes")
        else:
            print(f"[WARN] No COUNTRY_PROFILES entry for {country}")

        ctx = COUNTRY_CONTEXT.get(country, {})
        if ctx:
            print(f"[OK] COUNTRY_CONTEXT: currency={ctx.get('currency')}, "
                  f"ocr_langs={ctx.get('ocr_langs', 'not set')}, "
                  f"agencies={len(ctx.get('known_agencies', []))}")
        else:
            print(f"[WARN] No COUNTRY_CONTEXT entry for {country}")

        all_issues = []
        included_total = 0
        review_total = 0
        skipped_total = 0

        for i, case in enumerate(cases):
            print(f"\n[Case {i+1}/{len(cases)}] {case['description']}")
            print(f"  File: {case['filename']} | Year: {case['year']}")

            simulate_scan_pass(case, verbose=verbose)

            if case["scan_expected"]:
                simulate_extract_pass(case, verbose=verbose)
                for item in case["extract_items"]:
                    if item["decision"] == "include":
                        included_total += 1
                    else:
                        review_total += 1
            else:
                skipped_total += 1

            for issue in case.get("issues_found", []):
                print(f"\n  [!] {issue}")
                all_issues.append(f"[{case['year']}] {issue}")

        print(f"\n{'─' * 60}")
        print(f"SUMMARY — {country}")
        print(f"  Cases simulated : {len(cases)}")
        print(f"  Items include   : {included_total}")
        print(f"  Items review    : {review_total}")
        print(f"  Pages skipped   : {skipped_total}")
        if all_issues:
            print(f"  Issues found    : {len(all_issues)}")
            for issue in all_issues:
                print(f"    ► {issue[:120]}")
        print()


def main():
    parser = argparse.ArgumentParser(description="Simulate LLM extraction for Sweden, Poland, Slovakia.")
    parser.add_argument("--country", nargs="*", default=["Sweden", "Poland", "Slovakia"],
                        metavar="COUNTRY")
    parser.add_argument("--verbose", action="store_true",
                        help="Print full prompt text alongside results")
    args = parser.parse_args()

    print("\nBUDGET PIPELINE — LLM EXTRACTION SIMULATION")
    print("Validates prompts and expected outputs without calling the API\n")

    run_simulation(args.country, verbose=args.verbose)

    print("\n" + "=" * 80)
    print("SIMULATION COMPLETE")
    print("\nTo run actual LLM extraction:")
    print("  python preextract_text.py --country Sweden Poland Slovakia")
    print("  python main.py --budget --country Sweden --llm-pipeline")
    print("  python main.py --budget --country Poland --llm-pipeline")
    print("  python main.py --budget --country Slovakia --llm-pipeline")


if __name__ == "__main__":
    main()
