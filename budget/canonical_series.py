"""
Canonical R&D agency series builder for budget.

The raw LLM extraction captures everything plausibly R&D-related, including
portfolio totals for broad ministries (Health, Industry, Education) that are
NOT pure R&D. Summing those gives a meaningless number.

This module defines, per country, the SPECIFIC AGENCIES to track for a
reliable, year-comparable R&D time series. For each agency we:

  1. Match rows in the results DataFrame by name variants
  2. Select the best single amount per year (agency-level total, preferring
     dedicated R&D agency totals over portfolio or line-item level)
  3. Flag gaps (years with no data) rather than interpolating
  4. Build a clean panel: one row per (country, agency, year)

Design principle:
  Track the same entities over time. When an agency renames itself
  (e.g. AAEC → ANSTO, Dept of Science → absorbed into Industry) the
  canonical series handles that via name_variants.

Adding a new country:
  Add a block to CANONICAL_AGENCIES. Each agency needs:
    - canonical_name  : stable name used in the output
    - category        : rd_category for this series
    - name_variants   : list of partial strings to match in line_description_en
      (case-insensitive, OR logic — first match wins)
    - preferred_item_type : which item_type to prefer (section_total or program_total)
    - active_years    : (start, end) inclusive — None means open
"""

from __future__ import annotations

import gzip
import logging
import re
import unicodedata
from functools import lru_cache
from pathlib import Path
from typing import Optional

import pandas as pd

from budget.manual_curation import get_locked_series_entries

logger = logging.getLogger(__name__)

__all__ = ["build_canonical_series", "CANONICAL_AGENCIES"]

_OUTPUT_UNIT_BY_CURRENCY = {
    "AUD": "dollar",
    "CAD": "dollar",
    "CRC": "colon",
    "CZK": "koruna",
    "SKK": "koruna",
    "NZD": "dollar",
    "USD": "dollar",
    "HUF": "forint",
    "ILS": "shekel",
    "JPY": "yen",
    "KRW": "won",
    "EUR": "euro",
    "DEM": "mark",
    "FRF": "franc",
    "LUF": "franc",
    "GBP": "pound",
    "PTE": "escudo",
    "DKK": "krone",
    "NOK": "krone",
    "SEK": "krona",
    "ISK": "krona",
    "FIM": "markka",   # Finnish markka (pre-2002)
    "NLG": "guilder",  # Netherlands guilder (pre-2002)
    "CHF": "franc",
    "BEF": "franc",
    "ATS": "schilling",
    "ITL": "lira",
    "EEK": "kroon",
    "RUB": "ruble",
    "LVL": "lats",
    "LTL": "litas",
    "TAL": "talonas",
    "TRL": "lira",
    "YTL": "lira",
    "TRY": "lira",
}

_SCALE_TO_BASE_UNIT = {
    "thousand": 1_000.0,
    "thousands": 1_000.0,
    "k": 1_000.0,
    "million": 1_000_000.0,
    "millions": 1_000_000.0,
    "billion": 1_000_000_000.0,
    "milliard": 1_000_000_000.0,
}

_FRANCE_FULL_TEXT_DIR = Path("Data/output/budget/full_text/France")
_JAPAN_FULL_TEXT_DIR = Path("Data/output/budget/full_text/Japan")

_FRANCE_GENERIC_DISCOVERED_PATTERNS = [
    re.compile(r"^payment credits? for\b", re.IGNORECASE),
    re.compile(r"^total(?: des)? cr[ée]dits? de paiement pour\b", re.IGNORECASE),
    re.compile(r"^total budget for\b", re.IGNORECASE),
    re.compile(r"^cr[ée]dits? de paiement\b", re.IGNORECASE),
    re.compile(r"^total pour\b", re.IGNORECASE),
    re.compile(r"^total for\b", re.IGNORECASE),
]

_FRANCE_CP_PREFIX_RE = re.compile(
    r"^(?:total\s+pour\s+|total\s+for\s+|cr[ée]dits?\s+de\s+paiement\s+pour\s+|payment\s+credits?\s+for\s+)",
    re.IGNORECASE,
)
_FRANCE_LEADING_ARTICLE_RE = re.compile(r"^(?:l'|le\s+|la\s+|les\s+|the\s+)", re.IGNORECASE)
_UK_ALLOWED_DISCOVERED_CANONICALS = {
    "advanced research and invention agency",
    "industrial strategy challenge fund",
    "national productivity investment fund",
    "r&d expenditure credit",
    "research infrastructure projects",
    "research partnership investment fund",
    "strength in places fund",
}
_ESTONIA_GENERIC_DISCOVERED_PATTERNS = [
    re.compile(r"\ballocated\b", re.IGNORECASE),
    re.compile(r"\ballocation(?:s)?\b", re.IGNORECASE),
    re.compile(r"\bfinancing\b", re.IGNORECASE),
    re.compile(r"\bfunding\b", re.IGNORECASE),
    re.compile(r"\bgrant(?:s)?\b", re.IGNORECASE),
    re.compile(r"\binfrastructure\b", re.IGNORECASE),
    re.compile(r"\bprogram(?:me)?\b", re.IGNORECASE),
    re.compile(r"\bresearch support\b", re.IGNORECASE),
    re.compile(r"\bresearch topics?\b", re.IGNORECASE),
    re.compile(r"\btarget(?:ed)? financing\b", re.IGNORECASE),
]
_ESTONIA_ORGANISATION_HINTS = re.compile(
    r"\b(?:"
    r"academy|agency|centre|center|council|foundation|fund|institute|institution|"
    r"laborator|museum|observator|university|ulikool|teadusagentuur|teadusfond|"
    r"teaduste akadeemia|archimedes|biocenter|genome center"
    r")\b",
    re.IGNORECASE,
)
_COLOMBIA_ORGANISATION_HINTS = re.compile(
    r"\b(?:"
    r"agencia|corporaci[oó]n|fondo|instituto|ministerio|servicio|unidad|"
    r"colciencias|minciencias|agrosavia|corpoica|ideam|metrolog[ií]a|salud"
    r")\b",
    re.IGNORECASE,
)
_NEW_ZEALAND_ORGANISATION_HINTS = re.compile(
    r"\b(?:"
    r"institute|institutes|callaghan|agresearch|niwa|gns|plant and food|"
    r"regional research institute"
    r")\b",
    re.IGNORECASE,
)
_ESTONIA_VERIFIED_DROPS = (
    ("University of Tartu", 2004),
    ("Tallinn University of Technology", 2004),
    ("University of Tartu", 2010),
    ("Tallinn University of Technology", 2010),
)
_NEW_ZEALAND_VERIFIED_DROPS: set[tuple[int, str]] = {
    # 1975 DSIR is a Works and Trading Account total, not the DSIR budget row.
    # 1977 and 1984 DSIR totals are OCR-corrupted vote aggregates, not
    # defensible agency appropriations. 1992 CRI "1" is a page/index artefact.
    # 1990/1995/2001 RST vote totals are not recoverable cleanly from the
    # cached original or collapse onto non-comparable vote lines.
    # 1996-1997 Marsden rows are OCR/index mismatches rather than clean fund
    # observations from the source page.
    (1975, "DSIR (New Zealand)"),
    (1977, "DSIR (New Zealand)"),
    (1984, "DSIR (New Zealand)"),
    (1992, "Crown Research Institutes (New Zealand)"),
    (1990, "Research, Science and Technology Vote (New Zealand)"),
    (1995, "Research, Science and Technology Vote (New Zealand)"),
    (2001, "Research, Science and Technology Vote (New Zealand)"),
    (1996, "Marsden Fund (New Zealand)"),
    (1997, "Marsden Fund (New Zealand)"),
}
_NEW_ZEALAND_VERIFIED_OVERRIDES: dict[int, dict[str, tuple[float, int, str, str, str]]] = {
    2015: {
        "Callaghan Innovation": (
            63_710_000.0,
            10,
            "NZD",
            "2015_Appropriation Estimates Act.pdf",
            "Science and Innovation: Callaghan Innovation",
        ),
    },
    2019: {
        "Callaghan Innovation": (
            75_151_000.0,
            10,
            "NZD",
            "2019_Appropriation Estimates Act.pdf",
            "Research, Science and Innovation: Callaghan Innovation - Operations",
        ),
    },
    2020: {
        "Callaghan Innovation": (
            80_288_000.0,
            9,
            "NZD",
            "2020_Appropriation Estimates Act.pdf",
            "Research, Science and Innovation: Callaghan Innovation - Operations",
        ),
    },
    2021: {
        "Callaghan Innovation": (
            79_823_000.0,
            11,
            "NZD",
            "2021_Appropriation Estimates Act.pdf",
            "Research, Science and Innovation: Callaghan Innovation - Operations",
        ),
    },
    2023: {
        "Callaghan Innovation": (
            85_868_000.0,
            10,
            "NZD",
            "2023_Appropriation Estimates Act.pdf",
            "Research, Science and Innovation: Callaghan Innovation - Operations",
        ),
    },
}
_BELGIUM_VERIFIED_DROPS = (
    # 2012-2013 science-policy tables in the current Belgium corpus are
    # January-March provisional appropriations ("janvier-mars" /
    # "januari-maart"), not comparable full-year annual budgets.
    ("BELSPO / Belgian Federal Science Policy", 2012),
    ("BELSPO / Belgian Federal Science Policy", 2013),
    ("SCK CEN", 1998),
    ("SCK CEN", 1999),
    ("SCK CEN", 2000),
    ("Institute of Radioelements (IRE)", 1998),
    ("Institute of Radioelements (IRE)", 1999),
    ("Institute of Radioelements (IRE)", 2000),
    ("Institute of Radioelements (IRE)", 2001),
)
_BELGIUM_VERIFIED_OVERRIDES: dict[int, dict[str, tuple[float, int, str, str]]] = {
    1994: {
        "BELSPO / Belgian Federal Science Policy": (
            6_121_300_000.0,
            87,
            "BEF",
            "1995 16_1.pdf",
        ),
    },
    1995: {
        "Scientific Institute of Public Health / Louis Pasteur": (
            333_600_000.0,
            168,
            "BEF",
            "1995 16_1.pdf",
        ),
    },
    1998: {
        "BELSPO / Belgian Federal Science Policy": (
            5_793_800_000.0,
            72,
            "BEF",
            "1999 03_2.pdf",
        ),
    },
    1999: {
        "BELSPO / Belgian Federal Science Policy": (
            6_803_200_000.0,
            72,
            "BEF",
            "1999 03_2.pdf",
        ),
    },
    2000: {
        "BELSPO / Belgian Federal Science Policy": (
            6_106_700_000.0,
            190,
            "BEF",
            "2001 Belgium 50K0905007.pdf",
        ),
    },
    2001: {
        "BELSPO / Belgian Federal Science Policy": (
            7_110_000_000.0,
            191,
            "BEF",
            "2001 Belgium 50K0905007.pdf",
        ),
    },
}
_ESTONIA_VERIFIED_OVERRIDES: dict[int, dict[str, tuple[float, int, str, str]]] = {
    2007: {
        "Estonian Research Council / Science Foundation": (
            124_156_000.0,
            16,
            "EEK",
            "2007 12768664.pdf",
        ),
    },
    2008: {
        "Estonian Research Council / Science Foundation": (
            160_193_000.0,
            16,
            "EEK",
            "2008 12901846.pdf",
        ),
    },
    2022: {
        "Estonia R&D / Innovation Programmes (post-2011)": (
            218_717_000.0,
            4,
            "EUR",
            "2022 125052022002.pdf",
        ),
    },
    2025: {
        "Estonia R&D / Innovation Programmes (post-2011)": (
            247_281_000.0,
            4,
            "EUR",
            "2025 123122024014.pdf",
        ),
    },
}
_LITHUANIA_VERIFIED_OVERRIDES: dict[int, dict[str, tuple[float, int, str, str]]] = {
    # Verified against original Lithuania budget tables / text cache.
    1993: {
        "State Science, Studies and Technology Service (Lithuania)": (
            6_531_015_000.0,
            2,
            "TAL",
            "1993 TAR.8C4914C2ACED.docx",
        ),
        "State Research Centre for the Genocide of the Lithuanian Population": (
            8_300_000.0,
            2,
            "TAL",
            "1993 TAR.8C4914C2ACED.docx",
        ),
    },
    1994: {
        "State Science, Studies and Technology Service (Lithuania)": (
            408_000.0,
            0,
            "LTL",
            "1994 TAIS_33485.docx",
        ),
    },
    1996: {
        "Lithuanian Genocide and Resistance Research Centre": (
            4_651_000.0,
            1,
            "LTL",
            "1996 TAR.11F4B795287C.docx",
        ),
    },
    1997: {
        "Lithuanian Genocide and Resistance Research Centre": (
            3_141_000.0,
            13,
            "LTL",
            "1997 TAIS_41654.docx",
        ),
        "Lithuanian Geological Survey under the Ministry of Construction and Urban Planning": (
            2_808_000.0,
            13,
            "LTL",
            "1997 TAIS_41654.docx",
        ),
    },
    1998: {
        "National Energy Efficiency Improvement and Strategy Programs": (
            4_166_000.0,
            1,
            "LTL",
            "1998 TAR.A94C9B003D30.docx",
        ),
        "Science and Studies Programme (Lithuania)": (
            541_169_000.0,
            0,
            "LTL",
            "1998 TAR.A94C9B003D30.docx",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            5_902_000.0,
            0,
            "LTL",
            "1998 TAR.A94C9B003D30.docx",
        ),
        "Lithuanian Geological Survey under the Ministry of Construction and Urban Planning": (
            3_506_000.0,
            1,
            "LTL",
            "1998 TAR.A94C9B003D30.docx",
        ),
    },
    1999: {
        "Science and Studies Programme (Lithuania)": (
            370_333_000.0,
            4,
            "LTL",
            "1999 TAIS_102336.docx",
        ),
        "State Science, Studies and Technology Service (Lithuania)": (
            1_419_000.0,
            8,
            "LTL",
            "1999 TAIS_102336.docx",
        ),
        "Fundamental Scientific Research": (
            3_390_000.0,
            5,
            "LTL",
            "1999 TAIS_102336.docx",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            6_631_000.0,
            8,
            "LTL",
            "1999 TAIS_102336.docx",
        ),
    },
    2001: {
        "Science and Studies Programme (Lithuania)": (
            611_927_000.0,
            1,
            "LTL",
            "2001 TAIS_156412.docx",
        ),
        "State Science, Studies and Technology Service (Lithuania)": (
            5_052_000.0,
            1,
            "LTL",
            "2001 TAIS_156412.docx",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            3_198_000.0,
            1,
            "LTL",
            "2001 TAIS_156412.docx",
        ),
    },
    2002: {
        "Lithuanian Genocide and Resistance Research Centre": (
            5_925_000.0,
            1,
            "LTL",
            "2002 TAR.E8D4F01C9643.docx",
        ),
    },
    2003: {
        "Lithuanian Research Council": (
            800_000.0,
            1,
            "LTL",
            "2003 TAR.BCA0F623B8BA.docx",
        ),
        "Lithuanian Academy of Sciences": (
            2_987_000.0,
            1,
            "LTL",
            "2003 TAR.BCA0F623B8BA.docx",
        ),
        "Lithuanian Energy Institute": (
            9_497_000.0,
            1,
            "LTL",
            "2003 TAR.BCA0F623B8BA.docx",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            4_875_000.0,
            1,
            "LTL",
            "2003 TAR.BCA0F623B8BA.docx",
        ),
        "Lithuanian History Institute": (
            2_089_000.0,
            1,
            "LTL",
            "2003 TAR.BCA0F623B8BA.docx",
        ),
        "Institute of the Lithuanian Language": (
            1_974_000.0,
            1,
            "LTL",
            "2003 TAR.BCA0F623B8BA.docx",
        ),
    },
    2004: {
        "Lithuanian Research Council": (
            800_000.0,
            1,
            "LTL",
            "2004 TAR.538D8DA9A346.docx",
        ),
        "Lithuanian Academy of Sciences": (
            3_571_000.0,
            1,
            "LTL",
            "2004 TAR.538D8DA9A346.docx",
        ),
        "Lithuanian Energy Institute": (
            10_754_000.0,
            1,
            "LTL",
            "2004 TAR.538D8DA9A346.docx",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            4_551_000.0,
            1,
            "LTL",
            "2004 TAR.538D8DA9A346.docx",
        ),
        "Lithuanian History Institute": (
            2_422_000.0,
            1,
            "LTL",
            "2004 TAR.538D8DA9A346.docx",
        ),
        "Institute of the Lithuanian Language": (
            2_053_000.0,
            1,
            "LTL",
            "2004 TAR.538D8DA9A346.docx",
        ),
    },
    2005: {
        "Lithuanian Research Council": (
            865_000.0,
            16,
            "LTL",
            "2005 TAIS_259480.pdf",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            5_191_000.0,
            14,
            "LTL",
            "2005 TAIS_259480.pdf",
        ),
    },
    2006: {
        "Lithuanian Research Council": (
            945_000.0,
            1,
            "LTL",
            "2006 TAR.1BAE24CE65A7.docx",
        ),
        "Lithuanian Academy of Sciences": (
            6_576_000.0,
            1,
            "LTL",
            "2006 TAR.1BAE24CE65A7.docx",
        ),
        "Lithuanian Energy Institute": (
            13_019_000.0,
            1,
            "LTL",
            "2006 TAR.1BAE24CE65A7.docx",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            5_632_000.0,
            1,
            "LTL",
            "2006 TAR.1BAE24CE65A7.docx",
        ),
        "Lithuanian History Institute": (
            2_935_000.0,
            1,
            "LTL",
            "2006 TAR.1BAE24CE65A7.docx",
        ),
        "Institute of the Lithuanian Language": (
            2_224_000.0,
            1,
            "LTL",
            "2006 TAR.1BAE24CE65A7.docx",
        ),
    },
    2007: {
        "Lithuanian Research Council": (
            984_000.0,
            1,
            "LTL",
            "2007 TAR.802CCF0B0455.docx",
        ),
        "Lithuanian Academy of Sciences": (
            6_522_000.0,
            1,
            "LTL",
            "2007 TAR.802CCF0B0455.docx",
        ),
        "Lithuanian Energy Institute": (
            14_271_000.0,
            1,
            "LTL",
            "2007 TAR.802CCF0B0455.docx",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            4_815_000.0,
            1,
            "LTL",
            "2007 TAR.802CCF0B0455.docx",
        ),
        "Lithuanian History Institute": (
            3_205_000.0,
            1,
            "LTL",
            "2007 TAR.802CCF0B0455.docx",
        ),
        "Institute of the Lithuanian Language": (
            2_965_000.0,
            1,
            "LTL",
            "2007 TAR.802CCF0B0455.docx",
        ),
    },
    2008: {
        "Lithuanian Research Council": (
            2_153_000.0,
            1,
            "LTL",
            "2008 TAR.E51A2DE98B9E.docx",
        ),
        "Lithuanian Academy of Sciences": (
            7_699_000.0,
            1,
            "LTL",
            "2008 TAR.E51A2DE98B9E.docx",
        ),
        "Lithuanian Energy Institute": (
            16_880_000.0,
            1,
            "LTL",
            "2008 TAR.E51A2DE98B9E.docx",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            6_261_000.0,
            1,
            "LTL",
            "2008 TAR.E51A2DE98B9E.docx",
        ),
        "Lithuanian History Institute": (
            3_955_000.0,
            1,
            "LTL",
            "2008 TAR.E51A2DE98B9E.docx",
        ),
        "Institute of the Lithuanian Language": (
            4_524_000.0,
            1,
            "LTL",
            "2008 TAR.E51A2DE98B9E.docx",
        ),
    },
    2009: {
        "Lithuanian Research Council": (
            6_492_000.0,
            1,
            "LTL",
            "2009 TAR.D641C5B5ADFD.docx",
        ),
        "Chemistry Institute": (
            7_949_000.0,
            2,
            "LTL",
            "2009 TAR.D641C5B5ADFD.docx",
        ),
        "Institute of Geology and Geography": (
            3_950_000.0,
            2,
            "LTL",
            "2009 TAR.D641C5B5ADFD.docx",
        ),
        "Institute of Lithuanian Literature and Folklore": (
            3_745_000.0,
            2,
            "LTL",
            "2009 TAR.D641C5B5ADFD.docx",
        ),
        "Institute of Semiconductor Physics": (
            10_019_000.0,
            2,
            "LTL",
            "2009 TAR.D641C5B5ADFD.docx",
        ),
        "Institute of the Lithuanian Language": (
            4_807_000.0,
            1,
            "LTL",
            "2009 TAR.D641C5B5ADFD.docx",
        ),
        "Lithuanian Academy of Sciences": (
            4_841_000.0,
            1,
            "LTL",
            "2009 TAR.D641C5B5ADFD.docx",
        ),
        "Lithuanian Energy Institute": (
            17_281_000.0,
            2,
            "LTL",
            "2009 TAR.D641C5B5ADFD.docx",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            7_585_000.0,
            2,
            "LTL",
            "2009 TAR.D641C5B5ADFD.docx",
        ),
        "Lithuanian History Institute": (
            3_951_000.0,
            2,
            "LTL",
            "2009 TAR.D641C5B5ADFD.docx",
        ),
        "Lithuanian Institute of Horticulture": (
            4_654_000.0,
            2,
            "LTL",
            "2009 TAR.D641C5B5ADFD.docx",
        ),
        "National Cancer Institute": (
            6_505_000.0,
            2,
            "LTL",
            "2009 TAR.D641C5B5ADFD.docx",
        ),
    },
    2010: {
        "Lithuanian Research Council": (
            54_909_000.0,
            1,
            "LTL",
            "2010 TAR.E5C7DCAD90FA.docx",
        ),
        "Chemistry Institute": (
            5_798_000.0,
            2,
            "LTL",
            "2010 TAR.E5C7DCAD90FA.docx",
        ),
        "Institute of Geology and Geography": (
            2_931_000.0,
            2,
            "LTL",
            "2010 TAR.E5C7DCAD90FA.docx",
        ),
        "Institute of Lithuanian Literature and Folklore": (
            2_868_000.0,
            2,
            "LTL",
            "2010 TAR.E5C7DCAD90FA.docx",
        ),
        "Institute of Semiconductor Physics": (
            6_660_000.0,
            2,
            "LTL",
            "2010 TAR.E5C7DCAD90FA.docx",
        ),
        "Institute of the Lithuanian Language": (
            3_266_000.0,
            1,
            "LTL",
            "2010 TAR.E5C7DCAD90FA.docx",
        ),
        "Lithuanian Academy of Sciences": (
            2_554_000.0,
            1,
            "LTL",
            "2010 TAR.E5C7DCAD90FA.docx",
        ),
        "Lithuanian Energy Institute": (
            13_993_000.0,
            2,
            "LTL",
            "2010 TAR.E5C7DCAD90FA.docx",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            5_325_000.0,
            1,
            "LTL",
            "2010 TAR.E5C7DCAD90FA.docx",
        ),
        "Lithuanian History Institute": (
            2_875_000.0,
            2,
            "LTL",
            "2010 TAR.E5C7DCAD90FA.docx",
        ),
        "Lithuanian Institute of Horticulture": (
            3_929_000.0,
            2,
            "LTL",
            "2010 TAR.E5C7DCAD90FA.docx",
        ),
        "National Cancer Institute": (
            3_695_000.0,
            2,
            "LTL",
            "2010 TAR.E5C7DCAD90FA.docx",
        ),
    },
    2011: {
        "Lithuanian Research Council": (
            91_018_000.0,
            1,
            "LTL",
            "2011 TAR.FE51590E2B56.docx",
        ),
        "Centre for Physical Sciences and Technology (Lithuania)": (
            17_635_000.0,
            2,
            "LTL",
            "2011 TAR.FE51590E2B56.docx",
        ),
        "Institute of Lithuanian Literature and Folklore": (
            2_917_000.0,
            2,
            "LTL",
            "2011 TAR.FE51590E2B56.docx",
        ),
        "Institute of the Lithuanian Language": (
            3_251_000.0,
            2,
            "LTL",
            "2011 TAR.FE51590E2B56.docx",
        ),
        "Lithuanian Academy of Sciences": (
            2_573_000.0,
            2,
            "LTL",
            "2011 TAR.FE51590E2B56.docx",
        ),
        "Lithuanian Culture Research Institute": (
            2_389_000.0,
            2,
            "LTL",
            "2011 TAR.FE51590E2B56.docx",
        ),
        "Lithuanian Energy Institute": (
            14_565_000.0,
            2,
            "LTL",
            "2011 TAR.FE51590E2B56.docx",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            5_669_000.0,
            1,
            "LTL",
            "2011 TAR.FE51590E2B56.docx",
        ),
        "Lithuanian History Institute": (
            3_027_000.0,
            2,
            "LTL",
            "2011 TAR.FE51590E2B56.docx",
        ),
        "Lithuanian Research Centre for Agriculture and Forestry": (
            15_712_000.0,
            2,
            "LTL",
            "2011 TAR.FE51590E2B56.docx",
        ),
        "National Cancer Institute": (
            3_904_000.0,
            2,
            "LTL",
            "2011 TAR.FE51590E2B56.docx",
        ),
        "Nature Research Center": (
            10_661_000.0,
            2,
            "LTL",
            "2011 TAR.FE51590E2B56.docx",
        ),
        "State Research Institute Center for Innovative Medicine": (
            5_886_000.0,
            2,
            "LTL",
            "2011 TAR.FE51590E2B56.docx",
        ),
    },
    2012: {
        "Lithuanian Research Council": (
            102_596_000.0,
            1,
            "LTL",
            "2012 TAR.B75745DE003E.docx",
        ),
        "Centre for Physical Sciences and Technology (Lithuania)": (
            18_577_000.0,
            1,
            "LTL",
            "2012 TAR.B75745DE003E.docx",
        ),
        "Institute of Lithuanian Literature and Folklore": (
            2_894_000.0,
            1,
            "LTL",
            "2012 TAR.B75745DE003E.docx",
        ),
        "Institute of the Lithuanian Language": (
            3_234_000.0,
            1,
            "LTL",
            "2012 TAR.B75745DE003E.docx",
        ),
        "Lithuanian Academy of Sciences": (
            8_460_000.0,
            1,
            "LTL",
            "2012 TAR.B75745DE003E.docx",
        ),
        "Lithuanian Culture Research Institute": (
            2_444_000.0,
            1,
            "LTL",
            "2012 TAR.B75745DE003E.docx",
        ),
        "Lithuanian Energy Institute": (
            13_705_000.0,
            1,
            "LTL",
            "2012 TAR.B75745DE003E.docx",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            6_007_000.0,
            1,
            "LTL",
            "2012 TAR.B75745DE003E.docx",
        ),
        "Lithuanian History Institute": (
            3_147_000.0,
            1,
            "LTL",
            "2012 TAR.B75745DE003E.docx",
        ),
        "Lithuanian Research Centre for Agriculture and Forestry": (
            16_802_000.0,
            1,
            "LTL",
            "2012 TAR.B75745DE003E.docx",
        ),
        "Nature Research Center": (
            11_429_000.0,
            1,
            "LTL",
            "2012 TAR.B75745DE003E.docx",
        ),
        "State Research Institute Center for Innovative Medicine": (
            8_149_000.0,
            1,
            "LTL",
            "2012 TAR.B75745DE003E.docx",
        ),
    },
    2013: {
        "Lithuanian Research Council": (
            101_257_000.0,
            1,
            "LTL",
            "2013 TAR.CABB5B7DAFB1.docx",
        ),
        "Centre for Physical Sciences and Technology (Lithuania)": (
            20_344_000.0,
            1,
            "LTL",
            "2013 TAR.CABB5B7DAFB1.docx",
        ),
        "Institute of Lithuanian Literature and Folklore": (
            3_162_000.0,
            1,
            "LTL",
            "2013 TAR.CABB5B7DAFB1.docx",
        ),
        "Institute of the Lithuanian Language": (
            3_489_000.0,
            1,
            "LTL",
            "2013 TAR.CABB5B7DAFB1.docx",
        ),
        "Lithuanian Academy of Sciences": (
            8_710_000.0,
            1,
            "LTL",
            "2013 TAR.CABB5B7DAFB1.docx",
        ),
        "Lithuanian Culture Research Institute": (
            2_317_000.0,
            1,
            "LTL",
            "2013 TAR.CABB5B7DAFB1.docx",
        ),
        "Lithuanian Energy Institute": (
            12_168_000.0,
            1,
            "LTL",
            "2013 TAR.CABB5B7DAFB1.docx",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            6_051_000.0,
            1,
            "LTL",
            "2013 TAR.CABB5B7DAFB1.docx",
        ),
        "Lithuanian History Institute": (
            3_276_000.0,
            1,
            "LTL",
            "2013 TAR.CABB5B7DAFB1.docx",
        ),
        "Lithuanian Research Centre for Agriculture and Forestry": (
            18_618_000.0,
            1,
            "LTL",
            "2013 TAR.CABB5B7DAFB1.docx",
        ),
        "Nature Research Center": (
            9_976_000.0,
            1,
            "LTL",
            "2013 TAR.CABB5B7DAFB1.docx",
        ),
        "State Research Institute Center for Innovative Medicine": (
            3_231_000.0,
            1,
            "LTL",
            "2013 TAR.CABB5B7DAFB1.docx",
        ),
    },
    2014: {
        "Lithuanian Research Council": (
            109_326_000.0,
            1,
            "LTL",
            "2014 TAIS_462848.docx",
        ),
        "Centre for Physical Sciences and Technology (Lithuania)": (
            22_769_000.0,
            1,
            "LTL",
            "2014 TAIS_462848.docx",
        ),
        "Institute of Lithuanian Literature and Folklore": (
            3_415_000.0,
            1,
            "LTL",
            "2014 TAIS_462848.docx",
        ),
        "Institute of the Lithuanian Language": (
            3_068_000.0,
            1,
            "LTL",
            "2014 TAIS_462848.docx",
        ),
        "Lithuanian Academy of Sciences": (
            9_143_000.0,
            1,
            "LTL",
            "2014 TAIS_462848.docx",
        ),
        "Lithuanian Culture Research Institute": (
            2_527_000.0,
            1,
            "LTL",
            "2014 TAIS_462848.docx",
        ),
        "Lithuanian Energy Institute": (
            12_063_000.0,
            1,
            "LTL",
            "2014 TAIS_462848.docx",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            6_260_000.0,
            1,
            "LTL",
            "2014 TAIS_462848.docx",
        ),
        "Lithuanian History Institute": (
            3_572_000.0,
            1,
            "LTL",
            "2014 TAIS_462848.docx",
        ),
        "Lithuanian Research Centre for Agriculture and Forestry": (
            20_049_000.0,
            1,
            "LTL",
            "2014 TAIS_462848.docx",
        ),
        "Nature Research Center": (
            10_451_000.0,
            1,
            "LTL",
            "2014 TAIS_462848.docx",
        ),
        "State Research Institute Center for Innovative Medicine": (
            3_501_000.0,
            1,
            "LTL",
            "2014 TAIS_462848.docx",
        ),
    },
    2015: {
        "Lithuanian Research Council": (
            23_549_554.0,
            1,
            "EUR",
            "2015 12-1408.docx",
        ),
        "Centre for Physical Sciences and Technology (Lithuania)": (
            7_145_012.0,
            1,
            "EUR",
            "2015 12-1408.docx",
        ),
        "Institute of Lithuanian Literature and Folklore": (
            1_067_917.0,
            1,
            "EUR",
            "2015 12-1408.docx",
        ),
        "Institute of the Lithuanian Language": (
            908_827.0,
            1,
            "EUR",
            "2015 12-1408.docx",
        ),
        "Lithuanian Academy of Sciences": (
            2_898_343.0,
            1,
            "EUR",
            "2015 12-1408.docx",
        ),
        "Lithuanian Culture Research Institute": (
            778_991.0,
            1,
            "EUR",
            "2015 12-1408.docx",
        ),
        "Lithuanian Energy Institute": (
            3_522_070.0,
            1,
            "EUR",
            "2015 12-1408.docx",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            1_872_798.0,
            1,
            "EUR",
            "2015 12-1408.docx",
        ),
        "Lithuanian History Institute": (
            1_138_699.0,
            1,
            "EUR",
            "2015 12-1408.docx",
        ),
        "Lithuanian Research Centre for Agriculture and Forestry": (
            6_033_683.0,
            1,
            "EUR",
            "2015 12-1408.docx",
        ),
        "National Cancer Institute": (
            1_023_343.0,
            1,
            "EUR",
            "2015 12-1408.docx",
        ),
        "Nature Research Center": (
            3_847_457.0,
            1,
            "EUR",
            "2015 12-1408.docx",
        ),
        "State Research Institute Center for Innovative Medicine": (
            1_143_622.0,
            1,
            "EUR",
            "2015 12-1408.docx",
        ),
    },
    2016: {
        "Lithuanian Research Council": (
            18_639_000.0,
            1,
            "EUR",
            "2016 12-2161.docx",
        ),
        "Centre for Physical Sciences and Technology (Lithuania)": (
            7_716_000.0,
            1,
            "EUR",
            "2016 12-2161.docx",
        ),
        "Institute of Lithuanian Literature and Folklore": (
            1_085_000.0,
            1,
            "EUR",
            "2016 12-2161.docx",
        ),
        "Institute of the Lithuanian Language": (
            1_007_000.0,
            1,
            "EUR",
            "2016 12-2161.docx",
        ),
        "Lithuanian Academy of Sciences": (
            3_103_000.0,
            1,
            "EUR",
            "2016 12-2161.docx",
        ),
        "Lithuanian Culture Research Institute": (
            761_000.0,
            1,
            "EUR",
            "2016 12-2161.docx",
        ),
        "Lithuanian Energy Institute": (
            3_545_000.0,
            1,
            "EUR",
            "2016 12-2161.docx",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            2_165_000.0,
            1,
            "EUR",
            "2016 12-2161.docx",
        ),
        "Lithuanian History Institute": (
            1_304_000.0,
            1,
            "EUR",
            "2016 12-2161.docx",
        ),
        "Lithuanian Research Centre for Agriculture and Forestry": (
            6_675_000.0,
            1,
            "EUR",
            "2016 12-2161.docx",
        ),
        "National Cancer Institute": (
            1_419_000.0,
            1,
            "EUR",
            "2016 12-2161.docx",
        ),
        "Nature Research Center": (
            3_840_000.0,
            1,
            "EUR",
            "2016 12-2161.docx",
        ),
        "State Research Institute Center for Innovative Medicine": (
            1_241_000.0,
            1,
            "EUR",
            "2016 12-2161.docx",
        ),
    },
    2017: {
        "Lithuanian Research Council": (
            18_641_000.0,
            2,
            "EUR",
            "2017 XIII-177.docx",
        ),
        "Centre for Physical Sciences and Technology (Lithuania)": (
            12_321_000.0,
            2,
            "EUR",
            "2017 XIII-177.docx",
        ),
        "Institute of Lithuanian Literature and Folklore": (
            1_141_000.0,
            2,
            "EUR",
            "2017 XIII-177.docx",
        ),
        "Institute of the Lithuanian Language": (
            1_054_000.0,
            2,
            "EUR",
            "2017 XIII-177.docx",
        ),
        "Lithuanian Academy of Sciences": (
            4_306_000.0,
            2,
            "EUR",
            "2017 XIII-177.docx",
        ),
        "Lithuanian Culture Research Institute": (
            801_000.0,
            2,
            "EUR",
            "2017 XIII-177.docx",
        ),
        "Lithuanian Energy Institute": (
            3_466_000.0,
            2,
            "EUR",
            "2017 XIII-177.docx",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            2_266_000.0,
            1,
            "EUR",
            "2017 XIII-177.docx",
        ),
        "Lithuanian History Institute": (
            1_574_000.0,
            1,
            "EUR",
            "2017 XIII-177.docx",
        ),
        "Lithuanian Research Centre for Agriculture and Forestry": (
            7_074_000.0,
            2,
            "EUR",
            "2017 XIII-177.docx",
        ),
        "National Cancer Institute": (
            3_839_000.0,
            2,
            "EUR",
            "2017 XIII-177.docx",
        ),
        "Nature Research Center": (
            4_569_000.0,
            2,
            "EUR",
            "2017 XIII-177.docx",
        ),
        "State Research Institute Center for Innovative Medicine": (
            1_420_000.0,
            2,
            "EUR",
            "2017 XIII-177.docx",
        ),
    },
    2018: {
        "Lithuanian Research Council": (
            18_913_000.0,
            2,
            "EUR",
            "2018 XIII-868.docx",
        ),
        "Centre for Physical Sciences and Technology (Lithuania)": (
            10_966_000.0,
            2,
            "EUR",
            "2018 XIII-868.docx",
        ),
        "Institute of Lithuanian Literature and Folklore": (
            1_213_000.0,
            2,
            "EUR",
            "2018 XIII-868.docx",
        ),
        "Institute of the Lithuanian Language": (
            1_078_000.0,
            2,
            "EUR",
            "2018 XIII-868.docx",
        ),
        "Lithuanian Academy of Sciences": (
            4_099_000.0,
            2,
            "EUR",
            "2018 XIII-868.docx",
        ),
        "Lithuanian Culture Research Institute": (
            884_000.0,
            2,
            "EUR",
            "2018 XIII-868.docx",
        ),
        "Lithuanian Energy Institute": (
            3_702_000.0,
            2,
            "EUR",
            "2018 XIII-868.docx",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            2_540_000.0,
            1,
            "EUR",
            "2018 XIII-868.docx",
        ),
        "Lithuanian History Institute": (
            1_620_000.0,
            2,
            "EUR",
            "2018 XIII-868.docx",
        ),
        "Lithuanian Research Centre for Agriculture and Forestry": (
            7_544_000.0,
            2,
            "EUR",
            "2018 XIII-868.docx",
        ),
        "National Cancer Institute": (
            3_775_000.0,
            2,
            "EUR",
            "2018 XIII-868.docx",
        ),
        "Nature Research Center": (
            4_321_000.0,
            2,
            "EUR",
            "2018 XIII-868.docx",
        ),
        "State Research Institute Center for Innovative Medicine": (
            1_215_000.0,
            2,
            "EUR",
            "2018 XIII-868.docx",
        ),
    },
    2019: {
        "Lithuanian Research Council": (
            19_125_000.0,
            1,
            "EUR",
            "2019 XIII-1710.docx",
        ),
        "Centre for Physical Sciences and Technology (Lithuania)": (
            10_347_000.0,
            2,
            "EUR",
            "2019 XIII-1710.docx",
        ),
        "Institute of Lithuanian Literature and Folklore": (
            1_485_000.0,
            2,
            "EUR",
            "2019 XIII-1710.docx",
        ),
        "Institute of the Lithuanian Language": (
            1_167_000.0,
            2,
            "EUR",
            "2019 XIII-1710.docx",
        ),
        "Lithuanian Academy of Sciences": (
            4_395_000.0,
            2,
            "EUR",
            "2019 XIII-1710.docx",
        ),
        "Lithuanian Culture Research Institute": (
            1_245_000.0,
            2,
            "EUR",
            "2019 XIII-1710.docx",
        ),
        "Lithuanian Energy Institute": (
            4_782_000.0,
            2,
            "EUR",
            "2019 XIII-1710.docx",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            2_688_000.0,
            2,
            "EUR",
            "2019 XIII-1710.docx",
        ),
        "Lithuanian History Institute": (
            2_169_000.0,
            2,
            "EUR",
            "2019 XIII-1710.docx",
        ),
        "Lithuanian Research Centre for Agriculture and Forestry": (
            8_062_000.0,
            2,
            "EUR",
            "2019 XIII-1710.docx",
        ),
        "National Cancer Institute": (
            2_363_000.0,
            2,
            "EUR",
            "2019 XIII-1710.docx",
        ),
        "Nature Research Center": (
            4_487_000.0,
            2,
            "EUR",
            "2019 XIII-1710.docx",
        ),
        "State Research Institute Center for Innovative Medicine": (
            1_284_000.0,
            2,
            "EUR",
            "2019 XIII-1710.docx",
        ),
    },
    2020: {
        "Lithuanian Research Council": (
            22_270_000.0,
            1,
            "EUR",
            "2020 XIII-2695.docx",
        ),
        "Centre for Physical Sciences and Technology (Lithuania)": (
            11_587_000.0,
            2,
            "EUR",
            "2020 XIII-2695.docx",
        ),
        "Institute of Lithuanian Literature and Folklore": (
            1_682_000.0,
            2,
            "EUR",
            "2020 XIII-2695.docx",
        ),
        "Institute of the Lithuanian Language": (
            1_349_000.0,
            2,
            "EUR",
            "2020 XIII-2695.docx",
        ),
        "Lithuanian Academy of Sciences": (
            4_461_000.0,
            2,
            "EUR",
            "2020 XIII-2695.docx",
        ),
        "Lithuanian Culture Research Institute": (
            1_613_000.0,
            2,
            "EUR",
            "2020 XIII-2695.docx",
        ),
        "Lithuanian Energy Institute": (
            5_156_000.0,
            2,
            "EUR",
            "2020 XIII-2695.docx",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            2_895_000.0,
            2,
            "EUR",
            "2020 XIII-2695.docx",
        ),
        "Lithuanian History Institute": (
            2_571_000.0,
            2,
            "EUR",
            "2020 XIII-2695.docx",
        ),
        "Lithuanian Research Centre for Agriculture and Forestry": (
            8_172_000.0,
            2,
            "EUR",
            "2020 XIII-2695.docx",
        ),
        "National Cancer Institute": (
            1_640_000.0,
            2,
            "EUR",
            "2020 XIII-2695.docx",
        ),
        "Nature Research Center": (
            4_977_000.0,
            2,
            "EUR",
            "2020 XIII-2695.docx",
        ),
        "State Research Institute Center for Innovative Medicine": (
            1_349_000.0,
            2,
            "EUR",
            "2020 XIII-2695.docx",
        ),
    },
    2021: {
        "Centre for Physical Sciences and Technology (Lithuania)": (
            11_584_000.0,
            22,
            "EUR",
            "2021 AR_2021-07-01.pdf",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            3_208_000.0,
            22,
            "EUR",
            "2021 AR_2021-07-01.pdf",
        ),
        "Lithuanian Research Centre for Agriculture and Forestry": (
            6_152_000.0,
            22,
            "EUR",
            "2021 AR_2021-07-01.pdf",
        ),
        "Lithuanian Energy Institute": (
            4_913_000.0,
            22,
            "EUR",
            "2021 AR_2021-07-01.pdf",
        ),
        "Nature Research Center": (
            5_337_000.0,
            22,
            "EUR",
            "2021 AR_2021-07-01.pdf",
        ),
        "Lithuanian Academy of Sciences": (
            5_628_000.0,
            22,
            "EUR",
            "2021 AR_2021-07-01.pdf",
        ),
        "Lithuanian Research Council": (
            22_329_000.0,
            23,
            "EUR",
            "2021 AR_2021-07-01.pdf",
        ),
        "National Cancer Institute": (
            1_715_000.0,
            22,
            "EUR",
            "2021 AR_2021-07-01.pdf",
        ),
        "Lithuanian History Institute": (
            2_936_000.0,
            22,
            "EUR",
            "2021 AR_2021-07-01.pdf",
        ),
        "Lithuanian Culture Research Institute": (
            1_857_000.0,
            22,
            "EUR",
            "2021 AR_2021-07-01.pdf",
        ),
        "Institute of Lithuanian Literature and Folklore": (
            1_933_000.0,
            22,
            "EUR",
            "2021 AR_2021-07-01.pdf",
        ),
        "Institute of the Lithuanian Language": (
            1_838_000.0,
            22,
            "EUR",
            "2021 AR_2021-07-01.pdf",
        ),
        "State Research Institute Center for Innovative Medicine": (
            1_538_000.0,
            22,
            "EUR",
            "2021 AR_2021-07-01.pdf",
        ),
    },
    2022: {
        "Lithuanian Research Council": (
            31_013_000.0,
            1,
            "EUR",
            "2022 XIV-745.docx",
        ),
        "Centre for Physical Sciences and Technology (Lithuania)": (
            12_845_000.0,
            1,
            "EUR",
            "2022 XIV-745.docx",
        ),
        "Institute of Lithuanian Literature and Folklore": (
            2_548_000.0,
            1,
            "EUR",
            "2022 XIV-745.docx",
        ),
        "Institute of the Lithuanian Language": (
            2_000_000.0,
            1,
            "EUR",
            "2022 XIV-745.docx",
        ),
        "Lithuanian Academy of Sciences": (
            5_882_000.0,
            1,
            "EUR",
            "2022 XIV-745.docx",
        ),
        "Lithuanian Culture Research Institute": (
            2_305_000.0,
            1,
            "EUR",
            "2022 XIV-745.docx",
        ),
        "Lithuanian Energy Institute": (
            5_830_000.0,
            1,
            "EUR",
            "2022 XIV-745.docx",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            3_766_000.0,
            1,
            "EUR",
            "2022 XIV-745.docx",
        ),
        "Lithuanian History Institute": (
            3_508_000.0,
            1,
            "EUR",
            "2022 XIV-745.docx",
        ),
        "Lithuanian Research Centre for Agriculture and Forestry": (
            6_110_000.0,
            1,
            "EUR",
            "2022 XIV-745.docx",
        ),
        "National Cancer Institute": (
            2_425_000.0,
            1,
            "EUR",
            "2022 XIV-745.docx",
        ),
        "Nature Research Center": (
            6_796_000.0,
            1,
            "EUR",
            "2022 XIV-745.docx",
        ),
        "State Research Institute Center for Innovative Medicine": (
            1_782_000.0,
            1,
            "EUR",
            "2022 XIV-745.docx",
        ),
    },
    2023: {
        "Lithuanian Research Council": (
            50_065_000.0,
            1,
            "EUR",
            "2023 XIV-1556.docx",
        ),
        "Centre for Physical Sciences and Technology (Lithuania)": (
            16_447_000.0,
            1,
            "EUR",
            "2023 XIV-1556.docx",
        ),
        "Institute of Lithuanian Literature and Folklore": (
            3_363_000.0,
            1,
            "EUR",
            "2023 XIV-1556.docx",
        ),
        "Institute of the Lithuanian Language": (
            2_486_000.0,
            1,
            "EUR",
            "2023 XIV-1556.docx",
        ),
        "Lithuanian Academy of Sciences": (
            9_701_000.0,
            1,
            "EUR",
            "2023 XIV-1556.docx",
        ),
        "Lithuanian Culture Research Institute": (
            2_783_000.0,
            1,
            "EUR",
            "2023 XIV-1556.docx",
        ),
        "Lithuanian Energy Institute": (
            7_106_000.0,
            1,
            "EUR",
            "2023 XIV-1556.docx",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            4_086_000.0,
            1,
            "EUR",
            "2023 XIV-1556.docx",
        ),
        "Lithuanian History Institute": (
            4_400_000.0,
            1,
            "EUR",
            "2023 XIV-1556.docx",
        ),
        "Lithuanian Research Centre for Agriculture and Forestry": (
            7_655_000.0,
            1,
            "EUR",
            "2023 XIV-1556.docx",
        ),
        "National Cancer Institute": (
            2_517_000.0,
            1,
            "EUR",
            "2023 XIV-1556.docx",
        ),
        "Nature Research Center": (
            8_984_000.0,
            1,
            "EUR",
            "2023 XIV-1556.docx",
        ),
        "State Research Institute Center for Innovative Medicine": (
            2_335_000.0,
            1,
            "EUR",
            "2023 XIV-1556.docx",
        ),
    },
    2024: {
        "Lithuanian Research Council": (
            59_870_000.0,
            1,
            "EUR",
            "2024 XIV-2297.docx",
        ),
        "Centre for Physical Sciences and Technology (Lithuania)": (
            18_909_000.0,
            1,
            "EUR",
            "2024 XIV-2297.docx",
        ),
        "Institute of Lithuanian Literature and Folklore": (
            3_765_000.0,
            1,
            "EUR",
            "2024 XIV-2297.docx",
        ),
        "Institute of the Lithuanian Language": (
            2_960_000.0,
            1,
            "EUR",
            "2024 XIV-2297.docx",
        ),
        "Lithuanian Academy of Sciences": (
            10_992_000.0,
            1,
            "EUR",
            "2024 XIV-2297.docx",
        ),
        "Lithuanian Culture Research Institute": (
            3_222_000.0,
            1,
            "EUR",
            "2024 XIV-2297.docx",
        ),
        "Lithuanian Energy Institute": (
            7_821_000.0,
            1,
            "EUR",
            "2024 XIV-2297.docx",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            4_734_000.0,
            1,
            "EUR",
            "2024 XIV-2297.docx",
        ),
        "Lithuanian History Institute": (
            5_463_000.0,
            1,
            "EUR",
            "2024 XIV-2297.docx",
        ),
        "Lithuanian Research Centre for Agriculture and Forestry": (
            9_213_000.0,
            1,
            "EUR",
            "2024 XIV-2297.docx",
        ),
        "National Cancer Institute": (
            1_807_000.0,
            1,
            "EUR",
            "2024 XIV-2297.docx",
        ),
        "Nature Research Center": (
            10_134_000.0,
            1,
            "EUR",
            "2024 XIV-2297.docx",
        ),
        "State Research Institute Center for Innovative Medicine": (
            2_667_000.0,
            1,
            "EUR",
            "2024 XIV-2297.docx",
        ),
    },
    2025: {
        "Lithuanian Research Council": (
            67_564_000.0,
            2,
            "EUR",
            "2025 XV-89.docx",
        ),
        "Centre for Physical Sciences and Technology (Lithuania)": (
            21_975_000.0,
            2,
            "EUR",
            "2025 XV-89.docx",
        ),
        "Institute of Lithuanian Literature and Folklore": (
            4_539_000.0,
            2,
            "EUR",
            "2025 XV-89.docx",
        ),
        "Institute of the Lithuanian Language": (
            3_669_000.0,
            2,
            "EUR",
            "2025 XV-89.docx",
        ),
        "Lithuanian Academy of Sciences": (
            8_493_000.0,
            2,
            "EUR",
            "2025 XV-89.docx",
        ),
        "Lithuanian Culture Research Institute": (
            3_911_000.0,
            2,
            "EUR",
            "2025 XV-89.docx",
        ),
        "Lithuanian Energy Institute": (
            8_866_000.0,
            2,
            "EUR",
            "2025 XV-89.docx",
        ),
        "Lithuanian Genocide and Resistance Research Centre": (
            4_923_000.0,
            2,
            "EUR",
            "2025 XV-89.docx",
        ),
        "Lithuanian History Institute": (
            6_515_000.0,
            2,
            "EUR",
            "2025 XV-89.docx",
        ),
        "Lithuanian Research Centre for Agriculture and Forestry": (
            11_227_000.0,
            2,
            "EUR",
            "2025 XV-89.docx",
        ),
        "National Cancer Institute": (
            3_110_000.0,
            2,
            "EUR",
            "2025 XV-89.docx",
        ),
        "Nature Research Center": (
            11_966_000.0,
            2,
            "EUR",
            "2025 XV-89.docx",
        ),
        "State Research Institute Center for Innovative Medicine": (
            3_883_000.0,
            2,
            "EUR",
            "2025 XV-89.docx",
        ),
    },
}
_LITHUANIA_VERIFIED_DROPS: set[tuple[int, str]] = {
    # 1993 is documented in thousand talonas, but this service row remains
    # methodologically incomparable to the post-stabilization series and
    # visually dominates all later years. Keep the trace in source audit, but
    # exclude it from the final panel.
    (1993, "State Science, Studies and Technology Service (Lithuania)"),
}
_HUNGARY_VERIFIED_OVERRIDES: dict[int, dict[str, tuple[float, int | None, str, str]]] = {
    1992: {
        "Hungarian Academy of Sciences (MTA)": (
            6_860_900_000.0,
            65,
            "HUF",
            "1992 1991. évi XCI. törvény.pdf",
        ),
    },
    1993: {
        "Hungarian Academy of Sciences (MTA)": (
            6_745_600_000.0,
            None,
            "HUF",
            "1993 1992. évi LXXX. törvény.pdf",
        ),
    },
    1994: {
        "Hungarian Academy of Sciences (MTA)": (
            6_598_300_000.0,
            None,
            "HUF",
            "1994 1993. évi CXI. törvény.pdf",
        ),
    },
    1995: {
        "Hungarian Academy of Sciences (MTA)": (
            10_081_500_000.0,
            None,
            "HUF",
            "1995 1994. évi CIV. törvény.pdf",
        ),
    },
    1996: {
        "Hungarian Academy of Sciences (MTA)": (
            10_933_700_000.0,
            None,
            "HUF",
            "1996 1995. évi CXXI. törvény.pdf",
        ),
    },
    1997: {
        "Hungarian Academy of Sciences (MTA)": (
            13_431_600_000.0,
            132,
            "HUF",
            "1997 1996. évi CXXIV. törvény.pdf",
        ),
    },
    1998: {
        "Hungarian Academy of Sciences (MTA)": (
            16_123_600_000.0,
            143,
            "HUF",
            "1998 1997. évi CXLVI. törvény.pdf",
        ),
    },
    1999: {
        "Hungarian Academy of Sciences (MTA)": (
            26_383_000_000.0,
            None,
            "HUF",
            "1999 (1998). évi XC. törvény.pdf",
        ),
    },
    2001: {
        "Hungarian Academy of Sciences (MTA)": (
            33_159_400_000.0,
            None,
            "HUF",
            "2001-2002  2000. évi CXXXIII. törvény.pdf",
        ),
    },
    2005: {
        "Hungarian Academy of Sciences (MTA)": (
            47_370_000_000.0,
            None,
            "HUF",
            "2005 2004. évi CXXXV. törvény.pdf",
        ),
    },
    2007: {
        "Hungarian Academy of Sciences (MTA)": (
            50_063_000_000.0,
            None,
            "HUF",
            "2007  2006. évi CXXVII. törvény.pdf",
        ),
    },
    2010: {
        "Hungarian Academy of Sciences (MTA)": (
            48_372_800_000.0,
            None,
            "HUF",
            "2010 MK_09_179.pdf",
        ),
        "Research and Technological Innovation Fund": (
            43_695_900_000.0,
            123,
            "HUF",
            "pdf_4e5ee92e2c1d__2010_MK_09_179.txt",
        ),
    },
    2011: {
        "Hungarian Academy of Sciences (MTA)": (
            51_505_800_000.0,
            None,
            "HUF",
            "2011 MK_10_200.pdf",
        ),
        "Research and Technological Innovation Fund": (
            45_977_900_000.0,
            0,
            "HUF",
            "pdf_fc70ccc08085__2011_MK_10_200.txt",
        ),
        "INTERREG IVC Programme": (
            2_777_000_000.0,
            0,
            "HUF",
            "pdf_fc70ccc08085__2011_MK_10_200.txt",
        ),
    },
    2012: {
        "Hungarian Academy of Sciences (MTA)": (
            57_483_100_000.0,
            None,
            "HUF",
            "2012 MK_161.pdf",
        ),
        "Research and Technological Innovation Fund": (
            45_200_000_000.0,
            0,
            "HUF",
            "pdf_ec2d2781b7dc__2012_MK_161.txt",
        ),
        "INTERREG IVC Programme": (
            1_469_000_000.0,
            0,
            "HUF",
            "pdf_ec2d2781b7dc__2012_MK_161.txt",
        ),
    },
    2013: {
        "Hungarian Academy of Sciences (MTA)": (
            59_109_500_000.0,
            None,
            "HUF",
            "2013 MK_12_172.pdf",
        ),
        "MTA Library and Information Centre": (
            1_426_000_000.0,
            0,
            "HUF",
            "pdf_809afb5be165__2013_MK_12_172.txt",
        ),
        "Research and Technological Innovation Fund": (
            37_664_500_000.0,
            0,
            "HUF",
            "pdf_809afb5be165__2013_MK_12_172.txt",
        ),
        "INTERREG IVC Programme": (
            5_417_000_000.0,
            0,
            "HUF",
            "pdf_809afb5be165__2013_MK_12_172.txt",
        ),
    },
    2014: {
        "Hungarian Academy of Sciences (MTA)": (
            63_203_500_000.0,
            None,
            "HUF",
            "2014 MK_13_216.pdf",
        ),
        "MTA Library and Information Centre": (
            1_426_000_000.0,
            0,
            "HUF",
            "pdf_5bc63b673739__2014_MK_13_216.txt",
        ),
        "Research and Technological Innovation Fund": (
            35_357_600_000.0,
            0,
            "HUF",
            "pdf_5bc63b673739__2014_MK_13_216.txt",
        ),
        "INTERREG IVC Programme": (
            1_503_000_000.0,
            0,
            "HUF",
            "pdf_5bc63b673739__2014_MK_13_216.txt",
        ),
    },
    2015: {
        "Hungarian Academy of Sciences (MTA)": (
            56_318_100_000.0,
            None,
            "HUF",
            "2015 MK_14_184.pdf",
        ),
        "MTA Library and Information Centre": (
            1_426_000_000.0,
            0,
            "HUF",
            "pdf_e3a7a139c79d__2015_MK_14_184.txt",
        ),
        "INTERREG IVC Programme": (
            1_357_900_000.0,
            187,
            "HUF",
            "pdf_e3a7a139c79d__2015_MK_14_184.txt",
        ),
        "National Research, Development and Innovation Fund (Hungary)": (
            77_951_600_000.0,
            187,
            "HUF",
            "pdf_e3a7a139c79d__2015_MK_14_184.txt",
        ),
    },
    2016: {
        "Hungarian Academy of Sciences (MTA)": (
            56_194_800_000.0,
            152,
            "HUF",
            "2016 MK_15_097.pdf",
        ),
        "MTA Library and Information Centre": (
            1_426_000_000.0,
            0,
            "HUF",
            "pdf_a5ef93808955__2016_MK_15_097.txt",
        ),
        "National Research, Development and Innovation Fund (Hungary)": (
            79_451_600_000.0,
            0,
            "HUF",
            "pdf_a5ef93808955__2016_MK_15_097.txt",
        ),
    },
    2017: {
        "Hungarian Academy of Sciences (MTA)": (
            68_945_500_000.0,
            91,
            "HUF",
            "2017 MK_16_091.pdf",
        ),
        "MTA Library and Information Centre": (
            1_426_000_000.0,
            0,
            "HUF",
            "pdf_8de59207bc3d__2017_MK_16_091.txt",
        ),
        "National Research, Development and Innovation Fund (Hungary)": (
            78_915_000_000.0,
            90,
            "HUF",
            "pdf_8de59207bc3d__2017_MK_16_091.txt",
        ),
    },
    2018: {
        "Hungarian Academy of Sciences (MTA)": (
            62_611_700_000.0,
            100,
            "HUF",
            "2018 MK_17_100.pdf",
        ),
        "MTA Library and Information Centre": (
            1_950_000_000.0,
            0,
            "HUF",
            "pdf_3efc4efd2e1c__2018_MK_17_100.txt",
        ),
        "National Research, Development and Innovation Fund (Hungary)": (
            82_073_000_000.0,
            155,
            "HUF",
            "pdf_3efc4efd2e1c__2018_MK_17_100.txt",
        ),
    },
    2019: {
        "Hungarian Academy of Sciences (MTA)": (
            56_146_500_000.0,
            84,
            "HUF",
            "2019 MK_18_123.pdf",
        ),
        "MTA Library and Information Centre": (
            1_950_000_000.0,
            85,
            "HUF",
            "pdf_17c40a8f2c21__2019_MK_18_123.txt",
        ),
        "National Research, Development and Innovation Fund (Hungary)": (
            78_587_000_000.0,
            85,
            "HUF",
            "pdf_17c40a8f2c21__2019_MK_18_123.txt",
        ),
    },
    2020: {
        "Hungarian Academy of Sciences (MTA)": (
            17_184_900_000.0,
            59,
            "HUF",
            "2020 MK_19_128.pdf",
        ),
        "MTA Library and Information Centre": (
            1_950_000_000.0,
            0,
            "HUF",
            "pdf_779604bc932b__2020_MK_19_128.txt",
        ),
        "Eötvös Loránd Research Network": (
            48_228_800_000.0,
            78,
            "HUF",
            "pdf_779604bc932b__2020_MK_19_128.txt",
        ),
        "National Research, Development and Innovation Fund (Hungary)": (
            137_812_200_000.0,
            71,
            "HUF",
            "pdf_779604bc932b__2020_MK_19_128.txt",
        ),
    },
    2021: {
        "Hungarian Academy of Sciences (MTA)": (
            17_134_400_000.0,
            40,
            "HUF",
            "2021 MK_20_170.pdf",
        ),
        "MTA Library and Information Centre": (
            1_950_000_000.0,
            0,
            "HUF",
            "pdf_c349f811a655__2021_MK_20_170.txt",
        ),
        "Eötvös Loránd Research Network": (
            70_666_400_000.0,
            59,
            "HUF",
            "pdf_c349f811a655__2021_MK_20_170.txt",
        ),
        "National Research, Development and Innovation Fund (Hungary)": (
            137_812_200_000.0,
            71,
            "HUF",
            "pdf_c349f811a655__2021_MK_20_170.txt",
        ),
    },
    2022: {
        "Hungarian Academy of Sciences (MTA)": (
            28_765_700_000.0,
            53,
            "HUF",
            "2022 MK_21_120.pdf",
        ),
        "MTA Library and Information Centre": (
            1_950_000_000.0,
            0,
            "HUF",
            "pdf_fe4092e4966b__2022_MK_21_120.txt",
        ),
        "Eötvös Loránd Research Network": (
            66_122_200_000.0,
            62,
            "HUF",
            "pdf_fe4092e4966b__2022_MK_21_120.txt",
        ),
        "National Research, Development and Innovation Fund (Hungary)": (
            143_946_900_000.0,
            73,
            "HUF",
            "pdf_fe4092e4966b__2022_MK_21_120.txt",
        ),
    },
    2023: {
        "Hungarian Academy of Sciences (MTA)": (
            30_566_400_000.0,
            74,
            "HUF",
            "2023 MK_22_127.pdf",
        ),
        "MTA Library and Information Centre": (
            1_950_000_000.0,
            0,
            "HUF",
            "pdf_8328edd9fb03__2023_MK_22_127.txt",
        ),
        "Eötvös Loránd Research Network": (
            67_001_700_000.0,
            125,
            "HUF",
            "pdf_8328edd9fb03__2023_MK_22_127.txt",
        ),
        "National Research, Development and Innovation Fund (Hungary)": (
            172_762_400_000.0,
            135,
            "HUF",
            "pdf_8328edd9fb03__2023_MK_22_127.txt",
        ),
    },
    2024: {
        "Hungarian Academy of Sciences (MTA)": (
            30_706_800_000.0,
            41,
            "HUF",
            "2024 MK_23_104.pdf",
        ),
        "MTA Library and Information Centre": (
            1_950_000_000.0,
            0,
            "HUF",
            "pdf_862d254bf1e1__2024_MK_23_104.txt",
        ),
        "Hungarian Research Network": (
            66_091_600_000.0,
            71,
            "HUF",
            "pdf_862d254bf1e1__2024_MK_23_104.txt",
        ),
        "National Research, Development and Innovation Fund (Hungary)": (
            154_118_800_000.0,
            83,
            "HUF",
            "pdf_862d254bf1e1__2024_MK_23_104.txt",
        ),
    },
    2025: {
        "Hungarian Academy of Sciences (MTA)": (
            37_063_700_000.0,
            43,
            "HUF",
            "2025 MK_24_136.pdf",
        ),
        "MTA Library and Information Centre": (
            1_950_000_000.0,
            0,
            "HUF",
            "pdf_ad532cad1ed4__2025_MK_24_136.txt",
        ),
        "Hungarian Research Network": (
            73_579_100_000.0,
            77,
            "HUF",
            "pdf_ad532cad1ed4__2025_MK_24_136.txt",
        ),
        "National Research, Development and Innovation Fund (Hungary)": (
            137_127_400_000.0,
            86,
            "HUF",
            "pdf_ad532cad1ed4__2025_MK_24_136.txt",
        ),
    },
}

_KOREA_VERIFIED_DROPS: set[tuple[int, str]] = {
    (2019, "Ministry of Science and ICT (Korea)"),
    (2021, "Ministry of Science and ICT (Korea)"),
    (2022, "Ministry of Science and ICT (Korea)"),
    (2018, "Strategic Technology R&D Programmes (Korea)"),
    (2019, "Strategic Technology R&D Programmes (Korea)"),
    (2021, "Strategic Technology R&D Programmes (Korea)"),
    (2024, "Strategic Technology R&D Programmes (Korea)"),
}

_KOREA_VERIFIED_OVERRIDES: dict[int, dict[str, tuple[float, int, str, str]]] = {
    2018: {
        "National R&D Programmes (Korea)": (
            19_633_800_000.0,
            55,
            "KRW",
            "2018년도 예산안 개요.pdf",
        ),
        "Strategic Technology R&D Programmes (Korea)": (
            1_400_000_000.0,
            28,
            "KRW",
            "2018년도 예산안 개요.pdf",
        ),
    },
    2019: {
        "National R&D Programmes (Korea)": (
            20_400_000_000.0,
            6,
            "KRW",
            "2019년도 예산안 개요.pdf",
        ),
        "Strategic Technology R&D Programmes (Korea)": (
            1_513_700_000.0,
            27,
            "KRW",
            "2019년도 예산안 개요.pdf",
        ),
    },
    2021: {
        "National R&D Programmes (Korea)": (
            27_400_000_000.0,
            17,
            "KRW",
            "7. 2021-2025년 국가재정운용계획 주요내용.pdf",
        ),
    },
    2022: {
        "Ministry of Science and ICT (Korea)": (
            9_626_200_000.0,
            101,
            "KRW",
            "3. 2022년 예산안.pdf",
        ),
        "National R&D Programmes (Korea)": (
            29_800_000_000.0,
            8,
            "KRW",
            "3. 2022년 예산안.pdf",
        ),
        "Strategic Technology R&D Programmes (Korea)": (
            6_200_000_000.0,
            49,
            "KRW",
            "3. 2022년 예산안.pdf",
        ),
    },
    2023: {
        "Ministry of Science and ICT (Korea)": (
            9_977_500_000.0,
            60,
            "KRW",
            "2. 2023년 예산안 홍보자료★.pdf",
        ),
        "National R&D Programmes (Korea)": (
            30_700_000_000.0,
            9,
            "KRW",
            "2. 2023년 예산안 홍보자료★.pdf",
        ),
        "Strategic Technology R&D Programmes (Korea)": (
            4_512_300_000.0,
            33,
            "KRW",
            "2. 2023년 예산안 홍보자료★.pdf",
        ),
    },
    2024: {
        "Ministry of Science and ICT (Korea)": (
            9_076_800_000.0,
            55,
            "KRW",
            "2. 2024년  예산안 홍보자료.pdf",
        ),
        "National R&D Programmes (Korea)": (
            25_900_000_000.0,
            7,
            "KRW",
            "2. 2024년  예산안 홍보자료.pdf",
        ),
        "Strategic Technology R&D Programmes (Korea)": (
            5_000_000_000.0,
            19,
            "KRW",
            "2. 2024년  예산안 홍보자료.pdf",
        ),
    },
    2025: {
        "Ministry of Science and ICT (Korea)": (
            10_911_200_000.0,
            51,
            "KRW",
            "2. 2025 예산안 홍보자료.pdf",
        ),
        "National R&D Programmes (Korea)": (
            29_700_000_000.0,
            6,
            "KRW",
            "2. 2025 예산안 홍보자료.pdf",
        ),
        "Strategic Technology R&D Programmes (Korea)": (
            3_544_600_000.0,
            20,
            "KRW",
            "2. 2025 예산안 홍보자료.pdf",
        ),
    },
}

_ISRAEL_VERIFIED_DROPS: set[tuple[int, str]] = {
    # 2013 space row surviving today is pulled from the 2014 half of the
    # biannual budget file. Do not keep it as a 2013 observation.
    (2013, "Israeli Space Agency (סוכנות החלל הישראלית)"),
    # 2019 ministry match currently comes from a noisy OCR summary page rather
    # than a clean ministry table.
    (2019, "Ministry of Science and Technology (Israel)"),
}

_ISRAEL_VERIFIED_OVERRIDES: dict[int, dict[str, tuple[float, int, str, str]]] = {
    2011: {
        "Ministry of Science and Technology (Israel)": (
            1_400_000_000.0,
            200,
            "ILS",
            "2011-2012_Israel.pdf",
        ),
    },
    1980: {
        "National Council for R&D (Israel, pre-1992)": (
            604_700_000_000.0,
            97,
            "ILS_OLD",
            "1980_Israel.pdf",
        ),
    },
    1981: {
        "National Council for R&D (Israel, pre-1992)": (
            1_000_000_000_000.0,
            27,
            "ILS_OLD",
            "1981_Israel.pdf",
        ),
    },
    1982: {
        "National Council for R&D (Israel, pre-1992)": (
            749_000_000_000.0,
            102,
            "ILS_OLD",
            "1982_Israel.pdf",
        ),
    },
    1983: {
        "National Council for R&D (Israel, pre-1992)": (
            156_300_000_000.0,
            11,
            "ILS_OLD",
            "1983_Israel.pdf",
        ),
    },
    1984: {
        "National Council for R&D (Israel, pre-1992)": (
            411_000_000_000.0,
            12,
            "ILS_OLD",
            "1984_Israel.pdf",
        ),
    },
    2013: {
        "Ministry of Science and Technology (Israel)": (
            1_117_322_000.0,
            28,
            "ILS",
            "2013-2014_Israel.pdf",
        ),
    },
    2015: {
        "Ministry of Science and Technology (Israel)": (
            1_361_253_000.0,
            31,
            "ILS",
            "2015-2016_Israel.pdf",
        ),
    },
    2025: {
        "Israel Innovation Authority (from 2016)": (
            2_008_207_000.0,
            48,
            "ILS",
            "2025_Israel.pdf",
        ),
    },
}

_KOREA_CANONICAL_NOTES: dict[str, str] = {
    "Ministry of Science and ICT (Korea)": "",
    "National R&D Programmes (Korea)": "Programmatic canonical for summary-style budget sources; not an institutional series.",
    "Strategic Technology R&D Programmes (Korea)": "Theme-level canonical for strategic R&D programmes in Korean budget briefs.",
}
_UK_GENERIC_DISCOVERED_PATTERNS = [
    re.compile(r"\bfund(?:ing)?\b", re.IGNORECASE),
    re.compile(r"\bprogramme\b|\bprogram\b", re.IGNORECASE),
    re.compile(r"\bscheme\b", re.IGNORECASE),
    re.compile(r"\bchallenge\b", re.IGNORECASE),
    re.compile(r"\binvestment\b", re.IGNORECASE),
    re.compile(r"\bsupport\b", re.IGNORECASE),
    re.compile(r"\bpackage\b", re.IGNORECASE),
    re.compile(r"\bmission\b", re.IGNORECASE),
]
_UK_MIN_AMOUNT_BY_CANONICAL = {
    "UKRI (UK Research and Innovation)": 100_000_000.0,
    "Industrial Strategy Challenge Fund": 10_000_000.0,
    "Research Infrastructure Projects": 100_000_000.0,
    "Research Partnership Investment Fund": 100_000_000.0,
    # Science Budget: any row below £200M is a wrong extraction
    # (the Science Budget covers all RCUK/UKRI spending, several billion GBP)
    "Science Budget": 200_000_000.0,
    # Core Research: UKRI core research is £4-5B; anything below £500M is noise
    "Core Research": 500_000_000.0,
    # Public R&D Investment: total public R&D announcements; below £100M is noise
    "Public R&D Investment": 100_000_000.0,
    # Research Councils (pre-UKRI): collective budget was several billion; below £200M is noise
    "Research Councils (pre-UKRI)": 200_000_000.0,
    # R&D Expenditure Credit: main scheme is >£100M; single-digit values are noise
    "R&D Expenditure Credit": 10_000_000.0,
}

# (canonical_name, year) pairs that cannot be correct regardless of extracted amount.
# ISCF (Industrial Strategy Challenge Fund) was created in November 2017 — any
# year before 2017 is a misclassification.
# Strength in Places Fund was announced in Budget 2019 — a 2016 row is impossible.
_UK_MANUAL_DROP_ROWS = (
    ("Industrial Strategy Challenge Fund", 2004),
    ("Industrial Strategy Challenge Fund", 2013),
    ("Strength in Places Fund", 2016),
)

_UK_POLICY_RECOVERY_SPECS = {
    "Advanced Research and Invention Agency": {
        "patterns": [r"advanced research and invention agency", r"\baria\b"],
        "min_amount": 100_000_000.0,
    },
    "Advanced Nuclear Fund": {
        "patterns": [r"advanced nuclear fund"],
        "min_amount": 100_000_000.0,
    },
    "Core Research": {
        "patterns": [r"of which:\s*core research", r"support for core research"],
    },
    "Exascale supercomputer and Artificial Intelligence Research Resource": {
        "patterns": [
            r"exascale supercomputer and artificial intelligence",
            r"exascale supercomputer and ai research resource",
        ],
        "min_amount": 100_000_000.0,
    },
    "Industrial Strategy Challenge Fund": {
        "patterns": [r"industrial strategy challenge fund"],
        "min_amount": 50_000_000.0,
    },
    "Innovate UK": {
        "patterns": [
            r"of which:\s*innovate uk",
            r"core innovate uk programmes",
            r"technology strategy board",
            r"\binnovate uk\b",
        ],
        "exclude": [r"growth catalyst", r"creative industries"],
        "min_amount": 50_000_000.0,
    },
    "National Productivity Investment Fund": {
        "patterns": [
            r"research and development funding",
            r"investment in r&d",
            r"additional spending in r&d",
            r"science and innovation",
            r"total investment for r&d from npif",
        ],
        "section_patterns": [r"national productivity investment fund"],
        "min_amount": 100_000_000.0,
    },
    "Public R&D Investment": {
        "patterns": [
            r"public r&d investment",
            r"total public r&d investment",
            r"total capital del expenditure on r&d",
        ],
    },
    "Quantum Technologies Development and Commercialisation Fund": {
        "patterns": [
            r"development and commercialisation of quantum technologies",
            r"investment for quantum technologies",
        ],
        "min_amount": 100_000_000.0,
    },
    "Research Infrastructure Projects": {
        "patterns": [r"science research infrastructure fund", r"research infrastructure projects"],
        "min_amount": 100_000_000.0,
    },
    "Research Partnership Investment Fund": {
        "patterns": [r"round of the research partnership investment fund", r"research partnership investment fund"],
        "min_amount": 100_000_000.0,
    },
    "Science Budget": {
        "patterns": [r"science budget allocation", r"dti science budget", r"science budget$"],
        "min_amount": 100_000_000.0,
    },
    "Strength in Places Fund": {
        "patterns": [r"strength in places fund"],
        "min_amount": 50_000_000.0,
    },
    "UK-wide R&D Funding for Low and Zero Emission Transport Technologies": {
        "patterns": [r"low and zero emission transport technologies"],
        "min_amount": 100_000_000.0,
    },
}

_GERMANY_ALLOWED_FINAL_CANONICALS = {
    "DFG (Deutsche Forschungsgemeinschaft)",
    "Helmholtz-Gemeinschaft (HGF)",
    "Fraunhofer-Gesellschaft",
    "Max-Planck-Gesellschaft (MPG)",
    "Leibniz-Gemeinschaft (WGL)",
    "German Aerospace Center",
    "PTB (Physikalisch-Technische Bundesanstalt)",
    "BAM (Bundesanstalt für Materialforschung)",
    "Alexander von Humboldt Foundation",
    "European Space Agency (ESA) Contribution",
    "German Electron Synchrotron",
    "Jülich Research Center",
    "GSI Helmholtz Centre for Heavy Ion Research",
    "Alfred Wegener Institute Helmholtz Centre for Polar and Marine Research",
    "Helmholtz Centre for Environmental Research (UFZ)",
    "German Research Centre for Geosciences (GFZ)",
    "Helmholtz Centre Munich",
    "Helmholtz Centre for Infection Research (HZI)",
    "Helmholtz Center Dresden-Rossendorf (HZDR)",
    "Helmholtz Centre for Ocean Research Kiel (GEOMAR)",
    # Additional large research organisations present in discovered agencies
    "German Cancer Research Center",
    "German Academic Exchange Service",
}

_GERMANY_MIN_AMOUNT_BY_CANONICAL = {
    "DFG (Deutsche Forschungsgemeinschaft)": 100_000_000.0,
    "Helmholtz-Gemeinschaft (HGF)": 100_000_000.0,
    "Fraunhofer-Gesellschaft": 100_000_000.0,
    "Max-Planck-Gesellschaft (MPG)": 100_000_000.0,
    "Leibniz-Gemeinschaft (WGL)": 100_000_000.0,
    "German Aerospace Center": 100_000_000.0,
    "European Space Agency (ESA) Contribution": 100_000_000.0,
    "PTB (Physikalisch-Technische Bundesanstalt)": 1_000_000.0,
    "BAM (Bundesanstalt für Materialforschung)": 1_000_000.0,
    "Alexander von Humboldt Foundation": 1_000_000.0,
    "German Electron Synchrotron": 10_000_000.0,
    "Jülich Research Center": 10_000_000.0,
    "GSI Helmholtz Centre for Heavy Ion Research": 10_000_000.0,
    "Alfred Wegener Institute Helmholtz Centre for Polar and Marine Research": 10_000_000.0,
    "Helmholtz Centre for Environmental Research (UFZ)": 10_000_000.0,
    "German Research Centre for Geosciences (GFZ)": 10_000_000.0,
    "Helmholtz Centre Munich": 50_000_000.0,
    "Helmholtz Centre for Infection Research (HZI)": 10_000_000.0,
    "Helmholtz Center Dresden-Rossendorf (HZDR)": 10_000_000.0,
    "Helmholtz Centre for Ocean Research Kiel (GEOMAR)": 10_000_000.0,
    "German Cancer Research Center": 10_000_000.0,
    "German Academic Exchange Service": 1_000_000.0,
}

_GERMANY_MAX_AMOUNT_BY_CANONICAL = {
    # Upper plausibility bounds (full EUR after unit expansion).
    # 2021 values from VA-Band3 are the best reference; these maxes are set
    # with ~20-30% headroom above the 2021 value to allow for future growth.
    # Big-5 federation-level institutions (federal block grant only):
    "DFG (Deutsche Forschungsgemeinschaft)": 3_500_000_000.0,   # 2021=2.85B, 2025=2.52B
    "Helmholtz-Gemeinschaft (HGF)": 3_500_000_000.0,             # 2021=2.77B
    "Fraunhofer-Gesellschaft":      1_500_000_000.0,              # 2021=0.78B (institutional only)
    "Max-Planck-Gesellschaft (MPG)": 2_500_000_000.0,            # 2021=1.17B
    "Leibniz-Gemeinschaft (WGL)":   1_500_000_000.0,              # 2021=0.62B
    "German Aerospace Center":      2_500_000_000.0,              # DLR ~1.5B in recent years
    # Large Helmholtz centres:
    "GSI Helmholtz Centre for Heavy Ion Research": 3_000_000_000.0,  # 2021=1.67B (FAIR project)
}

_GERMANY_HEADING_PATTERNS = re.compile(
    r"^(?:tgr\.\s*\d+\b|group\s+\d+\b|gruppe\s+\d+\b|title\s+group\s+\d+\b)",
    re.IGNORECASE,
)

_GERMANY_KEEP_LINE_PATTERNS = {
    # ── Big 5 science organisations — accept Bundeshaushalt "Tgr./Group N" format ──
    "DFG (Deutsche Forschungsgemeinschaft)": [
        r"institutional funding",
        r"german research foundation$",
        # Bundeshaushalt title-group prefix format (2003-2009 documents) — English
        r"tgr\.\s*\d+\s+german research foundation",
        r"group\s+\d+\s+german research foundation",
        # Bundeshaushalt title-group prefix format (2003-2009 documents) — German
        r"tgr\.\s*\d+\s+deutsche forschungsgemeinschaft",
        # Later explicit allocation descriptions (2022+)
        r"allocations for the german research foundation",
        r"zuwendungen.*deutsche forschungsgemeinschaft",
        # Bare standalone name or with legal suffix
        r"^german research foundation$",
        r"^deutsche forschungsgemeinschaft",
        r"deutsche forschungsgemeinschaft e\.?\s*v",
    ],
    "Helmholtz-Gemeinschaft (HGF)": [
        r"institutional grants to the helmholtz association",
        # Bundeshaushalt title-group format — English
        r"helmholtz association$",
        r"tgr\.\s*\d+\s+helmholtz association",
        r"group\s+\d+\s+helmholtz association",
        # Bundeshaushalt title-group format — German
        r"tgr\.\s*\d+\s+helmholtz-gemeinschaft",
        r"tgr\.\s*\d+\s+grossforschungseinrichtungen",
        r"tgr\.\s*\d+\s+großforschungseinrichtungen",
        r"^helmholtz-gemeinschaft",
        r"centers of the hermann von helmholtz association",
        r"hgf\b",
    ],
    "Fraunhofer-Gesellschaft": [
        r"institutional grants to the fraunhofer society",
        # Bundeshaushalt title-group format — English
        r"fraunhofer society$",
        r"tgr\.\s*\d+\s+fraunhofer",
        r"group\s+\d+\s+fraunhofer",
        r"^fraunhofer-gesellschaft",
        r"grant for the basic funding of the fraunhofer society",
        r"fraunhofer society for the promotion",
    ],
    "Leibniz-Gemeinschaft (WGL)": [
        r"grants to the leibniz association",
        # Bundeshaushalt title-group format — English
        r"leibniz association$",
        r"tgr\.\s*\d+\s+leibniz",
        r"group\s+\d+\s+leibniz",
        # Bundeshaushalt title-group format — German
        r"tgr\.\s*\d+\s+wissenschaftsgemeinschaft",
        r"^leibniz-gemeinschaft",
        r"blaue liste",
        r"gottfried wilhelm leibniz scientific community",
        r"gottfried wilhelm leibniz wissenschaftsgemeinschaft",
    ],
    "Max-Planck-Gesellschaft (MPG)": [
        r"total amount for the max planck society",
        # Bundeshaushalt title-group format — English
        r"max planck society$",
        r"tgr\.\s*\d+\s+max planck",
        r"group\s+\d+\s+max planck",
        # Bundeshaushalt title-group format — German
        r"tgr\.\s*\d+\s+max-planck",
        r"^max-planck-gesellschaft",
        r"^max planck society",
        r"mpg\b",
    ],
    # ── DLR (German Aerospace Center) ──────────────────────────────────────────
    "German Aerospace Center": [
        r"german aerospace center",
        r"deutsches zentrum für luft",
        r"deutsches zentrum fur luft",
        r"tgr\.\s*\d+\s+german aerospace center",
        r"group\s+\d+\s+german aerospace center",
        r"\bdlr\b",
        r"deutsche forschungsanstalt für luft",  # pre-1997 name (DFVLR)
    ],
    # ── Other institutional grants ─────────────────────────────────────────────
    "PTB (Physikalisch-Technische Bundesanstalt)": [
        r"gesamtausgaben",
        r"ptb in braunschweig und berlin",
        r"physikalisch-technische bundesanstalt",
    ],
    "BAM (Bundesanstalt für Materialforschung)": [
        r"total for bam",
        r"bundesanstalt für materialforschung",
    ],
    "Alexander von Humboldt Foundation": [
        r"alexander von humboldt-stiftung",
        r"alexander von humboldt foundation",
        r"operations and operational funds",
        r"research scholarships",
        r"research awards",
        r"avh\b",
    ],
    "German Electron Synchrotron": [
        r"institutional funding",
        r"stiftung deutsches elektronen-synchrotron",
        r"total for desy",
        r"german electron synchrotron",
        r"\bdesy\b",
    ],
    "Jülich Research Center": [
        r"total expenditures",
        r"jülich research center",
        r"forschungszentrum jülich",
        r"\bfzj\b",
    ],
    "Alfred Wegener Institute Helmholtz Centre for Polar and Marine Research": [
        r"total for awi",
        r"alfred wegener institute",
        r"meeresforschung",
        r"\bawi\b",
    ],
    "Helmholtz Centre for Environmental Research (UFZ)": [
        r"institutionelle förderung",
        r"helmholtz centre for environmental research",
        r"total for ufz",
        r"\bufz\b",
    ],
    "German Research Centre for Geosciences (GFZ)": [
        r"federal grants",
        r"total amount for research and development",
        r"german research centre for geosciences",
        r"\bgfz\b",
    ],
    "Helmholtz Centre Munich": [
        r"total amount for research and development",
        r"total expenditures",
        r"helmholtz centre munich",
        r"helmholtz zentrum münchen",
        r"\bhmgu\b",
    ],
    "Helmholtz Centre for Infection Research (HZI)": [
        r"helmholtz centre for infection research",
        r"helmholtz-zentrum für infektionsforschung",
        r"\bhzi\b",
    ],
    "Helmholtz Center Dresden-Rossendorf (HZDR)": [
        r"helmholtz center dresden-rossendorf",
        r"helmholtz-zentrum dresden-rossendorf",
        r"\bhzdr\b",
    ],
    "Helmholtz Centre for Ocean Research Kiel (GEOMAR)": [
        r"helmholtz center for ocean research kiel",
        r"helmholtz-zentrum für ozeanforschung kiel",
        r"\bgeomar\b",
    ],
    "European Space Agency (ESA) Contribution": [
        r"european space agency",
        r"\besa\b",
    ],
    "GSI Helmholtz Centre for Heavy Ion Research": [
        r"\bgsi\b",
        r"fair \(facility for antiproton and ion research\)",
        r"helmholtz-zentrum für schwerionenforschung",
    ],
}

_GERMANY_MANUAL_DROP_ROWS = (
    ("BAM (Bundesanstalt für Materialforschung)", 2003),
    ("DFG (Deutsche Forschungsgemeinschaft)", 2022),
    # -----------------------------------------------------------------------
    # Big-5 + DLR for Bundeshaushalt years 2003-2009: extraction is unreliable.
    # The LLM misassigned BMBF chapter totals (~1.93B EUR constant) to every
    # agency, and Titelgruppe rows contain sub-chapter sums, not single-agency
    # grants. 2021+ data from VA-Band3 is reliable and stands alone.
    # -----------------------------------------------------------------------
    ("DFG (Deutsche Forschungsgemeinschaft)", 2003),
    ("DFG (Deutsche Forschungsgemeinschaft)", 2004),
    ("DFG (Deutsche Forschungsgemeinschaft)", 2005),
    ("DFG (Deutsche Forschungsgemeinschaft)", 2006),
    ("DFG (Deutsche Forschungsgemeinschaft)", 2007),
    ("DFG (Deutsche Forschungsgemeinschaft)", 2008),
    ("DFG (Deutsche Forschungsgemeinschaft)", 2009),
    ("Helmholtz-Gemeinschaft (HGF)", 2003),
    ("Helmholtz-Gemeinschaft (HGF)", 2004),
    ("Helmholtz-Gemeinschaft (HGF)", 2005),
    ("Helmholtz-Gemeinschaft (HGF)", 2006),
    ("Helmholtz-Gemeinschaft (HGF)", 2007),
    ("Helmholtz-Gemeinschaft (HGF)", 2008),
    ("Helmholtz-Gemeinschaft (HGF)", 2009),
    ("Fraunhofer-Gesellschaft", 2003),
    ("Fraunhofer-Gesellschaft", 2004),
    ("Fraunhofer-Gesellschaft", 2005),
    ("Fraunhofer-Gesellschaft", 2006),
    ("Fraunhofer-Gesellschaft", 2007),
    ("Fraunhofer-Gesellschaft", 2008),
    ("Fraunhofer-Gesellschaft", 2009),
    ("Max-Planck-Gesellschaft (MPG)", 2003),
    ("Max-Planck-Gesellschaft (MPG)", 2004),
    ("Max-Planck-Gesellschaft (MPG)", 2005),
    ("Max-Planck-Gesellschaft (MPG)", 2006),
    ("Max-Planck-Gesellschaft (MPG)", 2007),
    ("Max-Planck-Gesellschaft (MPG)", 2008),
    ("Max-Planck-Gesellschaft (MPG)", 2009),
    ("Leibniz-Gemeinschaft (WGL)", 2003),
    ("Leibniz-Gemeinschaft (WGL)", 2004),
    ("Leibniz-Gemeinschaft (WGL)", 2005),
    ("Leibniz-Gemeinschaft (WGL)", 2006),
    ("Leibniz-Gemeinschaft (WGL)", 2007),
    ("Leibniz-Gemeinschaft (WGL)", 2008),
    ("Leibniz-Gemeinschaft (WGL)", 2009),
    ("German Aerospace Center", 2003),
    ("German Aerospace Center", 2004),
    ("German Aerospace Center", 2005),
    ("German Aerospace Center", 2006),
    ("German Aerospace Center", 2007),
    ("German Aerospace Center", 2008),
    ("German Aerospace Center", 2009),
    # PTB 1955: wrong year (amount in EUR for a 1955 row makes no sense)
    ("PTB (Physikalisch-Technische Bundesanstalt)", 1955),
)

_DENMARK_ALLOWED_FINAL_CANONICALS = {
    "Kobenhavns Universitet (KU)",
    "Aarhus Universitet (AU)",
    "Danmarks Tekniske Universitet (DTU)",
    "Aalborg Universitet (AAU)",
    "Syddansk Universitet (SDU)",
    "Roskilde Universitetscenter (RUC)",
    "Copenhagen Business School (CBS)",
    "Universiteterne (collective)",
    "Statens teknisk-videnskabelige Forskningsfond (STvF)",
    "Statens naturvidenskabelige Forskningsrad (SNF)",
    "Statens samfundsvidenskabelige Forskningsrad",
    "Statens humanistiske Forskningsrad",
    "Statens laegervidenskabelige Forskningsrad",
    "Det Strategiske Forskningsrad",
    "Det Frie Forskningsraad / Danmarks Frie Forskningsfond",
    "Danmarks Innovationsfond",
    "Hoejteknologifonden",
    "Danmarks Grundforskningsfond (DNRF)",
    "Atomenergikommissionen",
    "Riso Nationallaboratorium",
}

_DENMARK_VERIFIED_DROPS: set[tuple[int, str]] = {
    (1987, "Statens samfundsvidenskabelige Forskningsrad"),
    (1987, "Statens humanistiske Forskningsrad"),
    (1987, "Statens laegervidenskabelige Forskningsrad"),
    (1988, "Statens teknisk-videnskabelige Forskningsfond (STvF)"),
    (1988, "Atomenergikommissionen"),
    # 1985: LLM extracted tiny/wrong amounts; real values set via overrides (5 universities)
    (1985, "Aalborg Universitet (AAU)"),
    (1985, "Danmarks Tekniske Universitet (DTU)"),
    (1985, "Copenhagen Business School (CBS)"),
    (1985, "Syddansk Universitet (SDU)"),
    (1985, "Roskilde Universitetscenter (RUC)"),
    # 1986: LLM extracted wrong capital/partial rows; all 7 universities replaced by overrides
    (1986, "Kobenhavns Universitet (KU)"),
    (1986, "Aarhus Universitet (AU)"),
    (1986, "Aalborg Universitet (AAU)"),
    (1986, "Danmarks Tekniske Universitet (DTU)"),
    (1986, "Copenhagen Business School (CBS)"),
    (1986, "Syddansk Universitet (SDU)"),
    (1986, "Roskilde Universitetscenter (RUC)"),
    # 1992: sequential-digit hallucinations; no verified replacement available
    (1992, "Kobenhavns Universitet (KU)"),
    (1992, "Aarhus Universitet (AU)"),
    (1992, "Aalborg Universitet (AAU)"),
    (1992, "Danmarks Tekniske Universitet (DTU)"),
    (1992, "Copenhagen Business School (CBS)"),
    (1992, "Syddansk Universitet (SDU)"),
    (1992, "Roskilde Universitetscenter (RUC)"),
    # 1993: tiny fragment rows (reservation sub-lines), not main operating appropriations
    (1993, "Kobenhavns Universitet (KU)"),
    (1993, "Aarhus Universitet (AU)"),
    # 1994: sequential-digit hallucinations; no verified replacement available
    (1994, "Kobenhavns Universitet (KU)"),
    (1994, "Aarhus Universitet (AU)"),
    (1994, "Aalborg Universitet (AAU)"),
    (1994, "Danmarks Tekniske Universitet (DTU)"),
    (1994, "Copenhagen Business School (CBS)"),
    (1994, "Syddansk Universitet (SDU)"),
    (1994, "Roskilde Universitetscenter (RUC)"),
    # 1995: round-multiple hallucinations (universities/councils); Riso=negative artefact
    (1995, "Kobenhavns Universitet (KU)"),
    (1995, "Aarhus Universitet (AU)"),
    (1995, "Aalborg Universitet (AAU)"),
    (1995, "Danmarks Tekniske Universitet (DTU)"),
    (1995, "Copenhagen Business School (CBS)"),
    (1995, "Syddansk Universitet (SDU)"),
    (1995, "Roskilde Universitetscenter (RUC)"),
    (1995, "Statens teknisk-videnskabelige Forskningsfond (STvF)"),
    (1995, "Statens naturvidenskabelige Forskningsrad (SNF)"),
    (1995, "Statens humanistiske Forskningsrad"),
    (1995, "Statens laegervidenskabelige Forskningsrad"),
    (1995, "Riso Nationallaboratorium"),
    # 1997: tiny hallucinated sequential values; real values set via overrides
    (1997, "Statens teknisk-videnskabelige Forskningsfond (STvF)"),
    (1997, "Statens naturvidenskabelige Forskningsrad (SNF)"),
    (1997, "Statens samfundsvidenskabelige Forskningsrad"),
    (1997, "Statens humanistiske Forskningsrad"),
    (1997, "Statens laegervidenskabelige Forskningsrad"),
    # 2008: KU=1.5B and AU=1.2B from p.180 text notes (round caps); real Reservationsbev. set via overrides
    (2008, "Kobenhavns Universitet (KU)"),
    (2008, "Aarhus Universitet (AU)"),
    # 2009: wrong row selection (reservation/partial appropriations); replaced by overrides
    (2009, "Kobenhavns Universitet (KU)"),
    (2009, "Aarhus Universitet (AU)"),
    (2009, "Aalborg Universitet (AAU)"),
    (2009, "Danmarks Tekniske Universitet (DTU)"),
    (2009, "Copenhagen Business School (CBS)"),
    (2009, "Syddansk Universitet (SDU)"),
    (2009, "Roskilde Universitetscenter (RUC)"),
    # 2010: anonymous Basic Grant rows (~1/3 of real values); replaced by overrides
    (2010, "Kobenhavns Universitet (KU)"),
    (2010, "Aarhus Universitet (AU)"),
    (2010, "Aalborg Universitet (AAU)"),
    (2010, "Danmarks Tekniske Universitet (DTU)"),
    (2010, "Copenhagen Business School (CBS)"),
    (2010, "Syddansk Universitet (SDU)"),
    (2010, "Roskilde Universitetscenter (RUC)"),
    # 2013: round-number hallucinations; replaced by overrides
    (2013, "Kobenhavns Universitet (KU)"),
    (2013, "Aarhus Universitet (AU)"),
    (2013, "Aalborg Universitet (AAU)"),
    (2013, "Danmarks Tekniske Universitet (DTU)"),
    (2013, "Copenhagen Business School (CBS)"),
    (2013, "Syddansk Universitet (SDU)"),
    (2013, "Roskilde Universitetscenter (RUC)"),
    # 2015: unit bug (DKK millions stored as unit=thousand); replaced by corrected overrides
    (2015, "Kobenhavns Universitet (KU)"),
    (2015, "Aarhus Universitet (AU)"),
    (2015, "Aalborg Universitet (AAU)"),
    (2015, "Danmarks Tekniske Universitet (DTU)"),
    (2015, "Copenhagen Business School (CBS)"),
    (2015, "Syddansk Universitet (SDU)"),
    (2015, "Roskilde Universitetscenter (RUC)"),
    # 1979: CBS=4M and DTU=9M are tiny fragments (expected ~75M and ~200M); page 176
    (1979, "Copenhagen Business School (CBS)"),
    (1979, "Danmarks Tekniske Universitet (DTU)"),
    # 1993: SDU=345K is a 345-DKK sub-line fragment; expected ~240M range
    (1993, "Syddansk Universitet (SDU)"),
    # 1996: KU=785K, AU=49K, DTU=60K from page 112 — tiny reference/footnote amounts
    (1996, "Kobenhavns Universitet (KU)"),
    (1996, "Aarhus Universitet (AU)"),
    (1996, "Danmarks Tekniske Universitet (DTU)"),
    # 2000: CBS=1.5M and DTU=7M are partial grant sub-lines, not main operating appropriations
    (2000, "Copenhagen Business School (CBS)"),
    (2000, "Danmarks Tekniske Universitet (DTU)"),
    # 2005: LLM extracted tiny Reservationsbev. sub-lines (~1M each) not main appropriations
    (2005, "Kobenhavns Universitet (KU)"),
    (2005, "Aarhus Universitet (AU)"),
    (2005, "Aalborg Universitet (AAU)"),
    (2005, "Danmarks Tekniske Universitet (DTU)"),
    (2005, "Copenhagen Business School (CBS)"),
    (2005, "Syddansk Universitet (SDU)"),
    (2005, "Roskilde Universitetscenter (RUC)"),
    # 2014: multiple conflicting rows (NaN headers, round multiples, small grant fragments)
    # Real Selvejebev. totals set via overrides
    (2014, "Kobenhavns Universitet (KU)"),
    (2014, "Aarhus Universitet (AU)"),
    (2014, "Aalborg Universitet (AAU)"),
    (2014, "Danmarks Tekniske Universitet (DTU)"),
    (2014, "Copenhagen Business School (CBS)"),
    (2014, "Syddansk Universitet (SDU)"),
    (2014, "Roskilde Universitetscenter (RUC)"),
    # 2017: LLM extracted all zeros (table headers without amounts); real values set via overrides
    (2017, "Kobenhavns Universitet (KU)"),
    (2017, "Aarhus Universitet (AU)"),
    (2017, "Aalborg Universitet (AAU)"),
    (2017, "Danmarks Tekniske Universitet (DTU)"),
    (2017, "Copenhagen Business School (CBS)"),
    (2017, "Syddansk Universitet (SDU)"),
    (2017, "Roskilde Universitetscenter (RUC)"),
    # 2021: values are exact 2010 duplicates (source document not processed); nullify
    (2021, "Kobenhavns Universitet (KU)"),
    (2021, "Aarhus Universitet (AU)"),
    (2021, "Aalborg Universitet (AAU)"),
    (2021, "Danmarks Tekniske Universitet (DTU)"),
    (2021, "Copenhagen Business School (CBS)"),
    (2021, "Syddansk Universitet (SDU)"),
    (2021, "Roskilde Universitetscenter (RUC)"),
    # 2022: DNRF = 1.2B misread from page 3 overview; no dedicated DNRF appropriation
    # line exists in §19 of 2022 Finance Bill (DNRF is a foundation, not an annual grant)
    (2022, "Danmarks Grundforskningsfond (DNRF)"),
    # 2025: sequential digit hallucinations (234.1M, 345.2M, 456.3M...); PDF column format
    # changed in 2025 (compact code table separate from names); real values set via overrides
    (2025, "Kobenhavns Universitet (KU)"),
    (2025, "Aarhus Universitet (AU)"),
    (2025, "Aalborg Universitet (AAU)"),
    (2025, "Danmarks Tekniske Universitet (DTU)"),
    (2025, "Copenhagen Business School (CBS)"),
    (2025, "Syddansk Universitet (SDU)"),
    (2025, "Roskilde Universitetscenter (RUC)"),
    # 1978: LLM extracted partial/fragment amounts; correct Driftsudgifter set via overrides
    (1978, "Kobenhavns Universitet (KU)"),
    (1978, "Aarhus Universitet (AU)"),
    (1978, "Aalborg Universitet (AAU)"),
    (1978, "Syddansk Universitet (SDU)"),
    # 1984: LLM extracted partial/fragment amounts; correct Driftsudgifter set via overrides
    (1984, "Kobenhavns Universitet (KU)"),
    (1984, "Aarhus Universitet (AU)"),
    (1984, "Aalborg Universitet (AAU)"),
    # 2019: DTU=25M is a tiny fragment row (expected ~2.4B); correct value set via override
    (2019, "Danmarks Tekniske Universitet (DTU)"),
}

_DENMARK_VERIFIED_OVERRIDES: dict[int, dict[str, tuple[float, int]]] = {
    # ── 1978 (1978 19771_L101_som_vedtaget) ──────────────────────────────────
    # Driftsudgifter (gross operating appropriations) from §20 table (pre-reform methodology).
    1978: {
        "Kobenhavns Universitet (KU)":                      (489_504_600.0, 0),
        "Aarhus Universitet (AU)":                          (255_239_700.0, 0),
        "Syddansk Universitet (SDU)":                       (97_485_200.0,  0),
        "Roskilde Universitetscenter (RUC)":                (47_827_000.0,  0),
        "Aalborg Universitet (AAU)":                        (92_897_500.0,  0),
        "Danmarks Tekniske Universitet (DTU)":              (183_747_800.0, 0),
        "Copenhagen Business School (CBS)":                 (61_383_200.0,  0),
    },
    # ── 1979 (1979 9781_L1_som_vedtaget) ─────────────────────────────────────
    # Driftsudgifter from §20 table.  CBS/DTU dropped (fragment rows); AAU/RUC were
    # already correct in series — safe to override with verified values.
    1979: {
        "Kobenhavns Universitet (KU)":                      (657_835_200.0, 0),
        "Aarhus Universitet (AU)":                          (350_592_400.0, 0),
        "Syddansk Universitet (SDU)":                       (137_915_300.0, 0),
        "Roskilde Universitetscenter (RUC)":                (65_170_000.0,  0),
        "Aalborg Universitet (AAU)":                        (125_300_000.0, 0),
        "Danmarks Tekniske Universitet (DTU)":              (251_849_000.0, 0),
        "Copenhagen Business School (CBS)":                 (88_877_800.0,  0),
    },
    # ── 1980 (1980 19792_L1_som_vedtaget) ────────────────────────────────────
    # Driftsudgifter from §20 table.
    1980: {
        "Kobenhavns Universitet (KU)":                      (706_215_300.0, 0),
        "Aarhus Universitet (AU)":                          (369_796_900.0, 0),
        "Syddansk Universitet (SDU)":                       (149_321_100.0, 0),
        "Roskilde Universitetscenter (RUC)":                (70_037_000.0,  0),
        "Aalborg Universitet (AAU)":                        (135_330_000.0, 0),
        "Danmarks Tekniske Universitet (DTU)":              (264_977_900.0, 0),
        "Copenhagen Business School (CBS)":                 (97_784_100.0,  0),
    },
    # ── 1981 (1981 19801_L1_som_vedtaget) ────────────────────────────────────
    # Driftsudgifter from §20 table.
    1981: {
        "Kobenhavns Universitet (KU)":                      (746_334_700.0, 0),
        "Aarhus Universitet (AU)":                          (387_209_100.0, 0),
        "Syddansk Universitet (SDU)":                       (161_331_700.0, 0),
        "Roskilde Universitetscenter (RUC)":                (75_670_300.0,  0),
        "Aalborg Universitet (AAU)":                        (145_110_200.0, 0),
        "Danmarks Tekniske Universitet (DTU)":              (282_517_800.0, 0),
        "Copenhagen Business School (CBS)":                 (106_883_000.0, 0),
    },
    # ── 1982 (1982 19812_L1_som_vedtaget) ────────────────────────────────────
    # Driftsudgifter from §20 table.
    1982: {
        "Kobenhavns Universitet (KU)":                      (787_533_400.0, 0),
        "Aarhus Universitet (AU)":                          (409_057_400.0, 0),
        "Syddansk Universitet (SDU)":                       (170_411_000.0, 0),
        "Roskilde Universitetscenter (RUC)":                (82_215_000.0,  0),
        "Aalborg Universitet (AAU)":                        (153_830_000.0, 0),
        "Danmarks Tekniske Universitet (DTU)":              (307_976_000.0, 0),
        "Copenhagen Business School (CBS)":                 (118_567_000.0, 0),
    },
    # ── 1984 (1984 19832_L1_som_vedtaget) ────────────────────────────────────
    # Driftsudgifter from §20 table.
    1984: {
        "Kobenhavns Universitet (KU)":                      (937_335_100.0, 0),
        "Aarhus Universitet (AU)":                          (496_960_800.0, 0),
        "Syddansk Universitet (SDU)":                       (214_923_500.0, 0),
        "Roskilde Universitetscenter (RUC)":                (102_756_000.0, 0),
        "Aalborg Universitet (AAU)":                        (200_095_000.0, 0),
        "Danmarks Tekniske Universitet (DTU)":              (395_319_000.0, 0),
        "Copenhagen Business School (CBS)":                 (157_249_100.0, 0),
    },
    # ── 1985 (pdf_5295777e3af6__1985_19841_L1_som_vedtaget) ──────────────────
    # Verified from §20 appropriation table: KU/AU already correct in LLM output;
    # AAU/DTU/CBS/SDU/RUC extracted wrong — replaced here.
    1985: {
        "Aalborg Universitet (AAU)":                        (216_743_900.0, 0),
        "Danmarks Tekniske Universitet (DTU)":              (408_672_500.0, 0),
        "Copenhagen Business School (CBS)":                 (177_020_000.0, 0),
        "Syddansk Universitet (SDU)":                       (222_464_200.0, 0),
        "Roskilde Universitetscenter (RUC)":                (106_845_800.0, 0),
    },
    # ── 1986 (pdf_d9a8c9db969e__1986_19851_L1_som_vedtaget) ──────────────────
    # All 7 universities replaced; LLM extracted wrong partial rows.
    1986: {
        "Kobenhavns Universitet (KU)":                      (909_929_500.0, 0),
        "Aarhus Universitet (AU)":                          (502_137_200.0, 0),
        "Aalborg Universitet (AAU)":                        (227_852_000.0, 0),
        "Danmarks Tekniske Universitet (DTU)":              (406_483_000.0, 0),
        "Copenhagen Business School (CBS)":                 (188_803_400.0, 0),
        "Syddansk Universitet (SDU)":                       (214_892_000.0, 0),
        "Roskilde Universitetscenter (RUC)":                (102_491_000.0, 0),
    },
    1987: {
        "Kobenhavns Universitet (KU)": (961_498_800.0, 133),
        "Aarhus Universitet (AU)": (539_958_200.0, 133),
        "Syddansk Universitet (SDU)": (232_763_000.0, 134),
        "Roskilde Universitetscenter (RUC)": (111_135_000.0, 135),
        "Aalborg Universitet (AAU)": (254_162_000.0, 135),
        "Danmarks Tekniske Universitet (DTU)": (434_333_000.0, 135),
        "Copenhagen Business School (CBS)": (209_992_300.0, 138),
    },
    1988: {
        "Kobenhavns Universitet (KU)": (1_030_719_500.0, 151),
        "Aarhus Universitet (AU)": (575_174_800.0, 151),
        "Syddansk Universitet (SDU)": (249_946_600.0, 152),
        "Roskilde Universitetscenter (RUC)": (117_211_500.0, 152),
        "Aalborg Universitet (AAU)": (283_819_000.0, 153),
        "Danmarks Tekniske Universitet (DTU)": (468_193_400.0, 153),
        "Copenhagen Business School (CBS)": (233_282_600.0, 156),
        "Riso Nationallaboratorium": (237_120_000.0, 232),
    },
    1989: {
        "Kobenhavns Universitet (KU)": (1_103_277_500.0, 131),
        "Aarhus Universitet (AU)": (622_085_900.0, 132),
        "Syddansk Universitet (SDU)": (274_805_000.0, 132),
        "Roskilde Universitetscenter (RUC)": (134_721_000.0, 133),
        "Aalborg Universitet (AAU)": (321_111_000.0, 133),
        "Danmarks Tekniske Universitet (DTU)": (513_662_000.0, 135),
        "Copenhagen Business School (CBS)": (263_080_000.0, 138),
        "Statens naturvidenskabelige Forskningsrad (SNF)": (104_910_000.0, 137),
        "Statens samfundsvidenskabelige Forskningsrad": (34_890_000.0, 137),
        "Statens humanistiske Forskningsrad": (48_660_000.0, 137),
        "Statens laegervidenskabelige Forskningsrad": (56_930_000.0, 137),
        "Statens teknisk-videnskabelige Forskningsfond (STvF)": (129_570_000.0, 137),
    },
    1990: {
        "Kobenhavns Universitet (KU)": (1_115_789_500.0, 116),
        "Aarhus Universitet (AU)": (625_479_500.0, 116),
        "Syddansk Universitet (SDU)": (272_128_000.0, 117),
        "Roskilde Universitetscenter (RUC)": (146_155_000.0, 118),
        "Aalborg Universitet (AAU)": (329_207_000.0, 118),
        "Danmarks Tekniske Universitet (DTU)": (525_990_000.0, 118),
        "Copenhagen Business School (CBS)": (226_626_000.0, 122),
        "Statens naturvidenskabelige Forskningsrad (SNF)": (107_986_000.0, 129),
        "Statens samfundsvidenskabelige Forskningsrad": (36_610_000.0, 129),
        "Statens humanistiske Forskningsrad": (51_840_000.0, 129),
        "Statens laegervidenskabelige Forskningsrad": (62_178_700.0, 129),
        "Statens teknisk-videnskabelige Forskningsfond (STvF)": (112_420_000.0, 129),
        "Riso Nationallaboratorium": (234_740_000.0, 201),
    },
    # ── 1991 (pdf_f40b057295d3__1991_19902_L1_som_vedtaget) ──────────────────
    # 1991 was NaN in series; verified operating appropriation totals from §20 table.
    1991: {
        "Kobenhavns Universitet (KU)":                      (1_171_700_000.0, 0),
        "Aarhus Universitet (AU)":                          (677_500_000.0, 0),
        "Syddansk Universitet (SDU)":                       (308_400_000.0, 0),
        "Roskilde Universitetscenter (RUC)":                (160_700_000.0, 0),
        "Aalborg Universitet (AAU)":                        (374_100_000.0, 0),
        "Danmarks Tekniske Universitet (DTU)":              (574_800_000.0, 0),
        "Copenhagen Business School (CBS)":                 (245_700_000.0, 0),
    },
    # ── 1992 (1992 19911_L1_som_vedtaget) ────────────────────────────────────
    # Driftsbev. totals from §20 table (pre-reform; comparable to Driftsudgifter era).
    # LLM rows dropped (sequential-digit hallucinations).
    1992: {
        "Kobenhavns Universitet (KU)":                      (1_295_100_000.0, 0),
        "Aarhus Universitet (AU)":                          (749_400_000.0,  0),
        "Syddansk Universitet (SDU)":                       (321_400_000.0,  0),
        "Roskilde Universitetscenter (RUC)":                (166_300_000.0,  0),
        "Aalborg Universitet (AAU)":                        (393_900_000.0,  0),
        "Danmarks Tekniske Universitet (DTU)":              (571_800_000.0,  0),
        "Copenhagen Business School (CBS)":                 (255_800_000.0,  0),
    },
    # ── 1993 (1993 19921_L1_som_vedtaget) ────────────────────────────────────
    # Driftsbev. totals from §20 table (pre-reform; comparable to Driftsudgifter era).
    # LLM KU/AU/SDU rows dropped (reservation sub-lines); AAU/DTU/CBS/RUC may be correct
    # but overridden here with verified totals for consistency.
    1993: {
        "Kobenhavns Universitet (KU)":                      (1_357_800_000.0, 0),
        "Aarhus Universitet (AU)":                          (792_700_000.0,  0),
        "Syddansk Universitet (SDU)":                       (345_500_000.0,  0),
        "Roskilde Universitetscenter (RUC)":                (187_300_000.0,  0),
        "Aalborg Universitet (AAU)":                        (414_100_000.0,  0),
        "Danmarks Tekniske Universitet (DTU)":              (590_300_000.0,  0),
        "Copenhagen Business School (CBS)":                 (268_600_000.0,  0),
    },
    # ── 1994 (1994 19931_L1_som_vedtaget) ────────────────────────────────────
    # POST-REFORM: basistilskud only (structural break vs. pre-1994 Driftsudgifter).
    # 1993 University Act reform shifted universities to net-budgeting; basistilskud
    # covers only the block grant portion (lower than gross appropriation).
    # LLM rows dropped (sequential-digit hallucinations).
    1994: {
        "Kobenhavns Universitet (KU)":                      (894_000_000.0,  0),
        "Aarhus Universitet (AU)":                          (463_500_000.0,  0),
        "Syddansk Universitet (SDU)":                       (172_900_000.0,  0),
        "Roskilde Universitetscenter (RUC)":                (90_200_000.0,   0),
        "Aalborg Universitet (AAU)":                        (205_200_000.0,  0),
        "Danmarks Tekniske Universitet (DTU)":              (378_400_000.0,  0),
        "Copenhagen Business School (CBS)":                 (144_400_000.0,  0),
    },
    # ── 1995 (1995 19941_L1_som_vedtaget) ────────────────────────────────────
    # POST-REFORM: basistilskud only (same methodology as 1994).
    # LLM rows dropped (round-multiple hallucinations).
    1995: {
        "Kobenhavns Universitet (KU)":                      (964_200_000.0,  0),
        "Aarhus Universitet (AU)":                          (508_500_000.0,  0),
        "Syddansk Universitet (SDU)":                       (197_200_000.0,  0),
        "Roskilde Universitetscenter (RUC)":                (113_900_000.0,  0),
        "Aalborg Universitet (AAU)":                        (215_700_000.0,  0),
        "Danmarks Tekniske Universitet (DTU)":              (404_700_000.0,  0),
        "Copenhagen Business School (CBS)":                 (172_900_000.0,  0),
    },
    # ── 1997 (pdf_35d6f9df9f86__1997_19961_L1_som_vedtaget) ──────────────────
    # Research councils: LLM extracted tiny sequential values; real amounts from §32 table.
    1997: {
        "Statens teknisk-videnskabelige Forskningsfond (STvF)": (162_200_000.0, 0),
        "Statens naturvidenskabelige Forskningsrad (SNF)":      (236_100_000.0, 0),
        "Statens samfundsvidenskabelige Forskningsrad":         (68_900_000.0, 0),
        "Statens humanistiske Forskningsrad":                   (94_200_000.0, 0),
    },
    # ── 2008 (pdf_019da1cfc2ef__2008_A20080000130) ───────────────────────────
    # KU and AU: LLM extracted round text-note ceilings from p.180; real Reservationsbev. here.
    # DTU/AAU/SDU/RUC/CBS were correctly extracted from the Reservationsbev. table.
    2008: {
        "Kobenhavns Universitet (KU)":                      (4_251_600_000.0, 89),
        "Aarhus Universitet (AU)":                          (2_529_400_000.0, 89),
    },
    # ── 2009 (pdf_b9956fb90eaa__2009_A20080000330) ───────────────────────────
    # Universities: LLM picked reservation sub-lines; replaced with full Reservationsbev. totals.
    # DTU: LLM had NaN for 2009 (not matched individually); verified from p.89 table.
    2009: {
        "Kobenhavns Universitet (KU)":                      (4_521_500_000.0, 0),
        "Aarhus Universitet (AU)":                          (2_771_900_000.0, 0),
        "Syddansk Universitet (SDU)":                       (1_391_900_000.0, 0),
        "Roskilde Universitetscenter (RUC)":                (550_100_000.0,   0),
        "Aalborg Universitet (AAU)":                        (1_300_900_000.0, 0),
        "Copenhagen Business School (CBS)":                 (749_600_000.0,   0),
        "Danmarks Tekniske Universitet (DTU)":              (1_844_100_000.0, 89),
    },
    # ── 2010 (pdf_f76eb2254887__2010_A20090000230) ───────────────────────────
    # Universities: LLM picked anonymous Basic Grant rows (~1/3 of actual); replaced here.
    2010: {
        "Kobenhavns Universitet (KU)":                      (4_684_000_000.0, 0),
        "Aarhus Universitet (AU)":                          (2_981_700_000.0, 0),
        "Syddansk Universitet (SDU)":                       (1_462_900_000.0, 0),
        "Roskilde Universitetscenter (RUC)":                (563_000_000.0, 0),
        "Aalborg Universitet (AAU)":                        (1_417_600_000.0, 0),
        "Copenhagen Business School (CBS)":                 (808_800_000.0, 0),
        "Danmarks Tekniske Universitet (DTU)":              (1_932_500_000.0, 0),
    },
    # ── 2013 (pdf_b0ec927aac0f__2013_A20120000330) ───────────────────────────
    # Universities: round-number hallucinations; replaced with verified operating grant totals.
    2013: {
        "Kobenhavns Universitet (KU)":                      (5_034_900_000.0, 0),
        "Aarhus Universitet (AU)":                          (3_257_100_000.0, 0),
        "Syddansk Universitet (SDU)":                       (1_759_100_000.0, 0),
        "Roskilde Universitetscenter (RUC)":                (625_600_000.0, 0),
        "Aalborg Universitet (AAU)":                        (1_793_700_000.0, 0),
        "Copenhagen Business School (CBS)":                 (914_400_000.0, 0),
        "Danmarks Tekniske Universitet (DTU)":              (2_101_000_000.0, 0),
    },
    # ── 2015 (pdf_d970af9cf0fc__2015_A20140000230) ───────────────────────────
    # Unit bug: LLM stored DKK millions as unit=thousand (×1000 short); corrected here.
    # AAU=2,052.3M, CBS=952.9M, RUC=672.0M verified directly from document table.
    # KU, AU, SDU, DTU corrected by same ×1000 factor applied to LLM-extracted values.
    2015: {
        "Kobenhavns Universitet (KU)":                      (5_277_400_000.0, 0),
        "Aarhus Universitet (AU)":                          (3_619_300_000.0, 0),
        "Syddansk Universitet (SDU)":                       (2_048_400_000.0, 0),
        "Roskilde Universitetscenter (RUC)":                (672_000_000.0, 0),
        "Aalborg Universitet (AAU)":                        (2_052_300_000.0, 0),
        "Copenhagen Business School (CBS)":                 (952_900_000.0, 0),
        "Danmarks Tekniske Universitet (DTU)":              (2_240_700_000.0, 0),
    },
    # ── 2011 (pdf_d469cf3f8146__2011_A20100000230) ───────────────────────────
    # KU=NaN in LLM output (extracted as NaN on page 184); all others correct on page 91.
    # KU=4,933.3M verified from Reservationsbev. table.
    2011: {
        "Kobenhavns Universitet (KU)":                      (4_933_300_000.0, 91),
    },
    # ── 2014 (pdf_ea36f7174774__2014_A20130000230) ───────────────────────────
    # LLM extracted competing rows: NaN headers (p.110), round multiples (p.143),
    # 1M placeholder rows (p.30), small grant fragments (p.86).
    # Selvejebev. totals from §19.22 table verified here.
    2014: {
        "Kobenhavns Universitet (KU)":                      (5_161_500_000.0, 0),
        "Aarhus Universitet (AU)":                          (3_606_800_000.0, 0),
        "Syddansk Universitet (SDU)":                       (1_951_600_000.0, 0),
        "Roskilde Universitetscenter (RUC)":                (646_500_000.0, 0),
        "Aalborg Universitet (AAU)":                        (1_899_300_000.0, 0),
        "Copenhagen Business School (CBS)":                 (919_500_000.0, 0),
        "Danmarks Tekniske Universitet (DTU)":              (2_218_500_000.0, 0),
    },
    # ── 2017 (pdf_49003402903a__2017_A20160000230) ───────────────────────────
    # LLM extracted zeros (table headers on p.178 without amounts).
    # Selvejebev. totals from §19.22 table verified here.
    2017: {
        "Kobenhavns Universitet (KU)":                      (5_317_700_000.0, 0),
        "Aarhus Universitet (AU)":                          (3_751_200_000.0, 0),
        "Syddansk Universitet (SDU)":                       (2_166_600_000.0, 0),
        "Roskilde Universitetscenter (RUC)":                (653_100_000.0,   0),
        "Aalborg Universitet (AAU)":                        (2_038_700_000.0, 0),
        "Copenhagen Business School (CBS)":                 (998_500_000.0,   0),
        "Danmarks Tekniske Universitet (DTU)":              (2_397_400_000.0, 0),
    },
    # ── 2018 (2018 A20170000230.pdf) ─────────────────────────────────────────
    # PDF uses custom font encoding (Caesar +29); direct text extraction garbled.
    # Amounts recovered via PyMuPDF OCR on §19.22 Selvejebev. table (page 104).
    # Values in Mio. kr. → base DKK: 52982→5298.2M, 37778→3777.8M, etc.
    # Plausible against 2017/2019 verified anchor points.
    2018: {
        "Kobenhavns Universitet (KU)":                      (5_298_200_000.0, 104),
        "Aarhus Universitet (AU)":                          (3_777_800_000.0, 104),
        "Syddansk Universitet (SDU)":                       (2_152_100_000.0, 104),
        "Roskilde Universitetscenter (RUC)":                (652_500_000.0,   104),
        "Aalborg Universitet (AAU)":                        (2_102_300_000.0, 104),
        "Copenhagen Business School (CBS)":                 (1_022_900_000.0, 104),
        "Danmarks Tekniske Universitet (DTU)":              (2_442_400_000.0, 104),
    },
    # ── 2019 (AE2602.pdf) ────────────────────────────────────────────────────
    # LLM DTU=25M fragment (dropped); all 7 universities verified from §19.22 table.
    2019: {
        "Kobenhavns Universitet (KU)":                      (5_344_100_000.0, 0),
        "Aarhus Universitet (AU)":                          (3_818_500_000.0, 0),
        "Syddansk Universitet (SDU)":                       (2_194_500_000.0, 0),
        "Roskilde Universitetscenter (RUC)":                (640_200_000.0,   0),
        "Aalborg Universitet (AAU)":                        (2_087_000_000.0, 0),
        "Copenhagen Business School (CBS)":                 (1_084_200_000.0, 0),
        "Danmarks Tekniske Universitet (DTU)":              (2_446_800_000.0, 0),
    },
    # ── 2005 (20041_L1_som_vedtaget.pdf) ────────────────────────────────────
    # Unit bug: LLM stored Mio. kr. amounts with unit=thousand (1000x too small).
    # Amounts verified from §19.22 individual Selvejebev. lines on page 86.
    # Bad rows dropped via _DENMARK_VERIFIED_DROPS; correct million-scale values here.
    2005: {
        "Kobenhavns Universitet (KU)":                      (2_780_600_000.0, 86),
        "Aarhus Universitet (AU)":                          (1_653_400_000.0, 86),
        "Syddansk Universitet (SDU)":                       (1_093_400_000.0, 86),
        "Roskilde Universitetscenter (RUC)":                (457_000_000.0,   86),
        "Aalborg Universitet (AAU)":                        (1_026_800_000.0, 86),
        "Copenhagen Business School (CBS)":                 (600_100_000.0,   86),
        "Danmarks Tekniske Universitet (DTU)":              (1_185_400_000.0, 86),
    },
    # ── 2020 (AE2755 (3).pdf) ────────────────────────────────────────────────
    # LLM extracted only research fund rows (§19.41); missed §19.22 individual universities.
    # Amounts verified from page 107, §19.22 Selvejebev. table (Mio. kr.).
    2020: {
        "Kobenhavns Universitet (KU)":                      (5_190_800_000.0, 107),
        "Aarhus Universitet (AU)":                          (3_838_300_000.0, 107),
        "Syddansk Universitet (SDU)":                       (2_118_700_000.0, 107),
        "Roskilde Universitetscenter (RUC)":                (639_600_000.0,   107),
        "Aalborg Universitet (AAU)":                        (2_080_700_000.0, 107),
        "Copenhagen Business School (CBS)":                 (1_110_600_000.0, 107),
        "Danmarks Tekniske Universitet (DTU)":              (2_487_700_000.0, 107),
    },
    # ── 2021 (AE2845.pdf) ────────────────────────────────────────────────────
    # LLM extracted 2010 duplicates for universities (dropped); real values from page 110.
    # Amounts verified from §19.22 Selvejebev. table (Mio. kr.).
    2021: {
        "Kobenhavns Universitet (KU)":                      (5_288_400_000.0, 110),
        "Aarhus Universitet (AU)":                          (3_893_900_000.0, 110),
        "Syddansk Universitet (SDU)":                       (2_170_700_000.0, 110),
        "Roskilde Universitetscenter (RUC)":                (652_600_000.0,   110),
        "Aalborg Universitet (AAU)":                        (2_095_500_000.0, 110),
        "Copenhagen Business School (CBS)":                 (1_123_300_000.0, 110),
        "Danmarks Tekniske Universitet (DTU)":              (2_533_100_000.0, 110),
    },
    # ── 2022 (AE2954.pdf) ────────────────────────────────────────────────────
    # LLM extracted only research fund rows; missed §19.22 individual universities.
    # Amounts verified from page 117, §19.22 Selvejebev. table (Mio. kr.).
    2022: {
        "Kobenhavns Universitet (KU)":                      (5_350_300_000.0, 117),
        "Aarhus Universitet (AU)":                          (3_924_800_000.0, 117),
        "Syddansk Universitet (SDU)":                       (2_189_200_000.0, 117),
        "Roskilde Universitetscenter (RUC)":                (646_200_000.0,   117),
        "Aalborg Universitet (AAU)":                        (2_139_000_000.0, 117),
        "Copenhagen Business School (CBS)":                 (1_125_600_000.0, 117),
        "Danmarks Tekniske Universitet (DTU)":              (2_584_000_000.0, 117),
    },
    # ── 2025 (AE3209.pdf) ────────────────────────────────────────────────────
    # 2025 PDF format changed: amounts in compact code table (01./05./11. etc.) separate from
    # university name list — LLM extracted names without amounts (sequential digit hallucinations
    # dropped). Amounts verified from page 123, §19.22 lines (Mio. kr.).
    2025: {
        "Kobenhavns Universitet (KU)":                      (5_994_100_000.0, 123),
        "Aarhus Universitet (AU)":                          (4_502_400_000.0, 123),
        "Syddansk Universitet (SDU)":                       (2_577_800_000.0, 123),
        "Roskilde Universitetscenter (RUC)":                (720_100_000.0,   123),
        "Aalborg Universitet (AAU)":                        (2_408_500_000.0, 123),
        "Copenhagen Business School (CBS)":                 (1_335_800_000.0, 123),
        "Danmarks Tekniske Universitet (DTU)":              (2_955_000_000.0, 123),
    },
}

_DENMARK_VERIFIED_SOURCE_FILES = {
    1978: "1978 19771_L101_som_vedtaget.pdf",
    1979: "1979 9781_L1_som_vedtaget.pdf",
    1980: "1980 19792_L1_som_vedtaget.pdf",
    1981: "1981 19801_L1_som_vedtaget.pdf",
    1982: "1982 19812_L1_som_vedtaget.pdf",
    1984: "1984 19832_L1_som_vedtaget.pdf",
    1985: "1985 19841_L1_som_vedtaget.pdf",
    1986: "1986 19851_L1_som_vedtaget.pdf",
    1987: "1987 19861_L1_som_vedtaget.pdf",
    1988: "1988 19871_L1_som_vedtaget.pdf",
    1989: "1989 19881_L1_som_vedtaget.pdf",
    1990: "1990 19891_L1_som_vedtaget.pdf",
    1991: "1991 19902_L1_som_vedtaget.pdf",
    1992: "1992 19911_L1_som_vedtaget.pdf",
    1993: "1993 19921_L1_som_vedtaget.pdf",
    1994: "1994 19931_L1_som_vedtaget.pdf",
    1995: "1995 19941_L1_som_vedtaget.pdf",
    1997: "1997 19961_L1_som_vedtaget.pdf",
    2008: "A20080000130.pdf",
    2009: "A20080000330.pdf",
    2010: "A20090000230.pdf",
    2011: "A20100000230.pdf",
    2013: "A20120000330.pdf",
    2014: "A20130000230.pdf",
    2015: "A20140000230.pdf",
    2017: "A20160000230.pdf",
    2018: "A20170000230.pdf",
    2019: "AE2602.pdf",
    2005: "20041_L1_som_vedtaget.pdf",
    2020: "AE2755 (3).pdf",
    2021: "AE2845.pdf",
    2022: "AE2954.pdf",
    2025: "AE3209.pdf",
}

_SPAIN_VERIFIED_OVERRIDES: dict[int, dict[str, tuple[float, int, str, str]]] = {
    # Verified against the original Spain budget tables in the 2023 BOE file
    # (2022 extraction year in this pipeline). Amounts are full EUR units.
    2004: {
        "Plan Nacional I+D (total R&D appropriation)":
            (563_045_980.0, 149, "EUR", "BOE-A-2004-21688-consolidado para 2005.pdf"),
    },
    2005: {
        "Plan Nacional I+D (total R&D appropriation)":
            (1_008_543_870.0, 151, "EUR", "BOE-A-2005-21525-consolidado para 2006.pdf"),
    },
    2007: {
        "Plan Nacional I+D (total R&D appropriation)":
            (1_905_919_950.0, 177, "EUR", "BOE-A-2007-22295-consolidado para 2008.pdf"),
    },
    2010: {
        "Plan Nacional I+D (total R&D appropriation)":
            (2_139_768_610.0, 202, "EUR", "BOE-A-2010-19703-consolidado para 2011.pdf"),
    },
    2011: {
        "Plan Nacional I+D (total R&D appropriation)":
            (2_139_768_610.0, 202, "EUR", "2011 BOE-A-2010-19703-consolidado.pdf"),
    },
    2022: {
        "CIEMAT":
            (144_707_990.0, 640, "EUR", "2023 BOE-A-2022-22128.pdf"),
        "ISCIII (Instituto de Salud Carlos III)":
            (487_532_150.0, 640, "EUR", "2023 BOE-A-2022-22128.pdf"),
        "Esteban Terradas National Institute of Aerospace Technology":
            (196_019_400.0, 637, "EUR", "2023 BOE-A-2022-22128.pdf"),
        "Centre for Sociological Research":
            (12_659_110.0, 640, "EUR", "2023 BOE-A-2022-22128.pdf"),
        "Spanish Metrology Center":
            (17_482_090.0, 637, "EUR", "2023 BOE-A-2022-22128.pdf"),
        "National Transplant Organization":
            (6_706_760.0, 640, "EUR", "2023 BOE-A-2022-22128.pdf"),
    },
}

_FINLAND_VERIFIED_OVERRIDES: dict[int, dict[str, tuple[float, int, str, str]]] = {
    # Verified against original Finnish budget proposal texts.
    # Structure: {year: {canonical_name: (amount_local, page, currency, source_file)}}
    # Academy = moment 29.88.50 + 29.88.53 (lottery funds for science) combined.
    # All FIM amounts are full markka units.
    1992: {
        "Suomen Akatemia — tutkimusmäärärahat (research grants)":
            (146_310_000, 288, "FIM", "1992 proposal fi19910057.pdf"),
    },
    1993: {
        "Suomen Akatemia — tutkimusmäärärahat (research grants)":
            (385_150_000, 234, "FIM", "1993 proposal fi19920122.pdf"),  # 131.15M(50)+254M(53)
        "GTK (Geological Survey of Finland)":
            (183_830_000, 248, "FIM", "1993 proposal fi19920122.pdf"),
    },
    1996: {
        "Suomen Akatemia — tutkimusmäärärahat (research grants)":
            (430_211_000, 234, "FIM", "1996 proposal fi19950072.pdf"),  # 50.31M(50)+379.9M(53)
    },
    1999: {
        "Suomen Akatemia — tutkimusmäärärahat (research grants)":
            (713_380_000, 266, "FIM", "1999 proposal fi19980105.pdf"),  # 314.68M(50)+398.7M(53)
        "GTK (Geological Survey of Finland)":
            (210_877_000, 406, "FIM", "1999 proposal fi19980105.pdf"),
        "VTT (Technical Research Centre of Finland)":
            (358_570_000, 407, "FIM", "1999 proposal fi19980105.pdf"),
    },
    2001: {
        "Suomen Akatemia — tutkimusmäärärahat (research grants)":
            (749_360_000, 282, "FIM", "2001 proposal budget bill draft.pdf"),  # 296.36M(50)+453M(53)
        "GTK (Geological Survey of Finland)":
            (215_747_000, 380, "FIM", "2001 proposal budget bill draft.pdf"),
        "Business Finland / Tekes (innovation agency)":
            (138_000_000, 389, "FIM", "2001 proposal budget bill draft.pdf"),
    },
}

_FRANCE_PRE_LOLF_KEEP_CANONICALS = {
    "Research (Pre-LOLF Ministry Chapter)",
    "Universities and Higher Education (Pre-LOLF Chapter)",
    "Industrial and Scientific Development",
    "BRGM (Bureau de Recherches Géologiques et Minières)",
    "CEA (Commissariat à l'Énergie Atomique)",
    "CNES (Centre National d'Études Spatiales)",
    "CNRS (Centre National de la Recherche Scientifique)",
    "IFREMER (Institut Français de Recherche pour l'Exploitation de la Mer)",
    "INRAE (Institut National de Recherche pour l'Agriculture)",
    "INRIA (Institut National de Recherche en Informatique)",
    "INSERM (Institut National de la Santé et de la Recherche Médicale)",
}

_NETHERLANDS_CLIP_TO_OBSERVED = {
    "Erasmus Universiteit Rotterdam (EUR)",
    "NIOZ (Royal Netherlands Institute for Sea Research)",
    "NWO-TTW / STW (Technology Foundation)",
    "Radboud Universiteit Nijmegen",
    "Rijksuniversiteit Groningen (RUG)",
    "TU Delft (Delft University of Technology)",
    "TU Eindhoven (TU/e)",
    "Universiteit Leiden",
    "Universiteit Utrecht (UU)",
    "Universiteit van Amsterdam (UvA)",
    "Vrije Universiteit Amsterdam (VU)",
    "Deltares (water and subsurface research)",
    "NLR (National Aerospace Laboratory)",
    "RVO / Senter / SenterNovem (innovation instruments)",
    "Wageningen Universiteit (WUR)",
}

_CHILE_VERIFIED_OVERRIDES: dict[int, dict[str, tuple[float, int, str, str]]] = {
    # Verified against original Chile budget text extracts / source tables.
    # Structure: {year: {canonical_name: (amount_local, page, currency, source_file)}}
    2016: {
        "Excellence Centers - CORFO":
            (3_321_600_000.0, 0, "CLP", "pdf_2a8a4335247c__2016_Ley_de_presupuestos.txt"),
        "Technological Consortiums - CORFO":
            (3_193_420_000.0, 0, "CLP", "pdf_2a8a4335247c__2016_Ley_de_presupuestos.txt"),
        "FIE - Strategic Public Goods Project (Innova Committee)":
            (2_559_403_000.0, 0, "CLP", "pdf_2a8a4335247c__2016_Ley_de_presupuestos.txt"),
    },
    2017: {
        "Public Innovation Committee":
            (3_164_654_000.0, 0, "CLP", "pdf_0be4f6acdb77__2017_Ley_de_presupuestos.txt"),
        "FIE-Innovation and R&D for Enterprises (Innova Committee)":
            (1_369_900_000.0, 0, "CLP", "pdf_0be4f6acdb77__2017_Ley_de_presupuestos.txt"),
    },
    2021: {
        "Technological Consortiums - CORFO":
            (9_939_882_000.0, 0, "CLP", "pdf_a202d9a1404b__2021_Ley_de_presupuestos.txt"),
    },
    2022: {
        "CONICYT / ANID":
            (300_858_107_000.0, 0, "CLP", "pdf_59f02122fee1__2022_Ley_de_presupuestos.txt"),
        "Technological Consortiums - CORFO":
            (8_969_143_000.0, 0, "CLP", "pdf_59f02122fee1__2022_Ley_de_presupuestos.txt"),
    },
    2023: {
        "Technological Consortiums - CORFO":
            (9_014_891_000.0, 0, "CLP", "pdf_01662b31fcf6__2023_Ley_de_presupuestos.txt"),
    },
    2024: {
        "CONICYT / ANID":
            (322_532_595_000.0, 0, "CLP", "pdf_74da6471bcd9__2024_Ley_de_presupuestos.txt"),
        "Technological Consortiums - CORFO":
            (12_305_582_000.0, 0, "CLP", "pdf_74da6471bcd9__2024_Ley_de_presupuestos.txt"),
    },
    2025: {
        "CONICYT / ANID":
            (340_604_113_000.0, 0, "CLP", "pdf_e0cd9ea5ce26__2025_Ley_de_presupuestos.txt"),
        "Technological Consortiums - CORFO":
            (12_114_163_000.0, 0, "CLP", "pdf_e0cd9ea5ce26__2025_Ley_de_presupuestos.txt"),
    },
}

_COSTA_RICA_VERIFIED_OVERRIDES: dict[int, dict[str, tuple]] = {
    2010: {
        "CONICIT (Consejo Nacional para Investigaciones Científicas y Tecnológicas)": (
            751_503_500.0,
            96,
            "CRC",
            "pdf_4c260828c63b__2010_Ley_de_presupuesto.txt",
        ),
        "INCIENSA (health and nutrition research)": (
            154_500_000.0,
            89,
            "CRC",
            "pdf_4c260828c63b__2010_Ley_de_presupuesto.txt",
            "operating transfer on page 89",
        ),
        "UCR (Universidad de Costa Rica)": (
            13_100_000.0,
            97,
            "CRC",
            "pdf_4c260828c63b__2010_Ley_de_presupuesto.txt",
            "convenio CITA/MAG/UCR/MICIT operating transfer on page 97",
        ),
    },
    2011: {
        "CATIE (Centro Agronómico Tropical de Investigación y Enseñanza)": (
            11_805_900.0,
            429,
            "CRC",
            "pdf_2cd269e29d69__2011_Ley_de_presupuesto.txt",
            "ordinary annual quota on page 429",
        ),
        "INTA (Instituto Nacional de Innovación y Transferencia en Tecnología Agropecuaria)": (
            12_387_747_000.0,
            440,
            "CRC",
            "pdf_2cd269e29d69__2011_Ley_de_presupuesto.txt",
        ),
        "UCR (Universidad de Costa Rica)": (
            5_900_000.0,
            428,
            "CRC",
            "pdf_2cd269e29d69__2011_Ley_de_presupuesto.txt",
            "sede regional limon transfer on page 428",
        ),
    },
    2012: {},
    2013: {
        "CATIE (Centro Agronómico Tropical de Investigación y Enseñanza)": (
            26_000_000.0,
            144,
            "CRC",
            "pdf_2ca1f7962cd2__2013_Tomo2_Ley_de_presupuesto.txt",
        ),
        "CONICIT (Consejo Nacional para Investigaciones Científicas y Tecnológicas)": (
            876_300_000.0,
            96,
            "CRC",
            "pdf_aa090ab603b8__2013_Tomo5_Ley_de_presupuesto.txt",
            "fondo de incentivos transfer on tomo 5 page 96",
        ),
        "INCIENSA (health and nutrition research)": (
            3_246_436_000.0,
            28,
            "CRC",
            "pdf_676af6f56c56__2013_Tomo4_Ley_de_presupuesto.txt",
        ),
        "INTA (Instituto Nacional de Innovación y Transferencia en Tecnología Agropecuaria)": (
            4_043_611_000.0,
            161,
            "CRC",
            "pdf_2ca1f7962cd2__2013_Tomo2_Ley_de_presupuesto.txt",
            "program total for investigacion agropecuaria on tomo 2 page 161; replaces prior double-counted summary",
        ),
        "UCR (Universidad de Costa Rica)": (
            47_300_000.0,
            96,
            "CRC",
            "pdf_2ca1f7962cd2__2013_Tomo2_Ley_de_presupuesto.txt; pdf_aa090ab603b8__2013_Tomo5_Ley_de_presupuesto.txt",
            "sum of sede regional limon transfer 6,400,000 on tomo 2 page 151 and convenio CITA-MAG transfer 40,900,000 on tomo 5 page 96",
        ),
    },
    2014: {
        "CONICIT (Consejo Nacional para Investigaciones Científicas y Tecnológicas)": (
            642_232_000.0,
            89,
            "CRC",
            "pdf_4f4ae1d3c84d__2014_Tomo5_Ley_de_presupuesto.txt",
        ),
        "INCIENSA (health and nutrition research)": (
            3_821_328_000.0,
            22,
            "CRC",
            "pdf_286dfbab2290__2014_Tomo4_Ley_de_presupuesto.txt",
        ),
        "UCR (Universidad de Costa Rica)": (
            163_550_000.0,
            89,
            "CRC",
            "pdf_4f4ae1d3c84d__2014_Tomo5_Ley_de_presupuesto.txt",
            "sum of convenio CITA-MAG transfer 40,900,000 and convenio CITA/MAG/UCR/MICIT transfer 122,650,000 on tomo 5 page 89",
        ),
    },
    2017: {
        "CATIE (Centro Agronómico Tropical de Investigación y Enseñanza)": (
            27_510_000.0,
            363,
            "CRC",
            "pdf_b7628d47c028__2017_Tomo1_Ley_de_presupuesto.txt",
        ),
        "CONICIT (Consejo Nacional para Investigaciones Científicas y Tecnológicas)": (
            100_000_000.0,
            217,
            "CRC",
            "pdf_1cbc124dbce0__2017_Tomo2_Ley_de_presupuesto.txt",
        ),
        "INCIENSA (health and nutrition research)": (
            4_934_000_000.0,
            228,
            "CRC",
            "pdf_1cbc124dbce0__2017_Tomo2_Ley_de_presupuesto.txt",
        ),
        "MICITT (Ministerio de Ciencia, Innovación, Tecnología y Telecomunicaciones)": (
            6_753_000_922.0,
            40,
            "CRC",
            "pdf_b6df2745474e__2017_Tomo3_Ley_de_presupuesto.txt",
        ),
        "UCR (Universidad de Costa Rica)": (
            46_500_000.0,
            363,
            "CRC",
            "pdf_b7628d47c028__2017_Tomo1_Ley_de_presupuesto.txt; pdf_b6df2745474e__2017_Tomo3_Ley_de_presupuesto.txt",
            "sum of sede regional limon transfer 15,500,000 on tomo 1 page 361 and convenio CITA-MAG transfer 31,000,000 on tomo 3 page 57",
        ),
    },
    2016: {},
    2018: {},
    2019: {},
    2020: {
        "CATIE (Centro Agronómico Tropical de Investigación y Enseñanza)": (
            30_550_000.0,
            368,
            "CRC",
            "pdf_65d032f4c4ef__2020_Ley_de_presupuesto.txt",
        ),
        "CONICIT (Consejo Nacional para Investigaciones Científicas y Tecnológicas)": (
            1_856_800_000.0,
            645,
            "CRC",
            "pdf_65d032f4c4ef__2020_Ley_de_presupuesto.txt",
        ),
        "INTA (Instituto Nacional de Innovación y Transferencia en Tecnología Agropecuaria)": (
            383_160_000.0,
            368,
            "CRC",
            "pdf_65d032f4c4ef__2020_Ley_de_presupuesto.txt",
        ),
        "UCR (Universidad de Costa Rica)": (
            16_200_000.0,
            369,
            "CRC",
            "pdf_65d032f4c4ef__2020_Ley_de_presupuesto.txt",
            "sede regional limon transfer on page 369",
        ),
    },
    2021: {
        "CATIE (Centro Agronómico Tropical de Investigación y Enseñanza)": (
            30_600_000.0,
            435,
            "CRC",
            "pdf_ac902c30ecd9__2021_Ley_de_presupuesto.txt",
        ),
        "CONICIT (Consejo Nacional para Investigaciones Científicas y Tecnológicas)": (
            1_839_934_582.0,
            1432,
            "CRC",
            "pdf_ac902c30ecd9__2021_Ley_de_presupuesto.txt",
        ),
        "INTA (Instituto Nacional de Innovación y Transferencia en Tecnología Agropecuaria)": (
            4_629_768_709.0,
            415,
            "CRC",
            "pdf_ac902c30ecd9__2021_Ley_de_presupuesto.txt",
        ),
        "MICITT (Ministerio de Ciencia, Innovación, Tecnología y Telecomunicaciones)": (
            5_205_572_746.0,
            1416,
            "CRC",
            "pdf_ac902c30ecd9__2021_Ley_de_presupuesto.txt",
        ),
        "UCR (Universidad de Costa Rica)": (
            48_600_000.0,
            434,
            "CRC",
            "pdf_ac902c30ecd9__2021_Ley_de_presupuesto.txt",
            "sum of sede regional limon transfer 16,200,000 on page 434 and convenio CITA-MAG transfer 32,400,000 on page 1431",
        ),
    },
    2022: {
    },
    2023: {
    },
    2024: {
        "CATIE (Centro Agronómico Tropical de Investigación y Enseñanza)": (
            27_090_000.0,
            434,
            "CRC",
            "pdf_70b402f40a19__2024_Ley_de_presupuesto.txt",
        ),
        "INCIENSA (health and nutrition research)": (
            7_405_300_000.0,
            1061,
            "CRC",
            "pdf_70b402f40a19__2024_Ley_de_presupuesto.txt",
        ),
        "MICITT (Ministerio de Ciencia, Innovación, Tecnología y Telecomunicaciones)": (
            8_789_530_987.0,
            1564,
            "CRC",
            "pdf_70b402f40a19__2024_Ley_de_presupuesto.txt",
        ),
        "Promotora Costarricense de Innovación e Investigación (PCII)": (
            1_571_508_437.0,
            1423,
            "CRC",
            "pdf_70b402f40a19__2024_Ley_de_presupuesto.txt",
        ),
        "UCR (Universidad de Costa Rica)": (
            48_600_000.0,
            432,
            "CRC",
            "pdf_70b402f40a19__2024_Ley_de_presupuesto.txt",
            "sum of sede regional limon transfer 16,200,000 on page 432 and convenio CITA-MAG transfer 32,400,000 on page 1578",
        ),
    },
    2025: {
        "CATIE (Centro Agronómico Tropical de Investigación y Enseñanza)": (
            30_100_000.0,
            471,
            "CRC",
            "pdf_ef12aca9f998__2025_Ley_de_presupuesto.txt",
        ),
        "INCIENSA (health and nutrition research)": (
            6_440_000_000.0,
            884,
            "CRC",
            "pdf_ef12aca9f998__2025_Ley_de_presupuesto.txt",
        ),
        "MICITT (Ministerio de Ciencia, Innovación, Tecnología y Telecomunicaciones)": (
            7_890_373_607.0,
            1416,
            "CRC",
            "pdf_ef12aca9f998__2025_Ley_de_presupuesto.txt",
        ),
        "Promotora Costarricense de Innovación e Investigación (PCII)": (
            1_571_508_437.0,
            1426,
            "CRC",
            "pdf_ef12aca9f998__2025_Ley_de_presupuesto.txt",
        ),
        "UCR (Universidad de Costa Rica)": (
            948_600_000.0,
            61,
            "CRC",
            "pdf_ef12aca9f998__2025_Ley_de_presupuesto.txt",
            "sum of RSN transfer 900,000,000 on page 61, sede regional limon transfer 16,200,000 on page 459, and convenio CITA-MAG transfer 32,400,000 on page 1425",
        ),
    },
}

_COSTA_RICA_VERIFIED_DROPS: set[tuple[int, str]] = {
    (2010, "FEES (Fondo Especial de Educación Superior)"),
    (2011, "FEES (Fondo Especial de Educación Superior)"),
    (2012, "FEES (Fondo Especial de Educación Superior)"),
    (2013, "FEES (Fondo Especial de Educación Superior)"),
    (2014, "FEES (Fondo Especial de Educación Superior)"),
    (2015, "FEES (Fondo Especial de Educación Superior)"),
    (2016, "FEES (Fondo Especial de Educación Superior)"),
    (2017, "FEES (Fondo Especial de Educación Superior)"),
    (2018, "FEES (Fondo Especial de Educación Superior)"),
    (2019, "FEES (Fondo Especial de Educación Superior)"),
    (2020, "FEES (Fondo Especial de Educación Superior)"),
    (2021, "FEES (Fondo Especial de Educación Superior)"),
    (2022, "FEES (Fondo Especial de Educación Superior)"),
    (2023, "FEES (Fondo Especial de Educación Superior)"),
    (2024, "FEES (Fondo Especial de Educación Superior)"),
    (2025, "FEES (Fondo Especial de Educación Superior)"),
    (2011, "CONICIT (Consejo Nacional para Investigaciones Científicas y Tecnológicas)"),
    (2012, "CONICIT (Consejo Nacional para Investigaciones Científicas y Tecnológicas)"),
    (2010, "CATIE (Centro Agronómico Tropical de Investigación y Enseñanza)"),
    (2012, "CATIE (Centro Agronómico Tropical de Investigación y Enseñanza)"),
    (2014, "CATIE (Centro Agronómico Tropical de Investigación y Enseñanza)"),
    (2015, "CATIE (Centro Agronómico Tropical de Investigación y Enseñanza)"),
    (2016, "CATIE (Centro Agronómico Tropical de Investigación y Enseñanza)"),
    (2018, "CATIE (Centro Agronómico Tropical de Investigación y Enseñanza)"),
    (2019, "CATIE (Centro Agronómico Tropical de Investigación y Enseñanza)"),
    (2022, "CATIE (Centro Agronómico Tropical de Investigación y Enseñanza)"),
    (2023, "CATIE (Centro Agronómico Tropical de Investigación y Enseñanza)"),
    (2010, "INCIENSA (health and nutrition research)"),
    (2010, "INTA (Instituto Nacional de Innovación y Transferencia en Tecnología Agropecuaria)"),
    (2010, "ITCR / TEC (Instituto Tecnológico de Costa Rica)"),
    (2011, "ITCR / TEC (Instituto Tecnológico de Costa Rica)"),
    (2010, "UCR (Universidad de Costa Rica)"),
    (2010, "MICITT (Ministerio de Ciencia, Innovación, Tecnología y Telecomunicaciones)"),
    (2011, "INCIENSA (health and nutrition research)"),
    (2011, "UCR (Universidad de Costa Rica)"),
    (2012, "MICITT (Ministerio de Ciencia, Innovación, Tecnología y Telecomunicaciones)"),
    (2012, "ITCR / TEC (Instituto Tecnológico de Costa Rica)"),
    (2012, "INTA (Instituto Nacional de Innovación y Transferencia en Tecnología Agropecuaria)"),
    (2012, "INCIENSA (health and nutrition research)"),
    (2012, "UCR (Universidad de Costa Rica)"),
    (2013, "CONICIT (Consejo Nacional para Investigaciones Científicas y Tecnológicas)"),
    (2013, "INCIENSA (health and nutrition research)"),
    (2013, "UCR (Universidad de Costa Rica)"),
    (2013, "ITCR / TEC (Instituto Tecnológico de Costa Rica)"),
    (2014, "INTA (Instituto Nacional de Innovación y Transferencia en Tecnología Agropecuaria)"),
    (2014, "CONICIT (Consejo Nacional para Investigaciones Científicas y Tecnológicas)"),
    (2014, "INCIENSA (health and nutrition research)"),
    (2014, "UCR (Universidad de Costa Rica)"),
    (2014, "ITCR / TEC (Instituto Tecnológico de Costa Rica)"),
    (2015, "CONICIT (Consejo Nacional para Investigaciones Científicas y Tecnológicas)"),
    (2015, "ITCR / TEC (Instituto Tecnológico de Costa Rica)"),
    (2015, "INTA (Instituto Nacional de Innovación y Transferencia en Tecnología Agropecuaria)"),
    (2015, "INCIENSA (health and nutrition research)"),
    (2015, "Promotora Costarricense de Innovación e Investigación (PCII)"),
    (2015, "UCR (Universidad de Costa Rica)"),
    (2016, "CATIE (Centro Agronómico Tropical de Investigación y Enseñanza)"),
    (2016, "CONICIT (Consejo Nacional para Investigaciones Científicas y Tecnológicas)"),
    (2016, "ITCR / TEC (Instituto Tecnológico de Costa Rica)"),
    (2016, "INTA (Instituto Nacional de Innovación y Transferencia en Tecnología Agropecuaria)"),
    (2016, "INCIENSA (health and nutrition research)"),
    (2016, "Promotora Costarricense de Innovación e Investigación (PCII)"),
    (2016, "UCR (Universidad de Costa Rica)"),
    (2017, "INTA (Instituto Nacional de Innovación y Transferencia en Tecnología Agropecuaria)"),
    (2017, "CONICIT (Consejo Nacional para Investigaciones Científicas y Tecnológicas)"),
    (2017, "INCIENSA (health and nutrition research)"),
    (2017, "UCR (Universidad de Costa Rica)"),
    (2017, "ITCR / TEC (Instituto Tecnológico de Costa Rica)"),
    (2011, "MICITT (Ministerio de Ciencia, Innovación, Tecnología y Telecomunicaciones)"),
    (2013, "MICITT (Ministerio de Ciencia, Innovación, Tecnología y Telecomunicaciones)"),
    (2014, "MICITT (Ministerio de Ciencia, Innovación, Tecnología y Telecomunicaciones)"),
    (2015, "MICITT (Ministerio de Ciencia, Innovación, Tecnología y Telecomunicaciones)"),
    (2016, "MICITT (Ministerio de Ciencia, Innovación, Tecnología y Telecomunicaciones)"),
    (2017, "Promotora Costarricense de Innovación e Investigación (PCII)"),
    (2018, "CATIE (Centro Agronómico Tropical de Investigación y Enseñanza)"),
    (2018, "CONICIT (Consejo Nacional para Investigaciones Científicas y Tecnológicas)"),
    (2018, "ITCR / TEC (Instituto Tecnológico de Costa Rica)"),
    (2018, "INTA (Instituto Nacional de Innovación y Transferencia en Tecnología Agropecuaria)"),
    (2018, "INCIENSA (health and nutrition research)"),
    (2018, "Promotora Costarricense de Innovación e Investigación (PCII)"),
    (2018, "UCR (Universidad de Costa Rica)"),
    (2018, "MICITT (Ministerio de Ciencia, Innovación, Tecnología y Telecomunicaciones)"),
    (2019, "CATIE (Centro Agronómico Tropical de Investigación y Enseñanza)"),
    (2019, "CONICIT (Consejo Nacional para Investigaciones Científicas y Tecnológicas)"),
    (2019, "ITCR / TEC (Instituto Tecnológico de Costa Rica)"),
    (2019, "INTA (Instituto Nacional de Innovación y Transferencia en Tecnología Agropecuaria)"),
    (2019, "INCIENSA (health and nutrition research)"),
    (2019, "Promotora Costarricense de Innovación e Investigación (PCII)"),
    (2019, "UCR (Universidad de Costa Rica)"),
    (2019, "MICITT (Ministerio de Ciencia, Innovación, Tecnología y Telecomunicaciones)"),
    (2020, "UCR (Universidad de Costa Rica)"),
    (2020, "CONICIT (Consejo Nacional para Investigaciones Científicas y Tecnológicas)"),
    (2020, "INTA (Instituto Nacional de Innovación y Transferencia en Tecnología Agropecuaria)"),
    (2020, "ITCR / TEC (Instituto Tecnológico de Costa Rica)"),
    (2020, "Promotora Costarricense de Innovación e Investigación (PCII)"),
    (2020, "MICITT (Ministerio de Ciencia, Innovación, Tecnología y Telecomunicaciones)"),
    (2021, "ITCR / TEC (Instituto Tecnológico de Costa Rica)"),
    (2021, "INCIENSA (health and nutrition research)"),
    (2021, "Promotora Costarricense de Innovación e Investigación (PCII)"),
    (2021, "UCR (Universidad de Costa Rica)"),
    (2022, "ITCR / TEC (Instituto Tecnológico de Costa Rica)"),
    (2022, "CONICIT (Consejo Nacional para Investigaciones Científicas y Tecnológicas)"),
    (2022, "INTA (Instituto Nacional de Innovación y Transferencia en Tecnología Agropecuaria)"),
    (2022, "Promotora Costarricense de Innovación e Investigación (PCII)"),
    (2022, "INCIENSA (health and nutrition research)"),
    (2022, "UCR (Universidad de Costa Rica)"),
    (2022, "MICITT (Ministerio de Ciencia, Innovación, Tecnología y Telecomunicaciones)"),
    (2023, "CONICIT (Consejo Nacional para Investigaciones Científicas y Tecnológicas)"),
    (2023, "CATIE (Centro Agronómico Tropical de Investigación y Enseñanza)"),
    (2023, "ITCR / TEC (Instituto Tecnológico de Costa Rica)"),
    (2023, "INTA (Instituto Nacional de Innovación y Transferencia en Tecnología Agropecuaria)"),
    (2023, "Promotora Costarricense de Innovación e Investigación (PCII)"),
    (2020, "INCIENSA (health and nutrition research)"),
    (2023, "INCIENSA (health and nutrition research)"),
    (2023, "UCR (Universidad de Costa Rica)"),
    (2023, "MICITT (Ministerio de Ciencia, Innovación, Tecnología y Telecomunicaciones)"),
    (2024, "CONICIT (Consejo Nacional para Investigaciones Científicas y Tecnológicas)"),
    (2024, "ITCR / TEC (Instituto Tecnológico de Costa Rica)"),
    (2024, "INTA (Instituto Nacional de Innovación y Transferencia en Tecnología Agropecuaria)"),
    (2024, "UCR (Universidad de Costa Rica)"),
    (2025, "CONICIT (Consejo Nacional para Investigaciones Científicas y Tecnológicas)"),
    (2025, "ITCR / TEC (Instituto Tecnológico de Costa Rica)"),
    (2025, "INTA (Instituto Nacional de Innovación y Transferencia en Tecnología Agropecuaria)"),
    (2025, "UCR (Universidad de Costa Rica)"),
}

_ICELAND_VERIFIED_OVERRIDES: dict[int, dict[str, tuple[float, int, str, str]]] = {
    1988: {
        # Verified against the original 1988 Iceland budget text:
        # 22-233 Rannsóknasjóður shows transfer/government-contribution lines
        # around 4.910M ISK, while the repeated 321.381M "Total" appears under
        # multiple unrelated sections and is a page-summary artefact.
        "Rannsóknasjóður (Research Fund)":
            (4_910_000.0, 15862, "ISK", "1988 0434.pdf"),
    },
    1996: {
        # Verified against the original 1996 Iceland budget text:
        # Vísindaráð carries a 20.000M ISK treasury contribution / total line;
        # the 1.300M "General operations" row is only a subcomponent.
        "Vísindaráð (Science Council)":
            (20_000_000.0, 611, "ISK", "1996 0500.pdf"),
        # Verified against the original 1996 Iceland budget text:
        # Háskóli Íslands has an explicit "Samtals" / total line at 20.000M ISK.
        # The 13.300M "Rannsóknastarfsemi" row is a sub-line, not the
        # institution's full appropriation.
        "Háskóli Íslands (University of Iceland)":
            (20_000_000.0, 7037, "ISK", "1996 0500.pdf"),
    },
    2016: {
        # Verified against the original 2016 Iceland budget text (2016 0703.pdf),
        # which reports these institutional rows in m.kr. The pipeline often
        # captured the figures but left them as 'thousand' or selected
        # peripheral sub-lines instead of the main institutional row.
        "Háskóli Íslands (University of Iceland)":
            (18_129_500_000.0, 1681, "ISK", "2016 0703.pdf"),
        "Raunvísindastofnun Háskólans (Science Institute of the University of Iceland)":
            (1_354_800_000.0, 1733, "ISK", "2016 0703.pdf"),
        "Veðurstofa Íslands (Icelandic Meteorological Office)":
            (2_301_200_000.0, 8277, "ISK", "2016 0703.pdf"),
        "Tækniþróunarsjóður (Technology Development Fund)":
            (2_352_500_000.0, 4197, "ISK", "2016 0703.pdf"),
        "Landbúnaðarháskóli Íslands (Agricultural University of Iceland)":
            (1_396_500_000.0, 1787, "ISK", "2016 0703.pdf"),
        "Verkefnasjóður sjávarútvegsins (Project Fund for Fisheries)":
            (570_000_000.0, 4079, "ISK", "2016 0703.pdf"),
        "Hafrannsóknastofnun (Marine Research Institute)":
            (3_418_600_000.0, 4037, "ISK", "2016 0703.pdf"),
    },
    2011: {
        # Verified against the original 2011 Iceland budget text:
        # 11-205 Nýsköpunarmiðstöð Íslands -> "1.01 Nýsköpunarmiðstöð Íslands"
        # = 1.135,6 m.kr., i.e. 1,135,600,000 ISK. The raw pipeline row keeps
        # the figure but leaves the unit as 'thousand', so normalize it here.
        "Nýsköpunarmiðstöð Íslands (Innovation Centre of Iceland)":
            (1_135_600_000.0, 9152, "ISK", "2011 0556.pdf"),
    },
    2020: {
        # Verified against the original 2020 Iceland budget text
        # (2020 s0561-f_I.pdf). These rows are explicit institutional lines in
        # the original budget, while the current pipeline sometimes chooses a
        # sibling programme/grant row or misses the institution entirely.
        "Háskóli Íslands (University of Iceland)":
            (22_020_800_000.0, 8276, "ISK", "2020 s0561-f_I.pdf"),
        "Veðurstofa Íslands (Icelandic Meteorological Office)":
            (2_520_300_000.0, 5958, "ISK", "2020 s0561-f_I.pdf"),
        "Landbúnaðarháskóli Íslands (Agricultural University of Iceland)":
            (1_588_200_000.0, 8423, "ISK", "2020 s0561-f_I.pdf"),
        "Háskólinn í Reykjavík (Reykjavik University)":
            (3_659_900_000.0, 8487, "ISK", "2020 s0561-f_I.pdf"),
        "Verkefnasjóður sjávarútvegsins (Project Fund for Fisheries)":
            (264_000_000.0, 5165, "ISK", "2020 s0561-f_I.pdf"),
    },
}

_FRANCE_PRE_LOLF_MINISTRY_BAD_LINE_PATTERNS = [
    re.compile(r"subsid", re.IGNORECASE),
    re.compile(r"grant", re.IGNORECASE),
    re.compile(r"equipment", re.IGNORECASE),
    re.compile(r"institut|institute|center|centre|commission|agency|foundation|fund", re.IGNORECASE),
    re.compile(r"medical", re.IGNORECASE),
    re.compile(r"nuclear", re.IGNORECASE),
]

_FRANCE_PRE_LOLF_MINISTRY_GOOD_LINE_PATTERNS = [
    re.compile(r"payment credits for research$", re.IGNORECASE),
    re.compile(r"payment credits for research and technology$", re.IGNORECASE),
    re.compile(r"industry and research$", re.IGNORECASE),
    re.compile(r"industrial and scientific development$", re.IGNORECASE),
    re.compile(r"total net of credits$", re.IGNORECASE),
    re.compile(r"total budget for research( and industry)?$", re.IGNORECASE),
    re.compile(r"^research$", re.IGNORECASE),
    re.compile(r"^total$", re.IGNORECASE),
]

_FRANCE_PRE_LOLF_UNI_GOOD_LINE_PATTERNS = [
    re.compile(r"payment credits for universit", re.IGNORECASE),
    re.compile(r"totals? for universit", re.IGNORECASE),
    re.compile(r"total net of credits$", re.IGNORECASE),
    re.compile(r"total for higher education", re.IGNORECASE),
]

_FRANCE_VERIFIED_OVERRIDES = {
    1988: {
        "CNES (Centre National d'Études Spatiales)": 663_093_000.0,
        "INRAE (Institut National de Recherche pour l'Agriculture)": 1_745_385_824.0,
        "CEA (Commissariat à l'Énergie Atomique)": 1_633_780_000.0,
    },
    1994: {
        "INRAE (Institut National de Recherche pour l'Agriculture)": 2_546_534_794.0,
        "CEA (Commissariat à l'Énergie Atomique)": 3_477_513_600.0,
    },
}

_FRANCE_VERIFIED_OVERRIDES_MODERN: dict[int, dict[str, tuple[float, int, str, str, str]]] = {
    2018: {
        "Multidisciplinary Scientific and Technological Research": (
            6_766_603_666.0,
            112,
            "EUR",
            "JORF_2018.pdf",
            "Recherches scientifiques et technologiques pluridisciplinaires",
        ),
        "Space Research": (
            1_618_103_753.0,
            112,
            "EUR",
            "JORF_2018.pdf",
            "Recherche spatiale",
        ),
        "Research in the Fields of Energy, Development, and Sustainable Mobility": (
            1_734_154_531.0,
            112,
            "EUR",
            "JORF_2018.pdf",
            "Recherche dans les domaines de l’énergie, du développement et de la mobilité durables",
        ),
        "Research and Higher Education in Economic and Industrial Matters": (
            778_677_598.0,
            112,
            "EUR",
            "JORF_2018.pdf",
            "Recherche et enseignement supérieur en matière économique et industrielle",
        ),
        "Cultural Research and Scientific Culture": (
            111_881_973.0,
            112,
            "EUR",
            "JORF_2018.pdf",
            "Recherche culturelle et culture scientifique",
        ),
        "Higher Education and Agricultural Research": (
            345_984_489.0,
            112,
            "EUR",
            "JORF_2018.pdf",
            "Enseignement supérieur et recherche agricoles",
        ),
        "Applied Research and Innovation in Agriculture": (
            71_000_000.0,
            114,
            "EUR",
            "JORF_2018.pdf",
            "Recherche appliquée et innovation en agriculture",
        ),
    },
    2019: {
        "Multidisciplinary Scientific and Technological Research": (
            6_941_078_490.0,
            183,
            "EUR",
            "JORF_2019.pdf",
            "Recherches scientifiques et technologiques pluridisciplinaires",
        ),
        "Space Research": (
            1_820_012_789.0,
            183,
            "EUR",
            "JORF_2019.pdf",
            "Recherche spatiale",
        ),
        "Research in the Fields of Energy, Development, and Sustainable Mobility": (
            1_722_927_442.0,
            183,
            "EUR",
            "JORF_2019.pdf",
            "Recherche dans les domaines de l’énergie, du développement et de la mobilité durables",
        ),
        "Research and Higher Education in Economic and Industrial Matters": (
            728_818_603.0,
            183,
            "EUR",
            "JORF_2019.pdf",
            "Recherche et enseignement supérieur en matière économique et industrielle",
        ),
        "Cultural Research and Scientific Culture": (
            109_981_973.0,
            183,
            "EUR",
            "JORF_2019.pdf",
            "Recherche culturelle et culture scientifique",
        ),
        "Higher Education and Agricultural Research": (
            352_815_958.0,
            183,
            "EUR",
            "JORF_2019.pdf",
            "Enseignement supérieur et recherche agricoles",
        ),
        "Applied Research and Innovation in Agriculture": (
            71_000_000.0,
            185,
            "EUR",
            "JORF_2019.pdf",
            "Recherche appliquée et innovation en agriculture",
        ),
    },
    2020: {
        "Research in the Fields of Energy, Development, and Sustainable Mobility": (
            1_761_730_045.0,
            214,
            "EUR",
            "JORF_2020.pdf",
            "Recherche dans les domaines de l’énergie, du développement et de la mobilité durables",
        ),
        "Applied Research and Innovation in Agriculture": (
            71_000_000.0,
            216,
            "EUR",
            "JORF_2020.pdf",
            "Recherche appliquée et innovation en agriculture",
        ),
    },
    2021: {
        "Research in the Fields of Energy, Development, and Sustainable Mobility": (
            1_755_420_951.0,
            173,
            "EUR",
            "JORF_2021.pdf",
            "Recherche dans les domaines de l’énergie, du développement et de la mobilité durables",
        ),
        "Applied Research and Innovation in Agriculture": (
            65_934_600.0,
            175,
            "EUR",
            "JORF_2021.pdf",
            "Recherche appliquée et innovation en agriculture",
        ),
    },
    2022: {
        "Applied Research and Innovation in Agriculture": (
            65_520_000.0,
            694,
            "EUR",
            "JORF_2022.pdf",
            "Recherche appliquée et innovation en agriculture",
        ),
    },
}


def _uk_exactish_match_group(*terms: str) -> list[str]:
    out: list[str] = []
    for term in terms:
        t = re.escape(term)
        out.extend(
            [
                rf"^{t}$",
                rf"^{t}\b",
                rf"\b{t}$",
                rf"\b{t}\b",
            ]
        )
    return out


def _strip_accents(text: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", text or "")
        if not unicodedata.combining(ch)
    )


def _normalise_fr_text(text: str) -> str:
    cleaned = _strip_accents(str(text or "")).lower()
    cleaned = cleaned.replace("’", "'")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def _normalise_match_text(text: str) -> str:
    cleaned = _strip_accents(str(text or "")).lower()
    cleaned = cleaned.replace("’", "'")
    cleaned = re.sub(r"[^a-z0-9]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


@lru_cache(maxsize=128)
def _france_full_text_path(source_file: str) -> Optional[Path]:
    stem = Path(str(source_file or "")).stem
    if not stem:
        return None
    matches = sorted(_FRANCE_FULL_TEXT_DIR.glob(f"*__{stem}.txt.gz"))
    return matches[0] if matches else None


@lru_cache(maxsize=256)
def _france_page_text(source_file: str, page_number: int) -> str:
    path = _france_full_text_path(source_file)
    if path is None:
        return ""
    try:
        with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as fh:
            text = fh.read()
    except OSError:
        return ""

    marker = f"=== Page {int(page_number)}.0 |"
    idx = text.find(marker)
    if idx == -1:
        marker = f"Page {int(page_number)}.0 |"
        idx = text.find(marker)
    if idx == -1:
        return ""

    next_idx = text.find("=== Page ", idx + len(marker))
    if next_idx == -1:
        next_idx = len(text)
    return text[idx:next_idx]


@lru_cache(maxsize=128)
def _japan_full_text_path(source_file: str) -> Optional[Path]:
    stem = Path(str(source_file or "")).stem
    if not stem:
        return None
    candidates = {stem, stem.replace(" ", "_")}
    matches: list[Path] = []
    for candidate in candidates:
        matches.extend(sorted(_JAPAN_FULL_TEXT_DIR.glob(f"*__{candidate}.txt.gz")))
    return matches[0] if matches else None


@lru_cache(maxsize=512)
def _japan_page_text(source_file: str, page_number: int) -> str:
    text = _japan_full_text(source_file)
    if not text:
        return ""

    marker = f"=== Page {int(page_number)}.0 |"
    idx = text.find(marker)
    if idx == -1:
        marker = f"Page {int(page_number)}.0 |"
        idx = text.find(marker)
    if idx == -1:
        return ""

    next_idx = text.find("=== Page ", idx + len(marker))
    if next_idx == -1:
        next_idx = len(text)
    return text[idx:next_idx]


@lru_cache(maxsize=128)
def _japan_full_text(source_file: str) -> str:
    path = _japan_full_text_path(source_file)
    if path is None:
        return ""
    try:
        with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as fh:
            return fh.read()
    except OSError:
        return ""


def _japan_agency_recovery_tokens(agency: Optional[dict], row: Optional[pd.Series] = None) -> list[str]:
    tokens: list[str] = []
    if agency is not None:
        tokens.extend(_JAPAN_AGENCY_OWNER_TOKENS.get(str(agency.get("canonical_name", "")), []))
    if row is not None:
        tokens.extend(
            [
                str(row.get("line_description", "") or "").strip(),
                str(row.get("section_name", "") or "").strip(),
            ]
        )

    preferred = [
        token
        for token in tokens
        if token and re.search(r"[\u3040-\u30ff\u3400-\u9fff]", token)
    ]
    if preferred:
        return list(dict.fromkeys(preferred))
    return [token for token in dict.fromkeys(tokens) if token]


def _japan_operating_text_candidates(text: str, tokens: list[str]) -> list[tuple[int, int, int]]:
    if not text or not tokens:
        return []

    phrase_specs = [
        ("運営費交付金に必要な経費", 0),
        ("一般勘定運営費交付金", 0),
        ("運営費交付金", 1),
        ("運営費", 2),
    ]
    candidates: list[tuple[int, int, int]] = []
    seen: set[tuple[int, int, int]] = set()

    for token in tokens:
        token = str(token or "").strip()
        if not token:
            continue
        start = 0
        while True:
            idx = text.find(token, start)
            if idx == -1:
                break
            snippet = text[idx: idx + 260]
            priority = None
            for phrase, phrase_priority in phrase_specs:
                if phrase in snippet:
                    priority = phrase_priority
                    break
            if priority is not None:
                for match in re.finditer(r"\d{1,3}(?:,\d{3})+|\d{5,}", snippet):
                    try:
                        amount = int(match.group(0).replace(",", ""))
                    except ValueError:
                        continue
                    key = (priority, idx + match.start(), amount)
                    if key not in seen:
                        seen.add(key)
                        candidates.append(key)
                    break
            start = idx + len(token)

    return candidates


def _japan_recover_small_operating_row_thousand(row: pd.Series, agency: Optional[dict] = None) -> Optional[float]:
    try:
        page = int(float(row.get("page_number")))
        current_amount = float(row.get("amount_local"))
    except (TypeError, ValueError):
        return None

    if current_amount >= 100_000:
        return None

    row_text = _japan_row_text(row)
    if not _japan_row_matches_any(row_text, _JAPAN_OPERATING_ROW_PATTERNS):
        return None

    page_text = _japan_page_text(str(row.get("source_file", "")), page)
    if not page_text:
        return None

    needles = [
        str(row.get("line_description", "") or "").strip(),
        str(row.get("section_name", "") or "").strip(),
    ]
    if agency is not None:
        needles.extend(_JAPAN_AGENCY_OWNER_TOKENS.get(str(agency.get("canonical_name", "")), []))
    needles = [n for n in needles if n]
    if not needles:
        return None

    best = None
    compact_page = re.sub(r"\s+", "", page_text)
    compact_needles = [re.sub(r"\s+", "", needle) for needle in needles if needle]
    for needle in compact_needles:
        idx = compact_page.find(needle)
        if idx == -1:
            continue
        window = compact_page[idx: idx + 220]
        match = re.search(r"\d{1,3}(?:,\d{3})+|\d{5,}", window)
        if match:
            candidate = int(match.group(0).replace(",", ""))
            if candidate > current_amount * 100:
                best = float(candidate)
                break

    if best is None:
        lines = [line.strip() for line in page_text.splitlines() if line.strip()]
        for i, line in enumerate(lines):
            if not any(needle in line for needle in needles):
                continue
            window = lines[i:i + 3]
            values: list[int] = []
            for snippet in window:
                for token in re.findall(r"\d[\d, ]{2,}", snippet):
                    digits = re.sub(r"[,\s]+", "", token)
                    if len(digits) < 4:
                        continue
                    try:
                        values.append(int(digits))
                    except ValueError:
                        continue
            if values:
                for candidate in values:
                    if candidate > current_amount * 100:
                        best = float(candidate)
                        break
                if best is not None:
                    break
    return best


def _japan_historical_row_text(row: pd.Series) -> str:
    return " ".join(
        str(row.get(col, "") or "")
        for col in ["line_description", "line_description_en", "section_name", "section_name_en"]
    )


def _japan_historical_rollup_amount(group_df: pd.DataFrame) -> Optional[float]:
    if group_df.empty:
        return None

    work = group_df.copy()
    work["amount_local"] = pd.to_numeric(work["amount_local"], errors="coerce")
    work = work.dropna(subset=["amount_local"])
    if work.empty:
        return None

    text = work.apply(_japan_historical_row_text, axis=1)

    primary = work.loc[text.apply(lambda t: _japan_row_matches_any(t, _JAPAN_HISTORICAL_PRIMARY_PATTERNS))]
    if not primary.empty:
        return float(primary["amount_local"].max())

    subsidy = work.loc[text.apply(lambda t: _japan_row_matches_any(t, _JAPAN_HISTORICAL_SUBSIDY_PATTERNS))]
    investment = work.loc[text.apply(lambda t: _japan_row_matches_any(t, _JAPAN_HISTORICAL_INVESTMENT_PATTERNS))]

    total = 0.0
    found = False
    if not subsidy.empty:
        total += float(subsidy["amount_local"].max())
        found = True
    if not investment.empty:
        total += float(investment["amount_local"].max())
        found = True
    if found:
        return total

    return None


def _japan_compute_historical_rollup(
    subset: pd.DataFrame,
    canonical_name: str,
    year: int,
) -> tuple[Optional[float], Optional[str]]:
    if year > 2000:
        return None, None

    predecessor_groups = _JAPAN_HISTORICAL_ROLLUP_SPECS.get(canonical_name)
    if not predecessor_groups:
        return None, None

    if canonical_name == "NEDO (New Energy and Industrial Technology Development Organization)" and year < 1980:
        return None, None

    year_df = subset[pd.to_numeric(subset["year"], errors="coerce") == year].copy()
    if year_df.empty:
        return None, None

    total = 0.0
    matched_any = False
    text = year_df.apply(_japan_historical_row_text, axis=1)

    for patterns in predecessor_groups:
        alias_mask = text.apply(lambda t: any(re.search(p, t, re.IGNORECASE) for p in patterns))
        group_df = year_df.loc[alias_mask].copy()
        if group_df.empty:
            continue

        revenue_mask = group_df.apply(_japan_row_is_revenue_like, axis=1)
        if revenue_mask.any():
            trimmed = group_df.loc[~revenue_mask].copy()
            if not trimmed.empty:
                group_df = trimmed

        amount = _japan_historical_rollup_amount(group_df)
        if amount is None:
            continue
        total += float(amount)
        matched_any = True

    if not matched_any:
        return None, None

    source_series = year_df.dropna(subset=["source_file"])["source_file"].astype(str)
    source_file = source_series.mode().iloc[0] if not source_series.empty else None
    return total * 1000.0, source_file


def _france_recover_cp_thousand(source_file: str, page_number: object, line_description_raw: str) -> Optional[float]:
    try:
        page = int(float(page_number))
    except (TypeError, ValueError):
        return None

    page_text = _france_page_text(source_file, page)
    if not page_text:
        return None

    needle = _FRANCE_CP_PREFIX_RE.sub("", _normalise_fr_text(line_description_raw))
    needle = _FRANCE_LEADING_ARTICLE_RE.sub("", needle)
    if not needle:
        return None

    values = []
    lines = [line.strip() for line in page_text.splitlines() if line.strip()]
    for i, line in enumerate(lines):
        if needle not in _FRANCE_LEADING_ARTICLE_RE.sub("", _normalise_fr_text(line)):
            continue
        for follower in lines[i + 1:i + 8]:
            if re.search(r"[A-Za-zÀ-ÿ]", follower) and not re.search(r"\d", follower):
                break
            for token in re.findall(r"\d[\d ]{2,}", follower):
                digits = re.sub(r"\s+", "", token)
                if len(digits) < 4:
                    continue
                try:
                    values.append(int(digits))
                except ValueError:
                    continue
                if len(values) >= 2:
                    break
            if len(values) >= 2:
                break
        if values:
            break

    if not values:
        return None

    cp_full_eur = values[1] if len(values) >= 2 else values[0]
    return float(cp_full_eur) / 1000.0

_JAPAN_GENERIC_DISCOVERED_PATTERNS: list[re.Pattern] = [
    re.compile(r"^operating expenses? grant for\b", re.IGNORECASE),
    re.compile(r"^operating subsidy for\b", re.IGNORECASE),
    re.compile(r"^basic research operations grant for\b", re.IGNORECASE),
    re.compile(r"^operating expenses?\b", re.IGNORECASE),
    re.compile(r"^facility improvement subsidy\b", re.IGNORECASE),
    re.compile(r"^facility development\b", re.IGNORECASE),
    re.compile(r"^research facility development\b", re.IGNORECASE),
    re.compile(r"^expenses necessary for\b", re.IGNORECASE),
    re.compile(r"^funding for\b", re.IGNORECASE),
    re.compile(r"^investment and assistance\b", re.IGNORECASE),
    re.compile(r"^special research expenses?\b", re.IGNORECASE),
    re.compile(r"^grants? for\b", re.IGNORECASE),
    re.compile(r"^subsid(?:y|ies) for\b", re.IGNORECASE),
    re.compile(r"^expenses? for\b", re.IGNORECASE),
    re.compile(r"^commissioned\b", re.IGNORECASE),
    re.compile(r"^promotion of\b", re.IGNORECASE),
    re.compile(r"\bpromotion expenses?\b", re.IGNORECASE),
    re.compile(r"\bresearch and development expenses?\b", re.IGNORECASE),
    re.compile(r"\btesting and research expenses?\b", re.IGNORECASE),
    re.compile(r"\btest research expenses?\b", re.IGNORECASE),
    re.compile(r"\bscience promotion expenses?\b", re.IGNORECASE),
    re.compile(r"\bindustrial technology promotion\b", re.IGNORECASE),
    re.compile(r"\bscientific research (grant|expenses?)\b", re.IGNORECASE),
    re.compile(r"\bcommission(ed)? research\b", re.IGNORECASE),
    re.compile(r"\bsurvey expenses?\b", re.IGNORECASE),
    re.compile(r"\bpolicy expenses?\b", re.IGNORECASE),
    re.compile(r"\bfund subsidy\b", re.IGNORECASE),
    re.compile(r"\bproject subsidy\b", re.IGNORECASE),
    re.compile(r"\bgrant fund\b", re.IGNORECASE),
    re.compile(r"\bfacility development\b", re.IGNORECASE),
    re.compile(r"\bfacility improvement\b", re.IGNORECASE),
    re.compile(r"\bcontribution\s*$", re.IGNORECASE),
    re.compile(r"\bpayment\b", re.IGNORECASE),
    re.compile(r"納付金$"),
    re.compile(r"\btest(ing)? research\b", re.IGNORECASE),
    re.compile(r"試験研究"),
    re.compile(r"\b(subsidy|expenses?|grant|promotion|fund|project|survey|contract|investment|assistance)\s*$", re.IGNORECASE),
    re.compile(r"^(研究開発|研究振興|科学技術振興)(費|推進費)?$"),
]

_JAPAN_ORGANISATION_HINTS = re.compile(
    r"(agency|institute|organization|organisation|center|centre|society|foundation|corporation|university|"
    r"機構|研究所|研究機関|研究センター|センター|法人|協会|大学)",
    re.IGNORECASE,
)

_JAPAN_INSTITUTIONAL_ROW_PATTERNS = [
    r"operating subsidy",
    r"operating subsidies",
    r"operating expenses?\s+grant",
    r"operating expenses?\s+of",
    r"operating expenses?\s+for",
    r"operating appropriations?",
    r"運営費交付金",
    r"運営費交付金に必要な経費",
    r"(国立研究開発法人|独立行政法人).{0,40}運営費",
]

_JAPAN_REVENUE_ROW_PATTERNS = [
    r"歳入",
    r"主管歳入予算額",
    r"歳入予算額",
    r"納付金",
    r"受入見込額",
    r"所管合計",
    r"所管計",
    r"jurisdiction total",
    r"revenue budget",
    r"budget amount",
    r"payment from",
    r"expected payment",
]

_JAPAN_OPERATING_ROW_PATTERNS = [
    r"運営費交付金に必要な経費",
    r"運営費交付金",
    r"operating subsidy",
    r"operating subsidies",
    r"operating expenses?\s+grant",
    r"operating grant",
    r"operating expenses?\s+of",
    r"operating expenses?\s+for",
    r"operating appropriations?",
    r"運営費",
]

_JAPAN_FACILITY_ROW_PATTERNS = [
    r"施設整備",
    r"研究施設整備",
    r"facility improvement",
    r"facility development",
    r"vessel construction",
    r"equipment removal",
]

_JAPAN_AGENCY_OWNER_TOKENS: dict[str, list[str]] = {
    "JST (Japan Science and Technology Agency)": ["科学技術振興機構", "japan science and technology agency", "jst"],
    "JSPS (Japan Society for the Promotion of Science)": ["日本学術振興会", "japan society for the promotion of science", "jsps"],
    "RIKEN (Institute of Physical and Chemical Research)": ["理化学研究所", "riken"],
    "Power Reactor and Nuclear Fuel Development Corporation": ["動力炉・核燃料開発事業団", "power reactor and nuclear fuel development corporation", "pnc"],
    "NIMS (National Institute for Materials Science)": ["物質・材料研究機構", "national institute for materials science", "nims"],
    "JAMSTEC (Japan Agency for Marine-Earth Science and Technology)": ["海洋研究開発機構", "japan agency for marine-earth science", "jamstec"],
    "JAXA (Japan Aerospace Exploration Agency)": ["宇宙航空研究開発機構", "japan aerospace exploration agency", "jaxa"],
    "NEDO (New Energy and Industrial Technology Development Organization)": ["新エネルギー・産業技術総合開発機構", "new energy and industrial technology development organization", "nedo"],
    "AIST (National Institute of Advanced Industrial Science and Technology)": ["産業技術総合研究所", "national institute of advanced industrial science and technology", "aist"],
    "JAEA (Japan Atomic Energy Agency)": ["日本原子力研究開発機構", "japan atomic energy agency", "jaea"],
    "QST (National Institutes for Quantum Science and Technology)": ["量子科学技術研究開発機構", "quantum science and technology", "qst"],
    "National Institute for Environmental Studies": ["国立環境研究所", "national institute for environmental studies", "nies"],
    "National Institute of Radiological Sciences": ["放射線医学総合研究所", "national institute of radiological sciences", "nirs"],
    "National Institute of Biomedical Innovation, Health and Nutrition": ["医薬基盤・健康・栄養研究所", "national institute of biomedical innovation, health and nutrition", "nibiohn"],
}

_JAPAN_VERIFIED_OVERRIDES: dict[int, dict[str, int]] = {
    2000: {
        "JAXA (Japan Aerospace Exploration Agency)": 167_902_000_000,
        "JST (Japan Science and Technology Agency)": 81_940_072_000,
        "RIKEN (Institute of Physical and Chemical Research)": 65_490_000_000,
    },
    2005: {
        "AIST (National Institute of Advanced Industrial Science and Technology)": 67_431_520_000,
        "JAMSTEC (Japan Agency for Marine-Earth Science and Technology)": 32_692_784_000,
        "JAXA (Japan Aerospace Exploration Agency)": 131_411_464_000,
        "JSPS (Japan Society for the Promotion of Science)": 29_655_107_000,
        "JST (Japan Science and Technology Agency)": 99_611_126_000,
        "NEDO (New Energy and Industrial Technology Development Organization)": 41_670_822_000,
        "NIMS (National Institute for Materials Science)": 16_125_322_000,
        "RIKEN (Institute of Physical and Chemical Research)": 71_101_637_000,
    },
    2002: {
        "AIST (National Institute of Advanced Industrial Science and Technology)": 68_411_330_000,
        "NEDO (New Energy and Industrial Technology Development Organization)": 22_842_042_000,
        "National Institute for Environmental Studies": 9_515_867_000,
    },
    2008: {
        "AIST (National Institute of Advanced Industrial Science and Technology)": 64_237_356_000,
        "JAMSTEC (Japan Agency for Marine-Earth Science and Technology)": 38_430_626_000,
        "NEDO (New Energy and Industrial Technology Development Organization)": 40_834_570_000,
    },
    2010: {
        "AIST (National Institute of Advanced Industrial Science and Technology)": 61_406_811_000,
        "JAEA (Japan Atomic Energy Agency)": 63_468_679_000,
        "JAMSTEC (Japan Agency for Marine-Earth Science and Technology)": 36_336_563_000,
        "JAXA (Japan Aerospace Exploration Agency)": 130_391_959_000,
        "NEDO (New Energy and Industrial Technology Development Organization)": 39_608_141_000,
        "NIMS (National Institute for Materials Science)": 14_050_726_000,
        "National Institute for Environmental Studies": 2_835_409_000,
    },
    2012: {
        "JAEA (Japan Atomic Energy Agency)": 50_589_375_000,
        "JAMSTEC (Japan Agency for Marine-Earth Science and Technology)": 35_114_135_000,
        "JAXA (Japan Aerospace Exploration Agency)": 119_758_445_000,
        "JSPS (Japan Society for the Promotion of Science)": 29_229_437_000,
        "JST (Japan Science and Technology Agency)": 100_646_191_000,
        "NIMS (National Institute for Materials Science)": 13_481_876_000,
        "National Institute for Environmental Studies": 12_111_369_000,
    },
    2004: {
        "JAMSTEC (Japan Agency for Marine-Earth Science and Technology)": 30_713_740_000,
        "National Institute for Environmental Studies": 9_254_603_000,
    },
    2009: {
        "AIST (National Institute of Advanced Industrial Science and Technology)": 63_305_991_000,
        "JSPS (Japan Society for the Promotion of Science)": 28_672_449_000,
        "NIMS (National Institute for Materials Science)": 15_048_647_000,
        "National Institute for Environmental Studies": 9_292_205_000,
    },
    2011: {
        "JSPS (Japan Society for the Promotion of Science)": 29_229_937_000,
        "NEDO (New Energy and Industrial Technology Development Organization)": 60_439_000_000,
        "National Institute for Environmental Studies": 13_522_931_000,
    },
    2013: {
        "National Institute for Environmental Studies": 11_454_420_000,
    },
    2014: {
        "JAEA (Japan Atomic Energy Agency)": 46_916_707_000,
        "NIMS (National Institute for Materials Science)": 12_329_191_000,
        "National Institute for Environmental Studies": 10_828_427_000,
    },
    2016: {
        "AIST (National Institute of Advanced Industrial Science and Technology)": 62_847_560_000,
        "JAEA (Japan Atomic Energy Agency)": 34_614_821_000,
        "JAMSTEC (Japan Agency for Marine-Earth Science and Technology)": 30_618_486_000,
        "JAXA (Japan Aerospace Exploration Agency)": 105_342_777_000,
        "JST (Japan Science and Technology Agency)": 100_888_373_000,
        "NEDO (New Energy and Industrial Technology Development Organization)": 7_893_451_000,
        "NIMS (National Institute for Materials Science)": 12_020_623_000,
        "National Institute for Environmental Studies": 11_695_485_000,
        "National Institute of Biomedical Innovation, Health and Nutrition": 3_674_953_000,
        "QST (National Institutes for Quantum Science and Technology)": 21_557_994_000,
        "RIKEN (Institute of Physical and Chemical Research)": 51_591_219_000,
    },
    2018: {
        "NIMS (National Institute for Materials Science)": 13_517_272_000,
    },
    2024: {
        "AIST (National Institute of Advanced Industrial Science and Technology)": 65_000_661_000,
        "JAEA (Japan Atomic Energy Agency)": 36_478_799_000,
        "JAMSTEC (Japan Agency for Marine-Earth Science and Technology)": 30_366_656_000,
        "JAXA (Japan Aerospace Exploration Agency)": 122_397_995_000,
        "JSPS (Japan Society for the Promotion of Science)": 27_498_267_000,
        "JST (Japan Science and Technology Agency)": 100_970_256_000,
        "NEDO (New Energy and Industrial Technology Development Organization)": 12_556_204_000,
        "NIMS (National Institute for Materials Science)": 14_423_539_000,
        "National Institute of Biomedical Innovation, Health and Nutrition": 6_729_156_000,
        "QST (National Institutes for Quantum Science and Technology)": 21_788_072_000,
        "RIKEN (Institute of Physical and Chemical Research)": 55_348_412_000,
    },
}

_ITALY_VERIFIED_DROPS: set[tuple[int, str]] = {
    # 1992 19911231_305_SO_094.pdf pp. 903-905 show section/rubrica summaries
    # for "Universita' e ricerca scientifica"; the compiled FOE survivor at
    # 825,900,217,000 ITL does not correspond to a clean FOE line in the
    # audited annex block and is treated as a wrong-row attribution.
    (1992, "FOE — Fondo Ordinario per gli Enti di ricerca"),
    # 1987 19861230_301_SO_125.pdf p. 426 resolves to a broad education
    # summary page, not to an INFN line. The surviving 230,000,000,000 ITL
    # value is therefore not traceable enough for the final panel.
    (1987, "INFN — Istituto Nazionale di Fisica Nucleare"),
    # 2009 20081230_303_SO_286.pdf p. 534 is a programme-authorization
    # prospectus, not the clean annual CNR appropriation.
    (2009, "CNR — Consiglio Nazionale delle Ricerche"),
    # 2010 20091230_302_SO_244.pdf p. 53 is a ministry-wide spending
    # breakdown; p. 63 puts the 550.000 amount in the 2012 column.
    (2010, "FIRST / FAR / FIRB — Fondi per la ricerca"),
    (2010, "FOE — Fondo Ordinario per gli Enti di ricerca"),
    # 2013 20121229_302_SO_212.pdf p. 240 is a transfer-reduction annex for
    # research bodies, not the annual ASI/INAF appropriations.
    (2013, "ASI — Agenzia Spaziale Italiana"),
    (2013, "INAF — Istituto Nazionale di Astrofisica"),
    # 1996 19951229_302_SO_154.pdf p. 738 shows two separate ASI programme
    # lines (national/bilateral programmes and ESA collaboration), not a clean
    # annual ASI total. Keeping only one of them would understate the agency.
    (1996, "ASI — Agenzia Spaziale Italiana"),
    # 2016 20151230_302_SO_071.pdf p. 14 cites a 2,582,284 euro earmark
    # within the CNR allocation; 2020 20191230_304_SO_045.pdf p. 72 authorizes
    # a 750,000 euro earmark in favor of CNR. Neither is the full CNR budget.
    (2016, "CNR — Consiglio Nazionale delle Ricerche"),
    (2020, "CNR — Consiglio Nazionale delle Ricerche"),
}

_JAPAN_VERIFIED_DROPS: set[tuple[int, str]] = {
    (1996, "RIKEN (Institute of Physical and Chemical Research)"),
    (2005, "National Institute for Environmental Studies"),
    (2004, "NEDO (New Energy and Industrial Technology Development Organization)"),
    (2009, "NEDO (New Energy and Industrial Technology Development Organization)"),
    (2014, "NEDO (New Energy and Industrial Technology Development Organization)"),
}

_JAPAN_HISTORICAL_ROLLUP_SPECS: dict[str, list[list[str]]] = {
    "RIKEN (Institute of Physical and Chemical Research)": [
        [r"理化学研究所", r"\briken\b"],
    ],
    "Power Reactor and Nuclear Fuel Development Corporation": [
        [r"動力炉・核燃料開発事業団", r"power reactor and nuclear fuel development corporation", r"\bpnc\b"],
    ],
    "JSPS (Japan Society for the Promotion of Science)": [
        [r"日本学術振興会", r"japan society for the promotion of science", r"\bjsps\b"],
    ],
    "JAXA (Japan Aerospace Exploration Agency)": [
        [r"宇宙開発事業団", r"space development agency", r"\bnasda\b"],
    ],
    "JAMSTEC (Japan Agency for Marine-Earth Science and Technology)": [
        [r"海洋科学技術センター", r"ocean science and technology center"],
    ],
    # JST inherited the historical budget lines of both JRDC and JICST. For the
    # pre-1994 bridge, sum the best-supported amount from each predecessor.
    "JST (Japan Science and Technology Agency)": [
        [r"新技術開発事業団", r"新技術事業団", r"new technology development organization"],
        [r"日本科学技術情報センター", r"japan science and technology information center"],
    ],
    "JAEA (Japan Atomic Energy Agency)": [
        [r"日本原子力研究所", r"japan atomic energy research institute"],
    ],
    "NEDO (New Energy and Industrial Technology Development Organization)": [
        [r"新エネルギー・産業技術総合開発機構", r"new energy and industrial technology development organization", r"\bnedo\b"],
    ],
}

_JAPAN_HISTORICAL_PRIMARY_PATTERNS = [
    r"出資及び助成",
    r"補助に必要な経費",
    r"expenses necessary for",
    r"investment and assistance",
]

_JAPAN_HISTORICAL_SUBSIDY_PATTERNS = [
    r"補助金",
    r"補助費",
    r"subsidy",
    r"grants? for",
]

_JAPAN_HISTORICAL_INVESTMENT_PATTERNS = [
    r"出資金",
    r"investment",
    r"funding for",
]


def _japan_operating_match_group(*agency_terms: str) -> list[str]:
    agency_alt = "|".join(f"(?:{term})" for term in agency_terms)
    inst_alt = "|".join(f"(?:{term})" for term in _JAPAN_INSTITUTIONAL_ROW_PATTERNS)
    return [
        rf"(?:{agency_alt}).{{0,80}}(?:{inst_alt})",
        rf"(?:{inst_alt}).{{0,80}}(?:{agency_alt})",
    ]


def _japan_row_text(row: pd.Series) -> str:
    return " ".join(
        str(row.get(col, "") or "")
        for col in ["line_description_en", "section_name_en", "line_description", "section_name"]
    )


def _japan_row_matches_any(text: str, patterns: list[str]) -> bool:
    return any(re.search(pattern, text, re.IGNORECASE) for pattern in patterns)


def _japan_row_is_revenue_like(row: pd.Series) -> bool:
    return _japan_row_matches_any(_japan_row_text(row), _JAPAN_REVENUE_ROW_PATTERNS)


def _japan_row_priority(row: pd.Series) -> int:
    text = _japan_row_text(row)
    if _japan_row_is_revenue_like(row):
        return 99
    if _japan_row_matches_any(text, [
        r"運営費交付金に必要な経費",
        r"運営費交付金",
        r"operating subsidy",
        r"operating expenses?\s+grant",
        r"operating grant",
    ]):
        return 0
    if _japan_row_matches_any(text, [r"運営費", r"operating expenses?\s+of", r"operating expenses?\s+for"]):
        return 1
    if _japan_row_matches_any(text, _JAPAN_FACILITY_ROW_PATTERNS):
        return 3
    return 2


def _japan_row_owner(text: str) -> Optional[str]:
    text_lower = str(text or "").lower()
    for canonical_name, tokens in _JAPAN_AGENCY_OWNER_TOKENS.items():
        for token in tokens:
            token_lower = token.lower()
            if len(token_lower) <= 4:
                if re.search(r"(?<![a-z])" + re.escape(token_lower) + r"(?![a-z])", text_lower):
                    return canonical_name
            elif token_lower in text_lower:
                return canonical_name
    return None


def _japan_cross_agency_mismatch(row: pd.Series) -> bool:
    desc_owner = _japan_row_owner(
        " ".join(str(row.get(col, "") or "") for col in ["line_description", "line_description_en"])
    )
    section_owner = _japan_row_owner(
        " ".join(str(row.get(col, "") or "") for col in ["section_name", "section_name_en"])
    )
    return bool(desc_owner and section_owner and desc_owner != section_owner)


def _france_programme_match_groups(*programme_terms: str) -> list[list[str]]:
    term_alt = "|".join(f"(?:{term})" for term in programme_terms)
    return [
        [
            rf"(?:^| )(?:{term_alt})(?:$| )",
            rf"(?:payment credits? for|total for|cr[ée]dits? de paiement pour|total pour).{{0,120}}(?:{term_alt})",
        ],
        [
            rf"(?:{term_alt})",
            r"(?:payment credits? for|total for|cr[ée]dits? de paiement pour|total pour|programme\s+\d+)",
        ],
    ]


def _france_pre_lolf_match_groups(*terms: str) -> list[list[str]]:
    term_alt = "|".join(f"(?:{term})" for term in terms)
    return [
        [
            rf"(?:{term_alt})",
            r"(?:ministere|minist[eè]re|services du premier ministre|universit[eé]s|enseignement sup[eé]rieur|recherche)",
        ],
    ]


def _base_output_unit(currency: object, fallback_unit: object) -> object:
    cur = str(currency or "").strip().upper()
    if cur == "CLP":
        return "peso"
    if cur == "PLN":
        return "zloty"
    if cur in _OUTPUT_UNIT_BY_CURRENCY:
        return _OUTPUT_UNIT_BY_CURRENCY[cur]
    return fallback_unit


def _expand_to_base_unit(amount: object, unit: object, currency: object) -> tuple[object, object]:
    try:
        amt = float(amount)
    except (TypeError, ValueError):
        return amount, unit

    unit_norm = str(unit or "").strip().lower()
    factor = _SCALE_TO_BASE_UNIT.get(unit_norm)
    if factor is None:
        return amt, unit
    return amt * factor, _base_output_unit(currency, unit)

# ---------------------------------------------------------------------------
# Agency definitions
# ---------------------------------------------------------------------------

CANONICAL_AGENCIES: dict[str, list[dict]] = {

    # -----------------------------------------------------------------------
    # AUSTRALIA
    # Sourced from: ABS Cat. 8104 (R&D by funding source), Appropriation Acts
    # Audited: April 2025 against LLM extraction results 1975-2026
    # -----------------------------------------------------------------------
    "Australia": [
        {
            "canonical_name": "CSIRO",
            "category": "science_agency",
            "name_variants": [
                "commonwealth scientific and industrial research",
                "commonwealth scientific and industrial",  # catches truncated cell text
                "science and industry research act",
                "csiro",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "active_years": (1926, 2099),
            "notes": "Core public research agency. Pre-1949: Advisory Council of Science. "
                     "Continuously tracked since 1975 in these files.",
        },
        {
            "canonical_name": "Australian Research Council (ARC)",
            "category": "science_agency",
            "name_variants": [
                "australian research council",
                "arc —",
                "arc—",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1988, 2099),
            "notes": "Created 1988. Before 1988 equivalent grants appear as "
                     "'Research grants' under Dept of Science.",
        },
        {
            "canonical_name": "NHMRC / Medical Research Fund",
            "category": "science_agency",
            "name_variants": [
                "national health and medical research",
                "nhmrc",
                "medical research endowment fund",
                "medical research (for payment",
                "health research (including payments to the med",
                "health research (including payments",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1936, 2099),
            "notes": "NHMRC grants and endowment fund payments. "
                     "Early years appear as line items under Health.",
        },
        {
            "canonical_name": "ANSTO / Atomic Energy Commission",
            "category": "science_agency",
            "name_variants": [
                "australian nuclear science and technology",
                "ansto",
                "australian atomic energy",
                "atomic energy act",
                "atomic energy commission",
                "for expenditure under the australian nuclear science",
                "expenditure under the australian nuclear science",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1953, 2099),
            "notes": "AAEC renamed ANSTO in 1987.",
        },
        {
            "canonical_name": "Australian Institute of Marine Science (AIMS)",
            "category": "science_agency",
            "name_variants": [
                "australian institute of marine science",
                "aims",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1972, 2099),
        },
        {
            "canonical_name": "Australian Centre for International Agricultural Research",
            "category": "direct_rd",
            "name_variants": [
                "australian centre for international agricultural research",
                "aciar",
                "australian centre for international agricultural research trust account",
                "australian centre for international agricultural research trust fund",
            ],
            "preferred_item_type": ["line_item", "section_total", "program_total"],
            "active_years": (1982, 2099),
            "notes": "Dedicated agricultural R&D agency. Prefer annual No1 entity totals and trust-fund payment lines over supplementary appropriation rows.",
        },
        {
            "canonical_name": "Australian Renewable Energy Agency",
            "category": "innovation_instruments",
            "name_variants": [
                "australian renewable energy agency",
                "arena",
                "total: australian renewable energy agency",
            ],
            "preferred_item_type": ["section_total", "line_item", "program_total"],
            "active_years": (2011, 2099),
            "notes": "Dedicated renewable-energy innovation agency. Prefer annual No1 totals over supplementary appropriations.",
        },
        {
            "canonical_name": "Industrial R&D Grants",
            "category": "innovation_instruments",
            "name_variants": [
                "industrial research and development",
                "australian industrial research and development",
                "industry research and development",
                "industry innovation program (including payment",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1967, 2010),
            "notes": "Pre-AusIndustry era industrial R&D grants scheme.",
        },
        {
            "canonical_name": "Research Grants (Dept of Science)",
            "category": "direct_rd",
            "name_variants": [
                "research grants—support for research projects",
                "research grants — support",
                "research grants (general)",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "active_years": (1965, 1995),
            "notes": "General competitive research grants before ARC took over.",
        },
        {
            "canonical_name": "Geoscience Australia / Bureau of Mineral Resources",
            "category": "science_agency",
            "name_variants": [
                "geoscience australia",
                "bureau of mineral resources",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1946, 2099),
            "notes": "BMR renamed Geoscience Australia in 2001.",
        },
        {
            "canonical_name": "Bureau of Meteorology (Research)",
            "category": "science_agency",
            "name_variants": [
                "commonwealth bureau of meteorology",
                "bureau of meteorology",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1906, 2099),
            "notes": "Include only if extracted as R&D — BoM has a large operational budget.",
        },
    ],

    # -----------------------------------------------------------------------
    # DENMARK
    # Finanslov (annual Finance Bill). Digital 1975-2026.
    # UNIT ERA: 1975-2000 = 1.000 kr. (thousand DKK); 2001+ = Mio. kr. (million DKK).
    # MINISTRY ERA:
    #   §20 Undervisningsministeriet (pre-2001)
    #   §32 Forsknings-/Videnskabsministeriet (2001-2013)
    #   §19 Uddannelses- og Forskningsministeriet / UFM (2014+)
    # -----------------------------------------------------------------------
    "Denmark": [
        # --- Ministry totals (section-level aggregates) ---
        {
            "canonical_name": "Uddannelses- og Forskningsministeriet (UFM)",
            "category": "rd_ministry",
            "name_variants": [
                "uddannelses- og forskningsministeriet",
                "uddannelses og forskningsministeriet",
                "ministry of higher education and science",
                "ufm",
            ],
            "preferred_item_type": ["section_total"],
            "active_years": (2014, 2099),
            "notes": "§19 from 2014. Section overview total — tag aggregation_role=section.",
        },
        {
            "canonical_name": "Videnskabsministeriet",
            "category": "rd_ministry",
            "name_variants": [
                "videnskabsministeriet",
                "ministeriet for videnskab teknologi og udvikling",
                "ministry of science technology and innovation",
                "forskningsministeriet",
                "ministry of research",
            ],
            "preferred_item_type": ["section_total"],
            "active_years": (2001, 2013),
            "notes": "Approx §32 2001-2013. Name varied between years.",
        },
        {
            "canonical_name": "Undervisningsministeriet (research section)",
            "category": "rd_ministry",
            "name_variants": [
                "undervisningsministeriet",
                "ministry of education",
            ],
            "preferred_item_type": ["section_total"],
            "active_years": (1970, 2001),
            "notes": "§20 pre-2001. Only the research/university sub-section is in scope.",
        },

        # --- Universities ---
        {
            "canonical_name": "Kobenhavns Universitet (KU)",
            "category": "higher_education",
            "name_variants": [
                "k\u00f8benhavns universitet",
                "kobenhavns universitet",
                "university of copenhagen",
                "copenhagen university",
                "20.01. university of copenhagen",
                "08.01. university of copenhagen",
                "01. university of copenhagen",
                "ku",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1479, 2099),
            "max_amount_local": 7_000_000_000,
            "notes": "Annual basisbevilling line item under UFM / Undervisningsministeriet.",
        },
        {
            "canonical_name": "Aarhus Universitet (AU)",
            "category": "higher_education",
            "name_variants": [
                "aarhus universitet",
                "university of aarhus",
                "aarhus university",
                "20.02. aarhus university",
                "02. aarhus university",
                "au",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            # In the Belgium federal-budget corpus currently ingested, FNRS only
            # appears as an explicit federal appropriation / debt-service line in
            # the late-1990s to 2001 window. Keeping it open-ended only creates
            # fake gaps in years where the corpus does not expose a named FNRS
            # line at all.
            "active_years": (1996, 2001),
            "max_amount_local": 5_000_000_000,
        },
        {
            "canonical_name": "Danmarks Tekniske Universitet (DTU)",
            "category": "higher_education",
            "name_variants": [
                "danmarks tekniske universitet",
                "danmarks tekniske h\u00f8jskole",
                "danmarks tekniske hojskole",
                "technical university of denmark",
                "20.03. technical university of denmark",
                "dtu",
                "dth",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1829, 2099),
            "max_amount_local": 3_500_000_000,
            "notes": "Renamed from Danmarks tekniske Højskole (DTH) to DTU in 1994.",
        },
        {
            "canonical_name": "Aalborg Universitet (AAU)",
            "category": "higher_education",
            "name_variants": [
                "aalborg universitet",
                "aalborg universitetscenter",
                "aalborg university",
                "aalborg university center",
                "auc",
                "aau",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1974, 2099),
            "max_amount_local": 2_800_000_000,
        },
        {
            "canonical_name": "Syddansk Universitet (SDU)",
            "category": "higher_education",
            "name_variants": [
                "syddansk universitet",
                "odense universitet",
                "university of southern denmark",
                "sdu",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1966, 2099),
            "max_amount_local": 2_800_000_000,
            "notes": "Previously Odense Universitet (pre-1998).",
        },
        {
            "canonical_name": "Roskilde Universitetscenter (RUC)",
            "category": "higher_education",
            "name_variants": [
                "roskilde universitetscenter",
                "roskilde universitet",
                "roskilde university",
                "ruc",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1972, 2099),
            "max_amount_local": 900_000_000,
        },
        {
            "canonical_name": "Copenhagen Business School (CBS)",
            "category": "higher_education",
            "name_variants": [
                "copenhagen business school",
                "handelsh\u00f8jskolen i k\u00f8benhavn",
                "handelshojskolen i kobenhavn",
                "cbs",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1917, 2099),
            "max_amount_local": 1_400_000_000,
        },
        {
            "canonical_name": "Universiteterne (collective)",
            "category": "higher_education",
            "name_variants": [
                "universiteterne",
                "universities",
                "universiteternes basisbevilling",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1970, 2099),
            "notes": "Collective line for all universities — use only when individual lines unavailable.",
        },

        # --- Research councils (pre-2014 structure) ---
        {
            "canonical_name": "Statens teknisk-videnskabelige Forskningsfond (STvF)",
            "category": "science_agency",
            "name_variants": [
                "statens teknisk-videnskabelige forskningsfond",
                "teknisk-videnskabelige forskningsfond",
                "state technical-scientific research fund",
                "danish technical-scientific research council",
                "stvf",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (1946, 2003),
            "max_amount_local": 500_000_000,
            "notes": "Merged into Det Frie Forskningsraad in 2003.",
        },
        {
            "canonical_name": "Statens naturvidenskabelige Forskningsrad (SNF)",
            "category": "science_agency",
            "name_variants": [
                "statens naturvidenskabelige forskningsrad",
                "naturvidenskabelige forskningsr\u00e5d",
                "danish natural science research council",
                "natural science research council",
                "snf",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (1946, 2003),
            "max_amount_local": 400_000_000,
            "notes": "Merged into Det Frie Forskningsraad in 2003.",
        },
        {
            "canonical_name": "Statens samfundsvidenskabelige Forskningsrad",
            "category": "science_agency",
            "name_variants": [
                "statens samfundsvidenskabelige forskningsrad",
                "samfundsvidenskabelige forskningsr\u00e5d",
                "social science research council",
                "danish social science research council",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (1968, 2003),
            "max_amount_local": 500_000_000,
        },
        {
            "canonical_name": "Statens humanistiske Forskningsrad",
            "category": "science_agency",
            "name_variants": [
                "statens humanistiske forskningsrad",
                "humanistiske forskningsr\u00e5d",
                "humanities research council",
                "danish humanities research council",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (1968, 2003),
            "max_amount_local": 500_000_000,
        },
        {
            "canonical_name": "Statens laegervidenskabelige Forskningsrad",
            "category": "science_agency",
            "name_variants": [
                "statens l\u00e6gevidenskabelige forskningsr\u00e5d",
                "statens laegervidenskabelige forskningsrad",
                "l\u00e6gevidenskabelige forskningsr\u00e5d",
                "medical research council",
                "danish medical research council",
                "danish health sciences research council",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (1960, 2003),
            "max_amount_local": 600_000_000,
        },
        {
            "canonical_name": "Det Strategiske Forskningsrad",
            "category": "science_agency",
            "name_variants": [
                "det strategiske forskningsr\u00e5d",
                "strategiske forskningsrad",
                "danish strategic research council",
                "the strategic research council",
                "dsf",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (2004, 2014),
            "max_amount_local": 1_000_000_000,
            "notes": "Merged into Danmarks Innovationsfond in 2014.",
        },

        # --- Post-2014 consolidated research funding bodies ---
        {
            "canonical_name": "Det Frie Forskningsraad / Danmarks Frie Forskningsfond",
            "category": "science_agency",
            "name_variants": [
                "det frie forskningsr\u00e5d",
                "danmarks frie forskningsfond",
                "frie forskningsrad",
                "independent research fund denmark",
                "the independent research fund",
                "dff",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (2003, 2099),
            "max_amount_local": 3_000_000_000,
            "notes": "Created 2003 from merging old councils. Renamed Danmarks Frie Forskningsfond ~2018.",
        },
        {
            "canonical_name": "Danmarks Innovationsfond",
            "category": "science_agency",
            "name_variants": [
                "danmarks innovationsfond",
                "innovation fund denmark",
                "innovationsfonden",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (2014, 2099),
            "max_amount_local": 5_000_000_000,
            "notes": "Created 2014 from Hoejteknologifonden + Det Strategiske Forskningsraad + Grundforskningsfond grants.",
        },
        {
            "canonical_name": "Hoejteknologifonden",
            "category": "science_agency",
            "name_variants": [
                "h\u00f8jteknologifonden",
                "hoejteknologifonden",
                "danish advanced technology foundation",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (2005, 2014),
            "max_amount_local": 1_500_000_000,
            "notes": "Merged into Danmarks Innovationsfond in 2014.",
        },
        {
            "canonical_name": "Danmarks Grundforskningsfond (DNRF)",
            "category": "science_agency",
            "name_variants": [
                "danmarks grundforskningsfond",
                "danish national research foundation",
                "dnrf",
                "grundforskningsfonden",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (1991, 2099),
            "max_amount_local": 1_500_000_000,
        },

        # --- Atomic energy / nuclear ---
        {
            "canonical_name": "Atomenergikommissionen",
            "category": "science_agency",
            "name_variants": [
                "atomenergikommissionen",
                "atomic energy commission",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (1955, 2000),
            "max_amount_local": 500_000_000,
            "notes": "Dissolved ~1990s; Risoe transferred to DTU 2012.",
        },
        {
            "canonical_name": "Riso Nationallaboratorium",
            "category": "science_agency",
            "name_variants": [
                "ris\u00f8 nationallaboratorium",
                "risoe nationallaboratorium",
                "ris\u00f8 national laboratory",
                "ris\u00f8",
                "risoe",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (1955, 2012),
            "max_amount_local": 1_000_000_000,
            "notes": "Transferred to DTU as DTU Risoe in 2012.",
        },
    ],

    # -----------------------------------------------------------------------
    # NORWAY
    # Statsbudsjettet Blaabok (Blue Book). 2010-2026 fully digital.
    # 1975-1992: scanned — near-zero text yield.
    # UNIT: Full NOK on detail pages; 1000 NOK in Part I overview.
    # KEY CHAPTERS: Kap. 260-275 (universities), Kap. 285 (NFR), Kap. 920-930 (NFD R&D).
    # -----------------------------------------------------------------------
    "Norway": [
        # --- Research Council of Norway (primary R&D vehicle) ---
        {
            "canonical_name": "Norges Forskningsrad (NFR)",
            "category": "science_agency",
            "name_variants": [
                "norges forskningsr\u00e5d",
                "norges forskningsrad",
                "research council of norway",
                "nfr",
                "forskningsr\u00e5det",
                "forskningsradet",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1993, 2099),
            # NFR main chapter (Kap. 285) peaks ~8B NOK in 2024-2026.
            # Cross-ministry program aggregates can reach 11-17B — excluded here.
            # Individual sector-ministry posts to NFR (e.g. Ministry of Energy) are
            # typically 400M-4B and will be selected as the consistent measure.
            "max_amount_local": 8_000_000_000,
            "notes": "Created 1993 from NTNF, NAVF, NORAS, NLVF, NFFR. "
                     "Kap. 285 under Kunnskapsdepartementet is the main chapter. "
                     "Also appears under sector ministries (NFD Kap. 920, OED Kap. 1830). "
                     "Each Post 50/52/55/70 line should be extracted separately.",
        },
        {
            "canonical_name": "NTNF (pre-NFR research council)",
            "category": "science_agency",
            "name_variants": [
                "norges teknisk-naturvitenskapelige forskningsr\u00e5d",
                "ntnf",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1946, 1993),
            # NTNF total grant budget peaked ~600M NOK in 1992. Section-total rows
            # in older scanned documents can be 5-10× inflated due to column bleed.
            "max_amount_local": 2_000_000_000,
            "notes": "Merged into NFR in 1993.",
        },
        {
            "canonical_name": "NAVF (pre-NFR research council)",
            "category": "science_agency",
            "name_variants": [
                "norges allmennevitenskapelige forskningsr\u00e5d",
                "navf",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1949, 1993),
            # NAVF budget peaked ~600M NOK in 1992.
            "max_amount_local": 1_500_000_000,
            "notes": "Merged into NFR in 1993.",
        },

        # --- Ministry totals ---
        {
            "canonical_name": "Kunnskapsdepartementet (total)",
            "category": "rd_ministry",
            "name_variants": [
                "kunnskapsdepartementet",
                "ministry of education and research",
                "kd",
            ],
            "preferred_item_type": ["section_total"],
            "active_years": (2006, 2099),
            "notes": "Kap. 200-299. Section total — tag aggregation_role=section. "
                     "Only use if individual lines are not available.",
        },

        # --- Universities ---
        {
            "canonical_name": "NTNU (Norges teknisk-naturvitenskapelige universitet)",
            "category": "higher_education",
            "name_variants": [
                "norges teknisk-naturvitenskapelige universitet",
                "ntnu",
                "universitetet i trondheim",
                "nth",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1910, 2099),
            # NTNU block grant ~4-5B NOK in recent years.
            "max_amount_local": 8_000_000_000,
            "notes": "Renamed NTNU in 1996 (previously NTH + other Trondheim institutions).",
        },
        {
            "canonical_name": "Universitetet i Oslo (UiO)",
            "category": "higher_education",
            "name_variants": [
                "universitetet i oslo",
                "university of oslo",
                "uio",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1811, 2099),
            # UiO block grant ~4-5B NOK in recent years.
            "max_amount_local": 8_000_000_000,
        },
        {
            "canonical_name": "Universitetet i Bergen (UiB)",
            "category": "higher_education",
            "name_variants": [
                "universitetet i bergen",
                "university of bergen",
                "uib",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1946, 2099),
            "max_amount_local": 5_000_000_000,
        },
        {
            "canonical_name": "UiT Norges Arktiske Universitet",
            "category": "higher_education",
            "name_variants": [
                "uit \u2013 norges arktiske universitet",
                "uit norges arktiske universitet",
                "universitetet i troms\u00f8",
                "universitetet i tromso",
                "uit",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1968, 2099),
            "max_amount_local": 4_000_000_000,
            "notes": "Renamed UiT - Norges Arktiske Universitet in 2013.",
        },
        {
            "canonical_name": "Norges Handelshoyskole (NHH)",
            "category": "higher_education",
            "name_variants": [
                "norges handelsh\u00f8yskole",
                "norges handelshoyskole",
                "nhh",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1936, 2099),
            "max_amount_local": 2_000_000_000,
        },
        {
            "canonical_name": "Universitetene (collective)",
            "category": "higher_education",
            "name_variants": [
                "universitetene",
                "universities and university colleges",
                "universiteter og h\u00f8yskoler",
                "universiteter og hoygskoler",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1970, 2099),
            "notes": "Collective line — use only when individual lines unavailable.",
        },

        # --- Applied research institutes ---
        {
            "canonical_name": "SINTEF",
            "category": "science_agency",
            "name_variants": [
                "sintef",
                "selskapet for industriell og teknisk forskning",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1950, 2099),
            # SINTEF state grant peaked ~300M NOK. Scanned-doc section totals can
            # be 10-20× inflated.
            "max_amount_local": 1_000_000_000,
        },
        {
            "canonical_name": "Havforskningsinstituttet (IMR)",
            "category": "science_agency",
            "name_variants": [
                "havforskningsinstituttet",
                "institute of marine research",
                "imr",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (1900, 2099),
            # IMR budget ~1.2-1.5B NOK in recent years.
            "max_amount_local": 3_000_000_000,
        },
        {
            "canonical_name": "Folkehelseinstituttet (FHI)",
            "category": "science_agency",
            "name_variants": [
                "folkehelseinstituttet",
                "norwegian institute of public health",
                "fhi",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (2002, 2099),
            # FHI budget ~1.5-1.6B NOK in recent years.
            "max_amount_local": 3_000_000_000,
        },
        {
            "canonical_name": "Meteorologisk institutt",
            "category": "science_agency",
            "name_variants": [
                "meteorologisk institutt",
                "norwegian meteorological institute",
                "met.no",
                "met norge",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (1866, 2099),
            # Met.no operational budget ~300-600M NOK per year.
            # "Sum Fellesoppgaver" program totals can inflate to 1-2B if they
            # include international commitments — block with 1B cap.
            "max_amount_local": 1_000_000_000,
        },
        {
            "canonical_name": "Norsk Romsenter",
            "category": "science_agency",
            "name_variants": [
                "norsk romsenter",
                "norwegian space centre",
                "norwegian space agency",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (1987, 2099),
            # Norwegian Space Centre budget ~200-400M NOK; ESA contributions add
            # ~1B+ in some years.
            "max_amount_local": 3_000_000_000,
        },
    ],

    # -----------------------------------------------------------------------
    # NETHERLANDS
    # Source: Rijksbegroting per-ministry files (2002+) or Miljoenennota (pre-2002).
    # Key R&D agencies: NWO, KNAW, TNO, Dutch universities (OCW Art. 07),
    # STW/NWO-TTW, RVO/Senter (innovation instruments), WUR/LNV agri-research.
    # ERA SPLIT: NLG millions (pre-2002) → EUR thousands (2002+).
    # -----------------------------------------------------------------------
    "Netherlands": [
        # --- Primary research funders ---
        {
            "canonical_name": "NWO (Dutch Research Council)",
            "category": "science_agency",
            "name_variants": [
                "nwo",
                "nederlandse organisatie voor wetenschappelijk onderzoek",
                "dutch research council",
                # ZWO = predecessor to NWO (1950-1988)
                "zwo",
                "organisatie voor zuiver-wetenschappelijk onderzoek",
                "zuiver-wetenschappelijk onderzoek",
                "dutch organization for pure scientific research",
                "pure scientific research",
                "subsidy from zwo",
                "subsidie zwo",
                "contribution to nwo",
                "bijdrage nwo",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1950, 2099),
            # Pre-2002: plausible max ~1B NLG (expanded); post-2001: ~2B EUR (expanded)
            "max_amount_local": 3_000_000_000,   # 3B NLG or EUR base units
        },
        {
            "canonical_name": "KNAW (Royal Netherlands Academy)",
            "category": "science_agency",
            "name_variants": [
                "knaw",
                "koninklijke nederlandse akademie van wetenschappen",
                "royal netherlands academy",
                "royal netherlands academy of arts and sciences",
                "contribution to knaw",
                "bijdrage knaw",
                "subsidies knaw",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1808, 2099),
            "max_amount_local": 1_000_000_000,   # 1B NLG or EUR
        },
        {
            "canonical_name": "TNO (Netherlands Organisation for Applied Research)",
            "category": "science_agency",
            "name_variants": [
                "tno",
                "toegepast natuurwetenschappelijk onderzoek",
                "netherlands organisation for applied scientific research",
                "subsidie tno",
                "subsidy for tno",
                "subsidy tno",
                "tno research",
                "tno subsidies",
                "contribution to tno",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1932, 2099),
            "max_amount_local": 3_000_000_000,   # 3B NLG or EUR
        },
        {
            "canonical_name": "NWO-TTW / STW (Technology Foundation)",
            "category": "science_agency",
            "name_variants": [
                "nwo-ttw",
                "stw",
                "technologiestichting stw",
                "technology foundation",
                "applied and engineering sciences",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (1981, 2017),
            "max_amount_local": 1_000_000_000,   # 1B NLG or EUR
            "notes": "STW created 1981, merged into NWO as NWO-TTW in 2017.",
        },

        # --- OCW Article totals (ministry section) ---
        {
            "canonical_name": "OCW Art. 07 Wetenschappelijk onderwijs (universities)",
            "category": "higher_education",
            "name_variants": [
                "wetenschappelijk onderwijs",
                "art. 07",
                "artikel 7",
                "article 7",
                "hoger onderwijs en onderzoek",
                "universiteiten",
                # Budget memorandum variants (1975-2001 narrative documents)
                "scientific education",
                "total scientific education",
                "investments in scientific education",
                "expenditures for scientific education",
                "universities and colleges",
                "hoger onderwijs",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1975, 2099),
            # OCW university article: up to ~10B NLG or EUR (large aggregate)
            "max_amount_local": 15_000_000_000,  # 15B NLG or EUR
            "notes": "OCW ministry article for university block grants. Use when individual lines unavailable.",
        },
        {
            "canonical_name": "OCW Art. 16 Onderzoek en wetenschapsbeleid",
            "category": "science_agency",
            "name_variants": [
                "onderzoek en wetenschapsbeleid",
                "art. 16",
                "artikel 16",
                "research and science policy",
                # 2002-2010 variants used before article-number reporting became standard
                "research and sciences",
                "onderzoek en wetenschapsbeleid",
                "research policy",
                "onderzoekbeleid",
                "fundamental scientific research",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1975, 2099),
            "max_amount_local": 5_000_000_000,   # 5B EUR
            "notes": "OCW research-policy aggregate. Pre-2002 budget memoranda use narrative labels; 2002+ this corresponds to Art. 16 containing NWO, KNAW, SURF appropriations.",
        },

        # --- Individual universities ---
        {
            "canonical_name": "Universiteit van Amsterdam (UvA)",
            "category": "higher_education",
            "name_variants": [
                "universiteit van amsterdam",
                "uva",
                "university of amsterdam",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1877, 2099),
            "max_amount_local": 3_000_000_000,   # 3B NLG or EUR
        },
        {
            "canonical_name": "Vrije Universiteit Amsterdam (VU)",
            "category": "higher_education",
            "name_variants": [
                "vrije universiteit amsterdam",
                "vrije universiteit",
                "vu amsterdam",
                " vu ",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1880, 2099),
            "max_amount_local": 3_000_000_000,   # 3B NLG or EUR
        },
        {
            "canonical_name": "Universiteit Leiden",
            "category": "higher_education",
            "name_variants": [
                "universiteit leiden",
                "leiden universiteit",
                "leiden university",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1575, 2099),
            "max_amount_local": 3_000_000_000,   # 3B NLG or EUR
        },
        {
            "canonical_name": "Universiteit Utrecht (UU)",
            "category": "higher_education",
            "name_variants": [
                "universiteit utrecht",
                "utrecht university",
                " uu ",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1636, 2099),
            "max_amount_local": 3_000_000_000,   # 3B NLG or EUR
        },
        {
            "canonical_name": "Rijksuniversiteit Groningen (RUG)",
            "category": "higher_education",
            "name_variants": [
                "rijksuniversiteit groningen",
                "universiteit groningen",
                "rug",
                "university of groningen",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1614, 2099),
            "max_amount_local": 3_000_000_000,   # 3B NLG or EUR
        },
        {
            "canonical_name": "TU Delft (Delft University of Technology)",
            "category": "higher_education",
            "name_variants": [
                "tu delft",
                "technische universiteit delft",
                "delft university of technology",
                "delft",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1842, 2099),
            "max_amount_local": 3_000_000_000,   # 3B NLG or EUR
        },
        {
            "canonical_name": "TU Eindhoven (TU/e)",
            "category": "higher_education",
            "name_variants": [
                "tu/e",
                "tue",
                "technische universiteit eindhoven",
                "eindhoven university of technology",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1956, 2099),
            "max_amount_local": 3_000_000_000,   # 3B NLG or EUR
        },
        {
            "canonical_name": "Wageningen Universiteit (WUR)",
            "category": "higher_education",
            "name_variants": [
                "wageningen universiteit",
                "wageningen university",
                "wur",
                "landbouwuniversiteit wageningen",
                "wageningen research",
                "contribution to wageningen research",
                "landbouwhogeschool wageningen",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1918, 2099),
            "max_amount_local": 3_000_000_000,   # 3B NLG or EUR
        },
        {
            "canonical_name": "Erasmus Universiteit Rotterdam (EUR)",
            "category": "higher_education",
            "name_variants": [
                "erasmus universiteit rotterdam",
                "erasmus university rotterdam",
                "erasmus universiteit",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1913, 2099),
            "max_amount_local": 3_000_000_000,   # 3B NLG or EUR
        },
        {
            "canonical_name": "Radboud Universiteit Nijmegen",
            "category": "higher_education",
            "name_variants": [
                "radboud universiteit",
                "radboud university",
                "katholieke universiteit nijmegen",
                "kun",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1923, 2099),
            "max_amount_local": 3_000_000_000,   # 3B NLG or EUR
        },

        # --- Applied / sectoral research institutes ---
        {
            "canonical_name": "RIVM (National Institute for Public Health)",
            "category": "science_agency",
            "name_variants": [
                "rivm",
                "rijksinstituut voor volksgezondheid",
                "national institute for public health",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (1900, 2099),
            "max_amount_local": 1_000_000_000,   # 1B NLG or EUR
        },
        {
            "canonical_name": "NIOZ (Royal Netherlands Institute for Sea Research)",
            "category": "science_agency",
            "name_variants": [
                "nioz",
                "koninklijk nederlands instituut voor onderzoek der zee",
                "royal netherlands institute for sea research",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (1876, 2099),
            "max_amount_local": 200_000_000,   # 200M NLG or EUR
        },
        {
            "canonical_name": "Deltares (water and subsurface research)",
            "category": "science_agency",
            "name_variants": [
                "deltares",
                "waterloopkundig laboratorium",
                "delft hydraulics",
                "foundation for hydraulic engineering laboratory",
                "stichting waterbouwkundig laboratorium",
                "swl",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (1927, 2099),
            "max_amount_local": 500_000_000,   # 500M NLG or EUR
        },
        {
            "canonical_name": "KNMI (Royal Netherlands Meteorological Institute)",
            "category": "science_agency",
            "name_variants": [
                "knmi",
                "koninklijk nederlands meteorologisch instituut",
                "royal netherlands meteorological institute",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (1854, 2099),
            "max_amount_local": 300_000_000,   # 300M NLG or EUR
        },

        # --- NLR (National Aerospace Laboratory) ---
        {
            "canonical_name": "NLR (National Aerospace Laboratory)",
            "category": "science_agency",
            "name_variants": [
                "nlr",
                "nationaal lucht- en ruimtevaartlaboratorium",
                "national aerospace laboratory",
                "luchtvaartlaboratorium",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (1937, 2099),
            "max_amount_local": 300_000_000,   # 300M NLG or EUR
        },

        # --- ECN (Energy Research Centre Netherlands) ---
        {
            "canonical_name": "ECN (Energy Research Centre Netherlands)",
            "category": "science_agency",
            "name_variants": [
                "ecn",
                "energy research centre of the netherlands",
                "energy research center netherlands",
                "energy research centre netherlands",
                "energieonderzoek centrum nederland",
                "energy center netherlands",
                "energy centre netherlands",
                "reactor center netherlands",
                "reactor centre netherlands",
                "reactor centrum nederland",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (1975, 2019),  # ECN merged into TNO in 2019
            "max_amount_local": 500_000_000,   # 500M NLG or EUR
            "notes": "ECN founded 1976; merged into TNO as TNO Energy & Materials Transition in 2019.",
        },

        # --- EZ innovation instruments ---
        {
            "canonical_name": "EZ Art. 02 Bedrijvenbeleid / innovatie (incl. TNO)",
            "category": "innovation_instruments",
            "name_variants": [
                "bedrijvenbeleid",
                "innovatie en ondernemerschap",
                "ez art. 02",
                "art. 02",
                "topsectoren",
                "topconsortia voor kennis en innovatie",
                "tki",
                # Historical EZ R&D programme labels (1979-2001)
                "knowledge and innovation",
                "kennis en innovatie",
                "knowledge stimulation",
                "kennisstimulering",
                "knowledge development and innovation",
                "innovation and entrepreneurship",
                "innovation-oriented research",
                "specific business-oriented technology",
                "instir",
                "innovation stimulation scheme",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1975, 2099),
            "max_amount_local": 5_000_000_000,   # 5B EUR
            "notes": "EZ article for enterprise/innovation policy; historically used for EZ R&D programmes 1979-2001.",
        },
        {
            "canonical_name": "RVO / Senter / SenterNovem (innovation instruments)",
            "category": "innovation_instruments",
            "name_variants": [
                "rvo",
                "rijksdienst voor ondernemend nederland",
                "senternovem",
                "senter",
                "novem",
                "wbso",
                "r&d wage tax",
                "fiscal r&d wage",
                "structural r&d facility",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1994, 2099),
            "max_amount_local": 5_000_000_000,   # 5B EUR
            "notes": "Senter (1994) → SenterNovem (2004) → merged into RVO (2014). Administers WBSO.",
        },
    ],

    # -----------------------------------------------------------------------
    # SWITZERLAND
    # Source: Bundesblatt Bundesbeschluss (1975-2020, aggregate) or VA-Band3 (2021+).
    # Key R&D agencies: ETH-Bereich (block grant to ETH Domain), SNF, Innosuisse/KTI,
    # CERN contribution, ESA contributions, Agroscope.
    # UNIT: always FULL CHF. Space = thousands separator.
    # -----------------------------------------------------------------------
    "Switzerland": [
        # --- ETH Domain (Bereich ETH) — primary R&D vehicle ---
        {
            "canonical_name": "ETH-Bereich (ETH Domain block grant)",
            "category": "science_agency",
            "name_variants": [
                "eth-bereich",
                "bereich der eidgen\u00f6ssischen technischen hochschulen",
                "eth domain",
                "beitrag an den eth-bereich",
                "bundesbeitrag eth-bereich",
                "eth bereich",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1969, 2099),
            # After CHF correction (÷1000), correct values are 2–5B CHF.
            "max_amount_local": 10_000_000_000,   # 10B CHF
            "notes": "Annual block grant to the ETH Domain — the single largest Swiss federal R&D line.",
        },
        {
            "canonical_name": "ETH Z\u00fcrich",
            "category": "higher_education",
            "name_variants": [
                "eth z\u00fcrich",
                "eth zurich",
                "eidgen\u00f6ssische technische hochschule z\u00fcrich",
                "ethz",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1855, 2099),
            "max_amount_local": 3_000_000_000,   # 3B CHF
        },
        {
            "canonical_name": "EPFL (Lausanne)",
            "category": "higher_education",
            "name_variants": [
                "epfl",
                "ecole polytechnique f\u00e9d\u00e9rale de lausanne",
                "\u00e9cole polytechnique f\u00e9d\u00e9rale de lausanne",
                "eidgen\u00f6ssische technische hochschule lausanne",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1969, 2099),
            "max_amount_local": 3_000_000_000,   # 3B CHF
        },
        {
            "canonical_name": "PSI (Paul Scherrer Institut)",
            "category": "science_agency",
            "name_variants": [
                "psi",
                "paul scherrer institut",
                "paul scherrer institute",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1988, 2099),
            "max_amount_local": 500_000_000,   # 500M CHF
            "notes": "PSI created 1988 (merger of EIR and SIN).",
        },
        {
            "canonical_name": "Empa (Federal Materials Testing Institute)",
            "category": "science_agency",
            "name_variants": [
                "empa",
                "eidg. materialpr\u00fcfungs",
                "eidgen\u00f6ssische materialpr\u00fcfungs",
                "federal laboratories for materials science",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1880, 2099),
            "max_amount_local": 500_000_000,   # 500M CHF
        },
        {
            "canonical_name": "Eawag (Water Research Institute)",
            "category": "science_agency",
            "name_variants": [
                "eawag",
                "wasserforschungs-institut",
                "swiss federal institute of aquatic science",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1936, 2099),
            "max_amount_local": 500_000_000,   # 500M CHF
        },
        {
            "canonical_name": "WSL (Forest, Snow and Landscape Research)",
            "category": "science_agency",
            "name_variants": [
                "wsl",
                "eidg. forschungsanstalt f\u00fcr wald",
                "swiss federal institute for forest",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1885, 2099),
            "max_amount_local": 500_000_000,   # 500M CHF
        },

        # --- Research funding agencies ---
        {
            "canonical_name": "SNF / SNSF (Swiss National Science Foundation)",
            "category": "science_agency",
            "name_variants": [
                "snf",
                "snsf",
                "schweizerischer nationalfonds",
                "swiss national science foundation",
                "beitrag an den snf",
                "nationalfonds",
            ],
            "preferred_item_type": ["line_item", "section_total", "program_total"],
            "active_years": (1952, 2099),
            "max_amount_local": 2_000_000_000,   # 2B CHF
        },
        {
            "canonical_name": "Innosuisse / KTI (Swiss Innovation Agency)",
            "category": "innovation_instruments",
            "name_variants": [
                "innosuisse",
                "kti",
                "kommission f\u00fcr technologie und innovation",
                "commission for technology and innovation",
                "schweizerische agentur f\u00fcr innovationsf\u00f6rderung",
                "beitrag an innosuisse",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1943, 2099),
            "max_amount_local": 1_000_000_000,   # 1B CHF
            "notes": "KTI (Förderungskommission für angewandte Forschung und Entwicklung, from 1943) "
                     "renamed Innosuisse 2018.",
        },

        # --- International science contributions ---
        {
            "canonical_name": "CERN contribution (Swiss)",
            "category": "science_agency",
            "name_variants": [
                "cern",
                "organisation europ\u00e9enne pour la recherche nucl\u00e9aire",
                "europ\u00e4ische organisation f\u00fcr kernforschung",
                "beitrag an cern",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (1954, 2099),
            "max_amount_local": 500_000_000,   # 500M CHF
        },
        {
            "canonical_name": "ESA contributions (Swiss)",
            "category": "science_agency",
            "name_variants": [
                "esa-beitr\u00e4ge",
                "esa beitrag",
                "europ\u00e4ische weltraumorganisation",
                "european space agency",
                "beitr\u00e4ge an esa",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (1975, 2099),
            "max_amount_local": 500_000_000,   # 500M CHF
        },

        # --- Applied research ---
        {
            "canonical_name": "Agroscope (federal agricultural research)",
            "category": "science_agency",
            "name_variants": [
                "agroscope",
                "bundesamt f\u00fcr landwirtschaft forschung",
                "forschungsanstalten agroscope",
            ],
            "preferred_item_type": ["line_item", "section_total"],
            "active_years": (2013, 2099),
            "max_amount_local": 500_000_000,   # 500M CHF
            "notes": "Agroscope brand created 2013 (merger of former federal agricultural research stations).",
        },
        {
            "canonical_name": "SBFI / SERI (State Secretariat for Education, Research and Innovation)",
            "category": "science_agency",
            "name_variants": [
                "sbfi",
                "seri",
                "staatssekretariat f\u00fcr bildung, forschung und innovation",
                "state secretariat for education, research and innovation",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (2013, 2099),
            "max_amount_local": 20_000_000_000,  # 20B CHF (section-level aggregate)
            "notes": "SBFI / SERI created January 2013 (renamed from BBT/OPET). "
                     "Section-level aggregate for WBF R&D programmes.",
        },
    ],

    # -----------------------------------------------------------------------
    # CANADA
    # Source: Main Estimates / Supplementary Estimates (Mains, Supps A/B/C)
    # Key R&D agencies: granting councils (NSERC, SSHRC, CIHR), NRC, CFI,
    # AECL/CNL, DRDC, IRAP
    # -----------------------------------------------------------------------
    "Canada": [
        {
            "canonical_name": "NSERC",
            "category": "science_agency",
            "name_variants": [
                "natural sciences and engineering research council",
                "nserc",
                "conseil de recherches en sciences naturelles",
                "crsng",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1978, 2099),
            "notes": "Created 1978 from NRC grants function.",
        },
        {
            "canonical_name": "SSHRC",
            "category": "science_agency",
            "name_variants": [
                "social sciences and humanities research council",
                "sshrc",
                "conseil de recherches en sciences humaines",
                "crsh",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (2020, 2099),
            "notes": "Created 1977.",
        },
        {
            "canonical_name": "CIHR",
            "category": "science_agency",
            "name_variants": [
                "canadian institutes of health research",
                "cihr",
                "instituts de recherche en santé du canada",
                "irsc",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (2000, 2099),
            "notes": "Replaced Medical Research Council in 2000.",
        },
        {
            "canonical_name": "National Research Council (NRC)",
            "category": "science_agency",
            "name_variants": [
                "national research council",
                "conseil national de recherches",
                "nrc canada",
                "cnrc",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1916, 2099),
            "notes": "Includes IRAP (Industrial Research Assistance Program).",
        },
        {
            "canonical_name": "Canada Foundation for Innovation (CFI)",
            "category": "research_infrastructure",
            "name_variants": [
                "canada foundation for innovation",
                "fondation canadienne pour l'innovation",
                "cfi",
                "fci",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1997, 2099),
            "notes": "Funds research infrastructure at universities and hospitals.",
        },
        {
            "canonical_name": "AECL / Canadian Nuclear Laboratories",
            "category": "science_agency",
            "name_variants": [
                "atomic energy of canada",
                "énergie atomique du canada",
                "aecl",
                "eacl",
                "canadian nuclear laboratories",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1952, 2099),
            "notes": "AECL privatised operations to CNL in 2015; federal appropriation continues.",
        },
        {
            "canonical_name": "Defence Research and Development Canada (DRDC)",
            "category": "science_agency",
            "name_variants": [
                "defence research and development canada",
                "recherche et développement pour la défense canada",
                "drdc",
                "rddc",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1947, 2099),
        },
        {
            "canonical_name": "Medical Research Council (MRC Canada)",
            "category": "science_agency",
            "name_variants": [
                "medical research council",
                "medical research council of canada",
                "conseil de recherches médicales",
                "mrc canada",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1960, 2000),
            "notes": "Replaced by CIHR in 2000.",
        },
    ],

    # -----------------------------------------------------------------------
    # UK
    # Source: Main Supply Estimates / Science Budget (HM Treasury)
    # Key agencies: Research Councils (pre-2018), UKRI (post-2018 merger),
    # individual councils, Innovate UK, BEIS/DSIT science budget
    # -----------------------------------------------------------------------
    "UK": [
        {
            # -----------------------------------------------------------------
            # Headline "total government science/S&T spending" figure.
            #
            # AUDIT FINDING (2026-07): The Financial Statement and Budget
            # Report ("Red Book") is the SAME document series every year from
            # 1975-2025 — confirmed by inspecting page-1 headers across all 55
            # source files. From 1994 onward it periodically states a single
            # headline total, e.g.:
            #   1994: "Total central government spending on science and
            #          technology ... 1995-96 will be about £6.1 billion"
            #   1995: "Total central government spending on science and
            #          technology in 1996-97 is expected to be about £6 billion"
            #   1996: "Total central government spending on Science and
            #          Technology in 1997-98 is expected to be about £6 billion"
            #   2006/2007: "total UK science spending will be £5.4 billion"
            #
            # Despite near-identical phrasing across years, extraction was
            # inconsistent: 1995's identical-pattern sentence was missed
            # entirely (0 rows extracted that year); 1994/1996/2006 rows were
            # extracted but stuck in decision=review; 2006 vs 2007 same phrase
            # got different item_type from the LLM (section_total vs
            # line_item), and the UK cleaner's blanket "section_total -> review"
            # rule then silently dropped 2006 but let 2007 through. This is the
            # single clearest evidence of the "documents are consistent, the
            # algorithm wasn't" issue flagged by a peer reviewer.
            #
            # This is a TOP-LINE AGGREGATE, not additive with the individual
            # research-council / UKRI / fund lines below — do not sum this
            # series with other UK canonical series for the same year.
            # -----------------------------------------------------------------
            "canonical_name": "Total UK Science & Technology Spending (headline, HMT Budget)",
            "category": "national_total",
            "name_variants": [
                "total uk science spending",
                "total central government spending on science and technology",
                "total central government spending on civil science and technology",
            ],
            "preferred_item_type": ["line_item", "section_total", "program_total"],
            "preferred_match_groups": [[
                r"total uk science spending",
                r"total central government spending on (civil )?science and technology",
                r"total planned central government spending on (civil )?science and technology",
            ]],
            "enforce_preferred_match_groups": True,
            "strict_preferred_item_types": False,
            "active_years": (1975, 2099),
            "notes": (
                "Headline annual total government science/S&T spending figure, as "
                "announced in the Budget Red Book narrative (not a Supply Estimates "
                "line). Kept as a separate backbone series for cross-year "
                "comparability given sparse agency-level coverage pre-2010. NOT "
                "additive with other UK canonical series in the same year — do not "
                "sum into a country-year R&D total without deduplicating."
            ),
        },
        {
            "canonical_name": "UKRI (UK Research and Innovation)",
            "category": "science_agency",
            "name_variants": [
                "uk research and innovation",
                "ukri",
                "united kingdom research and innovation",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "preferred_match_groups": [
                [
                    r"total operating budget for ukri",
                    r"total budget for uk research and innovation",
                    r"total funding for uk research and innovation",
                    r"science budget allocation",
                    r"total resource del for ukri",
                ],
                _uk_exactish_match_group(
                    "uk research and innovation",
                    "ukri",
                ),
            ],
            "enforce_preferred_match_groups": True,
            "strict_preferred_item_types": True,
            "exclude_match_groups": [[
                r"fishing industr",
                r"industrial strategy challenge fund",
                r"strength in places fund",
                r"public r&d investment",
                r"national supercomputer",
                r"of which",
                r"directing £?9 billion",
            ]],
            "strict_exclude_match_groups": True,
            "active_years": (2018, 2099),
            "notes": "Created April 2018 merging 7 Research Councils + Innovate UK + RE.",
        },
        {
            "canonical_name": "Research Councils (pre-UKRI)",
            "category": "science_agency",
            "name_variants": [
                "research councils uk",
                "rcuk",
                "science and research councils",
                "research councils",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "preferred_match_groups": [[
                r"spending on the science base",
                r"research councils uk",
                r"science and research councils",
                r"research councils.*budget",
                r"research councils.*science base",
            ]],
            "enforce_preferred_match_groups": True,
            "strict_preferred_item_types": True,
            "exclude_match_groups": [[
                r"clinical research",
                r"crick institute",
                r"knowledge transfer",
                r"tsb programmes",
                r"research capital",
                r"funding for research councils",
            ]],
            "strict_exclude_match_groups": True,
            "active_years": (1965, 2018),
            "notes": "Umbrella term for the 7 research councils before UKRI.",
        },
        {
            "canonical_name": "Medical Research Council (MRC)",
            "category": "science_agency",
            "name_variants": [
                "medical research council",
                "mrc",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "preferred_match_groups": [
                _uk_exactish_match_group("medical research council", "mrc"),
            ],
            "enforce_preferred_match_groups": True,
            "strict_preferred_item_types": True,
            "exclude_match_groups": [[
                r"clinical research",
                r"crick institute",
                r"jodrell bank",
                r"sale of .* assets",
            ]],
            "strict_exclude_match_groups": True,
            "active_years": (1913, 2099),
            "notes": "One of the original research councils; continues as MRC within UKRI.",
        },
        {
            "canonical_name": "Engineering and Physical Sciences Research Council (EPSRC)",
            "category": "science_agency",
            "name_variants": [
                "engineering and physical sciences research council",
                "epsrc",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "strict_preferred_item_types": True,
            "active_years": (1994, 2099),
            "notes": "Split from SERC in 1994.",
        },
        {
            "canonical_name": "Biotechnology and Biological Sciences Research Council (BBSRC)",
            "category": "science_agency",
            "name_variants": [
                "biotechnology and biological sciences research council",
                "bbsrc",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "strict_preferred_item_types": True,
            "active_years": (1994, 2099),
        },
        {
            "canonical_name": "Natural Environment Research Council (NERC)",
            "category": "science_agency",
            "name_variants": [
                "natural environment research council",
                "nerc",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "strict_preferred_item_types": True,
            "active_years": (1965, 2099),
        },
        {
            "canonical_name": "Economic and Social Research Council (ESRC)",
            "category": "science_agency",
            "name_variants": [
                "economic and social research council",
                "esrc",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "strict_preferred_item_types": True,
            "active_years": (1965, 2099),
        },
        {
            "canonical_name": "Science and Technology Facilities Council (STFC)",
            "category": "science_agency",
            "name_variants": [
                "science and technology facilities council",
                "stfc",
                "particle physics and astronomy research council",
                "pparc",
                "council for the central laboratory of the research councils",
                "cclrc",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "strict_preferred_item_types": True,
            "active_years": (1994, 2099),
            "notes": "Formed 2007 from PPARC + CCLRC.",
        },
        {
            "canonical_name": "Arts and Humanities Research Council (AHRC)",
            "category": "science_agency",
            "name_variants": [
                "arts and humanities research council",
                "ahrc",
                "arts and humanities research board",
                "ahrb",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "strict_preferred_item_types": True,
            "active_years": (1998, 2099),
        },
        {
            "canonical_name": "Innovate UK",
            "category": "innovation_instruments",
            "name_variants": [
                "innovate uk",
                "technology strategy board",
                "tsb",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "preferred_match_groups": [
                [
                    r"of which:\s*innovate uk",
                    r"core innovate uk programmes",
                    r"technology strategy board",
                    r"innovate uk",
                ]
            ],
            "enforce_preferred_match_groups": True,
            "strict_preferred_item_types": True,
            "exclude_match_groups": [[
                r"growth catalyst",
                r"creative industries",
            ]],
            "strict_exclude_match_groups": True,
            "active_years": (2004, 2099),
            "notes": "Technology Strategy Board renamed Innovate UK in 2014.",
        },
        {
            "canonical_name": "Research England (RE)",
            "category": "higher_education",
            "name_variants": [
                "research england",
                "higher education funding council",
                "hefce",
                "heif",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "preferred_match_groups": [
                _uk_exactish_match_group("research england", "higher education funding council", "hefce", "heif"),
            ],
            "enforce_preferred_match_groups": True,
            "strict_preferred_item_types": True,
            "active_years": (1992, 2099),
            "notes": "HEFCE research funding; became Research England within UKRI in 2018.",
        },
        {
            "canonical_name": "Science Budget",
            "category": "rd_programme",
            "name_variants": [
                "science budget",
                "science budget allocation",
                "total budget for science and innovation",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "preferred_match_groups": [[
                r"science budget allocation",
                r"science budget$",
                r"total budget for science and innovation",
                r"dti science budget",
            ]],
            "enforce_preferred_match_groups": True,
            "active_years": (1999, 2099),
            "notes": "UK aggregate science budget / science and innovation budget line reported in budgets and spending reviews.",
        },
        {
            "canonical_name": "Public R&D Investment",
            "category": "rd_programme",
            "name_variants": [
                "public r&d investment",
                "total public r&d investment",
                "total capital del expenditure on r&d",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "preferred_match_groups": [[
                r"public r&d investment",
                r"total public r&d investment",
                r"total capital del expenditure on r&d",
            ]],
            "enforce_preferred_match_groups": True,
            "active_years": (2020, 2099),
        },
        {
            "canonical_name": "Core Research",
            "category": "rd_programme",
            "name_variants": [
                "core research",
                "support for core research",
                "increase to core funding for the uk’s world-leading universities and research institutions",
                "increase to core funding for the uk's world-leading universities and research institutions",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "preferred_match_groups": [[
                r"of which:\s*core research",
                r"support for core research",
                r"increase to core funding for the uk.?s world-leading universities and research institutions",
            ]],
            "enforce_preferred_match_groups": True,
            "active_years": (2021, 2099),
        },
        {
            "canonical_name": "Advanced Nuclear Fund",
            "category": "innovation_instruments",
            "name_variants": [
                "advanced nuclear fund",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"advanced nuclear fund"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2021, 2099),
        },
        {
            "canonical_name": "Advanced Research and Invention Agency",
            "category": "innovation_instruments",
            "name_variants": [
                "advanced research and invention agency",
                "aria",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[
                r"advanced research and invention agency",
                r"\baria\b",
            ]],
            "enforce_preferred_match_groups": True,
            "active_years": (2021, 2099),
        },
        {
            "canonical_name": "Exascale supercomputer and Artificial Intelligence Research Resource",
            "category": "innovation_instruments",
            "name_variants": [
                "exascale supercomputer and artificial intelligence research resource",
                "exascale supercomputer and ai research resource",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[
                r"exascale supercomputer and artificial intelligence",
                r"exascale supercomputer and ai research resource",
            ]],
            "enforce_preferred_match_groups": True,
            "active_years": (2023, 2099),
        },
        {
            "canonical_name": "Life Sciences Manufacturing Funding",
            "category": "innovation_instruments",
            "name_variants": [
                "life sciences manufacturing funding",
                "life sciences innovative manufacturing fund",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[
                r"life sciences manufacturing funding",
                r"life sciences innovative manufacturing fund",
                r"new funding for life sciences manufacturing",
            ]],
            "enforce_preferred_match_groups": True,
            "active_years": (2024, 2099),
        },
        {
            "canonical_name": "National Productivity Investment Fund",
            "category": "rd_programme",
            "name_variants": [
                "national productivity investment fund",
                "npif",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "preferred_match_groups": [[
                r"national productivity investment fund",
                r"\bnpif\b",
            ]],
            "enforce_preferred_match_groups": True,
            "active_years": (2018, 2099),
        },
        {
            "canonical_name": "Quantum Technologies Development and Commercialisation Fund",
            "category": "innovation_instruments",
            "name_variants": [
                "quantum technologies development and commercialisation fund",
                "investment for quantum technologies",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[
                r"development and commercialisation of quantum technologies",
                r"investment for quantum technologies",
            ]],
            "enforce_preferred_match_groups": True,
            "active_years": (2018, 2099),
        },
        {
            "canonical_name": "Research Partnership Investment Fund",
            "category": "innovation_instruments",
            "name_variants": [
                "research partnership investment fund",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"research partnership investment fund"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2012, 2099),
        },
        {
            "canonical_name": "UK-wide R&D Funding for Low and Zero Emission Transport Technologies",
            "category": "innovation_instruments",
            "name_variants": [
                "uk-wide r&d funding for low and zero emission transport technologies",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"low and zero emission transport technologies"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2021, 2099),
        },
        {
            "canonical_name": "University Challenge scheme",
            "category": "innovation_instruments",
            "name_variants": [
                "university challenge scheme",
                "university challenge fund",
                "university challenge funding",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"university challenge (scheme|fund|funding)"]],
            "enforce_preferred_match_groups": True,
            "active_years": (1998, 2099),
        },
        # ---------------------------------------------------------------
        # Newly-unlocked named one-off programmes (audit 2026-07): these
        # rows were previously stuck in decision=review purely because
        # their description happened to start with "£X million/billion"
        # (the removed _STARTS_WITH_AMOUNT heuristic in the UK cleaner —
        # see budget/cleaners/united_kingdom.py _is_multi_year()). Each is
        # a specific, named, single-year Budget announcement, verified
        # against source text.
        # ---------------------------------------------------------------
        {
            "canonical_name": "Science Enterprise Challenge",
            "category": "innovation_instruments",
            "name_variants": ["science enterprise challenge"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"science enterprise challenge"]],
            "enforce_preferred_match_groups": True,
            "active_years": (1999, 2099),
            "notes": "Budget 1999: £25 million Science Enterprise Challenge, verified p.~1999_UK.pdf.",
        },
        {
            "canonical_name": "Joint Infrastructure Fund",
            "category": "research_infrastructure",
            "name_variants": ["joint infrastructure fund"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"joint infrastructure fund"]],
            "enforce_preferred_match_groups": True,
            "active_years": (1999, 2099),
            "notes": "Budget 1999: £600 million Joint Infrastructure Fund (Government + Wellcome Trust), verified 1999_UK.pdf.",
        },
        {
            "canonical_name": "Institute of Web Science",
            "category": "research_institute",
            "name_variants": ["institute of web science"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"institute of web science"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2010, 2099),
            "notes": "Budget 2010: £30 million funding for the Institute of Web Science, verified 2010_03_UK.pdf.",
        },
        {
            "canonical_name": "Advanced Manufacturing Supply Chain Initiative",
            "category": "innovation_instruments",
            "name_variants": ["advanced manufacturing supply chain initiative"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"advanced manufacturing supply chain initiative"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2012, 2099),
            "notes": "Budget 2012: £125 million Advanced Manufacturing Supply Chain Initiative, verified 2012_UK.pdf.",
        },
        {
            "canonical_name": "UK Collaboration for Research in Infrastructure and Cities (UKCRIC)",
            "category": "research_infrastructure",
            "name_variants": [
                "uk collaboration for research in infrastructure and cities",
                "ukcric",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[
                r"uk collaboration for research in infrastructure and cities",
                r"\bukcric\b",
            ]],
            "enforce_preferred_match_groups": True,
            "active_years": (2015, 2099),
            "notes": "Budget 2015 (July): £128 million UKCRIC, verified 2015_07_UK.pdf.",
        },
        {
            "canonical_name": "Energy Research Accelerator",
            "category": "research_infrastructure",
            "name_variants": ["energy research accelerator"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"energy research accelerator"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2015, 2099),
            "notes": "Budget 2015 (March): £60m investment in a new Energy Research Accelerator, verified 2015_03_UK.pdf.",
        },
        {
            "canonical_name": "Compound Semiconductor Catapult",
            "category": "innovation_instruments",
            "name_variants": ["compound semiconductor catapult"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"compound semiconductor catapult"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2016, 2099),
            "notes": "Budget 2016: Compound Semiconductor Catapult, verified 2016_UK.pdf.",
        },
        {
            "canonical_name": "Digital Catapult",
            "category": "innovation_instruments",
            "name_variants": ["digital catapult"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"digital catapult"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2018, 2099),
            "notes": "Budget 2018: £115 million to extend funding for the Digital Catapult, verified 2018_UK.pdf.",
        },
        {
            "canonical_name": "DSIT R&D Budget",
            "category": "rd_ministry",
            "name_variants": ["dsit r&d budget", "dsit to invest in r&d"],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "preferred_match_groups": [[r"dsit.{0,20}(r&d|research and development)"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2023, 2099),
            "notes": (
                "Department for Science, Innovation and Technology's own R&D "
                "envelope — a component of, not equal to, the whole-of-government "
                "'Public R&D Investment' total for the same year. Do not sum."
            ),
        },
        {
            "canonical_name": "Alan Turing Institute",
            "category": "research_institute",
            "name_variants": ["alan turing institute"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"alan turing institute"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2014, 2099),
            "notes": "Budget 2014: £42 million over 5 years for the Alan Turing Institute (founding grant).",
        },
        {
            "canonical_name": "GovTech Fund",
            "category": "innovation_instruments",
            "name_variants": ["govtech fund"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"govtech fund"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2017, 2099),
            "notes": "Autumn Budget 2017: up to £20 million over 3 years of R&D NPIF funding for a GovTech Fund.",
        },
        # ---------------------------------------------------------------
        # Round-3 additions (audit 2026-07): case-by-case judgment pass over
        # ~280 candidate R&D/innovation mentions not previously captured.
        # Each entry below is a genuine single-year (or single-Budget-
        # attributable) R&D appropriation, verified against source text,
        # with multi-year/cumulative pledges and non-R&D adjacent policy
        # (tax credits, energy/EV/broadband deployment, general business
        # finance) deliberately excluded — see uk_audit_summary.md §7 for
        # the full include/exclude decision table.
        # ---------------------------------------------------------------
        {
            "canonical_name": "Venture Capital Challenge Competition",
            "category": "innovation_instruments",
            "name_variants": ["venture capital challenge"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"venture capital challenge"]],
            "enforce_preferred_match_groups": True,
            "active_years": (1999, 2099),
            "notes": "Budget 1999: £20 million Venture Capital Challenge Competition from the Capital Modernisation Fund.",
        },
        {
            "canonical_name": "UKTI International R&D Strategy",
            "category": "rd_programme",
            "name_variants": ["ukti international r&d strategy", "uk trade and investment international r&d"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"UKTI.{0,20}international R&D strategy"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2006, 2099),
            "notes": "Budget 2006: £9 million UKTI international R&D strategy funding.",
        },
        {
            "canonical_name": "Climate Change Research Capacity Programme",
            "category": "rd_programme",
            "name_variants": ["climate change research capacity"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"climate change research.{0,60}African researchers"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2007, 2099),
            "notes": "Budget 2007: £30 million programme (with Canadian IDRC) to build African research capacity on climate change.",
        },
        {
            "canonical_name": "University Enterprise Capital Fund",
            "category": "innovation_instruments",
            "name_variants": ["university enterprise capital fund"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"university enterprise capital fund"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2010, 2099),
            "notes": "Budget 2010 (March): £25 million University Enterprise Capital Fund for commercialising university innovations.",
        },
        {
            "canonical_name": "UK Centre for Aerodynamics",
            "category": "research_infrastructure",
            "name_variants": ["uk centre for aerodynamics"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"UK centre for aerodynamics"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2012, 2099),
            "notes": "Budget 2012: £60 million UK centre for aerodynamics, opened 2012-13, to support aerospace innovation.",
        },
        {
            "canonical_name": "TSB Digital Content Production Fund",
            "category": "innovation_instruments",
            "name_variants": ["digital content production", "visual effects industry investment"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"digital content production industry"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2013, 2099),
            "notes": "Budget 2013: Technology Strategy Board £15 million competitive fund for digital content production.",
        },
        {
            "canonical_name": "Centre for Process Innovation Chemical Innovation Fund",
            "category": "innovation_instruments",
            "name_variants": ["centre for process innovation"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"Centre for Process Innovation.{0,60}chemical"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2015, 2099),
            "notes": "Budget 2015 (March): £1 million to Centre for Process Innovation for North East chemicals sector innovation/knowledge transfer.",
        },
        {
            "canonical_name": "Northern Tech Incubator Investment",
            "category": "innovation_instruments",
            "name_variants": ["tech incub"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"£11 million investment in tech incub"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2015, 2099),
            "notes": "Budget 2015 (March): £11 million investment in tech incubation to accelerate innovative businesses in the North.",
        },
        {
            "canonical_name": "Francis Crick Institute (MRC asset reinvestment)",
            "category": "research_institute",
            "name_variants": ["francis crick institute"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"Francis Crick Institute"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2015, 2099),
            "notes": "Budget 2015 (March): £30 million reinvested from sale of MRC assets to support research at the Francis Crick Institute.",
        },
        {
            "canonical_name": "Digital Currency Technology Research",
            "category": "rd_programme",
            "name_variants": ["digital currency technology"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"digital currency technology"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2015, 2099),
            "notes": "Budget 2015 (March): £10 million increase in research funding for digital currency technology (research councils, Alan Turing Institute, Digital Catapult).",
        },
        {
            "canonical_name": "Internet of Things Research Programme",
            "category": "rd_programme",
            "name_variants": ["internet of things"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"Internet of Things technologies"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2015, 2099),
            "notes": "Budget 2015 (March): £40 million to develop Internet of Things technologies (demonstrators, incubator space, research centre).",
        },
        {
            "canonical_name": "SMR-Enabling Advanced Manufacturing R&D Programme",
            "category": "rd_programme",
            "name_variants": ["smr-enabling advanced manufacturing"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"SMR-enabling advanced manufacturing R&D"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2016, 2099),
            "notes": "Budget 2016: at least £30 million for an SMR-enabling advanced manufacturing R&D programme to develop nuclear skills capacity.",
        },
        {
            "canonical_name": "ONS Data Science Hub",
            "category": "research_infrastructure",
            "name_variants": ["hub for data science"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"hub for data science"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2016, 2099),
            "notes": "Budget 2016: over £10 million for a new ONS hub for data science and centre for excellence in economic measurement.",
        },
        {
            "canonical_name": "5G Research Facility",
            "category": "research_infrastructure",
            "name_variants": ["5g facility", "5g research"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"cutting edge 5G facility"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2017, 2099),
            "notes": "Spring Budget 2017: up to £16 million in a cutting-edge 5G facility, delivered through 5G research institutions.",
        },
        {
            "canonical_name": "NPIF Disruptive Technologies Initial Investment",
            "category": "rd_programme",
            "name_variants": ["disruptive technologies"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"kick-start the development of disruptive technologies"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2017, 2099),
            "notes": "Spring Budget 2017: £270 million initial investment in 2017-18 to kick-start disruptive technology development.",
        },
        {
            "canonical_name": "Turing AI Fellowships",
            "category": "rd_programme",
            "name_variants": ["turing ai fellowships"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"Turing AI Fellowships"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2018, 2099),
            "notes": "Budget 2018: up to £50 million in new Turing AI Fellowships to attract world-leading AI research talent.",
        },
        {
            "canonical_name": "Regulators' Pioneer Fund",
            "category": "innovation_instruments",
            "name_variants": ["regulators' pioneer fund", "regulators pioneer fund"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"Regulators.? Pioneer Fund"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2020, 2099),
            "notes": "Budget 2020: £10 million in a second round of the Regulators' Pioneer Fund to unlock emerging technologies.",
        },
        {
            "canonical_name": "National Institute for Health Research (uplift)",
            "category": "research_infrastructure",
            "name_variants": ["national institute for health research"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"National Institute for Health Research"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2020, 2099),
            "notes": "Budget 2020: extra £12 million for the National Institute for Health Research in 2020-21.",
        },
        {
            "canonical_name": "Government Chief Scientific Adviser / GO-Science",
            "category": "rd_ministry",
            "name_variants": ["government chief scientific adviser", "government office for science"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"Government Chief Scientific Adviser and the Government Office for Science"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2020, 2099),
            "notes": "Budget 2020: additional £2 million in 2020-21 for strategic science and resilience capability at GCSA/GO-Science.",
        },
        {
            "canonical_name": "Specialist Research Institutions Funding",
            "category": "research_institute",
            "name_variants": ["specialist institutions"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"invest £80 million to support the UK.?s foremost specialist institutions"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2020, 2099),
            "notes": "Budget 2020: £80 million to support the UK's foremost specialist research institutions (PhDs, fellowships, research projects).",
        },
        {
            "canonical_name": "Vaccines R&D and Manufacturing",
            "category": "rd_programme",
            "name_variants": ["research and development.{0,10}and vaccines manufacturing"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"for research and development \(R&D\) and vaccines manufacturing"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2021, 2099),
            "notes": "Budget 2021 (March): £128 million for R&D and vaccines manufacturing (part of the £733M UK Vaccines Taskforce 2021-22 allocation).",
        },
        {
            "canonical_name": "Innovation Accelerators Programme",
            "category": "innovation_instruments",
            "name_variants": ["innovation accelerators"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"innovation accelerators"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2023, 2099),
            "notes": "Spring Budget 2023: £100 million for the Innovation Accelerators programme (Glasgow City Region, Greater Manchester, West Midlands).",
        },
        {
            "canonical_name": "Cambridge Biomedical Campus",
            "category": "research_infrastructure",
            "name_variants": ["cambridge biomedical campus"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"Cambridge Biomedical Campus"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2024, 2099),
            "notes": "Spring Budget 2024: £10.2 million to support development of the Cambridge Biomedical Campus.",
        },
        {
            "canonical_name": "Cancer Research UK Funding",
            "category": "research_institute",
            "name_variants": ["cancer research uk"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"funding of £3 million for Cancer Research UK"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2024, 2099),
            "notes": "Spring Budget 2024: £3 million for Cancer Research UK.",
        },
        {
            "canonical_name": "Medical Research Charities Early Career Researchers Fund",
            "category": "rd_programme",
            "name_variants": ["medical research charities early career researchers"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"Medical Research Charities Early Career Researchers"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2024, 2099),
            "notes": "Spring Budget 2024: £45 million through the Medical Research Charities Early Career Researchers Support Fund.",
        },
        {
            "canonical_name": "UKRI R&D Missions Accelerator",
            "category": "rd_programme",
            "name_variants": ["r&d missions accelerator"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"R&D Missions Accelerator"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2025, 2099),
            "notes": "Budget 2025: UKRI's £500 million R&D Missions Accelerator programme.",
        },
        {
            "canonical_name": "Entrepreneurship-Focused Doctoral Training",
            "category": "rd_programme",
            "name_variants": ["entrepreneurship-focused doctoral training"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"entrepreneurship.?focused doctoral training"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2025, 2099),
            "notes": "Budget 2025: up to £25 million for new entrepreneurship-focused doctoral training schemes (UKRI).",
        },
        {
            "canonical_name": "Women in Innovation Awards",
            "category": "innovation_instruments",
            "name_variants": ["women in innovation awards"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"Women in Innovation Awards"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2025, 2099),
            "notes": "Budget 2025: new £4.5 million round of the Women in Innovation Awards (UKRI).",
        },
        {
            "canonical_name": "Studio Ulster (virtual production R&D studio)",
            "category": "research_infrastructure",
            "name_variants": ["studio ulster"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"Studio Ulster"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2025, 2099),
            "notes": "Budget 2025: £25.2 million government investment in Studio Ulster, a virtual production R&D studio.",
        },
        {
            "canonical_name": "Materials Processing Institute",
            "category": "research_institute",
            "name_variants": ["materials processing institute"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"materials processing institute"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2020, 2099),
            "notes": "Budget 2020: £22 million Materials Processing Institute, verified 2020_UK.pdf.",
        },
        # ---------------------------------------------------------------
        # Round-4 additions (audit 2026-07): 1997, 2002, 2019 and 2022 have
        # no cached source PDF in Data/input/finance_bills/UK — confirmed
        # these are genuine gaps in the source corpus, not extraction
        # failures. No full "Red Book" Budget was held in 2019 (Autumn
        # Budget 2019 was cancelled for the general election; only a Spring
        # Statement was delivered) or in 1997/2002 (checked against
        # gov.uk's published fiscal-event calendar). For 2022 there were two
        # major fiscal events (September mini-Budget "Growth Plan" and the
        # November Autumn Statement) that were simply never added to the
        # local corpus. Official PDFs were fetched directly from gov.uk and
        # manually reviewed line-by-line for genuine single-year R&D content
        # (see uk_audit_summary.md Round 4 section for full citations).
        # ---------------------------------------------------------------
        {
            "canonical_name": "Extreme Photonics Application Centre",
            "category": "research_infrastructure",
            "name_variants": ["extreme photonics application centre"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"extreme photonics application centre"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2019, 2099),
            "notes": (
                "Spring Statement 2019 (13 March 2019), Written Ministerial "
                "Statement, 'Science and Technology' section: 'Allocating £81 "
                "million to a national Extreme Photonics Application Centre in "
                "Oxfordshire.' No full Autumn Budget was held in 2019 (cancelled "
                "for the general election); this WMS is the only fiscal-event "
                "document for that year."
            ),
        },
        {
            "canonical_name": "European Bioinformatics Institute Infrastructure Upgrade",
            "category": "research_infrastructure",
            "name_variants": ["european bioinformatics institute", "bioinformatics infrastructure"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"european bioinformatics institute"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2019, 2099),
            "notes": (
                "Spring Statement 2019 WMS, 'Science and Technology' section: "
                "'Investing £45 million in a critical upgrade to data storage "
                "cloud computing infrastructure at the European Bioinformatics "
                "Institute in Cambridgeshire.'"
            ),
        },
        {
            "canonical_name": "ARCHER 2 Supercomputer",
            "category": "research_infrastructure",
            "name_variants": ["archer 2", "archer2 supercomputer"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"archer\s?2"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2019, 2099),
            "notes": (
                "Spring Statement 2019 WMS, 'Science and Technology' section: "
                "'Allocating £79 million to a new UK supercomputer (ARCHER 2) "
                "which will replace the current national high-performance "
                "computing platform (ARCHER).'"
            ),
        },
        {
            "canonical_name": "Joint European Torus (JET) Fusion Funding",
            "category": "research_infrastructure",
            "name_variants": ["joint european torus", "jet fusion funding", "jet funding"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"joint european torus", r"\bjet\b.{0,20}fusion"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2019, 2099),
            "notes": (
                "Spring Statement 2019 WMS, 'Science and Technology' section: "
                "'Setting aside up to £60 million to confirm funding is "
                "guaranteed for the [Joint European Torus] facility over "
                "2019/20.' Explicitly a single fiscal-year (2019/20) commitment."
            ),
        },
        {
            "canonical_name": "Long-Term Investment for Technology & Science (LIFTS)",
            "category": "innovation_instruments",
            "name_variants": ["lifts competition", "long-term investment for technology and science"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"long-term investment for technology"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2022, 2099),
            "notes": (
                "The Growth Plan 2022 (23 September 2022, 'mini-Budget'), para "
                "3.14: 'introducing the Long-Term Investment for Technology & "
                "Science (LIFTS) competition, providing up to £500 million to "
                "support new funds designed to catalyse investment from pensions "
                "schemes and other investors into the UK's pioneering science "
                "and technology businesses.' Financial-instrument category, "
                "same treatment as the existing Venture Capital Challenge and "
                "Research Partnership Investment Fund entries. Checked the "
                "companion November 2022 Autumn Statement (CP 751) for further "
                "single-year R&D items: found only multi-year/cumulative "
                "figures already excluded under the Round-3 rule set (Innovate "
                "UK's £2.6bn Spending-Review-period allocation, Catapults' "
                "£1.6bn 5-year funding-cycle increase, and the £20bn-by-2024-25 "
                "R&D target, which duplicates the 'Public R&D Investment' entry "
                "already locked for 2024) plus R&D tax-relief reform (a tax "
                "policy change, not spending). Chapter 5 'Policy Decisions' "
                "itemised tables were not recoverable via text extraction and "
                "were not reviewed line-by-line as a result — flagged as a "
                "residual limitation."
            ),
        },
        # ---------------------------------------------------------------
        # Round-5 additions (audit 2026-07): systematic recovery of
        # decision='include' rows in uk_docx_results.csv that were never
        # matched to any canonical pattern and so were silently dropped
        # from the final series — a structural gap, not a harvesting gap.
        # 91 orphaned include-rows were found; each was checked against
        # already-captured canonicals (to avoid double counting sub-lines
        # of an existing fund/programme) and against the annual-vs-
        # multi-year / R&D-vs-adjacent rule set before being added here.
        # See uk_audit_summary.md §10 for the full orphan disposition
        # table (34 included, 57 excluded with reasons).
        # ---------------------------------------------------------------
        {
            "canonical_name": "Industrial Innovation Support Measures (1982 package)",
            "category": "rd_programme",
            "name_variants": ["industrial innovation", "promote research and innovation in industry"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"promote research and innovation in industry"]],
            "enforce_preferred_match_groups": True,
            "active_years": (1982, 1984),
            "notes": (
                "Budget 1982: 'New measures to promote research and innovation "
                "in industry will involve additional expenditure of £20 million "
                "in 1982-83, £35 million in 1983-84 and £45 million in "
                "1984-85.' Per-year tranches explicitly disaggregated (not a "
                "vague multi-year total), so each year is locked separately. "
                "The 1983-84 figure was updated in Budget 1983 to £39 million "
                "('the cost is £39 million in 1983-84') — the more "
                "contemporaneous figure is used for 1983. The 1984-85 £45 "
                "million figure is the 1982 forecast only; it was not "
                "independently re-confirmed in the 1984 document (no matching "
                "mention found) — flagged as lower-confidence. This is the "
                "earliest confirmed genuine R&D appropriation found in the UK "
                "corpus, extending the series two years earlier than the prior "
                "1994 start of consistent coverage."
            ),
        },
        {
            "canonical_name": "Scientific Equipment Challenge Fund",
            "category": "research_infrastructure",
            "name_variants": ["challenge fund to finance scientific equipment"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"challenge fund to finance scientific equipment"]],
            "enforce_preferred_match_groups": True,
            "active_years": (1996, 2099),
            "notes": "Budget 1996 (1996_UK.pdf p.114): '£20 million in 1997-98 for a challenge fund to finance scientific equipment.'",
        },
        {
            "canonical_name": "Higher Education Innovation Fund (HEIF)",
            "category": "innovation_instruments",
            "name_variants": ["higher education innovation fund", "heif"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"higher education innovation fund"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2003, 2099),
            "notes": "Budget 2003 (2003_UK.pdf p.62): '£187 million Higher Education Innovation Fund (HEIF).'",
        },
        {
            "canonical_name": "PSRE/NHS Science Commercialisation Support",
            "category": "innovation_instruments",
            "name_variants": ["commercialisation of science and technology from public sector research"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"commercialisation of science and technology"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2003, 2099),
            "notes": "Budget 2003 (2003_UK.pdf p.62): '£15 million allocated this year to help catalyse commercialisation of science and technology from Public Sector Research Establishments and NHS Trusts.'",
        },
        {
            "canonical_name": "National Technology Strategy",
            "category": "rd_programme",
            "name_variants": ["national technology strategy"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"national technology strategy"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2004, 2099),
            "notes": "Budget 2004 (2004_UK.pdf p.69): £150 million National Technology Strategy.",
        },
        {
            "canonical_name": "Additional Clinical Research Funding (2005)",
            "category": "rd_programme",
            "name_variants": ["additional funding for clinical research"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"additional funding for clinical research"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2005, 2099),
            "notes": "Budget 2005 (2005_UK.pdf p.71): £25 million additional funding for clinical research.",
        },
        {
            "canonical_name": "Science Research Infrastructure Fund (SRIF)",
            "category": "research_infrastructure",
            "name_variants": ["science research infrastructure fund", "srif"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"science research infrastructure fund"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2005, 2099),
            "notes": "Budget 2005 (2005_UK.pdf p.72): 'Science Research Infrastructure Fund, which provides capital funding of £500 million per annum to renew university infrastructure.'",
        },
        {
            "canonical_name": "DfES Research and Knowledge Transfer Funding (English Universities)",
            "category": "rd_programme",
            "name_variants": ["dfes funding for research and knowledge transfer"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"dfes funding for research and knowledge transfer"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2007, 2099),
            "notes": "Budget 2007 (2007_UK.pdf p.183): DfES funding for research and knowledge transfer in English Universities, £1.655 million.",
        },
        {
            "canonical_name": "Research Councils Co-Investment in TSB Collaborative R&D",
            "category": "rd_programme",
            "name_variants": ["investment by research councils in tsb programmes"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"investment by research councils in tsb"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2007, 2099),
            "notes": "Budget 2007 (2007_UK.pdf p.70): 'Investment by Research Councils in TSB programmes to support collaborative R&D projects', £25 million. Distinct funding stream from the Technology Strategy Board's own £100 million (already captured under the 'Innovate UK' canonical for 2007) — flagged as a modest double-counting risk since both ultimately flow into TSB collaborative R&D, but the source text frames them as two separate contributions.",
        },
        {
            "canonical_name": "Low-Carbon Vehicle RD&D Programme",
            "category": "rd_programme",
            "name_variants": ["research, development and demonstration programme for low-carbon vehicles", "research, development and demonstration programme focusing on low-carbon vehicles"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"research, development and demonstration programme.{0,20}low-carbon vehicles"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2008, 2099),
            "notes": "Budget 2008 (2008_UK.pdf p.102/106, King Review): £40 million research, development and demonstration programme for low-carbon/ultra-low-carbon vehicles.",
        },
        {
            "canonical_name": "TSB Creative Industries R&D Programme",
            "category": "rd_programme",
            "name_variants": ["programme of research and development for the creative industries"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"research and development for the creative industries"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2009, 2099),
            "notes": "Budget 2009 (2009_UK.pdf p.89): '£10 million programme of research and development for the creative industries, led by the TSB.'",
        },
        {
            "canonical_name": "Low-Carbon Aircraft Engine R&D",
            "category": "rd_programme",
            "name_variants": ["research and technology critical to the development of low-carbon aircraft engines"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"low-carbon aircraft engines"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2010, 2099),
            "notes": "Budget March 2010 (2010_03_UK.pdf p.118): £45 million for research and technology critical to the development of low-carbon aircraft engines.",
        },
        {
            "canonical_name": "Science and Innovation Campuses Capital Funding",
            "category": "research_infrastructure",
            "name_variants": ["new capital funding in 2011-12 for science and innovation campuses"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"capital funding in 2011-12 for science and innovation campuses"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2011, 2099),
            "notes": "Budget 2011 (2011_UK.pdf p.56): 'provide £100 million of new capital funding in 2011-12 for science and innovation campuses.' This is the exact item flagged during the FY-notation cleaner fix (Round 1-2) as staying stuck in review — it turned out to also need its own canonical, since it never matched any existing agency pattern.",
        },
        {
            "canonical_name": "University Research Facilities Capital Funding (2012)",
            "category": "research_infrastructure",
            "name_variants": ["fund to support investment in major new university research facilities"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"investment in major new university research facilities"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2012, 2099),
            "notes": "Budget 2012 (2012_UK.pdf p.43): £100 million fund to support investment in major new university research facilities.",
        },
        {
            "canonical_name": "Digital Economy Centres",
            "category": "research_infrastructure",
            "name_variants": ["next generation digital economy centres", "digital economy centres"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"digital economy centres"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2015, 2099),
            "notes": "Summer Budget 2015 (2015_07_UK.pdf p.68/102): investment in 6 Next Generation Digital Economy Centres, £23 million.",
        },
        {
            "canonical_name": "Centre for Agricultural Informatics and Sustainability Metrics",
            "category": "research_institute",
            "name_variants": ["centre for agricultural informatics and sustainability metrics"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"agricultural informatics and sustainability metrics"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2015, 2099),
            "notes": "Budget March 2015 (2015_03_UK.pdf p.78): Centre for Agricultural Informatics and Sustainability Metrics, £11.8 million.",
        },
        {
            "canonical_name": "Advanced Wellbeing Research Centre",
            "category": "research_institute",
            "name_variants": ["advanced wellbeing research centre"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"advanced wellbeing research centre"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2015, 2099),
            "notes": "Budget March 2015 (2015_03_UK.pdf p.77): Advanced Wellbeing Research Centre, £14 million.",
        },
        {
            "canonical_name": "Birmingham STEAMhouse",
            "category": "innovation_instruments",
            "name_variants": ["birmingham steamhouse"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"birmingham steamhouse"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2016, 2099),
            "notes": "Budget 2016 (2016_UK.pdf p.130): Birmingham STEAMhouse innovation hub, £14 million.",
        },
        {
            "canonical_name": "Battery Technology R&D Support (Dyson)",
            "category": "rd_programme",
            "name_variants": ["dyson batteries"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"dyson batteries"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2016, 2099),
            "notes": "Budget 2016 (2016_UK.pdf p.130): government support for Dyson battery technology R&D, £16 million.",
        },
        {
            "canonical_name": "National Institute for Smart Data Innovation",
            "category": "research_institute",
            "name_variants": ["national institute for smart data innovation"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"national institute for smart data innovation"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2016, 2099),
            "notes": "Budget 2016 (2016_UK.pdf p.78/130): 'The government will invest £15 million in the National Institute for Smart Data Innovation.'",
        },
        {
            "canonical_name": "Jodrell Bank Discovery Centre",
            "category": "research_infrastructure",
            "name_variants": ["jodrell bank"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"jodrell bank"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2017, 2099),
            "notes": "Autumn Budget 2017 (2017_11_UK.pdf p.56): £4 million government contribution to Jodrell Bank's £20.5 million project.",
        },
        {
            "canonical_name": "5G Testbeds and Trials Programme",
            "category": "rd_programme",
            "name_variants": ["initial trial to test 5g applications"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"initial trial to test 5g applications"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2017, 2099),
            "notes": "Autumn Budget 2017 (2017_11_UK.pdf p.54): £5 million for an initial trial to test 5G applications and deployment on roads.",
        },
        {
            "canonical_name": "5G Security Testbed Facility",
            "category": "research_infrastructure",
            "name_variants": ["facilities where the security of 5g networks can be tested"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"security of 5g networks can be tested"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2017, 2099),
            "notes": "Autumn Budget 2017 (2017_11_UK.pdf p.54): £10 million to create facilities where the security of 5G networks can be tested.",
        },
        {
            "canonical_name": "NPIF Fellowship Programmes",
            "category": "rd_programme",
            "name_variants": ["npif funding for fellowship programmes"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"npif funding for fellowship programmes"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2017, 2099),
            "notes": "Spring Budget 2017 (2017_03_UK.pdf p.48): NPIF funding for fellowship programmes, £50 million.",
        },
        {
            "canonical_name": "Quantum Technology R&D Programme (2018)",
            "category": "rd_programme",
            "name_variants": ["quantum technology: research and development"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"quantum technology: research and development"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2018, 2099),
            "notes": "Budget 2018 (2018_UK.pdf p.40): Quantum Technology research and development, £5 million.",
        },
        {
            "canonical_name": "UK Nuclear Fusion R&D Support",
            "category": "rd_programme",
            "name_variants": ["support for uk nuclear fusion"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"support for uk nuclear fusion"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2018, 2099),
            "notes": "Budget 2018 (2018_UK.pdf p.40): support for UK nuclear fusion, £20 million.",
        },
        {
            "canonical_name": "International Research Fellowship Scheme (2018)",
            "category": "rd_programme",
            "name_variants": ["international fellowship scheme"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"international fellowship scheme"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2018, 2099),
            "notes": "Budget 2018 (2018_UK.pdf p.60): £100 million in an international fellowship scheme.",
        },
        {
            "canonical_name": "Life Sciences Investment Programme",
            "category": "innovation_instruments",
            "name_variants": ["life sciences investment programme"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"life sciences investment programme"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2020, 2099),
            "notes": "Budget 2020 (2020_UK.pdf p.88): Life Sciences Investment Programme, £200 million.",
        },
        {
            "canonical_name": "Animal Health Science Estate",
            "category": "research_infrastructure",
            "name_variants": ["animal health science estate"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"animal health science estate"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2020, 2099),
            "notes": "Budget 2020 (2020_UK.pdf p.88): Animal health science estate, £1.4 million.",
        },
        {
            "canonical_name": "Future Fund: Breakthrough",
            "category": "innovation_instruments",
            "name_variants": ["future fund: breakthrough", "future fund breakthrough"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"future fund:? breakthrough"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2021, 2099),
            "notes": "Budget March 2021 (2021_03_UK.pdf p.70): 'Future Fund: Breakthrough — Building on the government's Future Fund', £375 million co-investment vehicle for R&D-intensive, high-growth companies.",
        },
        {
            "canonical_name": "Global Underwater Hub",
            "category": "innovation_instruments",
            "name_variants": ["global underwater hub"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"global underwater hub"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2021, 2099),
            "notes": "Budget March 2021 (2021_03_UK.pdf p.68): support for the Global Underwater Hub, £5 million.",
        },
        {
            "canonical_name": "Quantum Computing Mission (initial funding)",
            "category": "rd_programme",
            "name_variants": ["progress the quantum computing mission"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"progress the quantum computing mission"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2024, 2099),
            "notes": "Budget March 2024 (2024_03_UK.pdf p.59): £1.6 million to progress the quantum computing mission.",
        },
        {
            "canonical_name": "Faraday Discovery Fellowships and Green Future Fellowships Endowments",
            "category": "rd_programme",
            "name_variants": ["faraday discovery fellowships", "green future fellowships"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"faraday discovery fellowships"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2024, 2099),
            "notes": "Budget March 2024 (2024_03_UK.pdf p.82): 'The £250 million Faraday Discovery Fellowships and £150 million Green Future Fellowships will be funded through endowments to the Royal Society and the Royal Academy of Engineering' — one-off endowment capital transfers (£400 million combined), treated like the Alan Turing Institute founding grant.",
        },
        {
            "canonical_name": "South Wales Semiconductor Technologies Cluster",
            "category": "research_infrastructure",
            "name_variants": ["south wales world-leading semiconductor technologies cluster"],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"south wales.{0,20}semiconductor technologies cluster"]],
            "enforce_preferred_match_groups": True,
            "active_years": (2025, 2099),
            "notes": "Budget 2025 (2025_UK.pdf p.110): £10 million invested in the South Wales world-leading semiconductor technologies cluster.",
        },
    ],

    # -----------------------------------------------------------------------
    # FRANCE
    # Source: Loi de Finances (PLF/LFI) — JORF / budget.gouv.fr
    # Key agencies: ANR, CNRS, CEA, INSERM, INRAE, INRIA, CNES, IFREMER, BRGM
    # Unit: millions of euros (or francs pre-2002)
    # -----------------------------------------------------------------------
    "France": [
        {
            "canonical_name": "Research (Pre-LOLF Ministry Chapter)",
            "category": "rd_ministry",
            "name_variants": [
                "ministère de la recherche",
                "ministere de la recherche",
                "industrie et recherche",
                "services du premier ministre",
                "recherche",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "preferred_match_groups": _france_pre_lolf_match_groups(
                r"minist[eè]re de la recherche",
                r"industrie et recherche",
                r"services du premier ministre",
                r"\brecherche\b",
            ),
            "enforce_preferred_match_groups": True,
            "exclude_match_groups": [[
                r"universit[eé]s",
                r"enseignement sup[eé]rieur",
            ]],
            "active_years": (1970, 2005),
            "notes": "Pre-2006 France tracks Etat B/C ministry-chapter research appropriations rather than LOLF mission/programme rows.",
        },
        {
            "canonical_name": "Universities and Higher Education (Pre-LOLF Chapter)",
            "category": "higher_education",
            "name_variants": [
                "universités",
                "universites",
                "enseignement supérieur",
                "enseignement superieur",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "preferred_match_groups": _france_pre_lolf_match_groups(
                r"universit[eé]s",
                r"enseignement sup[eé]rieur",
            ),
            "enforce_preferred_match_groups": True,
            "exclude_match_groups": [[
                r"\brecherche\b",
            ]],
            "strict_exclude_match_groups": True,
            "active_years": (1970, 2005),
            "notes": "Pre-2006 France higher-education chapter before LOLF programme structure.",
        },
        {
            "canonical_name": "Multidisciplinary Scientific and Technological Research",
            "category": "rd_programme",
            "name_variants": [
                "multidisciplinary scientific and technological research",
                "recherches scientifiques et technologiques pluridisciplinaires",
                "payment credits for multidisciplinary scientific and technological research",
                "total for multidisciplinary scientific and technological research",
                "payment credits for scientific research",
            ],
            "preferred_item_type": ["program_total", "section_total", "line_item"],
            "preferred_match_groups": _france_programme_match_groups(
                r"multidisciplinary scientific and technological research",
                r"recherches scientifiques et technologiques pluridisciplinaires",
                r"scientific research",
            ),
            "enforce_preferred_match_groups": True,
            "prefer_latest_page": True,
            "exclude_match_groups": [[
                r"environment and resource management",
                r"gestion des milieux et des ressources",
                r"space research",
                r"recherche spatiale",
                r"energy, development, and sustainable",
                r"matiere economique et industrielle",
                r"higher education and agricultural research",
                r"culture scientific",
            ]],
            "strict_exclude_match_groups": True,
            "active_years": (2006, 2099),
            "notes": "France JORF Programme 172 style line; prefer later budget pages over ETPT-style previews.",
        },
        {
            "canonical_name": "Space Research",
            "category": "rd_programme",
            "name_variants": [
                "space research",
                "recherche spatiale",
                "payment credits for space research",
                "total for space research",
            ],
            "preferred_item_type": ["program_total", "section_total", "line_item"],
            "preferred_match_groups": _france_programme_match_groups(
                r"space research",
                r"recherche spatiale",
            ),
            "enforce_preferred_match_groups": True,
            "prefer_latest_page": True,
            "exclude_match_groups": [[
                r"multidisciplinary scientific and technological research",
                r"gestion des milieux et des ressources",
                r"energy, development, and sustainable",
                r"matiere economique et industrielle",
                r"higher education and agricultural research",
                r"culture scientific",
            ]],
            "active_years": (2006, 2099),
        },
        {
            "canonical_name": "Research in the Fields of Energy, Development, and Sustainable Mobility",
            "category": "rd_programme",
            "name_variants": [
                "research in the fields of energy, development, and sustainable mobility",
                "research in the fields of energy, development, and sustainable planning",
                "recherche dans les domaines de l'énergie, du développement et de la mobilité durables",
                "recherche dans les domaines de l'énergie, du développement et de l'aménagement durables",
                "payment credits for research in the fields of energy, development, and sustainable mobility",
                "total for research in the fields of energy, development, and sustainable mobility",
                "total for research in the fields of energy, development, and sustainable planning",
            ],
            "preferred_item_type": ["program_total", "section_total", "line_item"],
            "preferred_match_groups": _france_programme_match_groups(
                r"research in the fields of energy, development, and sustainable mobility",
                r"research in the fields of energy, development, and sustainable planning",
                r"recherche dans les domaines de l'?energie, du developpement et de la mobilite durables",
                r"recherche dans les domaines de l'?energie, du developpement et de l'?amenagement durables",
            ),
            "enforce_preferred_match_groups": True,
            "prefer_latest_page": True,
            "exclude_match_groups": [[
                r"multidisciplinary scientific and technological research",
                r"space research",
                r"gestion des milieux et des ressources",
                r"matiere economique et industrielle",
                r"higher education and agricultural research",
                r"culture scientific",
            ]],
            "active_years": (2006, 2099),
        },
        {
            "canonical_name": "Research and Higher Education in Economic and Industrial Matters",
            "category": "rd_programme",
            "name_variants": [
                "research and higher education in economic and industrial matters",
                "economic and industrial research and higher education",
                "recherche et enseignement supérieur en matière économique et industrielle",
                "payment credits for research and higher education in economic and industrial matters",
                "total for research and higher education in economic and industrial matters",
                "total for economic and industrial research and higher education",
            ],
            "preferred_item_type": ["program_total", "section_total", "line_item"],
            "preferred_match_groups": _france_programme_match_groups(
                r"research and higher education in economic and industrial matters",
                r"economic and industrial research and higher education",
                r"recherche et enseignement superieur en matiere economique et industrielle",
            ),
            "enforce_preferred_match_groups": True,
            "prefer_latest_page": True,
            "exclude_match_groups": [[
                r"multidisciplinary scientific and technological research",
                r"space research",
                r"gestion des milieux et des ressources",
                r"energy, development, and sustainable",
                r"higher education and agricultural research",
                r"culture scientific",
            ]],
            "active_years": (2006, 2099),
        },
        {
            "canonical_name": "Cultural Research and Scientific Culture",
            "category": "rd_programme",
            "name_variants": [
                "cultural research and scientific culture",
                "recherche culturelle et culture scientifique",
                "payment credits for cultural research and scientific culture",
                "total for cultural research and scientific culture",
            ],
            "preferred_item_type": ["program_total", "section_total", "line_item"],
            "preferred_match_groups": _france_programme_match_groups(
                r"cultural research and scientific culture",
                r"recherche culturelle et culture scientifique",
            ),
            "enforce_preferred_match_groups": True,
            "prefer_latest_page": True,
            "active_years": (2006, 2099),
        },
        {
            "canonical_name": "Higher Education and Agricultural Research",
            "category": "rd_programme",
            "name_variants": [
                "higher education and agricultural research",
                "enseignement supérieur et recherche agricoles",
                "payment credits for higher education and agricultural research",
                "total for higher education and agricultural research",
            ],
            "preferred_item_type": ["program_total", "section_total", "line_item"],
            "preferred_match_groups": _france_programme_match_groups(
                r"higher education and agricultural research",
                r"enseignement superieur et recherche agricoles",
            ),
            "enforce_preferred_match_groups": True,
            "prefer_latest_page": True,
            "active_years": (2006, 2099),
        },
        {
            "canonical_name": "Applied Research and Innovation in Agriculture",
            "category": "rd_programme",
            "name_variants": [
                "applied research and innovation in agriculture",
                "payment credits for applied research and innovation in agriculture",
            ],
            "preferred_item_type": ["program_total", "section_total", "line_item"],
            "preferred_match_groups": _france_programme_match_groups(
                r"applied research and innovation in agriculture",
                r"recherche appliquee et innovation en agriculture",
            ),
            "enforce_preferred_match_groups": True,
            "prefer_latest_page": True,
            "active_years": (2015, 2099),
        },
        {
            "canonical_name": "ANR (Agence Nationale de la Recherche)",
            "category": "science_agency",
            "name_variants": [
                "agence nationale de la recherche",
                "anr",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (2005, 2099),
            "notes": "Created 2005 as the main competitive research funding agency.",
        },
        {
            "canonical_name": "CNRS (Centre National de la Recherche Scientifique)",
            "category": "science_agency",
            "name_variants": [
                "centre national de la recherche scientifique",
                "cnrs",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (2018, 2099),
        },
        {
            "canonical_name": "CEA (Commissariat à l'Énergie Atomique)",
            "category": "science_agency",
            "name_variants": [
                "commissariat à l'énergie atomique",
                "commissariat a l'energie atomique",
                "commissariat à l'énergie",
                "cea",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "exclude_match_groups": [[
                r"gies alternatives",
                r"impositions? de toutes natures",
                r"ressource affectee",
                r"personne affectataire",
                r"\bplafond\b",
                r"article l\.",
            ]],
            "strict_exclude_match_groups": True,
            "active_years": (1945, 2099),
            "notes": "Covers both civil nuclear research and defence applications.",
        },
        {
            "canonical_name": "INSERM (Institut National de la Santé et de la Recherche Médicale)",
            "category": "science_agency",
            "name_variants": [
                "inserm",
                "institut national de la santé et de la recherche",
                "santé et de la recherche médicale",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1964, 2099),
        },
        {
            "canonical_name": "INRAE (Institut National de Recherche pour l'Agriculture)",
            "category": "science_agency",
            "name_variants": [
                "inrae",
                "inra",
                "institut national de la recherche agronomique",
                "institut national de recherche pour l'agriculture",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1946, 2099),
            "notes": "INRA renamed INRAE in 2020 after merger with IRSTEA.",
        },
        {
            "canonical_name": "INRIA (Institut National de Recherche en Informatique)",
            "category": "science_agency",
            "name_variants": [
                "inria",
                "iria",
                "institut national de recherche en informatique",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1967, 2099),
        },
        {
            "canonical_name": "CNES (Centre National d'Études Spatiales)",
            "category": "science_agency",
            "name_variants": [
                "centre national d'études spatiales",
                "centre national d etudes spatiales",
                "cnes",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1961, 2099),
        },
        {
            "canonical_name": "IFREMER (Institut Français de Recherche pour l'Exploitation de la Mer)",
            "category": "science_agency",
            "name_variants": [
                "ifremer",
                "cnexo",
                "institut français de recherche pour l'exploitation de la mer",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1984, 2099),
            "notes": "Formed 1984 from CNEXO + ISTPM.",
        },
        {
            "canonical_name": "BRGM (Bureau de Recherches Géologiques et Minières)",
            "category": "science_agency",
            "name_variants": [
                "bureau de recherches géologiques et minières",
                "bureau de recherches geologiques",
                "brgm",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1959, 2099),
        },
        {
            "canonical_name": "ONERA (Office National d'Études et de Recherches Aérospatiales)",
            "category": "science_agency",
            "name_variants": [
                "onera",
                "office national d'études et de recherches aérospatiales",
                "office national d etudes et de recherches aerospatiales",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1946, 2099),
        },
    ],

    # -----------------------------------------------------------------------
    # GERMANY
    # Source: Bundeshaushalt (Bundeshaushaltsplan) — bundeshaushalt.de
    # Key agencies: DFG, Helmholtz, Fraunhofer, MPG, Leibniz, BMBF line items
    # Unit: millions of euros (or DM pre-2002, where 1 EUR ≈ 1.95583 DM)
    # -----------------------------------------------------------------------
    "Germany": [
        {
            "canonical_name": "DFG (Deutsche Forschungsgemeinschaft)",
            "category": "science_agency",
            "name_variants": [
                "deutsche forschungsgemeinschaft",
                "dfg",
                # English translations used by LLM extraction
                "german research foundation",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1920, 2099),
            "notes": "Main competitive research funding agency. "
                     "Grant funded by federal + Länder contributions.",
        },
        {
            "canonical_name": "Helmholtz-Gemeinschaft (HGF)",
            "category": "science_agency",
            "name_variants": [
                "helmholtz-gemeinschaft",
                "helmholtz gemeinschaft",
                "hgf",
                "großforschungseinrichtungen",
                "grossforschungseinrichtungen",
                "forschungszentren der helmholtz-gemeinschaft",
                # English translations used by LLM extraction
                "hermann von helmholtz association",
                "helmholtz association",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1958, 2099),
            "notes": "Formerly Arbeitsgemeinschaft der Großforschungseinrichtungen (AGF).",
        },
        {
            "canonical_name": "Fraunhofer-Gesellschaft",
            "category": "science_agency",
            "name_variants": [
                "fraunhofer-gesellschaft",
                "fraunhofer gesellschaft",
                "fraunhofer",
                # English translations used by LLM extraction
                "fraunhofer society",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1949, 2099),
        },
        {
            "canonical_name": "Max-Planck-Gesellschaft (MPG)",
            "category": "science_agency",
            "name_variants": [
                "max-planck-gesellschaft",
                "max planck gesellschaft",
                "mpg",
                "max-planck",
                "max planck",
                # English translations used by LLM extraction
                "max planck society",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1948, 2099),
        },
        {
            "canonical_name": "Leibniz-Gemeinschaft (WGL)",
            "category": "science_agency",
            "name_variants": [
                "leibniz-gemeinschaft",
                "leibniz gemeinschaft",
                "wissenschaftsgemeinschaft gottfried wilhelm leibniz",
                "gottfried wilhelm leibniz",
                "leibniz association",
                "blaue liste",
                "wgl",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (2020, 2099),
            "notes": "Formerly 'Blaue Liste' (Blue List) research institutes.",
        },
        {
            "canonical_name": "BMBF (Bundesministerium für Bildung und Forschung)",
            "category": "science_agency",
            "name_variants": [
                "bundesministerium für bildung und forschung",
                "bundesministerium fur bildung und forschung",
                "bmbf",
                "bundesminister für bildung und forschung",
                "bundesminister fur bildung und forschung",
                "bundesministerium für forschung",
                "bundesministerium fur forschung",
                "bundesministerium für bildung, wissenschaft",
                "bundesministerium fur bildung, wissenschaft",
                # Pre-1994 name: BMFT (Bundesministerium für Forschung und Technologie)
                "bundesministerium für forschung und technologie",
                "bundesministerium fur forschung und technologie",
                "bmft",
                "bundesminister für forschung und technologie",
                # Pre-1969 and transition names
                "bundesminister für wissenschaftliche forschung",
                "bundesministerium für wissenschaftliche forschung",
                # English translations used by LLM extraction (current BMBF name)
                "federal ministry of education and research",
                "federal ministry for education and research",
                "federal minister of education and research",
                "federal minister for education and research",
                # English translations — pre-reunification name (Bildung und Wissenschaft)
                "federal ministry of education and science",
                "federal ministry for education and science",
                "federal minister for education and science",
                # English translations — BMFT (pre-1994: Forschung und Technologie)
                "federal ministry for research and technology",
                "federal ministry of research and technology",
                "federal minister for research and technology",
                "federal ministry of research, technology",
            ],
            "preferred_item_type": ["section_total"],
            # Prefer rows that explicitly name the full ministry in the description
            # (e.g. "Gesamtausgaben für das Bundesministerium für Bildung und Forschung").
            # These are more reliable than generic "Summe Ausgaben" rows which can
            # refer to different table sections depending on context.
            "preferred_match_groups": [
                [r"gesamtausgaben.*bundesministerium.*bildung.*forschung"],
                [r"gesamtausgaben.*bmbf\b"],
                [r"gesamtausgaben.*bmft\b"],
            ],
            "enforce_preferred_match_groups": False,  # Fall back to all matches if none found
            "active_years": (1955, 2099),
            "notes": "Main federal ministry for R&D (BMFT pre-1994, BMBF 1994-2025, BMFTR 2025+). "
                     "Total appropriation (Epl 30 Summe Ausgaben) as reported in Bundeshaushalt.",
        },
        {
            "canonical_name": "PTB (Physikalisch-Technische Bundesanstalt)",
            "category": "science_agency",
            "name_variants": [
                "physikalisch-technische bundesanstalt",
                "ptb",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1887, 2099),
        },
        {
            "canonical_name": "BAM (Bundesanstalt für Materialforschung)",
            "category": "science_agency",
            "name_variants": [
                "bundesanstalt für materialforschung",
                "bundesanstalt fur materialforschung",
                "bam",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1870, 2099),
        },
        {
            "canonical_name": "Helmholtz Centre for Environmental Research (UFZ)",
            "category": "science_agency",
            "name_variants": [
                "helmholtz centre for environmental research",
                "helmholtz center for environmental research",
                "ufz",
                "ufz-umweltforschungszentrum leipzig-halle",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1991, 2099),
        },
        {
            "canonical_name": "German Research Centre for Geosciences (GFZ)",
            "category": "science_agency",
            "name_variants": [
                "german research centre for geosciences",
                "german research center for geosciences",
                "gfz",
                "helmholtz centre potsdam",
                "helmholtz center potsdam",
                "geo research center foundation",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1992, 2099),
        },
        {
            "canonical_name": "Helmholtz Centre Munich",
            "category": "science_agency",
            "name_variants": [
                "helmholtz centre munich",
                "helmholtz center munich",
                "hmgu",
                "german research center for health and environment",
                "german research centre for environmental health",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1954, 2099),
        },
        {
            "canonical_name": "Helmholtz Centre for Infection Research (HZI)",
            "category": "science_agency",
            "name_variants": [
                "helmholtz centre for infection research",
                "helmholtz center for infection research",
                "hzi",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1965, 2099),
        },
        {
            "canonical_name": "Helmholtz Center Dresden-Rossendorf (HZDR)",
            "category": "science_agency",
            "name_variants": [
                "helmholtz center dresden-rossendorf",
                "helmholtz centre dresden-rossendorf",
                "hzdr",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1992, 2099),
        },
        {
            "canonical_name": "Helmholtz Centre for Ocean Research Kiel (GEOMAR)",
            "category": "science_agency",
            "name_variants": [
                "helmholtz centre for ocean research kiel",
                "helmholtz center for ocean research kiel",
                "geomar",
                "helmholtz-zentrum für ozeanforschung kiel",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (2004, 2099),
        },
        {
            "canonical_name": "German Aerospace Center",
            "category": "science_agency",
            "name_variants": [
                "deutsches zentrum für luft- und raumfahrt",
                "deutsches zentrum fur luft- und raumfahrt",
                "dlr",
                "german aerospace center",
                "german aerospace centre",
                # Pre-1997 predecessor name
                "deutsche forschungsanstalt für luft- und raumfahrt",
                "deutsche forschungsanstalt fur luft- und raumfahrt",
                "dfvlr",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1969, 2099),
            "max_amount_local": 3_000_000_000,
            "notes": "DLR (formerly DFVLR). Federal institutional grant for aerospace, transport, energy, and digitisation R&D.",
        },
    ],

    # -----------------------------------------------------------------------
    # JAPAN
    # Source: 文部科学省 (MEXT) and 国家予算 (national budget)
    # Key agencies: JST, JSPS, RIKEN, NIMS, JAMSTEC, JAXA, NEDO
    # Unit: billions of yen (note: txt.gz amounts may be in millions or hundreds
    #       of millions — inspect first run and set unit rule accordingly)
    # -----------------------------------------------------------------------
    "Japan": [
        {
            "canonical_name": "JST (Japan Science and Technology Agency)",
            "category": "science_agency",
            "name_variants": [
                "japan science and technology agency",
                "jst",
                "科学技術振興機構",
                "jst (science and technology agency)",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "preferred_match_groups": [
                _japan_operating_match_group(r"japan science and technology agency", r"科学技術振興機構", r"\bjst\b"),
            ],
            "enforce_preferred_match_groups": True,
            "choose_smallest_match": True,
            "active_years": (1975, 2099),
            "notes": "JST created 1996 from JRDC + STA functions; pre-1996 bridge rolls up historical predecessor institutions.",
        },
        {
            "canonical_name": "JSPS (Japan Society for the Promotion of Science)",
            "category": "science_agency",
            "name_variants": [
                "japan society for the promotion of science",
                "jsps",
                "日本学術振興会",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "preferred_match_groups": [
                _japan_operating_match_group(r"japan society for the promotion of science", r"日本学術振興会", r"\bjsps\b"),
            ],
            "enforce_preferred_match_groups": True,
            "choose_smallest_match": True,
            "active_years": (1932, 2099),
        },
        {
            "canonical_name": "RIKEN (Institute of Physical and Chemical Research)",
            "category": "science_agency",
            "name_variants": [
                "riken",
                "理化学研究所",
                "institute of physical and chemical research",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "preferred_match_groups": [
                _japan_operating_match_group(r"\briken\b", r"理化学研究所"),
            ],
            "enforce_preferred_match_groups": True,
            "choose_smallest_match": True,
            "active_years": (1917, 2099),
        },
        {
            "canonical_name": "Power Reactor and Nuclear Fuel Development Corporation",
            "category": "historical_science_agency",
            "name_variants": [
                "power reactor and nuclear fuel development corporation",
                "pnc",
                "動力炉・核燃料開発事業団",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "active_years": (1976, 1998),
            "notes": "Historical predecessor in the Japanese nuclear R&D system. Kept as a separate series instead of forcing all support rows into JAEA.",
        },
        {
            "canonical_name": "NIMS (National Institute for Materials Science)",
            "category": "science_agency",
            "name_variants": [
                "national institute for materials science",
                "nims",
                "物質・材料研究機構",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "preferred_match_groups": [
                _japan_operating_match_group(r"national institute for materials science", r"物質・材料研究機構", r"\bnims\b"),
            ],
            "enforce_preferred_match_groups": True,
            "choose_smallest_match": True,
            "active_years": (2001, 2099),
        },
        {
            "canonical_name": "JAMSTEC (Japan Agency for Marine-Earth Science and Technology)",
            "category": "science_agency",
            "name_variants": [
                "japan agency for marine-earth science",
                "jamstec",
                "海洋研究開発機構",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "preferred_match_groups": [
                _japan_operating_match_group(r"japan agency for marine[- ]earth science and technology", r"海洋研究開発機構", r"\bjamstec\b"),
            ],
            "enforce_preferred_match_groups": True,
            "choose_smallest_match": True,
            "active_years": (1971, 2099),
        },
        {
            "canonical_name": "JAXA (Japan Aerospace Exploration Agency)",
            "category": "science_agency",
            "name_variants": [
                "japan aerospace exploration agency",
                "jaxa",
                "宇宙航空研究開発機構",
                "nasda",
                "institute of space and astronautical science",
                "isas",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "preferred_match_groups": [
                _japan_operating_match_group(r"japan aerospace exploration agency", r"宇宙航空研究開発機構", r"\bjaxa\b"),
            ],
            "enforce_preferred_match_groups": True,
            "choose_smallest_match": True,
            "active_years": (1969, 2099),
            "notes": "JAXA formed 2003 from NASDA + ISAS + NAL merger.",
        },
        {
            "canonical_name": "NEDO (New Energy and Industrial Technology Development Organization)",
            "category": "innovation_instruments",
            "name_variants": [
                "new energy and industrial technology development organization",
                "nedo",
                "新エネルギー・産業技術総合開発機構",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "preferred_match_groups": [
                _japan_operating_match_group(r"new energy and industrial technology development organization", r"新エネルギー・産業技術総合開発機構", r"\bnedo\b"),
            ],
            "enforce_preferred_match_groups": True,
            "choose_smallest_match": True,
            "active_years": (1980, 2099),
        },
        {
            "canonical_name": "AIST (National Institute of Advanced Industrial Science and Technology)",
            "category": "science_agency",
            "name_variants": [
                "national institute of advanced industrial science and technology",
                "aist",
                "産業技術総合研究所",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "preferred_match_groups": [
                _japan_operating_match_group(r"national institute of advanced industrial science and technology", r"産業技術総合研究所", r"\baist\b"),
            ],
            "enforce_preferred_match_groups": True,
            "choose_smallest_match": True,
            "active_years": (2001, 2099),
        },
        {
            "canonical_name": "JAEA (Japan Atomic Energy Agency)",
            "category": "science_agency",
            "name_variants": [
                "japan atomic energy agency",
                "jaea",
                "日本原子力研究開発機構",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "preferred_match_groups": [
                _japan_operating_match_group(r"japan atomic energy agency", r"日本原子力研究開発機構", r"\bjaea\b"),
            ],
            "enforce_preferred_match_groups": True,
            "choose_smallest_match": True,
            "active_years": (1976, 2099),
            "notes": "Pre-2005 bridge uses Japan Atomic Energy Research Institute support rows where explicitly identified.",
        },
        {
            "canonical_name": "QST (National Institutes for Quantum Science and Technology)",
            "category": "science_agency",
            "name_variants": [
                "national institutes for quantum science and technology",
                "national institute for quantum science and technology",
                "quantum science and technology agency",
                "quantum science and technology development agency",
                "qst",
                "量子科学技術研究開発機構",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "preferred_match_groups": [
                _japan_operating_match_group(r"quantum science and technology", r"量子科学技術研究開発機構", r"\bqst\b"),
            ],
            "enforce_preferred_match_groups": True,
            "choose_smallest_match": True,
            "active_years": (2016, 2099),
        },
        {
            "canonical_name": "National Institute of Radiological Sciences",
            "category": "historical_science_agency",
            "name_variants": [
                "national institute of radiological sciences",
                "nirs",
                "放射線医学総合研究所",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "preferred_match_groups": [
                _japan_operating_match_group(
                    r"national institute of radiological sciences",
                    r"放射線医学総合研究所",
                    r"\bnirs\b",
                ),
            ],
            "enforce_preferred_match_groups": False,
            "choose_smallest_match": True,
            "active_years": (2001, 2015),
            "notes": "Historical series kept separate ahead of the QST era; prioritise explicit operating-cost rows when available.",
        },
        {
            "canonical_name": "National Institute for Environmental Studies",
            "category": "science_agency",
            "name_variants": [
                "national institute for environmental studies",
                "nies",
                "国立環境研究所",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "preferred_match_groups": [
                _japan_operating_match_group(
                    r"national institute for environmental studies",
                    r"国立環境研究所",
                    r"\bnies\b",
                ),
            ],
            "enforce_preferred_match_groups": True,
            "choose_smallest_match": True,
            "active_years": (1974, 2099),
        },
        {
            "canonical_name": "National Institute of Biomedical Innovation, Health and Nutrition",
            "category": "science_agency",
            "name_variants": [
                "national institute of biomedical innovation, health and nutrition",
                "nibiohn",
                "医薬基盤・健康・栄養研究所",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "preferred_match_groups": [
                _japan_operating_match_group(
                    r"national institute of biomedical innovation, health and nutrition",
                    r"医薬基盤・健康・栄養研究所",
                    r"\bnibiohn\b",
                ),
            ],
            "enforce_preferred_match_groups": True,
            "choose_smallest_match": True,
            "active_years": (2005, 2099),
        },
        {
            "canonical_name": "MEXT (Ministry of Education, Culture, Sports, Science and Technology)",
            "category": "science_agency",
            "name_variants": [
                "ministry of education, culture, sports, science and technology",
                "mext",
                "文部科学省",
                "monbu kagakusho",
                "monkasho",
            ],
            "preferred_item_type": ["section_total"],
            "prefer_latest_page": True,
            "preferred_match_groups": [
                [
                    r"\bfy\s*\d{4}\s+(budget|approved budget)\b",
                    # NOTE: r"\bministry\s+jurisdiction\s+total\b" removed — it matched
                    # 所管合計 (ministry jurisdiction total = all-agency super-aggregate,
                    # e.g. ¥24T in 2010). Replaced by explicit patterns below.
                    r"\bministry\s+subtotal\b",
                    r"\bcommon expenses of the ministry\b",
                    r"\bministry of education, culture, sports, science and technology\b.*\b(total|budget|appropriation)\b",
                    # "Total for Ministry of Education..." (without "the" — avoids
                    # "Total for the Ministry..." which translates 所管合計)
                    r"\btotal for ministry of education\b",
                    # "Total under the Ministry of Education..." (early years)
                    r"\btotal under.*ministry of education\b",
                ],
                [
                    r"文部科学省.*(所管合計|本省計|合計)",
                    r"(令和|平成)\d+年度(予算額|当初予算額)",
                ],
            ],
            "enforce_preferred_match_groups": True,
            "choose_smallest_match": True,
            "exclude_match_groups": [
                [
                    r"(国立研究開発法人|独立行政法人).{0,40}運営費",
                    r"運営費交付金",
                    r"operating subsidy",
                    r"operating expenses?\s+grant",
                    r"operating expenses?\s+of",
                    r"operating expenses?\s+for",
                    r"operating appropriations?",
                    r"japan science and technology agency",
                    r"japan society for the promotion of science",
                    r"japan aerospace exploration agency",
                    r"japan atomic energy agency",
                    r"japan agency for marine[- ]earth science and technology",
                    r"national institute for materials science",
                    r"national institute of advanced industrial science and technology",
                    r"riken",
                    r"qst",
                    r"科学技術振興機構",
                    r"日本学術振興会",
                    r"宇宙航空研究開発機構",
                    r"日本原子力研究開発機構",
                    r"海洋研究開発機構",
                    r"物質・材料研究機構",
                    r"産業技術総合研究所",
                    r"理化学研究所",
                    r"量子科学技術研究開発機構",
                    # Exclude super-aggregate rows: 所管合計 = all-ministry jurisdiction
                    # total that bundles every subordinate agency (e.g. ¥24T in 2010).
                    # "total for the ministry" is the English translation of 所管合計.
                    r"\bjurisdiction total\b",
                    r"\b所管合計\b",
                    r"\bunder the jurisdiction\b",
                    r"\btotal for the ministry of education\b",
                    r"歳入",
                    r"主管歳入予算額",
                    r"納付金",
                    r"受入見込額",
                    r"revenue budget",
                    r"budget amount",
                    r"expected payment",
                    r"文部科学本省計",
                    r"ministry subtotal",
                    r"国立大学法人",
                ]
            ],
            "active_years": (2001, 2099),
            "notes": "Total MEXT budget appropriation — primary science ministry.",
        },
    ],

    # -----------------------------------------------------------------------
    # NEW ZEALAND
    # Hybrid panel across three eras:
    #   (1) DSIR / Scientific and Industrial Research
    #   (2) Crown Research / FRST transition
    #   (3) Vote Science, Innovation and Technology + named science funds
    # -----------------------------------------------------------------------
    "New Zealand": [
        {
            "canonical_name": "DSIR (New Zealand)",
            "category": "science_agency",
            "name_variants": [
                "department of scientific and industrial research",
                "scientific and industrial research",
                "vote-scientific and industrial research",
                "vote scientific and industrial research",
                "dsir",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1975, 1990),
            "max_amount_local": 300_000,
            "notes": "Dedicated science agency in the pre-CRI era.",
        },
        {
            "canonical_name": "Research, Science and Technology Vote (New Zealand)",
            "category": "rd_ministry",
            "name_variants": [
                "total for research, science and technology",
                "total appropriations for research, science and technology",
                "total for vote science and innovation",
            ],
            "preferred_item_type": ["section_total"],
            "strict_preferred_item_types": True,
            "active_years": (1990, 2011),
            "expected_years": [1990, 1995, 1996, 1997, 1998, 1999, 2001, 2010],
            "max_amount_local": 1_500_000_000,
            "notes": "Documented vote-level total for the FRST / RST transition era.",
        },
        {
            "canonical_name": "Crown Research Institutes (New Zealand)",
            "category": "science_agency",
            "name_variants": [
                "crown research institutes",
                "crown research institute",
                "crown research institute core funding",
                "crown research institutes core funding",
                "agresearch",
                "industrial research limited",
                "irl",
                "gns science",
                "institute of geological and nuclear sciences",
                "niwa",
                "national institute of water and atmospheric research",
                "landcare research",
                "plant and food research",
                "new zealand institute for plant and food research",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "strict_preferred_item_types": True,
            "active_years": (2011, 2015),
            "max_amount_local": 400_000,
            "notes": "Explicit CRI core-funding rows only; avoids indirect vote components.",
        },
        {
            "canonical_name": "Strategic Science Investment Fund (New Zealand)",
            "category": "rd_fund",
            "name_variants": [
                "strategic science investment fund",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "strict_preferred_item_types": True,
            "active_years": (2017, 2099),
            "expected_years": [2017, 2018, 2020, 2021, 2022, 2023, 2024, 2025],
            "max_amount_local": 500_000_000,
            "notes": "Explicit SSIF appropriations in the modern MBIE-era science vote.",
        },
        {
            "canonical_name": "Marsden Fund (New Zealand)",
            "category": "science_agency",
            "name_variants": [
                "marsden fund",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1998, 2099),
            "max_amount_local": 1_000_000,
            "notes": "Starts only once the fund amount is cleanly recoverable from the source tables.",
        },
        {
            "canonical_name": "Callaghan Innovation",
            "category": "science_agency",
            "name_variants": [
                "callaghan innovation - operations",
                "callaghan innovation operations",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (2015, 2099),
            "max_amount_local": 2_000_000,
            "notes": "Tracks the comparable agency / operations appropriation. Excludes early strategic-investment rows.",
        },
        {
            "canonical_name": "Endeavour Fund (New Zealand)",
            "category": "rd_fund",
            "name_variants": [
                "endeavour fund",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "strict_preferred_item_types": True,
            "active_years": (2018, 2099),
            "max_amount_local": 3_000_000,
            "notes": "Explicit modern contestable science fund visible in estimates acts.",
        },
        {
            "canonical_name": "Health Research Fund (New Zealand)",
            "category": "rd_fund",
            "name_variants": [
                "health research fund",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "strict_preferred_item_types": True,
            "active_years": (2016, 2099),
            "max_amount_local": 2_000_000,
        },
        {
            "canonical_name": "Partnered Research Fund (New Zealand)",
            "category": "rd_fund",
            "name_variants": [
                "partnered research fund",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "strict_preferred_item_types": True,
            "active_years": (2016, 2099),
            "max_amount_local": 1_000_000,
        },
        {
            "canonical_name": "Catalyst Fund (New Zealand)",
            "category": "rd_fund",
            "name_variants": [
                "catalyst fund",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "strict_preferred_item_types": True,
            "active_years": (2016, 2099),
            "max_amount_local": 1_000_000,
        },
    ],

    # -----------------------------------------------------------------------
    # SWEDEN
    # Currency: SEK. Unit: thousand throughout (tusental kronor / tkr).
    # Post-1994: Utgiftsområde (UO) system. UO 16 = key R&D area.
    # Pre-1994: Departement chapter structure.
    # Sources: Prop. XXXX/XX:1 Budgetproposition, annual Statsbudget.
    # -----------------------------------------------------------------------
    "Sweden": [

        # --- Post-2001 research councils ---
        {
            "canonical_name": "Vetenskapsradet (VR)",
            "category": "science_agency",
            "name_variants": [
                "vetenskapsr\u00e5det",
                "vetenskapsradet",
                "swedish research council",
                "vr",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (2001, 2099),
            # VR annual state appropriation ~8-10B SEK in 2024; >30B implausible.
            "max_amount_local": 30_000_000,   # in thousands SEK
            "notes": "Created 2001 from merger of NFR, TFR, MFR, HSFR, SJFR.",
        },
        {
            "canonical_name": "VINNOVA",
            "category": "science_agency",
            "name_variants": [
                "vinnova",
                "verket f\u00f6r innovationssystem",
                "swedish innovation agency",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (2001, 2099),
            # VINNOVA ~4B SEK/year in 2024.
            "max_amount_local": 15_000_000,
            "notes": "Swedish innovation agency created 2001. UO 24 Näringsliv.",
        },
        {
            "canonical_name": "Formas",
            "category": "science_agency",
            "name_variants": [
                "formas",
                "forskningsr\u00e5det f\u00f6r milj\u00f6",
                "swedish research council for environment",
                "swedish research council for sustainable development",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (2001, 2099),
            "max_amount_local": 5_000_000,
        },
        {
            "canonical_name": "Forte",
            "category": "science_agency",
            "name_variants": [
                "forte",
                "fas",
                "forskningsr\u00e5det f\u00f6r h\u00e4lsa",
                "swedish research council for health",
                "swedish research council for health working life and welfare",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1994, 2099),
            "max_amount_local": 3_000_000,
            "notes": "FAS until 2013 when renamed Forte.",
        },
        {
            "canonical_name": "SSF (Stiftelsen for Strategisk Forskning)",
            "category": "science_agency",
            "name_variants": [
                "ssf",
                "stiftelsen f\u00f6r strategisk forskning",
                "stiftelsen for strategisk forskning",
                "swedish foundation for strategic research",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1994, 2099),
            "max_amount_local": 3_000_000,
        },

        # --- Pre-2001 research councils (merged into VR 2001) ---
        {
            "canonical_name": "Naturvetenskapliga forskningsradet (NFR)",
            "category": "science_agency",
            "name_variants": [
                "naturvetenskapliga forskningsr\u00e5det",
                "naturvetenskapliga forskningsradet",
                "nfr",
                "swedish natural science research council",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1942, 2001),
            "max_amount_local": 5_000_000,
            "notes": "Merged into Vetenskapsradet 2001.",
        },
        {
            "canonical_name": "Teknikvetenskapliga forskningsradet (TFR)",
            "category": "science_agency",
            "name_variants": [
                "teknikvetenskapliga forskningsr\u00e5det",
                "teknikvetenskapliga forskningsradet",
                "tfr",
                "swedish research council for engineering sciences",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1994, 2001),
            "max_amount_local": 3_000_000,
            "notes": "Merged into Vetenskapsradet 2001.",
        },
        {
            "canonical_name": "Humanistisk-samhallsvetenskapliga forskningsradet (HSFR)",
            "category": "science_agency",
            "name_variants": [
                "humanistisk-samh\u00e4llsvetenskapliga forskningsr\u00e5det",
                "humanistisk-samhallsvetenskapliga forskningsradet",
                "hsfr",
                "swedish council for research in the humanities",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1977, 2001),
            "max_amount_local": 2_000_000,
            "notes": "Merged into Vetenskapsradet 2001.",
        },
        {
            "canonical_name": "Medicinska forskningsradet (MFR)",
            "category": "science_agency",
            "name_variants": [
                "medicinska forskningsr\u00e5det",
                "medicinska forskningsradet",
                "mfr",
                "swedish medical research council",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1945, 2001),
            "max_amount_local": 2_000_000,
            "notes": "Merged into Vetenskapsradet 2001.",
        },

        # --- Innovation agencies (pre-VINNOVA) ---
        {
            "canonical_name": "NUTEK (Narings- och teknikutvecklingsverket)",
            "category": "science_agency",
            "name_variants": [
                "nutek",
                "n\u00e4rings- och teknikutvecklingsverket",
                "narings- och teknikutvecklingsverket",
                "swedish national board for industrial and technical development",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1991, 2004),
            "max_amount_local": 5_000_000,
            "notes": "Succeeded STU 1991. R&D mandate transferred to VINNOVA 2001; "
                     "remaining functions to Tillväxtverket 2009.",
        },
        {
            "canonical_name": "STU (Styrelsen for Teknisk Utveckling)",
            "category": "science_agency",
            "name_variants": [
                "styrelsen f\u00f6r teknisk utveckling",
                "styrelsen for teknisk utveckling",
                "stu",
                "swedish board for technical development",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1968, 1991),
            "max_amount_local": 3_000_000,
            "notes": "Pre-NUTEK innovation agency. Succeeded by NUTEK 1991.",
        },

        # --- Applied research institutes ---
        {
            "canonical_name": "RISE (Research Institutes of Sweden)",
            "category": "science_agency",
            "name_variants": [
                "rise",
                "research institutes of sweden",
                "sp technical research institute",
                "sp sverige",
                "swerea",
                "innventia",
                "swedish institute for food and biotechnology",
                "sicomp",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (2005, 2099),
            "max_amount_local": 5_000_000,
            "notes": "RISE created 2017 from SP, Swerea, Innventia and others. "
                     "Predecessor SP dates to 1920.",
        },
        {
            "canonical_name": "SMHI",
            "category": "science_agency",
            "name_variants": [
                "smhi",
                "sveriges meteorologiska och hydrologiska institut",
                "swedish meteorological and hydrological institute",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1918, 2099),
            "max_amount_local": 2_000_000,
        },
        {
            "canonical_name": "Rymdstyrelsen",
            "category": "science_agency",
            "name_variants": [
                "rymdstyrelsen",
                "swedish national space agency",
                "swedish national space board",
                "snsa",
                "esa-avgiften",
                "rymdforskning",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1972, 2099),
            "max_amount_local": 3_000_000,
        },
        {
            "canonical_name": "FOI / FOA (Totalforsvaret forskningsinstitut)",
            "category": "science_agency",
            "name_variants": [
                "totalförsvarets forskningsinstitut",
                "totalforsvaret forskningsinstitut",
                "foi",
                "foa",
                "f\u00f6rsvarets forskningsanstalt",
                "forsvarets forskningsanstalt",
                "swedish defence research agency",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1945, 2099),
            "max_amount_local": 5_000_000,
            "notes": "FOA renamed FOI in 2001.",
        },

        # --- Universities ---
        {
            "canonical_name": "Kungliga Tekniska Hogskolan (KTH)",
            "category": "higher_education",
            "name_variants": [
                "kungliga tekniska h\u00f6gskolan",
                "kungliga tekniska hogskolan",
                "kth",
                "royal institute of technology",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1827, 2099),
            "max_amount_local": 15_000_000,
        },
        {
            "canonical_name": "Uppsala Universitet",
            "category": "higher_education",
            "name_variants": [
                "uppsala universitet",
                "university of uppsala",
                "uppsala university",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1477, 2099),
            "max_amount_local": 20_000_000,
        },
        {
            "canonical_name": "Lunds Universitet",
            "category": "higher_education",
            "name_variants": [
                "lunds universitet",
                "lund university",
                "lunds tekniska h\u00f6gskola",
                "lunds tekniska hogskola",
                "lth",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1666, 2099),
            "max_amount_local": 20_000_000,
        },
        {
            "canonical_name": "Stockholms Universitet",
            "category": "higher_education",
            "name_variants": [
                "stockholms universitet",
                "stockholm university",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1878, 2099),
            "max_amount_local": 15_000_000,
        },
        {
            "canonical_name": "Goteborgs Universitet",
            "category": "higher_education",
            "name_variants": [
                "g\u00f6teborgs universitet",
                "goteborgs universitet",
                "university of gothenburg",
                "goteborg university",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1891, 2099),
            "max_amount_local": 15_000_000,
        },
        {
            "canonical_name": "Umea Universitet",
            "category": "higher_education",
            "name_variants": [
                "ume\u00e5 universitet",
                "umea universitet",
                "ume\u00e5 university",
                "umea university",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1965, 2099),
            "max_amount_local": 10_000_000,
        },
        {
            "canonical_name": "Linkoepings Universitet",
            "category": "higher_education",
            "name_variants": [
                "link\u00f6pings universitet",
                "linkoepings universitet",
                "linkoping university",
                "liu",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1975, 2099),
            "max_amount_local": 10_000_000,
        },
        {
            "canonical_name": "Chalmers tekniska hogskola",
            "category": "higher_education",
            "name_variants": [
                "chalmers tekniska h\u00f6gskola",
                "chalmers tekniska hogskola",
                "chalmers",
                "chalmers university of technology",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1829, 2099),
            "max_amount_local": 10_000_000,
        },
        {
            "canonical_name": "Karolinska Institutet",
            "category": "higher_education",
            "name_variants": [
                "karolinska institutet",
                "karolinska institute",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1810, 2099),
            "max_amount_local": 10_000_000,
        },

        # --- Collective / ministry totals ---
        {
            "canonical_name": "Universiteterna (collective Sweden)",
            "category": "higher_education",
            "name_variants": [
                "universiteterna",
                "h\u00f6gskolor och universitet",
                "hogskolor och universitet",
                "swedish universities",
                "universities and university colleges",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1970, 2099),
            "notes": "Collective appropriation — use only when individual lines unavailable.",
        },
        {
            "canonical_name": "Utbildningsdepartementet / UO 16 (total)",
            "category": "rd_ministry",
            "name_variants": [
                "utbildningsdepartementet",
                "ministry of education and research",
                "utg.omr. 16",
                "utgiftsomr\u00e5de 16",
                "utgiftsomrade 16",
                "uo 16",
            ],
            "preferred_item_type": ["section_total"],
            "active_years": (1994, 2099),
            "notes": "UO 16 section total (post-1994). Use only if individual lines unavailable.",
        },
        {
            "canonical_name": "Naringsdepartementet / UO 24 (total)",
            "category": "rd_ministry",
            "name_variants": [
                "n\u00e4ringsdepartementet",
                "naringsdepartementet",
                "ministry of enterprise",
                "utg.omr. 24",
                "utgiftsomr\u00e5de 24",
                "uo 24",
            ],
            "preferred_item_type": ["section_total"],
            "active_years": (1994, 2099),
            "notes": "UO 24 section total. Contains VINNOVA and industrial R&D. "
                     "Use only if individual lines unavailable.",
        },
    ],

    # -----------------------------------------------------------------------
    # AUSTRIA
    # Currency: ATS (pre-2002) / EUR (2002+). Unit: thousand throughout.
    # Pre-2013: Einzelpläne (Kapitel). Post-2013: Untergliederungen (UG).
    # Sources: Bundesfinanzgesetz / Bundesvoranschlag, annual.
    # -----------------------------------------------------------------------
    "Austria": [

        # --- Primary R&D funding agencies ---
        {
            "canonical_name": "FWF (Fonds zur Forderung der wissenschaftlichen Forschung)",
            "category": "science_agency",
            "name_variants": [
                "fonds zur f\u00f6rderung der wissenschaftlichen forschung",
                "fonds zur forderung der wissenschaftlichen forschung",
                "fwf",
                "austrian science fund",
                "wissenschaftsfonds",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1967, 2099),
            "preferred_match_groups": [[
                r"\bfwf\b",
                r"fonds zur f[öo]rderung der wissenschaftlichen forschung",
                r"austrian science fund",
                r"wissenschaftsfonds",
            ]],
            "enforce_preferred_match_groups": True,
            "min_amount_local": 1,
            "max_amount_local": 1_500,
            "notes": "Annual state appropriation — core basic research funder.",
        },
        {
            "canonical_name": "FFG (Forschungsfoerderungsgesellschaft)",
            "category": "science_agency",
            "name_variants": [
                "forschungsf\u00f6rderungsgesellschaft",
                "forschungsforderungsgesellschaft",
                "ffg",
                "austrian research promotion agency",
                "fff",
                "forschungsf\u00f6rderungsfonds",
                "forschungsforderungsfonds",
                "forschungsf\u00f6rderungsfonds f\u00fcr die gewerbliche wirtschaft",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1967, 2099),
            "max_amount_local": 3_000,
            "notes": "FFG created 2004 from merger of FFF + BIT + ASA. "
                     "Pre-2004 match via 'fff' variant.",
        },
        {
            "canonical_name": "OAW (Osterreichische Akademie der Wissenschaften)",
            "category": "science_agency",
            "name_variants": [
                "\u00f6sterreichische akademie der wissenschaften",
                "osterreichische akademie der wissenschaften",
                "\u00f6aw",
                "oaw",
                "austrian academy of sciences",
                "akademie der wissenschaften",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1847, 2099),
            "preferred_match_groups": [[
                r"\böaw\b",
                r"\boaw\b",
                r"[öo]sterreichische akademie der wissenschaften",
                r"austrian academy of sciences",
                r"akademie der wissenschaften",
            ]],
            "enforce_preferred_match_groups": True,
            "min_amount_local": 1,
            "max_amount_local": 500,
        },
        {
            "canonical_name": "AIT Austrian Institute of Technology",
            "category": "science_agency",
            "name_variants": [
                "ait austrian institute of technology",
                "ait",
                "arsenal research",
                "arsenal forschungsgesellschaft",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (2009, 2099),
            "max_amount_local": 500,
            "notes": "Formed 2009 from Arsenal Research.",
        },
        {
            "canonical_name": "IST Austria",
            "category": "science_agency",
            "name_variants": [
                "institute of science and technology austria",
                "ista",
                "ist austria",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (2006, 2099),
            "max_amount_local": 500,
            "notes": "Annual state grant from BMBWF.",
        },
        {
            "canonical_name": "Christian Doppler Forschungsgesellschaft (CD-Labor)",
            "category": "science_agency",
            "name_variants": [
                "christian doppler forschungsgesellschaft",
                "christian doppler",
                "cd-labor",
                "cd labor",
                "cd-labore",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1988, 2099),
            "max_amount_local": 200,
        },
        {
            "canonical_name": "Ludwig Boltzmann Gesellschaft",
            "category": "science_agency",
            "name_variants": [
                "ludwig boltzmann gesellschaft",
                "lbg",
                "ludwig boltzmann institute",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1960, 2099),
            "max_amount_local": 100,
        },

        # --- International contributions ---
        {
            "canonical_name": "CERN-Beitrag (Austria)",
            "category": "direct_rd",
            "name_variants": [
                "cern-beitrag",
                "cern beitrag",
                "beitrag zu cern",
                "cern",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1959, 2099),
            "max_amount_local": 200,
        },
        {
            "canonical_name": "ESA-Beitrag (Austria)",
            "category": "direct_rd",
            "name_variants": [
                "esa-beitrag",
                "esa beitrag",
                "beitrag zur esa",
                "europ\u00e4ische weltraumorganisation",
                "europaische weltraumorganisation",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1975, 2099),
            "max_amount_local": 150,
        },

        # --- Universities ---
        {
            "canonical_name": "Universitat Wien",
            "category": "higher_education",
            "name_variants": [
                "universit\u00e4t wien",
                "universitat wien",
                "university of vienna",
                "wien universit\u00e4t",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1365, 2099),
            "max_amount_local": 1_500,  # in millions
        },
        {
            "canonical_name": "Technische Universitat Wien (TU Wien)",
            "category": "higher_education",
            "name_variants": [
                "technische universit\u00e4t wien",
                "technische universitat wien",
                "tu wien",
                "tu vienna",
                "technische hochschule wien",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1815, 2099),
            "max_amount_local": 1_000,  # in millions
        },
        {
            "canonical_name": "Universitat Graz (Karl-Franzens)",
            "category": "higher_education",
            "name_variants": [
                "universit\u00e4t graz",
                "universitat graz",
                "university of graz",
                "karl-franzens-universit\u00e4t graz",
                "karl-franzens-universitat graz",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1585, 2099),
            "max_amount_local": 700,  # in millions
        },
        {
            "canonical_name": "Technische Universitat Graz (TU Graz)",
            "category": "higher_education",
            "name_variants": [
                "technische universit\u00e4t graz",
                "technische universitat graz",
                "tu graz",
                "technische hochschule graz",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1811, 2099),
            "max_amount_local": 700_000,
        },
        {
            "canonical_name": "Johannes Kepler Universitat Linz (JKU)",
            "category": "higher_education",
            "name_variants": [
                "johannes kepler universit\u00e4t linz",
                "johannes kepler universitat linz",
                "jku",
                "jku linz",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1966, 2099),
            "max_amount_local": 500_000,
        },
        {
            "canonical_name": "Medizinische Universitat Wien (MedUni)",
            "category": "higher_education",
            "name_variants": [
                "medizinische universit\u00e4t wien",
                "medizinische universitat wien",
                "meduni wien",
                "medizinische universit\u00e4t",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (2004, 2099),
            "max_amount_local": 500_000,
            "notes": "Separated from Universität Wien in 2004.",
        },
        {
            "canonical_name": "Universitat Innsbruck",
            "category": "higher_education",
            "name_variants": [
                "universit\u00e4t innsbruck",
                "universitat innsbruck",
                "university of innsbruck",
                "leopold-franzens-universit\u00e4t innsbruck",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1669, 2099),
            "max_amount_local": 600_000,
        },
        {
            "canonical_name": "Wirtschaftsuniversitat Wien (WU)",
            "category": "higher_education",
            "name_variants": [
                "wirtschaftsuniversit\u00e4t wien",
                "wirtschaftsuniversitat wien",
                "wu wien",
                "vienna university of economics",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1898, 2099),
            "max_amount_local": 400_000,
        },
        {
            "canonical_name": "Universitat fur Bodenkultur Wien (BOKU)",
            "category": "higher_education",
            "name_variants": [
                "universit\u00e4t f\u00fcr bodenkultur wien",
                "universitat fur bodenkultur wien",
                "boku",
                "university of natural resources",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1872, 2099),
            "max_amount_local": 300_000,
        },

        # --- Collective / ministry totals ---
        {
            "canonical_name": "Universitaten (collective Austria)",
            "category": "higher_education",
            "name_variants": [
                "universit\u00e4ten",
                "universitat\u00e4n",
                "\u00f6sterreichische universit\u00e4ten",
                "globalbudgets universit\u00e4ten",
                "austrian universities",
                "globalbudget hochschulen",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1975, 2099),
            "notes": "Collective Globalbudget line — use only when individual lines unavailable.",
        },
        {
            "canonical_name": "BMBWF / Einzelplan 13 / UG 31 (total)",
            "category": "rd_ministry",
            "name_variants": [
                "bundesministerium f\u00fcr bildung wissenschaft und forschung",
                "bmbwf",
                "bmwf",
                "bmwv",
                "bmbwk",
                "bmwfw",
                "bundesministerium f\u00fcr wissenschaft und forschung",
                "ug 31",
                "untergliederung 31",
                "einzelplan 13",
                "wissenschaft und forschung",
            ],
            "preferred_item_type": ["section_total"],
            "strict_preferred_item_types": True,
            "preferred_match_groups": [[
                r"\btotal\b",
                r"\bsumme\b",
                r"total expenditures",
                r"wissenschaft und forschung",
                r"ug 31",
                r"untergliederung 31",
                r"einzelplan 13",
            ]],
            "enforce_preferred_match_groups": True,
            "active_years": (1975, 2099),
            "min_amount_local_ats": 10_000,
            "min_amount_local_eur": 1_000,
            "max_amount_local": 100_000,
            "notes": "Ministry name changed: BMWF → BMWV → BMBWK → BMWFW → BMBWF. "
                     "Use as section total only if individual FWF/FFG/ÖAW lines unavailable.",
        },
    ],
    # -----------------------------------------------------------------------
    # SPAIN
    # -----------------------------------------------------------------------
    "Spain": [
        {
            "canonical_name": "CSIC (Consejo Superior de Investigaciones Científicas)",
            "category": "science_agency",
            "name_variants": [
                "consejo superior de investigaciones científicas",
                "consejo superior de investigaciones cientificas",
                "csic",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "max_amount_local": 2_000_000_000,
            "active_years": (2018, 2099),
            "notes": "Core Spanish public research organism. Always present in budget.",
        },
        {
            "canonical_name": "AEI (Agencia Estatal de Investigación)",
            "category": "science_agency",
            "name_variants": [
                "agencia estatal de investigación",
                "agencia estatal de investigacion",
                "aei",
                "28.303",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "max_amount_local": 2_000_000_000,
            "active_years": (2018, 2099),
            "notes": "Created 2017, replaced DGI/DGICYT as main research grant body.",
        },
        {
            "canonical_name": "CDTI (Centro para el Desarrollo Tecnológico e Industrial)",
            "category": "innovation_instruments",
            "name_variants": [
                "centro para el desarrollo tecnológico e industrial",
                "centro para el desarrollo tecnologico e industrial",
                "centro para el desarrollo tecnológico y la innovación",
                "cdti",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "max_amount_local": 3_000_000_000,
            "active_years": (2020, 2020),
            "notes": "Industrial R&D loans and grants. Keep only for explicitly observed recent Spain year until additional years are verified.",
        },
        {
            "canonical_name": "ISCIII (Instituto de Salud Carlos III)",
            "category": "science_agency",
            "name_variants": [
                "instituto de salud carlos iii",
                "isciii",
                "28.106",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "max_amount_local": 1_000_000_000,
            "active_years": (2020, 2099),
            "notes": "Health research. Programme 465A. Keep only in recent explicit-organism years for Spain series.",
        },
        {
            "canonical_name": "CIEMAT",
            "category": "science_agency",
            "name_variants": [
                "centro de investigaciones energéticas medioambientales y tecnológicas",
                "centro de investigaciones energeticas medioambientales y tecnologicas",
                "ciemat",
                "28.103",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "max_amount_local": 250_000_000,
            "active_years": (2020, 2023),
            "notes": "Energy and environment research. Keep only in recent explicit-organism years for Spain series; omit the isolated 2009 fragment from the final panel.",
        },
        {
            "canonical_name": "Plan Nacional I+D (total R&D appropriation)",
            "category": "direct_rd",
            "name_variants": [
                "plan nacional de i+d",
                "fomento y coordinación de la investigación",
                "fomento y coordinacion de la investigacion",
                "promotion and coordination of scientific and technical research",
                "promotion and coordination of scientific research",
                "463b",
                "investigación científica y técnica",
                "investigacion cientifica y tecnica",
            ],
            "preferred_item_type": ["section_total"],
            "max_amount_local": 5_000_000_000,
            "active_years": (2002, 2016),
            "notes": "National R&D plan aggregate. Use as fallback when agency-level not available.",
            "aggregation_role_override": "section",
        },
        {
            "canonical_name": "Esteban Terradas National Institute of Aerospace Technology",
            "category": "science_agency",
            "name_variants": [
                "instituto nacional de tecnica aeroespacial esteban terradas",
                "instituto nacional de técnica aeroespacial esteban terradas",
                "inta esteban terradas",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "max_amount_local": 500_000_000,
            "active_years": (2022, 2022),
            "notes": "Explicit organism-level line observed in recent Spain budget tables; do not backcast historically.",
        },
        {
            "canonical_name": "Centre for Sociological Research",
            "category": "direct_rd",
            "name_variants": [
                "centro de investigaciones sociologicas",
                "centro de investigaciones sociológicas",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "max_amount_local": 250_000_000,
            "active_years": (2022, 2022),
            "notes": "Explicit institution line observed in recent Spain budget tables; keep only for observed recent year.",
        },
        {
            "canonical_name": "Spanish Metrology Center",
            "category": "direct_rd",
            "name_variants": [
                "centro espanol de metrologia",
                "centro español de metrología",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "max_amount_local": 250_000_000,
            "active_years": (2022, 2022),
            "notes": "Explicit institution line observed in recent Spain budget tables; keep only for observed recent year.",
        },
        {
            "canonical_name": "National Transplant Organization",
            "category": "direct_rd",
            "name_variants": [
                "organizacion nacional de trasplantes",
                "organización nacional de trasplantes",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "max_amount_local": 250_000_000,
            "active_years": (2022, 2022),
            "notes": "Explicit institution line observed in recent Spain budget tables; keep only for observed recent year.",
        },
        {
            "canonical_name": "Recovery and Resilience Mechanism",
            "category": "innovation_instruments",
            "name_variants": [
                "mecanismo de recuperacion y resiliencia",
                "mecanismo de recuperación y resiliencia",
                "recovery and resilience mechanism",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "max_amount_local": 3_000_000_000,
            "active_years": (2020, 2020),
            "notes": "Temporary recovery instrument; observed as a recent policy funding line, not a historical institution series.",
        },
    ],
    # -----------------------------------------------------------------------
    # FINLAND
    # -----------------------------------------------------------------------
    "Finland": [
        {
            "canonical_name": "Suomen Akatemia — tutkimusmäärärahat (research grants)",
            "category": "science_agency",
            "name_variants": [
                # Finnish canonical moment names
                "suomen akatemian tutkimusmäärärahat",
                "suomen akatemian tutkimusmaarararahat",
                "29.88.50",
                "29.60.50",
                # English translations used by LLM in line_description_en
                "academy of finland research grants",
                "academy research grants",
                "research grants of the academy of finland",
                "research grants from the academy of finland",
                "proposed research grants for the academy of finland",
                "research grants (transfer appropriation)",
                "scientific research (transfer appropriation)",
                "tutkimusmäärärahat",   # generic Finnish suffix catches section hits
            ],
            "preferred_item_type": ["line_item", "program_total"],
            # Caps: FIM era ≤ 900M (real peak ~835M in 2001); EUR era ≤ 600M
            "max_amount_local_fim": 900_000_000,
            "max_amount_local_eur": 600_000_000,
            "active_years": (1970, 2099),
            "notes": "Core basic research grants (moments 29.88.50+53). KEY Finnish R&D series.",
        },
        {
            "canonical_name": "Suomen Akatemia — toimintamenot (operating)",
            "category": "science_agency",
            "name_variants": [
                "suomen akatemian toimintamenot",
                "suomen akatemia toimintamenot",
                "29.88.21",
                "29.60.01",
                "academy operating costs",
                "operating costs of the academy of finland",
                "academy of finland operating costs",
                "suomen akatemian toiminta",
            ],
            "preferred_item_type": ["line_item"],
            "max_amount_local_fim": 200_000_000,
            "max_amount_local_eur": 45_000_000,
            "active_years": (1970, 2099),
            "notes": "Academy of Finland operating budget. Separate from research grants.",
        },
        {
            "canonical_name": "Business Finland / Tekes (innovation agency)",
            "category": "innovation_instruments",
            "name_variants": [
                "innovaatiorahoituskeskus business finland",
                "business finland",
                "tekes",
                "teknologian ja innovaatioiden kehittämiskeskus",
                "teknologian kehittämiskeskus",
                "teknologian kehittämiskeskuksen toimintamenot",
                "32.20.05",
                "32.20.06",
                "tekes operating costs",
                "operating costs of tekes",
                "business finland operating costs",
                "innovaatiorahoituskeskuksen toimintamenot",
                "technology development center",
                "technology development centre",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            # Tekes admin/operating only; FIM real range ~60-138M; EUR ~24-90M
            "max_amount_local_fim": 200_000_000,
            "max_amount_local_eur": 100_000_000,
            "active_years": (1983, 2099),
            "notes": "Tekes renamed Business Finland in 2018. Moment 32.20.06 (Tekes) → 32.20.05 (BF).",
        },
        {
            "canonical_name": "Business Finland / Tekes — public R&D grants",
            "category": "innovation_instruments",
            "name_variants": [
                "julkinen tutkimus- ja kehittämistoiminta",
                "avustukset tutkimukseen kehitykseen ja innovaatiotoimintaan",
                "avustukset teknologiseen tutkimukseen ja kehitykseen",
                "tutkimus- kehittämis- ja innovaatiotoiminnan tukeminen",
                "tutkimus- ja kehitystoiminta",
                "32.20.40",
                "32.20.83",
                "public r&d grants to companies",
                "public r&d activities",
                "grants for technological research and development",
                "research and development activities",
                "grants for research, development and innovation",
                "grants for research development and innovation",
                "support for research development and innovation",
                "support for research, development, and innovation",
            ],
            "preferred_item_type": ["line_item"],
            # R&D grants reached ~1B FIM (2001) and ~540M EUR (2025)
            "max_amount_local_fim": 1_100_000_000,
            "max_amount_local_eur": 600_000_000,
            "active_years": (1983, 2099),
            "notes": "Public R&D grants from Tekes/BF to companies and institutes. Moment 32.20.40.",
        },
        {
            "canonical_name": "VTT (Technical Research Centre of Finland)",
            "category": "science_agency",
            "name_variants": [
                "teknologian tutkimuskeskus vtt",
                "valtion teknillinen tutkimuskeskus",
                "vtt oy",
                "vtt",
                "32.01.02",
                "erityisavustus teknologian tutkimuskeskus",
                "valtionavustus teknologian tutkimuskeskus",
                "operating costs of vtt",
                "vtt operating costs",
                "state grant for vtt",
                "operating expenses of the technical research centre of finland",
                "operating costs of the technical research centre of finland",
                "state technical research centre paid research services",
                "paid research services of the technical research centre",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            # VTT state grant: FIM era ~89-570M (net budget incl commercial); EUR ~63-130M
            "max_amount_local_fim": 600_000_000,
            "max_amount_local_eur": 135_000_000,
            "active_years": (1942, 2099),
            "notes": "State-owned applied research company. Became VTT Oy in 2015.",
        },
        {
            "canonical_name": "GTK (Geological Survey of Finland)",
            "category": "science_agency",
            "name_variants": [
                "geologian tutkimuskeskus",
                "gtk",
                "32.01.04",
                "geologian tutkimuskeskuksen toimintamenot",
                "gtk operating costs",
                "operating costs of the geological survey",
                "geological survey of finland",
                "geologian tutkimuskeskuksen tulot",
            ],
            "preferred_item_type": ["line_item"],
            # GTK: FIM era ~15-225M; EUR era ~30-55M
            "max_amount_local_fim": 250_000_000,
            "max_amount_local_eur": 60_000_000,
            "active_years": (1885, 2099),
            "notes": "Geological Survey of Finland. Under TEM (Ministry of Economic Affairs).",
        },
        {
            "canonical_name": "Luke / MTT / Metla / RKTL (natural resources research)",
            "category": "science_agency",
            "name_variants": [
                "luonnonvarakeskus",
                "luke",
                "maa- ja elintarviketalouden tutkimuskeskus",
                "mtt",
                "metsäntutkimuslaitos",
                "metla",
                "riista- ja kalatalouden tutkimuslaitos",
                "rktl",
                "natural resources institute finland",
                "natural resources institute",
                "finnish food and natural resources agency",
                "forest research institute",
                "game and fisheries research",
            ],
            "preferred_item_type": ["line_item"],
            "max_amount_local_fim": 300_000_000,
            "max_amount_local_eur": 70_000_000,
            "active_years": (1900, 2099),
            "notes": "MTT + Metla + RKTL merged into Luke (Natural Resources Institute Finland) in 2015.",
        },
        {
            "canonical_name": "VATT (Government Institute for Economic Research)",
            "category": "science_agency",
            "name_variants": [
                "valtion taloudellinen tutkimuskeskus",
                "vatt",
                "28.30.02",
                "valtion taloudellisen tutkimuskeskuksen toimintamenot",
                "vatt operating costs",
                "total for vatt",
                "government institute for economic research",
            ],
            "preferred_item_type": ["line_item"],
            # VATT FIM ~19-125M (small econ research institute); EUR ~3-22M
            "max_amount_local_fim": 135_000_000,
            "max_amount_local_eur": 25_000_000,
            "active_years": (1990, 2099),
            "notes": "Government Institute for Economic Research (VATT). Founded 1990.",
        },
    ],
    "Belgium": [
        {
            "canonical_name": "BELSPO / Belgian Federal Science Policy",
            "category": "rd_ministry",
            "name_variants": [
                "belspo",
                "belgian science policy office",
                "wetenschapsbeleid",
                "politique scientifique",
                "space activities",
                "european space agency",
                "research and development at the international level",
                "r&d in the international framework",
                "r&d programs and actions - government initiatives",
                "pod wetenschapsbeleid",
                "spp politique scientifique",
                "diensten voor programmatie van het wetenschapsbeleid",
                "services de programmation de la politique scientifique",
                "programmatorische federale overheidsdienst wetenschapsbeleid",
                "politique scientifique fédérale",
                "federale diensten voor wetenschappelijke, technische en culturele aangelegenheden",
                "federal services for scientific, technical and cultural affairs",
                "services fédéraux des affaires scientifiques techniques et culturelles",
                "ostc",
                "sstc",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "strict_preferred_item_types": True,
            "preferred_match_groups": [[
                r"actions? (?:de|to) promot(?:ion|e).*(?:science|scientific) polic",
                r"research and development within the international framework",
                r"r&d at the international level",
                r"space activities",
                r"belgian participation in the activities of the european space agency",
                r"expenses related to contracts, agreements, and mandates concerning r&d programs and actions at the international level",
                r"expenses related to contracts, agreements, and mandates concerning r&d programs and actions at the national level",
                r"various international collaborations",
                r"operating expenses related to the belgian high representation for space policy",
                r"federal council of science policy",
            ]],
            "enforce_preferred_match_groups": True,
            "active_years": (1975, 2099),
        },
        {
            "canonical_name": "FNRS",
            "category": "science_agency",
            "name_variants": [
                "fnrs",
                "fonds de la recherche scientifique",
                "national foundation for scientific research",
                "nationale stichting voor de financiering van het wetenschappelijk onderzoek",
                "national foundation for the financing of scientific research",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            # In the current federal-budget corpus, FNRS only appears as a
            # named federal appropriation / debt-service line in the
            # late-1990s to 2001 window. Keeping it open-ended manufactures
            # fake gaps in years where the corpus does not expose FNRS as a
            # distinct federal budget line.
            "active_years": (1996, 2001),
            "expected_years": [1996, 1997, 1999, 2001],
        },
        {
            "canonical_name": "SCK CEN",
            "category": "science_agency",
            "name_variants": [
                "sck cen",
                "belgian nuclear research centre",
                "studiecentrum voor kernenergie",
                "centre d'etude de l'energie nucleaire",
                "centre d'étude de l'énergie nucléaire",
                "centre detude de lenergie nucleaire",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "preferred_match_groups": [[
                r"grant to",
                r"allocation to",
                r"subsid(?:y|ies) to",
                r"from the state budget",
                r"dotation",
            ]],
            "enforce_preferred_match_groups": True,
            # Belgium's federal budget starts surfacing SCK CEN as a distinct
            # named line in the extracted series from 1996 onward. Earlier
            # years mostly expose broader science-policy aggregates, so
            # expecting an institution-specific row manufactures fake gaps.
            "active_years": (1996, 2002),
        },
        {
            "canonical_name": "Royal Observatory of Belgium",
            "category": "science_agency",
            "name_variants": [
                "royal observatory of belgium",
                "observatoire royal de belgique",
                "koninklijke sterrenwacht van belgië",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[
                r"grant to",
                r"allocation to",
                r"from the state budget",
                r"dotation",
            ]],
            "enforce_preferred_match_groups": True,
            "active_years": (2001, 2004),
        },
        {
            "canonical_name": "Royal Meteorological Institute of Belgium",
            "category": "science_agency",
            "name_variants": [
                "royal meteorological institute of belgium",
                "institut royal météorologique de belgique",
                "institut royal meteorologique de belgique",
                "koninklijk meteorologisch instituut van belgië",
                "koninklijk meteorologisch instituut van belgie",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[
                r"grant to",
                r"allocation to",
                r"from the state budget",
                r"dotation",
            ]],
            "enforce_preferred_match_groups": True,
            "active_years": (2001, 2004),
        },
        {
            "canonical_name": "Belgian Institute for Space Aeronomy",
            "category": "science_agency",
            "name_variants": [
                "belgian institute for space aeronomy",
                "institut d'aéronomie spatiale de belgique",
                "institut d aeronomie spatiale de belgique",
                "institut daeronomie spatiale de belgique",
                "belgisch instituut voor ruimte-aeronomie",
                "belgisch instituut voor ruimte aeronomie",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[
                r"grant to",
                r"allocation to",
                r"from the state budget",
                r"dotation",
            ]],
            "enforce_preferred_match_groups": True,
            "active_years": (2001, 2004),
        },
        {
            "canonical_name": "Royal Belgian Institute of Natural Sciences",
            "category": "science_agency",
            "name_variants": [
                "royal belgian institute of natural sciences",
                "institut royal des sciences naturelles de belgique",
                "koninklijk belgisch instituut voor natuurwetenschappen",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[
                r"grant to",
                r"allocation to",
                r"from the state budget",
                r"dotation",
            ]],
            "enforce_preferred_match_groups": True,
            "active_years": (2001, 2004),
        },
        {
            "canonical_name": "Institute of Radioelements (IRE)",
            "category": "science_agency",
            "name_variants": [
                "institut de radioéléments",
                "institut de radioelements",
                "instituut voor radio-elementen",
                "institute of radioelements",
                "i.r.e.",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[
                r"grant to",
                r"allocation to",
                r"subsid(?:y|ies) to",
                r"from the state budget",
                r"dotation",
            ]],
            "enforce_preferred_match_groups": True,
            "active_years": (1996, 2002),
        },
        {
            "canonical_name": "Scientific Institute of Public Health / Louis Pasteur",
            "category": "science_agency",
            "name_variants": [
                "scientific institute of public health",
                "scientific institute of public health / louis pasteur",
                "institut scientifique de la santé publique - louis pasteur",
                "institut scientifique de la sante publique - louis pasteur",
                "institut scientifique de la santé publique",
                "institut scientifique de la sante publique",
                "wetenschappelijke instelling volksgezondheid - louis pasteur",
                "wetenschappelijk instituut volksgezondheid",
                "public health scientific institute - louis pasteur",
                "louis pasteur",
                "pasteur institute",
                "institut pasteur",
                "instituut pasteur",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "preferred_match_groups": [[
                r"totals? for (?:the )?program",
                r"total for public health scientific institute",
                r"pasteur institute$",
                r"operating expenses - pasteur institute",
                r"public health studies",
            ]],
            "enforce_preferred_match_groups": True,
            # The current federal corpus exposes a clean annual-scale row in
            # 1995 and again in the 2001 budget file. Later WIV/ISP lines in
            # 2012-2013 come from provisional January-March appropriations and
            # are intentionally excluded from the annual canonical panel.
            "active_years": (1995, 2001),
            "expected_years": [1995, 2001],
        },
        {
            "canonical_name": "Human Genetics Centers",
            "category": "science_agency",
            "name_variants": [
                "human genetics centers",
                "human genetics centre",
                "centres de génétique humaine",
                "centres de genetique humaine",
                "centra voor menselijke erfelijkheid",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[r"human genetics centers?"]],
            "enforce_preferred_match_groups": True,
            # Clean federal hits are limited to the late-BEF years; the 2002
            # EUR row in the current corpus is too thin to treat as a stable
            # annual series point.
            "active_years": (1997, 2001),
            "expected_years": [1997, 2001],
        },
        {
            "canonical_name": "Von Karman Institute",
            "category": "science_agency",
            "name_variants": [
                "von karman institute",
                "allocation to the von karman institute",
                "grant to the von karman institute",
                "institut von karman de dynamique des fluides",
                "von karman instituut voor stromingsdynamica",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "preferred_match_groups": [[
                r"allocation to the von karman institute",
                r"grant to the von karman institute",
                r"von karman institute",
            ]],
            "enforce_preferred_match_groups": True,
            # The federal panel only exposes clean named appropriations in
            # 2002 and 2004. The 2001 transport-ministry lines are related but
            # not clearly comparable one-for-one with the later federal grant.
            "active_years": (2002, 2004),
            "expected_years": [2002, 2004],
        },
    ],
    "Chile": [
        {
            "canonical_name": "CONICYT / ANID",
            "category": "science_agency",
            "name_variants": [
                "conicyt",
                "comisión nacional de investigación científica y tecnológica",
                "comision nacional de investigacion cientifica y tecnologica",
                "anid",
                "agencia nacional de investigación y desarrollo",
                "agencia nacional de investigacion y desarrollo",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1967, 2099),
        },
        {
            "canonical_name": "CORFO innovation and technology funding",
            "category": "innovation_instruments",
            "name_variants": [
                "comité innova chile",
                "comite innova chile",
                "innova chile",
                "corfo innova chile",
                "innovación empresarial - comité innova chile",
                "innovacion empresarial - comite innova chile",
                "fomento de la ciencia y la tecnología - comité innova chile",
                "fomento de la ciencia y la tecnologia - comite innova chile",
                "fomento de la ciencia y la tecnología - corfo",
                "fomento de la ciencia y la tecnologia - corfo",
                "fondo de innovación para la competitividad - emprendimiento",
                "fondo de innovacion para la competitividad - emprendimiento",
                "fondo de innovación, ciencia y tecnología",
                "fondo de innovacion, ciencia y tecnologia",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            # The observable CORFO innovation committee / Innova Chile lines
            # in the current Chile corpus start in 2006. Earlier years in the
            # folder do not expose a directly comparable CORFO innovation line.
            "active_years": (2006, 2099),
        },
        {
            "canonical_name": "Fund for the Promotion of Science and Technology",
            "category": "innovation_instruments",
            "name_variants": [
                "fondo de fomento ciencia y tecnología",
                "fondo de fomento ciencia y tecnologia",
                "fondo de fomento ciencia y tecnología (fondef)",
                "fondo de fomento ciencia y tecnologia (fondef)",
                "fund for the promotion of science and technology",
                "fund for the promotion of science and technology (fondef)",
                "fondef",
                "fomento de la ciencia y la tecnología - conicyt",
                "fomento de la ciencia y la tecnologia - conicyt",
                "promotion of science and technology - conicyt",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "active_years": (1981, 2099),
        },
        {
            "canonical_name": "INIA (Chile)",
            "category": "science_agency",
            "name_variants": ["inia", "instituto de investigaciones agropecuarias"],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1964, 2099),
        },
        {
            "canonical_name": "IFOP",
            "category": "science_agency",
            "name_variants": ["ifop", "instituto de fomento pesquero"],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1964, 2099),
        },
        {
            "canonical_name": "FIA (Chile)",
            "category": "innovation_instruments",
            "name_variants": [
                "fia",
                "fundación para la innovación agraria",
                "fundacion para la innovacion agraria",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1981, 2099),
        },
        {
            "canonical_name": "INACH (Instituto Antártico Chileno)",
            "category": "science_agency",
            "name_variants": [
                "instituto antártico chileno",
                "instituto antartico chileno",
                "inach",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1963, 2099),
        },
        {
            "canonical_name": "Chilean Institute of Public Health",
            "category": "science_agency",
            "name_variants": [
                "instituto de salud pública de chile",
                "instituto de salud publica de chile",
                "institute of public health of chile",
                "public health institute of chile",
                "chilean institute of public health",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1975, 2099),
        },
        {
            "canonical_name": "Chilean Nuclear Energy Commission",
            "category": "science_agency",
            "name_variants": [
                "comisión chilena de energía nuclear",
                "comision chilena de energia nuclear",
                "chilean nuclear energy commission",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1975, 2099),
        },
        {
            "canonical_name": "Fisheries Research Fund",
            "category": "direct_rd",
            "name_variants": [
                "fondo de investigación pesquera",
                "fondo de investigacion pesquera",
                "fisheries research fund",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "active_years": (1975, 2099),
        },
        {
            "canonical_name": "Public Innovation Committee",
            "category": "innovation_instruments",
            "name_variants": [
                "comité innovación publica",
                "comite innovacion publica",
                "public innovation committee",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "active_years": (2015, 2017),
            "expected_years": [2015, 2016, 2017],
        },
        {
            "canonical_name": "Excellence Centers - CORFO",
            "category": "innovation_instruments",
            "name_variants": [
                "centros de excelencia - corfo",
                "centers of excellence - corfo",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "active_years": (2016, 2020),
            "expected_years": [2016, 2017, 2018, 2019, 2020],
        },
        {
            "canonical_name": "Technological Consortiums - CORFO",
            "category": "innovation_instruments",
            "name_variants": [
                "consorcios tecnológicos - corfo",
                "consorcios tecnologicos - corfo",
                "programas y consorcios tecnológicos - corfo",
                "programas y consorcios tecnologicos - corfo",
                "technological consortiums - corfo",
                "technological programs and consortiums - corfo",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "active_years": (2016, 2025),
            "expected_years": [2016, 2021, 2022, 2023, 2024, 2025],
        },
        {
            "canonical_name": "FIE-Innovation and R&D for Enterprises (Innova Committee)",
            "category": "innovation_instruments",
            "name_variants": [
                "fie-innovación e i&d empresarial (comité innova)",
                "fie-innovacion e i&d empresarial (comite innova)",
                "fie - business innovation and r&d (innova committee)",
                "fie-innovation and r&d for enterprises (innova committee)",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "active_years": (2017, 2018),
            "expected_years": [2017, 2018],
        },
        {
            "canonical_name": "Internationalization of the Innovative Effort",
            "category": "innovation_instruments",
            "name_variants": [
                "internacionalización del esfuerzo innovador",
                "internacionalizacion del esfuerzo innovador",
                "internacionalización del esfuerzo innovador - comité innova chile",
                "internacionalizacion del esfuerzo innovador - comite innova chile",
                "internacionalización del esfuerzo innovador - comité innova",
                "internacionalizacion del esfuerzo innovador - comite innova",
                "internationalization of the innovative effort",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "active_years": (2010, 2010),
            "expected_years": [2010],
        },
        {
            "canonical_name": "Call for Agricultural Innovation Projects",
            "category": "innovation_instruments",
            "name_variants": [
                "convocatoria proyectos innovación agraria",
                "convocatoria proyectos innovacion agraria",
                "convocatoria proyectos innovación agraria (3030777-0)",
                "convocatoria proyectos innovacion agraria (3030777-0)",
                "fundacion para la innovación agraria - transferencia convocatoria proyectos de innovación agraria",
                "fundacion para la innovacion agraria - transferencia convocatoria proyectos de innovacion agraria",
                "fundación para la innovación agraria - transferencia convocatoria proyectos de innovación agraria",
                "call for agricultural innovation projects",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "active_years": (2018, 2018),
            "expected_years": [2018],
        },
    ],
    "Estonia": [
        {
            "canonical_name": "Ministry of Education and Research (Estonia)",
            "category": "rd_ministry",
            "name_variants": ["haridus- ja teadusministeerium", "ministry of education and research"],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1991, 2099),
        },
        {
            "canonical_name": "Estonian Research Council / Science Foundation",
            "category": "science_agency",
            "name_variants": [
                "eesti teadusagentuur",
                "estonian research council",
                "eesti teadusfond",
                "estonian science foundation",
                "teadusfond",
                "science foundation",
                "science fund",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1991, 2011),
            "exclude_match_groups": [
                [
                    r"hospital network",
                    r"acquisition and renovation of fixed assets",
                ],
            ],
        },
        {
            "canonical_name": "University of Tartu",
            "category": "higher_education",
            "name_variants": ["tartu ülikool", "tartu ulikool", "university of tartu"],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1994, 2006),
            "expected_years": [1994, 2006],
            "strict_exclude_match_groups": True,
            "exclude_match_groups": [
                [
                    r"resident",
                    r"genome center",
                    r"geenivaramu",
                    r"gene bank",
                    r"european college",
                    r"ajaloo ja arheoloogia",
                ],
            ],
        },
        {
            "canonical_name": "Tallinn University of Technology",
            "category": "higher_education",
            "name_variants": [
                "tallinna tehnikaülikool",
                "tallinna tehnikaulikool",
                "taltech",
                "tallinn university of technology",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1994, 2006),
            "expected_years": [1994, 2006],
            "strict_exclude_match_groups": True,
            "exclude_match_groups": [
                [
                    r"it crime",
                    r"kuritegude uurimise koolituskeskus",
                    r"technolog(y|ical) school",
                    r"tehnoloogiakool",
                ],
            ],
        },
        {
            "canonical_name": "Estonian Academy of Sciences",
            "category": "science_agency",
            "name_variants": [
                "eesti teaduste akadeemia",
                "estonian academy of sciences",
                "academy of sciences",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1991, 2011),
        },
        {
            "canonical_name": "Archimedes Foundation",
            "category": "science_agency",
            "name_variants": [
                "sihtasutus archimedes",
                "archimedes foundation",
                "archimedes",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (2003, 2011),
        },
        {
            "canonical_name": "Estonia R&D / Innovation Programmes (post-2011)",
            "category": "rd_programme",
            "name_variants": [
                "teadus- ja arendustegevuse ning innovatsiooni programm",
                "research and development and innovation program",
                "teadussiisteemi programm",
                "teadussusteemi programm",
                "support for the development of research institutions and the scientific community",
                "development support for research institutions and researchers",
                "teadusasutuste ja teadlaskonna arengu toetamine",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (2022, 2025),
            "expected_years": [2022, 2025],
            "notes": "Programmatic continuation after the institutional series. Use as a post-2011 hybrid bridge only, not as a directly comparable agency line.",
        },
    ],
    "Czech Republic": [
        {
            "canonical_name": "Grantová agentura České republiky (GA ČR)",
            "category": "science_agency",
            "notes": "Czech Science Foundation — competitive basic research grants. Major R&D funder from 1993.",
            "name_variants": [
                "grantová agentura české republiky",
                "grantova agentura ceske republiky",
                "ga čr",
                "ga cr",
                "grantová agentura",
                "grantova agentura",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1993, 2099),
            "min_amount_local": 1_000_000.0,
        },
        {
            "canonical_name": "Technologická agentura České republiky (TA ČR)",
            "category": "science_agency",
            "notes": "Technology Agency of the Czech Republic — applied research and innovation grants. Created 2009.",
            "name_variants": [
                "technologická agentura české republiky",
                "technologicka agentura ceske republiky",
                "ta čr",
                "ta cr",
                "technologická agentura",
                "technologicka agentura",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (2009, 2099),
            "min_amount_local": 1_000_000.0,
        },
        {
            "canonical_name": "Akademie věd České republiky (AV ČR)",
            "category": "science_agency",
            "notes": "Czech Academy of Sciences — network of research institutes. Major public R&D performer.",
            "name_variants": [
                "akademie věd české republiky",
                "akademie ved ceske republiky",
                "akademie věd čr",
                "akademie věd cr",
                "av čr",
                "av cr",
                "akademie věd",
                "akademie ved",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1993, 2099),
            "min_amount_local": 1_000_000.0,
        },
    ],
    "Iceland": [
        # ── Post-2003: Rannís ─────────────────────────────────────────────────
        {
            "canonical_name": "Rannís (Icelandic Centre for Research)",
            "category": "science_agency",
            "name_variants": [
                "rannís", "rannis", "icelandic centre for research",
                "rannsóknamiðstöð íslands",
                "research center of iceland",
                "research centre of iceland",
                "icelandic research centre",
                "icelandic research center",
                # Post-2015 functional category names (Iceland budget shifted from
                # institutional to objective-based presentation; Rannís no longer
                # labelled by name but appears under these headings):
                "samkeppnissjóðir í rannsóknum",
                "samkeppnissjodur i rannsoknum",
                "vísindi og samkeppnissjóðir í rannsóknum",
                "visindi og samkeppnissjodur i rannsoknum",
                "samkeppnissjóðir",
            ],
            "preferred_item_type": ["program_total", "section_total", "line_item"],
            "preferred_match_groups": [
                [
                    r"\brann[ií]s\b",
                    r"ranns[oó]knami[ðd]st[oö]?[ðd]\s+[ií]slands",
                    r"research cent(?:er|re) of iceland",
                    r"icelandic research cent(?:er|re)",
                    # Functional category patterns for 2015+ budget format:
                    r"samkeppnissjó[ðd]ir\s+[ií]\s+ranns[oó]knum",
                    r"v[ií]sindi\s+og\s+samkeppnissjó[ðd]ir",
                    r"samkeppnissjó[ðd]ir",
                ],
            ],
            "enforce_preferred_match_groups": True,
            "active_years": (2003, 2099),
            "max_amount_local": 10_000_000_000,
            "notes": "Created 2003 merging Rannsóknaráð + Vísindaráð + Vísindasjóður + Rannsóknasjóður. Post-2015 appears under functional category 'Samkeppnissjóðir í rannsóknum'.",
        },
        # ── Pre-2003 research governance ──────────────────────────────────────
        {
            "canonical_name": "Rannsóknaráð ríkisins (National Research Council)",
            "category": "science_agency",
            "name_variants": [
                "rannsóknaráð ríkisins", "rannsoknarad rikisins",
                "rannseknarad rikisins", "rannséknaråd rikisins",
                "national research council of iceland",
                "national research council",
                "02-232",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "active_years": (1960, 2003),
            "max_amount_local": 2_000_000_000,
            "notes": "National Research Council. 3-digit code 232 (early), two-tier 02-232 (later). Merged into Rannís 2003.",
        },
        {
            "canonical_name": "Vísindasjóður (Science Fund)",
            "category": "science_agency",
            "name_variants": [
                "vísindasjóður", "visindasjodur", "vísindasj", "visindasj",
                "science fund",
                "02-235",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "active_years": (1960, 2003),
            "max_amount_local": 2_000_000_000,
            "notes": "Science Fund. Code 975 in early budgets, later 02-235. Merged into Rannís 2003.",
        },
        {
            "canonical_name": "Vísindaráð (Science Council)",
            "category": "science_agency",
            "name_variants": [
                "vísindaráð", "visindarad", "visindarad",
                "science council",
                "02-234",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "active_years": (1975, 2003),
            "max_amount_local": 2_000_000_000,
            "notes": "Science Council. Merged into Rannís 2003.",
        },
        {
            "canonical_name": "Rannsóknasjóður (Research Fund)",
            "category": "science_agency",
            "name_variants": [
                "rannsóknasjóður", "rannsoknasjodur", "rannséknasj",
                "research fund",
                "02-233",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "active_years": (1975, 2003),
            "max_amount_local": 4_000_000_000,
            "notes": "Research Fund. Distinct from Vísindasjóður. Merged into Rannís 2003.",
        },
        # ── Universities ──────────────────────────────────────────────────────
        {
            "canonical_name": "Háskóli Íslands (University of Iceland)",
            "category": "higher_education",
            "name_variants": [
                "háskóli íslands", "haskoli islands", "university of iceland",
                "háskôli islands", "háskóli íslands",
                "02-201",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "exclude_match_groups": [[
                r"performance-based funding",
                r"social role",
                r"funding for the increase in university students",
                r"northern volcano station",
            ]],
            "active_years": (1911, 2099),
            "max_amount_local": 40_000_000_000,
        },
        {
            "canonical_name": "Háskólinn á Akureyri (University of Akureyri)",
            "category": "higher_education",
            "name_variants": [
                "háskólinn á akureyri", "haskólinn a akureyri",
                "university of akureyri",
                "02-210",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1987, 2099),
            "max_amount_local": 10_000_000_000,
        },
        {
            "canonical_name": "Raunvísindastofnun Háskólans (Science Institute of the University of Iceland)",
            "category": "science_agency",
            "name_variants": [
                "raunvísindastofnun háskólans",
                "raunvisindastofnun háskólans",
                "raunvisindastofnun haskolans",
                "science institute of the university of iceland",
                "science institute of university of iceland",
                "17-203",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "active_years": (1980, 2024),
            "max_amount_local": 3_000_000_000,
            "notes": "Validated against original Iceland budget files. Distinct science institute within the University of Iceland system.",
        },
        {
            "canonical_name": "Tilraunastöð Háskólans að Keldum (University Experimental Station at Keldur)",
            "category": "science_agency",
            "name_variants": [
                "tilraunastöð háskólans að keldum",
                "tilraunastöð háskólans á keldum",
                "tilraunastod háskolans að keldum",
                "tilraunastod haskolans a keldum",
                "experimental station of the university of iceland at keldur",
                "experimental station of the university of iceland at keldum",
                "university experimental station at keldur",
                "university experimental station at keldum",
                "02-202",
                "17-202",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "active_years": (1990, 2025),
            "max_amount_local": 1_500_000_000,
            "notes": "Validated against original Iceland budget files; recurring standalone experimental station under the University of Iceland.",
        },
        {
            "canonical_name": "Landbúnaðarháskóli Íslands (Agricultural University of Iceland)",
            "category": "higher_education",
            "name_variants": [
                "landbúnaðarháskóli íslands",
                "landbunadarhaskoli islands",
                "agricultural university of iceland",
                "17-216",
                "02-216",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "active_years": (2013, 2025),
            "max_amount_local": 3_000_000_000,
            "notes": "Validated against original Iceland budget files.",
        },
        # ── Research institutes ───────────────────────────────────────────────
        {
            "canonical_name": "Hafrannsóknastofnun (Marine Research Institute)",
            "category": "science_agency",
            "name_variants": [
                "hafrannsóknastofnun", "hafrannsoknarstofnun",
                "hafrannséknastofnun",
                "haf- og vatnarannsóknir",
                "haf og vatnarannsoknir",
                "marine research institute", "marine and freshwater research institute",
                "marine and freshwater research",
                "hafrannsóknastofnunin",
                "05-202",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "active_years": (1975, 2099),
            "max_amount_local": 12_000_000_000,
            "notes": "Under fisheries ministry (code 05-). Include research vessel and monitoring lines.",
        },
        {
            "canonical_name": "Fiskirannsóknastofnun / Rannsóknastofnun fiskiðnaðarins (Research Institute of Fisheries)",
            "category": "science_agency",
            "name_variants": [
                "fiskirannsóknastofnun",
                "fiskirannsoknastofnun",
                "rannsóknastofnun fiskiðnaðarins",
                "rannsoknastofnun fiskidnadarins",
                "research institute of fisheries",
                "research institute of the fishing industry",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "active_years": (1990, 2006),
            "max_amount_local": 600_000_000,
            "notes": "Validated against original Iceland budget files; separate fisheries research institute before later restructuring.",
        },
        {
            "canonical_name": "Rannsóknastofnun byggingariðnaðarins (Research Institute of the Construction Industry)",
            "category": "science_agency",
            "name_variants": [
                "rannsóknastofnun byggingariðnaðarins",
                "rannsoknastofnun byggingariðnaðarins",
                "rannsoknastofnun byggingaridnadarins",
                "research institute of the construction industry",
                "11-203",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "active_years": (1985, 2007),
            "max_amount_local": 500_000_000,
            "notes": "Validated against original Iceland budget files.",
        },
        {
            "canonical_name": "Orkustofnun / ÍSOR (Energy and Geothermal Research)",
            "category": "science_agency",
            "name_variants": [
                "orkustofnun", "national energy authority",
                "ísor", "isor", "iceland geosurvey",
                "energy agency",
                "11-301",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "active_years": (1975, 2099),
            "max_amount_local": 2_000_000_000,
            "notes": "Orkustofnun (code 11-301) under industry ministry. ÍSOR (Iceland GeoSurvey) spun off ~2003 for geothermal/geological research.",
        },
        {
            "canonical_name": "Veðurstofa Íslands (Icelandic Meteorological Office)",
            "category": "science_agency",
            "name_variants": [
                "veðurstofa íslands", "vedurstofa islands",
                "icelandic meteorological office",
                "veðurstofa íslands",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "active_years": (1975, 2099),
            "max_amount_local": 5_000_000_000,
        },
        {
            "canonical_name": "Náttúrufræðistofnun Íslands (Icelandic Institute of Natural History)",
            "category": "science_agency",
            "name_variants": [
                "náttúrufræðistofnun íslands",
                "natturufraedistofnun islands",
                "icelandic institute of natural history",
                "natural history institute of iceland",
                "14-401",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "active_years": (2019, 2024),
            "max_amount_local": 1_500_000_000,
            "notes": "Validated against original Iceland budget files.",
        },
        # ── Innovation and research funds ────────────────────────────────────
        {
            "canonical_name": "Tækniþróunarsjóður (Technology Development Fund)",
            "category": "innovation_instruments",
            "name_variants": [
                "tækniþróunarsjóður",
                "taeknithrounarsjodur",
                "technology development fund",
                "11-242",
                "04-511",
                "17-511",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "active_years": (2004, 2025),
            "max_amount_local": 4_000_000_000,
            "notes": "Validated against original Iceland budget files.",
        },
        {
            "canonical_name": "Rannsóknarnámssjóður (Research Scholarship Fund)",
            "category": "innovation_instruments",
            "name_variants": [
                "rannsóknarnámssjóður",
                "rannsoknarnamssjodur",
                "research scholarship fund",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "active_years": (2003, 2012),
            "max_amount_local": 150_000_000,
            "notes": "Validated against original Iceland budget files.",
        },
        {
            "canonical_name": "Verkefnasjóður sjávarútvegsins (Project Fund for Fisheries)",
            "category": "innovation_instruments",
            "name_variants": [
                "verkefnasjóður sjávarútvegsins",
                "verkefnasjodur sjavarutvegsins",
                "project fund for fisheries",
                "research fund to increase the value of marine products",
                "rannsóknasjóður til að auka verðmæti sjávarfangs",
                "rannsoknasjodur til ad auka verdmaeti sjavarfangs",
                "04-413",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "active_years": (2006, 2024),
            "max_amount_local": 500_000_000,
            "notes": "Validated against original Iceland budget files; later documents use Verkefnasjóður sjávarútvegsins for the same fisheries project-fund line.",
        },
        {
            "canonical_name": "Sjóður til síldarrannsókna (Fund for Herring Research)",
            "category": "innovation_instruments",
            "name_variants": [
                "sjóður til síldarrannsókna",
                "sjodur til sildarrannsokna",
                "fund for herring research",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "active_years": (2005, 2022),
            "max_amount_local": 50_000_000,
            "notes": "Validated against original Iceland budget files.",
        },
        {
            "canonical_name": "Byggingarsjóður rannsókna í þágu atvinnuveganna (Building Fund for Industry Research)",
            "category": "innovation_instruments",
            "name_variants": [
                "byggingarsjóður rannsókna í þágu atvinnuveganna",
                "byggingarsjodur rannsokna i thagu atvinnuveganna",
                "byggingasjóður rannsókna í þágu atvinnuveganna",
                "building fund for industry research",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "active_years": (1977, 2021),
            "max_amount_local": 4_000_000_000,
            "notes": "Validated against original Iceland budget files; long-lived fund supporting sectoral research infrastructure and projects.",
        },
        {
            "canonical_name": "Nýsköpunarmiðstöð Íslands (Innovation Centre of Iceland)",
            "category": "science_agency",
            "name_variants": [
                "nýsköpunarmiðstöð íslands",
                "nyskopunarmidstod islands",
                "innovation centre of iceland",
                "innovation center of iceland",
                "04-501",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "active_years": (2011, 2020),
            "max_amount_local": 2_000_000_000,
            "notes": "Validated against original Iceland budget files.",
        },
        # ── Additional higher-education institutions ────────────────────────
        {
            "canonical_name": "Háskólinn á Bifröst (University of Bifröst)",
            "category": "higher_education",
            "name_variants": [
                "háskólinn á bifröst",
                "haskolinn a bifrost",
                "university of bifröst",
                "university of bifrost",
                "17-225",
                "02-225",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "active_years": (2021, 2025),
            "max_amount_local": 1_000_000_000,
            "notes": "Validated against original Iceland budget files.",
        },
        {
            "canonical_name": "Háskólinn í Reykjavík (Reykjavik University)",
            "category": "higher_education",
            "name_variants": [
                "háskólinn í reykjavík",
                "haskolinn i reykjavik",
                "reykjavik university",
                "university of reykjavik",
                "17-227",
                "02-227",
            ],
            "preferred_item_type": ["line_item", "program_total", "section_total"],
            "active_years": (2004, 2024),
            "max_amount_local": 6_000_000_000,
            "notes": "Validated against original Iceland budget files.",
        },
    ],
    "Hungary": [
        {
            "canonical_name": "Hungarian Academy of Sciences (MTA)",
            "category": "science_agency",
            "name_variants": [
                "magyar tudományos akadémia",
                "magyar tudomanyos akademia",
                "mta",
                "mta kutatóközpontok",
                "mta kutatóintézetek",
                "mta támogatott kutatóhelyek",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1991, 2099),
            "max_amount_local": 200_000_000,
        },
        {
            "canonical_name": "MTA Library and Information Centre",
            "category": "science_agency",
            "name_variants": [
                "mta könyvtár és információs központ",
                "mta konyvtar es informacios kozpont",
                "mta könyvtára",
                "mta konyvtara",
                "library and information centre",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (2013, 2099),
            "max_amount_local": 20_000_000,
        },
        {
            "canonical_name": "Research and Technological Innovation Fund",
            "category": "innovation_instruments",
            "name_variants": [
                "kutatási és technológiai innovációs alap",
                "kutatasi es technologiai innovacios alap",
                "lxix. kutatási és technológiai innovációs alap",
                "lxix. kutatasi es technologiai innovacios alap",
                "hazai innováció támogatása",
                "hazai innovacio tamogatasa",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (2010, 2014),
            "max_amount_local": 60_000_000,
        },
        {
            "canonical_name": "National Research, Development and Innovation Fund (Hungary)",
            "category": "innovation_instruments",
            "name_variants": [
                "nemzeti kutatási, fejlesztési és innovációs alap",
                "nemzeti kutatasi, fejlesztesi es innovacios alap",
                "nkfi alap",
                "hazai innováció támogatása",
                "hazai innovacio tamogatasa",
                "a nemzetközi együttműködésben megvalósuló innováció támogatása",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (2015, 2099),
            "max_amount_local": 200_000_000,
        },
        {
            "canonical_name": "Eötvös Loránd Research Network",
            "category": "science_agency",
            "name_variants": [
                "xxxvi. eötvös loránd kutatási hálózat",
                "xxxvi. eotvos lorand kutatasi halozat",
                "elkh kutatóközpontok",
                "elkh kutatóintézetek",
                "elkh támogatott kutatóhelyek",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (2020, 2023),
            "max_amount_local": 100_000_000,
        },
        {
            "canonical_name": "Hungarian Research Network",
            "category": "science_agency",
            "name_variants": [
                "xxxvi. magyar kutatási hálózat",
                "xxxvi. magyar kutatasi halozat",
                "hun-ren központ és kutatóhálózat",
                "hun-ren kozpont es kutatohalozat",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (2024, 2099),
            "max_amount_local": 100_000_000,
        },
        {
            "canonical_name": "INTERREG IVC Programme",
            "category": "innovation_instruments",
            "name_variants": [
                "interreg ivc",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (2011, 2015),
            "max_amount_local": 10_000_000,
        },
        {
            "canonical_name": "National Agricultural Research and Innovation Centre (Hungary)",
            "category": "science_agency",
            "name_variants": [
                "nemzeti agrárkutatási és innovációs központ",
                "nemzeti agrarkutatasi es innovacios kozpont",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            # The institution is only visible in the audited source texts from
            # 2016 through 2021. Earlier and later years reference different
            # agricultural support structures, so keeping a wider active window
            # creates avoidable false gaps and blocks cleaner future discovery.
            "active_years": (2016, 2021),
            "max_amount_local": 50_000_000,
        },
    ],
    "Latvia": [
        {
            "canonical_name": "Science Programme (Latvia)",
            "category": "rd_ministry",
            "name_variants": [
                "zinātne",
                "zinatne",
                "kopā zinātnes finansēšanai",
                "kopa zinatnes finansesanai",
                "science programme",
            ],
            "preferred_item_type": ["program_total", "section_total"],
            "strict_preferred_item_types": True,
            "active_years": (1992, 2009),
            "expected_years": [1992, 1993, 1995, 1996, 1998, 1999, 2000, 2001, 2003, 2006],
            "min_amount_local": 250_000,
            "max_amount_local": 20_000_000,
            "exclude_match_groups": [[
                r"science base funding",
                r"fundamental scientific research",
                r"state-commissioned scientific research",
                r"state administration institution ordered research",
                r"state institutions' ordered research",
                r"development of scientific activities in universities",
                r"provision and development of scientific infrastructure in universities",
                r"promotion of scientific competitiveness",
                r"state research programs",
                r"latvian science council",
                r"participation in the eu framework programme",
                r"participation in european union framework programme",
                r"other european community programs",
            ]],
            "notes": "Hybrid programme-level canonical for Latvian science budget lines across eras.",
        },
        {
            "canonical_name": "Fundamental Scientific Research (Latvia)",
            "category": "direct_rd",
            "name_variants": [
                "fundamentālie zinātniskie pētījumi",
                "fundamentalie zinatniskie petijumi",
                "fundamentālie zinatniskie pētījumi",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "strict_preferred_item_types": True,
            "active_years": (1996, 2009),
            "expected_years": [1996, 2000, 2006],
            "max_amount_local": 50_000_000,
        },
        {
            "canonical_name": "Science Base Funding (Latvia)",
            "category": "science_agency",
            "name_variants": [
                "zinātnes bāzes finansējums",
                "zinatnes bazes finansējums",
                "science base funding",
                "zinātniskās darbības nodrošinājums",
                "zinatniskas darbibas nodrosinajums",
                "zinātniskās darbības nodrošināšana",
                "zinatniskas darbibas nodrosinasana",
                "ensuring scientific activity",
                "ensuring scientific activities",
                "provision of scientific activities",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "strict_preferred_item_types": True,
            "active_years": (1997, 2009),
            "expected_years": [1997, 1999, 2000, 2001, 2003, 2006, 2009],
            "max_amount_local": 20_000_000,
            "exclude_match_groups": [[
                r"resources for expenditure coverage",
                r"subsidy from general revenues?",
                r"grant from general revenues?",
                r"remuneration",
                r"current expenditures",
                r"maintenance expenditures",
            ]],
        },
        {
            "canonical_name": "State-Commissioned Scientific Research (Latvia)",
            "category": "direct_rd",
            "name_variants": [
                "valsts pārvaldes institūciju pasūtītie zinātniskie pētījumi",
                "valsts parvaldes instituciju pasutitie zinatniskie petijumi",
                "state-commissioned scientific research",
                "state administration institution ordered research",
                "state institutions' ordered research",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "strict_preferred_item_types": True,
            "active_years": (1996, 2009),
            "expected_years": [1996, 1997, 1998, 2000, 2001, 2003],
            "max_amount_local": 20_000_000,
        },
        {
            "canonical_name": "Latvian Academy of Sciences",
            "category": "science_agency",
            "name_variants": [
                "latvijas zinātņu akadēmija",
                "latvijas zinatnu akademija",
                "lza",
                "latvian academy of sciences",
                "latvijas akadēmiskā bibliotēka",
                "latvijas akademiska biblioteka",
            ],
            "preferred_item_type": ["program_total", "section_total", "line_item"],
            "active_years": (1992, 1993),
            "expected_years": [1992, 1993],
            "max_amount_local": 500_000,
        },
        {
            "canonical_name": "University Science Development (Latvia)",
            "category": "higher_education",
            "name_variants": [
                "zinātniskās darbības attīstība universitātēs",
                "zinatniskas darbibas attistiba universitatem",
                "zinātniskās infrastruktūras nodrošināšana un attīstība augstskolās",
                "zinatniskas infrastrukturas nodrosinasana un attistiba augstskolas",
                "investīcijas zinātnei",
                "investicijas zinatnei",
                "additional funding for the development of scientific activities in higher education institutions and colleges",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1996, 2015),
            "expected_years": [1996, 2000, 2006, 2015],
            "max_amount_local": 50_000_000,
        },
        {
            "canonical_name": "State Research Programmes (Latvia)",
            "category": "direct_rd",
            "name_variants": [
                "valsts pētījumu programmas",
                "valsts petijumu programmas",
                "state research programs",
                "state research program in energy",
                "valsts pētījumu programma enerģētikā",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "active_years": (2009, 2018),
            "expected_years": [2009, 2018],
            "max_amount_local": 20_000_000,
        },
    ],
    "Lithuania": [
        {
            "canonical_name": "Science and Studies Programme (Lithuania)",
            "category": "rd_ministry",
            "name_variants": [
                "mokslas ir studijos",
                "mokslas ir studijos.",
                "science and studies",
            ],
            "preferred_item_type": ["program_total", "section_total"],
            "active_years": (1991, 2099),
            "max_amount_local": 5_000_000_000,
            "notes": "Hybrid programme-level canonical for broad science-and-studies appropriations.",
        },
        {
            "canonical_name": "State Science, Studies and Technology Service (Lithuania)",
            "category": "science_agency",
            "name_variants": [
                "valstybinė mokslo, studijų ir technologijų tarnyba",
                "valstybine mokslo studiju ir technologiju tarnyba",
                "mokslo ir studijų departamentas prie švietimo ir mokslo ministerijos",
                "mokslo ir studiju departamentas prie svietimo ir mokslo ministerijos",
                "science and studies department under the ministry of education and science",
                "state science studies and technology service",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1991, 2004),
            "expected_years": [1994, 1999, 2001],
            "max_amount_local": 500_000_000,
            "notes": "1993 original is documented but excluded from the final panel because the pre-litas thousand-talonas appropriation is not methodologically comparable to the later series.",
        },
        {
            "canonical_name": "Lithuanian Research Council",
            "category": "science_agency",
            "name_variants": [
                "lietuvos mokslo taryba",
                "lithuanian research council",
                "research council of lithuania",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1991, 2099),
            "expected_years": [2005, 2009, 2010, 2011, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025],
            "max_amount_local": 1_000_000_000,
        },
        {
            "canonical_name": "Centre for Physical Sciences and Technology (Lithuania)",
            "category": "science_agency",
            "name_variants": [
                "valstybinis mokslinių tyrimų institutas fizinių ir technologijos mokslų centras",
                "valstybinis moksliniu tyrimu institutas fiziniu ir technologijos mokslu centras",
                "fizinių ir technologijos mokslų centras",
                "fiziniu ir technologijos mokslu centras",
                "centre for physical sciences and technology",
                "center for physical sciences and technology",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (2011, 2099),
            "max_amount_local": 2_000_000_000,
            "notes": "Explicit FTMC institutional appropriation only; avoids generic research-institutions false positives.",
        },
        {
            "canonical_name": "Lithuanian Genocide and Resistance Research Centre",
            "category": "science_agency",
            "name_variants": [
                "lietuvos gyventojų genocido ir rezistencijos tyrimo centras",
                "lithuanian genocide and resistance research centre",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1997, 2099),
            "expected_years": [1997, 1998, 1999, 2001, 2005, 2009, 2010, 2011, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025],
            "max_amount_local": 50_000_000,
            "notes": "Verified against original Lithuania budget files using the institution total column where multi-column appropriation tables are present.",
        },
    ],
    "Luxembourg": [
        # ── FNR ───────────────────────────────────────────────────────────────
        {
            "canonical_name": "FNR — Fonds National de la Recherche (Luxembourg)",
            "category": "rd_agency",
            "name_variants": [
                "Fonds National de la Recherche",
                "FNR",
                "fonds national de la recherche luxembourg",
                "national research fund",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "strict_preferred_item_types": True,
            "rd_category": "direct_rd",
            "active_years": (1999, 2099),
            "max_amount_local": 100_000_000,
            "notes": "Primary competitive R&D funder. Amounts in full EUR.",
        },
        # ── Public research institutes ────────────────────────────────────────
        {
            "canonical_name": "LIST / CRP Henri Tudor (Luxembourg)",
            "category": "research_institute",
            "name_variants": [
                "Luxembourg Institute of Science and Technology",
                "LIST",
                "Centre de Recherche Public Henri Tudor",
                "CRP Henri Tudor",
                "CRP Tudor",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "strict_preferred_item_types": True,
            "rd_category": "direct_rd",
            "active_years": (1987, 2099),
            "max_amount_local": 80_000_000,
        },
        {
            "canonical_name": "LISER / CEPS-INSTEAD (Luxembourg)",
            "category": "research_institute",
            "name_variants": [
                "Luxembourg Institute of Socio-Economic Research",
                "LISER",
                "CEPS/INSTEAD",
                "CEPS INSTEAD",
                "Centre d'Etudes de Populations, de Pauvreté et de Politiques Socio-Economiques",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "strict_preferred_item_types": True,
            "rd_category": "direct_rd",
            "active_years": (1990, 2099),
            "max_amount_local": 30_000_000,
        },
        {
            "canonical_name": "LIH / CRP Santé (Luxembourg)",
            "category": "research_institute",
            "name_variants": [
                "Luxembourg Institute of Health",
                "LIH",
                "Centre de Recherche Public de la Santé",
                "CRP Santé",
                "CRP de la Santé",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "strict_preferred_item_types": True,
            "rd_category": "direct_rd",
            "active_years": (1990, 2099),
            "max_amount_local": 60_000_000,
        },
        {
            "canonical_name": "CRP Gabriel Lippmann (Luxembourg)",
            "category": "research_institute",
            "name_variants": [
                "Centre de Recherche Public Gabriel Lippmann",
                "CRP Gabriel Lippmann",
                "CRP Lippmann",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "strict_preferred_item_types": True,
            "rd_category": "direct_rd",
            "active_years": (1987, 2015),
            "max_amount_local": 30_000_000,
        },
        # ── Université du Luxembourg ──────────────────────────────────────────
        {
            "canonical_name": "Université du Luxembourg",
            "category": "higher_education",
            "name_variants": [
                "Université du Luxembourg",
                "University of Luxembourg",
                "Uni.lu",
                "UniLu",
            ],
            "preferred_item_type": ["line_item"],
            "strict_preferred_item_types": True,
            "rd_category": "higher_education",
            "active_years": (2003, 2099),
            "max_amount_local": 300_000_000,
        },
        # ── Research ministry section ─────────────────────────────────────────
        {
            "canonical_name": "Ministère de l'Enseignement Supérieur et de la Recherche (Luxembourg)",
            "category": "rd_ministry",
            "name_variants": [
                "Ministère de l'enseignement supérieur et de la recherche",
                "Département de la culture, de l'enseignement supérieur et de la recherche",
                "enseignement supérieur et recherche",
                "section 03",
                "03 — ministere de l'enseignement superieur",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "rd_category": "direct_rd",
            "active_years": (1975, 2099),
            "section_total": True,
            "max_amount_local": 500_000_000,
        },
    ],
    "Mexico": [
        # ── Ramo 38: CONACYT / CONAHCyT ──────────────────────────────────────
        {
            "canonical_name": "CONACYT / CONAHCyT — Ramo 38 (Mexico)",
            "category": "rd_agency",
            "name_variants": [
                "Consejo Nacional de Ciencia y Tecnología",
                "CONACYT",
                "Consejo Nacional de Humanidades, Ciencias y Tecnologías",
                "CONAHCyT",
                "Ramo 38",
                "ramo 38 conacyt",
                "ramo 38 conahcyt",
                "38 CONACYT",
            ],
            "rd_category": "direct_rd",
            "active_years": (1975, 2099),
            "section_total": True,
            "preferred_match_groups": [[
                r"total budget for conacyt",
                r"total budget for ramo 38",
                r"total for ramo 38",
                r"total expenditure of the national council of science and technology",
                r"total budget for the national council of science and technology",
                r"total budget for national council of science and technology",
                r"total for the national council of science and technology",
            ]],
            "enforce_preferred_match_groups": True,
            "choose_smallest_match": True,
            "min_amount_local": 5_000_000_000,
            "max_amount_local": 40_000_000_000,
            "exclude_match_groups": [[
                r"investigaci[oó]n cient[ií]fica,\s*desarrollo e innovaci[oó]n",
                r"programa de ciencia y tecnolog[ií]a",
                r"science and technology program",
                r"becas de posgrado",
                r"postgraduate scholarships",
                r"sistema nacional de investigadores",
                r"national system of researchers",
                r"fomento regional de las capacidades",
                r"regional promotion of scientific, technological and innovation capacities",
                r"fortalecimiento .* capacidades",
                r"strengthening .* capacities",
                r"apoyos para actividades cient[ií]ficas",
            ]],
            "strict_exclude_match_groups": True,
            "notes": "Primary R&D ramo. Pre-1993 amounts in old pesos (×1000 to convert to new MXN). "
                     "Renamed CONAHCyT from 2022 but Ramo 38 code retained.",
        },
        # ── Research centres supervised by CONACYT ────────────────────────────
        {
            "canonical_name": "Centros Públicos de Investigación CONACYT (Mexico)",
            "category": "research_institute",
            "name_variants": [
                "Centros Públicos de Investigación",
                "centros públicos de investigación",
                "centros de investigación",
                "CICESE", "CIESAS", "CIO", "CIDESI", "CIQA", "CIAD",
                "CENAPRED", "CIATEJ", "CICY", "INFOTEC", "CENIDET", "CIMAV",
                "Apoyos Institucionales (Inversión en Centros Públicos de Investigación)",
            ],
            "rd_category": "direct_rd",
            "active_years": (1985, 2099),
            "exclude_match_groups": [[
                r"apoyos institucionales para actividades cient[ií]ficas",
                r"institutional support for scientific,\s*technological,\s*and innovation activities",
                r"apoyos institucionales$",
                r"institutional support$",
                r"\bpiit\b",
                r"construction of research centers",
                r"construcci[oó]n de centros de investigaci[oó]n",
            ]],
            "strict_exclude_match_groups": True,
            "max_amount_local": 10_000_000_000,
        },
        # ── IPN and CINVESTAV (Ramo 11 SEP) ──────────────────────────────────
        {
            "canonical_name": "IPN — Instituto Politécnico Nacional (Mexico)",
            "category": "higher_education",
            "name_variants": [
                "Instituto Politécnico Nacional",
                "Instituto Politecnico Nacional",
                "IPN",
                "politécnico",
            ],
            "rd_category": "higher_education",
            "active_years": (1975, 2099),
            "exclude_match_groups": [[
                r"centro de investigaci[oó]n y de estudios avanzados",
                r"center for research and advanced studies",
                r"\bcinvestav\b",
                r"investigaci[oó]n y desarrollo en el ipn",
                r"research and development at ipn",
                r"instituto politécnico nacional\s*-\s*investigaci[oó]n",
                r"national polytechnic institute\s*-\s*research",
            ]],
            "strict_exclude_match_groups": True,
            "max_amount_local": 20_000_000_000,
        },
        {
            "canonical_name": "CINVESTAV — Centro de Investigación y Estudios Avanzados (Mexico)",
            "category": "research_institute",
            "name_variants": [
                "Centro de Investigación y de Estudios Avanzados",
                "CINVESTAV",
                "cinvestav del ipn",
            ],
            "rd_category": "direct_rd",
            "active_years": (1975, 2099),
            "max_amount_local": 5_000_000_000,
        },
        {
            "canonical_name": "CICY — Centro de Investigación Científica de Yucatán (Mexico)",
            "category": "research_institute",
            "name_variants": [
                "Centro de Investigación Científica de Yucatán",
                "Scientific Research Center of Yucatán",
                "CICY",
                "Budget for Centro de Investigación Científica de Yucatán",
                "Budget for Scientific Research Center of Yucatán",
            ],
            "rd_category": "direct_rd",
            "active_years": (1975, 2099),
            "max_amount_local": 2_000_000_000,
        },
        {
            "canonical_name": "CIMAV — Centro de Investigación en Materiales Avanzados (Mexico)",
            "category": "research_institute",
            "name_variants": [
                "Centro de Investigación en Materiales Avanzados",
                "Advanced Materials Research Center",
                "CIMAV",
            ],
            "rd_category": "direct_rd",
            "active_years": (1975, 2099),
            "max_amount_local": 2_000_000_000,
        },
        {
            "canonical_name": "CIMAT — Centro de Investigación en Matemáticas (Mexico)",
            "category": "research_institute",
            "name_variants": [
                "Centro de Investigación en Matemáticas",
                "Mathematics Research Center",
                "CIMAT",
            ],
            "rd_category": "direct_rd",
            "active_years": (1975, 2099),
            "max_amount_local": 2_000_000_000,
        },
        # ── UNAM (Ramo 11 SEP) ────────────────────────────────────────────────
        {
            "canonical_name": "UNAM — Universidad Nacional Autónoma de México (Mexico)",
            "category": "higher_education",
            "name_variants": [
                "Universidad Nacional Autónoma de México",
                "Universidad Nacional Autonoma de Mexico",
                "UNAM",
            ],
            "rd_category": "higher_education",
            "active_years": (1975, 2099),
            "exclude_match_groups": [[
                r"museo,\s*investigaci[oó]n y docencia en biolog[ií]a marina",
                r"museum,\s*research,\s*and teaching in marine biology",
                r"investigaci[oó]n y desarrollo en la unam",
                r"research and development at unam",
                r"universidad nacional aut[oó]noma de m[eé]xico\s*-\s*investigaci[oó]n",
                r"national autonomous university of mexico\s*-\s*research",
            ]],
            "strict_exclude_match_groups": True,
            "max_amount_local": 50_000_000_000,
        },
        # ── ININ / nuclear research (Ramo 18 SENER) ──────────────────────────
        {
            "canonical_name": "ININ — Instituto Nacional de Investigaciones Nucleares (Mexico)",
            "category": "research_institute",
            "name_variants": [
                "Instituto Nacional de Investigaciones Nucleares",
                "ININ",
            ],
            "rd_category": "direct_rd",
            "active_years": (1975, 2099),
            "max_amount_local": 3_000_000_000,
        },
        # ── INIFAP / agricultural R&D ─────────────────────────────────────────
        {
            "canonical_name": "INIFAP — Instituto Nacional de Investigaciones Forestales, Agrícolas y Pecuarias (Mexico)",
            "category": "research_institute",
            "name_variants": [
                "Instituto Nacional de Investigaciones Forestales, Agrícolas y Pecuarias",
                "Instituto Nacional de Investigaciones Forestales",
                "INIFAP",
            ],
            "rd_category": "direct_rd",
            "active_years": (1985, 2099),
            "max_amount_local": 3_000_000_000,
        },
        {
            "canonical_name": "Mexican Space Agency",
            "category": "science_agency",
            "name_variants": [
                "Agencia Espacial Mexicana",
                "Mexican Space Agency",
                "AEM",
            ],
            "rd_category": "direct_rd",
            "preferred_item_type": ["line_item", "program_total"],
            "choose_smallest_match": True,
            "active_years": (2010, 2099),
        },
    ],
    "Israel": [
        # ── Core science ministry (from 1992) ─────────────────────────────────
        {
            "canonical_name": "Ministry of Science and Technology (Israel)",
            "category": "rd_ministry",
            "name_variants": [
                "משרד המדע והטכנולוגיה",
                "משרד המדע",
                "ministry of science and technology",
                "ministry of science",
                "science ministry",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "preferred_match_groups": [[
                r"\btotal general\b",
                r"\bbudget of the ministry of science\b",
                r"\btotal budget for the ministry of science and technology\b",
                r"\btotal\b",
                r"\bexpenditure\b",
            ]],
            "enforce_preferred_match_groups": True,
            "expected_years": [
                1992, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002,
                2004, 2005, 2007, 2008, 2009,
                # Biannual budgets (2009-2010, 2011-2012, …, 2017-2018): both years are valid
                2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018,
                2019, 2020, 2021, 2022, 2023, 2024, 2025,
            ],
            "active_years": (1992, 2099),
            "max_amount_local": 20_000_000_000,
            "notes": "Budget code 19. Created 1992. Sub-codes: 02=R&D Council, 03=Research, 05=Infrastructure, 07=Space Agency.",
        },
        # ── Pre-1992 science governance ───────────────────────────────────────
        {
            "canonical_name": "National Council for R&D (Israel, pre-1992)",
            "category": "science_agency",
            "name_variants": [
                "המועצה הלאומית למחקר ולפיתוח",
                "המועצה הלאומית למחקר",
                "national council for research and development",
                "national council for research",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "preferred_match_groups": [[
                r"\btotal\b",
                r"\btotal amount\b",
                r"\btotal expenditures\b",
                r"\bexpenses of the national council\b",
                r"\bnational council for r&d\b",
                r"\bresearch and development\b",
            ]],
            "enforce_preferred_match_groups": True,
            "exclude_match_groups": [[
                r"government participation",
                r"income from external sources",
                r"current expenses",
                r"supported bodies",
                r"ministry of science and development",
                r"earth science research",
                r"kamea",
                r"energy sources development",
                r"medical research",
                r"agricultural research",
                r"technological research",
                r"research grants",
                r"grants for r&d",
            ]],
            "active_years": (1960, 1992),
            "expected_years": [1975, 1976, 1977, 1978, 1979, 1980, 1981, 1982, 1983, 1984, 1985, 1986, 1987, 1988, 1989, 1990, 1991],
            "max_amount_local": 2_100_000_000,
            "notes": "Budget code 74 in 1985-era files. Precursor to Ministry of Science.",
        },
        # ── Innovation / industrial R&D ───────────────────────────────────────
        {
            "canonical_name": "Israel Innovation Authority (from 2016)",
            "category": "innovation_instruments",
            "name_variants": [
                "רשות החדשנות הישראלית",
                "רשות החדשנות",
                "israel innovation authority",
                "innovation authority",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "expected_years": [2017, 2025],
            "active_years": (2016, 2099),
            "max_amount_local": 10_000_000_000,
            "notes": "Replaced the Office of the Chief Scientist at Ministry of Economy in 2016.",
        },
        {
            "canonical_name": "Office of the Chief Scientist (Israel, pre-2016)",
            "category": "innovation_instruments",
            "name_variants": [
                "מדען ראשי",
                "משרד המדען הראשי",
                "office of the chief scientist",
                "chief scientist",
                "ocs",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "expected_years": [1986, 2013],
            "active_years": (1975, 2016),
            "max_amount_local": 10_000_000_000,
            "notes": "Appeared as sub-lines in Ministry of Industry/Economy. Replaced by Innovation Authority 2016.",
        },
        # ── Basic research funding ─────────────────────────────────────────────
        {
            "canonical_name": "Israel Science Foundation (ISF / קרן מדע ישראל)",
            "category": "science_agency",
            "name_variants": [
                "קרן מדע ישראל",
                "israel science foundation",
                "isf",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1975, 2099),
            "notes": "Funds basic research at Israeli universities. Previously linked to the National Academy.",
        },
        {
            "canonical_name": "KAMEA Fund (קרן קמ\"ח)",
            "category": "innovation_instruments",
            "name_variants": [
                "קרן קמ\"ח",
                "קמ\"ח",
                "kamea",
            ],
            "preferred_item_type": ["line_item"],
            "expected_years": [1987, 1991],
            "active_years": (1975, 2099),
            "max_amount_local": 2_000_000_000,
            "notes": "Competitive applied research fund. Appears as sub-line in industry ministry (code 31 05).",
        },
        # ── Space ──────────────────────────────────────────────────────────────
        {
            "canonical_name": "Israeli Space Agency (סוכנות החלל הישראלית)",
            "category": "science_agency",
            "name_variants": [
                "סוכנות החלל הישראלית",
                "israeli space agency",
                "space agency",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "expected_years": [1995, 2004, 2005, 2007, 2010, 2011, 2012, 2014, 2016, 2018, 2019, 2021, 2022],
            "active_years": (1983, 2099),
            "max_amount_local": 5_000_000_000,
        },
        # ── Universities / research institutes ────────────────────────────────
        {
            "canonical_name": "Weizmann Institute of Science",
            "category": "higher_education",
            "name_variants": [
                "מכון ויצמן למדע",
                "מרכז ויצמן",
                "weizmann institute",
                "weizmann institute of science",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1975, 2099),
        },
        {
            "canonical_name": "Technion — Israel Institute of Technology",
            "category": "higher_education",
            "name_variants": [
                "הטכניון",
                "טכניון",
                "technion",
                "israel institute of technology",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1975, 2099),
        },
        {
            "canonical_name": "Hebrew University of Jerusalem",
            "category": "higher_education",
            "name_variants": [
                "האוניברסיטה העברית",
                "אוניברסיטה עברית",
                "hebrew university",
                "hebrew university of jerusalem",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1975, 2099),
        },
        {
            "canonical_name": "Volcani Center (Agricultural Research)",
            "category": "science_agency",
            "name_variants": [
                "מרכז וולקני",
                "volcani center",
                "volcani centre",
                "agricultural research organization",
                "aro",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1975, 2099),
            "notes": "State agricultural research institute under Ministry of Agriculture.",
        },
    ],
    "Korea": [
        {
            "canonical_name": "Ministry of Science and ICT (Korea)",
            "category": "rd_ministry",
            "name_variants": [
                "과학기술정보통신부",
                "ministry of science and ict",
                "science and ict",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (2017, 2099),
            "max_amount_local": 200_000_000_000_000,
        },
        {
            "canonical_name": "National R&D Programmes (Korea)",
            "category": "innovation_instruments",
            "name_variants": [
                "국가연구개발",
                "연구개발",
                "r&d",
                "과학기술",
                "혁신성장",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "active_years": (2018, 2099),
            "max_amount_local": 300_000_000_000_000,
            "notes": "Programmatic canonical for summary-style budget sources; not an institutional series.",
        },
        {
            "canonical_name": "Strategic Technology R&D Programmes (Korea)",
            "category": "innovation_instruments",
            "name_variants": [
                "ai",
                "인공지능",
                "반도체",
                "우주",
                "바이오",
                "양자",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "active_years": (2018, 2099),
            "max_amount_local": 300_000_000_000_000,
            "notes": "Theme-level canonical for strategic R&D programmes in Korean budget briefs.",
        },
    ],
    "Colombia": [
        # ── Science ministry / main R&D funder ───────────────────────────────
        {
            "canonical_name": "COLCIENCIAS (Departamento Administrativo de Ciencia, Tecnología e Innovación)",
            "category": "science_agency",
            "name_variants": [
                "colciencias",
                "departamento administrativo de ciencia, tecnología e innovación",
                "departamento administrativo de ciencia tecnologia e innovacion",
                "0320",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1991, 2018),
            "notes": "Main Colombian science agency. Budget SECCIÓN ~1114. Replaced by MinCiencias in 2019.",
        },
        {
            "canonical_name": "MinCiencias (Ministerio de Ciencia, Tecnología e Innovación)",
            "category": "science_agency",
            "name_variants": [
                "minciencias",
                "ministerio de ciencia, tecnología e innovación",
                "ministerio de ciencia tecnologia e innovacion",
                "ministerio de ciencias",
                "3901",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (2019, 2099),
            "notes": "Replaced COLCIENCIAS from 2019. Created by Ley 1951 de 2019.",
        },
        {
            "canonical_name": "Fondo Francisco José de Caldas",
            "category": "science_agency",
            "name_variants": [
                "fondo francisco josé de caldas",
                "fondo francisco jose de caldas",
                "fondo caldas",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1991, 2099),
            "notes": "Competitive research grants fund managed by COLCIENCIAS/MinCiencias.",
        },
        # ── Applied / industrial R&D ──────────────────────────────────────────
        {
            "canonical_name": "SENA — R&D and Innovation (Servicio Nacional de Aprendizaje)",
            "category": "innovation_instruments",
            "name_variants": [
                "sena",
                "servicio nacional de aprendizaje",
                "3602",
                "fomento de la investigación, desarrollo tecnológico e innovación",
                "fomento de la investigacion, desarrollo tecnologico e innovacion",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "active_years": (1957, 2099),
            "max_amount_local": 1_000_000_000_000_000_000,
            "notes": "SECCIÓN 3602. Include only R&D/innovation investment sub-programmes, not pure vocational training.",
        },
        {
            "canonical_name": "iNNpulsa Colombia",
            "category": "innovation_instruments",
            "name_variants": [
                "innpulsa",
                "innpulsa colombia",
                "unidad de desarrollo e innovación",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (2012, 2099),
        },
        # ── Agricultural research ─────────────────────────────────────────────
        {
            "canonical_name": "AGROSAVIA / CORPOICA (agricultural research)",
            "category": "science_agency",
            "name_variants": [
                "agrosavia",
                "corporación colombiana de investigación agropecuaria",
                "corporacion colombiana de investigacion agropecuaria",
                "corpoica",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1962, 2099),
            "notes": "CORPOICA renamed AGROSAVIA in 2018. Agricultural R&D corporation.",
        },
        {
            "canonical_name": "ICA (Instituto Colombiano Agropecuario)",
            "category": "science_agency",
            "name_variants": [
                "ica",
                "instituto colombiano agropecuario",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1962, 2099),
        },
        # ── Technical / environmental research ───────────────────────────────
        {
            "canonical_name": "IDEAM (Instituto de Hidrología, Meteorología y Estudios Ambientales)",
            "category": "science_agency",
            "name_variants": [
                "ideam",
                "instituto de hidrología, meteorología y estudios ambientales",
                "instituto de hidrologia, meteorologia y estudios ambientales",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1993, 2099),
        },
        {
            "canonical_name": "INM (Instituto Nacional de Metrología)",
            "category": "science_agency",
            "name_variants": [
                "inm",
                "instituto nacional de metrología",
                "instituto nacional de metrologia",
                "3505",
            ],
            "preferred_item_type": ["line_item"],
            "active_years": (2011, 2099),
        },
        {
            "canonical_name": "Instituto Nacional de Salud (Colombia)",
            "category": "science_agency",
            "name_variants": [
                "instituto nacional de salud",
                "ins colombia",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1975, 2099),
        },
    ],
    "Costa Rica": [
        # ── Science ministry and main funders ─────────────────────────────────
        {
            "canonical_name": "MICITT (Ministerio de Ciencia, Innovación, Tecnología y Telecomunicaciones)",
            "category": "rd_ministry",
            "name_variants": [
                "micitt",
                "ministerio de ciencia, innovación, tecnología y telecomunicaciones",
                "ministerio de ciencia, tecnologia y telecomunicaciones",
                "micit",
            ],
            "preferred_item_type": ["section_total", "program_total", "line_item"],
            "active_years": (1990, 2099),
            "min_amount_local": 1_000_000.0,
            "max_amount_local": 20_000_000_000.0,
            "preferred_match_groups": [
                [
                    r"ministerio de ciencia",
                    r"coordinaci[oó]n y des\.\s*cient",
                    r"promoci[oó]n de la investigaci[oó]n",
                    r"direcci[oó]n de investigaci[oó]n y desarrollo tecnol",
                    r"transferencias al sector ciencia",
                    r"impulso de la econom[ií]a a trav[eé]s de la innovaci[oó]n",
                    r"desarrollo de planes,\s*programas y proyectos vinculados",
                ]
            ],
            "enforce_preferred_match_groups": True,
            "exclude_match_groups": [
                [
                    r"jefe unidad de planificaci[oó]n",
                    r"director de certificaciones",
                    r"director de fomento de la ciencia",
                    r"director de innovaci[oó]n",
                    r"asociaci[oó]n solidarista",
                    r"firma digital",
                    r"^ministerio de ciencia",
                    r"^ministry of science",
                    r"total budget for micitt",
                    r"total of the ministry of science",
                    r"instituto costarricense de investigaci[oó]n y ense[nñ]anza en nutrici[oó]n y salud",
                    r"research and development related to economic affairs",
                    r"servicios de desarrollo de sistemas inform",
                    r"equipo sanitario, de laboratorio e investigaci[oó]n",
                    r"tintas, pinturas y diluyentes",
                    r"bienes intangibles",
                    r"comunicaciones$",
                    r"rector[ií]a del sector telecomunicaciones",
                    r"servicios p[úu]blicos generales",
                    r"protecci[oó]n social",
                    r"protecci[oó]n del medio ambiente",
                    r"combustibles y energ[íi]a",
                    r"asuntos econ[oó]micos",
                    r"el transporte",
                    r"orden p[úu]blico",
                    r"la salud",
                ]
            ],
        },
        {
            "canonical_name": "CONICIT (Consejo Nacional para Investigaciones Científicas y Tecnológicas)",
            "category": "science_agency",
            "name_variants": [
                "conicit",
                "consejo nacional para investigaciones científicas y tecnológicas",
                "consejo nacional para investigaciones cientificas y tecnologicas",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1972, 2099),
            "min_amount_local": 10_000_000.0,
            "max_amount_local": 1_500_000_000.0,
            "preferred_match_groups": [[r"\bconicit\b", r"consejo nacional de investigaciones cient", r"consejo nacional para investigaciones cient"]],
            "enforce_preferred_match_groups": True,
            "notes": "Main competitive research funding body in Costa Rica.",
        },
        {
            "canonical_name": "Promotora Costarricense de Innovación e Investigación (PCII)",
            "category": "science_agency",
            "name_variants": [
                "promotora costarricense de innovación e investigación",
                "promotora costarricense de innovacion e investigacion",
                "pcii",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (2015, 2099),
            "min_amount_local": 5_000_000.0,
            "preferred_match_groups": [[r"\bpcii\b", r"promotora costarricense de innovaci[oó]n e investigaci[oó]n"]],
            "enforce_preferred_match_groups": True,
        },
        # ── Universities ──────────────────────────────────────────────────────
        {
            "canonical_name": "UCR (Universidad de Costa Rica)",
            "category": "higher_education",
            "name_variants": [
                "universidad de costa rica",
                "ucr",
                "vínculo externo ucr",
                "vinculo externo ucr",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1940, 2099),
            "min_amount_local": 1_000_000.0,
            "max_amount_local": 1_000_000_000.0,
            "preferred_match_groups": [[r"universidad de costa rica", r"\bucr\b", r"v[ií]nculo externo", r"cita/mag/ucr/micit"]],
            "enforce_preferred_match_groups": True,
            "notes": "Receives bulk FEES transfer; include only explicit research/Vínculo Externo sub-lines.",
        },
        {
            "canonical_name": "ITCR / TEC (Instituto Tecnológico de Costa Rica)",
            "category": "higher_education",
            "name_variants": [
                "instituto tecnológico de costa rica",
                "instituto tecnologico de costa rica",
                "itcr",
                "tec",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1971, 2099),
            "min_amount_local": 1_000_000.0,
            "preferred_match_groups": [[r"instituto tecnol[oó]gico de costa rica", r"\bitcr\b"]],
            "enforce_preferred_match_groups": True,
        },
        {
            "canonical_name": "FEES (Fondo Especial de Educación Superior)",
            "category": "higher_education",
            "name_variants": [
                "fondo especial de educación superior",
                "fondo especial de educacion superior",
                "fees",
            ],
            "preferred_item_type": ["section_total"],
            "active_years": (1975, 2099),
            "notes": "Bulk transfer to state universities. Mark aggregation_role='section'. Individual research lines are separate.",
        },
        # ── Agricultural & environmental research ─────────────────────────────
        {
            "canonical_name": "INTA (Instituto Nacional de Innovación y Transferencia en Tecnología Agropecuaria)",
            "category": "science_agency",
            "name_variants": [
                "inta",
                "instituto nacional de innovación y transferencia en tecnología agropecuaria",
                "instituto nacional de innovacion y transferencia en tecnologia agropecuaria",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (2000, 2099),
            "min_amount_local": 1_000_000.0,
            "max_amount_local": 13_000_000_000.0,
            "preferred_match_groups": [[r"instituto nacional de innovaci[oó]n y transferencia en tecnolog[ií]a agropecuaria", r"\binta\b", r"investigaci[oó]n agropecuaria"]],
            "enforce_preferred_match_groups": True,
            "exclude_match_groups": [[
                r"total budget for inta",
                r"total for inta",
                r"research and development in agricultural technology",
                r"agricultural research projects",
            ]],
        },
        {
            "canonical_name": "INCIENSA (health and nutrition research)",
            "category": "science_agency",
            "name_variants": [
                "inciensa",
                "instituto costarricense de investigación y enseñanza en nutrición y salud",
                "instituto costarricense de investigacion y ensenanza en nutricion y salud",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1975, 2099),
            "min_amount_local": 1_000_000.0,
            "max_amount_local": 8_000_000_000.0,
            "preferred_match_groups": [[r"\binciensa\b", r"instituto costarricense de investigaci[oó]n y ense[nñ]anza en nutrici[oó]n y salud"]],
            "enforce_preferred_match_groups": True,
            "exclude_match_groups": [[
                r"transfer to inciensa",
                r"cooperation agreement between the ministry of health and inciensa",
                r"dm-jg-3090-2019",
                r"super[aá]vit donaci[oó]n",
            ]],
        },
        {
            "canonical_name": "CATIE (Centro Agronómico Tropical de Investigación y Enseñanza)",
            "category": "science_agency",
            "name_variants": [
                "catie",
                "centro agronómico tropical de investigación y enseñanza",
                "centro agronomico tropical de investigacion y ensenanza",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1942, 2099),
            "min_amount_local": 1_000_000.0,
            "preferred_match_groups": [[r"\bcatie\b", r"centro agron[oó]mico tropical de investigaci[oó]n y ense[nñ]anza"]],
            "enforce_preferred_match_groups": True,
        },
    ],
    "Italy": [
        {
            "canonical_name": "Ministero dell'università e della ricerca (MUR/MIUR/MURST)",
            "category": "ministry",
            "notes": "Italy's main R&D ministry: MURST 1989–1999, MIUR 1999–2020, MUR 2020+.",
            "name_variants": [
                "ministero dell'università e della ricerca",
                "ministero dell'universita' e della ricerca",
                "mur",
                "ministero dell'istruzione, dell'università e della ricerca",
                "ministero dell'istruzione, dell'universita' e della ricerca",
                "miur",
                "ministero dell'università e della ricerca scientifica e tecnologica",
                "murst",
                "istruzione, università e ricerca",
                "universita' e ricerca",
                "istruzione, universita' e ricerca",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1989, 2099),
        },
        {
            "canonical_name": "FOE — Fondo Ordinario per gli Enti di ricerca",
            "category": "direct_rd",
            "notes": "Main block grant to MUR-supervised research institutes (CNR, ENEA, ASI, INFN, INAF).",
            "name_variants": [
                "fondo ordinario per gli enti di ricerca",
                "foe",
                "fondo per il finanziamento ordinario degli enti",
                "enti pubblici di ricerca",
                "fondo unico per gli enti di ricerca",
                "capitolo 1678",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1990, 2099),
            "min_amount_local": 100_000_000.0,
        },
        {
            "canonical_name": "FIRST / FAR / FIRB — Fondi per la ricerca",
            "category": "direct_rd",
            "notes": "Key competitive R&D fund (FIRST 2007+; FAR and FIRB pre-2007 predecessors).",
            "name_variants": [
                "fondo per gli investimenti nella ricerca scientifica e tecnologica",
                "first",
                "fondo agevolazioni alla ricerca",
                "far",
                "fondo per la ricerca di base",
                "firb",
                "fondo per gli investimenti della ricerca di base",
                "capitolo 1694",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1995, 2099),
            "min_amount_local": 10_000_000.0,
        },
        {
            "canonical_name": "PRIN — Progetti di Rilevante Interesse Nazionale",
            "category": "direct_rd",
            "notes": "Competitive university research grants of national relevance.",
            "name_variants": [
                "progetti di rilevante interesse nazionale",
                "prin",
                "programmi di ricerca di interesse nazionale",
                "ricerca di interesse nazionale delle università",
            ],
            "preferred_item_type": ["line_item"],
            "active_years": (1999, 2099),
        },
        {
            "canonical_name": "CNR — Consiglio Nazionale delle Ricerche",
            "category": "science_agency",
            "notes": "Italy's main multidisciplinary research council. MUR-supervised.",
            "name_variants": [
                "consiglio nazionale delle ricerche",
                "cnr",
                "c.n.r.",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1923, 2099),
            "min_amount_local": 50_000_000.0,
        },
        {
            "canonical_name": "ENEA",
            "category": "science_agency",
            "notes": "Energy and environment research agency. MUR-supervised.",
            "name_variants": [
                "agenzia nazionale per le nuove tecnologie",
                "enea",
                "ente per le nuove tecnologie, l'energia e l'ambiente",
                "ente per le nuove tecnologie, l'energia e lo sviluppo",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1952, 2099),
            "min_amount_local": 10_000_000.0,
        },
        {
            "canonical_name": "ASI — Agenzia Spaziale Italiana",
            "category": "science_agency",
            "notes": "Italian Space Agency, established 1988.",
            "name_variants": [
                "agenzia spaziale italiana",
                "asi",
                "a.s.i.",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1988, 2099),
            "min_amount_local": 10_000_000.0,
        },
        {
            "canonical_name": "INFN — Istituto Nazionale di Fisica Nucleare",
            "category": "science_agency",
            "notes": "National nuclear physics institute. MUR-supervised.",
            "name_variants": [
                "istituto nazionale di fisica nucleare",
                "infn",
                "i.n.f.n.",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1951, 2099),
            "min_amount_local": 10_000_000.0,
        },
        {
            "canonical_name": "INAF — Istituto Nazionale di Astrofisica",
            "category": "science_agency",
            "notes": "National astrophysics institute, created 2002.",
            "name_variants": [
                "istituto nazionale di astrofisica",
                "inaf",
                "i.n.a.f.",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (2002, 2099),
        },
        {
            "canonical_name": "Missione 17 — Ricerca e innovazione",
            "category": "direct_rd",
            "notes": "Cross-ministry R&D mission code (2010+ budget reform). Appears under MUR, health, environment, culture.",
            "name_variants": [
                "ricerca e innovazione",
                "missione 17",
                "ricerca scientifica e tecnologica di base e applicata",
                "ricerca di base e applicata",
                "ricerca per il settore della sanità pubblica",
                "ricerca per il settore zooprofilattico",
            ],
            "preferred_item_type": ["program_total", "section_total"],
            "active_years": (2010, 2099),
            "min_amount_local": 1_000_000.0,
        },
    ],
    "Slovenia": [
        {
            "canonical_name": "ARRS — Agencija za raziskovalno dejavnost Republike Slovenije",
            "category": "science_agency",
            "notes": "Slovenian Research Agency (2004+). Primary funder of research programmes/projects.",
            "name_variants": [
                "agencija za raziskovalno dejavnost",
                "agencija za raziskovalno dejavnost republike slovenije",
                "arrs",
                "javna agencija za tehnološki razvoj",
                "javna agencija za tehnoloski razvoj",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (2004, 2099),
            "min_amount_local": 1_000_000.0,
        },
        {
            "canonical_name": "Programme 0502 — Znanstveno raziskovalna dejavnost",
            "category": "direct_rd",
            "notes": "Primary R&D programme code in Slovenian budget across all years.",
            "name_variants": [
                "znanstveno raziskovalna dejavnost",
                "0502",
                "05023201",
                "05023330",
                "05023211",
                "raziskovalni programi in projekti",
                "050201",
                "mednarodne aktivnosti na področju znanosti",
                "050202",
                "podpora raziskovalni infrastrukturi",
                "050204",
                "ciljni raziskovalni projekti",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "active_years": (1995, 2099),
        },
        {
            "canonical_name": "SAZU — Slovenska akademija znanosti in umetnosti",
            "category": "science_agency",
            "notes": "Slovenian Academy of Sciences and Arts. Code 3911.",
            "name_variants": [
                "slovenska akademija znanosti in umetnosti",
                "sazu",
                "3911",
                "050203",
                "znanstveno raziskovalna dejavnost slovenske akademije",
            ],
            "preferred_item_type": ["program_total", "line_item"],
            "active_years": (1938, 2099),
            "min_amount_local": 500_000.0,
        },
        {
            "canonical_name": "Ministrstvo za visoko šolstvo, znanost in tehnologijo (MVZT)",
            "category": "ministry",
            "notes": "Ministry for Higher Education, Science and Technology, code 3211, 2004–2012.",
            "name_variants": [
                "ministrstvo za visoko šolstvo, znanost in tehnologijo",
                "ministrstvo za visoko solstvo, znanost in tehnologijo",
                "mvzt",
                "3211",
            ],
            "preferred_item_type": ["section_total"],
            "active_years": (2004, 2012),
        },
        {
            "canonical_name": "Ministrstvo za izobraževanje, znanost in šport (MIZŠ)",
            "category": "ministry",
            "notes": "Ministry of Education, Science and Sport, code 3330, 2012+. Main R&D ministry.",
            "name_variants": [
                "ministrstvo za izobraževanje, znanost in šport",
                "ministrstvo za izobrazevanje, znanost in sport",
                "mizš",
                "mizs",
                "3330",
            ],
            "preferred_item_type": ["section_total"],
            "active_years": (2012, 2099),
        },
        {
            "canonical_name": "Programme 0503 — Mladi raziskovalci / Človeški viri v podporo znanosti",
            "category": "direct_rd",
            "notes": "Young researchers scheme and researcher mobility — direct R&D human capital.",
            "name_variants": [
                "mladi raziskovalci",
                "človeški viri v podporo znanosti",
                "cloveški viri v podporo znanosti",
                "0503",
                "050302",
                "spodbude najboljšim raziskovalcem",
                "mobilnost in spodbude",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1995, 2099),
        },
        {
            "canonical_name": "European Space Agency Programs",
            "category": "direct_rd",
            "notes": "Slovenian budget appropriations for ESA programmes. Kept as a separate direct R&D series rather than folded into 0502.",
            "name_variants": [
                "programi evropske vesoljske agencije",
                "european space agency programs",
                "2130-17-0002",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (2023, 2025),
            "min_amount_local": 1_000_000.0,
        },
        {
            "canonical_name": "Development of Research and Innovation Capacities",
            "category": "innovation_instruments",
            "notes": "Research and innovation capacities block under 0504/050401 and ministry 1630. Audited as additive to the core 0502 science programme.",
            "name_variants": [
                "razvoj raziskovalne in inovacijske zmogljivosti",
                "development of research and innovation capacities",
                "1630-24-s001",
                "1630-24-0001",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (2024, 2025),
            "min_amount_local": 1_000_000.0,
        },
    ],

    # -----------------------------------------------------------------------
    # SLOVAKIA
    # Unit: tis. Sk (thousands SKK) 1992-2008; full EUR 2009+.
    # Key kapitoly: 20 (MŠ SR / science ministry), 51 (SAV).
    # -----------------------------------------------------------------------
    "Slovakia": [

        # --- Core R&D funding agencies ---
        {
            "canonical_name": "APVV (Agentúra na podporu výskumu a vývoja)",
            "category": "science_agency",
            "name_variants": [
                "apvv",
                "agentúra na podporu výskumu a vývoja",
                "agentúra pre vedecký výskum",
                "agency for the support of research and development",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (2005, 2099),
            # APVV ~90-120M EUR/year in 2020s; in SKK era, much smaller
            "max_amount_local": 500_000_000,   # EUR (full); in SKK era (thousands) ~3-5B
            "notes": "Created 2005, replacing Agentúra pre vedecký výskum. "
                     "Main competitive R&D grant agency under Ministerstvo školstva SR.",
        },
        {
            "canonical_name": "VEGA (Vedecká grantová agentúra MŠ SR a SAV)",
            "category": "science_agency",
            "name_variants": [
                "vega",
                "vedecká grantová agentúra",
                "vedecka grantova agentura",
                "scientific grant agency",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1993, 2099),
            "max_amount_local": 200_000_000,
            "notes": "Grant scheme for basic research at universities and SAV. "
                     "Jointly managed by MŠ SR and SAV.",
        },
        {
            "canonical_name": "SAV (Slovenská akadémia vied)",
            "category": "science_agency",
            "name_variants": [
                "sav",
                "slovenská akadémia vied",
                "slovenska akademia vied",
                "slovak academy of sciences",
                "kapitola 51",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1992, 2099),
            # SAV ~130-140M EUR/year in 2020s; plausible up to 300M EUR
            "max_amount_local": 300_000_000,
            "strict_preferred_item_types": True,
            "notes": "Slovak Academy of Sciences — own budget chapter (kapitola 51). "
                     "Entire kapitola 51 appropriation counts as R&D.",
        },
        {
            "canonical_name": "Ministerstvo školstva SR (kapitola 20)",
            "category": "higher_education",
            "name_variants": [
                "ministerstvo školstva sr",
                "ministerstvo školstva",
                "ministerstvo školstva, vedy, výskumu a športu",
                "ministerstvo školstva, výskumu, vývoja a mládeže",
                "mš sr",
                "mšvvš sr",
                "mšvvm sr",
                "ministry of education sr",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1992, 2099),
            "max_amount_local": 8_000_000_000,
            "strict_preferred_item_types": True,
            "notes": "Primary budget chapter for science and higher education. "
                     "Contains universities, APVV, VEGA, and research institutes. "
                     "Use section_total for total chapter; prefer named sub-items for detail.",
        },

        # --- Pre-APVV era ---
        {
            "canonical_name": "Agentúra pre vedecký výskum (pre-APVV)",
            "category": "science_agency",
            "name_variants": [
                "agentúra pre vedecký výskum",
                "agentúra pre vedecký výskum a vývoj",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1995, 2005),
            "max_amount_local": 3_000_000,   # thousands SKK
            "notes": "Predecessor to APVV. Replaced by APVV in 2005.",
        },

        # --- International R&D cooperation ---
        {
            "canonical_name": "CERN (Slovak contribution)",
            "category": "direct_rd",
            "name_variants": [
                "príspevok do cern",
                "prispevok do cern",
                "cern",
                "príspevok sr do cern",
            ],
            "preferred_item_type": ["line_item"],
            "active_years": (1993, 2099),
            "max_amount_local": 20_000_000,
            "notes": "Slovak annual contribution to CERN. Appears under MŠ SR or as "
                     "named line in international cooperation budget.",
        },

        # --- Budget division code for R&D ---
        {
            "canonical_name": "Oblasť 740 — veda a výskum (science and research)",
            "category": "direct_rd",
            "name_variants": [
                "740",
                "veda a výskum",
                "veda a vyskum",
                "science and research",
                "oblasť 740",
                "oblast 740",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1992, 2099),
            "max_amount_local": 5_000_000_000,
            "notes": "Budget sector code 740 = science and research. "
                     "All appropriations under this code are direct R&D.",
        },
    ],

    # -----------------------------------------------------------------------
    # POLAND
    "Portugal": [
        # ── FCT / JNICT ───────────────────────────────────────────────────────
        {
            "canonical_name": "FCT — Fundação para a Ciência e a Tecnologia (Portugal)",
            "category": "rd_agency",
            "name_variants": [
                "Fundação para a Ciência e a Tecnologia",
                "Fundação para a Ciência e Tecnologia",
                "FCT",
                "FCT, I.P.",
                "FCT I.P.",
                "fct ip",
                "fundacao para a ciencia e tecnologia",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "rd_category": "direct_rd",
            "active_years": (1997, 2099),
            "max_amount_local": 600_000_000,
            "notes": "Primary R&D funder from 1997. Amounts in full EUR (2002+) or PTE (pre-2002).",
        },
        {
            "canonical_name": "JNICT — Junta Nacional de Investigação Científica e Tecnológica (Portugal)",
            "category": "rd_agency",
            "name_variants": [
                "Junta Nacional de Investigação Científica e Tecnológica",
                "JNICT",
                "junta nacional de investigacao",
            ],
            "preferred_item_type": ["line_item"],
            "rd_category": "direct_rd",
            "active_years": (1977, 1997),
            "max_amount_local": 20_000_000_000,
            "notes": "Pre-FCT competitive R&D funder. Amounts in PTE (escudos).",
        },
        # ── Science ministry ──────────────────────────────────────────────────
        {
            "canonical_name": "Ministério da Ciência e Tecnologia — Capítulo 50 (Portugal)",
            "category": "rd_ministry",
            "name_variants": [
                "Ministério da Ciência e Tecnologia",
                "Ministério da Ciência, Inovação e Ensino Superior",
                "Ministério da Ciência, Tecnologia e Ensino Superior",
                "MCTES",
                "MCES",
                "capítulo 50",
                "capitulo 50",
                "Cap. 50",
            ],
            "rd_category": "direct_rd",
            "section_total": True,
            "active_years": (1977, 2099),
            "max_amount_local": 2_000_000_000,
        },
        # ── ANI ───────────────────────────────────────────────────────────────
        {
            "canonical_name": "ANI — Agência Nacional de Inovação (Portugal)",
            "category": "rd_agency",
            "name_variants": [
                "Agência Nacional de Inovação",
                "ANI",
                "ani ip",
                "agencia nacional de inovacao",
            ],
            "preferred_item_type": ["line_item"],
            "rd_category": "direct_rd",
            "active_years": (2009, 2099),
            "max_amount_local": 250_000_000,
        },
        # ── State laboratories ────────────────────────────────────────────────
        {
            "canonical_name": "LNEC — Laboratório Nacional de Engenharia Civil (Portugal)",
            "category": "research_institute",
            "name_variants": [
                "Laboratório Nacional de Engenharia Civil",
                "LNEC",
            ],
            "preferred_item_type": ["line_item"],
            "rd_category": "direct_rd",
            "active_years": (1977, 2099),
            "max_amount_local": 100_000_000,
        },
        {
            "canonical_name": "P002 — Investigação Científica e Tecnológica e Inovação (Portugal)",
            "category": "rd_programme",
            "name_variants": [
                "P002",
                "P-002",
                "Investigação Científica e Tecnológica e Inovação",
                "INVESTIGAÇÃO CIENTÍFICA E TECNOLÓGICA E INOVAÇÃO",
                "programa investigação científica",
            ],
            "rd_category": "direct_rd",
            "section_total": True,
            "active_years": (2005, 2099),
            "max_amount_local": 2_000_000_000,
        },
    ],
    # Unit: tys. zł (thousands PLN) throughout.
    # 1995 redenomination: 1 new PLN = 10,000 old PLN.
    # Key budget parts: Część 28 (HE & science), Część 67 (PAN).
    # -----------------------------------------------------------------------
    "Poland": [

        # --- Core R&D funding agencies (post-2007/2011) ---
        {
            "canonical_name": "NCN (Narodowe Centrum Nauki)",
            "category": "science_agency",
            "name_variants": [
                "ncn",
                "narodowe centrum nauki",
                "national science centre",
                "national science center",
            ],
            "exclude_match_groups": [[
                r"grants?\s+for\s+basic\s+research",
                r"badania\s+podstawowe",
                r"środki\s+(?:przekazane|przyznane)\s+innym\s+podmiotom",
            ]],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (2011, 2099),
            # NCN ~2-3B tys. zł/year in 2020s
            "max_amount_local": 10_000_000,   # thousands PLN
            "notes": "Created 2011 as the main basic research grant agency in Poland. "
                     "Appears in Część 28, Dział 740.",
        },
        {
            "canonical_name": "NCBiR (Narodowe Centrum Badań i Rozwoju)",
            "category": "science_agency",
            "name_variants": [
                "ncbir",
                "ncbr",
                "narodowe centrum badań i rozwoju",
                "national centre for research and development",
                "center for research and development",
            ],
            "exclude_match_groups": [[
                r"grants?\s+for\s+applied\s+research",
                r"badania\s+stosowane",
                r"środki\s+(?:przekazane|przyznane)\s+innym\s+podmiotom",
                r"financing\s+projects?\s+with\s+eu\s+funds",
            ]],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (2007, 2099),
            "max_amount_local": 10_000_000,
            "notes": "Applied R&D and innovation agency created 2007. "
                     "Manages EU-co-financed and national R&D programmes.",
        },
        {
            "canonical_name": "PAN (Polska Akademia Nauk)",
            "category": "science_agency",
            "name_variants": [
                "pan",
                "polska akademia nauk",
                "polish academy of sciences",
                "część 67",
                "czesc 67",
            ],
            "preferred_item_type": ["program_total", "section_total"],
            "active_years": (1990, 2099),
            "max_amount_local": 2_000_000,
            "notes": "Polish Academy of Sciences — own budget part (Część 67). "
                     "Full appropriation is R&D. Also appears as sub-items in Część 28.",
        },

        # --- Pre-2007 R&D agencies ---
        {
            "canonical_name": "KBN (Komitet Badań Naukowych)",
            "category": "science_agency",
            "name_variants": [
                "kbn",
                "komitet badań naukowych",
                "committee for scientific research",
                "państwowy komitet badań naukowych",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1991, 2005),
            "max_amount_local": 5_000_000,
            "notes": "State Committee for Scientific Research, 1991-2005. "
                     "Key R&D body before creation of MNiSW and NCBiR.",
        },

        # --- Ministry (as appropriating body) ---
        {
            "canonical_name": "MNiSW / MEiN (Ministry of Science and Higher Education)",
            "category": "higher_education",
            "name_variants": [
                "mnisw",
                "ministerstwo nauki i szkolnictwa wyższego",
                "ministerstwo nauki",
                "mein",
                "ministerstwo edukacji i nauki",
                "ministry of science and higher education",
                "ministry of education and science",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (2005, 2099),
            "max_amount_local": 100_000_000,
            "notes": "Created 2005. Renamed MEiN (2021-2024) then split back. "
                     "Administers Część 28 (Szkolnictwo wyższe i nauka).",
        },

        # --- Budget part / division codes ---
        {
            "canonical_name": "Część 28 — Szkolnictwo wyższe i nauka",
            "category": "higher_education",
            "name_variants": [
                "część 28",
                "czesc 28",
                "szkolnictwo wyższe i nauka",
                "szkolnictwo wyzsze i nauka",
                "higher education and science",
            ],
            "preferred_item_type": ["section_total"],
            "active_years": (1991, 2099),
            "max_amount_local": 500_000_000,
            "notes": "Primary budget part for R&D and HE in Poland. "
                     "Contains universities (Dział 730) and direct R&D (Dział 740).",
        },
        {
            "canonical_name": "Dział 740 — Działalność badawcza i rozwojowa",
            "category": "direct_rd",
            "name_variants": [
                "740",
                "działalność badawcza i rozwojowa",
                "prace badawczo-rozwojowe",
                "prace badawcze",
                "badania naukowe",
                "dział 740",
                "dzial 740",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1990, 2099),
            "max_amount_local": 20_000_000,
            "notes": "Budget division code 740 = R&D activity. "
                     "All lines under this division are direct R&D.",
        },

        # --- International R&D cooperation ---
        {
            "canonical_name": "CERN (Polish contribution)",
            "category": "direct_rd",
            "name_variants": [
                "cern",
                "składka do cern",
                "skladka do cern",
                "wkład do cern",
                "udział w cern",
            ],
            "preferred_item_type": ["line_item"],
            "active_years": (1991, 2099),
            "max_amount_local": 200_000,
            "notes": "Polish annual contribution to CERN. Appears under MNiSW or "
                     "international cooperation budget lines.",
        },

        # --- Universities (key ones, as budget series) ---
        {
            "canonical_name": "Subwencje dla uczelni (university block grants, Część 28)",
            "category": "higher_education",
            "name_variants": [
                "subwencje dla uczelni",
                "subwencja",
                "dotacje dla uczelni",
                "finansowanie uczelni",
                "73001",
                "rozdział 73001",
                "szkolnictwo wyższe",
                "dział 730",
            ],
            "preferred_item_type": ["program_total", "section_total"],
            "active_years": (1990, 2099),
            "max_amount_local": 400_000_000,
            "notes": "University block grants under Część 28, Dział 730. "
                     "Cover teaching and research — tag as higher_education.",
        },
    ],
    # Currency: TRL (hyperinflation, amounts in trillions) until 2004;
    # YTL 2005-2008 (1 YTL = 1,000,000 TRL); TL 2009+ (same as YTL).
    # TÜBİTAK, TÜBA, TAEK are ÖZEL BÜTÇELİ — (II) SAYILI CETVEL only.
    # Ministry of Industry and Technology is GENEL BÜTÇELİ — (I) SAYILI CETVEL.
    # -----------------------------------------------------------------------
    "Turkey": [

        # ── TÜBİTAK ──────────────────────────────────────────────────────────
        {
            "canonical_name": "TÜBİTAK (Turkey)",
            "category": "science_agency",
            "name_variants": [
                "tübitak",
                "tubitak",
                "türkiye bilimsel ve teknolojik araştırma kurumu",
                "turkiye bilimsel ve teknolojik arastirma kurumu",
                "scientific and technological research council of turkey",
                "tübitak genel sekreterliği",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1963, 2099),
            "expected_years": list(range(1975, 2010)),
            # Post-2005 (YTL/TL): TÜBİTAK budget ~3-8B TL in 2020s
            # Pre-2005 (TRL): amounts in quadrillions of old lira
            "max_amount_local": 50_000_000_000,
            "notes": "Özel Bütçeli (special budget) entity — appears in (II) SAYILI CETVEL. "
                     "NOT present in files containing only (I) SAYILI CETVEL. "
                     "Primary competitive R&D funder in Turkey.",
        },

        # ── TÜBA ─────────────────────────────────────────────────────────────
        {
            "canonical_name": "TÜBA (Turkey)",
            "category": "science_agency",
            "name_variants": [
                "tüba",
                "tuba",
                "türkiye bilimler akademisi",
                "turkiye bilimler akademisi",
                "turkish academy of sciences",
            ],
            "preferred_item_type": ["line_item"],
            "active_years": (1993, 2099),
            "expected_years": list(range(1993, 2010)),
            "max_amount_local": 500_000_000,
            "notes": "Özel Bütçeli — appears in (II) SAYILI CETVEL.",
        },

        # ── TAEK ─────────────────────────────────────────────────────────────
        {
            "canonical_name": "TAEK — Türkiye Atom Enerjisi Kurumu (Turkey)",
            "category": "science_agency",
            "name_variants": [
                "taek",
                "türkiye atom enerjisi kurumu",
                "turkiye atom enerjisi kurumu",
                "turkish atomic energy authority",
                "atom enerjisi kurumu",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1956, 2099),
            "expected_years": list(range(1975, 2010)),
            "max_amount_local": 5_000_000_000,
            "notes": "Nuclear R&D authority. Özel Bütçeli — appears in (II) SAYILI CETVEL.",
        },

        # ── Ministry of Industry and Technology ──────────────────────────────
        {
            "canonical_name": "Sanayi ve Teknoloji Bakanlığı (Turkey)",
            "category": "science_agency",
            "name_variants": [
                "sanayi ve teknoloji bakanlığı",
                "sanayi ve teknoloji bakanligi",
                "bilim sanayi ve teknoloji bakanlığı",
                "bilim sanayi ve teknoloji bakanligi",
                "sanayi ve ticaret bakanlığı",
                "ministry of industry and technology",
                "ministry of science industry and technology",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "section_total": True,
            "active_years": (1975, 2099),
            "max_amount_local": 100_000_000_000,
            "notes": "Genel Bütçeli — present in (I) SAYILI CETVEL. "
                     "R&D allocation is a subset of total ministry budget. "
                     "Name changed: Sanayi ve Ticaret (pre-2011), Bilim Sanayi ve Teknoloji (2011-2018), "
                     "Sanayi ve Teknoloji (2018+).",
        },

        # ── KOSGEB ───────────────────────────────────────────────────────────
        {
            "canonical_name": "KOSGEB (Turkey)",
            "category": "innovation_instruments",
            "name_variants": [
                "kosgeb",
                "küçük ve orta ölçekli işletmeleri geliştirme",
                "kucuk ve orta olcekli isletmeleri gelistirme",
                "sme development organization",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1990, 2099),
            "expected_years": list(range(1990, 2010)),
            "max_amount_local": 10_000_000_000,
            "notes": "Özel Bütçeli SME support agency with R&D/innovation mandate.",
        },

        # ── Turkish Space Agency ──────────────────────────────────────────────
        {
            "canonical_name": "Türkiye Uzay Ajansı / TUA (Turkey)",
            "category": "science_agency",
            "name_variants": [
                "türkiye uzay ajansı",
                "turkiye uzay ajansi",
                "tua",
                "turkish space agency",
            ],
            "preferred_item_type": ["line_item"],
            "active_years": (2018, 2099),
            "max_amount_local": 5_000_000_000,
            "notes": "Established 2018. Özel Bütçeli.",
        },
    ],
}


# ---------------------------------------------------------------------------
# Matching and series building
# ---------------------------------------------------------------------------

def _match_agency(desc: str, section: str, agency: dict, desc_raw: str = "", section_raw: str = "") -> bool:
    """Return True if the row matches any of the agency's name variants.

    Checks line_description_en, section_name_en, and the raw entity text
    (desc_raw) so that even if line_description_en is corrupted by a bad
    registry match, the raw entity name still triggers a match.

    Short variants (≤4 chars, e.g. 'ORE', 'ARC', 'AIMS') use word-boundary
    matching to prevent false matches like 'ORE' inside 'fOREign'.
    Longer variants use plain substring matching.
    """
    combined = _normalise_match_text(f"{desc} {section} {desc_raw} {section_raw}")
    for variant in agency["name_variants"]:
        v = _normalise_match_text(variant)
        if not v:
            continue
        if len(v) <= 4:
            if re.search(r"(?<![a-z])" + re.escape(v) + r"(?![a-z])", combined):
                return True
        else:
            if v in combined:
                return True
    return False


def _section_match_score(desc: str, section: str, agency: dict, desc_raw: str = "", section_raw: str = "") -> int:
    """
    Return a score indicating how directly this row belongs to the agency:
      2 = agency name appears in the section_name (it's the agency's own section)
      1 = agency name appears only in the line description
      0 = no match (should not happen if _match_agency returned True)

    Used to prefer the agency's own appropriation line over cross-references
    from other departments (e.g. furniture purchases coded under dept X for CSIRO).
    """
    sec_lower = _normalise_match_text(f"{section} {section_raw}")
    desc_lower = _normalise_match_text(f"{desc} {desc_raw}")
    for variant in agency["name_variants"]:
        v = _normalise_match_text(variant)
        if not v:
            continue
        if v in sec_lower:
            return 2
    for variant in agency["name_variants"]:
        v = _normalise_match_text(variant)
        if not v:
            continue
        if v in desc_lower:
            return 1
    return 0


def _best_amount_for_agency(
    matches: pd.DataFrame,
    preferred_types: list[str],
    agency: dict,
) -> Optional[pd.Series]:
    """
    From a set of matching rows for one agency in one year, pick the single
    best row to represent that agency's total for the year.

    Priority (in order):
      1. Prefer rows where the SECTION name contains the agency name variant
         (score=2) over rows where only the line description matches (score=1).
         This avoids picking cross-departmental references (e.g. CSIRO's
         furniture allocation under Dept of Science).
      2. Within same section-match score, prefer the highest-priority item_type.
      3. Within same type, pick the largest amount — but only if it is at least
         20% of the global maximum (avoids picking tiny sub-components).
      4. Fallback: return the globally largest match.
    """
    if matches.empty:
        return None

    matches = matches.copy()

    if agency.get("_country") == "Australia":
        desc_au = matches.get("line_description_en", pd.Series("", index=matches.index)).fillna("").astype(str)
        sec_au = matches.get("section_name_en", pd.Series("", index=matches.index)).fillna("").astype(str)
        src_au = matches.get("source_file", pd.Series("", index=matches.index)).fillna("").astype(str)
        year_au = pd.to_numeric(matches.get("year", pd.Series(dtype=float)), errors="coerce")

        # OCR/page-grid artifacts in older Australian Appropriation Acts can
        # survive as numeric-only sibling rows ("360", "128 474 313", etc.)
        # inside the agency's own section. If a descriptive agency row also
        # exists, drop the numeric-only artifacts before ranking.
        numeric_only_mask = (
            desc_au.str.strip().str.match(r"^[0-9][0-9\s,.\-]*$", na=False)
            & sec_au.str.strip().str.match(r"^[0-9][0-9\s,.\-]*$", na=False)
        )
        if numeric_only_mask.any() and (~numeric_only_mask).any():
            trimmed = matches.loc[~numeric_only_mask].copy()
            if not trimmed.empty:
                matches = trimmed
                desc_au = matches.get("line_description_en", pd.Series("", index=matches.index)).fillna("").astype(str)
                sec_au = matches.get("section_name_en", pd.Series("", index=matches.index)).fillna("").astype(str)
                src_au = matches.get("source_file", pd.Series("", index=matches.index)).fillna("").astype(str)
                year_au = pd.to_numeric(matches.get("year", pd.Series(dtype=float)), errors="coerce")

        # Department / portfolio totals are not defensible agency observations.
        # Keep only rows where the agency itself is named, not the broader
        # ministry total that happens to live on the same page.
        generic_total_mask = desc_au.str.match(
            r"^\s*total:\s*(department|portfolio|education|industry|foreign affairs|environment|climate change)\b",
            case=False,
            na=False,
        )
        if generic_total_mask.any():
            trimmed = matches.loc[~generic_total_mask].copy()
            if not trimmed.empty:
                matches = trimmed
                desc_au = matches.get("line_description_en", pd.Series("", index=matches.index)).fillna("").astype(str)
                sec_au = matches.get("section_name_en", pd.Series("", index=matches.index)).fillna("").astype(str)
                src_au = matches.get("source_file", pd.Series("", index=matches.index)).fillna("").astype(str)
                year_au = pd.to_numeric(matches.get("year", pd.Series(dtype=float)), errors="coerce")

        # For early Australian Appropriation Acts, the defendable agency row is
        # often the direct line item, while the sibling section_total is the full
        # department total. When an explicit line/program row exists, prefer it.
        early_explicit_mask = (
            year_au.lt(1990)
            & matches.get("item_type", pd.Series("", index=matches.index)).fillna("").isin(["line_item", "program_total"])
            & (
                desc_au.str.contains(
                    r"commonwealth scientific and industrial|atomic energy act|australian atomic energy|australian institute of marine science|bureau of mineral resources|geoscience australia|bureau of meteorology",
                    case=False,
                    regex=True,
                    na=False,
                )
                | sec_au.str.contains(
                    r"commonwealth scientific and industrial|atomic energy act|australian atomic energy|australian institute of marine science|bureau of mineral resources|geoscience australia|bureau of meteorology",
                    case=False,
                    regex=True,
                    na=False,
                )
            )
        )
        if early_explicit_mask.any():
            trimmed = matches.loc[early_explicit_mask].copy()
            if not trimmed.empty:
                matches = trimmed
                desc_au = matches.get("line_description_en", pd.Series("", index=matches.index)).fillna("").astype(str)
                src_au = matches.get("source_file", pd.Series("", index=matches.index)).fillna("").astype(str)
                year_au = pd.to_numeric(matches.get("year", pd.Series(dtype=float)), errors="coerce")

        # Modern Australian annual appropriation entity totals live in No1. When
        # both No1 and supplementary acts exist, keep the main annual bill.
        modern_no1_mask = year_au.ge(2014) & src_au.str.contains(r"\bNo1\b", case=False, regex=True, na=False)
        if modern_no1_mask.any():
            trimmed = matches.loc[modern_no1_mask].copy()
            if not trimmed.empty:
                matches = trimmed

    if agency.get("_country") == "Japan":
        revenue_mask = matches.apply(_japan_row_is_revenue_like, axis=1)
        if revenue_mask.any():
            trimmed = matches.loc[~revenue_mask].copy()
            if not trimmed.empty:
                matches = trimmed
            elif agency.get("canonical_name", "").startswith("MEXT "):
                return None

        mismatch_mask = matches.apply(_japan_cross_agency_mismatch, axis=1)
        if mismatch_mask.any():
            trimmed = matches.loc[~mismatch_mask].copy()
            if not trimmed.empty:
                matches = trimmed

        if matches.empty:
            return None

    exclude_groups = agency.get("exclude_match_groups") or []
    if exclude_groups:
        combined_text = (
            matches.get("line_description_en", pd.Series("", index=matches.index)).fillna("").astype(str)
            + " "
            + matches.get("line_description", pd.Series("", index=matches.index)).fillna("").astype(str)
            + " "
            + matches.get("section_name_en", pd.Series("", index=matches.index)).fillna("").astype(str)
            + " "
            + matches.get("section_name", pd.Series("", index=matches.index)).fillna("").astype(str)
        )
        # Accent-fold before matching: source text (esp. French/Spanish/Portuguese
        # line items) often carries diacritics (é, è, ç, ã, ñ...) that literal
        # regex terms in exclude_match_groups don't always account for. Folding
        # both sides to ASCII avoids false negatives from accent mismatches.
        combined_text = combined_text.map(_strip_accents)
        for group in exclude_groups:
            folded_group = [_strip_accents(pattern) for pattern in group]
            exclude_mask = combined_text.apply(
                lambda text: any(re.search(pattern, text, re.IGNORECASE) for pattern in folded_group)
            )
            trimmed = matches[~exclude_mask].copy()
            if not trimmed.empty:
                matches = trimmed
                break
            if agency.get("strict_exclude_match_groups", False):
                return None

    # Canada-specific safety net: when multiple rows for the same agency survive
    # within one source file, tiny residual fragments (e.g. 20) and huge OCR/
    # column-bleed values can coexist with the plausible appropriation row.
    # Trim only extreme sibling outliers before ranking.
    if len(matches) > 1 and agency.get("_country") == "Canada":
        amounts = pd.to_numeric(matches["amount_local"], errors="coerce").dropna().tolist()
        if len(amounts) >= 2:
            amounts_sorted = sorted(float(a) for a in amounts)
            min_amt = amounts_sorted[0]
            second_largest = amounts_sorted[-2]
            max_amt = amounts_sorted[-1]
            keep_mask = pd.Series(True, index=matches.index)

            if max_amt > 0 and min_amt <= max_amt * 0.001:
                keep_mask &= matches["amount_local"] != min_amt

            if second_largest > 0 and max_amt >= max(second_largest * 10.0, 1_000_000.0):
                keep_mask &= matches["amount_local"] != max_amt

            trimmed = matches[keep_mask].copy()
            if not trimmed.empty:
                matches = trimmed

    # France-specific cleanup: JORF rows can contain both a sane payment-credit
    # line and a massively inflated duplicate from OCR/column concatenation on
    # the same programme page. Trim only the clearly dominant upper sibling.
    if len(matches) > 1 and agency.get("_country") == "France":
        amounts = pd.to_numeric(matches["amount_local"], errors="coerce").dropna().tolist()
        if len(amounts) >= 2:
            amounts_sorted = sorted(float(a) for a in amounts)
            second_largest = amounts_sorted[-2]
            max_amt = amounts_sorted[-1]
            if second_largest > 0 and max_amt >= max(second_largest * 10_000.0, 1_000_000_000.0):
                trimmed = matches[matches["amount_local"] != max_amt].copy()
                if not trimmed.empty:
                    matches = trimmed

    # Japan hardcoded-agency sanity check: agency operating-grant series in the
    # extracted budget tables are typically in the tens or hundreds of millions
    # of "thousand yen". Values in the tens of billions of "thousand yen"
    # indicate that the extractor latched onto an index/end-matter artifact or a
    # broad budget bucket instead of the agency line.
    if (
        agency.get("_country") == "Japan"
        and agency.get("canonical_name") != "MEXT (Ministry of Education, Culture, Sports, Science and Technology)"
    ):
        matches = matches[pd.to_numeric(matches["amount_local"], errors="coerce") < 10_000_000_000].copy()
        if matches.empty:
            return None
    if agency.get("canonical_name") == "MEXT (Ministry of Education, Culture, Sports, Science and Technology)":
        matches = matches[pd.to_numeric(matches["amount_local"], errors="coerce") < 500_000_000].copy()
        if matches.empty:
            return None

    # Norway-specific quality controls:
    # (a) For all individual agencies (not ministry-total canonicals), exclude rows
    #     whose line_description starts with "Total" — these are section/ministry
    #     aggregates that share a section_name with the agency but are not the
    #     agency's own appropriation.
    # (b) Apply per-agency plausibility caps (max_amount_local) checked against the
    #     EXPANDED amount (post unit-scaling) to block inflated rows.
    # (c) Year-sensitive unit pre-normalisation: pre-1993 scanned documents use
    #     full NOK amounts but the LLM often labels them unit='thousand'. Amounts
    #     ≥ 1M with unit='thousand' in years < 1993 are treated as full NOK.
    if agency.get("_country") == "Norway":
        _no_aggregate_canonicals = {
            "Kunnskapsdepartementet (total)",
            "Universitetene (collective)",
        }
        if agency.get("canonical_name") not in _no_aggregate_canonicals:
            desc_col = (
                matches.get("line_description_en", pd.Series("", index=matches.index))
                .fillna("")
                .astype(str)
            )
            total_desc_mask = desc_col.str.match(r"^\s*total\b", case=False, na=False)
            if total_desc_mask.any():
                filtered = matches[~total_desc_mask].copy()
                if not filtered.empty:
                    matches = filtered

        # Pre-1993 year-sensitive unit normalisation (applied inside the cap logic
        # so we work on the correct expanded values without mutating the shared df).
        def _norway_expanded_amt(row: pd.Series) -> float:
            raw = float(row.get("amount_local") or 0)
            unit_r = str(row.get("unit") or "").strip().lower()
            yr_r = int(str(row.get("year") or 9999).split(".")[0])
            # For pre-1993 scanned docs: raw ≥ 1M with unit='thousand' → treat as full NOK
            if unit_r == "thousand" and yr_r < 1993 and raw >= 1_000_000:
                return raw  # no scaling; value is already in full NOK
            factor = _SCALE_TO_BASE_UNIT.get(unit_r, 1.0)
            return raw * factor

        max_amt_no = agency.get("max_amount_local")
        if max_amt_no is not None:
            expanded_no = matches.apply(_norway_expanded_amt, axis=1)
            filtered = matches[expanded_no <= max_amt_no].copy()
            if not filtered.empty:
                matches = filtered
            else:
                # All candidate rows exceed the plausibility cap — emit gap rather
                # than use clearly-wrong data (e.g. section totals picked up as the
                # only row for an agency in a given year).
                return None

    # Slovakia-specific unit correction:
    # Manual audit of the extracted rows for 2009, 2010, 2018 and 2022 shows
    # that EUR-era appropriations are often mislabeled as unit='thousand' even
    # though the page value already represents the full-euro amount. Relabel
    # before ranking so later expansion is a no-op instead of an erroneous ×1000.
    if agency.get("_country") == "Slovakia":
        yr_sk = pd.to_numeric(matches.get("year"), errors="coerce")
        curr_sk = matches.get("currency", pd.Series("", index=matches.index)).fillna("").astype(str).str.upper()
        unit_sk = matches.get("unit", pd.Series("", index=matches.index)).fillna("").astype(str).str.strip().str.lower()
        relabel_mask = yr_sk.ge(2009) & curr_sk.eq("EUR") & unit_sk.eq("thousand")
        if relabel_mask.any():
            matches = matches.copy()
            matches.loc[relabel_mask, "unit"] = "unit"

    # Denmark-specific quality controls:
    # Danish Finanslov amounts are in thousands of DKK (unit='thousand').
    # The LLM sometimes extracts inflated values (e.g. 9,000,000 thousand = 9B DKK
    # for DTU) instead of the correct operating grant (~2-3B DKK).
    # Apply per-agency plausibility caps on the EXPECTED FINAL amount (after
    # expansion and after the pre-2001 ÷1000 correction that post-processing applies).
    if agency.get("_country") == "Denmark":
        max_amt_dk = agency.get("max_amount_local")
        if max_amt_dk is not None:
            def _dk_final_amt(row: pd.Series) -> float:
                raw = float(row.get("amount_local") or 0)
                unit_r = str(row.get("unit") or "").strip().lower()
                yr_r = int(str(row.get("year") or 9999).split(".")[0])
                factor = _SCALE_TO_BASE_UNIT.get(unit_r, 1.0)
                expanded = raw * factor
                # Replicate the post-processing pre-2001 correction applied later
                if yr_r <= 2000 and expanded >= 10_000_000_000.0:
                    expanded = expanded / 1000.0
                return expanded

            final_amts_dk = matches.apply(_dk_final_amt, axis=1)
            filtered = matches[final_amts_dk <= max_amt_dk].copy()
            if not filtered.empty:
                matches = filtered
            else:
                # All rows exceed the plausibility cap — emit gap rather than
                # use clearly-wrong data.
                return None

    # Switzerland-specific quality controls:
    # Swiss budget documents express amounts in full CHF but the LLM labels them
    # as unit='thousand'. Post-2020 VA_Band3-d.pdf correctly uses thousands for
    # small numbers (raw < 1_000_000); everywhere else the raw value IS the full
    # CHF amount (misidentified as 'thousand').
    #
    # Fix: rewrite unit='thousand' → unit='unit' for any row with raw ≥ 1_000_000
    # so that the caller's _expand_to_base_unit applies a factor of 1.0 (no ×1000).
    # Small-thousands rows (raw < 1M) keep their unit and scale correctly (×1000).
    if agency.get("_country") == "Switzerland":
        amt_num = pd.to_numeric(matches["amount_local"], errors="coerce")
        unit_lower = matches["unit"].fillna("").astype(str).str.strip().str.lower()
        full_chf_mask = unit_lower.eq("thousand") & amt_num.ge(1_000_000)
        if full_chf_mask.any():
            matches = matches.copy()
            matches.loc[full_chf_mask, "unit"] = "unit"

        max_amt_ch = agency.get("max_amount_local")
        if max_amt_ch is not None:
            # After unit correction, full-CHF rows have unit='unit' (factor=1.0)
            # and small-thousands rows still have unit='thousand' (factor=1000).
            corrected_ch = matches.apply(
                lambda r: float(r.get("amount_local") or 0)
                * _SCALE_TO_BASE_UNIT.get(str(r.get("unit") or "").strip().lower(), 1.0),
                axis=1,
            )
            filtered = matches[corrected_ch <= max_amt_ch].copy()
            if not filtered.empty:
                matches = filtered
            else:
                return None  # all exceed cap → emit gap

    # Netherlands-specific quality controls:
    # Apply per-agency plausibility caps (max_amount_local) against the standard
    # expanded amount (raw × unit-scale-factor). Pre-2002 NLG amounts with unit
    # 'million' are expanded by ×1_000_000; post-2001 EUR amounts with unit
    # 'thousand' by ×1_000. Rows where the expanded value exceeds the cap are
    # section-level aggregates or OCR artefacts — filter them out.
    if agency.get("_country") == "Netherlands":
        max_amt_nl = agency.get("max_amount_local")
        if max_amt_nl is not None:
            def _nl_expanded_amt(row: pd.Series) -> float:
                raw = float(row.get("amount_local") or 0)
                unit_r = str(row.get("unit") or "").strip().lower()
                factor = _SCALE_TO_BASE_UNIT.get(unit_r, 1.0)
                return raw * factor

            expanded_nl = matches.apply(_nl_expanded_amt, axis=1)
            filtered = matches[expanded_nl <= max_amt_nl].copy()
            if not filtered.empty:
                matches = filtered
            else:
                return None  # all exceed cap → emit gap

        # When both vetted institutional rows (`include`) and looser fallback
        # rows (`review`) survive for the same agency-year, keep the vetted rows.
        # This prevents large aggregate/recovery rows from overriding a cleaner
        # named agency appropriation, while still allowing review-only fallback
        # years through when no include row remains after plausibility filtering.
        nl_decision = matches.get("decision", pd.Series("", index=matches.index)).fillna("").astype(str).str.lower()
        include_matches = matches[nl_decision.eq("include")].copy()
        if not include_matches.empty:
            matches = include_matches

    # Spain-specific quality controls:
    # When using pipeline results, a few rows survive with clearly inflated
    # totals (usually a page summary or a unit-expansion artefact). Apply
    # conservative per-agency caps only where the canonical definition sets one.
    if agency.get("_country") == "Spain":
        max_amt_es = agency.get("max_amount_local")
        if max_amt_es is not None:
            def _es_expanded_amt(row: pd.Series) -> float:
                raw = float(row.get("amount_local") or 0)
                unit_r = str(row.get("unit") or "").strip().lower()
                factor = _SCALE_TO_BASE_UNIT.get(unit_r, 1.0)
                return raw * factor

            expanded_es = matches.apply(_es_expanded_amt, axis=1)
            filtered = matches[(expanded_es > 0) & (expanded_es <= max_amt_es)].copy()
            if not filtered.empty:
                matches = filtered
            else:
                return None

    if agency.get("_country") == "Luxembourg":
        matches = matches.copy()
        canonical_name_lu = str(agency.get("canonical_name", "") or "")
        desc_en_lu = matches.get("line_description_en", pd.Series("", index=matches.index)).fillna("").astype(str)
        desc_raw_lu = matches.get("line_description", pd.Series("", index=matches.index)).fillna("").astype(str)
        sec_en_lu = matches.get("section_name_en", pd.Series("", index=matches.index)).fillna("").astype(str)
        sec_raw_lu = matches.get("section_name", pd.Series("", index=matches.index)).fillna("").astype(str)
        text_lu = desc_en_lu + " " + desc_raw_lu + " " + sec_en_lu + " " + sec_raw_lu
        amount_lu = pd.to_numeric(matches.get("amount_local"), errors="coerce")
        unit_lu = matches.get("unit", pd.Series("", index=matches.index)).fillna("").astype(str).str.strip().str.lower()
        currency_lu = matches.get("currency", pd.Series("", index=matches.index)).fillna("").astype(str).str.upper()

        relabel_lu = amount_lu.notna() & unit_lu.eq("thousand") & currency_lu.isin(["EUR", "LUF"])
        if relabel_lu.any():
            matches.loc[relabel_lu, "unit"] = "unit"

        if canonical_name_lu != "Ministère de l'Enseignement Supérieur et de la Recherche (Luxembourg)":
            total_line_mask = desc_en_lu.str.match(r"^\s*total\b", case=False, na=False) | desc_raw_lu.str.match(r"^\s*total\b", case=False, na=False)
            if total_line_mask.any():
                filtered = matches.loc[~total_line_mask].copy()
                if not filtered.empty:
                    matches = filtered
                    desc_en_lu = matches.get("line_description_en", pd.Series("", index=matches.index)).fillna("").astype(str)
                    desc_raw_lu = matches.get("line_description", pd.Series("", index=matches.index)).fillna("").astype(str)
                    sec_en_lu = matches.get("section_name_en", pd.Series("", index=matches.index)).fillna("").astype(str)
                    sec_raw_lu = matches.get("section_name", pd.Series("", index=matches.index)).fillna("").astype(str)
                    text_lu = desc_en_lu + " " + desc_raw_lu + " " + sec_en_lu + " " + sec_raw_lu

        bad_cross_ministry_mask = text_lu.str.contains(
            r"transport|housing|logement|recettes pour ordre|revenues for order|d[ée]penses pour ordre|expenses for order",
            case=False,
            regex=True,
            na=False,
        )
        if bad_cross_ministry_mask.any():
            filtered = matches.loc[~bad_cross_ministry_mask].copy()
            if not filtered.empty:
                matches = filtered

        max_amt_lu = agency.get("max_amount_local")
        if max_amt_lu is not None:
            expanded_lu = matches.apply(
                lambda r: float(r.get("amount_local") or 0)
                * _SCALE_TO_BASE_UNIT.get(str(r.get("unit") or "").strip().lower(), 1.0),
                axis=1,
            )
            filtered = matches[(expanded_lu > 0) & (expanded_lu <= float(max_amt_lu))].copy()
            if not filtered.empty:
                matches = filtered
            else:
                return None

    if agency.get("_country") == "Colombia":
        max_amt_co = agency.get("max_amount_local")
        if max_amt_co is not None:
            expanded_co = matches.apply(
                lambda r: float(r.get("amount_local") or 0)
                * _SCALE_TO_BASE_UNIT.get(str(r.get("unit") or "").strip().lower(), 1.0),
                axis=1,
            )
            filtered = matches[(expanded_co > 0) & (expanded_co <= max_amt_co)].copy()
            if not filtered.empty:
                matches = filtered
            else:
                return None

    if agency.get("_country") == "Israel":
        max_amt_il = agency.get("max_amount_local")
        if max_amt_il is not None:
            expanded_il = matches.apply(
                lambda r: float(r.get("amount_local") or 0)
                * _SCALE_TO_BASE_UNIT.get(str(r.get("unit") or "").strip().lower(), 1.0),
                axis=1,
            )
            filtered = matches[(expanded_il > 0) & (expanded_il <= max_amt_il)].copy()
            if not filtered.empty:
                matches = filtered
            else:
                return None

    if agency.get("_country") == "Latvia":
        min_amt_lv = agency.get("min_amount_local")
        max_amt_lv = agency.get("max_amount_local")
        if min_amt_lv is not None or max_amt_lv is not None:
            amt_lv = pd.to_numeric(matches.get("amount_local"), errors="coerce")
            keep_lv = amt_lv > 0
            if min_amt_lv is not None:
                keep_lv &= amt_lv >= float(min_amt_lv)
            if max_amt_lv is not None:
                keep_lv &= amt_lv <= float(max_amt_lv)
            filtered = matches[keep_lv].copy()
            if not filtered.empty:
                matches = filtered
            else:
                return None

    if agency.get("_country") == "Costa Rica":
        amt_cr = pd.to_numeric(matches.get("amount_local"), errors="coerce")
        min_amt_cr = agency.get("min_amount_local")
        max_amt_cr = agency.get("max_amount_local")
        keep = pd.Series(True, index=matches.index)
        if min_amt_cr is not None:
            keep &= amt_cr.ge(float(min_amt_cr))
        if max_amt_cr is not None:
            keep &= amt_cr.le(float(max_amt_cr))
        filtered = matches[keep.fillna(False)].copy()
        if not filtered.empty:
            matches = filtered
        elif min_amt_cr is not None or max_amt_cr is not None:
            return None

    if agency.get("_country") == "Austria":
        # Austria amounts are normalized to MILLIONS before canonical selection.
        # Use conservative per-agency min/max bounds to drop chapter/global
        # totals that share an agency name on summary pages, while preserving
        # document-verifiable institutional lines when they exist.
        amt_at = pd.to_numeric(matches.get("amount_local"), errors="coerce")
        curr_at = matches.get("currency", pd.Series("", index=matches.index)).fillna("").astype(str).str.upper()
        min_amt_at = agency.get("min_amount_local")
        max_amt_at = agency.get("max_amount_local")
        min_amt_at_ats = agency.get("min_amount_local_ats")
        min_amt_at_eur = agency.get("min_amount_local_eur")
        if (
            min_amt_at is not None
            or max_amt_at is not None
            or min_amt_at_ats is not None
            or min_amt_at_eur is not None
        ):
            keep = amt_at.gt(0)
            if min_amt_at is not None:
                keep &= amt_at.ge(float(min_amt_at))
            if min_amt_at_ats is not None:
                keep &= ~curr_at.eq("ATS") | amt_at.ge(float(min_amt_at_ats))
            if min_amt_at_eur is not None:
                keep &= ~curr_at.eq("EUR") | amt_at.ge(float(min_amt_at_eur))
            if max_amt_at is not None:
                keep &= amt_at.le(float(max_amt_at))
            filtered = matches[keep.fillna(False)].copy()
            if not filtered.empty:
                matches = filtered
            else:
                return None

    if agency.get("_country") == "Mexico":
        amt_mx = pd.to_numeric(matches.get("amount_local"), errors="coerce")
        min_amt_mx = agency.get("min_amount_local")
        max_amt_mx = agency.get("max_amount_local")
        if min_amt_mx is not None or max_amt_mx is not None:
            keep = amt_mx.gt(0)
            if min_amt_mx is not None:
                keep &= amt_mx.ge(float(min_amt_mx))
            if max_amt_mx is not None:
                keep &= amt_mx.le(float(max_amt_mx))
            filtered = matches[keep.fillna(False)].copy()
            if not filtered.empty:
                matches = filtered
            else:
                return None

    # Iceland-specific quality controls:
    # Pipeline output is the right source for Iceland, but some OCR-heavy rows
    # still inflate a thousands-valued figure into a full-ISK amount before the
    # later unit expansion. Use conservative agency-specific caps so obviously
    # bad 100B+ rows drop out and the selector can fall back to a smaller
    # institution-level row from the same year/file.
    if agency.get("_country") == "Iceland":
        canonical_name_is = str(agency.get("canonical_name", "") or "")
        desc_en_is = matches.get("line_description_en", pd.Series("", index=matches.index)).fillna("").astype(str)
        sec_en_is = matches.get("section_name_en", pd.Series("", index=matches.index)).fillna("").astype(str)
        desc_raw_is = matches.get("line_description", pd.Series("", index=matches.index)).fillna("").astype(str)
        sec_raw_is = matches.get("section_name", pd.Series("", index=matches.index)).fillna("").astype(str)
        unit_is = matches.get("unit", pd.Series("", index=matches.index)).fillna("").astype(str).str.strip().str.lower()
        amt_is = pd.to_numeric(matches.get("amount_local", pd.Series(dtype=float)), errors="coerce")
        year_is = pd.to_numeric(matches.get("year", pd.Series(dtype=float)), errors="coerce")

        if canonical_name_is == "Byggingarsjóður rannsókna í þágu atvinnuveganna (Building Fund for Industry Research)":
            false_program_mask = (
                desc_raw_is.str.contains(r"ranns[oó]knir og v[öo]ktun [aá] n[áa]tt[uú]ru [ií]slands", case=False, na=False)
                | desc_en_is.str.contains(r"research and monitoring of iceland'?s nature", case=False, na=False)
                | desc_en_is.str.contains(r"operating budget,\s*million\s+kr", case=False, na=False)
                | desc_raw_is.str.contains(r"ranns[oó]knar?-?\s*og þróunarstarfsemi", case=False, na=False)
                | desc_en_is.str.contains(r"research and development activities", case=False, na=False)
                | desc_raw_is.str.contains(r"ranns[oó]knir,\s*þr[óo]un og n[ýy]sk[öo]pun [ií] landb[úu]na[ðd]arm[áa]lum", case=False, na=False)
                | desc_en_is.str.contains(r"research,\s*development and innovation in agricultural matters", case=False, na=False)
                | sec_raw_is.str.contains(r"17\.20", case=False, na=False)
                | sec_en_is.str.contains(r"research and monitoring of iceland'?s nature", case=False, na=False)
            )
            if false_program_mask.any():
                filtered = matches.loc[~false_program_mask].copy()
                if filtered.empty:
                    return None
                matches = filtered
                desc_en_is = matches.get("line_description_en", pd.Series("", index=matches.index)).fillna("").astype(str)
                sec_en_is = matches.get("section_name_en", pd.Series("", index=matches.index)).fillna("").astype(str)
                desc_raw_is = matches.get("line_description", pd.Series("", index=matches.index)).fillna("").astype(str)
                sec_raw_is = matches.get("section_name", pd.Series("", index=matches.index)).fillna("").astype(str)
                unit_is = matches.get("unit", pd.Series("", index=matches.index)).fillna("").astype(str).str.strip().str.lower()
                amt_is = pd.to_numeric(matches.get("amount_local", pd.Series(dtype=float)), errors="coerce")
                year_is = pd.to_numeric(matches.get("year", pd.Series(dtype=float)), errors="coerce")

        early_full_isk_mask = (
            unit_is.eq("thousand")
            & year_is.between(1981, 1984, inclusive="both")
            & amt_is.ge(1_000_000)
        )
        if early_full_isk_mask.any():
            matches = matches.copy()
            matches.loc[early_full_isk_mask, "unit"] = "unit"
            unit_is = matches.get("unit", pd.Series("", index=matches.index)).fillna("").astype(str).str.strip().str.lower()

        million_text_mask = (
            desc_en_is.str.contains(r"million\s+kr", case=False, na=False)
            | sec_en_is.str.contains(r"million\s+kr", case=False, na=False)
        )
        million_kr_mask = (
            unit_is.eq("thousand")
            & amt_is.between(1, 100_000, inclusive="both")
            & million_text_mask
        )
        if million_kr_mask.any():
            matches = matches.copy()
            matches.loc[million_kr_mask, "unit"] = "million"

            # Some Icelandic English annex tables express the section summary as
            # "Operating budget, million kr." and sibling rows (e.g. "Operating
            # appropriations") inherit the same unit but are mislabeled as
            # 'thousand'. Propagate the corrected unit within the same
            # source-file/section block when the raw values are also decimal-scale.
            sibling_keys = pd.DataFrame({
                "source_file": matches.get("source_file", pd.Series("", index=matches.index)).fillna("").astype(str),
                "section_name_en": sec_en_is,
                "section_name": sec_raw_is,
            })
            corrected_keys = {
                tuple(row)
                for row in sibling_keys.loc[million_kr_mask, ["source_file", "section_name_en", "section_name"]]
                .drop_duplicates()
                .itertuples(index=False, name=None)
            }
            sibling_mask = sibling_keys.apply(
                lambda row: (row["source_file"], row["section_name_en"], row["section_name"]) in corrected_keys,
                axis=1,
            )
            propagate_mask = (
                unit_is.eq("thousand")
                & amt_is.between(1, 100_000, inclusive="both")
                & sibling_mask
                & ~desc_raw_is.str.contains(r"t[æa]ki|búnaður", case=False, na=False)
                & ~desc_en_is.str.contains(r"equipment|apparatus", case=False, na=False)
            )
            if propagate_mask.any():
                matches.loc[propagate_mask, "unit"] = "million"

        max_amt_is = agency.get("max_amount_local")
        if max_amt_is is not None:
            expanded_is = matches.apply(
                lambda r: float(r.get("amount_local") or 0)
                * _SCALE_TO_BASE_UNIT.get(str(r.get("unit") or "").strip().lower(), 1.0),
                axis=1,
            )
            filtered = matches[(expanded_is > 0) & (expanded_is <= max_amt_is)].copy()
            if not filtered.empty:
                matches = filtered
            else:
                return None

    # Finland-specific quality controls:
    # (a) Exclude revenue lines (tulot) and multi-year commitment authority rows —
    #     the Finnish budget documents contain early summary pages where "granting
    #     authority" totals (myöntämisvaltuus) dwarf the actual annual appropriation.
    # (b) Apply per-agency FIM-era (pre-2002) and EUR-era plausibility caps using
    #     max_amount_local_fim / max_amount_local_eur fields on the canonical def.
    #     Amounts in Finland results are already in full currency units (unit='unit')
    #     after the Finland unit normalization in build_canonical_series, so no
    #     expansion factor is needed here.
    if agency.get("_country") == "Finland":
        desc_en = (
            matches.get("line_description_en", pd.Series("", index=matches.index))
            .fillna("").str.lower()
        )
        excl = desc_en.str.contains(
            r"\brevenue\b|grants and loans awarded|granting authority|"
            r"total amount proposed for tekes",
            regex=True,
        )
        if excl.any():
            trimmed = matches[~excl].copy()
            if not trimmed.empty:
                matches = trimmed

        max_fim = agency.get("max_amount_local_fim")
        max_eur = agency.get("max_amount_local_eur")
        if max_fim is not None or max_eur is not None:
            yr_fi = pd.to_numeric(
                matches.get("year", pd.Series(dtype=float)), errors="coerce"
            )
            cur_fi = (
                matches.get("currency", pd.Series("", index=matches.index))
                .fillna("").str.upper()
            )
            amt_fi = pd.to_numeric(matches["amount_local"], errors="coerce")
            keep = pd.Series(True, index=matches.index)
            if max_fim is not None:
                fim_mask = (yr_fi < 2002) | cur_fi.eq("FIM")
                keep &= ~fim_mask | (amt_fi <= max_fim)
            if max_eur is not None:
                eur_mask = (yr_fi >= 2002) & ~cur_fi.eq("FIM")
                keep &= ~eur_mask | (amt_fi <= max_eur)
            trimmed = matches[keep].copy()
            if not trimmed.empty:
                matches = trimmed

    # Some countries need extra narrowing before we rank amounts. Japan is the
    # main case: a ministry page often lists both the agency's operating grant
    # and much broader programme buckets that contain the agency name. Let the
    # canonical definition restrict matching to the intended line family.
    pattern_groups = agency.get("preferred_match_groups") or []
    if pattern_groups:
        combined_text = (
            matches.get("line_description_en", pd.Series("", index=matches.index)).fillna("").astype(str)
            + " "
            + matches.get("line_description", pd.Series("", index=matches.index)).fillna("").astype(str)
            + " "
            + matches.get("section_name_en", pd.Series("", index=matches.index)).fillna("").astype(str)
            + " "
            + matches.get("section_name", pd.Series("", index=matches.index)).fillna("").astype(str)
        )
        # Accent-fold both the searched text and the regex terms (see the
        # exclude_match_groups accent-fold above for rationale). Several France
        # programme-name patterns are written without diacritics (e.g.
        # "superieur") while the extracted text keeps them ("supérieur"),
        # so an un-folded comparison silently fails and drops a real match.
        combined_text = combined_text.map(_strip_accents)
        narrowed = None
        for group in pattern_groups:
            folded_group = [_strip_accents(pattern) for pattern in group]
            group_mask = combined_text.apply(
                lambda text: any(re.search(pattern, text, re.IGNORECASE) for pattern in folded_group)
            )
            if group_mask.any():
                narrowed = matches[group_mask].copy()
                break
        if narrowed is not None:
            matches = narrowed
        elif agency.get("enforce_preferred_match_groups", False):
            return None

    if agency.get("prefer_latest_page", False) and "page_number" in matches.columns:
        page_as_num = pd.to_numeric(matches["page_number"].astype(str).str.extract(r"(\d+)")[0], errors="coerce")
        if page_as_num.notna().any():
            matches = matches.assign(_page_num=page_as_num)
            max_page = matches["_page_num"].max()
            narrowed = matches[matches["_page_num"] == max_page].copy()
            if not narrowed.empty:
                matches = narrowed

    if agency.get("_country") == "Japan" and not matches.empty:
        operating_mask = matches.apply(
            lambda r: _japan_row_matches_any(_japan_row_text(r), _JAPAN_OPERATING_ROW_PATTERNS),
            axis=1,
        )
        if operating_mask.any():
            repaired = matches.loc[operating_mask].apply(
                lambda r: _japan_recover_small_operating_row_thousand(r, agency),
                axis=1,
            )
            good_mask = repaired.notna()
            if good_mask.any():
                idx = repaired.index[good_mask]
                matches.loc[idx, "amount_local"] = repaired.loc[good_mask].astype(float)

    if len(matches) > 1 and agency.get("_country") == "Japan":
        priority = matches.apply(_japan_row_priority, axis=1)
        min_priority = priority.min()
        narrowed = matches.loc[priority == min_priority].copy()
        if not narrowed.empty:
            matches = narrowed

    # Japan-specific safety net: after narrowing to the intended institutional
    # row family, trim clearly dominant upper siblings AND tiny lower fragments.
    # Running this after preferred-group matching avoids discarding the true
    # operating-grant line.
    if len(matches) > 1 and agency.get("_country") == "Japan":
        amounts = pd.to_numeric(matches["amount_local"], errors="coerce").dropna().tolist()
        if len(amounts) >= 2:
            amounts_sorted = sorted(float(a) for a in amounts)
            min_amt = amounts_sorted[0]
            second_smallest = amounts_sorted[1]
            second_largest = amounts_sorted[-2]
            max_amt = amounts_sorted[-1]

            # Upper-outlier trim: max is 25× larger than second-largest and > 1M
            if second_largest > 0 and max_amt >= max(second_largest * 25.0, 1_000_000.0):
                trimmed = matches[matches["amount_local"] != max_amt].copy()
                if not trimmed.empty:
                    matches = trimmed
                    # Recalculate after upper trim
                    amounts_sorted = sorted(
                        float(a) for a in pd.to_numeric(matches["amount_local"], errors="coerce").dropna()
                    )
                    min_amt = amounts_sorted[0]
                    second_smallest = amounts_sorted[1] if len(amounts_sorted) >= 2 else min_amt

            # Lower-fragment trim: min is < 2% of second-smallest AND < 5M units.
            # Catches tiny facility/grant fragments that coexist with the main
            # operating-grant row (e.g. NEDO 2010: 746K fragment vs 39.6M correct).
            if (
                len(amounts_sorted) >= 2
                and second_smallest > 0
                and min_amt < second_smallest * 0.02
                and min_amt < 5_000_000
            ):
                trimmed = matches[matches["amount_local"] != min_amt].copy()
                if not trimmed.empty:
                    matches = trimmed

    matches["_sec_score"] = matches.apply(
        lambda r: _section_match_score(
            str(r.get("line_description_en", "")),
            str(r.get("section_name_en", "")),
            agency,
            str(r.get("line_description", "")),
            str(r.get("section_name", "")),
        ),
        axis=1,
    )

    # Global maximum across all matches (across all score tiers and types)
    global_max = float(matches["amount_local"].max())

    # Try section-name matches first (score=2), then description-only (score=1)
    choose_smallest = agency.get("choose_smallest_match", False) and agency.get("_country") != "Japan"

    for score in [2, 1, 0]:
        pool = matches[matches["_sec_score"] == score]
        if pool.empty:
            continue

        pool_max = float(pool["amount_local"].max())

        # Only use this tier if its best amount is at least 20% of the global max.
        # This avoids picking a tiny supplementary-act CSIRO section_total (score=2)
        # over the main-act appropriation that happens to sit under a portfolio
        # section (score=1).
        if agency.get("_country") != "Japan" and pool_max < global_max * 0.20:
            continue

        for itype in preferred_types:
            subset = pool[pool["item_type"] == itype]
            if subset.empty:
                continue
            if choose_smallest:
                best_in_type = subset.loc[subset["amount_local"].idxmin()]
            else:
                best_in_type = subset.loc[subset["amount_local"].idxmax()]
            type_max = float(best_in_type["amount_local"])
            # Accept if it's at least 20% of the pool's global max
            if type_max >= pool_max * 0.20:
                return best_in_type

        if agency.get("strict_preferred_item_types", False):
            continue

        # Fallback within this score tier: largest overall
        if choose_smallest:
            return pool.loc[pool["amount_local"].idxmin()]
        return pool.loc[pool["amount_local"].idxmax()]

    # Should never reach here, but be safe
    if agency.get("strict_preferred_item_types", False):
        return None
    return matches.loc[matches["amount_local"].idxmax()]


def _get_agencies_for_country(country: str) -> list[dict]:
    """
    Return the merged list of canonical agencies for a country:
    hardcoded CANONICAL_AGENCIES + auto-discovered agencies from
    discovered_agencies.json (produced by agency_discovery.py).

    Discovered agencies that duplicate an existing canonical_name are skipped.
    """
    hardcoded = CANONICAL_AGENCIES.get(country, [])
    if country == "Israel":
        _israel_drop = {
            "Israel Science Foundation (ISF / קרן מדע ישראל)",
            "Hebrew University of Jerusalem",
            "Technion — Israel Institute of Technology",
            "Weizmann Institute of Science",
            "Volcani Center (Agricultural Research)",
        }
        hardcoded = [
            agency for agency in hardcoded
            if agency.get("canonical_name") not in _israel_drop
        ]
    if country == "Hungary":
        hardcoded = [
            agency
            for agency in hardcoded
            if agency.get("canonical_name") != "National Agricultural Research and Innovation Centre (Hungary)"
        ]
    existing_names = {a["canonical_name"].lower() for a in hardcoded}
    existing_aliases = set()
    existing_aliases_norm = set()
    for agency in hardcoded:
        canonical = str(agency.get("canonical_name", "")).strip().lower()
        source_entity = str(agency.get("source_entity", "")).strip().lower()
        existing_aliases.add(canonical)
        existing_aliases.add(source_entity)
        existing_aliases_norm.add(_normalise_match_text(canonical))
        existing_aliases_norm.add(_normalise_match_text(source_entity))
        for variant in agency.get("name_variants", []) or []:
            variant_text = str(variant).strip().lower()
            existing_aliases.add(variant_text)
            existing_aliases_norm.add(_normalise_match_text(variant_text))
    existing_aliases.discard("")
    existing_aliases_norm.discard("")

    try:
        from budget.agency_discovery import load_discovered_agencies
        discovered = load_discovered_agencies(country)
    except Exception:
        discovered = []

    def _aliases_conflict(discovered_aliases: set[str]) -> bool:
        discovered_aliases_norm = {_normalise_match_text(alias) for alias in discovered_aliases if alias}
        if existing_aliases.intersection(discovered_aliases):
            return True
        if existing_aliases_norm.intersection(discovered_aliases_norm):
            return True
        for alias in discovered_aliases:
            if not alias:
                continue
            alias_norm = _normalise_match_text(alias)
            for existing in existing_aliases:
                if not existing:
                    continue
                # Catch "Operating expenses grant for JST" style discovered
                # labels that wrap an already-hardcoded agency name.
                if len(alias) >= 12 and existing in alias:
                    return True
                if len(existing) >= 12 and alias in existing:
                    return True
            for existing_norm in existing_aliases_norm:
                if not existing_norm or not alias_norm:
                    continue
                if len(alias_norm) >= 12 and existing_norm in alias_norm:
                    return True
                if len(existing_norm) >= 12 and alias_norm in existing_norm:
                    return True
        return False

    def _skip_discovered_agency(agency: dict) -> bool:
        if country == "France":
            fields = [
                str(agency.get("canonical_name", "")).strip(),
                str(agency.get("source_entity", "")).strip(),
            ]
            for field in fields:
                if any(pat.search(field) for pat in _FRANCE_GENERIC_DISCOVERED_PATTERNS):
                    return True
            return False

        if country == "UK":
            name = str(agency.get("canonical_name", "")).strip()
            source = str(agency.get("source_entity", "")).strip()
            lowered = name.lower()
            if lowered in _UK_ALLOWED_DISCOVERED_CANONICALS:
                return False
            fields = [x for x in [name, source] if x]
            if any(pat.search(field) for field in fields for pat in _UK_GENERIC_DISCOVERED_PATTERNS):
                return True
            return True

        if country == "Finland":
            # Finland's 8 hardcoded canonicals (Academy, Tekes/BF, VTT, GTK,
            # Luke/MTT/Metla/RKTL, VATT) cover all institutional R&D series.
            # Agency discovery adds ~135 generic programme/fund names (energy
            # research funds, one-off grants, ministry-total labels) that
            # fragment the series and cause duplicate rows for the same agency.
            # Block all discovered agencies; rely solely on hardcoded ones.
            return True

        if country == "Austria":
            # Austria discovery is still valuable for surfacing candidate
            # institutions in the review artefacts, but the current merge set
            # is dominated by programme labels, global budgets, ministry totals,
            # and one-off budget buckets. Keep discovery visible for audit, but
            # do not auto-merge it into the canonical institutional panel.
            return True

        if country == "Turkey":
            # Turkey discovery is still valuable for surfacing candidate
            # institutions and budget lines for manual review, but the current
            # discovered set is dominated by programme captions, project titles,
            # transfer labels, and one-off university lines. Keep those visible
            # in the audit artefacts without auto-merging them into the final
            # canonical institutional panel.
            return True

        if country == "Iceland":
            # Keep Iceland discovery outputs available for review, but do not
            # merge them into the canonical panel. The current discovered set is
            # dominated by programme labels and one-off research lines that
            # fragment the audited institutional series.
            return True

        if country == "Costa Rica":
            # Costa Rica discovery is currently dominated by functional budget
            # categories and input-object lines rather than stable institutions.
            # Keep the audited panel conservative and institutional for now.
            return True

        if country == "Israel":
            # Israel discovery is useful for review, but the current output is
            # dominated by programme labels and one-off budget lines rather than
            # stable institutions. Keep discoveries out of the canonical panel.
            return True

        if country == "Portugal":
            # Portugal discovery surfaces many programme captions, support lines
            # and one-off legal transfers. Keep them in review artefacts and
            # source results, but do not auto-merge them into the final
            # canonical institutional series until they are manually audited.
            return True

        if country == "Korea":
            # Korea discovery is useful for surfacing new programme / agency
            # candidates for review, but the current source family is dominated
            # by one-off budget-brief summaries. Keep discoveries in the review
            # artefacts without auto-merging them into the final canonical panel.
            return True

        if country == "Estonia":
            name = str(agency.get("canonical_name", "")).strip()
            source = str(agency.get("source_entity", "")).strip()
            fields = [x for x in [name, source] if x]
            text = " ".join(fields)
            if not text:
                return True
            if any(pat.search(field) for field in fields for pat in _ESTONIA_GENERIC_DISCOVERED_PATTERNS):
                return True
            return not bool(_ESTONIA_ORGANISATION_HINTS.search(text))

        if country == "Colombia":
            agency_type = str(agency.get("agency_type", "")).strip().lower()
            name = str(agency.get("canonical_name", "")).strip()
            source = str(agency.get("source_entity", "")).strip()
            text = " ".join(x for x in [name, source] if x)
            if not text:
                return True
            # Colombia discovery currently yields many one-year programme labels
            # from the 2018 annex. Keep those visible in review outputs, but do
            # not merge them into the canonical institutional panel.
            if agency_type == "rd_programme":
                return True
            return not bool(_COLOMBIA_ORGANISATION_HINTS.search(text))

        if country == "Latvia":
            # Latvia discovery is still useful for surfacing candidate
            # programmes and institutions, but current recurrent candidates are
            # programme labels rather than standalone institutions. Keep them in
            # the review artifacts without auto-merging them into the audited
            # final panel.
            return str(agency.get("agency_type", "")).strip().lower() == "rd_programme"

        if country == "Chile":
            # Chile's discovery output is useful for audit, but the current
            # recurrent additions are mostly intermittent programme / fund
            # labels and one-off project titles that fragment the institutional
            # panel and create false annual gaps. Keep only stable discovered
            # institutions available in the final canonical series.
            agency_type = str(agency.get("agency_type", "")).strip().lower()
            category = str(agency.get("category", "")).strip().lower()
            if agency_type == "science_agency":
                return False
            if agency_type == "dedicated_rd_agency" and category == "science_agency":
                return False
            return True

        if country == "New Zealand":
            agency_type = str(agency.get("agency_type", "")).strip().lower()
            if agency_type in {"rd_programme", "rd_fund"}:
                return True
            name = str(agency.get("canonical_name", "")).strip()
            source = str(agency.get("source_entity", "")).strip()
            text = " ".join(x for x in [name, source] if x)
            if not text:
                return True
            return not bool(_NEW_ZEALAND_ORGANISATION_HINTS.search(text))

        if country != "Japan":
            return False

        name = str(agency.get("canonical_name", "")).strip()
        source = str(agency.get("source_entity", "")).strip()
        text = " ".join(x for x in [name, source] if x).strip()
        if not text:
            return True
        fields = [x for x in [name, source] if x]
        if any(pat.search(field) for field in fields for pat in _JAPAN_GENERIC_DISCOVERED_PATTERNS):
            return True

        if not _JAPAN_ORGANISATION_HINTS.search(text):
            return True

        return False

    merged = list(hardcoded)
    added = 0
    for agency in discovered:
        discovered_aliases = {
            str(agency.get("canonical_name", "")).strip().lower(),
            str(agency.get("source_entity", "")).strip().lower(),
        }
        for variant in agency.get("name_variants", []) or []:
            discovered_aliases.add(str(variant).strip().lower())
        discovered_aliases.discard("")

        if (
            not _skip_discovered_agency(agency)
            and agency.get("canonical_name", "").lower() not in existing_names
            and not _aliases_conflict(discovered_aliases)
        ):
            # Ensure required fields exist
            agency.setdefault("preferred_item_type", ["section_total", "program_total", "line_item"])
            agency.setdefault("active_years", (1900, 2099))
            if country == "France":
                agency.setdefault("choose_smallest_match", True)
            # Always ensure canonical_name and source_entity are in name_variants
            # so _match_agency can find the raw text (e.g. "CANADIAN SPACE AGENCY")
            variants = agency.setdefault("name_variants", [])
            canonical_lc = agency["canonical_name"].lower()
            for v in [agency["canonical_name"], agency.get("source_entity", "")]:
                if v and v.lower() not in [x.lower() for x in variants]:
                    variants.append(v)
            merged.append(agency)
            existing_names.add(canonical_lc)
            existing_aliases.update(x.strip().lower() for x in variants if isinstance(x, str) and x.strip())
            existing_aliases_norm.update(_normalise_match_text(x) for x in variants if isinstance(x, str) and x.strip())
            existing_aliases.add(canonical_lc)
            existing_aliases_norm.add(_normalise_match_text(canonical_lc))
            if agency.get("source_entity"):
                existing_aliases.add(str(agency["source_entity"]).strip().lower())
                existing_aliases_norm.add(_normalise_match_text(str(agency["source_entity"]).strip().lower()))
            added += 1

    if added:
        logger.info(f"[{country}] Merged {len(hardcoded)} hardcoded + {added} discovered agencies")

    return merged


def _uk_policy_recovery_rows(
    subset: pd.DataFrame,
    spec: dict,
) -> pd.DataFrame:
    line_desc = subset.get("line_description_en", pd.Series("", index=subset.index)).fillna("").astype(str)
    section = subset.get("section_name_en", pd.Series("", index=subset.index)).fillna("").astype(str)
    text = (line_desc + " || " + section).str.lower()

    mask = pd.Series(False, index=subset.index)
    for pattern in spec.get("patterns", []):
        mask = mask | text.str.contains(pattern, case=False, regex=True, na=False)

    section_patterns = spec.get("section_patterns") or []
    if section_patterns:
        section_mask = pd.Series(False, index=subset.index)
        for pattern in section_patterns:
            section_mask = section_mask | section.str.lower().str.contains(pattern, case=False, regex=True, na=False)
        mask = mask & section_mask

    for pattern in spec.get("exclude", []) or []:
        mask = mask & ~text.str.contains(pattern, case=False, regex=True, na=False)

    rows = subset.loc[mask].copy()
    if rows.empty:
        return rows

    rows["_base_amount"] = rows.apply(
        lambda r: _expand_to_base_unit(
            r.get("amount_local"),
            r.get("unit"),
            r.get("currency"),
        )[0],
        axis=1,
    )
    rows["_base_amount"] = pd.to_numeric(rows["_base_amount"], errors="coerce")
    rows = rows.dropna(subset=["_base_amount"])
    if rows.empty:
        return rows

    minimum = spec.get("min_amount")
    if minimum is not None:
        rows = rows[rows["_base_amount"] >= float(minimum)].copy()

    return rows


def build_canonical_series(
    df: pd.DataFrame,
    country: str,
    decision_filter: Optional[list[str]] = None,
) -> pd.DataFrame:
    """
    Build the canonical R&D time series for a country.

    Parameters
    ----------
    df               : full results DataFrame (already cleaned + deduped)
    country          : country name (must match CANONICAL_AGENCIES key)
    decision_filter  : list of decisions to include (default: ['include', 'review'])
                       Includes 'review' so that agency-matched rows are captured
                       even if the registry hasn't classified them yet.

    Returns
    -------
    DataFrame with columns:
        country, year, canonical_name, category,
        amount_local, unit, currency,
        item_type, line_description_en, source_file, page_number,
        series_notes
    """
    if decision_filter is None:
        # Include both "include" and "review" — name_variants act as the quality gate.
        # A row matched by a specific agency name_variant is almost certainly that agency
        # regardless of whether the registry has classified it yet.
        decision_filter = ["include", "review"]

    agencies = _get_agencies_for_country(country)
    if not agencies:
        logger.warning(f"No canonical agencies defined for '{country}'")
        return pd.DataFrame()

    subset = df[
        (df["country"] == country)
        & (df["decision"].isin(decision_filter))
    ].copy()

    subset["amount_local"] = pd.to_numeric(subset["amount_local"], errors="coerce")
    subset = subset.dropna(subset=["amount_local"])

    if country == "France" and not subset.empty:
        jorf_mask = (
            subset["source_file"].astype(str).str.contains(r"JORF_", case=False, na=False)
            & pd.to_numeric(subset["year"], errors="coerce").ge(2006)
        )
        cp_mask = (
            subset.get("line_description", pd.Series("", index=subset.index))
            .astype(str)
            .str.contains(r"Total pour|Crédits de paiement|Credits de paiement", case=False, na=False)
        )
        suspicious_mask = jorf_mask & cp_mask
        if suspicious_mask.any():
            repaired = subset.loc[suspicious_mask].apply(
                lambda r: _france_recover_cp_thousand(
                    str(r.get("source_file", "")),
                    r.get("page_number"),
                    str(r.get("line_description", "")),
                ),
                axis=1,
            )
            good_mask = repaired.notna()
            if good_mask.any():
                idx = repaired.index[good_mask]
                subset.loc[idx, "amount_local"] = repaired.loc[good_mask].astype(float)
                subset.loc[idx, "unit"] = "thousand"

    if country == "Japan" and not subset.empty:
        amt = pd.to_numeric(subset.get("amount_local"), errors="coerce")
        suspect_small = (
            amt.lt(100_000)
            & (
                subset.get("line_description", pd.Series("", index=subset.index)).astype(str).str.contains("運営費|交付金", na=False)
                | subset.get("line_description_en", pd.Series("", index=subset.index)).astype(str).str.contains(
                    r"operating subsidy|operating expenses|operating grant", case=False, regex=True, na=False
                )
            )
        )
        if suspect_small.any():
            repaired = subset.loc[suspect_small].apply(_japan_recover_small_operating_row_thousand, axis=1)
            good_mask = repaired.notna()
            if good_mask.any():
                idx = repaired.index[good_mask]
                subset.loc[idx, "amount_local"] = repaired.loc[good_mask].astype(float)
                subset.loc[idx, "unit"] = "thousand"

    input_years = (
        sorted(
            {
                int(y)
                for y in pd.to_numeric(
                    subset.get("year", pd.Series(dtype=float)),
                    errors="coerce",
                ).dropna().tolist()
            }
        )
        if not subset.empty
        else []
    )

    if country == "Finland" and not subset.empty:
        # -----------------------------------------------------------------------
        # Finland unit normalization:
        #
        # The LLM extracts amounts from narrative budget text where full EUR/FIM
        # values are written out (e.g. "Momentille myönnetään 146 310 000 mk"),
        # but systematically labels them unit='thousand' because the surrounding
        # summary tables use "1000 EUR / mk" column headers.
        #
        # Result: every stored amount is already in full currency units but
        # mislabeled as 'thousand', causing a spurious ×1000 scaling.
        # Fix: override ALL unit='thousand' → 'unit' for Finland.
        #
        # Pre-2002 currency fix:
        # Some FIM-era rows (1985-2001) were labeled currency='EUR' by the LLM
        # when it saw "euro" unit labels. Correct to 'FIM'.
        # -----------------------------------------------------------------------
        unit_fi = subset.get("unit", pd.Series("", index=subset.index)).fillna("").str.lower()
        amt_fi = pd.to_numeric(subset.get("amount_local"), errors="coerce")
        year_fi = pd.to_numeric(subset.get("year", pd.Series(dtype=float)), errors="coerce")

        # All 'thousand' labels in Finland are mislabeled full values
        fin_thousand_mask = unit_fi.eq("thousand") & amt_fi.notna()
        if fin_thousand_mask.any():
            subset.loc[fin_thousand_mask, "unit"] = "unit"
            logger.debug(
                f"[Finland] Normalized {fin_thousand_mask.sum()} rows: "
                f"unit='thousand' → 'unit' (full EUR/FIM amounts from narrative text)"
            )

        # Fix 'euro'/'dollar'/'markka'/'unknown' unit labels → 'unit'
        fin_bad_unit_mask = unit_fi.isin(["euro", "dollar", "markka", "unknown"]) & amt_fi.notna()
        if fin_bad_unit_mask.any():
            subset.loc[fin_bad_unit_mask, "unit"] = "unit"
            logger.debug(
                f"[Finland] Normalized {fin_bad_unit_mask.sum()} rows: "
                f"unit in (euro/dollar/markka/unknown) → 'unit'"
            )

        # Fix pre-2002 currency: FIM not EUR
        pre2002_eur_mask = (year_fi < 2002) & (
            subset.get("currency", pd.Series("", index=subset.index))
            .fillna("").eq("EUR")
        )
        if pre2002_eur_mask.any():
            subset.loc[pre2002_eur_mask, "currency"] = "FIM"
            logger.debug(
                f"[Finland] Fixed {pre2002_eur_mask.sum()} pre-2002 rows: currency='EUR' → 'FIM'"
            )

    if country == "Norway" and not subset.empty:
        # -----------------------------------------------------------------------
        # Norway unit normalization — two-era heuristic:
        #
        # Post-1992 (modern Statsbudsjettet, beløp i 1 000 kr):
        #   raw >= 10M with unit='thousand' → treat as full NOK (no ×1000 scaling).
        #   A 10M raw in thousands = 10B NOK, implausible for any single R&D line.
        #
        # Pre-1993 (scanned budget books, full NOK amounts):
        #   raw >= 1M with unit='thousand' → treat as full NOK.
        #   In 1975-1992, a 1M-raw value in thousands = 1B NOK, implausible for a
        #   sub-programme line in that era. Values of 50–900K raw are legitimate
        #   thousands (= 50–900M NOK for main research council grants).
        # -----------------------------------------------------------------------
        unit_s_no = subset.get("unit", pd.Series("", index=subset.index)).fillna("").str.lower()
        amt_no = pd.to_numeric(subset.get("amount_local"), errors="coerce")
        year_no = pd.to_numeric(subset.get("year", pd.Series(dtype=float)), errors="coerce")

        # Post-1992: threshold 10M
        post92_mask = (
            unit_s_no.isin(["thousand"])
            & amt_no.notna()
            & year_no.ge(1993)
            & (amt_no >= 10_000_000)
        )
        # Pre-1993: threshold 1M (full-NOK scanned documents mislabeled as thousands)
        pre93_mask = (
            unit_s_no.isin(["thousand"])
            & amt_no.notna()
            & year_no.lt(1993)
            & (amt_no >= 1_000_000)
        )
        norway_fullnok_mask = post92_mask | pre93_mask
        if norway_fullnok_mask.any():
            subset.loc[norway_fullnok_mask, "unit"] = "unit"
            logger.debug(
                f"[Norway] Normalized {norway_fullnok_mask.sum()} rows: "
                f"unit='thousand' → 'unit' (full NOK). "
                f"Post-1992: {post92_mask.sum()} rows (≥10M); "
                f"pre-1993: {pre93_mask.sum()} rows (≥1M)."
            )

    if country == "Mexico" and not subset.empty:
        # Mexico 2003+ CTI annex rows are typically expressed in full pesos even
        # when the extractor labels them as "thousand". Relabeling avoids a
        # spurious ×1000 expansion in the final canonical series.
        year_mx = pd.to_numeric(subset.get("year", pd.Series(dtype=float)), errors="coerce")
        curr_mx = subset.get("currency", pd.Series("", index=subset.index)).fillna("").astype(str).str.upper()
        unit_mx = subset.get("unit", pd.Series("", index=subset.index)).fillna("").astype(str).str.strip().str.lower()
        amt_mx = pd.to_numeric(subset.get("amount_local"), errors="coerce")
        text_mx = (
            subset.get("section_name", pd.Series("", index=subset.index)).fillna("").astype(str)
            + " "
            + subset.get("section_name_en", pd.Series("", index=subset.index)).fillna("").astype(str)
            + " "
            + subset.get("line_description", pd.Series("", index=subset.index)).fillna("").astype(str)
            + " "
            + subset.get("line_description_en", pd.Series("", index=subset.index)).fillna("").astype(str)
        ).str.lower()
        relabel_mx = year_mx.ge(2007) & curr_mx.eq("MXN") & unit_mx.eq("thousand") & amt_mx.ge(100_000)
        relabel_mx |= year_mx.between(2003, 2006, inclusive="both") & curr_mx.eq("MXN") & unit_mx.eq("thousand") & amt_mx.ge(1_000_000)
        relabel_mx |= (
            year_mx.ge(2003)
            & curr_mx.eq("MXN")
            & unit_mx.eq("thousand")
            & text_mx.str.contains(r"agencia espacial mexicana|mexican space agency", regex=True, na=False)
        )
        if relabel_mx.any():
            subset.loc[relabel_mx, "unit"] = "unit"
            logger.debug(
                f"[Mexico] Normalized {relabel_mx.sum()} rows: "
                f"unit='thousand' → 'unit' for 2003+ CTI annex full-peso rows"
            )

    if country == "Luxembourg" and not subset.empty:
        # Luxembourg budget tables print full LUF/EUR amounts with periods as
        # thousands separators. The extractor frequently mistakes that layout
        # for a "thousand" scale. Relabel before matching so later expansion
        # leaves the documentary amount unchanged.
        unit_lu = subset.get("unit", pd.Series("", index=subset.index)).fillna("").str.lower()
        curr_lu = subset.get("currency", pd.Series("", index=subset.index)).fillna("").str.upper()
        amt_lu = pd.to_numeric(subset.get("amount_local"), errors="coerce")

        full_unit_mask = unit_lu.eq("thousand") & curr_lu.isin(["LUF", "EUR"]) & amt_lu.notna()
        if full_unit_mask.any():
            subset.loc[full_unit_mask, "unit"] = "unit"
            logger.debug(
                f"[Luxembourg] Normalized {full_unit_mask.sum()} rows: "
                f"unit='thousand' → 'unit' for full-value budget rows"
            )

    if country == "Estonia" and not subset.empty:
        # Estonia often exposes full-unit amounts in budget text/tables while the
        # extractor labels them as "thousand". Correct only the clearly inflated
        # survivors and repair pre-euro currency labels.
        unit_ee = subset.get("unit", pd.Series("", index=subset.index)).fillna("").str.lower()
        amt_ee = pd.to_numeric(subset.get("amount_local"), errors="coerce")
        year_ee = pd.to_numeric(subset.get("year", pd.Series(dtype=float)), errors="coerce")
        curr_ee = subset.get("currency", pd.Series("", index=subset.index)).fillna("").str.upper()

        full_unit_mask = unit_ee.eq("thousand") & amt_ee.ge(1_000_000)
        if full_unit_mask.any():
            subset.loc[full_unit_mask, "unit"] = "unit"
            logger.debug(
                f"[Estonia] Normalized {full_unit_mask.sum()} rows: "
                f"unit='thousand' → 'unit' for large full-value budget rows"
            )

        pre_euro_mask = year_ee.between(1992, 2010, inclusive="both") & curr_ee.eq("EUR")
        if pre_euro_mask.any():
            subset.loc[pre_euro_mask, "currency"] = "EEK"
            logger.debug(
                f"[Estonia] Fixed {pre_euro_mask.sum()} rows: currency='EUR' → 'EEK' in pre-2011 budgets"
            )

        rub_mask = year_ee.eq(1991) & curr_ee.eq("RUB") & unit_ee.eq("thousand")
        if rub_mask.any():
            subset.loc[rub_mask, "unit"] = "unit"
            logger.debug(
                f"[Estonia] Fixed {rub_mask.sum()} 1991 RUB rows: unit='thousand' → 'unit' (original law is in roubles, not thousand roubles)"
            )

    if country == "Latvia" and not subset.empty:
        # Latvia budget laws from 1995 onward mostly present full LVL/EUR
        # amounts, while 1992-1993 are explicitly in thousand rubles.
        # Preserve the early ruble unit/currency, but remove the spurious
        # "thousand" scaling for the later LVL/EUR eras before matching.
        unit_lv = subset.get("unit", pd.Series("", index=subset.index)).fillna("").str.lower()
        year_lv = pd.to_numeric(subset.get("year", pd.Series(dtype=float)), errors="coerce")
        curr_lv = subset.get("currency", pd.Series("", index=subset.index)).fillna("").str.upper()
        amt_lv = pd.to_numeric(subset.get("amount_local"), errors="coerce")

        early_rub_mask = year_lv.le(1993) & amt_lv.notna()
        if early_rub_mask.any():
            subset.loc[early_rub_mask, "currency"] = "RUB"

        thousand_mask = unit_lv.eq("thousand") & year_lv.ge(1994) & amt_lv.notna()
        if thousand_mask.any():
            subset.loc[thousand_mask, "unit"] = "unit"

        bad_unit_mask = unit_lv.isin(["euro", "unknown"]) & year_lv.ge(1994) & amt_lv.notna()
        if bad_unit_mask.any():
            subset.loc[bad_unit_mask, "unit"] = "unit"

        pre_euro_mask = year_lv.between(1994, 2013, inclusive="both") & curr_lv.eq("EUR") & amt_lv.notna()
        if pre_euro_mask.any():
            subset.loc[pre_euro_mask, "currency"] = "LVL"

        zero_mask = amt_lv.eq(0)
        if zero_mask.any():
            subset = subset.loc[~zero_mask].copy()

    if country == "Lithuania" and not subset.empty:
        # Lithuania uses three monetary eras in these files:
        #   1993 = talonas, 1994-2014 = litas, 2015+ = euro.
        # The extractor frequently mislabeled all rows as EUR/euro even when
        # the stored amount is already in full local-currency units.
        unit_lt = subset.get("unit", pd.Series("", index=subset.index)).fillna("").str.lower()
        year_lt = pd.to_numeric(subset.get("year", pd.Series(dtype=float)), errors="coerce")
        curr_lt = subset.get("currency", pd.Series("", index=subset.index)).fillna("").str.upper()
        amt_lt = pd.to_numeric(subset.get("amount_local"), errors="coerce")

        tal_mask = year_lt.eq(1993) & amt_lt.notna()
        if tal_mask.any():
            subset.loc[tal_mask, "currency"] = "TAL"

        litas_mask = year_lt.between(1994, 2014, inclusive="both") & amt_lt.notna()
        if litas_mask.any():
            subset.loc[litas_mask, "currency"] = "LTL"

        euro_mask = year_lt.ge(2015) & amt_lt.notna() & curr_lt.ne("EUR")
        if euro_mask.any():
            subset.loc[euro_mask, "currency"] = "EUR"

        bad_unit_mask = unit_lt.isin(["euro", "unknown"]) & amt_lt.notna()
        if bad_unit_mask.any():
            subset.loc[bad_unit_mask, "unit"] = subset.loc[bad_unit_mask, "currency"].apply(
                lambda cur: _base_output_unit(cur, "unit")
            )

    if country == "Belgium" and not subset.empty:
        year_be = pd.to_numeric(subset.get("year", pd.Series(dtype=float)), errors="coerce")
        curr_be = subset.get("currency", pd.Series("", index=subset.index)).fillna("").str.upper()
        unit_be = subset.get("unit", pd.Series("", index=subset.index)).fillna("").str.lower()
        amt_be = pd.to_numeric(subset.get("amount_local"), errors="coerce")
        source_be = subset.get("source_file", pd.Series("", index=subset.index)).fillna("").astype(str)
        text_be = (
            subset.get("section_name", pd.Series("", index=subset.index)).fillna("").astype(str)
            + " "
            + subset.get("section_name_en", pd.Series("", index=subset.index)).fillna("").astype(str)
            + " "
            + subset.get("line_description", pd.Series("", index=subset.index)).fillna("").astype(str)
            + " "
                + subset.get("line_description_en", pd.Series("", index=subset.index)).fillna("").astype(str)
            )

        # Belgium's 2001 science-policy PDF is laid out in "Mio BEF". A few
        # OCR-derived decimal values like "274,6" or "13,4" were parsed as
        # 274600000 / 13400000 before the later "thousand" scaling, which
        # inflates them by an extra factor of 1000. Repair only that file here.
        bad_2001_mio_bef = (
            year_be.eq(2001)
            & source_be.str.contains(r"50K0905007", case=False, regex=True, na=False)
            & curr_be.eq("BEF")
            & unit_be.eq("thousand")
            & amt_be.between(10_000_000, 999_999_999, inclusive="both")
        )
        if bad_2001_mio_bef.any():
            subset = subset.copy()
            subset.loc[bad_2001_mio_bef, "amount_local"] = (
                pd.to_numeric(subset.loc[bad_2001_mio_bef, "amount_local"], errors="coerce") / 1000.0
            )
            amt_be = pd.to_numeric(subset.get("amount_local"), errors="coerce")

        # Pre-2002 EUR rows in Belgium are almost always OCR/registry mistakes.
        # Keep them out of the canonical layer rather than silently re-labelling.
        bad_pre_euro = year_be.lt(2002) & curr_be.eq("EUR") & amt_be.notna()
        if bad_pre_euro.any():
            subset = subset.loc[~bad_pre_euro].copy()
            year_be = pd.to_numeric(subset.get("year", pd.Series(dtype=float)), errors="coerce")
            curr_be = subset.get("currency", pd.Series("", index=subset.index)).fillna("").str.upper()
            unit_be = subset.get("unit", pd.Series("", index=subset.index)).fillna("").str.lower()
            amt_be = pd.to_numeric(subset.get("amount_local"), errors="coerce")
            source_be = subset.get("source_file", pd.Series("", index=subset.index)).fillna("").astype(str)
            text_be = (
                subset.get("section_name", pd.Series("", index=subset.index)).fillna("").astype(str)
                + " "
                + subset.get("section_name_en", pd.Series("", index=subset.index)).fillna("").astype(str)
                + " "
                + subset.get("line_description", pd.Series("", index=subset.index)).fillna("").astype(str)
                + " "
                + subset.get("line_description_en", pd.Series("", index=subset.index)).fillna("").astype(str)
            )

        # Federal scientific institutes in 2002+ sometimes appear in annex-style
        # statements with full-EUR values mislabeled as "thousand". A 1,028,000
        # row for the Royal Observatory is plausible in EUR, not in thousand EUR.
        institute_like = text_be.str.contains(
            r"observatoire|sterrenwacht|meteorolog|a[eé]ronom|sciences naturelles|natuurwetenschappen|radio-?element|institut royal|koninklijk",
            case=False,
            regex=True,
            na=False,
        )
        clean_grant_like = text_be.str.contains(
            r"grant to|allocation to|subsid(?:y|ies) to|from the state budget|dotation",
            case=False,
            regex=True,
            na=False,
        )
        full_eur_institute = (
            year_be.ge(2002)
            & curr_be.eq("EUR")
            & unit_be.eq("thousand")
            & amt_be.ge(500_000)
            & institute_like
        )
        if full_eur_institute.any():
            subset = subset.copy()
            subset.loc[full_eur_institute, "unit"] = "unit"

        # Clean institute grants/allocations are often genuine "thousand EUR"
        # rows. Convert them to actual EUR here so the row ranking compares
        # apples to apples against neighbouring OCR rows we already repaired to
        # plain EUR above.
        grant_thousand_eur = (
            year_be.ge(2002)
            & curr_be.eq("EUR")
            & unit_be.eq("thousand")
            & institute_like
            & clean_grant_like
            & amt_be.between(100, 100_000, inclusive="both")
        )
        if grant_thousand_eur.any():
            subset = subset.copy()
            subset.loc[grant_thousand_eur, "amount_local"] = (
                pd.to_numeric(subset.loc[grant_thousand_eur, "amount_local"], errors="coerce") * 1000.0
            )
            subset.loc[grant_thousand_eur, "unit"] = "unit"

    if country == "Italy" and not subset.empty:
        year_it = pd.to_numeric(subset.get("year", pd.Series(dtype=float)), errors="coerce")
        curr_it = subset.get("currency", pd.Series("", index=subset.index)).fillna("").astype(str).str.upper()
        unit_it = subset.get("unit", pd.Series("", index=subset.index)).fillna("").astype(str).str.strip().str.lower()
        amt_it = pd.to_numeric(subset.get("amount_local"), errors="coerce")

        pre_euro_mask = year_it.le(2001) & curr_it.eq("ITL") & amt_it.notna()

        # Italy's pre-euro rows mix two recurring signatures:
        # 1. amounts already captured in full lire but mislabeled as "thousand"
        # 2. printed million-lire values also mislabeled as "thousand"
        # Use a conservative size split so later expansion lands on full currency units.
        full_lira_mask = pre_euro_mask & unit_it.eq("thousand") & amt_it.ge(10_000_000)
        million_lira_mask = pre_euro_mask & unit_it.eq("thousand") & amt_it.lt(10_000_000)
        if full_lira_mask.any():
            subset.loc[full_lira_mask, "unit"] = "unit"
        if million_lira_mask.any():
            subset.loc[million_lira_mask, "unit"] = "million"

        literal_lira_mask = pre_euro_mask & unit_it.isin(["", "lire", "lira", "unit"])
        if literal_lira_mask.any():
            subset.loc[literal_lira_mask, "unit"] = "unit"

        post_euro_mask = year_it.ge(2002) & curr_it.eq("EUR") & amt_it.notna()
        post_euro_full_mask = post_euro_mask & unit_it.isin(["", "unit", "euro"])
        if post_euro_full_mask.any():
            subset.loc[post_euro_full_mask, "unit"] = "unit"

    if country == "Turkey" and not subset.empty:
        # Turkey mixes three source families with different comparability:
        #   1. Budget laws / budget justifications -> budget appropriations
        #   2. Genel Faaliyet Raporu             -> execution / realization
        #   3. Kesin Hesap                       -> final accounts
        # Keep the final canonical panel on budget-family documents only.
        source_tr = subset.get("source_file", pd.Series("", index=subset.index)).fillna("").astype(str)
        pass_tr = subset.get("extraction_pass", pd.Series("", index=subset.index)).fillna("").astype(str)
        activity_mask = source_tr.str.contains(r"GenelFaaliyetRaporu", case=False, regex=True, na=False)
        final_account_mask = source_tr.str.contains(
            r"Kesin.?Hesab|Merkezi.?Y[öo]netim.?Kesin",
            case=False,
            regex=True,
            na=False,
        )
        targeted_mask = pass_tr.eq("targeted_recovery")
        subset = subset.loc[~(activity_mask | final_account_mask | targeted_mask)].copy()

        # Turkey budget-law / justification tables are typically printed in full
        # lira values even when extraction labels them as "thousand". Keep the
        # printed value and normalize later to the final base-currency unit.
        unit_tr = subset.get("unit", pd.Series("", index=subset.index)).fillna("").astype(str).str.lower()
        relabel_tr = unit_tr.eq("thousand")
        if relabel_tr.any():
            subset.loc[relabel_tr, "unit"] = "unit"

        subset = subset[pd.to_numeric(subset.get("amount_local"), errors="coerce").gt(0)].copy()

    if country == "Netherlands" and not subset.empty:
        # -----------------------------------------------------------------------
        # Netherlands pre-processing (runs before the canonical matching loop):
        #
        # 1. Unit fix for 2000-2001 per-ministry NLG files.
        #    Files named YYYY_ministryN.pdf express amounts in FULL guilders (e.g.
        #    65,469,000 NLG for ECN) but the LLM labels them unit='thousand'.
        #    After the standard ×1000 expansion this becomes 65.5 billion NLG —
        #    clearly wrong.  Divide by 1000 so the correct scale is restored:
        #    65,469,000 → 65,469 (thousands) → ×1000 → 65.5M NLG ✓.
        #
        # 2. Drop zero-amount NLG rows.
        #    Budget memoranda (narrative text) frequently produced amount=0 when
        #    the LLM located a known institution but failed to read its amount.
        #    These are extraction failures, not real zero appropriations.
        #
        # 3. Drop pre-1999 EUR-labelled rows.
        #    The Netherlands adopted EUR for government accounting on 1 Jan 2002.
        #    Any row with currency='EUR' before 1999 is a LLM mislabelling of NLG.
        #    (1999–2001 EUR rows from baten-lastendienst agencies are kept but
        #     subject to max_amount_local cap filtering downstream.)
        # -----------------------------------------------------------------------
        amt_nl = pd.to_numeric(subset.get("amount_local"), errors="coerce")
        unit_nl = subset.get("unit", pd.Series("", index=subset.index)).fillna("").str.lower()
        curr_nl = subset.get("currency", pd.Series("", index=subset.index)).fillna("").str.upper()
        year_nl = pd.to_numeric(subset.get("year"), errors="coerce")
        src_nl = subset.get("source_file", pd.Series("", index=subset.index)).fillna("").astype(str)

        # 1. Divide 2000-2001 per-ministry NLG amounts by 1000
        ministry_nlg_mask = (
            year_nl.between(2000, 2001)
            & src_nl.str.contains(r"\d{4}_ministry\d+", regex=True, na=False)
            & curr_nl.eq("NLG")
            & unit_nl.isin(["thousand", "thousands"])
            & amt_nl.notna()
            & amt_nl.gt(0)
        )
        if ministry_nlg_mask.any():
            subset = subset.copy()
            subset.loc[ministry_nlg_mask, "amount_local"] = amt_nl.loc[ministry_nlg_mask] / 1000.0
            amt_nl = pd.to_numeric(subset.get("amount_local"), errors="coerce")
            logger.debug(
                f"[Netherlands] Unit fix: divided {ministry_nlg_mask.sum()} "
                f"2000-2001 per-ministry NLG rows by 1000 "
                f"(full-NLG amounts mislabelled as thousands)"
            )

        # 2. Divide pre-2000 budget-memorandum NLG non-section rows by 1000
        #    when the OCR/LLM captured the full-guilder amount but still labeled
        #    the row as 'thousand'. This pattern shows up in late-1990s agency
        #    contribution lines such as "Bijdrage NWO 100 miljoen" extracted as
        #    100,000,000 with unit='thousand' instead of 100,000.
        item_type_nl = subset.get("item_type", pd.Series("", index=subset.index)).fillna("").astype(str)
        pre2000_budget_full_nlg_mask = (
            year_nl.lt(2000)
            & src_nl.str.contains(r"budget memorandum", case=False, na=False)
            & curr_nl.eq("NLG")
            & unit_nl.isin(["thousand", "thousands"])
            & amt_nl.notna()
            & amt_nl.ge(10_000_000)
            & item_type_nl.ne("section_total")
        )
        if pre2000_budget_full_nlg_mask.any():
            subset = subset.copy()
            subset.loc[pre2000_budget_full_nlg_mask, "amount_local"] = (
                amt_nl.loc[pre2000_budget_full_nlg_mask] / 1000.0
            )
            amt_nl = pd.to_numeric(subset.get("amount_local"), errors="coerce")
            logger.debug(
                f"[Netherlands] Unit fix: divided {pre2000_budget_full_nlg_mask.sum()} "
                f"pre-2000 budget-memorandum NLG non-section rows by 1000 "
                f"(full-NLG amounts mislabelled as thousands)"
            )

        # 3. Drop zero-amount NLG rows (extraction failures)
        zero_nlg_mask = curr_nl.isin(["NLG"]) & amt_nl.notna() & amt_nl.eq(0)
        if zero_nlg_mask.any():
            subset = subset[~zero_nlg_mask].copy()
            logger.debug(
                f"[Netherlands] Dropped {zero_nlg_mask.sum()} zero-amount NLG rows "
                f"(LLM extraction failures — no real zero appropriations)"
            )
            curr_nl = subset.get("currency", pd.Series("", index=subset.index)).fillna("").str.upper()
            year_nl = pd.to_numeric(subset.get("year"), errors="coerce")

        # 4. Drop pre-1999 EUR rows (wrong currency era)
        pre1999_eur_mask = year_nl.lt(1999) & curr_nl.eq("EUR") & pd.to_numeric(subset.get("amount_local"), errors="coerce").notna()
        if pre1999_eur_mask.any():
            subset = subset[~pre1999_eur_mask].copy()
            logger.debug(
                f"[Netherlands] Dropped {pre1999_eur_mask.sum()} pre-1999 EUR rows "
                f"(currency mislabel — EUR did not exist in NL government accounts before 1999)"
            )

    records = []

    for agency in agencies:
        agency = dict(agency)
        agency["_country"] = country
        canonical_name = agency["canonical_name"]
        active_start, active_end = agency.get("active_years", (1800, 2099))

        # Older discovered_agencies.json entries often use a placeholder
        # 1900-2099 range even when the entity is only observed in one recent
        # budget year. Constrain those agencies to the years actually seen in
        # the current input so they do not manufacture decades of fake gaps.
        source_entity = str(agency.get("source_entity", "") or "").strip()
        n_years_seen = int(agency.get("n_years_seen", 0) or 0)
        observed_years = None
        if (
            (source_entity and n_years_seen > 0 and (active_start, active_end) == (1900, 2099))
            or (
                country == "Netherlands"
                and canonical_name in _NETHERLANDS_CLIP_TO_OBSERVED
            )
        ):
            source_entity_norm = _normalise_match_text(source_entity)
            observed_years = sorted(
                {
                    int(y)
                    for y in subset.loc[
                        subset.apply(
                            lambda r: source_entity_norm in _normalise_match_text(
                                " ".join(
                                    [
                                        str(r.get("line_description_en", "")),
                                        str(r.get("section_name_en", "")),
                                        str(r.get("line_description", "")),
                                        str(r.get("section_name", "")),
                                    ]
                                )
                            ),
                            axis=1,
                        )
                        if source_entity_norm
                        else subset.apply(
                            lambda r: _match_agency(
                                str(r.get("line_description_en", "")),
                                str(r.get("section_name_en", "")),
                                agency,
                                str(r.get("line_description", "")),
                                str(r.get("section_name", "")),
                            ),
                            axis=1,
                        ),
                        "year",
                    ].dropna().tolist()
                }
            )

        if source_entity and n_years_seen > 0 and (active_start, active_end) == (1900, 2099):
            if observed_years:
                active_start, active_end = observed_years[0], observed_years[-1]
            else:
                continue

        if country == "Netherlands" and canonical_name in _NETHERLANDS_CLIP_TO_OBSERVED:
            if observed_years:
                active_start, active_end = observed_years[0], observed_years[-1]
            else:
                continue

        year_groups = {}
        for year, year_df in subset.groupby("year"):
            try:
                year_groups[int(str(year))] = year_df
            except ValueError:
                continue

        explicit_years = agency.get("expected_years")
        if (
            country == "Chile"
            and not explicit_years
            and str(agency.get("agency_type", "")).strip().lower() in {"rd_programme", "rd_fund"}
        ):
            # Chile's discovered programme/fund lines are often intermittent
            # regional or earmarked appropriations. Treat them as observed-year
            # programmes, not annual institutional panels spanning every year
            # between first and last sighting.
            explicit_years = observed_years
        if explicit_years:
            iter_years = [int(y) for y in explicit_years if int(y) in input_years]
        else:
            iter_years = input_years

        for yr_int in iter_years:
            year_df = year_groups.get(yr_int)
            if year_df is None:
                year_df = subset.iloc[0:0]

            if not (active_start <= yr_int <= active_end):
                continue

            # Find matching rows
            matches = year_df[
                year_df.apply(
                    lambda r: _match_agency(
                        str(r.get("line_description_en", "")),
                        str(r.get("section_name_en", "")),
                        agency,
                        str(r.get("line_description", "")),  # raw fallback
                        str(r.get("section_name", "")),      # raw section fallback
                    ),
                    axis=1,
                )
            ]

            if matches.empty:
                # Gap year for this agency
                records.append({
                    "country": country,
                    "year": yr_int,
                    "canonical_name": canonical_name,
                    "category": agency["category"],
                    "amount_local": None,
                    "unit": None,
                    "currency": None,
                    "item_type": None,
                    "line_description_en": None,
                    "source_file": None,
                    "page_number": None,
                    "series_notes": "gap: no matching rows in this year",
                })
                continue

            # One row per source file — keeps separate Act amounts so the
            # detail series shows e.g. CSIRO 587,072 (No1) AND 12,224 (No2)
            # for the same year. build_totals_series handles aggregation.
            emitted = 0
            for source_file, file_matches in matches.groupby("source_file"):
                preferred_item_type = agency.get(
                    "preferred_item_type",
                    ["section_total", "program_total", "line_item"],
                )
                best = _best_amount_for_agency(file_matches, preferred_item_type, agency)
                if best is None:
                    continue
                if country == "Costa Rica":
                    # Costa Rica compile outputs are already stored in the final
                    # reporting unit we want to keep: thousand CRC. Expanding a
                    # second time here reintroduces a 1,000x inflation in the
                    # canonical series.
                    amount_local = pd.to_numeric(best.get("amount_local"), errors="coerce")
                    unit = best.get("unit")
                else:
                    amount_local, unit = _expand_to_base_unit(
                        best.get("amount_local"),
                        best.get("unit"),
                        best.get("currency"),
                    )
                records.append({
                    "country": country,
                    "year": yr_int,
                    "canonical_name": canonical_name,
                    "category": agency["category"],
                    "amount_local": amount_local,
                    "unit": unit,
                    "currency": best.get("currency"),
                    "item_type": best.get("item_type"),
                    "line_description_en": best.get("line_description_en"),
                    "source_file": source_file,
                    "page_number": best.get("page_number"),
                    "series_notes": agency.get("notes", ""),
                })
                emitted += 1

            if emitted == 0:
                # Fallback gap
                records.append({
                    "country": country,
                    "year": yr_int,
                    "canonical_name": canonical_name,
                    "category": agency["category"],
                    "amount_local": None,
                    "unit": None,
                    "currency": None,
                    "item_type": None,
                    "line_description_en": None,
                    "source_file": None,
                    "page_number": None,
                    "series_notes": "gap: no matching rows in this year",
                })

    out = pd.DataFrame(records)
    if out.empty:
        return out
    if "page_number" in out.columns:
        out["page_number"] = out["page_number"].astype(object)

    if country == "Italy":
        amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
        canonical = out["canonical_name"].fillna("").astype(str)
        year_num = pd.to_numeric(out["year"], errors="coerce")
        currency = out["currency"].fillna("").astype(str).str.upper()
        unit_norm = out["unit"].fillna("").astype(str).str.strip().str.lower()
        line_desc = out["line_description_en"].fillna("").astype(str)
        hardcoded_names = {agency["canonical_name"] for agency in CANONICAL_AGENCIES.get("Italy", [])}

        aggregate_canonicals = {
            "Ministero dell'università e della ricerca (MUR/MIUR/MURST)",
            "Missione 17 — Ricerca e innovazione",
        }
        final_panel_names = hardcoded_names - aggregate_canonicals

        discovered_or_generic_mask = amount_num.notna() & ~canonical.isin(hardcoded_names)
        if discovered_or_generic_mask.any():
            out.loc[
                discovered_or_generic_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[discovered_or_generic_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[discovered_or_generic_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; retained only for discovery traceability: final Italy panel is restricted to audited institutional/fund canonicals".strip("; ").strip()
            )
            amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
            unit_norm = out["unit"].fillna("").astype(str).str.strip().str.lower()

        aggregate_mask = canonical.isin(aggregate_canonicals)
        if aggregate_mask.any():
            out.loc[
                aggregate_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[aggregate_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[aggregate_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; excluded from final Italy panel: broad ministry/mission aggregate retained only for audit traceability".strip("; ").strip()
            )
            amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
            unit_norm = out["unit"].fillna("").astype(str).str.strip().str.lower()

        pre_euro_eur_mask = year_num.le(2001) & currency.eq("EUR") & amount_num.notna()
        if pre_euro_eur_mask.any():
            out.loc[
                pre_euro_eur_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[pre_euro_eur_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[pre_euro_eur_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Italy audit: pre-2002 budget-law appropriations should be in lire, so this EUR-labelled survivor is treated as column leakage or unit confusion".strip("; ").strip()
            )
            amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
            unit_norm = out["unit"].fillna("").astype(str).str.strip().str.lower()

        total_heading_mask = (
            amount_num.notna()
            & canonical.isin(
                {
                    "FOE — Fondo Ordinario per gli Enti di ricerca",
                    "FIRST / FAR / FIRB — Fondi per la ricerca",
                    "PRIN — Progetti di Rilevante Interesse Nazionale",
                    "CNR — Consiglio Nazionale delle Ricerche",
                    "ENEA",
                    "ASI — Agenzia Spaziale Italiana",
                    "INFN — Istituto Nazionale di Fisica Nucleare",
                    "INAF — Istituto Nazionale di Astrofisica",
                }
            )
            & line_desc.str.contains(
                r"total|totale per il ministero|totale del ministero|totale della sezione|totale della rubrica|totale del titolo|totale generale",
                case=False,
                regex=True,
                na=False,
            )
        )
        if total_heading_mask.any():
            out.loc[
                total_heading_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[total_heading_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[total_heading_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Italy audit: generic total heading would mix aggregate and institutional levels".strip("; ").strip()
            )
            amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
            unit_norm = out["unit"].fillna("").astype(str).str.strip().str.lower()

        zero_or_negative_mask = amount_num.notna() & amount_num.le(0)
        if zero_or_negative_mask.any():
            out.loc[
                zero_or_negative_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[zero_or_negative_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[zero_or_negative_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Italy audit: zero-value placeholder is not a transparent budget appropriation".strip("; ").strip()
            )
            amount_num = pd.to_numeric(out["amount_local"], errors="coerce")

        first_text = line_desc.str.lower()
        explicit_first_mask = canonical.eq("FIRST / FAR / FIRB — Fondi per la ricerca")
        weak_first_match = explicit_first_mask & ~first_text.str.contains(
            r"fondo per gli investimenti nella ricerca|fund for investments in scientific and technological research|research facilitation fund|fondo agevolazioni alla ricerca|\bfar\b|\bfirb\b|fondo per la ricerca di base|fund for basic research",
            regex=True,
            na=False,
        )
        if weak_first_match.any():
            out.loc[
                weak_first_match,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[weak_first_match, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[weak_first_match, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Italy audit: weak FIRST/FAR/FIRB text match".strip("; ").strip()
            )
            amount_num = pd.to_numeric(out["amount_local"], errors="coerce")

        cnr_bad_text = canonical.eq("CNR — Consiglio Nazionale delle Ricerche") & line_desc.str.contains(
            r"strategic committee|comitato strategico",
            case=False,
            regex=True,
            na=False,
        )
        if cnr_bad_text.any():
            out.loc[
                cnr_bad_text,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[cnr_bad_text, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[cnr_bad_text, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Italy audit: committee operating support is not a clean CNR budget series anchor".strip("; ").strip()
            )
            amount_num = pd.to_numeric(out["amount_local"], errors="coerce")

        generic_unit_mask = amount_num.notna() & unit_norm.isin(["", "unit"])
        if generic_unit_mask.any():
            out.loc[generic_unit_mask, "unit"] = out.loc[generic_unit_mask].apply(
                lambda r: _base_output_unit(r.get("currency"), r.get("unit")),
                axis=1,
            )

        amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
        modern_caps = {
            "CNR — Consiglio Nazionale delle Ricerche": 5_000_000_000.0,
            "ASI — Agenzia Spaziale Italiana": 3_500_000_000.0,
            "ENEA": 2_000_000_000.0,
            "FOE — Fondo Ordinario per gli Enti di ricerca": 2_000_000_000.0,
            "FIRST / FAR / FIRB — Fondi per la ricerca": 1_000_000_000.0,
            "PRIN — Progetti di Rilevante Interesse Nazionale": 1_000_000_000.0,
            "INFN — Istituto Nazionale di Fisica Nucleare": 1_500_000_000.0,
            "INAF — Istituto Nazionale di Astrofisica": 1_500_000_000.0,
        }
        for canonical_name, maximum in modern_caps.items():
            cap_mask = (
                canonical.eq(canonical_name)
                & year_num.ge(2002)
                & currency.eq("EUR")
                & amount_num.notna()
                & amount_num.gt(float(maximum))
            )
            if not cap_mask.any():
                continue
            out.loc[
                cap_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[cap_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[cap_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Italy audit: exceeds conservative EUR cap for this institutional series".strip("; ").strip()
            )
            amount_num = pd.to_numeric(out["amount_local"], errors="coerce")

        early_floor_mask = (
            canonical.eq("CNR — Consiglio Nazionale delle Ricerche")
            & year_num.le(2001)
            & currency.eq("ITL")
            & amount_num.notna()
            & amount_num.lt(100_000_000.0)
        )
        if early_floor_mask.any():
            out.loc[
                early_floor_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[early_floor_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[early_floor_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Italy audit: implausibly small pre-euro CNR survivor".strip("; ").strip()
            )
            amount_num = pd.to_numeric(out["amount_local"], errors="coerce")

        pre_euro_min_amounts = {
            "ASI — Agenzia Spaziale Italiana": 10_000_000_000.0,
            "ENEA": 10_000_000_000.0,
            "FOE — Fondo Ordinario per gli Enti di ricerca": 10_000_000_000.0,
            "INFN — Istituto Nazionale di Fisica Nucleare": 10_000_000_000.0,
        }
        for canonical_name, minimum in pre_euro_min_amounts.items():
            floor_mask = (
                canonical.eq(canonical_name)
                & year_num.le(2001)
                & currency.eq("ITL")
                & amount_num.notna()
                & amount_num.lt(float(minimum))
            )
            if not floor_mask.any():
                continue
            out.loc[
                floor_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[floor_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[floor_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Italy audit: pre-euro survivor is far below the institution's documented scale and is treated as a sub-line or scale-loss artifact".strip("; ").strip()
            )
            amount_num = pd.to_numeric(out["amount_local"], errors="coerce")

        implausible_itl = year_num.le(2001) & currency.eq("ITL") & amount_num.gt(20_000_000_000_000.0)
        implausible_eur = year_num.ge(2002) & currency.eq("EUR") & amount_num.gt(20_000_000_000.0)
        italy_implausible = implausible_itl | implausible_eur
        if italy_implausible.any():
            out.loc[
                italy_implausible,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[italy_implausible, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[italy_implausible, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Italy audit: implausible scale after documentary unit normalization".strip("; ").strip()
            )

        for year, canonical_name in _ITALY_VERIFIED_DROPS:
            mask = canonical.eq(canonical_name) & year_num.eq(year)
            if not mask.any():
                continue
            out.loc[
                mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after original-file audit: modern survivor is a programme/reduction/legal-clause amount, not a clean institutional annual appropriation".strip("; ").strip()
            )

        out = out[out["canonical_name"].isin(final_panel_names)].copy()

    if country == "Turkey":
        raw_turkey = subset.copy()
        raw_turkey["_year_num"] = pd.to_numeric(raw_turkey.get("year"), errors="coerce")

        def _clean_turkey_series_note(text: object) -> str:
            note = str(text or "").strip()
            if note.lower().startswith("gap: no matching rows in this year"):
                return ""
            return note

        def _turkey_raw_pick(
            year: int,
            include_patterns: list[str],
            prefer_patterns: list[str],
            *,
            exclude_patterns: list[str] | None = None,
            min_amount: float | None = None,
            max_amount: float | None = None,
        ) -> Optional[pd.Series]:
            work = raw_turkey[raw_turkey["_year_num"].eq(int(year))].copy()
            if work.empty:
                return None

            text = (
                work.get("line_description_en", pd.Series("", index=work.index)).fillna("").astype(str)
                + " "
                + work.get("line_description", pd.Series("", index=work.index)).fillna("").astype(str)
                + " "
                + work.get("section_name_en", pd.Series("", index=work.index)).fillna("").astype(str)
                + " "
                + work.get("section_name", pd.Series("", index=work.index)).fillna("").astype(str)
            )

            include_mask = pd.Series(False, index=work.index)
            for pat in include_patterns:
                include_mask |= text.str.contains(pat, case=False, regex=True, na=False)
            work = work.loc[include_mask].copy()
            if work.empty:
                return None

            if exclude_patterns:
                exclude_mask = pd.Series(False, index=work.index)
                for pat in exclude_patterns:
                    exclude_mask |= text.loc[work.index].str.contains(pat, case=False, regex=True, na=False)
                trimmed = work.loc[~exclude_mask].copy()
                if not trimmed.empty:
                    work = trimmed

            expanded = work.apply(
                lambda r: _expand_to_base_unit(r.get("amount_local"), r.get("unit"), r.get("currency")),
                axis=1,
                result_type="expand",
            )
            work["_base_amount"] = pd.to_numeric(expanded[0], errors="coerce")
            work["_base_unit"] = expanded[1]
            work = work[work["_base_amount"].notna() & work["_base_amount"].gt(0)].copy()
            if work.empty:
                return None

            if min_amount is not None:
                work = work[work["_base_amount"].ge(float(min_amount))].copy()
            if max_amount is not None:
                work = work[work["_base_amount"].le(float(max_amount))].copy()
            if work.empty:
                return None

            text_work = text.loc[work.index]
            section_work = (
                work.get("section_name_en", pd.Series("", index=work.index)).fillna("").astype(str)
                + " "
                + work.get("section_name", pd.Series("", index=work.index)).fillna("").astype(str)
            )
            work["_prefer_score"] = 0
            for idx, pat in enumerate(prefer_patterns, start=1):
                work.loc[text_work.str.contains(pat, case=False, regex=True, na=False), "_prefer_score"] += (
                    (len(prefer_patterns) + 1 - idx) * 10
                )

            work["_generic_section"] = section_work.str.contains(
                r"special budget entities|general budget entities|transfers|özel bütçeli idareler|genel bütçeli idareler",
                case=False,
                regex=True,
                na=False,
            )
            work["_transfer_like"] = text_work.str.contains(
                r"to be paid to|payments? to independent budget|transfers? to independent budget|yard[ıi]mlar|ödenecektir",
                case=False,
                regex=True,
                na=False,
            )
            item_rank = {"line_item": 3, "program_total": 2, "section_total": 1}
            work["_item_rank"] = work.get("item_type", pd.Series("", index=work.index)).fillna("").map(item_rank).fillna(0)
            work["_page_rank"] = pd.to_numeric(
                work.get("page_number", pd.Series("", index=work.index)).astype(str).str.extract(r"(\d+)")[0],
                errors="coerce",
            ).fillna(0)

            work = work.sort_values(
                ["_prefer_score", "_generic_section", "_transfer_like", "_item_rank", "_base_amount", "_page_rank"],
                ascending=[False, True, True, False, False, False],
                kind="stable",
            )
            return work.iloc[0]

        turkey_specs = {
            "TÜBİTAK (Turkey)": {
                "years": [2006, 2007, 2008, 2009],
                "include": [
                    r"\btübitak\b|\btubitak\b",
                    r"scientific and technological research council of turkey",
                    r"türkiye bilimsel ve teknolojik araştırma kurumu|turkiye bilimsel ve teknolojik arastirma kurumu",
                ],
                "prefer": [
                    r"research and development projects|support for scientific research",
                    r"scientific and technological research council of turkey.*research and development projects",
                    r"budget of tübitak|budget of tubitak",
                    r"scientific and technological research council of turkey.*total appropriation",
                    r"scientific and technological research council of turkey$",
                    r"research projects support program|research funds",
                    r"total appropriation|genel ödenek toplam[ıi]|toplam ödenek",
                ],
                "exclude": [
                    r"carry forward unspent",
                    r"science education",
                    r"international organizations",
                ],
            },
            "TAEK — Türkiye Atom Enerjisi Kurumu (Turkey)": {
                "years": [1976, 1977, 2006, 2007, 2008, 2009],
                "include": [
                    r"\btaek\b",
                    r"turkish atomic energy authority",
                    r"türkiye atom enerjisi kurumu|turkiye atom enerjisi kurumu",
                ],
                "prefer": [
                    r"scientific and technical research and application in the field of nuclear energy",
                    r"nuclear research projects?",
                    r"budget of the turkish atomic energy authority",
                    r"turkish atomic energy authority.*total appropriation",
                    r"research and development$",
                    r"turkish atomic energy authority$",
                    r"total appropriation|genel ödenek toplam[ıi]|toplam ödenek",
                ],
                "exclude": [
                    r"carry forward unspent",
                ],
            },
            "TÜBA (Turkey)": {
                "years": [2006, 2007, 2008, 2009],
                "include": [
                    r"\btüba\b|\btuba\b",
                    r"turkish academy of sciences",
                    r"türkiye bilimler akademisi|turkiye bilimler akademisi",
                ],
                "prefer": [
                    r"turkish academy of sciences$",
                    r"total appropriation|genel ödenek toplam[ıi]|toplam ödenek",
                ],
                "min_amount": 1_000_000.0,
            },
            "KOSGEB (Turkey)": {
                "years": [2006, 2007, 2008, 2009],
                "include": [
                    r"\bkosgeb\b",
                    r"small and medium enterprises development and support administration",
                    r"küçük ve orta ölçekli sanayi geliştirme ve destekleme idaresi başkanlığı",
                ],
                "prefer": [
                    r"small and medium enterprises development and support administration$",
                    r"budget of kosgeb|r&d supports|ar-ge destekleri",
                ],
                "min_amount": 10_000_000.0,
            },
        }

        for canonical_name, spec in turkey_specs.items():
            year_mask = out["canonical_name"].eq(canonical_name)
            if not year_mask.any():
                continue
            for year in sorted(pd.to_numeric(out.loc[year_mask, "year"], errors="coerce").dropna().astype(int).unique()):
                best = _turkey_raw_pick(
                    int(year),
                    spec["include"],
                    spec["prefer"],
                    exclude_patterns=spec.get("exclude"),
                    min_amount=spec.get("min_amount"),
                    max_amount=spec.get("max_amount"),
                )
                if best is None:
                    continue
                mask = out["canonical_name"].eq(canonical_name) & pd.to_numeric(out["year"], errors="coerce").eq(int(year))
                if not mask.any():
                    continue
                target_idx = out.index[mask][0]
                out.loc[
                    mask,
                    ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
                ] = [None, None, None, None, None, None, None]
                out.at[target_idx, "amount_local"] = float(best["_base_amount"])
                out.at[target_idx, "unit"] = best["_base_unit"]
                out.at[target_idx, "currency"] = best.get("currency")
                out.at[target_idx, "item_type"] = best.get("item_type")
                out.at[target_idx, "line_description_en"] = best.get("line_description_en")
                out.at[target_idx, "source_file"] = best.get("source_file")
                out.at[target_idx, "page_number"] = best.get("page_number")
                notes = _clean_turkey_series_note(out.at[target_idx, "series_notes"])
                out.at[target_idx, "series_notes"] = (
                    f"{notes}; selected from Turkey budget-family source row after excluding activity reports, final accounts, and targeted recovery noise"
                    .strip("; ")
                    .strip()
                )

        turkey_verified_overrides = {
            (2006, "TÜBİTAK (Turkey)"): (965_158_000.0, 132, "YTL", "2006 tbmm22103031ss1028.pdf"),
            (2006, "TAEK — Türkiye Atom Enerjisi Kurumu (Turkey)"): (50_050_000.0, 132, "YTL", "2006 tbmm22103031ss1028.pdf"),
            (2006, "TÜBA (Turkey)"): (4_641_000.0, 132, "YTL", "2006 tbmm22103031ss1028.pdf"),
            (2006, "KOSGEB (Turkey)"): (209_890_000.0, 132, "YTL", "2006 tbmm22103031ss1028.pdf"),
            (2007, "TÜBİTAK (Turkey)"): (938_335_000.0, 89, "YTL", "2009-ButceGerekcesi_2009.pdf"),
            (2008, "TÜBİTAK (Turkey)"): (1_005_923_000.0, 89, "YTL", "2009-ButceGerekcesi_2009.pdf"),
            (2009, "TÜBİTAK (Turkey)"): (1_127_085_000.0, 89, "TRY", "2009-ButceGerekcesi_2009.pdf"),
            (2007, "TAEK — Türkiye Atom Enerjisi Kurumu (Turkey)"): (65_075_000.0, 89, "YTL", "2009-ButceGerekcesi_2009.pdf"),
            (2008, "TAEK — Türkiye Atom Enerjisi Kurumu (Turkey)"): (65_139_000.0, 89, "YTL", "2009-ButceGerekcesi_2009.pdf"),
            (2009, "TAEK — Türkiye Atom Enerjisi Kurumu (Turkey)"): (82_169_000.0, 89, "TRY", "2009-ButceGerekcesi_2009.pdf"),
            (2007, "TÜBA (Turkey)"): (6_275_000.0, 89, "YTL", "2009-ButceGerekcesi_2009.pdf"),
            (2008, "TÜBA (Turkey)"): (6_575_000.0, 89, "YTL", "2009-ButceGerekcesi_2009.pdf"),
            (2009, "TÜBA (Turkey)"): (7_997_000.0, 89, "TRY", "2009-ButceGerekcesi_2009.pdf"),
            (2007, "KOSGEB (Turkey)"): (221_968_000.0, 89, "YTL", "2009-ButceGerekcesi_2009.pdf"),
            (2008, "KOSGEB (Turkey)"): (222_368_000.0, 89, "YTL", "2009-ButceGerekcesi_2009.pdf"),
            (2009, "KOSGEB (Turkey)"): (329_330_000.0, 89, "TRY", "2009-ButceGerekcesi_2009.pdf"),
        }
        for (year, canonical_name), (amount_local, page_number, currency, source_file) in turkey_verified_overrides.items():
            mask = out["canonical_name"].eq(canonical_name) & pd.to_numeric(out["year"], errors="coerce").eq(int(year))
            if not mask.any():
                continue
            target_idx = out.index[mask][0]
            out.loc[
                mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            out.at[target_idx, "amount_local"] = float(amount_local)
            out.at[target_idx, "unit"] = "unit"
            out.at[target_idx, "currency"] = currency
            out.at[target_idx, "item_type"] = "verified_override"
            if int(year) == 2006 and source_file == "2006 tbmm22103031ss1028.pdf":
                out.at[target_idx, "line_description_en"] = "2006 budget proposal TABLO 2-b — special-budget other administrations audited manually"
            else:
                out.at[target_idx, "line_description_en"] = "2009 budget-justification multi-year table — audited annual column"
            out.at[target_idx, "source_file"] = source_file
            out.at[target_idx, "page_number"] = str(page_number)
            notes = _clean_turkey_series_note(out.at[target_idx, "series_notes"])
            if int(year) == 2006 and source_file == "2006 tbmm22103031ss1028.pdf":
                out.at[target_idx, "series_notes"] = (
                    f"{notes}; verified from 2006 bütçe tasarısı TABLO 2-b using the audited special-budget administrations table"
                    .strip("; ")
                    .strip()
                )
            else:
                out.at[target_idx, "series_notes"] = (
                    f"{notes}; verified from 2009 Bütçe Gerekçesi TABLO 9 using the audited 2007-2009 annual columns"
                    .strip("; ")
                    .strip()
                )

        turkey_drop_pairs = {
            (2005, "TÜBİTAK (Turkey)"),
            (2005, "TAEK — Türkiye Atom Enerjisi Kurumu (Turkey)"),
            (1976, "TÜBİTAK (Turkey)"),
            (1977, "TÜBİTAK (Turkey)"),
            (1980, "TÜBİTAK (Turkey)"),
            (1981, "TÜBİTAK (Turkey)"),
            (1979, "TÜBİTAK (Turkey)"),
            (2000, "TÜBİTAK (Turkey)"),
            (1981, "TAEK — Türkiye Atom Enerjisi Kurumu (Turkey)"),
            (2005, "TÜBA (Turkey)"),
        }
        turkey_drop_mask = out.apply(
            lambda r: (r.get("year"), r.get("canonical_name")) in turkey_drop_pairs,
            axis=1,
        )
        if turkey_drop_mask.any():
            out.loc[
                turkey_drop_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[turkey_drop_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[turkey_drop_mask, "series_notes"] = notes.apply(
                lambda s: (
                    f"{s}; dropped after Turkey source audit: surviving row is transfer-like, project-specific, or too generic to defend as a transparent institutional budget appropriation"
                    .strip("; ")
                    .strip()
                )
            )

        out = out.loc[~out["canonical_name"].eq("Sanayi ve Teknoloji Bakanlığı (Turkey)")].copy()

        amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
        unit_norm = out["unit"].fillna("").astype(str).str.strip().str.lower()
        thousand_mask = amount_num.notna() & unit_norm.eq("thousand")
        if thousand_mask.any():
            out.loc[thousand_mask, "amount_local"] = amount_num.loc[thousand_mask] * 1000.0
            out.loc[thousand_mask, "unit"] = out.loc[thousand_mask].apply(
                lambda r: _base_output_unit(r.get("currency"), r.get("unit")),
                axis=1,
            )
            amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
            unit_norm = out["unit"].fillna("").astype(str).str.strip().str.lower()

        generic_unit_mask = amount_num.notna() & unit_norm.isin(["", "unit"])
        if generic_unit_mask.any():
            out.loc[generic_unit_mask, "unit"] = out.loc[generic_unit_mask].apply(
                lambda r: _base_output_unit(r.get("currency"), r.get("unit")),
                axis=1,
            )

    if country == "Mexico":
        mexico_verified_overrides = {
            (2017, "Mexican Space Agency"): (
                92_482_883.0,
                159,
                "MXN",
                "2017 30112016-MAT.pdf",
                "Agencia Espacial Mexicana",
            ),
        }
        for (year, canonical_name), (amount_local, page_number, currency, source_file, line_desc) in mexico_verified_overrides.items():
            mask = (out["year"] == year) & (out["canonical_name"] == canonical_name)
            if not mask.any():
                continue
            target_idx = out.index[mask][0]
            out.loc[mask, ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"]] = [None, None, None, None, None, None, None]
            out.at[target_idx, "amount_local"] = float(amount_local)
            out.at[target_idx, "unit"] = "unit"
            out.at[target_idx, "currency"] = currency
            out.at[target_idx, "item_type"] = "verified_override"
            out.at[target_idx, "line_description_en"] = line_desc
            out.at[target_idx, "source_file"] = source_file
            out.at[target_idx, "page_number"] = str(page_number)
            notes = str(out.at[target_idx, "series_notes"] or "").strip()
            out.at[target_idx, "series_notes"] = f"{notes}; verified against original Mexico CTI annex".strip("; ").strip()

        # Earlier Mexico audits conservatively blanked several rows whose scale
        # was thought to be ambiguous. Re-checking the original annex pages
        # shows these are ordinary peso-denominated institutional rows, so we
        # no longer drop them here.
        mexico_drop_pairs = set()
        mx_pair_mask = out.apply(
            lambda r: (r.get("year"), r.get("canonical_name")) in mexico_drop_pairs,
            axis=1,
        )
        if mx_pair_mask.any():
            out.loc[
                mx_pair_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[mx_pair_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[mx_pair_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Mexico source audit: surviving row is real but the scale is not defendable enough for the final institutional panel".strip("; ").strip()
            )

    if country == "Austria":
        austria_verified_overrides = {
            (1996, "OAW (Osterreichische Akademie der Wissenschaften)"): (
                380_499_000.0,
                "schilling",
                "ATS",
                "1996_Austria_proposal.pdf",
                58,
                "Förderungen",
            ),
            (1997, "OAW (Osterreichische Akademie der Wissenschaften)"): (
                385_000_000.0,
                "schilling",
                "ATS",
                "1997_Austria_proposal.pdf",
                58,
                "Förderungen",
            ),
            (1998, "OAW (Osterreichische Akademie der Wissenschaften)"): (
                405_000_000.0,
                "schilling",
                "ATS",
                "1998_Austria.pdf",
                58,
                "Förderungen",
            ),
            (1999, "OAW (Osterreichische Akademie der Wissenschaften)"): (
                489_000_000.0,
                "schilling",
                "ATS",
                "1999_Austria.pdf",
                60,
                "Summe 1417",
            ),
            (2012, "OAW (Osterreichische Akademie der Wissenschaften)"): (
                81_519_000.0,
                "euro",
                "EUR",
                "2012 Anlagen COO_2026_100_2_717880.pdf",
                170,
                "Summe 3117",
            ),
            (2022, "FWF (Fonds zur Forderung der wissenschaftlichen Forschung)"): (
                120_000_000.0,
                "euro",
                "EUR",
                "2022 Anlagen_0001_244184DB_8F25_451C_846B_6C4C7BD15ADA.pdf",
                383,
                "FWF - Fonds zur Förderung der wissenschaftlichen Forschung",
            ),
            (2022, "OAW (Osterreichische Akademie der Wissenschaften)"): (
                30_000_000.0,
                "euro",
                "EUR",
                "2022 Anlagen_0001_244184DB_8F25_451C_846B_6C4C7BD15ADA.pdf",
                384,
                "ÖAW - Österreichische Akademie der Wissenschaften",
            ),
            (2022, "IST Austria"): (
                25_000_000.0,
                "euro",
                "EUR",
                "2022 Anlagen_0001_244184DB_8F25_451C_846B_6C4C7BD15ADA.pdf",
                384,
                "IST Austria - Institute of Science and Technology Austria",
            ),
            (2023, "FFG (Forschungsfoerderungsgesellschaft)"): (
                70_000_000.0,
                "euro",
                "EUR",
                "2023 Anlagen_0001_C4324C91_98E6_4FDE_B63A_4BFB9024E450.pdf",
                452,
                "FFG (Forschungsförderungsgesellschaft)",
            ),
            (2023, "FWF (Fonds zur Forderung der wissenschaftlichen Forschung)"): (
                100_000_000.0,
                "euro",
                "EUR",
                "2023 Anlagen_0001_C4324C91_98E6_4FDE_B63A_4BFB9024E450.pdf",
                452,
                "FWF (Fonds zur Förderung der wissenschaftlichen Forschung)",
            ),
            (2023, "OAW (Osterreichische Akademie der Wissenschaften)"): (
                50_000_000.0,
                "euro",
                "EUR",
                "2023 Anlagen_0001_C4324C91_98E6_4FDE_B63A_4BFB9024E450.pdf",
                452,
                "ÖAW (Österreichische Akademie der Wissenschaften)",
            ),
            (2023, "IST Austria"): (
                30_000_000.0,
                "euro",
                "EUR",
                "2023 Anlagen_0001_C4324C91_98E6_4FDE_B63A_4BFB9024E450.pdf",
                452,
                "IST Austria (Institute of Science and Technology Austria)",
            ),
        }
        for (year, canonical_name), (amount_local, unit, currency, source_file, page_number, line_desc) in austria_verified_overrides.items():
            mask = (out["year"] == year) & (out["canonical_name"] == canonical_name)
            if not mask.any():
                continue
            target_idx = out.index[mask][0]
            out.loc[mask, ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"]] = [None, None, None, None, None, None, None]
            out.at[target_idx, "amount_local"] = float(amount_local)
            out.at[target_idx, "unit"] = unit
            out.at[target_idx, "currency"] = currency
            out.at[target_idx, "item_type"] = "verified_override"
            out.at[target_idx, "line_description_en"] = line_desc
            out.at[target_idx, "source_file"] = source_file
            out.at[target_idx, "page_number"] = str(page_number)
            notes = str(out.at[target_idx, "series_notes"] or "").strip()
            out.at[target_idx, "series_notes"] = f"{notes}; verified against original Austria budget".strip("; ").strip()

        austria_verified_drops = {
            # Verified against original PDFs: these extractions come from
            # summary / indicator pages, not clean institution-level
            # appropriations, so keep them as explicit gaps.
            (2012, "FWF (Fonds zur Forderung der wissenschaftlichen Forschung)"),
            (2013, "FWF (Fonds zur Forderung der wissenschaftlichen Forschung)"),
            (2018, "FWF (Fonds zur Forderung der wissenschaftlichen Forschung)"),
            (2020, "FWF (Fonds zur Forderung der wissenschaftlichen Forschung)"),
            (2021, "FWF (Fonds zur Forderung der wissenschaftlichen Forschung)"),
            (2024, "FWF (Fonds zur Forderung der wissenschaftlichen Forschung)"),
            (2026, "FWF (Fonds zur Forderung der wissenschaftlichen Forschung)"),
            (2012, "FFG (Forschungsfoerderungsgesellschaft)"),
            (2013, "FFG (Forschungsfoerderungsgesellschaft)"),
            (2014, "FFG (Forschungsfoerderungsgesellschaft)"),
            (2015, "FFG (Forschungsfoerderungsgesellschaft)"),
            (2017, "FFG (Forschungsfoerderungsgesellschaft)"),
            (2019, "FFG (Forschungsfoerderungsgesellschaft)"),
            (2021, "FFG (Forschungsfoerderungsgesellschaft)"),
            (2022, "FFG (Forschungsfoerderungsgesellschaft)"),
            (2024, "FFG (Forschungsfoerderungsgesellschaft)"),
            (2026, "FFG (Forschungsfoerderungsgesellschaft)"),
            (2026, "OAW (Osterreichische Akademie der Wissenschaften)"),
            (2010, "AIT Austrian Institute of Technology"),
            (2010, "Christian Doppler Forschungsgesellschaft (CD-Labor)"),
            (2018, "IST Austria"),
            (2010, "IST Austria"),
            (2021, "IST Austria"),
            (2026, "IST Austria"),
            (2001, "Ludwig Boltzmann Gesellschaft"),
            (2012, "CERN-Beitrag (Austria)"),
            (2022, "CERN-Beitrag (Austria)"),
            (2026, "CERN-Beitrag (Austria)"),
            (2012, "ESA-Beitrag (Austria)"),
            (2022, "ESA-Beitrag (Austria)"),
            (2026, "ESA-Beitrag (Austria)"),
        }
        for year, canonical_name in austria_verified_drops:
            mask = (out["year"] == year) & (out["canonical_name"] == canonical_name)
            if not mask.any():
                continue
            out.loc[
                mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[mask, "series_notes"].fillna("").astype(str)
            out.loc[mask, "series_notes"] = notes.apply(
                lambda x: f"{x}; dropped after original-file audit: no clean institutional appropriation line".strip("; ").strip()
            )

    if country == "Japan":
        out.loc[
            out["canonical_name"] == "MEXT (Ministry of Education, Culture, Sports, Science and Technology)",
            ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
        ] = [None, None, None, None, None, None, None]

        for year, canonical_name in _JAPAN_VERIFIED_DROPS:
            mask = (out["year"] == year) & (out["canonical_name"] == canonical_name)
            if not mask.any():
                continue
            out.loc[
                mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]

        year_source_map = (
            subset.dropna(subset=["source_file"])
            .groupby("year")["source_file"]
            .agg(lambda s: s.astype(str).mode().iloc[0] if not s.astype(str).mode().empty else None)
            .to_dict()
        )
        for year, overrides in _JAPAN_VERIFIED_OVERRIDES.items():
            for canonical_name, amount_local in overrides.items():
                mask = (out["year"] == year) & (out["canonical_name"] == canonical_name)
                if not mask.any():
                    continue
                target_idx = out.index[mask][0]
                out.loc[mask, "amount_local"] = None
                out.loc[mask, "unit"] = None
                out.loc[mask, "currency"] = None
                out.loc[mask, "item_type"] = None
                out.loc[mask, "line_description_en"] = None
                out.loc[mask, "page_number"] = None

                out.at[target_idx, "amount_local"] = float(amount_local)
                out.at[target_idx, "unit"] = "yen"
                out.at[target_idx, "currency"] = "JPY"
                out.at[target_idx, "item_type"] = "verified_override"
                out.at[target_idx, "line_description_en"] = "Verified against original Japan budget tables"
                out.at[target_idx, "source_file"] = year_source_map.get(year)
                notes = str(out.at[target_idx, "series_notes"] or "").strip()
                note = "manual override from original Japan budget file"
                out.at[target_idx, "series_notes"] = f"{notes}; {note}".strip("; ").strip()

        for canonical_name in _JAPAN_HISTORICAL_ROLLUP_SPECS:
            hist_mask = (out["canonical_name"] == canonical_name) & pd.to_numeric(out["year"], errors="coerce").between(1975, 2000)
            if not hist_mask.any():
                continue
            for target_idx in out.index[hist_mask]:
                year = int(out.at[target_idx, "year"])
                if (year, canonical_name) in _JAPAN_VERIFIED_DROPS:
                    continue
                if canonical_name in _JAPAN_VERIFIED_OVERRIDES.get(year, {}):
                    continue
                amount_local, source_file = _japan_compute_historical_rollup(subset, canonical_name, year)
                if amount_local is None:
                    continue
                same_mask = (out["canonical_name"] == canonical_name) & (out["year"] == year)
                out.loc[same_mask, ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"]] = [None, None, None, None, None, None, None]
                out.at[target_idx, "amount_local"] = float(amount_local)
                out.at[target_idx, "unit"] = "yen"
                out.at[target_idx, "currency"] = "JPY"
                out.at[target_idx, "item_type"] = "historical_rollup"
                out.at[target_idx, "line_description_en"] = "Historical predecessor rollup from original Japan budget rows"
                out.at[target_idx, "source_file"] = source_file
                out.at[target_idx, "page_number"] = None
                notes = str(out.at[target_idx, "series_notes"] or "").strip()
                note = "historical rollup from predecessor institution rows"
                out.at[target_idx, "series_notes"] = f"{notes}; {note}".strip("; ").strip()

    if country == "France":
        year_num = pd.to_numeric(out["year"], errors="coerce")
        pre_lolf_mask = year_num.le(2005)
        if pre_lolf_mask.any():
            pre_euro_mask = year_num.lt(2002) & pd.to_numeric(out["amount_local"], errors="coerce").notna()
            if pre_euro_mask.any():
                out.loc[pre_euro_mask, "currency"] = "FRF"
                out.loc[pre_euro_mask, "unit"] = "franc"

            keep_mask = out["canonical_name"].isin(_FRANCE_PRE_LOLF_KEEP_CANONICALS)
            drop_mask = pre_lolf_mask & ~keep_mask
            if drop_mask.any():
                out.loc[
                    drop_mask,
                    ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
                ] = [None, None, None, None, None, None, None]

            line_desc = out["line_description_en"].fillna("").astype(str)
            ministry_mask = pre_lolf_mask & (out["canonical_name"] == "Research (Pre-LOLF Ministry Chapter)")
            if ministry_mask.any():
                bad = line_desc.apply(lambda t: any(p.search(t) for p in _FRANCE_PRE_LOLF_MINISTRY_BAD_LINE_PATTERNS))
                good = line_desc.apply(lambda t: any(p.search(t) for p in _FRANCE_PRE_LOLF_MINISTRY_GOOD_LINE_PATTERNS))
                drop = ministry_mask & (~good | bad)
                # Avoid duplicating the same early aggregate under both the generic
                # ministry chapter and the explicit industrial/scientific development series.
                duplicate_industrial = ministry_mask & line_desc.str.contains(
                    r"industrial and scientific development",
                    case=False,
                    na=False,
                )
                drop = drop | duplicate_industrial
                # Once named research organizations enter the panel, keep the explicit
                # institutions and suppress the catch-all ministry aggregate for those years.
                explicit_orgs = {
                    "BRGM (Bureau de Recherches Géologiques et Minières)",
                    "CEA (Commissariat à l'Énergie Atomique)",
                    "CNES (Centre National d'Études Spatiales)",
                    "CNRS (Centre National de la Recherche Scientifique)",
                    "IFREMER (Institut Français de Recherche pour l'Exploitation de la Mer)",
                    "INRAE (Institut National de Recherche pour l'Agriculture)",
                    "INRIA (Institut National de Recherche en Informatique)",
                    "INSERM (Institut National de la Santé et de la Recherche Médicale)",
                }
                years_with_explicit_orgs = set(
                    out.loc[
                        pre_lolf_mask
                        & out["canonical_name"].isin(explicit_orgs)
                        & pd.to_numeric(out["amount_local"], errors="coerce").notna(),
                        "year",
                    ].tolist()
                )
                if years_with_explicit_orgs:
                    drop = drop | (ministry_mask & out["year"].isin(years_with_explicit_orgs))
                if drop.any():
                    out.loc[
                        drop,
                        ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
                    ] = [None, None, None, None, None, None, None]

            uni_mask = pre_lolf_mask & (out["canonical_name"] == "Universities and Higher Education (Pre-LOLF Chapter)")
            if uni_mask.any():
                good = line_desc.apply(lambda t: any(p.search(t) for p in _FRANCE_PRE_LOLF_UNI_GOOD_LINE_PATTERNS))
                drop = uni_mask & (~good)
                if drop.any():
                    out.loc[
                        drop,
                        ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
                    ] = [None, None, None, None, None, None, None]

            year_source_map = (
                subset.dropna(subset=["source_file"])
                .groupby("year")["source_file"]
                .agg(lambda s: s.astype(str).mode().iloc[0] if not s.astype(str).mode().empty else None)
                .to_dict()
            )
            for year, overrides in _FRANCE_VERIFIED_OVERRIDES.items():
                for canonical_name, amount_local in overrides.items():
                    mask = (out["year"] == year) & (out["canonical_name"] == canonical_name)
                    if not mask.any():
                        continue
                    target_idx = out.index[mask][0]
                    out.loc[mask, "amount_local"] = None
                    out.loc[mask, "unit"] = None
                    out.loc[mask, "currency"] = None
                    out.loc[mask, "item_type"] = None
                    out.loc[mask, "line_description_en"] = None
                    out.loc[mask, "page_number"] = None

                    out.at[target_idx, "amount_local"] = float(amount_local)
                    out.at[target_idx, "unit"] = "franc"
                    out.at[target_idx, "currency"] = "FRF"
                    out.at[target_idx, "item_type"] = "verified_override"
                    out.at[target_idx, "line_description_en"] = "Verified against original France budget file"
                    out.at[target_idx, "source_file"] = year_source_map.get(year)
                    notes = str(out.at[target_idx, "series_notes"] or "").strip()
                    note = "manual override from original France budget file"
                    out.at[target_idx, "series_notes"] = f"{notes}; {note}".strip("; ").strip()

            for year, overrides in _FRANCE_VERIFIED_OVERRIDES_MODERN.items():
                for canonical_name, (amount_local, page_number, currency, source_file, line_desc) in overrides.items():
                    mask = (out["year"] == year) & (out["canonical_name"] == canonical_name)
                    if not mask.any():
                        ref_row = out[out["canonical_name"] == canonical_name]
                        if ref_row.empty:
                            continue
                        new_row = ref_row.iloc[0].copy()
                        new_row["year"] = year
                        new_row["amount_local"] = float(amount_local)
                        new_row["unit"] = "euro"
                        new_row["currency"] = currency
                        new_row["item_type"] = "verified_override"
                        new_row["line_description_en"] = line_desc
                        new_row["source_file"] = source_file
                        new_row["page_number"] = str(page_number)
                        notes = str(new_row.get("series_notes") or "").strip()
                        note = "manual override from original France budget file"
                        new_row["series_notes"] = f"{notes}; {note}".strip("; ").strip()
                        out = pd.concat([out, new_row.to_frame().T], ignore_index=True)
                        continue

                    target_idx = out.index[mask][0]
                    out.loc[mask, "amount_local"] = None
                    out.loc[mask, "unit"] = None
                    out.loc[mask, "currency"] = None
                    out.loc[mask, "item_type"] = None
                    out.loc[mask, "line_description_en"] = None
                    out.loc[mask, "source_file"] = None
                    out.loc[mask, "page_number"] = None

                    out.at[target_idx, "amount_local"] = float(amount_local)
                    out.at[target_idx, "unit"] = "euro"
                    out.at[target_idx, "currency"] = currency
                    out.at[target_idx, "item_type"] = "verified_override"
                    out.at[target_idx, "line_description_en"] = line_desc
                    out.at[target_idx, "source_file"] = source_file
                    out.at[target_idx, "page_number"] = str(page_number)
                    notes = str(out.at[target_idx, "series_notes"] or "").strip()
                    note = "manual override from original France budget file"
                    out.at[target_idx, "series_notes"] = f"{notes}; {note}".strip("; ").strip()

    if country == "Denmark":
        amounts = pd.to_numeric(out["amount_local"], errors="coerce")
        years = pd.to_numeric(out["year"], errors="coerce")
        pages = pd.to_numeric(out["page_number"], errors="coerce")

        not_allowed = ~out["canonical_name"].isin(_DENMARK_ALLOWED_FINAL_CANONICALS)
        if not_allowed.any():
            out.loc[
                not_allowed,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            amounts = pd.to_numeric(out["amount_local"], errors="coerce")

        # Ministry-level Danish totals are too aggregate for the final panel.
        # Must run BEFORE dup_summary_idx so that inflated ministry amounts do not
        # collide with individual-institution amounts on the same page/source.
        ministry_mask = out["canonical_name"].isin(
            {
                "Uddannelses- og Forskningsministeriet (UFM)",
                "Videnskabsministeriet",
                "Undervisningsministeriet (research section)",
            }
        )
        if ministry_mask.any():
            out.loc[
                ministry_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            amounts = pd.to_numeric(out["amount_local"], errors="coerce")

        # Suppress the collective "Universiteterne" line BEFORE the dup_summary check.
        # "Universiteterne (collective)" matches on section_name_en="Universities" which
        # covers all individual-university rows, so _best_amount_for_agency returns the
        # largest individual row (KU). Without this early suppression, the dup_summary_idx
        # filter would see (source, page, KU-amount) shared by both KU and the collective
        # canonical and nullify the real KU value.
        explicit_uni = {
            "Kobenhavns Universitet (KU)",
            "Aarhus Universitet (AU)",
            "Danmarks Tekniske Universitet (DTU)",
            "Aalborg Universitet (AAU)",
            "Syddansk Universitet (SDU)",
            "Roskilde Universitetscenter (RUC)",
            "Copenhagen Business School (CBS)",
        }
        years_with_explicit_uni = set(
            out.loc[
                out["canonical_name"].isin(explicit_uni)
                & pd.to_numeric(out["amount_local"], errors="coerce").notna(),
                "year",
            ].tolist()
        )
        collective_mask = out["canonical_name"].eq("Universiteterne (collective)") & out["year"].isin(years_with_explicit_uni)
        if collective_mask.any():
            out.loc[
                collective_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            amounts = pd.to_numeric(out["amount_local"], errors="coerce")

        # Summary-page ghosts in Danish finance bills often assign the same
        # amount to multiple institutions on one page (both old and newer docs).
        # Applies across all years. Run after ministry/collective suppression above.
        denmark_real = out[amounts.notna()].copy()
        dup_summary_idx = []
        for (src, page, amount), grp in denmark_real.groupby(["source_file", "page_number", "amount_local"], dropna=False):
            if pd.isna(amount) or float(amount) < 100_000_000.0:
                continue
            if len(grp["canonical_name"].dropna().unique()) < 2:
                continue
            dup_summary_idx.extend(grp.index.tolist())
        if dup_summary_idx:
            out.loc[
                dup_summary_idx,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            amounts = pd.to_numeric(out["amount_local"], errors="coerce")

        # Pre-2001 rows were frequently parsed as "thousand" even though the
        # source line is already expressed in base DKK. Correct only the clearly
        # inflated survivors.
        scale_mask = (
            years.le(2000)
            & out["currency"].fillna("").eq("DKK")
            & amounts.ge(10_000_000_000.0)
        )
        if scale_mask.any():
            out.loc[scale_mask, "amount_local"] = amounts.loc[scale_mask] / 1000.0
            out.loc[scale_mask, "unit"] = "krone"
            notes = out.loc[scale_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[scale_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; corrected Denmark pre-2001 scale from misread thousand-unit row".strip("; ").strip()
            )
            amounts = pd.to_numeric(out["amount_local"], errors="coerce")

        # Ministry-level Danish totals are too aggregate for the final panel.
        # (Second pass kept here for symmetry; primary suppression is above.)
        ministry_mask = out["canonical_name"].isin(
            {
                "Uddannelses- og Forskningsministeriet (UFM)",
                "Videnskabsministeriet",
                "Undervisningsministeriet (research section)",
            }
        )
        if ministry_mask.any():
            out.loc[
                ministry_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]

        # Older Danish finance bills contain early overview pages that repeat
        # giant ministry/university summary figures and misassign them to
        # institutions. Keep only later detailed chapter pages for pre-2001.
        early_summary_mask = (
            years.le(2000)
            & pages.notna()
            & pages.le(40)
            & pd.to_numeric(out["amount_local"], errors="coerce").notna()
            & out["canonical_name"].isin(_DENMARK_ALLOWED_FINAL_CANONICALS)
        )
        if early_summary_mask.any():
            out.loc[
                early_summary_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]

        for year, canonical_name in _DENMARK_VERIFIED_DROPS:
            mask = (out["year"] == year) & (out["canonical_name"] == canonical_name)
            if not mask.any():
                continue
            out.loc[
                mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]

        for year, overrides in _DENMARK_VERIFIED_OVERRIDES.items():
            for canonical_name, (amount_local, page_number) in overrides.items():
                mask = (out["year"] == year) & (out["canonical_name"] == canonical_name)
                if not mask.any():
                    # No existing row — create a stub row (e.g. year missing from LLM results)
                    ref_row = out[out["canonical_name"] == canonical_name]
                    if ref_row.empty:
                        continue  # canonical not known at all — skip
                    new_row = ref_row.iloc[0].copy()
                    new_row["year"] = year
                    new_row["amount_local"] = float(amount_local)
                    new_row["unit"] = "krone"
                    new_row["currency"] = "DKK"
                    new_row["item_type"] = "verified_override"
                    new_row["line_description_en"] = "Verified against original Denmark budget file"
                    new_row["source_file"] = _DENMARK_VERIFIED_SOURCE_FILES.get(year)
                    new_row["page_number"] = str(page_number)
                    new_row["series_notes"] = "manual override from original Denmark budget file"
                    out = pd.concat([out, new_row.to_frame().T], ignore_index=True)
                    continue
                target_idx = out.index[mask][0]
                out.loc[mask, "amount_local"] = None
                out.loc[mask, "unit"] = None
                out.loc[mask, "currency"] = None
                out.loc[mask, "item_type"] = None
                out.loc[mask, "line_description_en"] = None
                out.loc[mask, "page_number"] = None

                out.at[target_idx, "amount_local"] = float(amount_local)
                out.at[target_idx, "unit"] = "krone"
                out.at[target_idx, "currency"] = "DKK"
                out.at[target_idx, "item_type"] = "verified_override"
                out.at[target_idx, "line_description_en"] = "Verified against original Denmark budget file"
                out.at[target_idx, "source_file"] = _DENMARK_VERIFIED_SOURCE_FILES.get(year)
                out.at[target_idx, "page_number"] = str(page_number)
                notes = str(out.at[target_idx, "series_notes"] or "").strip()
                note = "manual override from original Denmark budget file"
                out.at[target_idx, "series_notes"] = f"{notes}; {note}".strip("; ").strip()

    if country == "Switzerland":
        # --------------------------------------------------------------------
        # Unit label cleanup: _best_amount_for_agency rewrites full-CHF rows
        # from unit='thousand' → unit='unit' before returning, so the caller's
        # _expand_to_base_unit applies factor=1.0.  The series then contains the
        # raw CHF amount with unit='unit'.  Relabel these as 'franc' for clarity.
        # Small-thousands rows (raw < 1M) keep unit='thousand' and were correctly
        # scaled by ×1000 by _expand_to_base_unit → relabel as 'franc' too.
        # Also apply a safety ÷1000 pass for any rare cases that slipped through
        # (e.g. if unit='thousand' survived and amount > 10B).
        # --------------------------------------------------------------------
        amounts = pd.to_numeric(out["amount_local"], errors="coerce")

        # Relabel unit='unit' → 'franc' for all CHF rows
        unit_is_unit = out["unit"].fillna("").astype(str).str.strip().str.lower().eq("unit")
        if unit_is_unit.any():
            out.loc[unit_is_unit, "unit"] = "franc"

        # Safety pass: any remaining 'thousand'-unit Swiss amount > 10B is still
        # 1000× inflated (slipped through the unit-mutation in _best_amount_for_agency).
        residual_inflate = (
            amounts.notna()
            & (amounts > 10_000_000_000)
            & out["unit"].fillna("").astype(str).str.strip().str.lower().eq("thousand")
        )
        if residual_inflate.any():
            out.loc[residual_inflate, "amount_local"] = amounts[residual_inflate] / 1000.0
            out.loc[residual_inflate, "unit"] = "franc"
            amounts = pd.to_numeric(out["amount_local"], errors="coerce")

    if country == "Spain":
        for year, overrides in _SPAIN_VERIFIED_OVERRIDES.items():
            for canonical_name, (amount_local, page_number, currency, source_file) in overrides.items():
                mask = (out["year"] == year) & (out["canonical_name"] == canonical_name)
                if not mask.any():
                    ref_row = out[out["canonical_name"] == canonical_name]
                    if ref_row.empty:
                        continue
                    new_row = ref_row.iloc[0].copy()
                    new_row["year"] = year
                    new_row["amount_local"] = float(amount_local)
                    new_row["unit"] = "euro"
                    new_row["currency"] = currency
                    new_row["item_type"] = "verified_override"
                    new_row["line_description_en"] = "Verified against original Spain budget file"
                    new_row["source_file"] = source_file
                    new_row["page_number"] = str(page_number)
                    new_row["series_notes"] = "manual override from original Spain budget file"
                    out = pd.concat([out, new_row.to_frame().T], ignore_index=True)
                    continue
                target_idx = out.index[mask][0]
                out.loc[mask, "amount_local"] = None
                out.loc[mask, "unit"] = None
                out.loc[mask, "currency"] = None
                out.loc[mask, "item_type"] = None
                out.loc[mask, "line_description_en"] = None
                out.loc[mask, "page_number"] = None
                out.at[target_idx, "amount_local"] = float(amount_local)
                out.at[target_idx, "unit"] = "euro"
                out.at[target_idx, "currency"] = currency
                out.at[target_idx, "item_type"] = "verified_override"
                out.at[target_idx, "line_description_en"] = "Verified against original Spain budget file"
                out.at[target_idx, "source_file"] = source_file
                out.at[target_idx, "page_number"] = str(page_number)
                notes = str(out.at[target_idx, "series_notes"] or "").strip()
                out.at[target_idx, "series_notes"] = f"{notes}; manual override from original Spain budget file".strip("; ").strip()

    if country == "Finland":
        for year, overrides in _FINLAND_VERIFIED_OVERRIDES.items():
            for canonical_name, (amount_local, page_number, currency, source_file) in overrides.items():
                mask = (out["year"] == year) & (out["canonical_name"] == canonical_name)
                if not mask.any():
                    ref_row = out[out["canonical_name"] == canonical_name]
                    if ref_row.empty:
                        continue
                    new_row = ref_row.iloc[0].copy()
                    new_row["year"] = year
                    new_row["amount_local"] = float(amount_local)
                    new_row["unit"] = "unit"
                    new_row["currency"] = currency
                    new_row["item_type"] = "verified_override"
                    new_row["line_description_en"] = "Verified against original Finland budget file"
                    new_row["source_file"] = source_file
                    new_row["page_number"] = str(page_number)
                    new_row["series_notes"] = "manual override from original Finland budget file"
                    out = pd.concat([out, new_row.to_frame().T], ignore_index=True)
                    continue
                target_idx = out.index[mask][0]
                out.loc[mask, "amount_local"] = None
                out.loc[mask, "unit"] = None
                out.loc[mask, "currency"] = None
                out.loc[mask, "item_type"] = None
                out.loc[mask, "line_description_en"] = None
                out.loc[mask, "page_number"] = None
                out.at[target_idx, "amount_local"] = float(amount_local)
                out.at[target_idx, "unit"] = "unit"
                out.at[target_idx, "currency"] = currency
                out.at[target_idx, "item_type"] = "verified_override"
                out.at[target_idx, "line_description_en"] = "Verified against original Finland budget file"
                out.at[target_idx, "source_file"] = source_file
                out.at[target_idx, "page_number"] = str(page_number)
                notes = str(out.at[target_idx, "series_notes"] or "").strip()
                out.at[target_idx, "series_notes"] = f"{notes}; manual override from original Finland budget file".strip("; ").strip()

    if country == "Estonia":
        for year, overrides in _ESTONIA_VERIFIED_OVERRIDES.items():
            for canonical_name, (amount_local, page_number, currency, source_file) in overrides.items():
                mask = (out["year"] == year) & (out["canonical_name"] == canonical_name)
                if not mask.any():
                    ref_row = out[out["canonical_name"] == canonical_name]
                    if ref_row.empty:
                        continue
                    new_row = ref_row.iloc[0].copy()
                    new_row["year"] = year
                    new_row["amount_local"] = float(amount_local)
                    new_row["unit"] = "unit"
                    new_row["currency"] = currency
                    new_row["item_type"] = "verified_override"
                    new_row["line_description_en"] = "Verified against original Estonia budget file"
                    new_row["source_file"] = source_file
                    new_row["page_number"] = str(page_number)
                    new_row["series_notes"] = "manual override from original Estonia budget file"
                    out = pd.concat([out, new_row.to_frame().T], ignore_index=True)
                    continue
                target_idx = out.index[mask][0]
                out.loc[mask, "amount_local"] = None
                out.loc[mask, "unit"] = None
                out.loc[mask, "currency"] = None
                out.loc[mask, "item_type"] = None
                out.loc[mask, "line_description_en"] = None
                out.loc[mask, "page_number"] = None
                out.at[target_idx, "amount_local"] = float(amount_local)
                out.at[target_idx, "unit"] = "unit"
                out.at[target_idx, "currency"] = currency
                out.at[target_idx, "item_type"] = "verified_override"
                out.at[target_idx, "line_description_en"] = "Verified against original Estonia budget file"
                out.at[target_idx, "source_file"] = source_file
                out.at[target_idx, "page_number"] = str(page_number)
                notes = str(out.at[target_idx, "series_notes"] or "").strip()
                out.at[target_idx, "series_notes"] = f"{notes}; manual override from original Estonia budget file".strip("; ").strip()

    if country == "Lithuania":
        for year, overrides in _LITHUANIA_VERIFIED_OVERRIDES.items():
            for canonical_name, (amount_local, page_number, currency, source_file) in overrides.items():
                mask = (out["year"] == year) & (out["canonical_name"] == canonical_name)
                unit = _base_output_unit(currency, "unit")
                page_value = str(int(page_number)) if page_number is not None else None
                if not mask.any():
                    ref_row = out[out["canonical_name"] == canonical_name]
                    if ref_row.empty:
                        continue
                    new_row = ref_row.iloc[0].copy()
                    new_row["year"] = year
                    new_row["amount_local"] = float(amount_local)
                    new_row["unit"] = unit
                    new_row["currency"] = currency
                    new_row["item_type"] = "verified_override"
                    new_row["line_description_en"] = "Verified against original Lithuania budget file"
                    new_row["source_file"] = source_file
                    new_row["page_number"] = page_value
                    new_row["series_notes"] = "manual override from original Lithuania budget file"
                    out = pd.concat([out, new_row.to_frame().T], ignore_index=True)
                    continue
                target_idx = out.index[mask][0]
                out.loc[mask, "amount_local"] = None
                out.loc[mask, "unit"] = None
                out.loc[mask, "currency"] = None
                out.loc[mask, "item_type"] = None
                out.loc[mask, "line_description_en"] = None
                out.loc[mask, "page_number"] = None
                out.at[target_idx, "amount_local"] = float(amount_local)
                out.at[target_idx, "unit"] = unit
                out.at[target_idx, "currency"] = currency
                out.at[target_idx, "item_type"] = "verified_override"
                out.at[target_idx, "line_description_en"] = "Verified against original Lithuania budget file"
                out.at[target_idx, "source_file"] = source_file
                out.at[target_idx, "page_number"] = page_value
                notes = str(out.at[target_idx, "series_notes"] or "").strip()
                out.at[target_idx, "series_notes"] = f"{notes}; manual override from original Lithuania budget file".strip("; ").strip()

        lt_manual_drop = pd.Series(False, index=out.index)
        for year, canonical_name in _LITHUANIA_VERIFIED_DROPS:
            lt_manual_drop = lt_manual_drop | (
                out["canonical_name"].eq(canonical_name)
                & pd.to_numeric(out["year"], errors="coerce").eq(year)
            )
        if lt_manual_drop.any():
            out.loc[
                lt_manual_drop,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]

        lt_name = out["canonical_name"].fillna("").astype(str)
        university_mask = lt_name.str.contains(r"university", case=False, regex=True)
        arts_mask = lt_name.isin(
            {
                "Lithuanian Academy of Music and Theatre",
                "Vilnius Academy of Arts",
            }
        )
        overlapping_programme_mask = lt_name.eq("Science and Studies Programme (Lithuania)")
        special_investigation_mask = lt_name.eq("Special Investigation Service of the Republic of Lithuania")
        out = out.loc[
            ~(university_mask | arts_mask | overlapping_programme_mask | special_investigation_mask)
        ].reset_index(drop=True)

    if country == "Chile":
        for year, overrides in _CHILE_VERIFIED_OVERRIDES.items():
            for canonical_name, (amount_local, page_number, currency, source_file) in overrides.items():
                mask = (out["year"] == year) & (out["canonical_name"] == canonical_name)
                if not mask.any():
                    ref_row = out[out["canonical_name"] == canonical_name]
                    if ref_row.empty:
                        continue
                    new_row = ref_row.iloc[0].copy()
                    new_row["year"] = year
                    new_row["amount_local"] = float(amount_local)
                    new_row["unit"] = "peso"
                    new_row["currency"] = currency
                    new_row["item_type"] = "verified_override"
                    new_row["line_description_en"] = "Verified against original Chile budget file"
                    new_row["source_file"] = source_file
                    new_row["page_number"] = str(page_number)
                    new_row["series_notes"] = "manual override from original Chile budget file"
                    out = pd.concat([out, new_row.to_frame().T], ignore_index=True)
                    continue
                target_idx = out.index[mask][0]
                out.loc[mask, "amount_local"] = None
                out.loc[mask, "unit"] = None
                out.loc[mask, "currency"] = None
                out.loc[mask, "item_type"] = None
                out.loc[mask, "line_description_en"] = None
                out.loc[mask, "page_number"] = None
                out.at[target_idx, "amount_local"] = float(amount_local)
                out.at[target_idx, "unit"] = "peso"
                out.at[target_idx, "currency"] = currency
                out.at[target_idx, "item_type"] = "verified_override"
                out.at[target_idx, "line_description_en"] = "Verified against original Chile budget file"
                out.at[target_idx, "source_file"] = source_file
                out.at[target_idx, "page_number"] = str(page_number)
                notes = str(out.at[target_idx, "series_notes"] or "").strip()
                out.at[target_idx, "series_notes"] = f"{notes}; manual override from original Chile budget file".strip("; ").strip()

    if country == "Costa Rica":
        for year, overrides in _COSTA_RICA_VERIFIED_OVERRIDES.items():
            for canonical_name, override in overrides.items():
                if len(override) == 4:
                    amount_local, page_number, currency, source_file = override
                    override_notes = ""
                else:
                    amount_local, page_number, currency, source_file, override_notes = override
                mask = (out["year"] == year) & (out["canonical_name"] == canonical_name)
                if not mask.any():
                    ref_row = out[out["canonical_name"] == canonical_name]
                    if ref_row.empty:
                        continue
                    new_row = ref_row.iloc[0].copy()
                    new_row["year"] = year
                    new_row["amount_local"] = float(amount_local)
                    new_row["unit"] = "thousand"
                    new_row["currency"] = currency
                    new_row["item_type"] = "verified_override"
                    new_row["line_description_en"] = "Verified against original Costa Rica budget file"
                    new_row["source_file"] = source_file
                    new_row["page_number"] = str(page_number)
                    note_parts = ["manual override from original Costa Rica budget file"]
                    if override_notes:
                        note_parts.append(str(override_notes).strip())
                    new_row["series_notes"] = "; ".join(note_parts)
                    out = pd.concat([out, new_row.to_frame().T], ignore_index=True)
                    continue
                target_idx = out.index[mask][0]
                out.loc[mask, "amount_local"] = None
                out.loc[mask, "unit"] = None
                out.loc[mask, "currency"] = None
                out.loc[mask, "item_type"] = None
                out.loc[mask, "line_description_en"] = None
                out.loc[mask, "page_number"] = None
                out.at[target_idx, "amount_local"] = float(amount_local)
                out.at[target_idx, "unit"] = "thousand"
                out.at[target_idx, "currency"] = currency
                out.at[target_idx, "item_type"] = "verified_override"
                out.at[target_idx, "line_description_en"] = "Verified against original Costa Rica budget file"
                out.at[target_idx, "source_file"] = source_file
                out.at[target_idx, "page_number"] = str(page_number)
                notes = str(out.at[target_idx, "series_notes"] or "").strip()
                note_parts = [notes, "manual override from original Costa Rica budget file"]
                if override_notes:
                    note_parts.append(str(override_notes).strip())
                out.at[target_idx, "series_notes"] = "; ".join(part for part in note_parts if part)
        for year, canonical_name in _COSTA_RICA_VERIFIED_DROPS:
            mask = (out["year"] == year) & (out["canonical_name"] == canonical_name)
            if not mask.any():
                continue
            out.loc[
                mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
        out["_cr_has_amount"] = out["amount_local"].notna().astype(int)
        out["_cr_is_override"] = (out["item_type"] == "verified_override").astype(int)
        out = (
            out.sort_values(
                ["canonical_name", "year", "_cr_has_amount", "_cr_is_override"],
                ascending=[True, True, False, False],
                kind="stable",
            )
            .drop_duplicates(["canonical_name", "year"], keep="first")
            .drop(columns=["_cr_has_amount", "_cr_is_override"])
            .reset_index(drop=True)
        )
        amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
        unit_norm = out["unit"].fillna("").astype(str).str.strip().str.lower()
        thousand_mask = amount_num.notna() & unit_norm.eq("thousand")
        if thousand_mask.any():
            out.loc[thousand_mask, "amount_local"] = amount_num.loc[thousand_mask] * 1000.0
            out.loc[thousand_mask, "unit"] = out.loc[thousand_mask].apply(
                lambda r: _base_output_unit(r.get("currency"), r.get("unit")),
                axis=1,
            )

    if country == "New Zealand":
        for year, overrides in _NEW_ZEALAND_VERIFIED_OVERRIDES.items():
            for canonical_name, (amount_local, page_number, currency, source_file, line_description) in overrides.items():
                mask = (out["year"] == year) & (out["canonical_name"] == canonical_name)
                if not mask.any():
                    ref_row = out[out["canonical_name"] == canonical_name]
                    if ref_row.empty:
                        continue
                    new_row = ref_row.iloc[0].copy()
                    new_row["year"] = year
                    new_row["amount_local"] = float(amount_local)
                    new_row["unit"] = "dollar"
                    new_row["currency"] = currency
                    new_row["item_type"] = "verified_override"
                    new_row["line_description_en"] = line_description
                    new_row["source_file"] = source_file
                    new_row["page_number"] = str(page_number)
                    new_row["series_notes"] = "manual override from original New Zealand budget file"
                    out = pd.concat([out, new_row.to_frame().T], ignore_index=True)
                    continue
                target_idx = out.index[mask][0]
                out.loc[mask, "amount_local"] = None
                out.loc[mask, "unit"] = None
                out.loc[mask, "currency"] = None
                out.loc[mask, "item_type"] = None
                out.loc[mask, "line_description_en"] = None
                out.loc[mask, "page_number"] = None
                out.at[target_idx, "amount_local"] = float(amount_local)
                out.at[target_idx, "unit"] = "dollar"
                out.at[target_idx, "currency"] = currency
                out.at[target_idx, "item_type"] = "verified_override"
                out.at[target_idx, "line_description_en"] = line_description
                out.at[target_idx, "source_file"] = source_file
                out.at[target_idx, "page_number"] = str(page_number)
                notes = str(out.at[target_idx, "series_notes"] or "").strip()
                out.at[target_idx, "series_notes"] = f"{notes}; manual override from original New Zealand budget file".strip("; ").strip()
        for year, canonical_name in _NEW_ZEALAND_VERIFIED_DROPS:
            mask = (out["year"] == year) & (out["canonical_name"] == canonical_name)
            if not mask.any():
                continue
            out.loc[
                mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
        out["_nz_has_amount"] = out["amount_local"].notna().astype(int)
        out = (
            out.sort_values(
                ["canonical_name", "year", "_nz_has_amount"],
                ascending=[True, True, False],
                kind="stable",
            )
            .drop_duplicates(["canonical_name", "year"], keep="first")
            .drop(columns=["_nz_has_amount"])
            .reset_index(drop=True)
        )

    if country == "Iceland":
        for year, overrides in _ICELAND_VERIFIED_OVERRIDES.items():
            for canonical_name, (amount_local, page_number, currency, source_file) in overrides.items():
                mask = (out["year"] == year) & (out["canonical_name"] == canonical_name)
                if not mask.any():
                    ref_row = out[out["canonical_name"] == canonical_name]
                    if ref_row.empty:
                        continue
                    new_row = ref_row.iloc[0].copy()
                    new_row["year"] = year
                    new_row["amount_local"] = float(amount_local)
                    new_row["unit"] = "krona"
                    new_row["currency"] = currency
                    new_row["item_type"] = "verified_override"
                    new_row["line_description_en"] = "Verified against original Iceland budget file"
                    new_row["source_file"] = source_file
                    new_row["page_number"] = str(page_number)
                    new_row["series_notes"] = "manual override from original Iceland budget file"
                    out = pd.concat([out, new_row.to_frame().T], ignore_index=True)
                    continue
                target_idx = out.index[mask][0]
                out.loc[mask, "amount_local"] = None
                out.loc[mask, "unit"] = None
                out.loc[mask, "currency"] = None
                out.loc[mask, "item_type"] = None
                out.loc[mask, "line_description_en"] = None
                out.loc[mask, "page_number"] = None
                out.at[target_idx, "amount_local"] = float(amount_local)
                out.at[target_idx, "unit"] = "krona"
                out.at[target_idx, "currency"] = currency
                out.at[target_idx, "item_type"] = "verified_override"
                out.at[target_idx, "line_description_en"] = "Verified against original Iceland budget file"
                out.at[target_idx, "source_file"] = source_file
                out.at[target_idx, "page_number"] = str(page_number)
                notes = str(out.at[target_idx, "series_notes"] or "").strip()
                out.at[target_idx, "series_notes"] = f"{notes}; manual override from original Iceland budget file".strip("; ").strip()

    if country == "Israel":
        israel_year = pd.to_numeric(out.get("year"), errors="coerce")

        for year, canonical_name in _ISRAEL_VERIFIED_DROPS:
            mask = out["canonical_name"].eq(canonical_name) & israel_year.eq(year)
            if not mask.any():
                continue
            out.loc[
                mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Israel source audit: surviving match was a noisy summary row or wrong budget-year half".strip("; ").strip()
            )

        for year, overrides in _ISRAEL_VERIFIED_OVERRIDES.items():
            for canonical_name, (amount_local, page_number, currency, source_file) in overrides.items():
                mask = out["canonical_name"].eq(canonical_name) & israel_year.eq(year)
                if not mask.any():
                    ref_row = out[out["canonical_name"] == canonical_name]
                    if ref_row.empty:
                        continue
                    new_row = ref_row.iloc[0].copy()
                    new_row["year"] = year
                    new_row["amount_local"] = float(amount_local)
                    new_row["unit"] = "thousand"
                    new_row["currency"] = currency
                    new_row["item_type"] = "verified_override"
                    new_row["line_description_en"] = "Verified against original Israel budget file"
                    new_row["source_file"] = source_file
                    new_row["page_number"] = str(page_number)
                    new_row["series_notes"] = "manual override from original Israel budget file"
                    out = pd.concat([out, new_row.to_frame().T], ignore_index=True)
                    continue

                target_idx = out.index[mask][0]
                out.loc[
                    mask,
                    ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
                ] = [None, None, None, None, None, None, None]
                out.at[target_idx, "amount_local"] = float(amount_local)
                out.at[target_idx, "unit"] = "thousand"
                out.at[target_idx, "currency"] = currency
                out.at[target_idx, "item_type"] = "verified_override"
                out.at[target_idx, "line_description_en"] = "Verified against original Israel budget file"
                out.at[target_idx, "source_file"] = source_file
                out.at[target_idx, "page_number"] = str(page_number)
                notes = str(out.at[target_idx, "series_notes"] or "").strip()
                out.at[target_idx, "series_notes"] = f"{notes}; manual override from original Israel budget file".strip("; ").strip()

        israel_amounts = pd.to_numeric(out["amount_local"], errors="coerce")
        israel_unit = out["unit"].fillna("").astype(str).str.strip().str.lower()
        israel_thousand = israel_amounts.notna() & israel_unit.eq("thousand")
        if israel_thousand.any():
            out.loc[israel_thousand, "amount_local"] = israel_amounts.loc[israel_thousand] * 1000.0
            out.loc[israel_thousand, "unit"] = out.loc[israel_thousand].apply(
                lambda r: _base_output_unit(r.get("currency"), r.get("unit")),
                axis=1,
            )

    if country == "Hungary":
        for year, overrides in _HUNGARY_VERIFIED_OVERRIDES.items():
            for canonical_name, (amount_local, page_number, currency, source_file) in overrides.items():
                mask = (out["year"] == year) & (out["canonical_name"] == canonical_name)
                if not mask.any():
                    ref_row = out[out["canonical_name"] == canonical_name]
                    if ref_row.empty:
                        continue
                    new_row = ref_row.iloc[0].copy()
                    new_row["year"] = year
                    new_row["amount_local"] = float(amount_local)
                    new_row["unit"] = "forint"
                    new_row["currency"] = currency
                    new_row["item_type"] = "verified_override"
                    if canonical_name == "Hungarian Academy of Sciences (MTA)":
                        new_row["line_description_en"] = "Verified MTA chapter total from original Hungary budget PDF"
                        new_row["series_notes"] = "manual override from original Hungary budget file (MTA chapter total)"
                    elif canonical_name == "MTA Library and Information Centre":
                        new_row["line_description_en"] = "Verified MTA Library support line from original Hungary budget text"
                        new_row["series_notes"] = "manual override from original Hungary budget file (library support provision)"
                    elif canonical_name in {"Eötvös Loránd Research Network", "Hungarian Research Network"}:
                        new_row["line_description_en"] = "Verified chapter total for Hungary research network from original budget table"
                        new_row["series_notes"] = "manual override from original Hungary budget file (research-network chapter total)"
                    elif canonical_name == "INTERREG IVC Programme":
                        new_row["line_description_en"] = "Verified INTERREG IVC programme appropriation from original Hungary budget table"
                        new_row["series_notes"] = "manual override from original Hungary budget file (INTERREG IVC row)"
                    else:
                        new_row["line_description_en"] = "Verified R&D fund total from original Hungary budget table"
                        new_row["series_notes"] = "manual override from original Hungary budget file (R&D fund total)"
                    new_row["source_file"] = source_file
                    new_row["page_number"] = str(page_number)
                    out = pd.concat([out, new_row.to_frame().T], ignore_index=True)
                    continue

                target_idx = out.index[mask][0]
                out.loc[
                    mask,
                    ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
                ] = [None, None, None, None, None, None, None]
                out.at[target_idx, "amount_local"] = float(amount_local)
                out.at[target_idx, "unit"] = "forint"
                out.at[target_idx, "currency"] = currency
                out.at[target_idx, "item_type"] = "verified_override"
                out.at[target_idx, "source_file"] = source_file
                out.at[target_idx, "page_number"] = page_number
                if canonical_name == "Hungarian Academy of Sciences (MTA)":
                    desc = "Verified MTA chapter total from original Hungary budget PDF"
                    note = "manual override from original Hungary budget file (MTA chapter total)"
                elif canonical_name == "MTA Library and Information Centre":
                    desc = "Verified MTA Library support line from original Hungary budget text"
                    note = "manual override from original Hungary budget file (library support provision)"
                elif canonical_name in {"Eötvös Loránd Research Network", "Hungarian Research Network"}:
                    desc = "Verified chapter total for Hungary research network from original budget table"
                    note = "manual override from original Hungary budget file (research-network chapter total)"
                elif canonical_name == "INTERREG IVC Programme":
                    desc = "Verified INTERREG IVC programme appropriation from original Hungary budget table"
                    note = "manual override from original Hungary budget file (INTERREG IVC row)"
                else:
                    desc = "Verified R&D fund total from original Hungary budget table"
                    note = "manual override from original Hungary budget file (R&D fund total)"
                out.at[target_idx, "line_description_en"] = desc
                notes = str(out.at[target_idx, "series_notes"] or "").strip()
                out.at[target_idx, "series_notes"] = f"{notes}; {note}".strip("; ").strip()

    if country == "Korea":
        for year, canonical_name in _KOREA_VERIFIED_DROPS:
            mask = out["canonical_name"].eq(canonical_name) & pd.to_numeric(out["year"], errors="coerce").eq(year)
            if not mask.any():
                continue
            out.loc[
                mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Korea source audit: surviving match was a noisy fragment or non-comparable summary row".strip("; ").strip()
            )

        for year, overrides in _KOREA_VERIFIED_OVERRIDES.items():
            for canonical_name, (amount_local, page_number, currency, source_file) in overrides.items():
                mask = out["canonical_name"].eq(canonical_name) & pd.to_numeric(out["year"], errors="coerce").eq(year)
                if not mask.any():
                    ref_row = out[out["canonical_name"] == canonical_name]
                    if ref_row.empty:
                        continue
                    new_row = ref_row.iloc[0].copy()
                    new_row["year"] = year
                    new_row["amount_local"] = float(amount_local)
                    new_row["unit"] = "thousand"
                    new_row["currency"] = currency
                    new_row["item_type"] = "verified_override"
                    if canonical_name == "Ministry of Science and ICT (Korea)":
                        new_row["line_description_en"] = "Verified MSIT / science-technology-communication budget total from original Korea budget brief"
                        note = "manual override from original Korea budget brief (MSIT total)"
                    elif canonical_name == "National R&D Programmes (Korea)":
                        new_row["line_description_en"] = "Verified Korea annual total R&D budget from original budget brief / fiscal-plan table"
                        note = "manual override from original Korea budget brief (annual total R&D)"
                    else:
                        new_row["line_description_en"] = "Verified strategic-technology R&D subtotal from original Korea budget brief"
                        note = "manual override from original Korea budget brief (strategic-technology subtotal)"
                    base_note = _KOREA_CANONICAL_NOTES.get(canonical_name, "").strip()
                    new_row["series_notes"] = "; ".join(part for part in [base_note, note] if part)
                    new_row["source_file"] = source_file
                    new_row["page_number"] = page_number
                    out = pd.concat([out, new_row.to_frame().T], ignore_index=True)
                    continue

                target_idx = out.index[mask][0]
                out.loc[
                    mask,
                    ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
                ] = [None, None, None, None, None, None, None]
                out.at[target_idx, "amount_local"] = float(amount_local)
                out.at[target_idx, "unit"] = "thousand"
                out.at[target_idx, "currency"] = currency
                out.at[target_idx, "item_type"] = "verified_override"
                out.at[target_idx, "source_file"] = source_file
                out.at[target_idx, "page_number"] = str(page_number)
                if canonical_name == "Ministry of Science and ICT (Korea)":
                    desc = "Verified MSIT / science-technology-communication budget total from original Korea budget brief"
                    note = "manual override from original Korea budget brief (MSIT total)"
                elif canonical_name == "National R&D Programmes (Korea)":
                    desc = "Verified Korea annual total R&D budget from original budget brief / fiscal-plan table"
                    note = "manual override from original Korea budget brief (annual total R&D)"
                else:
                    desc = "Verified strategic-technology R&D subtotal from original Korea budget brief"
                    note = "manual override from original Korea budget brief (strategic-technology subtotal)"
                out.at[target_idx, "line_description_en"] = desc
                base_note = _KOREA_CANONICAL_NOTES.get(canonical_name, "").strip()
                out.at[target_idx, "series_notes"] = "; ".join(part for part in [base_note, note] if part)

        out["_kr_has_amount"] = out["amount_local"].notna().astype(int)
        out["_kr_is_override"] = (out["item_type"] == "verified_override").astype(int)
        out = (
            out.sort_values(
                ["canonical_name", "year", "_kr_has_amount", "_kr_is_override"],
                ascending=[True, True, False, False],
                kind="stable",
            )
            .drop_duplicates(["canonical_name", "year"], keep="first")
            .drop(columns=["_kr_has_amount", "_kr_is_override"])
            .reset_index(drop=True)
        )
        amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
        unit_norm = out["unit"].fillna("").astype(str).str.strip().str.lower()
        thousand_mask = amount_num.notna() & unit_norm.eq("thousand")
        if thousand_mask.any():
            out.loc[thousand_mask, "amount_local"] = amount_num.loc[thousand_mask] * 1000.0
            out.loc[thousand_mask, "unit"] = out.loc[thousand_mask].apply(
                lambda r: _base_output_unit(r.get("currency"), r.get("unit")),
                axis=1,
            )

    if country == "Australia":
        amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
        unit_norm = out["unit"].fillna("").astype(str).str.lower().str.strip()
        thousand_mask = amount_num.notna() & unit_norm.eq("thousand")
        if thousand_mask.any():
            out.loc[thousand_mask, "amount_local"] = amount_num.loc[thousand_mask] * 1000.0
            out.loc[thousand_mask, "unit"] = "dollar"

        improved_name = "Improved Health and Medical Knowledge Programme"
        nhmrc_name = "NHMRC / Medical Research Fund"
        improved_mask = out["canonical_name"].eq(improved_name)
        if improved_mask.any():
            improved_years = set(
                out.loc[
                    improved_mask & pd.to_numeric(out["amount_local"], errors="coerce").notna(),
                    "year",
                ].tolist()
            )
            nhmrc_years = set(
                out.loc[
                    out["canonical_name"].eq(nhmrc_name)
                    & pd.to_numeric(out["amount_local"], errors="coerce").notna(),
                    "year",
                ].tolist()
            )
            drop_improved = improved_mask & out["year"].isin(improved_years & nhmrc_years)
            if drop_improved.any():
                out.loc[
                    drop_improved,
                    ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
                ] = [None, None, None, None, None, None, None]

            remap_improved = improved_mask & out["year"].isin(improved_years - nhmrc_years)
            if remap_improved.any():
                out.loc[remap_improved, "canonical_name"] = nhmrc_name
                notes = out.loc[remap_improved, "series_notes"].fillna("").astype(str).str.strip()
                out.loc[remap_improved, "series_notes"] = notes.apply(
                    lambda s: f"{s}; mapped from Improved Health and Medical Knowledge Programme".strip("; ").strip()
                )

    if country == "Colombia":
        canonical = out["canonical_name"].fillna("").astype(str)
        year_num = pd.to_numeric(out["year"], errors="coerce")

        # Audit against the original Colombia source files shows these rows are
        # traceable to real documents, but not defensible as final R&D
        # observations:
        #   - pre-2018 SENA rows are institution-wide budget totals rather than
        #     explicit R&D/innovation appropriations;
        #   - 2005 National Environmental Fund is a broad investment-fund total.
        weak_mask = (
            (canonical.eq("SENA — R&D and Innovation (Servicio Nacional de Aprendizaje)") & year_num.ne(2018))
            | canonical.eq("National Environmental Fund")
        )
        if weak_mask.any():
            out.loc[
                weak_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[weak_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[weak_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after source audit: traceable but not a defensible final R&D appropriation".strip("; ").strip()
            )

    if country == "Poland":
        canonical = out["canonical_name"].fillna("").astype(str)
        year_num = pd.to_numeric(out["year"], errors="coerce")
        amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
        unit_norm = out["unit"].fillna("").astype(str).str.strip().str.lower()
        allowed_poland_canonicals = {
            "CERN (Polish contribution)",
            "KBN (Komitet Badań Naukowych)",
            "NCBiR (Narodowe Centrum Badań i Rozwoju)",
            "NCN (Narodowe Centrum Nauki)",
            "PAN (Polska Akademia Nauk)",
            "Polish Space Agency Activities",
            "Regional Initiative of Excellence",
            "Research Reactor MARIA Modernization Program",
            "Łukasiewicz Research Network",
        }
        section_amounts = {
            int(year): float(amount)
            for year, amount in out.loc[
                canonical.eq("Część 28 — Szkolnictwo wyższe i nauka") & amount_num.notna(),
                ["year", "amount_local"],
            ].itertuples(index=False, name=None)
            if pd.notna(year) and pd.notna(amount)
        }

        residual_thousand = amount_num.notna() & unit_norm.eq("thousand")
        if residual_thousand.any():
            out.loc[residual_thousand, "amount_local"] = amount_num.loc[residual_thousand] * 1000.0
            out.loc[residual_thousand, "unit"] = "zloty"
            amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
            unit_norm = out["unit"].fillna("").astype(str).str.strip().str.lower()

        residual_million = amount_num.notna() & unit_norm.eq("million")
        if residual_million.any():
            out.loc[residual_million, "amount_local"] = amount_num.loc[residual_million] * 1_000_000.0
            out.loc[residual_million, "unit"] = "zloty"
            amount_num = pd.to_numeric(out["amount_local"], errors="coerce")

        generic_poland_mask = ~canonical.isin(allowed_poland_canonicals)
        if generic_poland_mask.any():
            out.loc[
                generic_poland_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[generic_poland_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[generic_poland_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Poland source audit: final panel restricted to audited institution/program canonicals".strip("; ").strip()
            )

        broad_poland_mask = canonical.isin(
            {
                "Część 28 — Szkolnictwo wyższe i nauka",
                "MNiSW / MEiN (Ministry of Science and Higher Education)",
            }
        )
        if broad_poland_mask.any():
            out.loc[
                broad_poland_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[broad_poland_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[broad_poland_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Poland source audit: broad ministry/part aggregate, not a defensible institutional series".strip("; ").strip()
            )

        amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
        university_dup_mask = canonical.eq("Subwencje dla uczelni (university block grants, Część 28)") & (
            out["item_type"].fillna("").astype(str).str.lower().eq("section_total")
            | out.apply(
                lambda r: (
                    pd.notna(r.get("amount_local"))
                    and pd.notna(r.get("year"))
                    and section_amounts.get(int(r["year"])) == float(r["amount_local"])
                ),
                axis=1,
            )
        )
        if university_dup_mask.any():
            out.loc[
                university_dup_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[university_dup_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[university_dup_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Poland source audit: duplicate of broad Part 28 aggregate, not a distinct university block-grant line".strip("; ").strip()
            )

        amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
        suspicious_round_mask = (
            canonical.isin(
                {
                    "Dział 740 — Działalność badawcza i rozwojowa",
                    "NCN (Narodowe Centrum Nauki)",
                    "NCBiR (Narodowe Centrum Badań i Rozwoju)",
                    "PAN (Polska Akademia Nauk)",
                    "Subwencje dla uczelni (university block grants, Część 28)",
                }
            )
            & amount_num.isin(
                {
                    9_876_543_000.0,
                    12_345_678_000.0,
                    30_000_000_000.0,
                    50_000_000_000.0,
                    100_000_000_000.0,
                    200_000_000_000.0,
                    300_000_000_000.0,
                    500_000_000_000.0,
                    921_000_000_000.0,
                }
            )
        )
        if suspicious_round_mask.any():
            out.loc[
                suspicious_round_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[suspicious_round_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[suspicious_round_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Poland source audit: synthetic placeholder/summary value not supported by the original budget page".strip("; ").strip()
            )

        raw_poland = subset.copy()
        raw_poland["year"] = pd.to_numeric(raw_poland["year"], errors="coerce")

        def _poland_raw_pick(
            year: int,
            include_patterns: list[str],
            prefer_patterns: list[str],
            *,
            exclude_patterns: list[str] | None = None,
            max_amount: float | None = None,
        ) -> Optional[pd.Series]:
            work = raw_poland[raw_poland["year"].eq(year)].copy()
            if work.empty:
                return None

            text = (
                work.get("line_description_en", pd.Series("", index=work.index)).fillna("").astype(str)
                + " "
                + work.get("line_description", pd.Series("", index=work.index)).fillna("").astype(str)
                + " "
                + work.get("section_name_en", pd.Series("", index=work.index)).fillna("").astype(str)
                + " "
                + work.get("section_name", pd.Series("", index=work.index)).fillna("").astype(str)
            )
            include_mask = pd.Series(False, index=work.index)
            for pat in include_patterns:
                include_mask |= text.str.contains(pat, case=False, regex=True, na=False)
            work = work[include_mask].copy()
            if work.empty:
                return None

            if exclude_patterns:
                exclude_mask = pd.Series(False, index=work.index)
                for pat in exclude_patterns:
                    exclude_mask |= text.loc[work.index].str.contains(pat, case=False, regex=True, na=False)
                trimmed = work.loc[~exclude_mask].copy()
                if not trimmed.empty:
                    work = trimmed

            base_rows = work.apply(
                lambda r: _expand_to_base_unit(r.get("amount_local"), r.get("unit"), r.get("currency")),
                axis=1,
                result_type="expand",
            )
            work["_base_amount"] = pd.to_numeric(base_rows[0], errors="coerce")
            work["_base_unit"] = base_rows[1]
            work = work[work["_base_amount"].notna()].copy()
            if work.empty:
                return None
            if max_amount is not None:
                trimmed = work[work["_base_amount"] <= float(max_amount)].copy()
                if not trimmed.empty:
                    work = trimmed

            pref_text = text.loc[work.index]
            work["_prefer_score"] = 0
            for idx, pat in enumerate(prefer_patterns, start=1):
                work.loc[pref_text.str.contains(pat, case=False, regex=True, na=False), "_prefer_score"] += (10 - idx)

            item_rank = {"program_total": 3, "line_item": 2, "section_total": 1}
            work["_item_rank"] = work.get("item_type", pd.Series("", index=work.index)).fillna("").map(item_rank).fillna(0)
            work["_page_rank"] = pd.to_numeric(work.get("page_number"), errors="coerce").fillna(0)
            work = work.sort_values(
                ["_prefer_score", "_item_rank", "_base_amount", "_page_rank"],
                ascending=[False, False, False, False],
                kind="stable",
            )
            return work.iloc[0]

        poland_recover_specs = {
            "NCN (Narodowe Centrum Nauki)": {
                "years": range(2011, 2026),
                "include": [
                    r"activities of the national science centre",
                    r"\bnational science centre\b",
                    r"narodowe centrum nauki",
                    r"\bncn\b",
                ],
                "prefer": [
                    r"activities of the national science centre",
                    r"plan finansowy",
                    r"\bnational science centre\b$",
                ],
                "exclude": [
                    r"grants?\s+for\s+basic\s+research",
                    r"division 740.*national science centre",
                    r"środki\s+(?:przekazane|przyznane)\s+innym\s+podmiotom",
                ],
                "max_amount": 5_000_000_000.0,
            },
            "NCBiR (Narodowe Centrum Badań i Rozwoju)": {
                "years": range(2007, 2026),
                "include": [
                    r"activities of the national centre for research and development",
                    r"\bnational centre for research and development\b",
                    r"narodowe centrum badań i rozwoju",
                    r"narodowe centrum badan i rozwoju",
                    r"\bncbir\b|\bncbr\b",
                ],
                "prefer": [
                    r"activities of the national centre for research and development",
                    r"plan finansowy",
                    r"\bnational centre for research and development\b$",
                ],
                "exclude": [
                    r"grants?\s+for\s+applied\s+research",
                    r"division 740.*national centre for research and development",
                    r"środki\s+(?:przekazane|przyznane)\s+innym\s+podmiotom",
                ],
                "max_amount": 5_000_000_000.0,
            },
            "PAN (Polska Akademia Nauk)": {
                "years": range(1990, 2026),
                "include": [
                    r"polish academy of sciences",
                    r"polska akademia nauk",
                    r"\bpan\b",
                ],
                "prefer": [
                    r"total expenditures",
                    r"suma ogólna|suma ogolna",
                    r"polish academy of sciences$",
                ],
                "exclude": [
                    r"auxiliary scientific units",
                    r"remaining activities",
                    r"activities of the bodies and",
                ],
                "max_amount": 2_500_000_000.0,
            },
            "Dział 740 — Działalność badawcza i rozwojowa": {
                "years": range(1990, 2026),
                "include": [
                    r"division 740 research and development in science",
                    r"research and development activities",
                    r"dział 740",
                    r"dzial 740",
                ],
                "prefer": [
                    r"division 740 research and development in science",
                    r"research and development activities",
                    r"dział 740",
                ],
                "exclude": [
                    r"national science centre",
                    r"national centre for research and development",
                    r"grants?\s+for\s+basic\s+research",
                    r"grants?\s+for\s+applied\s+research",
                ],
                "max_amount": 5_000_000_000.0,
            },
        }

        for canonical_name, spec in poland_recover_specs.items():
            for year in spec["years"]:
                mask = canonical.eq(canonical_name) & year_num.eq(year)
                if not mask.any():
                    continue
                best = _poland_raw_pick(
                    int(year),
                    spec["include"],
                    spec["prefer"],
                    exclude_patterns=spec.get("exclude"),
                    max_amount=spec.get("max_amount"),
                )
                if best is None:
                    continue
                target_idx = out.index[mask][0]
                out.loc[
                    mask,
                    ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
                ] = [None, None, None, None, None, None, None]
                out.at[target_idx, "amount_local"] = float(best["_base_amount"])
                out.at[target_idx, "unit"] = best["_base_unit"]
                out.at[target_idx, "currency"] = best.get("currency")
                out.at[target_idx, "item_type"] = best.get("item_type")
                out.at[target_idx, "line_description_en"] = best.get("line_description_en")
                out.at[target_idx, "source_file"] = best.get("source_file")
                out.at[target_idx, "page_number"] = best.get("page_number")
                notes = str(out.at[target_idx, "series_notes"] or "").strip()
                out.at[target_idx, "series_notes"] = f"{notes}; recovered from Poland source-audited row".strip("; ").strip()

        amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
        poland_caps = {
            "NCN (Narodowe Centrum Nauki)": 5_000_000_000.0,
            "NCBiR (Narodowe Centrum Badań i Rozwoju)": 5_000_000_000.0,
            "PAN (Polska Akademia Nauk)": 2_500_000_000.0,
            "Dział 740 — Działalność badawcza i rozwojowa": 5_000_000_000.0,
        }
        for canonical_name, maximum in poland_caps.items():
            cap_mask = canonical.eq(canonical_name) & amount_num.notna() & amount_num.gt(float(maximum))
            if not cap_mask.any():
                continue
            out.loc[
                cap_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[cap_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[cap_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Poland source audit: exceeds plausible institutional ceiling and behaves like a summary/placeholder row".strip("; ").strip()
            )

        amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
        zero_or_low_mask = (
            (
                canonical.eq("NCBiR (Narodowe Centrum Badań i Rozwoju)")
                & amount_num.notna()
                & amount_num.lt(50_000_000.0)
            )
            | (
                canonical.eq("NCN (Narodowe Centrum Nauki)")
                & amount_num.notna()
                & amount_num.lt(50_000_000.0)
            )
            | (
                canonical.isin(
                    {
                        "Dział 740 — Działalność badawcza i rozwojowa",
                        "PAN (Polska Akademia Nauk)",
                    }
                )
                & amount_num.notna()
                & amount_num.le(0)
            )
        )
        if zero_or_low_mask.any():
            out.loc[
                zero_or_low_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[zero_or_low_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[zero_or_low_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Poland source audit: zero/near-zero survivor is not a defensible annual appropriation".strip("; ").strip()
            )

        poland_verified_overrides = {
            (1999, "KBN (Komitet Badań Naukowych)"): (
                2_742_811_000.0,
                "zloty",
                "PLN",
                "verified_override",
                "Research and development activities (main KBN budget section)",
                "1999 D1999017015402.pdf",
                "41.0",
                "overridden after Poland source audit with exact main budget-section appropriation",
            ),
            (2013, "NCBiR (Narodowe Centrum Badań i Rozwoju)"): (
                1_695_208_000.0,
                "zloty",
                "PLN",
                "verified_override",
                "Dotacje ogółem (financial plan, cash basis)",
                "2013 text.pdf",
                "356.0",
                "overridden after Poland source audit with exact financial-plan appropriation",
            ),
            (2019, "NCBiR (Narodowe Centrum Badań i Rozwoju)"): (
                1_408_735_000.0,
                "zloty",
                "PLN",
                "verified_override",
                "Dotacje ogółem (financial plan, cash basis)",
                "2019 text.pdf",
                "312.0",
                "overridden after Poland source audit with exact financial-plan appropriation",
            ),
            (2020, "NCBiR (Narodowe Centrum Badań i Rozwoju)"): (
                1_332_715_000.0,
                "zloty",
                "PLN",
                "verified_override",
                "Dotacje ogółem (financial plan, cash basis)",
                "2020 text.pdf",
                "292.0",
                "overridden after Poland source audit with exact financial-plan appropriation",
            ),
        }
        for (override_year, override_name), (
            override_amount,
            override_unit,
            override_currency,
            override_item_type,
            override_desc,
            override_file,
            override_page,
            override_note,
        ) in poland_verified_overrides.items():
            override_mask = canonical.eq(override_name) & year_num.eq(override_year)
            if not override_mask.any():
                continue
            target_idx = out.index[override_mask][0]
            out.loc[
                override_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            out.at[target_idx, "amount_local"] = float(override_amount)
            out.at[target_idx, "unit"] = override_unit
            out.at[target_idx, "currency"] = override_currency
            out.at[target_idx, "item_type"] = override_item_type
            out.at[target_idx, "line_description_en"] = override_desc
            out.at[target_idx, "source_file"] = override_file
            if pd.api.types.is_numeric_dtype(out["page_number"]):
                out.at[target_idx, "page_number"] = pd.to_numeric(override_page, errors="coerce")
            else:
                out.at[target_idx, "page_number"] = str(override_page)
            notes = str(out.at[target_idx, "series_notes"] or "").strip()
            out.at[target_idx, "series_notes"] = f"{notes}; {override_note}".strip("; ").strip()

        pan_bad_source_mask = canonical.eq("PAN (Polska Akademia Nauk)") & (
            year_num.isin([2014, 2015, 2017, 2019, 2020, 2021])
            | (
                pd.to_numeric(out.get("page_number"), errors="coerce").eq(615)
                & out["source_file"].fillna("").astype(str).str.contains(r"2021", case=False, regex=True, na=False)
            )
        )
        if pan_bad_source_mask.any():
            out.loc[
                pan_bad_source_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[pan_bad_source_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[pan_bad_source_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Poland source audit: source page belongs to a generic grant block or another agency, not a defensible PAN appropriation".strip("; ").strip()
            )

        amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
        kbn_old_currency_mask = canonical.eq("KBN (Komitet Badań Naukowych)") & year_num.eq(1991) & amount_num.notna()
        if kbn_old_currency_mask.any():
            out.loc[
                kbn_old_currency_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[kbn_old_currency_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[kbn_old_currency_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Poland source audit: 1991 KBN is a pre-redenomination old-zloty total that remains non-comparable under the current extracted series".strip("; ").strip()
            )

        kbn_low_or_aux_mask = canonical.eq("KBN (Komitet Badań Naukowych)") & amount_num.notna() & amount_num.lt(10_000_000.0)
        if kbn_low_or_aux_mask.any():
            out.loc[
                kbn_low_or_aux_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[kbn_low_or_aux_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[kbn_low_or_aux_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Poland source audit: low KBN survivor comes from an auxiliary-budget row, not the main annual appropriation".strip("; ").strip()
            )

        weak_traceability_mask = (
            (canonical.eq("CERN (Polish contribution)") & year_num.eq(2014))
            | (canonical.eq("PAN (Polska Akademia Nauk)") & year_num.eq(2018))
            | (canonical.eq("Regional Initiative of Excellence") & year_num.eq(2024))
        )
        if weak_traceability_mask.any():
            out.loc[
                weak_traceability_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[weak_traceability_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[weak_traceability_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Poland source audit: still traceability-weak or organizationally ambiguous for a real final series".strip("; ").strip()
            )

        lukasiewicz_level_mismatch_mask = canonical.eq("Łukasiewicz Research Network") & year_num.isin([2021, 2023, 2024])
        if lukasiewicz_level_mismatch_mask.any():
            out.loc[
                lukasiewicz_level_mismatch_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[lukasiewicz_level_mismatch_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[lukasiewicz_level_mismatch_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Poland source audit: Łukasiewicz survivors mix a network-wide activity row (2021) with centre-only or centre-plus-institutes rows (2023-2024), so the level is not comparable enough for the final panel".strip("; ").strip()
            )

        poland_verification_path = Path("Data/output/budget/Poland/poland_final_manual_verification.csv")
        if poland_verification_path.exists():
            try:
                verification_df = pd.read_csv(poland_verification_path)
            except Exception:
                verification_df = pd.DataFrame()
            if not verification_df.empty:
                verification_df = verification_df.copy()
                verification_df["year"] = pd.to_numeric(verification_df["year"], errors="coerce")
                verification_df["page_number"] = pd.to_numeric(verification_df["page_number"], errors="coerce")
                verification_df["canonical_name"] = verification_df["canonical_name"].fillna("").astype(str)
                verification_df["source_file"] = verification_df["source_file"].fillna("").astype(str)
                verification_df["verification_status"] = verification_df["verification_status"].fillna("").astype(str)
                strict_drop_statuses = {
                    "weak_text_trace",
                    "amount_only_same_page",
                    "heading_neighbor_amount_weak",
                    "heading_neighbor_amount_same_page",
                    "name_same_page_amount_weak",
                }
                strict_drop_df = verification_df[
                    verification_df["verification_status"].isin(strict_drop_statuses)
                ].dropna(subset=["year"])
                if not strict_drop_df.empty:
                    drop_keys = {
                        (
                            int(row["year"]),
                            str(row["canonical_name"]),
                            str(row["source_file"]),
                            pd.to_numeric(row["page_number"], errors="coerce"),
                        )
                        for _, row in strict_drop_df.iterrows()
                    }
                    out_page_num = pd.to_numeric(out.get("page_number"), errors="coerce")
                    strict_trace_mask = pd.Series(False, index=out.index)
                    for drop_year, drop_name, drop_file, drop_page in drop_keys:
                        strict_trace_mask = strict_trace_mask | (
                            canonical.eq(drop_name)
                            & year_num.eq(drop_year)
                            & out["source_file"].fillna("").astype(str).eq(drop_file)
                            & out_page_num.eq(drop_page)
                        )
                    if strict_trace_mask.any():
                        out.loc[
                            strict_trace_mask,
                            ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
                        ] = [None, None, None, None, None, None, None]
                        notes = out.loc[strict_trace_mask, "series_notes"].fillna("").astype(str).str.strip()
                        out.loc[strict_trace_mask, "series_notes"] = notes.apply(
                            lambda s: f"{s}; dropped after Poland final manual verification: page-level trace is too weak for a strict final panel".strip("; ").strip()
                        )

        canonical = out["canonical_name"].fillna("").astype(str)
        final_generic_poland_mask = ~canonical.isin(allowed_poland_canonicals)
        if final_generic_poland_mask.any():
            out.loc[
                final_generic_poland_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[final_generic_poland_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[final_generic_poland_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; final Poland panel excludes non-audited generic or aggregate canonicals".strip("; ").strip()
            )

        out = out[out["canonical_name"].isin(allowed_poland_canonicals)].copy()

    if country == "Slovenia":
        canonical = out["canonical_name"].fillna("").astype(str)
        year_num = pd.to_numeric(out["year"], errors="coerce")
        amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
        line_text = out["line_description_en"].fillna("").astype(str)
        source_text = out["source_file"].fillna("").astype(str)
        hardcoded_names = {agency["canonical_name"] for agency in CANONICAL_AGENCIES.get("Slovenia", [])}
        ministry_names = {
            "Ministrstvo za visoko šolstvo, znanost in tehnologijo (MVZT)",
            "Ministrstvo za izobraževanje, znanost in šport (MIZŠ)",
        }
        final_panel_names = hardcoded_names - ministry_names

        # Keep discovery visible in slovenia_discovery_review.csv, but keep the
        # final canonical panel restricted to audited canonicals until more
        # institutions are verified against the original PDFs.
        discovered_generic_mask = ~canonical.isin(hardcoded_names)
        if discovered_generic_mask.any():
            out.loc[
                discovered_generic_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[discovered_generic_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[discovered_generic_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; retained in discovery review but excluded from final Slovenia panel pending source audit".strip("; ").strip()
            )

        # Ministry-wide totals are traceable, but they mix incompatible levels
        # with agency/programme rows and visually dominate the time series.
        ministry_mask = canonical.isin(ministry_names)
        if ministry_mask.any():
            out.loc[
                ministry_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[ministry_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[ministry_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Slovenia source audit: ministry total is traceable but not comparable with agency/programme appropriations".strip("; ").strip()
            )

        # 2024-2025 ARRS survivors are not the core ARRS operating grant; they
        # are PPUD sub-lines inside targeted agricultural projects.
        aris_subline_mask = (
            canonical.eq("ARRS — Agencija za raziskovalno dejavnost Republike Slovenije")
            & year_num.isin([2024, 2025])
            & out["item_type"].fillna("").astype(str).ne("verified_override")
        )
        if aris_subline_mask.any():
            out.loc[
                aris_subline_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[aris_subline_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[aris_subline_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Slovenia source audit: 2024-2025 survivor is a PPUD sub-line from agricultural targeted research projects, not the ARRS base appropriation".strip("; ").strip()
            )

        # ARRS should reflect the agency's operating appropriation, not
        # equipment, international-cooperation bundles, or generic project lines.
        aris_mask = canonical.eq("ARRS — Agencija za raziskovalno dejavnost Republike Slovenije") & amount_num.notna()
        aris_operation_mask = line_text.str.contains(
            r"delovanje arrs|operation of arrs|operation of the slovenian research agency|delovanje aris|operation of aris",
            case=False,
            regex=True,
            na=False,
        ) | out["item_type"].fillna("").astype(str).eq("verified_override")
        bad_aris_mask = aris_mask & ~aris_operation_mask
        if bad_aris_mask.any():
            out.loc[
                bad_aris_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[bad_aris_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[bad_aris_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Slovenia source audit: ARRS series keeps only defendable operating appropriations (Delovanje ARRS/ARIS)".strip("; ").strip()
            )

        # Programme 0503 is often polluted by SAZU support activity,
        # electronic-communications lines, or inflated project/package totals.
        prog0503_mask = canonical.eq("Programme 0503 — Mladi raziskovalci / Človeški viri v podporo znanosti") & amount_num.notna()
        bad_0503_text = line_text.str.contains(
            r"support(?:ing)? activit|electronic communications|development and promotion in the field of electronic communications|support activit|programs in support of science|human resources in support of science$|0503 human resources in support of science$",
            case=False,
            regex=True,
            na=False,
        )
        bad_0503_source = out["source_file"].fillna("").astype(str).eq("2004 2005 u2013102.pdf")
        bad_0503_size = (
            ((out["currency"].fillna("") == "EUR") & amount_num.gt(10_000_000.0))
            | ((out["currency"].fillna("") == "SIT") & amount_num.gt(500_000_000.0))
        )
        bad_0503_tiny = (
            (out["currency"].fillna("") == "SIT")
            & amount_num.gt(0.0)
            & amount_num.lt(1_000_000.0)
        )
        bad_0503_zero = amount_num.eq(0.0) & out["item_type"].fillna("").astype(str).ne("verified_override")
        bad_0503_mask = prog0503_mask & (
            bad_0503_text | bad_0503_source | bad_0503_size | bad_0503_tiny | bad_0503_zero
        )
        if bad_0503_mask.any():
            out.loc[
                bad_0503_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[bad_0503_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[bad_0503_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Slovenia source audit: Programme 0503 survivor is a misfiled source, SAZU/generic summary line, electronic-communications line, or implausible artefact".strip("; ").strip()
            )

        # Some early 0502 survivors are clearly tiny sub-lines (e.g. forestry,
        # space projects) or OCR-zero artefacts rather than the full science
        # programme total.
        prog0502_mask = canonical.eq("Programme 0502 — Znanstveno raziskovalna dejavnost") & amount_num.notna()
        bad_0502_tiny_sit = (
            prog0502_mask
            & out["currency"].fillna("").eq("SIT")
            & amount_num.ge(0.0)
            & amount_num.lt(1_000_000_000.0)
            & out["item_type"].fillna("").astype(str).ne("verified_override")
        )
        if bad_0502_tiny_sit.any():
            out.loc[
                bad_0502_tiny_sit,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[bad_0502_tiny_sit, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[bad_0502_tiny_sit, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Slovenia source audit: early 0502 survivor is a tiny sub-line or OCR-zero artefact rather than the full science programme total".strip("; ").strip()
            )

        # SAZU should keep only rows explicitly traceable to the institution,
        # not generic science-system packages or infrastructure bundles that
        # happen to co-occur with academy keywords elsewhere in the document.
        sazu_mask = canonical.eq("SAZU — Slovenska akademija znanosti in umetnosti") & amount_num.notna()
        sazu_explicit_mask = line_text.str.contains(
            r"\bsazu\b|academy of sciences and arts|scientific research center sazu|scientific research center sat|scientific research activity of the slovenian academy|support activities - sazu|total for sazu|slovenian academy of sciences and arts",
            case=False,
            regex=True,
            na=False,
        ) | out["item_type"].fillna("").astype(str).eq("verified_override")
        bad_sazu_source = source_text.eq("2004 2005 u2013102.pdf")
        bad_sazu_size = (
            ((out["currency"].fillna("") == "EUR") & amount_num.gt(20_000_000.0))
            | ((out["currency"].fillna("") == "SIT") & amount_num.gt(2_000_000_000.0))
        )
        bad_sazu_mask = sazu_mask & (~sazu_explicit_mask | bad_sazu_source | bad_sazu_size)
        if bad_sazu_mask.any():
            out.loc[
                bad_sazu_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[bad_sazu_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[bad_sazu_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Slovenia source audit: SAZU series keeps only institution-explicit rows traceable to the original budget line".strip("; ").strip()
            )

        # Slovenia outputs should be in base units in the final series. Convert
        # surviving SIT rows that already passed through base-unit expansion to
        # plain unit labels, and flatten cosmetic 'euro' labels likewise.
        sit_thousand_mask = canonical.isin(hardcoded_names) & amount_num.notna() & out["currency"].fillna("").eq("SIT") & out["unit"].fillna("").str.lower().eq("thousand")
        if sit_thousand_mask.any():
            out.loc[sit_thousand_mask, "unit"] = "unit"

        euro_label_mask = canonical.isin(hardcoded_names) & amount_num.notna() & out["currency"].fillna("").eq("EUR") & out["unit"].fillna("").str.lower().eq("euro")
        if euro_label_mask.any():
            out.loc[euro_label_mask, "unit"] = "unit"

        out = out[out["canonical_name"].isin(final_panel_names)].copy()

    if country == "Luxembourg":
        canonical = out["canonical_name"].fillna("").astype(str)
        year_num = pd.to_numeric(out["year"], errors="coerce")
        amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
        currency_lu = out["currency"].fillna("").astype(str).str.upper()
        unit_lu = out["unit"].fillna("").astype(str).str.lower()
        line_text = out["line_description_en"].fillna("").astype(str) + " " + out.get("line_description", pd.Series("", index=out.index)).fillna("").astype(str)
        hardcoded_names = {agency["canonical_name"] for agency in CANONICAL_AGENCIES.get("Luxembourg", [])}

        thousand_mask = amount_num.notna() & unit_lu.eq("thousand") & currency_lu.isin(["EUR", "LUF"])
        if thousand_mask.any():
            out.loc[thousand_mask, "amount_local"] = amount_num.loc[thousand_mask] * 1000.0
            out.loc[thousand_mask, "unit"] = out.loc[thousand_mask, "currency"].map({"EUR": "euro", "LUF": "franc"}).fillna("unit")
            amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
            unit_lu = out["unit"].fillna("").astype(str).str.lower()

        discovered_generic_mask = ~canonical.isin(hardcoded_names)
        if discovered_generic_mask.any():
            out.loc[
                discovered_generic_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[discovered_generic_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[discovered_generic_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; retained in discovery review but excluded from final Luxembourg panel pending source audit".strip("; ").strip()
            )

        euro_unit_mask = amount_num.notna() & currency_lu.eq("EUR") & unit_lu.eq("unit")
        if euro_unit_mask.any():
            out.loc[euro_unit_mask, "unit"] = "euro"
        luf_unit_mask = amount_num.notna() & currency_lu.eq("LUF") & unit_lu.eq("unit")
        if luf_unit_mask.any():
            out.loc[luf_unit_mask, "unit"] = "franc"

        ministry_mask = canonical.eq("Ministère de l'Enseignement Supérieur et de la Recherche (Luxembourg)")
        bad_ministry_text = ministry_mask & ~line_text.str.contains(
            r"higher education|enseignement sup[ée]rieur|research|recherche",
            case=False,
            regex=True,
            na=False,
        )
        if bad_ministry_text.any():
            out.loc[
                bad_ministry_text,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[bad_ministry_text, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[bad_ministry_text, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Luxembourg source audit: ministry survivor does not explicitly describe higher-education/research appropriations".strip("; ").strip()
            )

        late_ministry_mask = ministry_mask & year_num.ge(1999)
        if late_ministry_mask.any():
            out.loc[
                late_ministry_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[late_ministry_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[late_ministry_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Luxembourg source audit: post-1999 ministry totals mix incompatible levels and duplicate institution-level appropriations".strip("; ").strip()
            )

        weak_ministry_item_mask = ministry_mask & ~out["item_type"].fillna("").astype(str).isin(["section_total", "program_total", "verified_override"])
        if weak_ministry_item_mask.any():
            out.loc[
                weak_ministry_item_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[weak_ministry_item_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[weak_ministry_item_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Luxembourg source audit: ministry series keeps only section/program totals, not embedded line items".strip("; ").strip()
            )

        early_ministry_total_mask = (
            ministry_mask
            & year_num.le(1998)
            & out["item_type"].fillna("").astype(str).isin(["section_total", "program_total"])
        )
        if early_ministry_total_mask.any():
            out.loc[
                early_ministry_total_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[early_ministry_total_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[early_ministry_total_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Luxembourg source audit: early ministry totals (1975-1998) are not yet page-verified and several traced pages resolve to non-matching education/health sections, so the aggregate series is excluded until rebuilt from original files".strip("; ").strip()
            )

        wrong_total_mask = line_text.str.contains(
            r"transport|housing|logement|recettes pour ordre|revenues for order|d[ée]penses pour ordre|expenses for order",
            case=False,
            regex=True,
            na=False,
        )
        if wrong_total_mask.any():
            out.loc[
                wrong_total_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[wrong_total_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[wrong_total_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Luxembourg source audit: cross-ministry or ordre total misassigned to the research panel".strip("; ").strip()
            )

        unilu_mask = canonical.eq("Université du Luxembourg") & amount_num.notna()
        weak_unilu_mask = unilu_mask & (
            amount_num.lt(5_000_000.0)
            | line_text.str.contains(
                r"cooperation agreement|water management|salaries of civil servants|collaboration with the university|research programs and projects undertaken in collaboration",
                case=False,
                regex=True,
                na=False,
            )
        ) & out["item_type"].fillna("").astype(str).ne("verified_override")
        if weak_unilu_mask.any():
            out.loc[
                weak_unilu_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[weak_unilu_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[weak_unilu_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Luxembourg source audit: UniLu survivor is a small thematic sub-line or administrative fragment, not the institution's core appropriation".strip("; ").strip()
            )

        list_mask = canonical.eq("LIST / CRP Henri Tudor (Luxembourg)") & amount_num.notna()
        weak_list_mask = list_mask & (
            amount_num.lt(5_000_000.0)
            | line_text.str.contains(
                r"management of the network|compensation for third-party services|research projects",
                case=False,
                regex=True,
                na=False,
            )
        ) & out["item_type"].fillna("").astype(str).ne("verified_override")
        if weak_list_mask.any():
            out.loc[
                weak_list_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[weak_list_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[weak_list_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Luxembourg source audit: LIST/CRP Henri Tudor survivor is a targeted project or service-compensation sub-line, not the institution's core appropriation".strip("; ").strip()
            )

        preverified_list_mask = (
            canonical.eq("LIST / CRP Henri Tudor (Luxembourg)")
            & year_num.isin([1997, 2006, 2007])
            & out["item_type"].fillna("").astype(str).ne("verified_override")
        )
        if preverified_list_mask.any():
            out.loc[
                preverified_list_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[preverified_list_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[preverified_list_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Luxembourg source audit: early LIST survivors (1997, 2006-2007) were traced to non-matching pages and are excluded until explicitly verified".strip("; ").strip()
            )

        lippmann_2006_mask = (
            canonical.eq("CRP Gabriel Lippmann (Luxembourg)")
            & year_num.eq(2006)
            & out["item_type"].fillna("").astype(str).ne("verified_override")
        )
        if lippmann_2006_mask.any():
            out.loc[
                lippmann_2006_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[lippmann_2006_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[lippmann_2006_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Luxembourg source audit: 2006 CRP Gabriel Lippmann survivor was traced to a non-matching source page and is excluded until explicitly verified".strip("; ").strip()
            )

        out = out[pd.to_numeric(out["amount_local"], errors="coerce").notna()].copy()
        out = out[out["canonical_name"].isin(hardcoded_names)].copy()

    if country == "Czech Republic":
        canonical = out["canonical_name"].fillna("").astype(str)
        year_num = pd.to_numeric(out["year"], errors="coerce")
        amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
        source_text = out["source_file"].fillna("").astype(str)
        hardcoded_names = {agency["canonical_name"] for agency in CANONICAL_AGENCIES.get("Czech Republic", [])}

        # The 1993-labelled file tied to Act 434/2024 is a known source anomaly
        # in the Czech corpus. Keep it visible in audit inputs, but do not let it
        # manufacture a fake 1993 baseline in the final canonical panel.
        misfiled_1993_mask = (
            year_num.eq(1993)
            & source_text.str.contains(r"434-2024|2025-12-20", case=False, regex=True, na=False)
        )
        if misfiled_1993_mask.any():
            out.loc[
                misfiled_1993_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[misfiled_1993_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[misfiled_1993_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Czech source audit: 1993-labelled source is a misfiled 2024/2025 law artefact".strip("; ").strip()
            )

        # Early Czech pipeline rows mix some plausible million-CZK appropriations
        # with clearly inflated summary/OCR artefacts. Drop only the survivors
        # whose expanded CZK amounts are far beyond a defensible institutional
        # series, rather than trying to guess a corrective divisor.
        inflated_science_mask = (
            canonical.isin(
                {
                    "Akademie věd České republiky (AV ČR)",
                    "Grantová agentura České republiky (GA ČR)",
                    "Technologická agentura České republiky (TA ČR)",
                }
            )
            & amount_num.gt(20_000_000_000.0)
        )
        inflated_ministry_mask = (
            canonical.eq("Ministerstvo školství, mládeže a tělovýchovy (MŠMT)")
            & amount_num.gt(200_000_000_000.0)
        )
        inflated_direct_rd_mask = (
            canonical.eq("Výzkum a vývoj (R&D appropriations)")
            & amount_num.gt(100_000_000_000.0)
        )
        czech_inflated_mask = inflated_science_mask | inflated_ministry_mask | inflated_direct_rd_mask
        if czech_inflated_mask.any():
            out.loc[
                czech_inflated_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[czech_inflated_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[czech_inflated_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Czech source audit: implausibly inflated summary/unit artefact".strip("; ").strip()
            )

        # Keep Czech agency discovery for audit/review, but do not let generic
        # programme labels auto-enter the final panel. The reliable final series
        # should stay institutional: GA CR, TA CR, AV CR.
        discovered_generic_mask = ~canonical.isin(hardcoded_names)
        if discovered_generic_mask.any():
            out.loc[
                discovered_generic_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[discovered_generic_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[discovered_generic_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Czech source audit: discovery candidate retained for review but excluded from final institutional panel".strip("; ").strip()
            )

        # Zero rows in late Czech budget laws are metadata placeholders for
        # aggregate RDI clauses, not defensible appropriations for the agencies.
        zero_or_negative_mask = canonical.isin(hardcoded_names) & amount_num.le(0)
        if zero_or_negative_mask.any():
            out.loc[
                zero_or_negative_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[zero_or_negative_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[zero_or_negative_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Czech source audit: zero-value placeholder is not a usable agency appropriation".strip("; ").strip()
            )

        # 2004 GA CR = 1,000,000 CZK is far below the surrounding series and is
        # best treated as a parsing/unit artefact until a better source row is
        # recovered.
        implausibly_low_ga_mask = (
            canonical.eq("Grantová agentura České republiky (GA ČR)")
            & amount_num.notna()
            & amount_num.lt(50_000_000.0)
        )
        if implausibly_low_ga_mask.any():
            out.loc[
                implausibly_low_ga_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[implausibly_low_ga_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[implausibly_low_ga_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Czech source audit: implausibly low against adjacent GA CR appropriations".strip("; ").strip()
            )

        ambiguous_ga_2000_mask = (
            canonical.eq("Grantová agentura České republiky (GA ČR)")
            & year_num.eq(2000)
            & amount_num.notna()
        )
        if ambiguous_ga_2000_mask.any():
            out.loc[
                ambiguous_ga_2000_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[ambiguous_ga_2000_mask, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[ambiguous_ga_2000_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Czech source audit: 2000 GA CR has conflicting candidate amounts in the same source and needs manual recovery".strip("; ").strip()
            )

        # Early Czech budget annexes (1997-2000) contain a small set of agency
        # rows where the extracted numeric value is plausible only if treated as
        # already being in base CZK rather than "thousand CZK". Recover only the
        # rows with a stable institutional owner and a coherent adjacent-series
        # trajectory.
        czech_verified_overrides = {
            (1997, "Akademie věd České republiky (AV ČR)"): (
                300_000_000.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Funding for research and development",
                "1997 Zak_1996-315_Prilohy-k-zakonu-c-3151996-Sb.pdf",
                14,
            ),
            (1998, "Akademie věd České republiky (AV ČR)"): (
                2_147_124_000.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "1998 Zak_1997-348_Prilohy-k-zakonu-c-3481997-Sb.pdf",
                24,
            ),
            (1999, "Akademie věd České republiky (AV ČR)"): (
                2_410_327_000.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures on research and development",
                "1999 Zak_1999-022_Prilohy-k-zakonu-c-221999-Sb.pdf",
                24,
            ),
            (2000, "Akademie věd České republiky (AV ČR)"): (
                2_149_521_000.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures on research and development",
                "2000 Zak_2000-058_Prilohy-k-zakonu-c-582000-Sb.pdf",
                24,
            ),
            (2003, "Akademie věd České republiky (AV ČR)"): (
                3_651_134_000.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Akademie věd České republiky",
                "2003 czech_budget_annexes_CONTENT_2003.docx",
                2,
            ),
            (2004, "Akademie věd České republiky (AV ČR)"): (
                4_033_338_000.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Akademie věd České republiky",
                "2004 czech_budget_annexes_CONTENT_2004.docx",
                2,
            ),
            (1997, "Grantová agentura České republiky (GA ČR)"): (
                500_000_000.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Support for research and development",
                "1997 Zak_1996-315_Prilohy-k-zakonu-c-3151996-Sb.pdf",
                7,
            ),
            (1999, "Grantová agentura České republiky (GA ČR)"): (
                965_414_000.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "1999 Zak_1999-022_Prilohy-k-zakonu-c-221999-Sb.pdf",
                12,
            ),
            (2016, "Grantová agentura České republiky (GA ČR)"): (
                3_833_110_000.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2016 Zak_2015-400_Zakon-o-statnim-rozpoctu-CR-na-rok-2016-Kompletni-vcetne-priloh.pdf",
                24,
            ),
            (2012, "Grantová agentura České republiky (GA ČR)"): (
                3_023_794_000.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2012 Zak_2011-455_Prilohy-k-zakonu-c-4552011-Sb-o-statnim-rozpoctu-Ceske-republiky-na-rok-2012.pdf",
                22,
            ),
            (2013, "Grantová agentura České republiky (GA ČR)"): (
                3_309_429_000.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2013 Zak_2012-504_Prilohy-k-zakonu-c-5042012-Sb-o-statnim-rozpoctu-Ceske-republiky-na-rok-2013.pdf",
                21,
            ),
            (2014, "Grantová agentura České republiky (GA ČR)"): (
                3_464_547_000.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2014 Zak_2013-475_Prilohy-k-zakonu-o-statnim-rozpoctu-Ceske-republiky-na-rok-2014.pdf",
                21,
            ),
            (2015, "Grantová agentura České republiky (GA ČR)"): (
                3_683_086_907.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2015 Zak_2014-345_Prilohy-k-zakonu-o-statnim-rozpoctu-Ceske-republiky-na-rok-2015.pdf",
                21,
            ),
            (2017, "Grantová agentura České republiky (GA ČR)"): (
                4_257_427_000.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2017 Zak_2016-457_Zakon-o-statnim-rozpoctu-Ceske-republiky-na-rok-2017.pdf",
                25,
            ),
            (2018, "Grantová agentura České republiky (GA ČR)"): (
                4_333_066_000.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2018 Zak_2017-474_Zakon-o-statnim-rozpoctu-Ceske-republiky-na-rok-2018.pdf",
                25,
            ),
            (2019, "Grantová agentura České republiky (GA ČR)"): (
                4_390_784_794.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2019 Zak_2018-336_Zakon-o-statnim-rozpoctu-Ceske-republiky-na-rok-2019.pdf",
                25,
            ),
            (2020, "Grantová agentura České republiky (GA ČR)"): (
                4_360_546_000.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2020 Zak_2019-355_Zakon-o-statnim-rozpoctu-Ceske-republiky-na-rok-2020.pdf",
                24,
            ),
            (2021, "Grantová agentura České republiky (GA ČR)"): (
                4_380_546_000.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2021 Zak_2020-600_Zakon-o-statni-rozpoctu-Ceske-republiky-na-rok-2021.pdf",
                23,
            ),
            (2024, "Grantová agentura České republiky (GA ČR)"): (
                4_600_000_000.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2024 2023-12-29_Zakon-o-statnim-rozpoctu-Ceske-republiky-na-rok-2024.pdf",
                24,
            ),
            (2016, "Akademie věd České republiky (AV ČR)"): (
                4_829_411_000.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2016 Zak_2015-400_Zakon-o-statnim-rozpoctu-CR-na-rok-2016-Kompletni-vcetne-priloh.pdf",
                47,
            ),
            (2012, "Akademie věd České republiky (AV ČR)"): (
                4_668_406_000.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2012 Zak_2011-455_Prilohy-k-zakonu-c-4552011-Sb-o-statnim-rozpoctu-Ceske-republiky-na-rok-2012.pdf",
                45,
            ),
            (2013, "Akademie věd České republiky (AV ČR)"): (
                4_449_192_000.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2013 Zak_2012-504_Prilohy-k-zakonu-c-5042012-Sb-o-statnim-rozpoctu-Ceske-republiky-na-rok-2013.pdf",
                44,
            ),
            (2014, "Akademie věd České republiky (AV ČR)"): (
                4_452_257_359.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2014 Zak_2013-475_Prilohy-k-zakonu-o-statnim-rozpoctu-Ceske-republiky-na-rok-2014.pdf",
                44,
            ),
            (2015, "Akademie věd České republiky (AV ČR)"): (
                4_522_355_819.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2015 Zak_2014-345_Prilohy-k-zakonu-o-statnim-rozpoctu-Ceske-republiky-na-rok-2015.pdf",
                44,
            ),
            (2017, "Akademie věd České republiky (AV ČR)"): (
                5_133_171_000.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2017 Zak_2016-457_Zakon-o-statnim-rozpoctu-Ceske-republiky-na-rok-2017.pdf",
                50,
            ),
            (2018, "Akademie věd České republiky (AV ČR)"): (
                5_684_692_000.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2018 Zak_2017-474_Zakon-o-statnim-rozpoctu-Ceske-republiky-na-rok-2018.pdf",
                50,
            ),
            (2019, "Akademie věd České republiky (AV ČR)"): (
                6_022_421_793.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2019 Zak_2018-336_Zakon-o-statnim-rozpoctu-Ceske-republiky-na-rok-2019.pdf",
                50,
            ),
            (2020, "Akademie věd České republiky (AV ČR)"): (
                6_563_390_450.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2020 Zak_2019-355_Zakon-o-statnim-rozpoctu-Ceske-republiky-na-rok-2020.pdf",
                49,
            ),
            (2021, "Akademie věd České republiky (AV ČR)"): (
                6_789_651_580.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2021 Zak_2020-600_Zakon-o-statni-rozpoctu-Ceske-republiky-na-rok-2021.pdf",
                48,
            ),
            (2022, "Akademie věd České republiky (AV ČR)"): (
                7_081_401_581.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2022 Zak_2022-057_Zakon-o-statnim-rozpoctu-Ceske-republiky-na-rok-2022.pdf",
                49,
            ),
            (2023, "Akademie věd České republiky (AV ČR)"): (
                7_177_502_810.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2023 2023-01-01_Zakon-c-449-2022-Sb-o-statnim-rozpoctu-Ceske-republiky-na-rok-2023.pdf",
                49,
            ),
            (2024, "Akademie věd České republiky (AV ČR)"): (
                7_642_481_529.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2024 2023-12-29_Zakon-o-statnim-rozpoctu-Ceske-republiky-na-rok-2024.pdf",
                49,
            ),
            (2016, "Technologická agentura České republiky (TA ČR)"): (
                2_958_939_000.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2016 Zak_2015-400_Zakon-o-statnim-rozpoctu-CR-na-rok-2016-Kompletni-vcetne-priloh.pdf",
                52,
            ),
            (2012, "Technologická agentura České republiky (TA ČR)"): (
                2_170_206_000.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2012 Zak_2011-455_Prilohy-k-zakonu-c-4552011-Sb-o-statnim-rozpoctu-Ceske-republiky-na-rok-2012.pdf",
                49,
            ),
            (2013, "Technologická agentura České republiky (TA ČR)"): (
                2_962_491_761.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2013 Zak_2012-504_Prilohy-k-zakonu-c-5042012-Sb-o-statnim-rozpoctu-Ceske-republiky-na-rok-2013.pdf",
                49,
            ),
            (2014, "Technologická agentura České republiky (TA ČR)"): (
                2_864_898_160.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2014 Zak_2013-475_Prilohy-k-zakonu-o-statnim-rozpoctu-Ceske-republiky-na-rok-2014.pdf",
                49,
            ),
            (2015, "Technologická agentura České republiky (TA ČR)"): (
                2_864_898_160.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2015 Zak_2014-345_Prilohy-k-zakonu-o-statnim-rozpoctu-Ceske-republiky-na-rok-2015.pdf",
                49,
            ),
            (2019, "Technologická agentura České republiky (TA ČR)"): (
                4_274_646_444.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2019 Zak_2018-336_Zakon-o-statnim-rozpoctu-Ceske-republiky-na-rok-2019.pdf",
                58,
            ),
            (2020, "Technologická agentura České republiky (TA ČR)"): (
                4_102_464_850.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2020 Zak_2019-355_Zakon-o-statnim-rozpoctu-Ceske-republiky-na-rok-2020.pdf",
                57,
            ),
            (2021, "Technologická agentura České republiky (TA ČR)"): (
                4_926_456_032.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2021 Zak_2020-600_Zakon-o-statni-rozpoctu-Ceske-republiky-na-rok-2021.pdf",
                56,
            ),
            (2022, "Technologická agentura České republiky (TA ČR)"): (
                5_664_987_717.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2022 Zak_2022-057_Zakon-o-statnim-rozpoctu-Ceske-republiky-na-rok-2022.pdf",
                58,
            ),
            (2022, "Grantová agentura České republiky (GA ČR)"): (
                4_669_819_125.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2022 Zak_2022-057_Zakon-o-statnim-rozpoctu-Ceske-republiky-na-rok-2022.pdf",
                24,
            ),
            (2023, "Technologická agentura České republiky (TA ČR)"): (
                6_327_572_010.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2023 2023-01-01_Zakon-c-449-2022-Sb-o-statnim-rozpoctu-Ceske-republiky-na-rok-2023.pdf",
                59,
            ),
            (2024, "Technologická agentura České republiky (TA ČR)"): (
                6_282_134_018.0,
                "koruna",
                "CZK",
                "manual_recovery",
                "Total expenditures",
                "2024 2023-12-29_Zakon-o-statnim-rozpoctu-Ceske-republiky-na-rok-2024.pdf",
                58,
            ),
        }
        czech_hardcoded_meta = {
            agency["canonical_name"]: {
                "category": agency.get("category"),
                "notes": agency.get("notes"),
            }
            for agency in CANONICAL_AGENCIES.get("Czech Republic", [])
        }
        for (year, canonical_name), (
            amount_local,
            unit,
            currency,
            item_type,
            line_description_en,
            source_file,
            page_number,
        ) in czech_verified_overrides.items():
            mask = canonical.eq(canonical_name) & year_num.eq(year)
            note = "recovered from audited Czech agency source row or chapter summary block with explicit total and page traceability"
            if not mask.any():
                template = {col: None for col in out.columns}
                template["country"] = country
                template["year"] = int(year)
                template["canonical_name"] = canonical_name
                template["category"] = czech_hardcoded_meta.get(canonical_name, {}).get("category")
                template["amount_local"] = float(amount_local)
                template["unit"] = unit
                template["currency"] = currency
                template["item_type"] = item_type
                template["line_description_en"] = line_description_en
                template["source_file"] = source_file
                template["page_number"] = str(page_number)
                base_notes = str(czech_hardcoded_meta.get(canonical_name, {}).get("notes") or "").strip()
                template["series_notes"] = f"{base_notes}; gap: no matching rows in this year; {note}".strip("; ").strip()
                out = pd.concat([out, pd.DataFrame([template])], ignore_index=True)
                canonical = out["canonical_name"].fillna("").astype(str)
                year_num = pd.to_numeric(out["year"], errors="coerce")
                amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
                source_text = out["source_file"].fillna("").astype(str)
                continue

            out.loc[
                mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            target_idx = out.index[mask][0]
            out.at[target_idx, "amount_local"] = float(amount_local)
            out.at[target_idx, "unit"] = unit
            out.at[target_idx, "currency"] = currency
            out.at[target_idx, "item_type"] = item_type
            out.at[target_idx, "line_description_en"] = line_description_en
            out.at[target_idx, "source_file"] = source_file
            out.at[target_idx, "page_number"] = str(page_number)
            notes = str(out.at[target_idx, "series_notes"] or "").strip()
            out.at[target_idx, "series_notes"] = f"{notes}; {note}".strip("; ").strip()

        out = out[out["canonical_name"].isin(hardcoded_names)].copy()

    if country == "UK":
        line_desc = out["line_description_en"].fillna("").astype(str)

        challenge_noise = (
            (out["canonical_name"] == "Industrial Strategy Challenge Fund")
            & line_desc.str.contains(r"full-fibre networks", case=False, na=False)
        )
        if challenge_noise.any():
            out.loc[
                challenge_noise,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]

        tax_credit_noise = (
            (out["canonical_name"] == "R&D Expenditure Credit")
            & (pd.to_numeric(out["amount_local"], errors="coerce") <= 0)
        )
        if tax_credit_noise.any():
            out.loc[
                tax_credit_noise,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]

        for canonical_name, spec in _UK_POLICY_RECOVERY_SPECS.items():
            candidates = _uk_policy_recovery_rows(subset, spec)
            if candidates.empty:
                continue
            for year, year_rows in candidates.groupby("year"):
                same_mask = (out["canonical_name"] == canonical_name) & (out["year"] == int(year))
                if not same_mask.any():
                    continue
                best = year_rows.loc[year_rows["_base_amount"].idxmax()]
                amount_local, unit = _expand_to_base_unit(
                    best.get("amount_local"),
                    best.get("unit"),
                    best.get("currency"),
                )
                out.loc[
                    same_mask,
                    ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
                ] = [None, None, None, None, None, None, None]
                target_idx = out.index[same_mask][0]
                out.at[target_idx, "amount_local"] = float(amount_local)
                out.at[target_idx, "unit"] = unit
                out.at[target_idx, "currency"] = best.get("currency")
                out.at[target_idx, "item_type"] = "policy_fund_recovery"
                out.at[target_idx, "line_description_en"] = best.get("line_description_en")
                out.at[target_idx, "source_file"] = best.get("source_file")
                out.at[target_idx, "page_number"] = best.get("page_number")
                notes = str(out.at[target_idx, "series_notes"] or "").strip()
                note = "recovered from UK policy fund / R&D package row"
                out.at[target_idx, "series_notes"] = f"{notes}; {note}".strip("; ").strip()

        # ── Post-recovery filters (must run AFTER policy_recovery_specs) ──────
        # Min-amount guard: null any row whose amount is below canonical's floor.
        # Runs after recovery so the recovery cannot reinstate noise rows.
        for canonical_name, minimum in _UK_MIN_AMOUNT_BY_CANONICAL.items():
            mask = (out["canonical_name"] == canonical_name) & (
                pd.to_numeric(out["amount_local"], errors="coerce") < float(minimum)
            )
            if not mask.any():
                continue
            out.loc[
                mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]

        # Manual impossible-year drops (run last so recovery cannot override).
        uk_manual_drop = pd.Series(False, index=out.index)
        for canonical_name, year in _UK_MANUAL_DROP_ROWS:
            uk_manual_drop = uk_manual_drop | (
                out["canonical_name"].eq(canonical_name) & pd.to_numeric(out["year"], errors="coerce").eq(year)
            )
        if uk_manual_drop.any():
            out.loc[
                uk_manual_drop,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]

    if country == "Netherlands":
        # --------------------------------------------------------------------
        # Netherlands outlier cleanup:
        # After _best_amount_for_agency cap-filtering, a handful of rows may
        # still survive with implausible amounts (e.g. from total-ministry rows
        # that matched an agency name variant despite being section aggregates).
        # Clear any series amount that exceeds a hard upper bound:
        #   • Pre-2002 NLG (unit='million'):  > 50B NLG (50_000 million guilders)
        #   • Post-2001 EUR (unit='euro' or 'thousand'): > 50B EUR
        # These values cannot represent a single Dutch R&D agency.
        # Additionally, clear the transition year 2001 rows with suspiciously
        # large NLG amounts from full-ministry totals that slipped through.
        # --------------------------------------------------------------------
        amounts = pd.to_numeric(out["amount_local"], errors="coerce")

        # Hard ceiling: any NL series row > 15B is certainly a ministry total.
        # (OCW university block grants max ~10B NLG in 2001; all other agencies < 5B.)
        hard_ceiling_mask = amounts.notna() & (amounts > 15_000_000_000)
        if hard_ceiling_mask.any():
            out.loc[
                hard_ceiling_mask,
                ["amount_local", "unit", "currency", "item_type",
                 "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            amounts = pd.to_numeric(out["amount_local"], errors="coerce")

        # Per-hardcoded-agency cap: null any surviving value that still exceeds
        # the agency's own max_amount_local (catches the 200B NWO 1984 case
        # that slipped through _best_amount_for_agency if no better row existed).
        for ag_def in agencies:
            cap = ag_def.get("max_amount_local")
            if cap is None:
                continue
            name = ag_def["canonical_name"]
            mask = (out["canonical_name"] == name) & amounts.notna() & (amounts > cap)
            if not mask.any():
                continue
            out.loc[
                mask,
                ["amount_local", "unit", "currency", "item_type",
                 "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            amounts = pd.to_numeric(out["amount_local"], errors="coerce")

    if country == "Germany":
        line_desc = out["line_description_en"].fillna("").astype(str)
        line_desc_clean = line_desc.str.strip()
        source_file = out["source_file"].fillna("").astype(str)
        amounts = pd.to_numeric(out["amount_local"], errors="coerce")

        not_allowed = ~out["canonical_name"].isin(_GERMANY_ALLOWED_FINAL_CANONICALS)
        if not_allowed.any():
            out.loc[
                not_allowed,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]

        # Agencies that have explicit curated keep-patterns handle their own
        # row selection — do NOT let the bare heading filter wipe them first.
        # (e.g. "Group 30 German Research Foundation" should survive to be
        # matched by the DFG keep pattern r"german research foundation$".)
        heading_noise = (
            out["canonical_name"].isin(_GERMANY_ALLOWED_FINAL_CANONICALS)
            & ~out["canonical_name"].isin(_GERMANY_KEEP_LINE_PATTERNS)
            & line_desc_clean.str.contains(_GERMANY_HEADING_PATTERNS, na=False)
        )
        if heading_noise.any():
            out.loc[
                heading_noise,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]

        ministry_noise = out["canonical_name"].eq("BMBF (Bundesministerium für Bildung und Forschung)")
        if ministry_noise.any():
            out.loc[
                ministry_noise,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]

        bgbl_heading_noise = (
            out["canonical_name"].isin(_GERMANY_ALLOWED_FINAL_CANONICALS)
            & ~out["canonical_name"].isin(_GERMANY_KEEP_LINE_PATTERNS)
            & source_file.str.contains(r"bgbl", case=False, na=False)
            & out["item_type"].fillna("").astype(str).str.lower().isin({"program_total", "section_total"})
            & line_desc_clean.str.contains(_GERMANY_HEADING_PATTERNS, na=False)
        )
        if bgbl_heading_noise.any():
            out.loc[
                bgbl_heading_noise,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]

        for canonical_name, minimum in _GERMANY_MIN_AMOUNT_BY_CANONICAL.items():
            mask = (out["canonical_name"] == canonical_name) & (amounts < float(minimum))
            if not mask.any():
                continue
            out.loc[
                mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]

        for canonical_name, maximum in _GERMANY_MAX_AMOUNT_BY_CANONICAL.items():
            mask = (out["canonical_name"] == canonical_name) & (amounts > float(maximum))
            if not mask.any():
                continue
            out.loc[
                mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]

        # Manual curation for Germany: keep only explicit institutional totals /
        # named federal grants for the institutions we chose to expose.
        curated_keep = pd.Series(False, index=out.index)
        for canonical_name, patterns in _GERMANY_KEEP_LINE_PATTERNS.items():
            canonical_mask = out["canonical_name"].eq(canonical_name)
            if not canonical_mask.any():
                continue
            keep_mask = pd.Series(False, index=out.index)
            for pattern in patterns:
                keep_mask = keep_mask | line_desc_clean.str.contains(pattern, case=False, na=False, regex=True)
            curated_keep = curated_keep | (canonical_mask & keep_mask)

        curated_drop = out["canonical_name"].isin(_GERMANY_KEEP_LINE_PATTERNS) & ~curated_keep
        if curated_drop.any():
            out.loc[
                curated_drop,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]

        manual_drop = pd.Series(False, index=out.index)
        for canonical_name, year in _GERMANY_MANUAL_DROP_ROWS:
            manual_drop = manual_drop | (
                out["canonical_name"].eq(canonical_name) & pd.to_numeric(out["year"], errors="coerce").eq(year)
            )
        if manual_drop.any():
            out.loc[
                manual_drop,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]

        # If a canonical-year has both a large total and a much smaller fragment,
        # suppress the fragment to avoid partial line items surviving alongside
        # the real institutional total.
        german_real = out[pd.to_numeric(out["amount_local"], errors="coerce").notna()].copy()
        for (canonical_name, year), grp in german_real.groupby(["canonical_name", "year"]):
            if len(grp) < 2:
                continue
            vals = pd.to_numeric(grp["amount_local"], errors="coerce").dropna().sort_values()
            if len(vals) < 2:
                continue
            max_val = float(vals.iloc[-1])
            frag_idxs = grp.index[pd.to_numeric(grp["amount_local"], errors="coerce") < (0.35 * max_val)]
            if len(frag_idxs):
                out.loc[
                    frag_idxs,
                    ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
                ] = [None, None, None, None, None, None, None]

    if country == "Estonia":
        amounts = pd.to_numeric(out["amount_local"], errors="coerce")
        canonical = out["canonical_name"].fillna("").astype(str)

        ministry_mask = canonical.eq("Ministry of Education and Research (Estonia)")
        if ministry_mask.any():
            out.loc[
                ministry_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]

        generic_discovered = pd.Series(False, index=out.index)
        for pat in _ESTONIA_GENERIC_DISCOVERED_PATTERNS:
            generic_discovered = generic_discovered | canonical.str.contains(pat, na=False)

        institution_like = canonical.str.contains(_ESTONIA_ORGANISATION_HINTS, na=False)
        hardcoded_names = {agency["canonical_name"] for agency in CANONICAL_AGENCIES.get("Estonia", [])}
        hardcoded_mask = canonical.isin(hardcoded_names)
        drop_generic = generic_discovered & ~institution_like & ~hardcoded_mask
        if drop_generic.any():
            out.loc[
                drop_generic,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]

        pre_euro_eur_mask = (
            pd.to_numeric(out["year"], errors="coerce").between(1992, 2010, inclusive="both")
            & out["currency"].fillna("").astype(str).str.upper().eq("EUR")
            & amounts.notna()
        )
        if pre_euro_eur_mask.any():
            out.loc[pre_euro_eur_mask, "currency"] = "EEK"
            out.loc[pre_euro_eur_mask, "unit"] = out.loc[pre_euro_eur_mask, "unit"].replace({"euro": "krone"})

        normalized_unit_mask = amounts.notna() & out["unit"].fillna("").astype(str).str.lower().isin(["", "unit"])
        if normalized_unit_mask.any():
            out.loc[normalized_unit_mask, "unit"] = out.loc[normalized_unit_mask].apply(
                lambda r: _base_output_unit(r.get("currency"), r.get("unit")),
                axis=1,
            )

        negative_mask = amounts.lt(0)
        if negative_mask.any():
            out.loc[
                negative_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]

        for canonical_name, year in _ESTONIA_VERIFIED_DROPS:
            mask = out["canonical_name"].eq(canonical_name) & pd.to_numeric(out["year"], errors="coerce").eq(year)
            if not mask.any():
                continue
            out.loc[
                mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]

        # Exclude the ministry aggregate entirely from Estonia's final panel:
        # it mixes portfolio totals with institution lines and only adds fake gaps.
        out = out[~out["canonical_name"].eq("Ministry of Education and Research (Estonia)")].copy()

    if country == "Portugal":
        amounts = pd.to_numeric(out["amount_local"], errors="coerce")
        canonical = out["canonical_name"].fillna("").astype(str)
        years = pd.to_numeric(out["year"], errors="coerce")
        currency = out["currency"].fillna("").astype(str).str.upper()
        source = out["source_file"].fillna("").astype(str)

        # Keep the final Portugal panel institution-focused. Chapter/programme
        # aggregates remain available in the country-level results/audit files,
        # but they are not methodologically comparable to agency appropriations.
        portugal_aggregate_canonicals = {
            "Ministério da Ciência e Tecnologia — Capítulo 50 (Portugal)",
            "P002 — Investigação Científica e Tecnológica e Inovação (Portugal)",
        }
        aggregate_mask = canonical.isin(portugal_aggregate_canonicals)
        if aggregate_mask.any():
            out.loc[
                aggregate_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            amounts = pd.to_numeric(out["amount_local"], errors="coerce")

        # Rows carrying EUR before the euro-accounting transition are too
        # unstable to anchor the final transparent panel.
        pre1999_eur_mask = years.lt(1999) & currency.eq("EUR") & amounts.notna()
        if pre1999_eur_mask.any():
            out.loc[
                pre1999_eur_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            amounts = pd.to_numeric(out["amount_local"], errors="coerce")

        # Present remaining rows in full local-currency units.
        generic_unit_mask = amounts.notna() & out["unit"].fillna("").astype(str).str.strip().str.lower().isin(["", "unit", "thousand"])
        if generic_unit_mask.any():
            out.loc[generic_unit_mask, "unit"] = out.loc[generic_unit_mask].apply(
                lambda r: _base_output_unit(r.get("currency"), r.get("unit")),
                axis=1,
            )

        # Suppress tiny earmarks / pass-through transfers that matched FCT/ANI
        # by name but are not credible agency-budget observations.
        min_amount_masks = {
            "FCT — Fundação para a Ciência e a Tecnologia (Portugal)": 5_000_000.0,
            "ANI — Agência Nacional de Inovação (Portugal)": 1_000_000.0,
        }
        amounts = pd.to_numeric(out["amount_local"], errors="coerce")
        for canonical_name, minimum in min_amount_masks.items():
            mask = (
                out["canonical_name"].eq(canonical_name)
                & out["currency"].fillna("").astype(str).str.upper().eq("EUR")
                & amounts.notna()
                & amounts.lt(minimum)
            )
            if not mask.any():
                continue
            out.loc[
                mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            amounts = pd.to_numeric(out["amount_local"], errors="coerce")

        # 1997 has duplicate source files with the same underlying budget.
        # Prefer the canonical "Lei orcamento..." file when observations are identical.
        out["_pt_source_pref"] = source.str.contains(r"^Lei orcamento para ", case=False, regex=True).astype(int)
        out["_pt_amount_key"] = pd.to_numeric(out["amount_local"], errors="coerce")
        dedup_cols = [
            "canonical_name",
            "year",
            "_pt_amount_key",
            "unit",
            "currency",
            "item_type",
            "line_description_en",
        ]
        out = (
            out.sort_values(
                dedup_cols + ["_pt_source_pref", "source_file"],
                ascending=[True, True, True, True, True, True, True, False, True],
                kind="stable",
            )
            .drop_duplicates(dedup_cols, keep="first")
            .drop(columns=["_pt_source_pref", "_pt_amount_key"])
            .reset_index(drop=True)
        )
        canonical = out["canonical_name"].fillna("").astype(str)
        years = pd.to_numeric(out["year"], errors="coerce")

        portugal_verified_overrides = {
            (1987, "JNICT — Junta Nacional de Investigação Científica e Tecnológica (Portugal)"): (
                249_849_000.0,
                "escudo",
                "PTE",
                "Lei orcamento para 1987.pdf",
                31,
                "Junta Nacional de Investigação Científica e Tecnológica",
                "manual override from original Portugal budget file (1987 institutional appropriations list; keeps the state-budget column used by the extracted series)",
            ),
            (1992, "JNICT — Junta Nacional de Investigação Científica e Tecnológica (Portugal)"): (
                10_000_000.0,
                "escudo",
                "PTE",
                "Lei orcamento para 1992.pdf",
                41,
                "Junta Nacional de Investigação Científica e Tecnológica",
                "manual override from original Portugal budget file (1992 institutional appropriations list)",
            ),
            (1994, "JNICT — Junta Nacional de Investigação Científica e Tecnológica (Portugal)"): (
                13_629_276_000.0,
                "escudo",
                "PTE",
                "Lei orcamento para 1994.pdf",
                59,
                "JUNTA NACIONAL DE INVESTIGAÇÃO CIENTIFICA E TECNOLOGICA",
                "manual override from original Portugal budget file (1994 institutional appropriations list)",
            ),
            (2006, "LNEC — Laboratório Nacional de Engenharia Civil (Portugal)"): (
                36_747_743.0,
                "euro",
                "EUR",
                "2006 00020361.pdf",
                77,
                "LABORATÓRIO NACIONAL DE ENGENHARIA CIVIL",
                "manual override from original Portugal budget file (MAPA VII / 2006 deterministic page-aligned parser)",
            ),
            (2007, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)"): (
                552_084_349.0,
                "euro",
                "EUR",
                "Lei orcamento para 2007.pdf",
                86,
                "FUNDAÇÃO PARA A CIÊNCIA E TECNOLOGIA",
                "manual override from original Portugal budget file (MAPA V / 2007 autonomous-services table)",
            ),
            (2007, "LNEC — Laboratório Nacional de Engenharia Civil (Portugal)"): (
                31_002_012.0,
                "euro",
                "EUR",
                "Lei orcamento para 2007.pdf",
                95,
                "LABORATÓRIO NACIONAL DE ENGENHARIA CIVIL",
                "manual override from original Portugal budget file (MAPA VII / 2007 deterministic page-aligned parser)",
            ),
            (2009, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)"): (
                654_236_704.0,
                "euro",
                "EUR",
                "Lei orcamento para 2009.pdf",
                104,
                "FUNDAÇÃO PARA A CIÊNCIA E TECNOLOGIA, I.P.",
                "manual override from original Portugal budget file (MAPA VII / 2009 autonomous-services table)",
            ),
            (2009, "LNEC — Laboratório Nacional de Engenharia Civil (Portugal)"): (
                34_204_446.0,
                "euro",
                "EUR",
                "Lei orcamento para 2009.pdf",
                102,
                "LABORATÓRIO NACIONAL DE ENGENHARIA CIVIL",
                "manual override from original Portugal budget file (MAPA VII / 2009 deterministic page-aligned parser)",
            ),
            (2010, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)"): (
                501_451_988.0,
                "euro",
                "EUR",
                "Lei orcamento para 2010.pdf",
                90,
                "FUNDAÇÃO PARA A CIÊNCIA E TECNOLOGIA, I.P.",
                "manual override from original Portugal budget file (MAPA VII / 2010 autonomous-services table)",
            ),
            (2010, "LNEC — Laboratório Nacional de Engenharia Civil (Portugal)"): (
                38_977_133.0,
                "euro",
                "EUR",
                "Lei orcamento para 2010.pdf",
                89,
                "LABORATÓRIO NACIONAL DE ENGENHARIA CIVIL",
                "manual override from original Portugal budget file (MAPA VII / 2010 deterministic page-aligned parser)",
            ),
            (2011, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)"): (
                82_335_508.0,
                "euro",
                "EUR",
                "Lei orcamento para 2011.pdf",
                94,
                "FUNDAÇÃO PARA A CIÊNCIA E TECNOLOGIA, I.P.",
                "manual override from original Portugal budget file (MAPA VII / 2011 deterministic page-aligned parser)",
            ),
            (2011, "LNEC — Laboratório Nacional de Engenharia Civil (Portugal)"): (
                35_886_658.0,
                "euro",
                "EUR",
                "Lei orcamento para 2011.pdf",
                93,
                "LABORATÓRIO NACIONAL DE ENGENHARIA CIVIL",
                "manual override from original Portugal budget file (MAPA VII / 2011 deterministic page-aligned parser)",
            ),
            (2012, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)"): (
                394_575_542.0,
                "euro",
                "EUR",
                "Lei orcamento para 2012.pdf",
                128,
                "FUNDAÇÃO PARA A CIÊNCIA E TECNOLOGIA, I.P.",
                "manual override from original Portugal budget file (MAPA VII / 2012 autonomous-services table)",
            ),
            (2012, "LNEC — Laboratório Nacional de Engenharia Civil (Portugal)"): (
                30_112_820.0,
                "euro",
                "EUR",
                "Lei orcamento para 2012.pdf",
                127,
                "LABORATÓRIO NACIONAL DE ENGENHARIA CIVIL",
                "manual override from original Portugal budget file (MAPA VII / 2012 deterministic page-aligned parser)",
            ),
            (2013, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)"): (
                420_884_807.0,
                "euro",
                "EUR",
                "Lei orcamento para 2013.pdf",
                131,
                "FUNDAÇÃO PARA A CIÊNCIA E TECNOLOGIA, I.P.",
                "manual override from original Portugal budget file (MAPA VII / 2013 autonomous-services table)",
            ),
            (2013, "LNEC — Laboratório Nacional de Engenharia Civil (Portugal)"): (
                30_050_258.0,
                "euro",
                "EUR",
                "Lei orcamento para 2013.pdf",
                130,
                "LABORATÓRIO NACIONAL DE ENGENHARIA CIVIL",
                "manual override from original Portugal budget file (MAPA VII / 2013 deterministic page-aligned parser)",
            ),
            (2014, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)"): (
                404_198_171.0,
                "euro",
                "EUR",
                "Lei orcamento para 2014.pdf",
                123,
                "FUNDAÇÃO PARA A CIÊNCIA E TECNOLOGIA, I.P.",
                "manual override from original Portugal budget file (MAPA VII / 2014 deterministic page-aligned parser)",
            ),
            (2014, "LNEC — Laboratório Nacional de Engenharia Civil (Portugal)"): (
                58_314_791.0,
                "euro",
                "EUR",
                "Lei orcamento para 2014.pdf",
                121,
                "LABORATÓRIO NACIONAL DE ENGENHARIA CIVIL",
                "manual override from original Portugal budget file (MAPA VII / 2014 deterministic page-aligned parser)",
            ),
            (2015, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)"): (
                426_506_331.0,
                "euro",
                "EUR",
                "Lei orcamento para 2015.pdf",
                125,
                "FUNDAÇÃO PARA A CIÊNCIA E TECNOLOGIA, I.P.",
                "manual override from original Portugal budget file (MAPA VII / 2015 autonomous-services table)",
            ),
            (2015, "LNEC — Laboratório Nacional de Engenharia Civil (Portugal)"): (
                65_587_229.0,
                "euro",
                "EUR",
                "Lei orcamento para 2015.pdf",
                122,
                "LABORATÓRIO NACIONAL DE ENGENHARIA CIVIL",
                "manual override from original Portugal budget file (MAPA VII / 2015 deterministic page-aligned parser)",
            ),
            (2016, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)"): (
                425_726_708.0,
                "euro",
                "EUR",
                "Lei orcamento para 2016.pdf",
                102,
                "FUNDAÇÃO PARA A CIÊNCIA E TECNOLOGIA, I.P.",
                "manual override from original Portugal budget file (2016 autonomous-services table)",
            ),
            (2016, "LNEC — Laboratório Nacional de Engenharia Civil (Portugal)"): (
                46_466_908.0,
                "euro",
                "EUR",
                "Lei orcamento para 2016.pdf",
                107,
                "LABORATÓRIO NACIONAL DE ENGENHARIA CIVIL",
                "manual override from original Portugal budget file (MAPA VII / 2016 deterministic page-aligned parser)",
            ),
            (2016, "ANI — Agência Nacional de Inovação (Portugal)"): (
                9_399_812.0,
                "euro",
                "EUR",
                "Lei orcamento para 2016.pdf",
                108,
                "AGENCIA NACIONAL DE INOVAÇAO, SA",
                "manual override from original Portugal budget file (MAPA VII / 2016 deterministic page-aligned parser)",
            ),
            (2017, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)"): (
                444_782_248.0,
                "euro",
                "EUR",
                "2017 Lei_42_2016-OE2017_VersaoDR.pdf",
                100,
                "FUNDAÇÃO PARA A CIÊNCIA E TECNOLOGIA, I.P.",
                "manual override from original Portugal budget file (2017 autonomous-services table)",
            ),
            (2017, "LNEC — Laboratório Nacional de Engenharia Civil (Portugal)"): (
                57_212_675.0,
                "euro",
                "EUR",
                "2017 Lei_42_2016-OE2017_VersaoDR.pdf",
                119,
                "LABORATÓRIO NACIONAL DE ENGENHARIA CIVIL",
                "manual override from original Portugal budget file (MAPA VII / 2017 deterministic page-aligned parser)",
            ),
            (2017, "ANI — Agência Nacional de Inovação (Portugal)"): (
                7_908_853.0,
                "euro",
                "EUR",
                "2017 Lei_42_2016-OE2017_VersaoDR.pdf",
                119,
                "AGENCIA NACIONAL DE INOVAÇAO, SA",
                "manual override from original Portugal budget file (MAPA VII / 2017 deterministic page-aligned parser)",
            ),
            (2018, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)"): (
                507_718_842.0,
                "euro",
                "EUR",
                "Lei orcamento para 2018.pdf",
                129,
                "FUNDAÇÃO PARA A CIÊNCIA E TECNOLOGIA, I.P.",
                "manual override from original Portugal budget file (2018 autonomous-services table)",
            ),
            (2018, "LNEC — Laboratório Nacional de Engenharia Civil (Portugal)"): (
                8_103_487.0,
                "euro",
                "EUR",
                "Lei orcamento para 2018.pdf",
                134,
                "LABORATÓRIO NACIONAL DE ENGENHARIA CIVIL",
                "manual override from original Portugal budget file (MAPA VII / 2018 deterministic page-aligned parser)",
            ),
            (2018, "ANI — Agência Nacional de Inovação (Portugal)"): (
                12_233_793.0,
                "euro",
                "EUR",
                "Lei orcamento para 2018.pdf",
                134,
                "AGENCIA NACIONAL DE INOVAÇAO, SA",
                "manual override from original Portugal budget file (MAPA VII / 2018 deterministic page-aligned parser)",
            ),
            (2019, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)"): (
                560_122_571.0,
                "euro",
                "EUR",
                "Lei orcamento para 2019.pdf",
                129,
                "FUNDAÇÃO PARA A CIÊNCIA E TECNOLOGIA, I.P.",
                "manual override from original Portugal budget file (2019 autonomous-services table)",
            ),
            (2019, "LNEC — Laboratório Nacional de Engenharia Civil (Portugal)"): (
                14_546_389.0,
                "euro",
                "EUR",
                "Lei orcamento para 2019.pdf",
                135,
                "LABORATÓRIO NACIONAL DE ENGENHARIA CIVIL",
                "manual override from original Portugal budget file (MAPA VII / 2019 deterministic page-aligned parser)",
            ),
            (2019, "ANI — Agência Nacional de Inovação (Portugal)"): (
                2_068_804.0,
                "euro",
                "EUR",
                "Lei orcamento para 2019.pdf",
                135,
                "AGENCIA NACIONAL DE INOVAÇAO, SA",
                "manual override from original Portugal budget file (MAPA VII / 2019 deterministic page-aligned parser)",
            ),
            (2020, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)"): (
                557_463_880.0,
                "euro",
                "EUR",
                "Lei orcamento para 2020.pdf",
                222,
                "FUNDAÇÃO PARA A CIÊNCIA E TECNOLOGIA, I.P.",
                "manual override from original Portugal budget file (2020 autonomous-services table)",
            ),
            (2020, "LNEC — Laboratório Nacional de Engenharia Civil (Portugal)"): (
                6_395_138.0,
                "euro",
                "EUR",
                "Lei orcamento para 2020.pdf",
                242,
                "LABORATÓRIO NACIONAL DE ENGENHARIA CIVIL",
                "manual override from original Portugal budget file (MAPA VII / 2020 deterministic page-aligned parser)",
            ),
            (2020, "ANI — Agência Nacional de Inovação (Portugal)"): (
                18_289_688.0,
                "euro",
                "EUR",
                "Lei orcamento para 2020.pdf",
                233,
                "AGENCIA NACIONAL DE INOVAÇAO, SA",
                "manual override from original Portugal budget file (MAPA VII / 2020 deterministic page-aligned parser)",
            ),
        }
        for (override_year, override_name), (
            override_amount,
            override_unit,
            override_currency,
            override_file,
            override_page,
            override_desc,
            override_note,
        ) in portugal_verified_overrides.items():
            override_mask = canonical.eq(override_name) & years.eq(override_year)
            if not override_mask.any():
                continue
            target_idx = out.index[override_mask][0]
            notes = str(out.at[target_idx, "series_notes"] or "").strip()
            out.at[target_idx, "amount_local"] = float(override_amount)
            out.at[target_idx, "unit"] = override_unit
            out.at[target_idx, "currency"] = override_currency
            out.at[target_idx, "item_type"] = "verified_override"
            out.at[target_idx, "line_description_en"] = override_desc
            out.at[target_idx, "source_file"] = override_file
            out.at[target_idx, "page_number"] = str(override_page)
            out.at[target_idx, "series_notes"] = f"{notes}; {override_note}".strip("; ").strip()

        # Manual drops used to be a hardcoded Python list here. Moved to a plain CSV
        # (Data/output/budget/Portugal/portugal_manual_drops.csv: year, canonical_name,
        # reason) so a non-programmer can review, audit, or add an entry without
        # touching this 800KB+ file — and so a diff on that CSV shows exactly what
        # changed, instead of disappearing into a Python tuple list. Falls back to
        # the old hardcoded list (with a generic reason) if the CSV is ever missing,
        # so this refactor can't silently turn into "Portugal audit rules stopped
        # applying" if the file gets moved/deleted.
        _portugal_drops_csv = Path("Data/output/budget/Portugal/portugal_manual_drops.csv")
        _generic_drop_reason = (
            "legal text, plurianual project table, or non-institutional support line rather than a clean agency appropriation"
        )
        if _portugal_drops_csv.exists():
            try:
                _drops_df = pd.read_csv(_portugal_drops_csv)
                portugal_manual_drops = list(
                    zip(_drops_df["year"].astype(int), _drops_df["canonical_name"], _drops_df["reason"])
                )
            except Exception:
                portugal_manual_drops = []
        else:
            portugal_manual_drops = [
                (1991, "JNICT — Junta Nacional de Investigação Científica e Tecnológica (Portugal)", _generic_drop_reason),
                (1993, "JNICT — Junta Nacional de Investigação Científica e Tecnológica (Portugal)", _generic_drop_reason),
                (1995, "JNICT — Junta Nacional de Investigação Científica e Tecnológica (Portugal)", _generic_drop_reason),
                (2000, "LNEC — Laboratório Nacional de Engenharia Civil (Portugal)", _generic_drop_reason),
                (2001, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)", _generic_drop_reason),
                (2002, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)", _generic_drop_reason),
                (2002, "LNEC — Laboratório Nacional de Engenharia Civil (Portugal)", _generic_drop_reason),
                (2003, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)", _generic_drop_reason),
                (2003, "LNEC — Laboratório Nacional de Engenharia Civil (Portugal)", _generic_drop_reason),
                (2004, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)", _generic_drop_reason),
                (2005, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)", _generic_drop_reason),
                (2009, "ANI — Agência Nacional de Inovação (Portugal)", _generic_drop_reason),
                (2014, "ANI — Agência Nacional de Inovação (Portugal)", _generic_drop_reason),
                (2021, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)", _generic_drop_reason),
                (2022, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)", _generic_drop_reason),
                (2024, "ANI — Agência Nacional de Inovação (Portugal)", _generic_drop_reason),
                (2024, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)", _generic_drop_reason),
                (2025, "ANI — Agência Nacional de Inovação (Portugal)", _generic_drop_reason),
                (2025, "FCT — Fundação para a Ciência e a Tecnologia (Portugal)", _generic_drop_reason),
            ]
        for drop_year, drop_name, drop_reason in portugal_manual_drops:
            drop_mask = canonical.eq(drop_name) & years.eq(drop_year)
            if not drop_mask.any():
                continue
            notes = out.loc[drop_mask, "series_notes"].fillna("").astype(str)
            out.loc[drop_mask, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Portugal source audit: {drop_reason}".strip("; ").strip()
            )
            out.loc[
                drop_mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]

        out = out.loc[~out["canonical_name"].isin(portugal_aggregate_canonicals)].reset_index(drop=True)

    if country == "Slovakia":
        amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
        unit_norm = out["unit"].fillna("").astype(str).str.strip().str.lower()

        # Keep final units human-readable and directly comparable to the audited
        # page values: full koruna/euro, not 'thousand' or generic 'unit'.
        thousand_mask = amount_num.notna() & unit_norm.eq("thousand")
        if thousand_mask.any():
            out.loc[thousand_mask, "amount_local"] = amount_num.loc[thousand_mask] * 1000.0
            out.loc[thousand_mask, "unit"] = out.loc[thousand_mask].apply(
                lambda r: _base_output_unit(r.get("currency"), r.get("unit")),
                axis=1,
            )
            amount_num = pd.to_numeric(out["amount_local"], errors="coerce")
            unit_norm = out["unit"].fillna("").astype(str).str.strip().str.lower()

        generic_unit_mask = amount_num.notna() & unit_norm.isin(["", "unit"])
        if generic_unit_mask.any():
            out.loc[generic_unit_mask, "unit"] = out.loc[generic_unit_mask].apply(
                lambda r: _base_output_unit(r.get("currency"), r.get("unit")),
                axis=1,
            )

        # Deduplicate mirrored source files such as "...pdf" and "... (1).pdf"
        # when they point to the same selected observation.
        source_series = out.get("source_file", pd.Series("", index=out.index)).fillna("").astype(str)
        out["_sk_source_norm"] = source_series.str.replace(r" \(\d+\)(?=\.pdf$)", "", regex=True)
        out["_sk_has_copy_suffix"] = source_series.str.contains(r" \(\d+\)(?=\.pdf$)", regex=True).astype(int)
        out["_sk_amount_key"] = pd.to_numeric(out["amount_local"], errors="coerce")
        dedup_cols = [
            "canonical_name",
            "year",
            "_sk_source_norm",
            "_sk_amount_key",
            "unit",
            "currency",
            "item_type",
            "line_description_en",
        ]
        out = (
            out.sort_values(
                dedup_cols + ["_sk_has_copy_suffix"],
                ascending=[True, True, True, True, True, True, True, True, True],
                kind="stable",
            )
            .drop_duplicates(dedup_cols, keep="first")
            .drop(columns=["_sk_source_norm", "_sk_has_copy_suffix", "_sk_amount_key"])
            .reset_index(drop=True)
        )

        # Manual audit: in 2022 the generic "Support for Research and
        # Development" line is a duplicate rendering of the more specific
        # APVV-supported R&D tasks line on the same page with the same amount.
        amt_sk = pd.to_numeric(out["amount_local"], errors="coerce")
        generic_support = out["canonical_name"].eq("Support for Research and Development")
        specific_support = out["canonical_name"].eq(
            "Research and Development Tasks Supported by the Agency for Research and Development Support"
        )
        for year, source_file, amount in out.loc[
            specific_support & amt_sk.notna(),
            ["year", "source_file", "amount_local"],
        ].itertuples(index=False, name=None):
            dup_mask = (
                generic_support
                & out["year"].eq(year)
                & out["source_file"].eq(source_file)
                & pd.to_numeric(out["amount_local"], errors="coerce").eq(float(amount))
            )
            if dup_mask.any():
                out.loc[
                    dup_mask,
                    ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
                ] = [None, None, None, None, None, None, None]
                notes = out.loc[dup_mask, "series_notes"].fillna("").astype(str).str.strip()
                out.loc[dup_mask, "series_notes"] = notes.apply(
                    lambda s: f"{s}; dropped after Slovakia source audit: duplicate wording of the audited APVV-supported R&D tasks line".strip("; ").strip()
                )

        # Manual audit of the 2025 source file: the single-page table supports
        # the ministry chapter total and the SAV chapter total, but it does not
        # explicitly name APVV or VEGA anywhere in the original text. The
        # extracted APVV/VEGA amounts are therefore not document-verifiable in
        # this source and should not appear as audited point observations.
        unverifiable_2025 = (
            out["year"].eq(2025)
            & out["source_file"].fillna("").astype(str).eq("2025 20250101_5681740-2.pdf")
            & out["canonical_name"].isin([
                "APVV (Agentúra na podporu výskumu a vývoja)",
                "VEGA (Vedecká grantová agentúra MŠ SR a SAV)",
            ])
        )
        if unverifiable_2025.any():
            out.loc[
                unverifiable_2025,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            notes = out.loc[unverifiable_2025, "series_notes"].fillna("").astype(str).str.strip()
            out.loc[unverifiable_2025, "series_notes"] = notes.apply(
                lambda s: f"{s}; dropped after Slovakia source audit: 2025 source file does not explicitly name this agency/line in the original table".strip("; ").strip()
            )

    if country == "Latvia":
        amounts = pd.to_numeric(out["amount_local"], errors="coerce")
        years = pd.to_numeric(out["year"], errors="coerce")
        canonical = out["canonical_name"].fillna("").astype(str)
        line_desc = out["line_description_en"].fillna("").astype(str)
        source_file = out["source_file"].fillna("").astype(str)
        latvia_verified_overrides = {
            (1997, "Science Base Funding (Latvia)"): (
                6_731_662.0,
                24.0,
                "LVL",
                "likumi_lv_305681_12.12.1997__lv.pdf",
                "Verified 1997 scientific-activities provision from the original Latvia budget table",
                "manual override from original Latvia budget file (page 24, programme 05.01.00 total expenditures)",
            ),
            (1999, "Science Base Funding (Latvia)"): (
                7_415_859.0,
                42.0,
                "LVL",
                "likumi_lv_22778_03.11.1999__lv.pdf",
                "Verified 1999 scientific-activities provision from the original Latvia budget table",
                "manual override from original Latvia budget file (page 42, programme 05.01.00 total expenditures)",
            ),
            (2001, "Science Base Funding (Latvia)"): (
                7_646_507.0,
                60.0,
                "LVL",
                "likumi_lv_13735_07.07.2001__lv.pdf",
                "Verified 2001 provision of scientific activities from the original Latvia budget table",
                "manual override from original Latvia budget file (page 60, programme 05.01.00 total expenditures)",
            ),
            (2003, "Science Base Funding (Latvia)"): (
                7_930_511.0,
                61.0,
                "LVL",
                "2003 likumi_lv_72288_27.11.2003__lv.pdf",
                "Verified 2003 ensuring scientific activity from the original Latvia budget table",
                "manual override from original Latvia budget file (page 61, programme 05.01.00 total expenditures)",
            ),
            (2006, "Science Base Funding (Latvia)"): (
                4_625_554.0,
                64.0,
                "LVL",
                "likumi_lv_121006_09.11.2006__lv.pdf",
                "Verified 2006 science base funding from the original Latvia budget table",
                "manual override from original Latvia budget file (page 64, programme 05.02.00 total expenditures)",
            ),
            (2000, "State-Commissioned Scientific Research (Latvia)"): (
                529_571.0,
                55.0,
                "LVL",
                "likumi_lv_305669_18.11.2000__lv.pdf",
                "Verified 2000 total expenditures for State-commissioned scientific research from the original Latvia budget table",
                "manual override from original Latvia budget file (page 55, programme 05.06.00 total expenditures)",
            ),
            (2001, "State-Commissioned Scientific Research (Latvia)"): (
                554_571.0,
                60.0,
                "LVL",
                "likumi_lv_13735_07.07.2001__lv.pdf",
                "Verified 2001 state-commissioned scientific research from the original Latvia budget table",
                "manual override from original Latvia budget file (page 60, programme 05.06.00 total expenditures)",
            ),
            (2003, "State-Commissioned Scientific Research (Latvia)"): (
                70_209.0,
                61.0,
                "LVL",
                "2003 likumi_lv_72288_27.11.2003__lv.pdf",
                "Verified 2003 state-commissioned scientific research from the original Latvia budget table",
                "manual override from original Latvia budget file (page 61, programme 05.06.00 total expenditures)",
            ),
            (2018, "State Research Programmes (Latvia)"): (
                2_000_000.0,
                10.0,
                "EUR",
                "likumi_lv_295569_01.01.2018__lv.pdf",
                "Verified 2018 state research programme in energy from the original Latvia budget law",
                "manual override from original Latvia budget file (page 10, subprogramme 29.05.00 energy state research programme)",
            ),
            (2006, "Science Programme (Latvia)"): (
                8_544_825.0,
                64.0,
                "LVL",
                "likumi_lv_121006_09.11.2006__lv.pdf",
                "Verified 2006 ensuring scientific activity total from the original Latvia budget table",
                "manual override from original Latvia budget file (page 64, programme 05.01.00 total expenditures)",
            ),
            (2006, "University Science Development (Latvia)"): (
                2_400_000.0,
                63.0,
                "LVL",
                "likumi_lv_121006_09.11.2006__lv.pdf",
                "Verified 2006 development of scientific activities in universities from the original Latvia budget table",
                "manual override from original Latvia budget file (page 63, programme 03.12.00 total expenditures)",
            ),
        }

        # Latvia's rich tables frequently emit both a substantive programme row
        # and nearby financing mechanics for the same budget line. Keep the
        # programme/institution rows and null the financing shells.
        finance_mechanics = line_desc.str.contains(
            r"total revenue|resources for expenditure coverage|grant from general revenues|subsidy from general revenue|"
            r"paid services and other own revenues|foreign financial assistance|maintenance expenditures|current expenditures|"
            r"expenditure - total|total liabilities|compensation|goods and services|state basic budget",
            case=False,
            regex=True,
            na=False,
        )
        if finance_mechanics.any():
            out.loc[
                finance_mechanics,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            amounts = pd.to_numeric(out["amount_local"], errors="coerce")

        # Broad ministry and non-core legal/forensic rows are not part of the
        # core science-budget panel.
        non_core = canonical.isin(
            {
                "Scientific research of judicial expertise",
                "Patent Office",
            }
        )
        if non_core.any():
            out.loc[
                non_core,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            amounts = pd.to_numeric(out["amount_local"], errors="coerce")

        # Keep the early Academy series historical only; later rows are mostly
        # OCR/targeted-recovery misassignments from broader science programmes.
        late_academy = canonical.eq("Latvian Academy of Sciences") & years.gt(1993)
        if late_academy.any():
            out.loc[
                late_academy,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            amounts = pd.to_numeric(out["amount_local"], errors="coerce")

        # The 1993 "Latvian Science Council" survivor is not a real Council
        # appropriation. The original table is "Zinātnu akadēmija — kopā".
        false_science_council = canonical.eq("Latvian Science Council") & years.eq(1993)
        if false_science_council.any():
            out.loc[
                false_science_council,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            amounts = pd.to_numeric(out["amount_local"], errors="coerce")

        # Normalize documentary era currencies and final unit labels.
        early_rub = years.le(1993) & amounts.notna()
        pre_euro = years.between(1994, 2013, inclusive="both") & amounts.notna()
        post_euro = years.ge(2014) & amounts.notna()
        if early_rub.any():
            out.loc[early_rub, "amount_local"] = amounts.loc[early_rub] / 1000.0
            out.loc[early_rub, "currency"] = "RUB"
            out.loc[early_rub, "unit"] = "thousand"
        if pre_euro.any():
            out.loc[pre_euro, "currency"] = "LVL"
            out.loc[pre_euro, "unit"] = "unit"
        if post_euro.any():
            out.loc[post_euro, "currency"] = "EUR"
            out.loc[post_euro, "unit"] = "euro"

        # Duplicate English/local wrappers of the exact same legal text should
        # count as restatements, not additive acts. Keep the official Latvian
        # law file when both are present with the same amount.
        duplicate_exact = []
        real = out[pd.to_numeric(out["amount_local"], errors="coerce").notna()].copy()
        for (canon_name, year, amount), grp in real.groupby(["canonical_name", "year", "amount_local"], dropna=False):
            if len(grp) < 2:
                continue
            if not grp["source_file"].astype(str).str.contains(r"Finance law for|BUDZETS\.DOC", case=False, regex=True, na=False).any():
                continue
            preferred = grp[grp["source_file"].astype(str).str.contains(r"likumi_lv_", case=False, regex=True, na=False)]
            if preferred.empty:
                continue
            keep_idx = preferred.index[0]
            duplicate_exact.extend(idx for idx in grp.index if idx != keep_idx)
        if duplicate_exact:
            out.loc[
                duplicate_exact,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]

        amounts = pd.to_numeric(out["amount_local"], errors="coerce")
        unit_norm = out["unit"].fillna("").astype(str).str.strip().str.lower()
        residual_thousand = amounts.notna() & unit_norm.eq("thousand")
        if residual_thousand.any():
            out.loc[residual_thousand, "amount_local"] = amounts.loc[residual_thousand] * 1000.0
            out.loc[residual_thousand, "unit"] = out.loc[residual_thousand].apply(
                lambda r: _base_output_unit(r.get("currency"), r.get("unit")),
                axis=1,
            )

        for (year, canonical_name), (
            amount_local,
            page_number,
            currency,
            source_name,
            description,
            note,
        ) in latvia_verified_overrides.items():
            mask = canonical.eq(canonical_name) & years.eq(year)
            if mask.any():
                target_idx = out.index[mask][0]
            else:
                ref_row = out[canonical.eq(canonical_name)]
                if ref_row.empty:
                    continue
                target_idx = len(out)
                new_row = ref_row.iloc[0].copy()
                new_row["year"] = int(year)
                new_row["amount_local"] = None
                new_row["unit"] = None
                new_row["currency"] = None
                new_row["item_type"] = None
                new_row["line_description_en"] = None
                new_row["source_file"] = None
                new_row["page_number"] = None
                out = pd.concat([out, new_row.to_frame().T], ignore_index=True)
                canonical = out["canonical_name"].fillna("").astype(str)
                years = pd.to_numeric(out["year"], errors="coerce")
                mask = canonical.eq(canonical_name) & years.eq(year)
            out.loc[
                mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            out.at[target_idx, "amount_local"] = float(amount_local)
            out.at[target_idx, "unit"] = "unit"
            out.at[target_idx, "currency"] = currency
            out.at[target_idx, "item_type"] = "verified_override"
            out.at[target_idx, "line_description_en"] = description
            out.at[target_idx, "source_file"] = source_name
            out.at[target_idx, "page_number"] = str(page_number)
            notes = str(out.at[target_idx, "series_notes"] or "").strip()
            out.at[target_idx, "series_notes"] = f"{notes}; {note}".strip("; ").strip()

        # Latvia overrides can null duplicate survivor rows when multiple
        # candidates existed for the same canonical-year cell. Drop those
        # empty placeholders from the final exported series.
        out = out[pd.to_numeric(out["amount_local"], errors="coerce").notna()].copy()

    if country == "Belgium":
        canonical = out["canonical_name"].fillna("").astype(str)
        hardcoded_names = {agency["canonical_name"] for agency in CANONICAL_AGENCIES.get("Belgium", [])}

        institution_like = canonical.str.contains(
            r"belspo|fnrs|fwo|observatory|meteorological|aeronomy|natural sciences|radioelements|sck|pasteur|niras|von karman",
            case=False,
            regex=True,
            na=False,
        )
        generic_programmatic = canonical.str.contains(
            r"actions?|allocations?|contribution|contracts?|costs?|envelope|expenses?|expenditures?|funding|government|grants?|initiatives?|medical scientific research|national|participation|programs?|reimbursements?|research and development|research in the field|scientific research|space activities|studies|subsid(?:y|ies)|voluntary",
            case=False,
            regex=True,
            na=False,
        )
        drop_generic = ~canonical.isin(hardcoded_names) & generic_programmatic & ~institution_like
        if drop_generic.any():
            out = out.loc[~drop_generic].copy()

        belgium_year = pd.to_numeric(out.get("year"), errors="coerce")
        for year, overrides in _BELGIUM_VERIFIED_OVERRIDES.items():
            for canonical_name, (amount_local, page_number, currency, source_file) in overrides.items():
                mask = out["canonical_name"].eq(canonical_name) & belgium_year.eq(year)
                if not mask.any():
                    continue
                target_idx = out.index[mask][0]
                out.loc[
                    mask,
                    ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
                ] = [None, None, None, None, None, None, None]
                out.at[target_idx, "amount_local"] = float(amount_local)
                out.at[target_idx, "unit"] = "franc"
                out.at[target_idx, "currency"] = currency
                out.at[target_idx, "item_type"] = "verified_override"
                if year == 1994:
                    desc = "Verified 1994 voted amount for international R&D in Federal Science Policy from the 1995 budget table"
                    note = "manual override from original Belgium budget file (1995 table, 1994 voted column)"
                elif year == 1999:
                    desc = "Verified 1999 total for Belgium's international R&D / ESA programme from the original budget table"
                    note = "manual override from original Belgium budget file (1999 table, program 11.60.2 total)"
                elif year == 1998:
                    desc = "Verified 1998 adjusted amount for Belgium's ESA / international R&D programme from the 1999 budget table"
                    note = "manual override from original Belgium budget file (1999 table, 1998 adjusted column)"
                elif year == 2000:
                    desc = "Verified 2000 adjusted amount for Belgium's ESA / international R&D programme from the 2001 budget table"
                    note = "manual override from original Belgium budget file (2001 table, 2000 adjusted column)"
                elif year == 2001:
                    desc = "Verified 2001 total for Belgium's international R&D / ESA programme from the original budget table"
                    note = "manual override from original Belgium budget file (2001 table, program 11.60.2 total)"
                else:
                    desc = "Verified Belgium science-policy amount from the original budget table"
                    note = "manual override from original Belgium budget file"
                out.at[target_idx, "line_description_en"] = desc
                out.at[target_idx, "source_file"] = source_file
                out.at[target_idx, "page_number"] = str(page_number)
                notes = str(out.at[target_idx, "series_notes"] or "").strip()
                out.at[target_idx, "series_notes"] = f"{notes}; {note}".strip("; ").strip()

        for canonical_name, year in _BELGIUM_VERIFIED_DROPS:
            drop_mask = out["canonical_name"].eq(canonical_name) & belgium_year.eq(year)
            if not drop_mask.any():
                continue
            out = out.loc[~drop_mask].copy()
            belgium_year = pd.to_numeric(out.get("year"), errors="coerce")

        belgium_verified_overrides = {
            ("Royal Observatory of Belgium", 2004): r"Grant to the Royal Observatory of Belgium",
            ("Royal Belgian Institute of Natural Sciences", 2004): r"Grant to the Royal Belgian Institute of Natural Sciences",
        }
        if belgium_verified_overrides:
            raw_country = df[df["country"] == country].copy()
            raw_country["_year_num"] = pd.to_numeric(raw_country.get("year"), errors="coerce")
            raw_country["_amt_num"] = pd.to_numeric(raw_country.get("amount_local"), errors="coerce")
            raw_text = (
                raw_country.get("line_description_en", pd.Series("", index=raw_country.index)).fillna("").astype(str)
                + " "
                + raw_country.get("line_description", pd.Series("", index=raw_country.index)).fillna("").astype(str)
            )
            out_year = pd.to_numeric(out.get("year"), errors="coerce")

            for (canonical_name, year), line_pattern in belgium_verified_overrides.items():
                out_mask = out["canonical_name"].eq(canonical_name) & out_year.eq(year)
                if not out_mask.any():
                    continue
                candidate_mask = raw_country["_year_num"].eq(year) & raw_text.str.contains(
                    line_pattern, case=False, regex=True, na=False
                )
                candidates = raw_country.loc[candidate_mask].copy()
                if candidates.empty:
                    continue
                candidates = candidates[candidates["_amt_num"].notna()]
                if candidates.empty:
                    continue
                chosen = candidates.loc[candidates["_amt_num"].idxmax()]
                amount = float(chosen["_amt_num"])
                unit = str(chosen.get("unit", "") or "").strip().lower()
                currency = str(chosen.get("currency", "") or "").strip().upper()
                if unit == "thousand":
                    amount *= 1000.0
                    unit = "euro" if currency == "EUR" else "franc"
                elif currency == "EUR":
                    unit = "euro"
                elif currency == "BEF":
                    unit = "franc"

                out.loc[out_mask, "amount_local"] = amount
                out.loc[out_mask, "unit"] = unit
                out.loc[out_mask, "currency"] = currency
                out.loc[out_mask, "item_type"] = chosen.get("item_type")
                out.loc[out_mask, "line_description_en"] = chosen.get("line_description_en")
                if "line_description" in out.columns:
                    out.loc[out_mask, "line_description"] = chosen.get("line_description")
                if "source_file" in out.columns:
                    out.loc[out_mask, "source_file"] = chosen.get("source_file")
                if "page_number" in out.columns:
                    out.loc[out_mask, "page_number"] = chosen.get("page_number")

    locked_entries = get_locked_series_entries(country)
    if locked_entries:
        for year, canonical_name, amount_local, unit, currency, source_file, page_number, item_type, line_description_en in locked_entries:
            mask = out["canonical_name"].eq(canonical_name) & pd.to_numeric(out["year"], errors="coerce").eq(int(year))
            if not mask.any():
                ref_row = out[out["canonical_name"] == canonical_name]
                if ref_row.empty:
                    continue
                new_row = ref_row.iloc[0].copy()
                new_row["year"] = int(year)
                new_row["amount_local"] = float(amount_local)
                new_row["unit"] = unit
                new_row["currency"] = currency
                new_row["item_type"] = item_type
                new_row["line_description_en"] = line_description_en
                new_row["source_file"] = source_file
                new_row["page_number"] = str(page_number) if page_number is not None else None
                base_note = str(new_row.get("series_notes", "") or "").strip()
                new_row["series_notes"] = "; ".join(part for part in [base_note, "locked manual curation"] if part)
                out = pd.concat([out, new_row.to_frame().T], ignore_index=True)
                continue

            target_idx = out.index[mask][0]
            out.loc[
                mask,
                ["amount_local", "unit", "currency", "item_type", "line_description_en", "source_file", "page_number"],
            ] = [None, None, None, None, None, None, None]
            out.at[target_idx, "amount_local"] = float(amount_local)
            out.at[target_idx, "unit"] = unit
            out.at[target_idx, "currency"] = currency
            out.at[target_idx, "item_type"] = item_type
            out.at[target_idx, "line_description_en"] = line_description_en
            out.at[target_idx, "source_file"] = source_file
            out.at[target_idx, "page_number"] = str(page_number) if page_number is not None else None
            notes = str(out.at[target_idx, "series_notes"] or "").strip()
            out.at[target_idx, "series_notes"] = "; ".join(part for part in [notes, "locked manual curation"] if part)

    # Final series should contain at most one row per canonical-year. When a
    # locked override is added after gap rows were already materialized, keep
    # the verified row and discard the empty duplicate.
    out["_is_verified_override"] = (out["item_type"] == "verified_override").astype(int)
    out["_has_amount"] = pd.to_numeric(out["amount_local"], errors="coerce").notna().astype(int)
    out = (
        out.sort_values(
            ["canonical_name", "year", "_is_verified_override", "_has_amount"],
            ascending=[True, True, False, False],
        )
        .drop_duplicates(["canonical_name", "year"], keep="first")
        .drop(columns=["_is_verified_override", "_has_amount"])
        .reset_index(drop=True)
    )

    out = out.sort_values(["canonical_name", "year"]).reset_index(drop=True)
    final_agency_count = out["canonical_name"].nunique()

    logger.info(
        f"Canonical series [{country}]: {final_agency_count} agencies, "
        f"{out['year'].nunique()} years, "
        f"{out['amount_local'].notna().sum()} data points, "
        f"{out['amount_local'].isna().sum()} gaps."
    )
    return out


def build_totals_series(
    detail_df: pd.DataFrame,
    country: str,
) -> pd.DataFrame:
    """
    Build an aggregated totals series from the detail series.

    For each (canonical_name, year), sum all amounts across Acts, but flag
    potential restatements (where two Acts have the exact same amount for
    the same agency — likely the same money re-appropriated, not additional).

    Returns a DataFrame with one row per (canonical_name, year):
        canonical_name, year, amount_total, amount_primary, n_acts,
        acts, additive_flag, currency, unit, category

    additive_flag values:
        "additive"      — amounts from different Acts appear genuinely additional
        "restatement"   — at least two Acts share the same amount (possible double-count)
        "single"        — only one Act contributed (no ambiguity)
        "gap"           — no data for this agency-year
    """
    rows = detail_df[
        (detail_df["country"] == country)
        & detail_df["amount_local"].notna()
    ].copy()

    if rows.empty:
        return pd.DataFrame()

    records = []
    for (canonical_name, year), grp in rows.groupby(["canonical_name", "year"]):
        amounts = grp["amount_local"].tolist()
        acts = grp["source_file"].tolist()
        currency = grp["currency"].iloc[0] if "currency" in grp.columns else None
        unit = grp["unit"].iloc[0] if "unit" in grp.columns else None
        category = grp["category"].iloc[0] if "category" in grp.columns else None

        # Primary amount = the row from the lowest Act number
        primary_row = grp.iloc[0]  # already sorted by source_file in detail series
        amount_primary = float(primary_row["amount_local"])

        # Detect restatements: any two rows share the same amount
        has_restatement = len(amounts) > 1 and len(set(round(a, 0) for a in amounts)) < len(amounts)

        if len(amounts) == 1:
            additive_flag = "single"
            amount_total = amounts[0]
        elif has_restatement:
            additive_flag = "restatement"
            # Use primary amount only — do not sum to avoid double-count
            amount_total = amount_primary
        else:
            additive_flag = "additive"
            amount_total = sum(amounts)

        records.append({
            "country": country,
            "canonical_name": canonical_name,
            "year": year,
            "amount_total": float(amount_total),
            "amount_primary": float(amount_primary),
            "n_acts": len(amounts),
            "acts": " | ".join(str(a) for a in acts),
            "additive_flag": additive_flag,
            "currency": currency,
            "unit": unit,
            "category": category,
        })

    # Also add gap rows from detail_df (where amount_local is None)
    gap_rows = detail_df[
        (detail_df["country"] == country)
        & detail_df["amount_local"].isna()
    ]
    for _, row in gap_rows.iterrows():
        records.append({
            "country": country,
            "canonical_name": row["canonical_name"],
            "year": row["year"],
            "amount_total": None,
            "amount_primary": None,
            "n_acts": 0,
            "acts": "",
            "additive_flag": "gap",
            "currency": row.get("currency"),
            "unit": row.get("unit"),
            "category": row.get("category"),
        })

    out = pd.DataFrame(records).sort_values(["canonical_name", "year"]).reset_index(drop=True)

    n_additive = (out["additive_flag"] == "additive").sum()
    n_restatement = (out["additive_flag"] == "restatement").sum()
    if n_restatement:
        logger.warning(
            f"[{country}] {n_restatement} agency-years have possible restatements "
            f"(same amount in multiple Acts) — check 'restatement' rows before summing."
        )
    logger.info(
        f"Totals series [{country}]: {n_additive} additive, "
        f"{n_restatement} restatement, "
        f"{(out['additive_flag']=='single').sum()} single-act rows"
    )
    return out
