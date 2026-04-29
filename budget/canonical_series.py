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

logger = logging.getLogger(__name__)

__all__ = ["build_canonical_series", "CANONICAL_AGENCIES"]

_OUTPUT_UNIT_BY_CURRENCY = {
    "AUD": "dollar",
    "CAD": "dollar",
    "NZD": "dollar",
    "USD": "dollar",
    "JPY": "yen",
    "EUR": "euro",
    "DEM": "mark",
    "FRF": "franc",
    "GBP": "pound",
    "DKK": "krone",
    "NOK": "krone",
    "SEK": "krona",
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
        # Bundeshaushalt title-group prefix format (2003-2009 documents)
        r"tgr\.\s*\d+\s+german research foundation",
        r"group\s+\d+\s+german research foundation",
        # Later explicit allocation descriptions (2022+)
        r"allocations for the german research foundation",
        r"zuwendungen.*deutsche forschungsgemeinschaft",
        # Bare standalone name (2025)
        r"^german research foundation$",
        r"^deutsche forschungsgemeinschaft$",
    ],
    "Helmholtz-Gemeinschaft (HGF)": [
        r"institutional grants to the helmholtz association",
        # Bundeshaushalt title-group format
        r"helmholtz association$",
        r"tgr\.\s*\d+\s+helmholtz association",
        r"group\s+\d+\s+helmholtz association",
        r"^helmholtz-gemeinschaft",
        r"centers of the hermann von helmholtz association",
        r"hgf\b",
    ],
    "Fraunhofer-Gesellschaft": [
        r"institutional grants to the fraunhofer society",
        # Bundeshaushalt title-group format
        r"fraunhofer society$",
        r"tgr\.\s*\d+\s+fraunhofer",
        r"group\s+\d+\s+fraunhofer",
        r"^fraunhofer-gesellschaft",
        r"grant for the basic funding of the fraunhofer society",
        r"fraunhofer society for the promotion",
    ],
    "Leibniz-Gemeinschaft (WGL)": [
        r"grants to the leibniz association",
        # Bundeshaushalt title-group format
        r"leibniz association$",
        r"tgr\.\s*\d+\s+leibniz",
        r"group\s+\d+\s+leibniz",
        r"^leibniz-gemeinschaft",
        r"blaue liste",
        r"gottfried wilhelm leibniz scientific community",
        r"gottfried wilhelm leibniz wissenschaftsgemeinschaft",
    ],
    "Max-Planck-Gesellschaft (MPG)": [
        r"total amount for the max planck society",
        # Bundeshaushalt title-group format
        r"max planck society$",
        r"tgr\.\s*\d+\s+max planck",
        r"group\s+\d+\s+max planck",
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
            "preferred_item_type": ["section_total", "program_total", "line_item"],
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
            "active_years": (1928, 2099),
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
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (2002, 2099),
            "max_amount_local": 5_000_000_000,   # 5B EUR
            "notes": "Contains NWO, KNAW, SURF appropriations as a combined article total.",
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
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (2002, 2099),
            "max_amount_local": 5_000_000_000,   # 5B EUR
            "notes": "EZ article for enterprise/innovation policy, contains TNO grants and TKI.",
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
            "active_years": (1977, 2099),
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
            "active_years": (2017, 2099),
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
            "active_years": (1939, 2099),
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
            "active_years": (1977, 2099),
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
    # NEW ZEALAND — skeleton
    # -----------------------------------------------------------------------
    "New Zealand": [
        {
            "canonical_name": "DSIR / Crown Research Institutes",
            "category": "science_agency",
            "name_variants": [
                "department of scientific and industrial research",
                "dsir",
                "crown research institute",
                "foundation for research, science",
            ],
            "preferred_item_type": ["section_total", "program_total"],
            "active_years": (1926, 2099),
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
            # FWF ~280M EUR in 2023 (= 280,000 thousand EUR). >1B implausible single grant.
            "max_amount_local": 1_500_000,   # in thousands of currency unit
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
            # FFG ~700M EUR in recent years (includes programme-specific grants).
            "max_amount_local": 3_000_000,
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
            # ÖAW ~100M EUR/year.
            "max_amount_local": 500_000,
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
            "max_amount_local": 500_000,
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
            # IST Austria state grant ~200M EUR/year.
            "max_amount_local": 500_000,
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
            "max_amount_local": 200_000,
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
            "max_amount_local": 100_000,
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
            "max_amount_local": 200_000,
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
            "max_amount_local": 150_000,
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
            "max_amount_local": 1_500_000,
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
            "max_amount_local": 1_000_000,
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
            "max_amount_local": 700_000,
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
            "active_years": (1975, 2099),
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
            "active_years": (1939, 2099),
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
            "active_years": (2017, 2099),
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
            "active_years": (1977, 2099),
            "notes": "Industrial R&D loans and grants. Under Industria/Ciencia ministry.",
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
            "active_years": (1986, 2099),
            "notes": "Health research. Programme 465A. Key agency for biomedical R&D.",
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
            "active_years": (1986, 2099),
            "notes": "Energy and environment research. Formerly JEN (Junta de Energía Nuclear) pre-1986.",
        },
        {
            "canonical_name": "Plan Nacional I+D (total R&D appropriation)",
            "category": "direct_rd",
            "name_variants": [
                "plan nacional de i+d",
                "fomento y coordinación de la investigación",
                "463b",
                "investigación científica y técnica",
            ],
            "preferred_item_type": ["section_total"],
            "active_years": (1988, 2016),
            "notes": "National R&D plan aggregate. Use as fallback when agency-level not available.",
            "aggregation_role_override": "section",
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
                "suomen akatemian tutkimusmäärärahat",
                "suomen akatemian tutkimusmaarararahat",
                "29.60.50",
                "academy of finland research grants",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1970, 2099),
            "notes": "Core basic research grants. Moment 29.60.50. This is the KEY Finnish R&D series.",
        },
        {
            "canonical_name": "Suomen Akatemia — toimintamenot (operating)",
            "category": "science_agency",
            "name_variants": [
                "suomen akatemian toimintamenot",
                "29.60.01",
                "suomen akatemia toimintamenot",
            ],
            "preferred_item_type": ["line_item"],
            "active_years": (1970, 2099),
            "notes": "Academy operating budget. Separate from research grants (29.60.50).",
        },
        {
            "canonical_name": "Business Finland / Tekes (innovation agency)",
            "category": "innovation_instruments",
            "name_variants": [
                "innovaatiorahoituskeskus business finland",
                "business finland",
                "tekes",
                "teknologian ja innovaatioiden kehittämiskeskus",
                "32.20.05",
                "32.20.06",
            ],
            "preferred_item_type": ["line_item", "program_total"],
            "active_years": (1983, 2099),
            "notes": "Tekes renamed Business Finland in 2018. Moment 32.20.06 (Tekes) → 32.20.05 (BF).",
        },
        {
            "canonical_name": "Business Finland / Tekes — public R&D grants",
            "category": "innovation_instruments",
            "name_variants": [
                "julkinen tutkimus- ja kehittämistoiminta",
                "avustukset tutkimukseen kehitykseen ja innovaatiotoimintaan",
                "tutkimus- kehittämis- ja innovaatiotoiminnan tukeminen",
                "32.20.40",
                "32.20.83",
            ],
            "preferred_item_type": ["line_item"],
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
            ],
            "preferred_item_type": ["line_item", "program_total"],
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
            ],
            "preferred_item_type": ["line_item"],
            "active_years": (1885, 2099),
            "notes": "Geological survey. Under TEM (Ministry of Economic Affairs).",
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
            ],
            "preferred_item_type": ["line_item"],
            "active_years": (1900, 2099),
            "notes": "MTT + Metla + RKTL merged into Luke (Natural Resources Institute) in 2015.",
        },
        {
            "canonical_name": "VATT (Government Institute for Economic Research)",
            "category": "science_agency",
            "name_variants": [
                "valtion taloudellinen tutkimuskeskus",
                "vatt",
                "28.30.02",
            ],
            "preferred_item_type": ["line_item"],
            "active_years": (1990, 2099),
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
    combined = f"{desc} {section} {desc_raw} {section_raw}".lower()
    for variant in agency["name_variants"]:
        v = variant.lower()
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
    sec_lower = f"{section} {section_raw}".lower()
    desc_lower = f"{desc} {desc_raw}".lower()
    for variant in agency["name_variants"]:
        v = variant.lower()
        if v in sec_lower:
            return 2
    for variant in agency["name_variants"]:
        v = variant.lower()
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
        for group in exclude_groups:
            exclude_mask = combined_text.apply(
                lambda text: any(re.search(pattern, text, re.IGNORECASE) for pattern in group)
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
        narrowed = None
        for group in pattern_groups:
            group_mask = combined_text.apply(
                lambda text: any(re.search(pattern, text, re.IGNORECASE) for pattern in group)
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
    existing_names = {a["canonical_name"].lower() for a in hardcoded}
    existing_aliases = set()
    for agency in hardcoded:
        existing_aliases.add(str(agency.get("canonical_name", "")).strip().lower())
        existing_aliases.add(str(agency.get("source_entity", "")).strip().lower())
        for variant in agency.get("name_variants", []) or []:
            existing_aliases.add(str(variant).strip().lower())
    existing_aliases.discard("")

    try:
        from budget.agency_discovery import load_discovered_agencies
        discovered = load_discovered_agencies(country)
    except Exception:
        discovered = []

    def _aliases_conflict(discovered_aliases: set[str]) -> bool:
        if existing_aliases.intersection(discovered_aliases):
            return True
        for alias in discovered_aliases:
            if not alias:
                continue
            for existing in existing_aliases:
                if not existing:
                    continue
                # Catch "Operating expenses grant for JST" style discovered
                # labels that wrap an already-hardcoded agency name.
                if len(alias) >= 12 and existing in alias:
                    return True
                if len(existing) >= 12 and alias in existing:
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
            existing_aliases.add(canonical_lc)
            if agency.get("source_entity"):
                existing_aliases.add(str(agency["source_entity"]).strip().lower())
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

    records = []

    for agency in agencies:
        agency = dict(agency)
        agency["_country"] = country
        canonical_name = agency["canonical_name"]
        active_start, active_end = agency.get("active_years", (1800, 2099))

        for year, year_df in subset.groupby("year"):
            try:
                yr_int = int(str(year))
            except ValueError:
                continue

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
                best = _best_amount_for_agency(file_matches, agency["preferred_item_type"], agency)
                if best is None:
                    continue
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
                    new_row["page_number"] = float(page_number)
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
                out.at[target_idx, "page_number"] = float(page_number)
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

    out = out.sort_values(["canonical_name", "year"]).reset_index(drop=True)

    logger.info(
        f"Canonical series [{country}]: {len(agencies)} agencies, "
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
