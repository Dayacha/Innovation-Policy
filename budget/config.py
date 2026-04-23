"""
budget pipeline configuration.

All paths, model parameters, country context, and chunking settings live here.
Settings are merged from config.yaml at runtime (see pipeline.py load_config).
"""

from __future__ import annotations
from pathlib import Path

# ---------------------------------------------------------------------------
# Project-level paths (resolved relative to this file)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "Data"

PDF_ROOT = DATA_DIR / "input" / "finance_bills"

# Output directory for this pipeline (separate from budget/)
OUTPUT_DIR = DATA_DIR / "output" / "budget"

# Cache for extracted PDF text (shared with budget/ to save re-OCR)
PDF_TEXT_CACHE_DIR = DATA_DIR / "output" / "budget" / "full_text"

# Per-document JSON cache for LLM responses
LLM_CACHE_DIR = OUTPUT_DIR / "llm_cache"

# Final outputs
RESULTS_CSV = OUTPUT_DIR / "results.csv"
RESULTS_EXCEL = OUTPUT_DIR / "results.xlsx"
LLM_USAGE_FILE = OUTPUT_DIR / "llm_usage.json"
RUN_LOG_FILE = OUTPUT_DIR / "run_log.jsonl"

# ---------------------------------------------------------------------------
# Default LLM settings (overridden by config.yaml llm: block)
# ---------------------------------------------------------------------------
DEFAULT_LLM_CONFIG = {
    "provider": "openai",
    "model": "gpt-4o-mini",
    "max_tokens": 4096,
    "temperature": 0,
    "api_delay": 0.5,
}

# Model used for the cheap scan pass (pass 1).
SCAN_MODEL = "gpt-4o-mini"

# Model used for deep extraction (pass 2).
EXTRACT_MODEL = "gpt-4o-mini"

# ---------------------------------------------------------------------------
# Chunking settings
# ---------------------------------------------------------------------------
# Maximum characters per LLM chunk for pass-2 extraction
CHUNK_SIZE = 15_000
# Overlap between chunks (avoids cutting a line item mid-sentence)
CHUNK_OVERLAP = 300
# Maximum pages to send in a single extraction call
MAX_PAGES_PER_CHUNK = 10

# ---------------------------------------------------------------------------
# Processing controls
# ---------------------------------------------------------------------------
# Skip files whose cache entry already exists
SKIP_CACHED = True
# If True, run scan pass (pass 1); if False, send all pages to pass 2
USE_SCAN_PASS = True
# Minimum scan-pass confidence to include a page in the extraction pass
SCAN_THRESHOLD = 0.4

# ---------------------------------------------------------------------------
# Country → context mapping
# Each entry provides the LLM with country-specific priors.
# Keys must match directory names under PDF_ROOT exactly.
# ---------------------------------------------------------------------------
COUNTRY_CONTEXT: dict[str, dict] = {
    "UK": {
        "currency": "GBP",
        "currency_symbol": "£",
        "language": "english",
        "unit_hint": "billion",  # DEL tables use £ billion
        "known_agencies": [
            "Department for Business, Innovation and Skills (BIS)",
            "Department for Business, Energy and Industrial Strategy (BEIS)",
            "Department of Trade and Industry (DTI)",
            "Department for Innovation, Universities and Skills (DIUS)",
            "Office of Science and Technology (OST)",
            "Research Councils UK (RCUK)",
            "UK Research and Innovation (UKRI)",
            "Medical Research Council (MRC)",
            "Engineering and Physical Sciences Research Council (EPSRC)",
            "Natural Environment Research Council (NERC)",
            "Science and Technology Facilities Council (STFC)",
            "Biotechnology and Biological Sciences Research Council (BBSRC)",
            "Economic and Social Research Council (ESRC)",
            "Arts and Humanities Research Council (AHRC)",
            "Innovate UK",
            "Higher Education Funding Council for England (HEFCE)",
        ],
        # Bodies whose section_totals should NOT be marked 'include' (too broad)
        "mixed_ministries": [
            "Department of Health", "NHS", "Ministry of Defence",
            "Department for Education", "Department for Work and Pensions",
            "HM Treasury", "Home Office", "Ministry of Justice",
            "Department for Transport", "Department for Communities",
            "Department for Environment", "Foreign and Commonwealth Office",
        ],
        "doc_type_hint": "Departmental Expenditure Limits (DEL) tables in the Spending Review / Budget. "
                         "Look for Resource DEL and Capital DEL rows under science/research departments.",
    },
    "Australia": {
        "currency": "AUD",
        "currency_symbol": "A$",
        "language": "english",
        "unit_hint": "thousand",  # Appropriation Acts use thousands
        "known_agencies": [
            "Department of Industry, Science and Resources",
            "Department of Education, Science and Training",
            "Australian Research Council (ARC)",
            "Commonwealth Scientific and Industrial Research Organisation (CSIRO)",
            "National Health and Medical Research Council (NHMRC)",
            "Cooperative Research Centres (CRC)",
            "Australian Nuclear Science and Technology Organisation (ANSTO)",
            "Geoscience Australia",
            "Bureau of Meteorology",
        ],
        "mixed_ministries": [
            "Hospitals and Health Services Commission", "Department of Health",
            "Commission on Advanced Education", "Commission on Technical and Further Education",
            "Department of Education", "Department of Social Security",
            "Department of Housing", "Department of Transport",
            "Department of Primary Industry", "Department of Minerals and Energy",
            "Department of Foreign Affairs", "Department of Immigration",
            "Department of Defence", "Air Transport Group",
        ],
        "doc_type_hint": "Appropriation Acts / Portfolio Budget Statements. "
                         "Look for departmental and administered appropriations.",
    },
    "Canada": {
        "currency": "CAD",
        "currency_symbol": "C$",
        "language": "english",
        "unit_hint": "dollar",  # Main Estimates use full dollars or thousands
        "known_agencies": [
            "Natural Sciences and Engineering Research Council (NSERC)",
            "Social Sciences and Humanities Research Council (SSHRC)",
            "Canadian Institutes of Health Research (CIHR)",
            "Canada Foundation for Innovation (CFI)",
            "National Research Council (NRC)",
            "Industrial Research Assistance Program (IRAP)",
            "Atomic Energy of Canada Limited (AECL)",
            "Defence Research and Development Canada (DRDC)",
            "Communications Research Centre (CRC)",
            "Statistics Canada",
        ],
        "mixed_ministries": [
            "Department of National Defence", "Department of Health Canada",
            "Department of Foreign Affairs", "Canadian International Development Agency (CIDA)",
            "Department of Human Resources", "Department of Transport",
            "Department of Agriculture", "Department of Natural Resources",
            "Canada Revenue Agency", "Department of Justice",
        ],
        "doc_type_hint": "Main Estimates / Appropriation Acts. "
                         "Look for grants and contributions to research agencies.",
    },
    "New Zealand": {
        "currency": "NZD",
        "currency_symbol": "NZ$",
        "language": "english",
        "unit_hint": "thousand",
        "known_agencies": [
            "Department of Scientific and Industrial Research (DSIR)",
            "Foundation for Research, Science and Technology (FRST)",
            "Marsden Fund",
            "Ministry of Business, Innovation and Employment (MBIE)",
            "Crown Research Institutes (CRIs)",
            "AgResearch",
            "Industrial Research Limited (IRL)",
            "Institute of Geological and Nuclear Sciences (GNS)",
            "National Institute of Water and Atmospheric Research (NIWA)",
            "Landcare Research",
            "New Zealand Institute for Plant and Food Research",
            "Tertiary Education Commission (TEC)",
            "Endeavour Fund",
            "Strategic Science Investment Fund (SSIF)",
            "Callaghan Innovation",
        ],
        "mixed_ministries": [
            "Ministry of Health", "Ministry of Education",
            "Ministry of Foreign Affairs and Trade", "New Zealand Aid Programme",
            "Ministry of Defence", "New Zealand Police",
            "Ministry of Social Development", "Ministry of Transport",
            "Ministry of Agriculture and Forestry",
        ],
        "doc_type_hint": "Appropriation (Estimates) Bills / Supply Bills. "
                         "Look for 'Vote Science and Innovation' and research appropriations.",
    },
    "Denmark": {
        "currency": "DKK",
        "currency_symbol": "kr",
        "language": "danish",
        "unit_hint": "thousand",  # Finanslov amounts in 1000s DKK
        "known_agencies": [
            "Undervisningsministeriet (Ministry of Education)",
            "Forskningsministeriet (Ministry of Research)",
            "Videnskabsministeriet (Ministry of Science)",
            "Atomenergikommissionen (Atomic Energy Commission)",
            "Statens teknisk-videnskabelige Forskningsfond (State Technical-Scientific Research Fund)",
            "Statens naturvidenskabelige Forskningsrad (State Natural Science Research Council)",
            "Statens samfundsvidenskabelige Forskningsrad",
            "Statens humanistiske Forskningsrad",
            "Statens laegervidenskabelige Forskningsrad",
            "Danmarks tekniske Hojskole (DTH)",
        ],
        "mixed_ministries": [
            "Indenrigsministeriet (Ministry of the Interior)",
            "Socialministeriet (Ministry of Social Affairs)",
            "Sundhedsministeriet (Ministry of Health)",
            "Udenrigsministeriet (Ministry of Foreign Affairs)",
            "Forsvarsministeriet (Ministry of Defence)",
            "Trafikministeriet (Ministry of Transport)",
            "Finansministeriet (Ministry of Finance)",
            "Justitsministeriet (Ministry of Justice)",
        ],
        "doc_type_hint": "Finanslov (Finance Bill). "
                         "Look for § 20 Undervisningsministeriet, § 32 Forskningsministeriet, "
                         "and research fund appropriations.",
    },
    "France": {
        "currency": "EUR",
        "currency_symbol": "€",
        "language": "french",
        "unit_hint": "million",
        "known_agencies": [
            "Centre National de la Recherche Scientifique (CNRS)",
            "Agence Nationale de la Recherche (ANR)",
            "Institut National de la Santé et de la Recherche Médicale (INSERM)",
            "Commissariat à l'Énergie Atomique (CEA)",
            "Institut National de Recherche en Informatique et en Automatique (INRIA)",
            "Agence Nationale pour la Gestion des Déchets Radioactifs (ANDRA)",
        ],
        "mixed_ministries": [
            "Ministère de la Défense", "Ministère des Affaires étrangères",
            "Ministère de la Santé", "Ministère de l'Éducation nationale",
            "Ministère de l'Agriculture", "Ministère de l'Intérieur",
            "Ministère des Transports", "Ministère du Travail",
        ],
        "doc_type_hint": "Loi de finances / Budget de l'État (JORF). "
                         "Look for Mission 'Recherche et enseignement supérieur'. "
                         "UNIT RULE: The main JORF mission table uses MILLIONS d'euros. "
                         "Always set unit='million' — '2 417' means 2417 million EUR = €2.4B. "
                         "Exception: if a table is explicitly headed 'milliers d'euros' or 'en milliers €', "
                         "use unit='thousand'.",
    },
    "Germany": {
        "currency": "EUR",
        "currency_symbol": "€",
        "language": "german",
        "unit_hint": "thousand",
        "known_agencies": [
            "Bundesministerium für Bildung und Forschung (BMBF)",
            "Deutsche Forschungsgemeinschaft (DFG)",
            "Fraunhofer-Gesellschaft",
            "Max-Planck-Gesellschaft",
            "Helmholtz-Gemeinschaft",
            "Leibniz-Gemeinschaft",
        ],
        "mixed_ministries": [
            "Bundesministerium der Verteidigung (BMVg)",
            "Bundesministerium für Gesundheit (BMG)",
            "Bundesministerium des Innern (BMI)",
            "Bundesministerium für Arbeit und Soziales (BMAS)",
            "Bundesministerium für Ernährung und Landwirtschaft (BMEL)",
            "Auswärtiges Amt (Foreign Office)",
        ],
        "doc_type_hint": "Bundeshaushalt. "
                         "Look for Einzelplan 30 (BMBF) and science appropriations.",
    },
    "Norway": {
        "currency": "NOK",
        "currency_symbol": "kr",
        "language": "norwegian",
        "unit_hint": "thousand",
        "known_agencies": [
            "Norges forskningsråd (Research Council of Norway)",
            "SINTEF",
            "Universiteter og høyskoler",
        ],
        "mixed_ministries": [
            "Forsvarsdepartementet (Ministry of Defence)",
            "Helse- og omsorgsdepartementet (Ministry of Health)",
            "Utenriksdepartementet (Ministry of Foreign Affairs)",
            "Justis- og beredskapsdepartementet (Ministry of Justice)",
            "Samferdselsdepartementet (Ministry of Transport)",
        ],
        "doc_type_hint": "Statsbudsjettet. Look for research and science appropriations.",
    },
    "Sweden": {
        "currency": "SEK",
        "currency_symbol": "kr",
        "language": "swedish",
        "unit_hint": "thousand",
        "known_agencies": [
            "Vetenskapsrådet (Swedish Research Council)",
            "VINNOVA (Swedish Innovation Agency)",
            "Riksbankens Jubileumsfond",
        ],
        "mixed_ministries": [
            "Försvarsdepartementet (Ministry of Defence)",
            "Socialdepartementet (Ministry of Social Affairs)",
            "Utrikesdepartementet (Ministry of Foreign Affairs)",
            "Finansdepartementet (Ministry of Finance)",
            "Justitiedepartementet (Ministry of Justice)",
        ],
        "doc_type_hint": "Statsbudget / Budgetpropositionen. Look for research appropriations.",
    },
    "Japan": {
        "currency": "JPY",
        "currency_symbol": "¥",
        "language": "japanese",
        "unit_hint": "million",  # 予算書 amounts in 百万円 (millions of yen)
        "known_agencies": [
            "Japan Science and Technology Agency (JST / 科学技術振興機構)",
            "Japan Society for the Promotion of Science (JSPS / 日本学術振興会)",
            "RIKEN (理化学研究所)",
            "National Institute for Materials Science (NIMS / 物質・材料研究機構)",
            "Japan Agency for Marine-Earth Science and Technology (JAMSTEC / 海洋研究開発機構)",
            "Japan Aerospace Exploration Agency (JAXA / 宇宙航空研究開発機構)",
            "New Energy and Industrial Technology Development Organization (NEDO / 新エネルギー・産業技術総合開発機構)",
            "Ministry of Education, Culture, Sports, Science and Technology (MEXT / 文部科学省)",
            "Ministry of Economy, Trade and Industry (METI / 経済産業省)",
            "National Institutes of Health Sciences (NIHS)",
            "Japan Atomic Energy Agency (JAEA / 日本原子力研究開発機構)",
        ],
        "mixed_ministries": [
            "Ministry of Defense (防衛省)",
            "Ministry of Foreign Affairs (外務省)",
            "Ministry of Health, Labour and Welfare (厚生労働省)",
            "Ministry of Finance (財務省)",
            "Ministry of Land, Infrastructure, Transport and Tourism (国土交通省)",
            "Ministry of Internal Affairs and Communications (総務省)",
            "Ministry of Justice (法務省)",
        ],
        "doc_type_hint": "Japanese national budget (予算書 / 予算案). "
                         "Look for 文部科学省 (MEXT) and 経済産業省 (METI) R&D appropriations. "
                         "Amounts typically in 百万円 (millions of yen). "
                         "Key line items: 科学技術振興費 (S&T promotion), 研究開発 (R&D).",
    },
}

# Fallback for countries not listed above
DEFAULT_COUNTRY_CONTEXT = {
    "currency": "LOCAL",
    "currency_symbol": "",
    "language": "english",
    "unit_hint": "unknown",
    "known_agencies": [],
    "doc_type_hint": "Government Finance Bill / Budget document.",
}


def get_country_context(country: str) -> dict:
    """Return country context, falling back to defaults for unknown countries."""
    return COUNTRY_CONTEXT.get(country, {**DEFAULT_COUNTRY_CONTEXT, "currency": "LOCAL"})


# ---------------------------------------------------------------------------
# R&D category taxonomy (GBARD / Frascati)
# Used in prompts and output schema validation
# ---------------------------------------------------------------------------
RD_CATEGORIES = {
    "direct_rd": "Direct R&D funding: grants, contracts, and appropriations explicitly for research and development",
    "higher_education": "R&D through higher education institutions (universities, colleges)",
    "research_infrastructure": "Research infrastructure: equipment, facilities, databases, supercomputers",
    "innovation_instruments": "Innovation support: technology transfer, startup support, industry R&D incentives",
    "science_agency": "Core funding for dedicated science/research agencies (research councils, national labs)",
    "rd_adjacent": "R&D-adjacent: activities supporting research (e.g. science communication, international cooperation)",
    "unclear": "Relevant but category unclear from available text",
}

# Output CSV columns — must remain stable (downstream tools depend on this schema)
OUTPUT_COLUMNS = [
    "country",
    "year",
    "item_type",
    "section_code",
    "section_name",
    "section_name_en",
    "line_code",
    "line_description",
    "line_description_en",
    "amount_local",
    "unit",
    "currency",
    "rd_category",
    "decision",
    "confidence",
    "source_file",
    "page_number",
    "llm_model",
    "extraction_pass",
    "notes",
]
