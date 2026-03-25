"""Central configuration for the budget extraction pipeline (Finance Bills)."""

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"

# ── Input data ────────────────────────────────────────────────────────────────
PDF_ROOT                  = DATA_DIR / "input" / "finance_bills"
TAXONOMY_FILE             = DATA_DIR / "input" / "taxonomy" / "Full search library.xlsx"
TAXONOMY_JSON             = DATA_DIR / "input" / "taxonomy" / "search_library.json"
TRANSLATION_GLOSSARY_FILE = DATA_DIR / "input" / "taxonomy" / "translation_glossary.json"

# ── Output data ───────────────────────────────────────────────────────────────
PROCESSED_DIR    = DATA_DIR / "output" / "budget"        # backwards-compat alias
INTERMEDIATE_DIR = PROCESSED_DIR / "intermediate"
FULLTEXT_DIR     = PROCESSED_DIR / "full_text"
FULLTEXT_EN_DIR  = PROCESSED_DIR / "full_text_en"

FILE_INVENTORY_FILE          = INTERMEDIATE_DIR / "file_inventory.csv"
PAGE_EXTRACTION_FILE         = INTERMEDIATE_DIR / "page_text.csv"
PER_FILE_SUMMARY_FILE        = INTERMEDIATE_DIR / "file_text_summary.csv"
KEYWORD_HITS_FILE            = INTERMEDIATE_DIR / "keyword_hits.csv"
FULLTEXT_EXPORT_MANIFEST_FILE = INTERMEDIATE_DIR / "full_text_exports.csv"
CANDIDATES_FILE              = INTERMEDIATE_DIR / "innovation_candidates.csv"
CANDIDATE_PAGES_FILE         = PROCESSED_DIR / "candidate_pages_detected.csv"
BUDGET_ITEMS_FILE            = INTERMEDIATE_DIR / "budget_items_detected.csv"

RESULTS_FILE              = PROCESSED_DIR / "results.txt"
RESULTS_JSON_FILE         = PROCESSED_DIR / "results.json"
RESULTS_CSV_FILE          = PROCESSED_DIR / "results.csv"
RESULTS_EXCEL_FILE        = PROCESSED_DIR / "results.xlsx"
RESULTS_AI_VERIFIED_FILE  = PROCESSED_DIR / "results_ai_verified.csv"
RESULTS_REVIEW_STATUS_FILE = PROCESSED_DIR / "results_review_status.csv"
RUNS_DIR                  = PROCESSED_DIR / "runs"


# OCR and extraction settings
MIN_DIRECT_TEXT_CHARS = 120
MIN_ALNUM_RATIO = 0.25
OCR_ZOOM = 2.0
TESSERACT_LANGS = "eng+fra+dan"


# Country inference map (token -> country).
# Keys are normalised (lowercase, no accents). Longer keys take priority.
# Rule: exact token match always wins; substring match only for len >= 5.
COUNTRY_TOKEN_MAP = {
    # ── Denmark ───────────────────────────────────────────────────────────────
    "denmark": "Denmark",
    "danmark": "Denmark",
    "vedtaget": "Denmark",        # "adopted" — appears in DK Finance Bill titles
    "folketing": "Denmark",       # Danish parliament
    "finanslov": "Denmark",       # Danish Finance Bill
    "finanslovforslaget": "Denmark",
    # ── France ────────────────────────────────────────────────────────────────
    "france": "France",
    "french": "France",
    "loi_de_finances": "France",
    "budget_de_letat": "France",
    "republique_francaise": "France",
    # ── Germany ───────────────────────────────────────────────────────────────
    "germany": "Germany",
    "deutschland": "Germany",
    "bundeshaushalt": "Germany",  # Federal budget
    "bundesrepublik": "Germany",
    "bundestag": "Germany",
    # ── United Kingdom ────────────────────────────────────────────────────────
    "united_kingdom": "United Kingdom",
    "unitedkingdom": "United Kingdom",
    "england": "United Kingdom",
    "britain": "United Kingdom",
    "hmtreasury": "United Kingdom",
    "supply_estimates": "United Kingdom",
    # ── Sweden ────────────────────────────────────────────────────────────────
    "sweden": "Sweden",
    "sverige": "Sweden",
    "statsbudget": "Sweden",       # Swedish state budget
    "riksdag": "Sweden",
    # ── Norway ────────────────────────────────────────────────────────────────
    "norway": "Norway",
    "norge": "Norway",
    "statsbudsjettet": "Norway",
    "stortinget": "Norway",
    # ── Finland ───────────────────────────────────────────────────────────────
    "finland": "Finland",
    "suomi": "Finland",
    "valtion_talousarvio": "Finland",
    "eduskunta": "Finland",
    # ── Netherlands ───────────────────────────────────────────────────────────
    "netherlands": "Netherlands",
    "nederland": "Netherlands",
    "rijksbegroting": "Netherlands",
    "tweede_kamer": "Netherlands",
    # ── Belgium ───────────────────────────────────────────────────────────────
    "belgium": "Belgium",
    "belgique": "Belgium",
    "belgie": "Belgium",
    "budget_federal": "Belgium",
    # ── Austria ───────────────────────────────────────────────────────────────
    "austria": "Austria",
    "osterreich": "Austria",
    "bundesbudget": "Austria",
    # ── Switzerland ───────────────────────────────────────────────────────────
    "switzerland": "Switzerland",
    "schweiz": "Switzerland",
    "suisse": "Switzerland",
    "bundeshaushalt_ch": "Switzerland",
    # ── Spain ─────────────────────────────────────────────────────────────────
    "spain": "Spain",
    "espana": "Spain",
    "presupuestos_generales": "Spain",
    # ── Italy ─────────────────────────────────────────────────────────────────
    "italy": "Italy",
    "italia": "Italy",
    "legge_di_bilancio": "Italy",
    # ── USA ───────────────────────────────────────────────────────────────────
    "united_states": "United States",
    "unitedstates": "United States",
    "federal_budget_us": "United States",
    # ── Canada ────────────────────────────────────────────────────────────────
    "canada": "Canada",
    "budget_canada": "Canada",
    # ── Australia ─────────────────────────────────────────────────────────────
    "australia": "Australia",
    "commonwealth_budget": "Australia",
    # ── Japan ─────────────────────────────────────────────────────────────────
    "japan": "Japan",
    "kokka_yosan": "Japan",
}


# ── Section name maps ─────────────────────────────────────────────────────────
# Danish Finanslov section numbers → English ministry names.
# Covers the 1970s–1990s structure (section numbers shifted over the decades;
# add new entries as you encounter them in PDFs).
DK_SECTION_MAP: dict[str, str] = {
    "§1":  "The Crown (Civil List)",
    "§2":  "Parliament (Folketing)",
    "§3":  "The Ombudsman",
    "§4":  "Court Administration",
    "§5":  "Prime Minister's Office",
    "§6":  "Ministry of Finance",
    "§7":  "Ministry of Taxation",
    "§8":  "Ministry of Economic Affairs",
    "§9":  "Ministry for Greenland",
    "§10": "Ministry of Foreign Affairs",
    "§11": "Ministry of Agriculture",
    "§12": "Ministry of Fisheries",
    "§13": "Ministry of the Interior",
    "§14": "Ministry of Housing",
    "§15": "Ministry of Social Affairs",
    "§16": "Ministry of Public Works",
    "§17": "Ministry of Commerce",
    "§18": "Ministry of Labour",
    "§19": "Ministry of Cultural Affairs",
    "§20": "Ministry of Education",
    "§21": "Ministry of Ecclesiastical Affairs",
    "§22": "Ministry of Defence",
    "§23": "Ministry of Justice",
    "§24": "Ministry of the Environment",
    "§25": "State Railways (DSB)",
    "§26": "Post and Telecommunications",
    "§27": "Ministry of the Environment",       # renamed/split in some years
    "§28": "Ministry of Labour",                # sometimes distinct
    "§29": "Ministry of Energy",
    "§30": "Ministry of Industry",
    "§31": "Ministry of Health",
    "§32": "Ministry of Science and Technology",
    "§33": "Ministry of Research",
    "§34": "Ministry of Education and Research",
    # Add more as encountered in later Finance Bills
}


# ── Language-aware keyword dictionaries ───────────────────────────────────────
KEYWORDS_BY_LANGUAGE = {
    "english": {
        "policy_terms": {
            "innovation",
            "research",
            "science",
            "technology",
            "r&d",
            "r and d",
            "university",
            "higher education",
            "tertiary education",
            "research funding",
            "research grant",
        },
        "ministry_terms": {
            "ministry of education",
            "ministry of science",
            "department for science",
            "research council",
            "science council",
            "innovation agency",
            "higher education ministry",
        },
    },
    "french": {
        "policy_terms": {
            "innovation",
            "recherche",
            "science",
            "technologie",
            "r&d",
            "universite",
            "enseignement superieur",
            "financement de la recherche",
            "subvention de recherche",
        },
        "ministry_terms": {
            "ministere de l'education",
            "ministere de la recherche",
            "ministere de la science",
            "conseil de la recherche",
            "agence d'innovation",
        },
    },
    "danish": {
        "policy_terms": {
            "innovation",
            "forskning",
            "videnskab",
            "teknologi",
            "fou",
            "universitet",
            "hojere uddannelse",
            "forskningsfinansiering",
            "forskningsbevilling",
        },
        "ministry_terms": {
            "uddannelsesministeriet",
            "forskningsministeriet",
            "videnskabsministeriet",
            "uddannelse og forskning",
            "forskningsrad",
            "innovationsfond",
        },
    },
}
