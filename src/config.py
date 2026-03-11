"""Central configuration for the PDF processing pipeline."""

from pathlib import Path


# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PDF_ROOT = PROJECT_ROOT / "data" / "pdf"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
INTERMEDIATE_DIR = PROCESSED_DIR / "intermediate"
FULLTEXT_DIR = PROCESSED_DIR / "full_text"
FULLTEXT_EN_DIR = PROCESSED_DIR / "full_text_en"

FILE_INVENTORY_FILE = PROCESSED_DIR / "file_inventory.csv"
PAGE_EXTRACTION_FILE = INTERMEDIATE_DIR / "page_text.csv"
PER_FILE_SUMMARY_FILE = INTERMEDIATE_DIR / "file_text_summary.csv"
KEYWORD_HITS_FILE = INTERMEDIATE_DIR / "keyword_hits.csv"
FULLTEXT_EXPORT_MANIFEST_FILE = INTERMEDIATE_DIR / "full_text_exports.csv"
FULLTEXT_EN_EXPORT_MANIFEST_FILE = INTERMEDIATE_DIR / "full_text_en_exports.csv"
CANDIDATES_FILE = PROCESSED_DIR / "innovation_candidates.csv"
CANDIDATE_PAGES_FILE = PROCESSED_DIR / "candidate_pages_detected.csv"
BUDGET_ITEMS_FILE = PROCESSED_DIR / "budget_items_detected.csv"
RESULTS_FILE = PROCESSED_DIR / "results.txt"
RESULTS_JSON_FILE = PROCESSED_DIR / "results.json"
RESULTS_EXCEL_FILE = PROCESSED_DIR / "results.xlsx"
RUNS_DIR = PROCESSED_DIR / "runs"


# OCR and extraction settings
MIN_DIRECT_TEXT_CHARS = 120
MIN_ALNUM_RATIO = 0.25
OCR_ZOOM = 2.0
TESSERACT_LANGS = "eng+fra+dan"


# Country inference map (token -> country)
COUNTRY_TOKEN_MAP = {
    "denmark": "Denmark",
    "danmark": "Denmark",
    "vedtaget": "Denmark",
    "folketing": "Denmark",
    "finanslov": "Denmark",
    "dk": "Denmark",
    "france": "France",
    "fr": "France",
    "french": "France",
    "united_kingdom": "United Kingdom",
    "uk": "United Kingdom",
    "england": "United Kingdom",
    "germany": "Germany",
    "deutschland": "Germany",
    "de": "Germany",
    "sweden": "Sweden",
    "sverige": "Sweden",
    "norway": "Norway",
    "norge": "Norway",
    "finland": "Finland",
    "suomi": "Finland",
    "netherlands": "Netherlands",
    "nederland": "Netherlands",
    "belgium": "Belgium",
    "spain": "Spain",
    "espana": "Spain",
    "italy": "Italy",
}


# Language-aware keyword dictionaries
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
