"""Load and cache the R&D budget taxonomy.

Primary source: Data/search_library.json (pre-processed, structured)
Fallback:       Data/Full search library.xlsx (original Excel)

JSON scoring mapping (implements J_Rules from sheet J):
  auto_include.keywords  → +2  (broad positive signals)
  institutions.keywords  → +2  (D-pillar: institution names)
  sectoral_rd.keywords   → +1  (E-pillar: sector-specific R&D)
  budget_terms.keywords  → +1  (F-pillar: budget instruments)
  DANISH_CORE_RD         → +3  (language extension: unambiguous R&D terms)
  ambiguous (anchored)   → +0.5
  ambiguous (bare)       → -2
  exclusions             → -3  (H-pillar false positives)

Include if score >= 3 | Review 1–2 | Skip <= 0
"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path

import pandas as pd

from budget.utils import normalize_text

_JSON_FILE = Path(__file__).resolve().parent.parent / "Data" / "input" / "taxonomy" / "search_library.json"
_EXCEL_FILE = Path(__file__).resolve().parent.parent / "Data" / "input" / "taxonomy" / "Full search library.xlsx"

# J_Rules scoring weights
SCORE_CORE_RD = 3        # unambiguous R&D phrase
SCORE_AUTO_INCLUDE = 2   # broad positive signal (auto_include pool)
SCORE_INSTITUTION = 2    # institution keyword
SCORE_SECTORAL = 1       # sector-specific R&D
SCORE_INSTRUMENT = 1     # budget instrument
SCORE_EXCLUSION = -3     # known false positive
SCORE_AMBIGUOUS_BARE = -2  # ambiguous without required anchor

INCLUDE_THRESHOLD = 3
REVIEW_THRESHOLD = 1

# ── Language extensions ───────────────────────────────────────────────────────
# Each country's native-language terms map to taxonomy categories.
# The taxonomy JSON is English-only; these additions cover source documents.
# Add new languages in src/languages/<lang>.py and register here.

_LANGUAGE_EXTENSIONS: dict[str, dict[str, frozenset[str]]] = {
    "danish": {
        "core_rd": frozenset({
            "forskning", "videnskab", "forskning og udvikling", "fou",
            "grundforskning", "anvendt forskning", "forsøgsvirksomhed",
            "teknologisk forskning", "videnskabelig forskning",
            "eksperimentel udvikling",
        }),
        "institutions": frozenset({
            "universitetet", "universiteter", "universiteterne",
            "hojere laereanstalt", "hojere laereanstalter",
            "polyteknisk laereanstalt",
            "forskningsrad", "forskningsradet", "statens forskningsrad",
            "videnskabsministeriet", "forskningsministeriet",
            "uddannelsesministeriet", "undervisningsministeriet",
            "statens teknisk-videnskabelige",
            "danmarks tekniske hojskole", "danmarks tekniske universitet",
            "dth", "dtu",
            "laboratorium", "laboratoriet",
            "innovationsfond", "innovationsfonden",
            "akademiet for de tekniske videnskaber",
            "atomenergikommissionen", "atomenergi",
        }),
        "instruments": frozenset({
            "forskningsbevilling", "forskningsbevillinger",
            "tilskud til forskning",
        }),
    },
    "french": {
        "core_rd": frozenset({
            "recherche", "recherche et developpement", "r&d",
            "recherche fondamentale", "recherche appliquee",
            "recherche scientifique", "developpement experimental",
        }),
        "institutions": frozenset({
            "universite", "universites", "cnrs", "inria", "inserm",
            "ecole polytechnique", "grandes ecoles",
            "conseil de la recherche", "agence nationale de la recherche",
            "anr", "ministere de la recherche", "ministere de l education",
            "laboratoire", "laboratoires nationaux",
        }),
        "instruments": frozenset({
            "credit", "credits", "dotation", "subvention de recherche",
            "financement de la recherche",
        }),
    },
    "german": {
        "core_rd": frozenset({
            "forschung", "forschung und entwicklung", "fou",
            "grundlagenforschung", "angewandte forschung",
            "wissenschaft", "technologieentwicklung",
        }),
        "institutions": frozenset({
            "universitat", "universitaten", "hochschule", "hochschulen",
            "forschungsrat", "deutsche forschungsgemeinschaft", "dfg",
            "fraunhofer", "max-planck", "helmholtz",
            "bundesministerium fur bildung und forschung", "bmbf",
            "wissenschaftsrat",
        }),
        "instruments": frozenset({
            "forderung", "forschungsforderung", "zuweisung",
            "forschungsausgaben",
        }),
    },
    "swedish": {
        "core_rd": frozenset({
            "forskning", "forskning och utveckling", "fou",
            "grundforskning", "tillampad forskning", "vetenskap",
        }),
        "institutions": frozenset({
            "universitet", "hogskola", "vetenskapsradet",
            "vinnova", "riksdagen", "kungliga tekniska hogskolan", "kth",
        }),
        "instruments": frozenset({
            "anslag", "bidrag till forskning", "forskningsanslag",
        }),
    },
    "norwegian": {
        "core_rd": frozenset({
            "forskning", "forskning og utvikling", "fou",
            "grunnforskning", "anvendt forskning",
        }),
        "institutions": frozenset({
            "universitet", "norges forskningsrad", "forskningsradet",
            "ntnu", "universitetet i oslo",
        }),
        "instruments": frozenset({
            "bevilgning", "forskningsmidler", "tilskudd til forskning",
        }),
    },
    "finnish": {
        "core_rd": frozenset({
            "tiede", "tutkimus", "tiede ja tutkimus",
            "tutkimus ja kehitys", "tutkimus- ja kehitys",
            "tutkimus-, kehittamis- ja innovaatiotoiminta",
            "perustutkimus", "soveltava tutkimus",
            "tieteellinen tutkimus", "teknologian kehittaminen",
            "innovaatiotoiminta", "tki", "tki-toiminta",
        }),
        "institutions": frozenset({
            "suomen akatemia", "finlands akademi",
            "tekes", "teknologian kehittamiskeskus",
            "business finland", "innovaatiorahoituskeskus business finland",
            "innovationsfinansieringsverket business finland",
            "opetusministerio", "opetus- ja kulttuuriministerio",
            "kauppa- ja teollisuusministerio",
            "tyo- ja elinkeinoministerio",
            "yliopisto", "yliopistot", "korkeakoulu", "korkeakoulut",
            "tiedeakatemia", "tutkimuslaitos", "tutkimuskeskus",
        }),
        "instruments": frozenset({
            "maararaha", "tutkimusmaararahat", "toimintamenot",
            "avustukset", "lainat", "voittovarat",
            "tieteen edistamiseen", "tieteen tukemiseen",
            "tukeminen", "rahoitus", "anslag", "omkostnader",
            "forskningsanslag",
        }),
    },
    "japanese": {
        "core_rd": frozenset({
            "研究", "科学", "科学技術", "科学技術振興",
            "研究開発", "学術", "学術研究", "基礎研究",
            "応用研究", "実験開発", "科学研究費",
        }),
        "institutions": frozenset({
            "文部科学省", "文部省", "科学技術庁",
            "日本学術会議", "日本学術振興会", "科学技術振興機構",
            "理化学研究所", "宇宙航空研究開発機構",
            "大学", "国立大学", "研究所", "研究機構",
        }),
        "instruments": frozenset({
            "所管合計", "歳出合計", "計", "運営費",
            "施設整備費", "補助金", "交付金", "振興費",
        }),
    },
}

_SKIP_TERMS: frozenset[str] = frozenset({
    "nan", "notes", "variants to include", "exact keyword / phrase",
    "category", "subpillar", "use", "type", "caution / note",
    "strong", "medium", "useful", "very strong", "very useful",
    "medium-strong", "medium alone", "adjacent", "weak",
})


class Taxonomy:
    __slots__ = (
        "core_rd", "auto_include", "institutions", "sectoral",
        "instruments", "ambiguous", "ambiguous_anchors",
        "ambiguous_exclude", "exclusions",
    )

    def __init__(self) -> None:
        self.core_rd: frozenset[str] = frozenset()
        self.auto_include: frozenset[str] = frozenset()
        self.institutions: frozenset[str] = frozenset()
        self.sectoral: frozenset[str] = frozenset()
        self.instruments: frozenset[str] = frozenset()
        self.ambiguous: frozenset[str] = frozenset()
        self.ambiguous_anchors: dict[str, frozenset[str]] = {}
        self.ambiguous_exclude: dict[str, frozenset[str]] = {}
        self.exclusions: frozenset[str] = frozenset()


def _norm_set(items: list[str]) -> frozenset[str]:
    out: set[str] = set()
    for item in items:
        t = normalize_text(str(item).strip())
        if t and len(t) > 1 and t not in _SKIP_TERMS:
            out.add(t)
    return frozenset(out)


def _load_from_json() -> Taxonomy:
    """Load taxonomy from JSON (preferred source)."""
    with open(_JSON_FILE, encoding="utf-8") as f:
        data = json.load(f)

    tax = Taxonomy()
    tax.auto_include = _norm_set(data.get("auto_include", {}).get("keywords", []))
    tax.institutions = _norm_set(data.get("institutions", {}).get("keywords", []))
    tax.sectoral = _norm_set(data.get("sectoral_rd", {}).get("keywords", []))
    tax.instruments = _norm_set(data.get("budget_terms", {}).get("keywords", []))
    tax.exclusions = _norm_set(data.get("exclusions", {}).get("keywords", []))

    # Ambiguous terms: richer structure in JSON with require_anchor + exclude_if_near
    amb_data = data.get("ambiguous", {}).get("terms", {})
    amb_set: set[str] = set()
    anchors: dict[str, frozenset[str]] = {}
    excl_ctx: dict[str, frozenset[str]] = {}
    for term_raw, meta in amb_data.items():
        t = normalize_text(term_raw.strip())
        if not t or len(t) <= 1:
            continue
        amb_set.add(t)
        if isinstance(meta, dict):
            if meta.get("require_anchor"):
                anchors[t] = frozenset(normalize_text(a) for a in meta["require_anchor"] if a)
            if meta.get("exclude_if_near"):
                excl_ctx[t] = frozenset(normalize_text(e) for e in meta["exclude_if_near"] if e)

    tax.ambiguous = frozenset(amb_set)
    tax.ambiguous_anchors = anchors
    tax.ambiguous_exclude = excl_ctx
    tax.core_rd = frozenset()  # populated by language extensions
    return tax


def _load_from_excel() -> Taxonomy:
    """Fallback: load taxonomy from Excel."""
    from budget.taxonomy_excel import load_from_excel  # late import to avoid hard dependency
    return load_from_excel(_EXCEL_FILE)


def _apply_language_extensions(tax: Taxonomy, languages: list[str]) -> Taxonomy:
    """Merge native-language keyword extensions into the taxonomy."""
    extra_core: set[str] = set()
    extra_inst: set[str] = set()
    extra_instr: set[str] = set()

    for lang in languages:
        ext = _LANGUAGE_EXTENSIONS.get(lang, {})
        extra_core.update(ext.get("core_rd", frozenset()))
        extra_inst.update(ext.get("institutions", frozenset()))
        extra_instr.update(ext.get("instruments", frozenset()))

    tax.core_rd = frozenset(normalize_text(t) for t in extra_core if t)
    tax.institutions = frozenset(tax.institutions | frozenset(normalize_text(t) for t in extra_inst))
    tax.instruments = frozenset(tax.instruments | frozenset(normalize_text(t) for t in extra_instr))
    return tax


@lru_cache(maxsize=1)
def load_taxonomy(languages: tuple[str, ...] = ("danish",)) -> Taxonomy:
    """Load and cache the taxonomy. Extends with native-language keywords.

    Args:
        languages: tuple of language names to include extensions for.
                   Defaults to ("danish",) for current Denmark focus.
                   Example: ("danish", "french") for multi-country runs.
    """
    try:
        tax = _load_from_json()
    except Exception:
        tax = _load_from_excel()

    tax = _apply_language_extensions(tax, list(languages))
    return tax


def _term_in(term: str, norm: str) -> bool:
    """Check if `term` appears in `norm` as a whole word.

    Short terms (≤ 3 chars, e.g. 'sti', 'ip', 'pro') are matched with word
    boundaries so they don't hit as substrings inside longer words.
    Longer terms use simple substring search (faster).
    """
    if len(term) <= 3:
        return bool(re.search(r"(?<![a-z])" + re.escape(term) + r"(?![a-z])", norm))
    return term in norm


def score_text(text: str, tax: Taxonomy | None = None) -> tuple[float, list[str], str]:
    """Score text using J_Rules weights.

    Returns (score, matched_terms, category).
    Include if score >= INCLUDE_THRESHOLD; review 1 <= score < INCLUDE_THRESHOLD.

    Context window tip: pass section_name + line_description + neighbors
    to get ministry-level context, not just the bare line.
    """
    if tax is None:
        tax = load_taxonomy()

    norm = normalize_text(text)
    score = 0.0
    hits: list[str] = []

    # Exclusions: hard stop unless a native core R&D term is also present
    excl_hits = [t for t in tax.exclusions if _term_in(t, norm)]
    has_core_override = any(_term_in(t, norm) for t in tax.core_rd)
    if excl_hits and not has_core_override:
        return float(SCORE_EXCLUSION * len(excl_hits)), excl_hits, "excluded"

    # Check ambiguous terms' exclude_if_near context (J3 rules)
    for t in tax.ambiguous:
        if _term_in(t, norm):
            excl_ctx = tax.ambiguous_exclude.get(t, frozenset())
            if excl_ctx and any(_term_in(e, norm) for e in excl_ctx):
                score += SCORE_EXCLUSION
                hits.append(f"{t}(-context)")

    # Native-language core R&D: +3 (unambiguous, language-specific)
    for t in tax.core_rd:
        if _term_in(t, norm) and t not in hits:
            score += SCORE_CORE_RD
            hits.append(t)

    # Auto-include pool: +2 (broad positive signals from English taxonomy)
    for t in tax.auto_include:
        if _term_in(t, norm) and t not in hits:
            score += SCORE_AUTO_INCLUDE
            hits.append(t)

    # Institutions: +2 (D-pillar)
    for t in tax.institutions:
        if _term_in(t, norm) and t not in hits:
            score += SCORE_INSTITUTION
            hits.append(t)

    # Sectoral R&D: +1 (E-pillar)
    for t in tax.sectoral:
        if _term_in(t, norm) and t not in hits:
            score += SCORE_SECTORAL
            hits.append(t)

    # Budget instruments: +1 (F-pillar)
    for t in tax.instruments:
        if _term_in(t, norm) and t not in hits:
            score += SCORE_INSTRUMENT
            hits.append(t)

    # Ambiguous: anchored → +0.5, bare → -2 (G-pillar / J2-J3 rules)
    for t in tax.ambiguous:
        if _term_in(t, norm) and t not in hits and f"{t}(-context)" not in hits:
            anchors = tax.ambiguous_anchors.get(t, frozenset())
            if anchors and any(_term_in(a, norm) for a in anchors):
                score += 0.5
                hits.append(f"{t}(+anchor)")
            else:
                score += SCORE_AMBIGUOUS_BARE

    # Category (J1 priority order)
    if any(_term_in(t, norm) for t in tax.core_rd):
        cat = "direct_rd"
    elif any(_term_in(t, norm) for t in tax.institutions) and any(_term_in(t, norm) for t in tax.instruments):
        cat = "institution_funding"
    elif any(_term_in(t, norm) for t in tax.sectoral):
        cat = "sectoral_rd"
    elif hits:
        cat = "possible_rd"
    else:
        cat = "not_rd"

    return round(score, 2), hits, cat
