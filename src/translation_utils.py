"""Glossary-based translation for key budget fields shown to non-Danish readers.

Only used on short fields: section_name_en and line_description_en in the
budget output. NOT applied to full document text (unnecessary and slow).

Coverage priority:
  1. Danish ministry names (§ section headers)
  2. Research institutions and funding bodies
  3. Budget line types (Driftsudgifter, Tilskud, etc.)
  4. Common descriptive terms in line items
"""

import json
import re
from functools import lru_cache
from pathlib import Path

from src.config import TRANSLATION_GLOSSARY_FILE

# ── Core glossary (built-in default) ─────────────────────────────────────────
# Order matters: longer / more specific patterns come first to avoid
# partial replacements. Each entry: (pattern, english_replacement)

DEFAULT_GLOSSARY: list[tuple[str, str]] = [

    # ── Ministry names (§ sections) ──────────────────────────────────────────
    (r"\bUndervisningsministeriet\b",           "Ministry of Education"),
    (r"\bForskningsministeriet\b",              "Ministry of Research"),
    (r"\bVidenskabsministeriet\b",              "Ministry of Science"),
    (r"\bUddannelsesministeriet\b",             "Ministry of Education and Research"),
    (r"\bUddannelse og [Ff]orskning\b",         "Education and Research"),
    (r"\bMilj[øo]ministeriet\b",               "Ministry of the Environment"),
    (r"\bSocialministeriet\b",                  "Ministry of Social Affairs"),
    (r"\bForsvarsministeriet\b",                "Ministry of Defence"),
    (r"\bFinansministeriet\b",                  "Ministry of Finance"),
    (r"\b[ØO]konomiministeriet\b",             "Ministry of Economic Affairs"),
    (r"\bHandelsministeriet\b",                 "Ministry of Commerce"),
    (r"\bJustitsministeriet\b",                 "Ministry of Justice"),
    (r"\bIndenrigsministeriet\b",               "Ministry of the Interior"),
    (r"\bUdenrigsministeriet\b",                "Ministry of Foreign Affairs"),
    (r"\bKirkeministeriet\b",                   "Ministry of Ecclesiastical Affairs"),
    (r"\bBoligministeriet\b",                   "Ministry of Housing"),
    (r"\bLandbrugsministeriet\b",               "Ministry of Agriculture"),
    (r"\bFiskeriministeri\w+\b",                "Ministry of Fisheries"),
    (r"\bMinisteriet for [Oo]ffentlige [Aa]rbejder\b", "Ministry of Public Works"),
    (r"\bMinisteriet for [Kk]ulturelle [Aa]nliggender\b", "Ministry of Cultural Affairs"),
    (r"\bMinisteriet for [Ss]katter og [Aa]fgifter\b", "Ministry of Taxation"),
    (r"\bMinisteriet for [Gg]r[øo]nland\b",   "Ministry for Greenland"),
    (r"\bArb[ej]+dsministeriet\b",             "Ministry of Labour"),
    (r"\bStatsministeriet\b",                   "Prime Minister's Office"),
    (r"\bPensionsvæsenet\b",                    "Pension Administration"),
    (r"\bPensionsvaesenet\b",                   "Pension Administration"),

    # ── Research institutions ─────────────────────────────────────────────────
    (r"\bStatens [Tt]eknisk-[Vv]idenskabelige [Ff]orskningsfond\b",
        "State Technical-Scientific Research Fund"),
    (r"\bStatens [Nn]aturvidenskabelige [Ff]orskningsr[åa]d\b",
        "State Natural Science Research Council"),
    (r"\bStatens [Hh]umanistiske [Ff]orskningsr[åa]d\b",
        "State Humanities Research Council"),
    (r"\bStatens [Ss]amfundsvidenskabelige [Ff]orskningsr[åa]d\b",
        "State Social Science Research Council"),
    (r"\bStatens [Ll][æa]gevidenskabelige [Ff]orskningsr[åa]d\b",
        "State Medical Research Council"),
    (r"\bStatens [Jj]ordbrugs- og [Vv]eterinerf[øo]rskning\b",
        "State Agricultural and Veterinary Research"),
    (r"\bDanmarks [Tt]ekniske H[øo]jskole\b",         "Technical University of Denmark"),
    (r"\bDanmarks [Tt]ekniske [Uu]niversitet\b",     "Technical University of Denmark"),
    (r"\bPolyteknisk L(?:ae|æ|a)reanstalt\b",        "Polytechnic Institute"),
    # Handle OCR variants: ø→o, æ→ae or a, å→aa
    (r"\b[Uu]niversiteter og [Hh][øo]jere [Ll](?:ae|æ|a)reanstalter\b",
        "Universities and Higher Education Institutions"),
    (r"\b[Hh][øo]jere [Ll](?:ae|æ|a)reanstalter?\b", "Higher Education Institutions"),
    (r"\bInnovationsfonden\b",                        "Innovation Fund Denmark"),
    (r"\bForskningsr[åa]det\b",                       "Research Council"),
    (r"\bForskningsr[åa]d\b",                         "Research Council"),
    (r"\bStatens [Ff]orskningsr[åa]d\b",             "National Research Council"),
    (r"\bAkademiet for de [Tt]ekniske [Vv]idenskaber\b",
        "Danish Academy of Technical Sciences"),
    (r"\bRis[øo] [Nn]ational[Ll]aboratoriet?\b",     "Risø National Laboratory"),
    (r"\bDanmarks [Mm]eteorologi Institut\b",         "Danish Meteorological Institute"),
    (r"\bCopenhagen [Uu]niversit\w+\b",               "University of Copenhagen"),
    (r"\bAarhus [Uu]niversit\w+\b",                   "Aarhus University"),

    # ── Universities (generic patterns) ──────────────────────────────────────
    (r"\bUniversitets?et\b",                          "University"),
    (r"\bUniversiteter\b",                            "Universities"),
    (r"\bUniversiteterne\b",                          "Universities"),

    # ── Budget line types (most frequent in line descriptions) ───────────────
    (r"\bDriftsudgifter\b",                           "Operating expenditures"),
    (r"\bDriftsbudget\b",                             "Operating budget"),
    (r"\bDriftsbevillingen?\b",                       "Operating appropriation"),
    (r"\bAnl[æa]gsudgifter\b",                       "Capital expenditures"),
    (r"\bAnl[æa]gsbudget\b",                         "Capital budget"),
    (r"\bAnl[æa]gstilskud\b",                        "Capital grants"),
    (r"\bAnl[æa]gsbevillingen?\b",                   "Capital appropriation"),
    (r"\bEjendomserhvervelser\b",                     "Property acquisitions"),
    (r"\bKapitalindt[æa]gter\b",                     "Capital revenues"),
    (r"\bIndt[æa]gter\b",                            "Revenues"),
    (r"\bUdgifter\b",                                 "Expenditures"),
    (r"\bNettotal\b",                                 "Net total"),
    (r"\bTilskud\b",                                  "Grants"),
    (r"\bBevillingen?\b",                             "Appropriation"),
    (r"\bBevillinger\b",                              "Appropriations"),
    (r"\bForskningsbevillingen?\b",                   "Research appropriation"),
    (r"\bForskningsbevillinger\b",                    "Research appropriations"),
    (r"\bUdl[åa]n\b",                                "Lending"),
    (r"\bL[øo]nninger\b",                            "Salaries"),
    (r"\bPensioner\b",                                "Pensions"),
    (r"\bVedligeholdelse\b",                          "Maintenance"),
    (r"\bAnskaffelser\b",                             "Acquisitions"),
    (r"\bFaste anl[æa]g\b",                          "Fixed assets"),
    (r"\bNyanl[æa]g\b",                              "New construction"),
    (r"\bRullende materiel\b",                        "Rolling stock"),

    # ── Research and science terms ────────────────────────────────────────────
    (r"\bForskning og [Uu]dvikling\b",               "Research and Development"),
    (r"\bForskningsvirksomhed\b",                     "Research activities"),
    (r"\bVidenskabelig [Vv]irksomhed\b",             "Scientific activities"),
    (r"\bForsøgsvirksomhed\b",                        "Experimental activities"),
    (r"\bTeknologisk [Uu]dvikling\b",                "Technological development"),
    (r"\bGrundforskning\b",                           "Basic research"),
    (r"\bAnvendt [Ff]orskning\b",                    "Applied research"),
    (r"\bKerneforskning\b",                           "Nuclear research"),
    (r"\bKerneenergi\b",                              "Nuclear energy"),
    (r"\bKernekraft\b",                               "Nuclear power"),
    (r"\bEnergi[Ff]orskning\b",                      "Energy research"),
    (r"\bBygge[Ff]orskning\b",                        "Building research"),
    (r"\bByplan[Ff]orskning\b",                       "Urban planning research"),
    (r"\bUddannelses[Ff]orskning\b",                 "Education research"),
    (r"\bLandbrugs[Ff]orskning\b",                   "Agricultural research"),
    (r"\bMiljø[Ff]orskning\b",                       "Environmental research"),
    (r"\bSamfunds[Ff]orskning\b",                    "Social science research"),
    (r"\bBiologi[Ff]orskning\b",                     "Biology research"),
    (r"\bBiologi[Ff]orskningsprogram\w*\b",          "Biology research programme"),
    (r"\bKomplementær\w*\b",                          "complementary"),
    (r"\b[Ee]fter[Ff]orskning\b",                    "Exploration"),
    (r"\bForskning\b",                                "Research"),
    (r"\bVidenskab\b",                                "Science"),
    (r"\bTeknologi\b",                                "Technology"),
    (r"\bInnovation\b",                               "Innovation"),

    # ── Research line item descriptions ──────────────────────────────────────
    (r"\bForskerstipendier?\b",                       "Research fellowships"),
    (r"\bKandidatstipendier?\b",                      "Graduate fellowships"),
    (r"\bScholarstipendie[rn]?\b",                    "Scholar fellowships"),
    (r"\bStipendier?\b",                              "Fellowships"),
    (r"\bDoktordisputats\w*\b",                       "Doctoral dissertations"),
    (r"\bUdbygning\b",                                "Expansion"),
    (r"\bUdvikling\b",                                "Development"),
    (r"\bBidrag\b",                                   "Contribution"),
    (r"\bDansk[et]?\b",                               "Danish"),
    (r"\bGeotermi\w*\b",                              "Geothermal"),
    (r"\b[Ee]nergi\b",                                "Energy"),
    (r"\b[Aa]tom\b",                                  "Atomic/Nuclear"),
    (r"\bAtomenergik\w+\b",                           "Atomic Energy Commission"),
    (r"\bFysik\b",                                    "Physics"),
    (r"\bKemi\b",                                     "Chemistry"),
    (r"\bBiologi\b",                                  "Biology"),
    (r"\bMedicin\b",                                  "Medicine"),
    (r"\bIngeni[øo]r\w*\b",                          "Engineering"),
    (r"\bAlmindelig\b",                               "General"),
    (r"\bVirksomhed\b",                               "Activities"),
    (r"\bProgrammet?\b",                              "Programme"),
    (r"\bInstitut(?:tet)?\b",                         "Institute"),
    (r"\bCenter\b",                                   "Centre"),
    (r"\bCenter for\b",                               "Centre for"),
    (r"\bKongresser?\b",                              "Conferences"),
    (r"\bKonference[rn]?\b",                          "Conferences"),
    (r"\bForsøg\b",                                   "Experiments"),
    (r"\bAfgifter\b",                                 "Duties/Fees"),
    (r"\bDrift\b",                                    "Operations"),
    (r"\bDriftstilskud\b",                            "Operating grant"),
    (r"\bDrifts\b",                                   "Operating"),
    (r"\bUdenlandsk\w*\b",                            "Foreign"),
    (r"\bAfholdt\b",                                  "held"),
    (r"\bByggeforskningsinstitut\w*\b",               "Building Research Institute"),
    (r"\bBygge[Ff]orskning\b",                        "Building research"),

    # ── Named centres commonly appearing in lines ───────────────────────────
    (r"\bForskningscentret\b",                        "Research Centre"),

    # ── Small glue words that often stay Danish in short phrases ───────────
    (r"\bog\b",                                       "and"),
    (r"\bved\b",                                      "at"),

    # ── International organisations (common in Danish R&D budget lines) ───────
    (r"\bEF[s']?\b",                                  "EC"),        # European Community (pre-EU)
    (r"\bEuroatom\b",                                 "Euratom"),
    (r"\bEurochemic\b",                               "Eurochemic"),
    (r"\bNordisk\b",                                  "Nordic"),
    (r"\bInternationale?\b",                          "International"),
    (r"\bF[øo]lles[Ee]uropæiske?\b",                 "Pan-European"),
    (r"\bF[øo]lles\b",                                "Joint/Common"),
    (r"\bOECD\b",                                     "OECD"),
    (r"\bFN\b",                                       "UN"),
    (r"\bNATO\b",                                     "NATO"),
    (r"\bCERN\b",                                     "CERN"),
    (r"\bIAEA\b",                                     "IAEA"),
    (r"\bEMBO\b",                                     "EMBO"),
    (r"\bESA\b",                                      "ESA"),
    (r"\bESRO\b",                                     "ESRO"),
    (r"\bISLand\b",                                   "Iceland"),
    (r"\bHalden\b",                                   "Halden (NO)"),
    (r"\bTrieste\b",                                  "Trieste (IT)"),
    (r"\bK[øo]benhavn\w*\b",                         "Copenhagen"),

    # ── Education terms ───────────────────────────────────────────────────────
    (r"\bUddannelse\b",                               "Education"),
    (r"\bH[øo]jere [Uu]ddannelse\b",                "Higher education"),
    (r"\bGymnas\w+\b",                               "Upper secondary schools"),
    (r"\bFolkeh[øo]jskole\b",                        "Folk high school"),
    (r"\bLandbrugsskole\b",                           "Agricultural school"),
    (r"\bL(?:ae|æ|a)reanstalt\b",                    "Educational institution"),

    # ── Government/administrative terms ──────────────────────────────────────
    (r"\bStatens\b",                                  "State"),
    (r"\bStatsvirksomhederne\b",                      "State enterprises"),
    (r"\bStatsbaner\w*\b",                            "State railways"),
    (r"\bStatsbanerne\b",                             "State railways"),
    (r"\bFolketinget\b",                              "Parliament"),
    (r"\bDronningen\b",                               "The Queen"),
    (r"\bDet kgl\. hus\b",                           "The Royal House"),
    (r"\bRentekonto\b",                               "Interest account"),
    (r"\bDepartementet\b",                            "Department (ministry HQ)"),
    (r"\bMinisteriets?\b",                            "Ministry"),

    # ── Common abbreviations / noise ─────────────────────────────────────────
    (r"\bm\. v\.\b",                                  "etc."),
    (r"\bm\.v\.\b",                                   "etc."),
    (r"\bf\.eks\.\b",                                 "e.g."),
    (r"\bjf\.\b",                                     "cf."),

    # ── French terms (for future multi-country use) ───────────────────────────
    (r"\brecherche\b",                                "research"),
    (r"\btechnologie\b",                              "technology"),
    (r"\buniversit[eé]\b",                           "university"),
    (r"\benseignement sup[eé]rieur\b",               "higher education"),
    (r"\bministère\b",                                "ministry"),
    (r"\bdotation\b",                                 "appropriation"),
    (r"\bcr[eé]dit\b",                               "credit/appropriation"),
]


def translate_key_fields(text: str) -> str:
    """Translate Danish budget terms in a short field (section name, line description).

    Applies the glossary in order; longer patterns first to avoid partial hits.
    Returns the translated string, preserving any untranslated text as-is.
    """
    if not text:
        return ""
    result = normalize_for_translation(text)
    for pattern, replacement in _load_glossary():
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
    return result


# Keep the old name for backward compatibility with existing imports
def translate_to_english_glossary(text: str) -> str:
    """Alias for translate_key_fields — use on short fields only, not full pages."""
    return translate_key_fields(text)


def normalize_for_translation(text: str) -> str:
    """Fix hyphen/newline splits and collapse whitespace before translation."""
    if not isinstance(text, str):
        return ""
    text = re.sub(r"(\w+)-\s+(\w+)", r"\1\2", text)
    text = re.sub(r"[\r\n\t]+", " ", text)
    text = re.sub(r"\s{2,}", " ", text).strip()
    return text


def preclean_text(text: str) -> str:
    """Apply normalization plus heal common OCR-split ministry names."""
    cleaned = normalize_for_translation(text)
    no_space = re.sub(r"\\s+", "", cleaned).lower()
    # Heuristics on space-stripped tokens
    if "undervis" in no_space:
        cleaned = "Undervisningsministeriet"
    elif "handels" in no_space:
        cleaned = "Handelsministeriet"
    elif "indenrig" in no_space:
        cleaned = "Indenrigsministeriet"
    elif "landbrugs" in no_space:
        cleaned = "Landbrugsministeriet"
    elif "boligmini" in no_space:
        cleaned = "Boligministeriet"
    elif "gronland" in no_space or "grønland" in no_space:
        cleaned = "Ministeriet for Grønland"
    replacements = [
        (r"U\\s*nd\\s*er\\s*vis\\s*nings\\s*ministeriet", "Undervisningsministeriet"),
        (r"Ha\\s*nd\\s*els\\s*ministeriet", "Handelsministeriet"),
        (r"In\\s*den\\s*rigs\\s*ministeriet", "Indenrigsministeriet"),
        (r"Lan\\s*dbrugs\\s*ministeriet", "Landbrugsministeriet"),
        (r"Bo\\s*lig\\s*ministeriet", "Boligministeriet"),
        (r"Mi\\s*nisteriet\\s*for\\s*Gr[oø]nland", "Ministeriet for Grønland"),
    ]
    for pattern, repl in replacements:
        cleaned = re.sub(pattern, repl, cleaned, flags=re.IGNORECASE)
    # Collapse letter-by-letter runs (common OCR split)
    cleaned = re.sub(r"(?<=[A-Za-zÆØÅæøå])\\s+(?=[A-Za-zÆØÅæøå])", "", cleaned)
    # Insert spaces before section symbols/numbers stuck to text
    cleaned = re.sub(r"§", " § ", cleaned)
    cleaned = re.sub(r"(?<=[A-Za-zÆØÅæøå])(?=\\d)", " ", cleaned)
    cleaned = re.sub(r"\\s{2,}", " ", cleaned).strip()
    return cleaned


@lru_cache(maxsize=1)
def _load_glossary() -> list[tuple[str, str]]:
    """Load glossary from JSON file if present; fall back to built-in default."""
    path = Path(TRANSLATION_GLOSSARY_FILE)
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            glossary = [
                (item["pattern"], item["replacement"])
                for item in data
                if isinstance(item, dict) and "pattern" in item and "replacement" in item
            ]
            if glossary:
                return glossary
        except Exception:
            pass
    return DEFAULT_GLOSSARY
