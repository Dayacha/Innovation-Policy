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
        # Unit varies by era and table type — see country_profiles.py year_notes for
        # per-year instructions. Science chapter tables (2003-2009) use £ million;
        # DEL/Spending Review tables (2010+) use £ billion.
        # UNIT RULE: varies by era. Injected verbatim into the extraction prompt.
        # DO NOT write "see year_notes" here — the LLM interprets that literally
        # and defaults to 'thousand'. State the rule explicitly.
        "unit_hint": (
            "ERA-DEPENDENT UNITS — YOU MUST USE THE CORRECT UNIT: "
            "(a) Science chapter tables 2003-2009: the table header says '£ million'. "
            "A value like '5,397' means £5,397 MILLION. Set unit='million'. "
            "(b) DEL / Spending Review tables 2010+: amounts stated as '£X billion' or "
            "in billion-scale DEL tables. Set unit='billion'. "
            "(c) Narrative policy announcements: use the unit as stated in the sentence "
            "('£300 million' → unit='million'; '£1.6 billion' → unit='billion'). "
            "NEVER default to unit='thousand' for UK budget documents."
        ),
        "known_agencies": [
            # Pre-2000 era
            "Office of Science and Technology (OST)",
            "Department of Trade and Industry (DTI)",
            # 2000s era
            "Department for Innovation, Universities and Skills (DIUS)",
            "Research Councils UK (RCUK)",
            "Higher Education Funding Council for England (HEFCE)",
            "Technology Strategy Board (TSB)",
            # 2010s era — BIS
            "Department for Business, Innovation and Skills (BIS)",
            # 2016+ — BEIS
            "Department for Business, Energy and Industrial Strategy (BEIS)",
            # 2018+ — UKRI and research councils
            "UK Research and Innovation (UKRI)",
            "Medical Research Council (MRC)",
            "Engineering and Physical Sciences Research Council (EPSRC)",
            "Natural Environment Research Council (NERC)",
            "Science and Technology Facilities Council (STFC)",
            "Biotechnology and Biological Sciences Research Council (BBSRC)",
            "Economic and Social Research Council (ESRC)",
            "Arts and Humanities Research Council (AHRC)",
            "Innovate UK",
            "Research England",
            # 2021+ — ARIA
            "Advanced Research and Invention Agency (ARIA)",
            # 2023+ — DSIT
            "Department for Science, Innovation and Technology (DSIT)",
        ],
        # Bodies whose section_totals should NOT be marked 'include' (too broad)
        "mixed_ministries": [
            "Department of Health", "NHS", "Ministry of Defence",
            "Department for Education", "Department for Work and Pensions",
            "HM Treasury", "Home Office", "Ministry of Justice",
            "Department for Transport", "Department for Communities",
            "Department for Environment", "Foreign and Commonwealth Office",
            "Department for Business and Trade (DBT)",
        ],
        "doc_type_hint": (
            "UK HM Treasury budget document. Structure varies by era: "
            "1975-1992: Financial Statement macro overview only — no R&D data; "
            "2003-2009: Budget Report with Science chapter and science spending table (£ million); "
            "2010+: Spending Review / Budget with DEL tables by department (£ billion). "
            "Look for BEIS/DSIT/BIS Resource DEL and Capital DEL rows, "
            "UKRI sub-lines, and named science programme amounts."
        ),
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
        "unit_hint": (
            "ERA-DEPENDENT units: "
            "(1) 1975–2000: amounts in THOUSANDS of DKK (1.000 kr.) — set unit='thousand'. "
            "(2) 2001+: amounts in MILLIONS of DKK (mio. kr.) — set unit='million'. "
            "IMPORTANT: The page header or table caption will usually state 'Mio. kr.' or '1.000 kr.' "
            "— always check and use that label to decide the unit. "
            "Danish number format: period '.' is the thousands separator, comma ',' is the decimal. "
            "Example: '1.234,5' means 1234.5. "
            "Do NOT use a fixed unit — read the document header for the actual unit used."
        ),
        "known_agencies": [
            # Ministry (name changed over the decades)
            "Undervisningsministeriet (Ministry of Education, § 20, pre-2001)",
            "Forskningsministeriet (Ministry of Research, § 32, 2001-2010 approx)",
            "Videnskabsministeriet (Ministry of Science, Technology and Innovation, 2001-2011)",
            "Uddannelses- og Forskningsministeriet (Ministry of Higher Education and Science, § 19, 2014+)",
            # Research councils (pre-2014 names)
            "Statens teknisk-videnskabelige Forskningsfond (State Technical-Scientific Research Fund)",
            "Statens naturvidenskabelige Forskningsrad (Danish Natural Science Research Council, SNF)",
            "Statens samfundsvidenskabelige Forskningsrad (Social Science Research Council)",
            "Statens humanistiske Forskningsrad (Humanities Research Council)",
            "Statens laegervidenskabelige Forskningsrad (Medical Research Council)",
            # Post-2014 consolidated councils
            "Det Frie Forskningsraad / Danmarks Frie Forskningsfond (Independent Research Fund Denmark)",
            "Det Strategiske Forskningsrad (Danish Strategic Research Council, pre-2014)",
            # Innovation and applied research
            "Danmarks Innovationsfond (Innovation Fund Denmark, 2014+)",
            "Hoejteknologifonden (Danish Advanced Technology Foundation, pre-2014)",
            "Teknologiradet (Danish Board of Technology)",
            "Danmarks Grundforskningsfond (Danish National Research Foundation, DNRF)",
            # Atomic energy / nuclear
            "Atomenergikommissionen (Atomic Energy Commission, pre-1990s)",
            "Ris\u00f8 Nationallaboratorium / Ris\u00f8 National Laboratory",
            "Nukleare anlaeg / nuclear facilities",
            # Universities (historically funded via Undervisningsministeriet/UFM)
            "Kobenhavns Universitet (University of Copenhagen, KU)",
            "Aarhus Universitet (University of Aarhus, AU)",
            "Danmarks tekniske Hojskole / Danmarks Tekniske Universitet (DTU)",
            "Aalborg Universitet (AAU)",
            "Syddansk Universitet (SDU)",
            "Roskilde Universitetscenter (RUC)",
            "Handelshojskolen (Copenhagen Business School, CBS)",
            "Universiteterne (universities collective line)",
            # Sector research institutes
            "Danmarks Meteorologiske Institut (DMI)",
            "Danmarks og Gronlands Geologiske Undersogelse (GEUS)",
            "Statens Serum Institut (SSI)",
            "Det Nationale Forskningscenter for Arbejdsmiljo (NFA)",
        ],
        "mixed_ministries": [
            "Indenrigsministeriet (Ministry of the Interior)",
            "Socialministeriet (Ministry of Social Affairs)",
            "Sundhedsministeriet (Ministry of Health)",
            "Udenrigsministeriet (Ministry of Foreign Affairs)",
            "Forsvarsministeriet (Ministry of Defence)",
            "Trafikministeriet / Transportministeriet (Ministry of Transport)",
            "Finansministeriet (Ministry of Finance)",
            "Justitsministeriet (Ministry of Justice)",
            "Ministeriet for Foedevarer, Landbrug og Fiskeri (Ministry of Food and Agriculture)",
            "Miljoeministeriet (Ministry of Environment)",
            "Erhvervs- og Vaekstministeriet (Ministry of Business)",
            "Beskaftigelsesministeriet (Ministry of Employment)",
            "Skatteministeriet (Ministry of Taxation)",
        ],
        "doc_type_hint": (
            "Finanslov (Danish annual Finance Bill / Budget Act). "
            "STRUCTURE: Organised by § (paragraph) number. Each § is a ministry or major agency. "
            "KEY SECTIONS FOR R&D: "
            "§ 20 Undervisningsministeriet (pre-2001): universities + research councils. "
            "§ 19 Uddannelses- og Forskningsministeriet (UFM, 2014+): universities + research funds. "
            "§ 32 Forskningsministeriet / Videnskabsministeriet (approx 2001-2013): research ministry. "
            "UNIT ERA: Before 2001, amounts are in '1.000 kr.' (thousands DKK). "
            "From 2001 onwards, amounts switch to 'Mio. kr.' (millions DKK). "
            "Always read the column header to confirm the unit. "
            "SECTION OVERVIEW ROWS: Each § starts with a one-line total (e.g. '§ 19. I alt 28.789,7 mio. kr.'). "
            "These overview totals are useful as section-level aggregates; tag them accordingly. "
            "Below the overview, individual account lines appear (e.g. '19.11.01 Kobenhavns Universitet'). "
            "INCLUDE the individual university and research-fund lines — these are the key time-series. "
            "SKIP: student grants/loans (SU-laan, Statens Uddannelsesstotte), "
            "folkeskole (primary school) lines, daycare, infrastructure unless R&D-labelled, "
            "and generic Driftsudgifter lines in mixed-purpose sections. "
            "NUMBER FORMAT: '.' is thousands separator, ',' is decimal — '1.234,5' = 1234.5."
        ),
    },
    "France": {
        "currency": "EUR",           # EUR from 2002; FRF (French Franc) before 2002
        "currency_symbol": "€",
        "language": "french",
        "unit_hint": "million",      # LOLF era (2006+): full-euro amounts → divide by 1M
        "known_agencies": [
            # Core research agencies (CNRS, CEA etc. appear in PAP annexes, not main JORF text)
            "Centre National de la Recherche Scientifique (CNRS)",
            "Agence Nationale de la Recherche (ANR)",
            "Institut National de la Santé et de la Recherche Médicale (INSERM)",
            "Commissariat à l'Énergie Atomique et aux Énergies Alternatives (CEA)",
            "Institut National de Recherche en Informatique et en Automatique (INRIA)",
            "Centre National d'Études Spatiales (CNES)",
            "Institut Français de Recherche pour l'Exploitation de la Mer (IFREMER)",
            "Institut National de Recherche pour l'Agriculture, l'Alimentation et l'Environnement (INRAE)",
            "Agence de l'Environnement et de la Maîtrise de l'Énergie (ADEME)",
            "Institut de Radioprotection et de Sûreté Nucléaire (IRSN)",
            "Agence Nationale pour la Gestion des Déchets Radioactifs (ANDRA)",
            # LOLF Programmes (2006+)
            "Programme 150 — Formations supérieures et recherche universitaire",
            "Programme 172 — Recherches scientifiques et technologiques pluridisciplinaires",
            "Programme 187 — Recherche agricole et agroalimentaire",
            "Programme 190 — Recherche dans les domaines de l'énergie",
            "Programme 191 — Recherche dans les domaines du risque",
            "Programme 192 — Recherche et enseignement supérieur économique",
            "Programme 193 — Recherche spatiale",
        ],
        "mixed_ministries": [
            "Ministère de la Défense", "Ministère des Affaires étrangères",
            "Ministère de la Santé", "Ministère de l'Éducation nationale",
            "Ministère de l'Agriculture", "Ministère de l'Intérieur",
            "Ministère des Transports", "Ministère du Travail",
        ],
        "doc_type_hint": (
            "JORF Loi de finances (French annual budget law). "
            "ERA GUIDE: "
            "(1) Pre-2002 (FRF era): État B/C tables by ministry chapter; currency='FRF'. "
            "R&D under 'Recherche' (Premier Ministre section) and 'Universités'. "
            "(2) 2002-2005 (EUR, pre-LOLF): same structure, currency='EUR'. "
            "(3) 2006+ (LOLF era): Mission 'Recherche et enseignement supérieur' with numbered programmes. "
            "CRITICAL FTE WARNING: The LOLF JORF contains two sequential tables — "
            "an ETPT staffing table (headcounts like 203,561 FTEs) appears BEFORE the credit table. "
            "ONLY extract from the credit table (État B, AE/CP columns). "
            "UNIT RULE (LOLF era): The credit table shows FULL EUROS — divide by 1,000,000. "
            "Set unit='million'. Example: 24,763,980,271 → amount_local=24764, unit='million'. "
            "Number format: French uses SPACE as thousands separator; comma as decimal."
        ),
    },
    "Germany": {
        "currency": "EUR",  # EUR from 2002; DEM (Deutsche Mark) before 2002
        "currency_symbol": "€",
        "language": "german",
        "unit_hint": "thousand",  # All Bundeshaushalt amounts in 1 000 DM or 1 000 EUR
        "known_agencies": [
            # Pre-1994: BMFT + BMBW as separate ministries
            "Bundesministerium für Forschung und Technologie (BMFT, Epl 30, pre-1994)",
            "Bundesministerium für Bildung und Wissenschaft (BMBW, Epl 31, pre-1994)",
            # 1994+: Merged as BMBF
            "Bundesministerium für Bildung und Forschung (BMBF, Epl 30)",
            # 2025+: Renamed
            "Bundesministerium für Forschung, Technologie und Raumfahrt (BMFTR, Epl 30)",
            # Key research organisations funded by BMBF
            "Deutsche Forschungsgemeinschaft (DFG)",
            "Max-Planck-Gesellschaft (MPG)",
            "Fraunhofer-Gesellschaft",
            "Helmholtz-Gemeinschaft / Hermann von Helmholtz-Gemeinschaft (HGF)",
            "Leibniz-Gemeinschaft / Wissenschaftsgemeinschaft Gottfried Wilhelm Leibniz (WGL)",
            "Deutsches Zentrum für Luft- und Raumfahrt (DLR)",
            "Alexander von Humboldt-Stiftung",
            "Deutscher Akademischer Austauschdienst (DAAD)",
        ],
        "mixed_ministries": [
            "Bundesministerium der Verteidigung (BMVg, Epl 14)",
            "Bundesministerium für Gesundheit (BMG)",
            "Bundesministerium des Innern (BMI, Epl 06)",
            "Bundesministerium für Arbeit und Soziales (BMAS)",
            "Bundesministerium für Ernährung und Landwirtschaft (BMEL)",
            "Auswärtiges Amt (Epl 05)",
            "Bundesministerium für Wirtschaft (Epl 09)",
            "Bundesministerium für Verkehr (Epl 12)",
            "Allgemeine Finanzverwaltung (Epl 60)",
        ],
        "doc_type_hint": (
            "German federal budget (Bundeshaushalt). Two main document types: "
            "(1) Gesamtplan overview (most files): shows one row per Einzelplan — "
            "extract Epl 30 (BMBF/BMFT) 'Summe Ausgaben' as the ministry total. "
            "(2) Full Einzelplan 30 chapter (large files ~1500KB): detailed BMBF "
            "programme lines — extract Titelgruppe totals (DFG, MPG, Fraunhofer, "
            "Helmholtz, Leibniz, DLR) ONLY, not individual Titel sub-lines. "
            "Number format: SPACE is thousands separator ('14 053 404' = 14,053,404). "
            "Currency: DEM (1000 DM) until 2001; EUR (1000 EUR) from 2002."
        ),
    },
    "Norway": {
        "currency": "NOK",
        "currency_symbol": "kr",
        "language": "norwegian",
        "unit_hint": (
            "DUAL-SCALE DOCUMENT — read the header carefully: "
            "(1) Part I overview table (first pages): amounts in '1 000 kroner' (thousands NOK) — set unit='thousand'. "
            "(2) Detailed Kap./Post line-item pages (most of the document): amounts in FULL NOK (single kroner) — set unit='unit'. "
            "The overview table has a header row containing '1 000 kroner' or '(i 1 000 kr.)'. "
            "The detail pages show e.g. 'Post 50 Norges forskningsrad 2 500 000 000'. "
            "ALWAYS prefer the detail-page values (full NOK). Only fall back to overview if detail is absent. "
            "Norwegian number format: space or period as thousands separator, comma as decimal. "
            "Example: '2 500 000 000' = 2,500,000,000 NOK (full NOK, not thousands)."
        ),
        "known_agencies": [
            # Research Council of Norway — primary vehicle for public R&D
            "Norges forskningsrad (Research Council of Norway, NFR)",
            "Forskningsradet (Research Council, short form)",
            # Universities (funded via Kunnskapsdepartementet, Kap 260-)
            "Universitetet i Oslo (University of Oslo, UiO)",
            "Norges teknisk-naturvitenskapelige universitet (NTNU, Trondheim)",
            "Universitetet i Bergen (UiB)",
            "Universitetet i Troms\u00f8 / UiT \u2013 Norges Arktiske Universitet",
            "Universitetet i Stavanger (UiS)",
            "Universitetet i Agder (UiA)",
            "Norges milj\u00f8- og biovitenskapelige universitet (NMBU, Aas)",
            "Norges Handelsh\u00f8yskole (NHH)",
            "Universitetene (universities collective line)",
            # Applied research institutes
            "SINTEF (multi-disciplinary research institute)",
            "Havforskningsinstituttet (Institute of Marine Research, IMR)",
            "Folkehelseinstituttet (Norwegian Institute of Public Health, FHI)",
            "Norsk institutt for naturforskning (NINA)",
            "Norsk institutt for luftforskning (NILU)",
            "Meteorologisk institutt (Norwegian Meteorological Institute)",
            "Norsk Romsenter (Norwegian Space Agency / Centre)",
            "Christian Michelsen Research (CMR)",
            "Fafo (research foundation)",
            # Energy / oil research
            "Oljeforskningsprogrammet (oil research programme)",
            "Petoro / SDOEE (state oil companies — exclude unless research grant)",
            # Ministry totals
            "Kunnskapsdepartementet (Ministry of Education and Research, KD)",
            "Naerings- og fiskeridepartementet (Ministry of Trade / Industry, NFD)",
            "Olje- og energidepartementet (Ministry of Petroleum and Energy, OED)",
        ],
        "mixed_ministries": [
            "Forsvarsdepartementet (Ministry of Defence)",
            "Helse- og omsorgsdepartementet (Ministry of Health and Care Services)",
            "Utenriksdepartementet (Ministry of Foreign Affairs)",
            "Justis- og beredskapsdepartementet (Ministry of Justice)",
            "Samferdselsdepartementet (Ministry of Transport)",
            "Finansdepartementet (Ministry of Finance)",
            "Klima- og milj\u00f8departementet (Ministry of Climate and Environment)",
            "Kommunal- og moderniseringsdepartementet (Ministry of Local Government)",
            "Arbeids- og sosialdepartementet (Ministry of Labour)",
        ],
        "doc_type_hint": (
            "Statsbudsjettet Bl\u00e5bok (Norwegian State Budget, Blue Book). "
            "DIGITAL QUALITY: 1975-1992 documents are FULLY SCANNED (no machine-readable text) "
            "— these years should yield very few or no extractable rows. "
            "1993-2009: partly scanned or low-quality text, limited extraction. "
            "2010-2026: excellent digital text with clean Kap./Post structure — focus here. "
            "STRUCTURE (2010+): organised by Departement (ministry) then Kap. (chapter) then Post. "
            "Each Post line has a description and an amount in full NOK (kroner). "
            "Overview tables in Part I use '1 000 kroner' (thousands) — identify by page header. "
            "KEY R&D SECTIONS: "
            "Kap. 285+ under Kunnskapsdepartementet: Norges forskningsrad (Research Council). "
            "Kap. 260-270: Universities (NTNU, UiO, UiB, UiT etc.). "
            "Kap. 920-930 under Naerings- og fiskeridepartementet: industry/applied R&D. "
            "Post 50 = block grant to research institutions. "
            "Post 52/55 = specific research programmes. "
            "Post 70/71 = grants to external research institutes. "
            "SKIP: student loans and grants (Laanekassen, Kap 2410), "
            "plain infrastructure (Vegvesen, Bane NOR), "
            "oil/gas production subsidies without 'forskning' in description, "
            "defence procurement lines, pension funds. "
            "INCLUDE: any line with 'forskning', 'forskningsrad', 'forskningsprogram', "
            "'universitets-', or a named research institution."
        ),
    },
    "Netherlands": {
        "currency": "EUR",           # EUR from 2002; NLG (guilder) before 2002
        "currency_symbol": "\u20ac",
        "language": "dutch",
        "unit_hint": (
            "ERA-DEPENDENT units: "
            "(1) 1975-2001 (NLG era, Miljoenennota): amounts in MILLIONS of guilders (miljoenen guldens) "
            "— set currency='NLG', unit='million'. "
            "(2) 2002+ (EUR era, per-ministry Begrotingsstaat): amounts in THOUSANDS of EUR "
            "(bedragen x \u20ac 1.000) — set currency='EUR', unit='thousand'. "
            "ALWAYS read the document header for the unit label. "
            "Dutch number format: period '.' is the thousands separator, comma ',' is the decimal. "
            "Example: '1.670.345' = 1,670,345 (thousands EUR = ~1.67 billion EUR)."
        ),
        "known_agencies": [
            # Primary R&D funding bodies
            "NWO (Nederlandse Organisatie voor Wetenschappelijk Onderzoek / Dutch Research Council)",
            "STW / NWO-TTW (Technology Foundation / Applied and Engineering Sciences)",
            "KNAW (Koninklijke Nederlandse Akademie van Wetenschappen / Royal Netherlands Academy)",
            "TNO (Toegepast Natuurwetenschappelijk Onderzoek / Netherlands Organisation for Applied Scientific Research)",
            "NWO-I (Institutes Organisation of NWO)",
            # Universities (funded via OCW, Art. 07 Wetenschappelijk onderwijs)
            "Universiteit van Amsterdam (UvA)",
            "Vrije Universiteit Amsterdam (VU)",
            "Leiden Universiteit",
            "Delft University of Technology (TU Delft)",
            "Eindhoven University of Technology (TU/e)",
            "Universiteit Utrecht (UU)",
            "Universiteit Groningen (RUG)",
            "Erasmus Universiteit Rotterdam (EUR)",
            "Radboud Universiteit Nijmegen",
            "Universiteit Maastricht",
            "Universiteit Twente",
            "Tilburg University",
            "Wageningen Universiteit (WUR)",
            "Universiteiten (collective line for all universities)",
            # Innovation and applied research
            "Rijksdienst voor Ondernemend Nederland (RVO, enterprise agency)",
            "Topconsortia voor Kennis en Innovatie (TKI, top sector knowledge consortia)",
            "SURF (national research ICT infrastructure)",
            "NIOZ (Koninklijk Nederlands Instituut voor Onderzoek der Zee, Royal NIOZ)",
            "RIVM (Rijksinstituut voor Volksgezondheid en Milieu, National Institute for Public Health)",
            "PBL (Planbureau voor de Leefomgeving, Netherlands Environmental Assessment Agency)",
            "KNMI (Koninklijk Nederlands Meteorologisch Instituut, Royal Netherlands Meteorological Institute)",
            "Deltares (water and subsurface research)",
            "Rathenau Instituut (science and technology assessment)",
        ],
        "mixed_ministries": [
            "Ministerie van Defensie (Ministry of Defence, X)",
            "Ministerie van Binnenlandse Zaken (Ministry of Interior, VII)",
            "Ministerie van Justitie en Veiligheid (Ministry of Justice, VI)",
            "Ministerie van Buitenlandse Zaken (Ministry of Foreign Affairs, V)",
            "Ministerie van Financi\u00ebn (Ministry of Finance, IX)",
            "Ministerie van Sociale Zaken en Werkgelegenheid (Ministry of Social Affairs, XV)",
            "Ministerie van Volksgezondheid, Welzijn en Sport (VWS, Ministry of Health, XVI)",
            "Ministerie van Infrastructuur en Waterstaat (IenW, Ministry of Infrastructure, XII)",
        ],
        "doc_type_hint": (
            "Dutch Rijksbegroting (State Budget). Two main document types: "
            "(1) 1975-2001 (NLG era): SINGLE DOCUMENT — Miljoenennota (budget memorandum) "
            "or Rijksbegroting overview covering all ministries. Unit: miljoenen guldens (millions NLG). "
            "(2) 2002+ (EUR era): SEPARATE FILE PER MINISTRY — named <year>_ministry<N>.pdf. "
            "Unit: duizenden euro's (thousands EUR) — stated as 'bedragen x \u20ac 1.000'. "
            "KEY R&D MINISTRIES (modern era): "
            "ministry8 = OCW (Onderwijs, Cultuur en Wetenschap, Ministry VIII): "
            "  Art. 07 Wetenschappelijk onderwijs (university block grants), "
            "  Art. 16 Onderzoek en wetenschapsbeleid (NWO, KNAW, research policy). "
            "ministry13 = EZ (Economische Zaken, Ministry XIII): "
            "  Art. 02 Bedrijvenbeleid: innovatie en ondernemerschap (innovation/enterprise), "
            "  Art. 03 Toekomstfonds (Future Fund). "
            "ministry14 = LNV (Landbouw, Visserij, Voedselzekerheid en Natuur, Ministry XIV): "
            "  Art. 23 Kennis en innovatie (knowledge and innovation). "
            "SKIP: ministry10 (Defensie / defence), ministry12 (IenW / infrastructure), "
            "ministry16 (VWS / health, unless RIVM line present). "
            "STRUCTURE: Each ministry file has 3 pages: title page, signature page, Begrotingsstaat table. "
            "The table has Art. (article) rows with Verplichtingen / Uitgaven / Ontvangsten columns. "
            "Extract the Uitgaven (expenditure) column for R&D-relevant articles. "
            "NUMBER FORMAT: period '.' = thousands separator; comma ',' = decimal."
        ),
    },
    "Switzerland": {
        "currency": "CHF",
        "currency_symbol": "Fr.",
        "language": "german",
        "unit_hint": (
            "Amounts are in FULL SWISS FRANCS (Franken) — unit='unit'. "
            "There is NO scaling by thousands or millions unless the text explicitly says 'Mio.' or 'Mrd.' "
            "before the number. "
            "Number format: SPACE is the thousands separator (e.g., '83 845 192 500' = 83,845,192,500 CHF). "
            "Occasional use of 'Mio.' = millions CHF, 'Mrd.' = billions CHF in narrative text. "
            "IMPORTANT: The pre-2021 Bundesblatt files contain only aggregate totals (full CHF). "
            "The VA-Band3 files (2021+) contain section C Budgetpositionen with detailed line items (full CHF)."
        ),
        "known_agencies": [
            # ETH Domain (Bereich ETH) — primary Swiss R&D vehicle
            "ETH-Bereich / ETH Domain (umbrella for all ETH institutions)",
            "ETH Z\u00fcrich (Eidgen\u00f6ssische Technische Hochschule Z\u00fcrich)",
            "EPFL (Ecole polytechnique f\u00e9d\u00e9rale de Lausanne)",
            "PSI (Paul Scherrer Institut)",
            "Empa (Eidg. Materialpr\u00fcfungs- und Forschungsanstalt)",
            "Eawag (Wasserforschungs-Institut des ETH-Bereichs)",
            "WSL (Eidg. Forschungsanstalt f\u00fcr Wald, Schnee und Landschaft)",
            # Research funding agencies
            "SNF / SNSF (Schweizerischer Nationalfonds zur F\u00f6rderung der wissenschaftlichen Forschung / Swiss National Science Foundation)",
            "Innosuisse (Schweizerische Agentur f\u00fcr Innovationsf\u00f6rderung / Swiss Innovation Agency)",
            "KTI (Kommission f\u00fcr Technologie und Innovation, predecessor to Innosuisse, pre-2018)",
            # Federal department
            "SBFI / SERI (Staatssekretariat f\u00fcr Bildung, Forschung und Innovation / State Secretariat for Education, Research and Innovation)",
            "WBF (Eidg. Departement f\u00fcr Wirtschaft, Bildung und Forschung / DEFR)",
            # Space and other applied research
            "ESA-Beitr\u00e4ge (Swiss contributions to European Space Agency)",
            "CERN (European Organization for Nuclear Research, Swiss contribution)",
            "Agroscope (federal agricultural research)",
            "Swisstopo (federal topographic institute)",
        ],
        "mixed_ministries": [
            "VBS / DDPS (Eidg. Departement f\u00fcr Verteidigung, Bev\u00f6lkerungsschutz und Sport / Defence)",
            "EDA (Eidg. Departement f\u00fcr ausw\u00e4rtige Angelegenheiten / Foreign Affairs)",
            "UVEK / DETEC (Departement f\u00fcr Umwelt, Verkehr, Energie und Kommunikation / Environment, Transport)",
            "EJPD (Eidg. Justiz- und Polizeidepartement / Justice and Police)",
            "EFD (Eidg. Finanzdepartement / Finance)",
        ],
        "doc_type_hint": (
            "Swiss Federal Budget (Voranschlag der Schweizerischen Eidgenossenschaft). "
            "TWO DISTINCT DOCUMENT TYPES in our dataset: "
            "(1) 1975-2020: Bundesbeschluss published in the Bundesblatt (Federal Gazette). "
            "These are SHORT legislative approval documents (3-10 pages). "
            "They contain ONLY aggregate authorization totals (Erfolgsrechnung, Investitionsrechnung) "
            "and special Verpflichtungskredite (commitment credits) for specific projects. "
            "R&D extractable: ETH-Bereich Bauprogramm credit, SNF credit if listed, total R&D envelope. "
            "YIELD IS LOW for these years — expect 1-5 R&D-relevant rows per document. "
            "(2) 2021-2025: VA-Band3-d.pdf = Voranschlag Band 3 (German). "
            "Contains section C 'Budgetpositionen' with detailed departmental line items in full CHF. "
            "KEY R&D SECTION: WBF (Departement f\u00fcr Wirtschaft, Bildung und Forschung). "
            "Look for: Beitrag an den ETH-Bereich (lump-sum to ETH Domain, ~3.7B CHF), "
            "Beitrag an den SNF (grant to Swiss NSF, ~1.1B CHF), "
            "Beitrag an Innosuisse (innovation agency, ~300M CHF), "
            "CERN-Beitrag (Swiss contribution to CERN, ~150M CHF), "
            "ESA-Beitr\u00e4ge (space agency contributions). "
            "UNIT: All amounts in FULL CHF (Franken). Space = thousands separator. "
            "Do NOT divide by any scaling factor. "
            "Example: '3 714 600 000' = CHF 3.7 billion (ETH-Bereich block grant)."
        ),
    },
    "Sweden": {
        "currency": "SEK",
        "currency_symbol": "kr",
        "language": "swedish",
        "unit_hint": (
            "Amounts are in THOUSANDS of Swedish kronor (tusental kronor / tkr). "
            "Set unit='thousand'. "
            "Swedish number format: SPACE is the thousands separator, comma ',' is the decimal. "
            "Example: '3 500 000' = 3,500,000 thousand SEK = 3.5 billion SEK. "
            "Each anslag (appropriation) line has a code like '2:1 Vetenskapsrådet'. "
            "DO NOT set unit='million' unless the page header explicitly says 'miljoner kronor'."
        ),
        "known_agencies": [
            # Research councils — post-2001 consolidated structure
            "Vetenskapsrådet (Swedish Research Council, VR)",
            "VINNOVA (Verket för innovationssystem / Swedish Innovation Agency)",
            "Formas (Swedish Research Council for Environment, Agricultural Sciences and Spatial Planning)",
            "Forte (Swedish Research Council for Health, Working Life and Welfare)",
            # Pre-2001 research councils (merged into VR)
            "Naturvetenskapliga forskningsrådet (NFR, Swedish Natural Science Research Council, pre-2001)",
            "Teknikvetenskapliga forskningsrådet (TFR, pre-2001)",
            "Humanistisk-samhällsvetenskapliga forskningsrådet (HSFR, pre-2001)",
            "Medicinska forskningsrådet (MFR, Medical Research Council, pre-2001)",
            "Skogs- och jordbrukets forskningsråd (SJFR, pre-2001)",
            # Innovation / applied research
            "NUTEK (Näringstekniska rådet / National Board for Industrial and Technical Development, pre-2001)",
            "STU (Styrelsen för teknisk utveckling / Swedish Board for Technical Development, pre-1991)",
            "RISE (Research Institutes of Sweden, from 2017)",
            "SP / Swerea / Innventia (applied research predecessors to RISE)",
            "SSF (Stiftelsen för Strategisk Forskning / Swedish Foundation for Strategic Research)",
            "Riksbankens Jubileumsfond (humanities and social science)",
            # Specific institutes
            "SMHI (Sveriges meteorologiska och hydrologiska institut)",
            "Rymdstyrelsen (Swedish National Space Agency)",
            "FOI / FOA (Totalförsvarets forskningsinstitut / Swedish Defence Research Agency)",
            # Universities (funded under UO 16)
            "KTH (Kungliga Tekniska Högskolan / Royal Institute of Technology)",
            "Lunds Universitet (Lund University, including LTH)",
            "Uppsala Universitet (Uppsala University)",
            "Stockholms Universitet (Stockholm University)",
            "Göteborgs Universitet (University of Gothenburg)",
            "Umeå Universitet (Umeå University)",
            "Linköpings Universitet (Linköping University)",
            "Chalmers tekniska högskola",
            "Karolinska Institutet",
            "Universiteterna (collective line for all Swedish universities)",
        ],
        "mixed_ministries": [
            "Försvarsdepartementet (Ministry of Defence)",
            "Socialdepartementet (Ministry of Social Affairs)",
            "Utrikesdepartementet (Ministry of Foreign Affairs)",
            "Finansdepartementet (Ministry of Finance)",
            "Justitiedepartementet (Ministry of Justice)",
            "Trafikdepartementet / Infrastrukturdepartementet (Ministry of Infrastructure)",
            "Kulturdepartementet (Ministry of Culture)",
            "Arbetsmarknadsdepartementet (Ministry of Labour)",
        ],
        "doc_type_hint": (
            "Swedish Statsbudget / Budgetproposition (Prop. XXXX/XX:1). "
            "STRUCTURE (post-1994 Utgiftsområde reform): "
            "Budget is divided into 27 Utgiftsområden (UO = expenditure areas). "
            "KEY R&D AREAS: "
            "UO 16 Utbildning och universitetsforskning — universities, Vetenskapsrådet, Formas, Forte, VINNOVA. "
            "UO 20 Allmän miljö- och naturvård — SMHI, environmental research. "
            "UO 24 Näringsliv — VINNOVA (partly), industrial R&D, RISE. "
            "Each anslag (appropriation) is identified by a code like '2:1' followed by the agency name. "
            "STRUCTURE (pre-1994): budget by Departement chapter (§). "
            "Key pre-1994 chapters: § 8 Utbildningsdepartementet (universities), "
            "§ 16 Industridepartementet (STU/NUTEK). "
            "UNIT: belopp i tusental kronor (amounts in thousands SEK). "
            "Space = thousands separator. '3 500 000' = 3,500,000 tkr = 3.5 billion SEK. "
            "SKIP: Studiemedel / CSN / studiebidrag (student loans and grants — not R&D), "
            "Trafikverket / Vägverket / Banverket (transport infrastructure without forskning), "
            "Försvarsmakten / FMV (defence procurement without forskning), "
            "social insurance and transfer payments. "
            "INCLUDE: any anslag line with 'forskning', 'FoU', 'vetenskap', 'innovation', "
            "or a named research agency (Vetenskapsrådet, VINNOVA, Formas, Forte, SMHI, etc.)."
        ),
    },
    "Austria": {
        "currency": "EUR",           # EUR from 2002; ATS (Schilling) before 2002
        "currency_symbol": "€",
        "language": "german",
        "unit_hint": (
            "ERA-DEPENDENT UNITS — check the document header: "
            "(1) 1975-2001 (ATS era): amounts in THOUSANDS of Austrian Schillings (in Tausend ATS). "
            "Set currency='ATS', unit='thousand'. "
            "(2) 2002+ (EUR era): amounts in THOUSANDS of euros (in Tausend EUR). "
            "Set currency='EUR', unit='thousand'. "
            "IMPORTANT: Austrian number format: period '.' is the thousands separator, "
            "comma ',' is the decimal. Example: '280.000' = 280,000 thousand ATS/EUR. "
            "NEVER assume unit='million' unless the text explicitly says 'Millionen'. "
            "The 1 EUR = 13.7603 ATS conversion rate was fixed from 1999."
        ),
        "known_agencies": [
            # Core R&D funding agencies
            "FWF (Fonds zur Förderung der wissenschaftlichen Forschung / Austrian Science Fund)",
            "FFG (Forschungsförderungsgesellschaft / Austrian Research Promotion Agency, from 2004)",
            "FFF (Forschungsförderungsfonds für die gewerbliche Wirtschaft, pre-2004 predecessor to FFG)",
            "AWS (Austria Wirtschaftsservice GmbH)",
            "Christian Doppler Forschungsgesellschaft (CD-Labor)",
            "ÖAW (Österreichische Akademie der Wissenschaften / Austrian Academy of Sciences)",
            "Ludwig Boltzmann Gesellschaft",
            "Joanneum Research (Graz, applied research)",
            "AIT (Austrian Institute of Technology, from 2009, formerly Arsenal Research)",
            "IST Austria / ISTA (Institute of Science and Technology Austria)",
            # Ministry totals (R&D-focused)
            "BMBWF / BMWFW / BMWF / BMWV / BMBWK (Bundesministerium für Wissenschaft und Forschung)",
            "UG 31 Wissenschaft und Forschung (post-2013 Untergliederung for science & research)",
            "UG 33 Wirtschaft (post-2013 Untergliederung, includes FFG/AWS innovation funding)",
            "Einzelplan 13 Wissenschaft und Forschung (pre-2013 Kapitel for science)",
            # Universities (funded via BMBWF/Globalbudget)
            "Universität Wien",
            "Technische Universität Wien (TU Wien)",
            "Universität Graz (Karl-Franzens-Universität)",
            "Technische Universität Graz (TU Graz)",
            "Johannes Kepler Universität Linz (JKU)",
            "Universität Innsbruck",
            "Medizinische Universität Wien (MedUni Wien)",
            "Universität Salzburg",
            "Wirtschaftsuniversität Wien (WU Wien)",
            "Universität für Bodenkultur Wien (BOKU)",
            "Universität Klagenfurt",
            "Universität Leoben (Montanuniversität)",
            "Universitäten (collective Globalbudget line)",
            # International contributions
            "CERN-Beitrag (Austrian contribution to CERN)",
            "ESA-Beitrag (Austrian contribution to ESA)",
        ],
        "mixed_ministries": [
            "Bundesministerium für Landesverteidigung (BMLV / Heer — defence)",
            "Bundesministerium für Soziales (BMAS — social affairs)",
            "Bundesministerium für Inneres (BMI — interior)",
            "Bundesministerium für Finanzen (BMF — finance)",
            "Bundesministerium für Verkehr, Innovation und Technologie (BMVIT — mixed mandate)",
            "Bundesministerium für Land- und Forstwirtschaft (BMLF — agriculture)",
            "Bundesministerium für auswärtige Angelegenheiten (BMAA — foreign affairs)",
        ],
        "doc_type_hint": (
            "Austrian Federal Budget: Bundesfinanzgesetz (BFG) / Bundesvoranschlag (BVA). "
            "STRUCTURAL ERAS: "
            "(1) Pre-2013 (Kapitel/Einzelplan system): "
            "Budget structured by Einzelpläne. "
            "KEY R&D CHAPTERS: Einzelplan 13 = Wissenschaft und Forschung (BMWF). "
            "Einzelplan 07 = Verkehr, Innovation und Technologie (BMVIT — mixed). "
            "(2) Post-2013 (Haushaltsrechtsreform — Untergliederung/UG system): "
            "KEY R&D CHAPTERS: UG 31 = Wissenschaft und Forschung (FWF, ÖAW, universities). "
            "UG 33 = Wirtschaft (FFG, AWS, AIT). "
            "UG 34 = Verkehr, Innovation und Technologie (BMVIT — mixed). "
            "Within each UG: Globalbudgets (GB) then Detailbudgets (DB) with individual line items. "
            "UNITS: amounts in Tausend ATS (pre-2002) or Tausend EUR (2002+). "
            "Austrian number format: '.' = thousands separator, ',' = decimal. "
            "'280.000' = 280,000 thousand (= 280 million EUR). "
            "UNIVERSITY FUNDING: Post-2002, universities receive a Globalbudget (block grant) "
            "via Leistungsvereinbarungen (performance agreements). "
            "The state budget shows only the total grant — not split into research vs teaching. "
            "Tag these as higher_education. "
            "SKIP: Bundesministerium für Landesverteidigung (defence) lines without 'Forschung', "
            "Sozialversicherung / AMS / Pensionsversicherung (social transfers), "
            "Straßenbau / Schieneninfrastruktur / ASFINAG without 'Forschung' (pure infrastructure), "
            "EU-Kofinanzierung overhead lines (administrative matching funds). "
            "INCLUDE: FWF, FFG/FFF, ÖAW, CD-Labor, CERN/ESA contributions, "
            "university Globalbudgets under UG 31, any line with 'Forschung', 'Wissenschaft', "
            "or a named research institution."
        ),
    },
    "Spain": {
        "currency": "EUR",           # EUR from 2002; ESP (pesetas) before 2002
        "currency_symbol": "€",
        "language": "spanish",
        "unit_hint": (
            "ERA-DEPENDENT units: "
            "(1) Pre-2002 (ESP era): amounts in MILLIONS of pesetas (millones de pesetas). "
            "Set currency='ESP', unit='million'. "
            "(2) 2002+ (EUR era): amounts in THOUSANDS of euros ('Miles de euros' header). "
            "Set currency='EUR', unit='thousand'. "
            "ALWAYS read the document header to confirm. "
            "Spanish number format: period '.' is thousands separator, comma ',' is decimal. "
            "Example: '199.350,68' = 199,350,680 (thousands EUR = ~199 million EUR)."
        ),
        "known_agencies": [
            # Core public research organisms (OPIs)
            "CSIC (Consejo Superior de Investigaciones Científicas)",
            "AEI (Agencia Estatal de Investigación, from 2017)",
            "CDTI (Centro para el Desarrollo Tecnológico e Industrial / Centro para el Desarrollo Tecnológico y la Innovación)",
            "ISCIII (Instituto de Salud Carlos III)",
            "CIEMAT (Centro de Investigaciones Energéticas, Medioambientales y Tecnológicas)",
            "INIA / INIA-CSIC (Instituto Nacional de Investigación y Tecnología Agraria y Alimentaria)",
            "IGME (Instituto Geológico y Minero de España)",
            "IEO / IEO-CSIC (Instituto Español de Oceanografía)",
            "FECYT (Fundación Española para la Ciencia y la Tecnología)",
            # Pre-AEI research coordination
            "CAICYT (Centro de Información y Documentación Científica, pre-1986 research coordinator)",
            "CICYT (Comisión Interministerial de Ciencia y Tecnología, 1986-2000)",
            "DGI / DGICYT (Dirección General de Investigación Científica y Técnica)",
            # Ministry
            "Ministerio de Ciencia e Innovación (MICINN, Sección 28)",
            "Ministerio de Ciencia, Innovación y Universidades",
            "Secretaría de Estado de I+D+i",
            # Budget programmes (programme codes)
            "Programa 541A — Investigación Científica",
            "Programa 542A — Investigación Técnica",
            "Programa 542E — Investigación y Desarrollo Tecnológico",
            "Programa 463B — Fomento y coordinación de la investigación científica y técnica",
            "Programa 465A — Investigación Sanitaria",
            "Plan Nacional de I+D+i (national R&D plan)",
        ],
        "mixed_ministries": [
            "Ministerio de Defensa",
            "Ministerio de Interior",
            "Ministerio de Trabajo",
            "Ministerio de Sanidad (unless ISCIII explicitly named)",
            "Ministerio de Educación (unless university research explicitly named)",
            "Ministerio de Agricultura (unless INIA explicitly named)",
            "Ministerio de Transportes",
            "Ministerio de Hacienda",
            "Seguridad Social",
        ],
        "doc_type_hint": (
            "Spanish Presupuestos Generales del Estado (PGE), published in BOE (Boletín Oficial del Estado). "
            "ERA GUIDE: "
            "(1) Pre-1986: single-volume law text; R&D scattered across ministries. "
            "Key section: Sección 18 Educación y Ciencia — look for Servicio 25 'Investigación'. "
            "Programme codes: 541A (Investigación Científica), 542A (Técnica), 542E (I+D Tecnológico). "
            "(2) 1986-2000 (post-Ley de Ciencia 13/1986): CICYT coordinates Plan Nacional. "
            "CDTI appears under Industria; CSIC under Educación y Ciencia. "
            "(3) 2000-2011: Ministerio de Ciencia e Innovación (MICINN) created ~2008, absorbs R&D. "
            "Programme 463B 'Fomento y coordinación de la investigación' is the main R&D appropriation. "
            "(4) 2017+: AEI (Agencia Estatal de Investigación) created as independent funding agency. "
            "Organism code 28.303 in the budget. ISCIII is organism 28.106. CIEMAT is 28.103. "
            "UNIT RULE: post-2002 amounts are in 'Miles de euros' (thousands EUR). "
            "SKIP: university teaching (non-research), social security, defence procurement, "
            "transport infrastructure, regional development unless explicitly R&D-labelled. "
            "NUMBER FORMAT: period '.' = thousands separator, comma ',' = decimal."
        ),
    },
    "Finland": {
        "currency": "EUR",           # EUR from 2002; FIM (Finnish markka) before 2002
        "currency_symbol": "€",
        "language": "finnish",
        "unit_hint": (
            "Amounts are in FULL EUROS (or full FIM before 2002) — unit='unit'. "
            "There is NO scaling; amounts are stated in full units. "
            "Example: '169 941 000' = 169,941,000 EUR (Suomen Akatemia research grants 2009). "
            "Finnish number format: SPACE is the thousands separator, comma ',' is the decimal. "
            "Pre-2002 (FIM era): same full-unit convention in Finnish markka. "
            "NEVER use unit='million' or 'thousand' unless the document header explicitly states a scale."
        ),
        "known_agencies": [
            # Science funding
            "Suomen Akatemia (Academy of Finland) — moments 29.60.01 (operating) and 29.60.50 (research grants)",
            # Applied/innovation funding
            "Tekes (Teknologian ja innovaatioiden kehittämiskeskus, pre-2018) — moment 32.20.06",
            "Business Finland (Innovaatiorahoituskeskus Business Finland, from 2018) — moment 32.20.05",
            # Research institutes
            "VTT (Teknologian tutkimuskeskus VTT / Technical Research Centre of Finland) — moment 32.01.02/49",
            "GTK (Geologian tutkimuskeskus / Geological Survey of Finland) — moment 32.01.04 / TEM chapter",
            "VATT (Valtion taloudellinen tutkimuskeskus / Government Institute for Economic Research) — 28.30.02",
            "Luke (Luonnonvarakeskus / Natural Resources Institute Finland, from 2015)",
            "MTT (Maa- ja elintarviketalouden tutkimuskeskus, pre-2015 predecessor to Luke)",
            "RKTL (Riista- ja kalatalouden tutkimuslaitos, pre-2015 predecessor to Luke)",
            "Metla (Metsäntutkimuslaitos / Finnish Forest Research Institute, pre-2015 predecessor to Luke)",
            "SYKE (Suomen ympäristökeskus / Finnish Environment Institute)",
            "STUK (Säteilyturvakeskus / Radiation and Nuclear Safety Authority)",
            "THL (Terveyden ja hyvinvoinnin laitos / National Institute for Health and Welfare)",
            "IL (Ilmatieteen laitos / Finnish Meteorological Institute)",
            # Universities (funded via OKM, chapter 29.40)
            "Yliopistot (universities collective, chapter 29.40 Korkeakouluopetus ja tutkimus)",
            "Helsingin yliopisto (University of Helsinki)",
            "Aalto-yliopisto (Aalto University, from 2010; formerly TKK + HSE + TAIK)",
            "Teknillinen korkeakoulu (TKK, predecessor to Aalto, pre-2010)",
        ],
        "mixed_ministries": [
            "Puolustusministeriön hallinnonala (Ministry of Defence, chapter 27)",
            "Sisäasiainministeriön hallinnonala (Ministry of Interior, chapter 26)",
            "Sosiaali- ja terveysministeriön hallinnonala (Social and Health, chapter 33) — unless THL/research",
            "Oikeusministeriön hallinnonala (Ministry of Justice, chapter 25)",
            "Ulkoasiainministeriön hallinnonala (Foreign Affairs, chapter 24)",
            "Liikenneministeriön hallinnonala / Traficom (Transport, chapter 31)",
            "Valtiovarainministeriön hallinnonala (Finance, chapter 28) — except VATT",
        ],
        "doc_type_hint": (
            "Finnish Valtion talousarvio (State Budget). "
            "DOCUMENT QUALITY BY ERA: "
            "(1) 1985-1991: Hallituksen esitys (budget proposal), scanned — OCR quality poor. "
            "Expect limited extractable rows. Key sections: chapter 29 Opetusministeriö "
            "(Suomen Akatemia, universities) and chapter 32 Kauppa- ja teollisuusministeriö (VTT, Tekes). "
            "(2) 1992-2001 (FIM era): improving digital quality. Same chapter structure. "
            "Currency FIM (Finnish markka), full FIM amounts. "
            "(3) 2002+ (EUR era): fully digital, excellent quality. "
            "KEY R&D CHAPTERS: "
            "Chapter 29.40 — Korkeakouluopetus ja tutkimus (university education and research). "
            "Chapter 29.60 — Tiede (Science): "
            "  29.60.01 = Suomen Akatemian toimintamenot (Academy operating costs). "
            "  29.60.50 = Suomen Akatemian tutkimusmäärärahat (Academy research grants — KEY). "
            "Chapter 32.20 — Innovaatiopolitiikka (Innovation policy): "
            "  32.20.06 = Tekes toimintamenot (pre-2018) / 32.20.05 Business Finland (from 2018). "
            "  32.20.40 = Julkinen tutkimus- ja kehittämistoiminta (public R&D grants to companies). "
            "Chapter 32.01 — Geologian tutkimuskeskus (GTK) and VTT. "
            "UNIT RULE: ALL amounts are in FULL EUR (or full FIM pre-2002). "
            "Space = thousands separator. '169 941 000' = 169,941,000 EUR. "
            "SKIP: student grants (opintotuki, chapter 29.70), defence R&D unless civilian, "
            "general university operating costs without research label, social insurance. "
            "INCLUDE: Suomen Akatemia research grants (29.60.50), Tekes/Business Finland "
            "innovation appropriations (32.20), VTT institutional grant, GTK, individual "
            "research institute operating budgets."
        ),
    },
    "Belgium": {
        "currency": "EUR",
        "currency_symbol": "€",
        "language": "french/dutch",
        "unit_hint": (
            "ERA-DEPENDENT UNITS — always read the budget header. "
            "(1) Pre-2002 Belgium may use BEF (Belgian francs), often in THOUSANDS or MILLIONS. "
            "If the header says 'en milliers de francs' / 'in duizend frank', set currency='BEF', unit='thousand'. "
            "If it says 'en millions', set unit='million'. "
            "(2) 2002+ usually uses EUR, often in THOUSANDS of euros. "
            "If the header says 'en milliers d'euros' / 'in duizend euro', set currency='EUR', unit='thousand'. "
            "Do not guess the scale from the number alone; use the table header."
        ),
        "known_agencies": [
            "BELSPO (Belgian Science Policy Office)",
            "SSTC / OSTC (Services fédéraux des affaires scientifiques, techniques et culturelles / Office for Scientific Technical and Cultural Affairs)",
            "FWO (Research Foundation - Flanders)",
            "FNRS (Fonds de la Recherche Scientifique)",
            "SCK CEN (Belgian Nuclear Research Centre)",
            "Royal Observatory of Belgium",
            "Royal Meteorological Institute of Belgium",
            "Royal Belgian Institute of Natural Sciences",
            "Belgian Institute for Space Aeronomy",
        ],
        "mixed_ministries": [
            "Ministry of Defence",
            "Ministry of Social Affairs",
            "Ministry of Transport",
            "Ministry of Finance",
            "Social security",
        ],
        "doc_type_hint": (
            "Belgian federal budget / finance law. Belgium mixes federal, community, and regional structures. "
            "Prefer explicit science-policy, research-council, and named institute lines over broad ministry totals. "
            "Look for BELSPO / SSTC / OSTC, FNRS, FWO, nuclear, space, meteorology, and observatory institutes."
        ),
    },
    "Chile": {
        "currency": "CLP",
        "currency_symbol": "$",
        "language": "spanish",
        "unit_hint": (
            "Most Chilean budget law tables appear with a header like 'En miles de pesos'. "
            "If the header says 'miles de pesos', set currency='CLP', unit='thousand'. "
            "If it states plain pesos, set unit='unit'. "
            "Spanish number format: period '.' thousands separator, comma ',' decimal separator."
        ),
        "known_agencies": [
            "CONICYT (Comisión Nacional de Investigación Científica y Tecnológica)",
            "ANID (Agencia Nacional de Investigación y Desarrollo)",
            "CORFO innovation / technological development lines",
            "INIA (Instituto de Investigaciones Agropecuarias)",
            "IFOP (Instituto de Fomento Pesquero)",
        ],
        "mixed_ministries": [
            "Ministry of Defence",
            "Ministry of Social Development",
            "Ministry of Public Works",
            "Ministry of Transport",
            "broad education totals",
        ],
        "doc_type_hint": (
            "Chilean budget law / decreto ley. Prefer explicit science and innovation agencies, agricultural and fisheries research institutes, "
            "and named R&D support funds. Broad ministry totals, social programmes, public works, and defence procurement should be treated conservatively."
        ),
    },
    "Estonia": {
        "currency": "EUR",
        "currency_symbol": "€",
        "language": "estonian",
        "unit_hint": (
            "ERA-DEPENDENT CURRENCY — read the header. "
            "1991 may use roubles / RUB (sample law says 'rublades'). "
            "1992-2010 generally use EEK (Estonian kroon); euro-era years use EUR. "
            "The scale may be full units or thousands depending on the budget table. "
            "If the header says 'tuhat' / 'thousand', set unit='thousand'; otherwise use unit='unit'."
        ),
        "known_agencies": [
            "Haridus- ja Teadusministeerium (Ministry of Education and Research)",
            "Eesti Teadusagentuur (Estonian Research Council)",
            "Eesti Teadusfond (Estonian Science Foundation)",
            "SA Archimedes",
            "Tartu Ülikool (University of Tartu)",
            "Tallinna Tehnikaülikool (TalTech / Tallinn University of Technology)",
        ],
        "mixed_ministries": [
            "social insurance",
            "municipal transfers",
            "transport and infrastructure ministries",
            "broad education spending without research signal",
        ],
        "doc_type_hint": (
            "Estonian state budget law. Research may appear under the education/research ministry, research council/foundation lines, "
            "and university or institute appropriations. Keep ministry totals conservative unless they are clearly research-specific."
        ),
    },
    "Iceland": {
        "currency": "ISK",
        "currency_symbol": "kr",
        "language": "icelandic",
        "unit_hint": (
            "ERA-DEPENDENT scale — always read the document header first. "
            "(1) Pre-1993 (e.g. 1975–1992): amounts in THOUSANDS of króna. "
            "Header says 'Þús. kr.' or 'ÞÚ. kr.' — set unit='thousand'. "
            "(2) 1993 onward: amounts in MILLIONS of króna. "
            "Header says 'M.kr.', 'm.kr.', or 'mkr.' — set unit='million'. "
            "Do NOT infer scale from the size of the numbers alone; always confirm from the header."
        ),
        "known_agencies": [
            # Pre-2003 research governance bodies
            "Rannsóknaráð ríkisins (National Research Council of Iceland, code 02-232 / 232)",
            "Vísindaráð (Science Council, code 02-234 / 234)",
            "Vísindasjóður (Science Fund, code 02-235 / 975)",
            "Rannsóknasjóður (Research Fund, code 02-233 / 233)",
            "Byggingarsjóður rannsókna í þágu atvinnuveganna (Building Fund for Industry Research, code 02-276 / 276)",
            # Post-2003 consolidated research centre
            "Rannís (Icelandic Centre for Research) — created 2003, consolidated pre-existing research councils",
            # Universities
            "Háskóli Íslands (University of Iceland, code 02-201 / 201)",
            "Háskólinn á Akureyri (University of Akureyri, code 02-210)",
            "Landbúnaðarháskóli Íslands (Agricultural University of Iceland)",
            "Raunvísindastofnun Háskólans (Science Institute of the University of Iceland, code 02-203)",
            # Research institutes — marine, energy, environment
            "Hafrannsóknastofnun (Marine and Freshwater Research Institute, code 05-202)",
            "Orkustofnun (National Energy Authority — includes geothermal research, code 11-301)",
            "ÍSOR (Iceland GeoSurvey — geothermal and geological research, spun off from Orkustofnun ~2003)",
            "Veðurstofa Íslands (Icelandic Meteorological Office — atmospheric research)",
            "Náttúrufræðistofnun Íslands (Icelandic Institute of Natural History)",
            # Functional budget categories (2023 structure)
            "07.10 Vísindi og samkeppnissjóðir í rannsóknum (Science and competition funds in research)",
            "21.10 Háskólar og rannsóknastarfsemi (Universities and research activities)",
            "12.20 Rannsóknir, þróun og nýsköpun í landbúnaðarmálum (Agricultural R&D)",
            "13.20 Rannsóknir, þróun og nýsköpun í sjávarútvegi (Fisheries R&D)",
            "17.20 Rannsóknir og vöktun á náttúru Íslands (Nature research and monitoring)",
        ],
        "mixed_ministries": [
            "Sjávarútvegur — fisheries OPERATIONAL support lines without 'rannsókn/rannsóknir'",
            "Samgönguráðuneytið — public works, roads, transport without research signal",
            "Tryggingamálastofnun — social insurance without research signal",
            "Broad ministry chapter totals (samtals/alls lines)",
        ],
        "doc_type_hint": (
            "Icelandic Fjárlög (Finance Law / Annual Budget). "
            "CHAPTER CODE FORMATS BY ERA: "
            "(1) 1975–1985: simple 3-digit codes — e.g. '201 Háskóli Íslands', '232 Rannsóknaráð', '301 Orkustofnun'. "
            "(2) 1986–2002: two-tier XX-YYY codes — ministry prefix (02=Education, 05=Fisheries, 11=Industry) + 3-digit agency code. "
            "E.g. '02-232 Rannsóknaráð ríkisins', '05-202 Hafrannsóknastofnun', '11-301 Orkustofnun'. "
            "(3) 2003+: Rannís created by merging Rannsóknaráð + Vísindaráð + Vísindasjóður + Rannsóknasjóður. "
            "Functional decimal codes: '07.10 Vísindi og samkeppnissjóðir í rannsóknum', '21.10 Háskólar og rannsóknastarfsemi'. "
            "KEY R&D TERMS: rannsókn/rannsóknir (research), vísindi (science), nýsköpun (innovation), þróun (development), "
            "háskóli (university), tilraunastöð (experimental station). "
            "UNIT RULE: pre-1993 = Þús. kr. (thousands), 1993+ = M.kr. (millions). "
            "SKIP: fisheries operational vessels/quota lines without 'rannsókn', "
            "road/port construction, broad 'samtals' totals, social insurance transfers."
        ),
    },
    "Hungary": {
        "currency": "HUF",
        "currency_symbol": "Ft",
        "language": "hungarian",
        "ocr_langs": "hun+eng",
        "ocr_zoom": 2.5,
        "unit_hint": (
            "Hungarian annual budget laws usually state amounts in MILLIONS of forint "
            "('millió forint'). Set currency='HUF', unit='million' unless the table header "
            "explicitly states a different scale. Do NOT infer 'thousand' from number size alone."
        ),
        "known_agencies": [
            "Magyar Tudományos Akadémia (MTA / Hungarian Academy of Sciences)",
            "MTA Könyvtár és Információs Központ (MTA Library and Information Centre)",
            "Nemzeti Kutatási, Fejlesztési és Innovációs Alap (National Research, Development and Innovation Fund)",
            "Nemzeti Kutatási, Fejlesztési és Innovációs Hivatal (NRDI Office)",
            "Nemzeti Agrárkutatási és Innovációs Központ",
            "Hazai innováció támogatása (domestic innovation support)",
            "A nemzetközi együttműködésben megvalósuló innováció támogatása",
            "kutatóközpont", "kutatóintézet", "kutatási infrastruktúra",
        ],
        "mixed_ministries": [
            "Broad 'fejezet összesen', 'cím összesen', 'alcím összesen', 'mindösszesen' totals",
            "Road, rail, and general infrastructure development lines without kutatás/innováció signal",
            "Defence, police, and security lines without explicit kutatás/fejlesztés signal",
            "General education, welfare, and healthcare operating lines without research signal",
        ],
        "doc_type_hint": (
            "Hungarian annual budget law / Magyar Közlöny budget text. "
            "The source family is usable for extraction, but earlier years often behave more like "
            "legal wrappers while modern years contain richer named budget rows. Look for named research bodies and "
            "programme rows containing kutatás (research), fejlesztés (development), innováció "
            "(innovation), kutatóközpont / kutatóintézet, MTA, and Nemzeti Kutatási. "
            "Strong targets include the NRDI Fund, MTA bodies, agricultural research bodies, "
            "and explicit innovation support lines. Keep broad chapter totals conservative."
        ),
    },
    "Latvia": {
        "currency": "EUR",
        "currency_symbol": "€",
        "language": "latvian",
        "ocr_langs": "lav+eng",
        "ocr_zoom": 2.5,
        "unit_hint": (
            "ERA-DEPENDENT currency, but amounts appear to be in FULL currency units, not thousands. "
            "(1) Pre-2014 budgets: Latvian lats (LVL / latu). Set currency='LVL', unit='unit'. "
            "(2) 2014 onward: euro. Set currency='EUR', unit='unit'. "
            "If a document header explicitly states another scale, follow the header; otherwise do NOT invent thousands or millions."
        ),
        "known_agencies": [
            "Izglītības un zinātnes ministrija (Ministry of Education and Science)",
            "Latvijas Zinātnes padome (Latvian Science Council)",
            "Latvijas Zinātņu akadēmija (Latvian Academy of Sciences / LZA)",
            "Zinātnes bāzes finansējums (science base funding)",
            "Fundamentālie zinātniskie pētījumi (fundamental scientific research)",
            "Valsts pārvaldes institūciju pasūtītie zinātniskie pētījumi",
            "Zinātniskās darbības attīstība universitātēs",
            "Zinātniskās infrastruktūras nodrošināšana un attīstība augstskolās",
            "Zinātnes konkurētspējas veicināšana",
        ],
        "mixed_ministries": [
            "Broad ministry totals for Izglītības un zinātnes ministrija — section is too broad and includes sports, loans, and non-R&D education items",
            "Police and defence academies without explicit scientific research wording",
            "University hospitals and general healthcare service bodies",
            "Sports, student-credit, and cultural infrastructure lines without research wording",
        ],
        "doc_type_hint": (
            "Latvian annual budget law. The source family is mixed: early 1990s and some modern years behave like short legal wrappers, "
            "while mid-1990s to 2000s annex-style files contain richer programme tables. Strong targets are explicit science programmes and "
            "named science bodies: Latvijas Zinātnes padome, Latvijas Zinātņu akadēmija, science base funding, fundamental research, "
            "state-commissioned scientific research, and university science development/infrastructure lines. Keep broad ministry totals conservative."
        ),
    },
    "Israel": {
        "currency": "ILS",
        "currency_symbol": "₪",
        "language": "hebrew",
        "unit_hint": (
            "ERA-DEPENDENT currency and scale — read the opening 'סכום של...' declaration first. "
            "(1) 1975–1979: Israeli Pound / Lira (לירות / ל״י). Full lira units. Set currency='ILP', unit='unit'. "
            "(2) 1980–1985: Old Shekel (שקל / שקלים). Amounts in MILLIONS of shekels (מיליוני שקלים). Set currency='ILS_OLD', unit='million'. "
            "(3) 1986–~2019: New Israeli Shekel (שקל חדש / ₪). Amounts stated as 'אלפי שקלים חדשים' = THOUSANDS of NIS. Set currency='ILS', unit='thousand'. "
            "(4) 2020+: Full NIS, no scaling (שקלים חדשים only, no 'אלפי'). Set currency='ILS', unit='unit'. "
            "Always confirm from the document header — do not infer from number size alone."
        ),
        "known_agencies": [
            # Core science ministry (from 1992)
            "משרד המדע והטכנולוגיה / משרד המדע (Ministry of Science and Technology, budget code 19, from 1992)",
            "סעיף 19 — internal sub-codes: 02=R&D Council, 03=Research programmes, 05=Science infrastructure, 07=Space Agency",
            # Pre-1992 R&D governance
            "המועצה הלאומית למחקר ולפיתוח (National Council for Research and Development, code 74, pre-1992)",
            # Innovation / industrial R&D
            "רשות החדשנות הישראלית (Israel Innovation Authority, from 2016 — replaced the Office of the Chief Scientist)",
            "מדען ראשי (Office of the Chief Scientist / Chief Scientist) — appears as sub-line in Ministry of Economy/Industry, pre-2016",
            "קרן קמ\"ח (KAMEA competitive research fund — appears under code 31 05 in industry ministry)",
            # Basic research funding
            "קרן מדע ישראל (Israel Science Foundation / ISF — funds basic research at universities)",
            "האקדמיה הלאומית למדעים (Israel National Academy of Sciences and Humanities)",
            # Space
            "סוכנות החלל הישראלית (Israeli Space Agency, under Ministry of Science, sub-code 07)",
            # Universities — include when a named research or infrastructure line appears
            "האוניברסיטה העברית בירושלים (Hebrew University of Jerusalem)",
            "הטכניון — מכון טכנולוגי לישראל (Technion — Israel Institute of Technology)",
            "מכון ויצמן למדע (Weizmann Institute of Science)",
            "אוניברסיטת תל אביב (Tel Aviv University)",
            "אוניברסיטת בר-אילן (Bar-Ilan University)",
            "אוניברסיטת בן-גוריון (Ben-Gurion University of the Negev)",
            "אוניברסיטת חיפה (University of Haifa)",
            # Health / agriculture research
            "מרכז לבקרת מחלות (Center for Disease Control — health research)",
            "מרכז וולקני (Volcani Center — agricultural research, under Ministry of Agriculture)",
        ],
        "mixed_ministries": [
            "משרד הביטחון (Ministry of Defence) — skip unless 'מחקר/פיתוח' explicitly in the line description",
            "משרד החינוך (Ministry of Education) — skip broad totals; include only named science/research programmes",
            "ביטוח לאומי (National Insurance Institute / Bituah Leumi) — social insurance, not R&D",
            "משרד הבינוי והשיכון (Housing and Construction) — infrastructure, not R&D",
            "משרד התחבורה (Transport) — skip unless road-safety/tech research explicitly named",
            "Broad ministry section totals (two-digit code only, no sub-code) — these are aggregates",
        ],
        "doc_type_hint": (
            "Israeli חוק התקציב (Budget Law / Finance Act). "
            "DOCUMENT STRUCTURE: opening 'סכום של...' declares total and unit. "
            "Budget hierarchy: 2-digit ministry code (סעיף) → sub-code (תחום פעולה) → programme line (תכנית). "
            "E.g. '19 02 03' = Ministry of Science (19) / R&D Council (02) / Research programmes (03). "
            "MINISTRY CODE GUIDE: 19=Science, 20=Education, 15=Defence, 26=Environment. "
            "PRE-1992 R&D: no Ministry of Science; look for code 74 (National Council for R&D) and "
            "'מדען ראשי' sub-lines in the industry/economy ministry. "
            "POST-1992: Ministry of Science (code 19) is the primary R&D vehicle. "
            "POST-2016: Israel Innovation Authority (רשות החדשנות) replaces Chief Scientist at Ministry of Economy. "
            "KEY HEBREW R&D TERMS: מחקר (research), פיתוח (development), מו\"פ (R&D), מדע (science), "
            "טכנולוגיה (technology), חדשנות (innovation), מדען ראשי (chief scientist), קרן (fund). "
            "SKIP: defence procurement, social insurance (ביטוח לאומי), housing, roads, "
            "broad 2-digit ministry totals without sub-code detail."
        ),
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
    "Korea": {
        "currency": "KRW",
        "currency_symbol": "₩",
        "language": "korean",
        "ocr_langs": "kor+eng",
        "ocr_zoom": 3.0,
        "unit_hint": (
            "Korean budget-summary documents are document-dependent. Use the unit stated in the "
            "text exactly: 조원=billion-scale won summary amount, 억원=100 million won, 원=full won. "
            "Do NOT convert between units unless the document explicitly does so."
        ),
        "known_agencies": [
            "과학기술정보통신부 (Ministry of Science and ICT)",
            "국가연구개발 / 국가 R&D / 연구개발 / R&D",
            "AI", "반도체", "우주", "바이오", "양자",
            "과학기술", "혁신성장", "미래산업 전략 R&D",
        ],
        "mixed_ministries": [
            "Broad macro fiscal totals like 총지출, 총수입, 재정수지, 국가채무",
            "Welfare, housing, labour, and regional-support lines without R&D signal",
            "Loan, fund, and credit-support announcements without explicit R&D appropriation language",
            "PR / slogan / infographic captions without a concrete amount-programme pair",
        ],
        "doc_type_hint": (
            "Korean source files in this folder are mostly budget proposal summaries / briefs, "
            "not classic line-item appropriations laws. Extract only when an explicit won amount "
            "is tied to a named R&D programme, theme, or science ministry line. Prefer programme-"
            "level extraction over institutional hallucination. Several PDFs are image/graphic-heavy and "
            "may yield near-zero machine text; if a page is only macro narrative or no readable text, skip it."
        ),
    },
    "Colombia": {
        "currency": "COP",
        "currency_symbol": "$",
        "language": "spanish",
        "unit_hint": (
            "Amounts are in FULL Colombian pesos (pesos moneda legal) — set unit='unit', currency='COP'. "
            "There is NO 'miles de' or 'millones de' scaling in the detailed appropriations tables. "
            "Number format: period '.' is thousands separator (e.g. '92.568.145.000' = 92,568,145,000 COP). "
            "Comma ',' is the decimal separator. "
            "Do NOT divide amounts; extract them verbatim as full pesos."
        ),
        "known_agencies": [
            # Science & innovation ministry — primary R&D vehicle
            "COLCIENCIAS — Departamento Administrativo de Ciencia, Tecnología e Innovación (pre-2019, SECCIÓN ~1114)",
            "MinCiencias — Ministerio de Ciencia, Tecnología e Innovación (from 2019, replaces COLCIENCIAS)",
            "Fondo Francisco José de Caldas (managed by COLCIENCIAS/MinCiencias — competitive research grants)",
            # Industrial & applied R&D
            "SENA — Servicio Nacional de Aprendizaje (SECCIÓN 3602) — vocational training BUT also has significant R&D/innovation investment budget",
            "SENA sub-programmes: 'Fomento de la investigación, desarrollo tecnológico e innovación'",
            "iNNpulsa Colombia (agency for entrepreneurship and innovation, under MinCIT)",
            # Agricultural research
            "AGROSAVIA — Corporación Colombiana de Investigación Agropecuaria (from 2018, formerly CORPOICA)",
            "CORPOICA — Corporación Colombiana de Investigación Agropecuaria (pre-2018)",
            "ICA — Instituto Colombiano Agropecuario (plant/animal health + agri research)",
            # Environmental & technical research institutes
            "IDEAM — Instituto de Hidrología, Meteorología y Estudios Ambientales",
            "IGAC — Instituto Geográfico Agustín Codazzi",
            "INM — Instituto Nacional de Metrología (SECCIÓN 3505)",
            "ICONTEC / ONAC (metrology and standards, R&D-adjacent)",
            # Health research
            "Instituto Nacional de Salud (INS) — public health research",
            "Instituto Nacional de Cancerología",
            # Universities (include only explicit research/innovation programme lines)
            "Universidad Nacional de Colombia (UNAL) — research and innovation investment lines",
            "Universidades públicas with explicit 'investigación' or 'innovación' investment projects",
            # CTel royalties fund (off-budget but important context)
            "Sistema General de Regalías — Fondo de Ciencia, Tecnología e Innovación (CTel) — note: off-budget, may not appear in PGN",
        ],
        "mixed_ministries": [
            "Ministerio de Defensa (SECCIÓN 1501+) — skip unless 'investigación' or 'desarrollo tecnológico' explicit",
            "Ministerio de Educación (SECCIÓN 2201) — skip broad education totals; include only named research programmes",
            "Ministerio de Salud — skip general health transfers; include named research institute lines (INS, Cancerología)",
            "ICBF — social protection, not R&D",
            "Ministerio de Hacienda administrative lines",
            "Broad INTERSUBSECTORIAL lines (cross-ministry pooled appropriations) without research signal",
        ],
        "doc_type_hint": (
            "Colombian Ley del Presupuesto General de la Nación (PGN). "
            "DOCUMENT TYPES: Some years are plain legal text (legal wrapper only, no detailed annex tables); "
            "other years have detailed SECCIÓN-by-SECCIÓN tables. If the document is narrative law text only, "
            "yield will be sparse — that is expected, not an error. "
            "BUDGET STRUCTURE: SECCIÓN (4-digit entity code) → FUNCIONAMIENTO (A) or INVERSIÓN (C) → programme items. "
            "Key SECCIÓN codes: 1114=COLCIENCIAS/MinCiencias, 3602=SENA, 3505=INM, 1301=AGROSAVIA/CORPOICA. "
            "INVESTMENT PROGRAMMES (PRESUPUESTO DE INVERSION) are the most important for R&D — "
            "look for programme names containing 'investigación', 'ciencia', 'tecnología', 'innovación', 'I+D'. "
            "UNIT: full COP pesos, period=thousands separator. "
            "SKIP: defence (1501+), social protection (ICBF), housing, roads, broad 'TOTAL SECCIÓN' lines."
        ),
    },
    "Costa Rica": {
        "currency": "CRC",
        "currency_symbol": "₡",
        "language": "spanish",
        "unit_hint": (
            "Amounts are in FULL Costa Rican colones (colones corrientes) — set unit='unit', currency='CRC'. "
            "Modern documents (2010+) state 'en colones corrientes' in the header. "
            "Number format: period '.' is thousands separator (e.g. '9.500.000' = 9,500,000 CRC). "
            "1989 document may use narrative 'millones de colones' — read the context carefully. "
            "Do NOT scale amounts; extract verbatim."
        ),
        "known_agencies": [
            # Science ministry and main research funder
            "MICITT — Ministerio de Ciencia, Innovación, Tecnología y Telecomunicaciones",
            "CONICIT — Consejo Nacional para Investigaciones Científicas y Tecnológicas (main research funding body)",
            "Promotora Costarricense de Innovación e Investigación (PCII, budget code 001 in 2023+)",
            # Universities (FEES = Fondo Especial de Educación Superior is the bulk transfer; include research-labelled sub-lines)
            "UCR — Universidad de Costa Rica (large FEES transfer; include explicit research/Vínculo Externo lines)",
            "ITCR / TEC — Instituto Tecnológico de Costa Rica",
            "UNA — Universidad Nacional de Costa Rica",
            "UNED — Universidad Estatal a Distancia",
            "FEES — Fondo Especial de Educación Superior (bulk university transfer — mark as 'higher_education')",
            # Agricultural & environmental research
            "INTA — Instituto Nacional de Innovación y Transferencia en Tecnología Agropecuaria",
            "CATIE — Centro Agronómico Tropical de Investigación y Enseñanza",
            "SENASA (animal health research lines)",
            "SINAC research lines",
            # Health research
            "INCIENSA — Instituto Costarricense de Investigación y Enseñanza en Nutrición y Salud",
            "CCSS research lines (Caja Costarricense de Seguro Social — only explicit research sub-lines)",
            # Technical/standards
            "LANAMME-UCR — Laboratorio Nacional de Materiales y Modelos Estructurales",
            "INTECO / MEIC research lines",
        ],
        "mixed_ministries": [
            "Ministerio de Educación Pública — broad education; include only named research/science programmes",
            "Ministerio de Salud — include only INCIENSA or named research institute lines",
            "Ministerio de Obras Públicas y Transportes (MOPT) — infrastructure, not R&D",
            "CCSS general health services — only explicit research sub-lines",
            "Broad 'Transferencias' totals to autonomous institutions without research signal",
        ],
        "doc_type_hint": (
            "Costa Rican Ley de Presupuesto de la República. "
            "MULTI-VOLUME STRUCTURE: Several years (2013, 2014, 2017) are split into multiple Tomos (volumes). "
            "Each tomo covers different ministries/sectors. R&D agencies may appear in ANY tomo. "
            "Do not conclude a year is R&D-absent if only one tomo was reviewed. "
            "BUDGET CODE FORMAT (2010+): highly structured multi-column format — "
            "'C.SC.G. SG. P. SP. R. SR. FF' (Cuenta.Subcuenta.Grupo... Fuente de Financiamiento). "
            "R&D agencies appear as named line items (CONCEPTO) alongside these codes. "
            "KEY R&D TERMS (Spanish): investigación (research), desarrollo (development), innovación (innovation), "
            "ciencia (science), tecnología (technology), transferencia tecnológica, Vínculo externo UCR. "
            "UNIT: full colones, period=thousands separator. "
            "SKIP: MOPT roads/infrastructure, broad education transfers without research label, "
            "CCSS general health services, pension funds, public debt service."
        ),
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
