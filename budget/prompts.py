"""
Prompt templates for the budget pipeline.

Two passes:
  PASS 1 — SCAN: cheap relevance filter (haiku / gpt-4o-mini)
            Given a page of text, decide if it contains R&D budget data.
  PASS 2 — EXTRACT: structured extraction (sonnet / gpt-4o)
            Given relevant pages, extract all R&D line items as JSON.

Design principles:
  - Anti-hallucination: instruct the model to copy amounts verbatim, not infer.
  - Unit awareness: explicit instruction to preserve unit (million/thousand/billion).
  - Currency detection: extract symbol/code as written.
  - Confidence: 0–1 per item based on clarity of evidence in text.
  - No extrapolation: if data is absent, omit the item rather than guess.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# PASS 1 — SCAN SYSTEM PROMPT
# ---------------------------------------------------------------------------

SCAN_SYSTEM_PROMPT = """\
You are a specialist in government finance documents. Your task is to determine \
whether a page of text from a government Finance Bill or Budget document \
contains information about R&D (research and development), science, technology, \
or innovation SPENDING — i.e., appropriations, allocations, grants, or budget \
line items with monetary amounts.

For this pipeline, treat R&D as including:
- Core R&D phrases: research and development, R&D, scientific research, basic research, fundamental research, applied research, industrial research, translational research, experimental development
- Research-linked downstream stages when clearly tied to R&D: prototype development, pilot plant, demonstration project, testbed, validation, proof of concept
- Research infrastructure and science-system support: research laboratories, test facilities, instrumentation, observatories, supercomputing, data infrastructure, competitive research funding, doctoral/postdoctoral research programmes

Do NOT treat the following as R&D unless there is a clear research/prototype anchor:
- Innovation alone, technology alone, development alone
- Technology adoption, diffusion, incubators, accelerators, startup support, innovation vouchers
- Generic university funding, university operations, teaching, student welfare
- Generic economic development, digital government operations, hospital operations, routine laboratory diagnostics
- Defence procurement, military operations, readiness, personnel, bases, weapons acquisition

You answer with a JSON object only. No prose outside the JSON.

Respond with:
{
  "relevant": true | false,
  "confidence": 0.0–1.0,
  "reason": "one-sentence explanation"
}

Mark relevant=true if the page contains:
- Budget line items for research, science, technology, or innovation
- Appropriations for universities, research councils, national labs
- Departmental spending tables that include R&D-related agencies
- R&D fund allocations or grant programmes with amounts

Mark relevant=false if the page is:
- A table of contents, index, or title page
- Administrative text, legislative preamble, or definitions
- Spending on roads, defence (weapons), social welfare, health services, agriculture
  (unless explicitly for research)
- Narrative text with no amounts
"""


def build_scan_user_prompt(page_text: str, country: str, year: int, doc_hint: str = "") -> str:
    """Build the pass-1 user prompt for a single page."""
    hint_block = f"\nDocument type hint: {doc_hint}" if doc_hint else ""
    country_hint = _COUNTRY_SCAN_HINTS.get(country, "")
    country_block = f"\nCountry-specific signals: {country_hint}" if country_hint else ""
    return (
        f"Country: {country}\nYear: {year}{hint_block}{country_block}\n\n"
        f"--- PAGE TEXT ---\n{page_text[:4000]}\n--- END ---\n\n"
        "Is this page relevant for R&D budget extraction? Respond with JSON only."
    )


# Country-specific scan signals — injected into the scan prompt to help the
# cheap model identify relevant pages more accurately per document type.
_COUNTRY_SCAN_HINTS: dict[str, str] = {
    "Austria": (
        "Mark relevant=true for pages containing: "
        "'FWF', 'Fonds zur Förderung der wissenschaftlichen Forschung', 'Wissenschaftsfonds', "
        "'FFG', 'Forschungsförderungsgesellschaft', 'Austrian Research Promotion Agency', "
        "'FFF', 'Forschungsförderungsfonds', "
        "'ÖAW', 'Österreichische Akademie der Wissenschaften', 'Akademie der Wissenschaften', "
        "'AIT', 'Austrian Institute of Technology', 'Arsenal Research', "
        "'IST Austria', 'ISTA', 'Institute of Science and Technology Austria', "
        "'Christian Doppler', 'CD-Labor', "
        "'Ludwig Boltzmann Gesellschaft', "
        "'CERN-Beitrag', 'ESA-Beitrag', 'Europäische Weltraumorganisation', "
        "'UG 31', 'Untergliederung 31', 'Wissenschaft und Forschung', "
        "'Einzelplan 13', 'Wissenschaft und Forschung', "
        "'GB 31.03', 'Forschung und Entwicklung', "
        "'Universität Wien', 'TU Wien', 'TU Graz', 'JKU Linz', 'Universität Graz', "
        "'Universität Innsbruck', 'MedUni Wien', 'BOKU', 'WU Wien', "
        "'Bundesministerium für Wissenschaft', 'BMBWF', 'BMWFW', 'BMWF', "
        "or any page showing '(Beträge in Millionen Schilling)' or '(Beträge in Millionen Euro)' "
        "together with a named R&D institution or chapter. "
        "Mark relevant=false for: "
        "'Bundesministerium für Landesverteidigung', 'Heer', 'Miliz', 'Rüstung' without 'Forschung', "
        "'Pensionsversicherung', 'Krankenversicherung', 'AMS', 'Familienbeihilfe', 'Pflegegeld' "
        "(social insurance — NOT R&D), "
        "'ASFINAG', 'Straßenbau', 'ÖBB-Infrastruktur', 'Schieneninfrastruktur' without 'Forschung', "
        "'Bundesministerium für Inneres' without 'Forschung', "
        "and table-of-contents or legislative preamble pages. "
        "NOTE: 1975 and 1987 files in the main folder are SHORT (14-20 pages) — "
        "they contain only the budget law text, not the detailed appropriations tables. "
        "Accept any page from these files that mentions a research institution. "
        "NOTE: Files 1998-2004 have garbled budget table encoding "
        "('B U N D E S V O R A N S C H L A G' spaced letters, ©©© separators) — "
        "OCR will be used automatically; accept pages where OCR yields R&D terms."
    ),
    "Germany": (
        # Positive signals — pages to mark relevant=true
        "Mark relevant=true for pages containing: "
        "'Epl 30', 'Einzelplan 30', 'Bundesministerium für Bildung und Forschung', "
        "'BMBF', 'BMFT', 'Bundesminister für Forschung', 'Forschung und Technologie', "
        "'Deutsche Forschungsgemeinschaft', 'DFG', 'Tgr. 30', 'Tgr. 40', "
        "'Max-Planck-Gesellschaft', 'MPG', 'Fraunhofer-Gesellschaft', "
        "'Helmholtz-Gemeinschaft', 'Leibniz-Gemeinschaft', 'WGL', "
        "'Deutsches Zentrum für Luft- und Raumfahrt', 'DLR', "
        "'Funktionenübersicht', 'Funktion 137', 'Funktion 164', 'Funktion 165', "
        "'Wissenschaft, Forschung, Entwicklung', "
        "or any Haushaltsübersicht (Ausgaben) table whose rows include ministry Epl 30. "
        "IMPORTANT: Haushaltsübersicht pages list ALL Einzelpläne and ALWAYS include "
        "Epl 30 (BMBF/BMFT). Mark these pages relevant=true even if 'Forschung' is "
        "not visible in the excerpt — the BMBF row may appear after the excerpt cuts off. "
        # Negative signals — pages to mark relevant=false
        "Mark relevant=false for: "
        "Kreditfinanzierungsplan pages (debt and borrowing tables), "
        "Verpflichtungsermächtigungen pages ('dürfen fällig werden'), "
        "Flexibilisierte Ausgaben pages (internal budget flexibility rules), "
        "Stellenplan pages (staff headcount tables with grade columns A1-A16), "
        "Haushaltsvermerk / Erläuterungen pages (administrative text only, no amounts), "
        "Einzelplan pages for Epl 14 (Verteidigung), Epl 06 (Innern), Epl 60 (Finanzen), "
        "and pages showing only Mehrjährige Maßnahmen investment project lists. "
        "Numbers use SPACE as thousands separator: '14 053 404' = 14,053,404."
    ),
    "UK": (
        "Mark relevant=true for pages containing: "
        "'science budget', 'ring-fenced science', 'science and innovation', "
        "'research council', 'UKRI', 'BEIS', 'DSIT', 'BIS', 'DTI', 'DIUS', 'OST', "
        "'MRC', 'EPSRC', 'NERC', 'BBSRC', 'ESRC', 'AHRC', 'STFC', 'Innovate UK', "
        "'Industrial Strategy Challenge Fund', 'ISCF', 'ARIA', "
        "'research and development' with a £ amount, "
        "DEL tables headed 'R&D' or listing BEIS/DSIT/BIS rows, "
        "or tables with column headers 'Resource DEL' / 'Capital DEL' that include "
        "a science department row. "
        "Mark relevant=false for: "
        "macro overview pages (PSBR, GDP, monetary growth targets only), "
        "OBR economic and fiscal outlook tables, "
        "fiscal balance tables (current/capital account totals), "
        "tax schedule and VAT tables, debt management pages, "
        "non-science department DEL rows (Defence, Health, DWP, HMRC, Home Office), "
        "and pages discussing only R&D tax credits or 'fiscal cost of relief'."
    ),
    "France": (
        "Mark relevant=true for pages with: 'crédits de paiement', "
        "'Autorisations d'engagement', 'Programme 172', 'Programme 187', "
        "'Programme 190', 'Programme 193', 'ANR', 'CNRS', 'CEA', 'INSERM', "
        "'INRIA', 'CNES', 'État B', or any named R&D programme with a euro amount "
        "in the billions. "
        "CRITICAL: Mark relevant=FALSE for ANY page that contains 'ETPT', "
        "'équivalents temps plein', 'PLAFOND DES AUTORISATIONS D'EMPLOIS', "
        "'Plafond d'emplois', or 'temps plein travaillé' — even if it also "
        "mentions 'Recherche et enseignement supérieur'. These are staffing "
        "headcount pages with FTE integers, NOT euro budget pages. "
        "Also mark relevant=false for: tax code amendments ('montant ... est remplacé'), "
        "fiscal balance tables (solde structurel/conjoncturel), legislative preamble "
        "text (L'Assemblée nationale... a délibéré), and property tax articles."
    ),
    "Japan": (
        "Mark relevant=true for pages containing: 文部科学省 (MEXT), "
        "科学技術振興機構 (JST), 日本学術振興会 (JSPS), 理化学研究所 (RIKEN), "
        "宇宙航空研究開発機構 (JAXA), 海洋研究開発機構 (JAMSTEC), "
        "新エネルギー・産業技術総合開発機構 (NEDO), 科学技術, 研究費, "
        "研究振興費, or numeric amounts with 百万円 unit. "
        "Mark relevant=false for: 防衛省 (Defense), 国債費 (debt service), "
        "地方交付税 (local allocation tax), pages that are mostly OCR artifacts."
    ),
    "Australia": (
        "Mark relevant=true for pages with CSIRO, ARC, NHMRC, ANSTO, AIMS, "
        "Geoscience Australia, or research grant programmes. "
        "Mark relevant=false for: education commission pages, scholarship schemes, "
        "capital works tables, Defence department tables."
    ),
    "Sweden": (
        "Mark relevant=true for pages containing: "
        "'Vetenskapsrådet', 'VR', 'VINNOVA', 'Verket för innovationssystem', "
        "'Formas', 'Forte', 'FAS', 'SSF', 'Stiftelsen för Strategisk Forskning', "
        "'SMHI', 'Rymdstyrelsen', 'FOI', 'FOA', 'Totalförsvarets forskningsinstitut', "
        "'RISE', 'Swerea', 'SP ', 'Innventia', "
        "'NFR', 'TFR', 'MFR', 'HSFR', 'SJFR', 'NUTEK', 'STU ', "
        "'KTH', 'Chalmers', 'Karolinska', 'Uppsala universitet', "
        "'forskning', 'vetenskap', 'FoU', 'F&U', 'innovation', "
        "'UO 16', 'Utgiftsområde 16', 'Utbildning och universitetsforskning', "
        "'UO 24', 'Näringsliv', 'UO 20', 'miljö- och naturvård', "
        "anslag lines with codes like '2:1 Vetenskapsrådet', '1:1 Vetenskapsrådet', "
        "'tusental kronor', 'tkr', or any page where amounts appear alongside "
        "agency names that include 'forskning' or 'vetenskap'. "
        "Mark relevant=false for: "
        "'Studiemedel', 'studiebidrag', 'CSN', 'Centrala studiestödsnämnden' "
        "(student loans — NOT research), "
        "'Trafikverket', 'Vägverket', 'Banverket' without 'forskning' (transport), "
        "'Försvarsmakten', 'FMV', 'materielanskaffning', 'flygsystem' without 'forskning' "
        "(defence procurement — FOI/FOA IS legitimate), "
        "'Försäkringskassan', 'Pensionsmyndigheten', 'A-kassa' (social transfers), "
        "general cultural subsidies (teater, opera, film) without forskning, "
        "table-of-contents pages, and pages with no monetary amounts."
    ),
    "Poland": (
        "Mark relevant=true for pages containing: "
        "'NCN', 'Narodowe Centrum Nauki', 'National Science Centre', "
        "'NCBiR', 'NCBR', 'Narodowe Centrum Badań i Rozwoju', "
        "'PAN', 'Polska Akademia Nauk', 'Polish Academy of Sciences', "
        "'KBN', 'Komitet Badań Naukowych', "
        "'MNiSW', 'Ministerstwo Nauki i Szkolnictwa Wyższego', "
        "'MEiN', 'Ministerstwo Edukacji i Nauki', "
        "'Część 28', 'Czesc 28', 'szkolnictwo wyższe i nauka', 'Szkolnictwo wyzsze', "
        "'Część 67' (PAN), "
        "'Dział 740', 'badania naukowe', 'prace badawczo-rozwojowe', 'B+R', "
        "'CERN', 'składka do CERN', "
        "'Politechnika', 'Uniwersytet', 'AGH', "
        "or any table with 'tys. zł' amounts alongside research/university line items. "
        "IMPORTANT: pages showing Część 28 budget tables are ALWAYS relevant. "
        "Mark relevant=false for: "
        "'ZUS', 'Zakład Ubezpieczeń Społecznych', 'KRUS', "
        "'Część 73' or 'Część 74' (social insurance — very large amounts, clearly not R&D), "
        "'Część 29' without 'badania' (defence), "
        "'obsługa długu', 'dług publiczny' (public debt service), "
        "'GDDKiA', 'PKP', 'infrastruktura drogowa' without 'badania' (transport), "
        "'oświata i wychowanie' without 'badania' (primary/secondary school), "
        "rezerwa ogólna (general reserve without named R&D programme), "
        "and table-of-contents or legislative preamble pages."
    ),
    "Slovakia": (
        "Mark relevant=true for pages containing: "
        "'SAV', 'Slovenská akadémia vied', 'Slovak Academy of Sciences', "
        "'APVV', 'Agentúra na podporu výskumu a vývoja', "
        "'VEGA', 'Vedecká grantová agentúra', "
        "'Ministerstvo školstva SR', 'Ministerstvo školstva, vedy, výskumu', "
        "'Ministerstvo školstva, výskumu, vývoja a mládeže', "
        "'kapitola 20', 'kapitola 51', "
        "'výskum', 'vývoj', 'veda', 'vedeck', 'inováci', "
        "'oblasť 740', 'oblast 740', 'veda a výskum', "
        "'CERN', 'príspevok do CERN', "
        "'tis. Sk' or 'EUR' amounts alongside any research or university line items, "
        "or any page showing Ministerstvo školstva or SAV budget tables. "
        "Mark relevant=false for: "
        "'Ministerstvo obrany SR' without 'výskum' (defence), "
        "'Sociálna poisťovňa', 'sociálne dávky', 'dôchodky' (social transfers), "
        "'dlhová služba', 'štátny dlh' (debt service), "
        "'základné školstvo', 'stredné školstvo', 'materské školy' without 'výskum' "
        "(primary/secondary school, not R&D), "
        "'cestná infraštruktúra', 'diaľnice', 'železnice' without 'výskum' (transport), "
        "and legislative preamble or table-of-contents pages. "
        "CRITICAL: The file '1990 text.pdf' is actually a POLISH budget document "
        "(Dziennik Ustaw Rzeczypospolitej Polskiej) — mark ALL pages relevant=false."
    ),
    "Luxembourg": (
        "Mark relevant=true for pages containing: "
        "'FNR', 'Fonds National de la Recherche', 'Fonds national de la recherche', "
        "'Université du Luxembourg', 'Uni.lu', 'UniLu', "
        "'LIST', 'Luxembourg Institute of Science and Technology', "
        "'LISER', 'CRP Henri Tudor', 'CRP Gabriel Lippmann', 'CRP Santé', "
        "'LIH', 'Luxembourg Institute of Health', "
        "'CEPS/INSTEAD', 'CEPS INSTEAD', "
        "'Service de Coordination de la Recherche et de l'Innovation', 'SCRI', "
        "'Ministère de l'enseignement supérieur et de la recherche', "
        "'Département de la culture, de l'enseignement supérieur et de la recherche', "
        "'03 — MINISTERE DE L'ENSEIGNEMENT SUPERIEUR', "
        "'Section 03', '03.0', '03.1', '03.2', "
        "'recherche', 'Recherche', 'innovation', 'subvention de recherche', "
        "'programme de recherche', 'fonds de recherche', 'technologie', "
        "or any page showing amounts for a named R&D institute or research-ministry section. "
        "Mark relevant=false for: "
        "'Caisse Nationale d'Assurance Pension', 'CNAP', 'pensions' (social insurance), "
        "'service de la dette', 'intérêts de la dette' (debt service), "
        "'Ministère de la Défense' without 'recherche' (defence), "
        "'routes', 'autoroutes', 'travaux publics' without 'recherche' (infrastructure), "
        "'Ministère de l'Education nationale' broad totals without named university/research line, "
        "and table-of-contents or legislative preamble pages. "
        "NOTE: Year 1986 is missing — no file. "
        "NOTE: The 1997 and 2002 files are large scanned images; mark relevant=true broadly for "
        "any page that OCR suggests is a budget table in the research/education section."
    ),
    "Portugal": (
        "Mark relevant=true for pages containing: "
        "'FCT', 'Fundação para a Ciência e a Tecnologia', 'FCT, I.P.', "
        "'JNICT', 'Junta Nacional de Investigação Científica e Tecnológica', "
        "'INIC', 'Instituto Nacional de Investigação Científica', "
        "'ANI', 'Agência Nacional de Inovação', "
        "'LNEC', 'Laboratório Nacional de Engenharia Civil', "
        "'LNETI', 'INETI', 'Instituto Nacional de Engenharia e Tecnologia Industrial', "
        "'INIAV', 'Instituto Nacional de Investigação Agrária', "
        "'Ministério da Ciência', 'MCTES', 'MCES', "
        "'Capítulo 50', 'Cap. 50', "
        "'P002', 'P-002', 'Investigação Científica e Tecnológica e Inovação', "
        "'investigação científica', 'I&D', 'ciência e tecnologia', "
        "'bolsas de doutoramento', 'bolsas de investigação', 'centros de investigação', "
        "'CERN', 'ESA', "
        "'MAPA V', 'MAPA VII', 'DESPESAS DOS SERVIÇOS E FUNDOS AUTÓNOMOS', "
        "'RECEITAS DOS SERVIÇOS E FUNDOS AUTÓNOMOS', "
        "or any page showing a NAMED institutional budget row for FCT/JNICT/ANI/LNEC in an annual budget table. "
        "CRITICAL — Mark relevant=false for pages containing: "
        "'Polícia Judiciária', 'PJ', 'PGR', 'PSP', 'GNR', 'SEF' "
        "(investigação here = criminal investigation, NOT scientific R&D), "
        "'Segurança Social', 'prestações sociais', 'pensões' (social transfers), "
        "'serviço da dívida', 'encargos com a dívida' (debt service), "
        "'Forças Armadas', 'Defesa Nacional' without 'investigação científica' label, "
        "'Infraestruturas de Portugal', 'EP Estradas', 'REFER' (transport infrastructure), "
        "'ensino básico', 'ensino secundário' broad totals without named R&D, "
        "'responsabilidades contratuais plurianuais', 'programação financeira plurianual', "
        "'MAPA 14', 'PIDDAC APOIOS', 'PIDDAC TRADICIONAL', 'Nº Projectos', "
        "pages that only show project financing schedules or plurianual columns rather than a current-year institutional appropriation, "
        "and pages that only mention transfers to FCT/ANI/LNEC/JNICT in legal text "
        "('transferência', 'transfer of funds', 'até ao montante', 'Fundo Azul', 'Fundo de Contragarantia Mútuo'). "
        "Treat those pages as not relevant unless they ALSO contain a named annual budget row for the institution. "
        "and table-of-contents or legislative preamble pages. "
        "NOTE: Files 1977-2000 are scanned — accept any page that OCR suggests "
        "contains a science ministry section or named R&D institution. "
        "NOTE: 'Lei orcamento para 1985.pdf' is an exact duplicate of the 1986 file — "
        "mark ALL pages relevant=false for the 1985 filename."
    ),
    "Mexico": (
        "DOCUMENT STRUCTURE — CRITICAL: Each file is a full Diario Oficial de la Federación (DOF) issue "
        "containing MANY separate decrees and laws — not just the budget. "
        "The Presupuesto de Egresos de la Federación (PEF) is ONE decree within the DOF, "
        "always published in the ÚLTIMA SECCIÓN (final section — called 'Segunda Sección', "
        "'Cuarta Sección', 'Quinta Sección', or 'Última Sección' depending on year). "
        "'Primera Sección' of the DOF contains OTHER laws and decrees (NOT the budget) — "
        "mark Primera Sección pages relevant=false unless they explicitly name R&D agencies. "
        "Look for 'DECRETO por el que se expide el Presupuesto de Egresos' to identify the budget start. "
        "\n"
        "PRIORITY PAGES — always mark relevant=true: "
        "Pages titled 'ANEXO N. PROGRAMA DE CIENCIA Y TECNOLOGÍA' or "
        "'ANEXO N. PROGRAMA DE CIENCIA, TECNOLOGÍA E INNOVACIÓN' (N = any number, e.g. 8, 9, 10) — "
        "these are the key R&D expenditure breakdown tables. "
        "TABLE COLUMN STRUCTURE on ANEXO tables: "
        "Ramo | Unidad Responsable | Proyecto | AMPLIACIONES | Aprobado | Recursos Propios | Monto Total. "
        "Extract the 'Aprobado' column (approved federal appropriation). "
        "Do NOT use 'Monto Total' — it adds own-source revenues and double-counts. "
        "Unit is always full PESOS on these tables (header says '(pesos)' — set unit='unit'). "
        "\n"
        "Mark relevant=true for pages containing: "
        "'CONACYT', 'CONAHCyT', 'Consejo Nacional de Ciencia y Tecnología', "
        "'Consejo Nacional de Humanidades, Ciencias y Tecnologías', "
        "'Ramo 38', '38 CONACYT', '38 CONAHCyT', "
        "'CINVESTAV', 'CICESE', 'CIESAS', 'CIDESI', 'CIQA', 'CIAD', 'CENAPRED', "
        "'CIATEJ', 'CICY', 'INFOTEC', 'CENIDET', 'CIMAV', "
        "'ININ', 'Instituto Nacional de Investigaciones Nucleares', "
        "'INIFAP', 'Instituto Nacional de Investigaciones Forestales', "
        "'Centros Públicos de Investigación', 'centros de investigación', "
        "'Agencia Espacial Mexicana', 'AEM ', "
        "'investigación científica', 'investigación y desarrollo', "
        "'ciencia y tecnología', 'humanidades, ciencias, tecnologías e innovación', "
        "'Fondo Sectorial', 'Fondo Mixto', 'Fondo Institucional', 'FOINS', "
        "'PRONACES', 'Programas Nacionales Estratégicos', "
        "'posgrado e investigación', 'innovación tecnológica', "
        "'IPN', 'Instituto Politécnico Nacional', 'UNAM investigación', "
        "or any table showing pesos amounts alongside any of these agency names. "
        "\n"
        "FALSE POSITIVE WARNING — 'investigación' appears THROUGHOUT the DOF in non-R&D contexts: "
        "international treaties on judicial/criminal cooperation ('asistencia jurídica', "
        "'cooperación judicial', 'extradición'), procurement law ('licitación e investigación de mercado'), "
        "criminal procedure codes ('investigación ministerial', 'investigación penal'), "
        "health inspections ('investigación epidemiológica'), and many other decrees. "
        "A page mentioning 'investigación' is NOT automatically relevant — "
        "it must also name a science agency (CONACYT, IPN, UNAM, CPI name) OR "
        "be from the PEF budget decree AND show R&D appropriation lines. "
        "\n"
        "Mark relevant=false for: "
        "'IMSS' or 'Instituto Mexicano del Seguro Social' pension/guarderías lines "
        "(social security — NOT R&D; amounts in billions), "
        "'ISSSTE' pension lines, "
        "'Ramo 06' or 'Ramo 24' (debt service — servicio de la deuda), "
        "'Ramo 07' SEDENA or 'Ramo 13' SEMAR without explicit R&D signal, "
        "'SCT' or 'SICT' road/transport infrastructure without 'investigación', "
        "'Ramo 28' or 'Ramo 33' or 'Ramo 39' (federal transfers to states — aportaciones), "
        "'Guardia Nacional', 'PGR', 'FGR', 'Ministerio Público' crime investigation "
        "('investigación' here = criminal investigation, not scientific R&D), "
        "international treaties and judicial-cooperation agreements (Primera Sección staples), "
        "'BIENESTAR', 'SEDESOL' social programmes without research label, "
        "'servicio de la deuda', 'intereses de la deuda' (debt interest), "
        "'gasto neto total' or 'presupuesto total' macro-summary rows only, "
        "and table-of-contents or legislative preamble pages. "
        "NOTE: Files '1999 MEX ...' and '2000 MEX ...' are exact duplicate files — "
        "mark ALL pages relevant=false on these duplicate files."
    ),
    "Turkey": (
        "Mark relevant=true for pages containing: "
        "'TÜBİTAK', 'TUBITAK', 'Türkiye Bilimsel ve Teknolojik Araştırma Kurumu', "
        "'TÜBA', 'Türkiye Bilimler Akademisi', "
        "'TAEK', 'Türkiye Atom Enerjisi Kurumu', 'Atom Enerjisi', "
        "'Sanayi ve Teknoloji Bakanlığı', 'Bilim, Sanayi ve Teknoloji', "
        "'KOSGEB', 'Küçük ve Orta Ölçekli İşletmeler', "
        "'Türkiye Uzay Ajansı', 'TUA', "
        "'Ar-Ge', 'araştırma-geliştirme', 'bilimsel araştırma', "
        "'teknoloji geliştirme', 'inovasyon', "
        "'Bilimsel Araştırma Projeleri', 'BAP', "
        "'YÖK', 'Yükseköğretim Kurulu', "
        "'(II) SAYILI CETVEL', 'ÖZEL BÜTÇELİ İDARELER', "
        "or any page showing appropriation tables for named R&D institutions. "
        "CRITICAL — Mark relevant=false for pages containing: "
        "'(I) SAYILI CETVEL' ONLY if TÜBİTAK/TÜBA/TAEK are not also on the page "
        "(these agencies are in (II) SAYILI CETVEL and absent from the general budget table), "
        "'kamu borç', 'borç servisi', 'Hazine', 'faiz giderleri' (debt service), "
        "'Sosyal Güvenlik Kurumu', 'SGK', 'emeklilik', 'Bağ-Kur' (social security/pensions), "
        "'İçişleri Bakanlığı', 'Emniyet', 'Jandarma' without explicit 'Ar-Ge' label "
        "('araştırma' here = criminal investigation), "
        "'Millî Savunma' without explicit 'Ar-Ge' label, "
        "'Karayolları', 'Devlet Demiryolları', 'TCDD' transport infrastructure without Ar-Ge, "
        "'İlköğretim', 'Ortaöğretim', 'MEB' K-12 education broad totals, "
        "and table-of-contents or legislative preamble pages. "
        "DOCUMENT TYPE WARNINGS — Turkey collection contains mixed document types: "
        "(1) 'GenelFaaliyetRaporu_XXXX.pdf' = post-facto General Activity Report, NOT a budget "
        "appropriation document — mark ALL pages relevant=false. "
        "(2) 'Merkezi-Yonetim-Kesin-Hesabi' = Central Government Final Accounts (actual "
        "spending, not appropriations) — mark relevant=false unless you need out-turn data. "
        "(3) 'tbmm22140033ss1270/ss1271.pdf' = Kesin Hesap Kanunu (Final Accounts Law) — skip. "
        "(4) 'kanuntbmmc09XXXXX.pdf' = budget law articles only, no appropriation tables — "
        "mark relevant=false (budget amounts are in the annex Cetveller, not the law text). "
        "(5) '2-a-XXXX-Yılı-Genel-Bütçeli-İdareler-Ekonomik-Kod-İcmali' (3-page files, "
        "2010-2025) = General Budget economic-code summary, NO TÜBİTAK/TÜBA/TAEK — "
        "mark ALL pages relevant=false. "
        "BEST FILES: large UUID-named PDFs (e.g. 'f8229ba6-...pdf', 'dfd35a0d-...pdf'), "
        "'ButceGerekcesi_XXXX.pdf', or the multi-hundred-page numbered files (e.g. '1981 17265.pdf'). "
        "NOTE: 1990 file is an exact duplicate of 1991 — mark all 1990 pages relevant=false. "
        "NOTE: Pre-2005 TRL amounts appear as 12-15 digit numbers (hyperinflation era) — "
        "this is expected, not an error."
    ),
    "Canada": (
        "Mark relevant=true for pages containing: "
        "'NSERC' or 'Natural Sciences and Engineering Research Council', "
        "'SSHRC' or 'Social Sciences and Humanities Research Council', "
        "'CIHR' or 'Canadian Institutes of Health Research', "
        "'NRC' or 'National Research Council', "
        "'CFI' or 'Canada Foundation for Innovation', "
        "'Canadian Space Agency', 'TRIUMF', 'Genome Canada', "
        "'Canada Research Chairs', 'AECL' or 'Atomic Energy of Canada', "
        "or any Vote labelled 'Grants and contributions' for a science agency. "
        "Mark relevant=false for: National Defence operating pages, RCMP, "
        "CRA tax administration, general infrastructure departments, "
        "employee benefit plan schedules, and statutory payment tables."
    ),
}


# ---------------------------------------------------------------------------
# PASS 2 — EXTRACTION SYSTEM PROMPT
# ---------------------------------------------------------------------------

EXTRACT_SYSTEM_PROMPT = """\
You are an expert government budget analyst specialising in R&D and science \
spending (GBARD — Government Budget Appropriations and Outlays for R&D). \
Your task is to extract ALL budget line items related to R&D, science, \
technology, higher education, and innovation from the provided Finance Bill text.

## Output format
Return a JSON object with a single key "items" containing an array of objects.

Each item must have an "item_type" that distinguishes:
- "section_total"  — the rolled-up total for a whole ministry/portfolio/vote
- "program_total"  — a named sub-programme within a section that has its own total
- "line_item"      — an individual appropriation line within a programme

Always extract BOTH the aggregate total AND the individual programme lines when
both are present. This lets downstream analysis cross-check totals vs components.

{
  "items": [
    {
      "item_type":          "section_total | program_total | line_item",
      "section_code":       "string — ministry/vote/chapter code as printed, e.g. '20', 'Vote:SciInno', 'DEPT'",
      "section_name":       "string — ministry/portfolio/vote name in original language",
      "section_name_en":    "string — English translation (copy if already English)",
      "line_description":   "string — programme or line description in original language",
      "line_description_en":"string — English translation (copy if already English)",
      "amount_local":       "number | null — numeric amount EXACTLY as printed (no scaling)",
      "unit":               "string — unit from document header or context: 'million', 'thousand', 'billion', 'dollar', 'kr', or 'as_printed'",
      "currency":           "string — ISO 4217 code: 'GBP', 'DKK', 'AUD', 'CAD', 'NZD', 'EUR', 'USD', etc.",
      "rd_category":        "one of: direct_rd | higher_education | research_infrastructure | innovation_instruments | science_agency | rd_adjacent | unclear",
      "decision":           "one of: include | review",
      "confidence":         "number 0.0–1.0",
      "page_number":        "string — page number(s) where found",
      "notes":              "string — any caveats, ambiguities, or context (e.g. 'total includes non-R&D components')"
    }
  ]
}

## What R&D means in this project
INCLUDE the following as in-scope:
- Core R&D: research and development, R&D, scientific research, basic research, fundamental \
research, applied research, industrial research, translational research, experimental development
- R&D-linked activity: prototype development, pilot plant, demonstration project, testbed, \
proof of concept, validation study (when tied to a research programme)
- Research infrastructure: research laboratories, test facilities, instrumentation, \
observatories, supercomputing, data infrastructure FOR RESEARCH
- Dedicated science agencies (e.g. CSIRO, research councils, national labs, medical research \
funds): their OPERATING appropriations are in-scope
- Competitive research grants, research fellowships (when awarded to carry out research, \
not just study)
- Higher education institutions: include ONLY if the line is explicitly for research \
activities, not for general teaching operations

EXCLUDE the following — these are the most common false positives found in government budgets.
WARNING: These exclusion rules apply even when the item appears inside a Department of \
Science or a research agency section. The SECTION context does NOT override the LINE \
DESCRIPTION rule. Read the line description carefully before deciding.

PATTERN 1 — FOREIGN AID / DEVELOPMENT ASSISTANCE:
  Any item under a foreign affairs, development assistance, or overseas aid ministry/agency \
  that funds activities in another country, even if the words "research" or "science" appear \
  in the description. Examples: "Agricultural Research Centres [in Asia]", "Registry of \
  Scientific and Technical Services [for developing countries]", "Regional Training and \
  Research Centres [overseas]", "International science, technology and research programs \
  [development assistance context]".
  Rule: if the beneficiary is a foreign country or the programme is administered by a \
  development assistance agency, SKIP it.

PATTERN 2 — EDUCATION MONITORING, EVALUATION AND CURRICULUM:
  Items in education departments or higher-education commissions for monitoring school \
  outcomes, curriculum development, evaluative studies, or education policy research. \
  Examples: "Research and Development in Education [Aboriginal secondary grants evaluation]", \
  "Research and investigations [Commission on Advanced Education]", \
  "Research and investigations [Commission on Technical and Further Education]", \
  "Evaluative studies [school performance]", "Curriculum Development Centre", \
  "Bureau of Labour Market Research".
  Rule: SKIP unless the item explicitly funds natural-science or engineering research \
  conducted at a university or dedicated research institute. The phrase "research and \
  investigations" alone — in an education, training, or welfare body — is NOT sufficient.

PATTERN 3 — CAPITAL INFRASTRUCTURE, EQUIPMENT PROCUREMENT AND DISBURSEMENT MECHANISMS:
  Building grants, capital works, equipment purchases, maintenance, and inter-government \
  transfer mechanisms that are not tied to a specific ongoing research programme. \
  Examples: "Grant towards cost of new building [for a medical institute]", \
  "Instruments and apparatus [Bureau of Meteorology]", "Plant and Equipment", \
  "Capital works — Plant and Equipment", "Division NNN — CAPITAL WORKS AND SERVICES", \
  "Repairs and Maintenance", "Payments to or for the States [block transfer]".
  Rule: SKIP pure capital/construction/procurement/maintenance/transfer items. \
  Include only if the description explicitly says the infrastructure is FOR a specific \
  research purpose (e.g. "construction of research laboratory", "telescope instrumentation").

PATTERN 4 — SCHOLARSHIPS, FELLOWSHIPS AND STUDENT SUPPORT:
  Student scholarships, travel fellowships, and exchange programmes that fund \
  individuals to study or travel, not to conduct defined research projects. Examples: \
  "Commonwealth Scholarship and Fellowship Plan", "Queen Elizabeth II Fellowship Scheme", \
  "Colombo Plan scholarships", "Student assistance".
  Rule: SKIP scholarships and student grants — including named schemes such as the \
  Queen Elizabeth II Fellowship Scheme and Colombo Plan. Include only postdoctoral or \
  competitive research fellowships explicitly awarded for conducting a research project.

PATTERN 5 — REGULATORY, ADVISORY AND ADMINISTRATIVE SERVICES:
  Reimbursements for regulatory oversight, advisory committees, information services, \
  and general administration. Examples: "Reimbursement for environmental regulatory \
  services for uranium mining", "Advisory committee on science", "Registry of technical \
  services", "Assistance to inventors [patent advice service]", \
  "Consumer protection—development costs, investigations and grants to consumers".
  Rule: SKIP if the activity is regulation, compliance, advice, consumer protection, \
  or information dissemination rather than research.

PATTERN 6 — SECTION TOTALS FOR MIXED-PURPOSE MINISTRIES:
  Do NOT extract a section_total as "include" if the ministry has a broad mandate. \
  Examples of mixed ministries whose totals must NOT be included: Department of Health, \
  Hospitals and Health Services Commission, Department of Education, Commission on \
  Advanced Education, Commission on Technical and Further Education, Department of \
  Transport, Department of Housing, Department of Primary Industry, Department of \
  Minerals and Energy, any department with "Foreign Affairs" or "Overseas" in the name. \
  Only extract section_totals as "include" for dedicated science or research agencies \
  (e.g. Department of Science, CSIRO, Atomic Energy Commission, Research Councils, \
  Medical Research Council). For mixed ministries, mark as "review" if you extract \
  the total at all, or better yet SKIP the section_total and extract only the R&D lines.

PATTERN 7 — COMMERCIAL SUBSIDIES AND OIL/MINERAL EXPLORATION:
  Items labelled "subsidy" or "exploration grant" for commercial resource extraction \
  are NOT R&D. Examples: "Search for oil—Subsidy", "Mineral exploration subsidy", \
  "Petroleum exploration incentive". A subsidy to an oil company to drill is not \
  research even if a science agency administers it.
  Rule: SKIP any line with "subsidy" or "exploration grant" unless the description \
  explicitly says "geological research", "scientific survey", or equivalent.

PATTERN 8 — ADMINISTRATIVE AND MAINTENANCE EXPENSES AT RESEARCH FACILITIES:
  A line for "administrative expenses", "running costs", "salaries", "overhead", \
  or "repairs and maintenance" at a research station or institute is NOT R&D spending. \
  Examples: "Kimberley Research Station—Administrative expenses", \
  "Department of Science—Repairs and Maintenance", "Research station—Salaries and overhead".
  Rule: SKIP if the line funds general administration or maintenance of a facility \
  that happens to have "research" in its name. Only include lines that explicitly \
  fund research activities themselves.

PATTERN 9 — "PROMOTION AND RESEARCH" COMBINED ITEMS:
  Line items that combine promotion/awareness/safety campaigns with research \
  (e.g. "Road safety promotion and research", "Health promotion and research") \
  are typically dominated by the promotion component.
  Rule: SKIP these combined items unless the research component is separately \
  quantified in the text. Do NOT mark them as "include".

## Applying the rules — decision logic
For every candidate item, apply a strict three-question test:
  1. Does the LINE DESCRIPTION (not just the section name) contain explicit R&D content?
  2. Is the beneficiary a domestic research performer (not a foreign recipient, \
     consumer protection scheme, or education evaluation body)?
  3. Is the activity operational R&D (not capital procurement, maintenance, \
     regulation, scholarships, or inter-government transfer)?
If the answer to all three is YES → include.
If uncertain on any one → review.
If any is clearly NO → SKIP (do not extract at all).

IMPORTANT: "Department of Science" in the section_name does NOT automatically make \
every line under it R&D. Each line_description must independently pass the three-question \
test above.

## Category definitions
- direct_rd:              Grants, contracts, appropriations labelled as R&D or research
- higher_education:       Funding to universities explicitly for research activities
- research_infrastructure: Equipment, facilities, computing explicitly for research use
- innovation_instruments:  Technology transfer, industry R&D incentives (not scholarships)
- science_agency:         Core operating funding for dedicated science agencies (CSIRO, \
                          research councils, national labs, medical research funds)
- rd_adjacent:            Science communication, international research cooperation (not aid)
- unclear:                Relevant but category not determinable from text

## Decision rules
- include:  Clear evidence this is domestic R&D/science spending based on the LINE \
            DESCRIPTION (not just the section heading). Confidence ≥ 0.7.
- review:   Plausibly relevant but line description is ambiguous or mixed-purpose. \
            Confidence 0.4–0.69.
Do NOT extract items you would mark as "skip" — just omit them from the output.

## CRITICAL instructions — anti-hallucination
1. NEVER invent numbers. Only extract amounts that appear VERBATIM in the text.
2. NEVER scale amounts. If text says "387,000" write 387000; unit comes from the \
   document header (e.g. "$ thousand" → unit="thousand").
3. If an amount is absent, set amount_local to null.
4. Read the FULL line description before deciding. The section name alone is not \
   sufficient — "Department of Science" can contain non-R&D lines.
5. NEVER fabricate a budget row from narrative/legal prose. If the page contains only \
   legal authorisations, policy commitments, or descriptive text without an explicit \
   budget table row or clearly printed appropriation amount tied to the item, DO NOT \
   emit a synthetic "total", "funding", or "appropriation" line.
6. If a page mentions a science body (e.g. FCT, ANI, university, laboratory) but does \
   not print a defendable annual budget amount for that body on the page, return no item \
   for that body. Mentions alone are not enough.
7. Output valid JSON only. No prose, no markdown fences, no explanation outside JSON.

## Number format guide by country
Different countries use different thousands separators. Parse accordingly:
- Germany: SPACE as thousands separator → "14 053 404" = 14053404 (fourteen million)
  "356 400" = 356400 (three hundred fifty-six thousand)
- France: SPACE as thousands separator → "1 234 567" = 1234567; also "1.234.567"
- Japan: Commas or no separator → "357,048" = 357048 (millions of yen)
- UK: Commas → "1,234" = 1234; prose uses "£1.6 billion"
- Australia/Canada: Commas → "387,000" = 387000 (already in thousands)
"""


def build_extract_user_prompt(
    pages_text: str,
    country: str,
    year: int,
    currency: str,
    unit_hint: str,
    doc_hint: str,
    known_agencies: list[str],
    mixed_ministries: list[str] | None = None,
    page_range: str = "",
) -> str:
    """Build the pass-2 user prompt from concatenated page text."""
    from budget.country_profiles import build_country_addendum

    agencies_block = ""
    if known_agencies:
        sample = known_agencies[:10]
        agencies_block = (
            "\n\nKnown R&D agencies for this country (section_totals for these ARE in-scope):\n"
            + "\n".join(f"  - {a}" for a in sample)
        )

    mixed_block = ""
    if mixed_ministries:
        sample = mixed_ministries[:10]
        mixed_block = (
            "\n\nMixed-purpose ministries — do NOT mark their section_totals as 'include':\n"
            + "\n".join(f"  - {a}" for a in sample)
        )

    country_addendum = build_country_addendum(country, year=year)
    addendum_block = f"\n\n{country_addendum}" if country_addendum else ""

    page_block = f" (pages {page_range})" if page_range else ""
    unit_note = f"Currency in this document is typically {currency}. Amounts are typically in {unit_hint}."

    return (
        f"Country: {country}\n"
        f"Year: {year}\n"
        f"Document type: {doc_hint}\n"
        f"{unit_note}"
        f"{agencies_block}"
        f"{mixed_block}"
        f"{addendum_block}\n\n"
        f"--- BUDGET DOCUMENT TEXT{page_block} ---\n"
        f"{pages_text}\n"
        f"--- END OF TEXT ---\n\n"
        "Extract ALL R&D-relevant budget items from the text above.\n"
        "IMPORTANT: For each R&D-related section or portfolio, extract:\n"
        "  1. The SECTION TOTAL (item_type='section_total') — total budget for the ministry/vote\n"
        "  2. Each PROGRAMME TOTAL (item_type='program_total') — named sub-programmes with their own totals\n"
        "  3. Individual LINE ITEMS (item_type='line_item') — specific appropriation lines\n"
        "If a document only shows totals (no breakdown), extract what is available.\n"
        "Return a JSON object with key 'items'. If no relevant items found, return {\"items\": []}."
    )


# ---------------------------------------------------------------------------
# PASS 2 — SELF-CHECK / CONSISTENCY PROMPT (optional 3rd pass)
# ---------------------------------------------------------------------------

CONSISTENCY_SYSTEM_PROMPT = """\
You are a budget data quality reviewer. You will receive a list of extracted \
R&D budget items from a Finance Bill. Your task is to:
1. Flag items where the amount seems implausible given the country/year/unit context.
2. Flag items where the rd_category seems wrong.
3. Correct obvious unit errors (e.g., an amount of 12000 labelled "billion" when it \
   should be "thousand").
4. Remove duplicate items (same description + amount appearing twice).
5. Return the corrected list in the same JSON format.

Return JSON only: {"items": [...]}
"""


def build_consistency_user_prompt(items: list[dict], country: str, year: int) -> str:
    import json as _json
    return (
        f"Country: {country}, Year: {year}\n\n"
        f"Extracted items:\n{_json.dumps(items, indent=2, ensure_ascii=False)}\n\n"
        "Review and correct the items. Return {\"items\": [...]} with the corrected list."
    )


# ---------------------------------------------------------------------------
# PASS 1 — BATCH SCAN (scan multiple pages at once for cost efficiency)
# ---------------------------------------------------------------------------

BATCH_SCAN_SYSTEM_PROMPT = """\
You are a specialist in government finance documents. You will receive multiple \
pages of text from a Finance Bill. For each page, determine if it contains \
genuine R&D budget line items with monetary amounts.

Return ONLY this exact JSON structure — no extra fields, no reasons, no prose:
{"pages": [{"page_id": <number>, "relevant": <true|false>, "confidence": <0.0-1.0>}, ...]}

Mark relevant=true ONLY for pages with:
- Appropriations for dedicated science/research agencies (CSIRO, research councils, \
  national labs, medical research funds)
- Explicit research grant programmes with amounts
- R&D fund allocations or industrial R&D incentives
- University research (not general teaching) appropriations

Mark relevant=false for pages that only contain:
- Table of contents, preamble, legislative text, definitions
- Narrative legal articles, policy declarations, or authorisation clauses without a \
  budget table row or explicit appropriation amount
- Foreign aid, development assistance, or overseas programmes (even if labelled "research")
- Education department spending on curriculum, monitoring, evaluation, or scholarships
- Capital works and equipment procurement tables
- Regulatory services, advisory committees, information services
- General welfare, housing, transport, defence (weapons/operations), agriculture
- Scholarship and fellowship schemes
"""


def build_batch_scan_user_prompt(
    pages: list[tuple[str, str]],  # list of (page_id, page_text)
    country: str,
    year: int,
    doc_hint: str = "",
) -> str:
    """Build a batch scan prompt for multiple pages."""
    hint = f"\nDocument type: {doc_hint}" if doc_hint else ""
    country_hint = _COUNTRY_SCAN_HINTS.get(country, "")
    country_block = f"\nCountry-specific signals: {country_hint}" if country_hint else ""
    lines = [f"Country: {country}, Year: {year}{hint}{country_block}\n"]
    for page_id, text in pages:
        snippet = text[:3000].replace("\n", " ")
        lines.append(f"[PAGE {page_id}]\n{snippet}\n")
    lines.append(
        "\nReturn JSON only — exactly 3 fields per entry, no extra fields:\n"
        "{\"pages\": [{\"page_id\": <n>, \"relevant\": <true|false>, \"confidence\": <0.0-1.0>}, ...]}"
    )
    return "\n".join(lines)
