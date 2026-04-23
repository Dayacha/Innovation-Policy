"""
Country-specific extraction profiles for the budget pipeline.

This file is a LIVING DOCUMENT. Add to it as you process each country and
discover patterns that the universal prompt rules miss.

Structure per country:
  skip_if:      Short descriptions of line types to SKIP (false positive patterns
                found in real documents). Each entry is a concise rule, not
                a regex — the LLM interprets them.
  include_note: Clarifications for lines that LOOK like false positives but are
                actually in-scope for this country.
  terminology:  Country/language-specific terms that signal R&D (for non-English
                documents). Supplements the universal known_agencies list.

Design principle — keep it CHEAP:
  The snippet injected into the prompt is short (< 200 tokens per country).
  Only add entries when the universal 9 patterns genuinely fail on real data.
  Do not duplicate rules already in the system prompt.

Source of truth: empirical audits of extracted results vs source documents.
  AU 1975-1978: audited → added patterns 7, 8, 9 and the entries below.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Per-country profiles
# ---------------------------------------------------------------------------

COUNTRY_PROFILES: dict[str, dict] = {

    # -----------------------------------------------------------------------
    # AUSTRALIA
    # Audited: 1975, 1976, 1977, 1978 (311 files total being processed)
    # Key document type: Appropriation Acts ($ thousand units)
    # -----------------------------------------------------------------------
    "Australia": {
        "skip_if": [
            # Education commissions do policy research, not natural science R&D
            "Any 'Research and investigations' or 'Research and development' line "
            "under Commission on Advanced Education or Commission on Technical and "
            "Further Education — these are education policy studies, not R&D.",

            # Named scholarship schemes — Pattern 4 catches generics but these
            # names appear verbatim and still slip through
            "'Queen Elizabeth II Fellowship Scheme' — student fellowship, not research grant.",

            # Commercial subsidy under Bureau of Mineral Resources
            "'Search for oil—Subsidy' and similar mineral exploration subsidies "
            "— commercial drilling incentive, not geological research.",

            # Maintenance/capital at Department of Science
            "'Repairs and Maintenance' lines even when the section is Department of Science "
            "— facility upkeep, not R&D spending. Confirmed false positive in 1977, 1981.",

            # Budget division labels, not R&D appropriations — confirmed in 1977, 1980, 1981, 1985
            "Any line whose description IS EXACTLY or STARTS WITH 'CAPITAL WORKS AND SERVICES', "
            "'Division 927', 'Division 927.', 'Capital Works and Services' "
            "— these are administrative division headers, not R&D appropriations. SKIP.",

            "Any line whose description IS EXACTLY or STARTS WITH 'PAYMENTS TO OR FOR THE STATES', "
            "'Division 928', 'Division 928.', 'Payments to or for the States' "
            "— inter-government transfer mechanism, not R&D. SKIP.",

            # Promotion-dominated combined items — confirmed in 1975, 1984
            "'Road safety promotion and research' — dominated by promotion; SKIP always.",

            # Instruments at operational bureaus (not research labs)
            "'Instruments and apparatus' under Bureau of Meteorology or similar "
            "operational agencies — routine equipment procurement, not research infrastructure.",

            # Hospitals Commission total — mixed-purpose body
            "Section totals for 'Hospitals and Health Services Commission' — "
            "this is a service-delivery body; only extract specific research grant lines within it.",
        ],
        "include_note": [
            # CRITICAL — two-row amount structure in modern Appropriation Acts (2000+)
            "Australian Appropriation Acts show TWO rows of amounts per agency: "
            "row 1 = plain figures (CURRENT YEAR, e.g. 2025-2026) and "
            "row 2 = italic figures (PRIOR YEAR actual, e.g. 2024-2025). "
            "ALWAYS take the FIRST row. The second row is prior year — do NOT use it.",

            # Sectoral R&D looks like agriculture but is genuine science
            "'Barley research', 'Wine research', 'Wool research', 'Wheat research' "
            "— these are legitimate sectoral R&D programmes (CSIRO/industry-funded), include them.",

            # Serum Labs does real research (vaccines, biologics)
            "'Commonwealth Serum Laboratories Commission—Advance for research under the "
            "Science and Industry Research Act' — genuine R&D, include it.",

            # Medical research fund payments are core R&D
            "'Medical research (for payment to the Medical Research Endowment Fund)' "
            "or similar NHMRC payment lines — include these.",

            # Coal/water resources research under minerals or environment departments
            "'Coal research' and 'Water resources research' — genuine applied R&D "
            "even when under a resources/energy department, include them.",
        ],
    },

    # -----------------------------------------------------------------------
    # UNITED KINGDOM
    # Document type: Autumn Budget / Spending Review reports (HM Treasury)
    # Units: GBP billion in prose; GBP thousand in Supply Estimates tables
    # Structure: Narrative prose with embedded DEL tables by department.
    #   - R&D amounts appear as "£X billion" in narrative policy chapters
    #   - DEL tables show Resource DEL / Capital DEL by department
    #   - BEIS (Dept for Business, Energy & Industrial Strategy) / DSIT is the
    #     main science department — look for its DEL table rows
    #   - UKRI row appears within BEIS/DSIT DEL as a sub-line or footnote
    # -----------------------------------------------------------------------
    "UK": {
        "skip_if": [
            # Narrative commitments without a specific budget appropriation line
            "General policy statements like '£X billion for science and innovation' "
            "in narrative text that do NOT correspond to a specific budget line — "
            "only extract if the amount appears in a table row or a defined budget item.",

            # Broad departmental DEL totals from non-science departments
            "Section totals for Ministry of Defence, Department of Health, Home Office, "
            "Department for Education (general teaching), HMRC, DWP, or other "
            "non-science departments — SKIP their totals even if they mention 'research'.",

            # Financial transactions and adjustments
            "'Financial Transactions' lines and 'RDEL to CDEL switch' adjustments "
            "— these are accounting reclassifications, not new R&D appropriations.",

            # OBR and HM Treasury administrative entries
            "Office for Budget Responsibility (OBR) and HM Treasury administrative "
            "entries — these are not R&D spending.",

            # Loan and equity injection instruments
            "Lines labelled 'Equity injection', 'Loan', or 'Financial instrument' "
            "— these are investment vehicles, not direct R&D appropriations.",
        ],
        "include_note": [
            # BEIS/DSIT is the primary science department
            "BEIS (Department for Business, Energy and Industrial Strategy) and DSIT "
            "(Department for Science, Innovation and Technology) are the main science "
            "departments — extract their Resource DEL and Capital DEL totals as "
            "program_total with decision=review; extract named sub-lines (UKRI, "
            "research councils) as section_total with decision=include.",

            # Each Research Council is a dedicated science agency
            "UKRI (UK Research and Innovation) and each constituent research council "
            "(MRC, EPSRC, NERC, BBSRC, ESRC, AHRC, STFC, Innovate UK, Research England) "
            "are dedicated science agencies — their full operating appropriations are include.",

            # Science Budget and Industrial Strategy Challenge Fund
            "The 'science budget', 'Industrial Strategy Challenge Fund', and "
            "'Strength in Places Fund' are R&D-specific allocations — include them.",

            # Unit handling — CRITICAL
            "UK Budget documents use TWO different unit conventions: "
            "(a) Narrative prose and Spending Review / DEL tables: amounts in £ BILLION "
            "    → set unit='billion', e.g. '£1.6 billion' → amount_local=1.6, unit='billion'. "
            "    DO NOT convert billions to thousands yourself — return the raw number with unit='billion'. "
            "(b) Detailed Supply Estimates tables: amounts in £ THOUSAND "
            "    → set unit='thousand'. "
            "(c) NPIF / policy decision tables: amounts in £ MILLION "
            "    → set unit='million', e.g. '£500 million' → amount_local=500, unit='million'. "
            "When the text says '£1.6 billion', return amount_local=1.6 and unit='billion'. "
            "When the text says '£500 million', return amount_local=500 and unit='million'. "
            "NEVER multiply billions by 1000 and call the result thousands.",
        ],
        "year_notes": {
            # 2003–2009: Budget documents contain a dedicated 'Science and Innovation'
            # chapter (typically Chapter 3) with a science spending summary table.
            # That table shows amounts in £ MILLION (not thousand, not billion).
            # The key rows to extract are:
            #   - "DTI Science Budget" / "Ring-fenced science budget" → unit='million'
            #   - "DfES / DIUS funding for research in universities" → unit='million'
            #   - "Total UK science spending" / "Total public investment in science" → unit='million'
            # UNIT CRITICAL: these table values like "3,383" or "5,397" are in £ MILLION.
            #   Return amount_local=3383, unit='million'  (NOT unit='thousand').
            # Also extract any named programme with a specific £amount in this chapter.
            # Do NOT extract percentages ("rising 2.5% in real terms") — only £ amounts.
            2003: (
                "Science chapter: look for science spending table in Chapter 3. "
                "Extract ring-fenced science budget total if a £ figure is stated. "
                "Table amounts are in £ MILLION → unit='million'."
            ),
            2004: (
                "Science chapter: look for science spending table in Chapter 3. "
                "Extract ring-fenced science budget total if a £ figure is stated. "
                "Table amounts are in £ MILLION → unit='million'. "
                "NOTE: 'Industrial Strategy Challenge Fund' did NOT exist in 2004 — "
                "do NOT extract it. Any 'Challenge Fund' reference is a different programme."
            ),
            2005: (
                "Science chapter: extract ring-fenced science budget total from Chapter 3 table. "
                "Table amounts are in £ MILLION → unit='million'."
            ),
            2006: (
                "Science chapter: extract ring-fenced science budget total from Chapter 3 table. "
                "Table amounts are in £ MILLION → unit='million'."
            ),
            2007: (
                "Budget 2007 contains a specific science spending table (look for "
                "'DTI Science Budget Departmental Expenditure Limits' and "
                "'Total UK science spending'). "
                "UNIT CRITICAL: values in this table like '3,383' and '5,397' are in "
                "£ MILLION → return amount_local=3383, unit='million' and "
                "amount_local=5397, unit='million'. "
                "Also extract: DfES research funding, Energy Technologies Institute £100M."
            ),
            2008: (
                "Science chapter: extract ring-fenced science budget total from Chapter 3 table. "
                "Table amounts are in £ MILLION → unit='million'. "
                "Also extract any named R&D programme announcements with specific £ amounts."
            ),
            2009: (
                "Science chapter: extract ring-fenced science budget total from Chapter 3 table. "
                "Table amounts are in £ MILLION → unit='million'. "
                "Also extract Technology Strategy Board funding and any named R&D programmes."
            ),
            2013: (
                "NOTE: 'Industrial Strategy Challenge Fund' did NOT exist in 2013 — "
                "do NOT extract it. It was created in 2017. "
                "Do NOT extract 'Science budget allocation' as a line item — it is too generic. "
                "Do extract: named specific programmes with £ amounts (SBRI, ATI, specific research funds)."
            ),
        },
    },

    # -----------------------------------------------------------------------
    # CANADA
    # Document type: Appropriation Acts (Supply Bills) — bilingual EN/FR.
    # Structure: SCHEDULE 1 lists all departments with Vote numbers and
    #   dollar amounts. Each Vote is either 'Program expenditures' (operating)
    #   or 'Grants and contributions' (transfer payments to agencies/research).
    # Unit: FULL Canadian dollars (NOT thousands). Return unit='dollar'.
    #   e.g. "$1,217,648,000" → amount_local=1217648000, unit='dollar'.
    # Multiple files per fiscal year (Main + Supplementary Estimates A/B/C).
    # Fiscal year: "2024-25" means April 2024–March 2025, use year=2024.
    # -----------------------------------------------------------------------
    "Canada": {
        "skip_if": [
            # Non-science departments — skip their totals
            "Department of National Defence, RCMP, Correctional Service, "
            "Canada Revenue Agency, Immigration/IRCC, Transport Canada infrastructure — "
            "skip their operating totals even if they mention 'research' or 'science'.",

            # Internal overhead / admin
            "Votes labelled 'Statutory', 'Minister's salary', 'Contributions to employee "
            "benefit plans' — these are administrative overhead, not R&D appropriations.",
        ],
        "include_note": [
            # The three federal granting councils — always include
            "NSERC (Natural Sciences and Engineering Research Council / Conseil de recherches "
            "en sciences naturelles et en génie): ALL votes are in-scope R&D. "
            "SSHRC (Social Sciences and Humanities Research Council): include grants votes. "
            "CIHR (Canadian Institutes of Health Research): include all votes.",

            # National Research Council
            "NRC (National Research Council of Canada / Conseil national de recherches): "
            "operating and grants votes are in-scope.",

            # Other core science agencies
            "Also include: Canada Foundation for Innovation (CFI), Canadian Space Agency (ASC), "
            "TRIUMF (particle physics lab), Atomic Energy of Canada (AECL) research votes, "
            "Genome Canada, Canada Research Chairs programme.",

            # Unit — critical
            "Amounts are in FULL Canadian dollars. Return unit='dollar', currency='CAD'. "
            "Example: '76 040 517' → amount_local=76040517, unit='dollar'. "
            "Do NOT convert to thousands yourself.",

            # Bilingual — use English description
            "Documents are bilingual. Use the English line description in line_description.",

            # Schedule structure — extract only the increment for THIS Act
            "Each Appropriation Act contains a SCHEDULE listing only the appropriations "
            "being voted in THAT specific Act (the increment), not cumulative totals from "
            "previous acts. Extract only the amounts in the schedule of this Act.",
        ],
        # Year-specific extraction notes — injected into the prompt automatically
        # when processing files for that fiscal year.
        "year_notes": {
            2020: (
                "CAUTION — 2020-21 COVID emergency appropriations:\n"
                "  The 2020-21 fiscal year included large COVID-19 emergency omnibus bills. "
                "Many Acts contain COVID-19 emergency allocations to Public Health Agency, "
                "hospitals, vaccine procurement, and income support. "
                "DO NOT extract these emergency COVID items — they are health emergency "
                "response spending, NOT R&D appropriations. Only extract the regular "
                "science agency votes for NSERC, CIHR, SSHRC, NRC, CSA, CFI, etc.\n"
                "  TABLE ACCURACY: Some 2020-21 tables contain the same large dollar "
                "figure in adjacent rows under different agency headings due to a "
                "formatting artifact in COVID omnibus schedules. Before assigning an "
                "amount to an agency, confirm that the section heading immediately above "
                "it names that specific agency. If the same amount appears under two "
                "different agency headings within one table, extract it only for the "
                "agency whose heading directly precedes it — do NOT assign it to both."
            ),
            2022: (
                "CAUTION — 2022-23 table structure:\n"
                "  Some 2022-23 Appropriation Acts (especially C16) present each agency's "
                "appropriation at two levels in the same section: a sub-programme line "
                "(line_item) and a programme-level summary (program_total) with the same "
                "or very similar amount. This creates double-counting if both are extracted. "
                "Rule: when a section contains both a specific line description AND a "
                "broader programme summary for the same vote amount, extract it ONCE as "
                "item_type='line_item' using the most specific description, and omit the "
                "program_total duplicate.\n"
                "  CIHR in 2022-23 received elevated appropriations for ongoing COVID "
                "research programs — these are legitimate R&D and should be included, "
                "but verify each amount is a distinct appropriation vote, not a re-listing "
                "of the same vote."
            ),
        },
    },

    # -----------------------------------------------------------------------
    # NEW ZEALAND
    # Not yet audited. Entries to be added after first run.
    # Key document type: Appropriation (Estimates) Bills (NZD thousand)
    # -----------------------------------------------------------------------
    "New Zealand": {
        "skip_if": [
            # Placeholder — add after running NZ documents
        ],
        "include_note": [
            "Vote Science and Innovation is the primary R&D vote — all lines within it "
            "are candidates for extraction.",
            "Crown Research Institute (CRI) operating funding is in-scope as science_agency.",
            "DSIR (Department of Scientific and Industrial Research) in early years "
            "is a dedicated research agency — include its operating appropriations.",
        ],
    },

    # -----------------------------------------------------------------------
    # DENMARK
    # Partially audited via the rule-based pipeline (1975-1984 Finanslov).
    # Key document type: Finanslov — Danish, § structure, DKK thousand
    # -----------------------------------------------------------------------
    "Denmark": {
        "skip_if": [
            # Generic 'bevilling' (appropriation) lines without research content
            "'Driftsudgifter' (operating expenditure) lines in mixed-purpose ministries "
            "— only include if the line description also contains a research term.",

            "Lines under Socialministeriet (Social Affairs) or Indenrigsministeriet "
            "(Interior) that use the word 'forskning' in a policy-evaluation context.",
        ],
        "include_note": [
            "§ 20 Undervisningsministeriet contains university funding — include lines "
            "for 'universiteter', 'forskning', and named research councils.",

            "Statens teknisk-videnskabelige Forskningsfond, Statens naturvidenskabelige "
            "Forskningsrad, and equivalent councils are dedicated R&D — include their "
            "full operating appropriations.",

            "Atomenergikommissionen (Atomic Energy Commission) is in-scope.",
        ],
    },

    # -----------------------------------------------------------------------
    # FRANCE
    # Document type: Loi de Finances (Finance Law) from JORF, plus
    #   Annexes budgétaires (PAP — Projets Annuels de Performance) when available.
    # Units: EUR million (Millions d'euros) for most programme lines
    # Structure: The Loi de Finances itself is a LEGISLATIVE TEXT — it contains
    #   mission/programme totals but NOT individual agency breakdowns.
    #   CNRS, ANR, CEA etc. appear only in the PAP annexes (not always present).
    #   The main R&D mission is "Recherche et enseignement supérieur" (code 172/150/187…)
    #   Structure: Mission → Programme → Action → Agence opérateur
    # -----------------------------------------------------------------------
    "France": {
        "skip_if": [
            # Legislative preamble and tax code amendments
            "Articles modifying tax code thresholds (e.g. 'le montant « 5 888 € » est "
            "remplacé par « 5 947 € »') — these are tax brackets, not R&D appropriations.",

            # Fiscal balance metrics are not appropriations
            "Lines for 'Solde structurel', 'Solde conjoncturel', 'Solde effectif', "
            "'Effort structurel' — these are macroeconomic balance metrics, not R&D spending.",

            # Inter-governmental transfers
            "'Prélèvements sur recettes' (revenue transfers to local governments or EU) "
            "— these are fiscal transfers, not R&D appropriations.",

            # Real estate categories mentioning 'recherche'
            "Property tax articles mentioning 'recherche' in the context of office "
            "categories (e.g. 'locaux spécialement aménagés pour l'exercice d'activités "
            "de recherche') — these are tax code definitions, not R&D appropriations.",

            # Defence research (handled separately, mixed civil/military)
            "Programme 144 (Environnement et prospective de la politique de défense) "
            "— this is defence intelligence/strategic analysis, not civil R&D.",

            # FTE staffing ceilings (plafonds d'emplois) — NOT budget amounts
            "Any page or table headed 'PLAFOND exprimé en équivalents temps plein travaillé' "
            "or 'Plafond d'emplois' — these are staff headcount ceilings (FTE counts like "
            "'259 352' meaning 259,352 full-time employees), NOT euro budget amounts. "
            "SKIP these pages entirely. Do NOT extract staffing numbers as amounts.",

            # ETPT staff tables
            "Tables with column headers 'ETPT' or 'ETP' (équivalents temps plein travaillé) "
            "— these are workforce headcount tables, not budget appropriations.",
        ],
        "include_note": [
            # The main R&D mission
            "Mission 'Recherche et enseignement supérieur' contains multiple R&D programmes. "
            "Extract programme-level totals (crédits de paiement / CP) as program_total. "
            "Key programmes: 150 (Formations supérieures), 172 (Recherches scientifiques), "
            "187 (Recherche agricole), 190 (Énergie, développement et mobilité durables), "
            "191 (Recherche dans les domaines du risque), 192 (Recherche et enseignement "
            "supérieur en matière économique), 193 (Recherche spatiale).",

            # Individual agency amounts if present (PAP annexes)
            "If individual agency amounts appear (CNRS, ANR, CEA, INSERM, INRIA, INRAE, "
            "CNES, IFREMER): extract them as section_total with decision=include. "
            "These appear only in the PAP (Projet Annuel de Performance) annexes, "
            "not in the main Loi de Finances text.",

            # Number format: French uses spaces and commas
            "French number format: 1 234 567 or 1 234 567,00 (space = thousands separator, "
            "comma = decimal). Extract the numeric value correctly: '14 053' = 14053.",

            # Currency and unit — CRITICAL
            "UNIT RULE (MANDATORY): The JORF main mission/programme tables show amounts "
            "in MILLIONS d'euros. ALWAYS set unit='million', currency='EUR'. "
            "Example: '2 417' in the Crédits de paiement column = 2,417 million EUR = €2.4B. "
            "Return amount_local=2417, unit='million'. "
            "ONLY use unit='thousand' if the table is explicitly headed 'milliers d'euros' "
            "or 'en milliers €'. If the column header just says 'Crédits de paiement' or "
            "'CP' with no explicit unit label, it is MILLIONS. "
            "NEVER default to unit='thousand' for JORF documents.",

            # Crédits de paiement vs Autorisations d'engagement
            "The budget has two columns: Autorisations d'engagement (AE, commitment) and "
            "Crédits de paiement (CP, payment). Prefer 'Crédits de paiement' (CP) when "
            "both are present — it represents actual cash flow for the year.",

            # FTE guard — even if Crédits de paiement appears as column header on an FTE page
            "WARNING: Some pages contain BOTH a staffing table (ETPT/ETP headcounts) AND "
            "budget labels. If the numbers on the page are in the range 1,000–300,000 and "
            "appear under a column labelled 'ETPT' or 'Plafond', they are headcounts NOT euros. "
            "A real programme budget (CP) is at least 100 million EUR (≥100 in the millions column). "
            "Do not extract headcounts as budget amounts.",
        ],
    },

    # -----------------------------------------------------------------------
    # GERMANY
    # Document types vary by year — all produce Gesamtplan-level data:
    #
    #   Drucksachen (parliamentary budget committee reports, ~16-30 pages):
    #     These are the Beschlussempfehlung des Haushaltsausschusses.
    #     They contain the Gesamtplan (Haushaltsübersicht) which shows:
    #     - Part I: Revenue and expenditure totals BY EINZELPLAN (one row per ministry)
    #     - Unit: 1 000 DM (pre-2002) or 1 000 EUR (2002+)
    #     There is NO breakdown within each Einzelplan (no Titel 685 data here).
    #     Extract: Epl 30 total as a section_total for BMBF/BMFT/BMBW.
    #
    #   Bundesgesetzblatt (bgbl) files: The enacted Haushaltsgesetz — legislative text
    #     only. Epl totals appear as article references. No tables. Extract totals if
    #     the text explicitly states 'Einzelplan 30 ... X Millionen/Milliarden EUR'.
    #
    #   Gesamtplan_und_Uebersichten.pdf (2025, possibly other years):
    #     The full Gesamtplan with a Funktionenübersicht (Part II) that classifies
    #     spending by function. This IS the best source — extract from it:
    #     - Funktion 137 = Deutsche Forschungsgemeinschaft (DFG) — extract as science_agency
    #     - Funktion 164 = Gemeinsame Forschungsförderung (joint R&D funding of Bund+Länder,
    #       includes Helmholtz, MPG, Fraunhofer, Leibniz combined) — extract as section_total
    #     - Funktion 165 = Forschung und experimentelle Entwicklung (R&D programmes) — section_total
    #     - Funktion 16 total = Wissenschaft, Forschung, Entwicklung außerhalb Hochschulen
    #
    # Units: 1 000 DM for years before 2002, 1 000 EUR from 2002 onwards.
    #   Set currency='DEM' for pre-2002 files, 'EUR' for 2002+ files.
    # Number format: SPACE as thousands separator: 14 053 404 = 14,053,404
    # -----------------------------------------------------------------------
    "Germany": {
        "skip_if": [
            # Non-R&D Einzelpläne in the Haushaltsübersicht — skip their rows
            "Rows for Epl 01 (Bundespräsident), Epl 02 (Bundestag), Epl 06 (Inneres), "
            "Epl 14 (Verteidigung / Defence) — even if labelled 'Forschung'. SKIP.",

            # Kreditermächtigung (borrowing authorisation) — not spending
            "Kreditermächtigung and Kreditfinanzierungsplan entries — borrowing limits, "
            "not R&D appropriations. SKIP.",

            # Non-R&D Funktionen in the Funktionenübersicht
            "Function codes for Allgemeine Dienste (0x), Auswärtige Angelegenheiten (02), "
            "Soziale Sicherung (04), Gesundheitswesen (05) — skip these even if they have "
            "some 'Forschung' sub-entries, unless the specific row is DFG (Funktion 137) "
            "or another explicitly named R&D organisation.",

            # Verpflichtungsermächtigungen (commitment authorisations) — future commitments
            "Verpflichtungsermächtigungen tables — these show future-year commitments, "
            "not the current-year budget. Do NOT extract from these tables.",
        ],
        "include_note": [
            # CRITICAL: document format for most files — Gesamtplan only
            "CRITICAL: Most Germany files are the Gesamtplan (budget summary), not the "
            "detailed Einzelplan 30. There is NO Titel 685 agency-grant breakdown in these "
            "files. Instead, extract: "
            "(a) The Epl 30 total from the Haushaltsübersicht Teil I (Ausgaben table) — "
            "this is the BMBF/BMFT/BMBWFT total. Label it as item_type='section_total', "
            "decision='review'. "
            "(b) If the file has a Funktionenübersicht (Teil II): extract Funktion 137 "
            "(Deutsche Forschungsgemeinschaft), Funktion 164 (Gemeinsame Forschungsförderung "
            "von Bund und Ländern without DFG), and Funktion 165 (Forschung und "
            "experimentelle Entwicklung) as separate line_items with decision='include'.",

            # CRITICAL: number format
            "CRITICAL: German budget tables use SPACES as thousands separators. "
            "'14 053 404' = 14,053,404 (14 million). '356 400' = 356,400. "
            "Parse space-separated numbers as single integers.",

            # Unit and currency
            "Amounts are in thousands (1 000 DM or 1 000 EUR). "
            "Pre-2002: currency='DEM', unit='thousand'. "
            "2002 onwards: currency='EUR', unit='thousand'. "
            "Example: '17 900 000' in the table = 17,900,000 thousand EUR = €17.9 billion.",

            # Epl 30 name changes over time
            "The R&D ministry (Epl 30) has been renamed several times: "
            "BMFT (Bundesministerium für Forschung und Technologie, 1969-1994), "
            "BMBF (Bundesministerium für Bildung und Forschung, 1994-2021), "
            "BMBF kept same name (2021-2025), "
            "BMFTR (Bundesministerium für Forschung, Technologie und Raumfahrt, 2025+). "
            "All these are Epl 30 — always extract as 'BMBF (Federal Research Ministry)'.",

            # Funktionenübersicht — available in 2025 Gesamtplan file
            "If the file includes a 'Funktionenübersicht' section: "
            "Funktion 137 = Deutsche Forschungsgemeinschaft (DFG) — extract as science_agency. "
            "Funktion 164 = Gemeinsame Forschungsförderung (joint science funding, "
            "Helmholtz+MPG+Fraunhofer+Leibniz combined) — extract as section_total. "
            "Funktion 165 = applied R&D programmes — extract as section_total.",
        ],
    },

    # -----------------------------------------------------------------------
    # JAPAN
    # Document type: 一般会計予算 (General Account Budget) — highly detailed.
    # Units: 百万円 (Hyakuman-en = millions of yen). Column header: 百万円
    # Number format: Standard digits, comma as thousands separator sometimes omitted.
    # Structure: Hierarchical — 所管 (jurisdiction/ministry) → 組織 (organisation)
    #   → 項 (item) → 目 (sub-item). Key ministry for R&D:
    #     文部科学省 (MEXT — Ministry of Education, Culture, Sports, Science and Technology)
    #     経済産業省 (METI — Ministry of Economy, Trade and Industry)
    #     内閣府 (Cabinet Office — strategic R&D programmes)
    # OCR quality: Many pages use OCR fallback — expect garbled characters,
    #   especially for kanji. Numeric values are usually preserved even with OCR artifacts.
    # -----------------------------------------------------------------------
    "Japan": {
        "skip_if": [
            # Defence research (防衛省)
            "防衛省 (Ministry of Defense) lines — even those labelled 技術研究 "
            "(technical research) — these are military procurement R&D. SKIP.",

            # Administrative and personnel overhead
            "'人件費' (personnel costs) and '一般管理費' (general administration) lines "
            "— these are overhead at research organisations, not R&D programme funding.",

            # OCR garbage lines — if a 'line' is mostly unreadable characters
            "Lines consisting primarily of garbled OCR artifacts (random symbols, "
            "misread kanji combinations) with no recognisable agency name or amount "
            "— skip rather than hallucinate a value.",

            # Inter-governmental transfers to local authorities
            "'地方交付税' (local allocation tax) and similar block transfers to "
            "prefectural governments — these are fiscal transfers, not R&D.",

            # Debt service
            "'国債費' (national debt service) lines — debt repayment, not R&D.",
        ],
        "include_note": [
            # MEXT is the primary R&D ministry
            "文部科学省 (MEXT) is the primary science and education ministry. "
            "Key R&D budget lines within MEXT: "
            "科学技術振興費 (science and technology promotion), "
            "研究振興費 (research promotion), "
            "国立大学法人運営費交付金 (national university operating grants — "
            "include as higher_education). "
            "Extract each named line as a separate item.",

            # Key research agencies by Japanese name
            "Key dedicated R&D agencies to extract as science_agency (include): "
            "科学技術振興機構 (JST), 日本学術振興会 (JSPS), 理化学研究所 (RIKEN), "
            "物質・材料研究機構 (NIMS), 海洋研究開発機構 (JAMSTEC), "
            "宇宙航空研究開発機構 (JAXA), 日本原子力研究開発機構 (JAEA), "
            "新エネルギー・産業技術総合開発機構 (NEDO).",

            # METI industrial R&D
            "経済産業省 (METI) contains industrial R&D programmes under "
            "産業技術環境局 (Industrial Science and Technology Policy Bureau). "
            "Extract NEDO and industrial R&D programme lines as innovation_instruments.",

            # Number format and units — CRITICAL
            "CRITICAL: Japan's 予算書 shows amounts in 千円 (thousands of yen). "
            "Return amount_local EXACTLY as it appears in the source document "
            "(e.g. if the document shows '115,923,000', return 115923000). "
            "Set unit='thousand', currency='JPY'. "
            "Do NOT multiply or convert — return the raw printed number. "
            "Example: JAXA line shows '115,923,000' → amount_local=115923000, unit='thousand'.",

            # OCR artifact handling
            "Some pages have OCR artifacts. If the agency name is partially garbled "
            "but recognisable (e.g. '科学技ﾞ振興機構' instead of '科学技術振興機構'), "
            "still extract the item and note the OCR quality in the notes field.",

            # Skip ministry-level aggregates — redundant
            "Do NOT extract rows labelled 所管合計 (ministry jurisdiction total), "
            "本省計 (ministry subtotal), or bare 合計 (total) — "
            "these are section aggregates that include non-R&D spending.",

            # Cabinet Office strategic funds
            "内閣府 (Cabinet Office) manages SIP (戦略的イノベーション創造プログラム) "
            "and ImPACT programmes — extract as innovation_instruments if amounts appear.",
        ],
    },

    # -----------------------------------------------------------------------
    # NORWAY
    # Not yet audited.
    # Key document type: Statsbudsjettet (NOK thousand)
    # -----------------------------------------------------------------------
    "Norway": {
        "skip_if": [
            # Placeholder
        ],
        "include_note": [
            "Norges forskningsråd (Research Council of Norway) appropriations are core R&D.",
            "Oil-related research (Oljeforskningsprogrammet) is legitimate applied R&D "
            "— include if the description says 'forskning' (research), but SKIP plain "
            "oil exploration subsidies.",
        ],
    },

    # -----------------------------------------------------------------------
    # SWEDEN
    # Not yet audited.
    # Key document type: Statsbudget (SEK thousand)
    # -----------------------------------------------------------------------
    "Sweden": {
        "skip_if": [
            # Placeholder
        ],
        "include_note": [
            "Vetenskapsrådet and VINNOVA appropriations are core R&D.",
            "Riksbankens Jubileumsfond (humanities/social science) is in-scope.",
        ],
    },
}


# ---------------------------------------------------------------------------
# Public helper — called from prompts.build_extract_user_prompt
# ---------------------------------------------------------------------------

def build_country_addendum(country: str, year: int | None = None) -> str:
    """
    Return a short, token-efficient prompt snippet with country-specific
    extraction guidance. Returns empty string if no profile exists.

    If `year` is provided, any year-specific notes from the profile's
    ``year_notes`` dict are appended after the general rules.

    The snippet is intentionally brief to keep LLM costs low.
    Only entries with actual content (non-empty lists) are included.
    """
    profile = COUNTRY_PROFILES.get(country)
    if not profile:
        return ""

    lines: list[str] = []

    skip = [s for s in profile.get("skip_if", []) if s.strip()]
    include = [s for s in profile.get("include_note", []) if s.strip()]

    if skip:
        lines.append(f"Country-specific SKIP rules for {country}:")
        for rule in skip:
            # Truncate very long entries to keep token count bounded
            lines.append(f"  - {rule[:200]}")

    if include:
        lines.append(f"Country-specific INCLUDE clarifications for {country}:")
        for note in include:
            lines.append(f"  - {note[:200]}")

    # Year-specific notes — appended verbatim (no truncation, intentionally detailed)
    if year is not None:
        year_notes: dict = profile.get("year_notes", {})
        year_note = year_notes.get(year, "")
        if year_note and year_note.strip():
            lines.append(f"\nYear-specific extraction rules for {country} {year}:")
            lines.append(year_note.strip())

    return "\n".join(lines) if lines else ""
