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
    # Document type: varies by era — see year_notes for per-year instructions.
    #
    # ERA GUIDE (critical — document structure changes completely by period):
    #   1975-1992: Financial Statement and Budget Report (FSBR). Macro overview
    #              only — no agency-level R&D data. Do NOT try to extract.
    #   1993-2002: Budget Report / Pre-Budget Report. Narrative prose with some
    #              £ billion policy commitments. Limited structured data.
    #   2003-2009: Budget Report with dedicated Science chapter (Ch.3 / Ch.4).
    #              Science spending table in £ MILLION — key extraction target.
    #   2010-2016: Spending Review / Budget with BIS DEL tables. £ billion.
    #   2017:      Industrial Strategy Challenge Fund created. BIS → BEIS.
    #   2018-2020: UKRI created April 2018. BEIS leads science. £ billion DEL.
    #   2021+:     SR21 Table 2.2 and successors — structured R&D DEL by agency.
    #              ARIA created 2021. DSIT created Feb 2023 (BEIS split).
    #
    # Units:
    #   - Science chapter tables (2003-2009): £ MILLION
    #   - DEL/Spending Review prose and tables (2010+): £ BILLION
    #   - Supply Estimates detailed tables: £ THOUSAND (do not use these)
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

            # Multi-year Spending Review commitments in prose
            "Spending announcements expressed as 'over X years' or 'by 2025' in "
            "narrative text — these are multi-year commitments, not annual appropriations. "
            "SKIP unless the annual breakdown is given explicitly in a table.",

            # R&D tax credit fiscal costs — these are revenue foregone, not spending
            "R&D tax credit entries with negative amounts or labelled 'fiscal cost', "
            "'revenue cost', 'cost to Exchequer', or 'tax relief' — these are tax "
            "expenditures, not budget appropriations. SKIP them.",

            # Non-R&D green / climate / infrastructure items commonly hallucinated
            # as R&D because they appear near science budget text in Budget documents.
            "Items primarily about: electric vehicle grants (Plug-in Car Grant, charging "
            "network rollout), Climate Change Levy, nature recovery funds (Nature for "
            "Climate Fund, Darwin Plus, Natural Environment Impact Fund), flood management, "
            "fly-tipping enforcement, brownfield land remediation, house-building — "
            "these are environmental/infrastructure programmes with no R&D component. SKIP. "
            "Also skip: 'Green Heat Networks', 'Carbon Capture and Storage Infrastructure "
            "Fund' (unless explicitly described as a research/pilot programme), "
            "'Plug-in Car Grant', 'fast-charging network rollout'. "
            "Rule: if the item is operational infrastructure or a consumer subsidy rather "
            "than a defined research or innovation programme, SKIP it.",

            # Anachronism guard — hard block for agencies that did not yet exist
            # These bodies are commonly hallucinated by the LLM in pre-creation years.
            "ANACHRONISM RULE — DO NOT extract ANY item mentioning: "
            "'Industrial Strategy Challenge Fund' or 'ISCF' if document year < 2017 "
            "(ISCF created November 2017); "
            "'UK Research and Innovation' or 'UKRI' if document year < 2018 "
            "(UKRI created April 2018); "
            "'Advanced Research and Invention Agency' or 'ARIA' if document year < 2021; "
            "'Department for Science, Innovation and Technology' or 'DSIT' if document year < 2023; "
            "'Department for Business, Energy and Industrial Strategy' or 'BEIS' if document year < 2016. "
            "If you see these names in a pre-creation document, the LLM has hallucinated them. SKIP.",
        ],
        "include_note": [
            # BEIS/DSIT is the primary science department
            "BEIS (Department for Business, Energy and Industrial Strategy) and DSIT "
            "(Department for Science, Innovation and Technology) are the main science "
            "departments — extract their Resource DEL and Capital DEL totals as "
            "program_total with decision=review; extract named sub-lines (UKRI, "
            "research councils) as section_total with decision=include.",

            # Each Research Council is a dedicated science agency
            "UKRI (UK Research and Innovation, created April 2018) and each constituent "
            "research council (MRC, EPSRC, NERC, BBSRC, ESRC, AHRC, STFC, Innovate UK, "
            "Research England) are dedicated science agencies — their full operating "
            "appropriations are include. NOTE: UKRI did NOT exist before 2018.",

            # Science Budget and Industrial Strategy Challenge Fund
            "The 'science budget', 'ring-fenced science budget', and "
            "'Industrial Strategy Challenge Fund (ISCF, created 2017)' are R&D-specific "
            "allocations — include them. NOTE: ISCF did NOT exist before 2017.",

            # Unit handling — CRITICAL
            "UK Budget documents use different unit conventions by era: "
            "(a) Science chapter tables (2003-2009): amounts in £ MILLION "
            "    → set unit='million', e.g. '5,397' → amount_local=5397, unit='million'. "
            "    DO NOT interpret these as thousands. "
            "(b) DEL Spending Review tables (2010+): amounts in £ BILLION "
            "    → set unit='billion', e.g. '£14.6 billion' → amount_local=14.6, unit='billion'. "
            "    DO NOT convert billions to thousands. "
            "(c) Policy announcements in prose: use the unit stated (million or billion). "
            "When the text says '£1.6 billion', return amount_local=1.6 and unit='billion'. "
            "When the text says '£500 million', return amount_local=500 and unit='million'. "
            "NEVER multiply billions by 1000 and call the result thousands.",
        ],
        "year_notes": {
            # ---------------------------------------------------------------
            # 1975-1992: Financial Statement and Budget Report (FSBR)
            # These documents are macroeconomic overviews — public spending
            # totals by function, PSBR projections, monetary policy. They do
            # NOT contain agency-level R&D appropriations. Return empty.
            # ---------------------------------------------------------------
            **{y: (
                "SKIP. This is a Financial Statement and Budget Report (FSBR) "
                "macro overview document. It contains only aggregate public "
                "expenditure projections, PSBR figures, and fiscal tables — "
                "NO agency-level R&D appropriations. Return {\"items\": []}."
            ) for y in range(1975, 1993)},

            # ---------------------------------------------------------------
            # 1993-2002: Budget Report / Pre-Budget Report
            # Narrative documents with some £ billion policy mentions.
            # Rarely contain structured R&D tables. Extract only if an explicit
            # £ amount is tied to a named R&D programme in the text.
            # ANACHRONISM GUARD: UKRI, ISCF, ARIA, DSIT, BEIS did NOT exist.
            # Known bodies: OST, DTI, Research Councils (individually named),
            # HEFCE, Technology Foresight Programme, Foresight programme.
            # ---------------------------------------------------------------
            **{y: (
                "Budget Report / Pre-Budget Report — narrative document. "
                "Extract ONLY if an explicit £ figure is directly tied to a "
                "named R&D programme or research council in the text. "
                "DO NOT extract: vague 'science investment' statements, "
                "percentages, or multi-year totals. "
                "ANACHRONISM GUARD: UKRI (created 2018), ISCF (created 2017), "
                "ARIA (created 2021), BEIS (created 2016), DSIT (created 2023) "
                "did NOT exist in this year — do NOT hallucinate these."
            ) for y in range(1993, 2003)},

            # ---------------------------------------------------------------
            # 2003-2009: Budget with Science Chapter
            # Contains a dedicated 'Science and Innovation Investment Framework'
            # chapter (typically Chapter 3 or 4). A science spending summary
            # table shows amounts in £ MILLION. Key rows:
            #   - "DTI Science Budget" / "Ring-fenced science budget"
            #   - "DfES / DIUS funding for research in universities"
            #   - "Total UK science spending" / "Total public investment in science"
            # UNIT CRITICAL: table values like '3,383' or '5,397' are £ MILLION.
            #   Return amount_local=3383, unit='million' (NOT thousand).
            # ANACHRONISM GUARD: UKRI (2018), ISCF (2017), BEIS (2016) did NOT exist.
            # ---------------------------------------------------------------
            2003: (
                "Science and Innovation chapter: look for science spending table. "
                "Extract: ring-fenced DTI Science Budget total, DfES university research "
                "funding, and 'Total UK science spending' if £ figures stated. "
                "Table amounts are in £ MILLION → unit='million'. "
                "ANACHRONISM GUARD: UKRI, ISCF, BEIS, DSIT did NOT exist in 2003 — "
                "do NOT extract them."
            ),
            2004: (
                "Science and Innovation chapter: look for science spending table. "
                "Extract ring-fenced science budget total and DfES university research "
                "funding if £ figures are stated. "
                "Table amounts are in £ MILLION → unit='million'. "
                "ANACHRONISM GUARD: UKRI, ISCF (created 2017), BEIS, DSIT did NOT "
                "exist in 2004 — do NOT extract them."
            ),
            2005: (
                "Science and Innovation Investment Framework chapter: look for science "
                "spending table. Extract ring-fenced science budget total and university "
                "research funding. Table amounts in £ MILLION → unit='million'. "
                "ANACHRONISM GUARD: UKRI, ISCF, BEIS, DSIT did NOT exist in 2005."
            ),
            2006: (
                "Science and Innovation Investment Framework chapter: look for science "
                "spending table. Extract ring-fenced science budget total. "
                "Table amounts in £ MILLION → unit='million'. "
                "ANACHRONISM GUARD: UKRI, ISCF, BEIS, DSIT did NOT exist in 2006."
            ),
            2007: (
                "Budget 2007: science spending table in science chapter. Look for "
                "'DTI Science Budget Departmental Expenditure Limits' and "
                "'Total UK science spending'. "
                "UNIT CRITICAL: values like '3,383' and '5,397' are in £ MILLION "
                "→ amount_local=3383, unit='million' and amount_local=5397, unit='million'. "
                "Also extract: DfES research funding, Energy Technologies Institute. "
                "ANACHRONISM GUARD: UKRI, ISCF, BEIS, DSIT did NOT exist in 2007."
            ),
            2008: (
                "Science chapter: extract ring-fenced science budget total from summary "
                "table. "
                "UNIT RULE — MANDATORY: The science spending table header says '£ million'. "
                "A value like '6,435' in that table means £6,435 MILLION = £6.4 BILLION. "
                "Set amount_local=6435, unit='million'. DO NOT set unit='thousand'. "
                "If the table shows 'Total public investment in science and research' with "
                "a number around 6,000–7,000: that is in £ million → unit='million'. "
                "For prose announcements like '£300 million for X': amount_local=300, unit='million'. "
                "ANACHRONISM GUARD: UKRI, ISCF, BEIS, DSIT did NOT exist in 2008 — DO NOT extract."
            ),
            2009: (
                "Science chapter: extract ring-fenced science budget total from summary "
                "table. "
                "UNIT RULE — MANDATORY: The science spending table header says '£ million'. "
                "A value like '6,912' in that table means £6,912 MILLION = £6.9 BILLION. "
                "Set amount_local=6912, unit='million'. DO NOT set unit='thousand'. "
                "If the table shows 'Total public investment in science and research' with "
                "a number around 6,000–7,500: that is in £ million → unit='million'. "
                "Also extract Technology Strategy Board funding and named R&D programmes. "
                "ANACHRONISM GUARD: UKRI, ISCF, BEIS, DSIT did NOT exist in 2009 — DO NOT extract."
            ),

            # ---------------------------------------------------------------
            # 2010-2015: Spending Review / Budget with BIS DEL tables
            # Comprehensive Spending Review 2010 (CSR10) restructured departments.
            # BIS (Dept for Business, Innovation and Skills) is now the main
            # science dept, inheriting DTI/DIUS R&D. DEL tables in £ billion.
            # Key items: BIS Capital DEL science ring-fence, research council totals.
            # ANACHRONISM GUARD: UKRI (2018), ISCF (2017) did NOT exist.
            # ---------------------------------------------------------------
            2010: (
                "Spending Review 2010: look for BIS (Business, Innovation and Skills) "
                "DEL table. Extract: BIS total Capital DEL, ring-fenced science budget "
                "within BIS (kept flat in real terms per SR10). "
                "Amounts in £ BILLION → unit='billion'. "
                "Also extract any named research council totals if stated with £ figures. "
                "ANACHRONISM GUARD: UKRI (created 2018), ISCF (created 2017) did NOT "
                "exist — do NOT extract them."
            ),
            2011: (
                "Budget 2011: extract BIS science-related DEL if a £ figure is stated "
                "for the ring-fenced science budget or a named research council. "
                "Amounts in £ BILLION → unit='billion'. "
                "ANACHRONISM GUARD: UKRI (created 2018), ISCF (created 2017) did NOT exist."
            ),
            2012: (
                "Budget 2012: extract BIS science DEL and any named R&D programme "
                "announcements with explicit £ amounts. "
                "Amounts in £ BILLION → unit='billion'. "
                "ANACHRONISM GUARD: UKRI (created 2018), ISCF (created 2017) did NOT exist."
            ),
            2013: (
                "Spending Round 2013: extract BIS science ring-fence total and any "
                "named R&D programmes with explicit £ amounts (e.g. Catapult centres, "
                "SBRI, ATI). "
                "Amounts in £ BILLION → unit='billion' (unless clearly stated as million). "
                "ANACHRONISM GUARD: UKRI (created 2018), ISCF (created 2017) did NOT "
                "exist in 2013 — do NOT extract them."
            ),
            2014: (
                "Budget 2014: extract BIS science DEL and named R&D programme "
                "announcements with explicit £ amounts. "
                "Amounts in £ BILLION → unit='billion'. "
                "ANACHRONISM GUARD: UKRI (created 2018), ISCF (created 2017) did NOT exist."
            ),
            2015: (
                "Spending Review 2015 / Budget 2015: extract BIS ring-fenced science "
                "budget total and named R&D programmes. "
                "Amounts in £ BILLION → unit='billion'. "
                "ANACHRONISM GUARD: UKRI (created 2018), ISCF (created 2017) did NOT exist."
            ),

            # ---------------------------------------------------------------
            # 2016: BIS still leads; last year before BEIS created (July 2016)
            # ---------------------------------------------------------------
            2016: (
                "Budget/Autumn Statement 2016: BIS → BEIS transition year (BEIS created "
                "July 2016). Extract BIS or BEIS science DEL and any named R&D programme "
                "announcements. Amounts in £ BILLION → unit='billion'. "
                "ANACHRONISM GUARD: UKRI (created April 2018) did NOT exist yet."
            ),

            # ---------------------------------------------------------------
            # 2017: Industrial Strategy White Paper; ISCF created; BIS→BEIS
            # ---------------------------------------------------------------
            2017: (
                "Autumn Budget 2017 / Industrial Strategy: ISCF (Industrial Strategy "
                "Challenge Fund) was created this year — extract it if a £ figure stated. "
                "Extract BEIS DEL science ring-fence. "
                "Amounts in £ BILLION → unit='billion'. "
                "ANACHRONISM GUARD: UKRI (created April 2018) did NOT exist in 2017 — "
                "do NOT extract UKRI."
            ),

            # ---------------------------------------------------------------
            # 2018: UKRI created April 2018; BEIS science budget
            # ---------------------------------------------------------------
            2018: (
                "Spring Statement / Budget 2018: UKRI was created in April 2018. "
                "Extract BEIS Capital DEL R&D ring-fence and UKRI total if stated. "
                "Also extract ISCF named challenges with £ amounts. "
                "Amounts in £ BILLION → unit='billion'. "
                "ANACHRONISM GUARD: ARIA (created 2021), DSIT (created 2023) did NOT exist."
            ),

            # ---------------------------------------------------------------
            # 2019-2020: BEIS / UKRI / ISCF era
            # ---------------------------------------------------------------
            2019: (
                "Spending Round 2019: extract BEIS total R&D DEL, UKRI budget, and ISCF "
                "named challenge totals if £ figures stated. "
                "Amounts in £ BILLION → unit='billion'. "
                "ANACHRONISM GUARD: ARIA (created 2021), DSIT (created 2023) did NOT exist."
            ),
            2020: (
                "Budget March 2020 / Spending Review Nov 2020: extract BEIS R&D DEL, "
                "UKRI budget, and ISCF challenge totals. "
                "R&D investment target '£22 billion per year by 2024-25' is a multi-year "
                "commitment — extract only the annual figure for this year if stated. "
                "Amounts in £ BILLION → unit='billion'. "
                "ANACHRONISM GUARD: ARIA (created 2021), DSIT (created 2023) did NOT exist."
            ),

            # ---------------------------------------------------------------
            # 2021: SR21 — most structured R&D DEL data available
            # Table 2.2 (or equivalent) shows R&D Capital DEL by department:
            #   - Total Capital DEL on R&D: £14.8B (2021-22) → £20.0B (2024-25)
            #   - BEIS: £11.3B → £14.2B
            #   - Core Research (UKRI): £4.8B → £5.9B
            #   - Innovate UK: £0.7B → £1.1B
            # ARIA (Advanced Research and Invention Agency) announced: £800M
            # Extract each year column as a separate row if the table is present.
            # ---------------------------------------------------------------
            2021: (
                "Spending Review 2021 (SR21): look for R&D DEL table (Table 2.2 or "
                "equivalent titled 'R&D spending by department'). "
                "Extract each named row for 2021-22 through 2024-25: "
                "BEIS total R&D DEL, UKRI Core Research budget, Innovate UK budget, "
                "ARIA (£800M total announced), and any other named agency with £ figure. "
                "Amounts in £ BILLION → unit='billion'. "
                "Also extract: government R&D investment target (£20B by 2024-25). "
                "ANACHRONISM GUARD: DSIT (created Feb 2023) did NOT exist in 2021."
            ),

            # ---------------------------------------------------------------
            # 2022: Autumn Statement 2022 / Spring Statement
            # ---------------------------------------------------------------
            2022: (
                "Autumn Statement 2022 / Spring Statement: extract BEIS R&D DEL, "
                "UKRI budget, and any named R&D programme amounts. "
                "Amounts in £ BILLION → unit='billion'. "
                "ANACHRONISM GUARD: DSIT (created Feb 2023) did NOT exist in 2022."
            ),

            # ---------------------------------------------------------------
            # 2023: DSIT created Feb 2023 (BEIS split into DSIT + DBT)
            # DSIT is now the main science department.
            # ---------------------------------------------------------------
            2023: (
                "Spring Budget / Autumn Statement 2023: DSIT (Dept for Science, "
                "Innovation and Technology) was created February 2023 when BEIS split. "
                "Extract DSIT total R&D DEL, UKRI budget, and named programme totals. "
                "Amounts in £ BILLION → unit='billion'. "
                "Also extract: Horizon Europe association deal if £ figure stated, "
                "Advanced Research and Invention Agency (ARIA) budget."
            ),

            # ---------------------------------------------------------------
            # 2024-2025: DSIT era; multi-year SR planned
            # ---------------------------------------------------------------
            2024: (
                "Spring Budget / Autumn Budget 2024: extract DSIT R&D DEL, UKRI total, "
                "and named programme totals with explicit £ figures. "
                "Amounts in £ BILLION → unit='billion'. "
                "Also extract: ARIA budget, Horizon Europe association costs, "
                "any named Innovate UK or research council totals."
            ),
            2025: (
                "Spring Statement / Spending Review 2025: extract DSIT R&D DEL, "
                "UKRI budget, and named programme totals. "
                "Amounts in £ BILLION → unit='billion'. "
                "Also extract: any new multi-year science spending commitments "
                "IF the annual breakdown is explicitly stated in a table."
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
    # Well-structured Finanslov (Finance Bill). Digital text 1975-2026.
    # KEY ERA SPLIT (units): 1975-2000 = 1.000 kr. (thousands); 2001+ = Mio. kr. (millions).
    # KEY SECTION SPLIT (ministry name):
    #   §20 Undervisningsministeriet (pre-2001 approx)
    #   §32 Forsknings-/Videnskabsministeriet (2001-2013 approx)
    #   §19 Uddannelses- og Forskningsministeriet (UFM, 2014+)
    # -----------------------------------------------------------------------
    "Denmark": {
        "skip_if": [
            # Student grants and loans — Statens Uddannelsesstotte (SU) is NOT R&D
            "Lines mentioning 'SU', 'uddannelsesstotte', 'laanekassen', 'studielan', "
            "'statens uddannelsesstotte' — these are student financial support, not R&D.",

            # Primary/secondary education
            "Lines under folkeskole, gymnasier, erhvervsuddannelse, or basic teacher training "
            "— only include if the line description also contains a research-specific term "
            "('forskning', 'forskningsprojekt', etc.).",

            # Generic operating lines in mixed ministries
            "'Driftsudgifter' (operating expenditure) lines in ministries with no R&D mandate "
            "(Socialministeriet, Justitsministeriet, Finansministeriet, Skatteministeriet, "
            "Trafikministeriet, Forsvarsministeriet) unless the line name explicitly contains "
            "a research term.",

            # Policy-evaluation 'forskning' — not budget R&D
            "Lines under Socialministeriet or Indenrigsministeriet containing 'forskning' only "
            "as a minor budget note (e.g. 'forskningsmidler' ≤ 5 Mio. kr.) — skip unless "
            "it is a dedicated research programme line.",

            # Pension and payroll overhead lines
            "Lines named 'Tjenestemandspension', 'pension', 'loensum', 'overhead' that are "
            "pure payroll/pension lines without an explicit R&D designation.",

            # Dakcare and school buildings
            "Lines for 'daginstitutioner', 'folkeskolebyggeri', 'skoler', 'dagtilbud'.",

            # Infrastructure/construction unless tagged R&D
            "Anlaegsudgifter (capital expenditure) lines that are clearly building/infrastructure "
            "projects, not research facility construction.",
        ],
        "include_note": [
            # Whole research ministry sections
            "The ENTIRE section § 19 Uddannelses- og Forskningsministeriet (UFM, 2014+) "
            "is R&D-relevant: include ministry-total overview line AND all sub-lines for "
            "universities, research councils, and innovation funds. Tag the § overview line "
            "as section_total / aggregation_role=section.",

            "§ 20 Undervisningsministeriet (pre-2001) and § 32 Forskningsministeriet / "
            "Videnskabsministeriet (2001-2013 approx) — same rule: include all sub-lines "
            "explicitly for universities, research councils, and research funds.",

            # Research councils — all in scope regardless of section
            "ALL named research councils are 100% R&D: "
            "Statens teknisk-videnskabelige Forskningsfond (STvF), "
            "Statens naturvidenskabelige Forskningsrad (SNF), "
            "Statens samfundsvidenskabelige Forskningsrad, "
            "Statens humanistiske Forskningsrad, "
            "Statens laegervidenskabelige Forskningsrad, "
            "Det Frie Forskningsraad / Danmarks Frie Forskningsfond, "
            "Det Strategiske Forskningsrad. "
            "Include their full annual appropriation.",

            # Innovation and applied research funds
            "Danmarks Innovationsfond (Innovation Fund Denmark) and its predecessor "
            "Hoejteknologifonden are fully in scope.",
            "Danmarks Grundforskningsfond (DNRF) is fully in scope.",

            # Universities
            "Individual university lines (Kobenhavns Universitet, Aarhus Universitet, DTU, "
            "AAU, SDU, RUC, CBS) appearing under the research/education ministry: "
            "include the full annual grant line (basisbevilling / tilskud).",

            # Atomic energy and Risoe
            "Atomenergikommissionen and Ris\u00f8 National Laboratory are fully in scope "
            "— include the full operating appropriation.",

            # Sector research institutes
            "Danmarks Meteorologiske Institut, GEUS (geological survey), Statens Serum Institut "
            "when they appear under the research ministry or as standalone budget lines.",
        ],
        "year_notes": {
            # Unit transition
            "1975": "Unit: 1.000 kr. (thousands DKK). § structure. Section § 20 = Undervisningsministeriet.",
            "1980": "Unit: 1.000 kr. (thousands DKK). Watch for multi-year comparison columns.",
            "1990": "Unit: 1.000 kr. (thousands DKK). § 20 still = Undervisningsministeriet.",
            "2000": "Unit: 1.000 kr. (thousands DKK). Last few years before switch to Mio. kr.",
            "2001": "UNIT SWITCH: From this year amounts are in Mio. kr. (millions DKK). "
                    "Also watch for ministry renaming around this period.",
            "2005": "Unit: Mio. kr. § 19 or § 32 = research ministry. New research council structure.",
            "2014": "§ 19 = Uddannelses- og Forskningsministeriet (UFM). "
                    "Danmarks Innovationsfond created (replaces Hoejteknologifonden + Strategisk Forskning).",
            "2015": "UFM § 19. Innovationsfond now operational. Det Frie Forskningsraad consolidated.",
            "2020": "Unit: Mio. kr. Structure stable. COVID supplements may appear as separate bills.",
        },
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
        "year_notes": {
            # ---------------------------------------------------------------
            # 1970–2001: Franc (FRF) era — État B/C tables by ministry chapter
            # Documents: JORF Loi de finances, 'État B' = dépenses ordinaires,
            # 'État C' = dépenses en capital. Amounts shown in FRANCS (not millions).
            # R&D appropriations appear under:
            #   - 'Services du Premier Ministre — V. Recherche' (pre-1984)
            #   - 'Ministère de la Recherche' or 'Industrie et Recherche' (1980s-1990s)
            #   - 'Universités' or 'Enseignement supérieur' as a separate section
            # ---------------------------------------------------------------
            **{
                y: (
                    f"JORF Loi de finances {y}. "
                    "Pre-LOLF era: État B/C tables by ministry chapter. "
                    "Currency: FRF (francs). UNIT: amounts shown in raw FRANCS — "
                    "set currency='FRF', unit='unit' (or unit='thousand' if table is headed 'milliers de francs'). "
                    "Key sections to extract: "
                    "'Recherche' under 'Services du Premier Ministre' or 'Ministère de la Recherche', "
                    "'Universités' or 'Enseignement supérieur', "
                    "'CNRS', 'CEA', 'INSERM', 'INRIA' if listed as separate appropriation lines. "
                    "SKIP: tax code amendments, fiscal balance tables, État B column headers. "
                    "No ETPT/FTE tables exist in this era. "
                    "Amounts in the billions of francs are plausible (1 billion FRF ≈ €150M in 2002 terms)."
                )
                for y in range(1970, 2002)
            },
            # ---------------------------------------------------------------
            # 2002–2005: Euro (EUR) era, pre-LOLF — État B/C structure continues
            # Same ministry-chapter structure but in euros. LOLF enacted 2001,
            # takes effect 2006. Documents are often short (Legifrance HTML extract).
            # ---------------------------------------------------------------
            **{
                y: (
                    f"JORF Loi de finances {y}. "
                    "Pre-LOLF era, post-euro conversion. État B/C tables. "
                    "Currency: EUR. Unit rule: use unit='unit' when the table is headed '(En euros)' "
                    "and unit='thousand' only when the table is explicitly headed '(En milliers d'euros)'. "
                    "Look for explicit État B/C ministry-chapter rows such as "
                    "'Ministère de la Recherche', 'Industrie et Recherche', "
                    "'Ministère de l'Éducation nationale et de la Recherche', "
                    "'Universités' or 'Enseignement supérieur'. "
                    "Also extract named operators only if they are explicitly listed "
                    "('CNRS', 'ANR' in 2005+, 'CEA', 'INSERM', 'INRIA', 'CNES'). "
                    "Do NOT extract Article totals, Titre V/VI grand totals, or the generic "
                    "'Ces crédits sont répartis par ministère conformément à l'état B/C annexé...' language. "
                    "NOTE: Many Legifrance extracts of this era reference tables as "
                    "'Vous pouvez consulter le tableau dans le JO' — if the actual table data "
                    "is absent and only references appear, return {\"items\": []}. "
                    "If the page only shows legislative prose plus a JO reference, it contains no usable budget rows. "
                    "No ETPT/FTE tables in this era."
                )
                for y in range(2002, 2006)
            },
            # ---------------------------------------------------------------
            # 2006–2025: LOLF era — Mission/Programme structure
            # CRITICAL: Documents contain TWO sequential table types:
            #   (A) ETPT/FTE staffing ceiling table (appears FIRST, pages ~55-75):
            #       Headed 'PLAFOND DES AUTORISATIONS D'EMPLOIS' or
            #       'Plafond exprimé en équivalents temps plein travaillé'.
            #       Shows headcount integers: 'Recherche et enseignement supérieur: 203 561'
            #       = 203,561 full-time employees. NOT euros. SKIP ENTIRELY.
            #   (B) Credit table — État B (appears LATER, pages ~100-200):
            #       Headed 'AUTORISATIONS D'ENGAGEMENT ET CRÉDITS DE PAIEMENT'.
            #       Shows euro amounts: 'Recherche et enseignement supérieur: 25 357 616 221 | 24 763 980 271'
            #       = €24.8 billion in full euros.
            # EXTRACTION RULE: Extract ONLY from the credit table (B). Convert to millions:
            #   divide full-euro amount by 1,000,000, set unit='million', currency='EUR'.
            #   Example: 24,763,980,271 → amount_local=24764, unit='million'.
            # ---------------------------------------------------------------
            **{
                y: (
                    f"JORF Loi de finances {y}. "
                    "LOLF era: Mission 'Recherche et enseignement supérieur'. "
                    "CRITICAL — two sequential tables in the document: "
                    "(A) ETPT staffing table (early pages ~55-75): "
                    "headed 'PLAFOND DES AUTORISATIONS D'EMPLOIS' or 'équivalents temps plein travaillé'. "
                    "Values like '203 561' = 203,561 FTE employees. "
                    "COMPLETELY SKIP TABLE A — do NOT extract any number from it. "
                    "(B) Credit table — État B (later pages ~100+): "
                    "Shows AE and CP columns in full euros. "
                    "ONLY extract from table B. "
                    "Convert full-euro amounts to millions: divide by 1,000,000, set unit='million'. "
                    "Prefer CP (Crédits de paiement) column over AE (Autorisations d'engagement). "
                    "Key programmes: 150 Formations supérieures (~12B EUR), "
                    "172 Recherches scientifiques (~5B EUR), 193 Recherche spatiale (~1.3B EUR). "
                    "Mission total 'Recherche et enseignement supérieur' ≈ 24,000–28,000 million EUR. "
                    "If extracted amount is < 5,000 million for the mission total, you likely "
                    "extracted from the ETPT table by mistake — discard and return no items."
                )
                for y in range(2006, 2026)
            },
        },
    },

    # -----------------------------------------------------------------------
    # GERMANY
    # Document type: Bundeshaushalt — varies significantly by era and file type.
    #
    # ERA GUIDE:
    #   1955-2002: Bundesgesetzblatt or Drucksache — Gesamtplan overview ONLY.
    #     One row per Einzelplan (ministry). No sub-programme breakdown.
    #     Currency: DM (1000 DM) until end 2001, EUR (1000 EUR) from 2002.
    #
    #   2003-2009: Two types per year:
    #     (a) Large Drucksache PDFs (1500150.pdf etc., ~1500KB each) — the FULL
    #         Bundeshaushaltsplan Einzelplan 30 chapter begins near the end
    #         (page ~2500 of ~2800). Contains detailed Titelgruppe breakdowns:
    #         DFG, MPG, Fraunhofer, Helmholtz, Leibniz, DLR, etc.
    #         Extract Titelgruppe TOTALS only (the Tgr. XX header line).
    #     (b) Small bgbl files (~15-25KB) — Gesamtplan overview only.
    #
    #   2010-2025: Similar two-type structure:
    #     (a) Numbered Drucksache files (1700624, 1703524 etc., ~8KB) — Gesamtplan.
    #     (b) Bundesgesetzblatt bgbl files (~40-150KB) — Gesamtplan + Verpflichtungs-
    #         ermächtigungen + Flexibilisierte Ausgaben tables.
    #     For 2021+: Large numbered files (1922600, 2001627 etc., ~1700KB) contain
    #         the full Einzelplan 30 — extract Titelgruppe totals only.
    #
    # Number format: SPACE as thousands separator: '14 053 404' = 14,053,404
    # Units: always 1 000 DM (pre-2002) or 1 000 EUR (2002+)
    # -----------------------------------------------------------------------
    "Germany": {
        "skip_if": [
            # Non-R&D Einzelpläne — skip all ministries except Epl 30 (BMBF/BMFT) and
            # Epl 31 (BMBW - Bildung und Wissenschaft, existed until 1994)
            "Rows for ALL Einzelpläne except Epl 30 and Epl 31: e.g. Epl 01 (Bundespräsident), "
            "Epl 02 (Bundestag), Epl 06 (Inneres), Epl 09 (Wirtschaft), Epl 10 (Landwirtschaft), "
            "Epl 14 (Verteidigung / Defence), Epl 60 (Allgemeine Finanzverwaltung) — "
            "even if they mention 'Forschung'. SKIP.",

            # Kreditermächtigung and Finanzierungsplan — not spending
            "Kreditermächtigung, Kreditfinanzierungsplan, and Finanzierungsübersicht entries "
            "— these are borrowing authorizations and fiscal balance tables. SKIP.",

            # Verpflichtungsermächtigungen — future-year commitments, not current budget
            "Verpflichtungsermächtigungen tables ('von dem Gesamtbetrag dürfen fällig werden') "
            "— these show multi-year commitment authorizations, NOT the current year's "
            "appropriation. Do NOT extract from these tables.",

            # Individual Titel sub-lines within a Titelgruppe — too granular
            "Individual Titel lines (Tit. 685 30, Tit. 685 40, Tit. 894 30 etc.) within a "
            "Titelgruppe — these are sub-components. SKIP them. Extract only the Titelgruppe "
            "TOTAL line (the 'Tgr. XX Bezeichnung (amount)' header line).",

            # Mehrjährige Maßnahmen — multi-year investment projects within Einzelplan
            "Mehrjährige Maßnahmen tables within Einzelplan 30 — these list individual "
            "multi-year capital projects. SKIP the project-level rows.",

            # Haushaltsvermerk and Erläuterungen — administrative notes
            "Haushaltsvermerk and Erläuterungen text blocks — these are administrative "
            "instructions and explanatory notes. SKIP, do not extract amounts from them.",

            # Stellenplan — personnel headcounts
            "Stellenplan tables (Planstellen, Stellen, A1-A16 grade tables) — "
            "these are staff headcount plans, not budget amounts. SKIP.",

            # Non-R&D Funktionen in Funktionenübersicht
            "Function codes for Allgemeine Dienste (0x), Auswärtige Angelegenheiten (02), "
            "Soziale Sicherung (04), Gesundheitswesen (05), Wohnungswesen (06) — "
            "skip even if they mention 'Forschung' as a sub-entry.",

            # Total federal budget grand-total rows — NOT BMBF
            # The Haushaltsübersicht table ends with a 'Summe Haushalt YYYY' row that
            # aggregates ALL Einzelpläne (typically €200–500B). This is the ENTIRE
            # federal budget, not Epl 30. SKIP. Also skip 'Gesamtbetrag' rows whose
            # amount exceeds 15,000,000 thousand (= €15B) — BMBF never exceeds ~€23B.
            "'Summe Haushalt YYYY' rows — these are total-federal-budget aggregates. SKIP. "
            "'Gesamtbetrag' rows with amounts above 15,000,000 (thousand) — these are "
            "supra-BMBF aggregates (Haushalt or Funktionenübersicht totals). SKIP. "
            "'Summe der Einzelpläne', 'Summe aller Einzelpläne', 'Gesamthaushalt' — SKIP.",
        ],
        "include_note": [
            # TWO document types — extract differently
            "CRITICAL — two document types require different extraction strategies: "
            "\n"
            "TYPE A — Gesamtplan overview (most files, especially pre-2003 and small bgbl): "
            "Contains one row per Einzelplan. Extract ONLY: "
            "(a) Epl 30 'Summe Ausgaben' = BMBF/BMFT total budget → decision='review'. "
            "(b) Epl 31 'Summe Ausgaben' = BMBW (Bildungsministerium, pre-1994) → decision='review'. "
            "These rows appear in the Haushaltsübersicht Teil I Ausgaben table. "
            "Do NOT invent sub-lines — there is no programme breakdown here. "
            "CRITICAL TRAP: the Haushaltsübersicht table ends with a grand-total row "
            "labelled 'Summe Haushalt YYYY' or 'Summe der Einzelpläne' — this is the "
            "ENTIRE federal budget (€200–500B), NOT BMBF. DO NOT extract this row. "
            "The Epl 30 row you want has the Einzelplan number '30' in the first column "
            "and an amount around €3B–€23B (thousand) depending on year. "
            "\n"
            "TYPE B — Full Einzelplan 30 chapter (large files: 1500150, 1503660, 1922600 etc.): "
            "Contains detailed BMBF programme lines. Extract TITELGRUPPE TOTALS ONLY: "
            "The header line 'Tgr. XX [Agency name] (amount)' for each Titelgruppe. "
            "Key Titelgruppen: DFG (Tgr 30), MPG (Tgr 40), Fraunhofer (Tgr 50/60), "
            "Helmholtz/HGF (Tgr 60/70), Leibniz/WGL (Tgr 50), DLR (Tgr 10/20). "
            "Do NOT extract individual Tit. 685/894 sub-lines within a Titelgruppe. "
            "CRITICAL — SINGLE-MINISTRY RULE: Large Bundeshaushaltsplan files contain "
            "ALL federal Einzelpläne (01 through 60) — Auswärtiges Amt (Epl 05), "
            "Inneres (Epl 06), Wirtschaft (Epl 09), Verteidigung (Epl 14), and many others. "
            "These other ministries also contain 'Forschung' lines (foreign cultural research, "
            "cybersecurity R&D, energy R&D etc.) but they are NOT in scope. "
            "SKIP every page that does not show the Einzelplan 30 header explicitly. "
            "Look for pages marked 'Einzelplan 30' or 'Epl. 30' at the top before extracting.",

            # Number format — critical
            "CRITICAL: German budget tables use SPACES as thousands separators. "
            "'14 053 404' = 14,053,404 (14 million). '356 400' = 356,400. "
            "A number like '1 930 303' means 1,930,303 (about 1.93 billion EUR). "
            "Parse space-separated digit groups as a single integer.",

            # Unit and currency
            "Amounts are always in 1 000 units (thousands): "
            "Pre-2002: currency='DEM', unit='thousand'. "
            "2002 onwards: currency='EUR', unit='thousand'. "
            "Example: '17 900 000' in the table = 17,900,000 thousand EUR = €17.9 billion.",

            # Epl 30 name changes
            "The R&D ministry (Epl 30) has been renamed several times: "
            "BMFT (Bundesministerium für Forschung und Technologie, 1969-1994), "
            "BMBF (Bundesministerium für Bildung und Forschung, 1994-2025), "
            "BMFTR (Bundesministerium für Forschung, Technologie und Raumfahrt, 2025+). "
            "All are Epl 30. Use 'BMBF' as the section_name in all cases.",

            # Funktionenübersicht — available in some Gesamtplan files
            "If the file has a Funktionenübersicht section: "
            "Funktion 137 = DFG → science_agency, decision=include. "
            "Funktion 164 = Gemeinsame Forschungsförderung (Helmholtz+MPG+Fraunhofer+Leibniz "
            "combined Bund+Länder joint funding) → section_total, decision=review. "
            "Funktion 165 = Forschung und experimentelle Entwicklung (applied R&D) → "
            "section_total, decision=review.",
        ],
        "year_notes": {
            # ---------------------------------------------------------------
            # 1955-1974: Only 1955 file available. Law text only — no tables.
            # ---------------------------------------------------------------
            1955: (
                "Single Bundesgesetzblatt file from 1955. "
                "Extract the Epl 30 total from the Haushaltsübersicht if present. "
                "Currency: DEM, unit='thousand'. "
                "Ministry was 'Bundesminister für Atomfragen' or 'für Kernenergie' — "
                "include if explicitly listed as Epl 30."
            ),

            # ---------------------------------------------------------------
            # 1975-1993: Gesamtplan overview only (Drucksache + bgbl files).
            # Two file types:
            #   (a) Numbered Drucksache (~13-40KB): legal text + Gesamtplan tables.
            #   (b) bgbl files (~15-20KB): Bundesgesetzblatt volume.
            # BOTH contain the Haushaltsübersicht Ausgaben table with Epl. 30 row.
            # Ministry name: Epl 30 = BMFT (Bundesminister für Forschung und Technologie)
            #                Epl 31 = BMBW (Bundesminister für Bildung und Wissenschaft)
            # Currency: DEM, unit='thousand'.
            # ---------------------------------------------------------------
            **{y: (
                "Gesamtplan overview document only — no Einzelplan 30 programme detail. "
                "Extract ONLY the single Epl 30 row from the Haushaltsübersicht Ausgaben "
                "table: 'Bundesminister für Forschung und Technologie (BMFT)' → "
                "section_name='BMFT (Epl 30)', decision='review', unit='thousand', "
                "currency='DEM'. "
                "Also extract Epl 31 (BMBW — Bundesminister für Bildung und Wissenschaft) "
                "if present: decision='review'. "
                "Do NOT invent sub-agency lines (DFG, MPG etc.) — these are NOT in this file. "
                "SANITY CHECK: BMFT budget in DM thousands was approximately: "
                "1975 ≈ 2,400,000; 1980 ≈ 3,600,000; 1985 ≈ 4,800,000; 1990 ≈ 6,800,000; 1993 ≈ 8,000,000. "
                "If your extracted amount is above 12,000,000 thousand DM, you have read the "
                "wrong row (likely Summe Haushalt or another Einzelplan). Re-check Epl 30 row."
            ) for y in range(1975, 1994)},

            # ---------------------------------------------------------------
            # 1994-2001: Same as above but BMBF replaces BMFT (merged in 1994).
            # From 1994: Epl 30 = BMBF (Bildung und Forschung, Epl 31 absorbed).
            # Currency still DEM through 2001.
            # ---------------------------------------------------------------
            **{y: (
                "Gesamtplan overview document only — no Einzelplan 30 programme detail. "
                "Extract ONLY the single Epl 30 row: "
                "'Bundesministerium für Bildung und Forschung (BMBF)' → "
                "section_name='BMBF (Epl 30)', decision='review', unit='thousand', "
                "currency='DEM'. "
                "Do NOT invent sub-agency lines. No Einzelplan 30 detail available. "
                "SANITY CHECK: BMBF budget in DM thousands was approximately: "
                "1994 ≈ 8,500,000; 1997 ≈ 7,800,000; 2001 ≈ 8,700,000 (some years had cuts). "
                "If your extracted amount is above 15,000,000 thousand DM, you have read the "
                "wrong row (likely the Haushaltsübersicht grand total). Re-check Epl 30 row."
            ) for y in range(1994, 2002)},

            # ---------------------------------------------------------------
            # 2002: Transition year — DM → EUR. Same Gesamtplan-only structure.
            # ---------------------------------------------------------------
            2002: (
                "Gesamtplan overview only. Extract Epl 30 (BMBF) total. "
                "Currency TRANSITION: 2002 was the first full EUR year. "
                "Use currency='EUR', unit='thousand'. "
                "If amounts appear in DM (file dated early 2002), use currency='DEM'."
            ),

            # ---------------------------------------------------------------
            # 2003-2009: TWO file types per year.
            # (a) Large numbered files (e.g. 1500150.pdf for 2003, ~1500KB):
            #     Full Bundeshaushaltsplan containing ALL Einzelpläne. Einzelplan 30
            #     starts near page 2500. Extract TITELGRUPPE TOTALS from Epl 30:
            #     Tgr 30 DFG, Tgr 40 MPG, Tgr 50 Fraunhofer, Tgr 60 Helmholtz,
            #     Tgr 50 Leibniz/WGL — whichever appear. currency='EUR', unit='thousand'.
            # (b) Small bgbl files (~15KB): Gesamtplan only.
            #     Extract single Epl 30 total row.
            # For TYPE A large files: DO NOT extract individual Tit. 685 sub-entries.
            # ---------------------------------------------------------------
            **{y: (
                f"Two file types for {y}: "
                "(a) LARGE DRUCKSACHE FILE (e.g. 1500150.pdf / 1503660.pdf): "
                "Contains the FULL Bundeshaushaltsplan with ALL Einzelpläne (01 through 60). "
                "SINGLE-MINISTRY RULE: This file contains Auswärtiges Amt (Epl 05), "
                "Inneres (Epl 06), Wirtschaft (Epl 09), Verteidigung (Epl 14), and many "
                "others — ALL of which contain 'Forschung' lines. SKIP EVERY PAGE that does "
                "not explicitly show 'Einzelplan 30' or 'Epl. 30' in the header. "
                "Navigate to Einzelplan 30 (starting near page 2500) and extract "
                "TITELGRUPPE TOTALS ONLY from Kapitel 3003/3004/3007: "
                "Tgr 30 DFG, Tgr 40 MPG, Tgr 50 Fraunhofer-Gesellschaft, "
                "Tgr 60 Helmholtz-Gemeinschaft (HGF), Tgr 50 Leibniz (WGL), "
                "Tgr 10 DLR, and any other named Titelgruppe. "
                "The Titelgruppe total is the BRACKETED amount on the Tgr header line "
                "(e.g. 'Tgr. 30 Deutsche Forschungsgemeinschaft (1 930 303)'). "
                "currency='EUR', unit='thousand'. decision='include' for DFG/MPG/Fraunhofer/HGF. "
                "DO NOT extract individual Tit. 685/894 sub-entries. "
                "DO NOT extract Mehrjährige Maßnahmen tables. "
                "(b) SMALL BGBl FILE: Extract only Epl 30 total from Haushaltsübersicht. "
                "section_name='BMBF (Epl 30)', decision='review'. "
                "SANITY CHECK: BMBF totals in EUR thousands were approx: "
                "2003 ≈ 8,700,000; 2005 ≈ 9,200,000; 2007 ≈ 11,300,000; 2009 ≈ 11,600,000. "
                "If your extracted BMBF total exceeds 25,000,000 thousand EUR, you have "
                "read the wrong row — likely a Haushalt total or double-counted."
            ) for y in range(2003, 2010)},

            # ---------------------------------------------------------------
            # 2010-2020: Numbered Drucksache + bgbl files (no full Einzelplan 30).
            # These give Gesamtplan overview + Verpflichtungsermächtigungen table.
            # Verpflichtungsermächtigungen = SKIP (future commitments, not spending).
            # Extract: Epl 30 Summe Ausgaben from Haushaltsübersicht.
            # If Funktionenübersicht available: extract Funktion 137/164/165.
            # currency='EUR', unit='thousand'.
            # ---------------------------------------------------------------
            **{y: (
                "Gesamtplan files (Drucksache + bgbl). "
                "Extract: Epl 30 (BMBF) 'Summe Ausgaben' from Haushaltsübersicht Teil I → "
                "section_name='BMBF (Epl 30)', decision='review', unit='thousand'. "
                "SKIP Verpflichtungsermächtigungen table entirely (future commitments). "
                "If Funktionenübersicht present: also extract Funktion 137 (DFG), "
                "Funktion 164 (Gemeinsame Forschungsförderung), Funktion 165 (F&E). "
                "currency='EUR', unit='thousand'."
            ) for y in range(2010, 2021)},

            # ---------------------------------------------------------------
            # 2021: Large numbered file (1922600.pdf, 3238 pages) has full Epl 30.
            # Einzelplan 30 starts at page ~2921 with 317 pages of detail.
            # EXTRACT TITELGRUPPE TOTALS ONLY — ~20-30 rows expected.
            # bgbl files give Gesamtplan overview (7-10 rows expected).
            # ---------------------------------------------------------------
            2021: (
                "TWO file types for 2021: "
                "(a) LARGE FILE (1922600.pdf): Full Einzelplan 30 starting at page ~2921. "
                "Extract TITELGRUPPE TOTALS ONLY — expect ~20-30 rows total: "
                "Tgr 30 DFG (~€1.93B), Tgr 40 MPG (~€1.17B), Tgr 50 Fraunhofer, "
                "Tgr 60 Helmholtz, Tgr 50 Leibniz (WGL), Tgr 10 DLR, and named Kapitel "
                "totals (3002 Bildungswesen, 3003 Wissenschaftssystem, 3004 Hightech, etc.). "
                "STRICTLY DO NOT extract: individual Tit. 685/894 sub-lines, Mehrjährige "
                "Maßnahmen tables, Haushaltsvermerk text, Erläuterungen, Stellenplan. "
                "The Titelgruppe total is the bracketed amount on the Tgr. header line. "
                "currency='EUR', unit='thousand'. "
                "(b) bgbl FILES (bgbl1_2020_66, bgbl120s3208): Gesamtplan overview. "
                "Extract Epl 30 Summe Ausgaben only."
            ),

            # ---------------------------------------------------------------
            # 2022-2025: Mix of large detailed files + bgbl Gesamtplan files.
            # For large files: same Titelgruppe-total-only approach as 2021.
            # ---------------------------------------------------------------
            **{y: (
                "Mix of Drucksache, bgbl, and potentially large Einzelplan files. "
                "For ALL files: extract Epl 30 BMBF totals and/or Titelgruppe totals. "
                "TITELGRUPPE RULE: if Einzelplan 30 detail is visible, extract only "
                "Tgr-level totals (DFG, MPG, Fraunhofer, Helmholtz, Leibniz, DLR). "
                "SKIP Verpflichtungsermächtigungen, individual Tit. lines, Mehrjährige "
                "Maßnahmen, Haushaltsvermerk, Erläuterungen, and Stellenplan. "
                "currency='EUR', unit='thousand'."
            ) for y in range(2022, 2026)},
        },
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
    # Statsbudsjettet Blåbok (Blue Book). 1975-1992: fully scanned (near-zero yield).
    # 1993-2009: partially scanned, limited quality. 2010-2026: excellent digital.
    # KEY MINISTRIES: Kunnskapsdepartementet (KD, Kap 260-290), Naerings- og
    #   fiskeridepartementet (NFD, Kap 920-930), Olje- og energidepartementet (OED).
    # UNIT: Full NOK in detail pages; 1000 NOK in Part I overview table.
    # -----------------------------------------------------------------------
    "Norway": {
        "skip_if": [
            # Scanned pre-1993 years — flag but don't hard-fail
            "Documents from 1975-1992 are fully scanned (no machine-readable text). "
            "If the extraction yields only a few short lines from these years, "
            "treat as low-confidence and mark accordingly.",

            # Student loans/grants — not R&D
            "Lines mentioning 'Statens laanekasse', 'laanekassen for utdanning', "
            "'studiestipend', 'utdanningsstipend', 'bostipend' — student financial aid, not R&D. "
            "Kap. 2410 (Statens laanekasse) is always skip.",

            # Plain infrastructure — roads, rail, buildings without R&D label
            "Lines under Statens vegvesen (road authority), Bane NOR, Avinor (airports), "
            "Kystverket (coast guard) unless the line name contains 'forskning' or 'FoU'.",

            # Oil/gas production subsidies — not R&D unless 'forskning' present
            "Lines for oil/gas field development, petroleum licensing fees, Petoro, SDOEE "
            "unless the line description explicitly contains 'forskning', 'FoU', "
            "'forskningsprogram', or similar.",

            # Defence procurement
            "Lines under Forsvarsdepartementet for weapons, materiel, personnel, or operations "
            "unless the line explicitly says 'forskning' or 'FoU'.",

            # Pension and overhead
            "Lines named 'pensjonspremie', 'arbeidsgiveravgift', 'fellesutgifter' that are "
            "pure payroll/pension overhead without an R&D designation.",

            # Healthcare operations without research label
            "Lines under Helse- og omsorgsdepartementet for hospital operations, "
            "patient treatment, drug reimbursement — only include if 'forskning' or "
            "'Folkehelseinstituttet' or 'Kreftregisteret' appears in the line name.",

            # Overview/summary totals — prefer detail lines
            "Part I overview table rows (identified by '1 000 kroner' column header): "
            "extract ONLY if no detail-page equivalent is found. "
            "Tag these as aggregation_role=section when included.",
        ],
        "include_note": [
            # Research Council — primary R&D vehicle
            "Norges forskningsrad (Research Council of Norway, NFR): "
            "ALL grant posts under KD and sector ministries are in scope. "
            "Typically Kap. 285 (under KD) with several posts: "
            "Post 50 = institution grant, Post 52/55 = programme grants, Post 70 = external grants. "
            "Also appears as Post 50 'Norges forskningsrad' under Kap. 920 (NFD) and Kap. 1830 (OED). "
            "Include every 'Norges forskningsrad' post found in the document.",

            # Universities — block grants
            "University block grants (basisbevilling) under Kunnskapsdepartementet "
            "(Kap. 260-275): NTNU, UiO, UiB, UiT, UiS, UiA, NMBU, NHH. "
            "Post 50 = block grant to the university. These are the main university time series. "
            "Include the Post 50 line for each university.",

            # Applied institutes
            "SINTEF (applied research) lines wherever they appear. "
            "Havforskningsinstituttet (Institute of Marine Research, IMR) under "
            "Naerings- og fiskeridepartementet: include. "
            "Folkehelseinstituttet (FHI) under Helse- og omsorgsdepartementet: include "
            "(it has a significant research mandate). "
            "Meteorologisk institutt (met.no): include. "
            "Norsk Romsenter (Norwegian Space Centre): include.",

            # Energy/oil research
            "Oljeforskningsprogrammet (oil research programme) and any line explicitly "
            "naming 'forskning' under OED (oil ministry) or NFD (industry ministry) — include. "
            "Petro-fund research lines under OED: include if the word 'forskning' is present.",

            # Section totals
            "Ministry-level totals for Kunnskapsdepartementet that explicitly cover R&D "
            "may be included as section-total rows. Tag aggregation_role=section.",

            # FoU labelling
            "Any line containing 'FoU' (Forskning og utvikling / R&D) in its description "
            "should be included regardless of the ministry.",
        ],
        "year_notes": {
            "1975": "SCANNED document — near-zero text extraction expected. Year likely unusable.",
            "1980": "SCANNED document — near-zero text extraction expected. Year likely unusable.",
            "1985": "SCANNED document — near-zero text extraction expected. Year likely unusable.",
            "1990": "SCANNED or low-quality OCR. Treat with low confidence.",
            "1993": "Transition period — partial digital text possible but quality variable.",
            "2000": "Quality variable. Some digital, some scanned. Check extraction yield.",
            "2010": "FULLY DIGITAL from this year. Kap./Post structure clear. Use detail pages.",
            "2015": "Excellent digital quality. Kunnskapsdepartementet Kap. 285 = NFR main chapter.",
            "2020": "Excellent digital quality. COVID supplements may appear as separate bills — "
                    "focus on main Blaabok.",
            "2024": "Latest available. NFR Kap. 285 Post 50 and sectoral posts visible. "
                    "Full NOK amounts on detail pages (e.g. 'Norges forskningsrad 6 500 000 000').",
        },
    },

    # -----------------------------------------------------------------------
    # NETHERLANDS
    # Document type: Rijksbegroting (State Budget).
    # ERA SPLIT (critical):
    #   1975-2001: SINGLE FILE — Miljoenennota or Rijksbegroting overview.
    #              Amounts in MILLIONS of guilders (miljoenen guldens, NLG).
    #   2002+:     SEPARATE FILE PER MINISTRY — <year>_ministry<N>.pdf.
    #              Amounts in THOUSANDS of euros (bedragen x € 1.000, EUR).
    # KEY R&D MINISTRIES (2002+ file numbering):
    #   ministry8  = OCW (Onderwijs, Cultuur en Wetenschap, Ministry VIII)
    #                Art. 07 = Wetenschappelijk onderwijs (university block grants)
    #                Art. 16 = Onderzoek en wetenschapsbeleid (NWO, KNAW)
    #   ministry13 = EZ (Economische Zaken, Ministry XIII)
    #                Art. 02 = Bedrijvenbeleid / innovatie (innovation/enterprise, TNO)
    #                Art. 03 = Toekomstfonds (Future Fund)
    #   ministry14 = LNV (Landbouw, Visserij, Voedselzekerheid en Natuur, Ministry XIV)
    #                Art. 23 = Kennis en innovatie (knowledge and innovation)
    # SKIP: ministry10 (Defensie), ministry12 (IenW / infrastructure),
    #        ministry16 (VWS / health unless RIVM line)
    # -----------------------------------------------------------------------
    "Netherlands": {
        "skip_if": [
            # Non-R&D ministry totals — skip even if they mention 'onderzoek'
            "Ministry-level totals for Ministerie van Defensie (ministry10), "
            "Ministerie van Infrastructuur en Waterstaat (IenW, ministry12), "
            "Ministerie van Sociale Zaken (XV) — skip their line items unless the "
            "description explicitly names a research institution.",

            # Student grants — studiefinanciering is NOT R&D
            "Lines mentioning 'studiefinanciering', 'studietoelage', 'studiebeurs', "
            "'studentenreisproduct', 'DUO' (Dienst Uitvoering Onderwijs student loans) "
            "— these are student financial support, not R&D.",

            # Infrastructure construction — not R&D unless 'onderzoek' present
            "Lines for 'aanleg', 'onderhoud', 'rijksinfrastructuur', 'Rijkswaterstaat' "
            "(road/waterway authority), 'ProRail', 'spoorwegen', 'luchtvaart' unless "
            "the description explicitly contains 'onderzoek', 'R&D', or 'kennis'.",

            # Defence procurement/operations — not R&D
            "Lines under Defensie for equipment procurement ('materieel'), personnel "
            "('personeel'), operations ('gereedstelling') unless explicitly tagged "
            "'onderzoek' or 'MIVD wetenschappelijk onderzoek'.",

            # Social insurance overhead (ABP pension, ZW, WW) — not R&D
            "Lines for 'ABP-premies', 'pensioenpremies', 'werkloosheidswet', "
            "'zorgverzekering' — these are social insurance and pension overhead.",

            # Generic 'overige' lines in non-R&D ministries
            "'Overige' (miscellaneous) lines in ministries that are NOT OCW, EZ, or LNV "
            "— only include if the section heading contains an explicit R&D institution name.",

            # Cultural subsidies without research component
            "Lines for museums, performing arts, heritage ('erfgoed', 'podiumkunsten', "
            "'musea') under OCW unless the line is explicitly for scientific collections "
            "or research infrastructure.",
        ],
        "include_note": [
            # NWO — primary public research funder
            "NWO (Nederlandse Organisatie voor Wetenschappelijk Onderzoek, Dutch Research "
            "Council) and its predecessor ZWO (Organisatie voor Zuiver-Wetenschappelijk "
            "Onderzoek, pre-1988) are the main public research funders — include all grant "
            "budget lines. Also include NWO-TTW / STW (Technology Foundation, merged into "
            "NWO in 2017) and NWO-I (Institutes Organisation of NWO).",

            # KNAW — Academy of Sciences
            "KNAW (Koninklijke Nederlandse Akademie van Wetenschappen, Royal Netherlands "
            "Academy of Arts and Sciences) is a dedicated science institution — include "
            "its full annual appropriation.",

            # TNO — applied research
            "TNO (Toegepast Natuurwetenschappelijk Onderzoek, Netherlands Organisation for "
            "Applied Scientific Research) is a dedicated R&D institution — include its "
            "institutional grant under EZ Art. 02.",

            # University block grants under OCW Art. 07
            "University block grants under OCW Art. 07 (Wetenschappelijk onderwijs) are "
            "the primary funding lines for Dutch universities — include the per-university "
            "lines and the collective 'Wetenschappelijk onderwijs' article total. "
            "Named universities: UvA, VU, UU, RUG, Leiden, TU Delft, TU/e, EUR, Radboud, "
            "Maastricht, Twente, Tilburg, WUR (Wageningen).",

            # OCW Art. 16 — research and science policy
            "OCW Art. 16 (Onderzoek en wetenschapsbeleid / Research and science policy) "
            "contains NWO, KNAW, and SURF appropriations — include this article total "
            "and any named sub-lines within it.",

            # EZ innovation instruments
            "EZ Art. 02 (Bedrijvenbeleid: innovatie en ondernemerschap) and Art. 03 "
            "(Toekomstfonds) contain TNO grants and innovation instruments (WBSO fiscal "
            "R&D credit, TKI top sector consortia) — include named research funding lines. "
            "RVO (Rijksdienst voor Ondernemend Nederland) administers the WBSO/TKI grants.",

            # LNV agri-food research
            "LNV Art. 23 (Kennis en innovatie) funds Wageningen Research and agricultural "
            "knowledge institutes — include this article and named sub-lines.",

            # Unit rule — critical
            "UNIT RULE (MANDATORY): "
            "1975-2001 documents: amounts in MILLIONS of guilders → unit='million', currency='NLG'. "
            "2002+ per-ministry files: amounts in THOUSANDS of euros → unit='thousand', currency='EUR'. "
            "Dutch number format: '.' = thousands separator, ',' = decimal. "
            "Example: '1.670.345' = 1,670,345 (thousands EUR = ~€1.67 billion).",
        ],
        "year_notes": {
            **{y: (
                f"Rijksbegroting {y} — NLG era (single-file or overview). "
                "Unit: miljoenen guldens (millions NLG). currency='NLG', unit='million'. "
                "Key sections: OCW (Onderwijs/Wetenschappen), EZ (Economische Zaken), "
                "NWO/ZWO block grant, KNAW, TNO, university lines. "
                "Number format: '.' = thousands sep., ',' = decimal."
            ) for y in range(1975, 2002)},

            2002: (
                "TRANSITION YEAR — first per-ministry EUR file. "
                "Files: ministry8 (OCW), ministry13 (EZ), ministry14 (LNV). "
                "Unit: bedragen x € 1.000 (thousands EUR). currency='EUR', unit='thousand'. "
                "NWO, KNAW, university block grants appear under ministry8 Art. 07/16."
            ),

            **{y: (
                f"Rijksbegroting {y} — EUR era, per-ministry files. "
                "Files of interest: ministry8 (OCW Art.07 universities + Art.16 NWO/KNAW), "
                "ministry13 (EZ Art.02 TNO/innovation), ministry14 (LNV Art.23 WUR/agri-research). "
                "Unit: bedragen x € 1.000. currency='EUR', unit='thousand'. "
                "SKIP ministry10 (Defensie), ministry12 (IenW), ministry16 (VWS, unless RIVM)."
            ) for y in range(2003, 2026)},
        },
    },

    # -----------------------------------------------------------------------
    # SWITZERLAND
    # Document type: Voranschlag der Schweizerischen Eidgenossenschaft.
    # ERA SPLIT (critical):
    #   1975-2020: Bundesblatt Bundesbeschluss — SHORT (3-10 pages), aggregate only.
    #              Contains: Erfolgsrechnung total, Investitionsrechnung total, and any
    #              special Verpflichtungskredite (e.g. ETH-Bereich Bauprogramm, SNF).
    #              YIELD IS LOW — expect 1-5 R&D-relevant rows per document.
    #   2021+:    VA-Band3-d.pdf = Voranschlag Band 3 (German).
    #              Section C 'Budgetpositionen' contains detailed departmental lines.
    #              KEY SECTION: WBF (Wirtschaft, Bildung, Forschung) with ETH-Bereich,
    #              SNF, Innosuisse, CERN, ESA contributions.
    # UNIT: always FULL CHF (Franken) — space as thousands separator.
    # -----------------------------------------------------------------------
    "Switzerland": {
        "skip_if": [
            # Defence R&D (VBS/DDPS) — mixed military/civilian, skip by default
            "Lines under VBS / DDPS (Eidg. Departement für Verteidigung / "
            "Bundesamt für Rüstung armasuisse) unless the line explicitly names "
            "a civilian research programme ('Forschung', 'RUAG Forschung').",

            # Infrastructure without research label
            "Lines for 'Strasseninfrastruktur', 'Eisenbahninfrastruktur', 'Bauten', "
            "'Nationalstrassen' under UVEK/DETEC unless the description explicitly "
            "contains 'Forschung' or 'Wissenschaft'.",

            # Social insurance transfers — not R&D
            "Lines for AHV (old-age insurance), IV (disability insurance), EO "
            "(income compensation), EL (supplementary benefits) — these are social "
            "security transfers, not R&D appropriations.",

            # Generic overhead lines
            "Lines labelled 'Personalaufwand' (personnel costs), 'Verwaltungsaufwand' "
            "(admin costs), 'Raumaufwand' (premises) without an explicit R&D designation.",

            # Foreign aid / development cooperation
            "Lines under EDA (Aussenpolitik, DEZA, humanitäre Hilfe) — development "
            "cooperation and foreign affairs, not domestic R&D.",

            # Pre-2021 docs: aggregate only — do not invent sub-lines
            "For 1975-2020 (Bundesblatt files): do NOT invent sub-institution lines. "
            "These documents contain only aggregate totals — extract only items that are "
            "explicitly listed in the text (e.g. 'Beitrag ETH-Bereich Fr. X').",
        ],
        "include_note": [
            # ETH-Bereich — primary R&D vehicle
            "ETH-Bereich (Bereich der Eidgenössischen Technischen Hochschulen) is the "
            "primary Swiss R&D funding mechanism. The annual 'Bundesbeitrag an den "
            "ETH-Bereich' (~CHF 3.7 billion in recent years) is the single largest R&D "
            "line in the Swiss federal budget. "
            "Sub-institutions (ETH Zürich, EPFL, PSI, Empa, Eawag, WSL) receive their "
            "share from this block grant — include both the block total and any named "
            "sub-institution lines if separately listed.",

            # SNF — main competitive research funder
            "SNF / SNSF (Schweizerischer Nationalfonds zur Förderung der "
            "wissenschaftlichen Forschung, Swiss National Science Foundation): "
            "the annual 'Bundesbeitrag an den SNF' (~CHF 1.1 billion in recent years) "
            "is a core R&D appropriation — include it.",

            # Innosuisse / KTI — innovation agency
            "Innosuisse (Schweizerische Agentur für Innovationsförderung, Swiss "
            "Innovation Agency, created 2018) and its predecessor KTI (Kommission für "
            "Technologie und Innovation, pre-2018) fund applied/industry-linked R&D "
            "— include their annual appropriation.",

            # CERN and ESA
            "CERN contributions (Swiss membership) and ESA-Beiträge (contributions to "
            "European Space Agency) are legitimate R&D international obligations — "
            "include both.",

            # Agroscope and Swisstopo
            "Agroscope (federal agricultural research, under WBF/BLW) and Swisstopo "
            "(federal topographic and geoscience institute) perform R&D — include if "
            "listed as a separate budget line.",

            # SBFI/SERI coordination
            "SBFI / SERI (Staatssekretariat für Bildung, Forschung und Innovation, "
            "State Secretariat for Education, Research and Innovation) is the main R&D "
            "coordinating body under WBF — include any programme lines under SBFI that "
            "are explicitly for research grants or international science cooperation.",

            # Unit rule — critical
            "UNIT RULE (MANDATORY): All amounts in FULL SWISS FRANCS (unit='unit', "
            "currency='CHF'). Space is the thousands separator. "
            "Example: '3 714 600 000' = CHF 3,714,600,000 (3.7 billion). "
            "Only use unit='million' if the text explicitly says 'Mio. Fr.' before the number.",
        ],
        "year_notes": {
            **{y: (
                f"Bundesblatt Bundesbeschluss {y} — AGGREGATE ONLY. "
                "This is a SHORT legislative document (3-10 pages) with only aggregate "
                "budget authorizations. "
                "Extract ONLY items explicitly listed: ETH-Bereich block grant total, "
                "SNF grant if listed, KTI if listed, CERN/ESA contributions if listed. "
                "DO NOT invent sub-institution breakdowns — they are not in this document. "
                "currency='CHF', unit='unit'. Space = thousands separator. "
                "YIELD: expect 1-5 R&D rows maximum."
            ) for y in range(1975, 2021)},

            **{y: (
                f"VA-Band3-d.pdf {y} — FULL DETAIL DOCUMENT. "
                "Section C 'Budgetpositionen' contains detailed departmental lines. "
                "KEY R&D SECTION: WBF (Wirtschaft, Bildung, Forschung): "
                "'Beitrag an den ETH-Bereich' (~3.7B CHF), "
                "'Beitrag an den SNF' (~1.1B CHF), "
                "'Beitrag an Innosuisse' (~300M CHF), "
                "'CERN-Beitrag' (~150M CHF), "
                "'ESA-Beiträge'. "
                "currency='CHF', unit='unit'. Space = thousands separator. "
                "Example: '3 714 600 000' = CHF 3.7 billion. DO NOT divide by any scale."
            ) for y in range(2021, 2026)},
        },
    },

    # -----------------------------------------------------------------------
    # SWEDEN
    # Document type: Statsbudget / Budgetproposition (prop. XXXX/XX:1), Swedish.
    # Unit: tusental kronor (thousands SEK). Space = thousands separator.
    # Utgiftsområde (UO) system from 1994. UO 16 is the primary R&D area.
    # -----------------------------------------------------------------------
    "Sweden": {
        "skip_if": [
            # Student financial aid — CSN and studiebidrag are the largest items
            # in UO 15 (studiestöd) and look like R&D at a glance.
            "'Studiemedel', 'studiebidrag', 'studiestöd', 'CSN', 'Centrala studiestödsnämnden', "
            "'studielån', 'kunskapslyftet' (adult education programme) — these are student "
            "financial aid and adult education grants, NOT research appropriations. SKIP always.",

            # Defence and procurement without explicit research label
            "Lines for Försvarsmakten, FMV (Försvarets materielverk), totalförsvar, "
            "or any 'materielanskaffning' / 'flygsystem' / 'marksystem' / 'sjösystem' line "
            "— these are defence operational/procurement lines. SKIP unless the description "
            "explicitly contains 'forskning' or 'FoU'.",

            # Transport infrastructure without research label
            "'Trafikverket', 'Vägverket', 'Banverket', 'Sjöfartsverket', 'Luftfartsverket', "
            "'Luftfartsverket', 'Sjöfartsverket', 'väghållning', 'järnvägsunderhåll' "
            "— pure transport infrastructure. SKIP unless 'forskning' is in the description.",

            # Cultural subsidies
            "Cultural funding lines: 'teater', 'opera', 'film', 'konsert', 'museer' "
            "(unless paired with 'forskning') — cultural subsidies, not R&D.",

            # Social insurance and pensions
            "'Försäkringskassan', 'Pensionsmyndigheten', 'Arbetsförmedlingen', "
            "'sjukpenning', 'barnbidrag', 'A-kassa', 'bostadsbidrag', 'äldreomsorgen' "
            "— social transfer payments. SKIP always.",
        ],
        "include_note": [
            # Core research councils — all should be included
            "Vetenskapsrådet (VR) and its predecessor research councils NFR, TFR, MFR, "
            "HSFR, SJFR (pre-2001 merger) are core R&D appropriations. Include their "
            "annual anslag lines under UO 16.",

            # Innovation agencies
            "VINNOVA (created 2001) and its predecessor NUTEK (partly) and STU (pre-1991) "
            "are the main innovation R&D agencies. Include under UO 24 (Näringsliv) "
            "or UO 16 where applicable.",

            # Research foundations listed in budget
            "Formas, Forte (created 2001), and SSF (Stiftelsen för Strategisk Forskning) "
            "appear as anslag lines — include them. "
            "Riksbankens Jubileumsfond covers humanities and social sciences — include.",

            # Applied research institutes
            "RISE (Research Institutes of Sweden, created 2017 from SP/Swerea/Innventia) "
            "and its predecessor institutes receive state grants — include. "
            "SMHI (meteorological/hydrological institute) — include its anslag. "
            "Rymdstyrelsen (space agency) and FOI/FOA (defence research) — include.",

            # University appropriations
            "University anslag under UO 16 (KTH, Chalmers, Uppsala, Lund, Stockholm, "
            "Göteborg, Umeå, Linköping, Karolinska) — include. These are block grants "
            "for education AND research; tag as higher_education.",
        ],
        "year_notes": {
            **{y: (
                "Pre-1994 Budgetproposition: budget structured by Departement chapters (§), "
                "NOT Utgiftsområden. "
                "Key R&D chapters: § 8 Utbildningsdepartementet (universities, NFR, TFR, HSFR), "
                "§ 16 Industridepartementet (STU then NUTEK for applied/industrial R&D). "
                "Unit: tusental kronor (thousands SEK) throughout this era. "
                "Extract anslag lines for named research councils and universities. "
                "Amounts around 100,000–2,000,000 thousand SEK are plausible for major agencies."
            ) for y in range(1975, 1994)},

            **{y: (
                "Post-1994 Budgetproposition using Utgiftsområde (UO) system. "
                "KEY R&D UOs: UO 16 (Utbildning och universitetsforskning) — "
                "universities, Vetenskapsrådet (from 2001), Formas, Forte, and predecessors. "
                "UO 24 (Näringsliv) — VINNOVA (from 2001), NUTEK, industrial R&D. "
                "UO 20 (Allmän miljö- och naturvård) — SMHI, environmental research. "
                "Each anslag has a code like '2:1 Vetenskapsrådet' or '25:1 VINNOVA'. "
                "Unit: tusental kronor (thousands SEK). "
                "Pre-2001: extract NFR, TFR, MFR, HSFR, SJFR, NUTEK lines. "
                "Post-2001: extract Vetenskapsrådet, VINNOVA, Formas, Forte."
            ) for y in range(1994, 2026)},
        },
    },

    # -----------------------------------------------------------------------
    # AUSTRIA
    # Document type: Bundesfinanzgesetz (BFG) / Bundesvoranschlag (BVA), German.
    # Unit: Tausend ATS (pre-2002) or Tausend EUR (2002+). Period = thousands sep.
    # Pre-2013: Kapitel/Einzelplan structure. Post-2013: UG/Untergliederung structure.
    # -----------------------------------------------------------------------
    "Austria": {
        "skip_if": [
            # Defence without research label
            "Lines for Bundesministerium für Landesverteidigung (BMLV), Heer, Miliz, "
            "Militär, or defence procurement — SKIP unless 'Forschung' or 'Rüstungsforschung' "
            "is explicitly in the description.",

            # Social transfers — very large amounts, clearly not R&D
            "'Pensionsversicherung', 'Krankenversicherung', 'Arbeitslosengeld', "
            "'AMS' (Arbeitsmarktservice), 'Familienbeihilfe', 'Pflegegeld', "
            "'Sozialhilfe', 'Notstandshilfe', 'Wochengeld', 'Kinderbetreuungsgeld' "
            "— social insurance and transfer payments. SKIP always.",

            # Pure infrastructure without research label
            "'Straßenbau', 'Schieneninfrastruktur', 'Hochbau' (construction), "
            "'ASFINAG' (motorway company), 'ÖBB-Infrastruktur' (railway infrastructure), "
            "'Bundesstraßen', 'Autobahnen' — pure infrastructure. "
            "SKIP unless 'Forschung' or 'Wissenschaft' is in the description.",

            # EU co-financing overhead (administrative matching funds, not R&D)
            "Lines labelled 'EU-Kofinanzierung' or 'EU-Kofinanzierungsanteil' that are "
            "administrative matching-fund transfers rather than named R&D programmes — "
            "SKIP unless the accompanying description names a specific R&D project.",

            # Budget chapter totals for non-R&D ministries
            "Section totals (Gesamtsumme, Summe, Gesamt) for ministries other than "
            "UG 31 (Wissenschaft und Forschung), UG 33 (Wirtschaft), and Einzelplan 13 "
            "— do not extract broad ministry totals for BMLV, BMI, BMF, BMAS, BMAA.",
        ],
        "include_note": [
            # Primary R&D agencies
            "FWF (Fonds zur Förderung der wissenschaftlichen Forschung) — annual "
            "state appropriation to the Austrian Science Fund; core basic research funder. "
            "FFG (Forschungsförderungsgesellschaft, from 2004; predecessor FFF pre-2004) "
            "— applied and industrial R&D promotion. Both are priority includes.",

            # Academy and advanced institutes
            "ÖAW (Österreichische Akademie der Wissenschaften) — state grant to the "
            "Austrian Academy of Sciences; include. "
            "IST Austria / ISTA (from 2006) — annual state grant; include. "
            "AIT (Austrian Institute of Technology, from 2009; formerly Arsenal Research) "
            "— state participation/grant; include.",

            # University Globalbudgets
            "Austrian university Globalbudgets (post-2002 Universitätsgesetz): "
            "each university's block grant appears as a single line in UG 31. "
            "These cover teaching AND research — tag as higher_education. "
            "Include: Universität Wien, TU Wien, TU Graz, Universität Graz, JKU Linz, "
            "Universität Innsbruck, MedUni Wien, BOKU, WU Wien, etc.",

            # International memberships
            "CERN-Beitrag (Austrian contribution to CERN) and ESA-Beitrag (Austrian "
            "contribution to ESA/European Space Agency) — include as rd_adjacent or direct_rd. "
            "CD-Labor (Christian Doppler Forschungsgesellschaft) — cooperative research labs "
            "co-funded by industry; include.",
        ],
        "year_notes": {
            **{y: (
                "Pre-2002 Bundesvoranschlag (ATS era): currency='ATS', unit='thousand'. "
                "Budget structured by Einzelpläne (Kapitel). "
                "KEY R&D CHAPTER: Einzelplan 13 = Wissenschaft und Forschung (BMWF/BMBWK). "
                "Contains: FWF, FFF (pre-FFG), ÖAW, Ludwig Boltzmann Gesellschaft, "
                "university block grants, CERN/ESA contributions. "
                "Also check Einzelplan 07 (BMVIT) for applied R&D lines. "
                "Austrian number format: '.' = thousands sep, ',' = decimal. "
                "'280.000' = 280,000 (thousand ATS)."
            ) for y in range(1975, 2002)},

            2002: (
                "TRANSITION YEAR: 2002 Bundesvoranschlag is the first EUR budget. "
                "Currency='EUR', unit='thousand'. "
                "Structure still uses Einzelpläne (pre-2013 reform). "
                "KEY R&D CHAPTER: Einzelplan 13. "
                "Fixed EUR/ATS rate: 1 EUR = 13.7603 ATS."
            ),

            **{y: (
                "EUR era Bundesvoranschlag: currency='EUR', unit='thousand'. "
                "Structure still uses Einzelpläne (Kapitel) pre-2013 Haushaltsrechtsreform. "
                "KEY R&D CHAPTER: Einzelplan 13 (Wissenschaft und Forschung). "
                "Contains: FWF, FFG (from 2004; FFF before), ÖAW, university block grants, "
                "CERN/ESA, CD-Labor, AIT (from 2009). "
                "Also check Einzelplan 07 (BMVIT) for BMVIT R&D programmes."
            ) for y in range(2003, 2013)},

            **{y: (
                "Post-2013 Haushaltsrechtsreform: Untergliederung (UG) system. "
                "Currency='EUR', unit='thousand'. "
                "KEY R&D UGs: UG 31 (Wissenschaft und Forschung) — FWF, ÖAW, universities, "
                "IST Austria, CERN/ESA. UG 33 (Wirtschaft) — FFG, AWS, AIT, CD-Labor. "
                "Structure within each UG: Globalbudgets (GB) then Detailbudgets (DB). "
                "University Globalbudgets appear as single lines — tag higher_education. "
                "Austrian number format: '.' = thousands sep, ',' = decimal."
            ) for y in range(2013, 2026)},
        },
    },

    # -----------------------------------------------------------------------
    # SPAIN
    # Documents: Presupuestos Generales del Estado (PGE), BOE, 1979–2023
    # Key agencies: CSIC, AEI (from 2017), CDTI, ISCIII, CIEMAT
    # Unit: millones de pesetas pre-2002; miles de euros 2002+
    # -----------------------------------------------------------------------
    "Spain": {
        "skip_if": [
            "Defence ministry (Ministerio de Defensa) lines — skip unless 'investigación' explicitly named.",
            "Social Security (Seguridad Social) transfers — not R&D.",
            "Student grants and scholarships (becas de estudios) that are not research fellowships.",
            "Transport infrastructure (carreteras, ferrocarril, puertos) without 'investigación'.",
            "Regional development (FEDER, fondos estructurales) overhead lines.",
            "EU co-financing administrative lines without explicit R&D programme name.",
            "Fondo de Garantía de Servicios Públicos and similar inter-government transfers.",
        ],
        "include_note": [
            "CSIC (Consejo Superior de Investigaciones Científicas) is the main public research body — always include.",
            "AEI (Agencia Estatal de Investigación, organism 28.303) — research grants agency from 2017.",
            "CDTI (organism under Industria/Ciencia) — industrial R&D loans and grants.",
            "ISCIII (Instituto de Salud Carlos III, organism 28.106) — health research.",
            "CIEMAT (Centro de Investigaciones Energéticas, Medioambientales y Tecnológicas, organism 28.103).",
            "Programme 463B 'Fomento y coordinación de la investigación científica y técnica' — key R&D programme.",
            "Programme codes 541A, 542A, 542E, 465A are always R&D.",
            "Plan Nacional de I+D+i appropriations are always R&D.",
        ],
        "year_notes": {
            **{y: (
                f"Spain {y} — ESP (peseta) era. "
                "Amounts in millones de pesetas. Set currency='ESP', unit='million'. "
                "Main R&D section: Sección 18 Educación y Ciencia, Servicio 25 Investigación. "
                "Programme codes: 541A Investigación Científica, 542A Investigación Técnica. "
                "CSIC is under Educación y Ciencia as Organismo Autónomo."
            ) for y in range(1979, 2002)},
            **{y: (
                f"Spain {y} — EUR era. Amounts in 'Miles de euros' (thousands). "
                "currency='EUR', unit='thousand'. "
                "Key organisms: CSIC, CDTI, ISCIII, CIEMAT under Ministerio de Ciencia. "
                "From 2017: AEI (Agencia Estatal de Investigación, 28.303) is the main grant agency."
            ) for y in range(2002, 2026)},
        },
    },

    # -----------------------------------------------------------------------
    # FINLAND
    # Documents: Valtion talousarvio (State Budget), 1985–2025
    # Key agencies: Suomen Akatemia (29.60.50), Tekes/Business Finland (32.20), VTT, GTK
    # Unit: full FIM pre-2002; full EUR 2002+
    # -----------------------------------------------------------------------
    "Finland": {
        "skip_if": [
            "Student grants/loans (opintotuki, opintolaina, Kela education benefits) — not R&D.",
            "Defence ministry lines (chapter 27 Puolustusministeriö) — skip unless civilian research named.",
            "Social and health transfers (chapter 33) — skip unless THL research line explicitly named.",
            "Transport infrastructure (Traficom, Väylävirasto) without tutkimus/kehittäminen.",
            "General university operating costs (chapter 29.40) unless the line explicitly labels research.",
            "Veikkaus lottery transfer lines (29.60.53) — earmarked for sports/culture, not R&D.",
        ],
        "include_note": [
            "Suomen Akatemia tutkimusmäärärahat (29.60.50) is the single most important R&D line — always include.",
            "Tekes (pre-2018) and Business Finland (from 2018) innovation appropriations (32.20) — always include.",
            "VTT institutional grant (chapter 32, 'erityisavustus' or 'valtionavustus VTT') — always include.",
            "GTK (Geologian tutkimuskeskus) toimintamenot — include.",
            "VATT (Valtion taloudellinen tutkimuskeskus) toimintamenot — include.",
            "Luke / MTT / RKTL / Metla — natural resources research institutes — include.",
            "Amounts are in FULL EUR (or full FIM pre-2002). Never divide by any scale.",
        ],
        "year_notes": {
            **{y: (
                f"Finland {y} — FIM (Finnish markka) era, scanned proposal. "
                "OCR quality may be poor. Amounts in full FIM. currency='FIM', unit='unit'. "
                "Chapter 29 Opetusministeriö: universities + Suomen Akatemia. "
                "Chapter 32 Kauppa- ja teollisuusministeriö: VTT + research institutes. "
                "Low extraction yield expected for early scanned years."
            ) for y in range(1985, 1993)},
            **{y: (
                f"Finland {y} — FIM era, improving quality. "
                "currency='FIM', unit='unit'. Full markka amounts. "
                "Suomen Akatemia at chapter 29.60. Tekes at chapter 32. VTT under KTM."
            ) for y in range(1993, 2002)},
            **{y: (
                f"Finland {y} — EUR era, fully digital. "
                "currency='EUR', unit='unit'. Full euro amounts. "
                "Space = thousands separator. '169 941 000' = 169,941,000 EUR. "
                "Suomen Akatemia research grants at 29.60.50 (~170-500M EUR depending on year). "
                "Business Finland (from 2018) / Tekes (pre-2018) at chapter 32.20."
            ) for y in range(2002, 2026)},
        },
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
