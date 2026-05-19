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
            "Any line containing 'Development' but not also containing a science/research signal — too many non-R&D development programmes appear in NZ Appropriation Acts.",
            "War pensions, housing development, regional development, and social development lines without a science/research signal.",
            "Student support and generic tertiary-education lines unless the line explicitly names a research fund, science vote, or research institute.",
            "Generic MBIE departmental administration lines without science/research content.",
        ],
        "include_note": [
            "Vote Science and Innovation is the primary R&D vote — all lines within it "
            "are candidates for extraction.",
            "Crown Research Institute (CRI) operating funding is in-scope as science_agency.",
            "DSIR (Department of Scientific and Industrial Research) in early years "
            "is a dedicated research agency — include its operating appropriations.",
            "Modern science funds explicitly named under Science, Innovation and Technology are in scope: Endeavour Fund, Health Research Fund, Marsden Fund, Partnered Research Fund, Catalyst Fund, and Callaghan Innovation.",
            "Crown Research Institutes, NIWA, GNS, AgResearch, Plant and Food, and Callaghan Innovation operating appropriations are in scope when explicitly named.",
        ],
        "year_notes": {
            **{y: (
                f"New Zealand {y}: early appropriations. Strong signals are DSIR and related Scientific and Industrial Research votes. "
                "Do not confuse general 'Development' programmes with R&D."
            ) for y in range(1975, 1993)},
            **{y: (
                f"New Zealand {y}: transition to Crown Research / Research, Science and Technology structure. "
                "Look for Crown Research Institutes, FRST-like science funding, and named research appropriations."
            ) for y in range(1993, 2015)},
            **{y: (
                f"New Zealand {y}: modern Vote Science, Innovation and Technology era. "
                "Strong targets: MBIE science vote, Endeavour Fund, Marsden Fund, Health Research Fund, Partnered Research Fund, Catalyst Fund, and Callaghan Innovation."
            ) for y in range(2015, 2026)},
        },
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
                "Pre-2002 Bundesvoranschlag (ATS era): currency='ATS', unit='million'. "
                "Table header: '(Beträge in Millionen Schilling)'. "
                "Budget structured by Einzelpläne (Kapitel). "
                "KEY R&D CHAPTER: Einzelplan 13 = Wissenschaft und Forschung (BMWF/BMBWK). "
                "Contains: FWF, FFF (pre-FFG), ÖAW, Ludwig Boltzmann Gesellschaft, "
                "university block grants, CERN/ESA contributions. "
                "Also check Einzelplan 07 (BMVIT) for applied R&D lines. "
                "Austrian number format: '.' = thousands sep, ',' = decimal. "
                "'280,000' = 280.000 million ATS (280 million). '6.303,815' = 6303.815 million."
            ) for y in range(1975, 2002)},

            2002: (
                "TRANSITION YEAR: 2002 Bundesvoranschlag is the first EUR budget. "
                "Currency='EUR', unit='million'. "
                "Table header: '(Beträge in Millionen Euro)'. "
                "Structure still uses Einzelpläne (pre-2013 reform). "
                "KEY R&D CHAPTER: Einzelplan 13. "
                "Fixed EUR/ATS rate: 1 EUR = 13.7603 ATS."
            ),

            **{y: (
                "EUR era Bundesvoranschlag: currency='EUR', unit='million'. "
                "Table header: '(Beträge in Millionen Euro)'. "
                "Structure still uses Einzelpläne (Kapitel) pre-2013 Haushaltsrechtsreform. "
                "KEY R&D CHAPTER: Einzelplan 13 (Wissenschaft und Forschung). "
                "Contains: FWF, FFG (from 2004; FFF before), ÖAW, university block grants, "
                "CERN/ESA, CD-Labor, AIT (from 2009). "
                "Also check Einzelplan 07 (BMVIT) for BMVIT R&D programmes."
            ) for y in range(2003, 2013)},

            **{y: (
                "Post-2013 Haushaltsrechtsreform: Untergliederung (UG) system. "
                "Currency='EUR', unit='million'. "
                "Table header: '(Beträge in Millionen Euro)'. "
                "KEY R&D UGs: UG 31 (Wissenschaft und Forschung) — FWF, ÖAW, universities, "
                "IST Austria, CERN/ESA. UG 33 (Wirtschaft) — FFG, AWS, AIT, CD-Labor. "
                "Structure within each UG: Globalbudgets (GB) then Detailbudgets (DB). "
                "University Globalbudgets appear as single lines — tag higher_education. "
                "Austrian number format: '.' = thousands sep, ',' = decimal."
            ) for y in range(2013, 2027)},
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
    "Czech Republic": {
        "skip_if": [
            "Broad legal totals such as 'CELKOVÝ PŘEHLED', 'PŘÍJMY CELKEM', 'VÝDAJE CELKEM', state debt, and broad treasury/financial-operations totals.",
            "Municipal/regional transfer pages (kraje, obce, okresní úřady) unless a named research body is explicitly funded.",
            "Defence, interior, police, and security lines without explicit výzkum / věda / vývoj wording.",
            "Transport, roads, rail, and infrastructure lines without explicit research or innovation wording.",
            "General social, pension, and employment-support lines without research wording.",
            "Broad ministry chapter totals unless a named research agency, programme, or institute is explicit.",
        ],
        "include_note": [
            "Akademie věd České republiky / AV ČR is always an in-scope science-agency target.",
            "Grantová agentura České republiky / GA ČR is a core research-grants agency and should be included when explicitly named.",
            "Technologická agentura České republiky / TA ČR (from 2009/2010 onward) is an innovation/R&D agency and should be included when explicitly named.",
            "Named university research, research institutes, and explicit lines containing výzkum, vývoj, věda, or inovace are in scope.",
            "Annex files (Přílohy) usually contain much richer institution-level detail than the main law text and should be trusted more when both exist.",
        ],
        "year_notes": {
            **{y: (
                f"Czech Republic {y}: early post-transition annex era. Many useful pages are in Přílohy PDFs rather than the short legal wrapper. "
                "Amounts are often in MILLIONS of CZK ('v mil. Kč'), but still verify the header. "
                "Prioritize named agencies like Akademie věd ČR and explicit research lines over broad chapter totals."
            ) for y in range(1993, 2001)},
            **{y: (
                f"Czech Republic {y}: annex/docx detail era. Many detailed tables are in annex PDFs or docx annex content. "
                "Amounts often appear in THOUSANDS of CZK ('v tis. Kč' / 'v tisících Kč'), but confirm from the header. "
                "Strong targets: Grantová agentura ČR, Akademie věd ČR, MŠMT research lines, MPO innovation/research lines."
            ) for y in range(2001, 2009)},
            **{y: (
                f"Czech Republic {y}: mixed modern law/annex era. Main-law pages can still be overly aggregated, so prefer named agencies, programmes, and annex-style rows when available. "
                "TA ČR becomes a valid target from 2010 onward. Always verify whether the page uses full Kč, thousand Kč, or million Kč from the header."
            ) for y in range(2009, 2026)},
        },
    },
    "Belgium": {
        "skip_if": [
            "Broad federal or community ministry totals unless they are explicitly science-policy or research-agency appropriations.",
            "Social security, pensions, family benefits, and health-service operating transfers.",
            "Transport, public works, mobility, and infrastructure lines unless explicitly research-labelled.",
            "Defence lines unless research or scientific institute language is explicit.",
        ],
        "include_note": [
            "Include BELSPO / SSTC / OSTC science-policy lines, FNRS/FWO when explicitly named, and federal scientific institutes such as observatory, meteorology, space, and nuclear research bodies.",
            "Belgium may mix federal and regional structures; prefer named agencies and institutes over broad education totals.",
        ],
        "year_notes": {
            **{y: (
                f"Belgium {y}: pre-euro budget. Check whether the header uses BEF and whether the table is in thousands or millions. "
                "Extract only explicit science-policy offices, research funds, and named institutes. Avoid broad ministry totals."
            ) for y in range(1975, 2002)},
            **{y: (
                f"Belgium {y}: euro-era budget. Check whether the table says thousands of euros. "
                "Prioritize BELSPO and named scientific institutes; treat education and welfare chapters conservatively."
            ) for y in range(2002, 2026)},
        },
    },
    "Chile": {
        "skip_if": [
            "Broad education totals unless a research body or research programme is explicitly named.",
            "Social programmes, housing, welfare, and public-health service delivery without research language.",
            "Public works, roads, rail, and port infrastructure without investigación/tecnología language.",
            "Defence procurement unless explicit research or technological development is named.",
        ],
        "include_note": [
            "Include CONICYT and ANID appropriations, explicit CORFO innovation/R&D lines, INIA, IFOP, and named public research institutes.",
            "Agricultural, fisheries, and mining research institutes are in-scope when they are clearly institutional research appropriations.",
        ],
        "year_notes": {
            **{y: (
                f"Chile {y}: read the law header for scale before extracting amounts. Sample 1976 law explicitly says 'En miles de pesos', so thousands of CLP are plausible for early years. "
                "The key target is named science, innovation, or research institutes, not article totals."
            ) for y in range(1976, 2026)},
        },
    },
    "Estonia": {
        "skip_if": [
            "Broad education spending without research signal.",
            "Municipal transfers, social insurance, and welfare transfers.",
            "Transport and infrastructure spending without research or innovation language.",
        ],
        "include_note": [
            "Include Haridus- ja Teadusministeerium research lines, Estonian research council/foundation bodies, Archimedes-type science funding bodies, and named university or institute research appropriations.",
        ],
        "year_notes": {
            1991: (
                "Estonia 1991: sample budget amendment law uses 'rublades' (roubles), not EEK or EUR. "
                "Treat 1991 as a special case: currency should be RUB if the header confirms roubles. "
                "Amounts appear as full units unless the header states otherwise."
            ),
            **{y: (
                f"Estonia {y}: pre-euro years may use EEK and later years EUR. Always confirm both currency and scale from the header. "
                "Prefer explicit research ministry, council, foundation, and university lines."
            ) for y in range(1992, 2026)},
        },
    },
    "Iceland": {
        "skip_if": [
            "Fisheries OPERATIONAL lines (vessel funding, quota management, Fiskistofa administration) — include only lines with 'rannsókn/rannsóknir' explicitly.",
            "Road, port, and transport infrastructure (Vegagerðin, samgönguframkvæmdir) without research signal.",
            "Social insurance and Tryggingamálastofnun transfers without research signal.",
            "Broad ministry chapter totals ('samtals', 'alls') — these are aggregates, not individual R&D lines.",
            "Veikfjárframlag / unemployment insurance transfers.",
        ],
        "include_note": [
            "Pre-2003 research governance: Rannsóknaráð ríkisins (code 232 / 02-232), Vísindasjóður (975 / 02-235), Vísindaráð (02-234), Rannsóknasjóður (02-233) — all R&D, always include.",
            "Post-2003: Rannís (Icelandic Centre for Research) consolidated all above — always include.",
            "Háskóli Íslands (University of Iceland, 201 / 02-201) — include research-labelled appropriations; skip pure teaching/operating lines unless explicitly R&D.",
            "Hafrannsóknastofnun (Marine Research Institute, 05-202) — always include.",
            "Orkustofnun (energy research component, 301 / 11-301) and ÍSOR (geothermal research) — include research lines.",
            "Functional categories 07.10 (science funds), 21.10 (university research), 12.20/13.20/17.20 (sectoral R&D) — always include.",
            "Amounts are in THOUSANDS of ISK pre-1993, MILLIONS of ISK from 1993 onward.",
        ],
        "year_notes": {
            **{y: (
                f"Iceland {y}: EARLY ERA — amounts in THOUSANDS of króna (Þús. kr. / ÞÚ. kr.). "
                "Set unit='thousand', currency='ISK'. "
                "Chapter codes are 3-digit (e.g. 201=Háskóli, 232=Rannsóknaráð, 975=Vísindasjóður, 301=Orkustofnun). "
                "R&D is split across Menntamálaráðuneyti (education ministry) and other ministries."
            ) for y in range(1975, 1993)},
            **{y: (
                f"Iceland {y}: TRANSITION/MID ERA — amounts in MILLIONS of króna (M.kr.). "
                "Set unit='million', currency='ISK'. "
                "Chapter codes are two-tier XX-YYY (02=Education, 05=Fisheries, 11=Industry). "
                "Key codes: 02-232 Rannsóknaráð, 02-233 Rannsóknasjóður, 02-234 Vísindaráð, 02-235 Vísindasjóður, "
                "02-201 Háskóli Íslands, 05-202 Hafrannsóknastofnun, 11-301 Orkustofnun."
            ) for y in range(1993, 2003)},
            **{y: (
                f"Iceland {y}: MODERN ERA — Rannís created 2003 by merging all prior research councils. "
                "Amounts in MILLIONS of króna (m.kr. / mkr.). Set unit='million', currency='ISK'. "
                "Key functional codes: 07.10 (science/competition funds), 21.10 (university research). "
                "Rannís administers competitive grants. Hafrannsóknastofnun under fisheries ministry. "
                "ÍSOR (Iceland GeoSurvey) under energy/industry."
            ) for y in range(2003, 2026)},
        },
    },
    "Hungary": {
        "skip_if": [
            "Broad 'fejezet összesen', 'cím összesen', 'alcím összesen', or 'mindösszesen' totals — these are aggregates, not final R&D rows.",
            "Road, rail, motorway, and generic infrastructure development lines without kutatás/innováció signal.",
            "Defence, police, and security lines without explicit kutatás/fejlesztés signal.",
            "General welfare, pension, social, and health-service operating lines without research signal.",
            "EU competitiveness or development-programme wrappers unless the specific line explicitly names research, innovation, or a research body.",
        ],
        "include_note": [
            "Nemzeti Kutatási, Fejlesztési és Innovációs Alap — always include.",
            "MTA / Magyar Tudományos Akadémia and MTA Library / research institute lines — always include.",
            "Hazai innováció támogatása and explicit international innovation-cooperation support lines — include as programme totals.",
            "Agrárkutatás támogatása and named kutatóközpont / kutatóintézet / kutatási infrastruktúra lines — include.",
            "Amounts are usually stated in 'millió forint' — unit='million', currency='HUF' unless the header says otherwise.",
        ],
        "year_notes": {
            **{y: (
                f"Hungary {y}: early post-transition budget law. These years may behave more like legal wrappers than rich budget annexes. "
                "Expect sparse yield unless a named research body, fund, or programme row is explicit. "
                "Prefer only strong matches with kutatás / fejlesztés / innováció / MTA / Nemzeti Kutatási. "
                "Use unit='million', currency='HUF' when the table says 'millió forint'."
            ) for y in range(1991, 2004)},
            **{y: (
                f"Hungary {y}: structured budget-law / annex era with named programme and institution rows. "
                "Strong targets: MTA bodies, research institutes, innovation support, and National R&D fund lines. "
                "Keep broad chapter totals conservative."
            ) for y in range(2004, 2015)},
            **{y: (
                f"Hungary {y}: modern budget-law era. Strong targets include Nemzeti Kutatási, Fejlesztési és Innovációs Alap, "
                "MTA-related bodies, agricultural research, and explicit innovation support lines. "
                "Programme-level extraction is acceptable when the line is clearly R&D-specific."
            ) for y in range(2015, 2026)},
        },
    },
    "Latvia": {
        "skip_if": [
            "Broad Izglītības un zinātnes ministrija totals — the ministry is too broad and includes sports, student loans, and general education administration.",
            "Police and defence academy lines unless they explicitly say scientific research.",
            "University hospital and general health-service lines.",
            "Student credit, study credit, sports, and cultural infrastructure lines without research wording.",
            "Macro legal-wrapper pages that mention ministries but do not contain a concrete science programme or institution row.",
        ],
        "include_note": [
            "Zinātne / science programme totals are in scope when clearly presented as budget programmes.",
            "Fundamentālie zinātniskie pētījumi, Zinātnes bāzes finansējums, and Valsts pārvaldes institūciju pasūtītie zinātniskie pētījumi are core R&D lines.",
            "Latvijas Zinātnes padome and Latvijas Zinātņu akadēmija are in scope when explicitly named.",
            "Zinātniskās darbības attīstība universitātēs and Zinātniskās infrastruktūras nodrošināšana un attīstība augstskolās are valid higher-education R&D lines.",
            "Pre-2014 likely uses full LVL units; 2014+ uses full EUR units. Do not rescale unless a header says otherwise.",
        ],
        "year_notes": {
            **{y: (
                f"Latvia {y}: early transition-era budget law. These years may alternate between short legal-wrapper texts and denser annex-style pages. "
                "Extract only strong science matches: 'Kopā zinātnes finansēšanai', Latvijas Zinātņu akadēmija, state-commissioned scientific research, "
                "fundamental research, or clearly named science institutions."
            ) for y in range(1991, 1997)},
            **{y: (
                f"Latvia {y}: richer programme-table era. Strong targets include Zinātne, Fundamentālie zinātniskie pētījumi, "
                "Latvijas Zinātnes padome, Investīcijas zinātnei, and university science development/infrastructure lines."
            ) for y in range(1997, 2014)},
            **{y: (
                f"Latvia {y}: many files are short legal-wrapper budgets with only selected earmarks visible. "
                "Extract explicit science earmarks and programme lines, but expect lower yield than the 1997–2013 detailed programme-table era."
            ) for y in range(2014, 2026)},
        },
    },
    "Lithuania": {
        "skip_if": [
            "Broad Švietimo ir mokslo ministerija / Švietimo, mokslo ir sporto ministerija totals — the ministry includes education, student loans, and non-R&D operations.",
            "Moksleivio krepšelis / school basket / general school-financing lines.",
            "Student loan, study loan, and tuition-support lines without explicit research content.",
            "Generic innovation or investment-support lines unless they explicitly mention research, technology development, or science institutions.",
            "General university operating or student-transfer lines without explicit scientific research wording.",
        ],
        "include_note": [
            "Mokslas ir studijos / science and studies programme lines are in scope when clearly budgeted.",
            "Valstybinė mokslo, studijų ir technologijų tarnyba and Lietuvos mokslo taryba are in scope when explicitly named.",
            "Mokslinių tyrimų įstaigoms numatyti asignavimai, state-commissioned research, and fundamental/basic research lines are core R&D.",
            "University and institute lines are in scope only when explicitly for research activity, scientific infrastructure, or research institutions.",
            "Pre-2015 usually means full LTL units; 2015+ usually means full EUR units unless the header says otherwise.",
        ],
        "year_notes": {
            **{y: (
                f"Lithuania {y}: transition-era budget. Some files are detailed tables, others much thinner. "
                "Prioritise explicit science/studies programmes, research institutions, and state-commissioned research lines."
            ) for y in range(1991, 2005)},
            **{y: (
                f"Lithuania {y}: mixed era. Richer programme tables appear in some years, but many lines still combine higher education and research. "
                "Extract only explicit research, science, technology, or research-institution rows."
            ) for y in range(2005, 2015)},
            **{y: (
                f"Lithuania {y}: modern legal-wrapper tendency. Expect many education-and-science ministry references but fewer direct R&D rows. "
                "Keep to explicit research, technology-development, or named science-institution appropriations."
            ) for y in range(2015, 2026)},
        },
    },
    "Luxembourg": {
        "skip_if": [
            "Caisse Nationale d'Assurance Pension (CNAP) / Inspection Générale de la Sécurité Sociale — "
            "pension and social-insurance transfers; not R&D.",
            "Service de la dette / intérêts de la dette publique — debt service; always skip.",
            "Ministère de la Défense — Luxembourg has minimal defence; skip without explicit recherche label.",
            "Travaux publics / routes / autoroutes — transport/road infrastructure; not R&D.",
            "Broad primary and secondary education ministry totals (Ministère de l'Education Nationale) "
            "without a named university or research sub-line.",
            "Cultural subsidies (opéra, cinéma, musées) without explicit recherche component.",
            "Section 01 (Présidence du Gouvernement) general administration lines.",
            "NOTE: Year 1986 is missing from the collection — no file to process.",
        ],
        "include_note": [
            "FNR — Fonds National de la Recherche: include ALL FNR lines (subventions, transfers, operational).",
            "Université du Luxembourg (from 2003): include named university research/operating lines.",
            "LIST / CRP Henri Tudor (pre-2015): include all operational and research lines.",
            "LISER / CEPS-INSTEAD (pre-2015): include research-institute operating lines.",
            "LIH / CRP Santé (pre-2015): include health-research lines.",
            "CRP Gabriel Lippmann (pre-2015): include environmental/materials research lines.",
            "Section 03 'Enseignement Supérieur et Recherche' — ALL lines in this section are relevant.",
            "Service de Coordination de la Recherche et de l'Innovation (SCRI) — include.",
            "Named bilateral research cooperation contributions (e.g. CERN, ESA, ESO) — include.",
            "'Subvention', 'dotation', 'transfert' lines going to named R&D institutions — include.",
        ],
        "year_notes": {
            **{y: (
                "LUF ERA: amounts in full Luxembourg franc (LUF), unit='unit'. 1 EUR = 40.3399 LUF. "
                "Research lines are in hundreds of thousands to tens of millions of LUF. "
                "Scanned images — OCR required for all pages."
            ) for y in range(1975, 2002)},
            1997: (
                "LUF ERA: amounts in full Luxembourg franc (LUF), unit='unit'. 1 EUR = 40.3399 LUF. "
                "SCANNED FILE (43.7 MB) — OCR required."
            ),
            2002: (
                "CURRENCY TRANSITION YEAR: Luxembourg switched to euro on 1 January 2002. "
                "This file covers fiscal year 2002 — amounts in full EUR, unit='unit'. "
                "SCANNED FILE (32 MB) — OCR required."
            ),
            **{y: (
                "EUR ERA (2003-2014): amounts in full euros, unit='unit'. "
                "Key institutions: FNR (Fonds National de la Recherche, dotation ~40-70M EUR), "
                "Université du Luxembourg (founded 2003, state contribution 03.2), "
                "CRP Henri Tudor, CRP Gabriel Lippmann, CRP Santé, CEPS/INSTEAD — "
                "these are the pre-merger names; after 2015 they become LIST/LISER/LIH. "
                "All R&D lines are in budget section 03 (Ministère de l'Enseignement Supérieur et de la Recherche). "
                "Text files have direct PDF layer — clean extraction, no OCR issues."
            ) for y in range(2003, 2015)},
            2003: (
                "EUR ERA: amounts in full euros, unit='unit'. "
                "INSTITUTION CHANGE: Université du Luxembourg founded by law November 2003 — "
                "first budget appearance may be small (startup costs only). "
                "CRP Henri Tudor, CRP Gabriel Lippmann, CRP Santé still active under old names."
            ),
            **{y: (
                "EUR ERA (2015+): amounts in full euros, unit='unit'. "
                "INSTITUTION REFORM 2015: the three CRP institutes were merged and renamed — "
                "CRP Henri Tudor → LIST (Luxembourg Institute of Science and Technology), "
                "CEPS/INSTEAD → LISER (Luxembourg Institute of Socio-Economic Research), "
                "CRP Santé → LIH (Luxembourg Institute of Health). "
                "CRP Gabriel Lippmann merged into LIST in 2015. "
                "Budget section 03.3 'Recherche et innovation' now shows: "
                "FNR dotation (~69M EUR in 2022), LISER contribution (~14M), "
                "LIST contribution (~53M), LIH contribution (~44M). "
                "Total Ministère budget ~600M EUR (includes Uni.lu ~220M in section 03.2). "
                "Text files have direct PDF layer — clean extraction."
            ) for y in range(2015, 2026)},
        },
    },
    "Mexico": {
        "skip_if": [
            "IMSS / ISSSTE pension and social-security lines (Ramo 19, Ramo 50) — huge totals, never R&D.",
            "Servicio de la deuda pública (Ramo 06, Ramo 24): debt service — always skip.",
            "SEDENA (Ramo 07) and SEMAR (Ramo 13) defence lines without explicit 'investigación' or 'tecnología'.",
            "SCT / SICT (Ramo 09) road and transport infrastructure without research label.",
            "Ramo 28 / Ramo 33 / Ramo 39 Aportaciones Federales — federal transfers to states/municipalities; not R&D.",
            "Ramo 23 Provisiones Salariales y Económicas — general salary reserve; skip.",
            "BIENESTAR / SEDESOL (Ramo 20) social-welfare programmes without research label.",
            "Ramo 06 HACIENDA general finance operations — skip (pension coordination, debt).",
            "Broad education ministry (SEP Ramo 11) macro totals without named IPN/UNAM/research-centre sub-line.",
            "Guardia Nacional / PF / PGR crime-investigation lines (investigación here = criminal investigation, not R&D).",
            "Duplicate files: '1999 MEX 31121998-MAT.pdf' and '2000 MEX 31121999-MAT (1).pdf' are exact copies of "
            "the same-year MAT files — mark all pages relevant=false on the MEX duplicate to avoid double-counting.",
        ],
        "include_note": [
            "Ramo 38 — ALL lines: CONACYT (1971-2022) and CONAHCyT (2022+) are the primary R&D agency; "
            "include every named line (becas, fondo sectorial, fondo mixto, centros públicos, apoyos institucionales).",
            "Centros Públicos de Investigación (CPIs) supervised by CONACYT/CONAHCyT: CICESE, CIESAS, CIO, "
            "CIDESI, CIQA, CIAD, CENAPRED, CIATEJ, CICY, INFOTEC, CENIDET, CIMAV — include their budget lines.",
            "ININ (Instituto Nacional de Investigaciones Nucleares) under SENER (Ramo 18) — direct R&D.",
            "IPN — Instituto Politécnico Nacional (Ramo 11 SEP): research and graduate education; "
            "include explicitly labelled IPN research / posgrado lines.",
            "UNAM (Ramo 11 SEP) — include lines labelled 'investigación' or 'posgrado'; "
            "skip broad subsidio educativo lump sum.",
            "CINVESTAV (Centro de Investigación y de Estudios Avanzados del IPN, Ramo 11) — direct R&D.",
            "INIFAP (Ramo 08 / SAGARPA/SADER) — agricultural R&D institute; include.",
            "Agencia Espacial Mexicana (AEM, from 2010) — appears under Ramo 38; include.",
            "PRONACES (Programas Nacionales Estratégicos, CONAHCyT 2020+) — include.",
            "Fondo Sectorial CONACYT + sector ministry (e.g. CONACYT-SEP, CONACYT-SENER) — include.",
            "Lines explicitly labelled 'investigación científica', 'investigación y desarrollo', "
            "'ciencia y tecnología', 'innovación tecnológica' in any ramo — include.",
        ],
        "year_notes": {
            **{y: (
                "SCANNED FILE ERA (1994-2005): MAT files are large scanned DOF images (up to 224 MB); "
                "OCR with spa+eng is required. Budget amounts are in full pesos or millions of pesos — "
                "read the table header carefully. "
                "NOTE: 1999 and 2000 each have two identical MAT files; process only one."
            ) for y in range(1994, 2006)},
            **{y: (
                "OLD PESO ERA (pre-1993): amounts are in old pesos (peso antiguo / moneda nacional). "
                "1 new peso (MXN, 1993+) = 1,000 old pesos. Apply conversion factor when comparing across eras."
            ) for y in range(1975, 1993)},
            1993: (
                "REDENOMINATION YEAR: January 1, 1993 Mexico replaced the old peso with the new peso "
                "(1 MXN = 1,000 old pesos). Budget amounts for fiscal 1993 should be in new pesos (MXN) "
                "but verify the document header."
            ),
            **{y: (
                "ANEXO TABLE ERA (2007+): The PEF contains a dedicated ANEXO titled "
                "'PROGRAMA DE CIENCIA Y TECNOLOGÍA' or 'PROGRAMA DE CIENCIA, TECNOLOGÍA E INNOVACIÓN' "
                "(Anexo number varies by year — e.g. Anexo 8 in 2007, Anexo 9 in 2011). "
                "This table is the primary source for R&D line items. "
                "TABLE STRUCTURE: Ramo | Unidad Responsable | Proyecto | AMPLIACIONES | Aprobado | Recursos Propios | Monto Total. "
                "EXTRACT ONLY 'Aprobado' column (approved federal appropriation). "
                "Do NOT use 'Monto Total' — it includes own-source revenues and overstates the budget figure. "
                "Unit is full PESOS (header says '(pesos)') — set unit='unit'. "
                "Example (2011): Ramo 38 CONACYT Aprobado = 17,279,570,709 pesos (~17.3 billion MXN). "
                "Sub-lines under Ramo 38 list individual Centros Públicos de Investigación (CICESE, CIAD, etc.) — "
                "include each named CPI as a separate line_item."
            ) for y in range(2007, 2030)},
            2007: (
                "ANEXO TABLE ERA (2007+): same as above. "
                "NOTE: 2007 Anexo 8 may show only an aggregate total for 'Ciencia y Tecnología' "
                "without a full CPI-level breakdown — extract the aggregate if no sub-lines are present."
            ),
            2022: (
                "CONACYT renamed to CONAHCyT (Consejo Nacional de Humanidades, Ciencias y Tecnologías) "
                "in 2022 (decree published April 2023 but budget already reflects new name from 2023). "
                "Ramo 38 remains the budget code. "
                "ANEXO table structure unchanged — same Aprobado/Monto Total column rules apply."
            ),
        },
    },
    "Israel": {
        "skip_if": [
            "Ministry of Defence (משרד הביטחון, code 15) lines — skip unless 'מחקר' or 'פיתוח' explicitly appears in the sub-line description.",
            "Broad Ministry of Education totals (code 20) without named science/research programme.",
            "National Insurance Institute (ביטוח לאומי) social transfers — not R&D.",
            "Housing, roads, and construction ministry lines without research signal.",
            "Bare 2-digit ministry totals with no sub-code breakdown — these are section aggregates.",
            "Immigrant absorption lines (קליטת עלייה) that are not specifically for scientists.",
        ],
        "include_note": [
            "PRE-1992: No Ministry of Science. Look for code 74 (המועצה הלאומית למחקר ולפיתוח — National Council for R&D) and 'מדען ראשי' sub-lines in the industry/economy ministry.",
            "1992+: Ministry of Science and Technology (משרד המדע והטכנולוגיה, code 19) — always include all sub-codes (02=R&D Council, 03=programmes, 05=infrastructure, 07=Space Agency).",
            "Chief Scientist (מדען ראשי) line items in ANY ministry are R&D — include them.",
            "Israel Science Foundation (קרן מדע ישראל / ISF) — include when named.",
            "2016+: Israel Innovation Authority (רשות החדשנות) replaces Chief Scientist at Ministry of Economy — always include.",
            "KAMEA fund (קרן קמ\"ח) — competitive research fund, include.",
            "Weizmann Institute (מכון ויצמן), Technion, Hebrew University research support lines — include when explicitly named as research grants/support (not teaching subsidies).",
            "Israeli Space Agency (סוכנות החלל הישראלית) under Ministry of Science sub-code 07 — include.",
            "קליטת מדענים עולים (immigrant scientist absorption) — borderline R&D; include if the sub-line is about placing scientists in research positions.",
        ],
        "year_notes": {
            # ── Document/source problems ──────────────────────────────────────
            1993: (
                "Israel 1993 file appears missing from the source set. Expect no usable extraction."
            ),
            2003: (
                "Israel 2003: source notes say this may be the State Comptroller budget rather than the main state budget. "
                "Treat all extracted rows with extreme caution and flag for manual review."
            ),
            2006: (
                "Israel 2006: source notes say the PDF may contain only the first two pages. "
                "Expect very poor extraction yield. Flag for targeted recovery after first compile."
            ),
            2020: (
                "Israel 2020: no normal annual budget was passed due to political crisis. "
                "Extracted values may be from a continuation/emergency budget — not a full annual law. "
                "Use with caution; flag as non-comparable."
            ),
            2021: (
                "Israel 2021: budget passed very late (November). Two-year budget structure (2021–2022) possible. "
                "Check whether amounts are annual or biennial; halve if biennial."
            ),
            # ── Currency era: Israeli Pound / Lira ───────────────────────────
            **{y: (
                f"Israel {y}: LIRA ERA — currency=Israeli Pound/Lira (לירות / ל\"י). Full lira units, no scaling. "
                "Set currency='ILP', unit='unit'. "
                "No dedicated Ministry of Science yet. Look for scattered 'מחקר' lines across ministries "
                "and the National Council for R&D (המועצה הלאומית למחקר) if it appears."
            ) for y in range(1975, 1980)},
            # ── Currency era: Old Shekel ──────────────────────────────────────
            **{y: (
                f"Israel {y}: OLD SHEKEL ERA — currency=Old Shekel (שקל / שקלים). "
                "Amounts in MILLIONS of shekels (מיליוני שקלים). Set currency='ILS_OLD', unit='million'. "
                "National Council for R&D (code 74) and Chief Scientist lines (מדען ראשי) are main R&D signals. "
                "No dedicated Ministry of Science yet."
            ) for y in range(1980, 1986)},
            # ── Currency era: New Shekel, pre-Ministry of Science ─────────────
            **{y: (
                f"Israel {y}: NEW SHEKEL ERA, PRE-SCIENCE MINISTRY — currency=NIS (שקל חדש). "
                "Amounts in THOUSANDS of NIS (אלפי שקלים חדשים). Set currency='ILS', unit='thousand'. "
                "No Ministry of Science yet (created 1992). Look for Chief Scientist (מדען ראשי) in "
                "the Ministry of Industry/Economy, and the National Council for R&D."
            ) for y in range(1986, 1992)},
            # ── Ministry of Science era ───────────────────────────────────────
            **{y: (
                f"Israel {y}: SCIENCE MINISTRY ERA — currency=NIS. "
                "Amounts in THOUSANDS of NIS (אלפי שקלים חדשים). Set currency='ILS', unit='thousand'. "
                "Ministry of Science and Technology (משרד המדע והטכנולוגיה) is code 19. "
                "Sub-codes: 02=R&D Council, 03=programmes, 05=infrastructure, 07=Space Agency. "
                "Chief Scientist (מדען ראשי) lines also appear under Ministry of Economy (code 31)."
            ) for y in range(1992, 2016) if y not in {1993, 2003, 2006}},
            # ── Innovation Authority era ──────────────────────────────────────
            **{y: (
                f"Israel {y}: INNOVATION AUTHORITY ERA — currency=NIS. "
                "Amounts in THOUSANDS of NIS (אלפי שקלים חדשים). Set currency='ILS', unit='thousand'. "
                "Israel Innovation Authority (רשות החדשנות) replaced Chief Scientist at Ministry of Economy in 2016. "
                "Ministry of Science (code 19) still active. ISF (קרן מדע ישראל) still active."
            ) for y in range(2016, 2020) if y not in {2006}},
            **{y: (
                f"Israel {y}: INNOVATION AUTHORITY ERA, FULL NIS — currency=NIS. "
                "Amounts in FULL NIS (שקלים חדשים, no 'אלפי' prefix). Set currency='ILS', unit='unit'. "
                "Ministry of Science (code 19), Israel Innovation Authority (רשות החדשנות) — main R&D vehicles."
            ) for y in range(2020, 2026) if y not in {2020, 2021}},
        },
    },
    "Korea": {
        "skip_if": [
            "Broad macro fiscal totals such as 총지출, 총수입, 재정수지, 국가채무, or economy-wide outlook pages.",
            "PR slogans, infographic captions, and narrative budget-theme pages without a concrete amount-programme pair.",
            "Housing, welfare, employment, or regional-support lines without explicit 연구개발 / R&D signal.",
            "Loan, guarantee, or fund-size announcements unless the text explicitly ties the amount to an R&D programme appropriation.",
            "University support, regional innovation, or talent-training lines without an explicit R&D appropriation signal.",
        ],
        "include_note": [
            "This source family is mostly programme-summary, not institutional appropriation law. Prefer program_total extraction over inventing agencies.",
            "Include only when an explicit won amount is tied to a named R&D theme or programme: 연구개발, 국가연구개발, AI, 반도체, 우주, 바이오, 양자, 과학기술.",
            "과학기술정보통신부 / Ministry of Science and ICT totals may be included only when the line is an annual budget appropriation, not a narrative policy statement.",
            "Keep the unit as stated: 조원, 억원, or 원. Do not rescale unless the document does it explicitly.",
            "Good candidates from the sampled 2024/2025 PDFs include explicit strategic-technology R&D lines such as AI, 첨단바이오, 양자, 우주산업 클러스터, and KARPA-H with 억원 amounts.",
        ],
        "year_notes": {
            **{y: (
                f"Korea {y}: budget proposal summary / brief, not classic line-item appropriations. "
                "If a page only gives macro narrative or slogans, return empty. "
                "Extract only explicit amount + named R&D programme/theme pairs."
            ) for y in range(2018, 2026)},
            2017: "Korea 2017 sampled PDF yielded effectively zero machine-readable text with pdftotext. Treat as image/graphic-heavy; expect empty extraction unless OCR is added.",
            2020: "Korea 2020 sampled PDF yielded effectively zero machine-readable text with pdftotext. Treat as image/graphic-heavy; expect empty extraction unless OCR is added.",
            2021: "Korea 2021 sampled PDF yielded effectively zero machine-readable text with pdftotext. Treat as image/graphic-heavy; expect empty extraction unless OCR is added.",
            2023: "Korea 2023 sampled PDF yielded effectively zero machine-readable text with pdftotext. Treat as image/graphic-heavy; expect empty extraction unless OCR is added.",
        },
    },
    "Colombia": {
        "skip_if": [
            "Ministerio de Defensa / Fuerzas Militares lines — skip unless 'investigación' or 'desarrollo tecnológico' is explicit in the programme name.",
            "Broad SECCIÓN totals (line shows only the 4-digit SECCIÓN code, no programme sub-detail).",
            "ICBF (Instituto Colombiano de Bienestar Familiar) — social protection, not R&D.",
            "Ministerio de Hacienda administrative and debt-service lines.",
            "Legal preamble and narrative text pages — these contain no budget amounts.",
            "INTERSUBSECTORIAL lines without an explicit research/innovation programme name.",
            "General higher-education transfers without a named research programme (include only explicit 'investigación' or 'innovación' sub-projects).",
        ],
        "include_note": [
            "COLCIENCIAS (Departamento Administrativo de Ciencia, Tecnología e Innovación, pre-2019) — entire SECCIÓN is R&D; always include.",
            "MinCiencias (Ministerio de Ciencia, Tecnología e Innovación, from 2019) — entire section is R&D; always include.",
            "Fondo Francisco José de Caldas (competitive grants managed by COLCIENCIAS/MinCiencias) — always include.",
            "SENA PRESUPUESTO DE INVERSIÓN lines containing 'investigación', 'tecnológico', or 'innovación' — include; skip SENA's pure vocational-training lines.",
            "AGROSAVIA / CORPOICA (agricultural research) — always include.",
            "INM (Instituto Nacional de Metrología, SECCIÓN 3505) — include as research infrastructure.",
            "IDEAM (environmental research), IGAC (cartography/geographic research) — include.",
            "Instituto Nacional de Salud (INS) — public health research; include.",
            "University PRESUPUESTO DE INVERSIÓN sub-projects labelled 'investigación' or 'innovación' — include; skip general university transfers.",
            "Amounts are full COP pesos, period=thousands separator. unit='unit', currency='COP'.",
        ],
        "year_notes": {
            **{y: (
                f"Colombia {y}: budget law likely a LEGAL WRAPPER only (narrative law text, annex tables published separately in Diario Oficial). "
                "Expect sparse or zero line-item extraction. This is expected — do not treat as data absence."
            ) for y in range(1995, 2002)},
            **{y: (
                f"Colombia {y}: mixed-quality year — some detail may be present. "
                "unit='unit', currency='COP', full pesos. Look for SECCIÓN-level tables. "
                "COLCIENCIAS is the primary R&D section. SENA investment budget may include R&D programmes."
            ) for y in range(2002, 2012)},
            **{y: (
                f"Colombia {y}: modern era — detailed PRESUPUESTO DE FUNCIONAMIENTO + INVERSIÓN tables expected. "
                "unit='unit', currency='COP', full pesos. "
                "COLCIENCIAS (pre-2019) or MinCiencias (from 2019) is the key SECCIÓN. "
                "SENA (SECCIÓN 3602) investment budget contains R&D programme lines."
            ) for y in range(2012, 2020)},
            2019: (
                "Colombia 2019: MinCiencias (Ministerio de Ciencia, Tecnología e Innovación) created by Ley 1951 de 2019 — "
                "replaces COLCIENCIAS from this budget year onward. Both names may appear in transition documents."
            ),
            **{y: (
                f"Colombia {y}: MinCiencias era. unit='unit', currency='COP', full pesos. "
                "Ministry of Science (MinCiencias) is the primary R&D section. "
                "SENA (SECCIÓN 3602) continues to carry R&D investment sub-programmes."
            ) for y in range(2020, 2026)},
        },
    },
    "Costa Rica": {
        "skip_if": [
            "MOPT (Ministerio de Obras Públicas y Transportes) — roads and infrastructure; skip unless 'investigación' explicit.",
            "Ministerio de Educación Pública — broad education; include only named science/research sub-programmes.",
            "CCSS (Caja Costarricense de Seguro Social) general health services — skip unless a named research institute line (INCIENSA) appears.",
            "Pension funds and public debt service lines.",
            "Broad 'Transferencias a instituciones autónomas' totals without named research content.",
            "FEES (Fondo Especial de Educación Superior) single-line totals — mark as higher_education, not direct R&D (unless a specific research sub-line is named).",
        ],
        "include_note": [
            "CONICIT (Consejo Nacional para Investigaciones Científicas y Tecnológicas) — main competitive research funder; always include.",
            "MICITT (Ministerio de Ciencia, Innovación, Tecnología y Telecomunicaciones) — always include all sub-lines.",
            "Promotora Costarricense de Innovación e Investigación (PCII) — always include.",
            "INTA (Instituto Nacional de Innovación y Transferencia en Tecnología Agropecuaria) — agricultural R&D; include.",
            "INCIENSA (health research) — always include.",
            "CATIE (international agri research centre hosted in Costa Rica) — include if named.",
            "UCR Vínculo Externo / research-labelled sub-lines — include; skip bulk FEES transfer line.",
            "ITCR/TEC research and development lines — include.",
            "MULTI-VOLUME WARNING: For years 2013 (Tomos 2,4,5,6), 2014 (Tomos 1,4,5), 2017 (Tomos 1,2,3) — "
            "each tomo covers different agencies. Compile will aggregate all tomos for the same year.",
            "Amounts are full CRC colones, period=thousands separator. unit='unit', currency='CRC'.",
        ],
        "year_notes": {
            1989: (
                "Costa Rica 1989 (Ley 7111): early document, likely narrative law text with limited annex detail. "
                "May use 'millones de colones' in narrative — read context carefully. "
                "unit='unit' unless document explicitly says otherwise. currency='CRC'. "
                "Expect sparse extraction."
            ),
            **{y: (
                f"Costa Rica {y}: modern era budget. unit='unit', currency='CRC', full colones. "
                "Document says 'en colones corrientes'. "
                "MICITT and CONICIT are key R&D sections. FEES covers university bulk transfers."
            ) for y in range(2010, 2026)},
            2013: (
                "Costa Rica 2013: MULTI-VOLUME year (Tomos 2, 4, 5, 6 present). "
                "Each tomo covers different ministry/sector. R&D may be split across tomos. "
                "unit='unit', currency='CRC'. Do not treat any single tomo as the complete picture."
            ),
            2014: (
                "Costa Rica 2014: MULTI-VOLUME year (Tomos 1, 4, 5 present). "
                "Same as 2013 — aggregate all tomos. unit='unit', currency='CRC'."
            ),
            2017: (
                "Costa Rica 2017: MULTI-VOLUME year (Tomos 1, 2, 3 present). "
                "Aggregate all tomos. unit='unit', currency='CRC'."
            ),
        },
    },
    "Italy": {
        "skip_if": [
            "FFO bulk transfer line without specific R&D label (Fondo di Finanziamento Ordinario for universities covers teaching; exclude unless the row explicitly names a research component)",
            "Debt service lines: interessi sul debito pubblico, rimborso titoli, ammortamento debito — always non-R&D",
            "Social protection and pension transfers: pensioni, TFR, assegni familiari, previdenza — non-R&D",
            "Transport infrastructure without research signal: strade, autostrade, ferrovie, porti, aeroporti — non-R&D",
            "Defence procurement without research signal: programmi navali/aerei, materiale militare — non-R&D unless labelled ricerca difesa",
            "Macro budget totals: totale entrate, totale spese, saldo, avanzo/disavanzo — always section aggregates",
            "Regional/municipal transfers without research label: Fondo perequativo, trasferimenti a regioni/comuni — non-R&D",
            "Pagina bianca / empty image pages from pre-1997 scanned Gazzetta Ufficiale — skip entirely",
        ],
        "include_note": [
            "Missione 17 (Ricerca e innovazione) programme lines under any ministry — include if named R&D programme",
            "FOE — Fondo Ordinario per gli Enti di ricerca: main block grant to CNR, ENEA, ASI, INFN, INAF etc. — include",
            "FIRST — Fondo per gli Investimenti nella Ricerca Scientifica e Tecnologica — include (key R&D fund since 2007)",
            "PRIN — Progetti di Rilevante Interesse Nazionale — include (university competitive R&D grants)",
            "Ricerca scientifica e tecnologica di base e applicata (programme 17.22 under MUR/MIUR) — include",
            "CNR, ENEA, ASI, INFN, INAF, INGV, OGS named appropriations — include",
            "Fondo Agevolazioni alla Ricerca (FAR) — include (industrial research subsidies)",
            "Ricerca per il settore della sanità pubblica under Ministry of Health (Missione 17) — include",
            "Assegni di ricerca, borse di dottorato — include as human capital component of R&D",
            "COMPANION FILE NOTE: for each year, one file is the main legge di bilancio (legal text); "
            "the companion file contains the tabular Stato di previsione per each ministry — prefer the companion for amounts",
        ],
        "year_notes": {
            **{y: (
                f"Italy {y}: PRE-DIGITAL ERA. "
                "Gazzetta Ufficiale Supplemento ordinario; likely SCANNED image PDF with zero or garbled machine-readable text. "
                "OCR output will be empty or have one-character-per-line column artefacts. "
                "Flag as image-heavy; amounts in milioni di lire (ITL). "
                "MURST (Ministero dell'Università e della Ricerca Scientifica e Tecnologica) is the key ministry. "
                "If any text is present look for: Capitolo 1678 (CNR), ricerca scientifica, università, enti di ricerca."
            ) for y in range(1986, 1998)},
            **{y: (
                f"Italy {y}: TRANSITION ERA — Gazzetta Ufficiale now partially machine-readable but often has "
                "vertical-column OCR artefacts (text printed sideways in tables). "
                "Currency: ITL (lire). Amounts in milioni di lire at programme level. "
                "Ministry: MIUR (Ministero dell'Istruzione, dell'Università e della Ricerca) from 1999. "
                "Structure: Unità previsionale di base (UPB) codes — look for UPB 4.2.x.x for R&D. "
                "KEY FUNDS: Fondo per la Ricerca di Base (FIRB), Fondo per le Agevolazioni alla Ricerca (FAR), "
                "Programmi di Ricerca di Interesse Nazionale (PRIN), CNR block grant."
            ) for y in range(1998, 2003)},
            2002: (
                "Italy 2002: first EUR budget year (Italy adopted EUR Jan 2002). "
                "Currency switches from ITL to EUR mid-series. "
                "Amounts now in MIGLIAIA DI EURO (thousands of euros) in the tabular sections. "
                "Both ITL and EUR conversions may appear in transition notes. "
                "Structure: UPB codes still in use but reform underway. MIUR is key ministry."
            ),
            **{y: (
                f"Italy {y}: EUR era, amounts in MIGLIAIA DI EURO (thousands of euros). "
                "Tables are labelled '(MIGLIAIA DI EURO)' — multiply extracted figures by 1,000 for full euros. "
                "TWO-FILE STRUCTURE: main SO_NNN file = legal text; companion SO_NNN+1 or SO_NNN-1 = detailed tables. "
                "MIUR is the key R&D ministry. Structure: Unità previsionali di base (pre-2009), then "
                "Missione/Programma/Capitolo from 2010 onwards. "
                "KEY ITEMS: FOE (enti di ricerca block grant), FIRB/FIRST, PRIN, Fondo Agevolazioni Ricerca."
            ) for y in range(2003, 2017)},
            2010: (
                "Italy 2010: first year of 'missioni e programmi' budget structure reform (D.Lgs. 90/2007). "
                "New structure: Missione 17 = Ricerca e innovazione, Missione 23 = Istruzione universitaria. "
                "Amounts in MIGLIAIA DI EURO. Companion SO_NNN+1 file has ALLEGATO A tables. "
                "MIUR split: Ministero dell'Università e della Ricerca (MUR-like) is still MIUR at this stage."
            ),
            **{y: (
                f"Italy {y}: modern era. Amounts in full EURO (euro interi) — BILANCIO PER AZIONI format. "
                "Structure firmly: Ministry → Missione → Programma → Azione. "
                "Missione 17 (Ricerca e innovazione) is the key mission across MUR and other ministries. "
                "MUR (from 2020) / MIUR (until 2020) has stato di previsione with FOE, FIRST, PRIN, PhD grants. "
                "COMPANION FILE: the -1 or second SO file contains multi-year BILANCIO PER AZIONI tables in full euros. "
                "MIMIT (from 2022) / MISE handles industrial R&D (Fondo per l'Innovazione). "
                "unit='unit', currency='EUR'."
            ) for y in range(2017, 2026)},
            2020: (
                "Italy 2020: MIUR renamed to MUR (Ministero dell'Università e della Ricerca) + "
                "Ministero dell'Istruzione (MI) as separate ministries. "
                "MUR takes R&D; MI takes schools. "
                "Amounts in full EUR. Missione 17 under MUR is the primary R&D appropriation. "
                "unit='unit', currency='EUR'."
            ),
        },
    },
    "Slovenia": {
        "skip_if": [
            "Servisiranje javnega dolga — debt service, always non-R&D",
            "Pokojnine, socialna varnost, nadomestila — pension and social protection transfers, non-R&D",
            "Obramba / vojska lines without research signal — defence without R&D label",
            "Ceste, avtoceste, železnice, pristanišča — transport infrastructure without R&D signal",
            "Vrtci, osnovna šola — kindergartens and primary school lines — non-R&D education",
            "Macro totals: skupaj prihodki, skupaj odhodki, račun financiranja — always section aggregates",
            "ZIPRS execution law pages (Zakon o izvrševanju proračuna) — legal wrapper text without appropriation tables",
            "Standalone 'YYYY.pdf' files that contain other laws (e.g. Zakon o RTV, ZIPRS amendments) — skip",
        ],
        "include_note": [
            "Programme code 0502 (Znanstveno raziskovalna dejavnost) — PRIMARY R&D CODE, always include named lines",
            "Programme code 0501 (Urejanje sistema na področju znanosti) — governance/admin of science system — include if labelled research",
            "Programme code 0503 (Mladi raziskovalci, mobilnost, spodbude) — researcher training — include",
            "Programme code 0504 (Podpora tehnološkim razvojnim projektom / Tehnološki razvoj) — tech development — include",
            "ARRS — Agencija za raziskovalno dejavnost RS: all ARRS appropriations are R&D — include",
            "SAZU (Slovenska akademija znanosti in umetnosti, code 3911) — academy R&D appropriations — include",
            "Sub-programme 050201 (Raziskovalni programi in projekti) — research programmes — always include",
            "Sub-programme 050204 (Podpora raziskovalni infrastrukturi) — research infrastructure — include",
            "Ciljni raziskovalni projekti (targeted research projects) — include",
            "RS_ companion files (P001/P002/P003): these contain project-level detail; scan for named research projects",
        ],
        "year_notes": {
            **{y: (
                f"Slovenia {y}: EARLY ERA — likely scanned image PDF (Uradni list RS). "
                "Expect zero or minimal machine-readable text. "
                "Currency: SIT (Slovenian tolar). Amounts in 000 tolarjih (thousands of tolars). "
                "Budget structure is early post-independence format. "
                "R&D agency: Javna agencija za tehnološki razvoj RS (pre-ARRS). "
                "If text present: look for 'Ministrstvo za šolstvo in šport' or 'Ministrstvo za znanost in tehnologijo'."
            ) for y in range(1991, 1996)},
            **{y: (
                f"Slovenia {y}: SIT era. Amounts in tisoč tolarjih (thousands of SIT). "
                "unit='thousand', currency='SIT'. "
                "Main file is u{y}XXX.pdf from Uradni list. "
                "Budget structure: ministry → policy area (05=science) → programme code. "
                "R&D ministry: varies — often Ministrstvo za šolstvo in šport or Ministrstvo za ekonomske zadeve. "
                "Programme 0502 is the key R&D code."
            ) for y in range(1996, 2004)},
            **{y: (
                f"Slovenia {y}: SIT era (ARRS created 2004). Amounts in tisoč tolarjih (thousands of SIT). "
                "unit='thousand', currency='SIT'. "
                "ARRS (code 3211 sub-agency) handles research programme/project funding. "
                "Ministrstvo za visoko šolstvo, znanost in tehnologijo (MVZT, code 3211) is key ministry. "
                "RS_ companion files available for some years — contain project-level detail. "
                "Programme 0502 (Znanstveno raziskovalna dejavnost) is the primary R&D code."
            ) for y in range(2004, 2007)},
            2007: (
                "Slovenia 2007: CURRENCY CHANGE year — EUR replaces SIT (1 EUR = 239.64 SIT). "
                "Budget switched to EUR from 1 January 2007. "
                "unit='unit', currency='EUR', full euros. "
                "MVZT (Ministrstvo za visoko šolstvo, znanost in tehnologijo, code 3211) is key ministry. "
                "ARRS is now established and administers research funding under programme 0502."
            ),
            **{y: (
                f"Slovenia {y}: EUR era. unit='unit', currency='EUR', full euros ('v EUR'). "
                "MVZT (code 3211) → renamed to MIZŠ (Ministrstvo za izobraževanje, znanost in šport, code 3330) in 2012. "
                "ARRS administers research programmes. Programme 0502 is the primary R&D appropriation. "
                "RS_ companion files available for some years. "
                "Some years are biennial budgets (2014+2015, 2016+2017, 2018+2019) — one doc covers two fiscal years."
            ) for y in range(2008, 2026)},
            2012: (
                "Slovenia 2012: ministry reorganisation — MVZT (3211) renamed/merged into MIZŠ "
                "(Ministrstvo za izobraževanje, znanost in šport, code 3330). "
                "unit='unit', currency='EUR'. Programme 0502 remains the key R&D code under MIZŠ."
            ),
            2014: (
                "Slovenia 2014+2015: biennial budget document (ZIPRS1415). "
                "One law covers both fiscal years. Extract figures for each year separately. "
                "unit='unit', currency='EUR'. MIZŠ (code 3330) is key ministry."
            ),
            2018: (
                "Slovenia 2018+2019: biennial budget document. "
                "One law covers both fiscal years. Extract figures for each year separately. "
                "unit='unit', currency='EUR'. MIZŠ (code 3330) is key ministry."
            ),
        },
    },

    # -----------------------------------------------------------------------
    # SLOVAKIA
    # Document type: zákon o štátnom rozpočte (Zbierka zákonov SR), Slovak.
    # Unit: tis. Sk (thousands SKK) pre-2009; full EUR 2009+.
    # Key R&D: kapitola 20 (MŠ SR / science ministry), kapitola 51 (SAV).
    # -----------------------------------------------------------------------
    "Slovakia": {
        "skip_if": [
            # Debt service — clearly non-R&D
            "Lines for 'dlhová služba', 'obsluha štátneho dlhu', 'splátky dlhu', "
            "'záväzky z dlhu', 'štátny dlh' — public debt repayment. SKIP always.",

            # Social transfers
            "'sociálne dávky', 'sociálne poistenie', 'Sociálna poisťovňa', "
            "'nemocenské', 'dôchodky', 'starobné dôchodky', 'invalidné dôchodky', "
            "'materské', 'rodinné prídavky' — social insurance and transfers. SKIP always.",

            # Defence without research label
            "Ministerstvo obrany SR (kapitola 21) lines without 'výskum' or 'vývoj': "
            "procurement (obstarávanie), military operations, armament. SKIP unless "
            "R&D is explicitly mentioned.",

            # Transport infrastructure
            "'cestná infraštruktúra', 'diaľnice', 'železnice', 'NDS' (Národná diaľničná spoločnosť), "
            "'ŽSR' (Železnice Slovenskej republiky) — transport infrastructure. "
            "SKIP unless 'výskum' or 'vývoj' is present.",

            # Broad ministry totals without R&D signal
            "Section totals for ministries other than kapitola 20 (MŠ SR) and kapitola 51 (SAV): "
            "broad Kapitola totals for Interior (23), Justice (22), Transport (31), Finance (09) — "
            "do not extract unless a specific R&D programme or agency is named.",

            # Education transfers without research signal (primary/secondary school)
            "Lines for 'základné školstvo' (primary schools), 'stredné školstvo' (secondary schools), "
            "'materské školy' (kindergartens), 'špeciálne školy' without 'výskum' — "
            "non-HE education, not R&D. SKIP.",

            # YEAR 1990 mislabelled file
            "The file '1990 text.pdf' in the Slovakia folder is ACTUALLY A POLISH BUDGET "
            "(Dziennik Ustaw Rzeczypospolitej Polskiej). Do NOT extract it as Slovak budget. "
            "Skip this file entirely for Slovakia.",
        ],
        "include_note": [
            # Primary R&D agencies
            "APVV (Agentúra na podporu výskumu a vývoja) — main competitive R&D grant agency "
            "from 2005. Appears as a named line under Ministerstvo školstva SR (kapitola 20). "
            "Include its annual appropriation.",

            "VEGA (Vedecká grantová agentúra MŠ SR a SAV) — grant scheme for basic research "
            "at universities and SAV institutes. Include.",

            "SAV (Slovenská akadémia vied, kapitola 51) — full appropriation to the Slovak "
            "Academy of Sciences for basic research. Include the entire SAV kapitola. "
            "EUR era: ~130-140M EUR/year; SKK era: ~1-2B tis. Sk.",

            # Universities under MŠ SR
            "University sub-items under Ministerstvo školstva SR (kapitola 20): "
            "look for named universities (Comenius, STU, TUKE, Žilinská, etc.) and "
            "their subvencje or dotácie lines. Tag as higher_education. "
            "Note: university block grants cover both teaching AND research — "
            "include but flag as higher_education for post-processing.",

            # R&D division codes
            "Budget oblasť (sector) 740 = veda a výskum (science and research) — "
            "all lines under this sector code are direct R&D appropriations. Include.",

            "CERN participation ('príspevok do CERN') — Slovak contribution to CERN. "
            "Include as international R&D cooperation.",

            "ESA contributions and other international science organization memberships "
            "— include as rd_adjacent or direct_rd.",

            # Research institutes under MŠ SR
            "Named research institutes under MŠ SR: 'Slovenský ústav technickej normalizácie', "
            "'Ústav súdneho inžinierstva', state-funded R&D institutes — include when explicitly named.",

            # Pre-APVV era
            "Pre-2005: 'Agentúra pre vedecký výskum' (predecessor to APVV) and direct "
            "'veda a výskum' lines under MŠ SR. Include.",
        ],
        "year_notes": {
            **{y: (
                "Slovakia 1992-2008 (SKK era): amounts in THOUSANDS of Slovak koruna (tis. Sk). "
                "unit='thousand', currency='SKK'. "
                "Budget in Zbierka zákonov SR. "
                "KEY R&D: kapitola 20 (Ministerstvo školstva SR) — search for 'veda a výskum', "
                "'výskum a vývoj', 'aplikovaný výskum na vysokých školách', VEGA grants, "
                "CERN/ESA contributions. Kapitola 51 = SAV. "
                "Pre-2005: no APVV yet; look for 'Agentúra pre vedecký výskum' or direct R&D lines. "
                "Numbers are plausible at millions of tis. Sk for SAV and major R&D programmes."
            ) for y in range(1992, 2009)},

            2009: (
                "Slovakia 2009: TRANSITION YEAR — eurozone accession 1 January 2009. "
                "Budget law approved late 2008 for fiscal year 2009, expressed in EUR. "
                "unit='unit', currency='EUR'. Full euros, not thousands. "
                "Conversion rate: 30.1260 SKK = 1 EUR. "
                "SAV and MŠ SR remain the key R&D kapitoly."
            ),

            **{y: (
                "Slovakia EUR era: unit='unit', currency='EUR'. Full euros — NOT thousands. "
                "KEY R&D: Ministerstvo školstva SR (kapitola 20) — APVV, universities, "
                "research institutes, VEGA. SAV (kapitola 51) — Slovak Academy of Sciences. "
                "From 2013: ministry renamed to 'Ministerstvo školstva, vedy, výskumu a športu SR'. "
                "From 2023: renamed again to 'Ministerstvo školstva, výskumu, vývoja a mládeže SR'. "
                "APVV annual budget ~90-120M EUR in recent years; SAV ~130-140M EUR."
            ) for y in range(2010, 2026)},

            1990: (
                "IMPORTANT: The file '1990 text.pdf' in the Slovakia folder is ACTUALLY A POLISH "
                "BUDGET (Dziennik Ustaw Rzeczypospolitej Polskiej). DO NOT extract this as Slovakia. "
                "Skip entirely."
            ),
        },
    },

    # -----------------------------------------------------------------------
    # POLAND
    # Document type: Ustawa Budżetowa (Dziennik Ustaw RP), Polish.
    "Portugal": {
        "skip_if": [
            "Polícia Judiciária, PGR (Procuradoria-Geral da República), PSP, GNR, SEF lines: "
            "'investigação' here means criminal investigation — NOT scientific R&D.",
            "Segurança social / transferências para a Segurança Social — social insurance; not R&D.",
            "Serviço da dívida pública / encargos com a dívida — debt service; always skip.",
            "Forças Armadas / Defesa Nacional without explicit 'investigação científica' label.",
            "Infra-estruturas rodoviárias e ferroviárias (EP, IP, REFER, Infraestruturas de Portugal) — transport.",
            "Ensino básico e secundário broad totals without named R&D component.",
            "Transferências correntes para Segurança Social, ADSE (public servants health) — non-R&D.",
            "DUPLICATE FILES: 'Lei orcamento para 1985.pdf' and 'Lei orcamento para 1986.pdf' are IDENTICAL — "
            "skip year 1985 file entirely (treat both as year 1986). "
            "Also '1997 02040557.pdf' and 'Lei orcamento para 1997.pdf' are identical — skip the code version.",
        ],
        "include_note": [
            "FCT (Fundação para a Ciência e Tecnologia) — include named institutional annual budget rows "
            "for FCT, especially in MAPA V / MAPA VII services-and-funds tables or other annual expenditure tables. "
            "Do NOT treat transfer authorisations, pass-through articles, or plurianual project schedules as the FCT institutional total.",
            "JNICT (pre-1997) — include all JNICT operational and programme lines.",
            "INIC (pre-1990s) — include all INIC lines.",
            "P002 — Programa Investigação Científica e Tecnológica e Inovação: include only when it is an annual budget table total. "
            "If it appears in plurianual/project-schedule format, keep for audit but do not treat it as the main institutional appropriation.",
            "Laboratórios de Estado with research mandate: LNEC, LNETI, INETI, INIAV, INRB — include.",
            "ANI (Agência Nacional de Inovação, from 2009) — include named institutional budget rows only; "
            "exclude legal transfer clauses and guarantee-fund pass-through mentions.",
            "Universidades públicas: include only when line explicitly labelled 'investigação', "
            "'centros de investigação', 'I&D', or 'bolsas de doutoramento' — not broad teaching subsidies.",
            "COMPETE / POCI / POSC EU co-financed R&D programmes under science ministry — include.",
            "Ministério da Ciência (various names) Capítulo 50 total — include as section aggregate.",
            "Named bilateral contributions: CERN, ESA — include.",
        ],
        "year_notes": {
            **{y: (
                "SCANNED FILE ERA (1977-2000): files are scanned image PDFs — OCR with por+eng required. "
                "Currency: PTE (escudo). 1 EUR = 200.482 PTE (conversion fixed Jan 1999). "
                "Unit: contos (1 conto = 1,000 escudos) or full escudos — check table header."
            ) for y in range(1977, 2001)},
            1985: (
                "DUPLICATE FILE: 'Lei orcamento para 1985.pdf' is IDENTICAL to the 1986 file "
                "(same byte count). The 1985 budget was the Lei 66-B/84 covering both years. "
                "Skip this file (year=1985) and use the 1986 file."
            ),
            1997: (
                "TWO IDENTICAL FILES: '1997 02040557.pdf' and 'Lei orcamento para 1997.pdf' "
                "have the same byte count — exact duplicates. Process only one."
            ),
            2002: (
                "CURRENCY TRANSITION: Portugal adopted the euro on 1 January 2002. "
                "Budget amounts for fiscal 2002 are in full EUR."
            ),
            **{y: (
                "EUR ERA (2003+): amounts in full euros, unit='unit'. "
                "FCT (Fundação para a Ciência e a Tecnologia) is the primary R&D funding agency — "
                "look for 'FCT', 'Fundação para a Ciência', 'ciência e tecnologia' appropriation lines. "
                "Budget structure: Ministério da Ciência, Tecnologia e Ensino Superior (or variant name). "
                "Text files have a direct PDF layer — clean extraction, no OCR issues."
            ) for y in range(2003, 2026)},
            **{y: (
                "EUR ERA — TROIKA AUSTERITY (2011-2014): FCT budget was cut sharply under the "
                "EU/IMF adjustment programme. Amounts significantly lower than 2009-2010 baseline. "
                "This is correct data — not a scanning error. amounts still in full EUR, unit='unit'."
            ) for y in range(2011, 2015)},
            2013: (
                "EUR ERA — TROIKA AUSTERITY: FCT budget at trough. "
                "Also: Portugal 2013 Lei do Orçamento was a supplemental/revised budget — "
                "check whether file is the original or the revised law."
            ),
        },
    },
    # -----------------------------------------------------------------------
    # TURKEY
    # Key document types: Bütçe Kanunu / Merkezi Yönetim Bütçe Kanunu
    # Currency: TRL (old lira, hyperinflation) until 2004; YTL 2005-2008; TL 2009+
    # 1 YTL = 1,000,000 old TRL (redenomination 1 January 2005)
    # Key R&D: TÜBİTAK, TÜBA, TAEK (all Özel Bütçeli — (II) SAYILI CETVEL only)
    #          Sanayi ve Teknoloji Bakanlığı (Genel Bütçeli — (I) SAYILI CETVEL)
    # CRITICAL: 2010-2025 '2-a' files contain ONLY (I) SAYILI CETVEL — TÜBİTAK ABSENT
    # -----------------------------------------------------------------------
    "Turkey": {
        "skip_if": [
            # Public debt service — always non-R&D; very large amounts
            "'Kamu Borç Yönetimi', 'Borç Servisine İlişkin', 'Hazine Müsteşarlığı borç', "
            "'faiz giderleri' (interest on debt), 'borçlanma' (borrowing): debt service. SKIP.",

            # Social security and pensions — large, clearly non-R&D
            "Sosyal Güvenlik Kurumu (SGK), emeklilik (pensions), Bağ-Kur, SSK, "
            "'sosyal güvenlik transferleri': social insurance transfers. SKIP always.",

            # General defence without Ar-Ge label
            "Millî Savunma Bakanlığı (MSB) general procurement and operations without "
            "explicit 'Ar-Ge', 'araştırma', or 'geliştirme' in the line description. SKIP.",

            # Criminal/police 'araştırma' — interior ministry uses araştırma for investigation
            "İçişleri Bakanlığı, Emniyet Genel Müdürlüğü, Jandarma Genel Komutanlığı, "
            "Cumhuriyet Başsavcılığı — 'araştırma' here means criminal investigation. SKIP.",

            # Transport infrastructure without research label
            "'Karayolları Genel Müdürlüğü', 'Devlet Demiryolları' (TCDD), "
            "'Ulaştırma Bakanlığı' road/rail infrastructure lines without 'Ar-Ge' label. SKIP.",

            # Broad primary and secondary education totals
            "'İlköğretim' (primary education), 'Ortaöğretim' (secondary education), "
            "'MEB genel' totals without named research component. SKIP.",

            # Macro/aggregate totals without named R&D entity
            "'Genel Toplam', 'Toplam Ödenek' budget-wide summary rows (not agency-specific). SKIP.",

            # Duplicate files — identical content
            "1990 and 1991 files are byte-for-byte identical (one is mislabelled). "
            "If processing both years, extract ONLY the 1991 file; mark 1990 as duplicate.",

            # 2001/2002 duplicate copies
            "2001 UUID-named scanned file and '2001 ... (1).pdf' copy: process only the "
            "'Kanun' text-layer file. Similarly for 2002 '(1)' copy.",
        ],
        "include_note": [
            # TÜBİTAK — primary R&D funder; ÖZEL BÜTÇELİ
            "TÜBİTAK (Türkiye Bilimsel ve Teknolojik Araştırma Kurumu): include the full annual "
            "appropriation. Appears in (II) SAYILI CETVEL (Özel Bütçeli). "
            "NOT present in files containing only (I) SAYILI CETVEL.",

            # TÜBA
            "TÜBA (Türkiye Bilimler Akademisi — Turkish Academy of Sciences): include appropriation. "
            "Özel Bütçeli entity; appears in (II) SAYILI CETVEL.",

            # TAEK — nuclear R&D
            "TAEK (Türkiye Atom Enerjisi Kurumu — Atomic Energy Authority): include appropriation. "
            "Özel Bütçeli entity; appears in (II) SAYILI CETVEL. Tag as direct_rd.",

            # Ministry of Industry and Technology
            "Sanayi ve Teknoloji Bakanlığı (and earlier names: Bilim, Sanayi ve Teknoloji Bakanlığı, "
            "Sanayi ve Ticaret Bakanlığı): Genel Bütçeli — present in (I) SAYILI CETVEL. "
            "Include lines explicitly labelled 'Ar-Ge', 'araştırma-geliştirme', or 'bilimsel araştırma'.",

            # KOSGEB
            "KOSGEB (Küçük ve Orta Ölçekli İşletmeleri Geliştirme): Özel Bütçeli. "
            "Include lines explicitly labelled Ar-Ge or teknoloji. Skip general SME support.",

            # Universities
            "Yükseköğretim Kurumları (universities) that appear in (II) SAYILI CETVEL: "
            "include lines specifically labelled 'bilimsel araştırma projeleri (BAP)', "
            "'araştırma fonu', or 'Ar-Ge'. Do NOT include broad 'öğretim' (teaching) totals.",

            # Turkish Space Agency
            "Türkiye Uzay Ajansı (TUA, from 2018): include full appropriation as science_agency.",

            # Bilim Araştırma Projeleri — university BAP scheme
            "'Bilimsel Araştırma Projeleri (BAP)': competitive research grant scheme within "
            "universities. Include when appearing as a named line.",

            # Pre-2005 hyperinflation note
            "Pre-2005 TRL amounts may appear as 10-13 digit numbers (trillions of old lira). "
            "This is correct — do not assume these are errors or totals.",

            # Ar-Ge in defence context
            "Lines in MSB or SSB explicitly labelled 'Ar-Ge', 'savunma araştırma', "
            "'savunma teknolojileri geliştirme': include as direct_rd (defence R&D).",
        ],
        "year_notes": {
            **{y: (
                "TRL ERA (1975-2004): amounts in full old Turkish lira (TRL), unit='unit'. "
                "TÜBİTAK appears as a transfer payment from the General Budget: "
                "'Bağımsız bütçeli idarelere yapılacak yardımlar (TÜBİTAK'a ödenecektir)'. "
                "Large budget documents (400-1100+ pages) contain the full Bütçe Cetvelleri. "
                "OCR quality varies — spaced/garbled letters in some scanned pages is expected."
            ) for y in range(1975, 1990)},
            1990: (
                "DUPLICATE YEAR: 1990 budget file is byte-for-byte identical to 1991. "
                "Treat data extracted from this file as 1991 only. Mark year=1990 decision=exclude."
            ),
            **{y: (
                "TRL ERA: amounts in full old Turkish lira (TRL), unit='unit'. "
                "TÜBİTAK appears as a transfer in the general budget Cetvelleri. "
                "Hyperinflation: by late 1990s, TÜBİTAK budget in tens of trillions TRL."
            ) for y in range(1991, 2001)},
            2001: (
                "TWO 2001 FILES: a UUID-named scanned PDF (garbled OCR) and a 'Kanun' text-layer file. "
                "Use the Kanun text file. Amounts in old TRL — hyperinflation era."
            ),
            2002: (
                "TWO 2002 FILES: an original and a '(1)' duplicate copy. Process only one. "
                "Hyperinflation era — amounts in old TRL."
            ),
            **{y: (
                "TRL ERA (late hyperinflation): amounts in old TRL, unit='unit'. "
                "Multiple document types in collection — prioritise UUID-named or 'f8229ba6'-style files "
                "which are the full Merkezi Yönetim Bütçe Kanunu with annexes."
            ) for y in range(2003, 2005)},
            2004: (
                "LAST YEAR of old Turkish lira (TRL) era. "
                "1 YTL = 1,000,000 TRL redenomination took effect 1 January 2005. "
                "Multiple files in collection: use the large UUID-named file for full appropriation tables."
            ),
            2005: (
                "REDENOMINATION YEAR: Turkey replaced old TRL with Yeni Türk Lirası (YTL) "
                "on 1 January 2005. 1 YTL = 1,000,000 old TRL. Budget amounts in YTL. "
                "TÜBİTAK budget dropped from trillions TRL to single-digit billions YTL. "
                "NOTE: 'tbmm22140033ss1270.pdf' and 'ss1271.pdf' are KESIN HESAP (final accounts) "
                "for prior years — not the FY2005 budget appropriation."
            ),
            **{y: (
                "YTL ERA (2006-2008): amounts in Yeni Türk Lirası (YTL), unit='unit'. "
                "DOCUMENT TYPES in collection: (a) 'kanuntbmmc09XXXXX.pdf' = budget law articles only "
                "(no appropriation tables); (b) 'GenelFaaliyetRaporu_XXXX.pdf' = post-facto activity "
                "report (NOT a budget document); (c) 'tbmm22103031ss1028.pdf' (2006) = TBMM budget "
                "committee report with policy discussion; (d) '2008-Merkezi-Yonetim-Kesin-Hesabi' = "
                "final accounts (actual spending, not appropriations). "
                "PRIORITISE: the large UUID-named PDF or 'ButceGerekcesi' document for actual appropriations."
            ) for y in range(2006, 2009)},
            2009: (
                "CURRENCY RENAME: 'Yeni' dropped; YTL became TL from 1 January 2009. Same currency. "
                "Multiple files: 'ButceGerekcesi_2009.pdf' (314p) = budget justification with detailed "
                "programme data; 'kanuntbmmc09305828.pdf' = law articles only."
            ),
            **{y: (
                "CRITICAL — NO TÜBİTAK DATA: The only file for this year is a 3-page "
                "'Ekonomik Kod İcmali' (Economic Code Summary) of the GENERAL BUDGET (I) SAYILI CETVEL. "
                "This shows aggregate spending by economic category for general-budget institutions only. "
                "TÜBİTAK, TÜBA, and TAEK are ÖZEL BÜTÇELİ (Special Budget, II SAYILI CETVEL) and "
                "DO NOT APPEAR in this file. Mark ALL pages relevant=false. "
                "The user needs to upload the (II) SAYILI CETVEL / Özel Bütçeli Idareler file "
                "to extract TÜBİTAK data for this year."
            ) for y in range(2010, 2026)},
        },
    },
    # Unit: tys. zł (thousands PLN) throughout all years.
    # 1995 redenomination: 1 new PLN = 10,000 old PLN.
    # Key R&D: Część 28 (Szkolnictwo wyższe i nauka), Część 67 (PAN).
    # -----------------------------------------------------------------------
    "Poland": {
        "skip_if": [
            # Social insurance (ZUS, KRUS) — very large, clearly non-R&D
            "Część 73 — ZUS (Zakład Ubezpieczeń Społecznych), Część 74 — KRUS (Kasa Rolniczego "
            "Ubezpieczenia Społecznego): social security contributions. SKIP always. "
            "These are among the largest items in the budget and are purely social transfers.",

            # Public debt service
            "'Obsługa długu publicznego', 'dług publiczny', 'obsługa zobowiązań Skarbu Państwa': "
            "public debt repayment and interest. SKIP always.",

            # Defence
            "Część 29 — Obrona Narodowa (Ministry of National Defence): operational defence "
            "and procurement lines. SKIP unless 'badania', 'badawczo-rozwojowe', 'B+R' "
            "is explicitly in the line description.",

            # Transport/infrastructure
            "Część 39 — Transport (roads, railways): 'infrastruktura drogowa', 'PKP', "
            "'GDDKiA' — transport infrastructure. SKIP unless research is explicitly named.",

            # General reserves
            "'Rezerwa ogólna', 'rezerwy celowe' without a specific R&D programme name — "
            "unallocated reserves. SKIP.",

            # Primary and secondary education without research label
            "Lines for 'oświata i wychowanie' (primary/secondary education), "
            "'szkolnictwo podstawowe', 'szkolnictwo ponadpodstawowe', "
            "'przedszkola' (kindergartens) — NOT R&D. SKIP unless 'badania' is present.",

            # Macro-totals
            "Broad totals like 'Ogółem wydatki', 'Razem' for non-R&D ministry chapters — "
            "only extract totals when they correspond to a named R&D programme or agency.",
        ],
        "include_note": [
            # Primary R&D funding agencies
            "NCN (Narodowe Centrum Nauki, created 2011) — basic research grants. "
            "Appears in Część 28, Dział 740. Include full annual appropriation.",

            "NCBiR / NCBR (Narodowe Centrum Badań i Rozwoju, created 2007) — applied R&D "
            "and industrial innovation. Appears in Część 28 (or sometimes Część 73 for "
            "EU-co-financed programmes). Include core state budget appropriation.",

            "PAN (Polska Akademia Nauk, Część 67) — Polish Academy of Sciences. "
            "This has its own budget part. Include the full PAN appropriation. "
            "Tag as science_agency.",

            # Pre-2007/2011 agencies
            "KBN (Komitet Badań Naukowych, 1991-2005) — the pre-MNiSW research funding committee. "
            "Appears as named lines in early budgets (1990-2005). Include as direct_rd.",

            # Universities under Part 28
            "University block grants (subwencje dla uczelni) under Część 28, Dział 730 "
            "(Szkolnictwo wyższe): these are block grants covering both teaching and research. "
            "Include individual university lines and tag as higher_education. "
            "Key universities: UW, PW, AGH, UJ, PG, PK, PWr, PŁ, PP, KUL, SGH.",

            # Ministry own R&D
            "MNISW / MEiN own R&D programmes: 'Doskonała nauka', 'Regionalna Inicjatywa Doskonałości', "
            "'SPUB' (subsydia na utrzymanie unikalnego sprzętu), "
            "international cooperation grants — include.",

            # International contributions
            "CERN contribution ('składka do CERN') — annual Polish contribution. Include. "
            "ESA, COST, ITER, and other international S&T organisation memberships — include.",

            # R&D division code
            "Dział 740 — Działalność badawcza i rozwojowa (R&D activity): ALL lines in "
            "this division are direct R&D appropriations. Include everything under Dział 740.",

            # Health R&D
            "NIH-type research: Część 46 — Zdrowie (Health ministry) when "
            "'badania' or 'instytut badawczy' or 'PAN' is explicitly in description. "
            "Include named medical research institutes (np. Centrum Onkologii, "
            "Instytut Hematologii, etc.).",
        ],
        "year_notes": {
            **{y: (
                "Poland 1990-1994: PRE-REDENOMINATION era (old złoty / stary złoty). "
                "currency='PLN' (stary złoty). "
                "Read the page header carefully: many annex pages are explicitly "
                "'w milionach złotych', so those rows must use unit='million'; "
                "if a page says 'w tys. zł' then use unit='thousand'. "
                "1 new PLN (after 1995) = 10,000 old PLN (before 1995). "
                "Documents are Ustawa Budżetowa in Dziennik Ustaw RP. "
                "R&D structure: look for KBN (Komitet Badań Naukowych) appropriations, "
                "PAN (Polska Akademia Nauk), university research grants. "
                "Post-1990 democratization: new budget structure replacing communist-era. "
                "Numbers are very large (hyperinflation recovery period) — extract the printed scale exactly."
            ) for y in range(1990, 1995)},

            **{y: (
                "Poland 1995-2006: post-redenomination era (new PLN). "
                "unit='thousand', currency='PLN'. "
                "KBN era (until 2005), then MNiSW created 2005. "
                "KEY R&D: Część 28 (Szkolnictwo wyższe i nauka) — primary R&D part. "
                "Dział 730 = szkolnictwo wyższe (HE); Dział 740 = prace badawczo-rozwojowe. "
                "KBN appears as named appropriation. PAN in Część 67. "
                "CERN participation. Universities receive dotacje (grants) under Część 28."
            ) for y in range(1995, 2007)},

            **{y: (
                "Poland 2007-2010: MNiSW era; NCBiR created 2007. "
                "unit='thousand', currency='PLN'. "
                "Część 28 remains primary R&D part. NCBiR appears as new line from 2007. "
                "NCN not yet created (comes 2011). "
                "Extract: NCBiR, MNiSW direct R&D programmes, PAN, universities under Część 28."
            ) for y in range(2007, 2011)},

            **{y: (
                "Poland 2011+: modern structure — NCN (basic research), NCBiR (applied R&D), "
                "subwencje for universities, PAN. "
                "unit='thousand', currency='PLN'. "
                "Część 28 (Szkolnictwo wyższe i nauka) = primary. Część 67 = PAN. "
                "Dział 730 Chapter 73001 = subwencje dla uczelni (university block grants). "
                "Dział 740 = NCN, NCBiR, MNiSW R&D programmes. "
                "From 2021: MEiN (Ministerstwo Edukacji i Nauki) merges education and science ministries. "
                "From 2024: Split back into separate ministries. "
                "Total budget 2025: ~921B tys. zł expenditures — focus on Część 28 and 67 only."
            ) for y in range(2011, 2026)},
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
