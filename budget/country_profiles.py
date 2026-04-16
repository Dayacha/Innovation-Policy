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
    # Not yet audited. Entries to be added after first run.
    # Key document type: Spending Review DEL tables (£ billion units)
    # -----------------------------------------------------------------------
    "UK": {
        "skip_if": [
            # Placeholder — add after running UK documents
        ],
        "include_note": [
            # UK Research Councils each have their own vote — all are in-scope
            "Each Research Council (MRC, EPSRC, NERC, BBSRC, ESRC, AHRC, STFC) "
            "is a dedicated science agency; their full operating appropriations are in-scope.",
        ],
    },

    # -----------------------------------------------------------------------
    # CANADA
    # Not yet audited. Entries to be added after first run.
    # Key document type: Main Estimates (C$ full dollars or thousands)
    # -----------------------------------------------------------------------
    "Canada": {
        "skip_if": [
            # Placeholder — add after running Canada documents
        ],
        "include_note": [
            "NSERC, SSHRC, CIHR grants and contributions are core R&D — include all.",
            "NRC (National Research Council) operating appropriations are in-scope.",
        ],
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
    # Not yet audited.
    # Key document type: Loi de finances (EUR million)
    # -----------------------------------------------------------------------
    "France": {
        "skip_if": [
            # Placeholder
        ],
        "include_note": [
            "Mission 'Recherche et enseignement supérieur' is the primary R&D mission — "
            "all programme-level lines within it are candidates.",
            "CNRS, INSERM, CEA, INRIA operating appropriations are in-scope.",
        ],
    },

    # -----------------------------------------------------------------------
    # GERMANY
    # Not yet audited.
    # Key document type: Bundeshaushalt (EUR thousand, Einzelplan 30 = BMBF)
    # -----------------------------------------------------------------------
    "Germany": {
        "skip_if": [
            # Placeholder
        ],
        "include_note": [
            "Einzelplan 30 (Bundesministerium für Bildung und Forschung) is the main R&D "
            "chapter — all lines are candidates.",
            "DFG (Deutsche Forschungsgemeinschaft), Fraunhofer, Max-Planck, Helmholtz, "
            "and Leibniz institutional grants are in-scope.",
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

def build_country_addendum(country: str) -> str:
    """
    Return a short, token-efficient prompt snippet with country-specific
    extraction guidance. Returns empty string if no profile exists.

    The snippet is intentionally brief (<200 tokens) to keep LLM costs low.
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
            lines.append(f"  - {rule[:180]}")

    if include:
        lines.append(f"Country-specific INCLUDE clarifications for {country}:")
        for note in include:
            lines.append(f"  - {note[:180]}")

    return "\n".join(lines) if lines else ""
