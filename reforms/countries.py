"""
OECD member countries with metadata.

Provides a comprehensive list of OECD member countries, their ISO codes,
and the year they joined the OECD (relevant for filtering surveys).
"""

# OECD member countries as of 2024
# Format: (ISO 3166-1 alpha-3, country name, OECD accession year)
OECD_COUNTRIES = [
    ("AUS", "Australia", 1971),
    ("AUT", "Austria", 1961),
    ("BEL", "Belgium", 1961),
    ("CAN", "Canada", 1961),
    ("CHL", "Chile", 2010),
    ("COL", "Colombia", 2020),
    ("CRI", "Costa Rica", 2021),
    ("CZE", "Czech Republic", 1995),
    ("DNK", "Denmark", 1961),
    ("EST", "Estonia", 2010),
    ("FIN", "Finland", 1969),
    ("FRA", "France", 1961),
    ("DEU", "Germany", 1961),
    ("GRC", "Greece", 1961),
    ("HUN", "Hungary", 1996),
    ("ISL", "Iceland", 1961),
    ("IRL", "Ireland", 1961),
    ("ISR", "Israel", 2010),
    ("ITA", "Italy", 1962),
    ("JPN", "Japan", 1964),
    ("KOR", "Korea", 1996),
    ("LVA", "Latvia", 2016),
    ("LTU", "Lithuania", 2018),
    ("LUX", "Luxembourg", 1961),
    ("MEX", "Mexico", 1994),
    ("NLD", "Netherlands", 1961),
    ("NZL", "New Zealand", 1973),
    ("NOR", "Norway", 1961),
    ("POL", "Poland", 1996),
    ("PRT", "Portugal", 1961),
    ("SVK", "Slovak Republic", 2000),
    ("SVN", "Slovenia", 2010),
    ("ESP", "Spain", 1961),
    ("SWE", "Sweden", 1961),
    ("CHE", "Switzerland", 1961),
    ("TUR", "Türkiye", 1961),
    ("GBR", "United Kingdom", 1961),
    ("USA", "United States", 1961),
]

# Lookup dictionaries
CODE_TO_NAME = {code: name for code, name, _ in OECD_COUNTRIES}
CODE_TO_YEAR = {code: year for code, _, year in OECD_COUNTRIES}
NAME_TO_CODE = {name: code for code, name, _ in OECD_COUNTRIES}

# Name variants used in OECD publications (for matching)
NAME_VARIANTS = {
    "AUS": ["Australia"],
    "AUT": ["Austria"],
    "BEL": ["Belgium"],
    "CAN": ["Canada"],
    "CHL": ["Chile"],
    "COL": ["Colombia"],
    "CRI": ["Costa Rica"],
    "CZE": ["Czech Republic", "Czechia"],
    "DNK": ["Denmark"],
    "EST": ["Estonia"],
    "FIN": ["Finland"],
    "FRA": ["France"],
    "DEU": ["Germany"],
    "GRC": ["Greece"],
    "HUN": ["Hungary"],
    "ISL": ["Iceland"],
    "IRL": ["Ireland"],
    "ISR": ["Israel"],
    "ITA": ["Italy"],
    "JPN": ["Japan"],
    "KOR": ["Korea", "South Korea", "Republic of Korea"],
    "LVA": ["Latvia"],
    "LTU": ["Lithuania"],
    "LUX": ["Luxembourg"],
    "MEX": ["Mexico"],
    "NLD": ["Netherlands", "The Netherlands"],
    "NZL": ["New Zealand"],
    "NOR": ["Norway"],
    "POL": ["Poland"],
    "PRT": ["Portugal"],
    "SVK": ["Slovak Republic", "Slovakia"],
    "SVN": ["Slovenia"],
    "ESP": ["Spain"],
    "SWE": ["Sweden"],
    "CHE": ["Switzerland"],
    "TUR": ["Türkiye", "Turkey"],
    "GBR": ["United Kingdom", "UK", "Great Britain"],
    "USA": ["United States", "US", "United States of America"],
}

# OECD iLibrary URL slug for each country (used in survey URLs)
CODE_TO_SLUG = {
    "AUS": "australia",
    "AUT": "austria",
    "BEL": "belgium",
    "CAN": "canada",
    "CHL": "chile",
    "COL": "colombia",
    "CRI": "costa-rica",
    "CZE": "czech-republic",
    "DNK": "denmark",
    "EST": "estonia",
    "FIN": "finland",
    "FRA": "france",
    "DEU": "germany",
    "GRC": "greece",
    "HUN": "hungary",
    "ISL": "iceland",
    "IRL": "ireland",
    "ISR": "israel",
    "ITA": "italy",
    "JPN": "japan",
    "KOR": "korea",
    "LVA": "latvia",
    "LTU": "lithuania",
    "LUX": "luxembourg",
    "MEX": "mexico",
    "NLD": "netherlands",
    "NZL": "new-zealand",
    "NOR": "norway",
    "POL": "poland",
    "PRT": "portugal",
    "SVK": "slovak-republic",
    "SVN": "slovenia",
    "ESP": "spain",
    "SWE": "sweden",
    "CHE": "switzerland",
    "TUR": "turkey",
    "GBR": "united-kingdom",
    "USA": "united-states",
}


def get_country_list(filter_codes=None, start_year=1995):
    """Return list of (code, name) tuples, optionally filtered.

    Args:
        filter_codes: If provided, only return these country codes.
        start_year: Only return countries that were OECD members by this year
                    (or joined later but had surveys conducted).

    Returns:
        List of (iso3_code, country_name) tuples.
    """
    result = []
    for code, name, accession_year in OECD_COUNTRIES:
        if filter_codes and code not in filter_codes:
            continue
        # Include all current OECD members; surveys may exist even before
        # formal accession for countries that joined after 1995
        result.append((code, name))
    return result
