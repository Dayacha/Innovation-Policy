from __future__ import annotations

from typing import Iterable

LOCKED_SERIES_OBSERVATIONS: dict[str, list[tuple]] = {
    "Israel": [
        (1975, "National Council for R&D (Israel, pre-1992)", 115000000.0, "unit", "ILP", "1975_Israel.pdf", 16.0, "section_total", "Total"),
        (1976, "National Council for R&D (Israel, pre-1992)", 194100.0, "unit", "ILP", "1976_Israel.pdf", 16.0, "section_total", "Total expenditures"),
        (1977, "National Council for R&D (Israel, pre-1992)", 1000000.0, "unit", "ILP", "1977_Israel.pdf", 55.0, "section_total", "Total"),
        (1978, "National Council for R&D (Israel, pre-1992)", 519286.0, "unit", "ILP", "1978_Israel.pdf", 134.0, "section_total", "Total"),
        (1979, "National Council for R&D (Israel, pre-1992)", 1764000.0, "unit", "ILP", "1979_Israel.pdf", 146.0, "section_total", "Total Expenditures"),
        (1980, "National Council for R&D (Israel, pre-1992)", 604700000.0, "thousand", "ILS_OLD", "1980_Israel.pdf", 97.0, "verified_override", "Verified against original Israel budget file"),
        (1981, "National Council for R&D (Israel, pre-1992)", 1000000000.0, "thousand", "ILS_OLD", "1981_Israel.pdf", 27.0, "verified_override", "Verified against original Israel budget file"),
        (1982, "National Council for R&D (Israel, pre-1992)", 749000000.0, "thousand", "ILS_OLD", "1982_Israel.pdf", 102.0, "verified_override", "Verified against original Israel budget file"),
        (1983, "National Council for R&D (Israel, pre-1992)", 156300000.0, "thousand", "ILS_OLD", "1983_Israel.pdf", 11.0, "verified_override", "Verified against original Israel budget file"),
        (1984, "National Council for R&D (Israel, pre-1992)", 411000000.0, "thousand", "ILS_OLD", "1984_Israel.pdf", 12.0, "verified_override", "Verified against original Israel budget file"),
        (1985, "National Council for R&D (Israel, pre-1992)", 2035.0, "million", "ILS_OLD", "1985_Israel.pdf", 167.0, "section_total", "Total amount"),
        (1986, "National Council for R&D (Israel, pre-1992)", 3708.0, "thousand", "ILS", "1986_Israel.pdf", 180.0, "section_total", "Total"),
        (1986, "Office of the Chief Scientist (Israel, pre-2016)", 486.0, "thousand", "ILS", "1986_Israel.pdf", 216.0, "line_item", "Chief Scientist"),
        (1987, "KAMEA Fund (קרן קמ\"ח)", 3783.0, "thousand", "ILS", "1987_Israel.pdf", 196.0, "line_item", "Research funded by KAMEA"),
        (1987, "National Council for R&D (Israel, pre-1992)", 6400.0, "thousand", "ILS", "1987_Israel.pdf", 146.0, "section_total", "Total amount"),
        (1988, "National Council for R&D (Israel, pre-1992)", 5741.0, "thousand", "ILS", "1988_Israel.pdf", 155.0, "section_total", "Total"),
        (1989, "National Council for R&D (Israel, pre-1992)", 7364.0, "thousand", "ILS", "1989_Israel.pdf", 5.0, "section_total", "Total"),
        (1990, "National Council for R&D (Israel, pre-1992)", 17199.0, "thousand", "ILS", "1990_Israel.pdf", 7.0, "section_total", "Total"),
        (1991, "KAMEA Fund (קרן קמ\"ח)", 3300.0, "thousand", "ILS", "1991_Israel.pdf", 88.0, "line_item", "Research funded by KAMEA"),
        (1991, "National Council for R&D (Israel, pre-1992)", 39503.0, "thousand", "ILS", "1991_Israel.pdf", 133.0, "section_total", "Total"),
        (1992, "Ministry of Science and Technology (Israel)", 21708.0, "thousand", "ILS", "1992_Israel.pdf", 42.0, "section_total", "Total"),
        (1994, "Ministry of Science and Technology (Israel)", 56624.0, "thousand", "ILS", "1994_Israel.pdf", 39.0, "section_total", "Total"),
        (1995, "Israeli Space Agency (סוכנות החלל הישראלית)", 4644.0, "thousand", "ILS", "1995_Israel.pdf", 45.0, "program_total", "Israeli Space Agency"),
        (1995, "Ministry of Science and Technology (Israel)", 152824.0, "thousand", "ILS", "1995_Israel.pdf", 45.0, "section_total", "Total General"),
        (1996, "Ministry of Science and Technology (Israel)", 176579.0, "thousand", "ILS", "1996_Israel.pdf", 44.0, "section_total", "Budget of the Ministry of Science"),
        (1997, "Ministry of Science and Technology (Israel)", 184250.0, "thousand", "ILS", "1997_Israel.pdf", 41.0, "section_total", "Total General"),
        (1998, "Ministry of Science and Technology (Israel)", 825272.0, "thousand", "ILS", "1998_Israel.pdf", 94.0, "section_total", "Total"),
        (1999, "Ministry of Science and Technology (Israel)", 172618.0, "thousand", "ILS", "1999_Israel.pdf", 38.0, "section_total", "Total"),
        (2000, "Ministry of Science and Technology (Israel)", 636117.0, "thousand", "ILS", "2000_Israel.pdf", 36.0, "section_total", "Expenditure"),
        (2001, "Ministry of Science and Technology (Israel)", 722050.0, "thousand", "ILS", "2001_Israel.pdf", 37.0, "section_total", "Total General"),
        (2002, "Ministry of Science and Technology (Israel)", 497804.0, "thousand", "ILS", "2002_Israel.pdf", 41.0, "section_total", "Total"),
        (2004, "Israeli Space Agency (סוכנות החלל הישראלית)", 405.0, "thousand", "ILS", "2004_Israel.pdf", 36.0, "line_item", "Israeli Space Agency"),
        (2004, "Ministry of Science and Technology (Israel)", 118947.0, "thousand", "ILS", "2004_Israel.pdf", 36.0, "section_total", "Total"),
        (2005, "Israeli Space Agency (סוכנות החלל הישראלית)", 15.343, "thousand", "ILS", "2005_Israel.pdf", 36.0, "line_item", "Activities of the Israeli Space Agency"),
        (2005, "Ministry of Science and Technology (Israel)", 118277.0, "thousand", "ILS", "2005_Israel.pdf", 36.0, "section_total", "Total General"),
        (2007, "Israeli Space Agency (סוכנות החלל הישראלית)", 444934.0, "thousand", "ILS", "2007_Israel.pdf", 31.0, "program_total", "Israeli Space Agency"),
        (2007, "Ministry of Science and Technology (Israel)", 700837.0, "thousand", "ILS", "2007_Israel.pdf", 31.0, "section_total", "Total General"),
        (2008, "Ministry of Science and Technology (Israel)", 709529.0, "thousand", "ILS", "2008_Israel.pdf", 29.0, "section_total", "Total General"),
        (2009, "Ministry of Science and Technology (Israel)", 979860.0, "thousand", "ILS", "2009-2010_Israel.pdf", 200.0, "section_total", "Total"),
        (2011, "Israeli Space Agency (סוכנות החלל הישראלית)", 271935.0, "thousand", "ILS", "2011-2012_Israel.pdf", 55.0, "line_item", "Support for Space Agency Activities"),
        (2011, "Ministry of Science and Technology (Israel)", 1400000.0, "thousand", "ILS", "2011-2012_Israel.pdf", 200.0, "verified_override", "Verified against original Israel budget file"),
        (2013, "Ministry of Science and Technology (Israel)", 1117322.0, "thousand", "ILS", "2013-2014_Israel.pdf", 28.0, "verified_override", "Verified against original Israel budget file"),
        (2013, "Office of the Chief Scientist (Israel, pre-2016)", 1684800.0, "thousand", "ILS", "2013-2014_Israel.pdf", 57.0, "program_total", "Chief Scientist"),
        (2015, "Ministry of Science and Technology (Israel)", 1361253.0, "thousand", "ILS", "2015-2016_Israel.pdf", 31.0, "verified_override", "Verified against original Israel budget file"),
        (2017, "Israel Innovation Authority (from 2016)", 175750.0, "thousand", "ILS", "2017-2018_Israel.pdf", 136.0, "line_item", "Operation of the Innovation Authority"),
        (2017, "Ministry of Science and Technology (Israel)", 1573033.0, "thousand", "ILS", "2017-2018_Israel.pdf", 113.0, "section_total", "Total"),
        (2019, "Israeli Space Agency (סוכנות החלל הישראלית)", 874951.0, "thousand", "ILS", "2019_Israel.pdf", 82.0, "program_total", "Budget for the Israeli Space Agency"),
        (2021, "Israeli Space Agency (סוכנות החלל הישראלית)", 561109.0, "thousand", "ILS", "2021_Israel.pdf", 84.0, "line_item", "Budget for the Israeli Space Agency"),
        (2021, "Ministry of Science and Technology (Israel)", 500513.603, "thousand", "ILS", "2021_Israel.pdf", 6.0, "section_total", "Total"),
        (2022, "Israeli Space Agency (סוכנות החלל הישראלית)", 100000.0, "thousand", "ILS", "2022_Israel.pdf", 27.0, "line_item", "Budget for the Israeli Space Agency - Research and Development"),
        (2022, "Ministry of Science and Technology (Israel)", 462500.134, "thousand", "ILS", "2022_Israel.pdf", 6.0, "section_total", "Total budget for the Ministry of Science and Technology"),
        (2023, "Ministry of Science and Technology (Israel)", 2535972.0, "thousand", "ILS", "2023_Israel.pdf", 6.0, "verified_override", "Section 19 total (science + culture bundle)"),
        (2024, "Ministry of Science and Technology (Israel)", 2749765.0, "thousand", "ILS", "2024_Israel.pdf", 6.0, "verified_override", "Section 19 total (science + culture bundle)"),
        (2025, "Israel Innovation Authority (from 2016)", 2008207.0, "thousand", "ILS", "2025_Israel.pdf", 48.0, "verified_override", "Verified against original Israel budget file"),
        (2025, "Ministry of Science and Technology (Israel)", 2807783.0, "thousand", "ILS", "2025_Israel.pdf", 6.0, "verified_override", "Section 19 total (science + culture bundle)"),
    ],
}


def get_locked_series_entries(country: str) -> list[tuple]:
    return list(LOCKED_SERIES_OBSERVATIONS.get(country, []))


def is_locked_observation(country: str, canonical_name: str, year: int) -> bool:
    entries: Iterable[tuple] = LOCKED_SERIES_OBSERVATIONS.get(country, [])
    return any(int(y) == int(year) and str(cn) == str(canonical_name) for y, cn, *_ in entries)
