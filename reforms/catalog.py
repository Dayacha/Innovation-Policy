"""
Survey catalog: builds and manages a catalog of OECD Economic Surveys.

The catalog maps (country_code, year) -> survey metadata, including
URLs, file paths, and processing status.
"""

import json
import logging
import os
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from .countries import CODE_TO_NAME, CODE_TO_SLUG, get_country_list

logger = logging.getLogger(__name__)

# Base URL patterns for OECD iLibrary
OECD_ILIBRARY_SEARCH = (
    "https://www.oecd-ilibrary.org/economics/"
    "oecd-economic-surveys-{slug}_{issn}"
)

# ISSN for OECD Economic Surveys series by country
# These are the series ISSNs used in the iLibrary URL structure
COUNTRY_ISSNS = {
    "AUS": "19990146",
    "AUT": "19990189",
    "BEL": "19990227",
    "CAN": "19990081",
    "CHL": "19990413",
    "COL": "25222074",
    "CRI": "25222082",
    "CZE": "19990348",
    "DNK": "19990499",
    "EST": "19990391",
    "FIN": "19990251",
    "FRA": "19990235",
    "DEU": "19990243",
    "GRC": "19990308",
    "HUN": "1999035x",
    "ISL": "1999014x",
    "IRL": "19990324",
    "ISR": "19990499",
    "ITA": "19990316",
    "JPN": "19990138",
    "KOR": "19990162",
    "LVA": "24132209",
    "LTU": "25222287",
    "LUX": "19990154",
    "MEX": "19990170",
    "NLD": "19990200",
    "NZL": "19990219",
    "NOR": "19990197",
    "POL": "19990332",
    "PRT": "19990297",
    "SVK": "19990405",
    "SVN": "19990383",
    "ESP": "1999026x",
    "SWE": "19990278",
    "CHE": "19990286",
    "TUR": "19990375",
    "GBR": "19990340",
    "USA": "19990103",
}


class SurveyCatalog:
    """Manages a catalog of OECD Economic Surveys."""

    def __init__(self, config):
        self.config = config
        self.catalog_path = Path(config["paths"]["output"]) / "survey_catalog.json"
        self.catalog = self._load_catalog()

    def _load_catalog(self):
        """Load existing catalog from disk, or return empty dict."""
        if self.catalog_path.exists():
            with open(self.catalog_path) as f:
                return json.load(f)
        return {}

    def save_catalog(self):
        """Persist catalog to disk."""
        self.catalog_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.catalog_path, "w") as f:
            json.dump(self.catalog, f, indent=2, ensure_ascii=False)

    def _make_key(self, country_code, year):
        return f"{country_code}_{year}"

    def get_entry(self, country_code, year):
        """Get catalog entry for a specific survey."""
        return self.catalog.get(self._make_key(country_code, year))

    def add_entry(self, country_code, year, **kwargs):
        """Add or update a catalog entry."""
        key = self._make_key(country_code, year)
        if key not in self.catalog:
            self.catalog[key] = {
                "country_code": country_code,
                "country_name": CODE_TO_NAME.get(country_code, country_code),
                "year": year,
                "pdf_path": None,
                "text_path": None,
                "reforms_path": None,
                "url": None,
                "status": "pending",
            }
        self.catalog[key].update(kwargs)
        return self.catalog[key]

    def build_catalog_from_local_pdfs(self, pdf_dir):
        """Scan a directory for PDFs and add them to the catalog.

        Expected naming convention: {COUNTRY_CODE}_{YEAR}.pdf
        Examples: FRA_2019.pdf, DEU_2022.pdf, USA_2020.pdf

        Also supports: {country_name}_{year}.pdf (case-insensitive)
        """
        pdf_dir = Path(pdf_dir)
        if not pdf_dir.exists():
            logger.warning(f"PDF directory does not exist: {pdf_dir}")
            return 0

        from .countries import NAME_TO_CODE

        count = 0
        for pdf_file in sorted(pdf_dir.glob("*.pdf")):
            name = pdf_file.stem
            parts = name.rsplit("_", 1)
            if len(parts) != 2:
                logger.warning(
                    f"Skipping {pdf_file.name}: expected format "
                    f"COUNTRY_YEAR.pdf"
                )
                continue

            country_part, year_part = parts
            try:
                year = int(year_part)
            except ValueError:
                logger.warning(
                    f"Skipping {pdf_file.name}: could not parse year"
                )
                continue

            # Resolve country code
            country_code = country_part.upper()
            if country_code not in CODE_TO_NAME:
                # Try matching by name
                matched = NAME_TO_CODE.get(country_part.title())
                if matched:
                    country_code = matched
                else:
                    logger.warning(
                        f"Skipping {pdf_file.name}: unknown country "
                        f"'{country_part}'"
                    )
                    continue

            self.add_entry(
                country_code,
                year,
                pdf_path=str(pdf_file.resolve()),
                status="pdf_available",
            )
            count += 1
            logger.info(
                f"Cataloged: {CODE_TO_NAME[country_code]} {year} "
                f"-> {pdf_file.name}"
            )

        self.save_catalog()
        return count

    def build_expected_catalog(self, countries=None, start_year=1995,
                               end_year=2025):
        """Build a catalog of expected surveys based on known publication
        patterns.

        OECD Economic Surveys are typically published every 1.5-2 years
        per country. This creates entries for known survey years.
        """
        country_list = get_country_list(filter_codes=countries)

        # Known survey years for major countries (non-exhaustive)
        # This serves as a starting template; actual years are filled
        # when PDFs are found or scraped from the OECD website
        typical_intervals = {}
        for code, name in country_list:
            # Generate expected years (every 2 years as approximation)
            years = list(range(start_year, end_year + 1, 2))
            typical_intervals[code] = years

        count = 0
        for code, name in country_list:
            slug = CODE_TO_SLUG.get(code, "")
            for year in typical_intervals.get(code, []):
                if year < start_year or year > end_year:
                    continue
                key = self._make_key(code, year)
                if key not in self.catalog:
                    url = (
                        f"https://www.oecd-ilibrary.org/economics/"
                        f"oecd-economic-surveys-{slug}-{year}_en"
                    )
                    self.add_entry(code, year, url=url)
                    count += 1

        self.save_catalog()
        logger.info(f"Built expected catalog with {count} new entries")
        return count

    def get_pending_surveys(self):
        """Return catalog entries that haven't been fully processed yet."""
        return [
            entry for entry in self.catalog.values()
            if entry.get("status") not in ("reforms_extracted", "completed")
        ]

    def get_surveys_with_pdfs(self):
        """Return catalog entries that have PDFs available."""
        return [
            entry for entry in self.catalog.values()
            if entry.get("pdf_path") and os.path.exists(entry["pdf_path"])
        ]

    def get_surveys_with_text(self):
        """Return entries that have extracted text."""
        return [
            entry for entry in self.catalog.values()
            if entry.get("text_path") and os.path.exists(entry["text_path"])
        ]

    def get_processed_surveys(self):
        """Return entries that have been fully processed."""
        return [
            entry for entry in self.catalog.values()
            if entry.get("status") in ("reforms_extracted", "completed")
        ]

    def summary(self):
        """Return a summary of the catalog state."""
        total = len(self.catalog)
        with_pdf = len(self.get_surveys_with_pdfs())
        with_text = len(self.get_surveys_with_text())
        processed = len(self.get_processed_surveys())
        return {
            "total_entries": total,
            "with_pdf": with_pdf,
            "with_text": with_text,
            "processed": processed,
            "pending": total - processed,
        }

    def scrape_oecd_survey_list(self, country_code, max_pages=5):
        """Attempt to scrape the list of available surveys for a country
        from the OECD iLibrary.

        This is best-effort; the OECD website structure may change.

        Returns:
            List of dicts with keys: year, title, url
        """
        slug = CODE_TO_SLUG.get(country_code)
        issn = COUNTRY_ISSNS.get(country_code)
        if not slug or not issn:
            logger.warning(
                f"No URL info for country {country_code}, skipping scrape"
            )
            return []

        base_url = OECD_ILIBRARY_SEARCH.format(slug=slug, issn=issn)
        surveys = []

        try:
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (compatible; OECDReformExtractor/1.0; "
                    "academic research)"
                )
            }
            resp = requests.get(base_url, headers=headers, timeout=30)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, "html.parser")

            # Look for publication listings - the structure varies
            # but typically includes links with year in title
            for link in soup.find_all("a", href=True):
                text = link.get_text(strip=True)
                if "economic survey" in text.lower():
                    # Try to extract year from the link text
                    import re
                    year_match = re.search(r"(19|20)\d{2}", text)
                    if year_match:
                        year = int(year_match.group())
                        href = link["href"]
                        if not href.startswith("http"):
                            href = (
                                "https://www.oecd-ilibrary.org" + href
                            )
                        surveys.append({
                            "year": year,
                            "title": text,
                            "url": href,
                        })

        except Exception as e:
            logger.warning(
                f"Could not scrape survey list for {country_code}: {e}"
            )

        # Update catalog with found surveys
        for s in surveys:
            self.add_entry(
                country_code,
                s["year"],
                url=s["url"],
                title=s.get("title"),
            )

        if surveys:
            self.save_catalog()

        return surveys
