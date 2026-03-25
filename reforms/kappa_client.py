"""
OECD Kappa API client for discovering and downloading Economic Survey PDFs.

The Kappa API is OECD's internal catalogue API. It provides:
  - Paginated search for publications by type, language, date range, and country
  - PDF download URLs for each publication

API key:
  - Set in config.yaml under  reforms.kappa_api_key
  - Or via environment variable  KAPPA_API_KEY

Usage example:
    client = KappaClient.from_config(config)
    catalog = client.build_survey_catalog(start_year=2000, end_year=2024)
    # catalog: {iso3_code: {year_str: {"pdf_url": ..., "read_url": ..., "publication_date": ...}}}
    client.download_pdf(catalog["DNK"]["2019"]["pdf_url"], Path("DNK_2019.pdf"))
"""

import json
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

KAPPA_API_HOST = "https://kappa.oecd.org/catalogue/api"
# catalogueType RID for "Economic Survey" in the Kappa taxonomy
ECONOMIC_SURVEY_TYPE_ID = "1301"

# XML namespaces used in Kappa responses
_NS = {
    "api": "http://www.oecd.org/ns/lambda/schema#api-response",
    "s":   "http://www.oecd.org/ns/lambda/schema/",
}


def _build_variant_map() -> Dict[str, str]:
    """Build a case-insensitive name → ISO3 lookup from countries.py."""
    from .countries import NAME_TO_CODE, NAME_VARIANTS
    mapping: Dict[str, str] = {}
    for name, code in NAME_TO_CODE.items():
        mapping[name.lower()] = code
    for code, variants in NAME_VARIANTS.items():
        for v in variants:
            mapping[v.lower()] = code
    return mapping


def _country_name_to_iso3(name: str, variant_map: Dict[str, str]) -> Optional[str]:
    """Convert a country name (from Kappa title) to an ISO3 code."""
    return variant_map.get(name.lower())


def _parse_title(title: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse an OECD survey title into (country_name, year).

    Handles titles like:
      "OECD Economic Surveys: Denmark 2024"
      "OECD Economic Surveys: Korea 2023"
      "OECD Economic Survey of France 2022"
    """
    # Pattern 1: "OECD Economic Surveys: <Country> <year>"
    m = re.search(r"OECD Economic Surveys?[:\s]+(.+?)\s+(\d{4})", title, re.IGNORECASE)
    if m:
        return m.group(1).strip(), m.group(2)
    # Pattern 2: "OECD Economic Survey of <Country> <year>"
    m = re.search(r"OECD Economic Survey\s+of\s+(.+?)\s+(\d{4})", title, re.IGNORECASE)
    if m:
        return m.group(1).strip(), m.group(2)
    return None, None


class KappaClient:
    """Client for the OECD Kappa catalogue API."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._headers = {"X-Kappa-ApiKey": api_key}
        self._variant_map = _build_variant_map()

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_config(cls, config: dict) -> Optional["KappaClient"]:
        """
        Build a KappaClient from a pipeline config dict.

        Looks for the key in order:
          1. config["kappa_api_key"]
          2. environment variable KAPPA_API_KEY

        Returns None (with a warning) if no key is found.
        """
        api_key = (
            config.get("kappa_api_key", "").strip()
            or os.environ.get("KAPPA_API_KEY", "").strip()
        )
        if not api_key:
            logger.warning(
                "No Kappa API key found. Set reforms.kappa_api_key in config.yaml "
                "or the KAPPA_API_KEY environment variable."
            )
            return None
        return cls(api_key)

    # ------------------------------------------------------------------
    # Internal HTTP helpers
    # ------------------------------------------------------------------

    def _get(self, endpoint: str, params=None, timeout: float = 30.0,
             retries: int = 2) -> str:
        """GET request to the Kappa API. Returns response text."""
        try:
            import httpx
        except ImportError:
            raise ImportError("httpx is required for Kappa API access: pip install httpx")

        url = f"{KAPPA_API_HOST}{endpoint}"
        last_exc: Optional[Exception] = None

        for attempt in range(retries + 1):
            try:
                with httpx.Client(
                    timeout=httpx.Timeout(timeout),
                    verify=False,
                    headers=self._headers,
                ) as client:
                    response = client.get(url, params=params)
                    if not response.is_success:
                        logger.warning(
                            "Kappa API %s returned HTTP %s", url, response.status_code
                        )
                    return response.text
            except Exception as exc:
                last_exc = exc
                if attempt < retries:
                    time.sleep(1.5)

        raise RuntimeError(f"Kappa API request failed after {retries + 1} attempts: {last_exc}")

    # ------------------------------------------------------------------
    # XML parsing
    # ------------------------------------------------------------------

    def _parse_response(self, xml_text: str) -> Tuple[List[dict], int]:
        """
        Parse a Kappa XML search response.

        Returns:
            (publications, total_count)
            Each publication has: title, publication_date, read_url, pdf_url
        """
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as exc:
            logger.error("Failed to parse Kappa XML response: %s", exc)
            return [], 0

        # Total result count
        total = 0
        sr = root.find(".//s:search-result", _NS)
        if sr is not None and "total" in sr.attrib:
            try:
                total = int(sr.attrib["total"])
            except ValueError:
                pass

        publications = []
        for result in root.findall(".//s:result[@slice='expression']", _NS):
            title = result.findtext("s:title", default="", namespaces=_NS)
            pub_date = result.findtext("s:dateOfPublication", default="", namespaces=_NS)
            read_url = result.findtext("s:readUrl", default="", namespaces=_NS)

            # Locate the PDF download URL from manifestations
            pdf_url = None
            for m in result.findall("s:result[@slice='manifestation']", _NS):
                medium = m.findtext("s:medium", default="", namespaces=_NS)
                if medium != "PDF":
                    continue
                for f in m.findall("s:file", _NS):
                    if f.attrib.get("version") == "main" and f.text:
                        raw = f.text.strip()
                        pdf_url = raw if raw.startswith("http") else "https:" + raw
                        break
                if pdf_url:
                    break

            publications.append({
                "title": title,
                "publication_date": pub_date,
                "read_url": read_url,
                "pdf_url": pdf_url,
            })

        return publications, total

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def search_surveys(
        self,
        start_date: str = "1990-01-01",
        end_date: Optional[str] = None,
        country_name: Optional[str] = None,
        page_size: int = 200,
        max_pages: int = 50,
    ) -> List[dict]:
        """
        Search Kappa for OECD Economic Surveys.

        Args:
            start_date:   "YYYY-MM-DD"
            end_date:     "YYYY-MM-DD" inclusive (None = today)
            country_name: Optional country name filter (e.g. "Denmark")
            page_size:    Results per page (max 200)
            max_pages:    Safety cap on pagination

        Returns:
            List of dicts: title, publication_date, read_url, pdf_url
        """
        # Build date range filter string
        date_clauses = [f">= {start_date}"]
        if end_date:
            try:
                end_plus = (
                    datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
                ).strftime("%Y-%m-%d")
                date_clauses.append(f"< {end_plus}")
            except ValueError:
                date_clauses.append(f"<= {end_date}")

        all_pubs: List[dict] = []
        seen: set = set()
        page = 1

        while page <= max_pages:
            params = [
                ("filter[w:catalogueType]", ECONOMIC_SURVEY_TYPE_ID),
                ("filter[e:language]", "en"),
                ("filter[e:dateOfPublication]", ";".join(date_clauses)),
                ("page", str(page)),
                ("pageSize", str(page_size)),
            ]
            if country_name:
                params.append(("filter[e:title]", country_name))

            try:
                xml_text = self._get("/v2/search/all", params=params)
            except Exception as exc:
                logger.error("Kappa search failed (page %d): %s", page, exc)
                break

            pubs, total = self._parse_response(xml_text)
            if not pubs:
                break

            new_count = 0
            for pub in pubs:
                key = pub.get("read_url") or pub.get("title", "")
                if key and key not in seen:
                    seen.add(key)
                    all_pubs.append(pub)
                    new_count += 1

            if len(all_pubs) >= total or new_count == 0:
                break
            page += 1

        logger.info("Kappa search returned %d surveys (total reported: %d)", len(all_pubs), total if total else "?")
        return all_pubs

    # ------------------------------------------------------------------
    # Catalog builder
    # ------------------------------------------------------------------

    def build_survey_catalog(
        self,
        start_year: int = 1995,
        end_year: int = 2025,
        countries: Optional[List[str]] = None,
    ) -> Dict[str, Dict[str, dict]]:
        """
        Build a catalog of survey metadata organized by ISO3 code and year.

        Args:
            start_year: First year to include
            end_year:   Last year to include
            countries:  ISO3 codes to filter (None = all OECD countries)

        Returns:
            {
              "DNK": {
                "2019": {"pdf_url": "...", "read_url": "...", "publication_date": "2019-03-15"},
                ...
              },
              ...
            }
        """
        from .countries import CODE_TO_NAME

        start_date = f"{start_year}-01-01"
        end_date = f"{end_year}-12-31"
        today = date.today()

        # If specific countries requested, query each individually so the title
        # filter is tight. Otherwise, one broad query.
        if countries:
            all_pubs: List[dict] = []
            for code in countries:
                name = CODE_TO_NAME.get(code, code)
                logger.info("Fetching Kappa catalog for %s (%s)...", name, code)
                pubs = self.search_surveys(
                    start_date=start_date,
                    end_date=end_date,
                    country_name=name,
                )
                all_pubs.extend(pubs)
        else:
            logger.info("Fetching full Kappa survey catalog (%d–%d)...", start_year, end_year)
            all_pubs = self.search_surveys(start_date=start_date, end_date=end_date)

        catalog: Dict[str, Dict[str, dict]] = {}
        skipped = 0

        for pub in all_pubs:
            title = pub.get("title", "")
            country_name_raw, year = _parse_title(title)
            if not country_name_raw or not year:
                skipped += 1
                continue

            # Map country name → ISO3
            iso3 = _country_name_to_iso3(country_name_raw, self._variant_map)
            if not iso3:
                logger.debug("Unknown country in title: '%s' (title: %s)", country_name_raw, title)
                skipped += 1
                continue

            # Skip future-dated publications
            pub_date_str = pub.get("publication_date", "")
            if pub_date_str:
                try:
                    if datetime.strptime(pub_date_str, "%Y-%m-%d").date() > today:
                        skipped += 1
                        continue
                except ValueError:
                    pass

            if iso3 not in catalog:
                catalog[iso3] = {}
            catalog[iso3][year] = {
                "pdf_url":          pub.get("pdf_url"),
                "read_url":         pub.get("read_url", ""),
                "publication_date": pub_date_str,
            }

        total_entries = sum(len(y) for y in catalog.values())
        logger.info(
            "Kappa catalog built: %d surveys across %d countries (%d entries skipped)",
            total_entries, len(catalog), skipped,
        )
        return catalog

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    def download_pdf(self, pdf_url: str, dest_path: Path, timeout: float = 60.0) -> bool:
        """
        Download a single PDF from a Kappa URL.

        Args:
            pdf_url:   Direct PDF URL from the Kappa manifest
            dest_path: Local path to save the file

        Returns:
            True on success, False on failure.
        """
        if not pdf_url:
            logger.warning("No pdf_url provided for %s", dest_path.name)
            return False

        try:
            import httpx
        except ImportError:
            raise ImportError("httpx is required: pip install httpx")

        try:
            with httpx.Client(
                verify=False,
                timeout=httpx.Timeout(timeout),
                headers=self._headers,
                follow_redirects=True,
            ) as client:
                response = client.get(pdf_url)
                if response.status_code != 200:
                    logger.warning(
                        "PDF download failed HTTP %s: %s", response.status_code, pdf_url
                    )
                    return False

                dest_path.parent.mkdir(parents=True, exist_ok=True)
                dest_path.write_bytes(response.content)
                size_kb = len(response.content) // 1024
                logger.info("Downloaded %s (%d KB)", dest_path.name, size_kb)
                return True

        except Exception as exc:
            logger.warning("PDF download error for %s: %s", dest_path.name, exc)
            return False

    # ------------------------------------------------------------------
    # Incremental catalog update
    # ------------------------------------------------------------------

    def update_catalog(self, existing_catalog: dict) -> dict:
        """
        Incrementally update a catalog with surveys published after the latest
        known date.

        Finds the most recent publication_date in existing_catalog, queries
        Kappa for anything newer, and merges the results.

        Returns the updated catalog dict (same format as build_survey_catalog).
        """
        # Find the latest known publication date
        latest_date: Optional[str] = None
        for years in existing_catalog.values():
            for entry in years.values():
                pub = entry.get("publication_date", "")
                if pub and (latest_date is None or pub > latest_date):
                    latest_date = pub

        start_date = latest_date or "1990-01-01"
        logger.info("Checking for new surveys published after %s...", start_date)

        new_pubs = self.search_surveys(start_date=start_date)
        if not new_pubs:
            logger.info("No new surveys found — catalog is up to date")
            return existing_catalog

        today = date.today()
        updated = {k: dict(v) for k, v in existing_catalog.items()}
        added = updated_existing = 0

        for pub in new_pubs:
            title = pub.get("title", "")
            country_name, year = _parse_title(title)
            if not country_name or not year:
                continue

            iso3 = _country_name_to_iso3(country_name, self._variant_map)
            if not iso3:
                continue

            pub_date = pub.get("publication_date", "")
            if pub_date:
                try:
                    if datetime.strptime(pub_date, "%Y-%m-%d").date() > today:
                        continue
                except ValueError:
                    pass

            if iso3 not in updated:
                updated[iso3] = {}

            if year not in updated[iso3]:
                updated[iso3][year] = {
                    "pdf_url":          pub.get("pdf_url"),
                    "read_url":         pub.get("read_url", ""),
                    "publication_date": pub_date,
                }
                added += 1
            elif pub.get("pdf_url"):
                # Refresh URL for existing entry (Kappa URLs can expire)
                updated[iso3][year]["pdf_url"]          = pub.get("pdf_url")
                updated[iso3][year]["read_url"]          = pub.get("read_url", "")
                if pub_date:
                    updated[iso3][year]["publication_date"] = pub_date
                updated_existing += 1

        logger.info("Catalog update: %d new entries, %d URLs refreshed", added, updated_existing)
        return updated

    # ------------------------------------------------------------------
    # Legacy data import
    # ------------------------------------------------------------------

    @staticmethod
    def import_from_legacy_data(legacy_path: Path) -> dict:
        """
        Convert an oecd_full_data.json file (from econ_surveys_analysis) to
        kappa_catalog format.

        Source format: {country_name: {year: {pdf_link, document_url, ...}}}
        Target format: {iso3:         {year: {pdf_url,  read_url,     publication_date}}}
        """
        variant_map = _build_variant_map()

        with open(legacy_path, encoding="utf-8") as f:
            legacy = json.load(f)

        catalog: dict = {}
        skipped: list = []

        for country_name, years in legacy.items():
            iso3 = _country_name_to_iso3(country_name, variant_map)
            if not iso3:
                skipped.append(country_name)
                continue
            catalog[iso3] = {}
            for year, entry in years.items():
                pdf_url = entry.get("pdf_link") or entry.get("pdf_url")
                if not pdf_url:
                    continue
                catalog[iso3][year] = {
                    "pdf_url":          pdf_url,
                    "read_url":         entry.get("document_url", ""),
                    "publication_date": entry.get("publication_date", ""),
                }

        total = sum(len(y) for y in catalog.values())
        logger.info(
            "Imported %d survey entries for %d OECD countries from legacy data "
            "(%d non-OECD/unmapped countries skipped)",
            total, len(catalog), len(skipped),
        )
        return catalog

    # ------------------------------------------------------------------
    # Catalog persistence
    # ------------------------------------------------------------------

    @staticmethod
    def save_catalog(catalog: dict, path: Path) -> None:
        """Save a survey catalog dict to a JSON file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(catalog, f, indent=2, ensure_ascii=False)
        total = sum(len(y) for y in catalog.values())
        logger.info("Catalog saved: %s (%d surveys, %d countries)", path, total, len(catalog))

    @staticmethod
    def load_catalog(path: Path) -> dict:
        """Load a previously saved survey catalog from JSON."""
        if not path.exists():
            return {}
        with open(path, encoding="utf-8") as f:
            return json.load(f)
