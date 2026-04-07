import argparse
import os
import json
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import httpx
import requests
import yaml

from reforms.countries import CODE_TO_NAME, NAME_TO_CODE, NAME_VARIANTS


KAPPA_API_HOST = "https://kappa.oecd.org/catalogue/api"
REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = REPO_ROOT / "Data" / "input" / "surveys"
DEFAULT_CATALOG_PATH = DEFAULT_OUTPUT_DIR / "kappa_catalog.json"
NS = {"schema": "http://www.oecd.org/ns/lambda/schema/"}


def load_kappa_api_key() -> str:
    env_key = os.environ.get("KAPPA_API_KEY", "").strip()
    if env_key:
        return env_key

    config_path = REPO_ROOT / "config.yaml"
    if not config_path.exists():
        return ""

    try:
        with config_path.open("r", encoding="utf-8") as handle:
            config = yaml.safe_load(handle) or {}
    except Exception:
        return ""

    return str((config.get("reforms", {}) or {}).get("kappa_api_key", "")).strip()


def build_date_filter(start_date: str, end_date: Optional[str]) -> str:
    clauses = [f">= {start_date}"]
    if end_date:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        clauses.append(f"< {end_dt.strftime('%Y-%m-%d')}")
    return ";".join(clauses)


def kappa_get(endpoint: str, params: List[Tuple[str, str]], api_key: str, timeout: float = 30.0) -> httpx.Response:
    headers = {"X-Kappa-ApiKey": api_key}
    url = f"{KAPPA_API_HOST}{endpoint}"

    last_error = None
    for _ in range(3):
        try:
            with httpx.Client(timeout=timeout, verify=False, headers=headers, http2=False) as client:
                response = client.get(url, params=params)
                response.raise_for_status()
                return response
        except Exception as exc:
            last_error = exc
            time.sleep(1)
    raise RuntimeError(f"Kappa request failed for {url}: {last_error}")


def get_catalogue_type_id(api_key: str, term: str = "economic survey") -> str:
    response = kappa_get("/v1/taxonomy/submodel", [("term", term)], api_key)
    root = ET.fromstring(response.text)
    rid_element = root.find(".//schema:rid", NS)
    if rid_element is None or not rid_element.text:
        raise RuntimeError(f"Could not resolve Kappa taxonomy id for '{term}'.")
    match = re.search(r":(\d+)$", rid_element.text)
    if not match:
        raise RuntimeError(f"Unexpected taxonomy rid format: {rid_element.text}")
    return match.group(1)


def parse_publications(xml_text: str) -> Tuple[List[Dict[str, str]], int]:
    root = ET.fromstring(xml_text)
    search_result = root.find(".//schema:search-result", NS)
    total = int(search_result.attrib.get("total", "0")) if search_result is not None else 0

    publications: List[Dict[str, str]] = []
    for result in root.findall(".//schema:result[@slice='expression']", NS):
        title = result.findtext("schema:title", default="", namespaces=NS).strip()
        publication_date = result.findtext("schema:dateOfPublication", default="", namespaces=NS).strip()
        pdf_url = ""

        for manifestation in result.findall("schema:result[@slice='manifestation']", NS):
            medium = manifestation.findtext("schema:medium", default="", namespaces=NS).strip()
            if medium != "PDF":
                continue
            for file_node in manifestation.findall("schema:file", NS):
                if file_node.attrib.get("version") == "main" and file_node.text:
                    pdf_url = file_node.text.strip()
                    if pdf_url.startswith("//"):
                        pdf_url = f"https:{pdf_url}"
                    break
            if pdf_url:
                break

        if title and publication_date and pdf_url:
            publications.append(
                {
                    "title": title,
                    "publication_date": publication_date,
                    "pdf_url": pdf_url,
                }
            )

    return publications, total


def fetch_publications(
    api_key: str,
    country_query: str,
    start_date: str,
    end_date: Optional[str],
    page_size: int = 100,
) -> List[Dict[str, str]]:
    catalogue_type_id = get_catalogue_type_id(api_key)
    all_publications: List[Dict[str, str]] = []
    seen_titles = set()
    page = 1

    while True:
        params = [
            ("filter[w:catalogueType]", catalogue_type_id),
            ("filter[e:language]", "en"),
            ("filter[e:dateOfPublication]", build_date_filter(start_date, end_date)),
            ("filter[e:title]", country_query),
            ("page", str(page)),
            ("pageSize", str(page_size)),
        ]
        response = kappa_get("/v2/search/all", params, api_key)
        publications, total = parse_publications(response.text)

        if not publications:
            break

        new_items = 0
        for publication in publications:
            title = publication["title"]
            if title in seen_titles:
                continue
            seen_titles.add(title)
            all_publications.append(publication)
            new_items += 1

        if new_items == 0 or len(all_publications) >= total:
            break
        page += 1

    return all_publications


def parse_country_and_year(title: str) -> Optional[Tuple[str, str]]:
    patterns = [
        r"OECD Economic Surveys?[:\s]+(.+?)\s+(\d{4})$",
        r"OECD Economic Survey\s+of\s+(.+?)\s+(\d{4})$",
    ]
    for pattern in patterns:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            country_name = match.group(1).strip()
            year = match.group(2)
            return country_name, year
    return None


def resolve_iso3(country_name: str) -> Optional[str]:
    normalized = country_name.strip()
    if not normalized:
        return None

    if len(normalized) == 3 and normalized.upper() in CODE_TO_NAME:
        return normalized.upper()

    exact = NAME_TO_CODE.get(normalized)
    if exact:
        return exact

    lowered = normalized.lower()
    for iso3, variants in NAME_VARIANTS.items():
        if any(variant.lower() == lowered for variant in variants):
            return iso3
    return None


def load_kappa_catalog(catalog_path: Path = DEFAULT_CATALOG_PATH) -> Dict[str, Dict[str, Dict[str, str]]]:
    if not catalog_path.exists():
        return {}

    try:
        with catalog_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception:
        return {}

    return data if isinstance(data, dict) else {}


def _year_range_from_dates(start_date: str, end_date: Optional[str]) -> Tuple[int, int]:
    start_year = datetime.strptime(start_date, "%Y-%m-%d").year
    end_year = datetime.strptime(end_date, "%Y-%m-%d").year if end_date else 9999
    return start_year, end_year


def publications_from_catalog(
    country_query: str,
    start_date: str,
    end_date: Optional[str],
    catalog: Optional[Dict[str, Dict[str, Dict[str, str]]]] = None,
) -> List[Dict[str, str]]:
    catalog = catalog if catalog is not None else load_kappa_catalog()
    iso3 = resolve_iso3(country_query)
    if not iso3:
        return []

    country_entries = catalog.get(iso3, {})
    if not isinstance(country_entries, dict):
        return []

    country_name = CODE_TO_NAME.get(iso3, iso3)
    start_year, end_year = _year_range_from_dates(start_date, end_date)
    publications: List[Dict[str, str]] = []

    for year_str, meta in sorted(country_entries.items(), key=lambda item: int(item[0])):
        try:
            year = int(year_str)
        except (TypeError, ValueError):
            continue

        if year < start_year or year > end_year:
            continue

        pdf_url = str(meta.get("pdf_url", "")).strip()
        if not pdf_url:
            continue

        publication_date = str(meta.get("publication_date", "")).strip() or f"{year}-01-01"
        publications.append(
            {
                "title": f"OECD Economic Surveys: {country_name} {year}",
                "publication_date": publication_date,
                "pdf_url": pdf_url,
            }
        )

    return publications


def remove_partial_file(path: Path) -> None:
    try:
        if path.exists() and path.stat().st_size == 0:
            path.unlink()
    except OSError:
        pass


def download_file(
    pdf_url: str,
    destination: Path,
    api_key: str,
    timeout: float = 60.0,
    retries: int = 3,
) -> None:
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (compatible; OECDSurveyDownloader/1.0)",
            }
            if "pac-files.oecd.org" in pdf_url:
                headers["X-Kappa-ApiKey"] = api_key

            with requests.Session() as session:
                response = session.get(
                    pdf_url,
                    headers=headers,
                    timeout=timeout,
                    stream=True,
                    allow_redirects=True,
                    verify=False,
                )
                response.raise_for_status()
                with destination.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            handle.write(chunk)
                return
        except Exception as exc:
            last_error = exc
            remove_partial_file(destination)
            if attempt < retries:
                time.sleep(min(attempt * 2, 10))

    raise RuntimeError(f"Download failed after {retries} attempts for {destination.name}: {last_error}")


def download_surveys(
    country_query: str,
    start_date: str,
    end_date: Optional[str],
    output_dir: Path,
) -> int:
    api_key = load_kappa_api_key()
    if not api_key:
        raise RuntimeError("KAPPA_API_KEY is missing. Set it in the environment or config.yaml.")

    output_dir.mkdir(parents=True, exist_ok=True)
    catalog = load_kappa_catalog()
    publications = publications_from_catalog(country_query, start_date, end_date, catalog=catalog)
    if publications:
        print(
            f"Using catalog entries for '{country_query}' from {DEFAULT_CATALOG_PATH.name} "
            f"({len(publications)} match(es))."
        )
    else:
        publications = fetch_publications(api_key, country_query, start_date, end_date)
    if not publications:
        print(f"No publications found for '{country_query}'.")
        return 0

    downloaded = 0
    failed = 0
    for publication in publications:
        parsed = parse_country_and_year(publication["title"])
        if not parsed:
            print(f"Skipping unrecognized title: {publication['title']}")
            continue

        country_name, year = parsed
        iso3 = resolve_iso3(country_name)
        if not iso3:
            print(f"Skipping because ISO3 could not be resolved: {country_name}")
            continue

        destination = output_dir / f"{iso3}_{year}.pdf"
        if destination.exists() and destination.stat().st_size > 0:
            print(f"Skipping existing file: {destination.name}")
            continue
        try:
            download_file(publication["pdf_url"], destination, api_key)
            downloaded += 1
            print(f"Downloaded {publication['title']} -> {destination.name}")
        except Exception as exc:
            failed += 1
            print(f"Failed {destination.name}: {exc}")

    if failed:
        print(f"Completed with {failed} failed download(s).")
    return downloaded


def download_all_surveys(
    start_date: str,
    end_date: Optional[str],
    output_dir: Path,
) -> int:
    api_key = load_kappa_api_key()
    if not api_key:
        raise RuntimeError("KAPPA_API_KEY is missing. Set it in the environment or config.yaml.")

    output_dir.mkdir(parents=True, exist_ok=True)
    catalog = load_kappa_catalog()
    start_year, end_year = _year_range_from_dates(start_date, end_date)

    downloaded = 0
    failed = 0
    for iso3, years in sorted(catalog.items()):
        if not isinstance(years, dict):
            continue

        valid_items = []
        for year_str, meta in years.items():
            try:
                year = int(year_str)
            except (TypeError, ValueError):
                continue
            valid_items.append((year, meta))

        for year, meta in sorted(valid_items, key=lambda item: item[0]):
            if year < start_year or year > end_year:
                continue

            pdf_url = str(meta.get("pdf_url", "")).strip()
            if not pdf_url:
                continue

            destination = output_dir / f"{iso3}_{year}.pdf"
            if destination.exists() and destination.stat().st_size > 0:
                print(f"Skipping existing file: {destination.name}")
                continue

            try:
                download_file(pdf_url, destination, api_key)
                downloaded += 1
                print(f"Downloaded {destination.name}")
            except Exception as exc:
                failed += 1
                print(f"Failed {destination.name}: {exc}")

    if failed:
        print(f"Completed with {failed} failed download(s).")
    return downloaded


def main() -> int:
    parser = argparse.ArgumentParser(description="Download OECD Economic Survey PDFs from Kappa.")
    parser.add_argument("--country", help="Country to search, for example Denmark")
    parser.add_argument("--all-countries", action="store_true", help="Download all countries from the catalog")
    parser.add_argument("--year", type=int, default=None, help="Optional publication year filter")
    parser.add_argument("--start-date", default="1900-01-01", help="Inclusive start date, YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="Inclusive end date, YYYY-MM-DD")
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory where PDFs will be saved",
    )
    args = parser.parse_args()

    start_date = args.start_date
    end_date = args.end_date
    if args.year is not None:
        start_date = f"{args.year}-01-01"
        end_date = f"{args.year}-12-31"

    if not args.country and not args.all_countries:
        parser.error("Provide --country or --all-countries")

    try:
        if args.all_countries:
            count = download_all_surveys(
                start_date=start_date,
                end_date=end_date,
                output_dir=Path(args.output_dir),
            )
        else:
            count = download_surveys(
                country_query=args.country,
                start_date=start_date,
                end_date=end_date,
                output_dir=Path(args.output_dir),
            )
    except Exception as exc:
        print(f"Download failed: {exc}")
        return 1

    print(f"Finished. Downloaded {count} PDF(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
