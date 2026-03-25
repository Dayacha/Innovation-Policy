"""
PDF downloader for OECD Economic Surveys.

Handles downloading survey PDFs from the OECD website, with caching,
retries, and rate limiting.

Note: Many OECD Economic Surveys require institutional access or purchase.
This module will:
1. Attempt to download freely available PDFs
2. Check for already-downloaded PDFs in the local directory
3. Provide clear messages when a PDF is not freely available
"""

import logging
import os
import time
from pathlib import Path
from typing import Dict, List, Optional

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from .countries import CODE_TO_NAME, CODE_TO_SLUG

logger = logging.getLogger(__name__)


class PDFDownloader:
    """Downloads OECD Economic Survey PDFs."""

    def __init__(self, config):
        self.config = config
        self.pdf_dir = Path(config["paths"]["raw_pdfs"])
        self.pdf_dir.mkdir(parents=True, exist_ok=True)
        self.api_delay = config.get("processing", {}).get("api_delay", 2.0)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (compatible; OECDReformExtractor/1.0; "
                "academic research)"
            ),
        })

    def get_pdf_path(self, country_code, year):
        """Return the expected local path for a survey PDF."""
        return self.pdf_dir / f"{country_code}_{year}.pdf"

    def is_downloaded(self, country_code, year):
        """Check if a PDF has already been downloaded."""
        path = self.get_pdf_path(country_code, year)
        return path.exists() and path.stat().st_size > 0

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
    )
    def _download_url(self, url, dest_path):
        """Download a file from a URL with retries."""
        resp = self.session.get(url, stream=True, timeout=60)
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "")
        if "pdf" not in content_type.lower() and not url.endswith(".pdf"):
            raise ValueError(
                f"URL did not return a PDF (Content-Type: {content_type})"
            )

        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info(f"Downloaded: {dest_path.name} ({dest_path.stat().st_size} bytes)")
        return dest_path

    def _extract_doi(self, read_url: str) -> Optional[str]:
        """Extract the OECD DOI code from a read_url.

        e.g. 'https://read.oecd.org/10.1787/d5c6f307-en' → 'd5c6f307-en'
        """
        import re
        m = re.search(r"10\.1787/([a-zA-Z0-9][-a-zA-Z0-9]+)", read_url or "")
        return m.group(1) if m else None

    def _download_with_playwright(self, read_url: str, dest_path: Path) -> Optional[Path]:
        """
        Use a headless browser to open the OECD publication page, find the
        'Download PDF' link, and save the file.  Required because oecd.org is
        behind Cloudflare, which blocks plain HTTP clients.
        """
        try:
            from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
        except ImportError:
            logger.debug("playwright not installed — skipping browser download")
            return None

        logger.info("Launching headless browser for %s …", read_url)
        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True)
                ctx = browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                    accept_downloads=True,
                )
                page = ctx.new_page()

                # Navigate and wait for the Cloudflare challenge to resolve
                page.goto(read_url, wait_until="domcontentloaded", timeout=60_000)
                # Wait until we are actually on the oecd.org publication page
                try:
                    page.wait_for_url("**/oecd.org/**", timeout=30_000)
                except PWTimeout:
                    pass  # proceed anyway, may still find the link

                # Give the page time to render fully
                page.wait_for_load_state("networkidle", timeout=30_000)

                # Find the PDF download link.  OECD pages typically have
                # an <a> whose href ends in .pdf or whose text says "Download"
                pdf_url = page.evaluate("""() => {
                    // Look for a direct .pdf href
                    const links = Array.from(document.querySelectorAll('a[href]'));
                    const pdf = links.find(a =>
                        /\\.pdf(\\?|$)/i.test(a.href) ||
                        /download.*pdf|pdf.*download/i.test(a.textContent)
                    );
                    return pdf ? pdf.href : null;
                }""")

                if pdf_url:
                    logger.debug("Found PDF link via browser: %s", pdf_url)
                    # Download by intercepting the network response
                    with page.expect_download(timeout=60_000) as dl_info:
                        page.evaluate(f"window.location.href = {repr(pdf_url)}")
                    download = dl_info.value
                    download.save_as(str(dest_path))
                    browser.close()
                    size = dest_path.stat().st_size
                    logger.info("Downloaded via browser: %s (%d bytes)", dest_path.name, size)
                    return dest_path

                # Fallback: intercept any PDF response while clicking Download buttons
                with page.expect_download(timeout=30_000) as dl_info:
                    clicked = page.evaluate("""() => {
                        const btns = Array.from(document.querySelectorAll('a, button'));
                        const dl = btns.find(el =>
                            /download/i.test(el.textContent) ||
                            /download/i.test(el.getAttribute('aria-label') || '')
                        );
                        if (dl) { dl.click(); return true; }
                        return false;
                    }""")
                    if not clicked:
                        browser.close()
                        return None

                download = dl_info.value
                download.save_as(str(dest_path))
                browser.close()
                size = dest_path.stat().st_size
                logger.info("Downloaded via browser: %s (%d bytes)", dest_path.name, size)
                return dest_path

        except Exception as exc:
            logger.debug("Playwright download failed for %s: %s", read_url, exc)
            return None

    def download_survey(self, country_code, year, url=None, read_url=None):
        """Download a single survey PDF.

        Tries multiple strategies in order:
          1. Provided direct URL
          2. pac-files.oecd.org URL from kappa_catalog (if passed as url)
          3. DOI delivery URL derived from read_url
          4. Scraping the read_url page for a PDF link
          5. Legacy oecd-ilibrary.org slug-based URL patterns

        Args:
            country_code: ISO 3166-1 alpha-3 country code
            year:         Survey year
            url:          Direct PDF URL (e.g. from kappa_catalog pdf_url)
            read_url:     Publication page URL (e.g. read.oecd.org/10.1787/...)

        Returns:
            Path to downloaded PDF, or None if all strategies failed.
        """
        dest_path = self.get_pdf_path(country_code, year)
        name = CODE_TO_NAME.get(country_code, country_code)

        if self.is_downloaded(country_code, year):
            logger.info("Already downloaded: %s %d", name, year)
            return dest_path

        # Strategy 1 — direct URL (pac-files or any provided URL)
        if url:
            try:
                return self._download_url(url, dest_path)
            except Exception as e:
                logger.debug("Direct URL failed for %s %d: %s", name, year, e)

        # Strategy 2 — headless browser (handles Cloudflare, clicks Download button)
        if read_url:
            result = self._download_with_playwright(read_url, dest_path)
            if result:
                return result

        # Strategy 5 — legacy oecd-ilibrary.org slug patterns
        slug = CODE_TO_SLUG.get(country_code, "")
        candidate_urls = [
            f"https://www.oecd-ilibrary.org/deliver/oecd-economic-surveys-{slug}-{year}.pdf",
            f"https://doi.org/10.1787/eco_surveys-{slug.replace('-', '_')}-{year}-en",
        ]
        for candidate_url in candidate_urls:
            try:
                result = self._download_url(candidate_url, dest_path)
                time.sleep(self.api_delay)
                return result
            except Exception:
                continue

        logger.warning(
            "Could not download %s %d via any method. "
            "Place the PDF manually at: %s",
            name, year, dest_path,
        )
        return None

    def download_batch(self, catalog_entries, max_downloads=None):
        """Download multiple survey PDFs.

        Args:
            catalog_entries: List of catalog entry dicts
            max_downloads: Maximum number to download (None = all)

        Returns:
            Dict with counts: downloaded, skipped, failed
        """
        stats = {"downloaded": 0, "skipped": 0, "failed": 0}

        for i, entry in enumerate(catalog_entries):
            if max_downloads and stats["downloaded"] >= max_downloads:
                break

            country_code = entry["country_code"]
            year = entry["year"]
            url = entry.get("url")

            if self.is_downloaded(country_code, year):
                stats["skipped"] += 1
                continue

            result = self.download_survey(country_code, year, url=url)
            if result:
                stats["downloaded"] += 1
            else:
                stats["failed"] += 1

            # Rate limiting
            time.sleep(self.api_delay)

        return stats

    def download_from_kappa(
        self,
        kappa_catalog: Dict[str, Dict[str, dict]],
        country_code: Optional[str] = None,
        year: Optional[int] = None,
    ) -> Dict[str, int]:
        """
        Download PDFs using a Kappa catalog dict.

        Args:
            kappa_catalog: {iso3: {year_str: {"pdf_url": ..., ...}}}
            country_code:  If set, only download surveys for this ISO3 country.
            year:          If set, only download surveys for this year.

        Returns:
            Stats dict: {"downloaded": n, "skipped": n, "failed": n}
        """
        from .kappa_client import KappaClient

        # We need a KappaClient to download with auth headers. Build one from
        # the same api_key that was used to build the catalog (stored in config).
        kappa = KappaClient.from_config(self.config)
        if kappa is None:
            logger.error("Cannot download from Kappa — no API key in config.")
            return {"downloaded": 0, "skipped": 0, "failed": 0}

        stats = {"downloaded": 0, "skipped": 0, "failed": 0}

        for code, years in kappa_catalog.items():
            if country_code and code != country_code:
                continue
            for year_str, meta in years.items():
                yr = int(year_str)
                if year and yr != year:
                    continue

                dest = self.get_pdf_path(code, yr)
                if self.is_downloaded(code, yr):
                    logger.info("Already downloaded: %s %d", CODE_TO_NAME.get(code, code), yr)
                    stats["skipped"] += 1
                    continue

                pdf_url = meta.get("pdf_url")
                if not pdf_url:
                    logger.warning("No PDF URL in catalog for %s %d", code, yr)
                    stats["failed"] += 1
                    continue

                success = kappa.download_pdf(pdf_url, dest)
                if success:
                    stats["downloaded"] += 1
                else:
                    # Fall back to DOI/scraping/slug patterns using catalog metadata
                    result = self.download_survey(
                        code, yr,
                        url=meta.get("pdf_url"),
                        read_url=meta.get("read_url"),
                    )
                    if result:
                        stats["downloaded"] += 1
                    else:
                        stats["failed"] += 1

                time.sleep(self.api_delay)

        return stats

    def scan_local_pdfs(self):
        """Scan the PDF directory and return a list of available surveys.

        Returns:
            List of (country_code, year, path) tuples for found PDFs.
        """
        found = []
        for pdf_file in sorted(self.pdf_dir.glob("*.pdf")):
            parts = pdf_file.stem.rsplit("_", 1)
            if len(parts) == 2:
                code, year_str = parts
                try:
                    year = int(year_str)
                    if code.upper() in CODE_TO_NAME:
                        found.append((code.upper(), year, pdf_file))
                except ValueError:
                    continue
        return found
