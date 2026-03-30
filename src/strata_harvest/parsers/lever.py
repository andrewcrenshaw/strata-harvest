"""Lever ATS parser — JSON API extraction."""

from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

from strata_harvest.models import ATSProvider, JobListing
from strata_harvest.parsers.base import BaseParser
from strata_harvest.utils.http import safe_fetch

logger = logging.getLogger(__name__)

_LI_PATTERN = re.compile(r"<li[^>]*>(.*?)</li>", re.DOTALL | re.IGNORECASE)
_TAG_PATTERN = re.compile(r"<[^>]+>")


class LeverParser(BaseParser):
    """Parse job listings from Lever career pages.

    Lever exposes a public JSON API at ``/v0/postings/{site}?mode=json``
    that returns structured posting data.  This parser handles both the
    list endpoint (JSON array) and single-posting endpoint (JSON object).
    """

    provider = ATSProvider.LEVER

    def parse(self, content: str, *, url: str) -> list[JobListing]:
        """Parse Lever JSON API response into job listings.

        *content* is the raw JSON string from the Lever postings API.
        Returns an empty list for non-JSON or malformed input.
        """
        try:
            data = json.loads(content)
        except (json.JSONDecodeError, TypeError):
            logger.debug("Content is not valid JSON for Lever parser")
            return []

        postings: list[dict[str, Any]]
        if isinstance(data, list):
            postings = data
        elif isinstance(data, dict):
            postings = [data]
        else:
            return []

        results: list[JobListing] = []
        for posting in postings:
            if not isinstance(posting, dict):
                continue
            try:
                listing = self._parse_posting(posting)
                results.append(listing)
            except Exception:
                logger.debug(
                    "Skipping malformed Lever posting: %s",
                    posting.get("id", "?"),
                )
                continue

        return results

    # ------------------------------------------------------------------
    # Fetching with pagination
    # ------------------------------------------------------------------

    @classmethod
    async def fetch_all(
        cls,
        url: str,
        *,
        limit: int = 100,
        max_pages: int = 50,
        client: Any = None,
    ) -> list[JobListing]:
        """Fetch all postings from a Lever site, handling pagination.

        Converts *url* (career-page or API URL) to the JSON API endpoint,
        then pages through results using ``skip`` / ``limit``.
        """
        api_base = cls.build_api_url(url)
        parser = cls()
        all_listings: list[JobListing] = []
        skip = 0

        for _ in range(max_pages):
            sep = "&" if "?" in api_base else "?"
            page_url = f"{api_base}{sep}skip={skip}&limit={limit}"
            result = await safe_fetch(page_url, client=client)

            if not result.ok or not result.content:
                if result.error:
                    logger.warning("Lever fetch error at skip=%d: %s", skip, result.error)
                break

            page_listings = parser.parse(result.content, url=url)
            if not page_listings:
                break

            all_listings.extend(page_listings)

            if len(page_listings) < limit:
                break

            skip += limit

        return all_listings

    # ------------------------------------------------------------------
    # URL helpers
    # ------------------------------------------------------------------

    @staticmethod
    def build_api_url(url: str) -> str:
        """Convert a Lever career-page or API URL to the JSON API endpoint.

        Handles both global (``jobs.lever.co``) and EU (``jobs.eu.lever.co``)
        instances. Already-correct API URLs are returned as-is (with
        ``mode=json`` appended if missing).
        """
        if "api.lever.co" in url or "api.eu.lever.co" in url:
            if "mode=json" not in url:
                sep = "&" if "?" in url else "?"
                return f"{url}{sep}mode=json"
            return url

        parsed = urlparse(url)
        path = parsed.path.rstrip("/")

        if "eu.lever.co" in parsed.netloc:
            return f"https://api.eu.lever.co/v0/postings{path}?mode=json"

        return f"https://api.lever.co/v0/postings{path}?mode=json"

    # ------------------------------------------------------------------
    # Internal parsing helpers
    # ------------------------------------------------------------------

    def _parse_posting(self, posting: dict[str, Any]) -> JobListing:
        """Map a single Lever posting dict to a ``JobListing``."""
        categories = posting.get("categories") or {}

        title = posting.get("text") or "Untitled Position"
        hosted_url = posting.get("hostedUrl") or ""
        if not hosted_url:
            posting_id = posting.get("id", "")
            if not posting_id:
                msg = "Lever posting has neither hostedUrl nor id"
                raise ValueError(msg)
            hosted_url = f"https://jobs.lever.co/unknown/{posting_id}"

        description = posting.get("descriptionPlain") or _strip_html(posting.get("description", ""))

        return JobListing(
            title=title,
            url=hosted_url,
            location=categories.get("location"),
            department=categories.get("department"),
            employment_type=categories.get("commitment"),
            description=description or None,
            requirements=self._extract_requirements(posting.get("lists")),
            salary_range=self._format_salary(posting.get("salaryRange")),
            posted_date=self._parse_timestamp(posting.get("createdAt")),
            ats_provider=ATSProvider.LEVER,
            raw_data=posting,
        )

    @staticmethod
    def _extract_requirements(lists: list[dict[str, str]] | None) -> list[str]:
        """Extract items from Lever's ``lists`` field (requirements, qualifications, etc.)."""
        if not lists:
            return []

        items_out: list[str] = []
        for section in lists:
            if not isinstance(section, dict):
                continue
            content = section.get("content", "")
            if not content:
                continue
            for raw_item in _LI_PATTERN.findall(content):
                cleaned = _TAG_PATTERN.sub("", raw_item).strip()
                if cleaned:
                    items_out.append(cleaned)

        return items_out

    @staticmethod
    def _format_salary(salary_range: dict[str, Any] | None) -> str | None:
        """Format a Lever ``salaryRange`` object into a human-readable string."""
        if not salary_range or not isinstance(salary_range, dict):
            return None

        currency = salary_range.get("currency", "")
        min_val = salary_range.get("min")
        max_val = salary_range.get("max")
        interval = salary_range.get("interval", "")

        if min_val is None and max_val is None:
            return None

        parts: list[str] = []
        if currency:
            parts.append(currency)

        if min_val is not None and max_val is not None:
            parts.append(f"{min_val:,.0f} - {max_val:,.0f}")
        elif min_val is not None:
            parts.append(f"{min_val:,.0f}+")
        elif max_val is not None:
            parts.append(f"up to {max_val:,.0f}")

        if interval:
            parts.append(interval)

        return " ".join(parts) if parts else None

    @staticmethod
    def _parse_timestamp(ts: int | float | None) -> datetime | None:
        """Convert a Lever millisecond Unix timestamp to a datetime."""
        if ts is None or not isinstance(ts, (int, float)):
            return None
        try:
            return datetime.fromtimestamp(ts / 1000, tz=UTC)
        except (OSError, ValueError, OverflowError):
            return None


def _strip_html(html: str) -> str:
    """Remove HTML tags, returning plain text."""
    if not html:
        return ""
    return _TAG_PATTERN.sub("", html).strip()
