"""Greenhouse ATS parser — REST API extraction.

Greenhouse exposes a public Job Board API at
``https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true``
that returns structured JSON with job listings, departments, offices,
and optional salary ranges.

Docs: https://developers.greenhouse.io/job-board.html
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

from strata_harvest.models import ATSProvider, JobListing
from strata_harvest.parsers.base import BaseParser
from strata_harvest.utils.http import safe_fetch

logger = logging.getLogger(__name__)

_TAG_PATTERN = re.compile(r"<[^>]+>")
_LI_PATTERN = re.compile(r"<li[^>]*>(.*?)</li>", re.DOTALL | re.IGNORECASE)

BOARDS_API_BASE = "https://boards-api.greenhouse.io/v1/boards"


class GreenhouseParser(BaseParser):
    """Parse job listings from Greenhouse career pages.

    Greenhouse exposes a REST API at /embed/api/v1/jobs that returns
    structured JSON. This parser handles both API and HTML fallback.
    """

    provider = ATSProvider.GREENHOUSE

    def parse(self, content: str, *, url: str) -> list[JobListing]:
        """Parse Greenhouse JSON API response into job listings.

        *content* is the raw JSON string from the Greenhouse Job Board API
        (with ``?content=true``). Returns an empty list for non-JSON or
        malformed input.
        """
        try:
            data = json.loads(content)
        except (json.JSONDecodeError, TypeError):
            logger.debug("Content is not valid JSON for Greenhouse parser")
            return []

        if not isinstance(data, dict) or "jobs" not in data:
            logger.debug("Greenhouse response missing 'jobs' key")
            return []

        raw_jobs: list[dict[str, Any]] = data.get("jobs", [])
        if not isinstance(raw_jobs, list):
            return []

        results: list[JobListing] = []
        for raw in raw_jobs:
            if not isinstance(raw, dict):
                continue
            try:
                listing = self._parse_job(raw)
                results.append(listing)
            except Exception:
                logger.debug(
                    "Skipping malformed Greenhouse job: %s",
                    raw.get("id", "?"),
                )
                continue

        expected = data.get("meta", {}).get("total")
        if expected is not None and len(results) != expected:
            logger.info(
                "Greenhouse parsed %d of %d listed jobs "
                "(some may have been skipped)",
                len(results),
                expected,
            )

        return results

    # ------------------------------------------------------------------
    # Fetching with pagination
    # ------------------------------------------------------------------

    @classmethod
    async def fetch_all(
        cls,
        url: str,
        *,
        client: Any = None,
    ) -> list[JobListing]:
        """Fetch all job listings from a Greenhouse board.

        Converts *url* (career-page or API URL) to the boards-api endpoint,
        fetches with ``?content=true``, and parses the response.

        Greenhouse returns all jobs in a single response (no server-side
        pagination), so this makes one request. The method signature matches
        ``LeverParser.fetch_all`` for consistency.
        """
        api_url = cls.build_api_url(url)
        result = await safe_fetch(api_url, client=client)

        if not result.ok or not result.content:
            if result.error:
                logger.warning("Greenhouse fetch error: %s", result.error)
            return []

        parser = cls()
        return parser.parse(result.content, url=url)

    # ------------------------------------------------------------------
    # URL helpers
    # ------------------------------------------------------------------

    @staticmethod
    def build_api_url(url: str) -> str:
        """Convert a Greenhouse career-page or embed URL to the boards-api endpoint.

        Handles:
        - ``https://boards.greenhouse.io/{token}`` → boards-api with content=true
        - ``https://boards.greenhouse.io/embed/job_board?for={token}`` → same
        - ``https://boards-api.greenhouse.io/v1/boards/{token}/jobs`` → add content=true
        - ``https://example.com/careers`` with known board token → best-effort
        """
        parsed = urlparse(url)

        if "boards-api.greenhouse.io" in parsed.netloc:
            if "content=true" not in url:
                sep = "&" if "?" in url else "?"
                return f"{url}{sep}content=true"
            return url

        if "boards.greenhouse.io" in parsed.netloc:
            if "/embed/" in parsed.path:
                query = parsed.query or ""
                for param in query.split("&"):
                    if param.startswith("for="):
                        token = param.split("=", 1)[1]
                        return f"{BOARDS_API_BASE}/{token}/jobs?content=true"

            path_parts = [p for p in parsed.path.strip("/").split("/") if p]
            if path_parts:
                token = path_parts[0]
                return f"{BOARDS_API_BASE}/{token}/jobs?content=true"

        return f"{BOARDS_API_BASE}/{parsed.path.strip('/')}/jobs?content=true"

    # ------------------------------------------------------------------
    # Internal parsing helpers
    # ------------------------------------------------------------------

    def _parse_job(self, raw: dict[str, Any]) -> JobListing:
        """Map a single Greenhouse job object to a ``JobListing``."""
        job_id = raw.get("id")
        title = raw.get("title")
        absolute_url = raw.get("absolute_url")

        if not title or not absolute_url:
            msg = f"Greenhouse job {job_id} missing title or absolute_url"
            raise ValueError(msg)

        location_obj = raw.get("location")
        location = None
        if isinstance(location_obj, dict):
            location = location_obj.get("name")

        description = _strip_html(raw.get("content") or "")
        requirements = _extract_requirements(raw.get("content") or "")

        department = self._extract_department(raw.get("departments"))
        salary = self._format_salary(raw.get("pay_input_ranges"))

        return JobListing(
            title=title,
            url=absolute_url,
            location=location,
            department=department,
            description=description or None,
            requirements=requirements,
            salary_range=salary,
            posted_date=_parse_iso_timestamp(raw.get("updated_at")),
            ats_provider=ATSProvider.GREENHOUSE,
            raw_data=raw,
        )

    @staticmethod
    def _extract_department(departments: list[dict[str, Any]] | None) -> str | None:
        """Extract primary department name from Greenhouse departments array.

        Returns the first leaf department (no children) if available,
        otherwise the first department.
        """
        if not departments or not isinstance(departments, list):
            return None

        leaf = next(
            (d["name"] for d in departments
             if isinstance(d, dict) and "name" in d and not d.get("child_ids")),
            None,
        )
        if leaf:
            return leaf

        first = next(
            (d["name"] for d in departments if isinstance(d, dict) and "name" in d),
            None,
        )
        return first

    @staticmethod
    def _format_salary(pay_ranges: list[dict[str, Any]] | None) -> str | None:
        """Format the first Greenhouse ``pay_input_ranges`` entry as a human-readable string."""
        if not pay_ranges or not isinstance(pay_ranges, list):
            return None

        first = next((p for p in pay_ranges if isinstance(p, dict)), None)
        if not first:
            return None

        min_cents = first.get("min_cents")
        max_cents = first.get("max_cents")
        currency = first.get("currency_type", "")

        if min_cents is None and max_cents is None:
            return None

        parts: list[str] = []
        if currency:
            parts.append(currency)

        if min_cents is not None and max_cents is not None:
            parts.append(f"{min_cents / 100:,.0f} - {max_cents / 100:,.0f}")
        elif min_cents is not None:
            parts.append(f"{min_cents / 100:,.0f}+")
        elif max_cents is not None:
            parts.append(f"up to {max_cents / 100:,.0f}")

        return " ".join(parts) if parts else None


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _strip_html(html: str) -> str:
    """Remove HTML tags, returning plain text."""
    if not html:
        return ""
    return _TAG_PATTERN.sub("", html).strip()


def _extract_requirements(html: str) -> list[str]:
    """Extract list items from HTML content (e.g. requirements lists)."""
    if not html:
        return []

    items: list[str] = []
    for raw_item in _LI_PATTERN.findall(html):
        cleaned = _TAG_PATTERN.sub("", raw_item).strip()
        if cleaned:
            items.append(cleaned)
    return items


def _parse_iso_timestamp(ts: str | None) -> datetime | None:
    """Parse an ISO 8601 timestamp string to a datetime."""
    if not ts or not isinstance(ts, str):
        return None
    try:
        return datetime.fromisoformat(ts)
    except (ValueError, TypeError):
        return None
