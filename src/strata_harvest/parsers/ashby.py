"""Ashby ATS parser — GraphQL API extraction."""

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

_TAG_PATTERN = re.compile(r"<[^>]+>")

# Slug extraction patterns for custom-domain Ashby career pages (ENH-04 / PCC-1736).
# Primary: JSON config blob embedded in page HTML.
_SLUG_PATTERN_JSON = re.compile(r'"organizationHostedJobsPageName"\s*:\s*"([^"]+)"')
# Fallback: ashbyhq.com/... URL anywhere in the DOM.
_SLUG_PATTERN_URL = re.compile(r'ashbyhq\.com(?:/job-board)?/([^/"\s\?#]+)')
_LI_PATTERN = re.compile(r"<li[^>]*>(.*?)</li>", re.DOTALL | re.IGNORECASE)

GRAPHQL_ENDPOINT = "https://jobs.ashbyhq.com/api/non-user-graphql"

_JOB_BOARD_QUERY = """\
query JobBoardWithPostings($organizationHostedJobsPageName: String!) {
  jobBoard: jobBoardWithTeams(
    organizationHostedJobsPageName: $organizationHostedJobsPageName
  ) {
    jobPostings {
      id
      title
      employmentType
      locationName
      workplaceType
      secondaryLocations {
        locationName
      }
      compensationTierSummary
    }
  }
}"""

_SINGLE_POSTING_QUERY = """\
query JobPosting($jobPostingId: String!) {
  jobPosting(id: $jobPostingId) {
    id
    title
    employmentType
    locationName
    workplaceType
    secondaryLocations {
      locationName
    }
    compensationTierSummary
  }
}"""


class AshbyParser(BaseParser):
    """Parse job listings from Ashby career pages.

    Ashby uses a GraphQL API at ``/api/non-user-graphql`` for job posting
    data.  This parser handles both full job-board responses
    (``data.jobBoard.jobPostings``) and single-posting responses
    (``data.jobPosting``).
    """

    provider = ATSProvider.ASHBY

    def parse(self, content: str, *, url: str) -> list[JobListing]:
        """Parse Ashby GraphQL JSON response into job listings.

        *content* is the raw JSON string from the GraphQL API.
        Returns an empty list for non-JSON, error responses, or malformed input.
        """
        try:
            data = json.loads(content)
        except (json.JSONDecodeError, TypeError):
            logger.debug("Content is not valid JSON for Ashby parser")
            return []

        if not isinstance(data, dict):
            return []

        if data.get("errors"):
            logger.debug("Ashby GraphQL returned errors: %s", data["errors"])

        gql_data = data.get("data")
        if not isinstance(gql_data, dict):
            return []

        postings = self._extract_postings(gql_data)
        results: list[JobListing] = []
        for posting in postings:
            if not isinstance(posting, dict):
                continue
            try:
                listing = self._parse_posting(posting)
                results.append(listing)
            except Exception:
                logger.debug(
                    "Skipping malformed Ashby posting: %s",
                    posting.get("id", "?"),
                )
                continue

        return results

    # ------------------------------------------------------------------
    # Fetching via GraphQL
    # ------------------------------------------------------------------

    @classmethod
    async def fetch_all(
        cls,
        url: str,
        *,
        html: str | None = None,
        client: Any = None,
    ) -> list[JobListing]:
        """Fetch all postings from an Ashby job board via GraphQL.

        Resolves the org slug from *url* first; if *html* is provided (e.g. a
        custom-domain career page that embeds Ashby), the slug is extracted
        from the HTML instead, which is required when the URL path contains no
        slug (ENH-04 / PCC-1736).

        If no slug can be determined, logs a warning and returns ``[]``.
        """
        # Prefer slug from HTML for custom-domain pages; fall back to URL.
        org_slug: str | None = None
        if html is not None:
            org_slug = cls.extract_slug_from_html(html)
            if not org_slug:
                logger.warning(
                    "Ashby: could not extract tenant slug from HTML for %s; "
                    "returning empty results",
                    url,
                )
                return []
        else:
            org_slug = cls.extract_org_slug(url)

        api_url = cls.build_graphql_url(url)
        payload = cls.build_job_board_query(org_slug)

        result = await safe_fetch(
            api_url,
            method="POST",
            json=payload,
            headers={"Content-Type": "application/json"},
            client=client,
        )

        if not result.ok or not result.content:
            if result.error:
                logger.warning("Ashby GraphQL fetch error: %s", result.error)
            return []

        parser = cls()
        return parser.parse(result.content, url=url)

    # ------------------------------------------------------------------
    # URL helpers
    # ------------------------------------------------------------------

    @staticmethod
    def build_graphql_url(url: str) -> str:
        """Build the Ashby GraphQL endpoint URL.

        Always returns the canonical Ashby GraphQL endpoint, even for
        custom-domain career pages, as companies don't typically proxy it.
        """
        return GRAPHQL_ENDPOINT

    @staticmethod
    def extract_org_slug(url: str) -> str:
        """Extract the organization slug from an Ashby career page URL.

        ``https://jobs.ashbyhq.com/acmecorp/some-job`` → ``acmecorp``
        """
        parsed = urlparse(url)
        parts = [p for p in parsed.path.strip("/").split("/") if p]
        if parts and parts[0] != "api":
            return parts[0]
        return parsed.netloc.split(".")[0]

    @staticmethod
    def extract_slug_from_html(html: str) -> str | None:
        """Extract the Ashby tenant slug from career page HTML.

        Tries two patterns in order of reliability:

        1. ``"organizationHostedJobsPageName": "<slug>"`` — JSON config blob
           injected by Ashby into the page (most specific).
        2. ``ashbyhq.com/job-board/<slug>`` — URL embedded in a ``<script>``
           tag, iframe ``src``, or anchor ``href``.

        Returns ``None`` when no slug can be found.
        """
        if not html:
            return None
        m = _SLUG_PATTERN_JSON.search(html)
        if m:
            return m.group(1)

        # Find all URL matches and take the first valid one
        for m in _SLUG_PATTERN_URL.finditer(html):
            slug = m.group(1)
            if slug not in ("api", "job-board"):
                return slug

        return None

    @staticmethod
    def build_job_board_query(org_slug: str) -> dict[str, Any]:
        """Build the GraphQL payload for fetching all postings on a board."""
        return {
            "query": _JOB_BOARD_QUERY,
            "variables": {"organizationHostedJobsPageName": org_slug},
        }

    @staticmethod
    def build_single_posting_query(posting_id: str) -> dict[str, Any]:
        """Build the GraphQL payload for fetching a single posting."""
        return {
            "query": _SINGLE_POSTING_QUERY,
            "variables": {"jobPostingId": posting_id},
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_postings(gql_data: dict[str, Any]) -> list[dict[str, Any]]:
        """Pull the postings list from either jobBoard or jobPosting response shapes."""
        job_board = gql_data.get("jobBoard")
        if isinstance(job_board, dict):
            postings = job_board.get("jobPostings")
            if isinstance(postings, list):
                return postings

        single = gql_data.get("jobPosting")
        if isinstance(single, dict):
            return [single]

        return []

    def _parse_posting(self, posting: dict[str, Any]) -> JobListing:
        """Map a single Ashby posting dict to a ``JobListing``."""
        title = posting.get("title")
        if not title:
            msg = "Ashby posting has no title"
            raise ValueError(msg)

        job_url = posting.get("jobUrl") or ""
        if not job_url:
            posting_id = posting.get("id")
            if not posting_id:
                msg = "Ashby posting has neither jobUrl nor id"
                raise ValueError(msg)
            job_url = f"https://jobs.ashbyhq.com/unknown/{posting_id}"

        description = posting.get("descriptionPlain") or _strip_html(
            posting.get("descriptionHtml", "")
        )

        location = self._build_location(posting)

        return JobListing(
            title=title,
            url=job_url,
            location=location,
            department=posting.get("departmentName"),
            employment_type=posting.get("employmentType"),
            description=description or None,
            requirements=_extract_requirements_from_html(posting.get("descriptionHtml", "")),
            salary_range=posting.get("compensationTierSummary"),
            posted_date=_parse_iso_date(posting.get("publishedDate")),
            ats_provider=ATSProvider.ASHBY,
            raw_data=posting,
        )

    @staticmethod
    def _build_location(posting: dict[str, Any]) -> str | None:
        """Combine primary location with remote flag and secondary locations."""
        primary_raw = posting.get("locationName")
        is_remote = (
            posting.get("isRemote", False)
            or str(posting.get("workplaceType", "")).lower() == "remote"
        )

        if not primary_raw and is_remote:
            return "Remote"
        if not primary_raw:
            return None

        loc = str(primary_raw)
        if is_remote and "remote" not in loc.lower():
            return f"{loc} (Remote)"

        return loc


def _extract_requirements_from_html(html: str) -> list[str]:
    """Extract list items from HTML description as requirements.

    Looks for ``<li>`` elements in sections following headings that
    contain keywords like "requirements", "qualifications", etc.
    Falls back to extracting all ``<li>`` items if no section markers found.
    """
    if not html:
        return []

    items: list[str] = []
    for raw_item in _LI_PATTERN.findall(html):
        cleaned = _TAG_PATTERN.sub("", raw_item).strip()
        if cleaned:
            items.append(cleaned)

    return items


def _strip_html(html: str) -> str:
    """Remove HTML tags, returning plain text."""
    if not html:
        return ""
    return _TAG_PATTERN.sub("", html).strip()


def _parse_iso_date(date_str: str | None) -> datetime | None:
    """Parse an ISO 8601 date string to a timezone-aware datetime."""
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt
    except (ValueError, TypeError):
        return None
