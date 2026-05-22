"""Personio ATS parser — XML positions feed extraction.

Personio job boards expose a public XML feed at
``https://{slug}.jobs.personio.de/xml`` (also ``.com``) whose root element is
``<workzag-jobs>`` (Personio's legacy internal codename) and which contains one
``<position>`` element per posting.

URL: {slug}.jobs.personio.de
Feed: {slug}.jobs.personio.de/xml
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

from strata_harvest.models import ATSProvider, JobListing
from strata_harvest.parsers.base import BaseParser

if TYPE_CHECKING:
    from xml.etree.ElementTree import Element

logger = logging.getLogger(__name__)


class PersonioParser(BaseParser):
    """Parse job listings from Personio career pages.

    Accepts the ``/xml`` positions feed.  Each ``<position>`` element is mapped
    to a :class:`~strata_harvest.models.JobListing`.  Non-XML input, error
    responses, and feeds with no positions yield an empty list.
    """

    provider = ATSProvider.PERSONIO

    def parse(self, content: str, *, url: str) -> list[JobListing]:
        """Parse a Personio XML positions feed into job listings."""
        if not content or not content.strip():
            return []

        try:
            root = ET.fromstring(content)  # noqa: S314 — stdlib expat disables external entities
        except ET.ParseError:
            logger.debug("Content is not valid XML for Personio parser")
            return []

        positions = root.findall(".//position")
        if not positions:
            logger.debug("Personio feed contains no <position> elements")
            return []

        results: list[JobListing] = []
        for position in positions:
            try:
                listing = self._parse_position(position, url)
                results.append(listing)
            except Exception:
                logger.debug(
                    "Skipping malformed Personio position: %s",
                    _text(position.find("id")) or "?",
                )
                continue

        return results

    @staticmethod
    def build_api_url(url: str) -> str:
        """Convert a Personio career-page URL to the XML positions feed endpoint."""
        parsed = urlparse(url)
        if parsed.path.rstrip("/").endswith("/xml"):
            return url
        return f"https://{parsed.netloc}/xml"

    def _parse_position(self, position: Element, source_url: str) -> JobListing:
        """Map one Personio ``<position>`` element to a JobListing."""
        title = _text(position.find("name"))
        if not title:
            msg = "Personio position missing name"
            raise ValueError(msg)

        job_id = _text(position.find("id"))
        parsed = urlparse(source_url)
        base_host = f"https://{parsed.netloc}"
        job_url = f"{base_host}/job/{job_id}" if job_id else base_host

        location = _text(position.find("office"))
        department = _text(position.find("department"))
        employment_type = _text(position.find("employmentType")) or _text(
            position.find("schedule")
        )
        description = _join_descriptions(position) or None
        posted_date = _parse_iso(_text(position.find("createdAt")))

        return JobListing(
            title=title,
            url=job_url,
            location=location,
            department=department,
            employment_type=employment_type,
            description=description,
            posted_date=posted_date,
            ats_provider=ATSProvider.PERSONIO,
            raw_data={child.tag: (child.text or "") for child in position},
        )


def _text(element: Element | None) -> str | None:
    """Return stripped text content of an element, or None when empty/absent."""
    if element is None or element.text is None:
        return None
    stripped = element.text.strip()
    return stripped or None


def _join_descriptions(position: Element) -> str:
    """Concatenate all ``<jobDescription>`` value blocks into plain text."""
    parts: list[str] = []
    for value in position.findall(".//jobDescription/value"):
        if value.text:
            cleaned = _strip_tags(value.text).strip()
            if cleaned:
                parts.append(cleaned)
    return "\n\n".join(parts)


def _strip_tags(html: str) -> str:
    import re

    return re.sub(r"<[^>]+>", "", html).strip()


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts or not isinstance(ts, str):
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
