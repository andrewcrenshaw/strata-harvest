"""Sitemap discovery and parsing for incremental crawling with lastmod tracking."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

from strata_harvest.utils.http import DEFAULT_USER_AGENT, safe_fetch

logger = logging.getLogger(__name__)

DEFAULT_SITEMAP_CACHE_TTL_S: float = 3600.0

# XML namespace for sitemap protocol
SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


@dataclass
class SitemapEntry:
    """Single URL entry from a sitemap."""

    url: str
    lastmod: str | None = None
    changefreq: str | None = None
    priority: str | None = None

    def lastmod_datetime(self) -> datetime | None:
        """Parse lastmod as datetime if present, else None."""
        if not self.lastmod:
            return None
        try:
            # Handles both date and datetime formats per sitemap spec
            # 2004-10-01 or 2004-10-01T18:23:17+00:00
            if "T" in self.lastmod:
                return datetime.fromisoformat(self.lastmod.replace("Z", "+00:00"))
            return datetime.fromisoformat(self.lastmod)
        except (ValueError, TypeError):
            return None


def _sitemap_url(page_url: str) -> str:
    """Return the /sitemap.xml URL for a given domain."""
    parsed = urlparse(page_url)
    scheme = parsed.scheme or "https"
    return f"{scheme}://{parsed.netloc}/sitemap.xml"


def _sitemap_index_url(page_url: str) -> str:
    """Return the /sitemap_index.xml URL for a given domain."""
    parsed = urlparse(page_url)
    scheme = parsed.scheme or "https"
    return f"{scheme}://{parsed.netloc}/sitemap_index.xml"


def _sitemal_url(page_url: str) -> str:
    """Return the /sitemal.xml URL (SAP SuccessFactors quirk)."""
    parsed = urlparse(page_url)
    scheme = parsed.scheme or "https"
    return f"{scheme}://{parsed.netloc}/sitemal.xml"


def _domain_key(page_url: str) -> str:
    """Return a cache key for a domain."""
    parsed = urlparse(page_url)
    return (parsed.netloc or "").lower()


def _extract_sitemap_entries(xml_content: str) -> list[SitemapEntry]:
    """Parse sitemap.xml or sitemap_index.xml and return entries.

    For sitemap.xml: returns URL entries with optional lastmod.
    For sitemap_index.xml: returns sitemap URLs (without lastmod).
    """
    entries: list[SitemapEntry] = []
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        logger.warning("Failed to parse sitemap XML: %s", e)
        return entries

    # Determine if this is a sitemap index or regular sitemap
    # Both use the same namespace
    namespace = SITEMAP_NS

    # Try to find url elements (regular sitemap)
    urls = root.findall(".//sm:url", namespace)
    if urls:
        for url_elem in urls:
            loc = url_elem.findtext("sm:loc", default=None, namespaces=namespace)
            if loc:
                lastmod = url_elem.findtext("sm:lastmod", default=None, namespaces=namespace)
                changefreq = url_elem.findtext("sm:changefreq", default=None, namespaces=namespace)
                priority = url_elem.findtext("sm:priority", default=None, namespaces=namespace)
                entries.append(
                    SitemapEntry(
                        url=loc,
                        lastmod=lastmod,
                        changefreq=changefreq,
                        priority=priority,
                    )
                )
    else:
        # Try sitemap index elements
        sitemaps = root.findall(".//sm:sitemap", namespace)
        for sm_elem in sitemaps:
            loc = sm_elem.findtext("sm:loc", default=None, namespaces=namespace)
            if loc:
                lastmod = sm_elem.findtext("sm:lastmod", default=None, namespaces=namespace)
                entries.append(SitemapEntry(url=loc, lastmod=lastmod))

    return entries


class SitemapFinder:
    """Discover and cache sitemaps for a domain with TTL.

    Attempts to fetch sitemaps in order:
    1. /sitemap.xml
    2. /sitemap_index.xml
    3. /sitemal.xml (SAP SuccessFactors quirk)

    Caches results per domain with configurable TTL.
    When a sitemap index is found, recursively fetches referenced sitemaps.
    """

    def __init__(
        self,
        *,
        ttl_seconds: float = DEFAULT_SITEMAP_CACHE_TTL_S,
        user_agent: str | None = None,
    ) -> None:
        self._ttl = ttl_seconds
        self._user_agent = user_agent or DEFAULT_USER_AGENT
        self._cache: dict[str, tuple[list[SitemapEntry], float]] = {}
        self._lock = asyncio.Lock()

    @property
    def cache_ttl_seconds(self) -> float:
        """TTL for cached sitemaps (seconds)."""
        return self._ttl

    async def find_job_urls(
        self,
        page_url: str,
        *,
        timeout: float,
        allow_private: bool = False,
    ) -> list[SitemapEntry]:
        """Discover all job URLs for a domain via sitemaps.

        Returns an empty list if no sitemap is found or cannot be parsed.
        Handles both sitemap.xml and sitemap_index.xml recursively.

        Parameters
        ----------
        page_url:
            Career page or job-board URL (domain is extracted from this).
        timeout:
            HTTP timeout in seconds.
        allow_private:
            Allow fetches to private addresses (SSRF mitigation).

        Returns
        -------
        list[SitemapEntry]
            All unique job URLs discovered from sitemaps, with lastmod if available.
        """
        parsed = urlparse(page_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            return []

        dk = _domain_key(page_url)
        now = time.monotonic()

        async with self._lock:
            entry = self._cache.get(dk)
            if entry is not None:
                entries, expires = entry
                if now < expires:
                    return entries

        # Try to find a sitemap
        entries = await self._fetch_sitemap_recursive(
            page_url, timeout=timeout, allow_private=allow_private
        )

        now = time.monotonic()
        async with self._lock:
            self._cache[dk] = (entries, now + self._ttl)

        return entries

    async def _fetch_sitemap_recursive(
        self,
        page_url: str,
        *,
        timeout: float,
        allow_private: bool,
        visited: set[str] | None = None,
        _is_direct_url: bool = False,
    ) -> list[SitemapEntry]:
        """Recursively fetch and parse sitemaps, handling both index and regular formats.

        Prevents infinite loops by tracking visited sitemap URLs.

        Parameters
        ----------
        page_url:
            Career page URL (domain is extracted) or direct sitemap URL
        _is_direct_url:
            Internal flag: if True, page_url is a direct sitemap URL to fetch
        """
        if visited is None:
            visited = set()

        all_entries: list[SitemapEntry] = []

        # If this is a direct sitemap URL (from recursion), just fetch it
        if _is_direct_url:
            if page_url in visited:
                return []
            visited.add(page_url)

            result = await safe_fetch(
                page_url,
                timeout=timeout,
                headers={"User-Agent": self._user_agent},
                allow_private=allow_private,
            )

            if result.ok and result.content:
                entries = _extract_sitemap_entries(result.content)
                if entries:
                    return entries
            return []

        # Try fetching sitemaps in order of preference from the domain
        for sitemap_url_func in [_sitemap_url, _sitemap_index_url, _sitemal_url]:
            sitemap_url = sitemap_url_func(page_url)

            if sitemap_url in visited:
                continue
            visited.add(sitemap_url)

            result = await safe_fetch(
                sitemap_url,
                timeout=timeout,
                headers={"User-Agent": self._user_agent},
                allow_private=allow_private,
            )

            if not result.ok or not result.content:
                continue

            entries = _extract_sitemap_entries(result.content)
            if not entries:
                continue

            # Check if this is a sitemap index (entries are other sitemap URLs)
            # A simple heuristic: if entries have no lastmod and all URLs end with .xml,
            # it's likely a sitemap index
            is_index = all(
                entry.url.endswith(".xml") and entry.lastmod is None for entry in entries
            )

            if is_index:
                # Recursively fetch referenced sitemaps
                for entry in entries:
                    sub_entries = await self._fetch_sitemap_recursive(
                        entry.url,
                        timeout=timeout,
                        allow_private=allow_private,
                        visited=visited,
                        _is_direct_url=True,
                    )
                    all_entries.extend(sub_entries)
            else:
                # Regular sitemap with job URLs
                all_entries.extend(entries)

            # Found a successful sitemap, return (prefer single file over index)
            if all_entries:
                break

        return all_entries


class SitemapLastmodTracker:
    """Track last-seen lastmod per URL for incremental crawling.

    Stores URL -> lastmod mappings in memory. When crawling a URL again,
    compare the new lastmod from sitemap with the previous one to skip
    unchanged pages.
    """

    def __init__(self) -> None:
        self._lastmods: dict[str, str] = {}

    def should_fetch(
        self,
        url: str,
        current_lastmod: str | None,
    ) -> bool:
        """Return True if URL should be fetched (lastmod changed or new URL).

        Parameters
        ----------
        url:
            The URL to check.
        current_lastmod:
            The lastmod value from the sitemap (or None if not available).

        Returns
        -------
        bool
            True if the URL is new or lastmod changed, False if unchanged.
        """
        previous_lastmod = self._lastmods.get(url)
        if previous_lastmod is None:
            # New URL, always fetch
            return True

        if current_lastmod is None:
            # No lastmod available, fetch to be safe
            return True

        # Compare lastmods: skip if unchanged
        return current_lastmod != previous_lastmod

    def update(self, url: str, lastmod: str | None) -> None:
        """Record the lastmod for a URL."""
        if lastmod:
            self._lastmods[url] = lastmod
        else:
            # No lastmod available, remove from tracking
            self._lastmods.pop(url, None)

    def get_lastmod(self, url: str) -> str | None:
        """Retrieve the last-seen lastmod for a URL."""
        return self._lastmods.get(url)
