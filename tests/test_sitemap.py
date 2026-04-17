"""Tests for sitemap discovery and parsing (PCC-1954)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from strata_harvest.models import FetchResult
from strata_harvest.utils.sitemap import (
    SitemapEntry,
    SitemapFinder,
    SitemapLastmodTracker,
    _extract_sitemap_entries,
    _sitemap_url,
)


class TestSitemapUrlGeneration:
    """Test sitemap URL generation helpers."""

    def test_sitemap_url_https(self) -> None:
        """Generate /sitemap.xml URL for HTTPS domain."""
        url = _sitemap_url("https://example.com/careers")
        assert url == "https://example.com/sitemap.xml"

    def test_sitemap_url_http(self) -> None:
        """Generate /sitemap.xml URL for HTTP domain."""
        url = _sitemap_url("http://example.com/careers")
        assert url == "http://example.com/sitemap.xml"

    def test_sitemap_url_with_port(self) -> None:
        """Preserve port in sitemap URL."""
        url = _sitemap_url("https://example.com:8443/careers")
        assert url == "https://example.com:8443/sitemap.xml"

    def test_sitemap_url_default_scheme(self) -> None:
        """Default to https when scheme is missing."""
        url = _sitemap_url("//example.com/careers")
        assert url == "https://example.com/sitemap.xml"


class TestSitemapEntry:
    """Test SitemapEntry model and methods."""

    def test_lastmod_datetime_iso_date(self) -> None:
        """Parse ISO date format."""
        entry = SitemapEntry(url="https://example.com/job1", lastmod="2024-01-15")
        dt = entry.lastmod_datetime()
        assert dt is not None
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 15

    def test_lastmod_datetime_iso_datetime(self) -> None:
        """Parse ISO datetime format with timezone."""
        entry = SitemapEntry(url="https://example.com/job1", lastmod="2024-01-15T12:30:45+00:00")
        dt = entry.lastmod_datetime()
        assert dt is not None
        assert dt.year == 2024
        assert dt.hour == 12

    def test_lastmod_datetime_iso_datetime_z(self) -> None:
        """Parse ISO datetime with Z suffix."""
        entry = SitemapEntry(url="https://example.com/job1", lastmod="2024-01-15T12:30:45Z")
        dt = entry.lastmod_datetime()
        assert dt is not None
        assert dt.year == 2024

    def test_lastmod_datetime_none(self) -> None:
        """Return None when lastmod is missing."""
        entry = SitemapEntry(url="https://example.com/job1")
        assert entry.lastmod_datetime() is None

    def test_lastmod_datetime_invalid(self) -> None:
        """Return None for invalid lastmod."""
        entry = SitemapEntry(url="https://example.com/job1", lastmod="invalid-date")
        assert entry.lastmod_datetime() is None


class TestSitemapParsing:
    """Test sitemap XML parsing."""

    def test_extract_sitemap_regular(self) -> None:
        """Parse regular sitemap.xml with URLs and lastmod."""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://example.com/jobs/1</loc>
    <lastmod>2024-01-15</lastmod>
    <changefreq>weekly</changefreq>
    <priority>0.8</priority>
  </url>
  <url>
    <loc>https://example.com/jobs/2</loc>
    <lastmod>2024-01-14</lastmod>
  </url>
</urlset>
"""
        entries = _extract_sitemap_entries(xml_content)
        assert len(entries) == 2
        assert entries[0].url == "https://example.com/jobs/1"
        assert entries[0].lastmod == "2024-01-15"
        assert entries[0].changefreq == "weekly"
        assert entries[0].priority == "0.8"
        assert entries[1].url == "https://example.com/jobs/2"

    def test_extract_sitemap_index(self) -> None:
        """Parse sitemap_index.xml with references to other sitemaps."""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap>
    <loc>https://example.com/sitemap-jobs.xml</loc>
    <lastmod>2024-01-15</lastmod>
  </sitemap>
  <sitemap>
    <loc>https://example.com/sitemap-pages.xml</loc>
  </sitemap>
</sitemapindex>
"""
        entries = _extract_sitemap_entries(xml_content)
        assert len(entries) == 2
        assert entries[0].url == "https://example.com/sitemap-jobs.xml"
        assert entries[0].lastmod == "2024-01-15"
        assert entries[1].url == "https://example.com/sitemap-pages.xml"

    def test_extract_sitemap_empty(self) -> None:
        """Return empty list for empty sitemap."""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
</urlset>
"""
        entries = _extract_sitemap_entries(xml_content)
        assert len(entries) == 0

    def test_extract_sitemap_invalid_xml(self) -> None:
        """Return empty list for invalid XML."""
        entries = _extract_sitemap_entries("<invalid>xml")
        assert len(entries) == 0

    def test_extract_sitemap_greenhouse_real_world(self) -> None:
        """Test with a real Greenhouse board dump format."""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://boards.greenhouse.io/example/jobs/123</loc>
    <lastmod>2024-02-20T10:00:00Z</lastmod>
  </url>
  <url>
    <loc>https://boards.greenhouse.io/example/jobs/124</loc>
    <lastmod>2024-02-19T15:30:00Z</lastmod>
  </url>
</urlset>
"""
        entries = _extract_sitemap_entries(xml_content)
        assert len(entries) == 2
        assert all(entry.lastmod is not None for entry in entries)
        assert entries[0].url.endswith("/123")


class TestSitemapLastmodTracker:
    """Test incremental crawling via lastmod tracking."""

    def test_should_fetch_new_url(self) -> None:
        """New URLs should always be fetched."""
        tracker = SitemapLastmodTracker()
        assert tracker.should_fetch("https://example.com/job/1", "2024-01-15") is True

    def test_should_fetch_changed_lastmod(self) -> None:
        """URL with changed lastmod should be fetched."""
        tracker = SitemapLastmodTracker()
        tracker.update("https://example.com/job/1", "2024-01-15")
        assert tracker.should_fetch("https://example.com/job/1", "2024-01-16") is True

    def test_should_skip_unchanged_lastmod(self) -> None:
        """URL with same lastmod should be skipped."""
        tracker = SitemapLastmodTracker()
        tracker.update("https://example.com/job/1", "2024-01-15")
        assert tracker.should_fetch("https://example.com/job/1", "2024-01-15") is False

    def test_should_fetch_no_lastmod_available(self) -> None:
        """No lastmod available should fetch to be safe."""
        tracker = SitemapLastmodTracker()
        tracker.update("https://example.com/job/1", "2024-01-15")
        assert tracker.should_fetch("https://example.com/job/1", None) is True

    def test_get_lastmod(self) -> None:
        """Retrieve stored lastmod for URL."""
        tracker = SitemapLastmodTracker()
        tracker.update("https://example.com/job/1", "2024-01-15")
        assert tracker.get_lastmod("https://example.com/job/1") == "2024-01-15"

    def test_get_lastmod_not_found(self) -> None:
        """Return None for unknown URL."""
        tracker = SitemapLastmodTracker()
        assert tracker.get_lastmod("https://example.com/unknown") is None

    def test_update_clears_none_lastmod(self) -> None:
        """Updating with None removes from tracking."""
        tracker = SitemapLastmodTracker()
        tracker.update("https://example.com/job/1", "2024-01-15")
        tracker.update("https://example.com/job/1", None)
        assert tracker.get_lastmod("https://example.com/job/1") is None


def _mock_response(
    *,
    status_code: int = 200,
    text: str = "",
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    """Build a mock httpx.Response."""
    resp_headers = headers or {}
    request = httpx.Request("GET", "https://example.com")
    return httpx.Response(
        status_code=status_code,
        text=text,
        headers=resp_headers,
        request=request,
    )


def _stream_cm(mock_resp: httpx.Response) -> MagicMock:
    """Async context manager yielded by ``AsyncClient.stream()``."""
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=mock_resp)
    cm.__aexit__ = AsyncMock(return_value=None)
    return cm


@pytest.mark.verification
class TestSitemapFinder:
    """Test SitemapFinder discovery and caching."""

    async def test_find_job_urls_success(self) -> None:
        """Successfully discover and parse sitemap."""
        sitemap_content = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://example.com/jobs/1</loc>
    <lastmod>2024-01-15</lastmod>
  </url>
  <url>
    <loc>https://example.com/jobs/2</loc>
  </url>
</urlset>
"""
        _mock_response(text=sitemap_content)

        with patch("strata_harvest.utils.sitemap.safe_fetch") as mock_fetch:
            mock_fetch.return_value = FetchResult(
                url="https://example.com/sitemap.xml",
                status_code=200,
                content=sitemap_content,
            )

            finder = SitemapFinder()
            entries = await finder.find_job_urls(
                "https://example.com/careers",
                timeout=10,
                allow_private=False,
            )

        assert len(entries) == 2
        assert entries[0].url == "https://example.com/jobs/1"
        assert entries[0].lastmod == "2024-01-15"

    async def test_find_job_urls_cached(self) -> None:
        """Sitemap results are cached per domain."""
        sitemap_content = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://example.com/jobs/1</loc>
  </url>
</urlset>
"""

        with patch("strata_harvest.utils.sitemap.safe_fetch") as mock_fetch:
            mock_fetch.return_value = FetchResult(
                url="https://example.com/sitemap.xml",
                status_code=200,
                content=sitemap_content,
            )

            finder = SitemapFinder(ttl_seconds=3600)

            # First call
            entries1 = await finder.find_job_urls(
                "https://example.com/careers",
                timeout=10,
                allow_private=False,
            )
            assert len(entries1) == 1

            # Second call should use cache
            entries2 = await finder.find_job_urls(
                "https://example.com/careers",
                timeout=10,
                allow_private=False,
            )
            assert entries1 == entries2
            # Should only have been called once due to caching
            assert mock_fetch.call_count == 1

    async def test_find_job_urls_no_sitemap(self) -> None:
        """Return empty list when no sitemap found."""
        with patch("strata_harvest.utils.sitemap.safe_fetch") as mock_fetch:
            mock_fetch.return_value = FetchResult(
                url="https://example.com/sitemap.xml",
                status_code=404,
                error="Not Found",
            )

            finder = SitemapFinder()
            entries = await finder.find_job_urls(
                "https://example.com/careers",
                timeout=10,
                allow_private=False,
            )

        assert len(entries) == 0

    async def test_find_job_urls_invalid_scheme(self) -> None:
        """Return empty list for invalid URL schemes."""
        finder = SitemapFinder()
        entries = await finder.find_job_urls(
            "ftp://example.com/careers",
            timeout=10,
            allow_private=False,
        )
        assert len(entries) == 0

    async def test_find_job_urls_sitemap_index_recursive(self) -> None:
        """Handle sitemap_index.xml with recursive sitemap fetches."""
        index_content = """<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap>
    <loc>https://example.com/sitemap-jobs.xml</loc>
  </sitemap>
</sitemapindex>
"""
        jobs_content = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://example.com/jobs/1</loc>
  </url>
</urlset>
"""

        fetch_count = 0

        async def mock_fetch_side_effect(url, **kwargs):
            nonlocal fetch_count
            fetch_count += 1
            # First call to /sitemap.xml returns the index
            if "sitemap.xml" in url and "sitemap-jobs.xml" not in url:
                return FetchResult(url=url, status_code=200, content=index_content)
            # Call to /sitemap-jobs.xml returns the jobs
            if "sitemap-jobs.xml" in url:
                return FetchResult(url=url, status_code=200, content=jobs_content)
            # Other calls return 404
            return FetchResult(url=url, status_code=404, error="Not Found")

        with patch("strata_harvest.utils.sitemap.safe_fetch") as mock_fetch:
            mock_fetch.side_effect = mock_fetch_side_effect

            finder = SitemapFinder()
            entries = await finder.find_job_urls(
                "https://example.com/careers",
                timeout=10,
                allow_private=False,
            )

        assert len(entries) == 1
        assert entries[0].url == "https://example.com/jobs/1"
        # Should have made at least 2 calls: /sitemap.xml (index) and /sitemap-jobs.xml (jobs)
        assert fetch_count >= 2
