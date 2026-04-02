"""Helpers for mocking robots.txt alongside career-page fetches (PCC-1610)."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any
from unittest.mock import AsyncMock, patch
from urllib.parse import urlparse

from strata_harvest.models import FetchResult

PERMISSIVE_ROBOTS_TXT = "User-agent: *\nDisallow:\n"


@contextmanager
def patch_all_safe_fetch(mock: AsyncMock):
    """Patch every module that binds ``safe_fetch`` so robots + page fetches use *mock*."""
    with (
        patch("strata_harvest.crawler.safe_fetch", mock),
        patch("strata_harvest.detector.safe_fetch", mock),
        patch("strata_harvest.utils.robots.safe_fetch", mock),
    ):
        yield mock


def is_robots_txt_url(url: str) -> bool:
    """True if *url* targets a robots.txt resource."""
    path = (urlparse(url).path or "").rstrip("/")
    return path.endswith("/robots.txt") or path == "/robots.txt"


def make_fetch_with_robots(
    *,
    page: FetchResult,
    robots_txt: str = PERMISSIVE_ROBOTS_TXT,
) -> AsyncMock:
    """Return an AsyncMock *safe_fetch* that serves *robots_txt* then *page* for other URLs."""

    async def fetch(url: str, **kwargs: Any) -> FetchResult:
        if is_robots_txt_url(url):
            return FetchResult(
                url=url,
                status_code=200,
                content=robots_txt,
                content_type="text/plain",
                elapsed_ms=1.0,
            )
        return page

    return AsyncMock(side_effect=fetch)
