"""robots.txt compliance — fetch, cache, and evaluate via stdlib RobotFileParser."""

from __future__ import annotations

import asyncio
import logging
import time
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

from strata_harvest.utils.http import DEFAULT_USER_AGENT, safe_fetch

logger = logging.getLogger(__name__)

DEFAULT_ROBOTS_CACHE_TTL_S: float = 3600.0


def _robots_txt_url(page_url: str) -> str:
    parsed = urlparse(page_url)
    scheme = parsed.scheme or "https"
    return f"{scheme}://{parsed.netloc}/robots.txt"


def _domain_key(page_url: str) -> str:
    parsed = urlparse(page_url)
    return (parsed.netloc or "").lower()


class RobotsTxtChecker:
    """Fetch robots.txt per origin via :func:`~strata_harvest.utils.http.safe_fetch`, parse with
    :class:`urllib.robotparser.RobotFileParser`, and cache by netloc with a TTL.

    When robots.txt cannot be fetched or parsed, crawling is allowed (permissive default),
    matching typical crawler behavior when no rules apply.
    """

    def __init__(
        self,
        *,
        ttl_seconds: float = DEFAULT_ROBOTS_CACHE_TTL_S,
        user_agent: str | None = None,
    ) -> None:
        self._ttl = ttl_seconds
        self._user_agent = user_agent or DEFAULT_USER_AGENT
        self._cache: dict[str, tuple[RobotFileParser, float]] = {}
        self._lock = asyncio.Lock()

    @property
    def cache_ttl_seconds(self) -> float:
        """TTL for cached ``robots.txt`` parsers (seconds)."""
        return self._ttl

    @property
    def user_agent(self) -> str:
        """Effective User-Agent string used for robots.txt evaluation and fetches."""
        return self._user_agent

    async def can_fetch(
        self,
        page_url: str,
        *,
        timeout: float,
        allow_private: bool = False,
    ) -> bool:
        """Return True if *page_url* is allowed for this checker's user-agent per cached rules."""
        parsed = urlparse(page_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            return True

        dk = _domain_key(page_url)
        now = time.monotonic()

        async with self._lock:
            entry = self._cache.get(dk)
            if entry is not None:
                rp, expires = entry
                if now < expires:
                    return rp.can_fetch(self._user_agent, page_url)

        robots_url = _robots_txt_url(page_url)
        result = await safe_fetch(
            robots_url,
            timeout=timeout,
            headers={"User-Agent": self._user_agent},
            allow_private=allow_private,
        )

        rp = RobotFileParser()
        rp.set_url(robots_url)
        if result.ok and result.content:
            rp.parse(result.content.splitlines())
        else:
            logger.debug(
                "robots.txt unavailable for %s (%s) — allowing fetch per permissive default",
                robots_url,
                result.error or f"HTTP {result.status_code}",
            )

        now = time.monotonic()
        async with self._lock:
            stale = self._cache.get(dk)
            if stale is not None:
                _rp, exp = stale
                if now < exp:
                    return _rp.can_fetch(self._user_agent, page_url)
            self._cache[dk] = (rp, now + self._ttl)

        return rp.can_fetch(self._user_agent, page_url)
