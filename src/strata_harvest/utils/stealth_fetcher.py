"""Scrapling-based stealth fetcher — optional stealth extra.

Wraps ``scrapling.StealthyFetcher`` to produce :class:`~strata_harvest.models.FetchResult`
objects compatible with the rest of the strata_harvest pipeline.

Used as **tier 3** in the tiered fetcher escalation stack:

- Tier 0: ATS JSON/GraphQL API (httpx async)
- Tier 1: Static HTML with known ATS shell (httpx + selectolax)
- Tier 2: Bot-protected / 403 on tier 1 (curl_cffi — separate ticket)
- **Tier 3: Stealth / Cloudflare Turnstile / dynamic ← this module**
- Tier 4: Heavy SPA (Crawl4AI)
- Tier 5: LLM fallback on pruned content

Requires the ``stealth`` extra::

    pip install strata-harvest[stealth]

Usage::

    from strata_harvest.utils.stealth_fetcher import StealthFetcher

    fetcher = StealthFetcher(timeout=45)
    result = await fetcher.fetch("https://cloudflare-protected.example.com/careers")
    if result.ok:
        print(result.content)

"""

from __future__ import annotations

import logging
import time

from strata_harvest.models import FetchResult

logger = logging.getLogger(__name__)

try:
    from scrapling.fetchers import StealthyFetcher as _ScraplingStealthyFetcher

    _SCRAPLING_AVAILABLE = True
except ImportError:
    _SCRAPLING_AVAILABLE = False


class StealthFetcher:
    """Fetch bot-protected pages using scrapling's ``StealthyFetcher``.

    Wraps the scrapling library to return a :class:`~strata_harvest.models.FetchResult`
    consistent with the rest of the strata_harvest pipeline.  Never raises —
    transport errors and scrapling failures are captured in the returned result.

    Parameters
    ----------
    timeout:
        Maximum seconds to wait for the page to load (default ``45``).
        Converted to milliseconds when passed to scrapling internally.

    Raises
    ------
    ImportError
        On instantiation when scrapling is not installed.

    Examples
    --------
    >>> import asyncio
    >>> from strata_harvest.utils.stealth_fetcher import StealthFetcher
    >>> async def demo() -> None:
    ...     fetcher = StealthFetcher()
    ...     result = await fetcher.fetch("https://example.com/careers")
    ...     assert isinstance(result.ok, bool)
    >>> asyncio.run(demo())  # doctest: +SKIP
    """

    def __init__(self, *, timeout: int = 45) -> None:
        if not _SCRAPLING_AVAILABLE:
            raise ImportError(
                "scrapling is not installed. Run: pip install 'strata-harvest[stealth]'"
            )
        self._timeout = timeout

    async def fetch(self, url: str) -> FetchResult:
        """Fetch *url* via scrapling's StealthyFetcher and return a :class:`FetchResult`.

        Never raises.  On any scrapling failure the returned result has ``ok=False``
        and ``error`` set to a description of the failure.

        Parameters
        ----------
        url:
            The URL to fetch with stealth mode enabled.

        Returns
        -------
        FetchResult
            Populated with ``status_code``, ``content``, and ``elapsed_ms`` on
            success; ``error`` set on failure.
        """
        logger.debug("StealthFetcher: fetching %s (timeout=%ds)", url, self._timeout)
        start_ms = _now_ms()

        try:
            # StealthyFetcher uses playwright/camoufox under the hood.
            # timeout is in milliseconds (Playwright convention).
            fetcher = _ScraplingStealthyFetcher(timeout=self._timeout * 1000)
            page = await fetcher.async_fetch(url)
        except Exception as exc:
            elapsed = _now_ms() - start_ms
            logger.warning("StealthFetcher failed for %s: %s", url, exc, exc_info=True)
            return FetchResult(
                url=url,
                error=f"{type(exc).__name__}: {exc}",
                elapsed_ms=elapsed,
            )

        elapsed = _now_ms() - start_ms
        status: int | None = getattr(page, "status", None)
        content: str = getattr(page, "html_content", None) or ""

        logger.debug(
            "StealthFetcher: %s → status=%s, %d bytes in %dms",
            url,
            status,
            len(content),
            elapsed,
        )

        if status is not None and status >= 400:
            return FetchResult(
                url=url,
                status_code=status,
                error=f"HTTP {status}",
                elapsed_ms=elapsed,
            )

        return FetchResult(
            url=url,
            status_code=status,
            content=content if content else None,
            content_type="text/html",
            elapsed_ms=elapsed,
        )


async def stealth_fetch(url: str, *, timeout: int = 45) -> FetchResult:
    """One-shot convenience wrapper around :class:`StealthFetcher`.

    Builds a fetcher, fetches *url*, and returns the :class:`FetchResult`.

    Parameters
    ----------
    url:
        Career page URL to fetch with stealth mode.
    timeout:
        Page load timeout in seconds (default ``45``).

    Returns
    -------
    FetchResult
        With content and status populated on success; ``error`` set on failure.

    Examples
    --------
    >>> import asyncio
    >>> from strata_harvest.utils.stealth_fetcher import stealth_fetch
    >>> async def demo() -> None:
    ...     result = await stealth_fetch("https://example.com/careers")
    ...     assert isinstance(result.ok, bool)
    >>> asyncio.run(demo())  # doctest: +SKIP
    """
    fetcher = StealthFetcher(timeout=timeout)
    return await fetcher.fetch(url)


def _now_ms() -> int:
    return int(time.monotonic() * 1000)
