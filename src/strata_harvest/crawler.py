"""Career page crawler — public API surface.

Provides:
- ``Crawler``: configured, reusable scraper with rate limiting
- ``create_crawler()``: factory with full configuration knobs
- ``harvest(url)``: one-shot convenience that auto-detects ATS
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from strata_harvest.detector import detect_ats
from strata_harvest.models import ATSProvider, JobListing, ScrapeResult
from strata_harvest.parsers.base import BaseParser
from strata_harvest.utils.hashing import content_hash
from strata_harvest.utils.http import safe_fetch
from strata_harvest.utils.rate_limiter import RateLimiter

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

_DEFAULT_RATE_LIMIT: float = 0.5  # requests per second → 2.0s between requests
_DEFAULT_TIMEOUT: float = 30.0
_DEFAULT_CONCURRENCY: int = 5


class Crawler:
    """Stateful crawler for repeated career-page scraping with shared settings.

    Obtain instances via :func:`create_crawler` rather than calling this
    constructor directly, so defaults stay consistent across releases.

    Parameters
    ----------
    rate_limit:
        Maximum requests per second (default ``0.5`` → one request every 2 s).
    timeout:
        Per-request HTTP timeout in seconds.
    user_agent:
        Optional ``User-Agent`` header for fetches.
    headless:
        Reserved for future headless-browser rendering.
    proxy:
        Reserved for future HTTP(S) proxy support.
    llm_provider:
        LiteLLM model id for the LLM fallback parser when ATS is unknown
        (e.g. ``openai/gpt-4o-mini``). Requires the ``llm`` extra.
    """

    def __init__(
        self,
        *,
        rate_limit: float = _DEFAULT_RATE_LIMIT,
        timeout: float = _DEFAULT_TIMEOUT,
        user_agent: str | None = None,
        headless: bool = False,
        proxy: str | None = None,
        llm_provider: str | None = None,
    ) -> None:
        self._rate_limiter = RateLimiter(requests_per_second=rate_limit)
        self._timeout = timeout
        self._user_agent = user_agent
        self._headless = headless
        self._proxy = proxy
        self._llm_provider = llm_provider

    async def scrape(self, url: str, *, previous_hash: str | None = None) -> ScrapeResult:
        """Scrape a career page and return structured results.

        Detects the ATS, fetches HTML, parses listings, and computes a content
        hash for change detection when *previous_hash* is supplied.

        Parameters
        ----------
        url:
            Career page or job-board URL to scrape.
        previous_hash:
            If set, compared to the new content hash to populate
            :attr:`ScrapeResult.changed`.

        Returns
        -------
        ScrapeResult
            Parsed jobs, ATS metadata, timing, and optional error string.

        Raises
        ------
        Exception
            Rarely, parser or dependency code may raise; HTTP failures are
            represented on the result instead of raising.

        Examples
        --------
        >>> import asyncio
        >>> from strata_harvest.crawler import create_crawler
        >>> async def main() -> None:
        ...     c = create_crawler(timeout=30.0)
        ...     r = await c.scrape("https://boards.greenhouse.io/example/jobs")
        ...     assert r.url.endswith("/jobs")
        >>> asyncio.run(main())  # doctest: +SKIP
        """
        await self._rate_limiter.acquire()

        ats_info = await detect_ats(url, timeout=self._timeout, user_agent=self._user_agent)
        parser = self._get_parser(ats_info.provider)

        fetch_headers = {"User-Agent": self._user_agent} if self._user_agent else None
        result = await safe_fetch(url, timeout=self._timeout, headers=fetch_headers)
        if not result.ok:
            return ScrapeResult(
                url=url,
                ats_info=ats_info,
                error=result.error or f"HTTP {result.status_code}",
                scrape_duration_ms=result.elapsed_ms,
            )

        page_hash = content_hash(result.content or "")
        changed = previous_hash is None or page_hash != previous_hash
        jobs = parser.parse(result.content or "", url=url)

        return ScrapeResult(
            url=url,
            jobs=jobs,
            content_hash=page_hash,
            changed=changed,
            ats_info=ats_info,
            scrape_duration_ms=result.elapsed_ms,
        )

    async def scrape_batch(
        self,
        urls: list[str],
        concurrency: int = _DEFAULT_CONCURRENCY,
    ) -> AsyncIterator[ScrapeResult]:
        """Scrape multiple URLs concurrently.

        Uses an :class:`asyncio.Semaphore` to cap parallelism at
        *concurrency*. Each URL is scraped via :meth:`scrape`, which applies
        the crawler's rate limiter; the semaphore bounds concurrent tasks while
        the limiter paces HTTP requests.

        Parameters
        ----------
        urls:
            URLs to scrape (empty list yields nothing).
        concurrency:
            Maximum concurrent scrape tasks.

        Yields
        ------
        ScrapeResult
            One result per input URL (order not guaranteed vs. input order).

        Raises
        ------
        Exception
            Worker failures are captured as :class:`ScrapeResult` with
            ``error`` set where possible; unexpected errors may still propagate.
        """
        if not urls:
            return

        semaphore = asyncio.Semaphore(concurrency)
        queue: asyncio.Queue[ScrapeResult] = asyncio.Queue()
        pending = len(urls)

        async def _worker(url: str) -> None:
            try:
                async with semaphore:
                    result = await self.scrape(url)
                    await queue.put(result)
            except Exception as exc:
                await queue.put(ScrapeResult(url=url, error=f"{type(exc).__name__}: {exc}"))

        tasks = [asyncio.create_task(_worker(u)) for u in urls]

        for _ in range(pending):
            yield await queue.get()

        await asyncio.gather(*tasks, return_exceptions=True)

    def _get_parser(self, provider: ATSProvider) -> BaseParser:
        """Return the appropriate parser, wiring llm_provider if needed."""
        if provider == ATSProvider.UNKNOWN and self._llm_provider:
            from strata_harvest.parsers.llm_fallback import LLMFallbackParser

            return LLMFallbackParser(llm_provider=self._llm_provider)
        return BaseParser.for_provider(provider)


def create_crawler(
    *,
    rate_limit: float = _DEFAULT_RATE_LIMIT,
    timeout: float = _DEFAULT_TIMEOUT,
    user_agent: str | None = None,
    headless: bool = False,
    proxy: str | None = None,
    llm_provider: str | None = None,
) -> Crawler:
    """Build a :class:`Crawler` with explicit tuning knobs.

    Parameters
    ----------
    rate_limit:
        Max requests per second (default ``0.5`` → one request every 2 s).
    timeout:
        Per-request HTTP timeout in seconds.
    user_agent:
        Custom ``User-Agent`` header for fetches.
    headless:
        Reserved for headless-browser rendering (not yet implemented).
    proxy:
        Reserved for HTTP(S) proxy (not yet implemented).
    llm_provider:
        LiteLLM model string for the LLM fallback parser when the ATS cannot
        be identified (e.g. ``openai/gpt-4o-mini``). Install ``strata-harvest[llm]``.

    Returns
    -------
    Crawler
        Configured instance; use :meth:`Crawler.scrape` or :meth:`Crawler.scrape_batch`.

    Examples
    --------
    >>> from strata_harvest.crawler import create_crawler
    >>> crawler = create_crawler(rate_limit=0.25, timeout=45.0)
    >>> crawler  # doctest: +ELLIPSIS
    <strata_harvest.crawler.Crawler object at ...>
    """
    return Crawler(
        rate_limit=rate_limit,
        timeout=timeout,
        user_agent=user_agent,
        headless=headless,
        proxy=proxy,
        llm_provider=llm_provider,
    )


async def harvest(url: str, *, timeout: float = _DEFAULT_TIMEOUT) -> list[JobListing]:
    """Scrape a career page once and return only the parsed job rows.

    Builds a default :class:`Crawler`, runs :meth:`Crawler.scrape`, and returns
    ``result.jobs``. For full diagnostics (HTTP errors, ATS detection, timing),
    use :func:`create_crawler` and inspect :class:`ScrapeResult`.

    Parameters
    ----------
    url:
        Career page or job-board URL.
    timeout:
        Per-request HTTP timeout in seconds passed to the internal crawler.

    Returns
    -------
    list[JobListing]
        Parsed postings. Empty when the fetch failed, parsing found no rows,
        or the page indicated an error (same semantics as filtering
        :attr:`ScrapeResult.jobs` after a failed scrape).

    Raises
    ------
    Exception
        The HTTP stack does not raise on transport errors (they appear on
        :class:`ScrapeResult` when using :meth:`Crawler.scrape`). Parser or
        dependency bugs may still raise.

    Examples
    --------
    >>> import asyncio
    >>> from strata_harvest.crawler import harvest
    >>> async def demo() -> None:
    ...     jobs = await harvest("https://boards.greenhouse.io/invalid-board-xyz/jobs")
    ...     assert isinstance(jobs, list)
    >>> asyncio.run(demo())  # doctest: +SKIP
    """
    crawler = create_crawler(timeout=timeout)
    result = await crawler.scrape(url)
    return result.jobs
