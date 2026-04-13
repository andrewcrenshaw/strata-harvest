"""Career page crawler — public API surface.

Provides:
- ``Crawler``: configured, reusable scraper with rate limiting
- ``create_crawler()``: factory with full configuration knobs
- ``harvest(url)``: one-shot convenience that auto-detects ATS
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import httpx

from strata_harvest.detector import detect_ats, detect_from_url
from strata_harvest.models import ATSProvider, JobListing, ScrapeResult
from strata_harvest.parsers.ashby import AshbyParser
from strata_harvest.parsers.base import BaseParser
from strata_harvest.parsers.llm_fallback import LLMFallbackParser
from strata_harvest.utils.hashing import content_hash
from strata_harvest.utils.http import safe_fetch
from strata_harvest.utils.rate_limiter import PerDomainRateLimiterRegistry, RateLimiter
from strata_harvest.utils.robots import RobotsTxtChecker

logger = logging.getLogger(__name__)

# API-oriented ATS boards: parsers call vendor APIs; skip robots.txt on the career URL fetch.
_ROBOTS_BYPASS_PROVIDERS: frozenset[ATSProvider] = frozenset(
    (ATSProvider.GREENHOUSE, ATSProvider.LEVER, ATSProvider.ASHBY),
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from strata_harvest.ocr.router import OcrRouter

_DEFAULT_RATE_LIMIT: float = 0.5  # global max requests per second (upper bound)
_DEFAULT_PER_DOMAIN_RATE: float = 0.5  # requests per second per hostname
_DEFAULT_PER_DOMAIN_IDLE_TTL: float = 3600.0  # drop idle per-domain limiters after 1h
_DEFAULT_TIMEOUT: float = 30.0
_DEFAULT_CONCURRENCY: int = 5


class Crawler:
    """Stateful crawler for repeated career-page scraping with shared settings.

    Obtain instances via :func:`create_crawler` rather than calling this
    constructor directly, so defaults stay consistent across releases.

    Parameters
    ----------
    rate_limit:
        Global maximum requests per second across all domains (default ``0.5``).
        Acts as an upper bound in addition to per-domain pacing.
    per_domain_rate:
        Maximum requests per second **per hostname** (default ``0.5``).
    per_domain_idle_ttl:
        Seconds after the last completed request to a hostname before its
        per-domain limiter entry may be evicted (default one hour).
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
    allow_private:
        When False (default), refuse URLs that resolve to private, loopback, or
        link-local addresses (SSRF mitigation). Set True only for tests or
        trusted environments.
    respect_robots:
        When True (default), load ``robots.txt`` before the career-page GET for
        non-API ATS paths. Greenhouse, Lever, and Ashby (URL-detected) skip this check.
    robots_cache_ttl:
        Seconds to cache parsed ``robots.txt`` per origin (default one hour).
    """

    def __init__(
        self,
        *,
        rate_limit: float = _DEFAULT_RATE_LIMIT,
        per_domain_rate: float = _DEFAULT_PER_DOMAIN_RATE,
        per_domain_idle_ttl: float = _DEFAULT_PER_DOMAIN_IDLE_TTL,
        timeout: float = _DEFAULT_TIMEOUT,
        user_agent: str | None = None,
        headless: bool = False,
        proxy: str | None = None,
        llm_provider: str | None = None,
        llm_api_base: str | None = None,
        allow_private: bool = False,
        respect_robots: bool = True,
        robots_cache_ttl: float = 3600.0,
        ocr_router: OcrRouter | None = None,
    ) -> None:
        self._global_rate_limiter = RateLimiter(requests_per_second=rate_limit)
        self._per_domain_registry = PerDomainRateLimiterRegistry(
            requests_per_second=per_domain_rate,
            idle_ttl_seconds=per_domain_idle_ttl,
        )
        self._timeout = timeout
        self._user_agent = user_agent
        self._headless = headless
        self._proxy = proxy
        self._llm_provider = llm_provider
        self._llm_api_base = llm_api_base
        self._allow_private = allow_private
        self._respect_robots = respect_robots
        self._robots_checker = RobotsTxtChecker(
            ttl_seconds=robots_cache_ttl,
            user_agent=user_agent,
        )
        self._ocr_router = ocr_router

    async def _acquire_rate_limits(self, url: str) -> None:
        """Apply global and per-hostname rate limits (global is the upper bound)."""
        await self._global_rate_limiter.acquire()
        hostname = urlparse(url).hostname or ""
        await self._per_domain_registry.acquire(hostname)

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
        await self._acquire_rate_limits(url)

        url_hint = detect_from_url(url)
        bypass_robots = url_hint.provider in _ROBOTS_BYPASS_PROVIDERS

        if self._respect_robots and not bypass_robots:
            allowed = await self._robots_checker.can_fetch(
                url,
                timeout=self._timeout,
                allow_private=self._allow_private,
            )
            if not allowed:
                logger.info(
                    "Skipping %s: robots.txt disallows this URL for User-Agent %r",
                    url,
                    self._robots_checker.user_agent,
                )
                return ScrapeResult(
                    url=url,
                    ats_info=url_hint,
                    error="robots.txt disallows fetching this URL for the configured user agent",
                )

        fetch_headers = {"User-Agent": self._user_agent} if self._user_agent else None
        result = await safe_fetch(
            url,
            timeout=self._timeout,
            headers=fetch_headers,
            allow_private=self._allow_private,
        )

        ats_info = await detect_ats(
            url,
            html=result.content or "",
            timeout=self._timeout,
            user_agent=self._user_agent,
            allow_private=self._allow_private,
        )

        if BaseParser.is_stub_provider(ats_info.provider) and not self._llm_provider:
            provider_name = ats_info.provider.value.title()
            return ScrapeResult(
                url=url,
                ats_info=ats_info,
                error=(
                    f"{provider_name} parser is not yet implemented. "
                    f"Configure llm_provider to use LLM extraction: "
                    f"create_crawler(llm_provider='gemini/gemini-2.0-flash')"
                ),
            )

        if not result.ok and ats_info.provider != ATSProvider.UNKNOWN:
            return ScrapeResult(
                url=url,
                ats_info=ats_info,
                error=result.error or f"HTTP {result.status_code}",
                scrape_duration_ms=result.elapsed_ms,
            )

        # --- API-first fetch strategy (AC1/AC4) ---
        # For provider-API ATS boards (Greenhouse, Lever, Ashby), the entrypoint
        # URL may serve HTML but the parsers expect structured JSON from the API.
        # When the detected ATS has a canonical API URL distinct from the requested
        # URL, re-fetch from the API endpoint so the parser receives the right payload.
        fetch_result = result
        if ats_info.provider in _ROBOTS_BYPASS_PROVIDERS and ats_info.api_url:
            api_url = ats_info.api_url
            # Only redirect when the effective fetch target differs from what we have.
            # Normalise comparison: strip trailing slashes, ignore query string on original.
            _orig_base = url.rstrip("/").split("?")[0]
            _api_base = api_url.rstrip("/").split("?")[0]
            if _api_base != _orig_base:
                logger.debug(
                    "Provider API redirect: %s → %s",
                    url,
                    api_url,
                )
                api_result = await safe_fetch(
                    api_url,
                    timeout=self._timeout,
                    headers=fetch_headers,
                    allow_private=self._allow_private,
                )
                if api_result.ok:
                    fetch_result = api_result
                else:
                    logger.warning(
                        "API fetch failed for %s (%s); falling back to entrypoint content",
                        api_url,
                        api_result.error or f"HTTP {api_result.status_code}",
                    )

        parser = self._get_parser(ats_info.provider)
        page_hash = content_hash(fetch_result.content or "")
        changed = previous_hash is None or page_hash != previous_hash

        html_content = fetch_result.content or ""
        stripped_text = re.sub(r"<[^>]+>", " ", html_content).strip()

        trigger_ocr = (
            len(stripped_text) < 200
            and ats_info.provider == ATSProvider.UNKNOWN
            and self._ocr_router is not None
        )
        if trigger_ocr:
            assert self._ocr_router is not None  # narrowed: trigger_ocr guards this branch
            raw_bytes = html_content.encode("utf-8")
            async with httpx.AsyncClient(timeout=httpx.Timeout(self._timeout)) as ocr_client:
                ocr_result = await self._ocr_router.run(raw_bytes, client=ocr_client)
            if ocr_result.ok and ocr_result.markdown:
                logger.info("OCR triggered for %s. Extracted markdown.", url)
                # Feed OCR markdown through LLM fallback parser
                if isinstance(parser, LLMFallbackParser):
                    jobs = await parser.parse_async(ocr_result.markdown, url=url)
                else:
                    jobs = parser.parse(ocr_result.markdown, url=url)
            else:
                if ocr_result.error:
                    logger.warning("OCR failed for %s: %s", url, ocr_result.error)
                jobs = []
        else:
            # ENH-04 / PCC-1736: For Ashby pages detected from DOM (custom domain,
            # api_url=None, detection_method="dom_probe"), html_content is a career
            # page that embeds Ashby — not a GraphQL JSON response.  Extract the org
            # slug from HTML and query the GraphQL API directly.
            if (
                ats_info.provider == ATSProvider.ASHBY
                and ats_info.detection_method == "dom_probe"
                and isinstance(parser, AshbyParser)
            ):
                jobs = await AshbyParser.fetch_all(url, html=html_content)
            # LLM extraction is synchronous in litellm; run it in a thread so the
            # asyncio event loop stays responsive under concurrent scrapes (PCC-1606).
            elif isinstance(parser, LLMFallbackParser):
                jobs = await parser.parse_async(html_content, url=url)
            else:
                jobs = parser.parse(html_content, url=url)

        # Phase 3 Fallback: Crawl4AI for UNKNOWN / SPA pages
        if ats_info.provider == ATSProvider.UNKNOWN and (
            not fetch_result.ok or not html_content.strip() or len(jobs) < 5
        ):
            from strata_harvest.parsers.crawl4ai_extractor import (
                _CRAWL4AI_AVAILABLE,
                Crawl4AIExtractor,
            )

            if _CRAWL4AI_AVAILABLE:
                logger.info("Triggering Crawl4AI fallback for %s", url)
                extractor = Crawl4AIExtractor()
                c4_jobs = await extractor.extract(url)
                if c4_jobs:
                    jobs = c4_jobs

        if not jobs and not fetch_result.ok:
            return ScrapeResult(
                url=url,
                ats_info=ats_info,
                error=fetch_result.error or f"HTTP {fetch_result.status_code}",
                scrape_duration_ms=fetch_result.elapsed_ms,
            )

        return ScrapeResult(
            url=url,
            jobs=jobs,
            content_hash=page_hash,
            changed=changed,
            ats_info=ats_info,
            scrape_duration_ms=fetch_result.elapsed_ms,
            fetch_ok=True,
        )

    async def scrape_batch(
        self,
        urls: list[str],
        concurrency: int = _DEFAULT_CONCURRENCY,
    ) -> AsyncIterator[ScrapeResult]:
        """Scrape multiple URLs concurrently.

        Uses an :class:`asyncio.Semaphore` to cap parallelism at
        *concurrency*. Each URL is scraped via :meth:`scrape`, which applies
        a global rate cap plus an independent per-hostname limiter; the
        semaphore bounds concurrent tasks while the limiters pace HTTP requests.

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
        """Return the appropriate parser, wiring llm_provider and llm_api_base if needed.

        Stub parsers fall through to LLM extraction automatically via
        ``BaseParser.for_provider()``.
        """
        return BaseParser.for_provider(
            provider,
            llm_provider=self._llm_provider,
            api_base=self._llm_api_base,
        )


def create_crawler(
    *,
    rate_limit: float = _DEFAULT_RATE_LIMIT,
    per_domain_rate: float = _DEFAULT_PER_DOMAIN_RATE,
    per_domain_idle_ttl: float = _DEFAULT_PER_DOMAIN_IDLE_TTL,
    timeout: float = _DEFAULT_TIMEOUT,
    user_agent: str | None = None,
    headless: bool = False,
    proxy: str | None = None,
    llm_provider: str | None = None,
    llm_api_base: str | None = None,
    allow_private: bool = False,
    respect_robots: bool = True,
    robots_cache_ttl: float = 3600.0,
    ocr_router: OcrRouter | None = None,
) -> Crawler:
    """Build a :class:`Crawler` with explicit tuning knobs.

    Parameters
    ----------
    rate_limit:
        Global max requests per second across all domains (upper bound).
    per_domain_rate:
        Max requests per second per hostname (default ``0.5``).
    per_domain_idle_ttl:
        Evict idle per-domain limiters after this many seconds without a
        completed request to that host.
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
    llm_api_base:
        Base URL for a custom LLM inference endpoint (e.g. a local model server).
        Passed directly to :class:`~strata_harvest.parsers.llm_fallback.LLMFallbackParser`
        as ``api_base``; the caller is responsible for supplying the correct address.
        When set, ``api_key`` is automatically sent as ``"not-required"``.
    allow_private:
        When True, allow fetches to private/loopback/link-local hosts (testing only).
    respect_robots:
        When True (default), honor ``robots.txt`` before scraping non-API career URLs.
    robots_cache_ttl:
        Cache duration for parsed ``robots.txt`` per site (seconds).

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
        per_domain_rate=per_domain_rate,
        per_domain_idle_ttl=per_domain_idle_ttl,
        timeout=timeout,
        user_agent=user_agent,
        headless=headless,
        proxy=proxy,
        llm_provider=llm_provider,
        llm_api_base=llm_api_base,
        allow_private=allow_private,
        respect_robots=respect_robots,
        robots_cache_ttl=robots_cache_ttl,
        ocr_router=ocr_router,
    )


async def harvest(
    url: str,
    *,
    timeout: float = _DEFAULT_TIMEOUT,
    allow_private: bool = False,
) -> list[JobListing]:
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
    allow_private:
        Passed to :func:`create_crawler` (see :class:`Crawler`).

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
    crawler = create_crawler(timeout=timeout, allow_private=allow_private)
    result = await crawler.scrape(url)
    return result.jobs
