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
    """Configured crawler for repeated career page scraping."""

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
        """Scrape a career page URL and return structured listings."""
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
        """Scrape multiple URLs concurrently, yielding results as they complete.

        Uses an asyncio.Semaphore to cap parallelism at *concurrency*.
        Each URL is scraped via ``self.scrape()`` which already applies
        rate limiting, so the semaphore controls task concurrency while
        the rate limiter controls request pacing.
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
                await queue.put(
                    ScrapeResult(url=url, error=f"{type(exc).__name__}: {exc}")
                )

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
    """Create a configured crawler instance.

    Parameters
    ----------
    rate_limit:
        Max requests per second (default 0.5 → one request every 2 s).
    timeout:
        Per-request HTTP timeout in seconds.
    user_agent:
        Custom User-Agent header.
    headless:
        Whether to use a headless browser for JS-rendered pages (future).
    proxy:
        HTTP(S) proxy URL (future).
    llm_provider:
        LiteLLM model string for the LLM fallback parser (e.g. ``openai/gpt-4o-mini``).
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
    """One-shot convenience: scrape a URL and return job listings.

    Auto-detects the ATS provider and uses the appropriate parser.
    """
    crawler = create_crawler(timeout=timeout)
    result = await crawler.scrape(url)
    return result.jobs
