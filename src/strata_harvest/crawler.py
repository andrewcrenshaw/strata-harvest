"""Career page crawler — public API surface."""

from __future__ import annotations

from strata_harvest.detector import detect_ats
from strata_harvest.models import JobListing, ScrapeResult
from strata_harvest.parsers.base import BaseParser
from strata_harvest.utils.hashing import content_hash
from strata_harvest.utils.http import safe_fetch
from strata_harvest.utils.rate_limiter import RateLimiter


class Crawler:
    """Configured crawler for repeated career page scraping."""

    def __init__(
        self,
        *,
        rate_limit: float = 1.0,
        timeout: float = 30.0,
        user_agent: str | None = None,
    ) -> None:
        self._rate_limiter = RateLimiter(requests_per_second=rate_limit)
        self._timeout = timeout
        self._user_agent = user_agent

    async def scrape(self, url: str) -> ScrapeResult:
        """Scrape a career page URL and return structured listings."""
        await self._rate_limiter.acquire()

        ats_info = await detect_ats(url, timeout=self._timeout, user_agent=self._user_agent)
        parser = BaseParser.for_provider(ats_info.provider)

        result = await safe_fetch(url, timeout=self._timeout, user_agent=self._user_agent)
        if not result.ok:
            return ScrapeResult(
                url=url,
                provider=ats_info.provider,
                error=result.error or f"HTTP {result.status_code}",
                elapsed_ms=result.elapsed_ms,
            )

        listings = parser.parse(result.content or "", url=url)
        return ScrapeResult(
            url=url,
            provider=ats_info.provider,
            listings=listings,
            content_hash=content_hash(result.content or ""),
            elapsed_ms=result.elapsed_ms,
        )


def create_crawler(
    *,
    rate_limit: float = 1.0,
    timeout: float = 30.0,
    user_agent: str | None = None,
) -> Crawler:
    """Create a configured crawler instance."""
    return Crawler(rate_limit=rate_limit, timeout=timeout, user_agent=user_agent)


async def harvest(url: str, *, timeout: float = 30.0) -> list[JobListing]:
    """One-shot convenience: scrape a URL and return job listings."""
    crawler = create_crawler(timeout=timeout)
    result = await crawler.scrape(url)
    return result.listings
