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
from strata_harvest.models import ATSInfo, ATSProvider, FetchResult, JobListing, ScrapeResult
from strata_harvest.parsers.ashby import AshbyParser
from strata_harvest.parsers.base import BaseParser
from strata_harvest.parsers.llm_fallback import LLMFallbackParser
from strata_harvest.utils.hashing import content_hash
from strata_harvest.utils.http import safe_fetch
from strata_harvest.utils.rate_limiter import PerDomainRateLimiterRegistry, RateLimiter
from strata_harvest.utils.robots import RobotsTxtChecker
from strata_harvest.validator.careers_page import CareersPageValidator

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
_DEFAULT_IMPERSONATION_TARGET: str = "chrome124"

# Cloudflare / generic bot-challenge body markers (lowercase).
_BOT_CHALLENGE_BODY_MARKERS: tuple[str, ...] = (
    "_cf_chl_opt",
    "cf-browser-verification",
    "just a moment",
    "checking your browser",
)


def _bot_challenge_reason(result: FetchResult) -> str | None:
    """Return a reason code if *result* looks like a bot-challenge page, else None.

    Used by the tier-1→tier-2 escalation path to decide whether to retry
    the fetch using curl_cffi browser impersonation.

    Reason codes
    ------------
    ``"http_403"``
        Server returned HTTP 403 Forbidden.
    ``"cloudflare_challenge_body"``
        Response body contains Cloudflare JS-challenge markers.
    """
    if result.status_code == 403:
        return "http_403"
    if result.content:
        lc = result.content.lower()
        if any(m in lc for m in _BOT_CHALLENGE_BODY_MARKERS):
            return "cloudflare_challenge_body"
    return None


# ---------------------------------------------------------------------------
# Tier-3 escalation helpers (PCC-1947)
# ---------------------------------------------------------------------------

# Minimum stripped-text length before a 200 response is treated as "empty"
# and escalated to tier-3 StealthyFetcher.
_TIER3_EMPTY_BODY_THRESHOLD: int = 200

# Reason codes emitted in the scrape audit log on tier-3 escalation.
_TIER3_REASON_403 = "TIER3_403"
_TIER3_REASON_CLOUDFLARE = "TIER3_CLOUDFLARE"
_TIER3_REASON_EMPTY_200 = "TIER3_EMPTY_200"


def _tier3_escalation_reason(result: FetchResult) -> str | None:
    """Return a tier-3 reason code when *result* warrants StealthyFetcher escalation.

    Checks (in order):

    1. HTTP 403 → ``"TIER3_403"``
    2. Cloudflare JS-challenge body → ``"TIER3_CLOUDFLARE"``
    3. HTTP 200 with near-empty body → ``"TIER3_EMPTY_200"``

    Returns ``None`` when no escalation is warranted.
    """
    if result.status_code == 403:
        return _TIER3_REASON_403
    content = result.content or ""
    lc = content.lower()
    if "cloudflare" in lc and any(m in lc for m in _BOT_CHALLENGE_BODY_MARKERS):
        return _TIER3_REASON_CLOUDFLARE
    stripped = re.sub(r"<[^>]+>", " ", content).strip()
    if result.status_code == 200 and len(stripped) < _TIER3_EMPTY_BODY_THRESHOLD:
        return _TIER3_REASON_EMPTY_200
    return None


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
    impersonation_target:
        curl_cffi browser profile used for tier-2 escalation when tier-1 hits a
        bot-challenge (403 or Cloudflare JS-challenge body).  Default ``chrome124``.
        Requires the ``[stealth]`` extra; has no effect when curl_cffi is not installed.
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
        impersonation_target: str = _DEFAULT_IMPERSONATION_TARGET,
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
        self._page_validator = CareersPageValidator()
        self._impersonation_target = impersonation_target
        # Sitemap discovery and caching for incremental crawling
        from strata_harvest.utils.sitemap import SitemapFinder  # noqa: PLC0415

        self._sitemap_finder = SitemapFinder(ttl_seconds=robots_cache_ttl)

    async def _acquire_rate_limits(self, url: str) -> None:
        """Apply global and per-hostname rate limits (global is the upper bound)."""
        await self._global_rate_limiter.acquire()
        hostname = urlparse(url).hostname or ""
        await self._per_domain_registry.acquire(hostname)

    async def discover_job_urls_from_sitemap(self, page_url: str) -> list[str]:
        """Discover job URLs for a domain via sitemap discovery.

        Attempts to find and parse /sitemap.xml, /sitemap_index.xml, and /sitemal.xml
        (SAP SuccessFactors quirk) for the domain. Recursively handles sitemap index files.

        Returns an empty list if no sitemap is found or the domain cannot be parsed.

        Parameters
        ----------
        page_url:
            Career page or job-board URL (domain is extracted from this).

        Returns
        -------
        list[str]
            Job URLs discovered from sitemaps (in order of appearance).
        """
        entries = await self._sitemap_finder.find_job_urls(
            page_url,
            timeout=self._timeout,
            allow_private=self._allow_private,
        )
        return [entry.url for entry in entries]

    async def scrape(
        self,
        url: str,
        *,
        previous_hash: str | None = None,
        previous_etag: str | None = None,
        previous_lastmod: str | None = None,
    ) -> ScrapeResult:
        """Scrape a career page and return structured results.

        Detects the ATS, fetches HTML, parses listings, and computes a content
        hash for change detection when *previous_hash* is supplied.

        Supports conditional requests via *previous_etag* and *previous_lastmod*
        to avoid redundant fetches when content hasn't changed (HTTP 304 Not Modified).

        Parameters
        ----------
        url:
            Career page or job-board URL to scrape.
        previous_hash:
            If set, compared to the new content hash to populate
            :attr:`ScrapeResult.changed`.
        previous_etag:
            ETag from a prior fetch; used in If-None-Match header for conditional requests.
        previous_lastmod:
            Last-Modified timestamp from a prior fetch or sitemap; used in
            If-Modified-Since header for conditional requests.

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

        # Handle 304 Not Modified by using cached content if available
        # This requires both the conditional headers AND the cached content to be passed
        result = await safe_fetch(
            url,
            timeout=self._timeout,
            headers=fetch_headers,
            allow_private=self._allow_private,
            if_none_match=previous_etag,
            if_modified_since=previous_lastmod,
            cached_content=None,  # We don't have cached content at this level
        )

        # --- Handle 304 Not Modified (PCC-1954) ---
        # When conditional request headers were provided and the server returned 304,
        # it means the content hasn't changed. Return early with no jobs parsed.
        if result.status_code == 304:
            logger.info(
                "Content unchanged for %s (304 Not Modified) — skipping parse",
                url,
            )
            return ScrapeResult(
                url=url,
                ats_info=url_hint,
                content_hash=None,  # No content available for hash
                changed=False,  # Explicitly unchanged
                scrape_duration_ms=result.elapsed_ms,
                fetch_ok=True,
            )

        # --- Tier-2 escalation: curl_cffi impersonation (PCC-1948) ---
        # When tier-1 (httpx) is blocked by a bot-challenge (HTTP 403 or a
        # Cloudflare JS-challenge body), retry with curl_cffi browser
        # impersonation before falling through to tier-3 (crawl4ai).
        _t2_reason = _bot_challenge_reason(result)
        if _t2_reason:
            from strata_harvest.utils.impersonating_fetcher import (  # noqa: PLC0415
                _CURL_CFFI_AVAILABLE,
            )
            from strata_harvest.utils.impersonating_fetcher import (
                safe_fetch as _impersonating_safe_fetch,
            )

            if _CURL_CFFI_AVAILABLE:
                logger.info(
                    "Tier escalation tier-1→tier-2 [%s]: %s (impersonate=%s)",
                    _t2_reason,
                    url,
                    self._impersonation_target,
                )
                tier2_result = await _impersonating_safe_fetch(
                    url,
                    timeout=self._timeout,
                    headers=fetch_headers,
                    impersonate=self._impersonation_target,
                    allow_private=self._allow_private,
                )
                if tier2_result.ok:
                    result = tier2_result
            else:
                logger.debug("curl_cffi not available; skipping tier-2 escalation for %s", url)

        # --- Tier-3 escalation: scrapling StealthyFetcher (PCC-1947) ---
        # Promote to tier 3 when the result (post tier-2) still indicates bot-blocking
        # or an empty page.  Skipped for API-native ATS providers (Greenhouse, Lever,
        # Ashby) that use direct API endpoints and don't need browser rendering.
        # Every escalation emits a structured log entry with the reason code so that
        # the scrape audit trail remains queryable.
        if url_hint.provider not in _ROBOTS_BYPASS_PROVIDERS:
            _t3_reason = _tier3_escalation_reason(result)
            if _t3_reason:
                from strata_harvest.utils.stealth_fetcher import (  # noqa: PLC0415
                    _SCRAPLING_AVAILABLE,
                    StealthFetcher,
                )

                if _SCRAPLING_AVAILABLE:
                    logger.info(
                        "Tier escalation tier-2→tier-3 [%s]: %s",
                        _t3_reason,
                        url,
                    )
                    tier3_result = await StealthFetcher(timeout=int(self._timeout)).fetch(url)
                    if tier3_result.ok:
                        logger.info("Tier-3 StealthyFetcher succeeded for %s", url)
                        result = tier3_result
                    else:
                        logger.warning(
                            "Tier-3 StealthyFetcher failed for %s: %s",
                            url,
                            tier3_result.error,
                        )
                else:
                    logger.debug(
                        "scrapling not available; skipping tier-3 escalation for %s [%s]",
                        url,
                        _t3_reason,
                    )

        ats_info = await detect_ats(
            url,
            html=result.content or "",
            timeout=self._timeout,
            user_agent=self._user_agent,
            allow_private=self._allow_private,
        )

        # --- Pre-harvest validation (PCC-1946) ---
        # Reject wrong-page false-positives before any parsing work begins.
        # Passes ats_info so detected ATS providers short-circuit as valid.
        validation = self._page_validator.validate(
            url,
            result.content or "",
            ats_info=ats_info,
        )
        if not validation.is_valid:
            logger.info(
                "CareersPageValidator rejected %s [%s]: %s",
                url,
                validation.reason_code,
                validation.reject_reason,
            )
            return ScrapeResult(
                url=url,
                ats_info=ats_info,
                error=(
                    f"Page rejected by validator [{validation.reason_code}]: "
                    f"{validation.reject_reason}"
                ),
                scrape_duration_ms=result.elapsed_ms,
                fetch_ok=result.ok,
            )
        if validation.suspect:
            logger.warning(
                "CareersPageValidator: %s is suspect [%s] — consider Exa heal",
                url,
                validation.reason_code,
            )

        if BaseParser.is_stub_provider(ats_info.provider) and not self._llm_provider:
            provider_name = ats_info.provider.value.title()
            return ScrapeResult(
                url=url,
                ats_info=ats_info,
                error=(
                    f"{provider_name} parser is not yet implemented. "
                    f"Configure llm_provider to use LLM extraction: "
                    f"create_crawler(llm_provider='gemini/gemini-2.5-flash')"
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
            # ENH-04 / PCC-1736: For Ashby pages, the parser expects GraphQL
            # JSON but the initial fetch returns HTML.  Query the GraphQL API
            # directly.  For DOM-probe detections (custom domain), extract the
            # org slug from HTML; for URL-pattern detections (ashbyhq.com),
            # extract the slug from the URL.
            if ats_info.provider == ATSProvider.ASHBY and isinstance(parser, AshbyParser):
                html_for_slug = html_content if ats_info.detection_method == "dom_probe" else None
                jobs = await AshbyParser.fetch_all(url, html=html_for_slug)
            # LLM extraction is synchronous in litellm; run it in a thread so the
            # asyncio event loop stays responsive under concurrent scrapes (PCC-1606).
            elif isinstance(parser, LLMFallbackParser):
                jobs = await parser.parse_async(html_content, url=url)
            else:
                jobs = parser.parse(html_content, url=url)

        # Phase 3 Fallback: Crawl4AI for UNKNOWN / SPA pages.
        # Rippling is handled by RipplingParser via __NEXT_DATA__ extraction and
        # no longer requires JS execution — removed from spa_providers.
        _crawl4ai_trigger = ats_info.provider == ATSProvider.UNKNOWN and (
            not fetch_result.ok or not html_content.strip() or len(jobs) < 5
        )
        if _crawl4ai_trigger:
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

    async def _group_sources_by_ats(
        self,
        urls: list[str],
    ) -> dict[tuple[ATSProvider, str | None], list[tuple[str, ATSInfo]]]:
        """Pre-scan URLs to detect ATS and group by (provider, api_url) for batching.

        Returns a dict mapping (provider, api_url) → [(url, ats_info), ...].
        This enables coalescing multiple URLs pointing to the same ATS org.

        For UNKNOWN providers (no api_url), each URL gets its own group so they
        are scraped independently without unnecessary batching.

        Parameters
        ----------
        urls:
            URLs to group.

        Returns
        -------
        dict[tuple[ATSProvider, str | None], list[tuple[str, ATSInfo]]]
            Groups of URLs sharing the same ATS provider and API endpoint.
        """
        groups: dict[tuple[ATSProvider, str | None], list[tuple[str, ATSInfo]]] = {}

        for url in urls:
            try:
                ats_info = await detect_ats(
                    url,
                    timeout=self._timeout,
                    user_agent=self._user_agent,
                    allow_private=self._allow_private,
                )
            except Exception as e:
                # If detection fails (e.g., network issue, timeout), fall back to UNKNOWN
                logger.debug("ATS detection failed for %s: %s; using UNKNOWN", url, e)
                ats_info = ATSInfo(provider=ATSProvider.UNKNOWN)

            # For batching: use api_url as the key. UNKNOWN providers have api_url=None,
            # so each gets a unique key to avoid grouping together.
            # This is done by appending a hash of the URL to the key for UNKNOWN providers.
            if ats_info.api_url:
                # Valid api_url: batch these together
                key: tuple[ATSProvider, str | None] = (ats_info.provider, ats_info.api_url)
            else:
                # No api_url (UNKNOWN or stub providers): each URL gets a unique group
                # by using a hash of the URL as the api_url component
                url_hash = str(hash(url) & 0xFFFFFFFF)  # Use hash as unique identifier
                key = (ats_info.provider, url_hash)

            if key not in groups:
                groups[key] = []
            groups[key].append((url, ats_info))

        return groups

    async def scrape_batch(
        self,
        urls: list[str],
        concurrency: int = _DEFAULT_CONCURRENCY,
    ) -> AsyncIterator[ScrapeResult]:
        """Scrape multiple URLs concurrently with per-ATS-org batching.

        Optimizes for cases where multiple companies share one ATS organization
        (e.g., subsidiaries on the same Greenhouse board). Pre-detects ATS for
        all URLs, groups by (provider, api_url), and fetches once per group
        before distributing results to each requesting source.

        Uses an :class:`asyncio.Semaphore` to cap parallelism at
        *concurrency*. Each URL group is scraped via :meth:`scrape`, which applies
        a global rate cap plus an independent per-hostname limiter; the
        semaphore bounds concurrent tasks while the limiters pace HTTP requests.

        Parameters
        ----------
        urls:
            URLs to scrape (empty list yields nothing).
        concurrency:
            Maximum concurrent scrape tasks per ATS org group.

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

        # Phase 1: Group sources by ATS provider and API endpoint
        groups = await self._group_sources_by_ats(urls)

        # Track batching optimization metric
        original_fetch_count = len(urls)
        batched_fetch_count = len(groups)
        batched_saves = original_fetch_count - batched_fetch_count
        if batched_saves > 0:
            logger.debug(
                "Per-ATS-org batching (PCC-1962): %d groups from %d URLs, saves %d fetches",
                batched_fetch_count,
                original_fetch_count,
                batched_saves,
            )

        # Phase 2: Process each ATS group with concurrency limiting
        semaphore = asyncio.Semaphore(concurrency)
        queue: asyncio.Queue[ScrapeResult] = asyncio.Queue()

        async def _batch_worker(
            group_key: tuple[ATSProvider, str | None],
            urls_with_info: list[tuple[str, ATSInfo]],
        ) -> None:
            """Scrape one representative URL and distribute to all group members."""
            try:
                async with semaphore:
                    # Scrape the first URL in the group (representative of the org)
                    primary_url = urls_with_info[0][0]
                    primary_result = await self.scrape(primary_url)

                    # Yield the primary result
                    await queue.put(primary_result)

                    # Distribute to other URLs in the same group
                    # (In practice, strata filters these by department/location client-side)
                    for secondary_url, _ in urls_with_info[1:]:
                        secondary_result = ScrapeResult(
                            url=secondary_url,
                            jobs=primary_result.jobs,
                            content_hash=primary_result.content_hash,
                            changed=primary_result.changed,
                            ats_info=primary_result.ats_info,
                            scrape_duration_ms=primary_result.scrape_duration_ms,
                            error=primary_result.error,
                            fetch_ok=primary_result.fetch_ok,
                        )
                        await queue.put(secondary_result)
            except Exception as exc:
                # Yield error result for primary URL
                await queue.put(
                    ScrapeResult(
                        url=urls_with_info[0][0],
                        error=f"{type(exc).__name__}: {exc}",
                    )
                )
                # Propagate error to secondary URLs too
                for secondary_url, _ in urls_with_info[1:]:
                    await queue.put(
                        ScrapeResult(
                            url=secondary_url,
                            error=f"{type(exc).__name__}: {exc}",
                        )
                    )

        # Create one task per ATS org group
        tasks = [
            asyncio.create_task(_batch_worker(key, urls_with_info))
            for key, urls_with_info in groups.items()
        ]

        # Yield results as they arrive
        for _ in range(len(urls)):
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
    impersonation_target: str = _DEFAULT_IMPERSONATION_TARGET,
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
    impersonation_target:
        curl_cffi browser profile for tier-2 escalation (default ``chrome124``).
        Requires ``strata-harvest[stealth]``; has no effect without curl_cffi.

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
        impersonation_target=impersonation_target,
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
