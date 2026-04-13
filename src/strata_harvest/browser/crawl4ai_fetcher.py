"""Crawl4AI-based SPA fetcher — optional browser extra.

Provides :class:`Crawl4AIFetcher` and the :func:`crawl4ai_fetch` convenience
helper.  Use these when httpx returns empty or JS-gated content from a career
page that requires a real browser to render.

Requires the ``browser`` extra::

    pip install strata-harvest[browser]

Usage::

    from strata_harvest.browser import crawl4ai_fetch

    html = await crawl4ai_fetch("https://ats.rippling.com/rippling/jobs")
    # html is the fully rendered page HTML string, or "" on failure

"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

try:
    from crawl4ai import (
        AsyncWebCrawler,
        BrowserConfig,
        CrawlerRunConfig,
    )

    _CRAWL4AI_AVAILABLE = True
except ImportError:
    _CRAWL4AI_AVAILABLE = False


class Crawl4AIFetcher:
    """Fetch JS-rendered HTML for a URL using a headless Chromium browser.

    Instantiate once and reuse across multiple :meth:`fetch` calls to share
    the underlying browser session startup cost.

    Parameters
    ----------
    headless:
        Run the browser in headless mode (default ``True``).
    timeout:
        Maximum seconds to wait for the page to load (default ``30``).
    verbose:
        Pass ``verbose=True`` to crawl4ai's AsyncWebCrawler for debug output.

    Examples
    --------
    >>> import asyncio
    >>> from strata_harvest.browser import Crawl4AIFetcher
    >>> async def demo() -> None:
    ...     fetcher = Crawl4AIFetcher()
    ...     html = await fetcher.fetch("https://ats.rippling.com/rippling/jobs")
    ...     assert isinstance(html, str)
    >>> asyncio.run(demo())  # doctest: +SKIP
    """

    def __init__(
        self,
        *,
        headless: bool = True,
        timeout: int = 30,
        verbose: bool = False,
    ) -> None:
        if not _CRAWL4AI_AVAILABLE:
            raise ImportError(
                "crawl4ai is not installed. Run: pip install 'strata-harvest[browser]'"
            )
        self._headless = headless
        self._timeout = timeout
        self._verbose = verbose

    async def fetch(self, url: str) -> str:
        """Fetch a URL using Crawl4AI and return the rendered HTML.

        Parameters
        ----------
        url:
            The career page URL to render.

        Returns
        -------
        str
            The rendered HTML content; empty string on error or no content.
        """
        logger.debug("Crawl4AIFetcher: fetching %s", url)

        browser_config = BrowserConfig(
            headless=self._headless,
            verbose=self._verbose,
        )
        run_config = CrawlerRunConfig(
            page_timeout=self._timeout * 1000,  # crawl4ai expects milliseconds
        )

        try:
            async with AsyncWebCrawler(config=browser_config) as crawler:
                result = await crawler.arun(url=url, config=run_config)
        except Exception:
            logger.warning("Crawl4AIFetcher failed for %s", url, exc_info=True)
            return ""

        if not result.success:
            logger.warning(
                "Crawl4AIFetcher returned failure for %s: %s",
                url,
                getattr(result, "error_message", "unknown error"),
            )
            return ""

        html: str = result.html or ""
        logger.debug(
            "Crawl4AIFetcher: fetched %s → %d bytes",
            url,
            len(html),
        )
        return html


async def crawl4ai_fetch(
    url: str,
    *,
    headless: bool = True,
    timeout: int = 30,
) -> str:
    """One-shot convenience wrapper around :class:`Crawl4AIFetcher`.

    Builds a fetcher, fetches *url*, and returns the rendered HTML.

    Parameters
    ----------
    url:
        Career page URL to render.
    headless:
        Whether to run the browser headlessly (default ``True``).
    timeout:
        Page load timeout in seconds (default ``30``).

    Returns
    -------
    str
        Rendered HTML, or ``""`` on any error.

    Examples
    --------
    >>> import asyncio
    >>> from strata_harvest.browser import crawl4ai_fetch
    >>> async def demo() -> None:
    ...     html = await crawl4ai_fetch("https://ats.rippling.com/rippling/jobs")
    ...     assert isinstance(html, str)
    >>> asyncio.run(demo())  # doctest: +SKIP
    """
    fetcher = Crawl4AIFetcher(headless=headless, timeout=timeout)
    return await fetcher.fetch(url)
