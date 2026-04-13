"""Browser-based fetching via Crawl4AI (optional extra).

Provides SPA/JS-rendered page fetching as a fallback when httpx cannot
parse dynamically rendered career pages.

Install the optional extra to use this module::

    pip install strata-harvest[browser]

"""

from strata_harvest.browser.crawl4ai_fetcher import Crawl4AIFetcher, crawl4ai_fetch

__all__ = ["Crawl4AIFetcher", "crawl4ai_fetch"]
