"""Crawl4AI-based extractor for UNKNOWN/SPA career pages."""

from __future__ import annotations

import json
import logging
import os

from pydantic import BaseModel, Field

from strata_harvest.models import ATSProvider, JobListing

logger = logging.getLogger(__name__)

# Import guarded to allow graceful failure if crawl4ai is not installed
try:
    from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, LLMConfig
    from crawl4ai.extraction_strategy import LLMExtractionStrategy  # type: ignore[import-not-found]

    _CRAWL4AI_AVAILABLE = True
except ImportError:
    AsyncWebCrawler = None
    CrawlerRunConfig = None
    LLMConfig = None
    LLMExtractionStrategy = None
    _CRAWL4AI_AVAILABLE = False


class ExtractedJob(BaseModel):
    """Schema for a single job extracted by the LLM."""

    title: str
    url: str
    location: str | None = None
    department: str | None = None
    employment_type: str | None = None
    description: str | None = None
    requirements: list[str] = Field(default_factory=list)
    salary_range: str | None = None


class JobPageSchema(BaseModel):
    """Schema for the entire extracted job page."""

    jobs: list[ExtractedJob]


JOB_PAGE_SCHEMA = JobPageSchema.model_json_schema()


class Crawl4AIExtractor:
    """Fallback extractor using Crawl4AI for complex SPA career pages.

    Invoked only when:
    1. ATS is UNKNOWN after detect_from_dom()
    2. safe_fetch() returned 403, empty body, or <5 jobs
    """

    async def extract(self, url: str) -> list[JobListing]:
        """Render page in headless browser and extract jobs via LLM."""
        if not _CRAWL4AI_AVAILABLE:
            raise ImportError(
                "crawl4ai is not installed. "
                "Install with `pip install strata-harvest[browser]` or `pip install crawl4ai`."
            )

        # LiteLLM underlying crawl4ai typically requires GEMINI_API_KEY in the environment
        if not os.getenv("GEMINI_API_KEY") and os.getenv("GOOGLE_API_KEY"):
            os.environ["GEMINI_API_KEY"] = os.environ["GOOGLE_API_KEY"]

        strategy = LLMExtractionStrategy(
            llm_config=LLMConfig(
                provider="gemini/gemini-2.5-flash", api_token=os.getenv("GEMINI_API_KEY")
            ),
            schema=JOB_PAGE_SCHEMA,
            instruction="Extract all job listings from this career page.",
        )
        try:
            async with AsyncWebCrawler(headless=True) as crawler:
                result = await crawler.arun(
                    url=url,
                    config=CrawlerRunConfig(
                        extraction_strategy=strategy,
                        js_code="window.scrollTo(0, document.body.scrollHeight);",
                        page_timeout=30000,
                    ),
                )
        except Exception as e:
            logger.warning("Crawl4AI extraction failed for %s: %s", url, e)
            return []

        if not result.success:
            logger.warning("Crawl4AI failed to scrape %s: %s", url, result.error_message)
            return []

        return self._map_to_listings(result.extracted_content)

    def _map_to_listings(self, extracted_content: str | None) -> list[JobListing]:
        if not extracted_content:
            return []

        # Crawl4AI with LLMExtractionStrategy returns a JSON string
        # based on the schema (JobPageSchema)
        if isinstance(extracted_content, str):
            try:
                data = json.loads(extracted_content)
            except json.JSONDecodeError:
                logger.warning(
                    "Failed to decode JSON from Crawl4AI extraction: %s",
                    extracted_content[:100],
                )
                return []
        else:
            data = extracted_content

        if isinstance(data, list):
            # Sometimes the LLM returns just the list of jobs directly if it misinterprets
            raw_jobs = data
        elif isinstance(data, dict):
            raw_jobs = data.get("jobs", [])
        else:
            return []

        listings = []
        for raw in raw_jobs:
            if not isinstance(raw, dict):
                continue
            title = raw.get("title")
            url = raw.get("url")
            if not title or not url:
                continue

            # ensure requirements is a list of strings
            reqs = raw.get("requirements", [])
            if not isinstance(reqs, list):
                reqs = []

            listings.append(
                JobListing(
                    title=title,
                    url=url,
                    location=raw.get("location"),
                    department=raw.get("department"),
                    employment_type=raw.get("employment_type"),
                    description=raw.get("description"),
                    requirements=[str(r) for r in reqs if r],
                    salary_range=raw.get("salary_range"),
                    ats_provider=ATSProvider.UNKNOWN,
                    raw_data=raw,
                )
            )

        return listings
