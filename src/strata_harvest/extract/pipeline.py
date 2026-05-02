"""5-tier token-minimizing extraction pipeline.

Tier 0: Structured data (JSON-LD, microdata, OpenGraph) — zero LLM tokens
Tier 1: CSS/XPath parsing for known ATS shells
Tier 2: trafilatura Markdown extraction → local LLM
Tier 3: Local Ollama Qwen2.5-7B + instructor (constrained JSON)
Tier 4: Fallback to hosted Gemini (only if enabled + Ollama unavailable)

Each tier is tried in sequence; extraction stops on first success.
This minimizes LLM calls and token usage drastically vs. sending raw HTML.
"""

from __future__ import annotations

import asyncio
import logging
import os

from strata_harvest.extract.prune import extract_json_ld, extract_markdown
from strata_harvest.extract.schema import JobPostingSchema
from strata_harvest.models import ATSProvider, JobListing

logger = logging.getLogger(__name__)

# Import litellm for Tier 4 fallback (guarded)
try:
    import litellm
except ImportError:
    litellm = None


def extract_with_pipeline(
    html: str,
    *,
    url: str,
    enable_ollama: bool = True,
    enable_gemini_fallback: bool = False,
    ollama_base_url: str = "http://localhost:11434",
    omlx_base_url: str | None = None,
    omlx_api_key: str | None = None,
) -> list[JobListing]:
    """Extract job listings using the 5-tier pipeline.

    Attempts extraction in order of token efficiency:
    1. Structured data (JSON-LD) — zero LLM tokens
    2. trafilatura + oMLX (fleet-local inference)
    3. Hosted Gemini (if enabled and oMLX unavailable)

    Parameters
    ----------
    html:
        Raw HTML page content.
    url:
        Page URL (used for relative link resolution, logging).
    enable_ollama:
        Whether to attempt local LLM extraction (default: True).
        Name kept for backward compatibility; now routes to oMLX.
    enable_gemini_fallback:
        Whether to fall back to hosted Gemini when oMLX unavailable
        (respects HOSTED_LLM_FALLBACK_ENABLED env var). Default: False.
    ollama_base_url:
        Unused; kept for backward compatibility.
    omlx_base_url:
        oMLX base URL override (default: OMLX_BASE_URL env var or http://studio1:8000).
    omlx_api_key:
        oMLX API key override (default: OMLX_API_KEY env var or "strata1").

    Returns
    -------
    list[JobListing]
        Extracted job listings (empty on failure).
        Never raises.
    """
    if not html or not html.strip():
        return []

    # Tier 0: Try structured data extraction (JSON-LD, microdata, OpenGraph)
    logger.debug("Tier 0: Extracting structured data from %s", url)
    jobs = _extract_tier_0_structured(html)
    if jobs:
        logger.info("Tier 0 success: extracted %d jobs from %s", len(jobs), url)
        return jobs

    # Tier 2: trafilatura + oMLX (if enabled)
    if enable_ollama:
        logger.debug("Tier 2: Attempting oMLX extraction from %s", url)
        jobs = _extract_tier_2_local_llm(
            html,
            url=url,
            base_url=ollama_base_url,
            omlx_base_url=omlx_base_url,
            omlx_api_key=omlx_api_key,
        )
        if jobs:
            logger.info(
                "Tier 2 success: extracted %d jobs from %s via oMLX",
                len(jobs),
                url,
            )
            return jobs

    # Tier 4: Fallback to hosted Gemini (if enabled and opt-in)
    gemini_enabled = enable_gemini_fallback or os.getenv("HOSTED_LLM_FALLBACK_ENABLED") == "1"
    if gemini_enabled:
        logger.debug("Tier 4: Falling back to hosted Gemini for %s", url)
        jobs = _extract_tier_4_gemini_fallback(html, url=url)
        if jobs:
            logger.info("Tier 4 success: extracted %d jobs from %s via Gemini", len(jobs), url)
            return jobs

    logger.warning("All extraction tiers failed for %s", url)
    return []


async def extract_with_pipeline_async(
    html: str,
    *,
    url: str,
    enable_ollama: bool = True,
    enable_gemini_fallback: bool = False,
    ollama_base_url: str = "http://localhost:11434",
    omlx_base_url: str | None = None,
    omlx_api_key: str | None = None,
) -> list[JobListing]:
    """Async version of extract_with_pipeline.

    Same behavior as sync version but runs in executor to avoid blocking.
    """
    return await asyncio.to_thread(
        extract_with_pipeline,
        html,
        url=url,
        enable_ollama=enable_ollama,
        enable_gemini_fallback=enable_gemini_fallback,
        ollama_base_url=ollama_base_url,
        omlx_base_url=omlx_base_url,
        omlx_api_key=omlx_api_key,
    )


# ============================================================================
# Tier implementations
# ============================================================================


def _extract_tier_0_structured(html: str) -> list[JobListing]:
    """Tier 0: Extract from JSON-LD, microdata, OpenGraph (zero LLM tokens).

    Returns list of JobListings if found, empty list otherwise.
    """
    jobs_from_json_ld = extract_json_ld(html)
    if not jobs_from_json_ld:
        return []

    # Filter to JobPosting types
    job_postings = [
        j for j in jobs_from_json_ld if isinstance(j, dict) and j.get("@type") == "JobPosting"
    ]

    if not job_postings:
        return []

    results = []
    for posting in job_postings:
        try:
            listing = _job_posting_to_listing(posting)
            if listing:
                results.append(listing)
        except Exception as exc:
            logger.debug("Failed to map JSON-LD posting: %s", exc)
            continue

    return results


def _extract_tier_2_local_llm(
    html: str,
    *,
    url: str,
    base_url: str,
    omlx_base_url: str | None = None,
    omlx_api_key: str | None = None,
) -> list[JobListing]:
    """Tier 2: trafilatura + oMLX OpenAI-compat extraction.

    Extracts Markdown using trafilatura, then sends to oMLX for structured extraction.
    Returns empty list if trafilatura or oMLX unavailable.
    """
    # Extract to Markdown (strips boilerplate, nav, ads, etc.)
    markdown = extract_markdown(html, url=url)
    if not markdown:
        return []

    # Import here to avoid hard dependency
    try:
        from strata_harvest.extract.local_llm import OmlxExtractor
    except ImportError:
        logger.warning("litellm not installed: pip install strata-harvest[llm]")
        return []

    extractor = OmlxExtractor(base_url=omlx_base_url, api_key=omlx_api_key)

    # Check if oMLX is reachable
    if not extractor.is_available():
        logger.debug("oMLX not available at %s", extractor.base_url)
        return []

    # Extract jobs list from Markdown
    instruction = (
        "Extract all job postings from the provided text. "
        "Return a JSON array of job posting objects with title, url, location, employment_type, "
        "remote_policy, description, and salary_min/max/currency if available."
    )
    postings = extractor.extract_list(
        markdown,
        JobPostingSchema,
        instruction=instruction,
    )

    if not postings:
        return []

    results = []
    for posting in postings:
        try:
            listing = _posting_schema_to_listing(posting)
            if listing:
                results.append(listing)
        except Exception as exc:
            logger.debug("Failed to convert extracted posting: %s", exc)
            continue

    return results


def _extract_tier_4_gemini_fallback(html: str, *, url: str) -> list[JobListing]:
    """Tier 4: Fallback to hosted Gemini Flash (only when local LLM unavailable).

    Sends cleaned HTML to Gemini via litellm. Requires GEMINI_API_KEY env var.
    Returns empty list if litellm not available or API call fails.
    """
    if litellm is None:
        logger.warning("litellm not installed: pip install strata-harvest[llm]")
        return []

    # Reuse existing HTML cleaning from llm_fallback
    from strata_harvest.parsers.llm_fallback import _clean_html

    cleaned = _clean_html(html)
    if not cleaned.strip():
        return []

    system_prompt = (
        "You are a structured data extraction engine. "
        "Extract job listings from career page content and return ONLY valid JSON "
        "with no additional text. "
        'Return format: {"jobs": [{"title": "...", "url": "...", '
        '"location": "...", "employment_type": "...", '
        '"description": "..."}]}'
    )

    user_prompt = f"""Extract all job listings from this career page.

Page URL: {url}

Page content:
---
{cleaned}
---

Return JSON only."""

    try:
        response = litellm.completion(
            model="gemini/gemini-2.5-flash",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.0,
        )
    except Exception as exc:
        logger.warning("Gemini fallback extraction failed for %s: %s", url, exc)
        return []

    # Parse response
    import json

    if not response.choices:
        return []

    raw_text = response.choices[0].message.content
    if not raw_text:
        return []

    # Extract JSON from markdown code fences if present
    raw_text = raw_text.strip()
    if raw_text.startswith("```"):
        first_newline = raw_text.index("\n") if "\n" in raw_text else len(raw_text)
        raw_text = raw_text[first_newline + 1 :]
        if raw_text.endswith("```"):
            raw_text = raw_text[:-3]
    raw_text = raw_text.strip()

    try:
        data = json.loads(raw_text)
    except (json.JSONDecodeError, ValueError):
        logger.debug("Gemini response not valid JSON")
        return []

    raw_jobs = data.get("jobs", []) if isinstance(data, dict) else []
    if not isinstance(raw_jobs, list):
        return []

    results = []
    for raw in raw_jobs:
        if not isinstance(raw, dict):
            continue
        try:
            listing = _raw_job_dict_to_listing(raw)
            if listing:
                results.append(listing)
        except Exception as exc:
            logger.debug("Failed to convert Gemini job: %s", exc)
            continue

    return results


# ============================================================================
# Conversion helpers
# ============================================================================


def _job_posting_to_listing(posting: dict[str, object]) -> JobListing | None:
    """Convert JSON-LD JobPosting to JobListing."""
    from strata_harvest.parsers._structured_data import salary_to_string

    title = posting.get("title")
    url = posting.get("url")

    if not title or not url:
        return None

    # Extract salary if present
    salary_range = None
    base_salary = posting.get("baseSalary")
    if base_salary:
        salary_range = salary_to_string(base_salary)

    # Extract location from jobLocation object or use directly
    location = None
    job_location = posting.get("jobLocation")
    if isinstance(job_location, dict):
        location = job_location.get("address", {}).get("addressLocality")
    else:
        location = job_location

    return JobListing(
        title=title,
        url=url,
        location=location,
        employment_type=posting.get("employmentType"),
        description=posting.get("description"),
        salary_range=salary_range,
        ats_provider=ATSProvider.UNKNOWN,
        raw_data=posting,
    )


def _posting_schema_to_listing(schema: JobPostingSchema) -> JobListing | None:
    """Convert JobPostingSchema to JobListing."""
    if not schema.title or not schema.url:
        return None

    # Format salary range if available
    salary_range = None
    if schema.salary_min is not None and schema.salary_max is not None:
        currency = schema.salary_currency or "USD"
        salary_range = f"{currency} {schema.salary_min:,.0f}–{schema.salary_max:,.0f}"
    elif schema.salary_min is not None:
        currency = schema.salary_currency or "USD"
        salary_range = f"{currency} {schema.salary_min:,.0f}+"

    return JobListing(
        title=schema.title,
        url=str(schema.url),
        location=schema.location,
        employment_type=schema.employment_type,
        description=schema.description,
        salary_range=salary_range,
        ats_provider=ATSProvider.UNKNOWN,
        raw_data=schema.model_dump(exclude_none=True),
    )


def _raw_job_dict_to_listing(raw: dict[str, object]) -> JobListing | None:
    """Convert raw dict from Gemini to JobListing."""
    title = raw.get("title")
    url = raw.get("url")

    if not title or not url:
        return None

    return JobListing(
        title=title,
        url=url,
        location=raw.get("location"),
        employment_type=raw.get("employment_type"),
        description=raw.get("description"),
        salary_range=raw.get("salary_range"),
        ats_provider=ATSProvider.UNKNOWN,
        raw_data=raw,
    )
