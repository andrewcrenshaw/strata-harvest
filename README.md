# strata-harvest

[![Status: Pre-Alpha](https://img.shields.io/badge/status-pre--alpha%20(0.x)-orange.svg)](https://semver.org/spec/v2.0.0.html)
[![PyPI version](https://img.shields.io/pypi/v/strata-harvest.svg)](https://pypi.org/project/strata-harvest/)
[![Python versions](https://img.shields.io/pypi/pyversions/strata-harvest.svg)](https://pypi.org/project/strata-harvest/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Career page scraping and ATS parsing library.** Point it at a company careers page, get back structured job listings — regardless of which applicant tracking system they use.

Every company posts jobs differently. Greenhouse uses a REST API. Lever has a JSON feed. Ashby hides behind GraphQL. Workday is... Workday. `strata-harvest` handles the detection and parsing so you don't have to reverse-engineer each one.

## Why This Exists

Job data is fragmented across dozens of ATS platforms, each with its own page structure, API format, and quirks. If you're building anything that needs to read job listings programmatically — a job board, a recruiting tool, a market research pipeline — you hit the same wall: every career page is a snowflake.

`strata-harvest` solves this with a three-step approach:

1. **Detect** — Identify the ATS provider from a URL using pattern matching and DOM probing
2. **Parse** — Use the provider-specific parser (REST, JSON, GraphQL) to extract structured data
3. **Fall back** — For unknown providers, use an optional LLM-based extractor that reads the page and returns structured listings anyway

The result is a single `harvest(url)` call that returns clean, typed job data from any career page.

## Use Cases

- **Job search automation** — Scrape target company career pages on a schedule, detect new postings, feed them into a matching pipeline
- **Recruiting intelligence** — Monitor competitor hiring patterns, track which roles are open/closed over time, identify market signals
- **Job board aggregation** — Build a focused job board for a niche (e.g., climate tech, AI/ML) by harvesting from curated company lists
- **HR analytics** — Track time-to-fill by monitoring when listings appear and disappear, analyze job requirement trends across an industry
- **Salary benchmarking** — Collect job descriptions at scale for compensation analysis and market positioning

## Quick Start

```python
import asyncio
from strata_harvest import harvest, create_crawler

async def main():
    # One-shot: get job listings from any career page
    listings = await harvest("https://boards.greenhouse.io/example/jobs")
    for job in listings:
        print(f"{job.title} — {job.location}")

    # Reusable crawler with rate limiting and diagnostics
    crawler = create_crawler(rate_limit=2.0)
    result = await crawler.scrape("https://jobs.lever.co/example")
    print(f"Found {len(result.jobs)} jobs via {result.ats_info.provider}")
    if result.error:
        print(f"Warning: {result.error}")

asyncio.run(main())
```

## Installation

```bash
pip install strata-harvest
```

For LLM-based fallback parsing (handles unknown ATS providers):

```bash
pip install strata-harvest[llm]
```

Requires **Python 3.11+**.

## How It Works

```
URL → ATS Detection → Provider-Specific Parser → Structured JobListings
         │                     │
         │                     ├── Greenhouse (REST API)
         │                     ├── Lever (JSON API)
         │                     ├── Ashby (GraphQL)
         │                     ├── Workday (planned)
         │                     ├── iCIMS (planned)
         │                     └── Unknown → LLM fallback
         │
         └── Pattern matching + DOM probing
             Returns ATSInfo with provider + confidence score
```

### ATS Detection

The detector identifies providers using URL patterns and DOM signatures, returning a confidence score. This means you don't need to know which ATS a company uses — just pass the careers URL.

```python
from strata_harvest.detector import detect_ats

info = await detect_ats("https://boards.greenhouse.io/stripe/jobs")
print(info.provider)    # ATSProvider.GREENHOUSE
print(info.confidence)  # 0.95
```

### Provider Parsers

Each supported ATS has a dedicated parser that knows how to call its API and normalize the response into `JobListing` objects:

| Provider | Detection | Parsing | API Type |
|----------|-----------|---------|----------|
| Greenhouse | URL + DOM | Full | REST (`/embed/api/v1/jobs`) |
| Lever | URL + DOM | Full | JSON feed |
| Ashby | URL + DOM | Full | GraphQL |
| Workday | URL + DOM | Planned | — |
| iCIMS | URL + DOM | Planned | — |
| Unknown | — | LLM fallback | Page content → structured extraction |

### LLM Fallback

When the detector can't identify the ATS, the optional LLM fallback reads the page content and extracts job listings using structured prompts. This handles the long tail of custom career pages and lesser-known ATS platforms.

```python
crawler = create_crawler(llm_provider="gemini-flash")
result = await crawler.scrape("https://custom-careers-page.com/jobs")
```

### Data Models

All parsed data uses typed Pydantic models:

```python
from strata_harvest.models import JobListing, ScrapeResult, ATSInfo

# JobListing: title, company, location, url, description, requirements, salary_range, ...
# ScrapeResult: jobs, ats_info, error, timing, content_hash
# ATSInfo: provider, confidence, detection_method
```

## Part of the Strata Ecosystem

`strata-harvest` is the data collection layer for [Strata](https://github.com/andrewcrenshaw/strata) — an autonomous AI job search platform where specialized agents collaborate to discover, evaluate, and match job opportunities. In that context, `strata-harvest` feeds the Scraper Agent, which runs daily sweeps across target company career pages and routes new listings through a deduplication and matching pipeline.

But `strata-harvest` is fully standalone. It has no dependency on the Strata platform and works anywhere you need structured job data from career pages.

## Development

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/) (or pip/venv).

```bash
git clone https://github.com/andrewcrenshaw/strata-harvest.git
cd strata-harvest

# Install with dev dependencies
uv sync --all-extras

# Run tests
uv run pytest

# Lint
uv run ruff check .

# Type check
uv run mypy src/strata_harvest
```

### Adding a New Parser

Each ATS provider gets its own parser module in `src/strata_harvest/parsers/`. Parsers extend the base class and implement `parse(url) -> list[JobListing]`. See `parsers/greenhouse.py` for the pattern.

## License

MIT
