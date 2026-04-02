# strata-harvest

**Career page scraping and ATS parsing library.** Point it at a company careers page, get back structured job listings — regardless of which applicant tracking system they use.

## Quick Start

```python
import asyncio
from strata_harvest import harvest

async def main():
    jobs = await harvest("https://boards.greenhouse.io/example/jobs")
    for job in jobs:
        print(f"{job.title} — {job.location}")

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

## Guides

- [Adding a New ATS Parser](adding-a-parser.md) — Step-by-step contributor guide
- [LLM Configuration](llm-configuration.md) — Set up Gemini, OpenAI, Ollama, or any LiteLLM provider
- [Advanced Usage](advanced-usage.md) — Custom crawlers, batch scraping, change detection, rate limiting

## API Reference

- [Public API](api/public.md) — `harvest()`, `create_crawler()`, and top-level exports
- [Models](api/models.md) — `JobListing`, `ScrapeResult`, `ATSInfo`, `ATSProvider`
- [Crawler](api/crawler.md) — `Crawler` class with `scrape()` and `scrape_batch()`
- [Detector](api/detector.md) — ATS detection functions
- [Parsers](api/parsers.md) — Provider-specific parser classes
