# strata-harvest

[![PyPI version](https://img.shields.io/pypi/v/strata-harvest.svg)](https://pypi.org/project/strata-harvest/)
[![Python versions](https://img.shields.io/pypi/pyversions/strata-harvest.svg)](https://pypi.org/project/strata-harvest/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Career page scraping and ATS (Applicant Tracking System) parsing library. Detects ATS providers, extracts structured job listings, and provides resilient HTTP fetching with rate limiting.

## Features

- **One-shot scraping** — `harvest(url)` returns parsed job rows from a career page URL.
- **Reusable crawler** — `create_crawler()` for rate limiting, timeouts, and full `ScrapeResult` diagnostics.
- **Multi-ATS** — Greenhouse, Lever, Ashby, and planned/stub coverage for Workday and iCIMS; unknown boards can use an optional LLM fallback (`strata-harvest[llm]`).
- **Structured models** — `JobListing`, `ScrapeResult`, and `ATSInfo` for downstream pipelines.
- **MIT licensed** — installable from PyPI for use in job-search and recruiting automation.

## Installation

```bash
pip install strata-harvest
```

For LLM-based fallback extraction (unknown ATS providers):

```bash
pip install strata-harvest[llm]
```

Requires **Python 3.11+**.

## Quick Start

```python
import asyncio

from strata_harvest import create_crawler, harvest

async def main() -> None:
    # One-shot: parsed job rows from a career page URL
    listings = await harvest("https://company.com/careers")
    print(len(listings))

    # Reusable crawler with rate limiting and full ScrapeResult diagnostics
    crawler = create_crawler(rate_limit=2.0)
    result = await crawler.scrape("https://company.com/careers")
    print(len(result.jobs), result.ats_info.provider, result.error)

asyncio.run(main())
```

### Advanced (submodules)

Detection, the `Crawler` class, and extra model types are imported from submodules so the package root stays minimal:

```python
from strata_harvest.detector import detect_ats
from strata_harvest.crawler import Crawler
from strata_harvest.models import ATSProvider
```

## Supported ATS Providers

| Provider | Detection | Parsing |
|----------|-----------|---------|
| Greenhouse | URL + DOM | REST API |
| Lever | URL + DOM | JSON API |
| Ashby | URL + DOM | GraphQL |
| Workday | URL + DOM | Planned |
| iCIMS | URL + DOM | Planned |
| Unknown | — | LLM fallback |

## Development

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/) (or pip/venv).

```bash
# Install with dev dependencies
uv sync --all-extras

# Run tests
uv run pytest

# Lint
uv run ruff check .

# Type check (package; strict — tests may have separate ignores)
uv run mypy src/strata_harvest
```

## License

MIT
