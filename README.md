# strata-harvest

Career page scraping and ATS (Applicant Tracking System) parsing library. Detects ATS providers, extracts structured job listings, and provides resilient HTTP fetching with rate limiting.

## Installation

```bash
pip install strata-harvest
```

For LLM-based fallback extraction (unknown ATS providers):

```bash
pip install strata-harvest[llm]
```

## Quick Start

```python
from strata_harvest import harvest, detect_ats, create_crawler

# One-shot harvest from a career page URL
listings = await harvest("https://company.com/careers")

# Detect which ATS a career page uses
ats_info = await detect_ats("https://company.com/careers")
print(ats_info.provider, ats_info.confidence)

# Create a configured crawler for repeated use
crawler = create_crawler(rate_limit=2.0)
listings = await crawler.scrape("https://company.com/careers")
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

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
# Install with dev dependencies
uv sync --all-extras

# Run tests
uv run pytest

# Lint
uv run ruff check .

# Type check
uv run mypy src/ tests/
```

## License

MIT
