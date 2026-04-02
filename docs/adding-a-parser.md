# Adding a New ATS Parser

This guide walks through adding a parser for a new applicant tracking system. By the end you'll have a parser that auto-registers with the detection pipeline and produces typed `JobListing` objects.

## Prerequisites

- Python 3.11+
- Dev dependencies installed: `uv sync --all-extras` or `pip install -e ".[dev]"`
- Familiarity with the target ATS's career page structure or API

## Architecture Overview

```
URL → detect_ats() → BaseParser.for_provider() → YourParser.parse() → list[JobListing]
```

Every parser lives in `src/strata_harvest/parsers/` and extends `BaseParser`. Setting the `provider` class attribute auto-registers the parser — no manual wiring needed.

## Step 1: Add the Provider Enum

If the ATS isn't already in `ATSProvider`, add it to `src/strata_harvest/models.py`:

```python
class ATSProvider(StrEnum):
    GREENHOUSE = "greenhouse"
    LEVER = "lever"
    ASHBY = "ashby"
    WORKDAY = "workday"
    ICIMS = "icims"
    YOUR_ATS = "your_ats"  # ← add here
    UNKNOWN = "unknown"
```

## Step 2: Add Detection Patterns

Update `src/strata_harvest/detector.py` with URL patterns and DOM signatures for the new provider.

**URL patterns** (fast, no network):

```python
_URL_PATTERNS: list[tuple[re.Pattern[str], ATSProvider, str | None]] = [
    # ... existing patterns ...
    (re.compile(r"careers\.your-ats\.com"), ATSProvider.YOUR_ATS, None),
]
```

The third element is an optional API URL template. Use `{board}` as a placeholder if the API URL derives from the career page path.

**DOM signatures** (fallback, requires fetched HTML):

```python
_DOM_SIGNATURES: list[tuple[re.Pattern[str], ATSProvider, float]] = [
    # ... existing signatures ...
    (re.compile(r"your-ats-widget|your-ats\.com", re.I), ATSProvider.YOUR_ATS, 0.80),
]
```

The float is a confidence score (0.0–1.0). Use 0.85+ for highly specific patterns, 0.70–0.80 for patterns that could have false positives.

## Step 3: Create the Parser Module

Create `src/strata_harvest/parsers/your_ats.py`:

```python
"""YourATS parser — [API type] extraction."""

from __future__ import annotations

import json
import logging
from typing import Any

from strata_harvest.models import ATSProvider, JobListing
from strata_harvest.parsers.base import BaseParser

logger = logging.getLogger(__name__)


class YourATSParser(BaseParser):
    """Parse job listings from YourATS career pages."""

    provider = ATSProvider.YOUR_ATS  # ← auto-registers this parser

    def parse(self, content: str, *, url: str) -> list[JobListing]:
        """Parse raw content into job listings.

        Parameters
        ----------
        content:
            Raw page content (HTML or JSON depending on what was fetched).
        url:
            The original URL that was scraped.

        Returns
        -------
        list[JobListing]
            Parsed listings. Empty list on failure — never raises.
        """
        try:
            data = json.loads(content)
        except (json.JSONDecodeError, TypeError):
            logger.debug("Content is not valid JSON for YourATS parser")
            return []

        # Extract job objects from the response structure
        raw_jobs = data.get("jobs", [])
        if not isinstance(raw_jobs, list):
            return []

        results: list[JobListing] = []
        for raw in raw_jobs:
            if not isinstance(raw, dict):
                continue
            try:
                results.append(self._parse_job(raw))
            except Exception:
                logger.debug("Skipping malformed YourATS job: %s", raw.get("id", "?"))
                continue

        return results

    @staticmethod
    def _parse_job(raw: dict[str, Any]) -> JobListing:
        """Map a single API response object to a JobListing."""
        title = raw.get("title")
        url = raw.get("url")
        if not title or not url:
            msg = f"YourATS job missing title or url: {raw.get('id', '?')}"
            raise ValueError(msg)

        return JobListing(
            title=title,
            url=url,
            location=raw.get("location"),
            department=raw.get("department"),
            employment_type=raw.get("employment_type"),
            description=raw.get("description"),
            requirements=raw.get("requirements", []),
            salary_range=raw.get("salary_range"),
            ats_provider=ATSProvider.YOUR_ATS,
            raw_data=raw,
        )
```

### Key conventions

| Convention | Why |
|-----------|-----|
| `parse()` never raises | The crawler expects empty lists on failure, not exceptions |
| Set `provider` class attribute | Auto-registers via `BaseParser.__init_subclass__` |
| Use `logger.debug()` for skip messages | Keeps output clean unless explicitly debugging |
| Store `raw_data` on each listing | Preserves provider-specific fields for downstream consumers |

## Step 4: Register the Import

Add the parser to `src/strata_harvest/parsers/__init__.py`:

```python
from strata_harvest.parsers.your_ats import YourATSParser

__all__ = [
    # ... existing parsers ...
    "YourATSParser",
]
```

## Step 5: Write Tests

Create `tests/test_your_ats_parser.py`. Follow the pattern in existing test files:

```python
"""Tests for YourATS parser."""

import pytest

from strata_harvest.models import ATSProvider
from strata_harvest.parsers.your_ats import YourATSParser


class TestYourATSParser:
    """Unit tests for YourATSParser."""

    def setup_method(self) -> None:
        self.parser = YourATSParser()

    def test_provider_is_registered(self) -> None:
        from strata_harvest.parsers.base import BaseParser
        parser = BaseParser.for_provider(ATSProvider.YOUR_ATS)
        assert isinstance(parser, YourATSParser)

    def test_parse_valid_json(self) -> None:
        content = '{"jobs": [{"title": "Engineer", "url": "https://example.com/1"}]}'
        jobs = self.parser.parse(content, url="https://careers.your-ats.com/acme")
        assert len(jobs) == 1
        assert jobs[0].title == "Engineer"
        assert jobs[0].ats_provider == ATSProvider.YOUR_ATS

    def test_parse_empty_content(self) -> None:
        assert self.parser.parse("", url="https://example.com") == []

    def test_parse_invalid_json(self) -> None:
        assert self.parser.parse("<html>not json</html>", url="https://example.com") == []

    def test_parse_skips_malformed_jobs(self) -> None:
        content = '{"jobs": [{"title": "Good", "url": "https://example.com/1"}, {"bad": true}]}'
        jobs = self.parser.parse(content, url="https://example.com")
        assert len(jobs) == 1
```

### Test categories

Use pytest markers to classify tests:

```python
@pytest.mark.verification  # Fast, no network — runs on every CI push
def test_parse_valid_json(self) -> None: ...

@pytest.mark.integration   # Requires network or external services
async def test_fetch_real_board(self) -> None: ...
```

## Step 6: Update the Provider Table

Update the provider table in `README.md`:

```markdown
| Provider | Detection | Parsing | API Type |
|----------|-----------|---------|----------|
| YourATS  | URL + DOM | Full    | REST / GraphQL / etc. |
```

## Step 7: Verify

```bash
# Lint
uv run ruff check src/strata_harvest/parsers/your_ats.py tests/test_your_ats_parser.py

# Type check
uv run mypy src/strata_harvest/parsers/your_ats.py

# Run your tests
uv run pytest tests/test_your_ats_parser.py -v

# Run full verification suite
uv run pytest -m verification --tb=short -q
```

## Optional: Add a `fetch_all()` Class Method

If the ATS has a paginated API, add a `fetch_all()` method for direct API access (bypassing the generic crawler fetch). See `LeverParser.fetch_all()` for pagination handling or `AshbyParser.fetch_all()` for GraphQL.

```python
@classmethod
async def fetch_all(cls, url: str, *, client: Any = None) -> list[JobListing]:
    """Fetch all listings directly from the YourATS API."""
    api_url = cls.build_api_url(url)
    result = await safe_fetch(api_url, client=client)
    if not result.ok or not result.content:
        return []
    return cls().parse(result.content, url=url)
```

## Optional: Add Test Fixtures

For realistic test data, add fixture files under `tests/fixtures/`:

```
tests/fixtures/
├── your_ats_board.json       # Sample API response
└── your_ats_single_job.json  # Single job response
```

Use `tests/fixture_loader.py` to load them in tests:

```python
from tests.fixture_loader import load_fixture

def test_parse_fixture(self) -> None:
    content = load_fixture("your_ats_board.json")
    jobs = self.parser.parse(content, url="https://example.com")
    assert len(jobs) > 0
```
