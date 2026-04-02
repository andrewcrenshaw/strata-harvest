# Advanced Usage

This guide covers power-user patterns: custom crawler configuration, batch scraping, change detection, rate limiting, and direct parser access.

## Custom Crawler Configuration

The `create_crawler()` factory exposes all tuning knobs:

```python
from strata_harvest import create_crawler

crawler = create_crawler(
    rate_limit=0.25,                       # 1 request every 4 seconds
    timeout=45.0,                          # 45s per-request timeout
    user_agent="MyBot/1.0 (contact@me.com)",  # custom User-Agent
    llm_provider="openai/gpt-4o-mini",     # LLM fallback for unknown ATS
)
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `rate_limit` | `0.5` | Max requests per second (0.5 = one request every 2 seconds) |
| `timeout` | `30.0` | Per-request HTTP timeout in seconds |
| `user_agent` | `strata-harvest/0.1` | `User-Agent` header sent with requests |
| `headless` | `False` | Reserved for future headless-browser rendering |
| `proxy` | `None` | Reserved for future HTTP(S) proxy support |
| `llm_provider` | `None` | LiteLLM model string for fallback extraction (see [LLM Configuration](llm-configuration.md)) |

## Change Detection

Track when career pages update by comparing content hashes across scrape runs:

```python
crawler = create_crawler()

# First scrape — no previous hash
first = await crawler.scrape("https://boards.greenhouse.io/example/jobs")
print(f"Hash: {first.content_hash}")
print(f"Changed: {first.changed}")  # True (no previous hash to compare)

# Later scrape — pass previous hash
second = await crawler.scrape(
    "https://boards.greenhouse.io/example/jobs",
    previous_hash=first.content_hash,
)
print(f"Changed: {second.changed}")  # False if page content is identical
```

The hash is computed on whitespace-normalized content, so cosmetic formatting changes don't trigger false positives.

### Change Detection in a Pipeline

```python
import json
from pathlib import Path

HASH_FILE = Path("scrape_hashes.json")

def load_hashes() -> dict[str, str]:
    if HASH_FILE.exists():
        return json.loads(HASH_FILE.read_text())
    return {}

def save_hashes(hashes: dict[str, str]) -> None:
    HASH_FILE.write_text(json.dumps(hashes, indent=2))

async def check_for_updates(urls: list[str]) -> None:
    crawler = create_crawler()
    hashes = load_hashes()

    async for result in crawler.scrape_batch(urls):
        previous = hashes.get(result.url)
        if previous and result.content_hash == previous:
            print(f"  No change: {result.url}")
            continue

        print(f"  Updated: {result.url} — {len(result.jobs)} jobs")
        if result.content_hash:
            hashes[result.url] = result.content_hash

    save_hashes(hashes)
```

## Batch Scraping

Scrape multiple URLs concurrently with `scrape_batch()`:

```python
crawler = create_crawler(rate_limit=1.0)

urls = [
    "https://boards.greenhouse.io/stripe/jobs",
    "https://jobs.lever.co/figma",
    "https://jobs.ashbyhq.com/linear",
]

async for result in crawler.scrape_batch(urls, concurrency=3):
    if result.ok:
        print(f"{result.url}: {len(result.jobs)} jobs via {result.ats_info.provider}")
    else:
        print(f"{result.url}: ERROR — {result.error}")
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `concurrency` | `5` | Maximum concurrent scrape tasks |

The rate limiter still applies globally — `concurrency` controls how many tasks can be in-flight, while `rate_limit` paces actual HTTP requests.

## Rate Limiting

The built-in token-bucket rate limiter prevents overwhelming target servers:

```python
# Conservative: 1 request every 4 seconds
crawler = create_crawler(rate_limit=0.25)

# Aggressive: 2 requests per second (be careful)
crawler = create_crawler(rate_limit=2.0)
```

The rate limiter is shared across all `scrape()` and `scrape_batch()` calls on the same `Crawler` instance. Create separate crawlers if you need independent rate limits for different target domains.

## Direct Parser Access

For advanced use cases, access parsers directly without going through the crawler:

### ATS Detection

```python
from strata_harvest.detector import detect_ats, detect_from_url, detect_from_dom

# Fast URL-only detection (no network)
info = detect_from_url("https://boards.greenhouse.io/stripe/jobs")
print(info.provider)    # ATSProvider.GREENHOUSE
print(info.confidence)  # 0.9

# Full detection with DOM fallback (may fetch the page)
info = await detect_ats("https://example.com/careers")
print(info.provider)    # ATSProvider.UNKNOWN if not recognized
print(info.detection_method)  # "url_pattern", "dom_probe", or "none"

# Detection with pre-fetched HTML (avoids extra fetch)
info = await detect_ats("https://example.com/careers", html=page_html)
```

### Direct Parser Use

```python
from strata_harvest.parsers.greenhouse import GreenhouseParser
from strata_harvest.parsers.lever import LeverParser
from strata_harvest.parsers.ashby import AshbyParser

# Parse pre-fetched content
parser = GreenhouseParser()
jobs = parser.parse(json_content, url="https://boards.greenhouse.io/acme/jobs")

# Fetch and parse directly via the parser's API method
jobs = await GreenhouseParser.fetch_all("https://boards.greenhouse.io/acme/jobs")
jobs = await LeverParser.fetch_all("https://jobs.lever.co/figma")
jobs = await AshbyParser.fetch_all("https://jobs.ashbyhq.com/linear")
```

### HTTP Utilities

The `safe_fetch()` function never raises — transport errors are captured as structured results:

```python
from strata_harvest.utils.http import safe_fetch

result = await safe_fetch(
    "https://api.example.com/jobs",
    timeout=10.0,
    headers={"Authorization": "Bearer token"},
)

if result.ok:
    print(result.content)       # Raw response body
    print(result.data)          # Parsed JSON (if response was JSON)
    print(result.elapsed_ms)    # Request duration
else:
    print(result.error)         # Human-readable error string
    print(result.status_code)   # HTTP status code (if request completed)
```

Responses are read in chunks; by default `max_response_bytes` is 10 MiB and oversize bodies return `result.ok is False` with an error describing received vs allowed size.

## Proxy Support (Planned)

Proxy support is reserved in the API but not yet implemented:

```python
# Reserved — not yet functional
crawler = create_crawler(proxy="http://proxy.example.com:8080")
```

Track progress on proxy support in the [strata-harvest issue tracker](https://github.com/andrewcrenshaw/strata-harvest/issues).

## Headless Browser Rendering (Planned)

Some career pages require JavaScript rendering. Headless browser support is reserved:

```python
# Reserved — not yet functional
crawler = create_crawler(headless=True)
```

## Error Handling Patterns

`strata-harvest` is designed to never raise on expected failures. Errors surface on the result objects:

```python
result = await crawler.scrape("https://example.com/404")

if result.error:
    print(f"Scrape failed: {result.error}")
    # e.g. "HTTP 404: Not Found"

if not result.ok:
    # ok is False when: error is set OR jobs list is empty
    print("No usable results")

# Even on error, ats_info and timing are populated
print(f"ATS: {result.ats_info.provider}")
print(f"Duration: {result.scrape_duration_ms}ms")
```

### Batch Error Handling

```python
successes = 0
failures = 0

async for result in crawler.scrape_batch(urls):
    if result.ok:
        successes += 1
        process_jobs(result.jobs)
    else:
        failures += 1
        log_error(result.url, result.error)

print(f"Done: {successes} succeeded, {failures} failed")
```

## Connection Pooling

For high-throughput scraping, pass an `httpx.AsyncClient` to parser `fetch_all()` methods for connection reuse:

```python
import httpx
from strata_harvest.parsers.greenhouse import GreenhouseParser

async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
    jobs_a = await GreenhouseParser.fetch_all(url_a, client=client)
    jobs_b = await GreenhouseParser.fetch_all(url_b, client=client)
```

## Logging

`strata-harvest` uses Python's standard `logging` module. Enable debug logging to see detection decisions, parser skips, and HTTP details:

```python
import logging

logging.basicConfig(level=logging.DEBUG)

# Or target specific modules
logging.getLogger("strata_harvest.detector").setLevel(logging.DEBUG)
logging.getLogger("strata_harvest.parsers").setLevel(logging.DEBUG)
logging.getLogger("strata_harvest.utils.http").setLevel(logging.DEBUG)
```
