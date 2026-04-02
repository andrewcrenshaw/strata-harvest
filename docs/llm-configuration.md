# LLM Configuration

When `strata-harvest` encounters a career page it can't identify (no known ATS detected), it can use an LLM to extract structured job listings from the raw page content. This guide covers setup for every supported provider.

## Prerequisites

Install the `llm` extra:

```bash
pip install strata-harvest[llm]
```

This adds [LiteLLM](https://docs.litellm.ai/) as a dependency, which routes to 100+ LLM providers through a unified interface.

## Quick Start

```python
from strata_harvest import create_crawler

crawler = create_crawler(llm_provider="gemini/gemini-2.0-flash")
result = await crawler.scrape("https://custom-careers-page.com/jobs")

for job in result.jobs:
    print(f"{job.title} — {job.location}")
```

The `llm_provider` string follows [LiteLLM's model naming convention](https://docs.litellm.ai/docs/providers): `provider/model-name`.

## Provider Setup

### Google Gemini (Default)

The default model is `gemini/gemini-2.0-flash` — fast, cheap, and effective for structured extraction.

```bash
export GEMINI_API_KEY="your-api-key"
```

```python
crawler = create_crawler(llm_provider="gemini/gemini-2.0-flash")
```

Other Gemini models:

| Model | Speed | Cost | Notes |
|-------|-------|------|-------|
| `gemini/gemini-2.0-flash` | Fast | Low | Default — good balance of speed and quality |
| `gemini/gemini-2.5-pro-preview-03-25` | Slower | Higher | Better for complex pages with non-standard layouts |

### OpenAI

```bash
export OPENAI_API_KEY="your-api-key"
```

```python
crawler = create_crawler(llm_provider="openai/gpt-4o-mini")
```

| Model | Speed | Cost | Notes |
|-------|-------|------|-------|
| `openai/gpt-4o-mini` | Fast | Low | Recommended for most use cases |
| `openai/gpt-4o` | Medium | Medium | Better extraction on complex pages |

### Anthropic Claude

```bash
export ANTHROPIC_API_KEY="your-api-key"
```

```python
crawler = create_crawler(llm_provider="anthropic/claude-sonnet-4-20250514")
```

### Ollama (Local / Self-hosted)

No API key needed — runs locally.

```bash
# Start Ollama (if not already running)
ollama serve

# Pull a model
ollama pull llama3.1
```

```python
crawler = create_crawler(llm_provider="ollama/llama3.1")
```

For Ollama running on a non-default host:

```bash
export OLLAMA_API_BASE="http://your-host:11434"
```

### Azure OpenAI

```bash
export AZURE_API_KEY="your-key"
export AZURE_API_BASE="https://your-resource.openai.azure.com"
export AZURE_API_VERSION="2024-02-01"
```

```python
crawler = create_crawler(llm_provider="azure/your-deployment-name")
```

### AWS Bedrock

```bash
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_REGION_NAME="us-east-1"
```

```python
crawler = create_crawler(llm_provider="bedrock/anthropic.claude-3-sonnet-20240229-v1:0")
```

## How It Works

When the ATS detector returns `ATSProvider.UNKNOWN` and an `llm_provider` is configured:

1. The page HTML is cleaned — scripts, styles, and tags are stripped while preserving link URLs
2. The cleaned text is sent to the LLM with a structured extraction prompt
3. The LLM returns JSON with `title`, `url`, `location`, `department`, and other fields
4. The response is parsed into typed `JobListing` objects

The extraction prompt instructs the LLM to:
- Extract **all** visible job listings from the page
- Require `title` and `url` for each listing (skip jobs missing either)
- Resolve relative URLs using the page's base URL
- Return an empty array if no listings are found
- Never invent data — only extract what's present

## When the LLM Fallback Activates

The LLM parser only runs when **both** conditions are met:

1. The ATS detector returns `UNKNOWN` (no URL pattern or DOM signature matched)
2. An `llm_provider` is configured on the crawler

If no `llm_provider` is set, unknown pages return an empty job list rather than calling any LLM.

```python
# No LLM — unknown ATS returns empty results
crawler = create_crawler()
result = await crawler.scrape("https://custom-page.com/jobs")
assert result.jobs == []
assert result.ats_info.provider.value == "unknown"

# With LLM — attempts extraction
crawler = create_crawler(llm_provider="gemini/gemini-2.0-flash")
result = await crawler.scrape("https://custom-page.com/jobs")
# result.jobs may now contain listings extracted by the LLM
```

## Cost and Performance Considerations

| Factor | Guidance |
|--------|----------|
| **Token usage** | Pages are cleaned before sending; typical page is 2K–10K tokens |
| **Latency** | LLM calls add 1–5 seconds per page depending on provider and model |
| **Accuracy** | Structured extraction works well for standard career page layouts; edge cases (PDFs, JavaScript-rendered pages) may return partial results |
| **Rate limits** | LiteLLM respects provider rate limits; the crawler's own rate limiter also applies |
| **Cost** | Flash/mini models cost fractions of a cent per page; budget ~$0.001–0.01 per extraction |

## Troubleshooting

### `LLM fallback requires the llm extra`

You haven't installed the `llm` extra:

```bash
pip install strata-harvest[llm]
```

### Empty results from LLM extraction

- Check that your API key is set and valid
- Try a different model — some handle structured extraction better than others
- Check `result.error` on the `ScrapeResult` for HTTP-level failures
- Enable debug logging to see the LLM request/response:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### LLM returns malformed JSON

The parser strips markdown code fences and retries JSON parsing. If the model consistently returns invalid output, switch to a model with stronger structured output support (GPT-4o-mini and Gemini Flash are reliable choices).
