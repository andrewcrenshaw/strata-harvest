"""LLM-based fallback parser for unknown ATS providers.

When no recognized ATS is detected on a career page, this parser sends the
cleaned page content to an LLM and asks it to extract structured job listings.
The LLM provider is configurable (default: Gemini Flash via litellm).

Truncation handling (PCC-1969):
- Detects ``finish_reason == "length"`` and raises :exc:`TruncatedCompletionError`.
- Automatically retries with ``max_tokens * 2`` on first truncation.
- Falls back to chunked HTML parsing if retry also truncates.

JSON salvage (PCC-1969):
- Attempts ``json_repair.loads()`` when ``json.loads`` fails.
- Tags repaired parses as :attr:`~strata_harvest.models.ParseStatus.SALVAGED`.
- Emits a WARN when source salvage rate exceeds 10 % over a rolling 24-hour window.

Requires the ``llm`` extra: ``pip install strata-harvest[llm]``
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections import defaultdict, deque
from typing import Any, NamedTuple
from urllib.parse import urlparse

try:
    import litellm
except ImportError:  # pragma: no cover - base install without ``[llm]`` extra
    litellm = None

try:
    import json_repair
except ImportError:  # pragma: no cover - base install without ``[llm]`` extra
    json_repair = None  # type: ignore[assignment]

from strata_harvest.models import ATSProvider, JobListing, ParseStatus
from strata_harvest.parsers.base import BaseParser

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "gemini/gemini-2.5-flash"

# Default token budget for retry doubling when no explicit max_tokens was set.
_DEFAULT_MAX_TOKENS = 4_096

# Characters per HTML chunk when falling back to chunked parsing.
_CHUNK_CHARS = 6_000

# Rolling window for salvage-rate telemetry (seconds).
_SALVAGE_RATE_WINDOW_S = 86_400  # 24 h

# Salvage-rate threshold above which a WARN is emitted.
_SALVAGE_RATE_THRESHOLD = 0.10

# Severity ordering for parse statuses (higher = worse).
_STATUS_SEVERITY: dict[ParseStatus, int] = {
    ParseStatus.CLEAN: 0,
    ParseStatus.SALVAGED: 1,
    ParseStatus.TRUNCATED: 2,
    ParseStatus.FAILED: 3,
}

_SCRIPT_STYLE_RE = re.compile(
    r"<(script|style|noscript)[^>]*>.*?</\1>",
    re.DOTALL | re.IGNORECASE,
)
_TAG_RE = re.compile(r"<[^>]+>")
_LINK_RE = re.compile(
    r'<a\s[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
    re.DOTALL | re.IGNORECASE,
)
_MULTI_NEWLINE_RE = re.compile(r"\n{3,}")
_MULTI_SPACE_RE = re.compile(r"[ \t]{2,}")

SYSTEM_PROMPT = """\
You are a structured data extraction engine. Your task is to extract job listings \
from career page content. Return ONLY valid JSON with no additional text.

Output format:
```json
{
  "jobs": [
    {
      "title": "Job Title (required)",
      "url": "Full URL to the job posting (required)",
      "location": "Location or null",
      "department": "Department or null",
      "employment_type": "Full-time, Part-time, Contract, Internship, or null",
      "description": "Brief job description or null",
      "requirements": ["requirement 1", "requirement 2"],
      "salary_range": "Salary range string or null"
    }
  ]
}
```

Rules:
- Extract ALL job listings visible on the page
- title and url are required — skip any job that lacks either
- For relative URLs, construct the full URL using the page's base URL
- Return an empty jobs array if no job listings are found
- Do NOT invent data — only extract what is present on the page\
"""

USER_PROMPT_TEMPLATE = """\
Extract all job listings from this career page.

Page URL: {url}

Page content:
---
{content}
---

Return JSON only.\
"""


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


class TruncatedCompletionError(Exception):
    """LLM response was cut off at the ``max_tokens`` limit.

    Raised by :meth:`LLMFallbackParser._completion_sync` when
    ``response.choices[0].finish_reason == "length"``.
    """


class ParseResult(NamedTuple):
    """Return type for :meth:`LLMFallbackParser.parse_with_status`."""

    jobs: list[JobListing]
    parse_status: ParseStatus


# ---------------------------------------------------------------------------
# Per-source parse-status tracker (PCC-1969)
# ---------------------------------------------------------------------------


class ParseStatusTracker:
    """Rolling 24-hour per-source parse-status counter.

    Records ``(clean | salvaged | truncated | failed)`` events keyed by
    ``(source_hostname, model)`` with a sliding 24-hour window.

    Call :func:`get_parse_tracker` to access the module-level singleton.
    """

    def __init__(self) -> None:
        # key: "{netloc}|{model}" → deque of (wall_time, ParseStatus)
        self._events: dict[str, deque[tuple[float, ParseStatus]]] = defaultdict(deque)

    def record(self, source: str, model: str, status: ParseStatus) -> None:
        """Append one event and prune events older than the rolling window."""
        key = f"{source}|{model}"
        self._events[key].append((time.time(), status))
        self._purge_old(key)

    def _purge_old(self, key: str) -> None:
        cutoff = time.time() - _SALVAGE_RATE_WINDOW_S
        q = self._events[key]
        while q and q[0][0] < cutoff:
            q.popleft()

    def salvage_rate(self, source: str, model: str) -> float:
        """Fraction of events in the rolling window that are SALVAGED."""
        key = f"{source}|{model}"
        self._purge_old(key)
        q = self._events[key]
        if not q:
            return 0.0
        return sum(1 for _, s in q if s == ParseStatus.SALVAGED) / len(q)

    def status_counts(self, source: str, model: str) -> dict[str, int]:
        """Return counts per status value for the rolling window."""
        key = f"{source}|{model}"
        self._purge_old(key)
        counts: dict[str, int] = {s.value: 0 for s in ParseStatus}
        for _, s in self._events[key]:
            counts[s.value] += 1
        return counts


_tracker = ParseStatusTracker()


def get_parse_tracker() -> ParseStatusTracker:
    """Return the module-level :class:`ParseStatusTracker` singleton."""
    return _tracker


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


class LLMFallbackParser(BaseParser):
    """Extract job listings using LLM when no known ATS parser matches.

    Uses configurable LLM provider (Gemini Flash by default) to extract
    structured job listing data from raw HTML/text content.

    Requires the ``llm`` extra: ``pip install strata-harvest[llm]``
    """

    provider = ATSProvider.UNKNOWN

    def __init__(
        self,
        *,
        llm_provider: str | None = None,
        api_base: str | None = None,
        max_tokens: int | None = None,
    ) -> None:
        self._model = llm_provider or DEFAULT_MODEL
        self._api_base = api_base  # e.g. "http://192.168.50.220:8080" — caller's responsibility
        self._max_tokens = max_tokens

    def parse(self, content: str, *, url: str) -> list[JobListing]:
        """Parse raw HTML content using LLM extraction.

        Cleans the HTML, sends it to the configured LLM, and returns
        structured ``JobListing`` objects.  Returns an empty list on any error.

        This method calls :func:`litellm.completion` synchronously and blocks the
        caller until the model returns. For asyncio code (e.g.
        :meth:`~strata_harvest.crawler.Crawler.scrape`), use :meth:`parse_async`
        instead so the event loop is not blocked.
        """
        return self._extract(content, url=url).jobs

    def parse_with_status(self, content: str, *, url: str) -> ParseResult:
        """Same as :meth:`parse` but also returns the :class:`ParseStatus`.

        Returns a :class:`ParseResult` named-tuple ``(jobs, parse_status)``.
        """
        return self._extract(content, url=url)

    async def parse_async(self, content: str, *, url: str) -> list[JobListing]:
        """Same as :meth:`parse`, but runs the LLM call in a worker thread.

        Use this from async contexts so :func:`litellm.completion` does not block
        the event loop (PCC-1606).
        """
        return (await self._extract_async(content, url=url)).jobs

    async def parse_async_with_status(self, content: str, *, url: str) -> ParseResult:
        """Same as :meth:`parse_with_status`, non-blocking async variant."""
        return await self._extract_async(content, url=url)

    # ------------------------------------------------------------------
    # Top-level extraction paths
    # ------------------------------------------------------------------

    def _extract(self, content: str, *, url: str) -> ParseResult:
        if not content or not content.strip():
            return ParseResult([], ParseStatus.FAILED)

        cleaned = _clean_html(content)
        if not cleaned.strip():
            return ParseResult([], ParseStatus.FAILED)

        if litellm is None:
            logger.warning("LLM fallback requires the llm extra: pip install strata-harvest[llm]")
            return ParseResult([], ParseStatus.FAILED)

        result = self._complete_and_parse(cleaned, url)
        self._record_and_warn(url, result.parse_status)
        return result

    async def _extract_async(self, content: str, *, url: str) -> ParseResult:
        if not content or not content.strip():
            return ParseResult([], ParseStatus.FAILED)

        cleaned = _clean_html(content)
        if not cleaned.strip():
            return ParseResult([], ParseStatus.FAILED)

        if litellm is None:
            logger.warning("LLM fallback requires the llm extra: pip install strata-harvest[llm]")
            return ParseResult([], ParseStatus.FAILED)

        result = await self._complete_and_parse_async(cleaned, url)
        self._record_and_warn(url, result.parse_status)
        return result

    def _record_and_warn(self, url: str, status: ParseStatus) -> None:
        source = urlparse(url).netloc or url
        _tracker.record(source, self._model, status)
        if status == ParseStatus.SALVAGED:
            rate = _tracker.salvage_rate(source, self._model)
            if rate > _SALVAGE_RATE_THRESHOLD:
                logger.warning(
                    "High salvage rate for source %r: %.1f%% over 24h (model=%s)",
                    source,
                    rate * 100,
                    self._model,
                )

    # ------------------------------------------------------------------
    # LLM completion with truncation detection
    # ------------------------------------------------------------------

    def _completion_sync(self, cleaned: str, url: str, max_tokens: int | None = None) -> Any:
        """Synchronous litellm call (runs in thread when using :meth:`parse_async`).

        Raises :exc:`TruncatedCompletionError` when
        ``response.choices[0].finish_reason == "length"`` (LiteLLM normalizes
        Ollama's ``done_reason`` to this value).
        """
        assert litellm is not None
        kwargs: dict[str, Any] = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": USER_PROMPT_TEMPLATE.format(url=url, content=cleaned),
                },
            ],
            "temperature": 0.0,
        }
        if max_tokens is not None:
            kwargs["max_tokens"] = max_tokens
        if self._api_base:
            kwargs["api_base"] = self._api_base
            kwargs["api_key"] = "not-required"
        response = litellm.completion(**kwargs)
        if response.choices and response.choices[0].finish_reason == "length":
            raise TruncatedCompletionError(
                f"LLM response truncated (max_tokens={max_tokens}) for {url}"
            )
        return response

    def _complete_and_parse(self, cleaned: str, url: str) -> ParseResult:
        try:
            response = self._completion_sync(cleaned, url, self._max_tokens)
        except TruncatedCompletionError:
            return self._handle_truncation(cleaned, url)
        except Exception:
            logger.warning("LLM extraction failed for %s", url, exc_info=True)
            return ParseResult([], ParseStatus.FAILED)

        jobs, status = self._parse_response_with_status(response)
        return ParseResult(jobs, status)

    async def _complete_and_parse_async(self, cleaned: str, url: str) -> ParseResult:
        try:
            response = await asyncio.to_thread(
                self._completion_sync, cleaned, url, self._max_tokens
            )
        except TruncatedCompletionError:
            return await asyncio.to_thread(self._handle_truncation, cleaned, url)
        except Exception:
            logger.warning("LLM extraction failed for %s", url, exc_info=True)
            return ParseResult([], ParseStatus.FAILED)

        jobs, status = self._parse_response_with_status(response)
        return ParseResult(jobs, status)

    def _handle_truncation(self, cleaned: str, url: str) -> ParseResult:
        """Remediation order: (1) retry 2× tokens, (2) chunk + merge."""
        retry_tokens = (self._max_tokens or _DEFAULT_MAX_TOKENS) * 2
        logger.debug("Truncated for %s; retrying with max_tokens=%d", url, retry_tokens)
        try:
            response = self._completion_sync(cleaned, url, retry_tokens)
        except TruncatedCompletionError:
            logger.warning("Still truncated after retry for %s; attempting chunked parse", url)
            return self._chunked_parse(cleaned, url)
        except Exception:
            logger.warning("Retry completion failed for %s", url, exc_info=True)
            return ParseResult([], ParseStatus.FAILED)

        jobs, status = self._parse_response_with_status(response)
        return ParseResult(jobs, status)

    def _chunked_parse(self, cleaned: str, url: str) -> ParseResult:
        """Split cleaned HTML into chunks, parse each, deduplicate and merge."""
        chunks = _split_text(cleaned, chunk_size=_CHUNK_CHARS)
        if not chunks:
            return ParseResult([], ParseStatus.TRUNCATED)

        all_jobs: list[JobListing] = []
        worst_status = ParseStatus.CLEAN
        any_success = False

        for chunk in chunks:
            try:
                response = self._completion_sync(chunk, url)
            except Exception:
                logger.debug("Chunk parse failed for %s", url, exc_info=True)
                if _STATUS_SEVERITY[ParseStatus.FAILED] > _STATUS_SEVERITY[worst_status]:
                    worst_status = ParseStatus.FAILED
                continue

            chunk_jobs, chunk_status = self._parse_response_with_status(response)
            if chunk_jobs:
                any_success = True
            all_jobs.extend(chunk_jobs)
            if _STATUS_SEVERITY[chunk_status] > _STATUS_SEVERITY[worst_status]:
                worst_status = chunk_status

        if not any_success:
            return ParseResult([], ParseStatus.TRUNCATED)

        seen: set[str] = set()
        unique: list[JobListing] = []
        for job in all_jobs:
            k = str(job.url)
            if k not in seen:
                seen.add(k)
                unique.append(job)

        return ParseResult(unique, worst_status)

    # ------------------------------------------------------------------
    # Response parsing with json-repair salvage
    # ------------------------------------------------------------------

    def _parse_response_with_status(self, response: Any) -> tuple[list[JobListing], ParseStatus]:
        """Extract jobs from a litellm response; attempts json-repair on bad JSON."""
        if not response.choices:
            return [], ParseStatus.FAILED

        raw_text = response.choices[0].message.content
        if not raw_text:
            return [], ParseStatus.FAILED

        raw_text = _extract_json_from_response(raw_text)

        try:
            data = json.loads(raw_text)
            parse_status = ParseStatus.CLEAN
        except (json.JSONDecodeError, TypeError):
            if json_repair is not None:
                logger.debug("JSON parse failed; attempting json-repair salvage")
                try:
                    data = json_repair.loads(raw_text)
                    parse_status = ParseStatus.SALVAGED
                except Exception:
                    logger.debug("json-repair salvage failed")
                    return [], ParseStatus.FAILED
            else:
                logger.debug("LLM response is not valid JSON")
                return [], ParseStatus.FAILED

        if not isinstance(data, dict):
            return [], ParseStatus.FAILED

        raw_jobs = data.get("jobs")
        if not isinstance(raw_jobs, list):
            return [], ParseStatus.FAILED

        results: list[JobListing] = []
        for raw in raw_jobs:
            if not isinstance(raw, dict):
                continue
            try:
                listing = self._parse_job(raw)
                results.append(listing)
            except Exception:
                logger.debug("Skipping malformed LLM-extracted job: %s", raw.get("title", "?"))
                continue

        return results, parse_status

    def _parse_response(self, response: Any) -> list[JobListing]:
        """Thin wrapper kept for backward compatibility."""
        jobs, _ = self._parse_response_with_status(response)
        return jobs

    @staticmethod
    def _parse_job(raw: dict[str, Any]) -> JobListing:
        """Map a single LLM-extracted job dict to a JobListing."""
        title = raw.get("title")
        url = raw.get("url")

        if not title or not url:
            msg = f"LLM job missing title or url: title={title!r}"
            raise ValueError(msg)

        requirements = raw.get("requirements", [])
        if not isinstance(requirements, list):
            requirements = []

        return JobListing(
            title=title,
            url=url,
            location=raw.get("location"),
            department=raw.get("department"),
            employment_type=raw.get("employment_type"),
            description=raw.get("description"),
            requirements=[str(r) for r in requirements if r],
            salary_range=raw.get("salary_range"),
            ats_provider=ATSProvider.UNKNOWN,
            raw_data=raw,
        )


# ---------------------------------------------------------------------------
# HTML helpers
# ---------------------------------------------------------------------------


def _clean_html(html: str) -> str:
    """Strip scripts, styles, and tags while preserving link URLs and readable text."""
    text = _SCRIPT_STYLE_RE.sub("", html)

    def _replace_link(m: re.Match[str]) -> str:
        href = m.group(1)
        label = _TAG_RE.sub("", m.group(2)).strip()
        if label:
            return f"{label} ({href})"
        return href

    text = _LINK_RE.sub(_replace_link, text)

    text = _TAG_RE.sub("\n", text)

    text = _MULTI_NEWLINE_RE.sub("\n\n", text)
    text = _MULTI_SPACE_RE.sub(" ", text)

    lines = [line.strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


def _extract_json_from_response(text: str) -> str:
    """Extract JSON from LLM response that may be wrapped in markdown code fences."""
    text = text.strip()
    if text.startswith("```"):
        first_newline = text.index("\n") if "\n" in text else len(text)
        text = text[first_newline + 1 :]
        if text.endswith("```"):
            text = text[:-3]
    return text.strip()


def _split_text(text: str, *, chunk_size: int) -> list[str]:
    """Split text into line-aligned chunks of at most ``chunk_size`` characters."""
    if len(text) <= chunk_size:
        return [text]

    lines = text.splitlines()
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    for line in lines:
        line_len = len(line) + 1  # +1 for newline
        if current_len + line_len > chunk_size and current:
            chunks.append("\n".join(current))
            current = []
            current_len = 0
        current.append(line)
        current_len += line_len

    if current:
        chunks.append("\n".join(current))

    return chunks
