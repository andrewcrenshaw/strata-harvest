"""LLM-based fallback parser for unknown ATS providers.

When no recognized ATS is detected on a career page, this parser sends the
cleaned page content to an LLM and asks it to extract structured job listings.
The LLM provider is configurable (default: Gemini Flash via litellm).

Requires the ``llm`` extra: ``pip install strata-harvest[llm]``
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

try:
    import litellm
except ImportError:  # pragma: no cover - base install without ``[llm]`` extra
    litellm = None  # type: ignore[assignment]

from strata_harvest.models import ATSProvider, JobListing
from strata_harvest.parsers.base import BaseParser

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "gemini/gemini-2.0-flash"

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


class LLMFallbackParser(BaseParser):
    """Extract job listings using LLM when no known ATS parser matches.

    Uses configurable LLM provider (Gemini Flash by default) to extract
    structured job listing data from raw HTML/text content.

    Requires the ``llm`` extra: ``pip install strata-harvest[llm]``
    """

    provider = ATSProvider.UNKNOWN

    def __init__(self, *, llm_provider: str | None = None) -> None:
        self._model = llm_provider or DEFAULT_MODEL

    def parse(self, content: str, *, url: str) -> list[JobListing]:
        """Parse raw HTML content using LLM extraction.

        Cleans the HTML, sends it to the configured LLM, and returns
        structured ``JobListing`` objects.  Returns an empty list on any error.
        """
        if not content or not content.strip():
            return []

        cleaned = _clean_html(content)
        if not cleaned.strip():
            return []

        if litellm is None:
            logger.warning("LLM fallback requires the llm extra: pip install strata-harvest[llm]")
            return []

        try:
            response = litellm.completion(
                model=self._model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": USER_PROMPT_TEMPLATE.format(url=url, content=cleaned),
                    },
                ],
                temperature=0.0,
            )
        except Exception:
            logger.warning("LLM extraction failed for %s", url, exc_info=True)
            return []

        return self._parse_response(response)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_response(self, response: Any) -> list[JobListing]:
        """Parse the LLM response into JobListing objects."""
        if not response.choices:
            return []

        raw_text = response.choices[0].message.content
        if not raw_text:
            return []

        raw_text = _extract_json_from_response(raw_text)

        try:
            data = json.loads(raw_text)
        except (json.JSONDecodeError, TypeError):
            logger.debug("LLM response is not valid JSON")
            return []

        if not isinstance(data, dict):
            return []

        raw_jobs = data.get("jobs")
        if not isinstance(raw_jobs, list):
            return []

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

        return results

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
