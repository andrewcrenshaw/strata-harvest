"""Resilient HTTP client — safe_fetch() never raises."""

from __future__ import annotations

import time

import httpx

from strata_harvest.models import FetchResult

_DEFAULT_USER_AGENT = "strata-harvest/0.1.0"
_DEFAULT_TIMEOUT = 30.0
_MAX_RETRIES = 3
_BACKOFF_FACTOR = 1.5


async def safe_fetch(
    url: str,
    *,
    method: str = "GET",
    timeout: float = _DEFAULT_TIMEOUT,
    user_agent: str | None = None,
    max_retries: int = _MAX_RETRIES,
) -> FetchResult:
    """Fetch a URL with retries and exponential backoff. Never raises.

    Returns a FetchResult with status, content, and error info.
    On transient failures, retries up to max_retries times.
    """
    headers = {"User-Agent": user_agent or _DEFAULT_USER_AGENT}
    last_error: str | None = None
    start = time.monotonic()

    for attempt in range(max_retries + 1):
        try:
            async with httpx.AsyncClient(
                timeout=timeout,
                follow_redirects=True,
            ) as client:
                response = await client.request(method, url, headers=headers)
                elapsed = (time.monotonic() - start) * 1000
                return FetchResult(
                    url=url,
                    status_code=response.status_code,
                    content=response.text,
                    content_type=response.headers.get("content-type"),
                    elapsed_ms=elapsed,
                )
        except httpx.TimeoutException:
            last_error = f"Timeout after {timeout}s (attempt {attempt + 1}/{max_retries + 1})"
        except httpx.HTTPError as exc:
            last_error = f"{type(exc).__name__}: {exc}"

        if attempt < max_retries:
            import asyncio

            await asyncio.sleep(_BACKOFF_FACTOR**attempt)

    elapsed = (time.monotonic() - start) * 1000
    return FetchResult(url=url, error=last_error, elapsed_ms=elapsed)
