"""Resilient HTTP client — safe_fetch() never raises."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import httpx

from strata_harvest.models import FetchResult

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT_S: float = 15
DEFAULT_RETRIES: int = 1
DEFAULT_USER_AGENT: str = "strata-harvest/0.1"


async def safe_fetch(
    url: str,
    *,
    method: str = "GET",
    json: dict[str, Any] | None = None,
    body: bytes | None = None,
    timeout: float = DEFAULT_TIMEOUT_S,
    retries: int = DEFAULT_RETRIES,
    headers: dict[str, str] | None = None,
    client: httpx.AsyncClient | None = None,
) -> FetchResult:
    """Fetch a URL with timeout, retries, and structured error return.

    Never raises — returns FetchResult with ok=False on failure.

    Supports GET (default) and POST/PUT for APIs that require request bodies
    (e.g., search endpoints that accept POST with filters).

    Pass an httpx.AsyncClient via *client* for connection pooling across a
    sweep; when omitted a short-lived client is created and closed
    automatically.
    """
    merged_headers = {"User-Agent": DEFAULT_USER_AGENT}
    if headers:
        merged_headers.update(headers)

    last_error: str | None = None
    owns_client = client is None

    if owns_client:
        client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            follow_redirects=True,
        )

    start_ms = _now_ms()
    assert client is not None  # ensured by owns_client branch above
    try:
        for attempt in range(retries + 1):
            try:
                response = await client.request(
                    method,
                    url,
                    headers=merged_headers,
                    json=json,
                    content=body,
                )
                duration = _now_ms() - start_ms

                if response.status_code >= 400:
                    error_body = response.text[:200]
                    last_error = f"HTTP {response.status_code}: {error_body}"
                    if attempt < retries:
                        await asyncio.sleep(2.0 * (attempt + 1))
                        continue
                    return FetchResult(
                        ok=False,
                        url=url,
                        status_code=response.status_code,
                        error=last_error,
                        elapsed_ms=duration,
                    )

                data = _parse_response_data(response)

                return FetchResult(
                    url=url,
                    status_code=response.status_code,
                    content=response.text,
                    content_type=response.headers.get("content-type"),
                    data=data,
                    elapsed_ms=duration,
                )

            except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                if attempt < retries:
                    await asyncio.sleep(2.0 * (attempt + 1))
                    continue

        return FetchResult(
            url=url,
            error=last_error,
            elapsed_ms=_now_ms() - start_ms,
        )
    finally:
        if owns_client:
            await client.aclose()


def _parse_response_data(response: httpx.Response) -> Any:
    """Try JSON parse; fall back to truncated raw text."""
    try:
        return response.json()
    except (ValueError, TypeError):
        return {"raw_text": response.text[:500]}


def _now_ms() -> int:
    return int(time.monotonic() * 1000)
