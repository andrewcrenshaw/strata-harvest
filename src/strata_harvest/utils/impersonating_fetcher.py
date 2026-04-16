"""Tier-2 impersonation fetcher using curl_cffi for JA3/TLS fingerprint evasion.

Presents the same ``safe_fetch()`` interface as :mod:`utils.http` but uses
curl_cffi's browser impersonation to bypass JA3/HTTP2 fingerprint-based
anti-bot defenses (Cloudflare, Akamai, DataDome, PerimeterX).

Requires the ``[stealth]`` extra::

    pip install strata-harvest[stealth]

When curl_cffi is not installed, :func:`safe_fetch` still runs the SSRF guard
(blocking private/loopback addresses) and then returns a structured
``FetchResult(ok=False)`` describing the missing dependency — it never raises.
"""

from __future__ import annotations

import asyncio
import json as _json
import logging
import time
from typing import Any

from strata_harvest.models import FetchResult
from strata_harvest.utils.http import _ssrf_block_reason

logger = logging.getLogger(__name__)

DEFAULT_IMPERSONATE: str = "chrome124"
DEFAULT_TIMEOUT_S: float = 15
DEFAULT_RETRIES: int = 1
DEFAULT_MAX_RESPONSE_BYTES: int = 10 * 1024 * 1024

try:
    from curl_cffi.requests import AsyncSession as _AsyncSession

    _CURL_CFFI_AVAILABLE = True
except ImportError:
    _CURL_CFFI_AVAILABLE = False
    _AsyncSession = None  # type: ignore[assignment,misc]


def _now_ms() -> int:
    return int(time.monotonic() * 1000)


def _parse_text_data(text: str) -> Any:
    try:
        return _json.loads(text)
    except (ValueError, TypeError):
        return {"raw_text": text[:500]}


async def safe_fetch(
    url: str,
    *,
    method: str = "GET",
    json: dict[str, Any] | None = None,
    body: bytes | None = None,
    timeout: float = DEFAULT_TIMEOUT_S,
    retries: int = DEFAULT_RETRIES,
    headers: dict[str, str] | None = None,
    impersonate: str = DEFAULT_IMPERSONATE,
    allow_private: bool = False,
    max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
) -> FetchResult:
    """Tier-2 fetch with curl_cffi browser impersonation.

    Same contract as :func:`strata_harvest.utils.http.safe_fetch` — never
    raises, always returns :class:`FetchResult`.  Uses curl_cffi to present a
    realistic TLS/JA3 fingerprint so that bot-detection stacks that fingerprint
    plain httpx connections are bypassed.

    Parameters
    ----------
    url:
        Target URL (http or https only).
    method:
        HTTP method (default ``GET``).
    json:
        Optional JSON body for POST/PUT requests.
    body:
        Optional raw bytes body for POST/PUT requests.
    timeout:
        Per-request timeout in seconds.
    retries:
        Number of retry attempts after the first failure (default ``1``).
    headers:
        Additional request headers.
    impersonate:
        curl_cffi browser profile to impersonate (default ``chrome124``).
        Other options include ``chrome131``, ``firefox135``, etc.
    allow_private:
        When False (default), URLs resolving to private/loopback/link-local
        addresses are rejected (SSRF mitigation).
    max_response_bytes:
        Maximum decompressed response body size (default 10 MiB).

    Returns
    -------
    FetchResult
        Always returned — ``ok=False`` on any failure with ``error`` set.

    Notes
    -----
    The SSRF guard runs unconditionally, even when curl_cffi is not installed,
    so private-IP URLs are always blocked regardless of availability.
    """
    # SSRF guard runs unconditionally before any HTTP activity.
    block = await _ssrf_block_reason(url, allow_private)
    if block:
        return FetchResult(url=url, error=block, elapsed_ms=0)

    if not _CURL_CFFI_AVAILABLE:
        return FetchResult(
            url=url,
            error="curl_cffi not installed — pip install strata-harvest[stealth]",
            elapsed_ms=0,
        )

    start_ms = _now_ms()
    last_error: str | None = None

    for attempt in range(retries + 1):
        try:
            async with _AsyncSession() as session:  # type: ignore[misc]
                kwargs: dict[str, Any] = {
                    "impersonate": impersonate,
                    "timeout": timeout,
                    "allow_redirects": True,
                }
                if headers:
                    kwargs["headers"] = headers
                if json is not None:
                    kwargs["json"] = json
                if body is not None:
                    kwargs["content"] = body

                response = await session.request(method, url, **kwargs)
                duration = _now_ms() - start_ms

                if response.status_code >= 400:
                    error_body = (response.content[:200] or b"").decode("utf-8", errors="replace")
                    last_error = f"HTTP {response.status_code}: {error_body}"
                    if attempt < retries:
                        await asyncio.sleep(2.0 * (attempt + 1))
                        continue
                    return FetchResult(
                        url=url,
                        status_code=response.status_code,
                        error=last_error,
                        elapsed_ms=duration,
                    )

                content_bytes: bytes = response.content
                if len(content_bytes) > max_response_bytes:
                    return FetchResult(
                        url=url,
                        error=(
                            f"Response body exceeds max_response_bytes "
                            f"(received {len(content_bytes)} bytes, "
                            f"limit {max_response_bytes} bytes)"
                        ),
                        elapsed_ms=_now_ms() - start_ms,
                    )

                encoding = response.encoding or "utf-8"
                text = content_bytes.decode(encoding, errors="replace")
                data = _parse_text_data(text)
                content_type = response.headers.get("content-type")

                return FetchResult(
                    url=url,
                    status_code=response.status_code,
                    content=text,
                    content_type=content_type,
                    data=data,
                    elapsed_ms=_now_ms() - start_ms,
                )

        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < retries:
                await asyncio.sleep(2.0 * (attempt + 1))
                continue

    return FetchResult(
        url=url,
        error=last_error,
        elapsed_ms=_now_ms() - start_ms,
    )
