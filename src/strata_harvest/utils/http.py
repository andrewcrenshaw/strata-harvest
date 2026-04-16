"""Resilient HTTP client — safe_fetch() never raises."""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import socket
import time
from typing import Any
from urllib.parse import urlparse

import httpx

from strata_harvest.models import FetchResult

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT_S: float = 15
DEFAULT_RETRIES: int = 1
DEFAULT_USER_AGENT: str = "strata-harvest/0.1"
DEFAULT_MAX_RESPONSE_BYTES: int = 10 * 1024 * 1024


def _is_blocked_ip(addr: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """True for RFC 1918 private, loopback, and link-local (IPv4 / IPv6)."""
    return bool(addr.is_private or addr.is_loopback or addr.is_link_local)


async def _ssrf_block_reason(url: str, allow_private: bool) -> str | None:
    """Return an error message if *url* must not be fetched (SSRF guard), else None."""
    if allow_private:
        return None
    try:
        parsed = urlparse(url)
    except Exception:
        return "SSRF: invalid URL"
    if parsed.scheme not in ("http", "https"):
        return f"SSRF: only http and https URLs are allowed, not {parsed.scheme!r}"
    host = parsed.hostname
    if not host:
        return "SSRF: missing host in URL"
    if host.lower() == "localhost":
        return "SSRF: blocked host (localhost)"
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        addr = None
    if addr is not None:
        if _is_blocked_ip(addr):
            return f"SSRF: blocked destination {addr}"
        return None
    port = parsed.port
    if port is None:
        port = 443 if parsed.scheme == "https" else 80
    try:
        infos = await asyncio.to_thread(
            socket.getaddrinfo,
            host,
            port,
            socket.AF_UNSPEC,
            socket.SOCK_STREAM,
        )
    except socket.gaierror as exc:
        return f"SSRF: DNS resolution failed: {exc}"
    for _fam, _socktype, _proto, _canon, sockaddr in infos:
        ip_str = sockaddr[0]
        try:
            resolved = ipaddress.ip_address(ip_str)
        except ValueError:
            continue
        if _is_blocked_ip(resolved):
            return f"SSRF: host {host!r} resolves to blocked address {resolved}"
    return None


async def _read_error_body_limited(response: httpx.Response, max_bytes: int) -> str:
    """Read at most *max_bytes* from a streaming response body for error reporting."""
    chunks: list[bytes] = []
    total = 0
    async for chunk in response.aiter_bytes():
        if total >= max_bytes:
            break
        take = min(len(chunk), max_bytes - total)
        if take:
            chunks.append(chunk[:take])
            total += take
        if total >= max_bytes:
            break
    return b"".join(chunks).decode("utf-8", errors="replace")


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
    allow_private: bool = False,
    max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
    if_none_match: str | None = None,
    if_modified_since: str | None = None,
    cached_content: str | None = None,
) -> FetchResult:
    """Fetch a URL with timeout, retries, and structured error return.

    Never raises — returns FetchResult with ok=False on failure.

    Supports GET (default) and POST/PUT for APIs that require request bodies
    (e.g., search endpoints that accept POST with filters).

    Pass an httpx.AsyncClient via *client* for connection pooling across a
    sweep; when omitted a short-lived client is created and closed
    automatically.

    By default, URLs whose host resolves to private, loopback, or link-local
    addresses are rejected (SSRF mitigation). Set *allow_private* to True
    only for controlled tests or trusted environments.

    Response bodies are fully read via ``aread()`` so that httpx handles
    content-encoding (gzip, brotli, zstd) transparently. If the decoded body
    exceeds *max_response_bytes* (default 10 MiB), the fetch fails with a
    structured error rather than retaining the oversized payload.

    Supports conditional requests:
    - *if_none_match*: ETag value to use in If-None-Match header
    - *if_modified_since*: Timestamp to use in If-Modified-Since header
    - *cached_content*: When both headers are provided and server returns 304,
      this content is returned in FetchResult (with 304 status code)
    """
    block = await _ssrf_block_reason(url, allow_private)
    if block:
        return FetchResult(url=url, error=block, elapsed_ms=0)

    merged_headers = {"User-Agent": DEFAULT_USER_AGENT}
    if headers:
        merged_headers.update(headers)

    # Add conditional request headers if provided
    if if_none_match:
        merged_headers["If-None-Match"] = if_none_match
    if if_modified_since:
        merged_headers["If-Modified-Since"] = if_modified_since

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
                # Use client.stream() so we can enforce the size cap before
                # buffering the full body.  Inside the context, response.aread()
                # triggers httpx's built-in content-encoding decompression
                # (gzip, deflate, brotli, zstd) — unlike aiter_bytes() which
                # yields raw compressed wire bytes and requires manual decoding.
                async with client.stream(
                    method,
                    url,
                    headers=merged_headers,
                    json=json,
                    content=body,
                ) as response:
                    duration = _now_ms() - start_ms

                    # Handle 304 Not Modified: return cached content with headers
                    if response.status_code == 304:
                        return FetchResult(
                            url=url,
                            status_code=304,
                            content=cached_content,
                            content_type=response.headers.get("content-type"),
                            etag=response.headers.get("etag"),
                            last_modified=response.headers.get("last-modified"),
                            elapsed_ms=duration,
                        )

                    if response.status_code >= 400:
                        error_body = await _read_error_body_limited(response, 200)
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

                    # Check Content-Length header first for a cheap size guard.
                    content_length_str = response.headers.get("content-length")
                    if content_length_str is not None:
                        try:
                            declared_length = int(content_length_str)
                            if declared_length > max_response_bytes:
                                return FetchResult(
                                    url=url,
                                    error=(
                                        "Response body exceeds max_response_bytes "
                                        f"(Content-Length: {declared_length} bytes, "
                                        f"limit {max_response_bytes} bytes)"
                                    ),
                                    elapsed_ms=duration,
                                )
                        except ValueError:
                            pass

                    # aread() decompresses the body (gzip/brotli/zstd/deflate)
                    # transparently.  This is the correct approach versus
                    # aiter_bytes(), which yields raw compressed bytes.
                    content_bytes = await response.aread()
                    if len(content_bytes) > max_response_bytes:
                        return FetchResult(
                            url=url,
                            error=(
                                "Response body exceeds max_response_bytes "
                                f"(received {len(content_bytes)} bytes, "
                                f"limit {max_response_bytes} bytes)"
                            ),
                            elapsed_ms=duration,
                        )

                    text = content_bytes.decode(response.encoding or "utf-8", errors="replace")
                    data = _parse_text_data(text)

                    return FetchResult(
                        url=url,
                        status_code=response.status_code,
                        content=text,
                        content_type=response.headers.get("content-type"),
                        data=data,
                        etag=response.headers.get("etag"),
                        last_modified=response.headers.get("last-modified"),
                        elapsed_ms=duration,
                    )

            except (
                httpx.TimeoutException,
                httpx.ConnectError,
                httpx.ReadError,
                httpx.DecodingError,  # brotli/gzip/zstd decode failures
            ) as exc:
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


def _parse_text_data(text: str) -> Any:
    """Try JSON parse on decoded text; fall back to truncated raw text."""
    import json as _json

    try:
        return _json.loads(text)
    except (ValueError, TypeError):
        return {"raw_text": text[:500]}


def _now_ms() -> int:
    return int(time.monotonic() * 1000)
