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


async def _read_bytes_limited(response: httpx.Response, max_bytes: int) -> bytes:
    """Read at most *max_bytes* from a streaming response body."""
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
    return b"".join(chunks)


async def _read_body_with_cap(
    response: httpx.Response, max_bytes: int
) -> tuple[bytes | None, str | None]:
    """Read full body; return (bytes, None) or (None, error) if *max_bytes* exceeded."""
    chunks: list[bytes] = []
    total = 0
    async for chunk in response.aiter_bytes():
        total += len(chunk)
        if total > max_bytes:
            return None, (
                "Response body exceeds max_response_bytes "
                f"(received at least {total} bytes, limit {max_bytes} bytes)"
            )
        chunks.append(chunk)
    return b"".join(chunks), None


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

    Response bodies are read in chunks; if the total exceeds *max_response_bytes*
    (default 10 MiB), the fetch fails with a structured error and does not
    retain the full body.
    """
    block = await _ssrf_block_reason(url, allow_private)
    if block:
        return FetchResult(url=url, error=block, elapsed_ms=0)

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
                async with client.stream(
                    method,
                    url,
                    headers=merged_headers,
                    json=json,
                    content=body,
                ) as response:
                    duration = _now_ms() - start_ms

                    if response.status_code >= 400:
                        err_bytes = await _read_bytes_limited(response, 200)
                        error_body = err_bytes.decode("utf-8", errors="replace")
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

                    body_bytes, size_error = await _read_body_with_cap(response, max_response_bytes)
                    if size_error:
                        return FetchResult(
                            url=url,
                            error=size_error,
                            elapsed_ms=duration,
                        )

                    synthetic = httpx.Response(
                        status_code=response.status_code,
                        content=body_bytes,
                        headers=response.headers,
                        request=response.request,
                    )
                    data = _parse_response_data(synthetic)
                    text = synthetic.text

                    return FetchResult(
                        url=url,
                        status_code=response.status_code,
                        content=text,
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
