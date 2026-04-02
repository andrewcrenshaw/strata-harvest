"""Token-bucket rate limiter for HTTP request throttling."""

from __future__ import annotations

import asyncio
import time

_DEFAULT_PER_DOMAIN_IDLE_TTL: float = 3600.0


class PerDomainRateLimiterRegistry:
    """Async per-hostname rate limiters with idle TTL eviction.

    Each hostname gets an independent :class:`RateLimiter`. Entries that have
    not completed a request within *idle_ttl_seconds* are removed to cap memory.
    """

    def __init__(
        self,
        requests_per_second: float = 0.5,
        idle_ttl_seconds: float = _DEFAULT_PER_DOMAIN_IDLE_TTL,
    ) -> None:
        self._requests_per_second = requests_per_second
        self._idle_ttl_seconds = idle_ttl_seconds
        self._by_host: dict[str, RateLimiter] = {}
        self._last_completed: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def acquire(self, hostname: str) -> None:
        """Wait for per-domain slot, then record activity and evict stale hosts."""
        async with self._lock:
            if hostname not in self._by_host:
                self._by_host[hostname] = RateLimiter(requests_per_second=self._requests_per_second)
            limiter = self._by_host[hostname]
        await limiter.acquire()
        now = time.monotonic()
        async with self._lock:
            self._last_completed[hostname] = now
            self._evict_stale_unlocked(now)

    def _evict_stale_unlocked(self, now: float) -> None:
        stale = [h for h, t in self._last_completed.items() if now - t > self._idle_ttl_seconds]
        for h in stale:
            self._by_host.pop(h, None)
            self._last_completed.pop(h, None)


class RateLimiter:
    """Async token-bucket rate limiter.

    Enforces a maximum number of requests per second to avoid
    overwhelming career page servers.
    """

    def __init__(self, requests_per_second: float = 1.0) -> None:
        self._interval = 1.0 / max(requests_per_second, 0.01)
        self._last_request: float = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until the next request is allowed."""
        async with self._lock:
            now = time.monotonic()
            wait = self._interval - (now - self._last_request)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request = time.monotonic()
