"""Token-bucket rate limiter for HTTP request throttling."""

from __future__ import annotations

import asyncio
import time


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
