"""Tests for the rate limiter."""

import time

import pytest

from strata_harvest.utils.rate_limiter import RateLimiter


@pytest.mark.verification
class TestRateLimiter:
    async def test_first_request_immediate(self) -> None:
        limiter = RateLimiter(requests_per_second=10.0)
        start = time.monotonic()
        await limiter.acquire()
        elapsed = time.monotonic() - start
        assert elapsed < 0.1

    async def test_respects_rate_limit(self) -> None:
        limiter = RateLimiter(requests_per_second=10.0)
        await limiter.acquire()
        start = time.monotonic()
        await limiter.acquire()
        elapsed = time.monotonic() - start
        assert elapsed >= 0.05  # at least ~0.1s interval for 10 rps
