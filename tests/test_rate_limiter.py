"""Tests for the rate limiter (PCC-1428 expansion)."""

import asyncio
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
        assert elapsed >= 0.05

    async def test_interval_computed_correctly(self) -> None:
        """1 req/s means 1.0s interval; 0.5 req/s means 2.0s interval."""
        limiter_1 = RateLimiter(requests_per_second=1.0)
        assert limiter_1._interval == pytest.approx(1.0)

        limiter_05 = RateLimiter(requests_per_second=0.5)
        assert limiter_05._interval == pytest.approx(2.0)

    async def test_very_high_rate_allows_rapid_requests(self) -> None:
        limiter = RateLimiter(requests_per_second=1000.0)
        start = time.monotonic()
        for _ in range(5):
            await limiter.acquire()
        elapsed = time.monotonic() - start
        assert elapsed < 0.5

    async def test_near_zero_rate_clamps(self) -> None:
        """Very small rate shouldn't cause division by zero — clamps to 0.01 min."""
        limiter = RateLimiter(requests_per_second=0.001)
        assert limiter._interval == pytest.approx(1.0 / 0.01)

    async def test_zero_rate_clamps(self) -> None:
        """Zero requests_per_second clamps to minimum."""
        limiter = RateLimiter(requests_per_second=0.0)
        assert limiter._interval == pytest.approx(1.0 / 0.01)

    async def test_negative_rate_clamps(self) -> None:
        """Negative rate clamps to minimum."""
        limiter = RateLimiter(requests_per_second=-5.0)
        assert limiter._interval == pytest.approx(1.0 / 0.01)

    async def test_sequential_acquires_are_serialized(self) -> None:
        """Multiple acquire() calls wait in sequence via the internal lock."""
        limiter = RateLimiter(requests_per_second=10.0)
        timestamps: list[float] = []

        async def record_acquire() -> None:
            await limiter.acquire()
            timestamps.append(time.monotonic())

        tasks = [asyncio.create_task(record_acquire()) for _ in range(3)]
        await asyncio.gather(*tasks)

        assert len(timestamps) == 3
        for i in range(1, len(timestamps)):
            gap = timestamps[i] - timestamps[i - 1]
            assert gap >= 0.05

    async def test_default_rate_is_one_per_second(self) -> None:
        limiter = RateLimiter()
        assert limiter._interval == pytest.approx(1.0)
