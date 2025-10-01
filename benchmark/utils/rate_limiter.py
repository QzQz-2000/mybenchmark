"""Rate limiting utilities similar to Guava's RateLimiter.

Provides smooth and bursty rate limiting for benchmark workload generation.
"""

import time
import asyncio
from typing import Optional
from enum import Enum


class RateLimiterType(Enum):
    """Rate limiter types."""
    SMOOTH = "smooth"  # Smooth rate limiting (default)
    BURSTY = "bursty"  # Allow bursts up to 1 second worth of permits


class AsyncRateLimiter:
    """Async rate limiter similar to Guava's RateLimiter.

    This implementation supports both smooth and bursty rate limiting,
    which is important for accurately simulating different workload patterns.
    """

    def __init__(
        self,
        permits_per_second: float,
        limiter_type: RateLimiterType = RateLimiterType.SMOOTH,
        max_burst_seconds: float = 1.0
    ):
        """Initialize rate limiter.

        Args:
            permits_per_second: Target rate in permits per second
            limiter_type: Type of rate limiting (smooth or bursty)
            max_burst_seconds: Maximum burst duration in seconds (for bursty type)
        """
        if permits_per_second <= 0:
            raise ValueError("permits_per_second must be positive")

        self.permits_per_second = permits_per_second
        self.limiter_type = limiter_type
        self.max_burst_seconds = max_burst_seconds

        # Internal state
        self._interval = 1.0 / permits_per_second  # Time between permits
        self._next_free_ticket_micros = self._now_micros()
        self._lock = asyncio.Lock()

        # For bursty rate limiting
        if limiter_type == RateLimiterType.BURSTY:
            self._max_permits = int(permits_per_second * max_burst_seconds)
            self._stored_permits = self._max_permits
        else:
            self._max_permits = 0
            self._stored_permits = 0

    def _now_micros(self) -> int:
        """Get current time in microseconds."""
        return int(time.time() * 1_000_000)

    async def acquire(self, permits: int = 1) -> float:
        """Acquire permits and wait if necessary.

        Args:
            permits: Number of permits to acquire (default 1)

        Returns:
            Time waited in seconds
        """
        if permits <= 0:
            raise ValueError("permits must be positive")

        async with self._lock:
            wait_micros = self._reserve_and_get_wait_length(permits)

        if wait_micros > 0:
            await asyncio.sleep(wait_micros / 1_000_000)
            return wait_micros / 1_000_000
        return 0.0

    async def try_acquire(self, permits: int = 1, timeout_seconds: float = 0) -> bool:
        """Try to acquire permits within timeout.

        Args:
            permits: Number of permits to acquire
            timeout_seconds: Maximum time to wait in seconds

        Returns:
            True if permits were acquired, False otherwise
        """
        if permits <= 0:
            raise ValueError("permits must be positive")

        async with self._lock:
            now_micros = self._now_micros()
            timeout_micros = int(timeout_seconds * 1_000_000)

            if self._next_free_ticket_micros > now_micros + timeout_micros:
                return False

            wait_micros = self._reserve_and_get_wait_length(permits)

        if wait_micros > timeout_micros:
            return False

        if wait_micros > 0:
            await asyncio.sleep(wait_micros / 1_000_000)

        return True

    def _reserve_and_get_wait_length(self, permits: int) -> int:
        """Reserve permits and return wait time in microseconds.

        This method must be called with the lock held.
        """
        now_micros = self._now_micros()

        if self.limiter_type == RateLimiterType.BURSTY:
            return self._reserve_and_get_wait_length_bursty(permits, now_micros)
        else:
            return self._reserve_and_get_wait_length_smooth(permits, now_micros)

    def _reserve_and_get_wait_length_smooth(self, permits: int, now_micros: int) -> int:
        """Reserve permits for smooth rate limiting."""
        # Wait until next free ticket
        wait_micros = max(0, self._next_free_ticket_micros - now_micros)

        # Schedule next free ticket
        interval_micros = int(self._interval * 1_000_000 * permits)
        self._next_free_ticket_micros = max(now_micros, self._next_free_ticket_micros) + interval_micros

        return wait_micros

    def _reserve_and_get_wait_length_bursty(self, permits: int, now_micros: int) -> int:
        """Reserve permits for bursty rate limiting.

        Allows burst consumption of stored permits with no wait.
        """
        # Resync stored permits based on elapsed time
        if now_micros > self._next_free_ticket_micros:
            elapsed_micros = now_micros - self._next_free_ticket_micros
            new_permits = elapsed_micros / 1_000_000 * self.permits_per_second
            self._stored_permits = min(self._max_permits, self._stored_permits + new_permits)
            self._next_free_ticket_micros = now_micros

        # Use stored permits first
        permits_from_stored = min(permits, int(self._stored_permits))
        permits_to_wait = permits - permits_from_stored
        self._stored_permits -= permits_from_stored

        # Calculate wait time for remaining permits
        wait_micros = 0
        if permits_to_wait > 0:
            wait_micros = int(permits_to_wait * self._interval * 1_000_000)
            self._next_free_ticket_micros += wait_micros

        return wait_micros

    async def set_rate(self, new_permits_per_second: float) -> None:
        """Update the rate dynamically.

        Args:
            new_permits_per_second: New target rate
        """
        if new_permits_per_second <= 0:
            raise ValueError("permits_per_second must be positive")

        async with self._lock:
            self.permits_per_second = new_permits_per_second
            self._interval = 1.0 / new_permits_per_second

            if self.limiter_type == RateLimiterType.BURSTY:
                old_max_permits = self._max_permits
                self._max_permits = int(new_permits_per_second * self.max_burst_seconds)

                # Adjust stored permits proportionally
                if old_max_permits > 0:
                    ratio = self._stored_permits / old_max_permits
                    self._stored_permits = ratio * self._max_permits

    def get_rate(self) -> float:
        """Get current rate in permits per second."""
        return self.permits_per_second


class NoOpRateLimiter:
    """No-op rate limiter for unlimited rate."""

    async def acquire(self, permits: int = 1) -> float:
        """Acquire without waiting."""
        return 0.0

    async def try_acquire(self, permits: int = 1, timeout_seconds: float = 0) -> bool:
        """Always succeeds immediately."""
        return True

    async def set_rate(self, new_permits_per_second: float) -> None:
        """No-op."""
        pass

    def get_rate(self) -> float:
        """Return infinity for unlimited rate."""
        return float('inf')


def create_rate_limiter(
    permits_per_second: Optional[float],
    limiter_type: RateLimiterType = RateLimiterType.SMOOTH
) -> AsyncRateLimiter:
    """Factory function to create appropriate rate limiter.

    Args:
        permits_per_second: Target rate, None or 0 for unlimited
        limiter_type: Type of rate limiting

    Returns:
        Rate limiter instance
    """
    if permits_per_second is None or permits_per_second <= 0:
        return NoOpRateLimiter()

    return AsyncRateLimiter(permits_per_second, limiter_type)