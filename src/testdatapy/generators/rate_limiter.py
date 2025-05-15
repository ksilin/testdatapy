"""Rate limiting utilities for controlled data generation."""
import time
from threading import Lock


class TokenBucket:
    """Token bucket implementation for rate limiting."""

    def __init__(self, rate: float, capacity: int | None = None):
        """Initialize the token bucket.

        Args:
            rate: Tokens per second to add to the bucket
            capacity: Maximum capacity of the bucket (defaults to rate)
        """
        self.rate = rate
        self.capacity = capacity or int(rate)
        self.tokens = float(self.capacity)
        self.last_update = time.time()
        self.lock = Lock()

    def consume(self, tokens: float = 1.0) -> bool:
        """Try to consume tokens from the bucket.

        Args:
            tokens: Number of tokens to consume

        Returns:
            True if tokens were consumed, False otherwise
        """
        with self.lock:
            now = time.time()
            elapsed = now - self.last_update

            # Add new tokens based on elapsed time
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_update = now

            # Try to consume tokens
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    def wait_for_tokens(self, tokens: float = 1.0, timeout: float | None = None) -> bool:
        """Wait until tokens are available and consume them.

        Args:
            tokens: Number of tokens to consume
            timeout: Maximum time to wait (None for unlimited)

        Returns:
            True if tokens were consumed, False if timed out
        """
        start_time = time.time()

        while True:
            if self.consume(tokens):
                return True

            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    return False

            # Calculate wait time
            with self.lock:
                tokens_needed = tokens - self.tokens
                wait_time = tokens_needed / self.rate if self.rate > 0 else 0
                wait_time = min(wait_time, 0.1)  # Cap at 100ms to check timeout

            time.sleep(wait_time)


class RateLimiter:
    """Simple rate limiter for controlling message production."""

    def __init__(self, rate_per_second: float):
        """Initialize the rate limiter.

        Args:
            rate_per_second: Target rate of messages per second
        """
        self.rate_per_second = rate_per_second
        self.interval = 1.0 / rate_per_second if rate_per_second > 0 else 0
        self.last_time = None

    def wait(self) -> None:
        """Wait if necessary to maintain the target rate."""
        if self.interval <= 0:
            return

        current_time = time.time()

        if self.last_time is not None:
            elapsed = current_time - self.last_time
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)

        self.last_time = time.time()
