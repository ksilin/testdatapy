"""Unit tests for rate limiter."""
import time

from testdatapy.generators.rate_limiter import RateLimiter, TokenBucket


class TestTokenBucket:
    """Test the TokenBucket class."""

    def test_initialization(self):
        """Test bucket initialization."""
        bucket = TokenBucket(rate=10.0, capacity=20)
        assert bucket.rate == 10.0
        assert bucket.capacity == 20
        assert bucket.tokens == 20.0

    def test_consume_tokens(self):
        """Test consuming tokens."""
        bucket = TokenBucket(rate=10.0, capacity=10)

        # Should be able to consume available tokens
        assert bucket.consume(5) is True
        assert abs(bucket.tokens - 5.0) < 0.1  # Allow small floating-point differences

        # Should not be able to consume more than available
        assert bucket.consume(6) is False
        assert abs(bucket.tokens - 5.0) < 0.1  # Allow small floating-point differences

    def test_token_replenishment(self):
        """Test that tokens are replenished over time."""
        bucket = TokenBucket(rate=10.0, capacity=10)

        # Consume all tokens
        assert bucket.consume(10) is True
        assert bucket.tokens == 0.0

        # Wait for tokens to replenish
        time.sleep(0.5)  # Should add 5 tokens
        assert bucket.consume(4) is True  # Should work
        assert bucket.consume(2) is False  # Should fail

    def test_capacity_limit(self):
        """Test that tokens don't exceed capacity."""
        bucket = TokenBucket(rate=10.0, capacity=5)

        # Start with full capacity
        assert bucket.tokens == 5.0

        # Wait for more tokens
        time.sleep(1.0)  # Would add 10 tokens, but capped at 5

        # Should still be at capacity
        assert bucket.consume(6) is False
        assert bucket.consume(5) is True

    def test_wait_for_tokens(self):
        """Test waiting for tokens."""
        bucket = TokenBucket(rate=10.0, capacity=10)

        # Consume all tokens
        bucket.consume(10)

        # Wait for tokens with timeout
        start_time = time.time()
        result = bucket.wait_for_tokens(5, timeout=0.6)
        elapsed = time.time() - start_time

        assert result is True
        assert 0.5 <= elapsed <= 0.7  # Should take about 0.5 seconds

    def test_wait_for_tokens_timeout(self):
        """Test timeout when waiting for tokens."""
        bucket = TokenBucket(rate=1.0, capacity=1)

        # Consume all tokens
        bucket.consume(1)

        # Try to wait for too many tokens
        result = bucket.wait_for_tokens(5, timeout=0.5)
        assert result is False


class TestRateLimiter:
    """Test the RateLimiter class."""

    def test_initialization(self):
        """Test rate limiter initialization."""
        limiter = RateLimiter(rate_per_second=5.0)
        assert limiter.rate_per_second == 5.0
        assert limiter.interval == 0.2

    def test_rate_limiting(self):
        """Test that rate limiter maintains target rate."""
        limiter = RateLimiter(rate_per_second=10.0)

        start_time = time.time()
        for _ in range(5):
            limiter.wait()
        elapsed = time.time() - start_time

        # Should take approximately 0.4 seconds (4 intervals)
        assert 0.35 <= elapsed <= 0.45

    def test_zero_rate(self):
        """Test rate limiter with zero rate (no limiting)."""
        limiter = RateLimiter(rate_per_second=0)

        start_time = time.time()
        for _ in range(100):
            limiter.wait()
        elapsed = time.time() - start_time

        # Should complete almost instantly
        assert elapsed < 0.1
