"""Test configuration for pytest."""
import pytest


@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081",
        "rate_per_second": 10,
        "max_messages": 100,
    }
