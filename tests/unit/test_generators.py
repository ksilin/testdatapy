"""Unit tests for generators."""
from testdatapy.generators.base import DataGenerator


class MockGenerator(DataGenerator):
    """Mock generator for testing."""

    def generate(self):
        """Generate mock data."""
        while self.should_continue():
            self.increment_count()
            yield {"id": self.message_count, "value": f"test_{self.message_count}"}


class TestDataGenerator:
    """Test the DataGenerator base class."""

    def test_generator_initialization(self):
        """Test generator initialization."""
        gen = MockGenerator(rate_per_second=5.0, max_messages=10)
        assert gen.rate_per_second == 5.0
        assert gen.max_messages == 10
        assert gen.message_count == 0

    def test_should_continue_with_limit(self):
        """Test should_continue with message limit."""
        gen = MockGenerator(max_messages=2)
        assert gen.should_continue() is True

        gen.increment_count()
        assert gen.should_continue() is True

        gen.increment_count()
        assert gen.should_continue() is False

    def test_should_continue_without_limit(self):
        """Test should_continue without message limit."""
        gen = MockGenerator(max_messages=None)
        for _ in range(1000):
            assert gen.should_continue() is True
            gen.increment_count()

    def test_generate_with_limit(self):
        """Test generation with message limit."""
        gen = MockGenerator(max_messages=3)
        messages = list(gen.generate())

        assert len(messages) == 3
        assert messages[0] == {"id": 1, "value": "test_1"}
        assert messages[2] == {"id": 3, "value": "test_3"}
