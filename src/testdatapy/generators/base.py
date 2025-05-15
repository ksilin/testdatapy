"""Base generator interface for test data generation."""
from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any


class DataGenerator(ABC):
    """Abstract base class for all data generators."""

    def __init__(self, rate_per_second: float = 10.0, max_messages: int | None = None):
        """Initialize the generator.

        Args:
            rate_per_second: Number of messages to generate per second
            max_messages: Maximum number of messages to generate (None for unlimited)
        """
        self.rate_per_second = rate_per_second
        self.max_messages = max_messages
        self._message_count = 0

    @abstractmethod
    def generate(self) -> Iterator[dict[str, Any]]:
        """Generate data records.

        Yields:
            Dict containing generated data
        """
        pass

    def should_continue(self) -> bool:
        """Check if generation should continue based on max_messages."""
        if self.max_messages is None:
            return True
        return self._message_count < self.max_messages

    def increment_count(self) -> None:
        """Increment the message counter."""
        self._message_count += 1

    @property
    def message_count(self) -> int:
        """Get the current message count."""
        return self._message_count
