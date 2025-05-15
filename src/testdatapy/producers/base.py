"""Base producer interface for Kafka message production."""
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

from testdatapy.topics import TopicManager


class KafkaProducer(ABC):
    """Abstract base class for all Kafka producers."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        config: dict[str, Any] | None = None,
        auto_create_topic: bool = True,
        topic_config: dict[str, Any] | None = None,
    ):
        """Initialize the producer.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Topic to produce to
            config: Additional producer configuration
            auto_create_topic: Whether to auto-create topic if it doesn't exist
            topic_config: Configuration for topic creation (partitions, replication_factor, etc.)
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.config = config or {}
        self._producer = None
        
        # Topic management
        self.auto_create_topic = auto_create_topic
        self.topic_config = topic_config or {}
        
        if self.auto_create_topic:
            self._ensure_topic_exists()

    def _ensure_topic_exists(self) -> None:
        """Ensure the topic exists, creating it if necessary."""
        topic_manager = TopicManager(
            bootstrap_servers=self.bootstrap_servers,
            config=self.config
        )
        
        # Extract topic creation parameters from config
        num_partitions = self.topic_config.get("num_partitions", 1)
        replication_factor = self.topic_config.get("replication_factor", 1)
        config = {k: v for k, v in self.topic_config.items() 
                 if k not in ["num_partitions", "replication_factor"]}
        
        topic_manager.ensure_topic_exists(
            self.topic,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            config=config
        )

    @abstractmethod
    def produce(
        self,
        key: str | None,
        value: dict[str, Any],
        on_delivery: Callable | None = None,
    ) -> None:
        """Produce a message to Kafka.

        Args:
            key: Message key
            value: Message value
            on_delivery: Callback for delivery reports
        """
        pass

    @abstractmethod
    def flush(self, timeout: float = 10.0) -> int:
        """Flush any pending messages.

        Args:
            timeout: Maximum time to wait for messages to be delivered

        Returns:
            Number of messages still in queue
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the producer and clean up resources."""
        pass

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
