"""JSON producer implementation for Kafka."""
import json
from collections.abc import Callable
from typing import Any

from confluent_kafka import Producer as ConfluentProducer
from confluent_kafka.serialization import StringSerializer

from testdatapy.producers.base import KafkaProducer


class JsonProducer(KafkaProducer):
    """Kafka producer for JSON messages."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        config: dict[str, Any] | None = None,
        key_field: str | None = None,
        auto_create_topic: bool = True,
        topic_config: dict[str, Any] | None = None,
    ):
        """Initialize the JSON producer.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Topic to produce to
            config: Additional producer configuration
            key_field: Field to use as message key
            auto_create_topic: Whether to auto-create topic if it doesn't exist
            topic_config: Configuration for topic creation
        """
        super().__init__(bootstrap_servers, topic, config, auto_create_topic, topic_config)
        self.key_field = key_field

        # Set up serializers
        self._key_serializer = StringSerializer("utf-8")

        # Merge configurations
        producer_config = {"bootstrap.servers": bootstrap_servers}
        if config:
            producer_config.update(config)

        self._producer = ConfluentProducer(producer_config)

    def produce(
        self,
        key: str | None,
        value: dict[str, Any],
        on_delivery: Callable | None = None,
    ) -> None:
        """Produce a JSON message to Kafka.

        Args:
            key: Message key (if None, will try to extract from value using key_field)
            value: Message value as dictionary
            on_delivery: Callback for delivery reports
        """
        # Extract key from value if not provided
        if key is None and self.key_field and self.key_field in value:
            key = str(value[self.key_field])

        # Serialize key
        serialized_key = self._key_serializer(key) if key else None

        # Serialize value to JSON
        serialized_value = json.dumps(value).encode("utf-8")

        # Produce message
        self._producer.produce(
            topic=self.topic,
            key=serialized_key,
            value=serialized_value,
            on_delivery=on_delivery or self._default_callback,
        )

        # Poll for callbacks
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        """Flush any pending messages.

        Args:
            timeout: Maximum time to wait for messages to be delivered

        Returns:
            Number of messages still in queue
        """
        if self._producer is not None:
          return self._producer.flush(timeout)
        else:
        # Producer already closed/cleaned up
          return 0

    def close(self) -> None:
        """Close the producer and clean up resources."""
        if self._producer:
            self._producer.flush()
            # Note: ConfluentProducer doesn't have a close method
            self._producer = None

    def _default_callback(self, err, msg):
        """Default delivery callback.

        Args:
            err: Error if delivery failed
            msg: Message that was delivered
        """
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(
                f"Message delivered to {msg.topic()} "
                f"[partition {msg.partition()}] at offset {msg.offset()}"
            )

    def poll(self, timeout: float = 0) -> int:
        """Poll for events.

        Args:
            timeout: Maximum time to wait

        Returns:
            Number of events processed
        """
        return self._producer.poll(timeout)

    @property
    def queue_size(self) -> int:
        """Get the current queue size.

        Returns:
            Number of messages in the queue
        """
        return len(self._producer)
