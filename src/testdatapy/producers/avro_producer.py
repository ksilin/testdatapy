"""Avro producer implementation for Kafka."""
import json
from collections.abc import Callable
from typing import Any

from confluent_kafka import Producer as ConfluentProducer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

from testdatapy.producers.base import KafkaProducer


class AvroProducer(KafkaProducer):
    """Kafka producer for Avro messages."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        schema_registry_url: str,
        schema_path: str | None = None,
        schema_str: str | None = None,
        config: dict[str, Any] | None = None,
        schema_registry_config: dict[str, Any] | None = None,
        key_field: str | None = None,
        auto_create_topic: bool = True,
        topic_config: dict[str, Any] | None = None,
    ):
        """Initialize the Avro producer.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Topic to produce to
            schema_registry_url: Schema Registry URL
            schema_path: Path to Avro schema file
            schema_str: Avro schema as string
            config: Additional producer configuration
            schema_registry_config: Schema Registry configuration
            key_field: Field to use as message key
            auto_create_topic: Whether to auto-create topic if it doesn't exist
            topic_config: Configuration for topic creation
        """
        super().__init__(bootstrap_servers, topic, config, auto_create_topic, topic_config)
        self.key_field = key_field

        # Load schema
        if schema_path:
            with open(schema_path) as f:
                self.schema_str = f.read()
        elif schema_str:
            self.schema_str = schema_str
        else:
            raise ValueError("Either schema_path or schema_str must be provided")

        # Set up Schema Registry client
        sr_config = {"url": schema_registry_url}
        if schema_registry_config:
            sr_config.update(schema_registry_config)
        self._schema_registry_client = SchemaRegistryClient(sr_config)

        # Set up serializers
        self._key_serializer = StringSerializer("utf-8")
        self._value_serializer = AvroSerializer(
            schema_str=self.schema_str,
            schema_registry_client=self._schema_registry_client,
        )

        # Set up producer
        producer_config = {"bootstrap.servers": bootstrap_servers}
        if config:
            producer_config.update(config)
        # Debug: Log configuration
        print(f"Producer config: {producer_config}")
        self._producer = ConfluentProducer(producer_config)

    def produce(
        self,
        key: str | None,
        value: dict[str, Any],
        on_delivery: Callable | None = None,
    ) -> None:
        """Produce an Avro message to Kafka.

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

        # Create serialization context
        ctx = SerializationContext(self.topic, MessageField.VALUE)

        # Serialize value with Avro
        serialized_value = self._value_serializer(value, ctx)

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

    def get_latest_schema(self) -> dict[str, Any]:
        """Get the latest schema from the Schema Registry.

        Returns:
            Latest schema as dictionary
        """
        subject = f"{self.topic}-value"
        schema = self._schema_registry_client.get_latest_version(subject)
        return json.loads(schema.schema.schema_str)

    def register_schema(self) -> int:
        """Register the schema with the Schema Registry.

        Returns:
            Schema ID
        """
        subject = f"{self.topic}-value"
        # Create a Schema object from the schema string
        schema = Schema(self.schema_str, schema_type="AVRO")
        registered_schema = self._schema_registry_client.register_schema(
            subject_name=subject,
            schema=schema,
        )
        return registered_schema
