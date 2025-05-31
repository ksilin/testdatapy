"""Protobuf producer implementation with Schema Registry support."""
import json
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any, Optional, Union

from confluent_kafka import Producer as ConfluentProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

from testdatapy.producers.base import KafkaProducer

logger = logging.getLogger(__name__)


class ProtobufProducer(KafkaProducer):
    """Kafka producer for Protobuf messages with Schema Registry support."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        schema_registry_url: str,
        schema_proto_class: Optional[type] = None,
        schema_file_path: Optional[Union[str, Path]] = None,
        config: Optional[dict[str, Any]] = None,
        schema_registry_config: Optional[dict[str, Any]] = None,
        key_field: Optional[str] = None,
        auto_create_topic: bool = True,
        topic_config: Optional[dict[str, Any]] = None,
    ):
        """Initialize the Protobuf producer.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Topic to produce to
            schema_registry_url: Schema Registry URL
            schema_proto_class: Protobuf message class for value serialization
            schema_file_path: Path to .proto file (for schema registration)
            config: Additional producer configuration
            schema_registry_config: Schema Registry configuration
            key_field: Field to use as message key
            auto_create_topic: Whether to auto-create topic if it doesn't exist
            topic_config: Configuration for topic creation
        """
        super().__init__(bootstrap_servers, topic, config, auto_create_topic, topic_config)
        
        if not schema_registry_url:
            raise ValueError("Schema Registry URL is required for Protobuf producer")
        
        if not schema_proto_class:
            raise ValueError("Protobuf message class is required")
        
        self.schema_registry_url = schema_registry_url
        self.schema_proto_class = schema_proto_class
        self.schema_file_path = Path(schema_file_path) if schema_file_path else None
        self.key_field = key_field
        
        # Set up Schema Registry client
        sr_config = schema_registry_config or {}
        if "url" not in sr_config:
            sr_config["url"] = schema_registry_url
        self.schema_registry = SchemaRegistryClient(sr_config)
        
        # Set up serializers
        self._key_serializer = StringSerializer("utf-8")
        self._value_serializer = ProtobufSerializer(
            schema_proto_class,
            self.schema_registry,
            {"use.deprecated.format": False}
        )
        
        # Merge configurations
        producer_config = {"bootstrap.servers": bootstrap_servers}
        if config:
            producer_config.update(config)
        
        self._producer = ConfluentProducer(producer_config)
        
        logger.info(
            f"Initialized Protobuf producer for topic '{topic}' "
            f"with schema registry at {schema_registry_url}"
        )

    def produce(
        self,
        value: dict[str, Any],
        key: Optional[str] = None,
        on_delivery: Optional[Callable] = None,
    ) -> None:
        """Produce a message to Kafka.

        Args:
            value: Message value as dictionary
            key: Optional message key (overrides key_field)
            on_delivery: Optional callback for delivery reports
        """
        # Determine key
        if key is None and self.key_field and self.key_field in value:
            key = str(value[self.key_field])
        
        # Serialize key
        serialized_key = self._key_serializer(key) if key else None
        
        # Convert dict to protobuf message
        protobuf_message = self._dict_to_protobuf(value)
        
        # Create serialization context
        ctx = SerializationContext(self.topic, MessageField.VALUE)
        
        # Serialize value
        serialized_value = self._value_serializer(protobuf_message, ctx)
        
        # Use provided callback or default
        callback = on_delivery or self._delivery_callback
        
        # Produce message
        self._producer.produce(
            topic=self.topic,
            key=serialized_key,
            value=serialized_value,
            on_delivery=callback
        )
        
        # Trigger any callbacks
        self._producer.poll(0)

    def _dict_to_protobuf(self, data: dict[str, Any]) -> Any:
        """Convert dictionary to protobuf message.

        Args:
            data: Dictionary data

        Returns:
            Protobuf message instance
        """
        message = self.schema_proto_class()
        
        # Define nested message field mappings
        # This allows for flexible handling of nested structures
        nested_mappings = {
            'address': ['street', 'city', 'postal_code', 'country_code']
        }
        
        # Handle nested message structures
        for nested_field, field_names in nested_mappings.items():
            if hasattr(message, nested_field) and any(field in data for field in field_names):
                nested_message = getattr(message, nested_field)
                for field_name in field_names:
                    if field_name in data:
                        setattr(nested_message, field_name, data[field_name])
        
        # Set all other fields directly on the message
        nested_field_names = {field for fields in nested_mappings.values() for field in fields}
        for field, value in data.items():
            if field not in nested_field_names and hasattr(message, field):
                setattr(message, field, value)
        
        return message

    def register_schema(self, subject: str) -> int:
        """Register the protobuf schema with Schema Registry.

        Args:
            subject: Subject name for schema registration

        Returns:
            Schema ID
        """
        if not self.schema_file_path or not self.schema_file_path.exists():
            logger.warning("No schema file path provided for registration")
            return -1
        
        # Read schema from file
        with open(self.schema_file_path) as f:
            schema_str = f.read()
        
        # Register schema
        schema_id = self.schema_registry.register_schema(
            subject,
            {"schemaType": "PROTOBUF", "schema": schema_str}
        )
        
        logger.info(f"Registered protobuf schema for subject '{subject}' with ID {schema_id}")
        return schema_id

    def get_latest_schema(self, subject: str) -> str:
        """Get the latest schema for a subject.

        Args:
            subject: Subject name

        Returns:
            Schema string
        """
        version = self.schema_registry.get_latest_version(subject)
        return version.schema.schema

    def _delivery_callback(self, err, msg):
        """Default delivery callback.

        Args:
            err: Delivery error if any
            msg: Message that was produced
        """
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(
                f"Message delivered to {msg.topic()} "
                f"[partition {msg.partition()}] at offset {msg.offset()}"
            )

    def flush(self, timeout: float = 10.0) -> int:
        """Flush any pending messages.

        Args:
            timeout: Maximum time to wait for messages to be delivered

        Returns:
            Number of messages still in queue
        """
        return self._producer.flush(timeout)

    def close(self) -> None:
        """Close the producer and clean up resources."""
        self.flush()
        # Producer doesn't have explicit close method
        logger.info(f"Closed Protobuf producer for topic '{self.topic}'")