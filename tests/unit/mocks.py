"""Test fixtures and utilities for producer tests."""
from typing import Any
from unittest.mock import MagicMock


class MockSchema:
    """Mock Schema object for testing."""
    
    def __init__(self, schema_str: str, schema_type: str = "AVRO"):
        self.schema_str = schema_str
        self.schema_type = schema_type
    
    def __eq__(self, other):
        if isinstance(other, str):
            return self.schema_str == other
        return self.schema_str == other.schema_str and self.schema_type == other.schema_type


class MockConfluentProducer:
    """Mock Confluent Kafka Producer for testing."""

    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.messages: list[dict[str, Any]] = []
        self._callbacks = []

    def produce(self, topic: str, key: Any, value: Any, on_delivery: Any | None = None):
        """Mock produce method."""
        message = {
            "topic": topic,
            "key": key,
            "value": value,
            "partition": 0,
            "offset": len(self.messages),
        }
        self.messages.append(message)

        if on_delivery:
            # Simulate successful delivery
            mock_msg = MagicMock()
            mock_msg.topic.return_value = topic
            mock_msg.partition.return_value = 0
            mock_msg.offset.return_value = len(self.messages) - 1
            on_delivery(None, mock_msg)

    def poll(self, timeout: float = 0):
        """Mock poll method."""
        return 0

    def flush(self, timeout: float = 10.0):
        """Mock flush method."""
        return 0

    def __len__(self):
        """Mock queue size."""
        return 0


class MockSchemaRegistryClient:
    """Mock Schema Registry Client for testing."""

    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.schemas = {}

    def register_schema(self, subject_name: str, schema: Any):
        """Mock schema registration."""
        # Store the actual Schema object
        self.schemas[subject_name] = schema
        return len(self.schemas)

    def get_latest_version(self, subject: str):
        """Mock getting latest schema version."""
        mock_schema = MagicMock()
        schema_obj = self.schemas.get(subject)
        if schema_obj:
            # Create nested structure to match real behavior
            mock_schema.schema.schema_str = schema_obj.schema_str if hasattr(schema_obj, 'schema_str') else str(schema_obj)
        else:
            mock_schema.schema.schema_str = "{}"
        return mock_schema


class MockAvroSerializer:
    """Mock Avro Serializer for testing."""

    def __init__(self, schema_str: str, schema_registry_client: Any):
        self.schema_str = schema_str
        self.schema_registry_client = schema_registry_client

    def __call__(self, value: dict[str, Any], ctx: Any) -> bytes:
        """Mock serialization."""
        # For testing, just return JSON bytes
        import json
        return json.dumps(value).encode("utf-8")


def create_mock_producer(producer_class: str = "ConfluentProducer"):
    """Create a mock producer for testing."""
    return MockConfluentProducer
