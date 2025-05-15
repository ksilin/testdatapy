"""Unit tests for Avro producer."""
import json
from unittest.mock import patch

import pytest

from testdatapy.producers.avro_producer import AvroProducer
from tests.unit.mocks import MockAvroSerializer, MockConfluentProducer, MockSchemaRegistryClient, MockSchema


class TestAvroProducer:
    """Test the AvroProducer class."""

    @pytest.fixture
    def sample_schema(self):
        """Sample Avro schema for testing."""
        return json.dumps({
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
            ]
        })

    @patch("testdatapy.producers.avro_producer.ConfluentProducer", MockConfluentProducer)
    @patch("testdatapy.producers.avro_producer.SchemaRegistryClient", MockSchemaRegistryClient)
    @patch("testdatapy.producers.avro_producer.AvroSerializer", MockAvroSerializer)
    def test_initialization_with_schema_str(self, sample_schema):
        """Test producer initialization with schema string."""
        producer = AvroProducer(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            schema_registry_url="http://localhost:8081",
            schema_str=sample_schema,
        )
        assert producer.bootstrap_servers == "localhost:9092"
        assert producer.topic == "test-topic"
        assert producer.schema_str == sample_schema

    @patch("testdatapy.producers.avro_producer.ConfluentProducer", MockConfluentProducer)
    @patch("testdatapy.producers.avro_producer.SchemaRegistryClient", MockSchemaRegistryClient)
    @patch("testdatapy.producers.avro_producer.AvroSerializer", MockAvroSerializer)
    def test_initialization_with_schema_file(self, sample_schema, tmp_path):
        """Test producer initialization with schema file."""
        schema_file = tmp_path / "test.avsc"
        schema_file.write_text(sample_schema)

        producer = AvroProducer(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            schema_registry_url="http://localhost:8081",
            schema_path=str(schema_file),
        )
        assert producer.schema_str == sample_schema

    def test_initialization_without_schema(self):
        """Test that initialization fails without schema."""
        with pytest.raises(ValueError, match="Either schema_path or schema_str must be provided"):
            AvroProducer(
                bootstrap_servers="localhost:9092",
                topic="test-topic",
                schema_registry_url="http://localhost:8081",
            )

    @patch("testdatapy.producers.avro_producer.ConfluentProducer", MockConfluentProducer)
    @patch("testdatapy.producers.avro_producer.SchemaRegistryClient", MockSchemaRegistryClient)
    @patch("testdatapy.producers.avro_producer.AvroSerializer", MockAvroSerializer)
    def test_produce_with_explicit_key(self, sample_schema):
        """Test producing with explicit key."""
        producer = AvroProducer(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            schema_registry_url="http://localhost:8081",
            schema_str=sample_schema,
        )

        test_data = {"id": 123, "name": "Test User"}
        producer.produce(key="test-key", value=test_data)

        # Check the message was produced
        assert len(producer._producer.messages) == 1
        message = producer._producer.messages[0]
        assert message["topic"] == "test-topic"
        assert message["key"] == b"test-key"

    @patch("testdatapy.producers.avro_producer.ConfluentProducer", MockConfluentProducer)
    @patch("testdatapy.producers.avro_producer.SchemaRegistryClient", MockSchemaRegistryClient)
    @patch("testdatapy.producers.avro_producer.AvroSerializer", MockAvroSerializer)
    def test_produce_with_key_field(self, sample_schema):
        """Test producing with key extracted from value."""
        producer = AvroProducer(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            schema_registry_url="http://localhost:8081",
            schema_str=sample_schema,
            key_field="id",
        )

        test_data = {"id": 123, "name": "Test User"}
        producer.produce(key=None, value=test_data)

        # Check the key was extracted
        assert len(producer._producer.messages) == 1
        message = producer._producer.messages[0]
        assert message["key"] == b"123"

    @patch("testdatapy.producers.avro_producer.ConfluentProducer", MockConfluentProducer)
    @patch("testdatapy.producers.avro_producer.SchemaRegistryClient", MockSchemaRegistryClient)
    @patch("testdatapy.producers.avro_producer.AvroSerializer", MockAvroSerializer)
    def test_schema_registry_config(self, sample_schema):
        """Test Schema Registry configuration."""
        sr_config = {
            "basic.auth.user.info": "user:pass",
            "ssl.ca.location": "/path/to/ca",
        }

        producer = AvroProducer(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            schema_registry_url="http://localhost:8081",
            schema_str=sample_schema,
            schema_registry_config=sr_config,
        )

        # Check that Schema Registry client was configured
        expected_config = {"url": "http://localhost:8081"}
        expected_config.update(sr_config)
        assert producer._schema_registry_client.config == expected_config

    @patch("testdatapy.producers.avro_producer.ConfluentProducer", MockConfluentProducer)
    @patch("testdatapy.producers.avro_producer.SchemaRegistryClient", MockSchemaRegistryClient)
    @patch("testdatapy.producers.avro_producer.AvroSerializer", MockAvroSerializer)
    @patch("testdatapy.producers.avro_producer.Schema", MockSchema)  # Add Schema mock
    def test_register_schema(self, sample_schema):
        """Test schema registration."""
        producer = AvroProducer(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            schema_registry_url="http://localhost:8081",
            schema_str=sample_schema,
        )

        schema_id = producer.register_schema()
        assert schema_id == 1  # Mock returns incrementing IDs

        # Check that schema was registered
        subject = f"{producer.topic}-value"
        assert subject in producer._schema_registry_client.schemas
        # The schema is stored as a Schema object now, not a string
        registered_schema = producer._schema_registry_client.schemas[subject]
        assert registered_schema.schema_str == sample_schema

    @patch("testdatapy.producers.avro_producer.ConfluentProducer", MockConfluentProducer)
    @patch("testdatapy.producers.avro_producer.SchemaRegistryClient", MockSchemaRegistryClient)
    @patch("testdatapy.producers.avro_producer.AvroSerializer", MockAvroSerializer)
    @patch("testdatapy.producers.avro_producer.Schema", MockSchema)  # Add Schema mock
    def test_get_latest_schema(self, sample_schema):
        """Test getting latest schema from registry."""
        producer = AvroProducer(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            schema_registry_url="http://localhost:8081",
            schema_str=sample_schema,
        )

        # Register schema first
        producer.register_schema()

        # Get latest schema
        latest_schema = producer.get_latest_schema()
        assert latest_schema == json.loads(sample_schema)

    @patch("testdatapy.producers.avro_producer.ConfluentProducer", MockConfluentProducer)
    @patch("testdatapy.producers.avro_producer.SchemaRegistryClient", MockSchemaRegistryClient)
    @patch("testdatapy.producers.avro_producer.AvroSerializer", MockAvroSerializer)
    def test_delivery_callback(self, sample_schema):
        """Test delivery callback is called."""
        producer = AvroProducer(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            schema_registry_url="http://localhost:8081",
            schema_str=sample_schema,
        )

        callback_called = False
        callback_error = None

        def delivery_callback(err, msg):
            nonlocal callback_called, callback_error
            callback_called = True
            callback_error = err

        test_data = {"id": 123, "name": "Test"}
        producer.produce(key="test", value=test_data, on_delivery=delivery_callback)

        assert callback_called is True
        assert callback_error is None
