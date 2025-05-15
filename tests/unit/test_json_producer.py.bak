"""Unit tests for JSON producer."""
import json
from unittest.mock import patch

from testdatapy.producers.json_producer import JsonProducer
from tests.unit.mocks import MockConfluentProducer


class TestJsonProducer:
    """Test the JsonProducer class."""

    @patch("testdatapy.producers.json_producer.ConfluentProducer", MockConfluentProducer)
    def test_initialization(self):
        """Test producer initialization."""
        producer = JsonProducer(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            config={"client.id": "test"},
            key_field="id",
        )
        assert producer.bootstrap_servers == "localhost:9092"
        assert producer.topic == "test-topic"
        assert producer.key_field == "id"
        assert producer.config == {"client.id": "test"}

    @patch("testdatapy.producers.json_producer.ConfluentProducer", MockConfluentProducer)
    def test_produce_with_explicit_key(self):
        """Test producing with explicit key."""
        producer = JsonProducer(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
        )

        test_data = {"id": 123, "name": "Test User"}
        producer.produce(key="test-key", value=test_data)

        # Check the message was produced
        assert len(producer._producer.messages) == 1
        message = producer._producer.messages[0]
        assert message["topic"] == "test-topic"
        assert message["key"] == b"test-key"  # Should be serialized
        assert json.loads(message["value"]) == test_data

    @patch("testdatapy.producers.json_producer.ConfluentProducer", MockConfluentProducer)
    def test_produce_with_key_field(self):
        """Test producing with key extracted from value."""
        producer = JsonProducer(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            key_field="id",
        )

        test_data = {"id": 123, "name": "Test User"}
        producer.produce(key=None, value=test_data)

        # Check the key was extracted
        assert len(producer._producer.messages) == 1
        message = producer._producer.messages[0]
        assert message["key"] == b"123"  # Should be string of id field

    @patch("testdatapy.producers.json_producer.ConfluentProducer", MockConfluentProducer)
    def test_produce_without_key(self):
        """Test producing without key."""
        producer = JsonProducer(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
        )

        test_data = {"name": "Test User"}
        producer.produce(key=None, value=test_data)

        # Check the message has no key
        assert len(producer._producer.messages) == 1
        message = producer._producer.messages[0]
        assert message["key"] is None

    @patch("testdatapy.producers.json_producer.ConfluentProducer", MockConfluentProducer)
    def test_delivery_callback(self):
        """Test delivery callback is called."""
        producer = JsonProducer(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
        )

        callback_called = False
        callback_error = None
        callback_msg = None

        def delivery_callback(err, msg):
            nonlocal callback_called, callback_error, callback_msg
            callback_called = True
            callback_error = err
            callback_msg = msg

        test_data = {"test": "data"}
        producer.produce(key="test", value=test_data, on_delivery=delivery_callback)

        assert callback_called is True
        assert callback_error is None
        assert callback_msg is not None

    @patch("testdatapy.producers.json_producer.ConfluentProducer", MockConfluentProducer)
    def test_flush(self):
        """Test flushing messages."""
        producer = JsonProducer(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
        )

        # Produce some messages
        for i in range(5):
            producer.produce(key=str(i), value={"id": i})

        # Flush should return 0 (no messages in queue)
        remaining = producer.flush()
        assert remaining == 0

    @patch("testdatapy.producers.json_producer.ConfluentProducer", MockConfluentProducer)
    def test_context_manager(self):
        """Test using producer as context manager."""
        with JsonProducer(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
        ) as producer:
            producer.produce(key="test", value={"test": "data"})
            assert len(producer._producer.messages) == 1

    @patch("testdatapy.producers.json_producer.ConfluentProducer", MockConfluentProducer)
    def test_json_serialization(self):
        """Test JSON serialization of complex data."""
        producer = JsonProducer(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
        )

        complex_data = {
            "string": "test",
            "int": 123,
            "float": 45.67,
            "bool": True,
            "null": None,
            "list": [1, 2, 3],
            "dict": {"nested": "value"},
        }

        producer.produce(key="complex", value=complex_data)

        message = producer._producer.messages[0]
        deserialized = json.loads(message["value"])
        assert deserialized == complex_data

    @patch("testdatapy.producers.json_producer.ConfluentProducer", MockConfluentProducer)
    def test_queue_size(self):
        """Test getting queue size."""
        producer = JsonProducer(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
        )

        assert producer.queue_size == 0  # Mock always returns 0
