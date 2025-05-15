"""Unit tests for producers."""
from testdatapy.producers.base import KafkaProducer


class MockProducer(KafkaProducer):
    """Mock producer for testing."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.messages = []

    def produce(self, key, value, on_delivery=None):
        """Mock produce method."""
        self.messages.append({"key": key, "value": value})
        if on_delivery:
            on_delivery(None, {"topic": self.topic, "key": key, "value": value})

    def flush(self, timeout=10.0):
        """Mock flush method."""
        return 0

    def close(self):
        """Mock close method."""
        pass


class TestKafkaProducer:
    """Test the KafkaProducer base class."""

    def test_producer_initialization(self):
        """Test producer initialization."""
        producer = MockProducer(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            config={"client.id": "test"},
        )
        assert producer.bootstrap_servers == "localhost:9092"
        assert producer.topic == "test-topic"
        assert producer.config == {"client.id": "test"}

    def test_produce_message(self):
        """Test producing a message."""
        producer = MockProducer(bootstrap_servers="localhost:9092", topic="test-topic")
        producer.produce(key="test-key", value={"test": "value"})

        assert len(producer.messages) == 1
        assert producer.messages[0]["key"] == "test-key"
        assert producer.messages[0]["value"] == {"test": "value"}

    def test_context_manager(self):
        """Test producer as context manager."""
        with MockProducer(bootstrap_servers="localhost:9092", topic="test-topic") as producer:
            producer.produce(key="test", value={"test": "data"})
            assert len(producer.messages) == 1

    def test_delivery_callback(self):
        """Test delivery callback."""
        callback_called = False
        callback_data = None

        def delivery_callback(err, msg):
            nonlocal callback_called, callback_data
            callback_called = True
            callback_data = msg

        producer = MockProducer(bootstrap_servers="localhost:9092", topic="test-topic")
        producer.produce(key="test", value={"test": "data"}, on_delivery=delivery_callback)

        assert callback_called is True
        assert callback_data["topic"] == "test-topic"
        assert callback_data["key"] == "test"
