"""Integration tests for Kafka connectivity."""
import json
import time
from typing import Any

import pytest
from click.testing import CliRunner
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

from testdatapy.cli import cli
from testdatapy.generators import FakerGenerator
from testdatapy.producers import AvroProducer, JsonProducer


class TestKafkaIntegration:
    """Integration tests with real Kafka cluster."""

    @pytest.fixture(scope="class")
    def kafka_config(self):
        """Kafka configuration for tests."""
        return {
            "bootstrap.servers": "localhost:9092",
        }

    @pytest.fixture(scope="class")
    def admin_client(self, kafka_config):
        """Create admin client."""
        return AdminClient(kafka_config)

    @pytest.fixture(scope="class")
    def schema_registry_url(self):
        """Schema Registry URL."""
        return "http://localhost:8081"

    @pytest.fixture(autouse=True)
    def setup_topics(self, admin_client):
        """Ensure test topics exist."""
        topics = [
            NewTopic("test-json", num_partitions=1, replication_factor=1),
            NewTopic("test-avro", num_partitions=1, replication_factor=1),
            NewTopic("test-integration", num_partitions=1, replication_factor=1),
        ]

        # Create topics
        fs = admin_client.create_topics(topics, request_timeout=10.0)
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print(f"Topic {topic} created")
            except Exception as e:
                # Topic might already exist - this is fine
                if "TOPIC_ALREADY_EXISTS" not in str(e):
                    print(f"Topic {topic} creation failed: {e}")
                else:
                    print(f"Topic {topic} already exists")

        # Wait for topics to be ready
        time.sleep(2)

    @pytest.fixture
    def consumer_config(self, kafka_config):
        """Consumer configuration."""
        config = kafka_config.copy()
        config.update({
            "group.id": f"test-consumer-{time.time()}",  # Unique group ID
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        return config

    @pytest.fixture
    def sample_schema(self):
        """Sample Avro schema."""
        return json.dumps({
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "message", "type": "string"},
            ]
        })

    @pytest.fixture
    def runner(self):
        """Create a CLI test runner."""
        return CliRunner()

    def consume_messages(
        self, topic: str, num_messages: int = 1, timeout: float = 10.0, 
        consumer_config: dict[str, Any] = None
    ) -> list[dict[str, Any]]:
        """Consume messages from a topic."""
        if consumer_config is None:
            consumer_config = {
                "bootstrap.servers": "localhost:9092",
                "group.id": f"test-consumer-{time.time()}",  # Unique group ID
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        
        consumer = Consumer(consumer_config)
        consumer.subscribe([topic])

        messages = []
        start_time = time.time()

        while len(messages) < num_messages and time.time() - start_time < timeout:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise Exception(msg.error())

            # Decode message
            try:
                value = json.loads(msg.value().decode("utf-8"))
                messages.append({
                    "key": msg.key().decode("utf-8") if msg.key() else None,
                    "value": value,
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                })
            except (json.JSONDecodeError, UnicodeDecodeError):
                # For Avro messages, just store raw bytes
                messages.append({
                    "key": msg.key().decode("utf-8") if msg.key() else None,
                    "value": msg.value(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                })

        consumer.close()
        return messages

    def test_json_producer(self, kafka_config, consumer_config):
        """Test JSON producer with real Kafka."""
        producer = JsonProducer(
            bootstrap_servers=kafka_config["bootstrap.servers"],
            topic="test-json",
            key_field="id",
        )

        # Produce messages
        test_data = [
            {"id": 1, "message": "Hello"},
            {"id": 2, "message": "World"},
        ]

        for data in test_data:
            producer.produce(key=None, value=data)

        producer.flush()

        # Consume and verify
        messages = self.consume_messages(
            "test-json", num_messages=2, consumer_config=consumer_config
        )
        assert len(messages) == 2

        # Verify content
        assert messages[0]["key"] == "1"
        assert messages[0]["value"]["message"] == "Hello"
        assert messages[1]["key"] == "2"
        assert messages[1]["value"]["message"] == "World"

    def test_avro_producer(self, kafka_config, schema_registry_url, sample_schema, consumer_config):
        """Test Avro producer with real Kafka and Schema Registry."""
        producer = AvroProducer(
            bootstrap_servers=kafka_config["bootstrap.servers"],
            topic="test-avro",
            schema_registry_url=schema_registry_url,
            schema_str=sample_schema,
            key_field="id",
        )

        # Register schema
        schema_id = producer.register_schema()
        assert schema_id > 0

        # Produce messages
        test_data = [
            {"id": 100, "message": "Avro Test 1"},
            {"id": 200, "message": "Avro Test 2"},
        ]

        for data in test_data:
            producer.produce(key=None, value=data)

        producer.flush()

        # Consume and verify (will be binary Avro)
        messages = self.consume_messages(
            "test-avro", num_messages=2, consumer_config=consumer_config
        )
        assert len(messages) == 2
        assert messages[0]["key"] == "100"
        assert messages[1]["key"] == "200"

    def test_faker_generator_with_producer(self, kafka_config, consumer_config):
        """Test Faker generator with real producer."""
        generator = FakerGenerator(rate_per_second=100, max_messages=5)
        producer = JsonProducer(
            bootstrap_servers=kafka_config["bootstrap.servers"],
            topic="test-integration",
        )

        # Generate and produce data
        for data in generator.generate():
            producer.produce(key=str(data.get("CustomerID", "")), value=data)

        producer.flush()
        
        # Wait a bit for messages to be available
        time.sleep(1)

        # Consume and verify
        messages = self.consume_messages(
            "test-integration", num_messages=5, consumer_config=consumer_config
        )
        assert len(messages) == 5

        # Verify structure
        for msg in messages:
            value = msg["value"]
            assert "CustomerID" in value
            assert "CustomerFirstName" in value
            assert "CustomerLastName" in value

    def test_cli_produce_json(self, runner, kafka_config, consumer_config):
        """Test CLI produce command with JSON."""
        result = runner.invoke(
            cli,
            [
                "produce",
                "--topic", "test-integration",
                "--format", "json",
                "--generator", "faker",
                "--count", "3",
                "--rate", "100",
            ],
        )

        assert result.exit_code == 0
        assert "Total messages: 3" in result.output
        
        # Wait a bit for messages to be available
        time.sleep(1)

        # Verify messages were produced
        messages = self.consume_messages(
            "test-integration", num_messages=3, consumer_config=consumer_config
        )
        assert len(messages) == 3

    def test_end_to_end_flow(
        self, kafka_config, schema_registry_url, tmp_path, runner, consumer_config
    ):
        """Test complete end-to-end flow."""
        # Create config file
        config = {
            "bootstrap.servers": kafka_config["bootstrap.servers"],
            "schema.registry.url": schema_registry_url,
            "rate_per_second": 50,
        }
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config))

        # Create schema file
        schema = {
            "type": "record",
            "name": "Customer",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": "string"},
            ],
        }
        schema_file = tmp_path / "schema.avsc"
        schema_file.write_text(json.dumps(schema))

        # Use CLI to produce
        result = runner.invoke(
            cli,
            [
                "produce",
                "--config", str(config_file),
                "--topic", "test-integration",
                "--format", "avro",
                "--schema-file", str(schema_file),
                "--count", "5",
                "--key-field", "id",
            ],
        )

        assert result.exit_code == 0
        assert "Total messages: 5" in result.output
        
        # Wait a bit for messages to be available
        time.sleep(1)

        # Verify messages
        messages = self.consume_messages(
            "test-integration", num_messages=5, consumer_config=consumer_config
        )
        assert len(messages) == 5
