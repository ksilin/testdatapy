"""Enhanced integration tests for Kafka connectivity and mTLS."""
import json
import time
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Consumer, KafkaError
from testcontainers.compose import DockerCompose

from testdatapy.cli import cli
from testdatapy.generators import CSVGenerator, FakerGenerator
from testdatapy.producers import AvroProducer, JsonProducer


class TestKafkaIntegrationEnhanced:
    """Enhanced integration tests with real Kafka cluster."""

    @pytest.fixture(scope="session")
    def docker_compose(self):
        """Start Docker Compose services for testing."""
        compose_file = Path(__file__).parent.parent.parent / "docker" / "docker-compose.yml"
        with DockerCompose(
            str(compose_file.parent), compose_file_name=compose_file.name
        ) as compose:
            # Wait for services to be ready
            time.sleep(30)
            yield compose

    @pytest.fixture
    def kafka_config(self, docker_compose):
        """Kafka configuration for tests."""
        return {
            "bootstrap.servers": "localhost:9092",
        }

    @pytest.fixture
    def schema_registry_url(self, docker_compose):
        """Schema Registry URL."""
        return "http://localhost:8081"

    def test_json_producer_with_rate_limiting(self, kafka_config):
        """Test JSON producer respects rate limiting."""
        rate_per_second = 5
        num_messages = 10
        
        generator = FakerGenerator(rate_per_second=rate_per_second, max_messages=num_messages)
        producer = JsonProducer(
            bootstrap_servers=kafka_config["bootstrap.servers"],
            topic="test-rate-limit",
        )

        start_time = time.time()
        for data in generator.generate():
            producer.produce(key=str(data.get("CustomerID", "")), value=data)
        producer.flush()
        elapsed = time.time() - start_time

        # Should take approximately (num_messages - 1) / rate_per_second seconds
        expected_time = (num_messages - 1) / rate_per_second
        assert elapsed >= expected_time * 0.9  # Allow 10% tolerance

    def test_csv_to_avro_integration(self, kafka_config, schema_registry_url, tmp_path):
        """Test CSV to Avro pipeline matching original functionality."""
        # Create test CSV
        csv_content = (
            "CustomerID,CustomerFirstName,CustomerLastName,PostalCode,Street,City,"
            "CountryCode\n"
            "1,John,Doe,12345,Main St,Anytown,US\n"
            "2,Jane,Smith,67890,Oak Ave,Somewhere,UK"
        )
        
        csv_file = tmp_path / "test_customers.csv"
        csv_file.write_text(csv_content)

        # Use CSV generator
        generator = CSVGenerator(
            csv_file=str(csv_file),
            rate_per_second=100,
            cycle=False,
        )

        # Use Avro producer
        schema = {
            "type": "record",
            "name": "Customer",
            "fields": [
                {"name": "CustomerID", "type": "int"},
                {"name": "CustomerFirstName", "type": "string"},
                {"name": "CustomerLastName", "type": "string"},
                {"name": "PostalCode", "type": "int"},
                {"name": "Street", "type": "string"},
                {"name": "City", "type": "string"},
                {"name": "CountryCode", "type": "string"},
            ]
        }

        producer = AvroProducer(
            bootstrap_servers=kafka_config["bootstrap.servers"],
            topic="test-csv-avro",
            schema_registry_url=schema_registry_url,
            schema_str=json.dumps(schema),
            key_field="CustomerID",
        )

        # Produce all records
        message_count = 0
        for data in generator.generate():
            # Convert types to match schema
            data["CustomerID"] = int(data["CustomerID"])
            data["PostalCode"] = int(data["PostalCode"])
            producer.produce(key=None, value=data)
            message_count += 1

        producer.flush()
        assert message_count == 2

        # Verify messages were produced
        messages = self.consume_messages(kafka_config, "test-csv-avro", num_messages=2)
        assert len(messages) == 2
        assert messages[0]["key"] == "1"
        assert messages[1]["key"] == "2"

    def test_schema_registry_integration(self, kafka_config, schema_registry_url):
        """Test Schema Registry integration."""
        schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "version", "type": "int"},
                {"name": "message", "type": "string"},
            ]
        }

        producer = AvroProducer(
            bootstrap_servers=kafka_config["bootstrap.servers"],
            topic="test-schema-registry",
            schema_registry_url=schema_registry_url,
            schema_str=json.dumps(schema),
        )

        # Register schema
        schema_id = producer.register_schema()
        assert schema_id > 0

        # Get latest schema
        latest_schema = producer.get_latest_schema()
        assert latest_schema["name"] == "TestRecord"
        assert len(latest_schema["fields"]) == 3

    def test_error_handling(self, kafka_config):
        """Test error handling in producers."""
        # Test with invalid bootstrap servers
        producer = JsonProducer(
            bootstrap_servers="invalid:9092",
            topic="test-error",
            config={"socket.timeout.ms": 1000, "retry.backoff.ms": 100},
            auto_create_topic=False,  # Disable auto-creation for error testing
        )
        
        # Produce a message
        producer.produce(key="test", value={"test": "data"})
        
        # Flush should fail due to connection error
        remaining = producer.flush(timeout=2.0)
        assert remaining > 0  # Messages should still be in queue due to failure

    def test_cli_with_config_file(self, kafka_config, tmp_path):
        """Test CLI with configuration file."""
        from click.testing import CliRunner

        # Create config file
        config = {
            "bootstrap.servers": kafka_config["bootstrap.servers"],
            "rate_per_second": 50,
            "max_messages": 5,
        }
        config_file = tmp_path / "test_config.json"
        config_file.write_text(json.dumps(config))

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "produce",
                "--config", str(config_file),
                "--topic", "test-cli-config",
                "--format", "json",
                "--generator", "faker",
            ],
        )

        assert result.exit_code == 0
        assert "Rate: 50.0 msg/s" in result.output
        assert "Total messages: 5" in result.output

    def test_concurrent_producers(self, kafka_config):
        """Test multiple concurrent producers."""
        import threading

        def produce_messages(producer_id, topic):
            producer = JsonProducer(
                bootstrap_servers=kafka_config["bootstrap.servers"],
                topic=topic,
            )
            
            for i in range(10):
                producer.produce(
                    key=f"producer-{producer_id}",
                    value={"producer_id": producer_id, "message_id": i}
                )
            producer.flush()

        threads = []
        topic = "test-concurrent"
        
        # Start 3 concurrent producers
        for i in range(3):
            thread = threading.Thread(target=produce_messages, args=(i, topic))
            thread.start()
            threads.append(thread)

        # Wait for all threads
        for thread in threads:
            thread.join()

        # Verify all messages
        messages = self.consume_messages(kafka_config, topic, num_messages=30, timeout=20)
        assert len(messages) == 30
        
        # Check distribution
        producer_counts = {}
        for msg in messages:
            producer_id = msg["value"]["producer_id"]
            producer_counts[producer_id] = producer_counts.get(producer_id, 0) + 1
        
        assert all(count == 10 for count in producer_counts.values())

    def consume_messages(
        self, kafka_config: dict[str, Any], topic: str, num_messages: int = 1, timeout: float = 10.0
    ) -> list[dict[str, Any]]:
        """Helper to consume messages from a topic."""
        consumer_config = kafka_config.copy()
        consumer_config.update({
            "group.id": f"test-consumer-{time.time()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })

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
