"""Unit tests for configuration loader."""
import json
import tempfile
from pathlib import Path

from testdatapy.config.loader import AppConfig, KafkaConfig


class TestAppConfig:
    """Test the AppConfig class."""

    def test_default_config(self):
        """Test default configuration values."""
        config = AppConfig()
        assert config.kafka.bootstrap_servers == "localhost:9092"
        assert config.kafka.security_protocol == "PLAINTEXT"
        assert config.schema_registry.url == "http://localhost:8081"
        assert config.producer.rate_per_second == 10.0

    def test_from_file_legacy_format(self):
        """Test loading configuration from legacy format file."""
        legacy_config = {
            "bootstrap.servers": "broker1:9092,broker2:9092",
            "schema.registry.url": "http://sr:8081",
            "security.protocol": "SSL",
            "ssl.ca.location": "/path/to/ca",
            "rate_per_second": 50,
            "max_messages": 1000,
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(legacy_config, f)
            f.flush()

            config = AppConfig.from_file(f.name)

            assert config.kafka.bootstrap_servers == "broker1:9092,broker2:9092"
            assert config.kafka.security_protocol == "SSL"
            assert config.kafka.ssl_ca_location == "/path/to/ca"
            assert config.schema_registry.url == "http://sr:8081"
            assert config.producer.rate_per_second == 50
            assert config.producer.max_messages == 1000

        Path(f.name).unlink()

    def test_to_confluent_config(self):
        """Test conversion to Confluent Kafka configuration."""
        config = AppConfig(
            kafka=KafkaConfig(
                bootstrap_servers="broker:9092",
                security_protocol="SSL",
                ssl_ca_location="/ca",
                ssl_certificate_location="/cert",
                ssl_key_location="/key",
                ssl_key_password="password",
            )
        )

        confluent_config = config.to_confluent_config()

        assert confluent_config["bootstrap.servers"] == "broker:9092"
        assert confluent_config["security.protocol"] == "SSL"
        assert confluent_config["ssl.ca.location"] == "/ca"
        assert confluent_config["ssl.certificate.location"] == "/cert"
        assert confluent_config["ssl.key.location"] == "/key"
        assert confluent_config["ssl.key.password"] == "password"

    def test_to_schema_registry_config(self):
        """Test conversion to Schema Registry configuration."""
        config = AppConfig()
        config.schema_registry.url = "http://sr:8081"
        config.schema_registry.basic_auth_user = "user"
        config.schema_registry.basic_auth_pass = "pass"

        sr_config = config.to_schema_registry_config()

        assert sr_config["url"] == "http://sr:8081"
        assert sr_config["basic.auth.user.info"] == "user:pass"
