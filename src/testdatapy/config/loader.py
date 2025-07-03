"""Configuration loader for test data generation."""
import json
from pathlib import Path
from typing import Any, List, Optional

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaConfig(BaseModel):
    """Kafka connection configuration."""

    bootstrap_servers: str = Field(default="localhost:9092")
    security_protocol: str = Field(default="PLAINTEXT")
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None
    sasl_kerberos_service_name: str | None = None
    sasl_kerberos_domain_name: str | None = None
    ssl_ca_location: str | None = None
    ssl_certificate_location: str | None = None
    ssl_key_location: str | None = None
    ssl_key_password: str | None = None
    ssl_keystore_location: str | None = None
    ssl_keystore_password: str | None = None
    ssl_truststore_location: str | None = None
    ssl_truststore_password: str | None = None


class SchemaRegistryConfig(BaseModel):
    """Schema Registry configuration."""

    url: str = Field(default="http://localhost:8081")
    basic_auth_user: str | None = None
    basic_auth_pass: str | None = None
    ssl_ca_location: str | None = None
    ssl_certificate_location: str | None = None
    ssl_key_location: str | None = None


class ProducerConfig(BaseModel):
    """Producer configuration."""

    rate_per_second: float = Field(default=10.0, gt=0)
    max_messages: int | None = Field(default=None, gt=0)
    max_duration_seconds: float | None = Field(default=None, gt=0)
    batch_size: int = Field(default=100, gt=0)


class GeneratorConfig(BaseModel):
    """Generator configuration."""

    type: str = Field(default="faker")  # faker, csv, etc.
    schema_path: str | None = None
    csv_path: str | None = None
    key_field: str | None = None


class ProtobufConfig(BaseModel):
    """Protobuf configuration."""

    schema_paths: List[str] = Field(default_factory=list)
    auto_compile: bool = Field(default=False)
    temp_compilation: bool = Field(default=True)
    validate_dependencies: bool = Field(default=False)
    protoc_path: Optional[str] = None
    timeout: int = Field(default=60, gt=0)

    @field_validator('schema_paths')
    @classmethod
    def validate_schema_paths(cls, v):
        """Validate that schema paths exist and are accessible."""
        for path in v:
            path_obj = Path(path)
            if not path_obj.exists():
                raise ValueError(f"Schema path does not exist: {path}")
            if not path_obj.is_dir():
                raise ValueError(f"Schema path is not a directory: {path}")
        return v

    @field_validator('timeout')
    @classmethod
    def validate_timeout(cls, v):
        """Validate timeout is positive."""
        if v <= 0:
            raise ValueError("Timeout must be positive")
        return v


class AppConfig(BaseSettings):
    """Application configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        env_prefix="TESTDATAPY_",
    )

    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    schema_registry: SchemaRegistryConfig = Field(default_factory=SchemaRegistryConfig)
    producer: ProducerConfig = Field(default_factory=ProducerConfig)
    generator: GeneratorConfig = Field(default_factory=GeneratorConfig)
    protobuf: ProtobufConfig = Field(default_factory=ProtobufConfig)

    @classmethod
    def from_file(cls, config_file: str) -> "AppConfig":
        """Load configuration from a JSON file.

        Args:
            config_file: Path to configuration file

        Returns:
            AppConfig instance
        """
        with open(config_file) as f:
            config_data = json.load(f)

        # Map legacy format to new format
        kafka_config = {}
        schema_registry_config = {}
        producer_config = {}
        generator_config = {}
        protobuf_config = {}

        for key, value in config_data.items():
            if key.startswith("schema.registry."):
                new_key = key.replace("schema.registry.", "").replace(".", "_")
                schema_registry_config[new_key] = value
            elif key == "bootstrap.servers":
                kafka_config["bootstrap_servers"] = value
            elif key.startswith("ssl."):
                kafka_config[key.replace(".", "_")] = value
            elif key == "security.protocol":
                kafka_config["security_protocol"] = value
            elif key.startswith("sasl."):
                kafka_config[key.replace(".", "_")] = value
            elif key == "protobuf":
                protobuf_config = value
            else:
                # Try to map to producer or generator config
                if key in ["rate_per_second", "max_messages", "max_duration_seconds"]:
                    producer_config[key] = value
                else:
                    generator_config[key] = value

        return cls(
            kafka=KafkaConfig(**kafka_config) if kafka_config else KafkaConfig(),
            schema_registry=SchemaRegistryConfig(**schema_registry_config)
            if schema_registry_config
            else SchemaRegistryConfig(),
            producer=ProducerConfig(**producer_config) if producer_config else ProducerConfig(),
            generator=GeneratorConfig(**generator_config)
            if generator_config
            else GeneratorConfig(),
            protobuf=ProtobufConfig(**protobuf_config) if protobuf_config else ProtobufConfig(),
        )

    def to_confluent_config(self) -> dict[str, Any]:
        """Convert to Confluent Kafka configuration format.

        Returns:
            Dictionary with Confluent Kafka configuration
        """
        config = {
            "bootstrap.servers": self.kafka.bootstrap_servers,
        }

        if self.kafka.security_protocol != "PLAINTEXT":
            config["security.protocol"] = self.kafka.security_protocol

            # SASL configuration
            if self.kafka.sasl_mechanism:
                config["sasl.mechanism"] = self.kafka.sasl_mechanism
            if self.kafka.sasl_username:
                config["sasl.username"] = self.kafka.sasl_username
            if self.kafka.sasl_password:
                config["sasl.password"] = self.kafka.sasl_password
            if self.kafka.sasl_kerberos_service_name:
                config["sasl.kerberos.service.name"] = self.kafka.sasl_kerberos_service_name
            if self.kafka.sasl_kerberos_domain_name:
                config["sasl.kerberos.domain.name"] = self.kafka.sasl_kerberos_domain_name

            # SSL configuration
            if self.kafka.ssl_ca_location:
                config["ssl.ca.location"] = self.kafka.ssl_ca_location
            if self.kafka.ssl_certificate_location:
                config["ssl.certificate.location"] = self.kafka.ssl_certificate_location
            if self.kafka.ssl_key_location:
                config["ssl.key.location"] = self.kafka.ssl_key_location
            if self.kafka.ssl_key_password:
                config["ssl.key.password"] = self.kafka.ssl_key_password
            if self.kafka.ssl_keystore_location:
                config["ssl.keystore.location"] = self.kafka.ssl_keystore_location
            if self.kafka.ssl_keystore_password:
                config["ssl.keystore.password"] = self.kafka.ssl_keystore_password
            if self.kafka.ssl_truststore_location:
                config["ssl.truststore.location"] = self.kafka.ssl_truststore_location
            if self.kafka.ssl_truststore_password:
                config["ssl.truststore.password"] = self.kafka.ssl_truststore_password

        return config

    def to_schema_registry_config(self) -> dict[str, Any]:
        """Convert to Schema Registry configuration format.

        Returns:
            Dictionary with Schema Registry configuration
        """
        config = {
            "url": self.schema_registry.url,
        }

        if self.schema_registry.basic_auth_user:
            config["basic.auth.user.info"] = (
                f"{self.schema_registry.basic_auth_user}:{self.schema_registry.basic_auth_pass}"
            )

        if self.schema_registry.ssl_ca_location:
            config["ssl.ca.location"] = self.schema_registry.ssl_ca_location
        if self.schema_registry.ssl_certificate_location:
            config["ssl.certificate.location"] = self.schema_registry.ssl_certificate_location
        if self.schema_registry.ssl_key_location:
            config["ssl.key.location"] = self.schema_registry.ssl_key_location

        return config

    def to_protobuf_config(self) -> dict[str, Any]:
        """Convert to protobuf configuration format.

        Returns:
            Dictionary with protobuf configuration
        """
        return {
            "schema_paths": self.protobuf.schema_paths,
            "auto_compile": self.protobuf.auto_compile,
            "temp_compilation": self.protobuf.temp_compilation,
            "validate_dependencies": self.protobuf.validate_dependencies,
            "protoc_path": self.protobuf.protoc_path,
            "timeout": self.protobuf.timeout,
        }
