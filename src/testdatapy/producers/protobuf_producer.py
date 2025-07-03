"""Protobuf producer implementation with Schema Registry support."""
import json
import logging
import time
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
from testdatapy.exceptions import (
    ProtobufSerializationError,
    SchemaRegistryConnectionError,
    SchemaRegistrationError,
    ProducerConnectionError,
    MessageProductionError,
    handle_and_reraise
)
from testdatapy.schema.exceptions import SchemaNotFoundError
from testdatapy.logging_config import get_schema_logger, PerformanceTimer

logger = get_schema_logger(__name__)


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
        
        # Set up Schema Registry client with error handling
        sr_config = schema_registry_config or {}
        if "url" not in sr_config:
            sr_config["url"] = schema_registry_url
        
        try:
            with PerformanceTimer(logger, "schema_registry_connection", url=schema_registry_url):
                self.schema_registry = SchemaRegistryClient(sr_config)
                # Test connection
                self.schema_registry.get_subjects()
                logger.info("Connected to Schema Registry", url=schema_registry_url)
        except Exception as e:
            raise SchemaRegistryConnectionError(
                registry_url=schema_registry_url,
                connection_error=e
            ) from e
        
        # Set up serializers
        self._key_serializer = StringSerializer("utf-8")
        self._value_serializer = ProtobufSerializer(
            schema_proto_class,
            self.schema_registry,
            {"use.deprecated.format": False}
        )
        
        # Set up Kafka producer with error handling
        producer_config = {"bootstrap.servers": bootstrap_servers}
        if config:
            producer_config.update(config)
        
        try:
            with PerformanceTimer(logger, "kafka_producer_connection", bootstrap_servers=bootstrap_servers):
                self._producer = ConfluentProducer(producer_config)
                logger.info("Created Kafka producer", bootstrap_servers=bootstrap_servers)
        except Exception as e:
            raise ProducerConnectionError(
                bootstrap_servers=bootstrap_servers,
                connection_error=e
            ) from e
        
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
        start_time = time.time()
        
        try:
            # Determine key
            if key is None and self.key_field and self.key_field in value:
                key = str(value[self.key_field])
            
            # Serialize key
            serialized_key = self._key_serializer(key) if key else None
            
            # Convert dict to protobuf message with error handling
            try:
                protobuf_message = self._dict_to_protobuf(value)
                logger.debug("Converted data to protobuf message", 
                           message_type=self.schema_proto_class.__name__,
                           data_fields=list(value.keys()))
            except Exception as e:
                raise ProtobufSerializationError(
                    data=value,
                    proto_class=self.schema_proto_class,
                    serialization_error=e
                ) from e
            
            # Create serialization context
            ctx = SerializationContext(self.topic, MessageField.VALUE)
            
            # Serialize value with error handling
            try:
                serialized_value = self._value_serializer(protobuf_message, ctx)
                message_size = len(serialized_value) if serialized_value else 0
                logger.debug("Serialized protobuf message", 
                           size_bytes=message_size,
                           topic=self.topic)
            except Exception as e:
                raise ProtobufSerializationError(
                    data=value,
                    proto_class=self.schema_proto_class,
                    serialization_error=e
                ) from e
            
            # Use provided callback or default
            callback = on_delivery or self._delivery_callback
            
            # Produce message with error handling
            try:
                self._producer.produce(
                    topic=self.topic,
                    key=serialized_key,
                    value=serialized_value,
                    on_delivery=callback
                )
                
                # Trigger any callbacks
                self._producer.poll(0)
                
                # Log successful production
                duration = time.time() - start_time
                logger.log_message_production(
                    topic=self.topic,
                    message_format="protobuf",
                    success=True,
                    duration=duration,
                    message_size=message_size
                )
                
            except Exception as e:
                raise MessageProductionError(
                    topic=self.topic,
                    message_data=value,
                    production_error=e
                ) from e
                
        except Exception as e:
            # Log failed production
            duration = time.time() - start_time
            logger.log_message_production(
                topic=self.topic,
                message_format="protobuf",
                success=False,
                duration=duration,
                error=str(e)
            )
            raise

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
        
        # Handle nested message structures first
        for nested_field, field_names in nested_mappings.items():
            if hasattr(message, nested_field):
                # Check if we have an address dict or individual address fields
                if nested_field in data and isinstance(data[nested_field], dict):
                    # Handle address as a nested dict
                    nested_message = getattr(message, nested_field)
                    address_data = data[nested_field]
                    for field_name in field_names:
                        if field_name in address_data:
                            setattr(nested_message, field_name, address_data[field_name])
                elif any(field in data for field in field_names):
                    # Handle individual address fields at the top level
                    nested_message = getattr(message, nested_field)
                    for field_name in field_names:
                        if field_name in data:
                            setattr(nested_message, field_name, data[field_name])
        
        # Set all other fields directly on the message
        nested_field_names = {field for fields in nested_mappings.values() for field in fields}
        nested_field_names.update(nested_mappings.keys())  # Also exclude the nested field itself
        
        for field, value in data.items():
            if field not in nested_field_names and hasattr(message, field):
                try:
                    setattr(message, field, value)
                except AttributeError as e:
                    # Some protobuf fields may not be assignable, log and skip
                    logger.debug(f"Cannot assign field {field}: {e}")
                    continue
        
        return message

    def register_schema(self, subject: str) -> int:
        """Register the protobuf schema with Schema Registry.

        Args:
            subject: Subject name for schema registration

        Returns:
            Schema ID
        """
        start_time = time.time()
        
        try:
            if not self.schema_file_path or not self.schema_file_path.exists():
                logger.warning("No schema file path provided for registration", 
                             subject=subject,
                             schema_file_path=str(self.schema_file_path) if self.schema_file_path else None)
                return -1
            
            # Read schema from file with error handling
            try:
                with open(self.schema_file_path) as f:
                    schema_str = f.read()
                logger.debug("Read schema file", 
                           schema_file=str(self.schema_file_path),
                           schema_size=len(schema_str))
            except Exception as e:
                handle_and_reraise(
                    original_error=e,
                    error_context=f"reading schema file {self.schema_file_path}",
                    suggestions=[
                        f"Verify file exists and is readable: {self.schema_file_path}",
                        "Check file permissions"
                    ]
                )
            
            # Create proper Schema object
            from confluent_kafka.schema_registry import Schema
            schema = Schema(schema_str, schema_type="PROTOBUF")
            
            # Register schema with error handling
            try:
                with PerformanceTimer(logger, "schema_registration", subject=subject):
                    schema_id = self.schema_registry.register_schema(subject, schema)
                
                # Log successful registration
                duration = time.time() - start_time
                logger.log_schema_registry_operation(
                    operation="register",
                    subject=subject,
                    success=True,
                    duration=duration,
                    schema_id=schema_id
                )
                
                logger.info(f"Registered protobuf schema for subject '{subject}' with ID {schema_id}")
                return schema_id
                
            except Exception as e:
                duration = time.time() - start_time
                logger.log_schema_registry_operation(
                    operation="register",
                    subject=subject,
                    success=False,
                    duration=duration,
                    error=str(e)
                )
                
                raise SchemaRegistrationError(
                    subject=subject,
                    schema_content=schema_str[:200] + "..." if len(schema_str) > 200 else schema_str,
                    registration_error=e
                ) from e
                
        except Exception as e:
            if not isinstance(e, (SchemaRegistrationError, SchemaNotFoundError)):
                # Re-raise as appropriate exception if not already handled
                handle_and_reraise(
                    original_error=e,
                    error_context=f"schema registration for subject {subject}",
                    suggestions=[
                        "Verify Schema Registry is accessible",
                        "Check authentication and permissions",
                        "Validate schema format and content"
                    ]
                )
            raise

    def get_latest_schema(self, subject: str) -> str:
        """Get the latest schema for a subject.

        Args:
            subject: Subject name

        Returns:
            Schema string
        """
        start_time = time.time()
        
        try:
            with PerformanceTimer(logger, "get_latest_schema", subject=subject):
                version = self.schema_registry.get_latest_version(subject)
                schema_str = version.schema.schema_str
                
            # Log successful retrieval
            duration = time.time() - start_time
            logger.log_schema_registry_operation(
                operation="get",
                subject=subject,
                success=True,
                duration=duration
            )
            
            logger.debug("Retrieved latest schema", 
                       subject=subject,
                       schema_size=len(schema_str))
            return schema_str
            
        except Exception as e:
            duration = time.time() - start_time
            logger.log_schema_registry_operation(
                operation="get",
                subject=subject,
                success=False,
                duration=duration,
                error=str(e)
            )
            
            handle_and_reraise(
                original_error=e,
                error_context=f"retrieving latest schema for subject {subject}",
                suggestions=[
                    f"Verify subject '{subject}' exists in Schema Registry",
                    "Check Schema Registry connectivity",
                    "Verify authentication and permissions"
                ]
            )

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