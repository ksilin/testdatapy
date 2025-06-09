"""Utilities for validating protobuf data."""
import json
import struct
from typing import Any, Optional, Tuple, Dict, List
from pathlib import Path

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from google.protobuf.message import Message


class ProtobufValidator:
    """Validates protobuf messages in Kafka topics."""
    
    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: Optional[str] = None
    ):
        """Initialize the validator.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            schema_registry_url: Schema Registry URL (optional)
        """
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        
        if schema_registry_url:
            self.sr_client = SchemaRegistryClient({"url": schema_registry_url})
        else:
            self.sr_client = None
    
    def validate_topic_messages(
        self,
        topic: str,
        proto_class: type,
        sample_size: int = 100,
        consumer_group: Optional[str] = None
    ) -> Dict[str, Any]:
        """Validate messages in a topic are valid protobuf.
        
        Args:
            topic: Topic to validate
            proto_class: Protobuf message class
            sample_size: Number of messages to sample
            consumer_group: Consumer group ID (auto-generated if None)
            
        Returns:
            Validation results dictionary
        """
        if not consumer_group:
            consumer_group = f"protobuf_validator_{topic}"
        
        consumer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False
        }
        
        consumer = Consumer(consumer_config)
        consumer.subscribe([topic])
        
        deserializer = ProtobufDeserializer(proto_class, {'use.deprecated.format': False})
        
        results = {
            "topic": topic,
            "total_messages": 0,
            "valid_protobuf": 0,
            "json_messages": 0,
            "invalid_messages": 0,
            "empty_messages": 0,
            "errors": [],
            "message_samples": []
        }
        
        messages_processed = 0
        
        try:
            while messages_processed < sample_size:
                msg = consumer.poll(timeout=5.0)
                
                if msg is None:
                    break
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        break
                    results["errors"].append(f"Consumer error: {msg.error()}")
                    continue
                
                results["total_messages"] += 1
                messages_processed += 1
                
                # Check for empty message
                if not msg.value():
                    results["empty_messages"] += 1
                    continue
                
                # Analyze message format
                value = msg.value()
                is_json, is_protobuf, error = self._analyze_message_format(
                    value, proto_class, deserializer
                )
                
                if is_json:
                    results["json_messages"] += 1
                    results["errors"].append(
                        f"Message at offset {msg.offset()} is JSON, not protobuf"
                    )
                elif is_protobuf:
                    results["valid_protobuf"] += 1
                    # Add sample of valid message
                    if len(results["message_samples"]) < 5:
                        try:
                            proto_msg = deserializer(value, None)
                            results["message_samples"].append({
                                "offset": msg.offset(),
                                "key": msg.key().decode('utf-8') if msg.key() else None,
                                "size_bytes": len(value),
                                "sample_fields": self._extract_sample_fields(proto_msg)
                            })
                        except Exception:
                            pass
                else:
                    results["invalid_messages"] += 1
                    if error:
                        results["errors"].append(
                            f"Message at offset {msg.offset()}: {error}"
                        )
        
        finally:
            consumer.close()
        
        # Calculate percentages
        if results["total_messages"] > 0:
            results["valid_percentage"] = (
                results["valid_protobuf"] / results["total_messages"] * 100
            )
            results["json_percentage"] = (
                results["json_messages"] / results["total_messages"] * 100
            )
        else:
            results["valid_percentage"] = 0
            results["json_percentage"] = 0
        
        return results
    
    def _analyze_message_format(
        self,
        value: bytes,
        proto_class: type,
        deserializer: ProtobufDeserializer
    ) -> Tuple[bool, bool, Optional[str]]:
        """Analyze whether a message is JSON or protobuf.
        
        Returns:
            Tuple of (is_json, is_protobuf, error_message)
        """
        # Check if it's JSON
        try:
            json_data = json.loads(value.decode('utf-8'))
            # Additional check - if it has expected JSON structure
            if isinstance(json_data, dict):
                return True, False, None
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass
        
        # Check if it's valid protobuf
        try:
            proto_msg = deserializer(value, None)
            # Verify it's a valid protobuf message
            if isinstance(proto_msg, Message):
                return False, True, None
        except Exception as e:
            return False, False, str(e)
        
        return False, False, "Unknown format"
    
    def _extract_sample_fields(self, proto_msg: Message) -> Dict[str, Any]:
        """Extract sample fields from a protobuf message."""
        sample = {}
        
        # Get first few fields
        for field, value in proto_msg.ListFields():
            field_name = field.name
            
            # Handle different field types
            if field.type == field.TYPE_MESSAGE:
                sample[field_name] = "<nested_message>"
            elif field.label == field.LABEL_REPEATED:
                sample[field_name] = f"<repeated: {len(value)} items>"
            else:
                sample[field_name] = str(value)[:50]  # Truncate long values
            
            if len(sample) >= 5:  # Limit to 5 fields
                break
        
        return sample
    
    def compare_topics(
        self,
        topics: List[str],
        proto_classes: Dict[str, type],
        sample_size: int = 100
    ) -> Dict[str, Any]:
        """Compare multiple topics for protobuf compliance.
        
        Args:
            topics: List of topics to compare
            proto_classes: Map of topic to protobuf class
            sample_size: Messages to sample per topic
            
        Returns:
            Comparison results
        """
        results = {
            "topics": {},
            "summary": {
                "all_protobuf": True,
                "total_json_messages": 0,
                "total_valid_messages": 0,
                "total_messages": 0
            }
        }
        
        for topic in topics:
            proto_class = proto_classes.get(topic)
            if not proto_class:
                results["topics"][topic] = {"error": "No protobuf class provided"}
                continue
            
            topic_results = self.validate_topic_messages(
                topic, proto_class, sample_size
            )
            results["topics"][topic] = topic_results
            
            # Update summary
            results["summary"]["total_messages"] += topic_results["total_messages"]
            results["summary"]["total_valid_messages"] += topic_results["valid_protobuf"]
            results["summary"]["total_json_messages"] += topic_results["json_messages"]
            
            if topic_results["json_messages"] > 0:
                results["summary"]["all_protobuf"] = False
        
        return results
    
    def validate_schema_registry_subject(
        self,
        subject: str
    ) -> Dict[str, Any]:
        """Validate a schema in Schema Registry is protobuf.
        
        Args:
            subject: Schema Registry subject
            
        Returns:
            Validation results
        """
        if not self.sr_client:
            return {"error": "Schema Registry client not configured"}
        
        try:
            latest = self.sr_client.get_latest_version(subject)
            schema = latest.schema
            
            result = {
                "subject": subject,
                "version": latest.version,
                "schema_id": latest.schema_id,
                "schema_type": schema.schema_type,
                "is_protobuf": schema.schema_type == "PROTOBUF",
                "schema_preview": schema.schema_str[:200] + "..." if len(schema.schema_str) > 200 else schema.schema_str
            }
            
            return result
            
        except Exception as e:
            return {
                "subject": subject,
                "error": str(e)
            }


def validate_protobuf_production(
    bootstrap_servers: str,
    topics: List[str],
    proto_classes: Dict[str, type],
    schema_registry_url: Optional[str] = None,
    sample_size: int = 100
) -> None:
    """Validate protobuf production across multiple topics.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
        topics: List of topics to validate
        proto_classes: Map of topic to protobuf class
        schema_registry_url: Schema Registry URL
        sample_size: Messages to sample per topic
    """
    validator = ProtobufValidator(bootstrap_servers, schema_registry_url)
    
    print("Protobuf Production Validation Report")
    print("=" * 50)
    
    # Validate each topic
    for topic in topics:
        proto_class = proto_classes.get(topic)
        if not proto_class:
            print(f"\n❌ {topic}: No protobuf class provided")
            continue
        
        print(f"\nValidating topic: {topic}")
        results = validator.validate_topic_messages(topic, proto_class, sample_size)
        
        print(f"  Total messages sampled: {results['total_messages']}")
        print(f"  Valid protobuf: {results['valid_protobuf']} ({results['valid_percentage']:.1f}%)")
        print(f"  JSON messages: {results['json_messages']} ({results['json_percentage']:.1f}%)")
        print(f"  Invalid messages: {results['invalid_messages']}")
        
        if results['valid_percentage'] == 100:
            print(f"  ✅ All messages are valid protobuf!")
        else:
            print(f"  ❌ Found non-protobuf messages")
            
        if results['errors']:
            print(f"  Errors:")
            for error in results['errors'][:5]:  # Show first 5 errors
                print(f"    - {error}")
        
        if results['message_samples']:
            print(f"  Sample message:")
            sample = results['message_samples'][0]
            print(f"    Offset: {sample['offset']}")
            print(f"    Size: {sample['size_bytes']} bytes")
            print(f"    Fields: {sample['sample_fields']}")
    
    # Validate Schema Registry if configured
    if schema_registry_url:
        print("\nSchema Registry Validation")
        print("-" * 30)
        
        for topic in topics:
            subject = f"{topic}-value"
            result = validator.validate_schema_registry_subject(subject)
            
            if "error" in result:
                print(f"  {subject}: ❌ {result['error']}")
            else:
                if result['is_protobuf']:
                    print(f"  {subject}: ✅ PROTOBUF (v{result['version']}, ID: {result['schema_id']})")
                else:
                    print(f"  {subject}: ❌ {result['schema_type']} (expected PROTOBUF)")


if __name__ == "__main__":
    # Example usage
    from testdatapy.schemas.protobuf import customer_pb2, order_pb2, payment_pb2
    
    validate_protobuf_production(
        bootstrap_servers="localhost:9092",
        topics=["customers", "orders", "payments"],
        proto_classes={
            "customers": customer_pb2.Customer,
            "orders": order_pb2.Order,
            "payments": payment_pb2.Payment
        },
        schema_registry_url="http://localhost:8081",
        sample_size=50
    )