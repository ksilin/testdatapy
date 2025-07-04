"""Cross-platform compatibility tests for protobuf implementation.

This module tests compatibility across different platforms, environments,
and configurations to ensure broad compatibility and consistent behavior.
"""

import json
import os
import platform
import sys
import time
import tempfile
import unittest
from pathlib import Path
from typing import Dict, List, Any
import uuid
import struct

import pytest
from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient

from testdatapy.schema.registry_manager import SchemaRegistryManager
from testdatapy.producers.protobuf_producer import ProtobufProducer
from testdatapy.config import AppConfig
from testdatapy.config.loader import KafkaConfig, SchemaRegistryConfig

# Import protobuf classes
from testdatapy.schemas.protobuf import customer_pb2, order_pb2


@pytest.mark.integration
class TestCrossPlatformCompatibility(unittest.TestCase):
    """Cross-platform compatibility tests for protobuf implementation."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.bootstrap_servers = "localhost:9092"
        cls.schema_registry_url = "http://localhost:8081"
        cls.test_topics = [
            "compat_serialization",
            "compat_unicode",
            "compat_performance",
            "compat_endianness",
            "compat_configs"
        ]
        
        # Create admin client
        cls.admin = AdminClient({"bootstrap.servers": cls.bootstrap_servers})
        
        # Clean up any existing topics
        try:
            cls.admin.delete_topics(cls.test_topics, request_timeout=10)
            time.sleep(2)
        except Exception:
            pass
        
        # Create topics
        topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in cls.test_topics]
        cls.admin.create_topics(topics)
        time.sleep(3)
        
        # Initialize components
        cls.sr_client = SchemaRegistryClient({"url": cls.schema_registry_url})
        cls.registry_manager = SchemaRegistryManager(
            schema_registry_url=cls.schema_registry_url
        )
        
        # Platform information
        cls.platform_info = {
            "system": platform.system(),
            "release": platform.release(),
            "version": platform.version(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "python_version": sys.version,
            "python_implementation": platform.python_implementation(),
            "endianness": sys.byteorder
        }
        
        print(f"Running tests on: {cls.platform_info}")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        try:
            cls.admin.delete_topics(cls.test_topics, request_timeout=10)
        except Exception:
            pass
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_run_id = str(uuid.uuid4())[:8]
        
    def tearDown(self):
        """Clean up test fixtures."""
        pass
    
    def _create_consumer(self, topic: str) -> Consumer:
        """Create a consumer for testing."""
        consumer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": f"compat_test_{self.test_run_id}_{int(time.time())}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        
        consumer = Consumer(consumer_config)
        consumer.subscribe([topic])
        return consumer
    
    def _consume_messages(self, topic: str, expected_count: int, timeout: float = 30.0) -> List[Dict[str, Any]]:
        """Consume messages from a topic and return them."""
        consumer = self._create_consumer(topic)
        messages = []
        start_time = time.time()
        
        try:
            while len(messages) < expected_count and time.time() - start_time < timeout:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise Exception(f"Consumer error: {msg.error()}")
                
                messages.append({
                    "key": msg.key().decode("utf-8") if msg.key() else None,
                    "value": msg.value(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "timestamp": msg.timestamp()[1] if msg.timestamp()[0] > 0 else None,
                    "headers": dict(msg.headers()) if msg.headers() else {}
                })
        finally:
            consumer.close()
        
        return messages
    
    def test_message_serialization_consistency(self):
        """Test that message serialization is consistent across different calls."""
        producer = ProtobufProducer(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.test_topics[0],
            schema_registry_url=self.schema_registry_url,
            schema_proto_class=customer_pb2.Customer,
            key_field='customer_id'
        )
        
        # Create test data with various data types
        test_customer = {
            'customer_id': f'SERIAL_{self.test_run_id}_001',
            'name': 'Serialization Test Customer',
            'email': 'serial@compat.test',
            'phone': '+1234567890',
            'tier': 'platinum',
            'address': {
                'street': '123 Serialization St',
                'city': 'Compat City',
                'postal_code': '12345',
                'country_code': 'US'
            },
            'created_at': '2024-01-01T00:00:00Z',
            'updated_at': '2024-01-01T00:00:00Z'
        }
        
        # Produce the same message multiple times
        for i in range(5):
            # Modify customer_id to make each message unique
            customer_copy = test_customer.copy()
            customer_copy['customer_id'] = f'SERIAL_{self.test_run_id}_{i:03d}'
            producer.produce(customer_copy)
        
        producer.flush()
        
        # Consume all messages
        messages = self._consume_messages(self.test_topics[0], 5)
        self.assertEqual(len(messages), 5)
        
        # Extract just the protobuf content (skip Schema Registry headers)
        protobuf_contents = []
        for msg in messages:
            raw_value = msg['value']
            # Skip Schema Registry magic byte (1 byte) + schema ID (4 bytes)
            protobuf_content = raw_value[5:]
            protobuf_contents.append(protobuf_content)
        
        # Parse protobuf messages and verify consistency
        parsed_customers = []
        for protobuf_content in protobuf_contents:
            customer = customer_pb2.Customer()
            customer.ParseFromString(protobuf_content)
            parsed_customers.append(customer)
        
        # Verify all have consistent structure (excluding customer_id)
        for customer in parsed_customers:
            self.assertEqual(customer.name, 'Serialization Test Customer')
            self.assertEqual(customer.email, 'serial@compat.test')
            self.assertEqual(customer.tier, 'platinum')
            self.assertEqual(customer.address.street, '123 Serialization St')
            self.assertEqual(customer.address.city, 'Compat City')
    
    def test_unicode_and_encoding_compatibility(self):
        """Test unicode and character encoding compatibility."""
        producer = ProtobufProducer(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.test_topics[1],
            schema_registry_url=self.schema_registry_url,
            schema_proto_class=customer_pb2.Customer,
            key_field='customer_id'
        )
        
        # Test various unicode characters and encodings
        unicode_test_cases = [
            {
                'customer_id': f'UNICODE_{self.test_run_id}_ASCII',
                'name': 'Plain ASCII Name',
                'email': 'ascii@test.com',
                'description': 'ASCII characters only'
            },
            {
                'customer_id': f'UNICODE_{self.test_run_id}_LATIN',
                'name': 'Café René François',  # Latin characters with accents
                'email': 'café@françois.com',
                'description': 'Latin characters with accents'
            },
            {
                'customer_id': f'UNICODE_{self.test_run_id}_CYRILLIC',
                'name': 'Александр Иванов',  # Cyrillic
                'email': 'александр@иванов.com',
                'description': 'Cyrillic characters'
            },
            {
                'customer_id': f'UNICODE_{self.test_run_id}_CJK',
                'name': '田中太郎',  # Japanese
                'email': '田中@example.com',
                'description': 'CJK characters'
            },
            {
                'customer_id': f'UNICODE_{self.test_run_id}_EMOJI',
                'name': 'Customer 😀🎉🚀',  # Emoji
                'email': 'emoji@🌟.test',
                'description': 'Emoji characters'
            },
            {
                'customer_id': f'UNICODE_{self.test_run_id}_MIXED',
                'name': 'Mixed: ABC-ДЕЖ-田中-😀',  # Mixed scripts
                'email': 'mixed@multi-script.test',
                'description': 'Mixed character scripts'
            }
        ]
        
        # Produce messages with various unicode content
        for test_case in unicode_test_cases:
            customer = {
                'customer_id': test_case['customer_id'],
                'name': test_case['name'],
                'email': test_case['email'],
                'tier': 'unicode',
                'created_at': '2024-01-01T00:00:00Z',
                'updated_at': '2024-01-01T00:00:00Z'
            }
            producer.produce(customer)
        
        producer.flush()
        
        # Consume and verify unicode handling
        messages = self._consume_messages(self.test_topics[1], len(unicode_test_cases))
        self.assertEqual(len(messages), len(unicode_test_cases))
        
        # Parse and verify unicode content preservation
        for i, msg in enumerate(messages):
            raw_value = msg['value']
            protobuf_content = raw_value[5:]  # Skip Schema Registry headers
            
            customer = customer_pb2.Customer()
            customer.ParseFromString(protobuf_content)
            
            # Verify unicode content is preserved
            expected_case = unicode_test_cases[i]
            self.assertEqual(customer.customer_id, expected_case['customer_id'])
            self.assertEqual(customer.name, expected_case['name'])
            self.assertEqual(customer.email, expected_case['email'])
            
            # Verify round-trip encoding
            self.assertIsInstance(customer.name, str)
            self.assertIsInstance(customer.email, str)
    
    def test_binary_compatibility_and_endianness(self):
        """Test binary compatibility and endianness handling."""
        producer = ProtobufProducer(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.test_topics[3],
            schema_registry_url=self.schema_registry_url,
            schema_proto_class=customer_pb2.Customer,
            key_field='customer_id'
        )
        
        # Test with various numeric values that could be affected by endianness
        customer = {
            'customer_id': f'ENDIAN_{self.test_run_id}_001',
            'name': f'Endianness Test {sys.byteorder}',
            'email': f'endian@{sys.byteorder}.test',
            'tier': 'binary_test',
            'created_at': '2024-01-01T00:00:00Z',
            'updated_at': '2024-01-01T00:00:00Z'
        }
        
        producer.produce(customer)
        producer.flush()
        
        # Consume and analyze binary structure
        messages = self._consume_messages(self.test_topics[3], 1)
        self.assertEqual(len(messages), 1)
        
        raw_value = messages[0]['value']
        
        # Verify Schema Registry magic byte and schema ID structure
        self.assertEqual(raw_value[0], 0, "Schema Registry magic byte should be 0")
        
        # Extract schema ID (4 bytes, big-endian)
        schema_id_bytes = raw_value[1:5]
        schema_id = struct.unpack('>I', schema_id_bytes)[0]  # Big-endian uint32
        self.assertGreater(schema_id, 0, "Schema ID should be positive")
        
        # Verify protobuf content
        protobuf_content = raw_value[5:]
        customer_parsed = customer_pb2.Customer()
        customer_parsed.ParseFromString(protobuf_content)
        
        self.assertEqual(customer_parsed.customer_id, f'ENDIAN_{self.test_run_id}_001')
        self.assertIn(sys.byteorder, customer_parsed.name)
    
    def test_different_kafka_configurations(self):
        """Test compatibility with different Kafka configurations."""
        base_config = {"bootstrap.servers": self.bootstrap_servers}
        
        # Test different producer configurations
        config_variations = [
            # Standard configuration
            {
                **base_config,
                "acks": "all",
                "retries": 3,
                "batch.size": 16384,
                "linger.ms": 1,
                "buffer.memory": 33554432
            },
            # High throughput configuration
            {
                **base_config,
                "acks": "1",
                "retries": 0,
                "batch.size": 1000000,
                "linger.ms": 5,
                "compression.type": "snappy"
            },
            # Low latency configuration
            {
                **base_config,
                "acks": "1",
                "retries": 0,
                "batch.size": 1,
                "linger.ms": 0,
                "compression.type": "none"
            }
        ]
        
        for i, config in enumerate(config_variations):
            # Create producer with specific configuration
            producer = ProtobufProducer(
                bootstrap_servers=self.bootstrap_servers,
                topic=self.test_topics[4],
                schema_registry_url=self.schema_registry_url,
                schema_proto_class=customer_pb2.Customer,
                key_field='customer_id',
                producer_config=config
            )
            
            customer = {
                'customer_id': f'CONFIG_{self.test_run_id}_{i:03d}',
                'name': f'Config Test Customer {i}',
                'email': f'config{i}@test.com',
                'tier': 'config_test',
                'created_at': '2024-01-01T00:00:00Z',
                'updated_at': '2024-01-01T00:00:00Z'
            }
            
            producer.produce(customer)
            producer.flush()
        
        # Consume all messages and verify they're all valid
        messages = self._consume_messages(self.test_topics[4], len(config_variations))
        self.assertEqual(len(messages), len(config_variations))
        
        # Verify all messages are valid protobuf
        for i, msg in enumerate(messages):
            raw_value = msg['value']
            protobuf_content = raw_value[5:]
            
            customer = customer_pb2.Customer()
            customer.ParseFromString(protobuf_content)
            
            self.assertEqual(customer.customer_id, f'CONFIG_{self.test_run_id}_{i:03d}')
            self.assertEqual(customer.tier, 'config_test')
    
    def test_performance_characteristics(self):
        """Test performance characteristics across different configurations."""
        import time
        
        # Test small vs large messages
        performance_results = {}
        
        # Small message test
        small_producer = ProtobufProducer(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.test_topics[2],
            schema_registry_url=self.schema_registry_url,
            schema_proto_class=customer_pb2.Customer,
            key_field='customer_id'
        )
        
        small_customer = {
            'customer_id': f'PERF_SMALL_{self.test_run_id}',
            'name': 'S',
            'email': 's@t.co'
        }
        
        # Measure small message performance
        message_count = 100
        start_time = time.time()
        
        for i in range(message_count):
            customer = small_customer.copy()
            customer['customer_id'] = f'PERF_SMALL_{self.test_run_id}_{i:03d}'
            small_producer.produce(customer)
        
        small_producer.flush()
        small_duration = time.time() - start_time
        performance_results['small_messages'] = {
            'count': message_count,
            'duration': small_duration,
            'rate': message_count / small_duration
        }
        
        # Large message test
        large_customer = {
            'customer_id': f'PERF_LARGE_{self.test_run_id}',
            'name': 'Large Customer Name ' * 100,  # ~2KB name
            'email': 'large.customer@very-long-domain-name-for-testing.com',
            'phone': '+1234567890' * 10,
            'tier': 'premium_large_data_tier',
            'address': {
                'street': '123 Very Long Street Name That Goes On And On ' * 10,
                'city': 'Very Long City Name ' * 20,
                'postal_code': 'LONGCODE123456',
                'country_code': 'US'
            },
            'created_at': '2024-01-01T00:00:00Z',
            'updated_at': '2024-01-01T00:00:00Z'
        }
        
        start_time = time.time()
        
        for i in range(message_count):
            customer = large_customer.copy()
            customer['customer_id'] = f'PERF_LARGE_{self.test_run_id}_{i:03d}'
            small_producer.produce(customer)  # Reuse producer
        
        small_producer.flush()
        large_duration = time.time() - start_time
        performance_results['large_messages'] = {
            'count': message_count,
            'duration': large_duration,
            'rate': message_count / large_duration
        }
        
        # Verify reasonable performance
        self.assertGreater(performance_results['small_messages']['rate'], 50,
                          f"Small message rate too low: {performance_results['small_messages']['rate']:.1f} msg/s")
        self.assertGreater(performance_results['large_messages']['rate'], 20,
                          f"Large message rate too low: {performance_results['large_messages']['rate']:.1f} msg/s")
        
        # Log performance results
        print(f"Performance results: {performance_results}")
        
        # Verify messages were produced correctly
        small_messages = self._consume_messages(self.test_topics[2], message_count * 2)  # Both small and large
        self.assertEqual(len(small_messages), message_count * 2)
    
    def test_environment_variable_configurations(self):
        """Test compatibility with environment variable configurations."""
        # Test different environment configurations
        original_env = os.environ.copy()
        
        try:
            # Test 1: UTF-8 locale
            os.environ['LANG'] = 'en_US.UTF-8'
            os.environ['LC_ALL'] = 'en_US.UTF-8'
            
            producer = ProtobufProducer(
                bootstrap_servers=self.bootstrap_servers,
                topic=self.test_topics[0],
                schema_registry_url=self.schema_registry_url,
                schema_proto_class=customer_pb2.Customer,
                key_field='customer_id'
            )
            
            customer = {
                'customer_id': f'ENV_UTF8_{self.test_run_id}',
                'name': 'Environment Test Üser',
                'email': 'env@tëst.com',
                'tier': 'env_test'
            }
            
            producer.produce(customer)
            producer.flush()
            
            # Verify message was produced successfully
            messages = self._consume_messages(self.test_topics[0], 1)
            self.assertEqual(len(messages), 1)
            
            raw_value = messages[0]['value']
            protobuf_content = raw_value[5:]
            
            parsed_customer = customer_pb2.Customer()
            parsed_customer.ParseFromString(protobuf_content)
            
            self.assertEqual(parsed_customer.name, 'Environment Test Üser')
            self.assertEqual(parsed_customer.email, 'env@tëst.com')
            
        finally:
            # Restore original environment
            os.environ.clear()
            os.environ.update(original_env)
    
    def test_cross_schema_compatibility(self):
        """Test compatibility between different schema types in the same application."""
        # Test Customer and Order schemas together
        customer_producer = ProtobufProducer(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.test_topics[0],
            schema_registry_url=self.schema_registry_url,
            schema_proto_class=customer_pb2.Customer,
            key_field='customer_id'
        )
        
        order_producer = ProtobufProducer(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.test_topics[1],
            schema_registry_url=self.schema_registry_url,
            schema_proto_class=order_pb2.Order,
            key_field='order_id'
        )
        
        # Create related customer and order
        customer_id = f'CROSS_{self.test_run_id}_CUSTOMER'
        order_id = f'CROSS_{self.test_run_id}_ORDER'
        
        customer = {
            'customer_id': customer_id,
            'name': 'Cross Schema Test Customer',
            'email': 'cross@schema.test',
            'tier': 'cross_test'
        }
        
        order = {
            'order_id': order_id,
            'customer_id': customer_id,
            'order_status': 'pending',
            'total_amount': 99.99,
            'currency': 'USD'
        }
        
        # Produce both messages
        customer_producer.produce(customer)
        order_producer.produce(order)
        
        customer_producer.flush()
        order_producer.flush()
        
        # Verify both schemas work correctly
        customer_messages = self._consume_messages(self.test_topics[0], 1)
        order_messages = self._consume_messages(self.test_topics[1], 1)
        
        self.assertEqual(len(customer_messages), 1)
        self.assertEqual(len(order_messages), 1)
        
        # Parse and verify cross-references
        customer_protobuf = customer_messages[0]['value'][5:]
        order_protobuf = order_messages[0]['value'][5:]
        
        parsed_customer = customer_pb2.Customer()
        parsed_customer.ParseFromString(customer_protobuf)
        
        parsed_order = order_pb2.Order()
        parsed_order.ParseFromString(order_protobuf)
        
        # Verify relationship
        self.assertEqual(parsed_customer.customer_id, customer_id)
        self.assertEqual(parsed_order.customer_id, customer_id)
        self.assertEqual(parsed_order.order_id, order_id)
    
    def test_platform_specific_characteristics(self):
        """Test platform-specific characteristics and edge cases."""
        # Log platform information for debugging
        platform_info = {
            "system": platform.system(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "python_version": sys.version_info,
            "endianness": sys.byteorder,
            "max_int": sys.maxsize,
            "float_info": sys.float_info
        }
        
        producer = ProtobufProducer(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.test_topics[0],
            schema_registry_url=self.schema_registry_url,
            schema_proto_class=customer_pb2.Customer,
            key_field='customer_id'
        )
        
        # Test with platform-specific data
        customer = {
            'customer_id': f'PLATFORM_{self.test_run_id}_{platform.system()}',
            'name': f'Platform Test on {platform.system()}',
            'email': f'platform@{platform.system().lower()}.test',
            'tier': f'{platform.machine()}_test',
            'created_at': '2024-01-01T00:00:00Z',
            'updated_at': '2024-01-01T00:00:00Z'
        }
        
        producer.produce(customer)
        producer.flush()
        
        # Verify message production and consumption
        messages = self._consume_messages(self.test_topics[0], 1)
        self.assertEqual(len(messages), 1)
        
        raw_value = messages[0]['value']
        protobuf_content = raw_value[5:]
        
        parsed_customer = customer_pb2.Customer()
        parsed_customer.ParseFromString(protobuf_content)
        
        self.assertIn(platform.system(), parsed_customer.customer_id)
        self.assertIn(platform.system(), parsed_customer.name)
        
        # Store results with platform information for analysis
        test_result = {
            "platform_info": platform_info,
            "message_size": len(raw_value),
            "protobuf_size": len(protobuf_content),
            "test_status": "passed"
        }
        
        print(f"Platform test result: {test_result}")


if __name__ == '__main__':
    unittest.main()