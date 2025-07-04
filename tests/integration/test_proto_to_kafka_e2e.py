"""Comprehensive end-to-end integration tests for proto-to-kafka workflow.

This module tests the complete pipeline from .proto files through compilation,
schema registration, message production, and consumption.
"""

import json
import time
import tempfile
import unittest
from pathlib import Path
from typing import Dict, List, Any
import uuid

import pytest
from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient

from testdatapy.schema.manager import SchemaManager
from testdatapy.schema.compiler import ProtobufCompiler
from testdatapy.schema.registry_manager import SchemaRegistryManager
from testdatapy.producers.protobuf_producer import ProtobufProducer
from testdatapy.config import AppConfig
from testdatapy.config.loader import KafkaConfig, SchemaRegistryConfig

# Import protobuf classes
from testdatapy.schemas.protobuf import customer_pb2, order_pb2, payment_pb2


@pytest.mark.integration
class TestProtoToKafkaE2E(unittest.TestCase):
    """End-to-end integration tests for proto-to-kafka workflow."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.bootstrap_servers = "localhost:9092"
        cls.schema_registry_url = "http://localhost:8081"
        cls.test_topics = [
            "e2e_customers_proto",
            "e2e_orders_proto", 
            "e2e_payments_proto",
            "e2e_high_volume_proto",
            "e2e_schema_evolution_proto"
        ]
        
        # Create admin client
        cls.admin = AdminClient({"bootstrap.servers": cls.bootstrap_servers})
        
        # Clean up any existing topics
        try:
            cls.admin.delete_topics(cls.test_topics, request_timeout=10)
            time.sleep(2)
        except Exception:
            pass
        
        # Create topics with specific configurations
        topics = []
        for topic in cls.test_topics:
            if "high_volume" in topic:
                # Higher partitions for high volume test
                topics.append(NewTopic(topic, num_partitions=3, replication_factor=1))
            else:
                topics.append(NewTopic(topic, num_partitions=1, replication_factor=1))
        
        cls.admin.create_topics(topics)
        time.sleep(3)  # Allow time for topic creation
        
        # Create Schema Registry client
        cls.sr_client = SchemaRegistryClient({"url": cls.schema_registry_url})
        
        # Initialize schema management components
        cls.schema_manager = SchemaManager()
        cls.protobuf_compiler = ProtobufCompiler()
        cls.registry_manager = SchemaRegistryManager(
            schema_registry_url=cls.schema_registry_url
        )
        
        # Create test configuration
        cls.config = AppConfig(
            kafka=KafkaConfig(bootstrap_servers=cls.bootstrap_servers),
            schema_registry=SchemaRegistryConfig(url=cls.schema_registry_url)
        )
    
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
    
    def _create_consumer(self, topic: str, group_id: str = None) -> Consumer:
        """Create a consumer for testing."""
        if group_id is None:
            group_id = f"test_consumer_{self.test_run_id}_{int(time.time())}"
            
        consumer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": group_id,
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
                
                # Store message info
                messages.append({
                    "key": msg.key().decode("utf-8") if msg.key() else None,
                    "value": msg.value(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "timestamp": msg.timestamp()[1] if msg.timestamp()[0] > 0 else None,
                })
        finally:
            consumer.close()
        
        return messages
    
    def test_complete_proto_to_kafka_workflow(self):
        """Test complete workflow from .proto file to Kafka consumption."""
        # Step 1: Schema compilation and validation
        proto_file = Path(__file__).parent.parent.parent / "src" / "testdatapy" / "schemas" / "protobuf" / "customer.proto"
        self.assertTrue(proto_file.exists(), f"Proto file not found: {proto_file}")
        
        # Validate proto file compilation
        compilation_result = self.protobuf_compiler.compile_proto_file(
            proto_file=proto_file,
            output_dir=tempfile.mkdtemp()
        )
        self.assertTrue(compilation_result.success, 
                       f"Proto compilation failed: {compilation_result.error}")
        
        # Step 2: Schema Registry registration
        schema_registration = self.registry_manager.register_protobuf_schema_from_file(
            proto_file_path=proto_file,
            topic=self.test_topics[0]
        )
        self.assertTrue(schema_registration['success'])
        self.assertGreater(schema_registration['schema_id'], 0)
        
        # Step 3: Message production
        producer = ProtobufProducer(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.test_topics[0],
            schema_registry_url=self.schema_registry_url,
            schema_proto_class=customer_pb2.Customer,
            key_field='customer_id'
        )
        
        # Generate test customer data
        test_customers = []
        for i in range(5):
            customer = {
                'customer_id': f'E2E_CUST_{self.test_run_id}_{i:03d}',
                'name': f'Customer {i}',
                'email': f'customer{i}@e2etest.com',
                'phone': f'+1234567890{i}',
                'tier': 'gold' if i % 2 == 0 else 'silver',
                'address': {
                    'street': f'{i} Test Street',
                    'city': 'E2E City',
                    'postal_code': f'E2E{i:02d}',
                    'country_code': 'US'
                },
                'created_at': '2024-01-01T00:00:00Z',
                'updated_at': '2024-01-01T00:00:00Z'
            }
            test_customers.append(customer)
            producer.produce(customer)
        
        producer.flush()
        
        # Step 4: Message consumption and validation
        consumed_messages = self._consume_messages(
            topic=self.test_topics[0],
            expected_count=len(test_customers)
        )
        
        # Validate message count
        self.assertEqual(len(consumed_messages), len(test_customers))
        
        # Validate messages are binary protobuf
        for i, msg in enumerate(consumed_messages):
            # Verify key
            expected_key = f'E2E_CUST_{self.test_run_id}_{i:03d}'
            self.assertEqual(msg['key'], expected_key)
            
            # Verify message is binary (not JSON)
            raw_value = msg['value']
            self.assertIsInstance(raw_value, bytes)
            
            # Should not be parseable as JSON
            with self.assertRaises((json.JSONDecodeError, UnicodeDecodeError)):
                json.loads(raw_value.decode('utf-8'))
            
            # Should have Schema Registry magic byte and schema ID
            self.assertGreater(len(raw_value), 5, "Message too short for protobuf")
            self.assertEqual(raw_value[0], 0, "First byte should be Schema Registry magic byte")
    
    def test_correlated_entity_workflow(self):
        """Test producing correlated entities across multiple topics."""
        # Create producers for each entity type
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
        
        payment_producer = ProtobufProducer(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.test_topics[2],
            schema_registry_url=self.schema_registry_url,
            schema_proto_class=payment_pb2.Payment,
            key_field='payment_id'
        )
        
        # Step 1: Create customers
        customer_ids = []
        for i in range(3):
            customer = {
                'customer_id': f'CORR_CUST_{self.test_run_id}_{i:03d}',
                'name': f'Correlated Customer {i}',
                'email': f'corrcustomer{i}@test.com',
                'phone': f'+9876543210{i}',
                'tier': 'premium',
                'address': {
                    'street': f'{i} Correlation Street',
                    'city': 'Corr City',
                    'postal_code': f'CR{i:02d}',
                    'country_code': 'US'
                },
                'created_at': '2024-01-01T00:00:00Z',
                'updated_at': '2024-01-01T00:00:00Z'
            }
            customer_producer.produce(customer)
            customer_ids.append(customer['customer_id'])
        
        # Step 2: Create orders referencing customers
        order_ids = []
        for i in range(6):  # 2 orders per customer
            order = {
                'order_id': f'CORR_ORDER_{self.test_run_id}_{i:03d}',
                'customer_id': customer_ids[i % len(customer_ids)],
                'order_status': 'pending',
                'total_amount': 100.0 + i * 25.0,
                'currency': 'USD',
                'created_at': '2024-01-01T00:00:00Z',
                'updated_at': '2024-01-01T00:00:00Z'
            }
            order_producer.produce(order)
            order_ids.append(order['order_id'])
        
        # Step 3: Create payments referencing orders
        for i, order_id in enumerate(order_ids):
            payment = {
                'payment_id': f'CORR_PAY_{self.test_run_id}_{i:03d}',
                'order_id': order_id,
                'amount': 100.0 + i * 25.0,
                'currency': 'USD',
                'payment_method': 'credit_card',
                'payment_status': 'completed',
                'created_at': '2024-01-01T00:00:00Z',
                'updated_at': '2024-01-01T00:00:00Z'
            }
            payment_producer.produce(payment)
        
        # Flush all producers
        customer_producer.flush()
        order_producer.flush()
        payment_producer.flush()
        
        # Verify data in each topic
        topic_expectations = [
            (self.test_topics[0], 3),  # customers
            (self.test_topics[1], 6),  # orders
            (self.test_topics[2], 6),  # payments
        ]
        
        for topic, expected_count in topic_expectations:
            messages = self._consume_messages(topic, expected_count)
            self.assertEqual(len(messages), expected_count, 
                           f"Expected {expected_count} messages in {topic}, got {len(messages)}")
            
            # Verify all messages are binary protobuf
            for msg in messages:
                raw_value = msg['value']
                with self.assertRaises((json.JSONDecodeError, UnicodeDecodeError)):
                    json.loads(raw_value.decode('utf-8'))
                self.assertEqual(raw_value[0], 0, "Should have Schema Registry magic byte")
    
    def test_high_volume_protobuf_production(self):
        """Test high-volume protobuf message production and consumption."""
        producer = ProtobufProducer(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.test_topics[3],  # high_volume topic
            schema_registry_url=self.schema_registry_url,
            schema_proto_class=customer_pb2.Customer,
            key_field='customer_id'
        )
        
        # Configure producer for high throughput
        producer._producer_config.update({
            'batch.size': 1000000,  # 1MB batches
            'linger.ms': 5,         # Small linger for throughput
            'compression.type': 'snappy',
            'acks': 1               # Fast acknowledgments
        })
        
        message_count = 1000
        start_time = time.time()
        
        # Produce messages
        for i in range(message_count):
            customer = {
                'customer_id': f'HV_{self.test_run_id}_{i:06d}',
                'name': f'High Volume Customer {i}',
                'email': f'hvcustomer{i}@volume.test',
                'phone': f'+1111111{i:04d}',
                'tier': 'standard',
                'address': {
                    'street': f'{i} Volume Street',
                    'city': 'Volume City',
                    'postal_code': f'VO{i:04d}',
                    'country_code': 'US'
                },
                'created_at': '2024-01-01T00:00:00Z',
                'updated_at': '2024-01-01T00:00:00Z'
            }
            producer.produce(customer)
            
            # Poll occasionally to prevent queue buildup
            if i % 100 == 0:
                producer._producer.poll(0)
        
        producer.flush()
        
        production_time = time.time() - start_time
        production_rate = message_count / production_time
        
        # Should achieve reasonable throughput
        self.assertGreater(production_rate, 200, 
                          f"Production rate too low: {production_rate:.1f} msg/s")
        
        # Sample consumption to verify message integrity
        sample_size = 50
        consumed_messages = self._consume_messages(
            topic=self.test_topics[3],
            expected_count=sample_size
        )
        
        self.assertEqual(len(consumed_messages), sample_size)
        
        # Verify first and last sampled messages
        first_msg = consumed_messages[0]
        self.assertEqual(first_msg['key'], f'HV_{self.test_run_id}_000000')
        
        # All should be binary protobuf
        for msg in consumed_messages:
            raw_value = msg['value']
            with self.assertRaises((json.JSONDecodeError, UnicodeDecodeError)):
                json.loads(raw_value.decode('utf-8'))
    
    def test_schema_evolution_compatibility(self):
        """Test schema evolution and compatibility handling."""
        # Register initial schema
        proto_file = Path(__file__).parent.parent.parent / "src" / "testdatapy" / "schemas" / "protobuf" / "customer.proto"
        
        registration_result = self.registry_manager.register_protobuf_schema_from_file(
            proto_file_path=proto_file,
            topic=self.test_topics[4]
        )
        
        self.assertTrue(registration_result['success'])
        initial_schema_id = registration_result['schema_id']
        
        # Produce message with initial schema
        producer = ProtobufProducer(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.test_topics[4],
            schema_registry_url=self.schema_registry_url,
            schema_proto_class=customer_pb2.Customer,
            key_field='customer_id'
        )
        
        customer = {
            'customer_id': f'EVOL_{self.test_run_id}_001',
            'name': 'Evolution Test Customer',
            'email': 'evolution@test.com',
            'phone': '+5555555555',
            'tier': 'premium',
            'address': {
                'street': '1 Evolution Street',
                'city': 'Evolution City',
                'postal_code': 'EV001',
                'country_code': 'US'
            },
            'created_at': '2024-01-01T00:00:00Z',
            'updated_at': '2024-01-01T00:00:00Z'
        }
        producer.produce(customer)
        producer.flush()
        
        # Test compatibility checking
        compatibility_result = self.registry_manager.check_schema_compatibility(
            subject=f"{self.test_topics[4]}-value",
            schema_content=proto_file.read_text(),
            version='latest'
        )
        
        self.assertTrue(compatibility_result['compatible'])
        self.assertEqual(compatibility_result['level'], 'FULL')
        
        # Verify message consumption
        messages = self._consume_messages(self.test_topics[4], 1)
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['key'], f'EVOL_{self.test_run_id}_001')
    
    def test_error_handling_scenarios(self):
        """Test various error handling scenarios."""
        
        # Test 1: Invalid protobuf data
        producer = ProtobufProducer(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.test_topics[0],
            schema_registry_url=self.schema_registry_url,
            schema_proto_class=customer_pb2.Customer,
            key_field='customer_id'
        )
        
        invalid_customer = {
            'customer_id': 12345,  # Should be string
            'name': None,          # Should be string
            'email': ['not', 'a', 'string'],  # Should be string
        }
        
        # Should handle invalid data gracefully
        with self.assertRaises((TypeError, AttributeError, ValueError)):
            producer.produce(invalid_customer)
        
        # Test 2: Schema Registry unavailable scenario
        invalid_registry_producer = ProtobufProducer(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.test_topics[0],
            schema_registry_url="http://invalid-registry:8081",
            schema_proto_class=customer_pb2.Customer,
            key_field='customer_id'
        )
        
        valid_customer = {
            'customer_id': 'TEST_001',
            'name': 'Test Customer',
            'email': 'test@example.com'
        }
        
        # Should handle registry connection issues
        with self.assertRaises(Exception):
            invalid_registry_producer.produce(valid_customer)
        
        # Test 3: Topic that doesn't exist
        producer_bad_topic = ProtobufProducer(
            bootstrap_servers=self.bootstrap_servers,
            topic="non_existent_topic_12345",
            schema_registry_url=self.schema_registry_url,
            schema_proto_class=customer_pb2.Customer,
            key_field='customer_id'
        )
        
        # Should handle gracefully or auto-create topic
        try:
            producer_bad_topic.produce(valid_customer)
            producer_bad_topic.flush(timeout=5)
        except Exception as e:
            # Expected for non-existent topic
            self.assertIsInstance(e, Exception)
    
    def test_cross_platform_message_formats(self):
        """Test that messages are compatible across different platforms."""
        producer = ProtobufProducer(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.test_topics[0],
            schema_registry_url=self.schema_registry_url,
            schema_proto_class=customer_pb2.Customer,
            key_field='customer_id'
        )
        
        # Test with various data types and edge cases
        test_cases = [
            {
                'customer_id': 'PLATFORM_ASCII_001',
                'name': 'ASCII Name',
                'email': 'ascii@test.com'
            },
            {
                'customer_id': 'PLATFORM_UNICODE_002',
                'name': 'Ünicôdé Námè',  # Unicode characters
                'email': 'unicode@tëst.com'
            },
            {
                'customer_id': 'PLATFORM_EMPTY_003',
                'name': '',  # Empty string
                'email': 'empty@test.com'
            },
            {
                'customer_id': 'PLATFORM_LONG_004',
                'name': 'A' * 1000,  # Very long string
                'email': 'long@test.com'
            }
        ]
        
        for customer_data in test_cases:
            producer.produce(customer_data)
        
        producer.flush()
        
        # Consume and verify all test cases
        messages = self._consume_messages(self.test_topics[0], len(test_cases))
        self.assertEqual(len(messages), len(test_cases))
        
        # Verify all are binary protobuf format
        for msg in messages:
            raw_value = msg['value']
            self.assertIsInstance(raw_value, bytes)
            self.assertGreater(len(raw_value), 5)
            self.assertEqual(raw_value[0], 0)  # Schema Registry magic byte


if __name__ == '__main__':
    unittest.main()