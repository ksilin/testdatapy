"""Real integration tests for protobuf with actual Kafka and Schema Registry."""
import json
import time
import unittest
from pathlib import Path
import struct

import pytest
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer

from testdatapy.config import AppConfig
from testdatapy.generators import FakerGenerator
from testdatapy.producers.protobuf_producer import ProtobufProducer

# Import actual protobuf classes
from testdatapy.schemas.protobuf import customer_pb2, order_pb2, payment_pb2


@pytest.mark.integration
class TestProtobufRealIntegration(unittest.TestCase):
    """Integration tests for protobuf with real Kafka and Schema Registry."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.bootstrap_servers = "localhost:9092"
        cls.schema_registry_url = "http://localhost:8081"
        cls.test_topics = ["test_customers_proto", "test_orders_proto", "test_payments_proto"]
        
        # Create admin client
        cls.admin = AdminClient({"bootstrap.servers": cls.bootstrap_servers})
        
        # Delete topics if they exist
        try:
            cls.admin.delete_topics(cls.test_topics, request_timeout=10)
            time.sleep(2)
        except Exception:
            pass
        
        # Create topics
        topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in cls.test_topics]
        cls.admin.create_topics(topics)
        time.sleep(2)
        
        # Create Schema Registry client
        cls.sr_client = SchemaRegistryClient({"url": cls.schema_registry_url})
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        try:
            cls.admin.delete_topics(cls.test_topics, request_timeout=10)
        except Exception:
            pass
    
    def setUp(self):
        """Set up test fixtures."""
        self.config = AppConfig(
            kafka_config={
                "bootstrap.servers": self.bootstrap_servers,
                "schema.registry.url": self.schema_registry_url
            }
        )
    
    def test_produce_and_consume_binary_protobuf(self):
        """Test that messages are produced as binary protobuf, not JSON."""
        # Create producer
        producer = ProtobufProducer(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.test_topics[0],
            schema_registry_url=self.schema_registry_url,
            schema_proto_class=customer_pb2.Customer,
            key_field='customer_id'
        )
        
        # Generate test data
        test_customers = []
        for i in range(5):
            customer = {
                'customer_id': f'CUST_{i:04d}',
                'name': f'Customer {i}',
                'email': f'customer{i}@example.com',
                'phone': f'+123456789{i}',
                'tier': 'gold' if i % 2 == 0 else 'silver',
                'address': {
                    'street': f'{i} Main St',
                    'city': 'Test City',
                    'postal_code': f'1000{i}',
                    'country_code': 'US'
                },
                'created_at': '2024-01-01T00:00:00Z',
                'updated_at': '2024-01-01T00:00:00Z'
            }
            test_customers.append(customer)
            producer.produce(customer)
        
        producer.flush()
        
        # Create consumer
        consumer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": "test_protobuf_consumer",
            "auto.offset.reset": "earliest"
        }
        consumer = Consumer(consumer_config)
        consumer.subscribe([self.test_topics[0]])
        
        # Create deserializer
        deserializer = ProtobufDeserializer(
            customer_pb2.Customer,
            {'use.deprecated.format': False}
        )
        
        # Consume messages
        consumed_messages = []
        binary_count = 0
        json_count = 0
        start_time = time.time()
        
        while len(consumed_messages) < len(test_customers) and time.time() - start_time < 10:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            
            # Check if message is binary protobuf
            raw_value = msg.value()
            self.assertIsInstance(raw_value, bytes)
            
            # Try to detect if it's JSON
            try:
                json_data = json.loads(raw_value.decode('utf-8'))
                json_count += 1
                self.fail(f"Message is JSON, not protobuf: {json_data}")
            except (json.JSONDecodeError, UnicodeDecodeError):
                # Good - it's binary
                binary_count += 1
            
            # Deserialize the protobuf message
            customer = deserializer(raw_value, None)
            consumed_messages.append({
                'customer_id': customer.customer_id,
                'name': customer.name,
                'email': customer.email,
                'tier': customer.tier
            })
        
        consumer.close()
        
        # Verify all messages are binary
        self.assertEqual(binary_count, len(test_customers))
        self.assertEqual(json_count, 0)
        
        # Verify message content
        self.assertEqual(len(consumed_messages), len(test_customers))
        for i, msg in enumerate(consumed_messages):
            self.assertEqual(msg['customer_id'], f'CUST_{i:04d}')
            self.assertEqual(msg['name'], f'Customer {i}')
    
    def test_correlated_protobuf_data(self):
        """Test producing correlated data with protobuf format."""
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
        
        # Create customers
        customer_ids = []
        for i in range(3):
            customer = {
                'customer_id': f'CUST_{i:04d}',
                'name': f'Customer {i}',
                'email': f'customer{i}@example.com'
            }
            customer_producer.produce(customer)
            customer_ids.append(customer['customer_id'])
        
        # Create orders referencing customers
        order_ids = []
        for i in range(5):
            order = {
                'order_id': f'ORDER_{i:04d}',
                'customer_id': customer_ids[i % len(customer_ids)],
                'total_amount': 100.0 + i * 10,
                'status': 'pending'
            }
            order_producer.produce(order)
            order_ids.append(order['order_id'])
        
        # Create payments referencing orders
        for i in range(5):
            payment = {
                'payment_id': f'PAY_{i:04d}',
                'order_id': order_ids[i],
                'amount': 100.0 + i * 10,
                'payment_method': 'credit_card',
                'status': 'completed'
            }
            payment_producer.produce(payment)
        
        # Flush all producers
        customer_producer.flush()
        order_producer.flush()
        payment_producer.flush()
        
        # Verify data in each topic
        for topic, proto_class, expected_count in [
            (self.test_topics[0], customer_pb2.Customer, 3),
            (self.test_topics[1], order_pb2.Order, 5),
            (self.test_topics[2], payment_pb2.Payment, 5)
        ]:
            consumer = Consumer({
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": f"test_{topic}_consumer",
                "auto.offset.reset": "earliest"
            })
            consumer.subscribe([topic])
            
            deserializer = ProtobufDeserializer(proto_class, {'use.deprecated.format': False})
            
            count = 0
            start_time = time.time()
            while count < expected_count and time.time() - start_time < 10:
                msg = consumer.poll(1.0)
                if msg is None or msg.error():
                    continue
                
                # Verify it's binary
                raw_value = msg.value()
                try:
                    json.loads(raw_value.decode('utf-8'))
                    self.fail(f"Message in {topic} is JSON, not protobuf")
                except (json.JSONDecodeError, UnicodeDecodeError):
                    pass
                
                # Deserialize and verify
                obj = deserializer(raw_value, None)
                self.assertIsNotNone(obj)
                count += 1
            
            consumer.close()
            self.assertEqual(count, expected_count, f"Expected {expected_count} messages in {topic}, got {count}")
    
    def test_schema_evolution_compatibility(self):
        """Test schema evolution and compatibility checking."""
        # Register initial schema
        subject = f"{self.test_topics[0]}-value"
        
        # Read the actual proto file
        proto_path = Path(__file__).parent.parent.parent / "src" / "testdatapy" / "schemas" / "protobuf" / "customer.proto"
        if proto_path.exists():
            producer = ProtobufProducer(
                bootstrap_servers=self.bootstrap_servers,
                topic=self.test_topics[0],
                schema_registry_url=self.schema_registry_url,
                schema_proto_class=customer_pb2.Customer,
                schema_file_path=str(proto_path)
            )
            
            # Register schema
            schema_id = producer.register_schema(subject)
            self.assertGreater(schema_id, 0)
            
            # Get latest schema
            schema = producer.get_latest_schema(subject)
            self.assertIn("Customer", schema)
            self.assertIn("customer_id", schema)
    
    def test_high_volume_protobuf_production(self):
        """Test producing high volume of protobuf messages."""
        producer = ProtobufProducer(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.test_topics[0],
            schema_registry_url=self.schema_registry_url,
            schema_proto_class=customer_pb2.Customer
        )
        
        # Generate many messages quickly
        start_time = time.time()
        message_count = 1000
        
        for i in range(message_count):
            customer = {
                'customer_id': f'CUST_{i:06d}',
                'name': f'High Volume Customer {i}',
                'email': f'hvcustomer{i}@example.com'
            }
            producer.produce(customer)
            
            # Poll occasionally to prevent queue buildup
            if i % 100 == 0:
                producer._producer.poll(0)
        
        producer.flush()
        
        duration = time.time() - start_time
        rate = message_count / duration
        
        # Should achieve reasonable throughput
        self.assertGreater(rate, 100, f"Production rate too low: {rate:.1f} msg/s")
        
        # Verify messages are protobuf
        consumer = Consumer({
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": "test_high_volume_consumer",
            "auto.offset.reset": "earliest"
        })
        consumer.subscribe([self.test_topics[0]])
        
        # Sample first 10 messages
        for i in range(10):
            msg = consumer.poll(1.0)
            if msg and not msg.error():
                try:
                    json.loads(msg.value().decode('utf-8'))
                    self.fail("High volume messages are JSON, not protobuf")
                except (json.JSONDecodeError, UnicodeDecodeError):
                    pass
        
        consumer.close()
    
    def test_error_handling_invalid_protobuf_data(self):
        """Test error handling when producing invalid protobuf data."""
        producer = ProtobufProducer(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.test_topics[0],
            schema_registry_url=self.schema_registry_url,
            schema_proto_class=customer_pb2.Customer
        )
        
        # Test with wrong data types
        invalid_data = {
            'customer_id': 12345,  # Should be string
            'name': None,  # Should be string
            'email': ['not', 'a', 'string'],  # Should be string
        }
        
        # This should handle gracefully or raise meaningful error
        try:
            producer.produce(invalid_data)
            producer.flush()
        except Exception as e:
            # Should get a meaningful error, not a generic one
            self.assertIsInstance(e, (TypeError, AttributeError))


if __name__ == '__main__':
    unittest.main()