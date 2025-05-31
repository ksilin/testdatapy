"""Integration tests for correlated data generation with protobuf format."""
import json
import time
import unittest
from pathlib import Path
from unittest.mock import patch, Mock
import tempfile
import yaml

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer

from testdatapy.config import AppConfig, CorrelationConfig
from testdatapy.generators import ReferencePool, CorrelatedDataGenerator
from testdatapy.generators.master_data_generator import MasterDataGenerator
from testdatapy.producers.protobuf_producer import ProtobufProducer

# Import protobuf classes
from testdatapy.schemas.protobuf import customer_pb2, order_pb2, payment_pb2


@pytest.mark.integration
class TestCorrelatedProtobufIntegration(unittest.TestCase):
    """Test correlated data generation with protobuf format."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.bootstrap_servers = "localhost:9092"
        cls.schema_registry_url = "http://localhost:8081"
        cls.test_topics = ["corr_customers_proto", "corr_orders_proto", "corr_payments_proto"]
        
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
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        try:
            cls.admin.delete_topics(cls.test_topics, request_timeout=10)
        except Exception:
            pass
    
    def test_correlated_protobuf_workflow(self):
        """Test complete correlated data workflow with protobuf."""
        # Create correlation config
        config_dict = {
            "master_data": {
                "customers": {
                    "source": "faker",
                    "count": 10,
                    "kafka_topic": self.test_topics[0],
                    "id_field": "customer_id",
                    "schema": {
                        "customer_id": {
                            "type": "string",
                            "format": "CUST_{seq:06d}"
                        },
                        "name": {
                            "type": "faker",
                            "method": "name"
                        },
                        "email": {
                            "type": "faker", 
                            "method": "email"
                        },
                        "tier": {
                            "type": "string",
                            "choices": ["gold", "silver", "bronze"]
                        }
                    }
                }
            },
            "transactional_data": {
                "orders": {
                    "kafka_topic": self.test_topics[1],
                    "rate_per_second": 10,
                    "max_messages": 20,
                    "relationships": {
                        "customer_id": {
                            "references": "customers.customer_id"
                        }
                    },
                    "derived_fields": {
                        "order_id": {
                            "type": "string",
                            "format": "ORDER_{seq:06d}"
                        },
                        "total_amount": {
                            "type": "float",
                            "min": 10.0,
                            "max": 1000.0
                        },
                        "status": {
                            "type": "string",
                            "initial_value": "pending"
                        }
                    }
                },
                "payments": {
                    "kafka_topic": self.test_topics[2],
                    "rate_per_second": 8,
                    "max_messages": 20,
                    "relationships": {
                        "order_id": {
                            "references": "orders.order_id",
                            "recency_bias": True
                        }
                    },
                    "derived_fields": {
                        "payment_id": {
                            "type": "string",
                            "format": "PAY_{seq:06d}"
                        },
                        "amount": {
                            "type": "reference",
                            "source": "orders.total_amount",
                            "via": "order_id"
                        },
                        "payment_method": {
                            "type": "string",
                            "choices": ["credit_card", "debit_card", "paypal"]
                        },
                        "status": {
                            "type": "string",
                            "initial_value": "completed"
                        }
                    }
                }
            }
        }
        
        correlation_config = CorrelationConfig(config_dict)
        ref_pool = ReferencePool()
        ref_pool.enable_stats()
        
        # Create protobuf producers for each entity
        producers = {
            "customers": ProtobufProducer(
                bootstrap_servers=self.bootstrap_servers,
                topic=self.test_topics[0],
                schema_registry_url=self.schema_registry_url,
                schema_proto_class=customer_pb2.Customer,
                key_field='customer_id'
            ),
            "orders": ProtobufProducer(
                bootstrap_servers=self.bootstrap_servers,
                topic=self.test_topics[1],
                schema_registry_url=self.schema_registry_url,
                schema_proto_class=order_pb2.Order,
                key_field='order_id'
            ),
            "payments": ProtobufProducer(
                bootstrap_servers=self.bootstrap_servers,
                topic=self.test_topics[2],
                schema_registry_url=self.schema_registry_url,
                schema_proto_class=payment_pb2.Payment,
                key_field='payment_id'
            )
        }
        
        # Phase 1: Load master data
        master_gen = MasterDataGenerator(
            config=correlation_config,
            reference_pool=ref_pool,
            producer=None  # We'll produce manually
        )
        
        master_gen.load_all()
        
        # Produce master data with protobuf
        for entity_type in correlation_config.config.get("master_data", {}):
            producer = producers[entity_type]
            data = master_gen._data.get(entity_type, [])
            for record in data:
                producer.produce(record)
            producer.flush()
        
        # Phase 2: Generate transactional data
        for entity_type, entity_config in correlation_config.config.get("transactional_data", {}).items():
            generator = CorrelatedDataGenerator(
                entity_type=entity_type,
                config=correlation_config,
                reference_pool=ref_pool,
                rate_per_second=entity_config.get("rate_per_second"),
                max_messages=entity_config.get("max_messages")
            )
            
            if entity_config.get("track_recent", False):
                ref_pool.enable_recent_tracking(entity_type, window_size=100)
            
            producer = producers[entity_type]
            count = 0
            for record in generator.generate():
                producer.produce(record)
                count += 1
                if count >= entity_config.get("max_messages", 100):
                    break
            
            producer.flush()
        
        # Verify data in all topics
        results = {}
        for i, (topic, proto_class) in enumerate([
            (self.test_topics[0], customer_pb2.Customer),
            (self.test_topics[1], order_pb2.Order),
            (self.test_topics[2], payment_pb2.Payment)
        ]):
            consumer = Consumer({
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": f"test_corr_{topic}_consumer",
                "auto.offset.reset": "earliest"
            })
            consumer.subscribe([topic])
            
            deserializer = ProtobufDeserializer(proto_class, {'use.deprecated.format': False})
            
            messages = []
            start_time = time.time()
            while time.time() - start_time < 5:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    continue
                
                # Verify it's binary protobuf
                try:
                    json.loads(msg.value().decode('utf-8'))
                    self.fail(f"Message in {topic} is JSON, not protobuf")
                except (json.JSONDecodeError, UnicodeDecodeError):
                    pass
                
                # Deserialize
                obj = deserializer(msg.value(), None)
                messages.append(obj)
            
            consumer.close()
            results[topic] = messages
        
        # Verify data relationships
        customers = results[self.test_topics[0]]
        orders = results[self.test_topics[1]]
        payments = results[self.test_topics[2]]
        
        self.assertEqual(len(customers), 10)
        self.assertEqual(len(orders), 20)
        self.assertEqual(len(payments), 20)
        
        # Verify all orders reference valid customers
        customer_ids = {c.customer_id for c in customers}
        for order in orders:
            self.assertIn(order.customer_id, customer_ids)
        
        # Verify all payments reference valid orders
        order_ids = {o.order_id for o in orders}
        for payment in payments:
            self.assertIn(payment.order_id, order_ids)
    
    def test_cli_correlated_with_protobuf(self):
        """Test CLI should support protobuf format (currently doesn't)."""
        # This test documents the current limitation
        from click.testing import CliRunner
        from testdatapy.cli_correlated import correlated
        
        # Create a temp config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            config = {
                "master_data": {
                    "customers": {
                        "source": "faker",
                        "count": 5,
                        "kafka_topic": "test_customers",
                        "id_field": "customer_id"
                    }
                }
            }
            yaml.dump(config, f)
            config_file = f.name
        
        runner = CliRunner()
        
        # Currently the CLI only supports JSON
        # This documents what SHOULD work but doesn't yet
        result = runner.invoke(correlated, [
            'generate',
            '--config', config_file,
            '--dry-run'
        ])
        
        # For now, it works but only produces JSON
        self.assertEqual(result.exit_code, 0)
        
        # TODO: Add --format protobuf option to correlated CLI
        # result = runner.invoke(correlated, [
        #     'generate',
        #     '--config', config_file,
        #     '--format', 'protobuf',
        #     '--dry-run'
        # ])
    
    def test_protobuf_with_complex_relationships(self):
        """Test protobuf with complex nested relationships."""
        config_dict = {
            "master_data": {
                "customers": {
                    "source": "faker",
                    "count": 5,
                    "kafka_topic": self.test_topics[0],
                    "id_field": "customer_id",
                    "schema": {
                        "customer_id": {"type": "string", "format": "CUST_{seq:04d}"},
                        "name": {"type": "faker", "method": "name"},
                        "email": {"type": "faker", "method": "email"},
                        "address": {
                            "type": "object",
                            "properties": {
                                "street": {"type": "faker", "method": "street_address"},
                                "city": {"type": "faker", "method": "city"},
                                "postal_code": {"type": "faker", "method": "postcode"},
                                "country_code": {"type": "faker", "method": "country_code"}
                            }
                        }
                    }
                }
            }
        }
        
        correlation_config = CorrelationConfig(config_dict)
        ref_pool = ReferencePool()
        
        # Generate master data
        master_gen = MasterDataGenerator(
            config=correlation_config,
            reference_pool=ref_pool,
            producer=None
        )
        
        master_gen.load_all()
        
        # Produce with protobuf
        producer = ProtobufProducer(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.test_topics[0],
            schema_registry_url=self.schema_registry_url,
            schema_proto_class=customer_pb2.Customer,
            key_field='customer_id'
        )
        
        for record in master_gen._data["customers"]:
            producer.produce(record)
        
        producer.flush()
        
        # Verify nested data is preserved
        consumer = Consumer({
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": "test_complex_consumer",
            "auto.offset.reset": "earliest"
        })
        consumer.subscribe([self.test_topics[0]])
        
        deserializer = ProtobufDeserializer(customer_pb2.Customer, {'use.deprecated.format': False})
        
        count = 0
        start_time = time.time()
        while count < 5 and time.time() - start_time < 5:
            msg = consumer.poll(1.0)
            if msg and not msg.error():
                customer = deserializer(msg.value(), None)
                # Verify nested address is preserved
                self.assertTrue(hasattr(customer, 'address'))
                self.assertTrue(hasattr(customer.address, 'street'))
                self.assertTrue(hasattr(customer.address, 'city'))
                count += 1
        
        consumer.close()
        self.assertEqual(count, 5)


if __name__ == '__main__':
    unittest.main()