"""Unit tests for protobuf with correlated data generation."""
import unittest
from unittest.mock import Mock, patch
from pathlib import Path

from testdatapy.config import AppConfig, CorrelationConfig
from testdatapy.generators import CorrelatedDataGenerator
from testdatapy.producers.protobuf_producer import ProtobufProducer


class TestProtobufCorrelatedData(unittest.TestCase):
    """Test cases for protobuf with correlated data."""
    
    def test_protobuf_producer_with_correlated_generator(self):
        """Test that protobuf producer works with correlated data generator."""
        # This test should fail until we fix the producer
        config_dict = {
            "master_data": {
                "customers": {
                    "source": "faker",
                    "count": 10,
                    "kafka_topic": "customers",
                    "id_field": "customer_id"
                }
            },
            "transactional_data": {
                "orders": {
                    "kafka_topic": "orders",
                    "relationships": {
                        "customer_id": {
                            "references": "customers.customer_id"
                        }
                    }
                }
            }
        }
        
        correlation_config = CorrelationConfig(config_dict)
        
        # CorrelatedDataGenerator needs config and reference_pool
        from testdatapy.generators import ReferencePool
        from testdatapy.config import AppConfig
        
        app_config = AppConfig()
        ref_pool = ReferencePool()
        generator = CorrelatedDataGenerator(
            config=correlation_config,
            reference_pool=ref_pool,
            producer=None  # Not needed for this test
        )
        
        # Create protobuf producer - this should work with topic parameter
        producer = ProtobufProducer(
            bootstrap_servers="localhost:9092",
            topic="test_topic",
            schema_registry_url="http://localhost:8081",
            schema_proto_class=Mock(),
            key_field="customer_id"
        )
        
        # Generate and produce data
        generator.load_master_data()
        
        for entity_type in ["customers", "orders"]:
            for record in generator.generate(entity_type, count=5):
                producer.produce(record)
        
        producer.flush()
    
    def test_protobuf_cli_integration(self):
        """Test CLI can create protobuf producer with correlated data."""
        from click.testing import CliRunner
        from testdatapy.cli import cli
        
        runner = CliRunner()
        
        # This should work when implementation is fixed
        result = runner.invoke(cli, [
            'produce',
            'test_topic',
            '--format', 'protobuf',
            '--proto-class', 'testdatapy.schemas.protobuf.customer_pb2.Customer',
            '--generator', 'faker',
            '--dry-run'
        ])
        
        self.assertEqual(result.exit_code, 0)
        self.assertIn('protobuf', result.output.lower())
    
    def test_protobuf_schema_compilation(self):
        """Test that protobuf schemas can be compiled and used."""
        proto_dir = Path(__file__).parent.parent.parent / "src" / "testdatapy" / "schemas" / "protobuf"
        
        # Check if proto files exist
        customer_proto = proto_dir / "customer.proto"
        self.assertTrue(customer_proto.exists(), f"Missing {customer_proto}")
        
        # Try to import compiled protobuf
        try:
            from testdatapy.schemas.protobuf import customer_pb2
            self.assertTrue(hasattr(customer_pb2, 'Customer'))
        except ImportError:
            self.fail("Protobuf schemas not compiled. Run: protoc --python_out=. customer.proto")
    
    def test_protobuf_serialization_with_references(self):
        """Test that protobuf correctly serializes referenced data."""
        from testdatapy.generators import ReferencePool
        
        ref_pool = ReferencePool()
        ref_pool.add_references("customers", ["CUST_001"])  # ReferencePool stores IDs only
        
        # Create order with reference
        order_data = {
            "order_id": "ORDER_001",
            "customer_id": ref_pool.get_random("customers"),
            "total_amount": 99.99
        }
        
        # This should serialize correctly with protobuf
        producer = ProtobufProducer(
            bootstrap_servers="localhost:9092",
            topic="orders",
            schema_registry_url="http://localhost:8081",
            schema_proto_class=Mock()  # Would be Order protobuf class
        )
        
        # Should not raise exception
        producer.produce(order_data)
    
    def test_protobuf_with_all_entity_types(self):
        """Test protobuf with customers, orders, and payments."""
        entities = ["customers", "orders", "payments"]
        
        for entity in entities:
            with self.subTest(entity=entity):
                # Mock all dependencies
                with patch('testdatapy.producers.protobuf_producer.ConfluentProducer'):
                    with patch('testdatapy.producers.protobuf_producer.SchemaRegistryClient'):
                        with patch('testdatapy.producers.protobuf_producer.ProtobufSerializer'):
                            producer = ProtobufProducer(
                                bootstrap_servers="localhost:9092",
                                topic=entity,
                                schema_registry_url="http://localhost:8081",
                                schema_proto_class=Mock()  # Would be actual protobuf class
                            )
                            
                            # Should be able to produce each entity type
                            test_data = {"id": f"{entity}_001", "test": "data"}
                            producer.produce(test_data)
                            producer.flush()


if __name__ == '__main__':
    unittest.main()