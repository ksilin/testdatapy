"""Unit tests for correlated CLI with protobuf support."""
import unittest
from unittest.mock import patch, Mock, MagicMock
import tempfile
import yaml

from click.testing import CliRunner

from testdatapy.cli_correlated import correlated


class TestCorrelatedCLIProtobuf(unittest.TestCase):
    """Test correlated CLI with protobuf format."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
        
        # Create a simple test config
        self.test_config = {
            "master_data": {
                "customers": {
                    "source": "faker",
                    "count": 5,
                    "kafka_topic": "test_customers",
                    "id_field": "customer_id",
                    "schema": {
                        "customer_id": {"type": "string", "format": "CUST_{seq:04d}"},
                        "name": {"type": "faker", "method": "name"},
                        "email": {"type": "faker", "method": "email"}
                    }
                }
            },
            "transactional_data": {
                "orders": {
                    "kafka_topic": "test_orders",
                    "rate_per_second": 10,
                    "max_messages": 10,
                    "relationships": {
                        "customer_id": {
                            "references": "customers.customer_id"
                        }
                    },
                    "derived_fields": {
                        "order_id": {"type": "string", "format": "ORDER_{seq:04d}"},
                        "total_amount": {"type": "float", "min": 10.0, "max": 100.0},
                        "status": {"type": "string", "initial_value": "pending"}
                    }
                }
            }
        }
    
    def test_correlated_generate_with_protobuf_format(self):
        """Test generate command with protobuf format."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(self.test_config, f)
            config_file = f.name
        
        # Test with dry run
        result = self.runner.invoke(correlated, [
            'generate',
            '--config', config_file,
            '--format', 'protobuf',
            '--schema-registry-url', 'http://localhost:8081',
            '--dry-run'
        ])
        
        # Should succeed in dry run mode
        self.assertEqual(result.exit_code, 0)
        self.assertIn("Loading master data", result.output)
        self.assertIn("customers: 5 records", result.output)
    
    def test_protobuf_format_requires_schema_registry(self):
        """Test that protobuf format requires schema registry URL."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(self.test_config, f)
            config_file = f.name
        
        # Test without schema registry URL
        result = self.runner.invoke(correlated, [
            'generate',
            '--config', config_file,
            '--format', 'protobuf',
            '--dry-run'
        ])
        
        # Should fail
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Protobuf format requires --schema-registry-url", result.output)
    
    @patch('testdatapy.cli_correlated.ProtobufProducer')
    def test_protobuf_producer_creation(self, mock_protobuf_producer):
        """Test that protobuf producers are created correctly."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(self.test_config, f)
            config_file = f.name
        
        # Create mock protobuf modules
        import sys
        from types import ModuleType
        
        # Create mock modules
        mock_customer_module = ModuleType('customer_pb2')
        mock_order_module = ModuleType('order_pb2')
        mock_payment_module = ModuleType('payment_pb2')
        
        # Add mock classes
        mock_customer_module.Customer = Mock()
        mock_order_module.Order = Mock()
        mock_payment_module.Payment = Mock()
        
        # Patch the imports
        with patch.dict('sys.modules', {
            'testdatapy.schemas.protobuf.customer_pb2': mock_customer_module,
            'testdatapy.schemas.protobuf.order_pb2': mock_order_module,
            'testdatapy.schemas.protobuf.payment_pb2': mock_payment_module
        }):
            # Create mock producer instance
            mock_producer_instance = Mock()
            mock_protobuf_producer.return_value = mock_producer_instance
            
            result = self.runner.invoke(correlated, [
                'generate',
                '--config', config_file,
                '--format', 'protobuf',
                '--schema-registry-url', 'http://localhost:8081',
                '--bootstrap-servers', 'localhost:9092'
            ])
            
            # Debug output if test fails
            if result.exit_code != 0:
                print(f"Command failed with exit code {result.exit_code}")
                print(f"Output: {result.output}")
                print(f"Exception: {result.exception}")
            
            # Should create protobuf producers
            self.assertTrue(mock_protobuf_producer.called)
            
            # Check producer was created with correct parameters
            calls = mock_protobuf_producer.call_args_list
            
            # Should have created producers for both customers and orders
            topics_created = {call.kwargs['topic'] for call in calls if 'topic' in call.kwargs}
            self.assertIn('test_customers', topics_created)
            self.assertIn('test_orders', topics_created)
            
            # Check schema registry URL was passed
            for call in calls:
                if 'schema_registry_url' in call.kwargs:
                    self.assertEqual(call.kwargs['schema_registry_url'], 'http://localhost:8081')
    
    def test_json_format_still_works(self):
        """Test that JSON format still works (backward compatibility)."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(self.test_config, f)
            config_file = f.name
        
        # Test with JSON format (default)
        result = self.runner.invoke(correlated, [
            'generate',
            '--config', config_file,
            '--dry-run'
        ])
        
        # Should succeed
        self.assertEqual(result.exit_code, 0)
        self.assertIn("Loading master data", result.output)
    
    def test_format_choice_validation(self):
        """Test that only valid format choices are accepted."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(self.test_config, f)
            config_file = f.name
        
        # Test with invalid format
        result = self.runner.invoke(correlated, [
            'generate',
            '--config', config_file,
            '--format', 'avro',  # Not supported yet
            '--dry-run'
        ])
        
        # Should fail
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Invalid value for '--format'", result.output)
    
    @patch('testdatapy.cli_correlated.ProtobufProducer')
    def test_protobuf_with_all_entity_types(self, mock_protobuf_producer):
        """Test protobuf support for all entity types."""
        # Extended config with payments
        extended_config = self.test_config.copy()
        extended_config['transactional_data']['payments'] = {
            "kafka_topic": "test_payments",
            "rate_per_second": 5,
            "max_messages": 5,
            "relationships": {
                "order_id": {
                    "references": "orders.order_id"
                }
            },
            "derived_fields": {
                "payment_id": {"type": "string", "format": "PAY_{seq:04d}"},
                "amount": {"type": "float", "min": 10.0, "max": 100.0},
                "status": {"type": "string", "initial_value": "completed"}
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(extended_config, f)
            config_file = f.name
        
        # Create mock protobuf modules
        import sys
        from types import ModuleType
        
        # Create mock modules
        mock_customer_module = ModuleType('customer_pb2')
        mock_order_module = ModuleType('order_pb2')
        mock_payment_module = ModuleType('payment_pb2')
        
        # Add mock classes
        mock_customer_module.Customer = Mock()
        mock_order_module.Order = Mock()
        mock_payment_module.Payment = Mock()
        
        # Patch the imports
        with patch.dict('sys.modules', {
            'testdatapy.schemas.protobuf.customer_pb2': mock_customer_module,
            'testdatapy.schemas.protobuf.order_pb2': mock_order_module,
            'testdatapy.schemas.protobuf.payment_pb2': mock_payment_module
        }):
            mock_producer_instance = Mock()
            mock_protobuf_producer.return_value = mock_producer_instance
            
            result = self.runner.invoke(correlated, [
                'generate',
                '--config', config_file,
                '--format', 'protobuf',
                '--schema-registry-url', 'http://localhost:8081'
            ])
            
            # Check that producers were created for all topics
            calls = mock_protobuf_producer.call_args_list
            topics_created = {call.kwargs['topic'] for call in calls if 'topic' in call.kwargs}
            
            self.assertIn('test_customers', topics_created)
            self.assertIn('test_orders', topics_created)
            self.assertIn('test_payments', topics_created)


if __name__ == '__main__':
    unittest.main()