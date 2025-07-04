"""Unit tests for protobuf configuration extensions in correlation config."""
import unittest
import tempfile
import yaml
from pathlib import Path

from testdatapy.config.correlation_config import CorrelationConfig, ValidationError


class TestCorrelationConfigProtobuf(unittest.TestCase):
    """Test protobuf configuration extensions."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.base_config = {
            "master_data": {
                "customers": {
                    "kafka_topic": "test-customers",
                    "source": "faker",
                    "count": 100
                }
            },
            "transactional_data": {
                "orders": {
                    "kafka_topic": "test-orders",
                    "relationships": {
                        "customer_id": {
                            "references": "customers.id"
                        }
                    }
                }
            }
        }
    
    def test_valid_protobuf_config_master_data(self):
        """Test valid protobuf configuration for master data."""
        config = self.base_config.copy()
        config["master_data"]["customers"].update({
            "protobuf_module": "customer_pb2",
            "protobuf_class": "Customer"
        })
        
        # Should not raise exception
        correlation_config = CorrelationConfig(config)
        
        # Test getter methods
        protobuf_config = correlation_config.get_protobuf_config("customers", is_master=True)
        self.assertIsNotNone(protobuf_config)
        self.assertEqual(protobuf_config["protobuf_module"], "customer_pb2")
        self.assertEqual(protobuf_config["protobuf_class"], "Customer")
        
        self.assertTrue(correlation_config.has_protobuf_config("customers", is_master=True))
    
    def test_valid_protobuf_config_transactional_data(self):
        """Test valid protobuf configuration for transactional data."""
        config = self.base_config.copy()
        config["transactional_data"]["orders"].update({
            "protobuf_module": "order_pb2",
            "protobuf_class": "Order"
        })
        
        # Should not raise exception
        correlation_config = CorrelationConfig(config)
        
        # Test getter methods
        protobuf_config = correlation_config.get_protobuf_config("orders", is_master=False)
        self.assertIsNotNone(protobuf_config)
        self.assertEqual(protobuf_config["protobuf_module"], "order_pb2")
        self.assertEqual(protobuf_config["protobuf_class"], "Order")
        
        self.assertTrue(correlation_config.has_protobuf_config("orders", is_master=False))
    
    def test_protobuf_config_with_schema_path(self):
        """Test protobuf configuration with custom schema path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.base_config.copy()
            config["transactional_data"]["orders"].update({
                "protobuf_module": "events_pb2",
                "protobuf_class": "VehicleStayCud",
                "schema_path": temp_dir
            })
            
            # Should not raise exception
            correlation_config = CorrelationConfig(config)
            
            # Test getter methods
            protobuf_config = correlation_config.get_protobuf_config("orders", is_master=False)
            self.assertIn("schema_path", protobuf_config)
            self.assertEqual(protobuf_config["schema_path"], temp_dir)
    
    def test_protobuf_config_with_proto_file_path(self):
        """Test protobuf configuration with proto file path."""
        config = self.base_config.copy()
        config["transactional_data"]["orders"].update({
            "protobuf_module": "events_pb2",
            "protobuf_class": "VehicleStayCud",
            "proto_file_path": "events.proto"
        })
        
        # Should not raise exception (we don't validate proto file exists)
        correlation_config = CorrelationConfig(config)
        
        # Test getter methods
        protobuf_config = correlation_config.get_protobuf_config("orders", is_master=False)
        self.assertIn("proto_file_path", protobuf_config)
        self.assertEqual(protobuf_config["proto_file_path"], "events.proto")
    
    def test_missing_protobuf_class(self):
        """Test validation error when protobuf_module is specified without protobuf_class."""
        config = self.base_config.copy()
        config["master_data"]["customers"]["protobuf_module"] = "customer_pb2"
        # Missing protobuf_class
        
        with self.assertRaises(ValidationError) as context:
            CorrelationConfig(config)
        
        self.assertIn("protobuf_module specified", str(context.exception))
        self.assertIn("protobuf_class is missing", str(context.exception))
        self.assertIn("customers", str(context.exception))
    
    def test_missing_protobuf_module(self):
        """Test validation error when protobuf_class is specified without protobuf_module."""
        config = self.base_config.copy()
        config["transactional_data"]["orders"]["protobuf_class"] = "Order"
        # Missing protobuf_module
        
        with self.assertRaises(ValidationError) as context:
            CorrelationConfig(config)
        
        self.assertIn("protobuf_class specified", str(context.exception))
        self.assertIn("protobuf_module is missing", str(context.exception))
        self.assertIn("orders", str(context.exception))
    
    def test_invalid_schema_path_not_exists(self):
        """Test validation error for non-existent schema path."""
        config = self.base_config.copy()
        config["transactional_data"]["orders"].update({
            "protobuf_module": "order_pb2",
            "protobuf_class": "Order",
            "schema_path": "/nonexistent/path"
        })
        
        with self.assertRaises(ValidationError) as context:
            CorrelationConfig(config)
        
        self.assertIn("does not exist", str(context.exception))
        self.assertIn("orders", str(context.exception))
    
    def test_invalid_schema_path_not_directory(self):
        """Test validation error when schema_path is not a directory."""
        with tempfile.NamedTemporaryFile() as temp_file:
            config = self.base_config.copy()
            config["transactional_data"]["orders"].update({
                "protobuf_module": "order_pb2",
                "protobuf_class": "Order",
                "schema_path": temp_file.name
            })
            
            with self.assertRaises(ValidationError) as context:
                CorrelationConfig(config)
            
            self.assertIn("not a directory", str(context.exception))
    
    def test_empty_schema_path(self):
        """Test validation error for empty schema path."""
        config = self.base_config.copy()
        config["transactional_data"]["orders"].update({
            "protobuf_module": "order_pb2",
            "protobuf_class": "Order",
            "schema_path": "   "  # Empty/whitespace string
        })
        
        with self.assertRaises(ValidationError) as context:
            CorrelationConfig(config)
        
        self.assertIn("empty 'schema_path'", str(context.exception))
    
    def test_invalid_proto_file_path_type(self):
        """Test validation error for invalid proto_file_path type."""
        config = self.base_config.copy()
        config["transactional_data"]["orders"].update({
            "protobuf_module": "order_pb2",
            "protobuf_class": "Order",
            "proto_file_path": 123  # Invalid type
        })
        
        with self.assertRaises(ValidationError) as context:
            CorrelationConfig(config)
        
        self.assertIn("invalid 'proto_file_path'", str(context.exception))
        self.assertIn("must be a string", str(context.exception))
    
    def test_no_protobuf_config(self):
        """Test behavior when no protobuf configuration is specified."""
        correlation_config = CorrelationConfig(self.base_config)
        
        # Should return None for entities without protobuf config
        self.assertIsNone(correlation_config.get_protobuf_config("customers", is_master=True))
        self.assertIsNone(correlation_config.get_protobuf_config("orders", is_master=False))
        
        self.assertFalse(correlation_config.has_protobuf_config("customers", is_master=True))
        self.assertFalse(correlation_config.has_protobuf_config("orders", is_master=False))
    
    def test_relative_schema_path(self):
        """Test relative schema path resolution."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a subdirectory
            subdir = Path(temp_dir) / "schemas"
            subdir.mkdir()
            
            # Create config with relative path
            config = self.base_config.copy()
            config["transactional_data"]["orders"].update({
                "protobuf_module": "order_pb2",
                "protobuf_class": "Order",
                "schema_path": "./schemas"
            })
            
            # Change working directory
            import os
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                
                # Should not raise exception
                correlation_config = CorrelationConfig(config)
                
                # Verify the path is resolved correctly
                protobuf_config = correlation_config.get_protobuf_config("orders", is_master=False)
                self.assertIn("schema_path", protobuf_config)
                
            finally:
                os.chdir(original_cwd)
    
    def test_yaml_load_with_protobuf_config(self):
        """Test loading configuration from YAML file with protobuf config."""
        yaml_content = """
master_data:
  vehicles:
    kafka_topic: "test-vehicles"
    source: "faker"
    count: 50
    protobuf_module: "objects_pb2"
    protobuf_class: "Trackable"

transactional_data:
  vehiclestays:
    kafka_topic: "test-vehiclestays"
    protobuf_module: "events_pb2"
    protobuf_class: "VehicleStayCud"
    relationships:
      vehicle_alias:
        references: "vehicles.alias"
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as temp_file:
            temp_file.write(yaml_content)
            temp_file.flush()
            
            try:
                # Load from YAML file
                correlation_config = CorrelationConfig.from_yaml_file(temp_file.name)
                
                # Verify protobuf config was loaded correctly
                vehicles_config = correlation_config.get_protobuf_config("vehicles", is_master=True)
                self.assertIsNotNone(vehicles_config)
                self.assertEqual(vehicles_config["protobuf_module"], "objects_pb2")
                self.assertEqual(vehicles_config["protobuf_class"], "Trackable")
                
                stays_config = correlation_config.get_protobuf_config("vehiclestays", is_master=False)
                self.assertIsNotNone(stays_config)
                self.assertEqual(stays_config["protobuf_module"], "events_pb2")
                self.assertEqual(stays_config["protobuf_class"], "VehicleStayCud")
                
            finally:
                # Clean up
                import os
                os.unlink(temp_file.name)


if __name__ == '__main__':
    unittest.main()