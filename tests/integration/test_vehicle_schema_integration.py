"""Integration test for vehicle schema support using dynamic loading."""
import unittest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import yaml
from pathlib import Path

from testdatapy.config.correlation_config import CorrelationConfig
from testdatapy.schemas.schema_loader import DynamicSchemaLoader
from testdatapy.generators.correlated_generator import CorrelatedDataGenerator
from testdatapy.generators.reference_pool import ReferencePool


class MockVehicleProtobufClass:
    """Mock vehicle protobuf class for testing."""
    
    def __init__(self, **kwargs):
        """Initialize with keyword arguments."""
        for key, value in kwargs.items():
            setattr(self, key, value)
    
    def SerializeToString(self):
        """Mock serialization method."""
        return b"mock_serialized_data"


class TestVehicleSchemaIntegration(unittest.TestCase):
    """Integration tests for vehicle schema support."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.vehicle_config = {
            "master_data": {
                "vehicles": {
                    "kafka_topic": "test-vehicles",
                    "id_field": "alias",
                    "source": "faker",
                    "count": 10,
                    "bulk_load": True,
                    "protobuf_module": "vehicle_objects_pb2",
                    "protobuf_class": "Trackable",
                    "schema_path": self.temp_dir,
                    "schema": {
                        "alias": {
                            "type": "string",
                            "format": "VEHICLE_{seq:04d}"
                        },
                        "vehicle_identification_number": {
                            "type": "string", 
                            "format": "WBA{seq:10d}"
                        }
                    }
                }
            },
            "transactional_data": {
                "vehiclestays": {
                    "kafka_topic": "test-vehiclestays",
                    "id_field": "alias",
                    "rate_per_second": 2,
                    "max_messages": 5,
                    "protobuf_module": "vehicle_events_pb2",
                    "protobuf_class": "VehicleStayCud",
                    "schema_path": self.temp_dir,
                    "relationships": {
                        "vehicle_alias": {
                            "references": "vehicles.alias",
                            "distribution": "uniform"
                        }
                    },
                    "derived_fields": {
                        "alias": {
                            "type": "string",
                            "format": "STAY_{seq:06d}"
                        },
                        "event_id": {
                            "type": "uuid"
                        },
                        "event_type": {
                            "type": "choice",
                            "choices": ["CREATED", "UPDATED", "DELETED"]
                        }
                    }
                }
            }
        }
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_vehicle_config_validation(self):
        """Test that vehicle configuration passes validation."""
        # Should not raise exception
        correlation_config = CorrelationConfig(self.vehicle_config)
        
        # Verify protobuf configuration is parsed correctly
        vehicles_protobuf = correlation_config.get_protobuf_config("vehicles", is_master=True)
        self.assertIsNotNone(vehicles_protobuf)
        self.assertEqual(vehicles_protobuf["protobuf_module"], "vehicle_objects_pb2")
        self.assertEqual(vehicles_protobuf["protobuf_class"], "Trackable")
        self.assertEqual(vehicles_protobuf["schema_path"], self.temp_dir)
        
        stays_protobuf = correlation_config.get_protobuf_config("vehiclestays", is_master=False)
        self.assertIsNotNone(stays_protobuf)
        self.assertEqual(stays_protobuf["protobuf_module"], "vehicle_events_pb2")
        self.assertEqual(stays_protobuf["protobuf_class"], "VehicleStayCud")
    
    @patch('testdatapy.schemas.schema_loader.importlib.import_module')
    def test_dynamic_vehicle_schema_loading(self, mock_import):
        """Test dynamic loading of vehicle protobuf schemas."""
        # Mock vehicle protobuf modules
        mock_objects_module = Mock()
        mock_events_module = Mock()
        
        # Create mock protobuf classes
        mock_trackable_class = Mock(side_effect=MockVehicleProtobufClass)
        mock_vehiclestay_class = Mock(side_effect=MockVehicleProtobufClass)
        
        mock_objects_module.Trackable = mock_trackable_class
        mock_events_module.VehicleStayCud = mock_vehiclestay_class
        
        # Configure import mock to return appropriate modules
        def import_side_effect(module_name):
            if module_name == "vehicle_objects_pb2":
                return mock_objects_module
            elif module_name == "vehicle_events_pb2":
                return mock_events_module
            else:
                raise ModuleNotFoundError(f"No module named '{module_name}'")
        
        mock_import.side_effect = import_side_effect
        
        # Test schema loading
        loader = DynamicSchemaLoader()
        
        # Load vehicle protobuf class
        vehicle_class = loader.load_protobuf_class("vehicle_objects_pb2", "Trackable", self.temp_dir)
        self.assertEqual(vehicle_class, mock_trackable_class)
        
        # Load vehicle stay protobuf class
        stay_class = loader.load_protobuf_class("vehicle_events_pb2", "VehicleStayCud", self.temp_dir)
        self.assertEqual(stay_class, mock_vehiclestay_class)
    
    @patch('testdatapy.schemas.schema_loader.importlib.import_module')
    def test_vehicle_data_generation_workflow(self, mock_import):
        """Test complete vehicle data generation workflow."""
        # Setup mock vehicle protobuf modules and classes
        mock_objects_module = Mock()
        mock_events_module = Mock()
        
        mock_trackable_class = Mock(side_effect=MockVehicleProtobufClass)
        mock_vehiclestay_class = Mock(side_effect=MockVehicleProtobufClass)
        
        mock_objects_module.Trackable = mock_trackable_class
        mock_events_module.VehicleStayCud = mock_vehiclestay_class
        
        def import_side_effect(module_name):
            if module_name == "vehicle_objects_pb2":
                return mock_objects_module
            elif module_name == "vehicle_events_pb2":
                return mock_events_module
            else:
                raise ModuleNotFoundError(f"No module named '{module_name}'")
        
        mock_import.side_effect = import_side_effect
        
        # Create correlation config
        correlation_config = CorrelationConfig(self.vehicle_config)
        
        # Create reference pool and populate with vehicle data
        ref_pool = ReferencePool(enable_stats=True)
        
        # Simulate loading master data (vehicles)
        vehicle_data = [
            {"alias": "VEHICLE_0001", "vehicle_identification_number": "WBA0000000001"},
            {"alias": "VEHICLE_0002", "vehicle_identification_number": "WBA0000000002"},
            {"alias": "VEHICLE_0003", "vehicle_identification_number": "WBA0000000003"},
        ]
        
        for vehicle in vehicle_data:
            ref_pool.add_reference("vehicles", vehicle["alias"], vehicle)
        
        # Create correlated data generator for vehicle stays
        stay_generator = CorrelatedDataGenerator(
            entity_type="vehiclestays",
            config=correlation_config,
            reference_pool=ref_pool,
            rate_per_second=10,  # Speed up for testing
            max_messages=3
        )
        
        # Generate vehicle stay data
        generated_stays = list(stay_generator.generate())
        
        # Verify data was generated
        self.assertEqual(len(generated_stays), 3)
        
        # Verify each stay has required fields
        for stay in generated_stays:
            self.assertIn("alias", stay)
            self.assertIn("vehicle_alias", stay)
            self.assertIn("event_id", stay)
            self.assertIn("event_type", stay)
            
            # Verify vehicle reference is valid
            self.assertIn(stay["vehicle_alias"], ["VEHICLE_0001", "VEHICLE_0002", "VEHICLE_0003"])
            
            # Verify event_type is from valid choices
            self.assertIn(stay["event_type"], ["CREATED", "UPDATED", "DELETED"])
        
        # Verify reference pool statistics
        stats = ref_pool.get_stats()
        self.assertIn("vehicles", stats)
        self.assertGreater(stats["vehicles"]["access_count"], 0)
    
    def test_vehicle_config_from_yaml(self):
        """Test loading vehicle configuration from YAML file."""
        yaml_content = """
master_data:
  vehicles:
    kafka_topic: "vehicle-vehicles"
    id_field: "alias"
    source: "faker"
    count: 50
    bulk_load: true
    protobuf_module: "vehicle_spp_objects_pb2"
    protobuf_class: "Trackable"
    proto_file_path: "vehicle-spp-objects.proto"

transactional_data:
  vehiclestays:
    kafka_topic: "vehicle-vehiclestays"
    protobuf_module: "vehicle_spp_events_pb2"
    protobuf_class: "VehicleStayCud"
    proto_file_path: "vehicle-spp-events.proto"
    relationships:
      vehicle_alias:
        references: "vehicles.alias"
        distribution: "zipf"
        alpha: 1.2
    derived_fields:
      alias:
        type: "string"
        format: "STAY_{seq:06d}"
      event_type:
        type: "choice"
        choices: ["CREATED", "UPDATED", "DELETED"]
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as temp_file:
            temp_file.write(yaml_content)
            temp_file.flush()
            
            try:
                # Load from YAML
                correlation_config = CorrelationConfig.from_yaml_file(temp_file.name)
                
                # Verify protobuf configuration
                vehicles_config = correlation_config.get_protobuf_config("vehicles", is_master=True)
                self.assertIsNotNone(vehicles_config)
                self.assertEqual(vehicles_config["protobuf_module"], "vehicle_spp_objects_pb2")
                self.assertEqual(vehicles_config["protobuf_class"], "Trackable")
                self.assertEqual(vehicles_config["proto_file_path"], "vehicle-spp-objects.proto")
                
                stays_config = correlation_config.get_protobuf_config("vehiclestays", is_master=False)
                self.assertIsNotNone(stays_config)
                self.assertEqual(stays_config["protobuf_module"], "vehicle_spp_events_pb2")
                self.assertEqual(stays_config["protobuf_class"], "VehicleStayCud")
                self.assertEqual(stays_config["proto_file_path"], "vehicle-spp-events.proto")
                
            finally:
                import os
                os.unlink(temp_file.name)
    
    @patch('testdatapy.schemas.schema_loader.get_protobuf_class_for_entity')
    def test_schema_loading_fallback(self, mock_get_class):
        """Test fallback behavior when vehicle schemas are not available."""
        # Simulate vehicle schemas not being found
        mock_get_class.side_effect = [
            None,  # First call returns None (no dynamic loading)
            None   # Second call also returns None (no hardcoded mapping)
        ]
        
        correlation_config = CorrelationConfig(self.vehicle_config)
        
        # Verify that the config still validates successfully
        # (The actual error would occur during data generation/production)
        self.assertTrue(correlation_config.has_protobuf_config("vehicles", is_master=True))
        self.assertTrue(correlation_config.has_protobuf_config("vehiclestays", is_master=False))
    
    def test_error_handling_for_invalid_vehicle_config(self):
        """Test error handling for invalid vehicle protobuf configuration."""
        # Test missing protobuf_class
        invalid_config = self.vehicle_config.copy()
        del invalid_config["master_data"]["vehicles"]["protobuf_class"]
        
        with self.assertRaises(Exception) as context:
            CorrelationConfig(invalid_config)
        
        self.assertIn("protobuf_module specified", str(context.exception))
        self.assertIn("protobuf_class is missing", str(context.exception))
    
    def test_vehicle_schema_path_resolution(self):
        """Test schema path resolution for vehicle schemas."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create subdirectory for vehicle schemas
            vehicle_schema_dir = Path(temp_dir) / "vehicle_schemas"
            vehicle_schema_dir.mkdir()
            
            # Update config with relative path
            config = self.vehicle_config.copy()
            config["transactional_data"]["vehiclestays"]["schema_path"] = str(vehicle_schema_dir)
            
            # Should validate successfully
            correlation_config = CorrelationConfig(config)
            
            protobuf_config = correlation_config.get_protobuf_config("vehiclestays", is_master=False)
            self.assertEqual(protobuf_config["schema_path"], str(vehicle_schema_dir))


if __name__ == '__main__':
    unittest.main()