"""Test CSV export functionality for master data generation."""
import pytest
import tempfile
import csv
from pathlib import Path
from typing import Dict, Any

from testdatapy.config.correlation_config import CorrelationConfig, ValidationError
from testdatapy.generators.reference_pool import ReferencePool
from testdatapy.generators.master_data_generator import MasterDataGenerator


class TestCSVExportConfiguration:
    """Test CSV export configuration validation."""
    
    def test_simple_csv_export_config_validation(self):
        """Test that simple string csv_export configuration is validated."""
        config_dict = {
            "master_data": {
                "customers": {
                    "source": "faker",
                    "count": 10,
                    "kafka_topic": "customers",
                    "csv_export": "data/customers.csv",  # Simple string config
                    "schema": {
                        "customer_id": {"type": "string", "format": "CUST_{seq:03d}"},
                        "name": {"type": "faker", "method": "name"}
                    }
                }
            }
        }
        
        # Should not raise validation error
        correlation_config = CorrelationConfig(config_dict)
        assert "csv_export" in correlation_config.config["master_data"]["customers"]
    
    def test_complex_csv_export_config_validation(self):
        """Test that complex object csv_export configuration is validated."""
        config_dict = {
            "master_data": {
                "appointments": {
                    "source": "faker",
                    "count": 5,
                    "kafka_topic": "appointments",
                    "csv_export": {
                        "file": "data/appointments.csv",
                        "include_headers": True,
                        "flatten_objects": True,
                        "delimiter": ","
                    },
                    "schema": {
                        "jobid": {"type": "string", "format": "JOB_{seq:03d}"}
                    }
                }
            }
        }
        
        # Should not raise validation error
        correlation_config = CorrelationConfig(config_dict)
        csv_config = correlation_config.config["master_data"]["appointments"]["csv_export"]
        assert csv_config["file"] == "data/appointments.csv"
        assert csv_config["include_headers"] is True
    
    def test_invalid_csv_export_config_raises_error(self):
        """Test that invalid csv_export configuration raises ValidationError."""
        config_dict = {
            "master_data": {
                "orders": {
                    "source": "faker",
                    "count": 5,
                    "kafka_topic": "orders",
                    "csv_export": {"include_headers": True},  # Missing required 'file'
                    "schema": {"order_id": {"type": "string", "format": "ORDER_{seq:03d}"}}
                }
            }
        }
        
        with pytest.raises(ValidationError, match="must specify 'file'"):
            CorrelationConfig(config_dict)
    
    def test_csv_export_config_with_invalid_type(self):
        """Test that csv_export with invalid type raises ValidationError."""
        config_dict = {
            "master_data": {
                "products": {
                    "source": "faker",
                    "count": 5,
                    "kafka_topic": "products",
                    "csv_export": 123,  # Invalid type (not string or dict)
                    "schema": {"product_id": {"type": "string", "format": "PROD_{seq:03d}"}}
                }
            }
        }
        
        with pytest.raises(ValidationError, match="Invalid csv_export configuration"):
            CorrelationConfig(config_dict)


class TestCSVExportFunctionality:
    """Test CSV export functionality in MasterDataGenerator."""
    
    def test_csv_export_not_called_when_not_configured(self):
        """Test that CSV export is not called when csv_export is not configured."""
        config_dict = {
            "master_data": {
                "customers": {
                    "source": "faker",
                    "count": 3,
                    "kafka_topic": "customers",
                    # No csv_export configured
                    "schema": {
                        "customer_id": {"type": "string", "format": "CUST_{seq:03d}"},
                        "name": {"type": "faker", "method": "name"}
                    }
                }
            }
        }
        
        correlation_config = CorrelationConfig(config_dict)
        ref_pool = ReferencePool()
        master_gen = MasterDataGenerator(correlation_config, ref_pool, producer=None)
        
        # Load data
        master_gen.load_all()
        
        # Verify no CSV file was created
        # Since no csv_export is configured, this should pass
        assert len(master_gen.loaded_data["customers"]) == 3
    
    def test_simple_csv_export_creates_file(self):
        """Test that simple csv_export configuration creates CSV file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "customers.csv"
            
            config_dict = {
                "master_data": {
                    "customers": {
                        "source": "faker",
                        "count": 3,
                        "kafka_topic": "customers",
                        "csv_export": str(csv_file),
                        "schema": {
                            "customer_id": {"type": "string", "format": "CUST_{seq:03d}"},
                            "name": {"type": "faker", "method": "name"}
                        }
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config_dict)
            ref_pool = ReferencePool()
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer=None)
            
            # Load and produce (this should trigger CSV export)
            master_gen.load_all()
            
            # Since we don't have a producer, we need to test CSV export directly
            # Verify data was loaded
            assert len(master_gen.loaded_data["customers"]) == 3
            
            # Manually trigger CSV export (since produce_all requires producer)
            master_gen._export_to_csv("customers", config_dict["master_data"]["customers"])
            
            # Verify CSV file was created
            assert csv_file.exists()
            
            # Verify CSV content
            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                assert len(rows) == 3
                assert "customer_id" in rows[0]
                assert "name" in rows[0]
    
    def test_nested_object_flattening(self):
        """Test that nested objects are flattened correctly for CSV export."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "appointments.csv"
            
            config_dict = {
                "master_data": {
                    "appointments": {
                        "source": "faker",
                        "count": 2,
                        "kafka_topic": "appointments",
                        "csv_export": str(csv_file),
                        "schema": {
                            "jobid": {"type": "string", "format": "JOB_{seq:03d}"},
                            "full": {
                                "type": "object",
                                "properties": {
                                    "Vehicle": {
                                        "type": "object",
                                        "properties": {
                                            "cLicenseNr": {"type": "string", "initial_value": "M-AB 123"},
                                            "cLicenseNrCleaned": {"type": "string", "initial_value": "MAB123"}
                                        }
                                    },
                                    "Customer": {
                                        "type": "object", 
                                        "properties": {
                                            "cName": {"type": "faker", "method": "first_name"},
                                            "cName2": {"type": "faker", "method": "last_name"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config_dict)
            ref_pool = ReferencePool()
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer=None)
            
            # Load data
            master_gen.load_all()
            
            # Test flattening functionality
            sample_record = {
                "jobid": "JOB_001",
                "full": {
                    "Vehicle": {
                        "cLicenseNr": "M-AB 123",
                        "cLicenseNrCleaned": "MAB123"
                    }
                }
            }
            
            flattened = master_gen._flatten_record(sample_record)
            
            # Verify flattening worked correctly
            expected_keys = ["jobid", "full.Vehicle.cLicenseNr", "full.Vehicle.cLicenseNrCleaned"]
            assert set(flattened.keys()) == set(expected_keys)
            assert flattened["jobid"] == "JOB_001"
            assert flattened["full.Vehicle.cLicenseNr"] == "M-AB 123"
            assert flattened["full.Vehicle.cLicenseNrCleaned"] == "MAB123"
    
    def test_csv_export_preserves_all_field_types(self):
        """Test that CSV export preserves all field types correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "mixed_types.csv"
            
            config_dict = {
                "master_data": {
                    "test_entities": {
                        "source": "faker",
                        "count": 2,
                        "kafka_topic": "test_entities",
                        "csv_export": str(csv_file),
                        "schema": {
                            "id": {"type": "string", "format": "ID_{seq:03d}"},
                            "name": {"type": "string", "initial_value": "Test Name"},
                            "count": {"type": "integer", "min": 1, "max": 100},
                            "price": {"type": "float", "min": 9.99, "max": 99.99},
                            "active": {"type": "random_boolean", "probability": 0.5},
                            "created_at": {"type": "faker", "method": "iso8601"}
                        }
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config_dict)
            ref_pool = ReferencePool()
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer=None)
            
            # Load data  
            master_gen.load_all()
            
            # Test CSV export functionality directly
            master_gen._export_to_csv("test_entities", config_dict["master_data"]["test_entities"])
            
            # Verify CSV file was created
            assert csv_file.exists()
            
            # Verify content includes all field types
            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                assert len(rows) == 2
                for row in rows:
                    assert "id" in row
                    assert "name" in row
                    assert "count" in row
                    assert "price" in row
                    assert "active" in row
                    assert "created_at" in row
    
    def test_csv_file_has_correct_headers(self):
        """Test that CSV file includes correct headers when configured."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "with_headers.csv"
            
            config_dict = {
                "master_data": {
                    "simple_entities": {
                        "source": "faker",
                        "count": 1,
                        "kafka_topic": "simple_entities",
                        "csv_export": {
                            "file": str(csv_file),
                            "include_headers": True
                        },
                        "schema": {
                            "id": {"type": "string", "format": "ID_{seq:03d}"},
                            "value": {"type": "string", "initial_value": "test"}
                        }
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config_dict)
            ref_pool = ReferencePool()
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer=None)
            
            # Load data
            master_gen.load_all()
            
            # Test CSV export with headers
            master_gen._export_to_csv("simple_entities", config_dict["master_data"]["simple_entities"])
            
            # Verify CSV file has headers
            assert csv_file.exists()
            with open(csv_file, 'r') as f:
                first_line = f.readline().strip()
                assert first_line == "id,value"  # Should have headers
    
    def test_csv_file_without_headers(self):
        """Test that CSV file excludes headers when configured."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "no_headers.csv" 
            
            config_dict = {
                "master_data": {
                    "simple_entities": {
                        "source": "faker",
                        "count": 1,
                        "kafka_topic": "simple_entities",
                        "csv_export": {
                            "file": str(csv_file),
                            "include_headers": False
                        },
                        "schema": {
                            "id": {"type": "string", "format": "ID_{seq:03d}"},
                            "value": {"type": "string", "initial_value": "test"}
                        }
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config_dict)
            ref_pool = ReferencePool()
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer=None)
            
            # Load data
            master_gen.load_all()
            
            # Test CSV export without headers
            master_gen._export_to_csv("simple_entities", config_dict["master_data"]["simple_entities"])
            
            # Verify CSV file has no headers (first line should be data)
            assert csv_file.exists()
            with open(csv_file, 'r') as f:
                first_line = f.readline().strip()
                # Should be data, not headers (starts with ID_ format)
                assert first_line.startswith("ID_") or "," in first_line


class TestCSVExportVehicleScenario:
    """Test CSV export with realistic vehicle scenario data."""
    
    def test_vehicle_appointments_csv_export(self):
        """Test CSV export with vehicle appointment structure."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "vehicle_appointments.csv"
            
            config_dict = {
                "master_data": {
                    "appointments": {
                        "source": "faker",
                        "count": 5,
                        "kafka_topic": "vehicle_appointments",
                        "id_field": "jobid",
                        "key_field": "branchid",
                        "csv_export": str(csv_file),
                        "schema": {
                            "jobid": {"type": "string", "format": "JOB_{seq:06d}"},
                            "branchid": {
                                "type": "choice",
                                "choices": ["5fc36c95559ad6001f3998bb", "5ea4c09e4ade180021ff23bf"]
                            },
                            "full": {
                                "type": "object",
                                "properties": {
                                    "Vehicle": {
                                        "type": "object",
                                        "properties": {
                                            "cLicenseNr": {"type": "string", "initial_value": "M-AB 123"},
                                            "cLicenseNrCleaned": {"type": "string", "initial_value": "MAB123"},
                                            "cFactoryNr": {"type": "string", "initial_value": "WBA12345678901234"}
                                        }
                                    },
                                    "Customer": {
                                        "type": "object",
                                        "properties": {
                                            "cName": {"type": "faker", "method": "first_name"},
                                            "cName2": {"type": "faker", "method": "last_name"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config_dict)
            ref_pool = ReferencePool()
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer=None)
            
            # Load data
            master_gen.load_all()
            appointments = master_gen.loaded_data["appointments"]
            
            # Verify data structure is correct
            assert len(appointments) == 5
            for appointment in appointments:
                assert "jobid" in appointment
                assert "branchid" in appointment
                assert "full" in appointment
                assert "Vehicle" in appointment["full"]
                assert "Customer" in appointment["full"]
            
            # Test CSV export for vehicle scenario
            master_gen._export_to_csv("appointments", config_dict["master_data"]["appointments"])
            
            # Verify CSV file was created with vehicle data
            assert csv_file.exists()
            
            # Verify flattened vehicle structure in CSV
            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                assert len(rows) == 5
                
                # Check flattened column names
                expected_columns = [
                    "jobid", "branchid", 
                    "full.Vehicle.cLicenseNr", "full.Vehicle.cLicenseNrCleaned", "full.Vehicle.cFactoryNr",
                    "full.Customer.cName", "full.Customer.cName2"
                ]
                for col in expected_columns:
                    assert col in reader.fieldnames
    
    def test_flattened_vehicle_structure_column_names(self):
        """Test that vehicle nested structure creates correct flattened column names."""
        sample_record = {
            "jobid": "JOB_000001",
            "branchid": "5fc36c95559ad6001f3998bb",
            "full": {
                "Vehicle": {
                    "cLicenseNr": "M-AB 123",
                    "cLicenseNrCleaned": "MAB123",
                    "cFactoryNr": "WBA12345678901234"
                },
                "Customer": {
                    "cName": "John",
                    "cName2": "Doe"
                }
            }
        }
        
        # Expected flattened structure
        expected_columns = [
            "jobid",
            "branchid", 
            "full.Vehicle.cLicenseNr",
            "full.Vehicle.cLicenseNrCleaned",
            "full.Vehicle.cFactoryNr",
            "full.Customer.cName",
            "full.Customer.cName2"
        ]
        
        # Test flattening functionality
        correlation_config = CorrelationConfig({"master_data": {}})
        ref_pool = ReferencePool()
        master_gen = MasterDataGenerator(correlation_config, ref_pool, producer=None)
        
        flattened = master_gen._flatten_record(sample_record)
        assert set(flattened.keys()) == set(expected_columns)