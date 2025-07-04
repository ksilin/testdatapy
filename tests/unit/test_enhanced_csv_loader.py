"""Tests for enhanced CSV loading functionality with structure reconstruction and type conversion."""
import pytest
import tempfile
import csv
from pathlib import Path
from typing import Dict, Any

from testdatapy.config.correlation_config import CorrelationConfig
from testdatapy.generators.reference_pool import ReferencePool
from testdatapy.generators.master_data_generator import MasterDataGenerator


class TestEnhancedCSVLoader:
    """Test enhanced CSV loading with flattened data reconstruction."""
    
    def test_load_simple_flat_csv(self):
        """Test loading simple flat CSV without dot-notation keys."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "simple.csv"
            
            # Create simple CSV
            with open(csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["id", "name", "age"])
                writer.writerow(["1", "Alice", "30"])
                writer.writerow(["2", "Bob", "25"])
            
            config_dict = {
                "master_data": {
                    "simple_data": {
                        "source": "csv",
                        "file": str(csv_file),
                        "bulk_load": False,
                        "schema": {
                            "id": {"type": "integer"},
                            "name": {"type": "string"},
                            "age": {"type": "integer"}
                        }
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config_dict)
            ref_pool = ReferencePool()
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer=None)
            
            # Load data
            master_gen.load_all()
            
            # Verify data was loaded and typed correctly
            data = master_gen.loaded_data["simple_data"]
            assert len(data) == 2
            
            assert data[0]["id"] == 1  # Converted to integer
            assert data[0]["name"] == "Alice"  # Remains string
            assert data[0]["age"] == 30  # Converted to integer
            
            assert data[1]["id"] == 2
            assert data[1]["name"] == "Bob"
            assert data[1]["age"] == 25
    
    def test_load_flattened_csv_with_reconstruction(self):
        """Test loading flattened CSV and reconstructing nested structure."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "flattened.csv"
            
            # Create flattened CSV (like vehicle export)
            with open(csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["jobid", "full.Vehicle.cLicenseNr", "full.Vehicle.cLicenseNrCleaned", "full.Customer.cName"])
                writer.writerow(["JOB_000001", "M-AB 123", "MAB123", "John"])
                writer.writerow(["JOB_000002", "B-CD 456", "BCD456", "Jane"])
            
            config_dict = {
                "master_data": {
                    "appointments": {
                        "source": "csv", 
                        "file": str(csv_file),
                        "bulk_load": False,
                        "id_field": "jobid",
                        "schema": {
                            "jobid": {"type": "string"},
                            "full": {
                                "type": "object",
                                "properties": {
                                    "Vehicle": {
                                        "type": "object",
                                        "properties": {
                                            "cLicenseNr": {"type": "string"},
                                            "cLicenseNrCleaned": {"type": "string"}
                                        }
                                    },
                                    "Customer": {
                                        "type": "object", 
                                        "properties": {
                                            "cName": {"type": "string"}
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
            
            # Verify data was reconstructed correctly
            data = master_gen.loaded_data["appointments"]
            assert len(data) == 2
            
            # Check first record
            record1 = data[0]
            assert record1["jobid"] == "JOB_000001"
            assert "full" in record1
            assert "Vehicle" in record1["full"]
            assert "Customer" in record1["full"]
            
            # Check nested structure
            assert record1["full"]["Vehicle"]["cLicenseNr"] == "M-AB 123"
            assert record1["full"]["Vehicle"]["cLicenseNrCleaned"] == "MAB123"
            assert record1["full"]["Customer"]["cName"] == "John"
            
            # Check second record
            record2 = data[1]
            assert record2["jobid"] == "JOB_000002"
            assert record2["full"]["Vehicle"]["cLicenseNr"] == "B-CD 456"
            assert record2["full"]["Vehicle"]["cLicenseNrCleaned"] == "BCD456"
            assert record2["full"]["Customer"]["cName"] == "Jane"
    
    def test_type_conversion_from_csv_strings(self):
        """Test type conversion from CSV strings to proper types."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "typed.csv"
            
            # Create CSV with various data types as strings
            with open(csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["id", "price", "active", "created_timestamp", "nested.count"])
                writer.writerow(["1", "29.99", "true", "1625097600", "5"])
                writer.writerow(["2", "19.95", "false", "1625184000", "10"])
            
            config_dict = {
                "master_data": {
                    "products": {
                        "source": "csv",
                        "file": str(csv_file),
                        "bulk_load": False,
                        "schema": {
                            "id": {"type": "integer"},
                            "price": {"type": "float"},
                            "active": {"type": "boolean"},
                            "created_timestamp": {"type": "timestamp"},
                            "nested": {
                                "type": "object",
                                "properties": {
                                    "count": {"type": "integer"}
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
            
            # Verify type conversions
            data = master_gen.loaded_data["products"]
            assert len(data) == 2
            
            record1 = data[0]
            assert record1["id"] == 1  # String "1" -> int 1
            assert record1["price"] == 29.99  # String "29.99" -> float 29.99
            assert record1["active"] is True  # String "true" -> bool True
            assert record1["created_timestamp"] == 1625097600  # String -> int timestamp
            assert record1["nested"]["count"] == 5  # String "5" -> int 5
            
            record2 = data[1]
            assert record2["id"] == 2
            assert record2["price"] == 19.95
            assert record2["active"] is False  # String "false" -> bool False
            assert record2["created_timestamp"] == 1625184000
            assert record2["nested"]["count"] == 10
    
    def test_vehicle_scenario_csv_loading(self):
        """Test vehicle scenario: load flattened appointment CSV for reference pool."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "vehicle_appointments.csv"
            
            # Create vehicle-style flattened CSV (as exported from stage 1)
            with open(csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    "jobid", "branchid", "dtStart", 
                    "full.Vehicle.cLicenseNr", "full.Vehicle.cLicenseNrCleaned", "full.Vehicle.cFactoryNr",
                    "full.Customer.cName", "full.Customer.cName2"
                ])
                writer.writerow([
                    "JOB_000001", "5fc36c95559ad6001f3998bb", "1625097600000",
                    "M-AB 123", "MAB123", "WBA12345678901234",
                    "John", "Doe"
                ])
                writer.writerow([
                    "JOB_000002", "5ea4c09e4ade180021ff23bf", "1625184000000", 
                    "B-CD 456", "BCD456", "WBA98765432109876",
                    "Jane", "Smith"
                ])
            
            config_dict = {
                "master_data": {
                    "appointments_from_csv": {
                        "source": "csv",
                        "file": str(csv_file),
                        "bulk_load": False,  # Reference only, no Kafka production
                        "id_field": "jobid",
                        "schema": {
                            "jobid": {"type": "string"},
                            "branchid": {"type": "string"}, 
                            "dtStart": {"type": "timestamp_millis"},
                            "full": {
                                "type": "object",
                                "properties": {
                                    "Vehicle": {
                                        "type": "object",
                                        "properties": {
                                            "cLicenseNr": {"type": "string"},
                                            "cLicenseNrCleaned": {"type": "string"},
                                            "cFactoryNr": {"type": "string"}
                                        }
                                    },
                                    "Customer": {
                                        "type": "object",
                                        "properties": {
                                            "cName": {"type": "string"},
                                            "cName2": {"type": "string"}
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
            
            # Verify vehicle data structure was reconstructed
            data = master_gen.loaded_data["appointments_from_csv"]
            assert len(data) == 2
            
            # Check first appointment
            appt1 = data[0]
            assert appt1["jobid"] == "JOB_000001"
            assert appt1["branchid"] == "5fc36c95559ad6001f3998bb"
            assert appt1["dtStart"] == 1625097600000  # Converted to int
            
            # Check nested vehicle structure
            assert appt1["full"]["Vehicle"]["cLicenseNr"] == "M-AB 123"
            assert appt1["full"]["Vehicle"]["cLicenseNrCleaned"] == "MAB123"
            assert appt1["full"]["Vehicle"]["cFactoryNr"] == "WBA12345678901234"
            assert appt1["full"]["Customer"]["cName"] == "John"
            assert appt1["full"]["Customer"]["cName2"] == "Doe"
            
            # Check second appointment
            appt2 = data[1]
            assert appt2["jobid"] == "JOB_000002"
            assert appt2["full"]["Vehicle"]["cLicenseNr"] == "B-CD 456"
            assert appt2["full"]["Customer"]["cName"] == "Jane"
    
    def test_reference_pool_population_with_nested_fields(self):
        """Test that reference pool is populated correctly with nested ID fields."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "nested_ids.csv"
            
            # Create CSV with nested ID structure
            with open(csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["data.record.id", "data.record.name", "status"])
                writer.writerow(["REC_001", "Record One", "active"])
                writer.writerow(["REC_002", "Record Two", "inactive"])
            
            config_dict = {
                "master_data": {
                    "nested_records": {
                        "source": "csv",
                        "file": str(csv_file),
                        "bulk_load": False,
                        "id_field": "data.record.id",  # Nested ID field
                        "schema": {
                            "data": {
                                "type": "object",
                                "properties": {
                                    "record": {
                                        "type": "object",
                                        "properties": {
                                            "id": {"type": "string"},
                                            "name": {"type": "string"}
                                        }
                                    }
                                }
                            },
                            "status": {"type": "string"}
                        }
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config_dict)
            ref_pool = ReferencePool()
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer=None)
            
            # Load data
            master_gen.load_all()
            
            # Verify data structure
            data = master_gen.loaded_data["nested_records"]
            assert len(data) == 2
            
            record1 = data[0]
            assert record1["data"]["record"]["id"] == "REC_001"
            assert record1["data"]["record"]["name"] == "Record One"
            assert record1["status"] == "active"
            
            # Verify reference pool was populated
            # Note: This tests the integration between CSV loading and reference pool
            assert ref_pool.get_type_count("nested_records") == 2
    
    def test_mixed_flat_and_nested_csv_keys(self):
        """Test CSV with mix of flat and nested keys."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "mixed.csv"
            
            # Create CSV with both flat and nested keys
            with open(csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["id", "name", "address.street", "address.city", "phone"])
                writer.writerow(["1", "Alice", "123 Main St", "Springfield", "555-1234"])
                writer.writerow(["2", "Bob", "456 Oak Ave", "Riverside", "555-5678"])
            
            config_dict = {
                "master_data": {
                    "contacts": {
                        "source": "csv",
                        "file": str(csv_file),
                        "bulk_load": False,
                        "schema": {
                            "id": {"type": "integer"},
                            "name": {"type": "string"},
                            "address": {
                                "type": "object",
                                "properties": {
                                    "street": {"type": "string"},
                                    "city": {"type": "string"}
                                }
                            },
                            "phone": {"type": "string"}
                        }
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config_dict)
            ref_pool = ReferencePool()
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer=None)
            
            # Load data
            master_gen.load_all()
            
            # Verify mixed structure
            data = master_gen.loaded_data["contacts"]
            assert len(data) == 2
            
            contact1 = data[0]
            assert contact1["id"] == 1  # Flat field
            assert contact1["name"] == "Alice"  # Flat field
            assert contact1["phone"] == "555-1234"  # Flat field
            assert contact1["address"]["street"] == "123 Main St"  # Reconstructed nested
            assert contact1["address"]["city"] == "Springfield"  # Reconstructed nested
    
    def test_csv_loading_error_handling(self):
        """Test error handling in CSV loading process."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Test missing CSV file
            config_dict = {
                "master_data": {
                    "missing_data": {
                        "source": "csv",
                        "file": "nonexistent.csv",
                        "bulk_load": False
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config_dict)
            ref_pool = ReferencePool()
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer=None)
            
            # Should raise FileNotFoundError
            with pytest.raises(FileNotFoundError, match="CSV file not found"):
                master_gen.load_all()
    
    def test_empty_csv_file_handling(self):
        """Test handling of empty CSV files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "empty.csv"
            
            # Create empty CSV (headers only)
            with open(csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["id", "name"])  # Headers but no data
            
            config_dict = {
                "master_data": {
                    "empty_data": {
                        "source": "csv",
                        "file": str(csv_file),
                        "bulk_load": False
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config_dict)
            ref_pool = ReferencePool()
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer=None)
            
            # Should handle empty file gracefully
            master_gen.load_all()
            
            # Verify empty data
            data = master_gen.loaded_data["empty_data"]
            assert len(data) == 0
    
    def test_type_conversion_error_handling(self):
        """Test error handling during type conversion."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "bad_types.csv"
            
            # Create CSV with invalid type data
            with open(csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["id", "amount", "active"])
                writer.writerow(["not_a_number", "invalid_float", "maybe"])
            
            config_dict = {
                "master_data": {
                    "bad_data": {
                        "source": "csv",
                        "file": str(csv_file),
                        "bulk_load": False,
                        "schema": {
                            "id": {"type": "integer"},
                            "amount": {"type": "float"},
                            "active": {"type": "boolean"}
                        }
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config_dict)
            ref_pool = ReferencePool()
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer=None)
            
            # Should handle type conversion errors gracefully
            master_gen.load_all()
            
            # Verify data is loaded with original string values (fallback)
            data = master_gen.loaded_data["bad_data"]
            assert len(data) == 1
            
            record = data[0]
            assert record["id"] == "not_a_number"  # Fallback to string
            assert record["amount"] == "invalid_float"  # Fallback to string  
            assert record["active"] == "maybe"  # Fallback to string