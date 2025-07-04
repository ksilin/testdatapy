"""Test CSV export functionality works independently of bulk_load setting."""
import pytest
import tempfile
import csv
from pathlib import Path

from testdatapy.config.correlation_config import CorrelationConfig
from testdatapy.generators.reference_pool import ReferencePool
from testdatapy.generators.master_data_generator import MasterDataGenerator


class TestCSVExportWithBulkLoad:
    """Test CSV export works correctly with various bulk_load configurations."""
    
    def test_csv_export_with_bulk_load_true(self):
        """Test CSV export when bulk_load is true (with Kafka production)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "bulk_true.csv"
            
            config_dict = {
                "master_data": {
                    "test_data": {
                        "source": "faker",
                        "count": 3,
                        "kafka_topic": "test_topic",
                        "bulk_load": True,
                        "csv_export": str(csv_file),
                        "schema": {
                            "id": {"type": "string", "format": "TEST_{seq:03d}"},
                            "value": {"type": "string", "initial_value": "test_value"}
                        }
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config_dict)
            ref_pool = ReferencePool()
            
            # No producer provided - should fail for Kafka but CSV should still work
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer=None)
            
            # Load data
            master_gen.load_all()
            assert len(master_gen.loaded_data["test_data"]) == 3
            
            # This should fail for Kafka production but still export CSV
            with pytest.raises(ValueError, match="No producer configured"):
                master_gen.produce_all()
            
            # CSV file should still be created despite Kafka failure
            assert csv_file.exists()
            
            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                assert len(rows) == 3
                for i, row in enumerate(rows):
                    assert row["id"] == f"TEST_{i+1:03d}"
                    assert row["value"] == "test_value"
    
    def test_csv_export_with_bulk_load_false(self):
        """Test CSV export when bulk_load is false (no Kafka production)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "bulk_false.csv"
            
            config_dict = {
                "master_data": {
                    "reference_data": {
                        "source": "faker",
                        "count": 5,
                        "kafka_topic": "reference_topic",  # Topic present but not used
                        "bulk_load": False,  # Don't produce to Kafka
                        "csv_export": str(csv_file),
                        "schema": {
                            "ref_id": {"type": "string", "format": "REF_{seq:04d}"},
                            "description": {"type": "string", "initial_value": "reference_item"}
                        }
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config_dict)
            ref_pool = ReferencePool()
            
            # No producer needed since bulk_load is false
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer=None)
            
            # Load and process
            master_gen.load_all()
            master_gen.produce_all()  # Should not fail despite no producer
            
            # Verify data was loaded
            assert len(master_gen.loaded_data["reference_data"]) == 5
            
            # Verify reference pool was populated
            assert ref_pool.get_type_count("reference_data") == 5
            
            # Verify CSV was exported
            assert csv_file.exists()
            
            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                assert len(rows) == 5
                for i, row in enumerate(rows):
                    assert row["ref_id"] == f"REF_{i+1:04d}"
                    assert row["description"] == "reference_item"
    
    def test_csv_export_without_kafka_topic(self):
        """Test CSV export for reference-only entity (no kafka_topic)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "no_topic.csv"
            
            config_dict = {
                "master_data": {
                    "lookup_data": {
                        "source": "faker",
                        "count": 4,
                        # No kafka_topic - purely for reference pool
                        "bulk_load": False,
                        "csv_export": str(csv_file),
                        "schema": {
                            "lookup_id": {"type": "string", "format": "LU_{seq:02d}"},
                            "name": {"type": "string", "initial_value": "lookup_name"}
                        }
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config_dict)
            ref_pool = ReferencePool()
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer=None)
            
            # Should work fine without producer
            master_gen.load_all()
            master_gen.produce_all()
            
            # Verify CSV was created
            assert csv_file.exists()
            
            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                assert len(rows) == 4
                for i, row in enumerate(rows):
                    assert row["lookup_id"] == f"LU_{i+1:02d}"
                    assert row["name"] == "lookup_name"
    
    def test_mixed_bulk_load_scenarios(self):
        """Test multiple entities with different bulk_load settings."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv1 = Path(temp_dir) / "produce_to_kafka.csv"
            csv2 = Path(temp_dir) / "csv_only.csv"
            csv3 = Path(temp_dir) / "reference_only.csv"
            
            config_dict = {
                "master_data": {
                    "kafka_entity": {
                        "source": "faker",
                        "count": 2,
                        "kafka_topic": "kafka_topic",
                        "bulk_load": True,  # Will try to produce to Kafka
                        "csv_export": str(csv1),
                        "schema": {"id": {"type": "string", "format": "K_{seq:01d}"}}
                    },
                    "csv_only_entity": {
                        "source": "faker",
                        "count": 3,
                        "kafka_topic": "csv_topic",
                        "bulk_load": False,  # CSV only, no Kafka
                        "csv_export": str(csv2),
                        "schema": {"id": {"type": "string", "format": "C_{seq:01d}"}}
                    },
                    "reference_entity": {
                        "source": "faker",
                        "count": 1,
                        # No kafka_topic
                        "bulk_load": False,
                        "csv_export": str(csv3),
                        "schema": {"id": {"type": "string", "format": "R_{seq:01d}"}}
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config_dict)
            ref_pool = ReferencePool()
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer=None)
            
            # Load all entities
            master_gen.load_all()
            
            # This should fail for kafka_entity but succeed for others
            with pytest.raises(ValueError, match="No producer configured"):
                master_gen.produce_all()
            
            # All CSV files should be created regardless of Kafka failure
            assert csv1.exists()  # Created despite Kafka failure
            assert csv2.exists()  # Created, no Kafka attempted
            assert csv3.exists()  # Created, no Kafka attempted
            
            # Verify CSV contents
            with open(csv1, 'r') as f:
                rows = list(csv.DictReader(f))
                assert len(rows) == 2
                assert rows[0]["id"] == "K_1"
            
            with open(csv2, 'r') as f:
                rows = list(csv.DictReader(f))
                assert len(rows) == 3
                assert rows[0]["id"] == "C_1"
            
            with open(csv3, 'r') as f:
                rows = list(csv.DictReader(f))
                assert len(rows) == 1
                assert rows[0]["id"] == "R_1"
    
    def test_csv_export_with_nested_vehicle_structure(self):
        """Test CSV export with vehicle-style nested structure and bulk_load: false."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "vehicle_staging.csv"
            
            config_dict = {
                "master_data": {
                    "vehicle_staging": {
                        "source": "faker",
                        "count": 2,
                        "kafka_topic": "vehicle_topic",
                        "bulk_load": False,  # Export to CSV only, don't produce to Kafka
                        "csv_export": {
                            "file": str(csv_file),
                            "include_headers": True,
                            "flatten_objects": True
                        },
                        "schema": {
                            "jobid": {"type": "string", "format": "JOB_{seq:06d}"},
                            "full": {
                                "type": "object",
                                "properties": {
                                    "Vehicle": {
                                        "type": "object",
                                        "properties": {
                                            "cLicenseNr": {"type": "string", "initial_value": "M-AB 123"},
                                            "cLicenseNrCleaned": {"type": "string", "initial_value": "MAB123"}
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
            
            # Execute workflow
            master_gen.load_all()
            master_gen.produce_all()  # Should succeed without producer
            
            # Verify CSV was created with flattened structure
            assert csv_file.exists()
            
            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                assert len(rows) == 2
                
                # Check flattened column names
                expected_columns = ["jobid", "full.Vehicle.cLicenseNr", "full.Vehicle.cLicenseNrCleaned"]
                for col in expected_columns:
                    assert col in reader.fieldnames
                
                # Check data
                for i, row in enumerate(rows):
                    assert row["jobid"] == f"JOB_{i+1:06d}"
                    assert row["full.Vehicle.cLicenseNr"] == "M-AB 123"
                    assert row["full.Vehicle.cLicenseNrCleaned"] == "MAB123"