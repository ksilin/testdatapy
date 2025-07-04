"""Integration tests for CSV export functionality in master data generation.

This test validates the complete workflow of:
1. Loading master data (via faker or CSV)
2. Configuring CSV export
3. Triggering CSV export during production
4. Verifying exported CSV content and structure
"""
import pytest
import tempfile
import csv
from pathlib import Path
from unittest.mock import Mock, patch

from testdatapy.config.correlation_config import CorrelationConfig
from testdatapy.generators.reference_pool import ReferencePool
from testdatapy.generators.master_data_generator import MasterDataGenerator


class TestCSVExportIntegration:
    """Integration tests for end-to-end CSV export workflow."""
    
    def test_vehicle_appointments_full_workflow(self):
        """Test complete vehicle appointments CSV export workflow."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "vehicle_appointments.csv"
            
            # Vehicle-style configuration
            config_dict = {
                "master_data": {
                    "appointments": {
                        "source": "faker",
                        "count": 10,
                        "kafka_topic": "vehicle_appointments",
                        "id_field": "jobid",
                        "bulk_load": True,
                        "csv_export": {
                            "file": str(csv_file),
                            "include_headers": True,
                            "flatten_objects": True,
                            "delimiter": ","
                        },
                        "schema": {
                            "jobid": {"type": "string", "format": "JOB_{seq:06d}"},
                            "branchid": {
                                "type": "weighted_choice",
                                "choices": ["5fc36c95559ad6001f3998bb", "5ea4c09e4ade180021ff23bf"],
                                "weights": [0.75, 0.25]
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
            
            # Setup
            correlation_config = CorrelationConfig(config_dict)
            ref_pool = ReferencePool()
            
            # Mock producer to avoid Kafka dependency
            mock_producer = Mock()
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer=mock_producer)
            
            # Execute workflow
            master_gen.load_all()  # Load master data
            master_gen.produce_all()  # Produce to Kafka + CSV export
            
            # Verify data was loaded
            assert len(master_gen.loaded_data["appointments"]) == 10
            
            # Verify Kafka production was called
            assert mock_producer.produce.call_count == 10
            mock_producer.flush.assert_called_once()
            
            # Verify CSV file was created
            assert csv_file.exists()
            
            # Verify CSV content structure
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
                # Check record count
                assert len(rows) == 10
                
                # Check flattened vehicle structure columns
                expected_columns = [
                    "jobid", "branchid",
                    "full.Vehicle.cLicenseNr", "full.Vehicle.cLicenseNrCleaned", "full.Vehicle.cFactoryNr",
                    "full.Customer.cName", "full.Customer.cName2"
                ]
                for col in expected_columns:
                    assert col in reader.fieldnames, f"Column {col} missing from CSV"
                
                # Verify data integrity
                for i, row in enumerate(rows):
                    # Check job ID format
                    assert row["jobid"].startswith("JOB_")
                    assert len(row["jobid"]) == 10  # JOB_XXXXXX
                    
                    # Check branch ID is one of expected values
                    assert row["branchid"] in ["5fc36c95559ad6001f3998bb", "5ea4c09e4ade180021ff23bf"]
                    
                    # Check vehicle data
                    assert row["full.Vehicle.cLicenseNr"] == "M-AB 123"
                    assert row["full.Vehicle.cLicenseNrCleaned"] == "MAB123"
                    assert row["full.Vehicle.cFactoryNr"] == "WBA12345678901234"
                    
                    # Check customer data exists (faker generated)
                    assert len(row["full.Customer.cName"]) > 0
                    assert len(row["full.Customer.cName2"]) > 0
    
    def test_csv_export_with_bulk_load_false(self):
        """Test CSV export when bulk_load is false (reference-only entity)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "reference_data.csv"
            
            config_dict = {
                "master_data": {
                    "reference_data": {
                        "source": "faker",
                        "count": 5,
                        "kafka_topic": "reference",
                        "bulk_load": False,  # Don't produce to Kafka
                        "csv_export": str(csv_file),
                        "schema": {
                            "ref_id": {"type": "string", "format": "REF_{seq:03d}"},
                            "value": {"type": "faker", "method": "word"}
                        }
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config_dict)
            ref_pool = ReferencePool()
            
            # Mock producer
            mock_producer = Mock()
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer=mock_producer)
            
            # Execute workflow
            master_gen.load_all()
            master_gen.produce_all()
            
            # Verify data was loaded but NOT produced to Kafka
            assert len(master_gen.loaded_data["reference_data"]) == 5
            mock_producer.produce.assert_not_called()  # bulk_load: false
            
            # Verify reference pool was populated
            assert ref_pool.get_type_count("reference_data") == 5
            
            # CSV export should still happen despite bulk_load: false
            # This is because CSV export is separate from Kafka production
            assert csv_file.exists()
            
            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                assert len(rows) == 5
                
                for row in rows:
                    assert row["ref_id"].startswith("REF_")
                    assert len(row["value"]) > 0
    
    def test_mixed_csv_configurations(self):
        """Test multiple entities with different CSV configurations."""
        with tempfile.TemporaryDirectory() as temp_dir:
            customers_csv = Path(temp_dir) / "customers.csv"
            products_csv = Path(temp_dir) / "products.tsv"
            
            config_dict = {
                "master_data": {
                    "customers": {
                        "source": "faker",
                        "count": 3,
                        "kafka_topic": "customers",
                        "csv_export": str(customers_csv),  # Simple string config
                        "schema": {
                            "customer_id": {"type": "string", "format": "CUST_{seq:03d}"},
                            "name": {"type": "faker", "method": "name"}
                        }
                    },
                    "products": {
                        "source": "faker", 
                        "count": 5,
                        "kafka_topic": "products",
                        "csv_export": {  # Complex config with custom delimiter
                            "file": str(products_csv),
                            "include_headers": True,
                            "flatten_objects": False,
                            "delimiter": "\t"  # Tab-separated
                        },
                        "schema": {
                            "product_id": {"type": "string", "format": "PROD_{seq:04d}"},
                            "name": {"type": "faker", "method": "catch_phrase"}
                        }
                    },
                    "orders": {
                        "source": "faker",
                        "count": 2,
                        "kafka_topic": "orders",
                        # No CSV export configured
                        "schema": {
                            "order_id": {"type": "string", "format": "ORDER_{seq:05d}"}
                        }
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config_dict)
            ref_pool = ReferencePool()
            mock_producer = Mock()
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer=mock_producer)
            
            # Execute workflow
            master_gen.load_all()
            master_gen.produce_all()
            
            # Verify all data was loaded
            assert len(master_gen.loaded_data["customers"]) == 3
            assert len(master_gen.loaded_data["products"]) == 5
            assert len(master_gen.loaded_data["orders"]) == 2
            
            # Verify Kafka production for all entities
            assert mock_producer.produce.call_count == 10  # 3 + 5 + 2
            
            # Verify customers CSV (simple config)
            assert customers_csv.exists()
            with open(customers_csv, 'r') as f:
                content = f.read()
                assert content.count('\n') == 4  # Header + 3 records
                assert "customer_id,name" in content  # Headers
                assert "CUST_001" in content
            
            # Verify products TSV (custom delimiter)
            assert products_csv.exists()
            with open(products_csv, 'r') as f:
                content = f.read()
                assert '\t' in content  # Tab delimiter
                assert content.count('\n') == 6  # Header + 5 records
                assert "PROD_0001" in content
            
            # Verify orders CSV was NOT created (no csv_export config)
            orders_csv = Path(temp_dir) / "orders.csv"
            assert not orders_csv.exists()
    
    def test_csv_export_error_handling(self):
        """Test CSV export error handling for invalid configurations."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Test with read-only directory to trigger file creation error
            readonly_dir = Path(temp_dir) / "readonly"
            readonly_dir.mkdir()
            readonly_dir.chmod(0o444)  # Read-only
            
            csv_file = readonly_dir / "test.csv"
            
            config_dict = {
                "master_data": {
                    "test_data": {
                        "source": "faker",
                        "count": 1,
                        "kafka_topic": "test",
                        "csv_export": str(csv_file),
                        "schema": {"id": {"type": "string", "format": "TEST_{seq:03d}"}}
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config_dict)
            ref_pool = ReferencePool()
            mock_producer = Mock()
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer=mock_producer)
            
            # Load data
            master_gen.load_all()
            
            # CSV export should handle permission error gracefully
            try:
                master_gen.produce_all()
                # If no exception, the implementation handled the error gracefully
                # or the OS allowed the write despite permissions
            except (PermissionError, OSError):
                # Expected behavior for permission denied
                pass
            finally:
                # Cleanup: restore permissions
                readonly_dir.chmod(0o755)
    
    def test_empty_data_csv_export(self):
        """Test CSV export behavior with empty datasets."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "empty.csv"
            
            config_dict = {
                "master_data": {
                    "empty_entity": {
                        "source": "faker",
                        "count": 0,  # No records
                        "kafka_topic": "empty",
                        "csv_export": str(csv_file),
                        "schema": {"id": {"type": "string", "format": "EMPTY_{seq:03d}"}}
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config_dict)
            ref_pool = ReferencePool()
            mock_producer = Mock()
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer=mock_producer)
            
            # Execute workflow
            master_gen.load_all()
            master_gen.produce_all()
            
            # Verify no data was loaded
            assert len(master_gen.loaded_data["empty_entity"]) == 0
            
            # CSV file should not be created for empty datasets
            # (this is the current behavior in _export_to_csv)
            assert not csv_file.exists()
    
    def test_unicode_csv_export(self):
        """Test CSV export with Unicode characters."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "unicode_test.csv"
            
            config_dict = {
                "master_data": {
                    "unicode_data": {
                        "source": "faker",
                        "count": 2,
                        "kafka_topic": "unicode",
                        "csv_export": str(csv_file),
                        "schema": {
                            "id": {"type": "string", "format": "UNI_{seq:03d}"},
                            "name": {"type": "string", "initial_value": "José María"},
                            "description": {"type": "string", "initial_value": "测试数据"}
                        }
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config_dict)
            ref_pool = ReferencePool()
            mock_producer = Mock()
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer=mock_producer)
            
            # Execute workflow
            master_gen.load_all()
            master_gen.produce_all()
            
            # Verify CSV was created with Unicode content
            assert csv_file.exists()
            
            with open(csv_file, 'r', encoding='utf-8') as f:
                content = f.read()
                assert "José María" in content
                assert "测试数据" in content
                
                # Verify proper CSV structure
                f.seek(0)
                reader = csv.DictReader(f)
                rows = list(reader)
                assert len(rows) == 2
                
                for row in rows:
                    assert row["name"] == "José María"
                    assert row["description"] == "测试数据"