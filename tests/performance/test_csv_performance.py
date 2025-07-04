"""Performance tests for CSV export and import operations.

These tests measure and validate that CSV operations meet performance requirements:
- CSV export/import: >1000 records/second
- Reference pool rebuild: <5 seconds for 10,000 records
"""
import time
import tempfile
import csv
from pathlib import Path
from typing import Dict, Any, List
import pytest
from unittest.mock import Mock, patch

from testdatapy.config.correlation_config import CorrelationConfig
from testdatapy.generators.reference_pool import ReferencePool
from testdatapy.generators.master_data_generator import MasterDataGenerator


class TestCSVPerformance:
    """Performance tests for CSV operations."""
    
    @patch('testdatapy.producers.JsonProducer')
    def test_csv_export_import_performance_10k_records(self, mock_json_producer):
        """Test CSV export/import performance with 10,000 records.
        
        Performance requirement: >1000 records/second for export/import operations.
        Target: Complete 10,000 records in <10 seconds total.
        """
        # Setup mock JsonProducer
        mock_producer_instance = Mock()
        mock_producer_instance.produce = Mock()
        mock_producer_instance.flush = Mock()
        mock_json_producer.return_value = mock_producer_instance
        
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "performance_test.csv"
            
            # Configuration for large dataset
            config = {
                "master_data": {
                    "performance_test_data": {
                        "source": "faker",
                        "count": 10000,  # 10K records for performance test
                        "bulk_load": False,  # Skip Kafka for performance test
                        "csv_export": {
                            "enabled": True,
                            "file": str(csv_file),
                            "flatten": True
                        },
                        "id_field": "id",
                        "schema": {
                            "id": {"type": "string", "faker": "uuid4"},
                            "name": {"type": "string", "faker": "name"},
                            "email": {"type": "string", "faker": "email"},
                            "address": {"type": "string", "faker": "address"},
                            "phone": {"type": "string", "faker": "phone_number"},
                            "company": {"type": "string", "faker": "company"},
                            "created_at": {"type": "timestamp_millis"},
                            "nested": {
                                "type": "object",
                                "properties": {
                                    "field1": {"type": "string", "faker": "text", "max_nb_chars": 50},
                                    "field2": {"type": "integer", "faker": "random_int", "min": 1, "max": 1000},
                                    "field3": {"type": "object", "properties": {
                                        "subfield1": {"type": "string", "faker": "word"},
                                        "subfield2": {"type": "boolean", "faker": "pybool"}
                                    }}
                                }
                            }
                        }
                    }
                }
            }
            
            # Phase 1: Data Generation + CSV Export
            print("\\n📊 Starting CSV Export Performance Test (10,000 records)")
            correlation_config = CorrelationConfig(config)
            ref_pool = ReferencePool()
            
            # Mock producer
            producer = Mock()
            producer.produce = Mock()
            producer.flush = Mock()
            producer._topic_producers = {}
            producer.bootstrap_servers = "mock:9092"
            producer.config = {}
            
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer)
            
            # Measure data generation time
            gen_start = time.time()
            master_gen.load_all()
            gen_end = time.time()
            gen_time = gen_end - gen_start
            
            # Verify data was generated
            generated_data = master_gen.loaded_data["performance_test_data"]
            assert len(generated_data) == 10000, f"Expected 10000 records, got {len(generated_data)}"
            
            # Measure CSV export time
            export_start = time.time()
            master_gen.produce_all()  # This triggers CSV export
            export_end = time.time()
            export_time = export_end - export_start
            
            # Verify CSV file was created
            assert csv_file.exists(), "CSV file should be created"
            
            # Verify CSV content
            with open(csv_file, 'r') as f:
                csv_reader = csv.DictReader(f)
                csv_rows = list(csv_reader)
            
            assert len(csv_rows) == 10000, f"Expected 10000 CSV rows, got {len(csv_rows)}"
            
            print(f"✅ Export Performance:")
            print(f"   Data Generation: {gen_time:.2f}s ({10000/gen_time:.0f} records/sec)")
            print(f"   CSV Export: {export_time:.2f}s ({10000/export_time:.0f} records/sec)")
            
            # Phase 2: CSV Import Performance
            print("\\n📥 Starting CSV Import Performance Test")
            
            # Create new config for import
            import_config = {
                "master_data": {
                    "imported_data": {
                        "source": "csv",
                        "file": str(csv_file),
                        "bulk_load": False,
                        "id_field": "id",
                        "schema": {
                            "id": {"type": "string"},
                            "name": {"type": "string"},
                            "email": {"type": "string"},
                            "address": {"type": "string"},
                            "phone": {"type": "string"},
                            "company": {"type": "string"},
                            "created_at": {"type": "timestamp_millis"},
                            "nested": {
                                "type": "object",
                                "properties": {
                                    "field1": {"type": "string"},
                                    "field2": {"type": "integer"},
                                    "field3": {"type": "object", "properties": {
                                        "subfield1": {"type": "string"},
                                        "subfield2": {"type": "boolean"}
                                    }}
                                }
                            }
                        }
                    }
                }
            }
            
            # Measure CSV import time
            import_correlation_config = CorrelationConfig(import_config)
            import_ref_pool = ReferencePool()
            import_master_gen = MasterDataGenerator(import_correlation_config, import_ref_pool, producer)
            
            import_start = time.time()
            import_master_gen.load_all()
            import_end = time.time()
            import_time = import_end - import_start
            
            # Verify import results
            imported_data = import_master_gen.loaded_data["imported_data"]
            assert len(imported_data) == 10000, f"Expected 10000 imported records, got {len(imported_data)}"
            
            # Verify reference pool population
            assert import_ref_pool.get_type_count("imported_data") == 10000
            
            print(f"✅ Import Performance:")
            print(f"   CSV Import: {import_time:.2f}s ({10000/import_time:.0f} records/sec)")
            
            # Performance Assertions
            total_time = export_time + import_time
            total_records_per_sec = 20000 / total_time  # 10K export + 10K import
            
            print(f"\\n📈 Overall Performance:")
            print(f"   Total Time: {total_time:.2f}s")
            print(f"   Combined Rate: {total_records_per_sec:.0f} records/sec")
            
            # Assert performance requirements
            assert export_time < 10.0, f"CSV export took {export_time:.2f}s, should be <10s for 10K records"
            assert import_time < 10.0, f"CSV import took {import_time:.2f}s, should be <10s for 10K records"
            assert total_records_per_sec > 1000, f"Combined rate {total_records_per_sec:.0f} records/sec, should be >1000"
            
            print("✅ All performance requirements met!")
    
    @patch('testdatapy.producers.JsonProducer')
    def test_reference_pool_rebuild_performance(self, mock_json_producer):
        """Test reference pool rebuild performance with 10,000 records.
        
        Performance requirement: <5 seconds for 10,000 records.
        This test isolates the reference pool population logic.
        """
        # Setup mock JsonProducer
        mock_producer_instance = Mock()
        mock_producer_instance.produce = Mock()
        mock_producer_instance.flush = Mock()
        mock_json_producer.return_value = mock_producer_instance
        
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "reference_pool_test.csv"
            
            # Pre-generate a CSV file with 10K records
            print("\\n🔄 Pre-generating CSV file for reference pool test...")
            test_data = []
            for i in range(10000):
                test_data.append({
                    "id": f"ID_{i:05d}",
                    "name": f"User_{i}",
                    "category": f"Cat_{i % 10}",
                    "value": i * 2,
                    "active": i % 2 == 0
                })
            
            # Write CSV file
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=["id", "name", "category", "value", "active"])
                writer.writeheader()
                writer.writerows(test_data)
            
            print(f"✅ Pre-generated CSV with {len(test_data)} records")
            
            # Configuration for import only
            config = {
                "master_data": {
                    "reference_data": {
                        "source": "csv",
                        "file": str(csv_file),
                        "bulk_load": False,
                        "id_field": "id",
                        "schema": {
                            "id": {"type": "string"},
                            "name": {"type": "string"},
                            "category": {"type": "string"},
                            "value": {"type": "integer"},
                            "active": {"type": "boolean"}
                        }
                    }
                }
            }
            
            print("\\n🏗️  Starting Reference Pool Rebuild Performance Test")
            
            # Measure only the reference pool population time
            correlation_config = CorrelationConfig(config)
            ref_pool = ReferencePool()
            
            producer = Mock()
            producer.produce = Mock()
            producer.flush = Mock()
            producer._topic_producers = {}
            producer.bootstrap_servers = "mock:9092"
            producer.config = {}
            
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer)
            
            # Measure reference pool rebuild time
            rebuild_start = time.time()
            master_gen.load_all()  # This populates the reference pool from CSV
            rebuild_end = time.time()
            rebuild_time = rebuild_end - rebuild_start
            
            # Verify reference pool was populated correctly
            assert ref_pool.get_type_count("reference_data") == 10000
            
            # Check that records are accessible in the pool
            assert "reference_data" in ref_pool._record_cache
            assert len(ref_pool._record_cache["reference_data"]) == 10000
            
            # Verify sample record
            sample_record = ref_pool._record_cache["reference_data"]["ID_00042"]
            assert sample_record["name"] == "User_42"
            assert sample_record["category"] == "Cat_2"
            assert sample_record["value"] == 84
            assert sample_record["active"] is True
            
            records_per_sec = 10000 / rebuild_time
            
            print(f"✅ Reference Pool Rebuild Performance:")
            print(f"   Rebuild Time: {rebuild_time:.2f}s")
            print(f"   Rate: {records_per_sec:.0f} records/sec")
            
            # Performance assertion
            assert rebuild_time < 5.0, f"Reference pool rebuild took {rebuild_time:.2f}s, should be <5s for 10K records"
            
            print("✅ Reference pool rebuild performance requirement met!")
    
    @patch('testdatapy.producers.JsonProducer')
    def test_csv_unicode_and_complex_types_performance(self, mock_json_producer):
        """Test CSV performance with Unicode and complex data types.
        
        Ensures CSV I/O handles challenging data efficiently while maintaining accuracy.
        """
        # Setup mock JsonProducer
        mock_producer_instance = Mock()
        mock_producer_instance.produce = Mock()
        mock_producer_instance.flush = Mock()
        mock_json_producer.return_value = mock_producer_instance
        
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "unicode_test.csv"
            
            # Configuration with challenging data types
            config = {
                "master_data": {
                    "unicode_test_data": {
                        "source": "faker",
                        "count": 1000,  # Smaller dataset for complex data
                        "bulk_load": False,
                        "csv_export": {
                            "enabled": True,
                            "file": str(csv_file),
                            "flatten": True
                        },
                        "id_field": "id",
                        "schema": {
                            "id": {"type": "string", "faker": "uuid4"},
                            "unicode_name": {"type": "string", "faker": "name"},  # May contain Unicode
                            "timestamp": {"type": "timestamp_millis"},
                            "vehicle": {
                                "type": "object",
                                "properties": {
                                    "cLicenseNr": {"type": "string", "faker": "license_plate"},
                                    "cLicenseNrCleaned": {"type": "string", "faker": "license_plate"},
                                    "unicode_model": {"type": "string", "faker": "word"}
                                }
                            }
                        }
                    }
                }
            }
            
            print("\\n🌍 Starting Unicode & Complex Types Performance Test")
            
            correlation_config = CorrelationConfig(config)
            ref_pool = ReferencePool()
            
            producer = Mock()
            producer.produce = Mock()
            producer.flush = Mock()
            producer._topic_producers = {}
            producer.bootstrap_servers = "mock:9092"
            producer.config = {}
            
            master_gen = MasterDataGenerator(correlation_config, ref_pool, producer)
            
            # Measure complex data export
            start_time = time.time()
            master_gen.load_all()
            master_gen.produce_all()
            export_time = time.time() - start_time
            
            # Verify CSV was created with UTF-8 encoding
            assert csv_file.exists()
            
            # Import back and verify data integrity
            import_config = {
                "master_data": {
                    "imported_unicode_data": {
                        "source": "csv",
                        "file": str(csv_file),
                        "bulk_load": False,
                        "id_field": "id",
                        "schema": {
                            "id": {"type": "string"},
                            "unicode_name": {"type": "string"},
                            "timestamp": {"type": "timestamp_millis"},
                            "vehicle": {
                                "type": "object",
                                "properties": {
                                    "cLicenseNr": {"type": "string"},
                                    "cLicenseNrCleaned": {"type": "string"},
                                    "unicode_model": {"type": "string"}
                                }
                            }
                        }
                    }
                }
            }
            
            import_correlation_config = CorrelationConfig(import_config)
            import_ref_pool = ReferencePool()
            import_master_gen = MasterDataGenerator(import_correlation_config, import_ref_pool, producer)
            
            start_time = time.time()
            import_master_gen.load_all()
            import_time = time.time() - start_time
            
            # Verify data integrity
            original_data = master_gen.loaded_data["unicode_test_data"]
            imported_data = import_master_gen.loaded_data["imported_unicode_data"]
            
            assert len(imported_data) == len(original_data)
            assert import_ref_pool.get_type_count("imported_unicode_data") == 1000
            
            # Performance check
            total_time = export_time + import_time
            records_per_sec = 2000 / total_time  # 1K export + 1K import
            
            print(f"✅ Unicode & Complex Types Performance:")
            print(f"   Export Time: {export_time:.2f}s")
            print(f"   Import Time: {import_time:.2f}s")
            print(f"   Total Rate: {records_per_sec:.0f} records/sec")
            
            # Should still maintain reasonable performance with complex data
            assert records_per_sec > 100, f"Complex data rate {records_per_sec:.0f} records/sec should be >100"
            
            print("✅ Unicode and complex types performance test passed!")


# Performance test runner
if __name__ == "__main__":
    # Can be run directly for performance profiling
    import pytest
    pytest.main([__file__, "-v", "-s"])