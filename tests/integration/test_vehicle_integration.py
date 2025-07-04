"""Integration test for vehicle scenario with all enhanced field types."""
import pytest
import tempfile
import yaml
from pathlib import Path
from datetime import datetime
import time

from testdatapy.generators.reference_pool import ReferencePool
from testdatapy.generators.correlated_generator import CorrelatedDataGenerator
from testdatapy.generators.master_data_generator import MasterDataGenerator
from testdatapy.config.correlation_config import CorrelationConfig, ValidationError


class TestVehicleIntegration:
    """Test the complete vehicle scenario with enhanced field types."""
    
    def test_vehicle_field_types_validation(self, tmp_path):
        """Test vehicle field type validation in CorrelationConfig."""
        
        # Create vehicle-style configuration with all new field types
        config_dict = {
            "master_data": {
                "appointments": {
                    "source": "faker",
                    "count": 100,
                    "kafka_topic": "appointments",
                    "id_field": "jobid",
                    "schema": {
                        "jobid": {
                            "type": "string",
                            "format": "JOB_{seq:06d}"
                        },
                        "full": {
                            "type": "object",
                            "properties": {
                                "Vehicle": {
                                    "type": "object", 
                                    "properties": {
                                        "cLicenseNr": {
                                            "type": "string",
                                            "format": "{random_letters:2}-{random_digits:3}-{random_letters:2}"
                                        },
                                        "cLicenseNrCleaned": {
                                            "type": "template",
                                            "template": "{prefix}{digits}{suffix}",
                                            "fields": {
                                                "prefix": {"type": "choice", "choices": ["M", "B", "A"]},
                                                "digits": {"type": "string", "format": "{random_digits:3}"},
                                                "suffix": {"type": "choice", "choices": ["AB", "CD", "EF"]}
                                            }
                                        }
                                    }
                                },
                                "Customer": {
                                    "type": "object",
                                    "properties": {
                                        "cKeyCustomer": {
                                            "type": "string",
                                            "format": "CUST_{seq:06d}"
                                        }
                                    }
                                },
                                "Job": {
                                    "type": "object", 
                                    "properties": {
                                        "cKeyJob": {
                                            "type": "reference",
                                            "source": "self.jobid"
                                        },
                                        "dtStart": {
                                            "type": "timestamp_millis"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "transactional_data": {
                "carinlane_events": {
                    "kafka_topic": "carinlane_events",
                    "rate_per_second": 100,
                    "max_messages": 250,
                    "relationships": {
                        "appointment_plate": {
                            "references": "appointments.full.Vehicle.cLicenseNrCleaned",
                            "percentage": 25
                        }
                    },
                    "derived_fields": {
                        "event_id": {
                            "type": "string",
                            "format": "EVT_{seq:08d}"
                        },
                        "sensor_id": {
                            "type": "conditional",
                            "condition_field": "appointment_plate",
                            "condition_value": None,
                            "when_true": {
                                "type": "weighted_choice",
                                "choices": ["ENTRY_1", "ENTRY_2", "EXIT_1", "EXIT_2"],
                                "weights": [0.3, 0.2, 0.3, 0.2]
                            },
                            "when_false": {
                                "type": "weighted_choice", 
                                "choices": ["RANDOM_1", "RANDOM_2"],
                                "weights": [0.6, 0.4]
                            }
                        },
                        "is_correlated": {
                            "type": "random_boolean",
                            "probability": 0.25
                        },
                        "event_timestamp": {
                            "type": "timestamp_millis",
                            "relative_to_reference": "appointments.full.Job.dtStart",
                            "offset_minutes_min": -30,
                            "offset_minutes_max": 30,
                            "fallback": "now"
                        },
                        "license_plate": {
                            "type": "conditional",
                            "condition_field": "appointment_plate", 
                            "condition_value": None,
                            "when_true": {
                                "type": "reference",
                                "source": "self.appointment_plate"
                            },
                            "when_false": {
                                "type": "string",
                                "format": "{random_letters:2}-{random_digits:3}-{random_letters:2}"
                            }
                        }
                    }
                }
            }
        }
        
        # Test configuration validation
        correlation_config = CorrelationConfig(config_dict)
        
        # Run vehicle-specific validation
        vehicle_validation = correlation_config.get_vehicle_specific_validation()
        
        # Should pass validation
        assert vehicle_validation["valid"], f"Vehicle validation failed: {vehicle_validation['errors']}"
        
        # Check that all vehicle field types are recognized
        field_validation = vehicle_validation["field_type_validation"]
        assert field_validation["valid"], f"Field type validation failed: {field_validation['errors']}"
    
    def test_vehicle_nested_reference_collection(self):
        """Test that nested references are properly collected."""
        
        config_dict = {
            "master_data": {
                "appointments": {
                    "source": "faker",
                    "count": 10,
                    "kafka_topic": "appointments",
                    "id_field": "jobid",
                    "schema": {
                        "jobid": {"type": "string"},
                        "full": {
                            "type": "object",
                            "properties": {
                                "Vehicle": {
                                    "type": "object",
                                    "properties": {
                                        "cLicenseNrCleaned": {"type": "string"}
                                    }
                                },
                                "Job": {
                                    "type": "object",
                                    "properties": {
                                        "dtStart": {"type": "timestamp_millis"}
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "transactional_data": {
                "carinlane_events": {
                    "kafka_topic": "carinlane_events",
                    "relationships": {
                        "appointment_plate": {
                            "references": "appointments.full.Vehicle.cLicenseNrCleaned"
                        }
                    }
                }
            }
        }
        
        # Should not raise ValidationError
        correlation_config = CorrelationConfig(config_dict)
        
        # Verify the nested reference was collected
        available_refs = set()
        correlation_config._collect_nested_references(
            available_refs, 
            "appointments", 
            config_dict["master_data"]["appointments"]["schema"], 
            ""
        )
        
        assert "appointments.full.Vehicle.cLicenseNrCleaned" in available_refs
        assert "appointments.full.Job.dtStart" in available_refs
    
    def test_vehicle_master_data_generation(self):
        """Test vehicle master data generation with nested objects."""
        
        config_dict = {
            "master_data": {
                "appointments": {
                    "source": "faker",
                    "count": 50,
                    "kafka_topic": "appointments",
                    "id_field": "jobid",
                    "schema": {
                        "jobid": {
                            "type": "string",
                            "format": "JOB_{seq:06d}"
                        },
                        "full": {
                            "type": "object",
                            "properties": {
                                "Vehicle": {
                                    "type": "object",
                                    "properties": {
                                        "cLicenseNrCleaned": {
                                            "type": "template",
                                            "template": "{prefix}{digits}{suffix}",
                                            "fields": {
                                                "prefix": {"type": "choice", "choices": ["M", "B"]},
                                                "digits": {"type": "string", "format": "{random_digits:3}"},
                                                "suffix": {"type": "choice", "choices": ["AB", "CD"]}
                                            }
                                        }
                                    }
                                },
                                "Job": {
                                    "type": "object",
                                    "properties": {
                                        "dtStart": {
                                            "type": "timestamp_millis"
                                        }
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
        
        # Create master data generator
        master_gen = MasterDataGenerator(
            config=correlation_config,
            reference_pool=ref_pool
        )
        
        # Load master data
        start_time = time.time()
        master_gen.load_all()
        duration = time.time() - start_time
        
        # Verify data was loaded
        assert ref_pool.get_type_count("appointments") == 50
        
        # Verify loading performance tracking
        loading_stats = master_gen.get_loading_stats()
        assert "appointments" in loading_stats
        assert loading_stats["appointments"]["record_count"] == 50
        assert loading_stats["appointments"]["duration_seconds"] > 0
        assert loading_stats["_total"]["total_records"] == 50
        
        # Verify nested object generation
        sample_data = master_gen.get_sample("appointments", count=5)
        for record in sample_data:
            assert "jobid" in record
            assert record["jobid"].startswith("JOB_")
            assert "full" in record
            assert "Vehicle" in record["full"]
            assert "cLicenseNrCleaned" in record["full"]["Vehicle"]
            assert "Job" in record["full"]
            assert "dtStart" in record["full"]["Job"]
            
            # Verify license plate template format
            license_plate = record["full"]["Vehicle"]["cLicenseNrCleaned"]
            assert len(license_plate) == 6  # prefix(1) + digits(3) + suffix(2)
            assert license_plate[0] in ["M", "B"]  # prefix choices
            assert license_plate[4:6] in ["AB", "CD"]  # suffix choices
            assert license_plate[1:4].isdigit()  # digits part
    
    def test_vehicle_transactional_data_generation(self):
        """Test vehicle transactional data generation with all field types."""
        
        config_dict = {
            "master_data": {
                "appointments": {
                    "source": "faker",
                    "count": 20,
                    "kafka_topic": "appointments",
                    "id_field": "jobid",
                    "schema": {
                        "jobid": {"type": "string", "format": "JOB_{seq:06d}"},
                        "full": {
                            "type": "object",
                            "properties": {
                                "Vehicle": {
                                    "type": "object",
                                    "properties": {
                                        "cLicenseNrCleaned": {"type": "string", "format": "M{random_digits:3}AB"}
                                    }
                                },
                                "Job": {
                                    "type": "object",
                                    "properties": {
                                        "dtStart": {"type": "timestamp_millis"}
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "transactional_data": {
                "carinlane_events": {
                    "kafka_topic": "carinlane_events",
                    "rate_per_second": 10,
                    "max_messages": 50,
                    "relationships": {
                        "appointment_plate": {
                            "references": "appointments.full.Vehicle.cLicenseNrCleaned",
                            "percentage": 25
                        }
                    },
                    "derived_fields": {
                        "event_id": {"type": "string", "format": "EVT_{seq:08d}"},
                        "sensor_id": {
                            "type": "conditional",
                            "condition_field": "appointment_plate",
                            "condition_value": None,
                            "when_true": {"type": "string", "initial_value": "CORRELATED_SENSOR"},
                            "when_false": {"type": "string", "initial_value": "RANDOM_SENSOR"}
                        },
                        "is_correlated": {"type": "random_boolean", "probability": 0.25},
                        "event_timestamp": {
                            "type": "timestamp_millis",
                            "relative_to_reference": "appointments.full.Job.dtStart",
                            "offset_minutes_min": -30,
                            "offset_minutes_max": 30,
                            "fallback": "now"
                        }
                    }
                }
            }
        }
        
        correlation_config = CorrelationConfig(config_dict)
        ref_pool = ReferencePool()
        
        # Load master data first
        master_gen = MasterDataGenerator(
            config=correlation_config,
            reference_pool=ref_pool
        )
        master_gen.load_all()
        
        # Generate transactional data
        generator = CorrelatedDataGenerator(
            entity_type="carinlane_events",
            config=correlation_config,
            reference_pool=ref_pool,
            max_messages=50
        )
        
        # Collect generated records
        records = []
        for record in generator.generate():
            records.append(record)
            if len(records) >= 50:
                break
        
        assert len(records) == 50
        
        # Verify field types work correctly
        correlated_count = 0
        for record in records:
            assert "event_id" in record
            assert record["event_id"].startswith("EVT_")
            assert "sensor_id" in record
            assert "is_correlated" in record
            assert isinstance(record["is_correlated"], bool)
            assert "event_timestamp" in record
            assert isinstance(record["event_timestamp"], int)
            
            # Count correlations
            if record.get("appointment_plate") is not None:
                correlated_count += 1
                # Verify correlated records have correct sensor
                assert record["sensor_id"] == "CORRELATED_SENSOR"
            else:
                # Verify non-correlated records have random sensor
                assert record["sensor_id"] == "RANDOM_SENSOR"
        
        # Verify percentage-based correlation (should be around 25%)
        correlation_ratio = correlated_count / len(records)
        assert 0.15 <= correlation_ratio <= 0.35, f"Correlation ratio {correlation_ratio} not around 25%"
    
    def test_vehicle_reference_pool_optimization(self):
        """Test vehicle reference pool optimizations for large datasets."""
        
        config_dict = {
            "master_data": {
                "appointments": {
                    "source": "faker",
                    "count": 1000,  # Large dataset to trigger optimizations
                    "kafka_topic": "appointments",
                    "id_field": "jobid",
                    "schema": {
                        "jobid": {"type": "string", "format": "JOB_{seq:06d}"},
                        "full": {
                            "type": "object",
                            "properties": {
                                "Vehicle": {
                                    "type": "object",
                                    "properties": {
                                        "cLicenseNrCleaned": {"type": "string", "format": "M{random_digits:3}AB"}
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
        
        # Load master data
        master_gen = MasterDataGenerator(
            config=correlation_config,
            reference_pool=ref_pool
        )
        master_gen.load_all()
        
        # Verify reference pool optimizations were applied
        assert ref_pool.get_type_count("appointments") == 1000
        
        # Verify record caching
        assert hasattr(ref_pool, '_record_cache')
        assert "appointments" in ref_pool._record_cache
        assert len(ref_pool._record_cache["appointments"]) == 1000
        
        # Verify indexing was built (for large datasets)
        assert hasattr(ref_pool, '_indices')
        
        # Test memory usage tracking
        memory_usage = ref_pool.get_memory_usage()
        assert memory_usage["total_references"] == 1000
        assert memory_usage["total_cached_records"] == 1000
        
        # Test batch operations
        batch_refs = ref_pool.get_random_batch("appointments", 100)
        assert len(batch_refs) == 100
        assert len(set(batch_refs)) == 100  # All unique
    
    def test_vehicle_template_field_processing(self):
        """Test vehicle template field processing with complex patterns."""
        
        config_dict = {
            "master_data": {
                "test_entities": {
                    "source": "faker",
                    "count": 10,
                    "kafka_topic": "test",
                    "schema": {
                        "id": {"type": "string", "format": "TEST_{seq:03d}"},
                        "complex_field": {
                            "type": "template",
                            "template": "{site}_{isEntryOrExit}_{lane}_{location}",
                            "fields": {
                                "site": {"type": "choice", "choices": ["SITE_A", "SITE_B"]},
                                "isEntryOrExit": {"type": "choice", "choices": ["ENTRY", "EXIT"]},
                                "lane": {"type": "string", "format": "LANE_{random_digits:2}"},
                                "location": {"type": "choice", "choices": ["NORTH", "SOUTH"]}
                            }
                        }
                    }
                }
            }
        }
        
        correlation_config = CorrelationConfig(config_dict)
        ref_pool = ReferencePool()
        
        master_gen = MasterDataGenerator(
            config=correlation_config,
            reference_pool=ref_pool
        )
        master_gen.load_all()
        
        # Verify template processing
        sample_data = master_gen.get_sample("test_entities", count=10)
        for record in sample_data:
            complex_field = record["complex_field"]
            parts = complex_field.split("_")
            assert len(parts) == 4
            assert parts[0] in ["SITE", "A", "SITE", "B"]  # site part
            assert parts[1] in ["ENTRY", "EXIT"]  # isEntryOrExit
            assert parts[2].startswith("LANE")  # lane part
            assert parts[3] in ["NORTH", "SOUTH"]  # location part

    @pytest.mark.slow
    def test_vehicle_performance_requirements(self):
        """Test vehicle performance requirements for 100K+ records."""
        
        config_dict = {
            "master_data": {
                "appointments": {
                    "source": "faker",
                    "count": 10000,  # Scaled down for test speed
                    "kafka_topic": "appointments",
                    "id_field": "jobid",
                    "schema": {
                        "jobid": {"type": "string", "format": "JOB_{seq:06d}"},
                        "full": {
                            "type": "object",
                            "properties": {
                                "Vehicle": {
                                    "type": "object",
                                    "properties": {
                                        "cLicenseNrCleaned": {"type": "string", "format": "M{random_digits:3}AB"}
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "transactional_data": {
                "carinlane_events": {
                    "kafka_topic": "carinlane_events",
                    "rate_per_second": 1000,
                    "max_messages": 1000,
                    "relationships": {
                        "appointment_plate": {
                            "references": "appointments.full.Vehicle.cLicenseNrCleaned",
                            "percentage": 25
                        }
                    },
                    "derived_fields": {
                        "event_id": {"type": "string", "format": "EVT_{seq:08d}"}
                    }
                }
            }
        }
        
        correlation_config = CorrelationConfig(config_dict)
        ref_pool = ReferencePool()
        
        # Test master data loading performance
        master_gen = MasterDataGenerator(
            config=correlation_config,
            reference_pool=ref_pool
        )
        
        start_time = time.time()
        master_gen.load_all()
        loading_duration = time.time() - start_time
        
        # Verify performance requirements
        loading_stats = master_gen.get_loading_stats()
        appointments_stats = loading_stats["appointments"]
        records_per_second = appointments_stats["records_per_second"]
        
        # Should load at least 1000 records/second
        assert records_per_second >= 1000, f"Loading too slow: {records_per_second} rec/s"
        
        # Test transactional data generation performance
        generator = CorrelatedDataGenerator(
            entity_type="carinlane_events",
            config=correlation_config,
            reference_pool=ref_pool,
            rate_per_second=1000,
            max_messages=1000
        )
        
        start_time = time.time()
        record_count = 0
        for record in generator.generate():
            record_count += 1
            if record_count >= 1000:
                break
        generation_duration = time.time() - start_time
        
        # Should generate at high rate (allowing for overhead)
        actual_rate = record_count / generation_duration
        assert actual_rate >= 500, f"Generation too slow: {actual_rate} rec/s"