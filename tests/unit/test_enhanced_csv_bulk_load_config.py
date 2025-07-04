"""Comprehensive tests for enhanced CSV export and bulk_load configuration parsing.

This test suite validates the configuration parser's ability to handle:
- csv_export parameters with various formats and options
- bulk_load parameters for selective Kafka production
- Nested CSV configurations with flattening options
- Error handling for invalid configurations
- Integration with vehicle scenario requirements
"""
import pytest
import tempfile
from pathlib import Path
from typing import Dict, Any

from testdatapy.config.correlation_config import CorrelationConfig, ValidationError


class TestCSVExportConfigurationParsing:
    """Test CSV export configuration parsing and validation."""
    
    def test_simple_csv_export_string_configuration(self):
        """Test parsing simple string csv_export configuration."""
        config_dict = {
            "master_data": {
                "customers": {
                    "source": "faker",
                    "count": 10,
                    "kafka_topic": "customers",
                    "csv_export": "data/customers.csv",
                    "schema": {
                        "customer_id": {"type": "string", "format": "CUST_{seq:03d}"},
                        "name": {"type": "faker", "method": "name"}
                    }
                }
            }
        }
        
        # Should parse without errors
        correlation_config = CorrelationConfig(config_dict)
        
        # Verify CSV export configuration is accessible
        csv_config = correlation_config.get_master_config("customers")["csv_export"]
        assert csv_config == "data/customers.csv"
    
    def test_complex_csv_export_dict_configuration(self):
        """Test parsing complex dictionary csv_export configuration."""
        config_dict = {
            "master_data": {
                "appointments": {
                    "source": "faker",
                    "count": 100,
                    "kafka_topic": "vehicle_appointments",
                    "csv_export": {
                        "file": "data/vehicle_appointments.csv",
                        "include_headers": True,
                        "flatten_objects": True,
                        "delimiter": ","
                    },
                    "schema": {
                        "jobid": {"type": "string", "format": "JOB_{seq:06d}"},
                        "branchid": {"type": "choice", "choices": ["branch1", "branch2"]},
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
        
        # Should parse without errors
        correlation_config = CorrelationConfig(config_dict)
        
        # Verify CSV export configuration details
        csv_config = correlation_config.get_master_config("appointments")["csv_export"]
        assert csv_config["file"] == "data/vehicle_appointments.csv"
        assert csv_config["include_headers"] is True
        assert csv_config["flatten_objects"] is True
        assert csv_config["delimiter"] == ","
    
    def test_csv_export_with_custom_delimiter(self):
        """Test CSV export configuration with custom delimiter."""
        config_dict = {
            "master_data": {
                "products": {
                    "source": "faker",
                    "count": 50,
                    "kafka_topic": "products",
                    "csv_export": {
                        "file": "data/products.tsv",
                        "include_headers": True,
                        "flatten_objects": False,
                        "delimiter": "\t"  # Tab-separated values
                    },
                    "schema": {
                        "product_id": {"type": "string", "format": "PROD_{seq:04d}"},
                        "name": {"type": "faker", "method": "catch_phrase"}
                    }
                }
            }
        }
        
        correlation_config = CorrelationConfig(config_dict)
        csv_config = correlation_config.get_master_config("products")["csv_export"]
        assert csv_config["delimiter"] == "\t"
        assert csv_config["flatten_objects"] is False
    
    def test_csv_export_without_headers(self):
        """Test CSV export configuration with headers disabled."""
        config_dict = {
            "master_data": {
                "orders": {
                    "source": "faker",
                    "count": 25,
                    "kafka_topic": "orders",
                    "csv_export": {
                        "file": "data/orders_no_headers.csv",
                        "include_headers": False,
                        "flatten_objects": True
                    },
                    "schema": {
                        "order_id": {"type": "string", "format": "ORDER_{seq:05d}"},
                        "customer_id": {"type": "string", "format": "CUST_{random:03d}"}
                    }
                }
            }
        }
        
        correlation_config = CorrelationConfig(config_dict)
        csv_config = correlation_config.get_master_config("orders")["csv_export"]
        assert csv_config["include_headers"] is False
    
    def test_multiple_entities_with_csv_export(self):
        """Test multiple entities each with different CSV export configurations."""
        config_dict = {
            "master_data": {
                "customers": {
                    "source": "faker",
                    "count": 100,
                    "kafka_topic": "customers",
                    "csv_export": "data/customers.csv",  # Simple string
                    "schema": {"customer_id": {"type": "string", "format": "CUST_{seq:03d}"}}
                },
                "products": {
                    "source": "faker", 
                    "count": 200,
                    "kafka_topic": "products",
                    "csv_export": {  # Complex object
                        "file": "data/products.csv",
                        "include_headers": True,
                        "flatten_objects": False
                    },
                    "schema": {"product_id": {"type": "string", "format": "PROD_{seq:04d}"}}
                },
                "suppliers": {
                    "source": "faker",
                    "count": 50,
                    "kafka_topic": "suppliers",
                    # No csv_export configured
                    "schema": {"supplier_id": {"type": "string", "format": "SUPP_{seq:02d}"}}
                }
            }
        }
        
        correlation_config = CorrelationConfig(config_dict)
        
        # Verify customers CSV config (string)
        customers_config = correlation_config.get_master_config("customers")
        assert customers_config["csv_export"] == "data/customers.csv"
        
        # Verify products CSV config (dict)
        products_config = correlation_config.get_master_config("products")
        assert products_config["csv_export"]["file"] == "data/products.csv"
        
        # Verify suppliers has no CSV config
        suppliers_config = correlation_config.get_master_config("suppliers")
        assert "csv_export" not in suppliers_config


class TestCSVExportConfigurationValidation:
    """Test CSV export configuration validation and error handling."""
    
    def test_csv_export_missing_file_in_dict_config(self):
        """Test validation error when dict csv_export missing required 'file' field."""
        config_dict = {
            "master_data": {
                "invalid_entity": {
                    "source": "faker",
                    "count": 10,
                    "kafka_topic": "invalid",
                    "csv_export": {
                        "include_headers": True,
                        "flatten_objects": True
                        # Missing required 'file' field
                    },
                    "schema": {"id": {"type": "string", "format": "ID_{seq:03d}"}}
                }
            }
        }
        
        with pytest.raises(ValidationError, match="must specify 'file'"):
            CorrelationConfig(config_dict)
    
    def test_csv_export_invalid_delimiter(self):
        """Test validation error for invalid delimiter (not single character)."""
        config_dict = {
            "master_data": {
                "invalid_entity": {
                    "source": "faker",
                    "count": 10,
                    "kafka_topic": "invalid",
                    "csv_export": {
                        "file": "data/test.csv",
                        "delimiter": "||"  # Invalid: more than one character
                    },
                    "schema": {"id": {"type": "string", "format": "ID_{seq:03d}"}}
                }
            }
        }
        
        with pytest.raises(ValidationError, match="delimiter must be a single character"):
            CorrelationConfig(config_dict)
    
    def test_csv_export_invalid_boolean_fields(self):
        """Test validation error for invalid boolean field values."""
        config_dict = {
            "master_data": {
                "invalid_entity": {
                    "source": "faker",
                    "count": 10,
                    "kafka_topic": "invalid",
                    "csv_export": {
                        "file": "data/test.csv",
                        "include_headers": "yes",  # Invalid: should be boolean
                        "flatten_objects": 1  # Invalid: should be boolean
                    },
                    "schema": {"id": {"type": "string", "format": "ID_{seq:03d}"}}
                }
            }
        }
        
        with pytest.raises(ValidationError, match="must be boolean"):
            CorrelationConfig(config_dict)
    
    def test_csv_export_invalid_type(self):
        """Test validation error for completely invalid csv_export type."""
        config_dict = {
            "master_data": {
                "invalid_entity": {
                    "source": "faker",
                    "count": 10,
                    "kafka_topic": "invalid",
                    "csv_export": 123,  # Invalid: not string or dict
                    "schema": {"id": {"type": "string", "format": "ID_{seq:03d}"}}
                }
            }
        }
        
        with pytest.raises(ValidationError, match="Invalid csv_export configuration.*must be string or dict"):
            CorrelationConfig(config_dict)


class TestBulkLoadConfigurationParsing:
    """Test bulk_load configuration parsing and validation."""
    
    def test_bulk_load_true_explicit(self):
        """Test bulk_load: true configuration parsing."""
        config_dict = {
            "master_data": {
                "customers": {
                    "source": "faker",
                    "count": 100,
                    "kafka_topic": "customers",
                    "bulk_load": True,
                    "schema": {"customer_id": {"type": "string", "format": "CUST_{seq:03d}"}}
                }
            }
        }
        
        correlation_config = CorrelationConfig(config_dict)
        customers_config = correlation_config.get_master_config("customers")
        assert customers_config["bulk_load"] is True
    
    def test_bulk_load_false_explicit(self):
        """Test bulk_load: false configuration parsing."""
        config_dict = {
            "master_data": {
                "reference_data": {
                    "source": "csv",
                    "file": "data/reference.csv",
                    "kafka_topic": "reference_data",
                    "bulk_load": False,  # Load into reference pool only
                    "schema": {"ref_id": {"type": "string"}}
                }
            }
        }
        
        correlation_config = CorrelationConfig(config_dict)
        ref_config = correlation_config.get_master_config("reference_data")
        assert ref_config["bulk_load"] is False
    
    def test_bulk_load_default_behavior_with_kafka_topic(self):
        """Test default bulk_load behavior when kafka_topic is present."""
        config_dict = {
            "master_data": {
                "products": {
                    "source": "faker",
                    "count": 50,
                    "kafka_topic": "products",
                    # bulk_load not specified - should default based on presence of kafka_topic
                    "schema": {"product_id": {"type": "string", "format": "PROD_{seq:03d}"}}
                }
            }
        }
        
        correlation_config = CorrelationConfig(config_dict)
        products_config = correlation_config.get_master_config("products")
        # bulk_load not explicitly set, but should be inferred from kafka_topic presence
        # The actual default logic is in MasterDataGenerator, not config parser
        assert "kafka_topic" in products_config
    
    def test_bulk_load_with_csv_export_combination(self):
        """Test bulk_load: false combined with csv_export configuration."""
        config_dict = {
            "master_data": {
                "staging_data": {
                    "source": "faker",
                    "count": 1000,
                    "kafka_topic": "staging_data",
                    "bulk_load": False,  # Don't produce to Kafka
                    "csv_export": {
                        "file": "data/staging_export.csv",
                        "include_headers": True,
                        "flatten_objects": True
                    },
                    "schema": {
                        "record_id": {"type": "string", "format": "REC_{seq:06d}"},
                        "data": {"type": "faker", "method": "text"}
                    }
                }
            }
        }
        
        correlation_config = CorrelationConfig(config_dict)
        staging_config = correlation_config.get_master_config("staging_data")
        
        # Verify both configurations are preserved
        assert staging_config["bulk_load"] is False
        assert staging_config["csv_export"]["file"] == "data/staging_export.csv"
        assert staging_config["kafka_topic"] == "staging_data"
    
    def test_multiple_entities_mixed_bulk_load_settings(self):
        """Test multiple entities with different bulk_load settings."""
        config_dict = {
            "master_data": {
                "active_customers": {
                    "source": "faker",
                    "count": 500,
                    "kafka_topic": "customers",
                    "bulk_load": True,  # Produce to Kafka
                    "schema": {"customer_id": {"type": "string", "format": "CUST_{seq:04d}"}}
                },
                "lookup_data": {
                    "source": "csv",
                    "file": "data/lookup.csv",
                    "kafka_topic": "lookup_data", 
                    "bulk_load": False,  # Reference only, don't produce
                    "schema": {"lookup_id": {"type": "string"}}
                },
                "archive_data": {
                    "source": "faker",
                    "count": 100,
                    # No kafka_topic - purely for reference pool
                    "bulk_load": False,
                    "csv_export": "data/archive.csv",
                    "schema": {"archive_id": {"type": "string", "format": "ARCH_{seq:03d}"}}
                }
            }
        }
        
        correlation_config = CorrelationConfig(config_dict)
        
        # Verify individual configurations
        active_config = correlation_config.get_master_config("active_customers")
        assert active_config["bulk_load"] is True
        assert "kafka_topic" in active_config
        
        lookup_config = correlation_config.get_master_config("lookup_data")
        assert lookup_config["bulk_load"] is False
        assert "kafka_topic" in lookup_config
        
        archive_config = correlation_config.get_master_config("archive_data")
        assert archive_config["bulk_load"] is False
        assert "kafka_topic" not in archive_config
        assert archive_config["csv_export"] == "data/archive.csv"


class TestVehicleScenarioConfiguration:
    """Test CSV export and bulk_load configuration with vehicle scenario requirements."""
    
    def test_vehicle_sequential_generation_configuration(self):
        """Test vehicle scenario: appointments with CSV export, then CarInLane with bulk_load: false."""
        config_dict = {
            "master_data": {
                "appointments": {
                    "source": "faker",
                    "count": 1000,
                    "kafka_topic": "vehicle_appointments",
                    "id_field": "jobid",
                    "key_field": "branchid",
                    "bulk_load": True,  # Produce to Kafka in first stage
                    "csv_export": {
                        "file": "data/vehicle_appointments.csv",
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
                                        "cLicenseNr": {"type": "faker", "method": "license_plate"},
                                        "cLicenseNrCleaned": {"type": "reference", "source": "self.full.Vehicle.cLicenseNr"},
                                        "cFactoryNr": {"type": "faker", "method": "vin"}
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
                },
                "appointments_from_csv": {
                    "source": "csv",
                    "file": "data/vehicle_appointments.csv",  # Load from CSV in second stage
                    "bulk_load": False,  # Don't re-produce to Kafka
                    "schema": {
                        "jobid": {"type": "string"},
                        "branchid": {"type": "string"},
                        "full.Vehicle.cLicenseNr": {"type": "string"},
                        "full.Vehicle.cLicenseNrCleaned": {"type": "string"},
                        "full.Vehicle.cFactoryNr": {"type": "string"},
                        "full.Customer.cName": {"type": "string"},
                        "full.Customer.cName2": {"type": "string"}
                    }
                }
            },
            "transactional_data": {
                "carinlane": {
                    "kafka_topic": "vehicle_carinlane",
                    "count": 2000,
                    "relationships": {
                        "appointment_id": {
                            "references": "appointments_from_csv.jobid",
                            "percentage": 80  # 80% correlation rate
                        }
                    },
                    "schema": {
                        "sensorid": {
                            "type": "template",
                            "template": "{site}_{isEntryOrExit}_{lane}_{location}",
                            "fields": {
                                "site": {"type": "reference", "source": "appointments_from_csv.branchid"},
                                "isEntryOrExit": {"type": "choice", "choices": ["Entry", "Exit"]},
                                "lane": {"type": "choice", "choices": ["Lane1", "Lane2", "Lane3"]},
                                "location": {"type": "choice", "choices": ["A", "B", "C"]}
                            }
                        },
                        "cLicenseNr": {
                            "type": "reference",
                            "source": "appointments_from_csv.full.Vehicle.cLicenseNr"
                        }
                    }
                }
            }
        }
        
        # Should parse without errors
        correlation_config = CorrelationConfig(config_dict)
        
        # Verify appointments configuration (stage 1: generate + export + produce)
        appointments_config = correlation_config.get_master_config("appointments")
        assert appointments_config["bulk_load"] is True
        assert appointments_config["csv_export"]["file"] == "data/vehicle_appointments.csv"
        assert appointments_config["csv_export"]["flatten_objects"] is True
        
        # Verify CSV-loaded appointments configuration (stage 2: load only, no produce)
        csv_appointments_config = correlation_config.get_master_config("appointments_from_csv")
        assert csv_appointments_config["bulk_load"] is False
        assert csv_appointments_config["source"] == "csv"
        assert csv_appointments_config["file"] == "data/vehicle_appointments.csv"
        
        # Verify transactional data can reference CSV-loaded master data
        carinlane_config = correlation_config.get_transaction_config("carinlane")
        appointment_rel = carinlane_config["relationships"]["appointment_id"]
        assert appointment_rel["references"] == "appointments_from_csv.jobid"
        assert appointment_rel["percentage"] == 80
    
    def test_vehicle_flattened_csv_field_references(self):
        """Test that vehicle scenario can reference flattened CSV fields correctly."""
        config_dict = {
            "master_data": {
                "appointments_csv": {
                    "source": "csv",
                    "file": "data/vehicle_appointments_flat.csv",
                    "bulk_load": False,
                    "schema": {
                        "jobid": {"type": "string"},
                        "branchid": {"type": "string"},
                        # Flattened vehicle fields from CSV
                        "full.Vehicle.cLicenseNr": {"type": "string"},
                        "full.Vehicle.cLicenseNrCleaned": {"type": "string"},
                        "full.Customer.cName": {"type": "string"},
                        "full.Customer.cName2": {"type": "string"}
                    }
                }
            },
            "transactional_data": {
                "carinlane": {
                    "kafka_topic": "vehicle_carinlane",
                    "count": 800,
                    "relationships": {
                        "appointment_id": {
                            "references": "appointments_csv.jobid",
                            "percentage": 80
                        }
                    },
                    "schema": {
                        "cLicenseNr": {
                            "type": "reference",
                            "source": "appointments_csv.full.Vehicle.cLicenseNr"  # Reference flattened field
                        },
                        "cLicenseNrCleaned": {
                            "type": "reference", 
                            "source": "appointments_csv.full.Vehicle.cLicenseNrCleaned"
                        }
                    }
                }
            }
        }
        
        # Should parse and validate references correctly
        correlation_config = CorrelationConfig(config_dict)
        
        # Verify transactional data configuration
        carinlane_config = correlation_config.get_transaction_config("carinlane")
        assert carinlane_config["schema"]["cLicenseNr"]["source"] == "appointments_csv.full.Vehicle.cLicenseNr"
        assert carinlane_config["schema"]["cLicenseNrCleaned"]["source"] == "appointments_csv.full.Vehicle.cLicenseNrCleaned"


class TestConfigurationEdgeCases:
    """Test edge cases and complex configuration scenarios."""
    
    def test_csv_export_with_relative_paths(self):
        """Test CSV export with relative file paths."""
        config_dict = {
            "master_data": {
                "test_data": {
                    "source": "faker",
                    "count": 10,
                    "kafka_topic": "test",
                    "csv_export": "../output/test_data.csv",  # Relative path
                    "schema": {"id": {"type": "string", "format": "TEST_{seq:03d}"}}
                }
            }
        }
        
        # Should parse without errors (path validation creates dirs)
        correlation_config = CorrelationConfig(config_dict)
        test_config = correlation_config.get_master_config("test_data")
        assert test_config["csv_export"] == "../output/test_data.csv"
    
    def test_csv_export_with_unicode_file_path(self):
        """Test CSV export with Unicode characters in file path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            unicode_path = f"{temp_dir}/dãta_ñámé.csv"
            
            config_dict = {
                "master_data": {
                    "unicode_data": {
                        "source": "faker",
                        "count": 5,
                        "kafka_topic": "unicode",
                        "csv_export": unicode_path,
                        "schema": {"id": {"type": "string", "format": "UNI_{seq:03d}"}}
                    }
                }
            }
            
            # Should handle Unicode paths correctly
            correlation_config = CorrelationConfig(config_dict)
            unicode_config = correlation_config.get_master_config("unicode_data")
            assert unicode_config["csv_export"] == unicode_path
    
    def test_empty_csv_export_configuration(self):
        """Test that entities without csv_export work normally."""
        config_dict = {
            "master_data": {
                "normal_entity": {
                    "source": "faker",
                    "count": 100,
                    "kafka_topic": "normal",
                    "bulk_load": True,
                    # No csv_export configured
                    "schema": {"id": {"type": "string", "format": "NORM_{seq:03d}"}}
                }
            }
        }
        
        correlation_config = CorrelationConfig(config_dict)
        normal_config = correlation_config.get_master_config("normal_entity")
        
        # Should not have csv_export field
        assert "csv_export" not in normal_config
        assert normal_config["bulk_load"] is True
    
    def test_bulk_load_with_no_kafka_topic(self):
        """Test bulk_load: false with no kafka_topic (reference-only entity)."""
        config_dict = {
            "master_data": {
                "reference_only": {
                    "source": "faker",
                    "count": 50,
                    # No kafka_topic - purely for reference pool
                    "bulk_load": False,
                    "csv_export": "data/reference_only.csv",
                    "schema": {"ref_id": {"type": "string", "format": "REF_{seq:03d}"}}
                }
            }
        }
        
        correlation_config = CorrelationConfig(config_dict)
        ref_config = correlation_config.get_master_config("reference_only")
        
        # Should be valid configuration
        assert ref_config["bulk_load"] is False
        assert "kafka_topic" not in ref_config
        assert ref_config["csv_export"] == "data/reference_only.csv"