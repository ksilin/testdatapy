"""Unit tests for configuration validation."""
import tempfile
import pytest
from pathlib import Path
from unittest.mock import patch

from testdatapy.config.validation import (
    ConfigurationError,
    validate_csv_export_configuration,
    validate_csv_source_configuration,
    validate_bulk_load_configuration,
    validate_schema_consistency,
    validate_correlation_config
)
from testdatapy.config.correlation_config import CorrelationConfig, ValidationError


class TestCSVExportValidation:
    """Test CSV export configuration validation."""
    
    def test_csv_export_disabled_no_validation(self):
        """Test that disabled CSV export skips validation."""
        config = {"csv_export": {"enabled": False}}
        # Should not raise any errors
        validate_csv_export_configuration(config, "test_entity")
    
    def test_csv_export_missing_file_path(self):
        """Test that missing file path raises error."""
        config = {"csv_export": {"enabled": True}}
        with pytest.raises(ConfigurationError) as exc_info:
            validate_csv_export_configuration(config, "test_entity")
        assert "no 'file' path specified" in str(exc_info.value)
        assert "test_entity" in str(exc_info.value)
    
    def test_csv_export_creates_directory(self):
        """Test that validation creates missing directories."""
        with tempfile.TemporaryDirectory() as temp_dir:
            export_file = Path(temp_dir) / "subdir" / "export.csv"
            config = {
                "csv_export": {
                    "enabled": True,
                    "file": str(export_file)
                }
            }
            
            # Directory doesn't exist initially
            assert not export_file.parent.exists()
            
            # Validation should create it
            validate_csv_export_configuration(config, "test_entity")
            
            # Directory should now exist
            assert export_file.parent.exists()
    
    def test_csv_export_unwritable_directory(self):
        """Test that unwritable directory raises error."""
        # This test is platform-specific and might not work on all systems
        config = {
            "csv_export": {
                "enabled": True,
                "file": "/root/protected/export.csv"  # Typically unwritable
            }
        }
        
        with pytest.raises(ConfigurationError) as exc_info:
            validate_csv_export_configuration(config, "test_entity")
        assert "Cannot create directory" in str(exc_info.value) or "not writable" in str(exc_info.value)


class TestCSVSourceValidation:
    """Test CSV source configuration validation."""
    
    def test_non_csv_source_no_validation(self):
        """Test that non-CSV sources skip validation."""
        config = {"source": "faker"}
        # Should not raise any errors
        validate_csv_source_configuration(config, "test_entity")
    
    def test_csv_source_missing_file_path(self):
        """Test that CSV source without file path raises error."""
        config = {"source": "csv"}
        with pytest.raises(ConfigurationError) as exc_info:
            validate_csv_source_configuration(config, "test_entity")
        assert "no 'file' path specified" in str(exc_info.value)
        assert "test_entity" in str(exc_info.value)
    
    def test_csv_source_nonexistent_file(self):
        """Test that nonexistent CSV file raises error."""
        config = {
            "source": "csv",
            "file": "/nonexistent/path/data.csv"
        }
        with pytest.raises(ConfigurationError) as exc_info:
            validate_csv_source_configuration(config, "test_entity")
        assert "does not exist" in str(exc_info.value)
    
    def test_csv_source_directory_instead_of_file(self):
        """Test that directory path raises error."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = {
                "source": "csv",
                "file": temp_dir  # Directory, not file
            }
            with pytest.raises(ConfigurationError) as exc_info:
                validate_csv_source_configuration(config, "test_entity")
            assert "is not a file" in str(exc_info.value)
    
    def test_csv_source_valid_file(self):
        """Test that valid CSV file passes validation."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
            temp_file.write("id,name\\n1,test\\n")
            temp_path = temp_file.name
        
        try:
            config = {
                "source": "csv",
                "file": temp_path
            }
            # Should not raise any errors
            validate_csv_source_configuration(config, "test_entity")
        finally:
            Path(temp_path).unlink()


class TestBulkLoadValidation:
    """Test bulk load configuration validation."""
    
    def test_bulk_load_csv_without_topic(self):
        """Test that bulk_load=true + CSV source without topic raises error."""
        config = {
            "source": "csv",
            "bulk_load": True
            # No kafka_topic
        }
        with pytest.raises(ConfigurationError) as exc_info:
            validate_bulk_load_configuration(config, "test_entity")
        assert "no kafka_topic specified" in str(exc_info.value)
        assert "set bulk_load to false" in str(exc_info.value)
    
    def test_bulk_load_csv_with_topic_valid(self):
        """Test that bulk_load=true + CSV source with topic is valid."""
        config = {
            "source": "csv",
            "bulk_load": True,
            "kafka_topic": "test-topic"
        }
        # Should not raise any errors
        validate_bulk_load_configuration(config, "test_entity")
    
    def test_bulk_load_false_csv_valid(self):
        """Test that bulk_load=false + CSV source is valid."""
        config = {
            "source": "csv",
            "bulk_load": False
        }
        # Should not raise any errors
        validate_bulk_load_configuration(config, "test_entity")
    
    def test_bulk_load_faker_source_valid(self):
        """Test that faker source with any bulk_load setting is valid."""
        config = {
            "source": "faker",
            "bulk_load": True,
            "kafka_topic": "test-topic"
        }
        # Should not raise any errors
        validate_bulk_load_configuration(config, "test_entity")
        
        config["bulk_load"] = False
        # Should not raise any errors
        validate_bulk_load_configuration(config, "test_entity")


class TestSchemaValidation:
    """Test schema configuration validation."""
    
    def test_csv_source_missing_schema(self):
        """Test that CSV source without schema raises error."""
        config = {"source": "csv"}
        with pytest.raises(ConfigurationError) as exc_info:
            validate_schema_consistency(config, "test_entity")
        assert "CSV source requires a schema definition" in str(exc_info.value)
    
    def test_csv_source_missing_id_field_in_schema(self):
        """Test that id_field not in schema raises error."""
        config = {
            "source": "csv",
            "id_field": "user_id",
            "schema": {
                "name": {"type": "string"},
                "email": {"type": "string"}
                # Missing user_id
            }
        }
        with pytest.raises(ConfigurationError) as exc_info:
            validate_schema_consistency(config, "test_entity")
        assert "id_field 'user_id' is not defined in schema" in str(exc_info.value)
    
    def test_csv_source_valid_schema(self):
        """Test that valid CSV schema passes validation."""
        config = {
            "source": "csv",
            "id_field": "user_id",
            "schema": {
                "user_id": {"type": "string"},
                "name": {"type": "string"},
                "email": {"type": "string"}
            }
        }
        # Should not raise any errors
        validate_schema_consistency(config, "test_entity")
    
    def test_faker_source_no_schema_required(self):
        """Test that faker source doesn't require schema."""
        config = {"source": "faker"}
        # Should not raise any errors
        validate_schema_consistency(config, "test_entity")


class TestFullConfigValidation:
    """Test full configuration validation."""
    
    def test_valid_configuration(self):
        """Test that valid configuration passes without errors."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
            temp_file.write("id,name\\n1,test\\n")
            temp_path = temp_file.name
        
        try:
            config = {
                "master_data": {
                    "users": {
                        "source": "csv",
                        "file": temp_path,
                        "bulk_load": False,
                        "id_field": "id",
                        "schema": {
                            "id": {"type": "string"},
                            "name": {"type": "string"}
                        }
                    },
                    "products": {
                        "source": "faker",
                        "count": 100,
                        "kafka_topic": "products",
                        "schema": {
                            "product_id": {"type": "string", "faker": "uuid4"}
                        }
                    }
                },
                "transactional_data": {
                    "orders": {
                        "kafka_topic": "orders",
                        "correlation_fields": {
                            "user_id": {"reference_type": "users"}
                        }
                    }
                }
            }
            
            warnings = validate_correlation_config(config)
            assert isinstance(warnings, list)
            # May have warnings about correlation but no fatal errors
        finally:
            Path(temp_path).unlink()
    
    def test_configuration_with_errors(self):
        """Test that invalid configuration raises errors."""
        config = {
            "master_data": {
                "invalid_entity": {
                    "source": "csv",
                    "bulk_load": True
                    # Missing file and kafka_topic - should cause multiple errors
                }
            }
        }
        
        with pytest.raises(ConfigurationError):
            validate_correlation_config(config)


class TestCorrelationConfigIntegration:
    """Test integration with CorrelationConfig class."""
    
    def test_correlation_config_with_validation_enabled(self):
        """Test CorrelationConfig with validation enabled."""
        config = {
            "master_data": {
                "test_entity": {
                    "source": "csv",
                    "file": "/nonexistent/file.csv",  # This will fail enhanced validation
                    "bulk_load": False,
                    "schema": {
                        "id": {"type": "string"}
                    }
                }
            }
        }
        
        with pytest.raises(ValidationError) as exc_info:
            CorrelationConfig(config, validate=True)
        # Should contain our enhanced validation error message
        assert "Configuration validation failed" in str(exc_info.value) or "does not exist" in str(exc_info.value)
    
    def test_correlation_config_with_validation_disabled(self):
        """Test CorrelationConfig with validation disabled."""
        config = {
            "master_data": {
                "test_entity": {
                    "source": "csv",
                    "bulk_load": True
                    # Missing file and kafka_topic - but validation is disabled
                }
            }
        }
        
        # Should not raise ConfigurationError (may still raise ValidationError from basic validation)
        try:
            CorrelationConfig(config, validate=False)
        except ValidationError as e:
            # Basic validation might still fail, but not enhanced validation
            assert "Configuration validation failed" not in str(e)
    
    def test_correlation_config_valid_enhanced_features(self):
        """Test CorrelationConfig with valid enhanced features."""
        with tempfile.TemporaryDirectory() as temp_dir:
            export_file = Path(temp_dir) / "export.csv"
            
            config = {
                "master_data": {
                    "test_entity": {
                        "source": "faker",
                        "count": 10,
                        "bulk_load": False,
                        "csv_export": {
                            "enabled": True,
                            "file": str(export_file)
                        },
                        "schema": {
                            "id": {"type": "string", "faker": "uuid4"}
                        }
                    }
                }
            }
            
            # Should not raise any errors
            correlation_config = CorrelationConfig(config, validate=True)
            assert correlation_config.config == config