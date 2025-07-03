"""Unit tests for transformation configuration validator."""

import pytest
import tempfile
import yaml
from pathlib import Path
from unittest.mock import Mock, patch
from pydantic import ValidationError

from src.testdatapy.transformers.config_validator import (
    ConfigurationValidator,
    ValidationResult,
    FieldValidationResult,
    validate_config_file,
    validate_config_dict,
    check_config_completeness
)
from src.testdatapy.transformers.config_schema import (
    TransformationConfig,
    ProtobufConfig,
    DataSourceConfig,
    OutputConfig,
    FieldMapping,
    FieldType,
    DataType,
    ValidationRule
)
from src.testdatapy.transformers.function_registry import FunctionRegistry
from src.testdatapy.exceptions import InvalidConfigurationError


class TestConfigurationValidator:
    """Test ConfigurationValidator class."""
    
    def test_validator_initialization(self):
        """Test validator initialization."""
        validator = ConfigurationValidator()
        assert validator.function_registry is None
        assert isinstance(validator._protobuf_cache, dict)
        
        # With function registry
        registry = FunctionRegistry()
        validator = ConfigurationValidator(function_registry=registry)
        assert validator.function_registry is registry
    
    def test_validate_valid_config(self):
        """Test validation of valid configuration."""
        # Create temporary proto file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.proto', delete=False) as f:
            f.write('syntax = "proto3";\n\nmessage TestMessage {\n  string test_field = 1;\n}\n')
            proto_path = f.name
        
        try:
            config = TransformationConfig(
                version="1.0",
                name="Valid Config",
                protobuf=ProtobufConfig(
                    proto_file=proto_path,
                    proto_class="TestMessage"
                ),
                data_source=DataSourceConfig(type="faker"),
                output=OutputConfig(format="binary"),
                field_mappings=[
                    FieldMapping(
                        target="test_field",
                        type=FieldType.COMPUTED,
                        function="faker.name"
                    )
                ]
            )
            
            validator = ConfigurationValidator()
            result = validator.validate_config(config)
            
            assert isinstance(result, ValidationResult)
            assert result.is_valid is True
            assert len(result.errors) == 0
            assert result.validated_config is not None
        finally:
            Path(proto_path).unlink()
    
    def test_validate_config_from_dict(self):
        """Test validation from dictionary."""
        # Create temporary proto file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.proto', delete=False) as f:
            f.write('syntax = "proto3";\n\nmessage TestMessage {\n  string test_field = 1;\n}\n')
            proto_path = f.name
        
        try:
            config_dict = {
                "version": "1.0",
                "name": "Dict Config",
                "protobuf": {
                    "proto_file": proto_path,
                    "proto_class": "TestMessage"
                },
                "data_source": {"type": "faker"},
                "output": {"format": "binary"},
                "field_mappings": [
                    {
                        "target": "test_field",
                        "type": "computed",
                        "function": "faker.name"
                    }
                ]
            }
            
            validator = ConfigurationValidator()
            result = validator.validate_config(config_dict)
            
            assert result.is_valid is True
            assert result.validated_config.name == "Dict Config"
        finally:
            Path(proto_path).unlink()
    
    def test_validate_config_with_errors(self):
        """Test validation of configuration with errors."""
        config = TransformationConfig(
            version="1.0",
            name="Invalid Config",
            protobuf=ProtobufConfig(
                proto_file="/nonexistent/file.proto",
                proto_class="TestMessage"
            ),
            data_source=DataSourceConfig(type="faker"),
            output=OutputConfig(format="binary"),
            field_mappings=[
                FieldMapping(
                    source="source_field",  # Add source to pass Pydantic validation
                    target="invalid_field",
                    type=FieldType.DIRECT
                )
            ]
        )
        
        validator = ConfigurationValidator()
        result = validator.validate_config(config)
        
        assert result.is_valid is False
        assert len(result.errors) > 0
        assert result.validated_config is None
        # Should have error about nonexistent proto file
        assert any("Proto file not found" in error for error in result.errors)
    
    def test_validate_strict_mode(self):
        """Test strict validation mode."""
        # Create temporary proto file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.proto', delete=False) as f:
            f.write('syntax = "proto3";\n\nmessage TestMessage {\n  string test_field = 1;\n}\n')
            proto_path = f.name
        
        try:
            config = TransformationConfig(
                version="1.0",
                name="Strict Test",
                protobuf=ProtobufConfig(
                    proto_file=proto_path,
                    proto_class="TestMessage"
                ),
                data_source=DataSourceConfig(type="faker"),
                output=OutputConfig(format="binary"),
                field_mappings=[
                    FieldMapping(
                        target="test_field",
                        type=FieldType.COMPUTED,
                        function="unknown_function"  # This will generate a warning
                    )
                ]
            )
            
            validator = ConfigurationValidator()
            
            # Normal mode - warnings allowed
            result = validator.validate_config(config, strict=False)
            assert result.is_valid is True
            assert len(result.warnings) > 0
            
            # Strict mode - warnings become errors
            result = validator.validate_config(config, strict=True)
            assert result.is_valid is False
            assert len(result.errors) > 0
        finally:
            Path(proto_path).unlink()


class TestProtobufConfigValidation:
    """Test protobuf configuration validation."""
    
    def test_validate_nonexistent_proto_file(self):
        """Test validation of non-existent proto file."""
        config = TransformationConfig(
            version="1.0",
            name="Test",
            protobuf=ProtobufConfig(
                proto_file="/nonexistent/file.proto",
                proto_class="Test"
            ),
            data_source=DataSourceConfig(type="faker"),
            output=OutputConfig(format="binary"),
            field_mappings=[]
        )
        
        validator = ConfigurationValidator()
        result = validator.validate_config(config)
        
        assert result.is_valid is False
        assert any("Proto file not found" in error for error in result.errors)
    
    def test_validate_proto_file_in_schema_paths(self):
        """Test proto file validation with schema paths."""
        # Create temporary proto file
        with tempfile.TemporaryDirectory() as temp_dir:
            proto_path = Path(temp_dir) / "test.proto"
            proto_path.write_text("syntax = \"proto3\";")
            
            config = TransformationConfig(
                version="1.0",
                name="Test",
                protobuf=ProtobufConfig(
                    proto_file="test.proto",  # Relative path
                    proto_class="Test",
                    schema_paths=[temp_dir]  # Should find file here
                ),
                data_source=DataSourceConfig(type="faker"),
                output=OutputConfig(format="binary"),
                field_mappings=[]
            )
            
            validator = ConfigurationValidator()
            result = validator.validate_config(config)
            
            # Should not have proto file error since it exists in schema path
            proto_file_errors = [error for error in result.errors if "Proto file not found" in error]
            assert len(proto_file_errors) == 0
    
    def test_validate_invalid_schema_paths(self):
        """Test validation of invalid schema paths."""
        config = TransformationConfig(
            version="1.0",
            name="Test",
            protobuf=ProtobufConfig(
                proto_file="test.proto",
                proto_class="Test",
                schema_paths=["/nonexistent/path", "/another/nonexistent"]
            ),
            data_source=DataSourceConfig(type="faker"),
            output=OutputConfig(format="binary"),
            field_mappings=[]
        )
        
        validator = ConfigurationValidator()
        result = validator.validate_config(config)
        
        # Should have warnings about missing schema paths
        assert len(result.warnings) >= 2


class TestDataSourceConfigValidation:
    """Test data source configuration validation."""
    
    def test_validate_csv_data_source(self):
        """Test CSV data source validation."""
        config = TransformationConfig(
            version="1.0",
            name="Test",
            protobuf=ProtobufConfig(proto_file="test.proto", proto_class="Test"),
            data_source=DataSourceConfig(
                type="csv",
                file_path="/nonexistent/file.csv"
            ),
            output=OutputConfig(format="binary"),
            field_mappings=[]
        )
        
        validator = ConfigurationValidator()
        result = validator.validate_config(config)
        
        assert any("Data source file not found" in error for error in result.errors)
    
    def test_validate_csv_without_file_path(self):
        """Test CSV data source without file path."""
        config = TransformationConfig(
            version="1.0",
            name="Test",
            protobuf=ProtobufConfig(proto_file="test.proto", proto_class="Test"),
            data_source=DataSourceConfig(type="csv"),  # Missing file_path
            output=OutputConfig(format="binary"),
            field_mappings=[]
        )
        
        validator = ConfigurationValidator()
        result = validator.validate_config(config)
        
        assert any("file_path required for csv" in error for error in result.errors)
    
    @patch('faker.Faker')
    def test_validate_faker_locale(self, mock_faker):
        """Test Faker locale validation."""
        mock_faker.side_effect = Exception("Invalid locale")
        
        config = TransformationConfig(
            version="1.0",
            name="Test",
            protobuf=ProtobufConfig(proto_file="test.proto", proto_class="Test"),
            data_source=DataSourceConfig(
                type="faker",
                locale="invalid_locale"
            ),
            output=OutputConfig(format="binary"),
            field_mappings=[]
        )
        
        validator = ConfigurationValidator()
        result = validator.validate_config(config)
        
        assert any("Invalid Faker locale" in warning for warning in result.warnings)
    
    def test_validate_custom_data_source(self):
        """Test custom data source validation."""
        config = TransformationConfig(
            version="1.0",
            name="Test",
            protobuf=ProtobufConfig(proto_file="test.proto", proto_class="Test"),
            data_source=DataSourceConfig(
                type="custom",
                class_path="nonexistent.module.Class"
            ),
            output=OutputConfig(format="binary"),
            field_mappings=[]
        )
        
        validator = ConfigurationValidator()
        result = validator.validate_config(config)
        
        assert any("Failed to load custom data source" in error for error in result.errors)


class TestFieldMappingValidation:
    """Test field mapping validation."""
    
    def test_validate_direct_mapping_without_source(self):
        """Test direct mapping validation without source."""
        # Test that Pydantic validation catches missing source field
        with pytest.raises(ValidationError) as exc_info:
            FieldMapping(
                target="test_field",
                type=FieldType.DIRECT
                # Missing source field
            )
        
        # Verify the error message
        assert "Direct mappings require 'source' field" in str(exc_info.value)
    
    def test_validate_computed_mapping_without_function(self):
        """Test computed mapping validation without function."""
        # Test that Pydantic validation catches missing function
        with pytest.raises(ValidationError) as exc_info:
            FieldMapping(
                target="test_field",
                type=FieldType.COMPUTED
                # Missing function
            )
        
        # Verify the error message
        assert "Computed mappings require 'function'" in str(exc_info.value)
    
    def test_validate_duplicate_target_fields(self):
        """Test validation of duplicate target fields."""
        # Test that Pydantic validation catches duplicate target fields
        with pytest.raises(ValidationError) as exc_info:
            TransformationConfig(
                version="1.0",
                name="Test",
                protobuf=ProtobufConfig(proto_file="test.proto", proto_class="Test"),
                data_source=DataSourceConfig(type="faker"),
                output=OutputConfig(format="binary"),
                field_mappings=[
                    FieldMapping(target="duplicate", type=FieldType.COMPUTED, function="faker.name"),
                    FieldMapping(target="duplicate", type=FieldType.COMPUTED, function="faker.email")
                ]
            )
        
        # Verify the error message
        assert "Duplicate target fields" in str(exc_info.value)
    
    def test_validate_function_references(self):
        """Test function reference validation."""
        # Create temporary proto file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.proto', delete=False) as f:
            f.write('syntax = "proto3";\n\nmessage Test {\n  string field1 = 1;\n  string field2 = 2;\n  string field3 = 3;\n}\n')
            proto_path = f.name
        
        try:
            # Test with function registry
            registry = FunctionRegistry()
            registry.register("custom_function", lambda x: x, "Test function")
            
            config = TransformationConfig(
                version="1.0",
                name="Test",
                protobuf=ProtobufConfig(proto_file=proto_path, proto_class="Test"),
                data_source=DataSourceConfig(type="faker"),
                output=OutputConfig(format="binary"),
                field_mappings=[
                    FieldMapping(target="field1", type=FieldType.COMPUTED, function="custom_function"),
                    FieldMapping(target="field2", type=FieldType.COMPUTED, function="unknown_function"),
                    FieldMapping(target="field3", type=FieldType.COMPUTED, function="faker.name")
                ]
            )
            
            validator = ConfigurationValidator(function_registry=registry)
            result = validator.validate_config(config)
            
            # Should have warnings about unknown_function 
            # (The validator may generate multiple warnings for the same function)
            function_warnings = [w for w in result.warnings if "unknown_function" in w]
            assert len(function_warnings) >= 1
        finally:
            Path(proto_path).unlink()


class TestValidationRuleValidation:
    """Test validation rule validation."""
    
    def test_validate_pattern_rule(self):
        """Test pattern validation rule validation."""
        # Valid pattern
        config = TransformationConfig(
            version="1.0",
            name="Test",
            protobuf=ProtobufConfig(proto_file="test.proto", proto_class="Test"),
            data_source=DataSourceConfig(type="faker"),
            output=OutputConfig(format="binary"),
            field_mappings=[
                FieldMapping(
                    target="test_field",
                    type=FieldType.COMPUTED,
                    function="faker.name",
                    validation=[
                        ValidationRule(type="pattern", value="^[A-Z][a-z]+$")
                    ]
                )
            ]
        )
        
        validator = ConfigurationValidator()
        result = validator.validate_config(config)
        
        # Should not have pattern-related errors
        pattern_errors = [e for e in result.errors if "pattern" in e.lower()]
        assert len(pattern_errors) == 0
    
    def test_validate_invalid_pattern_rule(self):
        """Test invalid pattern validation rule."""
        config = TransformationConfig(
            version="1.0",
            name="Test",
            protobuf=ProtobufConfig(proto_file="test.proto", proto_class="Test"),
            data_source=DataSourceConfig(type="faker"),
            output=OutputConfig(format="binary"),
            field_mappings=[
                FieldMapping(
                    target="test_field",
                    type=FieldType.COMPUTED,
                    function="faker.name",
                    validation=[
                        ValidationRule(type="pattern", value="[invalid regex")  # Invalid regex
                    ]
                )
            ]
        )
        
        validator = ConfigurationValidator()
        result = validator.validate_config(config)
        
        assert any("Invalid regex pattern" in error for error in result.errors)


class TestUtilityFunctions:
    """Test utility functions."""
    
    def test_validate_config_file_function(self):
        """Test validate_config_file utility function."""
        # Create temporary proto file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.proto', delete=False) as proto_f:
            proto_f.write('syntax = "proto3";\n\nmessage Test {\n  string field = 1;\n}\n')
            proto_path = proto_f.name
        
        try:
            config_data = {
                "version": "1.0",
                "name": "Test Config",
                "protobuf": {"proto_file": proto_path, "proto_class": "Test"},
                "data_source": {"type": "faker"},
                "output": {"format": "binary"},
                "field_mappings": []
            }
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                yaml.dump(config_data, f)
                config_path = f.name
            
            try:
                result = validate_config_file(config_path)
                assert isinstance(result, ValidationResult)
                assert result.is_valid is True
            finally:
                Path(config_path).unlink()
        finally:
            Path(proto_path).unlink()
    
    def test_validate_config_dict_function(self):
        """Test validate_config_dict utility function."""
        # Create temporary proto file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.proto', delete=False) as proto_f:
            proto_f.write('syntax = "proto3";\n\nmessage Test {\n  string field = 1;\n}\n')
            proto_path = proto_f.name
        
        try:
            config_dict = {
                "version": "1.0",
                "name": "Dict Test",
                "protobuf": {"proto_file": proto_path, "proto_class": "Test"},
                "data_source": {"type": "faker"},
                "output": {"format": "binary"},
                "field_mappings": []
            }
            
            result = validate_config_dict(config_dict)
            assert isinstance(result, ValidationResult)
            assert result.is_valid is True
        finally:
            Path(proto_path).unlink()
    
    def test_check_config_completeness(self):
        """Test configuration completeness checking."""
        # Complete config
        complete_config = TransformationConfig(
            version="1.0",
            name="Complete Config",
            protobuf=ProtobufConfig(proto_file="test.proto", proto_class="Test"),
            data_source=DataSourceConfig(type="faker"),
            output=OutputConfig(
                format="binary",
                topic="test.topic",
                schema_registry_url="http://localhost:8081"
            ),
            field_mappings=[
                FieldMapping(target="field1", type=FieldType.COMPUTED, function="faker.name"),
                FieldMapping(target="field2", type=FieldType.COMPUTED, function="faker.email"),
                FieldMapping(target="field3", type=FieldType.COMPUTED, function="faker.address")
            ]
        )
        
        is_complete, missing = check_config_completeness(complete_config)
        assert is_complete is True
        assert len(missing) == 0
        
        # Incomplete config
        incomplete_config = TransformationConfig(
            version="1.0",
            name="Incomplete Config",
            protobuf=ProtobufConfig(proto_file="test.proto", proto_class="Test"),
            data_source=DataSourceConfig(type="faker"),
            output=OutputConfig(format="binary", topic="test.topic"),  # Missing schema registry
            field_mappings=[
                FieldMapping(target="field1", type=FieldType.COMPUTED, function="faker.name")
            ]  # Too few mappings
        )
        
        is_complete, missing = check_config_completeness(incomplete_config)
        assert is_complete is False
        assert len(missing) > 0
        assert any("schema_registry_url" in item for item in missing)
        assert any("sufficient field mappings" in item for item in missing)