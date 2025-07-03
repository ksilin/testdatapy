"""Unit tests for transformation configuration schema."""

import pytest
import tempfile
import yaml
from pathlib import Path
from pydantic import ValidationError

from src.testdatapy.transformers.config_schema import (
    TransformationConfig,
    FieldMapping,
    ProtobufConfig,
    DataSourceConfig,
    OutputConfig,
    FieldType,
    DataType,
    ValidationRule,
    ConditionalMapping,
    NestedFieldMapping,
    FunctionRegistry,
    BasicTransformationConfig,
    validate_config_file,
    generate_config_template
)


class TestValidationRule:
    """Test ValidationRule model."""
    
    def test_pattern_validation_rule(self):
        """Test pattern validation rule creation."""
        rule = ValidationRule(
            type="pattern",
            value="^[A-Z]{3}[0-9]{4}$",
            message="Invalid format"
        )
        
        assert rule.type == "pattern"
        assert rule.value == "^[A-Z]{3}[0-9]{4}$"
        assert rule.message == "Invalid format"
    
    def test_min_max_validation_rule(self):
        """Test min/max validation rules."""
        min_rule = ValidationRule(type="min", value=0)
        max_rule = ValidationRule(type="max", value=100)
        
        assert min_rule.type == "min"
        assert min_rule.value == 0
        assert max_rule.type == "max"
        assert max_rule.value == 100
    
    def test_enum_validation_rule(self):
        """Test enum validation rule."""
        rule = ValidationRule(
            type="enum",
            value=["ACTIVE", "INACTIVE", "PENDING"]
        )
        
        assert rule.type == "enum"
        assert rule.value == ["ACTIVE", "INACTIVE", "PENDING"]
    
    def test_invalid_validation_rule(self):
        """Test validation rule with invalid value type."""
        with pytest.raises(ValidationError):
            ValidationRule(type="pattern", value=123)  # Should be string
        
        with pytest.raises(ValidationError):
            ValidationRule(type="min", value="invalid")  # Should be numeric


class TestConditionalMapping:
    """Test ConditionalMapping model."""
    
    def test_conditional_mapping_with_value(self):
        """Test conditional mapping with static value."""
        mapping = ConditionalMapping(
            condition="field1 == 'value'",
            field="target_field",
            value="static_value"
        )
        
        assert mapping.condition == "field1 == 'value'"
        assert mapping.field == "target_field"
        assert mapping.value == "static_value"
        assert mapping.function is None
    
    def test_conditional_mapping_with_function(self):
        """Test conditional mapping with function."""
        mapping = ConditionalMapping(
            condition="amount > 1000",
            field="category",
            function="faker.random_element",
            arguments={"elements": ["HIGH", "LOW"]}
        )
        
        assert mapping.condition == "amount > 1000"
        assert mapping.function == "faker.random_element"
        assert mapping.arguments == {"elements": ["HIGH", "LOW"]}
        assert mapping.value is None
    
    def test_conditional_mapping_validation_errors(self):
        """Test conditional mapping validation errors."""
        # Must provide either value or function
        with pytest.raises(ValidationError):
            ConditionalMapping(
                condition="field1 == 'value'",
                field="target_field"
            )
        
        # Cannot provide both value and function
        with pytest.raises(ValidationError):
            ConditionalMapping(
                condition="field1 == 'value'",
                field="target_field",
                value="static_value",
                function="faker.name"
            )


class TestNestedFieldMapping:
    """Test NestedFieldMapping model."""
    
    def test_nested_field_mapping(self):
        """Test nested field mapping creation."""
        mapping = NestedFieldMapping(
            source_path="parent.child.field",
            target_path="output.nested.field",
            type=DataType.STRING,
            required=True,
            transformation="faker.name"
        )
        
        assert mapping.source_path == "parent.child.field"
        assert mapping.target_path == "output.nested.field"
        assert mapping.type == DataType.STRING
        assert mapping.required is True
        assert mapping.transformation == "faker.name"
    
    def test_invalid_field_paths(self):
        """Test validation of field paths."""
        # Invalid source path
        with pytest.raises(ValidationError):
            NestedFieldMapping(
                source_path="invalid-path-with-hyphens",
                target_path="valid.path",
                type=DataType.STRING
            )
        
        # Invalid target path
        with pytest.raises(ValidationError):
            NestedFieldMapping(
                source_path="valid.path",
                target_path="invalid path with spaces",
                type=DataType.STRING
            )


class TestFieldMapping:
    """Test FieldMapping model."""
    
    def test_direct_field_mapping(self):
        """Test direct field mapping."""
        mapping = FieldMapping(
            source="source_field",
            target="target_field",
            type=FieldType.DIRECT,
            data_type=DataType.STRING,
            required=True
        )
        
        assert mapping.source == "source_field"
        assert mapping.target == "target_field"
        assert mapping.type == FieldType.DIRECT
        assert mapping.data_type == DataType.STRING
        assert mapping.required is True
    
    def test_computed_field_mapping(self):
        """Test computed field mapping."""
        mapping = FieldMapping(
            target="computed_field",
            type=FieldType.COMPUTED,
            function="faker.name",
            arguments={"locale": "en_US"},
            description="Generate random name"
        )
        
        assert mapping.target == "computed_field"
        assert mapping.type == FieldType.COMPUTED
        assert mapping.function == "faker.name"
        assert mapping.arguments == {"locale": "en_US"}
        assert mapping.description == "Generate random name"
    
    def test_conditional_field_mapping(self):
        """Test conditional field mapping."""
        conditions = [
            ConditionalMapping(
                condition="type == 'premium'",
                field="discount",
                value=0.1
            ),
            ConditionalMapping(
                condition="type == 'standard'",
                field="discount",
                value=0.05
            )
        ]
        
        mapping = FieldMapping(
            target="discount_rate",
            type=FieldType.CONDITIONAL,
            conditions=conditions
        )
        
        assert mapping.target == "discount_rate"
        assert mapping.type == FieldType.CONDITIONAL
        assert len(mapping.conditions) == 2
    
    def test_field_mapping_validation(self):
        """Test field mapping validation rules."""
        # Direct mapping without source should fail
        with pytest.raises(ValidationError):
            FieldMapping(
                target="target_field",
                type=FieldType.DIRECT
                # Missing source field
            )
        
        # Computed mapping without function should fail
        with pytest.raises(ValidationError):
            FieldMapping(
                target="target_field",
                type=FieldType.COMPUTED
                # Missing function
            )
        
        # Conditional mapping without conditions should fail
        with pytest.raises(ValidationError):
            FieldMapping(
                target="target_field",
                type=FieldType.CONDITIONAL
                # Missing conditions
            )
    
    def test_invalid_target_field_name(self):
        """Test validation of target field names."""
        with pytest.raises(ValidationError):
            FieldMapping(
                target="invalid-field-name",  # Hyphens not allowed
                type=FieldType.COMPUTED,
                function="faker.name"
            )


class TestProtobufConfig:
    """Test ProtobufConfig model."""
    
    def test_protobuf_config_with_proto_file(self):
        """Test protobuf config with proto file."""
        config = ProtobufConfig(
            proto_file="schemas/example.proto",
            schema_paths=["schemas/", "protos/"],
            strict_mode=True
        )
        
        assert config.proto_file == "schemas/example.proto"
        assert config.schema_paths == ["schemas/", "protos/"]
        assert config.strict_mode is True
    
    def test_protobuf_config_with_module_class(self):
        """Test protobuf config with module and class."""
        config = ProtobufConfig(
            proto_module="generated.example_pb2",
            proto_class="ExampleMessage",
            preserve_unknown_fields=True
        )
        
        assert config.proto_module == "generated.example_pb2"
        assert config.proto_class == "ExampleMessage"
        assert config.preserve_unknown_fields is True
    
    def test_protobuf_config_validation(self):
        """Test protobuf config validation rules."""
        # Must provide either proto_file or (proto_module + proto_class)
        with pytest.raises(ValidationError):
            ProtobufConfig()  # Missing required fields
        
        # proto_module without proto_class should fail
        with pytest.raises(ValidationError):
            ProtobufConfig(proto_module="example_pb2")


class TestDataSourceConfig:
    """Test DataSourceConfig model."""
    
    def test_faker_data_source(self):
        """Test Faker data source configuration."""
        config = DataSourceConfig(
            type="faker",
            locale="de_DE",
            seed=12345,
            providers=["faker_automotive"]
        )
        
        assert config.type == "faker"
        assert config.locale == "de_DE"
        assert config.seed == 12345
        assert config.providers == ["faker_automotive"]
    
    def test_csv_data_source(self):
        """Test CSV data source configuration."""
        config = DataSourceConfig(
            type="csv",
            file_path="data/test.csv",
            delimiter=";",
            header_row=False,
            encoding="utf-8"
        )
        
        assert config.type == "csv"
        assert config.file_path == "data/test.csv"
        assert config.delimiter == ";"
        assert config.header_row is False
        assert config.encoding == "utf-8"
    
    def test_custom_data_source(self):
        """Test custom data source configuration."""
        config = DataSourceConfig(
            type="custom",
            class_path="my_module.CustomDataSource",
            parameters={"param1": "value1", "param2": 42}
        )
        
        assert config.type == "custom"
        assert config.class_path == "my_module.CustomDataSource"
        assert config.parameters == {"param1": "value1", "param2": 42}


class TestOutputConfig:
    """Test OutputConfig model."""
    
    def test_kafka_output_config(self):
        """Test Kafka output configuration."""
        config = OutputConfig(
            format="binary",
            topic="test.topic",
            key_field="id",
            schema_registry_url="http://localhost:8081",
            subject_name="test-topic-value",
            batch_size=1000
        )
        
        assert config.format == "binary"
        assert config.topic == "test.topic"
        assert config.key_field == "id"
        assert config.schema_registry_url == "http://localhost:8081"
        assert config.subject_name == "test-topic-value"
        assert config.batch_size == 1000
    
    def test_file_output_config(self):
        """Test file output configuration."""
        config = OutputConfig(
            format="json",
            output_file="output/data.json",
            batch_size=500
        )
        
        assert config.format == "json"
        assert config.output_file == "output/data.json"
        assert config.batch_size == 500


class TestTransformationConfig:
    """Test complete TransformationConfig model."""
    
    def test_valid_transformation_config(self):
        """Test valid transformation configuration."""
        config = TransformationConfig(
            version="1.0",
            name="Test Configuration",
            description="Test transformation config",
            protobuf=ProtobufConfig(
                proto_file="test.proto",
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
        
        assert config.version == "1.0"
        assert config.name == "Test Configuration"
        assert len(config.field_mappings) == 1
        assert config.enable_validation is True
    
    def test_version_validation(self):
        """Test version format validation."""
        # Valid versions
        valid_versions = ["1.0", "2.1", "1.0.0", "2.1.3"]
        for version in valid_versions:
            config = TransformationConfig(
                version=version,
                name="Test",
                protobuf=ProtobufConfig(proto_file="test.proto", proto_class="Test"),
                data_source=DataSourceConfig(type="faker"),
                output=OutputConfig(format="binary"),
                field_mappings=[]
            )
            assert config.version == version
        
        # Invalid versions
        with pytest.raises(ValidationError):
            TransformationConfig(
                version="invalid",
                name="Test",
                protobuf=ProtobufConfig(proto_file="test.proto", proto_class="Test"),
                data_source=DataSourceConfig(type="faker"),
                output=OutputConfig(format="binary"),
                field_mappings=[]
            )
    
    def test_duplicate_target_fields(self):
        """Test validation of duplicate target fields."""
        with pytest.raises(ValidationError):
            TransformationConfig(
                version="1.0",
                name="Test",
                protobuf=ProtobufConfig(proto_file="test.proto", proto_class="Test"),
                data_source=DataSourceConfig(type="faker"),
                output=OutputConfig(format="binary"),
                field_mappings=[
                    FieldMapping(target="duplicate_field", type=FieldType.COMPUTED, function="faker.name"),
                    FieldMapping(target="duplicate_field", type=FieldType.COMPUTED, function="faker.email")
                ]
            )


class TestBasicTransformationConfig:
    """Test BasicTransformationConfig simplified model."""
    
    def test_basic_config(self):
        """Test basic transformation configuration."""
        config = BasicTransformationConfig(
            proto_file="user.proto",
            proto_class="User",
            mappings=[
                {"source": "name", "target": "full_name", "type": "string"},
                {"source": "age", "target": "user_age", "type": "int32", "default": 18}
            ]
        )
        
        assert config.proto_file == "user.proto"
        assert config.proto_class == "User"
        assert len(config.mappings) == 2
        assert config.data_source == "faker"


class TestConfigUtilities:
    """Test configuration utility functions."""
    
    def test_validate_config_file(self):
        """Test configuration file validation."""
        config_data = {
            "version": "1.0",
            "name": "Test Config",
            "protobuf": {
                "proto_file": "test.proto",
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
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            config_path = f.name
        
        try:
            config = validate_config_file(config_path)
            assert isinstance(config, TransformationConfig)
            assert config.name == "Test Config"
        finally:
            Path(config_path).unlink()
    
    def test_validate_nonexistent_config_file(self):
        """Test validation of non-existent config file."""
        with pytest.raises(FileNotFoundError):
            validate_config_file("/nonexistent/config.yaml")
    
    def test_generate_config_template(self):
        """Test configuration template generation."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            template_path = f.name
        
        try:
            # Test different template types
            for template_type in ["full", "basic", "minimal"]:
                generate_config_template(template_path, template_type)
                assert Path(template_path).exists()
                
                # Verify the file contains valid YAML
                with open(template_path, 'r') as f:
                    template_data = yaml.safe_load(f)
                
                assert isinstance(template_data, dict)
                
                if template_type == "full":
                    # Full template should be a valid TransformationConfig
                    config = TransformationConfig(**template_data)
                    assert config.name == "Example Transformation"
        
        finally:
            if Path(template_path).exists():
                Path(template_path).unlink()


class TestConfigSerialization:
    """Test configuration serialization and deserialization."""
    
    def test_config_to_dict(self):
        """Test configuration serialization to dictionary."""
        config = TransformationConfig(
            version="1.0",
            name="Serialization Test",
            protobuf=ProtobufConfig(proto_file="test.proto", proto_class="Test"),
            data_source=DataSourceConfig(type="faker"),
            output=OutputConfig(format="binary"),
            field_mappings=[
                FieldMapping(target="test_field", type=FieldType.COMPUTED, function="faker.name")
            ]
        )
        
        config_dict = config.model_dump(exclude_unset=True)
        
        assert config_dict["version"] == "1.0"
        assert config_dict["name"] == "Serialization Test"
        assert "protobuf" in config_dict
        assert "field_mappings" in config_dict
    
    def test_config_from_dict(self):
        """Test configuration deserialization from dictionary."""
        config_dict = {
            "version": "1.0",
            "name": "Deserialization Test",
            "protobuf": {
                "proto_file": "test.proto",
                "proto_class": "Test"
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
        
        config = TransformationConfig(**config_dict)
        
        assert config.version == "1.0"
        assert config.name == "Deserialization Test"
        assert len(config.field_mappings) == 1
        assert config.field_mappings[0].target == "test_field"