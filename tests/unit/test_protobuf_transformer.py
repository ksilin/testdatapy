"""Unit tests for ProtobufTransformer."""

import pytest
import tempfile
import yaml
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from google.protobuf.message import Message
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.timestamp_pb2 import Timestamp

from testdatapy.transformers.protobuf_transformer import (
    ProtobufTransformer,
    FieldMapping,
    TransformationFunction
)
from testdatapy.exceptions import (
    ProtobufException,
    ProtobufSerializationError,
    InvalidConfigurationError
)


class MockNestedMessage(Message):
    """Mock nested protobuf message for testing."""
    
    def __init__(self):
        super().__init__()
        self.street = ""
        self.city = ""
        self.country = ""
    
    class MockDescriptor:
        def __init__(self):
            self.fields = [
                self._create_field("street", FieldDescriptor.TYPE_STRING),
                self._create_field("city", FieldDescriptor.TYPE_STRING),
                self._create_field("country", FieldDescriptor.TYPE_STRING),
            ]
        
        def _create_field(self, name, field_type):
            field = Mock()
            field.name = name
            field.type = field_type
            field.label = FieldDescriptor.LABEL_OPTIONAL
            return field
    
    DESCRIPTOR = MockDescriptor()


class MockProtobufMessage(Message):
    """Mock protobuf message for testing."""
    
    def __init__(self):
        super().__init__()
        self.name = ""
        self.age = 0
        self.email = ""
        self.is_active = False
        self.created_at = None
        self.address = None
        self.tags = []
    
    class MockDescriptor:
        def __init__(self):
            self.fields = [
                self._create_field("name", FieldDescriptor.TYPE_STRING),
                self._create_field("age", FieldDescriptor.TYPE_INT32),
                self._create_field("email", FieldDescriptor.TYPE_STRING),
                self._create_field("is_active", FieldDescriptor.TYPE_BOOL),
                self._create_field("created_at", FieldDescriptor.TYPE_MESSAGE),
                self._create_field("address", FieldDescriptor.TYPE_MESSAGE),
                self._create_field("tags", FieldDescriptor.TYPE_STRING, FieldDescriptor.LABEL_REPEATED),
            ]
        
        def _create_field(self, name, field_type, label=FieldDescriptor.LABEL_OPTIONAL):
            field = Mock()
            field.name = name
            field.type = field_type
            field.label = label
            if field_type == FieldDescriptor.TYPE_MESSAGE:
                field.message_type = Mock()
                field.message_type._concrete_class = MockNestedMessage
            return field
    
    DESCRIPTOR = MockDescriptor()


class TestProtobufTransformer:
    """Test the ProtobufTransformer class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_proto_class = MockProtobufMessage
        self.sample_config = {
            "auto_mapping": {"enabled": True},
            "field_mappings": {
                "name": {
                    "source": "full_name",
                    "transform": "title",
                    "required": True
                },
                "email": {
                    "source": "email_address",
                    "transform": "lower",
                    "default": "test@example.com"
                },
                "age": "user_age"
            }
        }
        self.sample_data = {
            "full_name": "john doe",
            "email_address": "JOHN@EXAMPLE.COM",
            "user_age": 30,
            "is_active": True
        }
    
    def test_transformer_initialization_with_config_dict(self):
        """Test transformer initialization with configuration dictionary."""
        transformer = ProtobufTransformer(
            config=self.sample_config,
            proto_class=self.mock_proto_class
        )
        
        assert transformer.proto_class == self.mock_proto_class
        assert transformer.config == self.sample_config
        assert len(transformer.field_mappings) == 3
        assert "name" in transformer.field_mappings
        assert transformer.field_mappings["name"].source_field == "full_name"
        assert transformer.field_mappings["name"].transformation == "title"
        assert transformer.field_mappings["name"].required is True
    
    def test_transformer_initialization_with_config_file(self):
        """Test transformer initialization with configuration file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(self.sample_config, f)
            config_path = f.name
        
        try:
            transformer = ProtobufTransformer(
                config=config_path,
                proto_class=self.mock_proto_class
            )
            
            assert transformer.config == self.sample_config
            assert len(transformer.field_mappings) == 3
        finally:
            Path(config_path).unlink()
    
    def test_transformer_initialization_with_invalid_config_file(self):
        """Test transformer initialization with invalid configuration file."""
        with pytest.raises(InvalidConfigurationError):
            ProtobufTransformer(
                config="/nonexistent/config.yaml",
                proto_class=self.mock_proto_class
            )
    
    def test_builtin_transformation_functions(self):
        """Test built-in transformation functions."""
        transformer = ProtobufTransformer(proto_class=self.mock_proto_class)
        
        # Test string transformations
        assert transformer.apply_transformation("hello world", "upper") == "HELLO WORLD"
        assert transformer.apply_transformation("HELLO WORLD", "lower") == "hello world"
        assert transformer.apply_transformation("hello world", "title") == "Hello World"
        assert transformer.apply_transformation("  hello  ", "strip") == "hello"
        
        # Test type conversions
        assert transformer.apply_transformation("123", "int") == 123
        assert transformer.apply_transformation("123.45", "float") == 123.45
        assert transformer.apply_transformation(123, "str") == "123"
        assert transformer.apply_transformation("true", "bool") is True
        
        # Test timestamp transformation
        result = transformer.apply_transformation(None, "timestamp_now")
        assert isinstance(result, Timestamp)
    
    def test_register_custom_function(self):
        """Test registering custom transformation functions."""
        transformer = ProtobufTransformer(proto_class=self.mock_proto_class)
        
        def reverse_string(value):
            return str(value)[::-1]
        
        transformer.register_function("reverse", reverse_string, "Reverse a string")
        
        assert "reverse" in transformer.transformation_functions
        assert transformer.apply_transformation("hello", "reverse") == "olleh"
    
    def test_field_mapping_parsing_simple(self):
        """Test parsing simple field mappings."""
        config = {
            "field_mappings": {
                "name": "full_name",
                "age": "user_age"
            }
        }
        
        transformer = ProtobufTransformer(
            config=config,
            proto_class=self.mock_proto_class
        )
        
        assert len(transformer.field_mappings) == 2
        assert transformer.field_mappings["name"].source_field == "full_name"
        assert transformer.field_mappings["name"].transformation is None
        assert transformer.field_mappings["age"].source_field == "user_age"
    
    def test_field_mapping_parsing_complex(self):
        """Test parsing complex field mappings."""
        transformer = ProtobufTransformer(
            config=self.sample_config,
            proto_class=self.mock_proto_class
        )
        
        name_mapping = transformer.field_mappings["name"]
        assert name_mapping.source_field == "full_name"
        assert name_mapping.transformation == "title"
        assert name_mapping.required is True
        
        email_mapping = transformer.field_mappings["email"]
        assert email_mapping.source_field == "email_address"
        assert email_mapping.transformation == "lower"
        assert email_mapping.default_value == "test@example.com"
    
    def test_get_message_fields(self):
        """Test message field discovery using reflection."""
        transformer = ProtobufTransformer(proto_class=self.mock_proto_class)
        
        fields = transformer.get_message_fields(self.mock_proto_class)
        
        assert len(fields) == 7
        assert "name" in fields
        assert "age" in fields
        assert "email" in fields
        assert "is_active" in fields
        assert "created_at" in fields
        assert "address" in fields
        assert "tags" in fields
    
    def test_detect_field_type(self):
        """Test field type detection."""
        transformer = ProtobufTransformer(proto_class=self.mock_proto_class)
        
        # Create mock field descriptors
        string_field = Mock()
        string_field.type = FieldDescriptor.TYPE_STRING
        string_field.label = FieldDescriptor.LABEL_OPTIONAL
        
        repeated_field = Mock()
        repeated_field.type = FieldDescriptor.TYPE_STRING
        repeated_field.label = FieldDescriptor.LABEL_REPEATED
        
        message_field = Mock()
        message_field.type = FieldDescriptor.TYPE_MESSAGE
        message_field.label = FieldDescriptor.LABEL_OPTIONAL
        
        assert transformer.detect_field_type(string_field) == "string"
        assert transformer.detect_field_type(repeated_field) == "repeated_string"
        assert transformer.detect_field_type(message_field) == "message"
    
    def test_condition_evaluation(self):
        """Test field condition evaluation."""
        transformer = ProtobufTransformer(proto_class=self.mock_proto_class)
        
        data = {
            "name": "John",
            "age": 30,
            "email": "",
            "is_admin": True
        }
        
        # Test existence conditions
        assert transformer.evaluate_condition("exists:name", data) is True
        assert transformer.evaluate_condition("exists:missing", data) is False
        assert transformer.evaluate_condition("exists:email", data) is False  # Empty string
        
        # Test equality conditions
        assert transformer.evaluate_condition("name == John", data) is True
        assert transformer.evaluate_condition("name == Jane", data) is False
        assert transformer.evaluate_condition("age == 30", data) is True
        
        # Test simple field presence
        assert transformer.evaluate_condition("name", data) is True
        assert transformer.evaluate_condition("missing", data) is False
    
    def test_transform_with_mappings(self):
        """Test data transformation with field mappings."""
        transformer = ProtobufTransformer(
            config=self.sample_config,
            proto_class=self.mock_proto_class
        )
        
        # Mock the message creation and field setting
        with patch.object(self.mock_proto_class, '__new__') as mock_new:
            mock_message = Mock()
            mock_new.return_value = mock_message
            
            result = transformer.transform(self.sample_data)
            
            # Verify the transformation was attempted
            assert result == mock_message
    
    def test_transform_with_missing_required_field(self):
        """Test transformation with missing required field."""
        config = {
            "field_mappings": {
                "name": {
                    "source": "missing_field",
                    "required": True
                }
            }
        }
        
        transformer = ProtobufTransformer(
            config=config,
            proto_class=self.mock_proto_class
        )
        
        with pytest.raises(ProtobufSerializationError):
            transformer.transform({"other_field": "value"})
    
    def test_transform_with_unknown_transformation(self):
        """Test transformation with unknown transformation function."""
        config = {
            "field_mappings": {
                "name": {
                    "source": "full_name",
                    "transform": "unknown_function"
                }
            }
        }
        
        transformer = ProtobufTransformer(
            config=config,
            proto_class=self.mock_proto_class
        )
        
        with pytest.raises(ProtobufException):
            transformer.transform({"full_name": "John Doe"})
    
    def test_handle_nested_message(self):
        """Test nested message handling."""
        transformer = ProtobufTransformer(proto_class=self.mock_proto_class)
        
        # Create mock field descriptor for nested message
        field_descriptor = Mock()
        field_descriptor.message_type = Mock()
        field_descriptor.message_type._concrete_class = MockNestedMessage
        
        nested_data = {
            "street": "123 Main St",
            "city": "New York",
            "country": "USA"
        }
        
        # The method will create a new ProtobufTransformer instance internally
        # and call transform on it, so we need to patch the constructor
        with patch('testdatapy.transformers.protobuf_transformer.ProtobufTransformer') as mock_transformer_class:
            mock_nested_transformer = Mock()
            mock_nested_message = Mock()
            mock_nested_transformer.transform.return_value = mock_nested_message
            mock_transformer_class.return_value = mock_nested_transformer
            
            result = transformer.handle_nested_message(nested_data, field_descriptor)
            
            # Verify the nested transformer was created and used
            mock_transformer_class.assert_called_once()
            mock_nested_transformer.transform.assert_called_once_with(nested_data, MockNestedMessage)
            assert result == mock_nested_message
    
    def test_configuration_validation_valid(self):
        """Test configuration validation with valid config."""
        transformer = ProtobufTransformer(
            config=self.sample_config,
            proto_class=self.mock_proto_class
        )
        
        assert transformer.validate_config() is True
    
    def test_configuration_validation_invalid_transform_function(self):
        """Test configuration validation with invalid transformation function."""
        config = {
            "field_mappings": {
                "name": {
                    "source": "full_name",
                    "transform": "nonexistent_function"
                }
            }
        }
        
        transformer = ProtobufTransformer(
            config=config,
            proto_class=self.mock_proto_class
        )
        
        assert transformer.validate_config() is False
    
    def test_configuration_validation_invalid_proto_class(self):
        """Test configuration validation with invalid protobuf class."""
        transformer = ProtobufTransformer(
            config=self.sample_config,
            proto_class=str  # Not a protobuf Message subclass
        )
        
        assert transformer.validate_config() is False
    
    def test_get_supported_types(self):
        """Test getting supported transformation types."""
        transformer = ProtobufTransformer(proto_class=self.mock_proto_class)
        
        supported_types = transformer.get_supported_types()
        
        assert isinstance(supported_types, list)
        assert "string" in supported_types
        assert "int32" in supported_types
        assert "message" in supported_types
        assert "repeated_*" in supported_types
    
    def test_get_transformation_functions(self):
        """Test getting available transformation functions."""
        transformer = ProtobufTransformer(proto_class=self.mock_proto_class)
        
        functions = transformer.get_transformation_functions()
        
        assert isinstance(functions, dict)
        assert "upper" in functions
        assert "lower" in functions
        assert "int" in functions
        assert "timestamp_now" in functions
        
        # Check descriptions are present
        assert functions["upper"] == "Convert to uppercase"
    
    def test_transform_without_proto_class(self):
        """Test transformation without specifying protobuf class."""
        transformer = ProtobufTransformer()
        
        with pytest.raises(ProtobufException) as exc_info:
            transformer.transform({"name": "John"})
        
        assert "No protobuf class specified" in str(exc_info.value)
    
    def test_clear_cache(self):
        """Test clearing the transformation cache."""
        transformer = ProtobufTransformer(proto_class=self.mock_proto_class)
        
        # Add something to cache
        transformer._transformation_cache["test"] = "value"
        
        transformer.clear_cache()
        
        assert len(transformer._transformation_cache) == 0


class TestFieldMapping:
    """Test the FieldMapping class."""
    
    def test_field_mapping_creation(self):
        """Test creating field mappings."""
        mapping = FieldMapping(
            source_field="input_field",
            target_field="output_field",
            transformation="upper",
            default_value="default",
            required=True,
            condition="exists:input_field"
        )
        
        assert mapping.source_field == "input_field"
        assert mapping.target_field == "output_field"
        assert mapping.transformation == "upper"
        assert mapping.default_value == "default"
        assert mapping.required is True
        assert mapping.condition == "exists:input_field"


class TestTransformationFunction:
    """Test the TransformationFunction class."""
    
    def test_transformation_function_creation(self):
        """Test creating transformation functions."""
        def test_func(x):
            return x.upper()
        
        func = TransformationFunction(
            func=test_func,
            name="test_upper",
            description="Test uppercase function"
        )
        
        assert func.name == "test_upper"
        assert func.description == "Test uppercase function"
        assert func("hello") == "HELLO"
    
    def test_transformation_function_call(self):
        """Test calling transformation functions."""
        def multiply_by_two(x):
            return x * 2
        
        func = TransformationFunction(multiply_by_two, "multiply_by_two")
        
        assert func(5) == 10
        assert func("hi") == "hihi"


if __name__ == "__main__":
    pytest.main([__file__])