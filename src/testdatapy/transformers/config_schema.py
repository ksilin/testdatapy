"""Comprehensive configuration schema for transformation configurations.

This module defines Pydantic models for validating and parsing transformation
configuration files, ensuring type safety and providing clear error messages.
"""

from typing import Any, Dict, List, Optional, Union, Literal
from pydantic import BaseModel, Field, field_validator, model_validator
from enum import Enum
import re


class DataType(str, Enum):
    """Supported protobuf data types."""
    STRING = "string"
    INT32 = "int32"
    INT64 = "int64"
    UINT32 = "uint32"
    UINT64 = "uint64"
    FLOAT = "float"
    DOUBLE = "double"
    BOOL = "bool"
    BYTES = "bytes"
    ENUM = "enum"
    MESSAGE = "message"
    REPEATED = "repeated"


class FieldType(str, Enum):
    """Field relationship types for transformation."""
    DIRECT = "direct"           # Direct value mapping
    DERIVED = "derived"         # Derived from other fields
    COMPUTED = "computed"       # Computed using functions
    CONDITIONAL = "conditional" # Conditional based on other fields
    NESTED = "nested"          # Nested object mapping
    REPEATED = "repeated"      # Array/list mapping


class ValidationRule(BaseModel):
    """Validation rule for field values."""
    
    type: Literal["required", "min", "max", "pattern", "enum", "custom"] = Field(
        description="Type of validation rule"
    )
    value: Optional[Union[str, int, float, List[str]]] = Field(
        default=None,
        description="Validation value (regex pattern, min/max value, enum values)"
    )
    message: Optional[str] = Field(
        default=None,
        description="Custom error message for validation failure"
    )
    
    @field_validator('value')
    @classmethod
    def validate_value(cls, v, info):
        """Validate that value is appropriate for the validation type."""
        validation_type = info.data.get('type') if info.data else None
        
        if validation_type == 'pattern' and not isinstance(v, str):
            raise ValueError("Pattern validation requires string value")
        elif validation_type in ['min', 'max'] and not isinstance(v, (int, float)):
            raise ValueError("Min/max validation requires numeric value")
        elif validation_type == 'enum' and not isinstance(v, list):
            raise ValueError("Enum validation requires list of values")
            
        return v


class ConditionalMapping(BaseModel):
    """Conditional field mapping based on conditions."""
    
    condition: str = Field(
        description="Condition expression (e.g., 'field1 == \"value\"')"
    )
    field: str = Field(
        description="Target field name"
    )
    value: Optional[Any] = Field(
        default=None,
        description="Static value to assign"
    )
    function: Optional[str] = Field(
        default=None,
        description="Function to call for dynamic value"
    )
    arguments: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Arguments to pass to function"
    )
    
    @model_validator(mode='after')
    def validate_mapping(self):
        """Ensure either value or function is provided."""
        if not self.value and not self.function:
            raise ValueError("Either 'value' or 'function' must be provided")
        if self.value and self.function:
            raise ValueError("Cannot specify both 'value' and 'function'")
        return self


class NestedFieldMapping(BaseModel):
    """Mapping configuration for nested objects."""
    
    source_path: str = Field(
        description="Source field path (dot notation: parent.child.field)"
    )
    target_path: str = Field(
        description="Target field path (dot notation: parent.child.field)"
    )
    type: DataType = Field(
        description="Expected data type"
    )
    required: bool = Field(
        default=False,
        description="Whether field is required"
    )
    default_value: Optional[Any] = Field(
        default=None,
        description="Default value if source is missing"
    )
    transformation: Optional[str] = Field(
        default=None,
        description="Transformation function to apply"
    )
    
    @field_validator('source_path', 'target_path')
    @classmethod
    def validate_path(cls, v):
        """Validate field paths use valid identifiers."""
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', v):
            raise ValueError(f"Invalid field path: {v}")
        return v


class FieldMapping(BaseModel):
    """Field mapping configuration."""
    
    source: Optional[str] = Field(
        default=None,
        description="Source field name or path"
    )
    target: str = Field(
        description="Target field name in protobuf message"
    )
    type: FieldType = Field(
        default=FieldType.DIRECT,
        description="Type of field mapping"
    )
    data_type: Optional[DataType] = Field(
        default=None,
        description="Expected protobuf data type"
    )
    required: bool = Field(
        default=False,
        description="Whether field is required"
    )
    default_value: Optional[Any] = Field(
        default=None,
        description="Default value if source is missing"
    )
    
    # Transformation settings
    function: Optional[str] = Field(
        default=None,
        description="Transformation function name"
    )
    arguments: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Arguments to pass to transformation function"
    )
    
    # Conditional mapping
    conditions: Optional[List[ConditionalMapping]] = Field(
        default_factory=list,
        description="Conditional mappings based on other fields"
    )
    
    # Nested object mapping
    nested_mappings: Optional[List[NestedFieldMapping]] = Field(
        default_factory=list,
        description="Mappings for nested objects"
    )
    
    # Validation rules
    validation: Optional[List[ValidationRule]] = Field(
        default_factory=list,
        description="Validation rules for the field"
    )
    
    # Metadata
    description: Optional[str] = Field(
        default=None,
        description="Human-readable description of the mapping"
    )
    examples: Optional[List[str]] = Field(
        default_factory=list,
        description="Example values for documentation"
    )
    
    @model_validator(mode='after')
    def validate_field_mapping(self):
        """Validate field mapping consistency."""
        # Direct mappings need source field
        if self.type == FieldType.DIRECT and not self.source:
            raise ValueError("Direct mappings require 'source' field")
        
        # Computed mappings need function
        if self.type == FieldType.COMPUTED and not self.function:
            raise ValueError("Computed mappings require 'function'")
        
        # Conditional mappings need conditions
        if self.type == FieldType.CONDITIONAL and not self.conditions:
            raise ValueError("Conditional mappings require 'conditions'")
        
        return self
    
    @field_validator('target')
    @classmethod
    def validate_target_field(cls, v):
        """Validate target field name."""
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', v):
            raise ValueError(f"Invalid target field name: {v}")
        return v


class FunctionRegistry(BaseModel):
    """Configuration for custom transformation functions."""
    
    name: str = Field(
        description="Function name"
    )
    module: Optional[str] = Field(
        default=None,
        description="Python module containing the function"
    )
    callable_path: Optional[str] = Field(
        default=None,
        description="Full import path to the function"
    )
    description: Optional[str] = Field(
        default=None,
        description="Function description"
    )
    parameters: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Default parameters for the function"
    )
    
    @model_validator(mode='after')
    def validate_function_reference(self):
        """Ensure proper function reference."""
        if not self.module and not self.callable_path:
            raise ValueError("Either 'module' or 'callable_path' must be provided")
        
        return self


class DataSourceConfig(BaseModel):
    """Configuration for data sources."""
    
    type: Literal["faker", "csv", "json", "custom"] = Field(
        description="Type of data source"
    )
    
    # Faker configuration
    locale: Optional[str] = Field(
        default="en_US",
        description="Faker locale for realistic data generation"
    )
    seed: Optional[int] = Field(
        default=None,
        description="Random seed for reproducible data"
    )
    providers: Optional[List[str]] = Field(
        default_factory=list,
        description="Additional Faker providers to load"
    )
    
    # File-based sources
    file_path: Optional[str] = Field(
        default=None,
        description="Path to data file (CSV, JSON)"
    )
    encoding: Optional[str] = Field(
        default="utf-8",
        description="File encoding"
    )
    
    # CSV-specific settings
    delimiter: Optional[str] = Field(
        default=",",
        description="CSV delimiter"
    )
    header_row: Optional[bool] = Field(
        default=True,
        description="Whether CSV has header row"
    )
    
    # Custom data source
    class_path: Optional[str] = Field(
        default=None,
        description="Full path to custom data source class"
    )
    parameters: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Parameters for custom data source"
    )


class ProtobufConfig(BaseModel):
    """Protobuf-specific configuration."""
    
    # Schema information
    proto_file: Optional[str] = Field(
        default=None,
        description="Path to .proto file"
    )
    proto_module: Optional[str] = Field(
        default=None,
        description="Python module containing compiled protobuf classes"
    )
    proto_class: Optional[str] = Field(
        default=None,
        description="Protobuf message class name"
    )
    
    # Compilation settings
    schema_paths: Optional[List[str]] = Field(
        default_factory=list,
        description="Additional schema search paths"
    )
    protoc_options: Optional[List[str]] = Field(
        default_factory=list,
        description="Additional protoc compiler options"
    )
    
    # Runtime settings
    preserve_unknown_fields: bool = Field(
        default=False,
        description="Whether to preserve unknown fields during transformation"
    )
    strict_mode: bool = Field(
        default=True,
        description="Whether to enforce strict type checking"
    )
    
    @model_validator(mode='after')
    def validate_protobuf_config(self):
        """Ensure proper protobuf configuration."""
        if not self.proto_file and not (self.proto_module and self.proto_class):
            raise ValueError(
                "Either 'proto_file' or both 'proto_module' and 'proto_class' must be provided"
            )
        
        return self


class OutputConfig(BaseModel):
    """Output configuration for generated data."""
    
    format: Literal["binary", "json", "text"] = Field(
        default="binary",
        description="Output format for protobuf messages"
    )
    
    # Kafka producer settings
    topic: Optional[str] = Field(
        default=None,
        description="Kafka topic to produce messages to"
    )
    key_field: Optional[str] = Field(
        default=None,
        description="Field to use as message key"
    )
    
    # Schema Registry settings
    schema_registry_url: Optional[str] = Field(
        default=None,
        description="Schema Registry URL for automatic registration"
    )
    subject_name: Optional[str] = Field(
        default=None,
        description="Schema Registry subject name"
    )
    
    # File output settings
    output_file: Optional[str] = Field(
        default=None,
        description="File path for output (instead of Kafka)"
    )
    batch_size: Optional[int] = Field(
        default=1000,
        description="Number of messages per batch"
    )


class TransformationConfig(BaseModel):
    """Complete transformation configuration schema."""
    
    # Metadata
    version: str = Field(
        default="1.0",
        description="Configuration schema version"
    )
    name: str = Field(
        description="Descriptive name for this transformation configuration"
    )
    description: Optional[str] = Field(
        default=None,
        description="Detailed description of the transformation"
    )
    
    # Core configuration sections
    protobuf: ProtobufConfig = Field(
        description="Protobuf schema and compilation settings"
    )
    data_source: DataSourceConfig = Field(
        description="Data source configuration"
    )
    output: OutputConfig = Field(
        description="Output configuration"
    )
    
    # Field mappings
    field_mappings: List[FieldMapping] = Field(
        description="Field mapping configurations"
    )
    
    # Custom functions
    custom_functions: Optional[List[FunctionRegistry]] = Field(
        default_factory=list,
        description="Custom transformation functions to register"
    )
    
    # Global settings
    generation_settings: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Global data generation settings"
    )
    
    # Validation settings
    enable_validation: bool = Field(
        default=True,
        description="Whether to enable field validation"
    )
    strict_validation: bool = Field(
        default=False,
        description="Whether to use strict validation (fail on any error)"
    )
    
    @field_validator('version')
    @classmethod
    def validate_version(cls, v):
        """Validate version format."""
        if not re.match(r'^\d+\.\d+(?:\.\d+)?$', v):
            raise ValueError("Version must be in format 'major.minor' or 'major.minor.patch'")
        return v
    
    @field_validator('field_mappings')
    @classmethod
    def validate_unique_targets(cls, v):
        """Ensure target field names are unique."""
        targets = [mapping.target for mapping in v]
        duplicates = [target for target in targets if targets.count(target) > 1]
        if duplicates:
            raise ValueError(f"Duplicate target fields: {', '.join(set(duplicates))}")
        return v
    
    model_config = {
        "extra": "forbid",  # Disallow extra fields
        "validate_assignment": True,  # Validate on assignment
        "use_enum_values": True  # Use enum values in serialization
    }


# Convenience models for specific use cases

class SimpleFieldMapping(BaseModel):
    """Simplified field mapping for basic use cases."""
    
    source: str = Field(description="Source field name")
    target: str = Field(description="Target field name")
    type: Optional[DataType] = Field(default=None, description="Data type")
    function: Optional[str] = Field(default=None, description="Transformation function")
    default: Optional[Any] = Field(default=None, description="Default value")


class BasicTransformationConfig(BaseModel):
    """Simplified transformation configuration for basic use cases."""
    
    proto_file: str = Field(description="Path to .proto file")
    proto_class: str = Field(description="Protobuf message class name")
    mappings: List[SimpleFieldMapping] = Field(description="Field mappings")
    data_source: Literal["faker"] = Field(default="faker", description="Data source type")
    locale: Optional[str] = Field(default="en_US", description="Faker locale")


# Schema validation utilities

def validate_config_file(config_path: str) -> TransformationConfig:
    """Validate a transformation configuration file.
    
    Args:
        config_path: Path to YAML configuration file
        
    Returns:
        Validated TransformationConfig instance
        
    Raises:
        ValidationError: If configuration is invalid
        FileNotFoundError: If configuration file not found
    """
    import yaml
    from pathlib import Path
    
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_file, 'r') as f:
        config_data = yaml.safe_load(f)
    
    return TransformationConfig(**config_data)


def generate_config_template(output_path: str, config_type: str = "full") -> None:
    """Generate a configuration template file.
    
    Args:
        output_path: Path for output template file
        config_type: Type of template ('full', 'basic', 'minimal')
    """
    import yaml
    from pathlib import Path
    
    if config_type == "basic":
        template = {
            "proto_file": "path/to/schema.proto",
            "proto_class": "MessageClass",
            "mappings": [
                {
                    "source": "source_field",
                    "target": "target_field",
                    "type": "string",
                    "function": "faker.name"
                }
            ]
        }
    elif config_type == "minimal":
        template = {
            "version": "1.0",
            "name": "Example Transformation",
            "protobuf": {
                "proto_file": "schema.proto",
                "proto_class": "ExampleMessage"
            },
            "data_source": {
                "type": "faker"
            },
            "output": {
                "format": "binary"
            },
            "field_mappings": [
                {
                    "target": "example_field",
                    "type": "computed",
                    "function": "faker.name"
                }
            ]
        }
    else:  # full
        template = TransformationConfig(
            name="Example Transformation",
            description="Comprehensive example transformation configuration",
            protobuf=ProtobufConfig(
                proto_file="example.proto",
                proto_class="ExampleMessage"
            ),
            data_source=DataSourceConfig(type="faker"),
            output=OutputConfig(format="binary"),
            field_mappings=[
                FieldMapping(
                    target="example_field",
                    type=FieldType.COMPUTED,
                    function="faker.name",
                    description="Generate random name using Faker"
                )
            ]
        ).model_dump(exclude_unset=True, mode='json')
    
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w') as f:
        yaml.dump(template, f, default_flow_style=False, sort_keys=False)