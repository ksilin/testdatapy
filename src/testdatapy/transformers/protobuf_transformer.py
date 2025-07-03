"""ProtobufTransformer: Configuration-driven protobuf message transformation.

This module provides a comprehensive transformation engine for converting dictionary
data to protobuf messages using configuration-driven field mappings and reflection.
"""

import logging
import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Type, Callable
from google.protobuf.message import Message
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.duration_pb2 import Duration
from datetime import datetime, timedelta
import re
from faker import Faker

from .base import DataTransformer
from ..exceptions import (
    TestDataPyException,
    ProtobufException,
    ProtobufSerializationError,
    ConfigurationException,
    InvalidConfigurationError
)
from ..logging_config import get_schema_logger, PerformanceTimer

logger = get_schema_logger(__name__)


class TransformationFunction:
    """Represents a transformation function with metadata."""
    
    def __init__(self, func: Callable, name: str, description: str = ""):
        self.func = func
        self.name = name
        self.description = description
    
    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


class FieldMapping:
    """Represents a field mapping configuration."""
    
    def __init__(
        self,
        source_field: str,
        target_field: str,
        transformation: Optional[str] = None,
        default_value: Any = None,
        required: bool = False,
        condition: Optional[str] = None
    ):
        self.source_field = source_field
        self.target_field = target_field
        self.transformation = transformation
        self.default_value = default_value
        self.required = required
        self.condition = condition


class ProtobufTransformer(DataTransformer):
    """Configuration-driven protobuf message transformer.
    
    Transforms dictionary data to protobuf messages using:
    - Configuration-driven field mappings
    - Protobuf reflection for automatic field discovery
    - Custom transformation functions
    - Automatic nested message handling
    """
    
    def __init__(
        self,
        config: Optional[Union[Dict[str, Any], str, Path]] = None,
        proto_class: Optional[Type[Message]] = None
    ):
        """Initialize the ProtobufTransformer.
        
        Args:
            config: Configuration dictionary, file path, or None for auto-discovery
            proto_class: Target protobuf message class
        """
        super().__init__()
        
        self.proto_class = proto_class
        self.field_mappings: Dict[str, FieldMapping] = {}
        self.transformation_functions: Dict[str, TransformationFunction] = {}
        self.faker = Faker()
        
        # Load configuration
        if isinstance(config, (str, Path)):
            self.config = self._load_config_file(config)
        else:
            self.config = config or {}
        
        # Register built-in transformation functions
        self._register_builtin_functions()
        
        # Parse field mappings from config
        self._parse_field_mappings()
        
        logger.info("Initialized ProtobufTransformer", 
                   proto_class=proto_class.__name__ if proto_class else None,
                   config_source=type(config).__name__)
    
    def _load_config_file(self, config_path: Union[str, Path]) -> Dict[str, Any]:
        """Load transformation configuration from file.
        
        Args:
            config_path: Path to YAML configuration file
            
        Returns:
            Parsed configuration dictionary
        """
        config_path = Path(config_path)
        
        if not config_path.exists():
            raise InvalidConfigurationError(
                config_path=str(config_path),
                config_errors=[f"Configuration file not found: {config_path}"]
            )
        
        try:
            with PerformanceTimer(logger, "config_loading", config_path=str(config_path)):
                with open(config_path, 'r') as f:
                    config = yaml.safe_load(f)
                
                logger.debug("Loaded transformation config", 
                           config_path=str(config_path),
                           config_size=len(str(config)))
                
                return config or {}
                
        except yaml.YAMLError as e:
            raise InvalidConfigurationError(
                config_path=str(config_path),
                config_errors=[f"Invalid YAML syntax: {e}"]
            ) from e
        except Exception as e:
            raise InvalidConfigurationError(
                config_path=str(config_path),
                config_errors=[f"Failed to load config: {e}"]
            ) from e
    
    def _register_builtin_functions(self) -> None:
        """Register built-in transformation functions."""
        
        # String transformations
        self.register_function("upper", lambda x: str(x).upper(), "Convert to uppercase")
        self.register_function("lower", lambda x: str(x).lower(), "Convert to lowercase")
        self.register_function("title", lambda x: str(x).title(), "Convert to title case")
        self.register_function("strip", lambda x: str(x).strip(), "Strip whitespace")
        
        # Type conversions
        self.register_function("int", lambda x: int(x), "Convert to integer")
        self.register_function("float", lambda x: float(x), "Convert to float")
        self.register_function("str", lambda x: str(x), "Convert to string")
        self.register_function("bool", lambda x: bool(x), "Convert to boolean")
        
        # Date/time transformations
        self.register_function("timestamp_now", 
                             lambda _=None: self._create_timestamp(datetime.now()),
                             "Current timestamp")
        self.register_function("timestamp_from_iso", 
                             lambda x: self._create_timestamp(datetime.fromisoformat(str(x))),
                             "Convert ISO string to timestamp")
        
        # Faker-based generation
        self.register_function("fake_name", lambda _=None: self.faker.name(), "Generate fake name")
        self.register_function("fake_email", lambda _=None: self.faker.email(), "Generate fake email")
        self.register_function("fake_address", lambda _=None: self.faker.address(), "Generate fake address")
        self.register_function("fake_phone", lambda _=None: self.faker.phone_number(), "Generate fake phone")
        self.register_function("fake_company", lambda _=None: self.faker.company(), "Generate fake company")
        
        # Protobuf-specific
        self.register_function("duration_seconds", 
                             lambda x: self._create_duration(seconds=int(x)),
                             "Create duration from seconds")
        
        logger.debug("Registered built-in transformation functions", 
                    function_count=len(self.transformation_functions))
    
    def _create_timestamp(self, dt: datetime) -> Timestamp:
        """Create protobuf Timestamp from datetime."""
        timestamp = Timestamp()
        timestamp.FromDatetime(dt)
        return timestamp
    
    def _create_duration(self, seconds: int = 0, nanos: int = 0) -> Duration:
        """Create protobuf Duration."""
        duration = Duration()
        duration.seconds = seconds
        duration.nanos = nanos
        return duration
    
    def register_function(self, name: str, func: Callable, description: str = "") -> None:
        """Register a custom transformation function.
        
        Args:
            name: Function name for use in configurations
            func: Callable transformation function
            description: Optional description
        """
        self.transformation_functions[name] = TransformationFunction(func, name, description)
        logger.debug("Registered transformation function", 
                    function_name=name, 
                    description=description)
    
    def _parse_field_mappings(self) -> None:
        """Parse field mappings from configuration."""
        mappings_config = self.config.get("field_mappings", {})
        
        for target_field, mapping_config in mappings_config.items():
            if isinstance(mapping_config, str):
                # Simple mapping: target_field: source_field
                self.field_mappings[target_field] = FieldMapping(
                    source_field=mapping_config,
                    target_field=target_field
                )
            elif isinstance(mapping_config, dict):
                # Complex mapping with transformation rules
                self.field_mappings[target_field] = FieldMapping(
                    source_field=mapping_config.get("source", target_field),
                    target_field=target_field,
                    transformation=mapping_config.get("transform"),
                    default_value=mapping_config.get("default"),
                    required=mapping_config.get("required", False),
                    condition=mapping_config.get("condition")
                )
        
        logger.debug("Parsed field mappings", 
                    mapping_count=len(self.field_mappings),
                    mappings=list(self.field_mappings.keys()))
    
    def validate_config(self) -> bool:
        """Validate the transformation configuration.
        
        Returns:
            True if configuration is valid, False otherwise
        """
        try:
            # Validate required sections
            if not isinstance(self.config, dict):
                logger.error("Configuration must be a dictionary")
                return False
            
            # Validate field mappings section
            field_mappings = self.config.get("field_mappings", {})
            if not isinstance(field_mappings, dict):
                logger.error("field_mappings must be a dictionary")
                return False
            
            # Validate each mapping
            for target_field, mapping in field_mappings.items():
                if isinstance(mapping, dict):
                    # Validate transformation function exists
                    transform_func = mapping.get("transform")
                    if transform_func and transform_func not in self.transformation_functions:
                        logger.error("Unknown transformation function", 
                                   function=transform_func, 
                                   field=target_field)
                        return False
            
            # Validate protobuf class if provided
            if self.proto_class:
                if not issubclass(self.proto_class, Message):
                    logger.error("proto_class must be a protobuf Message subclass")
                    return False
            
            logger.info("Configuration validation successful")
            return True
            
        except Exception as e:
            logger.error("Configuration validation failed", error=str(e))
            return False
    
    def get_message_fields(self, message_class: Type[Message]) -> Dict[str, FieldDescriptor]:
        """Get all fields from a protobuf message class using reflection.
        
        Args:
            message_class: Protobuf message class
            
        Returns:
            Dictionary mapping field names to FieldDescriptors
        """
        with PerformanceTimer(logger, "message_reflection", 
                            message_class=message_class.__name__):
            fields = {}
            descriptor = message_class.DESCRIPTOR
            
            for field in descriptor.fields:
                fields[field.name] = field
            
            logger.debug("Discovered message fields", 
                        message_class=message_class.__name__,
                        field_count=len(fields),
                        fields=list(fields.keys()))
            
            return fields
    
    def detect_field_type(self, field_descriptor: FieldDescriptor) -> str:
        """Detect the type of a protobuf field.
        
        Args:
            field_descriptor: Protobuf field descriptor
            
        Returns:
            String representation of the field type
        """
        type_mapping = {
            FieldDescriptor.TYPE_DOUBLE: "double",
            FieldDescriptor.TYPE_FLOAT: "float", 
            FieldDescriptor.TYPE_INT64: "int64",
            FieldDescriptor.TYPE_UINT64: "uint64",
            FieldDescriptor.TYPE_INT32: "int32",
            FieldDescriptor.TYPE_FIXED64: "fixed64",
            FieldDescriptor.TYPE_FIXED32: "fixed32",
            FieldDescriptor.TYPE_BOOL: "bool",
            FieldDescriptor.TYPE_STRING: "string",
            FieldDescriptor.TYPE_BYTES: "bytes",
            FieldDescriptor.TYPE_UINT32: "uint32",
            FieldDescriptor.TYPE_ENUM: "enum",
            FieldDescriptor.TYPE_SFIXED32: "sfixed32",
            FieldDescriptor.TYPE_SFIXED64: "sfixed64",
            FieldDescriptor.TYPE_SINT32: "sint32",
            FieldDescriptor.TYPE_SINT64: "sint64",
            FieldDescriptor.TYPE_MESSAGE: "message"
        }
        
        field_type = type_mapping.get(field_descriptor.type, "unknown")
        
        if field_descriptor.label == FieldDescriptor.LABEL_REPEATED:
            field_type = f"repeated_{field_type}"
        
        return field_type
    
    def handle_nested_message(
        self, 
        data: Dict[str, Any], 
        field_descriptor: FieldDescriptor
    ) -> Message:
        """Handle transformation of nested protobuf messages.
        
        Args:
            data: Input data for the nested message
            field_descriptor: Field descriptor for the nested message
            
        Returns:
            Instantiated and populated nested message
        """
        nested_message_class = field_descriptor.message_type._concrete_class
        
        with PerformanceTimer(logger, "nested_message_transformation",
                            message_class=nested_message_class.__name__):
            
            # Create nested transformer with same configuration
            nested_transformer = ProtobufTransformer(
                config=self.config,
                proto_class=nested_message_class
            )
            
            # Transform data to nested message
            nested_message = nested_transformer.transform(data, nested_message_class)
            
            logger.debug("Transformed nested message",
                        message_class=nested_message_class.__name__,
                        data_fields=list(data.keys()))
            
            return nested_message
    
    def apply_transformation(self, value: Any, transformation: str) -> Any:
        """Apply a transformation function to a value.
        
        Args:
            value: Input value
            transformation: Name of transformation function
            
        Returns:
            Transformed value
        """
        if transformation not in self.transformation_functions:
            raise ProtobufException(
                message=f"Unknown transformation function: {transformation}",
                suggestions=[
                    f"Available functions: {', '.join(self.transformation_functions.keys())}",
                    "Register custom functions using register_function()"
                ]
            )
        
        try:
            func = self.transformation_functions[transformation]
            result = func(value)
            
            logger.debug("Applied transformation",
                        function=transformation,
                        input_type=type(value).__name__,
                        output_type=type(result).__name__)
            
            return result
            
        except Exception as e:
            raise ProtobufSerializationError(
                data={"value": value, "transformation": transformation},
                proto_class=type(None),
                serialization_error=e
            ) from e
    
    def evaluate_condition(self, condition: str, data: Dict[str, Any]) -> bool:
        """Evaluate a field condition.
        
        Args:
            condition: Condition string (simple field existence or value checks)
            data: Input data dictionary
            
        Returns:
            True if condition is met, False otherwise
        """
        try:
            # Simple field existence check
            if condition.startswith("exists:"):
                field_name = condition[7:].strip()
                return field_name in data and data[field_name] is not None and data[field_name] != ""
            
            # Simple value equality check
            if "==" in condition:
                field_name, expected_value = condition.split("==", 1)
                field_name = field_name.strip()
                expected_value = expected_value.strip().strip('"\'')
                return str(data.get(field_name, "")) == expected_value
            
            # Field presence (default)
            return condition in data and data[condition] is not None
            
        except Exception as e:
            logger.warning("Condition evaluation failed", 
                         condition=condition, 
                         error=str(e))
            return False
    
    def transform(
        self, 
        data: Dict[str, Any], 
        target_schema: Optional[Type[Message]] = None
    ) -> Message:
        """Transform dictionary data to protobuf message.
        
        Args:
            data: Input data dictionary
            target_schema: Target protobuf message class (uses self.proto_class if None)
            
        Returns:
            Populated protobuf message instance
        """
        proto_class = target_schema or self.proto_class
        
        if not proto_class:
            raise ProtobufException(
                message="No protobuf class specified for transformation",
                suggestions=[
                    "Provide proto_class in constructor",
                    "Pass target_schema to transform() method"
                ]
            )
        
        start_time = logger.logger.handlers[0].formatter.formatTime if logger.logger.handlers else None
        
        try:
            with PerformanceTimer(logger, "protobuf_transformation",
                                proto_class=proto_class.__name__,
                                data_fields=list(data.keys())):
                
                # Create message instance
                message = proto_class()
                
                # Get message fields using reflection
                message_fields = self.get_message_fields(proto_class)
                
                # Apply configured field mappings
                for target_field, mapping in self.field_mappings.items():
                    if target_field not in message_fields:
                        logger.warning("Target field not found in message",
                                     field=target_field,
                                     message_class=proto_class.__name__)
                        continue
                    
                    # Check condition if specified
                    if mapping.condition and not self.evaluate_condition(mapping.condition, data):
                        logger.debug("Field condition not met, skipping",
                                   field=target_field,
                                   condition=mapping.condition)
                        continue
                    
                    # Get source value
                    source_value = data.get(mapping.source_field, mapping.default_value)
                    
                    # Check required fields
                    if mapping.required and source_value is None:
                        raise ProtobufSerializationError(
                            data=data,
                            proto_class=proto_class,
                            serialization_error=ValueError(f"Required field missing: {mapping.source_field}")
                        )
                    
                    if source_value is None:
                        continue
                    
                    # Apply transformation if specified
                    if mapping.transformation:
                        source_value = self.apply_transformation(source_value, mapping.transformation)
                    
                    # Set field value
                    self._set_message_field(message, target_field, source_value, message_fields[target_field])
                
                # Handle unmapped fields using auto-discovery
                self._apply_automatic_mapping(message, data, message_fields)
                
                logger.info("Successfully transformed data to protobuf",
                          proto_class=proto_class.__name__,
                          mapped_fields=len(self.field_mappings),
                          total_fields=len(message_fields))
                
                return message
                
        except Exception as e:
            if not isinstance(e, (ProtobufException, ProtobufSerializationError)):
                raise ProtobufSerializationError(
                    data=data,
                    proto_class=proto_class,
                    serialization_error=e
                ) from e
            raise
    
    def _set_message_field(
        self, 
        message: Message, 
        field_name: str, 
        value: Any, 
        field_descriptor: FieldDescriptor
    ) -> None:
        """Set a field value in the protobuf message.
        
        Args:
            message: Target protobuf message
            field_name: Name of the field to set
            value: Value to set
            field_descriptor: Field descriptor from reflection
        """
        try:
            # Handle repeated fields
            if field_descriptor.label == FieldDescriptor.LABEL_REPEATED:
                if not isinstance(value, (list, tuple)):
                    value = [value]
                
                repeated_field = getattr(message, field_name)
                for item in value:
                    if field_descriptor.type == FieldDescriptor.TYPE_MESSAGE:
                        nested_msg = self.handle_nested_message(item, field_descriptor)
                        repeated_field.append(nested_msg)
                    else:
                        repeated_field.append(item)
                return
            
            # Handle nested messages
            if field_descriptor.type == FieldDescriptor.TYPE_MESSAGE:
                if isinstance(value, dict):
                    nested_message = self.handle_nested_message(value, field_descriptor)
                    getattr(message, field_name).CopyFrom(nested_message)
                else:
                    setattr(message, field_name, value)
                return
            
            # Handle primitive fields
            setattr(message, field_name, value)
            
        except Exception as e:
            logger.warning("Failed to set message field",
                         field=field_name,
                         value_type=type(value).__name__,
                         error=str(e))
            raise
    
    def _apply_automatic_mapping(
        self, 
        message: Message, 
        data: Dict[str, Any], 
        message_fields: Dict[str, FieldDescriptor]
    ) -> None:
        """Apply automatic field mapping for unmapped fields.
        
        Args:
            message: Target protobuf message
            data: Input data dictionary
            message_fields: Message field descriptors
        """
        # Get fields that haven't been explicitly mapped
        mapped_fields = set(self.field_mappings.keys())
        auto_mapping_enabled = self.config.get("auto_mapping", {}).get("enabled", True)
        
        if not auto_mapping_enabled:
            return
        
        auto_map_count = 0
        
        for field_name, field_descriptor in message_fields.items():
            if field_name in mapped_fields or field_name not in data:
                continue
            
            value = data[field_name]
            if value is None:
                continue
            
            try:
                self._set_message_field(message, field_name, value, field_descriptor)
                auto_map_count += 1
                
                logger.debug("Auto-mapped field",
                           field=field_name,
                           value_type=type(value).__name__)
                
            except Exception as e:
                logger.debug("Auto-mapping failed for field",
                           field=field_name,
                           error=str(e))
                continue
        
        if auto_map_count > 0:
            logger.info("Applied automatic field mapping",
                      auto_mapped_count=auto_map_count)
    
    def get_supported_types(self) -> List[str]:
        """Get list of supported transformation types.
        
        Returns:
            List of supported type names
        """
        return [
            "string", "int32", "int64", "uint32", "uint64", 
            "float", "double", "bool", "bytes", "enum",
            "message", "repeated_*", "timestamp", "duration"
        ]
    
    def get_transformation_functions(self) -> Dict[str, str]:
        """Get available transformation functions with descriptions.
        
        Returns:
            Dictionary mapping function names to descriptions
        """
        return {
            name: func.description 
            for name, func in self.transformation_functions.items()
        }