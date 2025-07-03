"""Configuration validation system for transformation configurations.

This module provides comprehensive validation for transformation configurations,
including schema validation, semantic validation, and runtime validation.
"""

import re
import importlib
import inspect
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from dataclasses import dataclass
import yaml

from .config_schema import (
    TransformationConfig, 
    FieldMapping, 
    ProtobufConfig,
    DataSourceConfig,
    FunctionRegistry,
    ValidationRule,
    FieldType,
    DataType
)
from .function_registry import FunctionRegistry as TransformationRegistry
from ..exceptions import (
    ConfigurationException,
    InvalidConfigurationError,
    SchemaNotFoundError,
    ProtobufClassNotFoundError
)
from ..logging_config import get_schema_logger

logger = get_schema_logger(__name__)


@dataclass
class ValidationResult:
    """Result of configuration validation."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    suggestions: List[str]
    validated_config: Optional[TransformationConfig] = None


@dataclass
class FieldValidationResult:
    """Result of field mapping validation."""
    field_name: str
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    suggestions: List[str]


class ConfigurationValidator:
    """Comprehensive configuration validator."""
    
    def __init__(self, function_registry: Optional[TransformationRegistry] = None):
        """Initialize configuration validator.
        
        Args:
            function_registry: Function registry for validating function references
        """
        self.function_registry = function_registry
        self._protobuf_cache: Dict[str, Any] = {}
        
    def validate_config(
        self, 
        config: Union[str, Path, Dict[str, Any], TransformationConfig],
        strict: bool = False
    ) -> ValidationResult:
        """Validate a transformation configuration.
        
        Args:
            config: Configuration to validate (file path, dict, or config object)
            strict: Whether to treat warnings as errors
            
        Returns:
            ValidationResult with validation details
        """
        errors = []
        warnings = []
        suggestions = []
        
        try:
            # Parse configuration if needed
            if isinstance(config, (str, Path)):
                parsed_config = self._load_config_file(config)
            elif isinstance(config, dict):
                parsed_config = TransformationConfig(**config)
            elif isinstance(config, TransformationConfig):
                parsed_config = config
            else:
                raise ValueError(f"Unsupported config type: {type(config)}")
            
            logger.info("Starting configuration validation", 
                       config_name=parsed_config.name)
            
            # Validate configuration sections
            self._validate_protobuf_config(parsed_config.protobuf, errors, warnings, suggestions)
            self._validate_data_source_config(parsed_config.data_source, errors, warnings, suggestions)
            self._validate_field_mappings(parsed_config.field_mappings, errors, warnings, suggestions)
            self._validate_custom_functions(parsed_config.custom_functions, errors, warnings, suggestions)
            self._validate_semantic_consistency(parsed_config, errors, warnings, suggestions)
            
            # Check if strict mode should treat warnings as errors
            if strict and warnings:
                errors.extend([f"Strict mode: {warning}" for warning in warnings])
                warnings.clear()
            
            is_valid = len(errors) == 0
            
            logger.info("Configuration validation completed",
                       is_valid=is_valid,
                       errors_count=len(errors),
                       warnings_count=len(warnings))
            
            return ValidationResult(
                is_valid=is_valid,
                errors=errors,
                warnings=warnings,
                suggestions=suggestions,
                validated_config=parsed_config if is_valid else None
            )
            
        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            return ValidationResult(
                is_valid=False,
                errors=[f"Configuration parsing failed: {e}"],
                warnings=[],
                suggestions=["Check configuration file syntax and structure"]
            )
    
    def validate_field_mappings_against_schema(
        self,
        field_mappings: List[FieldMapping],
        protobuf_config: ProtobufConfig
    ) -> List[FieldValidationResult]:
        """Validate field mappings against protobuf schema.
        
        Args:
            field_mappings: Field mappings to validate
            protobuf_config: Protobuf configuration
            
        Returns:
            List of field validation results
        """
        results = []
        
        try:
            # Load protobuf class for schema inspection
            proto_class = self._load_protobuf_class(protobuf_config)
            if not proto_class:
                # If we can't load the class, return basic validation
                return [self._validate_field_mapping_basic(mapping) for mapping in field_mappings]
            
            # Get protobuf field information
            proto_fields = self._get_protobuf_fields(proto_class)
            
            for mapping in field_mappings:
                result = self._validate_field_mapping_against_schema(mapping, proto_fields)
                results.append(result)
                
        except Exception as e:
            logger.warning(f"Schema-based validation failed: {e}")
            # Fallback to basic validation
            results = [self._validate_field_mapping_basic(mapping) for mapping in field_mappings]
        
        return results
    
    def _load_config_file(self, config_path: Union[str, Path]) -> TransformationConfig:
        """Load and parse configuration file."""
        config_file = Path(config_path)
        
        if not config_file.exists():
            raise SchemaNotFoundError(
                schema_path=str(config_path),
                schema_type="configuration"
            )
        
        try:
            with open(config_file, 'r') as f:
                config_data = yaml.safe_load(f)
            
            return TransformationConfig(**config_data)
            
        except yaml.YAMLError as e:
            raise InvalidConfigurationError(
                config_path=str(config_path),
                config_errors=[f"YAML parsing error: {e}"]
            )
        except Exception as e:
            raise InvalidConfigurationError(
                config_path=str(config_path),
                config_errors=[f"Configuration validation error: {e}"]
            )
    
    def _validate_protobuf_config(
        self,
        protobuf_config: ProtobufConfig,
        errors: List[str],
        warnings: List[str],
        suggestions: List[str]
    ) -> None:
        """Validate protobuf configuration."""
        
        # Check proto file existence if specified
        if protobuf_config.proto_file:
            proto_file = Path(protobuf_config.proto_file)
            if not proto_file.exists():
                # Check in schema paths
                found = False
                for schema_path in protobuf_config.schema_paths:
                    full_path = Path(schema_path) / proto_file.name
                    if full_path.exists():
                        found = True
                        break
                
                if not found:
                    errors.append(f"Proto file not found: {protobuf_config.proto_file}")
                    suggestions.append("Verify proto file path or add to schema_paths")
        
        # Validate protobuf class if module specified
        if protobuf_config.proto_module and protobuf_config.proto_class:
            try:
                self._load_protobuf_class(protobuf_config)
            except Exception as e:
                errors.append(f"Failed to load protobuf class: {e}")
                suggestions.append("Ensure protobuf module is compiled and importable")
        
        # Validate schema paths
        for schema_path in protobuf_config.schema_paths:
            path = Path(schema_path)
            if not path.exists():
                warnings.append(f"Schema path not found: {schema_path}")
            elif not path.is_dir():
                errors.append(f"Schema path is not a directory: {schema_path}")
    
    def _validate_data_source_config(
        self,
        data_source_config: DataSourceConfig,
        errors: List[str],
        warnings: List[str],
        suggestions: List[str]
    ) -> None:
        """Validate data source configuration."""
        
        source_type = data_source_config.type
        
        if source_type in ["csv", "json"]:
            # Validate file-based sources
            if not data_source_config.file_path:
                errors.append(f"file_path required for {source_type} data source")
            else:
                file_path = Path(data_source_config.file_path)
                if not file_path.exists():
                    errors.append(f"Data source file not found: {data_source_config.file_path}")
                elif source_type == "csv" and not file_path.suffix.lower() == ".csv":
                    warnings.append("CSV data source file does not have .csv extension")
                elif source_type == "json" and not file_path.suffix.lower() == ".json":
                    warnings.append("JSON data source file does not have .json extension")
        
        elif source_type == "faker":
            # Validate Faker configuration
            if data_source_config.locale:
                try:
                    from faker import Faker
                    # Test locale
                    Faker(data_source_config.locale)
                except Exception as e:
                    warnings.append(f"Invalid Faker locale '{data_source_config.locale}': {e}")
                    suggestions.append("Use standard locale codes like 'en_US', 'de_DE', etc.")
            
            # Validate custom providers
            for provider in data_source_config.providers or []:
                try:
                    importlib.import_module(provider)
                except ImportError:
                    warnings.append(f"Custom Faker provider not found: {provider}")
        
        elif source_type == "custom":
            # Validate custom data source
            if not data_source_config.class_path:
                errors.append("class_path required for custom data source")
            else:
                try:
                    self._import_class(data_source_config.class_path)
                except Exception as e:
                    errors.append(f"Failed to load custom data source: {e}")
    
    def _validate_field_mappings(
        self,
        field_mappings: List[FieldMapping],
        errors: List[str],
        warnings: List[str],
        suggestions: List[str]
    ) -> None:
        """Validate field mappings."""
        
        target_fields = set()
        
        for i, mapping in enumerate(field_mappings):
            field_prefix = f"Field mapping {i + 1} ('{mapping.target}')"
            
            # Check for duplicate targets
            if mapping.target in target_fields:
                errors.append(f"{field_prefix}: Duplicate target field")
            target_fields.add(mapping.target)
            
            # Validate field type consistency
            if mapping.type == FieldType.DIRECT and not mapping.source:
                errors.append(f"{field_prefix}: Direct mapping requires source field")
            
            if mapping.type == FieldType.COMPUTED and not mapping.function:
                errors.append(f"{field_prefix}: Computed mapping requires function")
            
            if mapping.type == FieldType.CONDITIONAL and not mapping.conditions:
                errors.append(f"{field_prefix}: Conditional mapping requires conditions")
            
            # Validate function references
            if mapping.function:
                self._validate_function_reference(
                    mapping.function, f"{field_prefix}: Function", 
                    errors, warnings, suggestions
                )
            
            # Validate nested mappings
            for j, nested in enumerate(mapping.nested_mappings or []):
                nested_prefix = f"{field_prefix}, nested {j + 1}"
                self._validate_nested_mapping(nested, nested_prefix, errors, warnings)
            
            # Validate conditions
            for j, condition in enumerate(mapping.conditions or []):
                condition_prefix = f"{field_prefix}, condition {j + 1}"
                self._validate_conditional_mapping(condition, condition_prefix, errors, warnings, suggestions)
            
            # Validate validation rules
            for j, rule in enumerate(mapping.validation or []):
                rule_prefix = f"{field_prefix}, validation {j + 1}"
                self._validate_validation_rule(rule, rule_prefix, errors, warnings)
    
    def _validate_custom_functions(
        self,
        custom_functions: List[FunctionRegistry],
        errors: List[str],
        warnings: List[str],
        suggestions: List[str]
    ) -> None:
        """Validate custom function registrations."""
        
        function_names = set()
        
        for i, func_config in enumerate(custom_functions or []):
            func_prefix = f"Custom function {i + 1} ('{func_config.name}')"
            
            # Check for duplicate names
            if func_config.name in function_names:
                errors.append(f"{func_prefix}: Duplicate function name")
            function_names.add(func_config.name)
            
            # Validate function reference
            try:
                if func_config.callable_path:
                    self._import_callable(func_config.callable_path)
                elif func_config.module:
                    module = importlib.import_module(func_config.module)
                    if not hasattr(module, func_config.name):
                        errors.append(f"{func_prefix}: Function not found in module")
                else:
                    errors.append(f"{func_prefix}: Either module or callable_path required")
                    
            except Exception as e:
                errors.append(f"{func_prefix}: Failed to load function: {e}")
    
    def _validate_semantic_consistency(
        self,
        config: TransformationConfig,
        errors: List[str],
        warnings: List[str],
        suggestions: List[str]
    ) -> None:
        """Validate semantic consistency across configuration."""
        
        # Check that required fields are mapped
        if config.protobuf.strict_mode:
            # Would need protobuf introspection to check required fields
            pass
        
        # Validate function references against registry
        used_functions = set()
        for mapping in config.field_mappings:
            if mapping.function:
                used_functions.add(mapping.function)
            for condition in mapping.conditions or []:
                if condition.function:
                    used_functions.add(condition.function)
        
        # Check if custom functions cover all used functions
        custom_function_names = {func.name for func in config.custom_functions or []}
        
        for func_name in used_functions:
            if not func_name.startswith('faker.') and func_name not in custom_function_names:
                if self.function_registry and not self.function_registry.get_function(func_name):
                    warnings.append(f"Function '{func_name}' not found in registry or custom functions")
                    suggestions.append(f"Add '{func_name}' to custom_functions or check function name")
    
    def _validate_function_reference(
        self,
        function_name: str,
        context: str,
        errors: List[str],
        warnings: List[str],
        suggestions: List[str]
    ) -> None:
        """Validate a function reference."""
        
        # Built-in functions (always valid)
        builtin_functions = {
            'upper', 'lower', 'strip', 'title', 'reverse',
            'abs', 'round', 'ceil', 'floor',
            'int', 'float', 'str', 'bool',
            'len', 'first', 'last'
        }
        
        if function_name in builtin_functions:
            return
        
        # Faker functions
        if function_name.startswith('faker.'):
            faker_method = function_name[6:]  # Remove 'faker.' prefix
            try:
                from faker import Faker
                fake = Faker()
                if not hasattr(fake, faker_method):
                    warnings.append(f"{context}: Faker method '{faker_method}' not found")
            except ImportError:
                warnings.append(f"{context}: Faker library not available")
            return
        
        # Check function registry
        if self.function_registry:
            if not self.function_registry.get_function(function_name):
                warnings.append(f"{context}: Function '{function_name}' not found in registry")
                suggestions.append(f"Register function '{function_name}' or check function name")
        else:
            warnings.append(f"{context}: Cannot validate function '{function_name}' - no registry available")
    
    def _validate_nested_mapping(
        self,
        nested_mapping,
        context: str,
        errors: List[str],
        warnings: List[str]
    ) -> None:
        """Validate nested field mapping."""
        
        # Validate path format
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', nested_mapping.source_path):
            errors.append(f"{context}: Invalid source path format")
        
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', nested_mapping.target_path):
            errors.append(f"{context}: Invalid target path format")
    
    def _validate_conditional_mapping(
        self,
        condition,
        context: str,
        errors: List[str],
        warnings: List[str],
        suggestions: List[str]
    ) -> None:
        """Validate conditional mapping."""
        
        # Basic condition syntax validation
        if not condition.condition.strip():
            errors.append(f"{context}: Empty condition")
            return
        
        # Validate function reference if present
        if condition.function:
            self._validate_function_reference(
                condition.function, context, errors, warnings, suggestions
            )
        
        # Check that either value or function is provided
        if not condition.value and not condition.function:
            errors.append(f"{context}: Either value or function must be provided")
    
    def _validate_validation_rule(
        self,
        rule: ValidationRule,
        context: str,
        errors: List[str],
        warnings: List[str]
    ) -> None:
        """Validate a validation rule."""
        
        if rule.type == "pattern":
            if not isinstance(rule.value, str):
                errors.append(f"{context}: Pattern rule requires string value")
            else:
                try:
                    re.compile(rule.value)
                except re.error as e:
                    errors.append(f"{context}: Invalid regex pattern: {e}")
        
        elif rule.type in ["min", "max"]:
            if not isinstance(rule.value, (int, float)):
                errors.append(f"{context}: Min/max rule requires numeric value")
        
        elif rule.type == "enum":
            if not isinstance(rule.value, list):
                errors.append(f"{context}: Enum rule requires list of values")
    
    def _validate_field_mapping_basic(self, mapping: FieldMapping) -> FieldValidationResult:
        """Basic field mapping validation without schema."""
        errors = []
        warnings = []
        suggestions = []
        
        # Basic validation logic here
        if mapping.type == FieldType.DIRECT and not mapping.source:
            errors.append("Direct mapping requires source field")
        
        return FieldValidationResult(
            field_name=mapping.target,
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            suggestions=suggestions
        )
    
    def _validate_field_mapping_against_schema(
        self,
        mapping: FieldMapping,
        proto_fields: Dict[str, Any]
    ) -> FieldValidationResult:
        """Validate field mapping against protobuf schema."""
        errors = []
        warnings = []
        suggestions = []
        
        # Check if target field exists in schema
        if mapping.target not in proto_fields:
            errors.append(f"Target field '{mapping.target}' not found in protobuf schema")
            suggestions.append(f"Available fields: {', '.join(proto_fields.keys())}")
        else:
            # Validate data type compatibility
            proto_field_info = proto_fields[mapping.target]
            if mapping.data_type:
                # Type compatibility checking logic would go here
                pass
        
        return FieldValidationResult(
            field_name=mapping.target,
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            suggestions=suggestions
        )
    
    def _load_protobuf_class(self, protobuf_config: ProtobufConfig) -> Optional[type]:
        """Load protobuf class for schema inspection."""
        
        if protobuf_config.proto_module and protobuf_config.proto_class:
            cache_key = f"{protobuf_config.proto_module}.{protobuf_config.proto_class}"
            
            if cache_key in self._protobuf_cache:
                return self._protobuf_cache[cache_key]
            
            try:
                module = importlib.import_module(protobuf_config.proto_module)
                proto_class = getattr(module, protobuf_config.proto_class)
                self._protobuf_cache[cache_key] = proto_class
                return proto_class
            except Exception as e:
                logger.warning(f"Failed to load protobuf class: {e}")
                return None
        
        return None
    
    def _get_protobuf_fields(self, proto_class: type) -> Dict[str, Any]:
        """Extract field information from protobuf class."""
        fields = {}
        
        try:
            # Use protobuf descriptor to get field information
            if hasattr(proto_class, 'DESCRIPTOR'):
                descriptor = proto_class.DESCRIPTOR
                for field in descriptor.fields:
                    fields[field.name] = {
                        'type': field.type,
                        'label': field.label,
                        'number': field.number
                    }
        except Exception as e:
            logger.warning(f"Failed to extract protobuf fields: {e}")
        
        return fields
    
    def _import_class(self, class_path: str) -> type:
        """Import a class from a dotted path."""
        module_path, class_name = class_path.rsplit('.', 1)
        module = importlib.import_module(module_path)
        return getattr(module, class_name)
    
    def _import_callable(self, callable_path: str) -> callable:
        """Import a callable from a dotted path."""
        module_path, callable_name = callable_path.rsplit('.', 1)
        module = importlib.import_module(module_path)
        return getattr(module, callable_name)


# Validation utilities

def validate_config_file(config_path: str, strict: bool = False) -> ValidationResult:
    """Validate a transformation configuration file.
    
    Args:
        config_path: Path to configuration file
        strict: Whether to treat warnings as errors
        
    Returns:
        ValidationResult
    """
    validator = ConfigurationValidator()
    return validator.validate_config(config_path, strict=strict)


def validate_config_dict(config_dict: Dict[str, Any], strict: bool = False) -> ValidationResult:
    """Validate a transformation configuration dictionary.
    
    Args:
        config_dict: Configuration dictionary
        strict: Whether to treat warnings as errors
        
    Returns:
        ValidationResult
    """
    validator = ConfigurationValidator()
    return validator.validate_config(config_dict, strict=strict)


def check_config_completeness(config: TransformationConfig) -> Tuple[bool, List[str]]:
    """Check if configuration is complete for production use.
    
    Args:
        config: Configuration to check
        
    Returns:
        Tuple of (is_complete, missing_items)
    """
    missing = []
    
    # Check essential fields
    if not config.field_mappings:
        missing.append("field_mappings")
    
    if not config.protobuf.proto_file and not (config.protobuf.proto_module and config.protobuf.proto_class):
        missing.append("protobuf schema specification")
    
    if config.output.topic and not config.output.schema_registry_url:
        missing.append("schema_registry_url for Kafka output")
    
    # Check for reasonable field coverage
    if len(config.field_mappings) < 3:
        missing.append("sufficient field mappings (recommend at least 3)")
    
    return len(missing) == 0, missing