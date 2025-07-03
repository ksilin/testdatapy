"""Custom exceptions for TestDataPy schema operations.

This module provides a comprehensive set of exceptions for schema-related operations,
enabling precise error handling and user-friendly error messages.
"""

import logging
from typing import Any, Optional


class TestDataPyException(Exception):
    """Base exception for all TestDataPy errors.
    
    Provides common functionality for error reporting and logging.
    """
    
    def __init__(
        self,
        message: str,
        details: Optional[str] = None,
        suggestions: Optional[list[str]] = None,
        original_error: Optional[Exception] = None
    ):
        """Initialize TestDataPy exception.
        
        Args:
            message: Main error message
            details: Additional technical details  
            suggestions: List of suggested solutions
            original_error: Original exception that caused this error
        """
        super().__init__(message)
        self.message = message
        self.details = details
        self.suggestions = suggestions or []
        self.original_error = original_error
        
        # Log the error
        self._log_error()
    
    def _log_error(self) -> None:
        """Log the error with appropriate level."""
        logger = logging.getLogger(self.__class__.__module__)
        logger.error(f"{self.__class__.__name__}: {self.message}")
        if self.details:
            logger.debug(f"Details: {self.details}")
        if self.original_error:
            logger.debug(f"Original error: {self.original_error}")
    
    def get_user_message(self) -> str:
        """Get user-friendly error message with suggestions."""
        msg = self.message
        if self.suggestions:
            msg += "\n\nSuggestions:"
            for i, suggestion in enumerate(self.suggestions, 1):
                msg += f"\n  {i}. {suggestion}"
        return msg


# Schema-related exceptions

class SchemaException(TestDataPyException):
    """Base exception for schema-related errors."""
    pass


class SchemaNotFoundError(SchemaException):
    """Raised when a schema file or resource cannot be found."""
    
    def __init__(
        self,
        schema_path: str,
        schema_type: str = "schema",
        search_paths: Optional[list[str]] = None
    ):
        suggestions = [
            f"Verify the {schema_type} file exists at: {schema_path}",
            f"Check file permissions for {schema_path}",
        ]
        
        if search_paths:
            suggestions.append(f"Searched in paths: {', '.join(search_paths)}")
            suggestions.append("Use --schema-path to specify additional search directories")
        
        super().__init__(
            message=f"{schema_type.title()} file not found: {schema_path}",
            suggestions=suggestions
        )
        self.schema_path = schema_path
        self.schema_type = schema_type


class SchemaCompilationError(SchemaException):
    """Raised when schema compilation fails."""
    
    def __init__(
        self,
        schema_path: str,
        compilation_error: str,
        compiler_output: Optional[str] = None
    ):
        suggestions = [
            "Check schema syntax and format",
            "Verify all dependencies are available",
            "Ensure protoc compiler is installed and accessible"
        ]
        
        details = f"Compilation output: {compiler_output}" if compiler_output else None
        
        super().__init__(
            message=f"Failed to compile schema: {schema_path}",
            details=f"Compiler error: {compilation_error}\n{details}" if details else compilation_error,
            suggestions=suggestions
        )
        self.schema_path = schema_path
        self.compilation_error = compilation_error


class SchemaValidationError(SchemaException):
    """Raised when schema validation fails."""
    
    def __init__(
        self,
        schema_path: str,
        validation_errors: list[str],
        schema_type: str = "schema"
    ):
        error_list = "\n".join(f"  - {error}" for error in validation_errors)
        
        suggestions = [
            f"Fix the validation errors in {schema_path}",
            f"Refer to {schema_type} specification documentation",
            "Validate dependencies and imports"
        ]
        
        super().__init__(
            message=f"Schema validation failed: {schema_path}",
            details=f"Validation errors:\n{error_list}",
            suggestions=suggestions
        )
        self.schema_path = schema_path
        self.validation_errors = validation_errors


# Protobuf-specific exceptions

class ProtobufException(SchemaException):
    """Base exception for protobuf-related errors."""
    pass


class ProtobufCompilerNotFoundError(ProtobufException):
    """Raised when protoc compiler is not found."""
    
    def __init__(self):
        suggestions = [
            "Install Protocol Buffers compiler (protoc)",
            "On macOS: brew install protobuf",
            "On Ubuntu: apt-get install protobuf-compiler",
            "On Windows: Download from https://github.com/protocolbuffers/protobuf/releases",
            "Ensure protoc is in your PATH"
        ]
        
        super().__init__(
            message="Protocol Buffers compiler (protoc) not found",
            suggestions=suggestions
        )


class ProtobufClassNotFoundError(ProtobufException):
    """Raised when protobuf class cannot be loaded."""
    
    def __init__(
        self,
        class_spec: str,
        module_name: Optional[str] = None,
        search_paths: Optional[list[str]] = None
    ):
        suggestions = [
            "Verify the protobuf module is compiled and available",
            "Check PYTHONPATH includes the protobuf module directory"
        ]
        
        if module_name:
            suggestions.extend([
                f"Ensure module '{module_name}' is importable",
                f"Try: python -c 'import {module_name}'"
            ])
        
        if search_paths:
            suggestions.append(f"Use --schema-path to add search directories: {', '.join(search_paths)}")
        
        super().__init__(
            message=f"Protobuf class not found: {class_spec}",
            suggestions=suggestions
        )
        self.class_spec = class_spec
        self.module_name = module_name


class ProtobufImportError(ProtobufException):
    """Raised when protobuf module import fails."""
    
    def __init__(
        self,
        module_name: str,
        import_error: Exception,
        search_paths: Optional[list[str]] = None
    ):
        suggestions = [
            f"Ensure protobuf module '{module_name}' is compiled",
            "Verify all protobuf dependencies are available",
            "Check PYTHONPATH includes protobuf module directories"
        ]
        
        if search_paths:
            suggestions.append(f"Add search paths: {', '.join(search_paths)}")
        
        super().__init__(
            message=f"Failed to import protobuf module: {module_name}",
            details=f"Import error: {import_error}",
            suggestions=suggestions,
            original_error=import_error
        )
        self.module_name = module_name


class ProtobufSerializationError(ProtobufException):
    """Raised when protobuf serialization fails."""
    
    def __init__(
        self,
        data: Any,
        proto_class: type,
        serialization_error: Exception
    ):
        suggestions = [
            "Verify data structure matches protobuf schema",
            "Check for missing required fields",
            "Ensure data types are compatible with protobuf field types",
            "Validate nested message structures"
        ]
        
        super().__init__(
            message=f"Failed to serialize data to protobuf message: {proto_class.__name__}",
            details=f"Serialization error: {serialization_error}",
            suggestions=suggestions,
            original_error=serialization_error
        )
        self.data = data
        self.proto_class = proto_class


# Schema Registry exceptions

class SchemaRegistryException(TestDataPyException):
    """Base exception for Schema Registry errors."""
    pass


class SchemaRegistryConnectionError(SchemaRegistryException):
    """Raised when Schema Registry connection fails."""
    
    def __init__(
        self,
        registry_url: str,
        connection_error: Exception
    ):
        suggestions = [
            f"Verify Schema Registry is running at: {registry_url}",
            "Check network connectivity",
            "Verify authentication credentials if required",
            "Check firewall and security group settings"
        ]
        
        super().__init__(
            message=f"Failed to connect to Schema Registry: {registry_url}",
            details=f"Connection error: {connection_error}",
            suggestions=suggestions,
            original_error=connection_error
        )
        self.registry_url = registry_url


class SchemaRegistrationError(SchemaRegistryException):
    """Raised when schema registration fails."""
    
    def __init__(
        self,
        subject: str,
        schema_content: str,
        registration_error: Exception
    ):
        suggestions = [
            "Verify schema is valid and well-formed",
            "Check schema compatibility with existing versions",
            "Ensure proper authentication and authorization",
            "Verify subject naming follows conventions"
        ]
        
        super().__init__(
            message=f"Failed to register schema for subject: {subject}",
            details=f"Registration error: {registration_error}",
            suggestions=suggestions,
            original_error=registration_error
        )
        self.subject = subject
        self.schema_content = schema_content


class SchemaCompatibilityError(SchemaRegistryException):
    """Raised when schema compatibility check fails."""
    
    def __init__(
        self,
        subject: str,
        new_schema: str,
        compatibility_errors: list[str]
    ):
        error_list = "\n".join(f"  - {error}" for error in compatibility_errors)
        
        suggestions = [
            "Review schema evolution guidelines",
            "Consider backward/forward compatibility requirements",
            "Make only compatible changes (add optional fields, etc.)",
            "Use schema versioning if breaking changes are necessary"
        ]
        
        super().__init__(
            message=f"Schema compatibility check failed for subject: {subject}",
            details=f"Compatibility errors:\n{error_list}",
            suggestions=suggestions
        )
        self.subject = subject
        self.new_schema = new_schema
        self.compatibility_errors = compatibility_errors


# Configuration exceptions

class ConfigurationException(TestDataPyException):
    """Base exception for configuration errors."""
    pass


class InvalidConfigurationError(ConfigurationException):
    """Raised when configuration is invalid."""
    
    def __init__(
        self,
        config_path: str,
        config_errors: list[str]
    ):
        error_list = "\n".join(f"  - {error}" for error in config_errors)
        
        suggestions = [
            f"Fix configuration errors in: {config_path}",
            "Refer to configuration documentation",
            "Validate JSON/YAML syntax",
            "Check required fields are present"
        ]
        
        super().__init__(
            message=f"Invalid configuration: {config_path}",
            details=f"Configuration errors:\n{error_list}",
            suggestions=suggestions
        )
        self.config_path = config_path
        self.config_errors = config_errors


class MissingConfigurationError(ConfigurationException):
    """Raised when required configuration is missing."""
    
    def __init__(
        self,
        missing_config: str,
        config_type: str = "configuration"
    ):
        suggestions = [
            f"Provide {config_type}: {missing_config}",
            f"Use configuration file or command-line options",
            "Refer to documentation for required settings"
        ]
        
        super().__init__(
            message=f"Missing required {config_type}: {missing_config}",
            suggestions=suggestions
        )
        self.missing_config = missing_config


# Producer exceptions

class ProducerException(TestDataPyException):
    """Base exception for producer errors."""
    pass


class ProducerConnectionError(ProducerException):
    """Raised when producer cannot connect to Kafka."""
    
    def __init__(
        self,
        bootstrap_servers: str,
        connection_error: Exception
    ):
        suggestions = [
            f"Verify Kafka is running at: {bootstrap_servers}",
            "Check network connectivity to Kafka brokers",
            "Verify authentication credentials",
            "Check firewall and security group settings"
        ]
        
        super().__init__(
            message=f"Failed to connect to Kafka: {bootstrap_servers}",
            details=f"Connection error: {connection_error}",
            suggestions=suggestions,
            original_error=connection_error
        )
        self.bootstrap_servers = bootstrap_servers


class MessageProductionError(ProducerException):
    """Raised when message production fails."""
    
    def __init__(
        self,
        topic: str,
        message_data: Any,
        production_error: Exception
    ):
        suggestions = [
            f"Verify topic '{topic}' exists and is accessible",
            "Check message format and serialization",
            "Verify producer permissions for the topic",
            "Check Kafka broker health and capacity"
        ]
        
        super().__init__(
            message=f"Failed to produce message to topic: {topic}",
            details=f"Production error: {production_error}",
            suggestions=suggestions,
            original_error=production_error
        )
        self.topic = topic
        self.message_data = message_data


# Utility functions for error handling

def handle_and_reraise(
    original_error: Exception,
    error_context: str,
    suggestions: Optional[list[str]] = None
) -> None:
    """Handle an error and re-raise as TestDataPy exception.
    
    Args:
        original_error: The original exception
        error_context: Description of what was being attempted
        suggestions: Optional list of suggestions for fixing the error
    
    Raises:
        TestDataPyException: Re-raised as appropriate TestDataPy exception
    """
    if isinstance(original_error, TestDataPyException):
        # Already a TestDataPy exception, just re-raise
        raise original_error
    
    # Map common exceptions to specific TestDataPy exceptions
    if isinstance(original_error, FileNotFoundError):
        # Extract file path from the error message
        file_path = str(original_error).replace("[Errno 2] No such file or directory: ", "").strip("'\"")
        if not file_path:
            file_path = str(original_error)
        
        # Create SchemaNotFoundError and add suggestions if provided
        exc = SchemaNotFoundError(schema_path=file_path, schema_type="schema")
        if suggestions:
            exc.suggestions.extend(suggestions)
        raise exc from original_error
    elif isinstance(original_error, ImportError):
        # Create ProtobufImportError
        exc = ProtobufImportError(
            module_name=error_context,
            import_error=original_error
        )
        if suggestions:
            exc.suggestions.extend(suggestions)
        raise exc from original_error
    else:
        # Generic TestDataPy exception
        raise TestDataPyException(
            message=f"Error in {error_context}: {original_error}",
            suggestions=suggestions,
            original_error=original_error
        ) from original_error


def create_error_logger(name: str) -> logging.Logger:
    """Create a logger specifically for error handling.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Only configure if not already configured
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    
    return logger