"""Schema-specific exception classes for TestDataPy.

This module provides specialized exceptions for schema operations with
comprehensive error reporting and user-friendly messages.
"""

import logging
from typing import Any, Dict, List, Optional
from pathlib import Path
from .error_messages import get_user_friendly_message, suggest_next_steps


class SchemaError(Exception):
    """Base exception for schema-related errors.
    
    Provides common functionality for schema error reporting and logging.
    """
    
    def __init__(
        self,
        message: str,
        details: Optional[str] = None,
        suggestions: Optional[List[str]] = None,
        original_error: Optional[Exception] = None,
        schema_path: Optional[str] = None
    ):
        """Initialize schema error.
        
        Args:
            message: Main error message
            details: Additional technical details
            suggestions: List of suggested solutions
            original_error: Original exception that caused this error
            schema_path: Path to schema file that caused the error
        """
        super().__init__(message)
        self.message = message
        self.details = details
        self.suggestions = suggestions or []
        self.original_error = original_error
        self.schema_path = schema_path
        
        # Log the error
        self._log_error()
    
    def _log_error(self) -> None:
        """Log the error with appropriate level."""
        logger = logging.getLogger(self.__class__.__module__)
        logger.error(f"{self.__class__.__name__}: {self.message}")
        if self.schema_path:
            logger.debug(f"Schema path: {self.schema_path}")
        if self.details:
            logger.debug(f"Details: {self.details}")
        if self.original_error:
            logger.debug(f"Original error: {self.original_error}")
    
    def get_user_message(self, include_debug_info: bool = False) -> str:
        """Get user-friendly error message with suggestions."""
        msg = self.message
        if self.schema_path:
            msg += f"\nSchema: {self.schema_path}"
        if self.suggestions:
            msg += "\n\nSuggestions:"
            for i, suggestion in enumerate(self.suggestions, 1):
                msg += f"\n  {i}. {suggestion}"
        return msg
    
    def get_comprehensive_message(
        self, 
        include_installation_guide: bool = True,
        include_debug_info: bool = False
    ) -> str:
        """Get comprehensive user-friendly error message.
        
        Args:
            include_installation_guide: Whether to include installation instructions
            include_debug_info: Whether to include debug information
            
        Returns:
            Comprehensive error message with troubleshooting guidance
        """
        # Default to basic user message if no specific error type
        return self.get_user_message(include_debug_info)


class CompilationError(SchemaError):
    """Exception raised when protobuf compilation fails."""
    
    def __init__(
        self,
        message: str,
        schema_path: Optional[str] = None,
        compiler_output: Optional[str] = None,
        exit_code: Optional[int] = None,
        original_error: Optional[Exception] = None
    ):
        """Initialize compilation error.
        
        Args:
            message: Main error message
            schema_path: Path to proto file that failed compilation
            compiler_output: Output from protoc compiler
            exit_code: Exit code from protoc compiler
            original_error: Original exception if any
        """
        suggestions = [
            "Check protobuf syntax and imports",
            "Verify all imported proto files exist",
            "Ensure protoc compiler is installed and accessible",
            "Check protobuf version compatibility"
        ]
        
        if schema_path:
            suggestions.insert(0, f"Review schema file: {schema_path}")
        
        details = []
        if compiler_output:
            details.append(f"Compiler output: {compiler_output}")
        if exit_code is not None:
            details.append(f"Exit code: {exit_code}")
        
        super().__init__(
            message=message,
            details="\n".join(details) if details else None,
            suggestions=suggestions,
            original_error=original_error,
            schema_path=schema_path
        )
        
        self.compiler_output = compiler_output
        self.exit_code = exit_code
    
    def get_comprehensive_message(
        self, 
        include_installation_guide: bool = True,
        include_debug_info: bool = False
    ) -> str:
        """Get comprehensive user-friendly error message for compilation errors."""
        context = {}
        if self.schema_path:
            context["schema_path"] = self.schema_path
        if self.compiler_output:
            context["compiler_output"] = self.compiler_output[:200] + "..." if len(self.compiler_output) > 200 else self.compiler_output
        if self.exit_code is not None:
            context["exit_code"] = str(self.exit_code)
        
        # Determine specific error type based on the error
        error_type = "proto_compilation_failed"
        if "not found" in self.message.lower() and "protoc" in self.message.lower():
            error_type = "protoc_not_found"
        elif "import" in self.message.lower() and ("not found" in self.message.lower() or "cannot" in self.message.lower()):
            error_type = "import_resolution_failed"
        elif "permission" in self.message.lower() or "denied" in self.message.lower():
            error_type = "permission_denied"
        
        return get_user_friendly_message(
            error_type=error_type,
            context=context,
            include_installation_guide=include_installation_guide,
            include_debug_info=include_debug_info
        )


class ValidationError(SchemaError):
    """Exception raised when protobuf schema validation fails."""
    
    def __init__(
        self,
        message: str,
        schema_path: Optional[str] = None,
        validation_errors: Optional[List[str]] = None,
        field_errors: Optional[Dict[str, str]] = None,
        original_error: Optional[Exception] = None
    ):
        """Initialize validation error.
        
        Args:
            message: Main error message
            schema_path: Path to schema file that failed validation
            validation_errors: List of specific validation errors
            field_errors: Dict of field names to error messages
            original_error: Original exception if any
        """
        suggestions = [
            "Review protobuf schema syntax",
            "Check field types and naming conventions",
            "Verify message structure and nesting",
            "Ensure all required fields are defined"
        ]
        
        if schema_path:
            suggestions.insert(0, f"Review schema file: {schema_path}")
        
        details = []
        if validation_errors:
            details.append("Validation errors:")
            details.extend(f"  - {error}" for error in validation_errors)
        
        if field_errors:
            details.append("Field errors:")
            details.extend(f"  - {field}: {error}" for field, error in field_errors.items())
        
        super().__init__(
            message=message,
            details="\n".join(details) if details else None,
            suggestions=suggestions,
            original_error=original_error,
            schema_path=schema_path
        )
        
        self.validation_errors = validation_errors or []
        self.field_errors = field_errors or {}


class SchemaNotFoundError(SchemaError):
    """Exception raised when a schema file cannot be found."""
    
    def __init__(
        self,
        schema_path: str,
        search_paths: Optional[List[str]] = None,
        schema_type: str = "protobuf schema"
    ):
        """Initialize schema not found error.
        
        Args:
            schema_path: Path to schema that was not found
            search_paths: List of paths that were searched
            schema_type: Type of schema (for error message)
        """
        suggestions = [
            f"Verify the {schema_type} file exists: {schema_path}",
            f"Check file permissions for {schema_path}",
            "Use absolute path or correct relative path"
        ]
        
        if search_paths:
            suggestions.extend([
                f"Searched in: {', '.join(search_paths)}",
                "Add the correct path to schema search paths"
            ])
        
        message = f"{schema_type.title()} file not found: {schema_path}"
        
        super().__init__(
            message=message,
            suggestions=suggestions,
            schema_path=schema_path
        )
        
        self.search_paths = search_paths or []
        self.schema_type = schema_type
    
    def get_comprehensive_message(
        self, 
        include_installation_guide: bool = False,
        include_debug_info: bool = False
    ) -> str:
        """Get comprehensive user-friendly error message for schema not found errors."""
        context = {
            "schema_path": self.schema_path,
            "schema_type": self.schema_type
        }
        if self.search_paths:
            context["search_paths"] = ", ".join(self.search_paths)
        
        return get_user_friendly_message(
            error_type="proto_file_not_found",
            context=context,
            include_installation_guide=include_installation_guide,
            include_debug_info=include_debug_info
        )


class SchemaDependencyError(SchemaError):
    """Exception raised when schema dependencies cannot be resolved."""
    
    def __init__(
        self,
        message: str,
        schema_path: str,
        missing_dependencies: Optional[List[str]] = None,
        dependency_chain: Optional[List[str]] = None,
        original_error: Optional[Exception] = None
    ):
        """Initialize schema dependency error.
        
        Args:
            message: Main error message
            schema_path: Path to schema with dependency issues
            missing_dependencies: List of missing dependency files
            dependency_chain: Chain of dependencies leading to error
            original_error: Original exception if any
        """
        suggestions = [
            "Check that all imported proto files exist",
            "Verify import paths in proto files",
            "Add missing dependencies to schema paths"
        ]
        
        if missing_dependencies:
            suggestions.append(f"Missing files: {', '.join(missing_dependencies)}")
        
        details = []
        if missing_dependencies:
            details.append(f"Missing dependencies: {', '.join(missing_dependencies)}")
        if dependency_chain:
            details.append(f"Dependency chain: {' -> '.join(dependency_chain)}")
        
        super().__init__(
            message=message,
            details="\n".join(details) if details else None,
            suggestions=suggestions,
            original_error=original_error,
            schema_path=schema_path
        )
        
        self.missing_dependencies = missing_dependencies or []
        self.dependency_chain = dependency_chain or []
    
    def get_comprehensive_message(
        self, 
        include_installation_guide: bool = False,
        include_debug_info: bool = False
    ) -> str:
        """Get comprehensive user-friendly error message for dependency errors."""
        context = {
            "schema_path": self.schema_path
        }
        if self.missing_dependencies:
            context["missing_dependencies"] = ", ".join(self.missing_dependencies)
        if self.dependency_chain:
            context["dependency_chain"] = " -> ".join(self.dependency_chain)
        
        # Determine if this is a circular dependency or missing import
        error_type = "import_resolution_failed"
        if "circular" in self.message.lower():
            error_type = "circular_dependency"
        
        return get_user_friendly_message(
            error_type=error_type,
            context=context,
            include_installation_guide=include_installation_guide,
            include_debug_info=include_debug_info
        )


class SchemaRegistrationError(SchemaError):
    """Exception raised when schema registration fails."""
    
    def __init__(
        self,
        message: str,
        subject: str,
        schema_registry_url: Optional[str] = None,
        schema_content: Optional[str] = None,
        original_error: Optional[Exception] = None
    ):
        """Initialize schema registration error.
        
        Args:
            message: Main error message
            subject: Schema Registry subject name
            schema_registry_url: URL of Schema Registry
            schema_content: Schema content that failed registration
            original_error: Original exception if any
        """
        suggestions = [
            "Verify Schema Registry is accessible",
            "Check authentication credentials",
            "Validate schema format and compatibility",
            "Ensure subject naming follows conventions"
        ]
        
        if schema_registry_url:
            suggestions.insert(0, f"Check Schema Registry at: {schema_registry_url}")
        
        details = []
        if subject:
            details.append(f"Subject: {subject}")
        if schema_registry_url:
            details.append(f"Registry URL: {schema_registry_url}")
        
        super().__init__(
            message=message,
            details="\n".join(details) if details else None,
            suggestions=suggestions,
            original_error=original_error
        )
        
        self.subject = subject
        self.schema_registry_url = schema_registry_url
        self.schema_content = schema_content


class SchemaCompatibilityError(SchemaError):
    """Exception raised when schema compatibility check fails."""
    
    def __init__(
        self,
        message: str,
        subject: str,
        new_schema: str,
        compatibility_errors: List[str],
        compatibility_level: Optional[str] = None,
        original_error: Optional[Exception] = None
    ):
        """Initialize schema compatibility error.
        
        Args:
            message: Main error message
            subject: Schema Registry subject name
            new_schema: New schema content
            compatibility_errors: List of compatibility errors
            compatibility_level: Current compatibility level
            original_error: Original exception if any
        """
        suggestions = [
            "Review schema evolution guidelines",
            "Make only backward-compatible changes",
            "Add optional fields instead of required ones",
            "Consider schema versioning for breaking changes"
        ]
        
        if compatibility_level:
            suggestions.insert(0, f"Current compatibility level: {compatibility_level}")
        
        error_list = "\n".join(f"  - {error}" for error in compatibility_errors)
        details = f"Compatibility errors:\n{error_list}"
        
        if compatibility_level:
            details += f"\nCompatibility level: {compatibility_level}"
        
        super().__init__(
            message=message,
            details=details,
            suggestions=suggestions,
            original_error=original_error
        )
        
        self.subject = subject
        self.new_schema = new_schema
        self.compatibility_errors = compatibility_errors
        self.compatibility_level = compatibility_level


# Utility functions for error handling

def handle_compilation_error(
    error: Exception,
    schema_path: str,
    compiler_output: Optional[str] = None,
    exit_code: Optional[int] = None
) -> CompilationError:
    """Convert a generic error into a CompilationError.
    
    Args:
        error: Original exception
        schema_path: Path to schema that failed compilation
        compiler_output: Output from protoc compiler
        exit_code: Exit code from protoc
        
    Returns:
        CompilationError with enhanced information
    """
    if isinstance(error, CompilationError):
        return error
    
    message = f"Failed to compile protobuf schema: {Path(schema_path).name}"
    
    return CompilationError(
        message=message,
        schema_path=schema_path,
        compiler_output=compiler_output,
        exit_code=exit_code,
        original_error=error
    )


def handle_validation_error(
    error: Exception,
    schema_path: str,
    validation_context: Optional[str] = None
) -> ValidationError:
    """Convert a generic error into a ValidationError.
    
    Args:
        error: Original exception
        schema_path: Path to schema that failed validation
        validation_context: Context about what was being validated
        
    Returns:
        ValidationError with enhanced information
    """
    if isinstance(error, ValidationError):
        return error
    
    message = f"Schema validation failed: {Path(schema_path).name}"
    if validation_context:
        message += f" ({validation_context})"
    
    return ValidationError(
        message=message,
        schema_path=schema_path,
        original_error=error
    )


def handle_schema_not_found(
    schema_path: str,
    search_paths: Optional[List[str]] = None,
    schema_type: str = "protobuf schema"
) -> SchemaNotFoundError:
    """Create a SchemaNotFoundError with comprehensive information.
    
    Args:
        schema_path: Path to schema that was not found
        search_paths: Paths that were searched
        schema_type: Type of schema
        
    Returns:
        SchemaNotFoundError with enhanced information
    """
    return SchemaNotFoundError(
        schema_path=schema_path,
        search_paths=search_paths,
        schema_type=schema_type
    )