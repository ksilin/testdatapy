"""Unit tests for schema-specific exception classes."""

import pytest
import logging
from unittest.mock import patch

from src.testdatapy.schema.exceptions import (
    SchemaError,
    CompilationError,
    ValidationError,
    SchemaNotFoundError,
    SchemaDependencyError,
    SchemaRegistrationError,
    SchemaCompatibilityError,
    handle_compilation_error,
    handle_validation_error,
    handle_schema_not_found
)


class TestSchemaError:
    """Test SchemaError base class."""
    
    def test_basic_schema_error(self):
        """Test basic schema error creation."""
        error = SchemaError("Test error message")
        
        assert str(error) == "Test error message"
        assert error.message == "Test error message"
        assert error.details is None
        assert error.suggestions == []
        assert error.original_error is None
        assert error.schema_path is None
    
    def test_schema_error_with_all_fields(self):
        """Test schema error with all fields."""
        original = ValueError("Original error")
        error = SchemaError(
            message="Main error",
            details="Detailed information",
            suggestions=["Fix this", "Try that"],
            original_error=original,
            schema_path="/path/to/schema.proto"
        )
        
        assert error.message == "Main error"
        assert error.details == "Detailed information"
        assert error.suggestions == ["Fix this", "Try that"]
        assert error.original_error is original
        assert error.schema_path == "/path/to/schema.proto"
    
    def test_get_user_message(self):
        """Test user-friendly message generation."""
        error = SchemaError(
            message="Something went wrong",
            schema_path="/path/to/schema.proto",
            suggestions=["Check syntax", "Verify imports"]
        )
        
        user_msg = error.get_user_message()
        
        assert "Something went wrong" in user_msg
        assert "/path/to/schema.proto" in user_msg
        assert "Suggestions:" in user_msg
        assert "1. Check syntax" in user_msg
        assert "2. Verify imports" in user_msg
    
    @patch('logging.getLogger')
    def test_error_logging(self, mock_logger):
        """Test that errors are logged properly."""
        mock_log = mock_logger.return_value
        
        SchemaError(
            message="Test error",
            schema_path="/test/path.proto",
            details="Test details"
        )
        
        mock_logger.assert_called_once()
        mock_log.error.assert_called_once()
        mock_log.debug.assert_called()


class TestCompilationError:
    """Test CompilationError class."""
    
    def test_basic_compilation_error(self):
        """Test basic compilation error."""
        error = CompilationError("Compilation failed")
        
        assert "Compilation failed" in str(error)
        assert error.compiler_output is None
        assert error.exit_code is None
        assert len(error.suggestions) > 0
        assert "Check protobuf syntax" in error.suggestions[0]
    
    def test_compilation_error_with_output(self):
        """Test compilation error with compiler output."""
        error = CompilationError(
            message="Compilation failed",
            schema_path="/test/schema.proto",
            compiler_output="syntax error on line 5",
            exit_code=1
        )
        
        assert error.schema_path == "/test/schema.proto"
        assert error.compiler_output == "syntax error on line 5"
        assert error.exit_code == 1
        assert "Compiler output: syntax error on line 5" in error.details
        assert "Exit code: 1" in error.details
        assert "Review schema file: /test/schema.proto" in error.suggestions


class TestValidationError:
    """Test ValidationError class."""
    
    def test_basic_validation_error(self):
        """Test basic validation error."""
        error = ValidationError("Validation failed")
        
        assert "Validation failed" in str(error)
        assert error.validation_errors == []
        assert error.field_errors == {}
        assert "Review protobuf schema syntax" in error.suggestions
    
    def test_validation_error_with_details(self):
        """Test validation error with validation details."""
        validation_errors = ["Missing required field", "Invalid type"]
        field_errors = {"name": "Required field missing", "age": "Invalid type"}
        
        error = ValidationError(
            message="Schema validation failed",
            schema_path="/test/schema.proto",
            validation_errors=validation_errors,
            field_errors=field_errors
        )
        
        assert error.validation_errors == validation_errors
        assert error.field_errors == field_errors
        assert "Validation errors:" in error.details
        assert "Field errors:" in error.details
        assert "name: Required field missing" in error.details


class TestSchemaNotFoundError:
    """Test SchemaNotFoundError class."""
    
    def test_basic_schema_not_found(self):
        """Test basic schema not found error."""
        error = SchemaNotFoundError("/missing/schema.proto")
        
        assert "/missing/schema.proto" in str(error)
        assert error.schema_path == "/missing/schema.proto"
        assert error.search_paths == []
        assert error.schema_type == "protobuf schema"
        assert "Verify the protobuf schema file exists" in error.suggestions[0]
    
    def test_schema_not_found_with_search_paths(self):
        """Test schema not found with search paths."""
        search_paths = ["/path1", "/path2", "/path3"]
        
        error = SchemaNotFoundError(
            schema_path="missing.proto",
            search_paths=search_paths,
            schema_type="custom schema"
        )
        
        assert error.search_paths == search_paths
        assert error.schema_type == "custom schema"
        assert "Custom schema file not found" in str(error)
        assert "Searched in: /path1, /path2, /path3" in error.suggestions
        assert "Add the correct path to schema search paths" in error.suggestions


class TestSchemaDependencyError:
    """Test SchemaDependencyError class."""
    
    def test_basic_dependency_error(self):
        """Test basic dependency error."""
        error = SchemaDependencyError(
            message="Dependencies not found",
            schema_path="/test/schema.proto"
        )
        
        assert error.schema_path == "/test/schema.proto"
        assert error.missing_dependencies == []
        assert error.dependency_chain == []
        assert "Check that all imported proto files exist" in error.suggestions
    
    def test_dependency_error_with_details(self):
        """Test dependency error with missing dependencies."""
        missing_deps = ["common.proto", "types.proto"]
        dep_chain = ["schema.proto", "common.proto", "missing.proto"]
        
        error = SchemaDependencyError(
            message="Missing dependencies",
            schema_path="/test/schema.proto",
            missing_dependencies=missing_deps,
            dependency_chain=dep_chain
        )
        
        assert error.missing_dependencies == missing_deps
        assert error.dependency_chain == dep_chain
        assert "Missing dependencies: common.proto, types.proto" in error.details
        assert "Dependency chain: schema.proto -> common.proto -> missing.proto" in error.details
        assert "Missing files: common.proto, types.proto" in error.suggestions


class TestSchemaRegistrationError:
    """Test SchemaRegistrationError class."""
    
    def test_basic_registration_error(self):
        """Test basic registration error."""
        error = SchemaRegistrationError(
            message="Registration failed",
            subject="test-subject"
        )
        
        assert error.subject == "test-subject"
        assert error.schema_registry_url is None
        assert error.schema_content is None
        assert "Subject: test-subject" in error.details
        assert "Verify Schema Registry is accessible" in error.suggestions
    
    def test_registration_error_with_details(self):
        """Test registration error with full details."""
        error = SchemaRegistrationError(
            message="Schema registration failed",
            subject="test-value",
            schema_registry_url="http://localhost:8081",
            schema_content='syntax = "proto3";'
        )
        
        assert error.schema_registry_url == "http://localhost:8081"
        assert error.schema_content == 'syntax = "proto3";'
        assert "Registry URL: http://localhost:8081" in error.details
        assert "Check Schema Registry at: http://localhost:8081" in error.suggestions[0]


class TestSchemaCompatibilityError:
    """Test SchemaCompatibilityError class."""
    
    def test_basic_compatibility_error(self):
        """Test basic compatibility error."""
        compatibility_errors = ["Field removed", "Type changed"]
        
        error = SchemaCompatibilityError(
            message="Compatibility check failed",
            subject="test-subject",
            new_schema='syntax = "proto3";',
            compatibility_errors=compatibility_errors
        )
        
        assert error.subject == "test-subject"
        assert error.new_schema == 'syntax = "proto3";'
        assert error.compatibility_errors == compatibility_errors
        assert error.compatibility_level is None
        assert "Field removed" in error.details
        assert "Type changed" in error.details
        assert "Review schema evolution guidelines" in error.suggestions
    
    def test_compatibility_error_with_level(self):
        """Test compatibility error with compatibility level."""
        error = SchemaCompatibilityError(
            message="Incompatible schema",
            subject="test-value",
            new_schema="schema content",
            compatibility_errors=["Breaking change"],
            compatibility_level="BACKWARD"
        )
        
        assert error.compatibility_level == "BACKWARD"
        assert "Compatibility level: BACKWARD" in error.details
        assert "Current compatibility level: BACKWARD" in error.suggestions[0]


class TestUtilityFunctions:
    """Test utility functions for error handling."""
    
    def test_handle_compilation_error_with_compilation_error(self):
        """Test handling when error is already CompilationError."""
        original_error = CompilationError("Original error")
        result = handle_compilation_error(original_error, "/test/schema.proto")
        
        assert result is original_error
    
    def test_handle_compilation_error_with_generic_error(self):
        """Test converting generic error to CompilationError."""
        original_error = ValueError("Generic error")
        result = handle_compilation_error(
            error=original_error,
            schema_path="/test/schema.proto",
            compiler_output="Error output",
            exit_code=2
        )
        
        assert isinstance(result, CompilationError)
        assert result.original_error is original_error
        assert result.schema_path == "/test/schema.proto"
        assert result.compiler_output == "Error output"
        assert result.exit_code == 2
        assert "Failed to compile protobuf schema: schema.proto" in result.message
    
    def test_handle_validation_error_with_validation_error(self):
        """Test handling when error is already ValidationError."""
        original_error = ValidationError("Original validation error")
        result = handle_validation_error(original_error, "/test/schema.proto")
        
        assert result is original_error
    
    def test_handle_validation_error_with_generic_error(self):
        """Test converting generic error to ValidationError."""
        original_error = TypeError("Type error")
        result = handle_validation_error(
            error=original_error,
            schema_path="/test/schema.proto",
            validation_context="field validation"
        )
        
        assert isinstance(result, ValidationError)
        assert result.original_error is original_error
        assert result.schema_path == "/test/schema.proto"
        assert "Schema validation failed: schema.proto (field validation)" in result.message
    
    def test_handle_schema_not_found(self):
        """Test creating SchemaNotFoundError."""
        result = handle_schema_not_found(
            schema_path="/missing/schema.proto",
            search_paths=["/path1", "/path2"],
            schema_type="custom schema"
        )
        
        assert isinstance(result, SchemaNotFoundError)
        assert result.schema_path == "/missing/schema.proto"
        assert result.search_paths == ["/path1", "/path2"]
        assert result.schema_type == "custom schema"


class TestErrorIntegration:
    """Test integration of errors with existing system."""
    
    def test_error_inheritance(self):
        """Test that all errors inherit from SchemaError."""
        errors = [
            CompilationError("test"),
            ValidationError("test"),
            SchemaNotFoundError("test"),
            SchemaDependencyError("test", "path"),
            SchemaRegistrationError("test", "subject"),
            SchemaCompatibilityError("test", "subject", "schema", [])
        ]
        
        for error in errors:
            assert isinstance(error, SchemaError)
            assert isinstance(error, Exception)
    
    def test_error_attributes(self):
        """Test that all errors have required attributes."""
        error = CompilationError("test message")
        
        assert hasattr(error, 'message')
        assert hasattr(error, 'details')
        assert hasattr(error, 'suggestions')
        assert hasattr(error, 'original_error')
        assert hasattr(error, 'schema_path')
        assert hasattr(error, 'get_user_message')
    
    def test_suggestions_are_helpful(self):
        """Test that error suggestions are helpful and actionable."""
        errors = [
            CompilationError("test"),
            ValidationError("test"),
            SchemaNotFoundError("test"),
            SchemaDependencyError("test", "path"),
            SchemaRegistrationError("test", "subject"),
            SchemaCompatibilityError("test", "subject", "schema", [])
        ]
        
        for error in errors:
            assert len(error.suggestions) > 0
            # Suggestions should be actionable (contain verbs like "check", "verify", etc.)
            suggestion_text = " ".join(error.suggestions).lower()
            action_words = ["check", "verify", "ensure", "review", "add", "use"]
            assert any(word in suggestion_text for word in action_words)