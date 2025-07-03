"""Unit tests for user-friendly error messaging system."""

import pytest
import tempfile
import json
from pathlib import Path
from unittest.mock import Mock, patch
from io import StringIO

from src.testdatapy.schema.exceptions import (
    SchemaError,
    CompilationError,
    ValidationError,
    SchemaNotFoundError,
    SchemaDependencyError
)
from src.testdatapy.schema.error_messages import (
    ErrorMessageTemplate,
    InstallationGuide,
    TroubleshootingGuide,
    get_user_friendly_message,
    format_validation_errors,
    suggest_next_steps,
    ERROR_TEMPLATES
)
from src.testdatapy.schema.error_display import (
    display_error,
    format_error_for_logging,
    suggest_help_command,
    ErrorFormatter,
    create_error_report
)


class TestErrorMessageTemplate:
    """Test ErrorMessageTemplate functionality."""
    
    def test_template_creation(self):
        """Test creating an error message template."""
        template = ErrorMessageTemplate(
            title="Test Error",
            description="This is a test error",
            common_causes=["Cause 1", "Cause 2"],
            solutions=["Solution 1", "Solution 2"],
            examples=["Example 1"],
            related_docs=["Doc 1"]
        )
        
        assert template.title == "Test Error"
        assert template.description == "This is a test error"
        assert len(template.common_causes) == 2
        assert len(template.solutions) == 2
        assert len(template.examples) == 1
        assert len(template.related_docs) == 1
    
    def test_message_formatting(self):
        """Test message formatting."""
        template = ErrorMessageTemplate(
            title="Test Error",
            description="Test description",
            common_causes=["Test cause"],
            solutions=["Test solution"]
        )
        
        message = template.format_message()
        
        assert "❌ Test Error" in message
        assert "📋 Description:" in message
        assert "Test description" in message
        assert "🤔 Common causes:" in message
        assert "Test cause" in message
        assert "💡 Solutions to try:" in message
        assert "Test solution" in message
    
    def test_message_formatting_with_context(self):
        """Test message formatting with context."""
        template = ErrorMessageTemplate(
            title="Test Error",
            description="Test description",
            common_causes=[],
            solutions=["Test solution"]
        )
        
        context = {"file": "test.proto", "line": "10"}
        message = template.format_message(context=context)
        
        assert "🔍 Context:" in message
        assert "file: test.proto" in message
        assert "line: 10" in message
    
    def test_message_formatting_with_debug_info(self):
        """Test message formatting with debug information."""
        template = ErrorMessageTemplate(
            title="Test Error",
            description="Test description",
            common_causes=[],
            solutions=["Test solution"]
        )
        
        message = template.format_message(include_debug_info=True)
        
        assert "🔧 Debug information:" in message
        assert "Python version:" in message
        assert "Platform:" in message
        assert "Working directory:" in message


class TestInstallationGuide:
    """Test InstallationGuide functionality."""
    
    @patch('platform.system')
    def test_protoc_installation_guide_macos(self, mock_system):
        """Test protoc installation guide for macOS."""
        mock_system.return_value = "Darwin"
        
        guide = InstallationGuide.get_protoc_installation_guide()
        
        assert "🍎 macOS Installation:" in guide
        assert "brew install protobuf" in guide
        assert "protoc --version" in guide
    
    @patch('platform.system')
    def test_protoc_installation_guide_linux(self, mock_system):
        """Test protoc installation guide for Linux."""
        mock_system.return_value = "Linux"
        
        guide = InstallationGuide.get_protoc_installation_guide()
        
        assert "🐧 Linux Installation:" in guide
        assert "apt-get install protobuf-compiler" in guide
        assert "yum install protobuf-compiler" in guide
    
    @patch('platform.system')
    def test_protoc_installation_guide_windows(self, mock_system):
        """Test protoc installation guide for Windows."""
        mock_system.return_value = "Windows"
        
        guide = InstallationGuide.get_protoc_installation_guide()
        
        assert "🪟 Windows Installation:" in guide
        assert "choco install protoc" in guide
        assert "protoc --version" in guide
    
    def test_python_protobuf_installation_guide(self):
        """Test Python protobuf installation guide."""
        guide = InstallationGuide.get_python_protobuf_installation_guide()
        
        assert "🐍 Python protobuf library installation:" in guide
        assert "pip install protobuf" in guide
        assert "conda install protobuf" in guide


class TestTroubleshootingGuide:
    """Test TroubleshootingGuide functionality."""
    
    @patch('shutil.which')
    def test_diagnose_protoc_issues_protoc_not_found(self, mock_which):
        """Test diagnosing protoc issues when protoc is not found."""
        mock_which.return_value = None
        
        suggestions = TroubleshootingGuide.diagnose_protoc_issues()
        
        assert any("protoc compiler not found" in suggestion for suggestion in suggestions)
    
    @patch('shutil.which')
    @patch('google.protobuf.__version__', '3.21.12', create=True)
    def test_diagnose_protoc_issues_protoc_found(self, mock_which):
        """Test diagnosing protoc issues when protoc is found."""
        mock_which.return_value = "/usr/bin/protoc"
        
        suggestions = TroubleshootingGuide.diagnose_protoc_issues()
        
        assert any("✅ Python protobuf library found" in suggestion for suggestion in suggestions)
    
    def test_proto_syntax_help(self):
        """Test proto syntax help."""
        help_text = TroubleshootingGuide.get_proto_syntax_help()
        
        assert "📝 Common protobuf syntax issues:" in help_text
        assert 'syntax = "proto3";' in help_text
        assert "Field numbers start at 1" in help_text


class TestUserFriendlyMessages:
    """Test user-friendly message generation."""
    
    def test_get_user_friendly_message_protoc_not_found(self):
        """Test user-friendly message for protoc not found."""
        message = get_user_friendly_message("protoc_not_found")
        
        assert "❌ Protocol Buffers compiler (protoc) not found" in message
        assert "💡 Solutions to try:" in message
        assert "Install protoc using your system package manager" in message
    
    def test_get_user_friendly_message_with_context(self):
        """Test user-friendly message with context."""
        context = {"schema_path": "/test/schema.proto"}
        message = get_user_friendly_message("proto_file_not_found", context=context)
        
        assert "❌ Protobuf schema file not found" in message
        assert "🔍 Context:" in message
        assert "/test/schema.proto" in message
    
    def test_get_user_friendly_message_unknown_error(self):
        """Test user-friendly message for unknown error type."""
        message = get_user_friendly_message("unknown_error_type")
        
        assert "Unknown error type: unknown_error_type" in message
    
    def test_format_validation_errors(self):
        """Test formatting validation errors."""
        errors = ["Missing field number", "Invalid syntax"]
        schema_path = "/test/schema.proto"
        
        formatted = format_validation_errors(errors, schema_path)
        
        assert "❌ Schema validation failed" in formatted
        assert "📄 Schema: /test/schema.proto" in formatted
        assert "🔍 Found 2 validation error(s):" in formatted
        assert "1. Missing field number" in formatted
        assert "2. Invalid syntax" in formatted
    
    def test_format_validation_errors_empty(self):
        """Test formatting empty validation errors."""
        formatted = format_validation_errors([])
        
        assert "No validation errors found." in formatted
    
    def test_suggest_next_steps(self):
        """Test suggesting next steps."""
        context = {"schema_path": "/test/schema.proto"}
        steps = suggest_next_steps("protoc_not_found", context)
        
        assert len(steps) > 0
        assert any("Install Protocol Buffers compiler" in step for step in steps)
        assert any("/test/schema.proto" in step for step in steps)


class TestSchemaErrorEnhancements:
    """Test enhanced schema error functionality."""
    
    def test_compilation_error_comprehensive_message(self):
        """Test CompilationError comprehensive message."""
        error = CompilationError(
            message="protoc compiler not found",
            schema_path="/test/schema.proto",
            compiler_output="command not found: protoc",
            exit_code=127
        )
        
        message = error.get_comprehensive_message()
        
        assert "❌ Protocol Buffers compiler (protoc) not found" in message
        assert "🍎 macOS Installation:" in message or "🐧 Linux Installation:" in message or "🪟 Windows Installation:" in message
    
    def test_schema_not_found_error_comprehensive_message(self):
        """Test SchemaNotFoundError comprehensive message."""
        error = SchemaNotFoundError(
            schema_path="/missing/schema.proto",
            search_paths=["/path1", "/path2"]
        )
        
        message = error.get_comprehensive_message()
        
        assert "❌ Protobuf schema file not found" in message
        assert "/missing/schema.proto" in message
    
    def test_schema_dependency_error_comprehensive_message(self):
        """Test SchemaDependencyError comprehensive message."""
        error = SchemaDependencyError(
            message="Cannot resolve import",
            schema_path="/test/schema.proto",
            missing_dependencies=["common.proto"]
        )
        
        message = error.get_comprehensive_message()
        
        assert "❌ Cannot resolve proto import" in message
        assert "common.proto" in message


class TestErrorDisplay:
    """Test error display utilities."""
    
    def test_display_error_basic(self):
        """Test basic error display."""
        error = CompilationError("Test compilation error")
        output = StringIO()
        
        display_error(error, file=output, use_colors=False)
        
        result = output.getvalue()
        assert "❌ Protobuf compilation failed" in result
        assert "=" * 50 in result
    
    def test_display_error_non_schema_error(self):
        """Test displaying non-schema error."""
        error = ValueError("Test value error")
        output = StringIO()
        
        display_error(error, file=output, use_colors=False)
        
        result = output.getvalue()
        assert "Error: Test value error" in result
    
    def test_format_error_for_logging(self):
        """Test formatting error for logging."""
        error = CompilationError(
            message="Test error",
            schema_path="/test/schema.proto"
        )
        
        formatted = format_error_for_logging(error)
        
        assert "SchemaError: Test error" in formatted
        assert "CompilationError" in formatted
        assert "/test/schema.proto" in formatted
    
    def test_suggest_help_command(self):
        """Test suggesting help commands."""
        error = CompilationError("Test error")
        
        help_cmd = suggest_help_command(error)
        
        assert help_cmd is not None
        assert "validate" in help_cmd
        assert "--help" in help_cmd
    
    def test_suggest_help_command_non_schema_error(self):
        """Test suggesting help command for non-schema error."""
        error = ValueError("Test error")
        
        help_cmd = suggest_help_command(error)
        
        assert help_cmd is None


class TestErrorFormatter:
    """Test ErrorFormatter functionality."""
    
    def test_formatter_for_cli(self):
        """Test CLI error formatting."""
        error = CompilationError("Test error")
        
        formatted = ErrorFormatter.for_cli(error)
        
        assert "❌ Protobuf compilation failed" in formatted
        assert "💭 For more help:" in formatted
    
    def test_formatter_for_cli_verbose(self):
        """Test verbose CLI error formatting."""
        error = CompilationError("Test error")
        
        formatted = ErrorFormatter.for_cli(error, verbose=True)
        
        assert "❌ Protobuf compilation failed" in formatted
        assert "🔧 Debug information:" in formatted or "Installation:" in formatted
    
    def test_formatter_for_api(self):
        """Test API error formatting."""
        error = CompilationError("Test error", schema_path="/test/schema.proto")
        
        formatted = ErrorFormatter.for_api(error)
        
        assert "error" in formatted
        assert formatted["error"]["type"] == "CompilationError"
        assert formatted["error"]["message"] == "Test error"
        assert formatted["error"]["schema_path"] == "/test/schema.proto"
    
    def test_formatter_for_web(self):
        """Test web error formatting."""
        error = CompilationError("Test error")
        
        formatted = ErrorFormatter.for_web(error)
        
        assert formatted["title"] == "Compilation Error"
        assert formatted["message"] == "Test error"
        assert formatted["severity"] == "error"
        assert formatted["actionable"] is True


class TestErrorReport:
    """Test error report generation."""
    
    def test_create_error_report(self):
        """Test creating error report."""
        error = CompilationError(
            message="Test error",
            schema_path="/test/schema.proto"
        )
        context = {"operation": "schema_compilation"}
        
        report = create_error_report(error, context)
        
        assert "TestDataPy Error Report" in report
        assert "ERROR INFORMATION:" in report
        assert "CompilationError" in report
        assert "SCHEMA ERROR DETAILS:" in report
        assert "CONTEXT INFORMATION:" in report
        assert "operation: schema_compilation" in report
        assert "SYSTEM INFORMATION:" in report
    
    def test_create_error_report_without_system_info(self):
        """Test creating error report without system info."""
        error = ValueError("Test error")
        
        report = create_error_report(error, include_system_info=False)
        
        assert "TestDataPy Error Report" in report
        assert "ERROR INFORMATION:" in report
        assert "SYSTEM INFORMATION:" not in report


class TestErrorTemplates:
    """Test predefined error templates."""
    
    def test_all_templates_exist(self):
        """Test that all expected error templates exist."""
        expected_templates = [
            "protoc_not_found",
            "proto_file_not_found", 
            "proto_compilation_failed",
            "import_resolution_failed",
            "permission_denied",
            "circular_dependency",
            "version_mismatch"
        ]
        
        for template_name in expected_templates:
            assert template_name in ERROR_TEMPLATES
            template = ERROR_TEMPLATES[template_name]
            assert isinstance(template, ErrorMessageTemplate)
            assert template.title
            assert template.description
            assert template.solutions
    
    def test_template_completeness(self):
        """Test that templates have complete information."""
        for template_name, template in ERROR_TEMPLATES.items():
            assert template.title, f"Template {template_name} missing title"
            assert template.description, f"Template {template_name} missing description"
            assert template.solutions, f"Template {template_name} missing solutions"
            assert len(template.solutions) > 0, f"Template {template_name} has no solutions"
            
            # Solutions should be actionable (contain action words)
            solution_text = " ".join(template.solutions).lower()
            action_words = ["install", "check", "verify", "add", "use", "update", "review", "ensure"]
            assert any(word in solution_text for word in action_words), \
                f"Template {template_name} solutions don't seem actionable"