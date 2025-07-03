"""Utilities for displaying user-friendly error messages in CLI and applications."""

import sys
from typing import Optional, TextIO
from .exceptions import SchemaError


def display_error(
    error: Exception,
    file: Optional[TextIO] = None,
    include_installation_guide: bool = True,
    include_debug_info: bool = False,
    use_colors: bool = True
) -> None:
    """Display a user-friendly error message to the user.
    
    Args:
        error: The exception to display
        file: Output file (defaults to stderr)
        include_installation_guide: Whether to include installation instructions
        include_debug_info: Whether to include debug information
        use_colors: Whether to use colored output (if supported)
    """
    if file is None:
        file = sys.stderr
    
    # Check if we can use colors
    if use_colors and hasattr(file, 'isatty') and file.isatty():
        # ANSI color codes
        RED = '\033[91m'
        YELLOW = '\033[93m'
        BLUE = '\033[94m'
        GREEN = '\033[92m'
        BOLD = '\033[1m'
        RESET = '\033[0m'
    else:
        RED = YELLOW = BLUE = GREEN = BOLD = RESET = ''
    
    # Use comprehensive message if it's a SchemaError
    if isinstance(error, SchemaError):
        try:
            message = error.get_comprehensive_message(
                include_installation_guide=include_installation_guide,
                include_debug_info=include_debug_info
            )
        except Exception:
            # Fallback to basic message if comprehensive fails
            message = error.get_user_message(include_debug_info)
    else:
        # For non-schema errors, provide basic information
        message = f"{RED}❌ Error: {str(error)}{RESET}"
    
    # Print the message
    print(message, file=file)
    
    # Add a separator for clarity
    print(f"\n{BLUE}{'=' * 50}{RESET}", file=file)


def format_error_for_logging(error: Exception) -> str:
    """Format an error for structured logging.
    
    Args:
        error: The exception to format
        
    Returns:
        Formatted error message suitable for logging
    """
    if isinstance(error, SchemaError):
        context = {
            "error_type": error.__class__.__name__,
            "message": error.message,
            "schema_path": error.schema_path,
            "suggestions_count": len(error.suggestions),
            "has_details": bool(error.details),
            "has_original_error": bool(error.original_error)
        }
        
        # Add specific error attributes
        if hasattr(error, 'compiler_output') and error.compiler_output:
            context["has_compiler_output"] = True
        if hasattr(error, 'missing_dependencies') and error.missing_dependencies:
            context["missing_dependencies_count"] = len(error.missing_dependencies)
        
        return f"SchemaError: {error.message} | Context: {context}"
    else:
        return f"Error: {type(error).__name__}: {str(error)}"


def suggest_help_command(error: Exception) -> Optional[str]:
    """Suggest a help command based on the error type.
    
    Args:
        error: The exception to analyze
        
    Returns:
        Suggested help command or None
    """
    if not isinstance(error, SchemaError):
        return None
    
    error_type = error.__class__.__name__
    
    suggestions = {
        "CompilationError": "testdatapy schema validate --help",
        "SchemaNotFoundError": "testdatapy schema list --help", 
        "SchemaDependencyError": "testdatapy schema dependencies --help",
        "ValidationError": "testdatapy schema validate --help",
        "SchemaRegistrationError": "testdatapy schema register --help"
    }
    
    return suggestions.get(error_type)


class ErrorFormatter:
    """Formatter for different error display contexts."""
    
    @staticmethod
    def for_cli(error: Exception, verbose: bool = False) -> str:
        """Format error for CLI display.
        
        Args:
            error: Exception to format
            verbose: Whether to include verbose information
            
        Returns:
            CLI-formatted error message
        """
        if isinstance(error, SchemaError):
            message = error.get_comprehensive_message(
                include_installation_guide=verbose,
                include_debug_info=verbose
            )
            
            # Add help suggestion
            help_cmd = suggest_help_command(error)
            if help_cmd:
                message += f"\n💭 For more help: {help_cmd}"
            
            return message
        else:
            return f"Error: {str(error)}"
    
    @staticmethod
    def for_api(error: Exception) -> dict:
        """Format error for API responses.
        
        Args:
            error: Exception to format
            
        Returns:
            Dictionary suitable for JSON API responses
        """
        if isinstance(error, SchemaError):
            return {
                "error": {
                    "type": error.__class__.__name__,
                    "message": error.message,
                    "details": error.details,
                    "suggestions": error.suggestions,
                    "schema_path": error.schema_path,
                    "user_message": error.get_user_message()
                }
            }
        else:
            return {
                "error": {
                    "type": type(error).__name__,
                    "message": str(error)
                }
            }
    
    @staticmethod
    def for_web(error: Exception) -> dict:
        """Format error for web interfaces.
        
        Args:
            error: Exception to format
            
        Returns:
            Dictionary suitable for web display
        """
        if isinstance(error, SchemaError):
            return {
                "title": error.__class__.__name__.replace("Error", " Error"),
                "message": error.message,
                "details": error.details,
                "suggestions": error.suggestions,
                "schema_path": error.schema_path,
                "severity": "error",
                "actionable": len(error.suggestions) > 0
            }
        else:
            return {
                "title": "Unexpected Error",
                "message": str(error),
                "severity": "error",
                "actionable": False
            }


def create_error_report(
    error: Exception,
    context: Optional[dict] = None,
    include_system_info: bool = True
) -> str:
    """Create a comprehensive error report for debugging.
    
    Args:
        error: Exception to report
        context: Additional context information
        include_system_info: Whether to include system information
        
    Returns:
        Comprehensive error report
    """
    import platform
    import sys
    from datetime import datetime
    
    lines = [
        "=" * 60,
        "TestDataPy Error Report",
        "=" * 60,
        f"Generated: {datetime.now().isoformat()}",
        ""
    ]
    
    # Error information
    lines.extend([
        "ERROR INFORMATION:",
        f"Type: {type(error).__name__}",
        f"Message: {str(error)}",
        ""
    ])
    
    # Schema-specific information
    if isinstance(error, SchemaError):
        lines.extend([
            "SCHEMA ERROR DETAILS:",
            f"Schema Path: {error.schema_path or 'N/A'}",
            f"Has Details: {bool(error.details)}",
            f"Suggestions Count: {len(error.suggestions)}",
            f"Original Error: {type(error.original_error).__name__ if error.original_error else 'N/A'}",
            ""
        ])
        
        if error.details:
            lines.extend([
                "Details:",
                error.details,
                ""
            ])
        
        if error.suggestions:
            lines.extend([
                "Suggestions:",
                *[f"  {i}. {suggestion}" for i, suggestion in enumerate(error.suggestions, 1)],
                ""
            ])
    
    # Context information
    if context:
        lines.extend([
            "CONTEXT INFORMATION:",
            *[f"{key}: {value}" for key, value in context.items()],
            ""
        ])
    
    # System information
    if include_system_info:
        lines.extend([
            "SYSTEM INFORMATION:",
            f"Python Version: {sys.version}",
            f"Platform: {platform.platform()}",
            f"Architecture: {platform.architecture()[0]}",
            f"Machine: {platform.machine()}",
            f"Processor: {platform.processor()}",
            ""
        ])
        
        # Check for protoc
        import shutil
        protoc_path = shutil.which("protoc")
        lines.extend([
            "PROTOBUF INFORMATION:",
            f"protoc found: {'Yes' if protoc_path else 'No'}",
            f"protoc path: {protoc_path or 'Not found'}",
            ""
        ])
        
        # Check Python protobuf library
        try:
            import google.protobuf
            lines.append(f"protobuf library: {google.protobuf.__version__}")
        except ImportError:
            lines.append("protobuf library: Not installed")
        
        lines.append("")
    
    lines.append("=" * 60)
    return "\n".join(lines)