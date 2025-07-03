"""User-friendly error messages and troubleshooting guidance for schema operations.

This module provides comprehensive error message templates, troubleshooting hints,
and installation guidance to help users resolve common schema-related issues.
"""

import platform
import sys
from typing import Dict, List, Optional, Tuple
from pathlib import Path


class ErrorMessageTemplate:
    """Template for generating user-friendly error messages."""
    
    def __init__(
        self,
        title: str,
        description: str,
        common_causes: List[str],
        solutions: List[str],
        examples: Optional[List[str]] = None,
        related_docs: Optional[List[str]] = None
    ):
        """Initialize error message template.
        
        Args:
            title: Brief error title
            description: Detailed error description
            common_causes: List of common causes for this error
            solutions: List of actionable solutions
            examples: Optional examples of correct usage
            related_docs: Optional links to related documentation
        """
        self.title = title
        self.description = description
        self.common_causes = common_causes
        self.solutions = solutions
        self.examples = examples or []
        self.related_docs = related_docs or []
    
    def format_message(
        self,
        context: Optional[Dict[str, str]] = None,
        include_debug_info: bool = False
    ) -> str:
        """Format the complete error message.
        
        Args:
            context: Context-specific information to include
            include_debug_info: Whether to include debug information
            
        Returns:
            Formatted user-friendly error message
        """
        lines = [
            f"❌ {self.title}",
            "",
            f"📋 Description:",
            f"   {self.description}",
            ""
        ]
        
        # Add context-specific information
        if context:
            lines.extend([
                "🔍 Context:",
                *[f"   • {key}: {value}" for key, value in context.items()],
                ""
            ])
        
        # Add common causes
        if self.common_causes:
            lines.extend([
                "🤔 Common causes:",
                *[f"   • {cause}" for cause in self.common_causes],
                ""
            ])
        
        # Add solutions
        lines.extend([
            "💡 Solutions to try:",
            *[f"   {i}. {solution}" for i, solution in enumerate(self.solutions, 1)],
            ""
        ])
        
        # Add examples
        if self.examples:
            lines.extend([
                "📝 Examples:",
                *[f"   {example}" for example in self.examples],
                ""
            ])
        
        # Add related documentation
        if self.related_docs:
            lines.extend([
                "📚 Related documentation:",
                *[f"   • {doc}" for doc in self.related_docs],
                ""
            ])
        
        # Add debug information
        if include_debug_info:
            lines.extend([
                "🔧 Debug information:",
                f"   • Python version: {sys.version}",
                f"   • Platform: {platform.platform()}",
                f"   • Working directory: {Path.cwd()}",
                ""
            ])
        
        return "\n".join(lines)


class InstallationGuide:
    """Provides installation guidance for missing dependencies."""
    
    @staticmethod
    def get_protoc_installation_guide() -> str:
        """Get platform-specific protoc installation instructions."""
        system = platform.system().lower()
        
        if system == "darwin":  # macOS
            return """
🍎 macOS Installation:

1. Using Homebrew (recommended):
   brew install protobuf

2. Using MacPorts:
   sudo port install protobuf3-cpp

3. Manual installation:
   • Download from: https://github.com/protocolbuffers/protobuf/releases
   • Extract and add to PATH

4. Verify installation:
   protoc --version
"""
        elif system == "linux":
            return """
🐧 Linux Installation:

1. Ubuntu/Debian:
   sudo apt-get update
   sudo apt-get install protobuf-compiler

2. CentOS/RHEL/Fedora:
   sudo yum install protobuf-compiler
   # or for newer versions:
   sudo dnf install protobuf-compiler

3. Arch Linux:
   sudo pacman -S protobuf

4. Manual installation:
   • Download from: https://github.com/protocolbuffers/protobuf/releases
   • Extract and add to PATH

5. Verify installation:
   protoc --version
"""
        elif system == "windows":
            return """
🪟 Windows Installation:

1. Using Chocolatey:
   choco install protoc

2. Using Scoop:
   scoop install protobuf

3. Manual installation:
   • Download protoc-XX.X-win64.zip from:
     https://github.com/protocolbuffers/protobuf/releases
   • Extract to a folder (e.g., C:\\protoc)
   • Add C:\\protoc\\bin to your PATH environment variable

4. Using vcpkg:
   vcpkg install protobuf

5. Verify installation:
   protoc --version
"""
        else:
            return """
📦 General Installation:

1. Download the appropriate release for your platform from:
   https://github.com/protocolbuffers/protobuf/releases

2. Extract the archive and add the 'bin' directory to your PATH

3. Verify installation:
   protoc --version
"""
    
    @staticmethod
    def get_python_protobuf_installation_guide() -> str:
        """Get Python protobuf library installation instructions."""
        return """
🐍 Python protobuf library installation:

1. Using pip (recommended):
   pip install protobuf

2. Using conda:
   conda install protobuf

3. Using poetry:
   poetry add protobuf

4. Verify installation:
   python -c "import google.protobuf; print(google.protobuf.__version__)"

⚠️  Note: Make sure your protobuf library version is compatible with your protoc compiler version.
"""


class TroubleshootingGuide:
    """Provides troubleshooting guidance for common issues."""
    
    @staticmethod
    def diagnose_protoc_issues() -> List[str]:
        """Diagnose common protoc-related issues."""
        suggestions = []
        
        # Check if protoc is in PATH
        import shutil
        if not shutil.which("protoc"):
            suggestions.append("protoc compiler not found in PATH - see installation guide above")
        
        # Check Python protobuf library
        try:
            import google.protobuf
            protobuf_version = google.protobuf.__version__
            suggestions.append(f"✅ Python protobuf library found (version {protobuf_version})")
        except ImportError:
            suggestions.append("❌ Python protobuf library not installed - see installation guide above")
        
        # Check PATH environment
        path_dirs = []
        import os
        for path_dir in os.environ.get("PATH", "").split(os.pathsep):
            if Path(path_dir).exists():
                path_dirs.append(path_dir)
        
        if len(path_dirs) < 5:  # Suspiciously few PATH directories
            suggestions.append("⚠️  PATH environment variable seems incomplete")
        
        return suggestions
    
    @staticmethod
    def get_proto_syntax_help() -> str:
        """Get help for proto syntax issues."""
        return """
📝 Common protobuf syntax issues:

1. Missing syntax declaration:
   ✅ Correct:   syntax = "proto3";
   ❌ Incorrect: (missing syntax line)

2. Invalid field numbers:
   ✅ Correct:   string name = 1;
   ❌ Incorrect: string name = 0;  // Field numbers start at 1

3. Reserved keywords:
   ❌ Avoid using: message, service, enum, import, option, etc. as field names

4. Import issues:
   ✅ Correct:   import "path/to/file.proto";
   ❌ Incorrect: import path/to/file.proto;  // Missing quotes

5. Message naming:
   ✅ Correct:   message UserProfile { }
   ❌ Incorrect: message user_profile { }  // Use PascalCase

6. Field naming:
   ✅ Correct:   string user_name = 1;
   ❌ Incorrect: string userName = 1;  // Use snake_case
"""


# Pre-defined error message templates
ERROR_TEMPLATES = {
    "protoc_not_found": ErrorMessageTemplate(
        title="Protocol Buffers compiler (protoc) not found",
        description="The protoc compiler is required to compile .proto files but was not found in your system PATH.",
        common_causes=[
            "protoc is not installed on your system",
            "protoc is installed but not in your PATH environment variable",
            "protoc executable has incorrect permissions",
            "Using a virtual environment without protoc access"
        ],
        solutions=[
            "Install protoc using your system package manager (see installation guide below)",
            "Add protoc to your PATH environment variable",
            "Verify protoc executable permissions (chmod +x)",
            "Use absolute path to protoc in configuration"
        ],
        examples=[
            "# Test if protoc is accessible:",
            "protoc --version",
            "",
            "# Expected output:",
            "libprotoc 3.21.12"
        ],
        related_docs=[
            "Protocol Buffers installation: https://protobuf.dev/downloads/",
            "TestDataPy protobuf guide: https://docs.testdatapy.com/protobuf/"
        ]
    ),
    
    "proto_file_not_found": ErrorMessageTemplate(
        title="Protobuf schema file not found",
        description="The specified .proto file could not be found at the given path.",
        common_causes=[
            "Incorrect file path specified",
            "File was moved or deleted",
            "Working directory is not what you expected",
            "Case sensitivity issues (especially on Linux/macOS)"
        ],
        solutions=[
            "Verify the file path is correct and the file exists",
            "Use absolute paths instead of relative paths",
            "Check file permissions (file must be readable)",
            "Verify current working directory with pwd/cd commands"
        ],
        examples=[
            "# Use absolute path:",
            "/full/path/to/schema.proto",
            "",
            "# Or relative to current directory:",
            "./schemas/user.proto"
        ]
    ),
    
    "proto_compilation_failed": ErrorMessageTemplate(
        title="Protobuf compilation failed",
        description="The protoc compiler encountered errors while compiling your .proto file.",
        common_causes=[
            "Syntax errors in the .proto file",
            "Missing imported proto files",
            "Circular dependencies between proto files",
            "Incompatible protobuf version"
        ],
        solutions=[
            "Check the compiler output for specific syntax errors",
            "Verify all imported proto files exist and are accessible",
            "Review proto syntax (see syntax guide below)",
            "Update protoc to a compatible version"
        ],
        examples=[
            "# Basic proto file structure:",
            'syntax = "proto3";',
            "",
            "message User {",
            "  string name = 1;",
            "  int32 age = 2;",
            "}"
        ]
    ),
    
    "import_resolution_failed": ErrorMessageTemplate(
        title="Cannot resolve proto import",
        description="The protoc compiler cannot find one or more imported proto files.",
        common_causes=[
            "Imported proto file does not exist",
            "Import path is incorrect",
            "Missing proto path directories",
            "Case sensitivity issues in file names"
        ],
        solutions=[
            "Verify all imported proto files exist",
            "Check import paths are correct and use forward slashes",
            "Add necessary directories to proto search paths",
            "Use --proto_path flag to specify additional search directories"
        ],
        examples=[
            "# Correct import syntax:",
            'import "google/protobuf/timestamp.proto";',
            'import "common/user.proto";',
            "",
            "# Common search paths:",
            "--proto_path=/usr/local/include",
            "--proto_path=./protos"
        ]
    ),
    
    "permission_denied": ErrorMessageTemplate(
        title="Permission denied accessing schema files",
        description="The system denied permission to read schema files or write compilation output.",
        common_causes=[
            "Insufficient file permissions",
            "Output directory is read-only",
            "File is locked by another process",
            "SELinux or security policies blocking access"
        ],
        solutions=[
            "Check file and directory permissions",
            "Ensure output directory is writable",
            "Run with appropriate user permissions",
            "Close any applications that might have the files open"
        ],
        examples=[
            "# Check file permissions:",
            "ls -la schema.proto",
            "",
            "# Make file readable:",
            "chmod 644 schema.proto",
            "",
            "# Make directory writable:",
            "chmod 755 output_dir"
        ]
    ),
    
    "circular_dependency": ErrorMessageTemplate(
        title="Circular dependency detected in proto imports",
        description="A circular dependency was detected in your proto file import chain.",
        common_causes=[
            "Proto files importing each other directly or indirectly",
            "Common definitions placed in files that create cycles",
            "Incorrect import structure"
        ],
        solutions=[
            "Review your import structure and break the cycle",
            "Move common definitions to a separate base proto file",
            "Use forward declarations where possible",
            "Restructure your proto files to avoid circular dependencies"
        ],
        examples=[
            "# Avoid this pattern:",
            "# file1.proto imports file2.proto",
            "# file2.proto imports file1.proto",
            "",
            "# Better pattern:",
            "# common.proto contains shared definitions",
            "# file1.proto and file2.proto both import common.proto"
        ]
    ),
    
    "version_mismatch": ErrorMessageTemplate(
        title="Protocol Buffers version mismatch",
        description="There's a compatibility issue between your protoc compiler and Python protobuf library versions.",
        common_causes=[
            "protoc compiler version doesn't match protobuf library version",
            "Using old protoc with new protobuf library",
            "Mixed protobuf installations"
        ],
        solutions=[
            "Update both protoc and protobuf library to compatible versions",
            "Check version compatibility matrix",
            "Reinstall protobuf tools to ensure consistency",
            "Use virtual environment with specific protobuf versions"
        ],
        examples=[
            "# Check versions:",
            "protoc --version",
            "python -c \"import google.protobuf; print(google.protobuf.__version__)\"",
            "",
            "# Install compatible versions:",
            "pip install protobuf==4.21.12"
        ]
    )
}


def get_user_friendly_message(
    error_type: str,
    context: Optional[Dict[str, str]] = None,
    include_installation_guide: bool = True,
    include_debug_info: bool = False
) -> str:
    """Generate a comprehensive user-friendly error message.
    
    Args:
        error_type: Type of error (key from ERROR_TEMPLATES)
        context: Context-specific information
        include_installation_guide: Whether to include installation instructions
        include_debug_info: Whether to include debug information
        
    Returns:
        Comprehensive user-friendly error message
    """
    if error_type not in ERROR_TEMPLATES:
        return f"Unknown error type: {error_type}"
    
    template = ERROR_TEMPLATES[error_type]
    message = template.format_message(context, include_debug_info)
    
    # Add installation guide for relevant errors
    if include_installation_guide and error_type in ["protoc_not_found", "version_mismatch"]:
        message += "\n" + "=" * 50 + "\n"
        message += InstallationGuide.get_protoc_installation_guide()
        message += "\n" + InstallationGuide.get_python_protobuf_installation_guide()
    
    # Add troubleshooting for protoc issues
    if error_type in ["protoc_not_found", "proto_compilation_failed"]:
        message += "\n" + "=" * 50 + "\n"
        message += "🔍 System diagnosis:\n"
        for suggestion in TroubleshootingGuide.diagnose_protoc_issues():
            message += f"   • {suggestion}\n"
    
    # Add syntax help for compilation errors
    if error_type == "proto_compilation_failed":
        message += "\n" + TroubleshootingGuide.get_proto_syntax_help()
    
    return message


def format_validation_errors(errors: List[str], schema_path: Optional[str] = None) -> str:
    """Format multiple validation errors in a user-friendly way.
    
    Args:
        errors: List of validation error messages
        schema_path: Optional path to the schema file
        
    Returns:
        Formatted validation error message
    """
    if not errors:
        return "No validation errors found."
    
    lines = [
        "❌ Schema validation failed",
        ""
    ]
    
    if schema_path:
        lines.extend([
            f"📄 Schema: {schema_path}",
            ""
        ])
    
    lines.extend([
        f"🔍 Found {len(errors)} validation error(s):",
        ""
    ])
    
    for i, error in enumerate(errors, 1):
        lines.append(f"   {i}. {error}")
    
    lines.extend([
        "",
        "💡 Common solutions:",
        "   • Review protobuf syntax and field definitions",
        "   • Check for typos in field names and types",
        "   • Verify field numbers are unique and start from 1",
        "   • Ensure proper message and field naming conventions"
    ])
    
    return "\n".join(lines)


def suggest_next_steps(error_type: str, context: Optional[Dict[str, str]] = None) -> List[str]:
    """Suggest next steps based on the error type and context.
    
    Args:
        error_type: Type of error
        context: Context information
        
    Returns:
        List of suggested next steps
    """
    base_suggestions = {
        "protoc_not_found": [
            "Install Protocol Buffers compiler using your package manager",
            "Verify protoc is in your PATH with: protoc --version",
            "Consider using a Docker container with protoc pre-installed"
        ],
        "proto_file_not_found": [
            "Double-check the file path and verify the file exists",
            "Use absolute paths to avoid working directory confusion",
            "Check file permissions and ownership"
        ],
        "proto_compilation_failed": [
            "Review the compiler output for specific syntax errors",
            "Use a protobuf syntax validator or IDE extension",
            "Start with a minimal working proto file and build up"
        ],
        "import_resolution_failed": [
            "Create a proto directory structure that mirrors your imports",
            "Use --proto_path flags to specify search directories",
            "Consider using well-known types from google/protobuf/"
        ]
    }
    
    suggestions = base_suggestions.get(error_type, [])
    
    # Add context-specific suggestions
    if context:
        if "schema_path" in context and context["schema_path"]:
            suggestions.append(f"Focus on the schema file: {context['schema_path']}")
        
        if "missing_file" in context:
            suggestions.append(f"Locate or create the missing file: {context['missing_file']}")
    
    return suggestions