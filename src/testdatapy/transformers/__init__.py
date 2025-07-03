"""Transformation engines for TestDataPy.

This module provides comprehensive transformation capabilities including:
- ProtobufTransformer for protobuf message transformation
- Function registry for custom transformation functions
- Function validation and safe execution
- Enhanced Faker integration
- Unified transformation management
"""

from .base import DataTransformer
from .protobuf_transformer import ProtobufTransformer
from .function_registry import (
    FunctionRegistry,
    FunctionCategory,
    FunctionMetadata,
    RegisteredFunction,
    global_registry
)
from .function_validator import (
    FunctionValidator,
    SafeExecutor,
    ValidationLevel,
    SecurityLevel,
    ValidationResult,
    ExecutionResult
)
from .faker_integration import (
    FakerIntegration,
    TestDataProvider,
    PatternProvider,
    LocalizedDataProvider
)
from .transformation_manager import (
    TransformationManager,
    global_transformation_manager
)

__all__ = [
    # Base classes
    "DataTransformer",
    "ProtobufTransformer",
    
    # Function registry
    "FunctionRegistry",
    "FunctionCategory", 
    "FunctionMetadata",
    "RegisteredFunction",
    "global_registry",
    
    # Validation and execution
    "FunctionValidator",
    "SafeExecutor",
    "ValidationLevel",
    "SecurityLevel",
    "ValidationResult",
    "ExecutionResult",
    
    # Faker integration
    "FakerIntegration",
    "TestDataProvider",
    "PatternProvider",
    "LocalizedDataProvider",
    
    # Unified management
    "TransformationManager",
    "global_transformation_manager"
]