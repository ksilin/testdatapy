"""Compatibility module for transformation system.

This module provides backward compatibility aliases to match documentation examples.
The actual implementation is in the transformers module.
"""

# Import the actual implementation and create aliases for documentation compatibility
from testdatapy.transformers.transformation_manager import (
    TransformationManager as TransformationEngine
)
from testdatapy.transformers import (
    DataTransformer,
    ProtobufTransformer,
    FunctionRegistry,
    FunctionValidator,
    FakerIntegration,
    global_transformation_manager
)

# Export everything under the documented names
__all__ = [
    "TransformationEngine",
    "DataTransformer", 
    "ProtobufTransformer",
    "FunctionRegistry",
    "FunctionValidator",
    "FakerIntegration",
    "global_transformation_manager"
]