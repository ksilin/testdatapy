"""Performance optimization module for TestDataPy.

This module contains optimized implementations and caching mechanisms
to improve performance of critical operations.
"""

from .compiler_cache import CompilerCache
from .function_cache import FunctionCache
from .performance_monitor import PerformanceMonitor
from .optimized_registry import OptimizedFunctionRegistry

__all__ = [
    'CompilerCache',
    'FunctionCache', 
    'PerformanceMonitor',
    'OptimizedFunctionRegistry'
]