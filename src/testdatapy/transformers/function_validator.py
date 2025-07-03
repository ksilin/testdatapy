"""Function validation and execution engine for transformation functions.

This module provides comprehensive validation and safe execution capabilities
for transformation functions with type checking, sandbox support, and error handling.
"""

import inspect
import sys
import traceback
from typing import Any, Callable, Dict, List, Optional, Set, Type, Union
from dataclasses import dataclass
from enum import Enum
import ast
import dis
from contextlib import contextmanager
import signal
import threading
import time

from ..exceptions import (
    TestDataPyException,
    ConfigurationException
)
from ..logging_config import get_schema_logger, PerformanceTimer

logger = get_schema_logger(__name__)


class ValidationLevel(Enum):
    """Validation strictness levels."""
    BASIC = "basic"          # Basic callable and signature checks
    STANDARD = "standard"    # Standard + type hints validation
    STRICT = "strict"        # Standard + code analysis
    PARANOID = "paranoid"    # Strict + sandbox execution testing


class SecurityLevel(Enum):
    """Security levels for function execution."""
    UNRESTRICTED = "unrestricted"  # No restrictions
    SAFE = "safe"                  # Block dangerous operations
    SANDBOX = "sandbox"            # Full sandbox environment


@dataclass
class ValidationResult:
    """Result of function validation."""
    is_valid: bool
    validation_level: ValidationLevel
    errors: List[str]
    warnings: List[str]
    suggestions: List[str]
    metadata: Dict[str, Any]
    
    def __bool__(self) -> bool:
        return self.is_valid


@dataclass
class ExecutionResult:
    """Result of function execution."""
    success: bool
    result: Any
    execution_time: float
    error: Optional[Exception]
    warnings: List[str]
    metadata: Dict[str, Any]


class FunctionValidator:
    """Comprehensive function validator with multiple validation levels."""
    
    # Dangerous operations to check for
    DANGEROUS_BUILTINS = {
        'exec', 'eval', 'compile', '__import__', 'open', 'file',
        'input', 'raw_input', 'reload', 'vars', 'locals', 'globals',
        'dir', 'hasattr', 'getattr', 'setattr', 'delattr'
    }
    
    DANGEROUS_MODULES = {
        'os', 'sys', 'subprocess', 'threading', 'multiprocessing',
        'socket', 'urllib', 'http', 'ftplib', 'smtplib', 'pickle',
        'marshal', 'shelve', 'dbm', 'sqlite3', 'tempfile'
    }
    
    SAFE_MODULES = {
        'math', 'random', 'datetime', 'time', 'string', 'json',
        'base64', 'hashlib', 'uuid', 'decimal', 'fractions',
        're', 'collections', 'itertools', 'functools', 'operator'
    }
    
    def __init__(self, validation_level: ValidationLevel = ValidationLevel.STANDARD):
        """Initialize the function validator.
        
        Args:
            validation_level: Default validation level to use
        """
        self.validation_level = validation_level
        self._validation_cache: Dict[str, ValidationResult] = {}
        
        logger.info(f"Initialized function validator with level: {validation_level.value}")
    
    def validate_function(
        self,
        func: Callable,
        expected_input_types: Optional[List[Type]] = None,
        expected_output_type: Optional[Type] = None,
        validation_level: Optional[ValidationLevel] = None,
        allow_dangerous: bool = False
    ) -> ValidationResult:
        """Validate a transformation function.
        
        Args:
            func: Function to validate
            expected_input_types: Expected input parameter types
            expected_output_type: Expected return type
            validation_level: Validation level to use
            allow_dangerous: Whether to allow potentially dangerous operations
            
        Returns:
            ValidationResult with validation details
        """
        level = validation_level or self.validation_level
        
        # Check cache
        cache_key = f"{func.__name__}_{level.value}_{hash(str(func))}"
        if cache_key in self._validation_cache:
            return self._validation_cache[cache_key]
        
        errors = []
        warnings = []
        suggestions = []
        metadata = {}
        
        try:
            with PerformanceTimer(logger, "function_validation", 
                                function_name=getattr(func, '__name__', 'unknown'),
                                validation_level=level.value):
                
                # Basic validation
                if level.value in [ValidationLevel.BASIC.value, ValidationLevel.STANDARD.value, 
                                 ValidationLevel.STRICT.value, ValidationLevel.PARANOID.value]:
                    basic_result = self._validate_basic(func)
                    errors.extend(basic_result.get('errors', []))
                    warnings.extend(basic_result.get('warnings', []))
                    suggestions.extend(basic_result.get('suggestions', []))
                    metadata.update(basic_result.get('metadata', {}))
                
                # Standard validation
                if level.value in [ValidationLevel.STANDARD.value, ValidationLevel.STRICT.value, 
                                 ValidationLevel.PARANOID.value]:
                    standard_result = self._validate_standard(
                        func, expected_input_types, expected_output_type
                    )
                    errors.extend(standard_result.get('errors', []))
                    warnings.extend(standard_result.get('warnings', []))
                    suggestions.extend(standard_result.get('suggestions', []))
                    metadata.update(standard_result.get('metadata', {}))
                
                # Strict validation
                if level.value in [ValidationLevel.STRICT.value, ValidationLevel.PARANOID.value]:
                    strict_result = self._validate_strict(func, allow_dangerous)
                    errors.extend(strict_result.get('errors', []))
                    warnings.extend(strict_result.get('warnings', []))
                    suggestions.extend(strict_result.get('suggestions', []))
                    metadata.update(strict_result.get('metadata', {}))
                
                # Paranoid validation
                if level.value == ValidationLevel.PARANOID.value:
                    paranoid_result = self._validate_paranoid(func)
                    errors.extend(paranoid_result.get('errors', []))
                    warnings.extend(paranoid_result.get('warnings', []))
                    suggestions.extend(paranoid_result.get('suggestions', []))
                    metadata.update(paranoid_result.get('metadata', {}))
        
        except Exception as e:
            errors.append(f"Validation process failed: {e}")
            logger.error(f"Function validation failed for {func.__name__}: {e}")
        
        result = ValidationResult(
            is_valid=len(errors) == 0,
            validation_level=level,
            errors=errors,
            warnings=warnings,
            suggestions=suggestions,
            metadata=metadata
        )
        
        # Cache result
        self._validation_cache[cache_key] = result
        
        logger.debug(f"Function validation completed",
                    function_name=func.__name__,
                    validation_level=level.value,
                    is_valid=result.is_valid,
                    errors_count=len(errors),
                    warnings_count=len(warnings))
        
        return result
    
    def _validate_basic(self, func: Callable) -> Dict[str, Any]:
        """Perform basic function validation."""
        errors = []
        warnings = []
        suggestions = []
        metadata = {}
        
        # Check if function is callable
        if not callable(func):
            errors.append("Object is not callable")
            return {'errors': errors, 'warnings': warnings, 'suggestions': suggestions, 'metadata': metadata}
        
        # Check function name
        if not hasattr(func, '__name__'):
            warnings.append("Function has no __name__ attribute")
        else:
            metadata['function_name'] = func.__name__
        
        # Check if function has docstring
        if not func.__doc__:
            warnings.append("Function has no docstring")
            suggestions.append("Add a docstring to describe the function")
        else:
            metadata['has_docstring'] = True
            metadata['docstring_length'] = len(func.__doc__)
        
        # Inspect signature
        try:
            sig = inspect.signature(func)
            metadata['parameter_count'] = len(sig.parameters)
            metadata['has_return_annotation'] = sig.return_annotation != inspect.Signature.empty
            
            # Check for required parameters
            required_params = [
                p for p in sig.parameters.values()
                if p.default == inspect.Parameter.empty and p.kind != inspect.Parameter.VAR_POSITIONAL
            ]
            
            if len(required_params) == 0:
                warnings.append("Function has no required parameters")
            
            metadata['required_parameters'] = len(required_params)
            
        except Exception as e:
            errors.append(f"Cannot inspect function signature: {e}")
        
        return {
            'errors': errors,
            'warnings': warnings,
            'suggestions': suggestions,
            'metadata': metadata
        }
    
    def _validate_standard(
        self,
        func: Callable,
        expected_input_types: Optional[List[Type]],
        expected_output_type: Optional[Type]
    ) -> Dict[str, Any]:
        """Perform standard function validation with type checking."""
        errors = []
        warnings = []
        suggestions = []
        metadata = {}
        
        try:
            sig = inspect.signature(func)
            
            # Check type annotations
            annotated_params = 0
            for param in sig.parameters.values():
                if param.annotation != inspect.Parameter.empty:
                    annotated_params += 1
            
            metadata['annotated_parameters'] = annotated_params
            metadata['total_parameters'] = len(sig.parameters)
            
            if annotated_params == 0:
                warnings.append("Function has no type annotations")
                suggestions.append("Add type hints to improve function clarity")
            elif annotated_params < len(sig.parameters):
                warnings.append("Some parameters lack type annotations")
            
            # Check return type annotation
            if sig.return_annotation == inspect.Signature.empty:
                warnings.append("Function has no return type annotation")
                suggestions.append("Add return type annotation")
            else:
                metadata['has_return_type'] = True
            
            # Validate expected types if provided
            if expected_input_types:
                self._validate_input_types(sig, expected_input_types, errors, warnings)
            
            if expected_output_type:
                self._validate_output_type(sig, expected_output_type, errors, warnings)
        
        except Exception as e:
            errors.append(f"Type validation failed: {e}")
        
        return {
            'errors': errors,
            'warnings': warnings,
            'suggestions': suggestions,
            'metadata': metadata
        }
    
    def _validate_strict(self, func: Callable, allow_dangerous: bool) -> Dict[str, Any]:
        """Perform strict function validation with code analysis."""
        errors = []
        warnings = []
        suggestions = []
        metadata = {}
        
        try:
            # Analyze function code
            source_code = inspect.getsource(func)
            metadata['source_length'] = len(source_code)
            
            # Parse AST for dangerous operations
            tree = ast.parse(source_code)
            dangerous_ops = self._analyze_ast_security(tree)
            
            if dangerous_ops and not allow_dangerous:
                errors.extend([f"Potentially dangerous operation: {op}" for op in dangerous_ops])
                suggestions.append("Remove dangerous operations or set allow_dangerous=True")
            
            metadata['dangerous_operations'] = dangerous_ops
            
            # Check for imports
            imports = self._extract_imports(tree)
            metadata['imports'] = imports
            
            dangerous_imports = [imp for imp in imports if imp in self.DANGEROUS_MODULES]
            if dangerous_imports and not allow_dangerous:
                errors.extend([f"Dangerous import: {imp}" for imp in dangerous_imports])
            
            # Check function complexity
            complexity = self._calculate_complexity(tree)
            metadata['complexity'] = complexity
            
            if complexity > 10:
                warnings.append(f"High function complexity: {complexity}")
                suggestions.append("Consider breaking down complex function")
        
        except Exception as e:
            warnings.append(f"Code analysis failed: {e}")
        
        return {
            'errors': errors,
            'warnings': warnings,
            'suggestions': suggestions,
            'metadata': metadata
        }
    
    def _validate_paranoid(self, func: Callable) -> Dict[str, Any]:
        """Perform paranoid validation with sandbox testing."""
        errors = []
        warnings = []
        suggestions = []
        metadata = {}
        
        try:
            # Test function execution in controlled environment
            test_inputs = [
                None, 0, 1, -1, "", "test", [], {}, True, False
            ]
            
            successful_tests = 0
            failed_tests = 0
            
            for test_input in test_inputs:
                try:
                    # Try to execute with timeout
                    with self._execution_timeout(1.0):  # 1 second timeout
                        result = func(test_input)
                    successful_tests += 1
                except Exception:
                    failed_tests += 1
            
            metadata['test_inputs_tried'] = len(test_inputs)
            metadata['successful_tests'] = successful_tests
            metadata['failed_tests'] = failed_tests
            
            if failed_tests == len(test_inputs):
                warnings.append("Function failed on all test inputs")
                suggestions.append("Check function robustness and error handling")
        
        except Exception as e:
            warnings.append(f"Sandbox testing failed: {e}")
        
        return {
            'errors': errors,
            'warnings': warnings,
            'suggestions': suggestions,
            'metadata': metadata
        }
    
    def _validate_input_types(
        self,
        sig: inspect.Signature,
        expected_types: List[Type],
        errors: List[str],
        warnings: List[str]
    ) -> None:
        """Validate input parameter types."""
        params = list(sig.parameters.values())
        
        if len(params) < len(expected_types):
            errors.append(f"Function has {len(params)} parameters but {len(expected_types)} expected")
            return
        
        for i, expected_type in enumerate(expected_types):
            if i < len(params):
                param = params[i]
                if param.annotation != inspect.Parameter.empty:
                    if param.annotation != expected_type:
                        warnings.append(
                            f"Parameter {param.name} has type {param.annotation} "
                            f"but expected {expected_type}"
                        )
    
    def _validate_output_type(
        self,
        sig: inspect.Signature,
        expected_type: Type,
        errors: List[str],
        warnings: List[str]
    ) -> None:
        """Validate output return type."""
        if sig.return_annotation != inspect.Signature.empty:
            if sig.return_annotation != expected_type:
                warnings.append(
                    f"Return type is {sig.return_annotation} but expected {expected_type}"
                )
    
    def _analyze_ast_security(self, tree: ast.AST) -> List[str]:
        """Analyze AST for potentially dangerous operations."""
        dangerous_ops = []
        
        for node in ast.walk(tree):
            # Check for dangerous built-in calls
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    if node.func.id in self.DANGEROUS_BUILTINS:
                        dangerous_ops.append(f"Call to {node.func.id}")
            
            # Check for attribute access to dangerous modules
            if isinstance(node, ast.Attribute):
                if isinstance(node.value, ast.Name):
                    if node.value.id in self.DANGEROUS_MODULES:
                        dangerous_ops.append(f"Access to {node.value.id}.{node.attr}")
        
        return dangerous_ops
    
    def _extract_imports(self, tree: ast.AST) -> List[str]:
        """Extract imported modules from AST."""
        imports = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.append(node.module)
        
        return imports
    
    def _calculate_complexity(self, tree: ast.AST) -> int:
        """Calculate cyclomatic complexity of function."""
        complexity = 1  # Base complexity
        
        for node in ast.walk(tree):
            if isinstance(node, (ast.If, ast.While, ast.For, ast.Try, ast.With)):
                complexity += 1
            elif isinstance(node, ast.ExceptHandler):
                complexity += 1
        
        return complexity
    
    @contextmanager
    def _execution_timeout(self, timeout_seconds: float):
        """Context manager for function execution timeout."""
        def timeout_handler(signum, frame):
            raise TimeoutError(f"Function execution exceeded {timeout_seconds} seconds")
        
        # Set up signal handler
        old_handler = signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(int(timeout_seconds))
        
        try:
            yield
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)


class SafeExecutor:
    """Safe execution environment for transformation functions."""
    
    def __init__(self, security_level: SecurityLevel = SecurityLevel.SAFE):
        """Initialize the safe executor.
        
        Args:
            security_level: Security level for execution
        """
        self.security_level = security_level
        self.execution_timeout = 30.0  # Default timeout in seconds
        self.max_memory_mb = 100  # Maximum memory usage in MB
        
        logger.info(f"Initialized safe executor with security level: {security_level.value}")
    
    def execute_function(
        self,
        func: Callable,
        *args,
        timeout: Optional[float] = None,
        context: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> ExecutionResult:
        """Execute a function safely with monitoring.
        
        Args:
            func: Function to execute
            *args: Positional arguments
            timeout: Execution timeout in seconds
            context: Additional execution context
            **kwargs: Keyword arguments
            
        Returns:
            ExecutionResult with execution details
        """
        start_time = time.time()
        timeout = timeout or self.execution_timeout
        warnings = []
        metadata = {
            'security_level': self.security_level.value,
            'timeout': timeout
        }
        
        try:
            with PerformanceTimer(logger, "safe_function_execution",
                                function_name=getattr(func, '__name__', 'unknown')):
                
                if self.security_level == SecurityLevel.SANDBOX:
                    result = self._execute_sandboxed(func, args, kwargs, timeout)
                elif self.security_level == SecurityLevel.SAFE:
                    result = self._execute_safe(func, args, kwargs, timeout)
                else:  # UNRESTRICTED
                    result = self._execute_unrestricted(func, args, kwargs)
                
                execution_time = time.time() - start_time
                
                logger.debug(f"Function executed successfully",
                           function_name=getattr(func, '__name__', 'unknown'),
                           execution_time=execution_time,
                           security_level=self.security_level.value)
                
                return ExecutionResult(
                    success=True,
                    result=result,
                    execution_time=execution_time,
                    error=None,
                    warnings=warnings,
                    metadata=metadata
                )
        
        except Exception as e:
            execution_time = time.time() - start_time
            
            logger.warning(f"Function execution failed",
                         function_name=getattr(func, '__name__', 'unknown'),
                         error=str(e),
                         execution_time=execution_time)
            
            return ExecutionResult(
                success=False,
                result=None,
                execution_time=execution_time,
                error=e,
                warnings=warnings,
                metadata=metadata
            )
    
    def _execute_unrestricted(self, func: Callable, args: tuple, kwargs: dict) -> Any:
        """Execute function without restrictions."""
        return func(*args, **kwargs)
    
    def _execute_safe(self, func: Callable, args: tuple, kwargs: dict, timeout: float) -> Any:
        """Execute function with basic safety measures."""
        # Use timeout for execution
        with self._execution_timeout(timeout):
            return func(*args, **kwargs)
    
    def _execute_sandboxed(self, func: Callable, args: tuple, kwargs: dict, timeout: float) -> Any:
        """Execute function in sandboxed environment."""
        # This is a simplified sandbox - in production you might want to use
        # more sophisticated sandboxing techniques like restricted execution
        # or containerization
        
        with self._execution_timeout(timeout):
            # Restrict built-ins
            original_builtins = __builtins__
            try:
                # Create restricted builtins
                safe_builtins = {
                    name: value for name, value in original_builtins.items()
                    if name not in FunctionValidator.DANGEROUS_BUILTINS
                }
                
                # Replace builtins temporarily (this is a simplified approach)
                func.__globals__['__builtins__'] = safe_builtins
                
                return func(*args, **kwargs)
            finally:
                # Restore original builtins
                func.__globals__['__builtins__'] = original_builtins
    
    @contextmanager
    def _execution_timeout(self, timeout_seconds: float):
        """Context manager for execution timeout."""
        def timeout_handler(signum, frame):
            raise TimeoutError(f"Function execution exceeded {timeout_seconds} seconds")
        
        old_handler = signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(int(timeout_seconds))
        
        try:
            yield
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)