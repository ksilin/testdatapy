"""Comprehensive transformation manager that integrates all transformation capabilities.

This module provides a unified interface for managing transformation functions,
validation, execution, and Faker integration.
"""

from typing import Any, Dict, List, Optional, Type, Union, Callable
from pathlib import Path
import yaml

from .function_registry import FunctionRegistry, FunctionCategory, FunctionMetadata, global_registry
from .function_validator import FunctionValidator, SafeExecutor, ValidationLevel, SecurityLevel
from .faker_integration import FakerIntegration
from ..exceptions import TestDataPyException, ConfigurationException
from ..logging_config import get_schema_logger, PerformanceTimer

logger = get_schema_logger(__name__)


class TransformationManager:
    """Unified transformation management system.
    
    Provides a comprehensive interface for:
    - Function registration and management
    - Validation and safe execution
    - Faker integration
    - Configuration loading
    - Batch operations
    """
    
    def __init__(
        self,
        registry: Optional[FunctionRegistry] = None,
        validation_level: ValidationLevel = ValidationLevel.STANDARD,
        security_level: SecurityLevel = SecurityLevel.SAFE,
        faker_locale: Union[str, List[str]] = "en_US",
        faker_seed: Optional[int] = None
    ):
        """Initialize the transformation manager.
        
        Args:
            registry: Function registry (uses global if None)
            validation_level: Default validation level
            security_level: Default security level
            faker_locale: Faker locale(s)
            faker_seed: Faker random seed
        """
        self.registry = registry or global_registry
        self.validator = FunctionValidator(validation_level)
        self.executor = SafeExecutor(security_level)
        self.faker_integration = FakerIntegration(faker_locale, seed=faker_seed)
        
        # Initialize with built-in functions
        self._register_builtin_functions()
        
        # Register Faker functions
        self.faker_integration.register_with_registry(self.registry, namespace="faker")
        
        logger.info("Initialized transformation manager",
                   validation_level=validation_level.value,
                   security_level=security_level.value,
                   faker_locale=faker_locale)
    
    def register_function(
        self,
        name: str,
        func: Callable,
        description: str = "",
        category: FunctionCategory = FunctionCategory.CUSTOM,
        input_types: Optional[List[Type]] = None,
        output_type: Optional[Type] = None,
        validate: bool = True,
        overwrite: bool = False,
        **metadata_kwargs
    ) -> bool:
        """Register a transformation function with validation.
        
        Args:
            name: Function name
            func: Callable function
            description: Function description
            category: Function category
            input_types: Expected input types
            output_type: Expected output type
            validate: Whether to validate function before registration
            overwrite: Whether to overwrite existing function
            **metadata_kwargs: Additional metadata
            
        Returns:
            True if registration successful, False otherwise
        """
        try:
            # Validate function if requested
            if validate:
                validation_result = self.validator.validate_function(
                    func, input_types, output_type
                )
                
                if not validation_result.is_valid:
                    logger.error(f"Function validation failed for {name}",
                               errors=validation_result.errors)
                    return False
                
                if validation_result.warnings:
                    logger.warning(f"Function validation warnings for {name}",
                                 warnings=validation_result.warnings)
            
            # Register with registry
            success = self.registry.register(
                name=name,
                func=func,
                description=description,
                category=category,
                input_types=input_types,
                output_type=output_type,
                overwrite=overwrite,
                **metadata_kwargs
            )
            
            if success:
                logger.info(f"Successfully registered function: {name}",
                           category=category.value)
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to register function {name}: {e}")
            return False
    
    def execute_function(
        self,
        name: str,
        *args,
        validation_level: Optional[ValidationLevel] = None,
        security_level: Optional[SecurityLevel] = None,
        timeout: Optional[float] = None,
        context: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Any:
        """Execute a transformation function with validation and safety.
        
        Args:
            name: Function name
            *args: Function arguments
            validation_level: Override validation level
            security_level: Override security level
            timeout: Execution timeout
            context: Additional context
            **kwargs: Function keyword arguments
            
        Returns:
            Function result
            
        Raises:
            TestDataPyException: If function not found or execution fails
        """
        # Get function from registry
        registered_func = self.registry.get_function(name)
        if not registered_func:
            available_functions = ", ".join(self.registry.list_functions())
            raise TestDataPyException(
                message=f"Transformation function not found: {name}",
                suggestions=[
                    f"Available functions: {available_functions}",
                    "Register the function using register_function()",
                    "Check function name spelling and case"
                ]
            )
        
        try:
            with PerformanceTimer(logger, "managed_function_execution", function_name=name):
                # Validate function if needed
                if validation_level:
                    validation_result = self.validator.validate_function(
                        registered_func.func,
                        validation_level=validation_level
                    )
                    if not validation_result.is_valid:
                        raise TestDataPyException(
                            message=f"Function validation failed: {name}",
                            details="; ".join(validation_result.errors)
                        )
                
                # Execute function safely
                if security_level:
                    executor = SafeExecutor(security_level)
                    execution_result = executor.execute_function(
                        registered_func.func, *args, timeout=timeout, context=context, **kwargs
                    )
                    
                    if not execution_result.success:
                        raise execution_result.error
                    
                    return execution_result.result
                else:
                    # Use default executor
                    execution_result = self.executor.execute_function(
                        registered_func.func, *args, timeout=timeout, context=context, **kwargs
                    )
                    
                    if not execution_result.success:
                        raise execution_result.error
                    
                    return execution_result.result
        
        except Exception as e:
            if isinstance(e, TestDataPyException):
                raise
            
            logger.error(f"Function execution failed: {name}", error=str(e))
            raise TestDataPyException(
                message=f"Transformation function execution failed: {name}",
                details=str(e),
                original_error=e
            ) from e
    
    def batch_execute(
        self,
        operations: List[Dict[str, Any]],
        stop_on_error: bool = False
    ) -> List[Dict[str, Any]]:
        """Execute multiple transformation operations in batch.
        
        Args:
            operations: List of operation dictionaries with 'function', 'args', 'kwargs'
            stop_on_error: Whether to stop on first error
            
        Returns:
            List of results with success/error information
        """
        results = []
        
        for i, operation in enumerate(operations):
            function_name = operation.get('function')
            args = operation.get('args', [])
            kwargs = operation.get('kwargs', {})
            
            try:
                result = self.execute_function(function_name, *args, **kwargs)
                results.append({
                    'index': i,
                    'function': function_name,
                    'success': True,
                    'result': result,
                    'error': None
                })
                
            except Exception as e:
                results.append({
                    'index': i,
                    'function': function_name,
                    'success': False,
                    'result': None,
                    'error': str(e)
                })
                
                if stop_on_error:
                    logger.error(f"Batch execution stopped at operation {i}: {e}")
                    break
        
        successful = len([r for r in results if r['success']])
        logger.info(f"Batch execution completed",
                   total_operations=len(operations),
                   successful=successful,
                   failed=len(results) - successful)
        
        return results
    
    def load_functions_from_config(
        self,
        config_path: Union[str, Path],
        namespace: Optional[str] = None,
        validate: bool = True
    ) -> int:
        """Load transformation functions from configuration file.
        
        Args:
            config_path: Path to configuration file
            namespace: Optional namespace for loaded functions
            validate: Whether to validate functions before registration
            
        Returns:
            Number of functions loaded
        """
        config_path = Path(config_path)
        
        if not config_path.exists():
            raise ConfigurationException(
                message=f"Configuration file not found: {config_path}"
            )
        
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            functions_config = config.get('functions', {})
            loaded_count = 0
            
            for func_name, func_config in functions_config.items():
                # This would need actual function loading implementation
                # For now, this is a placeholder structure
                logger.info(f"Loading function configuration: {func_name}")
                loaded_count += 1
            
            logger.info(f"Loaded {loaded_count} functions from config",
                       config_path=str(config_path),
                       namespace=namespace)
            
            return loaded_count
            
        except Exception as e:
            logger.error(f"Failed to load functions from config: {e}")
            raise ConfigurationException(
                message=f"Failed to load transformation functions from {config_path}",
                details=str(e)
            ) from e
    
    def export_functions(
        self,
        output_path: Union[str, Path],
        category: Optional[FunctionCategory] = None,
        namespace: Optional[str] = None,
        include_builtin: bool = False
    ) -> bool:
        """Export function registry to file.
        
        Args:
            output_path: Output file path
            category: Filter by category
            namespace: Filter by namespace
            include_builtin: Whether to include built-in functions
            
        Returns:
            True if export successful, False otherwise
        """
        try:
            registry_data = self.registry.export_registry()
            
            # Filter if needed
            if category or namespace or not include_builtin:
                filtered_functions = {}
                
                for func_name, func_info in registry_data['functions'].items():
                    # Category filter
                    if category and func_info['category'] != category.value:
                        continue
                    
                    # Namespace filter
                    if namespace and not func_name.startswith(f"{namespace}."):
                        continue
                    
                    # Built-in filter
                    if not include_builtin and func_info.get('tags', []):
                        if 'builtin' in func_info['tags']:
                            continue
                    
                    filtered_functions[func_name] = func_info
                
                registry_data['functions'] = filtered_functions
            
            # Save to file
            output_path = Path(output_path)
            with open(output_path, 'w') as f:
                yaml.dump(registry_data, f, default_flow_style=False, sort_keys=True)
            
            logger.info(f"Exported functions to {output_path}",
                       function_count=len(registry_data['functions']))
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to export functions: {e}")
            return False
    
    def search_functions(
        self,
        query: str,
        category: Optional[FunctionCategory] = None,
        tags: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Search for transformation functions.
        
        Args:
            query: Search query
            category: Filter by category
            tags: Filter by tags
            
        Returns:
            List of matching function information
        """
        matching_names = self.registry.search_functions(query)
        
        results = []
        for name in matching_names:
            func_info = self.registry.get_function_info(name)
            if func_info:
                # Apply filters
                if category and func_info['category'] != category.value:
                    continue
                
                if tags:
                    func_tags = set(func_info.get('tags', []))
                    if not any(tag in func_tags for tag in tags):
                        continue
                
                results.append(func_info)
        
        return results
    
    def get_function_suggestions(self, input_type: Type, output_type: Type) -> List[str]:
        """Get function suggestions based on input/output types.
        
        Args:
            input_type: Expected input type
            output_type: Expected output type
            
        Returns:
            List of suggested function names
        """
        suggestions = []
        
        all_functions = self.registry.list_functions()
        for func_name in all_functions:
            func_info = self.registry.get_function_info(func_name)
            if func_info:
                # Check type compatibility
                input_types = func_info.get('input_types', [])
                func_output_type = func_info.get('output_type')
                
                if input_types and input_type.__name__ in input_types:
                    if func_output_type and func_output_type == output_type.__name__:
                        suggestions.append(func_name)
        
        return suggestions
    
    def validate_all_functions(self) -> Dict[str, Any]:
        """Validate all registered functions.
        
        Returns:
            Validation report
        """
        return self.registry.validate_registry()
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive transformation manager statistics.
        
        Returns:
            Statistics dictionary
        """
        registry_stats = self.registry.get_statistics()
        faker_stats = self.faker_integration.get_statistics()
        
        return {
            "registry": registry_stats,
            "faker": faker_stats,
            "validation_level": self.validator.validation_level.value,
            "security_level": self.executor.security_level.value,
            "total_functions": registry_stats["total_functions"],
            "safe_functions": registry_stats["safe_functions"],
            "categories": registry_stats["categories"]
        }
    
    def _register_builtin_functions(self) -> None:
        """Register built-in transformation functions."""
        
        # String transformations
        builtin_functions = {
            "upper": (lambda x: str(x).upper(), "Convert to uppercase", FunctionCategory.STRING),
            "lower": (lambda x: str(x).lower(), "Convert to lowercase", FunctionCategory.STRING),
            "title": (lambda x: str(x).title(), "Convert to title case", FunctionCategory.STRING),
            "strip": (lambda x: str(x).strip(), "Strip whitespace", FunctionCategory.STRING),
            "reverse": (lambda x: str(x)[::-1], "Reverse string", FunctionCategory.STRING),
            
            # Numeric transformations
            "abs": (lambda x: abs(float(x)), "Absolute value", FunctionCategory.NUMERIC),
            "round": (lambda x, digits=0: round(float(x), digits), "Round number", FunctionCategory.NUMERIC),
            "ceil": (lambda x: int(__import__('math').ceil(float(x))), "Ceiling", FunctionCategory.NUMERIC),
            "floor": (lambda x: int(__import__('math').floor(float(x))), "Floor", FunctionCategory.NUMERIC),
            
            # Type conversions
            "int": (lambda x: int(x), "Convert to integer", FunctionCategory.NUMERIC),
            "float": (lambda x: float(x), "Convert to float", FunctionCategory.NUMERIC),
            "str": (lambda x: str(x), "Convert to string", FunctionCategory.STRING),
            "bool": (lambda x: bool(x), "Convert to boolean", FunctionCategory.BOOLEAN),
            
            # Boolean operations
            "not": (lambda x: not bool(x), "Logical NOT", FunctionCategory.BOOLEAN),
            
            # Collection operations
            "len": (lambda x: len(x), "Get length", FunctionCategory.COLLECTION),
            "first": (lambda x: x[0] if x else None, "Get first element", FunctionCategory.COLLECTION),
            "last": (lambda x: x[-1] if x else None, "Get last element", FunctionCategory.COLLECTION),
            
            # Formatting
            "format_number": (lambda x, decimals=2: f"{float(x):.{decimals}f}", "Format number", FunctionCategory.FORMATTING),
            "pad_left": (lambda x, width=10, char=' ': str(x).ljust(width, char), "Pad left", FunctionCategory.FORMATTING),
            "pad_right": (lambda x, width=10, char=' ': str(x).rjust(width, char), "Pad right", FunctionCategory.FORMATTING),
        }
        
        registered_count = 0
        for name, (func, description, category) in builtin_functions.items():
            success = self.registry.register(
                name=name,
                func=func,
                description=description,
                category=category,
                tags={"builtin"},
                is_safe=True,
                overwrite=True
            )
            
            if success:
                registered_count += 1
        
        logger.info(f"Registered {registered_count} built-in transformation functions")


# Global transformation manager instance
global_transformation_manager = TransformationManager()