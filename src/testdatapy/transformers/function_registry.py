"""Comprehensive transformation function registry for TestDataPy.

This module provides a centralized registry for transformation functions that can be
used across different transformation engines and contexts.
"""

import inspect
import logging
import re
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Set, Type, Union
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import importlib.util
import yaml

from ..exceptions import (
    TestDataPyException,
    ConfigurationException,
    InvalidConfigurationError
)
from ..logging_config import get_schema_logger, PerformanceTimer

logger = get_schema_logger(__name__)


class FunctionCategory(Enum):
    """Categories for transformation functions."""
    STRING = "string"
    NUMERIC = "numeric"
    DATE_TIME = "date_time"
    BOOLEAN = "boolean"
    COLLECTION = "collection"
    FAKER = "faker"
    CUSTOM = "custom"
    VALIDATION = "validation"
    FORMATTING = "formatting"
    PROTOBUF = "protobuf"


@dataclass
class FunctionMetadata:
    """Metadata for a transformation function."""
    name: str
    description: str
    category: FunctionCategory
    input_types: List[Type] = field(default_factory=list)
    output_type: Optional[Type] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    examples: List[Dict[str, Any]] = field(default_factory=list)
    tags: Set[str] = field(default_factory=set)
    version: str = "1.0.0"
    author: Optional[str] = None
    is_safe: bool = True  # Safe for sandboxed execution
    requires_context: bool = False  # Requires additional context beyond input


@dataclass
class RegisteredFunction:
    """Wrapper for a registered transformation function."""
    func: Callable
    metadata: FunctionMetadata
    
    def __call__(self, *args, **kwargs):
        """Execute the transformation function."""
        return self.func(*args, **kwargs)
    
    def validate_signature(self) -> bool:
        """Validate function signature against metadata."""
        try:
            sig = inspect.signature(self.func)
            params = list(sig.parameters.keys())
            
            # Basic validation - function should accept at least one parameter
            if len(params) == 0 and not self.metadata.requires_context:
                return False
            
            return True
        except Exception as e:
            logger.warning(f"Signature validation failed for {self.metadata.name}: {e}")
            return False


class FunctionRegistry:
    """Central registry for transformation functions.
    
    Provides registration, discovery, validation, and execution of transformation
    functions with comprehensive metadata support.
    """
    
    def __init__(self):
        """Initialize the function registry."""
        self._functions: Dict[str, RegisteredFunction] = {}
        self._categories: Dict[FunctionCategory, Set[str]] = defaultdict(set)
        self._tags: Dict[str, Set[str]] = defaultdict(set)
        self._aliases: Dict[str, str] = {}  # alias -> function_name mapping
        self._namespaces: Dict[str, Set[str]] = defaultdict(set)  # namespace -> function_names
        
        logger.info("Initialized transformation function registry")
    
    def register(
        self,
        name: str,
        func: Callable,
        description: str = "",
        category: FunctionCategory = FunctionCategory.CUSTOM,
        input_types: Optional[List[Type]] = None,
        output_type: Optional[Type] = None,
        parameters: Optional[Dict[str, Any]] = None,
        examples: Optional[List[Dict[str, Any]]] = None,
        tags: Optional[Set[str]] = None,
        version: str = "1.0.0",
        author: Optional[str] = None,
        is_safe: bool = True,
        requires_context: bool = False,
        aliases: Optional[List[str]] = None,
        namespace: Optional[str] = None,
        overwrite: bool = False
    ) -> bool:
        """Register a transformation function.
        
        Args:
            name: Function name (must be unique)
            func: Callable transformation function
            description: Human-readable description
            category: Function category
            input_types: Expected input types
            output_type: Expected output type
            parameters: Additional parameters/configuration
            examples: Usage examples
            tags: Tags for categorization
            version: Function version
            author: Function author
            is_safe: Whether function is safe for sandboxed execution
            requires_context: Whether function requires additional context
            aliases: Alternative names for the function
            namespace: Optional namespace for the function
            overwrite: Whether to overwrite existing function
            
        Returns:
            True if registration successful, False otherwise
        """
        # Validate function name
        if not self._validate_function_name(name):
            logger.error(f"Invalid function name: {name}")
            return False
        
        # Check for existing function
        full_name = f"{namespace}.{name}" if namespace else name
        if full_name in self._functions and not overwrite:
            logger.warning(f"Function already registered: {full_name}")
            return False
        
        # Validate function
        if not callable(func):
            logger.error(f"Provided function is not callable: {name}")
            return False
        
        # Create metadata
        metadata = FunctionMetadata(
            name=full_name,
            description=description,
            category=category,
            input_types=input_types or [],
            output_type=output_type,
            parameters=parameters or {},
            examples=examples or [],
            tags=tags or set(),
            version=version,
            author=author,
            is_safe=is_safe,
            requires_context=requires_context
        )
        
        # Create registered function
        registered_func = RegisteredFunction(func=func, metadata=metadata)
        
        # Validate function signature
        if not registered_func.validate_signature():
            logger.warning(f"Function signature validation failed: {full_name}")
        
        # Register function
        self._functions[full_name] = registered_func
        self._categories[category].add(full_name)
        
        # Register tags
        for tag in metadata.tags:
            self._tags[tag].add(full_name)
        
        # Register namespace
        if namespace:
            self._namespaces[namespace].add(full_name)
        
        # Register aliases
        if aliases:
            for alias in aliases:
                if self._validate_function_name(alias):
                    alias_name = f"{namespace}.{alias}" if namespace else alias
                    self._aliases[alias_name] = full_name
        
        logger.info(f"Registered transformation function: {full_name}",
                   category=category.value,
                   tags=list(metadata.tags),
                   namespace=namespace)
        
        return True
    
    def unregister(self, name: str) -> bool:
        """Unregister a transformation function.
        
        Args:
            name: Function name to unregister
            
        Returns:
            True if unregistration successful, False otherwise
        """
        if name not in self._functions:
            logger.warning(f"Function not found for unregistration: {name}")
            return False
        
        registered_func = self._functions[name]
        metadata = registered_func.metadata
        
        # Remove from main registry
        del self._functions[name]
        
        # Remove from category
        self._categories[metadata.category].discard(name)
        
        # Remove from tags
        for tag in metadata.tags:
            self._tags[tag].discard(name)
        
        # Remove from namespaces
        for namespace, func_names in self._namespaces.items():
            func_names.discard(name)
        
        # Remove aliases
        aliases_to_remove = [alias for alias, func_name in self._aliases.items() if func_name == name]
        for alias in aliases_to_remove:
            del self._aliases[alias]
        
        logger.info(f"Unregistered transformation function: {name}")
        return True
    
    def get_function(self, name: str) -> Optional[RegisteredFunction]:
        """Get a registered function by name or alias.
        
        Args:
            name: Function name or alias
            
        Returns:
            RegisteredFunction if found, None otherwise
        """
        # Check direct name
        if name in self._functions:
            return self._functions[name]
        
        # Check aliases
        if name in self._aliases:
            actual_name = self._aliases[name]
            return self._functions.get(actual_name)
        
        return None
    
    def execute_function(
        self,
        name: str,
        *args,
        context: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Any:
        """Execute a registered transformation function.
        
        Args:
            name: Function name or alias
            *args: Positional arguments for the function
            context: Additional context if function requires it
            **kwargs: Keyword arguments for the function
            
        Returns:
            Function result
            
        Raises:
            TestDataPyException: If function not found or execution fails
        """
        registered_func = self.get_function(name)
        if not registered_func:
            available_functions = ", ".join(self.list_functions())
            raise TestDataPyException(
                message=f"Transformation function not found: {name}",
                suggestions=[
                    f"Available functions: {available_functions}",
                    "Register the function using register()",
                    "Check function name spelling and case"
                ]
            )
        
        try:
            with PerformanceTimer(logger, "function_execution", function_name=name):
                # Add context if function requires it
                if registered_func.metadata.requires_context:
                    if context is None:
                        context = {}
                    kwargs['context'] = context
                
                result = registered_func(*args, **kwargs)
                
                logger.debug(f"Executed transformation function: {name}",
                           input_args=len(args),
                           input_kwargs=len(kwargs),
                           result_type=type(result).__name__)
                
                return result
                
        except Exception as e:
            logger.error(f"Function execution failed: {name}", error=str(e))
            raise TestDataPyException(
                message=f"Transformation function execution failed: {name}",
                details=str(e),
                suggestions=[
                    "Check function arguments and types",
                    "Verify function implementation",
                    "Check function metadata and requirements"
                ],
                original_error=e
            ) from e
    
    def list_functions(
        self,
        category: Optional[FunctionCategory] = None,
        tag: Optional[str] = None,
        namespace: Optional[str] = None,
        safe_only: bool = False
    ) -> List[str]:
        """List registered functions with optional filtering.
        
        Args:
            category: Filter by category
            tag: Filter by tag
            namespace: Filter by namespace
            safe_only: Only include safe functions
            
        Returns:
            List of function names
        """
        functions = set(self._functions.keys())
        
        # Filter by category
        if category:
            functions &= self._categories[category]
        
        # Filter by tag
        if tag:
            functions &= self._tags[tag]
        
        # Filter by namespace
        if namespace:
            functions &= self._namespaces[namespace]
        
        # Filter by safety
        if safe_only:
            functions = {
                name for name in functions
                if self._functions[name].metadata.is_safe
            }
        
        return sorted(list(functions))
    
    def get_function_info(self, name: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a function.
        
        Args:
            name: Function name
            
        Returns:
            Function information dictionary or None if not found
        """
        registered_func = self.get_function(name)
        if not registered_func:
            return None
        
        metadata = registered_func.metadata
        
        # Get function signature
        try:
            sig = inspect.signature(registered_func.func)
            signature = str(sig)
        except Exception:
            signature = "Unable to inspect signature"
        
        return {
            "name": metadata.name,
            "description": metadata.description,
            "category": metadata.category.value,
            "input_types": [t.__name__ for t in metadata.input_types],
            "output_type": metadata.output_type.__name__ if metadata.output_type else None,
            "parameters": metadata.parameters,
            "examples": metadata.examples,
            "tags": list(metadata.tags),
            "version": metadata.version,
            "author": metadata.author,
            "is_safe": metadata.is_safe,
            "requires_context": metadata.requires_context,
            "signature": signature
        }
    
    def search_functions(self, query: str) -> List[str]:
        """Search functions by name or description.
        
        Args:
            query: Search query
            
        Returns:
            List of matching function names
        """
        query_lower = query.lower()
        matches = []
        
        for name, registered_func in self._functions.items():
            metadata = registered_func.metadata
            
            # Search in name
            if query_lower in name.lower():
                matches.append(name)
                continue
            
            # Search in description
            if query_lower in metadata.description.lower():
                matches.append(name)
                continue
            
            # Search in tags
            if any(query_lower in tag.lower() for tag in metadata.tags):
                matches.append(name)
                continue
        
        return sorted(matches)
    
    def get_categories(self) -> Dict[str, List[str]]:
        """Get all categories with their functions.
        
        Returns:
            Dictionary mapping category names to function lists
        """
        return {
            category.value: sorted(list(functions))
            for category, functions in self._categories.items()
            if functions
        }
    
    def get_tags(self) -> Dict[str, List[str]]:
        """Get all tags with their functions.
        
        Returns:
            Dictionary mapping tag names to function lists
        """
        return {
            tag: sorted(list(functions))
            for tag, functions in self._tags.items()
            if functions
        }
    
    def export_registry(self, file_path: Optional[Union[str, Path]] = None) -> Dict[str, Any]:
        """Export registry to dictionary or file.
        
        Args:
            file_path: Optional file path to save registry
            
        Returns:
            Registry data as dictionary
        """
        registry_data = {
            "functions": {},
            "categories": self.get_categories(),
            "tags": self.get_tags(),
            "aliases": dict(self._aliases),
            "metadata": {
                "total_functions": len(self._functions),
                "safe_functions": len([f for f in self._functions.values() if f.metadata.is_safe]),
                "categories_count": len([c for c, funcs in self._categories.items() if funcs])
            }
        }
        
        # Export function details
        for name, registered_func in self._functions.items():
            registry_data["functions"][name] = self.get_function_info(name)
        
        # Save to file if specified
        if file_path:
            file_path = Path(file_path)
            with open(file_path, 'w') as f:
                yaml.dump(registry_data, f, default_flow_style=False, sort_keys=True)
            logger.info(f"Exported function registry to: {file_path}")
        
        return registry_data
    
    def import_registry(
        self,
        source: Union[str, Path, Dict[str, Any]],
        namespace: Optional[str] = None,
        overwrite: bool = False
    ) -> bool:
        """Import registry from file or dictionary.
        
        Args:
            source: File path or dictionary with registry data
            namespace: Optional namespace for imported functions
            overwrite: Whether to overwrite existing functions
            
        Returns:
            True if import successful, False otherwise
        """
        try:
            # Load data
            if isinstance(source, (str, Path)):
                with open(source, 'r') as f:
                    data = yaml.safe_load(f)
            else:
                data = source
            
            # Import functions (would need actual function implementations)
            # This is a placeholder for the structure
            imported_count = 0
            
            logger.info(f"Import registry completed",
                       imported_functions=imported_count,
                       namespace=namespace)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to import registry: {e}")
            return False
    
    def clear_registry(self, confirm: bool = False) -> bool:
        """Clear all registered functions.
        
        Args:
            confirm: Confirmation flag to prevent accidental clearing
            
        Returns:
            True if clearing successful, False otherwise
        """
        if not confirm:
            logger.warning("Registry clear cancelled - confirmation required")
            return False
        
        function_count = len(self._functions)
        
        self._functions.clear()
        self._categories.clear()
        self._tags.clear()
        self._aliases.clear()
        self._namespaces.clear()
        
        logger.info(f"Cleared function registry", cleared_functions=function_count)
        return True
    
    def validate_registry(self) -> Dict[str, Any]:
        """Validate all registered functions.
        
        Returns:
            Validation report
        """
        report = {
            "total_functions": len(self._functions),
            "valid_functions": 0,
            "invalid_functions": 0,
            "warnings": [],
            "errors": []
        }
        
        for name, registered_func in self._functions.items():
            try:
                # Validate signature
                if registered_func.validate_signature():
                    report["valid_functions"] += 1
                else:
                    report["invalid_functions"] += 1
                    report["warnings"].append(f"Invalid signature: {name}")
                
                # Additional validations can be added here
                
            except Exception as e:
                report["invalid_functions"] += 1
                report["errors"].append(f"Validation error for {name}: {e}")
        
        logger.info("Registry validation completed",
                   valid=report["valid_functions"],
                   invalid=report["invalid_functions"])
        
        return report
    
    def _validate_function_name(self, name: str) -> bool:
        """Validate function name format.
        
        Args:
            name: Function name to validate
            
        Returns:
            True if name is valid, False otherwise
        """
        # Function name should be a valid Python identifier
        # Allow alphanumeric characters, underscores, and dots (for namespaces)
        pattern = r'^[a-zA-Z_][a-zA-Z0-9_.]*$'
        return re.match(pattern, name) is not None
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get registry statistics.
        
        Returns:
            Statistics dictionary
        """
        stats = {
            "total_functions": len(self._functions),
            "categories": {cat.value: len(funcs) for cat, funcs in self._categories.items() if funcs},
            "safe_functions": len([f for f in self._functions.values() if f.metadata.is_safe]),
            "functions_with_examples": len([f for f in self._functions.values() if f.metadata.examples]),
            "total_aliases": len(self._aliases),
            "total_tags": len([tag for tag, funcs in self._tags.items() if funcs]),
            "namespaces": {ns: len(funcs) for ns, funcs in self._namespaces.items() if funcs}
        }
        
        return stats


# Global registry instance
global_registry = FunctionRegistry()