"""Dynamic schema loading utility for protobuf classes."""

import sys
import importlib
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Type
import os

logger = logging.getLogger(__name__)


class DynamicSchemaLoader:
    """Handles dynamic loading of protobuf classes from configurable locations."""
    
    def __init__(self):
        """Initialize the schema loader."""
        self._original_sys_path = sys.path.copy()
        self._added_paths: List[str] = []
        
    def load_protobuf_class(
        self, 
        module_name: str, 
        class_name: str, 
        schema_path: Optional[str] = None
    ) -> Type:
        """
        Dynamically load a protobuf class from module in specified path.
        
        Args:
            module_name: Name of the protobuf module (e.g., "events_pb2")
            class_name: Name of the protobuf class (e.g., "VehicleStayCud")
            schema_path: Optional path to directory containing protobuf modules
            
        Returns:
            The protobuf class type
            
        Raises:
            ModuleNotFoundError: If the module cannot be found
            AttributeError: If the class doesn't exist in the module
            ValueError: If schema_path is invalid
        """
        original_path = None
        
        try:
            # Add schema path to sys.path if specified
            if schema_path:
                resolved_path = self._resolve_schema_path(schema_path)
                original_path = self.add_schema_path_to_sys_path(resolved_path)
                logger.debug(f"Added schema path to sys.path: {resolved_path}")
            
            # Import the module
            logger.debug(f"Attempting to import module: {module_name}")
            module = importlib.import_module(module_name)
            
            # Get the class from the module
            if not hasattr(module, class_name):
                available_classes = [attr for attr in dir(module) 
                                   if not attr.startswith('_') and 
                                   hasattr(getattr(module, attr), '__module__')]
                
                raise AttributeError(
                    f"Protobuf class '{class_name}' not found in module '{module_name}'. "
                    f"Available classes: {available_classes[:10]}{'...' if len(available_classes) > 10 else ''}"
                )
            
            proto_class = getattr(module, class_name)
            logger.info(f"Successfully loaded protobuf class: {module_name}.{class_name}")
            
            return proto_class
            
        except ModuleNotFoundError as e:
            error_msg = (
                f"Protobuf module '{module_name}' not found. "
                f"Search paths: {sys.path if not schema_path else [schema_path] + sys.path[:3]}. "
                f"Please ensure the module is compiled and placed in the correct directory. "
                f"To compile: protoc --python_out=. your_schema.proto"
            )
            logger.error(error_msg)
            raise ModuleNotFoundError(error_msg) from e
            
        except Exception as e:
            logger.error(f"Unexpected error loading {module_name}.{class_name}: {e}")
            raise
            
        finally:
            # Clean up sys.path if we modified it
            if original_path is not None:
                self._restore_sys_path(original_path)
    
    def get_protobuf_class_for_entity(
        self, 
        entity_config: Dict[str, Any], 
        entity_type: str
    ) -> Optional[Type]:
        """
        Get protobuf class based on entity configuration with configurable schema path.
        
        Args:
            entity_config: Configuration dictionary for the entity
            entity_type: Type of entity (e.g., 'vehiclestays')
            
        Returns:
            Protobuf class if found, None if should fallback to hardcoded mapping
            
        Raises:
            ValueError: If protobuf configuration is invalid
        """
        # Check if protobuf configuration is provided
        protobuf_module = entity_config.get('protobuf_module')
        protobuf_class = entity_config.get('protobuf_class')
        schema_path = entity_config.get('schema_path')
        
        # If no protobuf config, return None to fallback to hardcoded mapping
        if not protobuf_module and not protobuf_class:
            logger.debug(f"No protobuf configuration for {entity_type}, will try fallback")
            return None
        
        # Validate protobuf configuration
        if protobuf_module and not protobuf_class:
            raise ValueError(
                f"protobuf_module specified for {entity_type} but protobuf_class is missing. "
                f"Both fields are required when using custom protobuf schemas."
            )
        
        if protobuf_class and not protobuf_module:
            raise ValueError(
                f"protobuf_class specified for {entity_type} but protobuf_module is missing. "
                f"Both fields are required when using custom protobuf schemas."
            )
        
        # Validate schema path if provided
        if schema_path:
            try:
                self._resolve_schema_path(schema_path)
            except ValueError as e:
                raise ValueError(f"Invalid schema_path for {entity_type}: {e}")
        
        # Load the protobuf class
        try:
            return self.load_protobuf_class(protobuf_module, protobuf_class, schema_path)
        except Exception as e:
            logger.error(f"Failed to load protobuf class for {entity_type}: {e}")
            raise
    
    def add_schema_path_to_sys_path(self, schema_path: str) -> List[str]:
        """
        Add custom schema directory to Python path for imports.
        
        Args:
            schema_path: Path to schema directory
            
        Returns:
            Original sys.path for restoration
        """
        original_path = sys.path.copy()
        
        if schema_path not in sys.path:
            sys.path.insert(0, schema_path)
            self._added_paths.append(schema_path)
            logger.debug(f"Added to sys.path: {schema_path}")
        
        return original_path
    
    def fallback_to_hardcoded_mapping(self, entity_type: str) -> Optional[Type]:
        """
        Fallback to existing hardcoded mappings for backward compatibility.
        
        Args:
            entity_type: Type of entity (e.g., 'customers')
            
        Returns:
            Protobuf class if found in hardcoded mappings, None otherwise
        """
        # Hardcoded mappings for backward compatibility
        hardcoded_mappings = {
            'customers': ('testdatapy.schemas.protobuf.customer_pb2', 'Customer'),
            'orders': ('testdatapy.schemas.protobuf.order_pb2', 'Order'),
            'payments': ('testdatapy.schemas.protobuf.payment_pb2', 'Payment')
        }
        
        if entity_type in hardcoded_mappings:
            module_name, class_name = hardcoded_mappings[entity_type]
            try:
                # Try to import from the default framework location
                module = importlib.import_module(module_name)
                proto_class = getattr(module, class_name)
                logger.info(f"Using hardcoded mapping for {entity_type}: {module_name}.{class_name}")
                return proto_class
            except (ModuleNotFoundError, AttributeError) as e:
                logger.warning(f"Hardcoded mapping failed for {entity_type}: {e}")
                return None
        
        logger.debug(f"No hardcoded mapping available for entity type: {entity_type}")
        return None
    
    def discover_protobuf_modules(self, schema_path: Optional[str] = None) -> List[str]:
        """
        Discover available protobuf modules in schema directory.
        
        Args:
            schema_path: Optional path to schema directory
            
        Returns:
            List of available protobuf module names
        """
        if schema_path:
            search_path = Path(self._resolve_schema_path(schema_path))
        else:
            # Use default framework schema path
            search_path = Path(__file__).parent / "protobuf"
        
        if not search_path.exists():
            logger.warning(f"Schema directory not found: {search_path}")
            return []
        
        # Find all *_pb2.py files
        protobuf_files = list(search_path.glob("*_pb2.py"))
        module_names = [f.stem for f in protobuf_files]
        
        logger.debug(f"Discovered protobuf modules in {search_path}: {module_names}")
        return module_names
    
    def _resolve_schema_path(self, schema_path: str) -> str:
        """
        Resolve schema path to absolute path.
        
        Args:
            schema_path: Schema path (absolute or relative)
            
        Returns:
            Absolute path to schema directory
            
        Raises:
            ValueError: If path is invalid or inaccessible
        """
        path = Path(schema_path).expanduser()
        
        # Convert to absolute path
        if not path.is_absolute():
            path = Path.cwd() / path
        
        # Resolve any symbolic links and normalize
        try:
            resolved_path = path.resolve()
        except (OSError, RuntimeError) as e:
            raise ValueError(f"Cannot resolve schema path '{schema_path}': {e}")
        
        # Validate path exists and is accessible
        if not resolved_path.exists():
            raise ValueError(f"Schema path does not exist: {resolved_path}")
        
        if not resolved_path.is_dir():
            raise ValueError(f"Schema path is not a directory: {resolved_path}")
        
        # Check if directory is readable
        if not os.access(resolved_path, os.R_OK):
            raise ValueError(f"Schema path is not readable: {resolved_path}")
        
        return str(resolved_path)
    
    def _restore_sys_path(self, original_path: List[str]) -> None:
        """
        Restore sys.path to original state.
        
        Args:
            original_path: Original sys.path to restore
        """
        sys.path[:] = original_path
        self._added_paths.clear()
        logger.debug("Restored sys.path to original state")
    
    def cleanup(self) -> None:
        """Clean up any modifications to sys.path."""
        if self._added_paths:
            for path in self._added_paths:
                if path in sys.path:
                    sys.path.remove(path)
            self._added_paths.clear()
            logger.debug("Cleaned up schema paths from sys.path")


# Global instance for easy access
_schema_loader = DynamicSchemaLoader()


def get_protobuf_class_for_entity(
    entity_config: Dict[str, Any], 
    entity_type: str
) -> Optional[Type]:
    """
    Convenience function to get protobuf class for an entity.
    
    Args:
        entity_config: Configuration dictionary for the entity
        entity_type: Type of entity (e.g., 'vehiclestays')
        
    Returns:
        Protobuf class if found, None if should fallback to hardcoded mapping
    """
    return _schema_loader.get_protobuf_class_for_entity(entity_config, entity_type)


def load_protobuf_class(
    module_name: str, 
    class_name: str, 
    schema_path: Optional[str] = None
) -> Type:
    """
    Convenience function to load a protobuf class.
    
    Args:
        module_name: Name of the protobuf module
        class_name: Name of the protobuf class
        schema_path: Optional path to schema directory
        
    Returns:
        The protobuf class type
    """
    return _schema_loader.load_protobuf_class(module_name, class_name, schema_path)


def fallback_to_hardcoded_mapping(entity_type: str) -> Optional[Type]:
    """
    Convenience function for hardcoded mapping fallback.
    
    Args:
        entity_type: Type of entity
        
    Returns:
        Protobuf class if found in hardcoded mappings, None otherwise
    """
    return _schema_loader.fallback_to_hardcoded_mapping(entity_type)