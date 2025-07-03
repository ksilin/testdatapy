"""Base transformer class for data transformations."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Union


class DataTransformer(ABC):
    """Abstract base class for data transformers.
    
    Provides the interface for transforming data from one format to another,
    with support for configuration-driven transformations.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the transformer.
        
        Args:
            config: Optional configuration dictionary for transformation rules
        """
        self.config = config or {}
        self._transformation_cache = {}
    
    @abstractmethod
    def transform(self, data: Dict[str, Any], target_schema: Any = None) -> Any:
        """Transform data according to the configured rules.
        
        Args:
            data: Input data dictionary
            target_schema: Target schema or message class
            
        Returns:
            Transformed data in the target format
        """
        pass
    
    @abstractmethod
    def validate_config(self) -> bool:
        """Validate the transformation configuration.
        
        Returns:
            True if configuration is valid, False otherwise
        """
        pass
    
    def clear_cache(self) -> None:
        """Clear the transformation cache."""
        self._transformation_cache.clear()
    
    def get_supported_types(self) -> list[str]:
        """Get list of supported transformation types.
        
        Returns:
            List of supported type names
        """
        return []