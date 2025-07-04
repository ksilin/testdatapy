"""Data flattening utilities for handling nested structures.

This module provides utilities for flattening nested dictionaries and objects
into flat structures with dot-notation keys, commonly used for CSV export
and data processing.
"""
from typing import Dict, Any, Union, List, Optional


class DataFlattener:
    """Utility class for flattening nested data structures."""
    
    @staticmethod
    def flatten_dict(
        data: Dict[str, Any], 
        prefix: str = "", 
        separator: str = ".",
        convert_to_strings: bool = True,
        null_value: str = "",
        boolean_format: str = "true_false"
    ) -> Dict[str, Any]:
        """Flatten a nested dictionary into a single-level dictionary with dot-notation keys.
        
        Args:
            data: Dictionary with potentially nested objects
            prefix: Current field prefix for nested fields
            separator: Separator character for nested keys (default: ".")
            convert_to_strings: Whether to convert all values to strings (default: True)
            null_value: String representation for None values (default: "")
            boolean_format: Format for boolean values. Options:
                - "true_false": Convert to "true"/"false" strings
                - "1_0": Convert to "1"/"0" strings
                - "yes_no": Convert to "yes"/"no" strings
                - "preserve": Keep as boolean (only if convert_to_strings=False)
        
        Returns:
            Flattened dictionary with dot-notation field names
            
        Raises:
            ValueError: If boolean_format is invalid
        """
        if boolean_format not in ["true_false", "1_0", "yes_no", "preserve"]:
            raise ValueError(f"Invalid boolean_format: {boolean_format}")
        
        flattened = {}
        
        for key, value in data.items():
            full_key = f"{prefix}{separator}{key}" if prefix else key
            
            if isinstance(value, dict):
                # Recursively flatten nested dictionaries
                nested_flattened = DataFlattener.flatten_dict(
                    value, full_key, separator, convert_to_strings, 
                    null_value, boolean_format
                )
                flattened.update(nested_flattened)
            elif isinstance(value, list):
                # Handle lists by indexing each element
                for i, item in enumerate(value):
                    list_key = f"{full_key}[{i}]"
                    if isinstance(item, dict):
                        nested_flattened = DataFlattener.flatten_dict(
                            item, list_key, separator, convert_to_strings,
                            null_value, boolean_format
                        )
                        flattened.update(nested_flattened)
                    else:
                        flattened[list_key] = DataFlattener._convert_value(
                            item, convert_to_strings, null_value, boolean_format
                        )
            else:
                # Handle primitive values
                flattened[full_key] = DataFlattener._convert_value(
                    value, convert_to_strings, null_value, boolean_format
                )
        
        return flattened
    
    @staticmethod
    def _convert_value(
        value: Any, 
        convert_to_strings: bool, 
        null_value: str, 
        boolean_format: str
    ) -> Any:
        """Convert a value according to specified formatting rules.
        
        Args:
            value: Value to convert
            convert_to_strings: Whether to convert all values to strings
            null_value: String representation for None values
            boolean_format: Format for boolean values
            
        Returns:
            Converted value
        """
        if value is None:
            return null_value if convert_to_strings else None
        
        if isinstance(value, bool):
            if boolean_format == "preserve" and not convert_to_strings:
                return value
            elif boolean_format == "true_false":
                return "true" if value else "false"
            elif boolean_format == "1_0":
                return "1" if value else "0"
            elif boolean_format == "yes_no":
                return "yes" if value else "no"
        
        if convert_to_strings:
            return str(value)
        
        return value
    
    @staticmethod
    def flatten_for_csv(
        data: Dict[str, Any], 
        include_arrays: bool = True,
        separator: str = "."
    ) -> Dict[str, str]:
        """Flatten a dictionary specifically for CSV export.
        
        This is a convenience method optimized for CSV output with
        sensible defaults for CSV compatibility.
        
        Args:
            data: Dictionary to flatten
            include_arrays: Whether to include array elements with [index] notation
            separator: Separator for nested keys (default: ".")
            
        Returns:
            Flattened dictionary with all string values suitable for CSV
        """
        if not include_arrays:
            # Remove arrays from data before flattening
            filtered_data = DataFlattener._filter_arrays(data)
        else:
            filtered_data = data
        
        return DataFlattener.flatten_dict(
            filtered_data,
            convert_to_strings=True,
            null_value="",
            boolean_format="true_false",
            separator=separator
        )
    
    @staticmethod
    def _filter_arrays(data: Dict[str, Any]) -> Dict[str, Any]:
        """Remove arrays from data structure recursively.
        
        Args:
            data: Dictionary that may contain arrays
            
        Returns:
            Dictionary with arrays removed
        """
        filtered = {}
        for key, value in data.items():
            if isinstance(value, list):
                continue  # Skip arrays
            elif isinstance(value, dict):
                filtered[key] = DataFlattener._filter_arrays(value)
            else:
                filtered[key] = value
        return filtered
    
    @staticmethod
    def unflatten_dict(
        flattened_data: Dict[str, Any], 
        separator: str = "."
    ) -> Dict[str, Any]:
        """Reconstruct a nested dictionary from a flattened one.
        
        Args:
            flattened_data: Dictionary with dot-notation keys
            separator: Separator used in the flattened keys
            
        Returns:
            Nested dictionary structure
        """
        result = {}
        
        for key, value in flattened_data.items():
            # Split the key into parts
            parts = key.split(separator)
            
            # Navigate/create the nested structure
            current = result
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
            
            # Set the final value
            current[parts[-1]] = value
        
        return result
    
    @staticmethod
    def get_flattened_keys(data: Dict[str, Any], separator: str = ".") -> List[str]:
        """Get all the keys that would be generated by flattening a dictionary.
        
        Useful for generating CSV headers before flattening data.
        
        Args:
            data: Dictionary to analyze
            separator: Separator for nested keys
            
        Returns:
            List of flattened key names
        """
        flattened = DataFlattener.flatten_dict(
            data, convert_to_strings=False, separator=separator
        )
        return list(flattened.keys())


# Convenience functions for common use cases
def flatten_for_csv(data: Dict[str, Any], separator: str = ".") -> Dict[str, str]:
    """Convenience function to flatten data for CSV export.
    
    Args:
        data: Dictionary to flatten
        separator: Separator for nested keys
        
    Returns:
        Flattened dictionary with string values
    """
    return DataFlattener.flatten_for_csv(data, separator=separator)


def flatten_dict(data: Dict[str, Any], separator: str = ".") -> Dict[str, Any]:
    """Convenience function to flatten a dictionary with default settings.
    
    Args:
        data: Dictionary to flatten  
        separator: Separator for nested keys
        
    Returns:
        Flattened dictionary
    """
    return DataFlattener.flatten_dict(data, separator=separator)


def unflatten_dict(flattened_data: Dict[str, Any], separator: str = ".") -> Dict[str, Any]:
    """Convenience function to unflatten a dictionary.
    
    Args:
        flattened_data: Dictionary with dot-notation keys
        separator: Separator used in keys
        
    Returns:
        Nested dictionary
    """
    return DataFlattener.unflatten_dict(flattened_data, separator=separator)