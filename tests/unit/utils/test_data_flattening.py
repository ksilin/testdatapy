"""Comprehensive tests for data flattening utilities."""
import pytest
from typing import Dict, Any

from testdatapy.utils.data_flattening import (
    DataFlattener, 
    flatten_for_csv, 
    flatten_dict, 
    unflatten_dict
)


class TestDataFlattener:
    """Test the DataFlattener class functionality."""
    
    def test_simple_flat_dictionary(self):
        """Test flattening of already flat dictionary."""
        data = {"name": "John", "age": 30, "active": True}
        
        result = DataFlattener.flatten_dict(data)
        
        expected = {"name": "John", "age": "30", "active": "true"}
        assert result == expected
    
    def test_single_level_nesting(self):
        """Test flattening of single-level nested dictionary."""
        data = {
            "customer": {
                "name": "Alice",
                "age": 25
            },
            "order_id": "12345"
        }
        
        result = DataFlattener.flatten_dict(data)
        
        expected = {
            "customer.name": "Alice",
            "customer.age": "25", 
            "order_id": "12345"
        }
        assert result == expected
    
    def test_multi_level_nesting(self):
        """Test flattening of deeply nested dictionary."""
        data = {
            "level1": {
                "level2": {
                    "level3": {
                        "value": "deep"
                    },
                    "other": "value"
                }
            },
            "top": "level"
        }
        
        result = DataFlattener.flatten_dict(data)
        
        expected = {
            "level1.level2.level3.value": "deep",
            "level1.level2.other": "value",
            "top": "level"
        }
        assert result == expected
    
    def test_vehicle_appointment_structure(self):
        """Test flattening of vehicle appointment structure."""
        data = {
            "jobid": "JOB_000001",
            "branchid": "5fc36c95559ad6001f3998bb",
            "full": {
                "Vehicle": {
                    "cLicenseNr": "M-AB 123",
                    "cLicenseNrCleaned": "MAB123",
                    "cFactoryNr": "WBA12345678901234"
                },
                "Customer": {
                    "cName": "John",
                    "cName2": "Doe"
                }
            }
        }
        
        result = DataFlattener.flatten_dict(data)
        
        expected = {
            "jobid": "JOB_000001",
            "branchid": "5fc36c95559ad6001f3998bb",
            "full.Vehicle.cLicenseNr": "M-AB 123",
            "full.Vehicle.cLicenseNrCleaned": "MAB123",
            "full.Vehicle.cFactoryNr": "WBA12345678901234",
            "full.Customer.cName": "John",
            "full.Customer.cName2": "Doe"
        }
        assert result == expected
    
    def test_custom_separator(self):
        """Test flattening with custom separator."""
        data = {
            "level1": {
                "level2": {
                    "value": "test"
                }
            }
        }
        
        result = DataFlattener.flatten_dict(data, separator="_")
        
        expected = {"level1_level2_value": "test"}
        assert result == expected
    
    def test_prefix_parameter(self):
        """Test flattening with prefix parameter."""
        data = {"inner": {"value": "test"}}
        
        result = DataFlattener.flatten_dict(data, prefix="prefix")
        
        expected = {"prefix.inner.value": "test"}
        assert result == expected


class TestValueConversion:
    """Test value conversion functionality."""
    
    def test_null_value_handling(self):
        """Test handling of None values."""
        data = {"field1": None, "field2": "value"}
        
        # Default null handling
        result = DataFlattener.flatten_dict(data)
        assert result == {"field1": "", "field2": "value"}
        
        # Custom null value
        result = DataFlattener.flatten_dict(data, null_value="NULL")
        assert result == {"field1": "NULL", "field2": "value"}
    
    def test_boolean_format_true_false(self):
        """Test boolean formatting as true/false."""
        data = {"active": True, "deleted": False}
        
        result = DataFlattener.flatten_dict(data, boolean_format="true_false")
        
        expected = {"active": "true", "deleted": "false"}
        assert result == expected
    
    def test_boolean_format_1_0(self):
        """Test boolean formatting as 1/0."""
        data = {"active": True, "deleted": False}
        
        result = DataFlattener.flatten_dict(data, boolean_format="1_0")
        
        expected = {"active": "1", "deleted": "0"}
        assert result == expected
    
    def test_boolean_format_yes_no(self):
        """Test boolean formatting as yes/no."""
        data = {"active": True, "deleted": False}
        
        result = DataFlattener.flatten_dict(data, boolean_format="yes_no")
        
        expected = {"active": "yes", "deleted": "no"}
        assert result == expected
    
    def test_boolean_format_preserve(self):
        """Test preserving boolean values."""
        data = {"active": True, "deleted": False}
        
        result = DataFlattener.flatten_dict(
            data, 
            boolean_format="preserve",
            convert_to_strings=False
        )
        
        expected = {"active": True, "deleted": False}
        assert result == expected
    
    def test_invalid_boolean_format(self):
        """Test error for invalid boolean format."""
        data = {"active": True}
        
        with pytest.raises(ValueError, match="Invalid boolean_format"):
            DataFlattener.flatten_dict(data, boolean_format="invalid")
    
    def test_convert_to_strings_false(self):
        """Test preserving original data types."""
        data = {
            "string": "text",
            "integer": 42,
            "float": 3.14,
            "boolean": True,
            "null": None
        }
        
        result = DataFlattener.flatten_dict(
            data,
            convert_to_strings=False,
            boolean_format="preserve"
        )
        
        expected = {
            "string": "text",
            "integer": 42,
            "float": 3.14,
            "boolean": True,
            "null": None
        }
        assert result == expected


class TestArrayHandling:
    """Test array/list handling in flattening."""
    
    def test_simple_array_flattening(self):
        """Test flattening of simple arrays."""
        data = {
            "items": ["item1", "item2", "item3"],
            "single": "value"
        }
        
        result = DataFlattener.flatten_dict(data)
        
        expected = {
            "items[0]": "item1",
            "items[1]": "item2", 
            "items[2]": "item3",
            "single": "value"
        }
        assert result == expected
    
    def test_nested_array_with_objects(self):
        """Test flattening arrays containing objects."""
        data = {
            "users": [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25}
            ]
        }
        
        result = DataFlattener.flatten_dict(data)
        
        expected = {
            "users[0].name": "Alice",
            "users[0].age": "30",
            "users[1].name": "Bob", 
            "users[1].age": "25"
        }
        assert result == expected
    
    def test_mixed_array_types(self):
        """Test flattening arrays with mixed data types."""
        data = {
            "mixed": [
                "string",
                42,
                {"nested": "object"},
                True,
                None
            ]
        }
        
        result = DataFlattener.flatten_dict(data)
        
        expected = {
            "mixed[0]": "string",
            "mixed[1]": "42",
            "mixed[2].nested": "object",
            "mixed[3]": "true",
            "mixed[4]": ""
        }
        assert result == expected


class TestCSVOptimizedFlattening:
    """Test CSV-optimized flattening functionality."""
    
    def test_flatten_for_csv_basic(self):
        """Test basic CSV flattening with default settings."""
        data = {
            "id": 123,
            "active": True,
            "metadata": {
                "created": "2023-01-01",
                "updated": None
            }
        }
        
        result = DataFlattener.flatten_for_csv(data)
        
        expected = {
            "id": "123",
            "active": "true",
            "metadata.created": "2023-01-01",
            "metadata.updated": ""
        }
        assert result == expected
        
        # Verify all values are strings
        for value in result.values():
            assert isinstance(value, str)
    
    def test_flatten_for_csv_with_arrays(self):
        """Test CSV flattening including arrays."""
        data = {
            "name": "Test",
            "tags": ["tag1", "tag2"]
        }
        
        result = DataFlattener.flatten_for_csv(data, include_arrays=True)
        
        expected = {
            "name": "Test",
            "tags[0]": "tag1",
            "tags[1]": "tag2"
        }
        assert result == expected
    
    def test_flatten_for_csv_exclude_arrays(self):
        """Test CSV flattening excluding arrays."""
        data = {
            "name": "Test",
            "tags": ["tag1", "tag2"],
            "metadata": {
                "items": [1, 2, 3],
                "count": 5
            }
        }
        
        result = DataFlattener.flatten_for_csv(data, include_arrays=False)
        
        expected = {
            "name": "Test",
            "metadata.count": "5"
        }
        assert result == expected


class TestUnflattening:
    """Test reconstruction of nested dictionaries from flattened ones."""
    
    def test_simple_unflatten(self):
        """Test unflattening of simple dot-notation keys."""
        flattened = {
            "name": "John",
            "address.street": "123 Main St",
            "address.city": "Anytown"
        }
        
        result = DataFlattener.unflatten_dict(flattened)
        
        expected = {
            "name": "John",
            "address": {
                "street": "123 Main St",
                "city": "Anytown"
            }
        }
        assert result == expected
    
    def test_deep_unflatten(self):
        """Test unflattening of deeply nested keys."""
        flattened = {
            "level1.level2.level3.value": "deep",
            "level1.level2.other": "value",
            "top": "level"
        }
        
        result = DataFlattener.unflatten_dict(flattened)
        
        expected = {
            "level1": {
                "level2": {
                    "level3": {
                        "value": "deep"
                    },
                    "other": "value"
                }
            },
            "top": "level"
        }
        assert result == expected
    
    def test_unflatten_with_custom_separator(self):
        """Test unflattening with custom separator."""
        flattened = {"level1_level2_value": "test"}
        
        result = DataFlattener.unflatten_dict(flattened, separator="_")
        
        expected = {
            "level1": {
                "level2": {
                    "value": "test"
                }
            }
        }
        assert result == expected
    
    def test_roundtrip_flatten_unflatten(self):
        """Test that flattening and unflattening is reversible."""
        original = {
            "level1": {
                "level2": {
                    "value": "test",
                    "other": 42
                }
            },
            "top": "level"
        }
        
        # Flatten then unflatten
        flattened = DataFlattener.flatten_dict(original, convert_to_strings=False)
        reconstructed = DataFlattener.unflatten_dict(flattened)
        
        assert reconstructed == original


class TestUtilityFunctions:
    """Test utility functions for getting flattened keys."""
    
    def test_get_flattened_keys(self):
        """Test getting list of flattened keys."""
        data = {
            "simple": "value",
            "nested": {
                "field1": "value1",
                "field2": "value2"
            }
        }
        
        keys = DataFlattener.get_flattened_keys(data)
        
        expected_keys = ["simple", "nested.field1", "nested.field2"]
        assert sorted(keys) == sorted(expected_keys)
    
    def test_get_flattened_keys_with_arrays(self):
        """Test getting flattened keys including arrays."""
        data = {
            "items": ["a", "b"],
            "objects": [{"name": "test"}]
        }
        
        keys = DataFlattener.get_flattened_keys(data)
        
        expected_keys = ["items[0]", "items[1]", "objects[0].name"]
        assert sorted(keys) == sorted(expected_keys)


class TestConvenienceFunctions:
    """Test standalone convenience functions."""
    
    def test_flatten_dict_convenience(self):
        """Test flatten_dict convenience function."""
        data = {"nested": {"value": "test"}}
        
        result = flatten_dict(data)
        
        expected = {"nested.value": "test"}
        assert result == expected
    
    def test_flatten_for_csv_convenience(self):
        """Test flatten_for_csv convenience function."""
        data = {"number": 42, "bool": True}
        
        result = flatten_for_csv(data)
        
        expected = {"number": "42", "bool": "true"}
        assert result == expected
        
        # Verify all values are strings
        for value in result.values():
            assert isinstance(value, str)
    
    def test_unflatten_dict_convenience(self):
        """Test unflatten_dict convenience function."""
        flattened = {"nested.value": "test"}
        
        result = unflatten_dict(flattened)
        
        expected = {"nested": {"value": "test"}}
        assert result == expected


class TestEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_empty_dictionary(self):
        """Test flattening empty dictionary."""
        result = DataFlattener.flatten_dict({})
        assert result == {}
    
    def test_empty_nested_dictionary(self):
        """Test flattening dictionary with empty nested objects."""
        data = {"empty": {}, "value": "test"}
        
        result = DataFlattener.flatten_dict(data)
        
        expected = {"value": "test"}
        assert result == expected
    
    def test_empty_arrays(self):
        """Test flattening with empty arrays."""
        data = {"empty_array": [], "value": "test"}
        
        result = DataFlattener.flatten_dict(data)
        
        expected = {"value": "test"}
        assert result == expected
    
    def test_special_characters_in_keys(self):
        """Test flattening with special characters in keys."""
        data = {
            "key-with-dash": "value1",
            "key_with_underscore": "value2",
            "nested": {
                "key.with.dots": "value3"
            }
        }
        
        result = DataFlattener.flatten_dict(data)
        
        expected = {
            "key-with-dash": "value1",
            "key_with_underscore": "value2",
            "nested.key.with.dots": "value3"
        }
        assert result == expected
    
    def test_numeric_string_keys(self):
        """Test flattening with numeric string keys."""
        data = {
            "123": "value1",
            "nested": {
                "456": "value2"
            }
        }
        
        result = DataFlattener.flatten_dict(data)
        
        expected = {
            "123": "value1",
            "nested.456": "value2"
        }
        assert result == expected