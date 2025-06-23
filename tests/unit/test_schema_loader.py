"""Unit tests for dynamic schema loading functionality."""
import unittest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import os
from pathlib import Path

from testdatapy.schemas.schema_loader import (
    DynamicSchemaLoader, 
    get_protobuf_class_for_entity,
    load_protobuf_class,
    fallback_to_hardcoded_mapping
)


class TestDynamicSchemaLoader(unittest.TestCase):
    """Test cases for DynamicSchemaLoader class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.loader = DynamicSchemaLoader()
        
    def tearDown(self):
        """Clean up after tests."""
        self.loader.cleanup()
    
    @patch('testdatapy.schemas.schema_loader.importlib.import_module')
    def test_load_protobuf_class_success(self, mock_import):
        """Test successful protobuf class loading."""
        # Mock the module and class
        mock_module = Mock()
        mock_class = Mock()
        mock_class.__module__ = "test_module"
        mock_module.TestClass = mock_class
        mock_import.return_value = mock_module
        
        # Test loading
        result = self.loader.load_protobuf_class("test_module", "TestClass")
        
        # Verify
        self.assertEqual(result, mock_class)
        mock_import.assert_called_once_with("test_module")
    
    @patch('testdatapy.schemas.schema_loader.importlib.import_module')
    def test_load_protobuf_class_module_not_found(self, mock_import):
        """Test handling of missing protobuf module."""
        mock_import.side_effect = ModuleNotFoundError("No module named 'missing_module'")
        
        with self.assertRaises(ModuleNotFoundError) as context:
            self.loader.load_protobuf_class("missing_module", "TestClass")
        
        self.assertIn("missing_module", str(context.exception))
        self.assertIn("protoc --python_out", str(context.exception))
    
    @patch('testdatapy.schemas.schema_loader.importlib.import_module')
    def test_load_protobuf_class_class_not_found(self, mock_import):
        """Test handling of missing protobuf class."""
        # Mock the module without the requested class
        mock_module = Mock()
        mock_module.OtherClass = Mock()
        del mock_module.TestClass  # Ensure TestClass doesn't exist
        mock_import.return_value = mock_module
        
        # Mock dir() to return available attributes
        with patch('builtins.dir', return_value=['OtherClass', '__module__']):
            with patch('builtins.hasattr', side_effect=lambda obj, attr: attr == 'OtherClass'):
                with patch('builtins.getattr', side_effect=lambda obj, attr: mock_module.OtherClass if attr == 'OtherClass' else None):
                    with self.assertRaises(AttributeError) as context:
                        self.loader.load_protobuf_class("test_module", "TestClass")
        
        self.assertIn("TestClass", str(context.exception))
        self.assertIn("not found in module", str(context.exception))
    
    def test_get_protobuf_class_for_entity_with_config(self):
        """Test getting protobuf class with configuration."""
        entity_config = {
            'protobuf_module': 'test_module',
            'protobuf_class': 'TestClass'
        }
        
        # Mock the load_protobuf_class method
        mock_class = Mock()
        with patch.object(self.loader, 'load_protobuf_class', return_value=mock_class):
            result = self.loader.get_protobuf_class_for_entity(entity_config, "test_entity")
        
        self.assertEqual(result, mock_class)
    
    def test_get_protobuf_class_for_entity_no_config(self):
        """Test getting protobuf class without configuration."""
        entity_config = {}
        
        result = self.loader.get_protobuf_class_for_entity(entity_config, "test_entity")
        
        self.assertIsNone(result)
    
    def test_get_protobuf_class_for_entity_incomplete_config(self):
        """Test error handling for incomplete protobuf configuration."""
        # Missing protobuf_class
        entity_config = {
            'protobuf_module': 'test_module'
        }
        
        with self.assertRaises(ValueError) as context:
            self.loader.get_protobuf_class_for_entity(entity_config, "test_entity")
        
        self.assertIn("protobuf_module specified", str(context.exception))
        self.assertIn("protobuf_class is missing", str(context.exception))
    
    def test_resolve_schema_path_absolute(self):
        """Test resolving absolute schema path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self.loader._resolve_schema_path(temp_dir)
            self.assertEqual(result, temp_dir)
    
    def test_resolve_schema_path_relative(self):
        """Test resolving relative schema path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a subdirectory
            subdir = Path(temp_dir) / "schemas"
            subdir.mkdir()
            
            # Change to temp_dir and test relative path
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                result = self.loader._resolve_schema_path("schemas")
                self.assertEqual(result, str(subdir))
            finally:
                os.chdir(original_cwd)
    
    def test_resolve_schema_path_not_exists(self):
        """Test error handling for non-existent schema path."""
        with self.assertRaises(ValueError) as context:
            self.loader._resolve_schema_path("/nonexistent/path")
        
        self.assertIn("does not exist", str(context.exception))
    
    def test_resolve_schema_path_not_directory(self):
        """Test error handling for schema path that is not a directory."""
        with tempfile.NamedTemporaryFile() as temp_file:
            with self.assertRaises(ValueError) as context:
                self.loader._resolve_schema_path(temp_file.name)
            
            self.assertIn("not a directory", str(context.exception))
    
    @patch('testdatapy.schemas.schema_loader.importlib.import_module')
    def test_fallback_to_hardcoded_mapping_customers(self, mock_import):
        """Test fallback to hardcoded mapping for customers."""
        # Mock the customer module
        mock_module = Mock()
        mock_class = Mock()
        mock_module.Customer = mock_class
        mock_import.return_value = mock_module
        
        result = self.loader.fallback_to_hardcoded_mapping("customers")
        
        self.assertEqual(result, mock_class)
        mock_import.assert_called_once_with("testdatapy.schemas.protobuf.customer_pb2")
    
    def test_fallback_to_hardcoded_mapping_unknown(self):
        """Test fallback for unknown entity type."""
        result = self.loader.fallback_to_hardcoded_mapping("unknown_entity")
        
        self.assertIsNone(result)
    
    def test_discover_protobuf_modules_with_path(self):
        """Test discovering protobuf modules in custom path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create some mock protobuf files
            test_files = ["test_pb2.py", "another_pb2.py", "regular_file.py"]
            for filename in test_files:
                (Path(temp_dir) / filename).touch()
            
            result = self.loader.discover_protobuf_modules(temp_dir)
            
            # Should only return the *_pb2.py files (without extension)
            expected = ["test_pb2", "another_pb2"]
            self.assertEqual(sorted(result), sorted(expected))
    
    def test_discover_protobuf_modules_nonexistent_path(self):
        """Test discovering protobuf modules in non-existent path."""
        result = self.loader.discover_protobuf_modules("/nonexistent/path")
        
        self.assertEqual(result, [])
    
    def test_add_schema_path_to_sys_path(self):
        """Test adding schema path to sys.path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            import sys
            original_path = sys.path.copy()
            
            try:
                original_sys_path = self.loader.add_schema_path_to_sys_path(temp_dir)
                
                # Verify path was added
                self.assertIn(temp_dir, sys.path)
                self.assertEqual(sys.path[0], temp_dir)
                
                # Verify original path is returned
                self.assertEqual(original_sys_path, original_path)
                
            finally:
                # Clean up
                self.loader._restore_sys_path(original_path)


class TestConvenienceFunctions(unittest.TestCase):
    """Test convenience functions for schema loading."""
    
    @patch('testdatapy.schemas.schema_loader._schema_loader')
    def test_get_protobuf_class_for_entity(self, mock_loader):
        """Test convenience function for getting protobuf class."""
        mock_class = Mock()
        mock_loader.get_protobuf_class_for_entity.return_value = mock_class
        
        entity_config = {'protobuf_module': 'test', 'protobuf_class': 'Test'}
        result = get_protobuf_class_for_entity(entity_config, "test_entity")
        
        self.assertEqual(result, mock_class)
        mock_loader.get_protobuf_class_for_entity.assert_called_once_with(entity_config, "test_entity")
    
    @patch('testdatapy.schemas.schema_loader._schema_loader')
    def test_load_protobuf_class(self, mock_loader):
        """Test convenience function for loading protobuf class."""
        mock_class = Mock()
        mock_loader.load_protobuf_class.return_value = mock_class
        
        result = load_protobuf_class("test_module", "TestClass", "/path/to/schemas")
        
        self.assertEqual(result, mock_class)
        mock_loader.load_protobuf_class.assert_called_once_with("test_module", "TestClass", "/path/to/schemas")
    
    @patch('testdatapy.schemas.schema_loader._schema_loader')
    def test_fallback_to_hardcoded_mapping(self, mock_loader):
        """Test convenience function for hardcoded mapping fallback."""
        mock_class = Mock()
        mock_loader.fallback_to_hardcoded_mapping.return_value = mock_class
        
        result = fallback_to_hardcoded_mapping("customers")
        
        self.assertEqual(result, mock_class)
        mock_loader.fallback_to_hardcoded_mapping.assert_called_once_with("customers")


if __name__ == '__main__':
    unittest.main()