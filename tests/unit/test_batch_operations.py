"""Unit tests for batch operations.

This module tests batch compilation, registration, and validation functionality.
"""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import pytest

from testdatapy.batch_operations import (
    BatchCompiler, BatchRegistryManager, BatchValidator,
    BatchItem, BatchResult, BatchStatus, ValidationLevel,
    RegistryItem, RegistryResult, ValidationItem, ValidationResult
)


class TestBatchCompiler(unittest.TestCase):
    """Test BatchCompiler functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.compiler = BatchCompiler(max_workers=2, use_cache=False)
        
        # Create temporary test files
        self.temp_dir = Path(tempfile.mkdtemp())
        self.test_schema1 = self.temp_dir / "test1.proto"
        self.test_schema2 = self.temp_dir / "test2.proto"
        
        # Write test schema content
        self.test_schema1.write_text('syntax = "proto3"; message Test1 { string name = 1; }')
        self.test_schema2.write_text('syntax = "proto3"; message Test2 { int32 value = 1; }')
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_batch_item_creation(self):
        """Test BatchItem creation and methods."""
        item = BatchItem(
            id="test_1",
            schema_path="/path/to/schema.proto",
            schema_type="protobuf",
            status=BatchStatus.PENDING
        )
        
        self.assertEqual(item.id, "test_1")
        self.assertEqual(item.schema_path, "/path/to/schema.proto")
        self.assertEqual(item.status, BatchStatus.PENDING)
        
        # Test to_dict conversion
        item_dict = item.to_dict()
        self.assertEqual(item_dict["status"], "pending")
        self.assertIn("schema_path", item_dict)
    
    def test_batch_result_properties(self):
        """Test BatchResult properties and methods."""
        items = [
            BatchItem("1", "schema1.proto", "protobuf", BatchStatus.COMPLETED),
            BatchItem("2", "schema2.proto", "protobuf", BatchStatus.FAILED),
            BatchItem("3", "schema3.proto", "protobuf", BatchStatus.COMPLETED),
        ]
        
        result = BatchResult(
            batch_id="test_batch",
            operation_type="compile",
            total_items=3,
            successful_items=2,
            failed_items=1,
            cancelled_items=0,
            total_duration=5.5,
            items=items,
            metadata={}
        )
        
        self.assertEqual(result.success_rate, 2/3)
        self.assertEqual(len(result.get_successful_items()), 2)
        self.assertEqual(len(result.get_failed_items()), 1)
    
    def test_discover_schemas(self):
        """Test schema discovery functionality."""
        # Test discovery in temp directory
        discovered = self.compiler.discover_schemas(
            directory=self.temp_dir,
            pattern="*.proto",
            recursive=False
        )
        
        self.assertEqual(len(discovered), 2)
        self.assertIn(str(self.test_schema1), discovered)
        self.assertIn(str(self.test_schema2), discovered)
    
    def test_discover_schemas_recursive(self):
        """Test recursive schema discovery."""
        # Create subdirectory with schema
        subdir = self.temp_dir / "subdir"
        subdir.mkdir()
        sub_schema = subdir / "sub.proto"
        sub_schema.write_text('syntax = "proto3"; message Sub { }')
        
        # Test non-recursive discovery
        non_recursive = self.compiler.discover_schemas(
            directory=self.temp_dir,
            pattern="*.proto",
            recursive=False
        )
        self.assertEqual(len(non_recursive), 2)  # Only top-level files
        
        # Test recursive discovery
        recursive = self.compiler.discover_schemas(
            directory=self.temp_dir,
            pattern="*.proto",
            recursive=True
        )
        self.assertEqual(len(recursive), 3)  # Include subdirectory file
    
    @patch('testdatapy.batch_operations.batch_compiler.ProtobufCompiler')
    def test_compile_batch_sequential(self, mock_compiler_class):
        """Test sequential batch compilation."""
        # Mock compiler
        mock_compiler = Mock()
        mock_compiler.compile_schema.return_value = "/output/compiled.py"
        mock_compiler_class.return_value = mock_compiler
        
        # Test compilation
        result = self.compiler.compile_batch(
            schema_paths=[str(self.test_schema1), str(self.test_schema2)],
            parallel=False
        )
        
        self.assertEqual(result.total_items, 2)
        self.assertEqual(result.operation_type, "compile")
        self.assertGreater(result.total_duration, 0)
        
        # Verify compiler was called
        self.assertEqual(mock_compiler.compile_schema.call_count, 2)
    
    @patch('testdatapy.batch_operations.batch_compiler.ProtobufCompiler')
    def test_compile_batch_parallel(self, mock_compiler_class):
        """Test parallel batch compilation."""
        # Mock compiler
        mock_compiler = Mock()
        mock_compiler.compile_schema.return_value = "/output/compiled.py"
        mock_compiler_class.return_value = mock_compiler
        
        # Test compilation
        result = self.compiler.compile_batch(
            schema_paths=[str(self.test_schema1), str(self.test_schema2)],
            parallel=True
        )
        
        self.assertEqual(result.total_items, 2)
        self.assertEqual(result.operation_type, "compile")
        
        # Verify compiler was called
        self.assertEqual(mock_compiler.compile_schema.call_count, 2)
    
    @patch('testdatapy.batch_operations.batch_compiler.ProtobufCompiler')
    def test_compile_batch_with_errors(self, mock_compiler_class):
        """Test batch compilation with errors."""
        # Mock compiler with errors
        mock_compiler = Mock()
        mock_compiler.compile_schema.side_effect = [
            "/output/compiled1.py",  # Success
            Exception("Compilation failed")  # Error
        ]
        mock_compiler_class.return_value = mock_compiler
        
        # Test compilation
        result = self.compiler.compile_batch(
            schema_paths=[str(self.test_schema1), str(self.test_schema2)],
            parallel=False
        )
        
        self.assertEqual(result.total_items, 2)
        self.assertEqual(result.successful_items, 1)
        self.assertEqual(result.failed_items, 1)
        
        # Check specific item statuses
        items = result.items
        self.assertEqual(items[0].status, BatchStatus.COMPLETED)
        self.assertEqual(items[1].status, BatchStatus.FAILED)
        self.assertIn("Compilation failed", items[1].error)
    
    def test_get_active_batches(self):
        """Test tracking of active batches."""
        # Initially no active batches
        active = self.compiler.get_active_batches()
        self.assertEqual(len(active), 0)
        
        # Note: In a real test, we'd need to mock the compilation to keep batches active
        # This is a basic structure test
    
    def test_cancel_batch(self):
        """Test batch cancellation."""
        # Test cancelling non-existent batch
        result = self.compiler.cancel_batch("non_existent")
        self.assertFalse(result)
    
    def test_get_compilation_stats(self):
        """Test compilation statistics."""
        stats = self.compiler.get_compilation_stats()
        
        self.assertIn("max_workers", stats)
        self.assertIn("use_cache", stats)
        self.assertIn("active_batches", stats)
        self.assertEqual(stats["max_workers"], 2)
        self.assertEqual(stats["use_cache"], False)


class TestBatchRegistryManager(unittest.TestCase):
    """Test BatchRegistryManager functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.registry_manager = BatchRegistryManager(
            schema_registry_url="http://localhost:8081",
            max_workers=2
        )
        
        # Create temporary test files
        self.temp_dir = Path(tempfile.mkdtemp())
        self.test_schema1 = self.temp_dir / "test1.proto"
        self.test_schema2 = self.temp_dir / "test2.proto"
        
        # Write test schema content
        self.test_schema1.write_text('syntax = "proto3"; message Test1 { string name = 1; }')
        self.test_schema2.write_text('syntax = "proto3"; message Test2 { int32 value = 1; }')
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_registry_item_creation(self):
        """Test RegistryItem creation."""
        from testdatapy.batch_operations.batch_registry import RegistryOperation
        
        item = RegistryItem(
            id="reg_1",
            schema_path="/path/to/schema.proto",
            schema_type="protobuf",
            status=BatchStatus.PENDING,
            operation=RegistryOperation.REGISTER,
            subject="test-subject",
            schema_content="test content"
        )
        
        self.assertEqual(item.subject, "test-subject")
        self.assertEqual(item.schema_content, "test content")
        
        # Test to_dict conversion
        item_dict = item.to_dict()
        self.assertEqual(item_dict["operation"], "register")
        self.assertEqual(item_dict["subject"], "test-subject")
    
    @patch('testdatapy.batch_operations.batch_registry.SchemaRegistryManager')
    def test_register_schemas_batch(self, mock_registry_class):
        """Test batch schema registration."""
        # Mock registry manager
        mock_registry = Mock()
        mock_registry.register_schema.return_value = {"id": 123, "version": 1}
        mock_registry_class.return_value = mock_registry
        
        # Test registration
        result = self.registry_manager.register_schemas_batch(
            schema_files=[str(self.test_schema1), str(self.test_schema2)],
            subject_template="{schema_name}-value",
            parallel=False
        )
        
        self.assertEqual(result.total_items, 2)
        self.assertEqual(result.operation_type, "register")
        
        # Verify registry calls
        self.assertEqual(mock_registry.register_schema.call_count, 2)
    
    @patch('testdatapy.batch_operations.batch_registry.SchemaRegistryManager')
    def test_check_compatibility_batch(self, mock_registry_class):
        """Test batch compatibility checking."""
        # Mock registry manager
        mock_registry = Mock()
        mock_registry.check_compatibility.return_value = {
            "is_compatible": True,
            "message": "Compatible"
        }
        mock_registry_class.return_value = mock_registry
        
        # Test compatibility check
        result = self.registry_manager.check_compatibility_batch(
            schema_files=[str(self.test_schema1), str(self.test_schema2)],
            subject_template="{schema_name}-value",
            parallel=False
        )
        
        self.assertEqual(result.total_items, 2)
        self.assertEqual(result.operation_type, "check_compatibility")
        
        # Verify registry calls
        self.assertEqual(mock_registry.check_compatibility.call_count, 2)
    
    def test_get_registry_stats(self):
        """Test registry statistics."""
        stats = self.registry_manager.get_registry_stats()
        
        self.assertIn("max_workers", stats)
        self.assertIn("active_batches", stats)
        self.assertEqual(stats["max_workers"], 2)


class TestBatchValidator(unittest.TestCase):
    """Test BatchValidator functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.validator = BatchValidator(max_workers=2)
        
        # Create temporary test files
        self.temp_dir = Path(tempfile.mkdtemp())
        self.test_schema1 = self.temp_dir / "test1.proto"
        self.test_schema2 = self.temp_dir / "test2.proto"
        self.test_config = self.temp_dir / "config.json"
        
        # Write test content
        self.test_schema1.write_text('syntax = "proto3"; message Test1 { string name = 1; }')
        self.test_schema2.write_text('syntax = "proto3"; message Test2 { int32 value = 1; }')
        self.test_config.write_text('{"setting": "value"}')
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_validation_item_creation(self):
        """Test ValidationItem creation and properties."""
        from testdatapy.batch_operations.batch_validator import ValidationType, ValidationIssue
        
        issues = [
            ValidationIssue(severity="error", type="syntax", message="Syntax error"),
            ValidationIssue(severity="warning", type="naming", message="Naming issue")
        ]
        
        item = ValidationItem(
            id="val_1",
            schema_path="/path/to/schema.proto",
            schema_type="protobuf",
            status=BatchStatus.COMPLETED,
            validation_type=ValidationType.SCHEMA_COMPILATION,
            validation_level=ValidationLevel.COMPREHENSIVE,
            issues=issues
        )
        
        self.assertTrue(item.has_errors)
        self.assertTrue(item.has_warnings)
        self.assertEqual(item.get_error_count(), 1)
        self.assertEqual(item.get_warning_count(), 1)
        
        # Test to_dict conversion
        item_dict = item.to_dict()
        self.assertEqual(item_dict["validation_type"], "schema_compilation")
        self.assertEqual(item_dict["has_errors"], True)
        self.assertEqual(item_dict["error_count"], 1)
    
    def test_validation_result_properties(self):
        """Test ValidationResult properties."""
        from testdatapy.batch_operations.batch_validator import ValidationType, ValidationIssue
        
        # Create items with different validation states
        items = [
            ValidationItem(
                id="1", schema_path="schema1.proto", schema_type="protobuf",
                status=BatchStatus.COMPLETED, validation_type=ValidationType.SCHEMA_SYNTAX,
                validation_level=ValidationLevel.BASIC, issues=[]
            ),
            ValidationItem(
                id="2", schema_path="schema2.proto", schema_type="protobuf",
                status=BatchStatus.COMPLETED, validation_type=ValidationType.SCHEMA_SYNTAX,
                validation_level=ValidationLevel.BASIC, 
                issues=[ValidationIssue(severity="warning", type="naming", message="Warning")]
            ),
            ValidationItem(
                id="3", schema_path="schema3.proto", schema_type="protobuf",
                status=BatchStatus.FAILED, validation_type=ValidationType.SCHEMA_SYNTAX,
                validation_level=ValidationLevel.BASIC,
                issues=[ValidationIssue(severity="error", type="syntax", message="Error")]
            )
        ]
        
        result = ValidationResult(
            batch_id="test_validation",
            operation_type="validate_schemas",
            validation_level=ValidationLevel.BASIC,
            total_items=3,
            successful_items=1,
            failed_items=2,
            cancelled_items=0,
            total_errors=1,
            total_warnings=1,
            total_duration=2.5,
            items=items,
            metadata={}
        )
        
        self.assertEqual(result.success_rate, 1/3)
        self.assertEqual(result.overall_status, "failed")  # Has errors
        self.assertEqual(len(result.get_successful_items()), 1)
        self.assertEqual(len(result.get_failed_items()), 2)
        self.assertEqual(len(result.get_items_with_warnings()), 1)
    
    @patch('testdatapy.batch_operations.batch_validator.ProtobufCompiler')
    @patch('testdatapy.batch_operations.batch_validator.ProtobufConfigValidator')
    def test_validate_schemas_batch(self, mock_validator_class, mock_compiler_class):
        """Test batch schema validation."""
        # Mock components
        mock_validator = Mock()
        mock_compiler = Mock()
        mock_validator_class.return_value = mock_validator
        mock_compiler_class.return_value = mock_compiler
        
        # Mock successful compilation
        mock_compiler.compile_schema.return_value = "/output/compiled.py"
        
        # Test validation
        result = self.validator.validate_schemas_batch(
            schema_files=[str(self.test_schema1), str(self.test_schema2)],
            validation_level=ValidationLevel.BASIC,
            parallel=False
        )
        
        self.assertEqual(result.total_items, 2)
        self.assertEqual(result.operation_type, "validate_schemas")
        self.assertEqual(result.validation_level, ValidationLevel.BASIC)
    
    @patch('testdatapy.batch_operations.batch_validator.ProtobufConfigValidator')
    def test_validate_configurations_batch(self, mock_validator_class):
        """Test batch configuration validation."""
        # Mock config validator
        mock_validator = Mock()
        mock_validator.validate_config_file.return_value = {
            "issues": [
                {"severity": "warning", "type": "deprecated", "message": "Setting deprecated"}
            ]
        }
        mock_validator_class.return_value = mock_validator
        
        # Test validation
        result = self.validator.validate_configurations_batch(
            config_files=[str(self.test_config)],
            validation_level=ValidationLevel.COMPREHENSIVE,
            parallel=False
        )
        
        self.assertEqual(result.total_items, 1)
        self.assertEqual(result.operation_type, "validate_configurations")
        
        # Verify validator was called
        mock_validator.validate_config_file.assert_called_once()
    
    def test_validate_syntax(self):
        """Test syntax validation."""
        issues = self.validator._validate_syntax(str(self.test_schema1))
        
        # Should pass basic syntax validation
        error_issues = [issue for issue in issues if issue.severity == "error"]
        self.assertEqual(len(error_issues), 0)
    
    def test_validate_naming_conventions(self):
        """Test naming convention validation."""
        # Test with good file name
        good_file = self.temp_dir / "good_schema.proto"
        good_file.write_text('syntax = "proto3";')
        
        issues = self.validator._validate_naming_conventions(str(good_file))
        naming_issues = [issue for issue in issues if issue.type == "naming_convention"]
        self.assertEqual(len(naming_issues), 0)
        
        # Test with bad file name
        bad_file = self.temp_dir / "BadSchema.proto"
        bad_file.write_text('syntax = "proto3";')
        
        issues = self.validator._validate_naming_conventions(str(bad_file))
        naming_issues = [issue for issue in issues if issue.type == "naming_convention"]
        self.assertGreater(len(naming_issues), 0)
    
    def test_discover_dependencies(self):
        """Test dependency discovery."""
        # Create schema with import
        schema_with_import = self.temp_dir / "importing.proto"
        schema_with_import.write_text('''
            syntax = "proto3";
            import "test1.proto";
            message Importing { Test1 test = 1; }
        ''')
        
        # Test dependency discovery
        dep_map = self.validator._discover_dependencies(
            schema_files=[str(schema_with_import)],
            include_paths=[str(self.temp_dir)]
        )
        
        self.assertIn(str(schema_with_import), dep_map)
        dependencies = dep_map[str(schema_with_import)]
        self.assertEqual(len(dependencies), 1)
        self.assertIn("test1.proto", dependencies[0])
    
    def test_detect_config_type(self):
        """Test configuration type detection."""
        self.assertEqual(self.validator._detect_config_type("config.json"), "json")
        self.assertEqual(self.validator._detect_config_type("config.yaml"), "yaml")
        self.assertEqual(self.validator._detect_config_type("config.yml"), "yaml")
        self.assertEqual(self.validator._detect_config_type("config.toml"), "toml")
        self.assertEqual(self.validator._detect_config_type("config.properties"), "properties")
        self.assertEqual(self.validator._detect_config_type("config.unknown"), "unknown")
    
    def test_get_validation_stats(self):
        """Test validation statistics."""
        stats = self.validator.get_validation_stats()
        
        self.assertIn("max_workers", stats)
        self.assertIn("active_batches", stats)
        self.assertIn("naming_patterns", stats)
        self.assertEqual(stats["max_workers"], 2)


class TestProgressTracker(unittest.TestCase):
    """Test progress tracking functionality."""
    
    def test_progress_tracker_creation(self):
        """Test progress tracker creation."""
        from testdatapy.batch_operations.batch_compiler import create_progress_tracker
        
        tracker = create_progress_tracker(show_details=True)
        self.assertIsNotNone(tracker)
        self.assertTrue(tracker.show_details)
        
        tracker_simple = create_progress_tracker(show_details=False)
        self.assertFalse(tracker_simple.show_details)
    
    def test_progress_callback(self):
        """Test progress callback functionality."""
        from testdatapy.batch_operations.batch_compiler import ProgressTracker
        
        tracker = ProgressTracker(show_details=False)
        
        # Mock item
        item = BatchItem("1", "test.proto", "protobuf", BatchStatus.RUNNING)
        
        # Test callback (should not raise exception)
        try:
            tracker("batch_1", 1, 5, item)
        except Exception as e:
            self.fail(f"Progress callback raised exception: {e}")


if __name__ == '__main__':
    unittest.main()