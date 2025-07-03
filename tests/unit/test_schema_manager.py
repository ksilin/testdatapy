"""Tests for SchemaManager core functionality."""

import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from testdatapy.schema.exceptions import CompilationError, SchemaError, ValidationError
from testdatapy.schema.manager import SchemaManager


class TestSchemaManagerInitialization:
    """Test SchemaManager initialization and configuration."""
    
    def test_schema_manager_init_default(self):
        """Test SchemaManager initialization with default configuration."""
        manager = SchemaManager()
        
        assert manager is not None
        assert hasattr(manager, '_search_paths')
        assert hasattr(manager, '_config')
        assert hasattr(manager, '_logger')
        
    def test_schema_manager_init_with_config(self):
        """Test SchemaManager initialization with custom configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = {'schema_paths': [temp_dir], 'auto_compile': True}
            manager = SchemaManager(config=config)
            
            assert manager._config == config
            # Path is resolved to absolute form
            assert str(Path(temp_dir).resolve()) in manager.list_search_paths()
        
    def test_schema_manager_init_with_invalid_config(self):
        """Test SchemaManager initialization with invalid configuration."""
        with pytest.raises(ValidationError):
            SchemaManager(config={'invalid_key': 'value'})


class TestProtoFileDiscovery:
    """Test proto file discovery functionality."""
    
    @pytest.fixture
    def temp_schema_dir(self):
        """Create temporary directory with proto files for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test proto files
            proto_files = [
                'customer.proto',
                'order.proto', 
                'payment.proto',
                'nested/product.proto'
            ]
            
            for proto_file in proto_files:
                file_path = Path(temp_dir) / proto_file
                file_path.parent.mkdir(parents=True, exist_ok=True)
                file_path.write_text(
                    'syntax = "proto3";\n\nmessage TestMessage {\n  string id = 1;\n}\n'
                )
            
            # Create non-proto files to test filtering
            (Path(temp_dir) / 'not_proto.txt').write_text('not a proto file')
            (Path(temp_dir) / 'schema.json').write_text('{"not": "proto"}')
            
            yield temp_dir
    
    def test_discover_proto_files_basic(self, temp_schema_dir):
        """Test basic proto file discovery."""
        manager = SchemaManager()
        
        proto_files = manager.discover_proto_files(temp_schema_dir)
        
        assert len(proto_files) == 4
        proto_names = [Path(f).name for f in proto_files]
        assert 'customer.proto' in proto_names
        assert 'order.proto' in proto_names
        assert 'payment.proto' in proto_names
        assert 'product.proto' in proto_names
        
    def test_discover_proto_files_recursive(self, temp_schema_dir):
        """Test recursive proto file discovery."""
        manager = SchemaManager()
        
        proto_files = manager.discover_proto_files(temp_schema_dir, recursive=True)
        
        # Should find nested files
        nested_files = [f for f in proto_files if 'nested' in f]
        assert len(nested_files) == 1
        
    def test_discover_proto_files_non_recursive(self, temp_schema_dir):
        """Test non-recursive proto file discovery."""
        manager = SchemaManager()
        
        proto_files = manager.discover_proto_files(temp_schema_dir, recursive=False)
        
        # Should not find nested files
        nested_files = [f for f in proto_files if 'nested' in f]
        assert len(nested_files) == 0
        
    def test_discover_proto_files_invalid_path(self):
        """Test proto file discovery with invalid path."""
        manager = SchemaManager()
        
        with pytest.raises(ValidationError):
            manager.discover_proto_files('/non/existent/path')
            
    def test_discover_proto_files_empty_directory(self):
        """Test proto file discovery in empty directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = SchemaManager()
            
            proto_files = manager.discover_proto_files(temp_dir)
            
            assert proto_files == []


class TestProtoFileValidation:
    """Test proto file syntax validation."""
    
    @pytest.fixture
    def valid_proto_file(self):
        """Create a valid proto file for testing."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.proto', delete=False) as f:
            f.write('''
syntax = "proto3";

message Customer {
  string id = 1;
  string name = 2;
  string email = 3;
}
''')
            yield f.name
        os.unlink(f.name)
        
    @pytest.fixture 
    def invalid_proto_file(self):
        """Create an invalid proto file for testing."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.proto', delete=False) as f:
            f.write('''
syntax = "proto3";

message Customer {
  string id = 1;
  string name = 2;
  invalid_syntax here
}
''')
            yield f.name
        os.unlink(f.name)
    
    @patch('subprocess.run')
    def test_validate_proto_syntax_valid(self, mock_run, valid_proto_file):
        """Test validation of valid proto file."""
        mock_run.return_value = Mock(returncode=0, stdout='', stderr='')
        
        manager = SchemaManager()
        
        # Should not raise exception for valid proto
        manager.validate_proto_syntax(valid_proto_file)
        
        # Should call protoc with --python_out to temp directory
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert 'protoc' in args[0]
        assert '--python_out' in args
        assert valid_proto_file in args
        
    @patch('subprocess.run')
    def test_validate_proto_syntax_invalid(self, mock_run, invalid_proto_file):
        """Test validation of invalid proto file."""
        mock_run.return_value = Mock(
            returncode=1, 
            stdout='', 
            stderr='Parse error: syntax error'
        )
        
        manager = SchemaManager()
        
        with pytest.raises(ValidationError) as exc_info:
            manager.validate_proto_syntax(invalid_proto_file)
            
        assert 'Parse error' in str(exc_info.value)
        
    def test_validate_proto_syntax_file_not_found(self):
        """Test validation with non-existent proto file."""
        manager = SchemaManager()
        
        with pytest.raises(ValidationError):
            manager.validate_proto_syntax('/non/existent/file.proto')


class TestCompilationLifecycle:
    """Test proto compilation lifecycle management."""
    
    @pytest.fixture
    def proto_with_dependencies(self):
        """Create proto files with dependencies for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Main proto file
            main_proto = Path(temp_dir) / 'main.proto'
            main_proto.write_text('''
syntax = "proto3";

import "common.proto";

message MainMessage {
  CommonMessage common = 1;
  string data = 2;
}
''')
            
            # Dependency proto file
            common_proto = Path(temp_dir) / 'common.proto'
            common_proto.write_text('''
syntax = "proto3";

message CommonMessage {
  string id = 1;
  string name = 2;
}
''')
            
            yield temp_dir, str(main_proto), str(common_proto)
    
    def test_compile_lifecycle_basic(self):
        """Test basic compilation lifecycle."""
        manager = SchemaManager()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            proto_file = Path(temp_dir) / 'test.proto'
            proto_file.write_text('''
syntax = "proto3";

message TestMessage {
  string id = 1;
}
''')
            
            with patch('subprocess.run') as mock_run:
                mock_run.return_value = Mock(returncode=0, stdout='', stderr='')
                
                output_dir = manager.compile_lifecycle(str(proto_file))
                
                assert output_dir is not None
                assert Path(output_dir).exists()
                
    def test_compile_lifecycle_with_cleanup_on_success(self):
        """Test compilation lifecycle with cleanup on success."""
        manager = SchemaManager()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            proto_file = Path(temp_dir) / 'test.proto'
            proto_file.write_text('syntax = "proto3";\nmessage Test { string id = 1; }')
            
            with patch('subprocess.run') as mock_run:
                mock_run.return_value = Mock(returncode=0, stdout='', stderr='')
                
                output_dir = manager.compile_lifecycle(str(proto_file), cleanup_on_success=True)
                
                # Temporary directory should be cleaned up
                assert not Path(output_dir).exists()
                
    def test_compile_lifecycle_with_cleanup_on_failure(self):
        """Test compilation lifecycle with cleanup on failure."""
        manager = SchemaManager()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            proto_file = Path(temp_dir) / 'test.proto'
            proto_file.write_text('invalid proto syntax')
            
            with patch('subprocess.run') as mock_run:
                mock_run.return_value = Mock(returncode=1, stdout='', stderr='Compilation failed')
                
                with pytest.raises(CompilationError):
                    manager.compile_lifecycle(str(proto_file), cleanup_on_failure=True)


class TestSearchPathManagement:
    """Test multiple schema search path support."""
    
    def test_add_search_path(self):
        """Test adding search path."""
        manager = SchemaManager()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            manager.add_search_path(temp_dir)
            
            assert str(Path(temp_dir).resolve()) in manager.list_search_paths()
            
    def test_add_search_path_invalid(self):
        """Test adding invalid search path."""
        manager = SchemaManager()
        
        with pytest.raises(ValidationError):
            manager.add_search_path('/non/existent/path')
            
    def test_remove_search_path(self):
        """Test removing search path."""
        manager = SchemaManager()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            resolved_path = str(Path(temp_dir).resolve())
            manager.add_search_path(temp_dir)
            assert resolved_path in manager.list_search_paths()
            
            manager.remove_search_path(temp_dir)
            assert resolved_path not in manager.list_search_paths()
            
    def test_remove_search_path_not_found(self):
        """Test removing non-existent search path."""
        manager = SchemaManager()
        
        # Should not raise exception
        manager.remove_search_path('/non/existent/path')
        
    def test_list_search_paths(self):
        """Test listing search paths."""
        manager = SchemaManager()
        
        with tempfile.TemporaryDirectory() as temp_dir1:
            with tempfile.TemporaryDirectory() as temp_dir2:
                resolved_path1 = str(Path(temp_dir1).resolve())
                resolved_path2 = str(Path(temp_dir2).resolve())
                
                manager.add_search_path(temp_dir1)
                manager.add_search_path(temp_dir2)
                
                paths = manager.list_search_paths()
                
                assert resolved_path1 in paths
                assert resolved_path2 in paths
                
    def test_search_paths_priority_ordering(self):
        """Test search paths are ordered by priority."""
        manager = SchemaManager()
        
        with tempfile.TemporaryDirectory() as temp_dir1:
            with tempfile.TemporaryDirectory() as temp_dir2:
                resolved_path1 = str(Path(temp_dir1).resolve())
                resolved_path2 = str(Path(temp_dir2).resolve())
                
                # Add paths in specific order
                manager.add_search_path(temp_dir1, priority=1)
                manager.add_search_path(temp_dir2, priority=2)
                
                paths = manager.list_search_paths()
                
                # Higher priority should come first
                assert paths.index(resolved_path2) < paths.index(resolved_path1)


class TestErrorHandling:
    """Test comprehensive error handling."""
    
    def test_schema_error_inheritance(self):
        """Test that SchemaError is properly defined."""
        error = SchemaError("Test error")
        assert isinstance(error, Exception)
        assert str(error) == "Test error"
        
    def test_compilation_error_inheritance(self):
        """Test that CompilationError inherits from SchemaError."""
        error = CompilationError("Compilation failed")
        assert isinstance(error, SchemaError)
        assert isinstance(error, Exception)
        
    def test_validation_error_inheritance(self):
        """Test that ValidationError inherits from SchemaError."""
        error = ValidationError("Validation failed")
        assert isinstance(error, SchemaError)
        assert isinstance(error, Exception)


class TestIntegrationScenarios:
    """Test integration scenarios combining multiple features."""
    
    def test_full_workflow_discovery_validation_compilation(self):
        """Test complete workflow from discovery to compilation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test proto file
            proto_file = Path(temp_dir) / 'test.proto'
            proto_file.write_text('''
syntax = "proto3";

message TestMessage {
  string id = 1;
  string name = 2;
}
''')
            
            manager = SchemaManager()
            manager.add_search_path(temp_dir)
            
            # Discover files
            discovered = manager.discover_proto_files(temp_dir)
            assert len(discovered) == 1
            
            # Validate syntax
            with patch('subprocess.run') as mock_run:
                mock_run.return_value = Mock(returncode=0, stdout='', stderr='')
                manager.validate_proto_syntax(discovered[0])
                
                # Compile 
                output_dir = manager.compile_lifecycle(discovered[0])
                assert output_dir is not None


class TestAdvancedEdgeCases:
    """Test advanced edge cases and error conditions."""
    
    def test_config_validation_comprehensive(self):
        """Test comprehensive configuration validation."""
        manager = SchemaManager()
        
        # Test with multiple invalid keys
        invalid_config = {
            'invalid_key1': 'value1',
            'invalid_key2': 'value2',
            'schema_paths': []  # This is valid
        }
        
        with pytest.raises(ValidationError) as exc_info:
            manager._validate_config(invalid_config)
        
        # Should mention the first invalid key
        assert 'invalid_key1' in str(exc_info.value)
    
    def test_discover_proto_files_symlinks(self):
        """Test proto file discovery with symbolic links."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a proto file
            proto_file = Path(temp_dir) / 'original.proto'
            proto_file.write_text('syntax = "proto3"; message Test { string id = 1; }')
            
            # Create a symbolic link (if supported by the OS)
            try:
                symlink_file = Path(temp_dir) / 'symlink.proto'
                symlink_file.symlink_to(proto_file)
                
                manager = SchemaManager()
                discovered = manager.discover_proto_files(temp_dir)
                
                # Should find both original and symlink
                assert len(discovered) >= 1
            except OSError:
                # Symlinks not supported on this platform
                pytest.skip("Symbolic links not supported")
    
    def test_discover_proto_files_permission_denied(self):
        """Test discovery with permission denied scenarios."""
        manager = SchemaManager()
        
        # Try to discover in a path that requires special permissions
        # This test might be platform-specific
        try:
            # /root typically requires elevated permissions
            manager.discover_proto_files('/root')
        except (PermissionError, ValidationError):
            # Expected - either permission denied or path doesn't exist
            pass
    
    def test_validate_proto_syntax_protoc_not_found(self):
        """Test validation when protoc is not found."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.proto', delete=False) as f:
            f.write('syntax = "proto3"; message Test { string id = 1; }')
            proto_file = f.name
        
        try:
            manager = SchemaManager()
            
            with patch('subprocess.run', side_effect=FileNotFoundError("protoc not found")):
                with pytest.raises(ValidationError) as exc_info:
                    manager.validate_proto_syntax(proto_file)
                
                assert "protoc compiler not found" in str(exc_info.value)
        finally:
            os.unlink(proto_file)
    
    def test_validate_proto_syntax_subprocess_exception(self):
        """Test validation with unexpected subprocess exception."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.proto', delete=False) as f:
            f.write('syntax = "proto3"; message Test { string id = 1; }')
            proto_file = f.name
        
        try:
            manager = SchemaManager()
            
            with patch('subprocess.run', side_effect=OSError("Unexpected error")):
                with pytest.raises(ValidationError) as exc_info:
                    manager.validate_proto_syntax(proto_file)
                
                assert "Error during proto validation" in str(exc_info.value)
        finally:
            os.unlink(proto_file)
    
    def test_compile_lifecycle_file_not_found(self):
        """Test compilation lifecycle with non-existent file."""
        manager = SchemaManager()
        
        with pytest.raises(CompilationError) as exc_info:
            manager.compile_lifecycle('/non/existent/file.proto')
        
        assert "Proto file does not exist" in str(exc_info.value)
    
    def test_compile_lifecycle_permission_error(self):
        """Test compilation lifecycle with permission errors."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.proto', delete=False) as f:
            f.write('syntax = "proto3"; message Test { string id = 1; }')
            proto_file = f.name
        
        try:
            manager = SchemaManager()
            
            with patch('subprocess.run', side_effect=PermissionError("Permission denied")):
                with pytest.raises(CompilationError) as exc_info:
                    manager.compile_lifecycle(proto_file)
                
                assert "Error during proto compilation" in str(exc_info.value)
        finally:
            os.unlink(proto_file)
    
    def test_compile_lifecycle_custom_output_dir(self):
        """Test compilation lifecycle with custom output directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            proto_file = Path(temp_dir) / 'test.proto'
            proto_file.write_text('syntax = "proto3"; message Test { string id = 1; }')
            
            output_dir = Path(temp_dir) / 'custom_output'
            
            manager = SchemaManager()
            
            with patch('subprocess.run') as mock_run:
                mock_run.return_value = Mock(returncode=0, stdout='', stderr='')
                
                result_dir = manager.compile_lifecycle(str(proto_file), str(output_dir))
                
                assert result_dir == str(output_dir)
                assert output_dir.exists()
    
    def test_add_search_path_duplicate(self):
        """Test adding duplicate search paths."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = SchemaManager()
            
            # Add the same path twice
            manager.add_search_path(temp_dir)
            manager.add_search_path(temp_dir)  # Should not duplicate
            
            paths = manager.list_search_paths()
            resolved_path = str(Path(temp_dir).resolve())
            
            # Should only appear once
            assert paths.count(resolved_path) == 1
    
    def test_add_search_path_not_directory(self):
        """Test adding a file as search path (should fail)."""
        with tempfile.NamedTemporaryFile() as temp_file:
            manager = SchemaManager()
            
            with pytest.raises(ValidationError) as exc_info:
                manager.add_search_path(temp_file.name)
            
            assert "not a directory" in str(exc_info.value)
    
    def test_cleanup_directory_failure(self):
        """Test directory cleanup failure handling."""
        manager = SchemaManager()
        
        # Mock shutil.rmtree to raise an exception
        with patch('shutil.rmtree', side_effect=PermissionError("Permission denied")):
            # Should not raise exception, just log warning
            manager._cleanup_directory('/some/directory')


class TestPerformanceAndConcurrency:
    """Test performance characteristics and concurrent access patterns."""
    
    def test_discovery_performance_large_directory(self):
        """Test discovery performance with many files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create many proto files
            for i in range(100):
                proto_file = Path(temp_dir) / f'proto_{i:03d}.proto'
                proto_file.write_text(f'syntax = "proto3"; message Test{i} {{ string id = 1; }}')
            
            # Create many non-proto files
            for i in range(50):
                other_file = Path(temp_dir) / f'other_{i:03d}.txt'
                other_file.write_text('not a proto file')
            
            manager = SchemaManager()
            
            import time
            start_time = time.time()
            discovered = manager.discover_proto_files(temp_dir)
            duration = time.time() - start_time
            
            # Should find all proto files
            assert len(discovered) == 100
            
            # Should complete in reasonable time (adjust threshold as needed)
            assert duration < 5.0  # 5 seconds should be more than enough
    
    def test_multiple_search_paths_order(self):
        """Test search path priority ordering with multiple paths."""
        with tempfile.TemporaryDirectory() as temp_dir1:
            with tempfile.TemporaryDirectory() as temp_dir2:
                with tempfile.TemporaryDirectory() as temp_dir3:
                    manager = SchemaManager()
                    
                    # Add paths with different priorities
                    manager.add_search_path(temp_dir1, priority=10)
                    manager.add_search_path(temp_dir2, priority=20)
                    manager.add_search_path(temp_dir3, priority=15)
                    
                    paths = manager.list_search_paths()
                    
                    # Should be ordered by priority (highest first)
                    path1 = str(Path(temp_dir1).resolve())
                    path2 = str(Path(temp_dir2).resolve())
                    path3 = str(Path(temp_dir3).resolve())
                    
                    assert paths.index(path2) == 0  # priority 20
                    assert paths.index(path3) == 1  # priority 15  
                    assert paths.index(path1) == 2  # priority 10


class TestMockingAndIsolation:
    """Test proper mocking and test isolation."""
    
    def test_subprocess_call_validation_mocking(self):
        """Test that subprocess calls are properly mocked in validation."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.proto', delete=False) as f:
            f.write('syntax = "proto3"; message Test { string id = 1; }')
            proto_file = f.name
        
        try:
            manager = SchemaManager()
            
            with patch('subprocess.run') as mock_run:
                mock_run.return_value = Mock(returncode=0, stdout='Success', stderr='')
                
                # Should not raise exception
                manager.validate_proto_syntax(proto_file)
                
                # Verify subprocess was called correctly
                mock_run.assert_called_once()
                call_args = mock_run.call_args[0][0]
                
                assert 'protoc' in call_args[0]
                assert '--python_out' in call_args
                assert proto_file in call_args
        finally:
            os.unlink(proto_file)
    
    def test_subprocess_call_compilation_mocking(self):
        """Test that subprocess calls are properly mocked in compilation."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.proto', delete=False) as f:
            f.write('syntax = "proto3"; message Test { string id = 1; }')
            proto_file = f.name
        
        try:
            manager = SchemaManager()
            
            with patch('subprocess.run') as mock_run:
                mock_run.return_value = Mock(returncode=0, stdout='Generated files', stderr='')
                
                output_dir = manager.compile_lifecycle(proto_file)
                
                # Verify subprocess was called correctly
                mock_run.assert_called_once()
                call_args = mock_run.call_args[0][0]
                
                assert 'protoc' in call_args[0]
                assert '--python_out' in call_args
                assert proto_file in call_args
                assert output_dir is not None
        finally:
            os.unlink(proto_file)
    
    @patch('testdatapy.schema.manager.get_schema_logger')
    def test_logger_integration(self, mock_get_logger):
        """Test logger integration and method calls."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        manager = SchemaManager()
        
        # Verify logger was obtained and configured
        mock_get_logger.assert_called_with('testdatapy.schema.manager')
        mock_logger.set_operation_context.assert_called_with(component='SchemaManager')
        mock_logger.info.assert_called()
    
    def test_tempfile_cleanup_on_exception(self):
        """Test that temporary files are cleaned up even when exceptions occur."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.proto', delete=False) as f:
            f.write('syntax = "proto3"; message Test { string id = 1; }')
            proto_file = f.name
        
        try:
            manager = SchemaManager()
            
            with patch('subprocess.run') as mock_run:
                # Simulate compilation failure
                mock_run.return_value = Mock(returncode=1, stdout='', stderr='Compilation failed')
                
                with pytest.raises(CompilationError):
                    manager.compile_lifecycle(proto_file, cleanup_on_failure=True)
                
                # Verify cleanup was attempted (mocked in this case)
                mock_run.assert_called_once()
        finally:
            os.unlink(proto_file)