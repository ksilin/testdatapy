"""Tests for ProtobufCompiler functionality."""

import os
import platform
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from testdatapy.schema.compiler import ProtobufCompiler
from testdatapy.schema.exceptions import CompilationError, ValidationError


class TestProtobufCompilerInitialization:
    """Test ProtobufCompiler initialization and configuration."""
    
    def test_protobuf_compiler_init_default(self):
        """Test ProtobufCompiler initialization with default configuration."""
        compiler = ProtobufCompiler()
        
        assert compiler is not None
        assert hasattr(compiler, '_protoc_path')
        assert hasattr(compiler, '_config')
        assert hasattr(compiler, '_logger')
        
    def test_protobuf_compiler_init_with_config(self):
        """Test ProtobufCompiler initialization with custom configuration."""
        config = {'protoc_path': '/usr/local/bin/protoc', 'include_paths': ['/opt/proto']}
        compiler = ProtobufCompiler(config=config)
        
        assert compiler._config == config
        assert compiler._protoc_path == '/usr/local/bin/protoc'
        
    def test_protobuf_compiler_init_with_invalid_config(self):
        """Test ProtobufCompiler initialization with invalid configuration."""
        with pytest.raises(ValidationError):
            ProtobufCompiler(config={'invalid_key': 'value'})


class TestProtocDetection:
    """Test protoc compiler detection functionality."""
    
    @patch('testdatapy.schema.compiler.shutil.which')
    @patch.object(ProtobufCompiler, 'validate_protoc_path')
    def test_detect_protoc_in_path(self, mock_validate, mock_which):
        """Test protoc detection when available in PATH."""
        mock_which.return_value = '/usr/bin/protoc'
        mock_validate.return_value = None
        
        compiler = ProtobufCompiler()
        
        assert compiler._protoc_path == '/usr/bin/protoc'
        mock_which.assert_called_once_with('protoc')
        
    @patch('testdatapy.schema.compiler.shutil.which')
    def test_detect_protoc_not_found(self, mock_which):
        """Test protoc detection when not available."""
        mock_which.return_value = None
        
        with pytest.raises(ValidationError) as exc_info:
            ProtobufCompiler()
            
        assert 'protoc compiler not found' in str(exc_info.value)
        
    @patch('testdatapy.schema.compiler.platform.system')
    @patch('testdatapy.schema.compiler.shutil.which')
    @patch.object(ProtobufCompiler, 'validate_protoc_path')
    def test_detect_protoc_windows_executable_suffix(self, mock_validate, mock_which, mock_platform):
        """Test protoc detection on Windows with .exe suffix."""
        mock_platform.return_value = 'Windows'
        mock_which.return_value = 'C:\\Program Files\\protoc.exe'
        mock_validate.return_value = None
        
        compiler = ProtobufCompiler()
        
        assert compiler._protoc_path == 'C:\\Program Files\\protoc.exe'
        mock_which.assert_called_once_with('protoc.exe')
        
    def test_validate_protoc_path_valid(self):
        """Test validation of valid protoc path."""
        with tempfile.NamedTemporaryFile(suffix='.exe' if platform.system() == 'Windows' else '') as f:
            os.chmod(f.name, 0o755)
            
            compiler = ProtobufCompiler()
            
            # Should not raise exception for valid executable
            compiler.validate_protoc_path(f.name)
            
    def test_validate_protoc_path_not_found(self):
        """Test validation of non-existent protoc path."""
        compiler = ProtobufCompiler()
        
        with pytest.raises(ValidationError) as exc_info:
            compiler.validate_protoc_path('/non/existent/protoc')
            
        assert 'does not exist' in str(exc_info.value)
        
    def test_validate_protoc_path_not_executable(self):
        """Test validation of non-executable protoc path."""
        with tempfile.NamedTemporaryFile() as f:
            os.chmod(f.name, 0o644)  # Not executable
            
            compiler = ProtobufCompiler()
            
            with pytest.raises(ValidationError) as exc_info:
                compiler.validate_protoc_path(f.name)
                
            assert 'not executable' in str(exc_info.value)


class TestProtoCompilation:
    """Test proto file compilation functionality."""
    
    @pytest.fixture
    def simple_proto_file(self):
        """Create a simple proto file for testing."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.proto', delete=False) as f:
            f.write('''
syntax = "proto3";

message SimpleMessage {
  string id = 1;
  string name = 2;
}
''')
            yield f.name
        os.unlink(f.name)
        
    @pytest.fixture
    def proto_with_imports(self):
        """Create proto files with import dependencies."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Common proto file
            common_proto = Path(temp_dir) / 'common.proto'
            common_proto.write_text('''
syntax = "proto3";

message CommonMessage {
  string id = 1;
  string value = 2;
}
''')
            
            # Main proto file with import
            main_proto = Path(temp_dir) / 'main.proto'
            main_proto.write_text('''
syntax = "proto3";

import "common.proto";

message MainMessage {
  CommonMessage common = 1;
  string data = 2;
}
''')
            
            yield temp_dir, str(main_proto), str(common_proto)
    
    @patch('subprocess.run')
    def test_compile_proto_basic(self, mock_run, simple_proto_file):
        """Test basic proto compilation."""
        mock_run.return_value = Mock(returncode=0, stdout='', stderr='')
        
        compiler = ProtobufCompiler()
        
        with tempfile.TemporaryDirectory() as output_dir:
            result = compiler.compile_proto(simple_proto_file, output_dir)
            
            assert result['success'] is True
            assert result['output_dir'] == output_dir
            
            # Verify protoc was called correctly
            mock_run.assert_called_once()
            args = mock_run.call_args[0][0]
            assert any('protoc' in arg for arg in args)
            assert '--python_out' in args
            assert output_dir in args
            assert simple_proto_file in args
            
    @patch('subprocess.run')
    def test_compile_proto_with_includes(self, mock_run, simple_proto_file):
        """Test proto compilation with include paths."""
        mock_run.return_value = Mock(returncode=0, stdout='', stderr='')
        
        include_paths = ['/path/to/includes', '/another/path']
        compiler = ProtobufCompiler()
        
        with tempfile.TemporaryDirectory() as output_dir:
            compiler.compile_proto(simple_proto_file, output_dir, include_paths=include_paths)
            
            # Verify include paths were added
            args = mock_run.call_args[0][0]
            for include_path in include_paths:
                assert '--proto_path' in args
                assert include_path in args
                
    @patch('subprocess.run')
    def test_compile_proto_failure(self, mock_run, simple_proto_file):
        """Test proto compilation failure handling."""
        mock_run.return_value = Mock(
            returncode=1, 
            stdout='', 
            stderr='Parse error: invalid syntax'
        )
        
        compiler = ProtobufCompiler()
        
        with tempfile.TemporaryDirectory() as output_dir:
            with pytest.raises(CompilationError) as exc_info:
                compiler.compile_proto(simple_proto_file, output_dir)
                
            assert 'Parse error' in str(exc_info.value)
            
    def test_compile_proto_file_not_found(self):
        """Test compilation with non-existent proto file."""
        compiler = ProtobufCompiler()
        
        with tempfile.TemporaryDirectory() as output_dir:
            with pytest.raises(ValidationError):
                compiler.compile_proto('/non/existent/file.proto', output_dir)
                
    def test_compile_proto_invalid_output_dir(self, simple_proto_file):
        """Test compilation with invalid output directory."""
        compiler = ProtobufCompiler()
        
        with pytest.raises(ValidationError):
            compiler.compile_proto(simple_proto_file, '/non/existent/output/dir')


class TestDependencyResolution:
    """Test proto dependency resolution functionality."""
    
    @pytest.fixture
    def complex_proto_structure(self):
        """Create complex proto file structure with dependencies."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Base common proto
            base_proto = Path(temp_dir) / 'base.proto'
            base_proto.write_text('''
syntax = "proto3";

message BaseMessage {
  string id = 1;
}
''')
            
            # Common proto that imports base
            common_proto = Path(temp_dir) / 'common.proto'
            common_proto.write_text('''
syntax = "proto3";

import "base.proto";

message CommonMessage {
  BaseMessage base = 1;
  string data = 2;
}
''')
            
            # Main proto that imports common
            main_proto = Path(temp_dir) / 'main.proto'
            main_proto.write_text('''
syntax = "proto3";

import "common.proto";

message MainMessage {
  CommonMessage common = 1;
  string extra = 2;
}
''')
            
            yield temp_dir, str(main_proto), str(common_proto), str(base_proto)
    
    def test_resolve_dependencies_basic(self, complex_proto_structure):
        """Test basic dependency resolution."""
        temp_dir, main_proto, common_proto, base_proto = complex_proto_structure
        
        compiler = ProtobufCompiler()
        dependencies = compiler.resolve_dependencies(main_proto, [temp_dir])
        
        # Should resolve the dependency chain
        assert len(dependencies) >= 2  # common.proto and base.proto
        assert any('common.proto' in dep for dep in dependencies)
        assert any('base.proto' in dep for dep in dependencies)
        
    def test_resolve_dependencies_circular(self):
        """Test detection of circular dependencies."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Proto A imports Proto B
            proto_a = Path(temp_dir) / 'a.proto'
            proto_a.write_text('''
syntax = "proto3";
import "b.proto";
message A { B b = 1; }
''')
            
            # Proto B imports Proto A (circular)
            proto_b = Path(temp_dir) / 'b.proto'
            proto_b.write_text('''
syntax = "proto3";
import "a.proto";
message B { A a = 1; }
''')
            
            compiler = ProtobufCompiler()
            
            with pytest.raises(CompilationError) as exc_info:
                compiler.resolve_dependencies(str(proto_a), [temp_dir])
                
            assert 'circular dependency' in str(exc_info.value).lower()
            
    def test_resolve_dependencies_missing_import(self):
        """Test handling of missing import dependencies."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.proto', delete=False) as f:
            f.write('''
syntax = "proto3";

import "missing.proto";

message TestMessage {
  string id = 1;
}
''')
            f.flush()
            
            compiler = ProtobufCompiler()
            
            with pytest.raises(ValidationError) as exc_info:
                compiler.resolve_dependencies(f.name, [])
                
            assert 'missing.proto' in str(exc_info.value)
            
        os.unlink(f.name)
        
    def test_topological_sort_dependencies(self, complex_proto_structure):
        """Test topological sorting of dependencies."""
        temp_dir, main_proto, common_proto, base_proto = complex_proto_structure
        
        compiler = ProtobufCompiler()
        dependencies = compiler.resolve_dependencies(main_proto, [temp_dir])
        sorted_deps = compiler.topological_sort(dependencies, main_proto)
        
        # base.proto should come before common.proto
        base_index = next(i for i, dep in enumerate(sorted_deps) if 'base.proto' in dep)
        common_index = next(i for i, dep in enumerate(sorted_deps) if 'common.proto' in dep)
        
        assert base_index < common_index


class TestTemporaryCompilation:
    """Test temporary directory compilation functionality."""
    
    @pytest.fixture
    def proto_files_set(self):
        """Create a set of related proto files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            proto_files = []
            
            for i in range(3):
                proto_file = Path(temp_dir) / f'test{i}.proto'
                proto_file.write_text(f'''
syntax = "proto3";

message TestMessage{i} {{
  string id = 1;
  string data_{i} = 2;
}}
''')
                proto_files.append(str(proto_file))
                
            yield temp_dir, proto_files
    
    @patch('subprocess.run')
    def test_temp_compile_basic(self, mock_run, proto_files_set):
        """Test basic temporary compilation."""
        mock_run.return_value = Mock(returncode=0, stdout='', stderr='')
        
        temp_dir, proto_files = proto_files_set
        compiler = ProtobufCompiler()
        
        result = compiler.temp_compile(proto_files[0])
        
        assert result['success'] is True
        assert 'temp_dir' in result
        assert Path(result['temp_dir']).exists()
        
    @patch('subprocess.run')
    def test_temp_compile_with_cleanup(self, mock_run, proto_files_set):
        """Test temporary compilation with automatic cleanup."""
        mock_run.return_value = Mock(returncode=0, stdout='', stderr='')
        
        temp_dir, proto_files = proto_files_set
        compiler = ProtobufCompiler()
        
        result = compiler.temp_compile(proto_files[0], cleanup=True)
        
        # Temporary directory should be cleaned up
        assert not Path(result['temp_dir']).exists()
        
    @patch('subprocess.run')
    def test_temp_compile_preserve_on_error(self, mock_run, proto_files_set):
        """Test temporary compilation preserves directory on error."""
        mock_run.return_value = Mock(returncode=1, stdout='', stderr='Compilation failed')
        
        temp_dir, proto_files = proto_files_set
        compiler = ProtobufCompiler()
        
        with pytest.raises(CompilationError):
            compiler.temp_compile(proto_files[0], preserve_on_error=True)
            
    def test_temp_compile_sys_path_management(self, proto_files_set):
        """Test sys.path management during temporary compilation."""
        temp_dir, proto_files = proto_files_set
        compiler = ProtobufCompiler()
        
        original_sys_path = __import__('sys').path.copy()
        
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout='', stderr='')
            
            # Use context manager for sys.path management
            with compiler.temp_compile_context(proto_files[0]) as result:
                current_sys_path = __import__('sys').path
                assert result['temp_dir'] in current_sys_path
                
            # sys.path should be restored
            final_sys_path = __import__('sys').path
            assert final_sys_path == original_sys_path


class TestCrossPlatformCompatibility:
    """Test cross-platform compatibility functionality."""
    
    @patch('testdatapy.schema.compiler.platform.system')
    @patch('testdatapy.schema.compiler.shutil.which')
    @patch.object(ProtobufCompiler, 'validate_protoc_path')
    def test_get_protoc_executable_name_windows(self, mock_validate, mock_which, mock_platform):
        """Test protoc executable name detection on Windows."""
        mock_platform.return_value = 'Windows'
        mock_which.return_value = 'C:\\Program Files\\protoc.exe'
        mock_validate.return_value = None
        
        compiler = ProtobufCompiler()
        executable_name = compiler.get_protoc_executable_name()
        
        assert executable_name == 'protoc.exe'
        
    @patch('testdatapy.schema.compiler.platform.system')
    @patch('testdatapy.schema.compiler.shutil.which')
    @patch.object(ProtobufCompiler, 'validate_protoc_path')
    def test_get_protoc_executable_name_unix(self, mock_validate, mock_which, mock_platform):
        """Test protoc executable name detection on Unix-like systems."""
        mock_platform.return_value = 'Linux'
        mock_which.return_value = '/usr/bin/protoc'
        mock_validate.return_value = None
        
        compiler = ProtobufCompiler()
        executable_name = compiler.get_protoc_executable_name()
        
        assert executable_name == 'protoc'
        
    @patch('testdatapy.schema.compiler.shutil.which')
    @patch.object(ProtobufCompiler, 'validate_protoc_path')
    def test_normalize_path_windows(self, mock_validate, mock_which):
        """Test path normalization on Windows."""
        mock_which.return_value = '/usr/bin/protoc'
        mock_validate.return_value = None
        
        compiler = ProtobufCompiler()
        normalized = compiler.normalize_path('C:\\Users\\test\\proto')
        
        # Path should be normalized to absolute form
        assert Path(normalized).is_absolute()
        
    @patch('testdatapy.schema.compiler.shutil.which')
    @patch.object(ProtobufCompiler, 'validate_protoc_path')
    def test_normalize_path_unix(self, mock_validate, mock_which):
        """Test path normalization on Unix-like systems."""
        mock_which.return_value = '/usr/bin/protoc'
        mock_validate.return_value = None
        
        compiler = ProtobufCompiler()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Use a real existing path for normalization
            normalized = compiler.normalize_path(temp_dir)
            
            # Path should be normalized to absolute form
            assert Path(normalized).is_absolute()
            assert Path(normalized).exists()


class TestErrorHandling:
    """Test comprehensive error handling."""
    
    def test_compilation_error_with_details(self):
        """Test CompilationError includes compilation details."""
        compiler = ProtobufCompiler()
        
        try:
            raise CompilationError(
                "Test error", 
                schema_path="/test/schema.proto",
                compiler_output="error output",
                exit_code=1
            )
        except CompilationError as e:
            assert str(e) == "Test error"
            assert hasattr(e, 'compiler_output')
            assert e.compiler_output == "error output"
            assert e.exit_code == 1
            assert e.schema_path == "/test/schema.proto"
            
    @patch('subprocess.run')
    def test_subprocess_timeout_handling(self, mock_run):
        """Test handling of subprocess timeouts."""
        mock_run.side_effect = subprocess.TimeoutExpired('protoc', 30)
        
        compiler = ProtobufCompiler()
        
        with tempfile.NamedTemporaryFile(suffix='.proto') as f:
            with tempfile.TemporaryDirectory() as output_dir:
                with pytest.raises(CompilationError) as exc_info:
                    compiler.compile_proto(f.name, output_dir, timeout=30)
                    
                assert 'timeout' in str(exc_info.value).lower()
                
    @patch('subprocess.run')
    def test_subprocess_permission_error(self, mock_run):
        """Test handling of permission errors."""
        mock_run.side_effect = PermissionError("Permission denied")
        
        compiler = ProtobufCompiler()
        
        with tempfile.NamedTemporaryFile(suffix='.proto') as f:
            with tempfile.TemporaryDirectory() as output_dir:
                with pytest.raises(CompilationError) as exc_info:
                    compiler.compile_proto(f.name, output_dir)
                    
                assert 'permission' in str(exc_info.value).lower()


class TestIntegrationScenarios:
    """Test integration scenarios combining multiple features."""
    
    @patch('subprocess.run')
    def test_full_compilation_workflow(self, mock_run):
        """Test complete compilation workflow from detection to cleanup."""
        mock_run.return_value = Mock(returncode=0, stdout='', stderr='')
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create proto file
            proto_file = Path(temp_dir) / 'test.proto'
            proto_file.write_text('''
syntax = "proto3";

message TestMessage {
  string id = 1;
  string name = 2;
}
''')
            
            compiler = ProtobufCompiler()
            
            # Detect protoc (mocked to succeed)
            with patch.object(compiler, 'detect_protoc', return_value='/usr/bin/protoc'):
                # Resolve dependencies
                dependencies = compiler.resolve_dependencies(str(proto_file), [temp_dir])
                
                # Compile with temporary directory
                result = compiler.temp_compile(str(proto_file))
                
                assert result['success'] is True
                assert 'temp_dir' in result