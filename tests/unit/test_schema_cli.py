"""Tests for schema management CLI commands."""

import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import yaml
from click.testing import CliRunner

from testdatapy.cli import cli


class TestProtoCLIGroup:
    """Test proto CLI command group."""
    
    def test_proto_command_group_exists(self):
        """Test that proto command group is available."""
        runner = CliRunner()
        result = runner.invoke(cli, ['proto', '--help'])
        
        assert result.exit_code == 0
        assert 'proto' in result.output.lower()
        assert 'Protobuf schema management commands' in result.output
    
    def test_proto_command_group_subcommands(self):
        """Test that all expected subcommands are available."""
        runner = CliRunner()
        result = runner.invoke(cli, ['proto', '--help'])
        
        assert result.exit_code == 0
        expected_commands = ['compile', 'discover', 'list', 'test', 'docs']
        
        for command in expected_commands:
            assert command in result.output


class TestSchemaCompileCommand:
    """Test schema compile command."""
    
    @pytest.fixture
    def proto_files_dir(self):
        """Create temporary directory with proto files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            proto_dir = Path(temp_dir)
            
            # Simple proto file
            (proto_dir / "simple.proto").write_text('''
syntax = "proto3";

message SimpleMessage {
  string id = 1;
  string name = 2;
}
''')
            
            # Proto file with dependencies
            (proto_dir / "common.proto").write_text('''
syntax = "proto3";

message Common {
  string id = 1;
}
''')
            
            (proto_dir / "customer.proto").write_text('''
syntax = "proto3";

import "common.proto";

message Customer {
  Common common = 1;
  string name = 2;
  string email = 3;
}
''')
            
            yield temp_dir
    
    def test_schema_compile_help(self):
        """Test schema compile command help."""
        runner = CliRunner()
        result = runner.invoke(cli, ['proto', 'compile', '--help'])
        
        assert result.exit_code == 0
        assert '--proto-dir' in result.output
        assert '--output-dir' in result.output
        assert '--include-path' in result.output
        assert '--verbose' in result.output
        assert '--dry-run' in result.output
    
    @patch('testdatapy.schema.manager.SchemaManager')
    @patch('testdatapy.schema.compiler.ProtobufCompiler')
    def test_schema_compile_basic(self, mock_compiler_class, mock_manager_class, proto_files_dir):
        """Test basic schema compilation."""
        mock_compiler = Mock()
        mock_compiler_class.return_value = mock_compiler
        mock_compiler.compile_proto.return_value = {
            'success': True,
            'output_dir': '/tmp/output',
            'stdout': '',
            'stderr': ''
        }
        
        mock_manager = Mock()
        mock_manager_class.return_value = mock_manager
        mock_manager.discover_proto_files.return_value = [
            f"{proto_files_dir}/simple.proto",
            f"{proto_files_dir}/customer.proto"
        ]
        
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as output_dir:
            result = runner.invoke(cli, [
                'proto', 'compile',
                '--proto-dir', proto_files_dir,
                '--output-dir', output_dir
            ])
            
            assert result.exit_code == 0
            assert 'Compiling proto files' in result.output
            assert 'Compilation completed successfully' in result.output
            mock_compiler.compile_proto.assert_called()
    
    @patch('testdatapy.schema.compiler.ProtobufCompiler')
    def test_schema_compile_with_includes(self, mock_compiler_class, proto_files_dir):
        """Test schema compilation with include paths."""
        mock_compiler = Mock()
        mock_compiler_class.return_value = mock_compiler
        mock_compiler.compile_proto.return_value = {
            'success': True,
            'output_dir': '/tmp/output',
            'stdout': '',
            'stderr': ''
        }
        
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as output_dir:
            with tempfile.TemporaryDirectory() as include_dir:
                result = runner.invoke(cli, [
                    'proto', 'compile',
                    '--proto-dir', proto_files_dir,
                    '--output-dir', output_dir,
                    '--include-path', include_dir
                ])
                
                assert result.exit_code == 0
                # Verify include path was passed to compiler
                call_args = mock_compiler.compile_proto.call_args_list
                assert len(call_args) > 0
    
    @patch('testdatapy.schema.compiler.ProtobufCompiler')
    def test_schema_compile_dry_run(self, mock_compiler_class, proto_files_dir):
        """Test schema compilation dry run mode."""
        mock_compiler = Mock()
        mock_compiler_class.return_value = mock_compiler
        mock_compiler.validate_proto_syntax.return_value = None
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            'proto', 'compile',
            '--proto-dir', proto_files_dir,
            '--dry-run'
        ])
        
        assert result.exit_code == 0
        assert 'DRY RUN: Validating syntax only' in result.output
        # Should validate but not compile in dry run
        mock_compiler.validate_proto_syntax.assert_called()
        mock_compiler.compile_proto.assert_not_called()
    
    @patch('testdatapy.schema.compiler.ProtobufCompiler')
    def test_schema_compile_failure(self, mock_compiler_class, proto_files_dir):
        """Test schema compilation failure handling."""
        from testdatapy.schema.exceptions import CompilationError
        
        mock_compiler = Mock()
        mock_compiler_class.return_value = mock_compiler
        mock_compiler.compile_proto.side_effect = CompilationError("Compilation failed")
        
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as output_dir:
            result = runner.invoke(cli, [
                'proto', 'compile',
                '--proto-dir', proto_files_dir,
                '--output-dir', output_dir
            ])
            
            assert result.exit_code == 1
            assert 'Error compiling' in result.output
            assert 'Compilation failed' in result.output
    
    def test_schema_compile_invalid_proto_dir(self):
        """Test schema compilation with invalid proto directory."""
        runner = CliRunner()
        result = runner.invoke(cli, [
            'proto', 'compile',
            '--proto-dir', '/non/existent/path'
        ])
        
        assert result.exit_code == 1
        assert 'Proto directory does not exist' in result.output


class TestSchemaDiscoverCommand:
    """Test schema discover command."""
    
    @pytest.fixture
    def discovery_test_dir(self):
        """Create test directory with various schema files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            schema_dir = Path(temp_dir)
            
            # Proto files
            (schema_dir / "user.proto").write_text('''
syntax = "proto3";
message User { string id = 1; string name = 2; }
''')
            
            (schema_dir / "product.proto").write_text('''
syntax = "proto3";
message Product { string id = 1; string title = 2; }
''')
            
            # Avro files
            (schema_dir / "order.avsc").write_text(json.dumps({
                "type": "record",
                "name": "Order",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "amount", "type": "double"}
                ]
            }))
            
            # Non-schema files
            (schema_dir / "readme.txt").write_text("Documentation")
            (schema_dir / "config.json").write_text("{}")
            
            yield temp_dir
    
    def test_schema_discover_help(self):
        """Test schema discover command help."""
        runner = CliRunner()
        result = runner.invoke(cli, ['proto', 'discover', '--help'])
        
        assert result.exit_code == 0
        assert '--proto-dir' in result.output
        assert '--format' in result.output
        assert '--recursive' in result.output
        assert '--register' in result.output
    
    @patch('testdatapy.schema.manager.SchemaManager')
    def test_schema_discover_basic(self, mock_manager_class, discovery_test_dir):
        """Test basic schema discovery."""
        mock_manager = Mock()
        mock_manager_class.return_value = mock_manager
        mock_manager.discover_proto_files.return_value = [
            f"{discovery_test_dir}/user.proto",
            f"{discovery_test_dir}/product.proto"
        ]
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            'proto', 'discover',
            '--proto-dir', discovery_test_dir
        ])
        
        assert result.exit_code == 0
        assert 'Discovered 2 proto files' in result.output
        assert 'user.proto' in result.output
        assert 'product.proto' in result.output
    
    @patch('testdatapy.schema.manager.SchemaManager')
    def test_schema_discover_json_format(self, mock_manager_class, discovery_test_dir):
        """Test schema discovery with JSON output format."""
        mock_manager = Mock()
        mock_manager_class.return_value = mock_manager
        mock_manager.discover_proto_files.return_value = [
            f"{discovery_test_dir}/user.proto"
        ]
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            'proto', 'discover',
            '--proto-dir', discovery_test_dir,
            '--format', 'json'
        ])
        
        assert result.exit_code == 0
        # Should output valid JSON
        output_lines = result.output.strip().split('\n')
        json_line = next((line for line in output_lines if line.strip().startswith('{')), None)
        assert json_line is not None
        parsed = json.loads(json_line)
        assert 'proto_files' in parsed
    
    @patch('testdatapy.schema.manager.SchemaManager')
    def test_schema_discover_recursive(self, mock_manager_class, discovery_test_dir):
        """Test recursive schema discovery."""
        # Create nested directory structure
        nested_dir = Path(discovery_test_dir) / "nested"
        nested_dir.mkdir()
        (nested_dir / "nested.proto").write_text('syntax = "proto3"; message Nested { string id = 1; }')
        
        mock_manager = Mock()
        mock_manager_class.return_value = mock_manager
        mock_manager.discover_proto_files.return_value = [
            f"{discovery_test_dir}/user.proto",
            f"{nested_dir}/nested.proto"
        ]
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            'proto', 'discover',
            '--proto-dir', discovery_test_dir,
            '--recursive'
        ])
        
        assert result.exit_code == 0
        mock_manager.discover_proto_files.assert_called_with(discovery_test_dir, recursive=True)
    
    @patch('testdatapy.schema.manager.SchemaManager')
    def test_schema_discover_with_register(self, mock_manager_class, discovery_test_dir):
        """Test schema discovery with Schema Registry registration."""
        mock_manager = Mock()
        mock_manager_class.return_value = mock_manager
        mock_manager.discover_proto_files.return_value = [
            f"{discovery_test_dir}/user.proto"
        ]
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            'proto', 'discover',
            '--proto-dir', discovery_test_dir,
            '--register',
            '--schema-registry-url', 'http://localhost:8081'
        ])
        
        assert result.exit_code == 0
        assert 'Registering schemas' in result.output


class TestSchemaListCommand:
    """Test schema list command."""
    
    def test_schema_list_help(self):
        """Test schema list command help."""
        runner = CliRunner()
        result = runner.invoke(cli, ['proto', 'list', '--help'])
        
        assert result.exit_code == 0
        assert '--schema-registry-url' in result.output
        assert '--format' in result.output
        assert '--subject-filter' in result.output
    
    @patch('testdatapy.schema_evolution.SchemaEvolutionManager')
    def test_schema_list_basic(self, mock_evolution_class):
        """Test basic schema listing."""
        mock_evolution = Mock()
        mock_evolution_class.return_value = mock_evolution
        mock_evolution.list_subjects.return_value = [
            'user-value',
            'product-value',
            'order-value'
        ]
        mock_evolution.get_latest_schema.return_value = {
            'id': 1,
            'version': 1,
            'schema': '{"type": "record", "name": "User"}'
        }
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            'proto', 'list',
            '--schema-registry-url', 'http://localhost:8081'
        ])
        
        assert result.exit_code == 0
        assert 'user-value' in result.output
        assert 'product-value' in result.output
        assert 'order-value' in result.output
    
    @patch('testdatapy.schema_evolution.SchemaEvolutionManager')
    def test_schema_list_json_format(self, mock_evolution_class):
        """Test schema listing with JSON format."""
        mock_evolution = Mock()
        mock_evolution_class.return_value = mock_evolution
        mock_evolution.list_subjects.return_value = ['user-value']
        mock_evolution.get_latest_schema.return_value = {
            'id': 1,
            'version': 1,
            'schema': '{"type": "record", "name": "User"}'
        }
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            'proto', 'list',
            '--schema-registry-url', 'http://localhost:8081',
            '--format', 'json'
        ])
        
        assert result.exit_code == 0
        # Should output valid JSON
        parsed = json.loads(result.output)
        assert 'subjects' in parsed
        assert len(parsed['subjects']) == 1
    
    @patch('testdatapy.schema_evolution.SchemaEvolutionManager')
    def test_schema_list_with_filter(self, mock_evolution_class):
        """Test schema listing with subject filter."""
        mock_evolution = Mock()
        mock_evolution_class.return_value = mock_evolution
        mock_evolution.list_subjects.return_value = [
            'user-value',
            'user-key', 
            'product-value'
        ]
        mock_evolution.get_latest_schema.return_value = {
            'id': 1,
            'version': 1,
            'schema': '{"type": "record", "name": "User"}'
        }
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            'proto', 'list',
            '--schema-registry-url', 'http://localhost:8081',
            '--subject-filter', 'user*'
        ])
        
        assert result.exit_code == 0
        assert 'user-value' in result.output
        assert 'user-key' in result.output
        assert 'product-value' not in result.output


class TestSchemaTestCommand:
    """Test schema test command."""
    
    @pytest.fixture
    def test_proto_file(self):
        """Create a test proto file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.proto', delete=False) as f:
            f.write('''
syntax = "proto3";

message TestMessage {
  string id = 1;
  string name = 2;
  int32 age = 3;
}
''')
            yield f.name
        Path(f.name).unlink()
    
    def test_schema_test_help(self):
        """Test schema test command help."""
        runner = CliRunner()
        result = runner.invoke(cli, ['proto', 'test', '--help'])
        
        assert result.exit_code == 0
        assert '--proto-file' in result.output
        assert '--include-path' in result.output
        assert '--verbose' in result.output
    
    @patch('testdatapy.schema.compiler.ProtobufCompiler')
    def test_schema_test_valid_proto(self, mock_compiler_class, test_proto_file):
        """Test schema testing with valid proto file."""
        mock_compiler = Mock()
        mock_compiler_class.return_value = mock_compiler
        mock_compiler.validate_proto_syntax.return_value = None
        mock_compiler.compile_proto.return_value = {
            'success': True,
            'output_dir': '/tmp/test',
            'stdout': '',
            'stderr': ''
        }
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            'proto', 'test',
            '--proto-file', test_proto_file
        ])
        
        assert result.exit_code == 0
        assert 'Syntax validation: PASSED' in result.output
        assert 'Compilation test: PASSED' in result.output
        assert 'All tests passed' in result.output
    
    @patch('testdatapy.schema.compiler.ProtobufCompiler')
    def test_schema_test_invalid_proto(self, mock_compiler_class, test_proto_file):
        """Test schema testing with invalid proto file."""
        from testdatapy.schema.exceptions import ValidationError
        
        mock_compiler = Mock()
        mock_compiler_class.return_value = mock_compiler
        mock_compiler.validate_proto_syntax.side_effect = ValidationError("Invalid syntax")
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            'proto', 'test',
            '--proto-file', test_proto_file
        ])
        
        assert result.exit_code == 1
        assert 'Syntax validation: FAILED' in result.output
        assert 'Invalid syntax' in result.output
    
    @patch('testdatapy.schema.compiler.ProtobufCompiler')
    def test_schema_test_compilation_failure(self, mock_compiler_class, test_proto_file):
        """Test schema testing with compilation failure."""
        from testdatapy.schema.exceptions import CompilationError
        
        mock_compiler = Mock()
        mock_compiler_class.return_value = mock_compiler
        mock_compiler.validate_proto_syntax.return_value = None
        mock_compiler.compile_proto.side_effect = CompilationError("Compilation error")
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            'proto', 'test',
            '--proto-file', test_proto_file
        ])
        
        assert result.exit_code == 1
        assert 'Syntax validation: PASSED' in result.output
        assert 'Compilation test: FAILED' in result.output
        assert 'Compilation error' in result.output
    
    def test_schema_test_file_not_found(self):
        """Test schema testing with non-existent file."""
        runner = CliRunner()
        result = runner.invoke(cli, [
            'proto', 'test',
            '--proto-file', '/non/existent/file.proto'
        ])
        
        assert result.exit_code == 1
        assert 'File does not exist' in result.output


class TestSchemaDocsCommand:
    """Test schema docs command."""
    
    @pytest.fixture
    def docs_test_dir(self):
        """Create test directory with documented proto files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            proto_dir = Path(temp_dir)
            
            # Well-documented proto file
            (proto_dir / "user.proto").write_text('''
syntax = "proto3";

// User service definitions
package user.v1;

// Represents a user in the system
message User {
  // Unique identifier for the user
  string id = 1;
  
  // Full name of the user
  string name = 2;
  
  // Email address for contact
  string email = 3;
  
  // User registration timestamp
  int64 created_at = 4;
}

// Service for user operations
service UserService {
  // Get user by ID
  rpc GetUser(GetUserRequest) returns (GetUserResponse);
  
  // Create a new user
  rpc CreateUser(CreateUserRequest) returns (CreateUserResponse);
}

message GetUserRequest {
  string user_id = 1;
}

message GetUserResponse {
  User user = 1;
}

message CreateUserRequest {
  User user = 1;
}

message CreateUserResponse {
  User user = 1;
}
''')
            
            yield temp_dir
    
    def test_schema_docs_help(self):
        """Test schema docs command help."""
        runner = CliRunner()
        result = runner.invoke(cli, ['proto', 'docs', '--help'])
        
        assert result.exit_code == 0
        assert '--proto-dir' in result.output
        assert '--output' in result.output
        assert '--format' in result.output
        assert '--include-services' in result.output
    
    @patch('testdatapy.schema.manager.SchemaManager')
    def test_schema_docs_markdown_format(self, mock_manager_class, docs_test_dir):
        """Test schema documentation generation in Markdown format."""
        mock_manager = Mock()
        mock_manager_class.return_value = mock_manager
        mock_manager.discover_proto_files.return_value = [
            f"{docs_test_dir}/user.proto"
        ]
        
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as output_dir:
            result = runner.invoke(cli, [
                'proto', 'docs',
                '--proto-dir', docs_test_dir,
                '--output', output_dir,
                '--format', 'markdown'
            ])
            
            assert result.exit_code == 0
            assert 'Generating documentation' in result.output
            assert 'Documentation generated' in result.output
            
            # Check that documentation files were created
            docs_file = Path(output_dir) / 'schema_docs.md'
            assert docs_file.exists()
    
    @patch('testdatapy.schema.manager.SchemaManager')
    def test_schema_docs_html_format(self, mock_manager_class, docs_test_dir):
        """Test schema documentation generation in HTML format."""
        mock_manager = Mock()
        mock_manager_class.return_value = mock_manager
        mock_manager.discover_proto_files.return_value = [
            f"{docs_test_dir}/user.proto"
        ]
        
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as output_dir:
            result = runner.invoke(cli, [
                'proto', 'docs',
                '--proto-dir', docs_test_dir,
                '--output', output_dir,
                '--format', 'html'
            ])
            
            assert result.exit_code == 0
            assert 'Generating documentation' in result.output
            
            # Check that HTML files were created
            docs_file = Path(output_dir) / 'index.html'
            assert docs_file.exists()
    
    @patch('testdatapy.schema.manager.SchemaManager')
    def test_schema_docs_exclude_services(self, mock_manager_class, docs_test_dir):
        """Test schema documentation excluding services."""
        mock_manager = Mock()
        mock_manager_class.return_value = mock_manager
        mock_manager.discover_proto_files.return_value = [
            f"{docs_test_dir}/user.proto"
        ]
        
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as output_dir:
            result = runner.invoke(cli, [
                'proto', 'docs',
                '--proto-dir', docs_test_dir,
                '--output', output_dir,
                '--no-include-services'
            ])
            
            assert result.exit_code == 0
            # Documentation should be generated without service definitions


class TestSchemaCLIIntegration:
    """Test integration scenarios for schema CLI commands."""
    
    @pytest.fixture
    def integration_test_project(self):
        """Create a complete test project structure."""
        with tempfile.TemporaryDirectory() as temp_dir:
            project_dir = Path(temp_dir)
            
            # Create proto directory
            proto_dir = project_dir / "protos"
            proto_dir.mkdir()
            
            # Create config file
            config_file = project_dir / "config.json"
            config = {
                "bootstrap.servers": "localhost:9092",
                "protobuf": {
                    "schema_paths": [str(proto_dir)],
                    "auto_compile": True
                }
            }
            config_file.write_text(json.dumps(config))
            
            # Create proto files
            (proto_dir / "customer.proto").write_text('''
syntax = "proto3";
message Customer {
  string id = 1;
  string name = 2;
  string email = 3;
}
''')
            
            (proto_dir / "order.proto").write_text('''
syntax = "proto3";
import "customer.proto";
message Order {
  string id = 1;
  Customer customer = 2;
  double amount = 3;
}
''')
            
            yield {
                'project_dir': str(project_dir),
                'proto_dir': str(proto_dir),
                'config_file': str(config_file)
            }
    
    @patch('testdatapy.schema.manager.SchemaManager')
    @patch('testdatapy.schema.compiler.ProtobufCompiler')
    def test_end_to_end_schema_workflow(self, mock_compiler_class, mock_manager_class, integration_test_project):
        """Test end-to-end schema workflow: discover -> test -> compile -> docs."""
        # Setup mocks
        mock_manager = Mock()
        mock_manager_class.return_value = mock_manager
        mock_manager.discover_proto_files.return_value = [
            f"{integration_test_project['proto_dir']}/customer.proto",
            f"{integration_test_project['proto_dir']}/order.proto"
        ]
        
        mock_compiler = Mock()
        mock_compiler_class.return_value = mock_compiler
        mock_compiler.validate_proto_syntax.return_value = None
        mock_compiler.compile_proto.return_value = {
            'success': True,
            'output_dir': '/tmp/compiled',
            'stdout': '',
            'stderr': ''
        }
        
        runner = CliRunner()
        
        # 1. Discover schemas
        result = runner.invoke(cli, [
            'proto', 'discover',
            '--proto-dir', integration_test_project['proto_dir']
        ])
        assert result.exit_code == 0
        assert 'customer.proto' in result.output
        assert 'order.proto' in result.output
        
        # 2. Test schemas
        result = runner.invoke(cli, [
            'proto', 'test',
            '--proto-file', f"{integration_test_project['proto_dir']}/customer.proto"
        ])
        assert result.exit_code == 0
        assert 'All tests passed' in result.output
        
        # 3. Compile schemas
        with tempfile.TemporaryDirectory() as output_dir:
            result = runner.invoke(cli, [
                'proto', 'compile',
                '--proto-dir', integration_test_project['proto_dir'],
                '--output-dir', output_dir
            ])
            assert result.exit_code == 0
            assert 'Compilation completed successfully' in result.output
        
        # 4. Generate documentation
        with tempfile.TemporaryDirectory() as docs_dir:
            result = runner.invoke(cli, [
                'proto', 'docs',
                '--proto-dir', integration_test_project['proto_dir'],
                '--output', docs_dir
            ])
            assert result.exit_code == 0
            assert 'Documentation generated' in result.output