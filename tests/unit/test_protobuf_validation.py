"""Unit tests for protobuf configuration validation."""

import json
import tempfile
import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from src.testdatapy.schema.validator import ProtobufConfigValidator, generate_validation_report
from src.testdatapy.schema.exceptions import ValidationError, CompilationError
from src.testdatapy.config.correlation_config import CorrelationConfig


# Base decorator for patching ProtobufCompiler
def mock_compiler(func):
    return patch('src.testdatapy.schema.validator.ProtobufCompiler')(func)


class TestProtobufConfigValidator:
    """Test ProtobufConfigValidator functionality."""
    
    @mock_compiler
    def test_validator_initialization(self, mock_compiler_class):
        """Test validator initialization."""
        mock_compiler_class.return_value = Mock()
        
        validator = ProtobufConfigValidator()
        assert validator.config is None
        assert validator.compiler is not None
        assert validator.manager is not None  # Created when config is None
        
        # With config
        config = Mock(spec=CorrelationConfig)
        validator = ProtobufConfigValidator(config)
        assert validator.config == config
        assert validator.manager is None  # Not created when config is provided
    
    @mock_compiler
    def test_validate_config_settings_no_config(self, mock_compiler_class):
        """Test validation without configuration."""
        mock_compiler_class.return_value = Mock()
        
        validator = ProtobufConfigValidator()
        results = validator.validate_config_settings()
        
        assert not results["valid"]
        assert "No configuration provided" in results["errors"]
    
    @mock_compiler
    @patch('shutil.which')
    def test_validate_global_settings_protoc_not_found(self, mock_which, mock_compiler_class):
        """Test global settings validation when protoc is not found."""
        mock_which.return_value = None
        mock_compiler_class.return_value = Mock()
        
        validator = ProtobufConfigValidator()
        settings = {
            "protoc_path": "protoc",
            "schema_paths": [],
            "timeout": 60,
            "auto_compile": True
        }
        
        results = validator._validate_global_settings(settings)
        
        assert not results["valid"]
        assert any("protoc compiler not found" in error for error in results["errors"])
    
    @mock_compiler
    @patch('shutil.which')
    def test_validate_global_settings_invalid_schema_paths(self, mock_which, mock_compiler_class):
        """Test global settings validation with invalid schema paths."""
        mock_which.return_value = "/usr/bin/protoc"
        mock_compiler_class.return_value = Mock()
        
        validator = ProtobufConfigValidator()
        settings = {
            "protoc_path": "protoc",
            "schema_paths": ["/nonexistent/path", "/another/invalid/path"],
            "timeout": 60
        }
        
        results = validator._validate_global_settings(settings)
        
        assert not results["valid"]
        assert len(results["errors"]) >= 2  # At least 2 invalid path errors
        assert any("/nonexistent/path" in error for error in results["errors"])
    
    @mock_compiler
    @patch('shutil.which')
    def test_validate_global_settings_invalid_timeout(self, mock_which, mock_compiler_class):
        """Test global settings validation with invalid timeout."""
        mock_which.return_value = "/usr/bin/protoc"
        mock_compiler_class.return_value = Mock()
        
        validator = ProtobufConfigValidator()
        settings = {
            "protoc_path": "protoc",
            "schema_paths": [],
            "timeout": -5  # Invalid negative timeout
        }
        
        results = validator._validate_global_settings(settings)
        
        assert not results["valid"]
        assert any("Invalid timeout value" in error for error in results["errors"])
    
    @mock_compiler
    @patch('shutil.which')
    def test_validate_global_settings_valid(self, mock_which, mock_compiler_class):
        """Test global settings validation with valid settings."""
        mock_which.return_value = "/usr/bin/protoc"
        mock_compiler_class.return_value = Mock()
        
        validator = ProtobufConfigValidator()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = {
                "protoc_path": "protoc",
                "schema_paths": [temp_dir],
                "timeout": 60,
                "auto_compile": True,
                "temp_compilation": False
            }
            
            results = validator._validate_global_settings(settings)
            
            assert results["valid"]
            assert len(results["errors"]) == 0
            assert results["checked_settings"]["protoc_available"] is True
            assert results["checked_settings"]["timeout"] == 60
    
    @mock_compiler
    def test_validate_single_entity_config_missing_required_fields(self, mock_compiler_class):
        """Test entity validation with missing required fields."""
        mock_compiler_class.return_value = Mock()
        
        config_dict = {
            "master_data": {
                "customers": {
                    "kafka_topic": "customers",
                    "protobuf_module": "customer_pb2"
                    # Missing protobuf_class
                }
            },
            "transactional_data": {}
        }
        
        config = CorrelationConfig(config_dict)
        validator = ProtobufConfigValidator(config)
        
        results = validator._validate_single_entity_config(
            "customers", 
            config_dict["master_data"]["customers"], 
            is_master=True
        )
        
        assert not results["valid"]
        assert any("Missing protobuf_class" in error for error in results["errors"])
    
    def test_validate_single_entity_config_valid(self):
        """Test entity validation with valid configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a dummy proto file
            proto_file = Path(temp_dir) / "customer.proto"
            proto_file.write_text("""
                syntax = "proto3";
                
                message Customer {
                    string id = 1;
                    string name = 2;
                }
            """)
            
            config_dict = {
                "master_data": {
                    "customers": {
                        "kafka_topic": "customers",
                        "protobuf_module": "customer_pb2",
                        "protobuf_class": "Customer",
                        "proto_file_path": "customer.proto",
                        "schema_path": temp_dir
                    }
                },
                "transactional_data": {}
            }
            
            config = CorrelationConfig(config_dict)
            validator = ProtobufConfigValidator(config)
            
            results = validator._validate_single_entity_config(
                "customers", 
                config_dict["master_data"]["customers"], 
                is_master=True
            )
            
            assert results["valid"]
            assert len(results["errors"]) == 0
    
    @patch.object(ProtobufConfigValidator, '_collect_proto_files')
    def test_test_schema_compilation_no_files(self, mock_collect):
        """Test schema compilation with no proto files."""
        mock_collect.return_value = {}
        
        config_dict = {"master_data": {}, "transactional_data": {}}
        config = CorrelationConfig(config_dict)
        validator = ProtobufConfigValidator(config)
        
        results = validator.test_schema_compilation()
        
        assert results["valid"]
        assert "No proto files found" in results["warnings"]
    
    @patch.object(ProtobufConfigValidator, '_collect_proto_files')
    def test_test_schema_compilation_success(self, mock_collect):
        """Test successful schema compilation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            proto_file = Path(temp_dir) / "test.proto"
            proto_file.write_text("syntax = \"proto3\"; message Test { string id = 1; }")
            
            mock_collect.return_value = {
                str(proto_file): {
                    "entity_name": "test",
                    "is_master": True,
                    "proto_file_path": "test.proto",
                    "schema_paths": [temp_dir]
                }
            }
            
            config_dict = {"master_data": {}, "transactional_data": {}}
            config = CorrelationConfig(config_dict)
            validator = ProtobufConfigValidator(config)
            
            # Mock successful compilation
            validator.compiler.compile_proto = Mock(return_value={
                "success": True,
                "generated_files": ["test_pb2.py"]
            })
            
            results = validator.test_schema_compilation()
            
            assert results["valid"]
            assert len(results["compiled_schemas"]) == 1
            assert len(results["failed_schemas"]) == 0
    
    @patch.object(ProtobufConfigValidator, '_collect_proto_files')
    def test_test_schema_compilation_failure(self, mock_collect):
        """Test schema compilation failure."""
        with tempfile.TemporaryDirectory() as temp_dir:
            proto_file = Path(temp_dir) / "test.proto"
            proto_file.write_text("invalid proto syntax")
            
            mock_collect.return_value = {
                str(proto_file): {
                    "entity_name": "test",
                    "is_master": True,
                    "proto_file_path": "test.proto",
                    "schema_paths": [temp_dir]
                }
            }
            
            config_dict = {"master_data": {}, "transactional_data": {}}
            config = CorrelationConfig(config_dict)
            validator = ProtobufConfigValidator(config)
            
            # Mock compilation failure
            validator.compiler.compile_proto = Mock(side_effect=CompilationError(
                "Syntax error",
                schema_path=str(proto_file),
                compiler_output="syntax error at line 1"
            ))
            
            results = validator.test_schema_compilation()
            
            assert not results["valid"]
            assert len(results["compiled_schemas"]) == 0
            assert len(results["failed_schemas"]) == 1
            assert any("Compilation failed" in error for error in results["errors"])
    
    def test_check_missing_dependencies(self):
        """Test missing dependencies detection."""
        validator = ProtobufConfigValidator()
        
        dependency_graph = {
            "file1.proto": ["existing.proto", "missing.proto"],
            "file2.proto": ["another_missing.proto"]
        }
        
        known_files = {"file1.proto", "file2.proto", "existing.proto"}
        
        missing = validator._check_missing_dependencies(dependency_graph, known_files)
        
        assert "missing.proto" in missing
        assert "another_missing.proto" in missing
        assert "existing.proto" not in missing
    
    def test_detect_circular_dependencies(self):
        """Test circular dependency detection."""
        validator = ProtobufConfigValidator()
        
        # Create circular dependency: A -> B -> C -> A
        dependency_graph = {
            "A.proto": ["B.proto"],
            "B.proto": ["C.proto"],
            "C.proto": ["A.proto"],
            "D.proto": []  # No circular dependency
        }
        
        cycles = validator._detect_circular_dependencies(dependency_graph)
        
        assert len(cycles) == 1
        cycle = cycles[0]
        # Should contain the circular chain
        assert "A.proto" in cycle
        assert "B.proto" in cycle
        assert "C.proto" in cycle
    
    def test_detect_no_circular_dependencies(self):
        """Test no circular dependencies detection."""
        validator = ProtobufConfigValidator()
        
        # No circular dependencies
        dependency_graph = {
            "A.proto": ["B.proto"],
            "B.proto": ["C.proto"],
            "C.proto": [],
            "D.proto": ["C.proto"]
        }
        
        cycles = validator._detect_circular_dependencies(dependency_graph)
        
        assert len(cycles) == 0
    
    def test_generate_summary(self):
        """Test validation summary generation."""
        validator = ProtobufConfigValidator()
        
        results = {
            "valid": True,
            "errors": ["Error 1", "Error 2"],
            "warnings": ["Warning 1"],
            "config_validation": {
                "valid": True,
                "entities_checked": 3,
                "errors": [],
                "warnings": ["Config warning"]
            },
            "schema_compilation": {
                "valid": True,
                "test_details": {
                    "total_files": 5,
                    "successful_compilations": 4,
                    "failed_compilations": 1,
                    "success_rate": 0.8
                }
            },
            "dependency_validation": {
                "valid": False,
                "validation_details": {
                    "total_proto_files": 5,
                    "total_dependencies": 10,
                    "missing_count": 2,
                    "circular_count": 1
                }
            }
        }
        
        summary = validator._generate_summary(results)
        
        assert summary["overall_valid"] is True
        assert summary["total_errors"] == 2
        assert summary["total_warnings"] == 1
        assert len(summary["components_checked"]) == 3
        
        # Check component summaries
        config_comp = next(c for c in summary["components_checked"] if c["component"] == "configuration")
        assert config_comp["entities_checked"] == 3
        
        schema_comp = next(c for c in summary["components_checked"] if c["component"] == "schema_compilation")
        assert schema_comp["total_files"] == 5
        assert schema_comp["success_rate"] == 0.8
        
        dep_comp = next(c for c in summary["components_checked"] if c["component"] == "dependencies")
        assert dep_comp["missing_dependencies"] == 2
        assert dep_comp["circular_dependencies"] == 1


class TestValidationReportGeneration:
    """Test validation report generation."""
    
    def test_generate_validation_report_json(self):
        """Test JSON format report generation."""
        results = {
            "valid": True,
            "errors": [],
            "warnings": ["Test warning"],
            "summary": {
                "overall_valid": True,
                "total_errors": 0,
                "total_warnings": 1
            }
        }
        
        report = generate_validation_report(results, "json")
        
        # Should be valid JSON
        parsed = json.loads(report)
        assert parsed["valid"] is True
        assert len(parsed["warnings"]) == 1
    
    def test_generate_validation_report_text_valid(self):
        """Test text format report generation for valid config."""
        results = {
            "valid": True,
            "errors": [],
            "warnings": ["Minor warning"],
            "summary": {
                "overall_valid": True,
                "total_errors": 0,
                "total_warnings": 1,
                "components_checked": [
                    {
                        "component": "configuration",
                        "valid": True,
                        "entities_checked": 2
                    },
                    {
                        "component": "schema_compilation",
                        "valid": True,
                        "total_files": 3,
                        "successful": 3,
                        "success_rate": 1.0
                    }
                ]
            }
        }
        
        report = generate_validation_report(results, "text")
        
        assert "PROTOBUF CONFIGURATION VALIDATION REPORT" in report
        assert "✅ Overall Status: VALID" in report
        assert "Total Errors: 0" in report
        assert "Total Warnings: 1" in report
        assert "Configuration:" in report
        assert "Schema_compilation:" in report
        assert "⚠️  WARNINGS:" in report
        assert "Minor warning" in report
    
    def test_generate_validation_report_text_invalid(self):
        """Test text format report generation for invalid config."""
        results = {
            "valid": False,
            "errors": ["Critical error 1", "Critical error 2"],
            "warnings": [],
            "summary": {
                "overall_valid": False,
                "total_errors": 2,
                "total_warnings": 0,
                "components_checked": [
                    {
                        "component": "dependencies",
                        "valid": False,
                        "total_proto_files": 2,
                        "missing_dependencies": 1,
                        "circular_dependencies": 1
                    }
                ]
            },
            "schema_compilation": {
                "failed_schemas": [
                    {
                        "proto_file": "/path/to/test.proto",
                        "entity": "test_entity",
                        "error": "Compilation failed"
                    }
                ]
            },
            "dependency_validation": {
                "missing_dependencies": ["missing.proto"],
                "circular_dependencies": [["A.proto", "B.proto", "A.proto"]]
            }
        }
        
        report = generate_validation_report(results, "text")
        
        assert "❌ Overall Status: INVALID" in report
        assert "Total Errors: 2" in report
        assert "❌ ERRORS:" in report
        assert "Critical error 1" in report
        assert "Critical error 2" in report
        assert "🚫 FAILED SCHEMA COMPILATIONS:" in report
        assert "test.proto" in report
        assert "🔍 MISSING DEPENDENCIES:" in report
        assert "missing.proto" in report
        assert "🔄 CIRCULAR DEPENDENCIES:" in report
        assert "A.proto -> B.proto -> A.proto" in report


class TestProtobufConfigValidatorIntegration:
    """Integration tests for ProtobufConfigValidator."""
    
    def test_validate_all_with_real_config(self):
        """Test complete validation with a real configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a valid proto file
            proto_file = Path(temp_dir) / "customer.proto"
            proto_file.write_text("""
                syntax = "proto3";
                
                package testdata;
                
                message Customer {
                    string customer_id = 1;
                    string name = 2;
                    string email = 3;
                    int32 age = 4;
                }
            """)
            
            # Create a correlation config file
            config_file = Path(temp_dir) / "config.yaml"
            config_content = f"""
protobuf_settings:
  schema_paths:
    - {temp_dir}
  auto_compile: true
  temp_compilation: true
  timeout: 60

master_data:
  customers:
    kafka_topic: "customers"
    protobuf_module: "customer_pb2"
    protobuf_class: "Customer"
    proto_file_path: "customer.proto"
    schema_path: "{temp_dir}"

transactional_data: {{}}
"""
            config_file.write_text(config_content)
            
            # Mock protoc availability
            with patch('shutil.which', return_value="/usr/bin/protoc"):
                # Mock successful compilation
                with patch.object(ProtobufConfigValidator, 'test_schema_compilation') as mock_compile:
                    mock_compile.return_value = {
                        "valid": True,
                        "errors": [],
                        "warnings": [],
                        "compiled_schemas": [{"proto_file": str(proto_file)}],
                        "failed_schemas": [],
                        "test_details": {
                            "total_files": 1,
                            "successful_compilations": 1,
                            "failed_compilations": 0,
                            "success_rate": 1.0
                        }
                    }
                    
                    # Mock dependency validation
                    with patch.object(ProtobufConfigValidator, 'validate_dependencies') as mock_deps:
                        mock_deps.return_value = {
                            "valid": True,
                            "errors": [],
                            "warnings": [],
                            "dependency_graph": {str(proto_file): []},
                            "missing_dependencies": [],
                            "circular_dependencies": [],
                            "validation_details": {
                                "total_proto_files": 1,
                                "total_dependencies": 0,
                                "missing_count": 0,
                                "circular_count": 0
                            }
                        }
                        
                        validator = ProtobufConfigValidator()
                        results = validator.validate_all(str(config_file))
                        
                        assert results["valid"] is True
                        assert len(results["errors"]) == 0
                        assert "config_validation" in results
                        assert "schema_compilation" in results
                        assert "dependency_validation" in results
                        assert "summary" in results
    
    def test_validate_all_with_invalid_config(self):
        """Test complete validation with invalid configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create an invalid config file (missing required fields)
            config_file = Path(temp_dir) / "config.yaml"
            config_content = """
protobuf_settings:
  schema_paths:
    - /nonexistent/path
  timeout: -5  # Invalid timeout

master_data:
  customers:
    kafka_topic: "customers"
    protobuf_module: "customer_pb2"
    # Missing protobuf_class

transactional_data: {}
"""
            config_file.write_text(config_content)
            
            # Mock protoc not available
            with patch('shutil.which', return_value=None):
                validator = ProtobufConfigValidator()
                results = validator.validate_all(str(config_file))
                
                assert results["valid"] is False
                assert len(results["errors"]) > 0
                
                # Should have multiple validation errors
                error_text = " ".join(results["errors"])
                assert "protoc compiler not found" in error_text or "Invalid schema path" in error_text