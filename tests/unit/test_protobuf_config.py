"""Tests for enhanced protobuf configuration support."""

import json
import tempfile
from pathlib import Path
from typing import Any, Dict

import pytest
import yaml

from testdatapy.config.correlation_config import CorrelationConfig, ValidationError
from testdatapy.config.loader import AppConfig


class TestProtobufCorrelationConfig:
    """Test enhanced protobuf support in CorrelationConfig."""
    
    def test_protobuf_settings_validation_valid(self):
        """Test validation of valid protobuf_settings section."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = {
                "protobuf_settings": {
                    "schema_paths": [temp_dir],
                    "auto_compile": True,
                    "temp_compilation": True,
                    "protoc_path": "/usr/bin/protoc"
                },
                "master_data": {
                    "customers": {
                        "kafka_topic": "customers",
                        "protobuf_module": "customer_pb2",
                        "protobuf_class": "Customer"
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config)
            
            # Should not raise exception for valid config
            assert correlation_config.get_protobuf_settings() is not None
            assert correlation_config.get_protobuf_settings()["auto_compile"] is True
            assert temp_dir in correlation_config.get_protobuf_settings()["schema_paths"]
    
    def test_protobuf_settings_validation_invalid_path(self):
        """Test validation rejects invalid schema paths."""
        config = {
            "protobuf_settings": {
                "schema_paths": ["/non/existent/path"],
                "auto_compile": True
            },
            "master_data": {
                "customers": {
                    "kafka_topic": "customers"
                }
            }
        }
        
        with pytest.raises(ValidationError) as exc_info:
            CorrelationConfig(config)
        
        assert "schema_paths" in str(exc_info.value)
        assert "/non/existent/path" in str(exc_info.value)
    
    def test_protobuf_settings_validation_invalid_types(self):
        """Test validation rejects invalid data types."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = {
                "protobuf_settings": {
                    "schema_paths": temp_dir,  # Should be list, not string
                    "auto_compile": "yes",     # Should be boolean, not string
                    "temp_compilation": 1      # Should be boolean, not int
                },
                "master_data": {
                    "customers": {
                        "kafka_topic": "customers"
                    }
                }
            }
            
            with pytest.raises(ValidationError) as exc_info:
                CorrelationConfig(config)
            
            assert "protobuf_settings" in str(exc_info.value)
    
    def test_protobuf_settings_optional(self):
        """Test protobuf_settings section is optional."""
        config = {
            "master_data": {
                "customers": {
                    "kafka_topic": "customers"
                }
            }
        }
        
        correlation_config = CorrelationConfig(config)
        
        # Should work without protobuf_settings
        assert correlation_config.get_protobuf_settings() is None
    
    def test_protobuf_settings_empty(self):
        """Test empty protobuf_settings section."""
        config = {
            "protobuf_settings": {},
            "master_data": {
                "customers": {
                    "kafka_topic": "customers"
                }
            }
        }
        
        correlation_config = CorrelationConfig(config)
        settings = correlation_config.get_protobuf_settings()
        
        # Should use defaults
        assert settings["auto_compile"] is False
        assert settings["temp_compilation"] is True
        assert settings["schema_paths"] == []
    
    def test_protobuf_settings_merge_with_entity_config(self):
        """Test merging global protobuf settings with entity-specific config."""
        with tempfile.TemporaryDirectory() as global_schema_dir:
            with tempfile.TemporaryDirectory() as entity_schema_dir:
                config = {
                    "protobuf_settings": {
                        "schema_paths": [global_schema_dir],
                        "auto_compile": True
                    },
                    "master_data": {
                        "customers": {
                            "kafka_topic": "customers",
                            "protobuf_module": "customer_pb2",
                            "protobuf_class": "Customer",
                            "schema_path": entity_schema_dir
                        }
                    }
                }
                
                correlation_config = CorrelationConfig(config)
                merged_config = correlation_config.get_merged_protobuf_config("customers", is_master=True)
                
                # Should merge global and entity-specific settings
                assert merged_config["auto_compile"] is True
                assert global_schema_dir in merged_config["schema_paths"]
                assert entity_schema_dir == merged_config["entity_schema_path"]
                assert merged_config["protobuf_module"] == "customer_pb2"
                assert merged_config["protobuf_class"] == "Customer"
    
    def test_protobuf_settings_entity_override(self):
        """Test entity-specific settings override global settings."""
        with tempfile.TemporaryDirectory() as schema_dir:
            config = {
                "protobuf_settings": {
                    "schema_paths": [schema_dir],
                    "auto_compile": False,
                    "temp_compilation": False
                },
                "master_data": {
                    "customers": {
                        "kafka_topic": "customers",
                        "protobuf_module": "customer_pb2",
                        "protobuf_class": "Customer",
                        "auto_compile": True  # Override global setting
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config)
            merged_config = correlation_config.get_merged_protobuf_config("customers", is_master=True)
            
            # Entity setting should override global
            assert merged_config["auto_compile"] is True
            assert merged_config["temp_compilation"] is False  # Global setting preserved
    
    def test_get_effective_schema_paths(self):
        """Test getting effective schema paths with priority ordering."""
        with tempfile.TemporaryDirectory() as global_dir1:
            with tempfile.TemporaryDirectory() as global_dir2:
                with tempfile.TemporaryDirectory() as entity_dir:
                    config = {
                        "protobuf_settings": {
                            "schema_paths": [global_dir1, global_dir2]
                        },
                        "master_data": {
                            "customers": {
                                "kafka_topic": "customers",
                                "schema_path": entity_dir
                            }
                        }
                    }
                    
                    correlation_config = CorrelationConfig(config)
                    paths = correlation_config.get_effective_schema_paths("customers", is_master=True)
                    
                    # Entity path should have highest priority
                    assert paths[0] == entity_dir
                    assert global_dir1 in paths
                    assert global_dir2 in paths
                    assert len(paths) == 3
    
    def test_validate_protobuf_dependencies(self):
        """Test validation of protobuf dependencies."""
        with tempfile.TemporaryDirectory() as schema_dir:
            # Create test proto files with dependencies
            proto_dir = Path(schema_dir)
            
            # Common proto
            (proto_dir / "common.proto").write_text('''
syntax = "proto3";
message Common { string id = 1; }
''')
            
            # Customer proto with dependency
            (proto_dir / "customer.proto").write_text('''
syntax = "proto3";
import "common.proto";
message Customer { Common common = 1; string name = 2; }
''')
            
            config = {
                "protobuf_settings": {
                    "schema_paths": [schema_dir],
                    "validate_dependencies": True
                },
                "master_data": {
                    "customers": {
                        "kafka_topic": "customers",
                        "protobuf_module": "customer_pb2",
                        "protobuf_class": "Customer",
                        "proto_file_path": "customer.proto"
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config)
            
            # Should validate dependencies successfully
            dependencies = correlation_config.get_protobuf_dependencies("customers", is_master=True)
            assert len(dependencies) >= 1
            assert any("common.proto" in dep for dep in dependencies)


class TestProtobufAppConfig:
    """Test enhanced protobuf support in AppConfig."""
    
    def test_protobuf_config_section(self):
        """Test new protobuf configuration section in AppConfig."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_data = {
                "bootstrap.servers": "localhost:9092",
                "protobuf": {
                    "schema_paths": [temp_dir],
                    "auto_compile": True,
                    "temp_compilation": False,
                    "protoc_path": "/usr/bin/protoc",
                    "timeout": 60
                }
            }
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                json.dump(config_data, f)
                config_file = f.name
            
            try:
                app_config = AppConfig.from_file(config_file)
                
                assert app_config.protobuf is not None
                assert app_config.protobuf.auto_compile is True
                assert app_config.protobuf.temp_compilation is False
                assert temp_dir in app_config.protobuf.schema_paths
                assert app_config.protobuf.protoc_path == "/usr/bin/protoc"
                assert app_config.protobuf.timeout == 60
            finally:
                Path(config_file).unlink()
    
    def test_protobuf_config_defaults(self):
        """Test default values for protobuf configuration."""
        config_data = {
            "bootstrap.servers": "localhost:9092"
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            config_file = f.name
        
        try:
            app_config = AppConfig.from_file(config_file)
            
            # Should use default protobuf config
            assert app_config.protobuf is not None
            assert app_config.protobuf.auto_compile is False
            assert app_config.protobuf.temp_compilation is True
            assert app_config.protobuf.schema_paths == []
            assert app_config.protobuf.protoc_path is None
            assert app_config.protobuf.timeout == 60
        finally:
            Path(config_file).unlink()
    
    def test_protobuf_config_validation(self):
        """Test validation of protobuf configuration values."""
        config_data = {
            "bootstrap.servers": "localhost:9092",
            "protobuf": {
                "schema_paths": ["/non/existent/path"],
                "timeout": -10  # Invalid negative timeout
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            config_file = f.name
        
        try:
            with pytest.raises(Exception) as exc_info:
                AppConfig.from_file(config_file)
            
            # Should fail validation
            assert "validation" in str(exc_info.value).lower() or "error" in str(exc_info.value).lower()
        finally:
            Path(config_file).unlink()
    
    def test_protobuf_config_to_dict(self):
        """Test conversion of protobuf config to dictionary."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_data = {
                "bootstrap.servers": "localhost:9092",
                "protobuf": {
                    "schema_paths": [temp_dir],
                    "auto_compile": True
                }
            }
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                json.dump(config_data, f)
                config_file = f.name
            
            try:
                app_config = AppConfig.from_file(config_file)
                protobuf_dict = app_config.to_protobuf_config()
                
                assert protobuf_dict["auto_compile"] is True
                assert temp_dir in protobuf_dict["schema_paths"]
                assert "timeout" in protobuf_dict
            finally:
                Path(config_file).unlink()


class TestProtobufConfigIntegration:
    """Test integration between CorrelationConfig and AppConfig for protobuf."""
    
    def test_merge_app_and_correlation_config(self):
        """Test merging protobuf settings from AppConfig and CorrelationConfig."""
        with tempfile.TemporaryDirectory() as app_schema_dir:
            with tempfile.TemporaryDirectory() as corr_schema_dir:
                # App config
                app_config_data = {
                    "bootstrap.servers": "localhost:9092",
                    "protobuf": {
                        "schema_paths": [app_schema_dir],
                        "auto_compile": False,
                        "protoc_path": "/usr/bin/protoc"
                    }
                }
                
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                    json.dump(app_config_data, f)
                    app_config_file = f.name
                
                # Correlation config
                correlation_config_data = {
                    "protobuf_settings": {
                        "schema_paths": [corr_schema_dir],
                        "auto_compile": True  # Override app config
                    },
                    "master_data": {
                        "customers": {
                            "kafka_topic": "customers",
                            "protobuf_module": "customer_pb2",
                            "protobuf_class": "Customer"
                        }
                    }
                }
                
                try:
                    app_config = AppConfig.from_file(app_config_file)
                    correlation_config = CorrelationConfig(correlation_config_data)
                    
                    # Test merging
                    merged_config = correlation_config.merge_with_app_config(app_config)
                    
                    # Correlation config should override app config
                    assert merged_config["auto_compile"] is True
                    
                    # Both schema paths should be included
                    assert app_schema_dir in merged_config["schema_paths"]
                    assert corr_schema_dir in merged_config["schema_paths"]
                    
                    # App config values should be preserved when not overridden
                    assert merged_config["protoc_path"] == "/usr/bin/protoc"
                    
                finally:
                    Path(app_config_file).unlink()
    
    def test_config_precedence_order(self):
        """Test configuration precedence: entity > correlation > app > defaults."""
        with tempfile.TemporaryDirectory() as app_dir:
            with tempfile.TemporaryDirectory() as corr_dir:
                with tempfile.TemporaryDirectory() as entity_dir:
                    # App config (lowest precedence)
                    app_config_data = {
                        "bootstrap.servers": "localhost:9092",
                        "protobuf": {
                            "schema_paths": [app_dir],
                            "auto_compile": False,
                            "temp_compilation": False
                        }
                    }
                    
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                        json.dump(app_config_data, f)
                        app_config_file = f.name
                    
                    # Correlation config (medium precedence)
                    correlation_config_data = {
                        "protobuf_settings": {
                            "schema_paths": [corr_dir],
                            "auto_compile": True  # Override app
                        },
                        "master_data": {
                            "customers": {
                                "kafka_topic": "customers",
                                "protobuf_module": "customer_pb2",
                                "protobuf_class": "Customer",
                                "schema_path": entity_dir,  # Entity level (highest precedence)
                                "temp_compilation": True    # Override correlation
                            }
                        }
                    }
                    
                    try:
                        app_config = AppConfig.from_file(app_config_file)
                        correlation_config = CorrelationConfig(correlation_config_data)
                        
                        final_config = correlation_config.get_final_protobuf_config(
                            "customers", 
                            is_master=True, 
                            app_config=app_config
                        )
                        
                        # Entity level should win
                        assert entity_dir in final_config["schema_paths"]
                        assert final_config["temp_compilation"] is True
                        
                        # Correlation level should override app level
                        assert final_config["auto_compile"] is True
                        
                        # All schema paths should be included with proper precedence
                        schema_paths = final_config["schema_paths"]
                        assert schema_paths.index(entity_dir) < schema_paths.index(corr_dir)
                        assert schema_paths.index(corr_dir) < schema_paths.index(app_dir)
                        
                    finally:
                        Path(app_config_file).unlink()
    
    def test_protobuf_config_direct_initialization(self):
        """Test protobuf configuration through direct initialization."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Test direct ProtobufConfig creation
            from testdatapy.config.loader import ProtobufConfig
            
            protobuf_config = ProtobufConfig(
                schema_paths=[temp_dir],
                auto_compile=True,
                temp_compilation=False,
                timeout=120
            )
            
            assert protobuf_config.auto_compile is True
            assert protobuf_config.temp_compilation is False
            assert protobuf_config.timeout == 120
            assert temp_dir in protobuf_config.schema_paths
            
            # Test AppConfig with explicit protobuf config
            app_config = AppConfig(protobuf=protobuf_config)
            
            protobuf_dict = app_config.to_protobuf_config()
            assert protobuf_dict["auto_compile"] is True
            assert protobuf_dict["temp_compilation"] is False
            assert protobuf_dict["timeout"] == 120
            assert temp_dir in protobuf_dict["schema_paths"]


class TestProtobufConfigUtilities:
    """Test utility functions for protobuf configuration."""
    
    def test_validate_protobuf_config_complete(self):
        """Test comprehensive protobuf configuration validation."""
        with tempfile.TemporaryDirectory() as schema_dir:
            config = {
                "protobuf_settings": {
                    "schema_paths": [schema_dir],
                    "auto_compile": True,
                    "temp_compilation": True,
                    "protoc_path": "/usr/bin/protoc",
                    "timeout": 60,
                    "validate_dependencies": True
                },
                "master_data": {
                    "customers": {
                        "kafka_topic": "customers",
                        "protobuf_module": "customer_pb2",
                        "protobuf_class": "Customer",
                        "schema_path": schema_dir
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config)
            
            # Should validate successfully
            validation_result = correlation_config.validate_protobuf_config_complete()
            assert validation_result["valid"] is True
            assert len(validation_result["warnings"]) >= 0
            assert len(validation_result["errors"]) == 0
    
    def test_get_protobuf_compilation_order(self):
        """Test getting protobuf compilation order based on dependencies."""
        with tempfile.TemporaryDirectory() as schema_dir:
            proto_dir = Path(schema_dir)
            
            # Create dependency chain: base -> common -> customer
            (proto_dir / "base.proto").write_text('''
syntax = "proto3";
message Base { string id = 1; }
''')
            
            (proto_dir / "common.proto").write_text('''
syntax = "proto3";
import "base.proto";
message Common { Base base = 1; string data = 2; }
''')
            
            (proto_dir / "customer.proto").write_text('''
syntax = "proto3";
import "common.proto";
message Customer { Common common = 1; string name = 2; }
''')
            
            config = {
                "protobuf_settings": {
                    "schema_paths": [schema_dir]
                },
                "master_data": {
                    "customers": {
                        "kafka_topic": "customers",
                        "protobuf_module": "customer_pb2",
                        "protobuf_class": "Customer",
                        "proto_file_path": "customer.proto"
                    }
                }
            }
            
            correlation_config = CorrelationConfig(config)
            compilation_order = correlation_config.get_protobuf_compilation_order()
            
            # Should return files in dependency order
            assert len(compilation_order) >= 1
            # base.proto should come before common.proto, which should come before customer.proto
            base_idx = next((i for i, f in enumerate(compilation_order) if "base.proto" in f), -1)
            common_idx = next((i for i, f in enumerate(compilation_order) if "common.proto" in f), -1)
            customer_idx = next((i for i, f in enumerate(compilation_order) if "customer.proto" in f), -1)
            
            if base_idx >= 0 and common_idx >= 0:
                assert base_idx < common_idx
            if common_idx >= 0 and customer_idx >= 0:
                assert common_idx < customer_idx