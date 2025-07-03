"""Comprehensive unit tests for SchemaRegistryManager."""

import json
import tempfile
import time
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch, mock_open

import pytest
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError

from testdatapy.schema.registry_manager import SchemaRegistryManager
from testdatapy.schema.exceptions import (
    SchemaRegistrationError,
    SchemaNotFoundError,
    ValidationError,
    CompilationError
)


class TestSchemaRegistryManagerInitialization:
    """Test SchemaRegistryManager initialization."""
    
    @patch('testdatapy.schema.registry_manager.SchemaRegistryClient')
    @patch('testdatapy.schema.registry_manager.ProtobufCompiler')
    def test_initialization_success(self, mock_compiler_class, mock_client_class):
        """Test successful initialization."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_subjects.return_value = []
        
        mock_compiler = Mock()
        mock_compiler_class.return_value = mock_compiler
        
        manager = SchemaRegistryManager(
            schema_registry_url="http://localhost:8081",
            config={"auth": "basic"},
            auto_register=True,
            subject_naming_strategy="topic_name"
        )
        
        assert manager.schema_registry_url == "http://localhost:8081"
        assert manager.auto_register is True
        assert manager.subject_naming_strategy == "topic_name"
        assert manager.client == mock_client
        assert manager.compiler == mock_compiler
        assert isinstance(manager._schema_cache, dict)
        
        # Verify client configuration
        expected_config = {
            "url": "http://localhost:8081",
            "auth": "basic"
        }
        mock_client_class.assert_called_once_with(expected_config)
        mock_client.get_subjects.assert_called_once()
    
    @patch('testdatapy.schema.registry_manager.SchemaRegistryClient')
    def test_initialization_connection_failure(self, mock_client_class):
        """Test initialization with Schema Registry connection failure."""
        mock_client_class.side_effect = Exception("Connection failed")
        
        with pytest.raises(ValidationError) as exc_info:
            SchemaRegistryManager("http://invalid:8081")
        
        assert "Cannot connect to Schema Registry" in str(exc_info.value)
        assert "Connection failed" in str(exc_info.value)
    
    @patch('testdatapy.schema.registry_manager.SchemaRegistryClient')
    def test_initialization_test_connection_failure(self, mock_client_class):
        """Test initialization with test connection failure."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.get_subjects.side_effect = Exception("Auth failed")
        
        with pytest.raises(ValidationError) as exc_info:
            SchemaRegistryManager("http://localhost:8081")
        
        assert "Cannot connect to Schema Registry" in str(exc_info.value)
        assert "Auth failed" in str(exc_info.value)


class TestSchemaRegistrationFromFile:
    """Test automatic schema registration from proto files."""
    
    @pytest.fixture
    def manager(self):
        """Create SchemaRegistryManager with mocked dependencies."""
        with patch('testdatapy.schema.registry_manager.SchemaRegistryClient') as mock_client_class:
            with patch('testdatapy.schema.registry_manager.ProtobufCompiler') as mock_compiler_class:
                mock_client = Mock()
                mock_client_class.return_value = mock_client
                mock_client.get_subjects.return_value = []
                
                mock_compiler = Mock()
                mock_compiler_class.return_value = mock_compiler
                
                return SchemaRegistryManager("http://localhost:8081")
    
    @pytest.fixture
    def proto_file(self):
        """Create temporary proto file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.proto', delete=False) as f:
            f.write('''syntax = "proto3";

package com.example;

// Customer message
message Customer {
  string id = 1;
  string name = 2;
  string email = 3;
}
''')
            f.flush()  # Ensure content is written
            yield f.name
        Path(f.name).unlink()
    
    def test_register_protobuf_schema_success(self, manager, proto_file):
        """Test successful protobuf schema registration."""
        # Mock compiler validation
        manager.compiler.validate_proto_syntax.return_value = None
        
        # Mock existing schema check (no existing schema)
        manager._get_existing_schema = Mock(return_value=None)
        
        # Mock schema registration
        manager._register_schema = Mock(return_value={
            "schema_id": 123,
            "version": 1,
            "subject": "test-topic-value",
            "schema_type": "PROTOBUF"
        })
        
        # Mock schema caching
        manager._cache_schema = Mock()
        
        result = manager.register_protobuf_schema_from_file(
            proto_file_path=proto_file,
            topic="test-topic"
        )
        
        assert result["action"] == "registered"
        assert result["subject"] == "test-topic-value"
        assert result["schema_id"] == 123
        assert result["version"] == 1
        assert "duration" in result
        
        # Verify method calls
        manager.compiler.validate_proto_syntax.assert_called_once_with(proto_file)
        manager._get_existing_schema.assert_called_once_with("test-topic-value")
        manager._register_schema.assert_called_once()
        manager._cache_schema.assert_called_once()
    
    def test_register_protobuf_schema_file_not_found(self, manager):
        """Test registration with non-existent proto file."""
        with pytest.raises(SchemaNotFoundError) as exc_info:
            manager.register_protobuf_schema_from_file(
                proto_file_path="/non/existent/file.proto",
                topic="test-topic"
            )
        
        assert exc_info.value.schema_path == "/non/existent/file.proto"
        assert exc_info.value.schema_type == "protobuf schema"
    
    def test_register_protobuf_schema_compilation_error(self, manager, proto_file):
        """Test registration with compilation error."""
        manager.compiler.validate_proto_syntax.side_effect = CompilationError(
            "Invalid syntax",
            schema_path=proto_file,
            compiler_output="error details",
            exit_code=1
        )
        
        with pytest.raises(CompilationError) as exc_info:
            manager.register_protobuf_schema_from_file(
                proto_file_path=proto_file,
                topic="test-topic"
            )
        
        assert "Invalid syntax" in str(exc_info.value)
        assert exc_info.value.schema_path == proto_file
    
    def test_register_protobuf_schema_already_exists(self, manager, proto_file):
        """Test registration when equivalent schema already exists."""
        # Mock compiler validation
        manager.compiler.validate_proto_syntax.return_value = None
        
        # Mock existing schema (equivalent)
        existing_schema = {
            "id": 456,
            "version": 2,
            "schema": "syntax = \"proto3\"; package com.example; message Customer { string id = 1; string name = 2; string email = 3; }",
            "schema_type": "PROTOBUF"
        }
        manager._get_existing_schema = Mock(return_value=existing_schema)
        manager._are_schemas_equivalent = Mock(return_value=True)
        
        result = manager.register_protobuf_schema_from_file(
            proto_file_path=proto_file,
            topic="test-topic"
        )
        
        assert result["action"] == "already_exists"
        assert result["schema_id"] == 456
        assert result["version"] == 2
        assert "duration" in result
    
    def test_register_protobuf_schema_force_register(self, manager, proto_file):
        """Test registration with force_register=True."""
        # Mock compiler validation
        manager.compiler.validate_proto_syntax.return_value = None
        
        # Mock existing schema (should be ignored with force_register)
        existing_schema = {
            "id": 456,
            "version": 2,
            "schema": "existing schema",
            "schema_type": "PROTOBUF"
        }
        manager._get_existing_schema = Mock(return_value=existing_schema)
        
        # Mock schema registration
        manager._register_schema = Mock(return_value={
            "schema_id": 789,
            "version": 3,
            "subject": "test-topic-value",
            "schema_type": "PROTOBUF"
        })
        manager._cache_schema = Mock()
        
        result = manager.register_protobuf_schema_from_file(
            proto_file_path=proto_file,
            topic="test-topic",
            force_register=True
        )
        
        assert result["action"] == "registered"
        assert result["schema_id"] == 789
        
        # Should not check existing schema with force_register
        manager._get_existing_schema.assert_not_called()
    
    def test_register_protobuf_schema_with_message_name(self, manager, proto_file):
        """Test registration with specific message name."""
        manager.compiler.validate_proto_syntax.return_value = None
        manager._get_existing_schema = Mock(return_value=None)
        manager._register_schema = Mock(return_value={
            "schema_id": 123,
            "version": 1,
            "subject": "test-topic-Customer",
            "schema_type": "PROTOBUF"
        })
        manager._cache_schema = Mock()
        
        # Set subject naming strategy to include message name
        manager.subject_naming_strategy = "topic_record_name"
        
        result = manager.register_protobuf_schema_from_file(
            proto_file_path=proto_file,
            topic="test-topic",
            message_name="Customer"
        )
        
        assert result["subject"] == "test-topic-Customer"


class TestBatchSchemaRegistration:
    """Test batch schema registration from directory."""
    
    @pytest.fixture
    def manager(self):
        """Create SchemaRegistryManager with mocked dependencies."""
        with patch('testdatapy.schema.registry_manager.SchemaRegistryClient') as mock_client_class:
            with patch('testdatapy.schema.registry_manager.ProtobufCompiler') as mock_compiler_class:
                mock_client = Mock()
                mock_client_class.return_value = mock_client
                mock_client.get_subjects.return_value = []
                
                mock_compiler = Mock()
                mock_compiler_class.return_value = mock_compiler
                
                return SchemaRegistryManager("http://localhost:8081")
    
    @pytest.fixture
    def proto_directory(self):
        """Create temporary directory with proto files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            proto_dir = Path(temp_dir)
            
            # Create proto files
            (proto_dir / "customer.proto").write_text('''
syntax = "proto3";
message Customer { string id = 1; }
''')
            
            (proto_dir / "order.proto").write_text('''
syntax = "proto3";
message Order { string id = 1; }
''')
            
            # Create subdirectory with proto file
            nested_dir = proto_dir / "nested"
            nested_dir.mkdir()
            (nested_dir / "product.proto").write_text('''
syntax = "proto3";
message Product { string id = 1; }
''')
            
            yield str(proto_dir)
    
    def test_register_protobuf_schemas_from_directory_success(self, manager, proto_directory):
        """Test successful batch registration from directory."""
        # Mock individual registration method
        manager.register_protobuf_schema_from_file = Mock(side_effect=[
            {"action": "registered", "schema_id": 1, "subject": "customer-value"},
            {"action": "registered", "schema_id": 2, "subject": "order-value"}
        ])
        
        results = manager.register_protobuf_schemas_from_directory(
            proto_dir=proto_directory,
            recursive=False
        )
        
        assert len(results) == 2
        assert any("customer.proto" in path for path in results.keys())
        assert any("order.proto" in path for path in results.keys())
        
        # Should register each proto file
        assert manager.register_protobuf_schema_from_file.call_count == 2
    
    def test_register_protobuf_schemas_recursive(self, manager, proto_directory):
        """Test recursive batch registration."""
        manager.register_protobuf_schema_from_file = Mock(side_effect=[
            {"action": "registered", "schema_id": 1, "subject": "customer-value"},
            {"action": "registered", "schema_id": 2, "subject": "order-value"},
            {"action": "registered", "schema_id": 3, "subject": "product-value"}
        ])
        
        results = manager.register_protobuf_schemas_from_directory(
            proto_dir=proto_directory,
            recursive=True
        )
        
        assert len(results) == 3
        assert any("product.proto" in path for path in results.keys())
        
        # Should register all proto files including nested
        assert manager.register_protobuf_schema_from_file.call_count == 3
    
    def test_register_protobuf_schemas_with_topic_mapping(self, manager, proto_directory):
        """Test batch registration with topic mapping."""
        topic_mapping = {
            "customer": "users",
            "order": "orders"
        }
        
        manager.register_protobuf_schema_from_file = Mock(return_value={
            "action": "registered", "schema_id": 1
        })
        
        manager.register_protobuf_schemas_from_directory(
            proto_dir=proto_directory,
            topic_mapping=topic_mapping,
            recursive=False
        )
        
        # Verify topics were mapped correctly
        call_args_list = manager.register_protobuf_schema_from_file.call_args_list
        topics_used = [call.args[1] for call in call_args_list]
        assert "users" in topics_used
        assert "orders" in topics_used
    
    def test_register_protobuf_schemas_no_files(self, manager):
        """Test batch registration with no proto files."""
        with tempfile.TemporaryDirectory() as empty_dir:
            results = manager.register_protobuf_schemas_from_directory(empty_dir)
            
            assert results == {}
    
    def test_register_protobuf_schemas_invalid_directory(self, manager):
        """Test batch registration with invalid directory."""
        with pytest.raises(ValidationError) as exc_info:
            manager.register_protobuf_schemas_from_directory("/non/existent/dir")
        
        assert "does not exist or is not a directory" in str(exc_info.value)
    
    def test_register_protobuf_schemas_partial_failure(self, manager, proto_directory):
        """Test batch registration with some failures."""
        def side_effect(proto_file, topic, **kwargs):
            if "customer" in str(proto_file):
                return {"action": "registered", "schema_id": 1}
            else:
                raise SchemaRegistrationError(
                    message="Registration failed",
                    subject="test-subject",
                    schema_content="content"
                )
        
        manager.register_protobuf_schema_from_file = Mock(side_effect=side_effect)
        
        results = manager.register_protobuf_schemas_from_directory(
            proto_dir=proto_directory,
            recursive=False
        )
        
        # Should have both success and failure results
        success_results = [r for r in results.values() if r.get("action") == "registered"]
        failure_results = [r for r in results.values() if r.get("action") == "failed"]
        
        assert len(success_results) == 1
        assert len(failure_results) == 1


class TestSchemaInfoExtraction:
    """Test schema information extraction from proto content."""
    
    @pytest.fixture
    def manager(self):
        """Create SchemaRegistryManager with mocked dependencies."""
        with patch('testdatapy.schema.registry_manager.SchemaRegistryClient') as mock_client_class:
            with patch('testdatapy.schema.registry_manager.ProtobufCompiler') as mock_compiler_class:
                mock_client = Mock()
                mock_client_class.return_value = mock_client
                mock_client.get_subjects.return_value = []
                
                mock_compiler = Mock()
                mock_compiler_class.return_value = mock_compiler
                
                return SchemaRegistryManager("http://localhost:8081")
    
    def test_extract_schema_info_complete(self, manager):
        """Test extraction of complete schema information."""
        proto_content = '''
syntax = "proto3";

package com.example.user;

import "common.proto";
import "timestamp.proto";

// User message definition
message User {
  string id = 1;
  string name = 2;
}

// UserList message
message UserList {
  repeated User users = 1;
}
'''
        
        schema_info = manager._extract_schema_info(proto_content)
        
        assert schema_info["package"] == "com.example.user"
        assert schema_info["syntax"] == "proto3"
        assert schema_info["messages"] == ["User", "UserList"]
        assert schema_info["message_name"] == "User"  # First message as default
        assert "common.proto" in schema_info["imports"]
        assert "timestamp.proto" in schema_info["imports"]
    
    def test_extract_schema_info_with_specific_message(self, manager):
        """Test extraction with specific message name."""
        proto_content = '''
syntax = "proto3";

message User { string id = 1; }
message Admin { string id = 1; }
'''
        
        schema_info = manager._extract_schema_info(proto_content, message_name="Admin")
        
        assert schema_info["message_name"] == "Admin"
        assert "User" in schema_info["messages"]
        assert "Admin" in schema_info["messages"]
    
    def test_extract_schema_info_invalid_message_name(self, manager):
        """Test extraction with invalid message name."""
        proto_content = '''
syntax = "proto3";
message User { string id = 1; }
'''
        
        with pytest.raises(ValidationError) as exc_info:
            manager._extract_schema_info(proto_content, message_name="InvalidMessage")
        
        assert "Message 'InvalidMessage' not found" in str(exc_info.value)
    
    def test_extract_schema_info_no_messages(self, manager):
        """Test extraction with no message definitions."""
        proto_content = '''
syntax = "proto3";
package com.example;
// No messages
'''
        
        with pytest.raises(ValidationError) as exc_info:
            manager._extract_schema_info(proto_content)
        
        assert "No message definitions found" in str(exc_info.value)
    
    def test_extract_schema_info_minimal(self, manager):
        """Test extraction with minimal proto content."""
        proto_content = '''
message SimpleMessage {
  string field = 1;
}
'''
        
        schema_info = manager._extract_schema_info(proto_content)
        
        assert schema_info["messages"] == ["SimpleMessage"]
        assert schema_info["message_name"] == "SimpleMessage"
        assert schema_info.get("package") is None
        assert schema_info.get("syntax") is None
        assert schema_info["imports"] == []


class TestSubjectNaming:
    """Test subject name generation strategies."""
    
    @pytest.fixture
    def manager(self):
        """Create SchemaRegistryManager with mocked dependencies."""
        with patch('testdatapy.schema.registry_manager.SchemaRegistryClient') as mock_client_class:
            with patch('testdatapy.schema.registry_manager.ProtobufCompiler') as mock_compiler_class:
                mock_client = Mock()
                mock_client_class.return_value = mock_client
                mock_client.get_subjects.return_value = []
                
                mock_compiler = Mock()
                mock_compiler_class.return_value = mock_compiler
                
                return SchemaRegistryManager("http://localhost:8081")
    
    def test_topic_name_strategy(self, manager):
        """Test topic_name subject naming strategy."""
        manager.subject_naming_strategy = "topic_name"
        
        subject = manager._generate_subject_name(
            topic="user-events",
            package="com.example",
            message_name="User"
        )
        
        assert subject == "user-events-value"
    
    def test_record_name_strategy(self, manager):
        """Test record_name subject naming strategy."""
        manager.subject_naming_strategy = "record_name"
        
        subject = manager._generate_subject_name(
            topic="user-events",
            package="com.example",
            message_name="User"
        )
        
        assert subject == "User"
    
    def test_topic_record_name_strategy(self, manager):
        """Test topic_record_name subject naming strategy."""
        manager.subject_naming_strategy = "topic_record_name"
        
        subject = manager._generate_subject_name(
            topic="user-events",
            package="com.example",
            message_name="User"
        )
        
        assert subject == "user-events-User"
    
    def test_fallback_strategy(self, manager):
        """Test fallback to topic_name for unknown strategies."""
        manager.subject_naming_strategy = "unknown_strategy"
        
        subject = manager._generate_subject_name(
            topic="user-events",
            message_name="User"
        )
        
        assert subject == "user-events-value"
    
    def test_record_name_without_message_name(self, manager):
        """Test record_name strategy without message name."""
        manager.subject_naming_strategy = "record_name"
        
        subject = manager._generate_subject_name(
            topic="user-events",
            message_name=None
        )
        
        # Should fallback to topic_name
        assert subject == "user-events-value"


class TestSchemaComparison:
    """Test schema equivalence checking."""
    
    @pytest.fixture
    def manager(self):
        """Create SchemaRegistryManager with mocked dependencies."""
        with patch('testdatapy.schema.registry_manager.SchemaRegistryClient') as mock_client_class:
            with patch('testdatapy.schema.registry_manager.ProtobufCompiler') as mock_compiler_class:
                mock_client = Mock()
                mock_client_class.return_value = mock_client
                mock_client.get_subjects.return_value = []
                
                mock_compiler = Mock()
                mock_compiler_class.return_value = mock_compiler
                
                return SchemaRegistryManager("http://localhost:8081")
    
    def test_schemas_equivalent_same_content(self, manager):
        """Test equivalent schemas with same content."""
        schema1 = 'syntax = "proto3"; message User { string id = 1; }'
        existing_schema = {
            "schema_type": "PROTOBUF",
            "schema": 'syntax = "proto3"; message User { string id = 1; }'
        }
        
        result = manager._are_schemas_equivalent(schema1, existing_schema)
        assert result is True
    
    def test_schemas_equivalent_normalized_whitespace(self, manager):
        """Test equivalent schemas with different whitespace."""
        schema1 = 'syntax="proto3";\nmessage User{\nstring id=1;\n}'
        existing_schema = {
            "schema_type": "PROTOBUF",
            "schema": 'syntax="proto3"; message User{ string id=1; }'
        }
        
        result = manager._are_schemas_equivalent(schema1, existing_schema)
        assert result is True
    
    def test_schemas_not_equivalent_different_content(self, manager):
        """Test non-equivalent schemas with different content."""
        schema1 = 'syntax = "proto3"; message User { string id = 1; }'
        existing_schema = {
            "schema_type": "PROTOBUF",
            "schema": 'syntax = "proto3"; message Customer { string id = 1; }'
        }
        
        result = manager._are_schemas_equivalent(schema1, existing_schema)
        assert result is False
    
    def test_schemas_not_equivalent_different_type(self, manager):
        """Test non-equivalent schemas with different schema type."""
        schema1 = 'syntax = "proto3"; message User { string id = 1; }'
        existing_schema = {
            "schema_type": "AVRO",
            "schema": '{"type": "record", "name": "User"}'
        }
        
        result = manager._are_schemas_equivalent(schema1, existing_schema)
        assert result is False


class TestSchemaRegistryOperations:
    """Test Schema Registry operations."""
    
    @pytest.fixture
    def manager(self):
        """Create SchemaRegistryManager with mocked dependencies."""
        with patch('testdatapy.schema.registry_manager.SchemaRegistryClient') as mock_client_class:
            with patch('testdatapy.schema.registry_manager.ProtobufCompiler') as mock_compiler_class:
                mock_client = Mock()
                mock_client_class.return_value = mock_client
                mock_client.get_subjects.return_value = []
                
                mock_compiler = Mock()
                mock_compiler_class.return_value = mock_compiler
                
                return SchemaRegistryManager("http://localhost:8081")
    
    def test_get_existing_schema_success(self, manager):
        """Test successful retrieval of existing schema."""
        mock_version = Mock()
        mock_version.schema_id = 123
        mock_version.version = 2
        mock_version.schema.schema_str = "schema content"
        mock_version.schema.schema_type = "PROTOBUF"
        
        manager.client.get_latest_version.return_value = mock_version
        
        result = manager._get_existing_schema("test-subject")
        
        assert result["id"] == 123
        assert result["version"] == 2
        assert result["schema"] == "schema content"
        assert result["schema_type"] == "PROTOBUF"
    
    def test_get_existing_schema_not_found(self, manager):
        """Test retrieval of non-existent schema."""
        manager.client.get_latest_version.side_effect = Exception("Subject not found")
        
        result = manager._get_existing_schema("non-existent-subject")
        
        assert result is None
    
    def test_register_schema_success(self, manager):
        """Test successful schema registration."""
        manager.client.register_schema.return_value = 456
        
        mock_version = Mock()
        mock_version.version = 3
        manager.client.get_latest_version.return_value = mock_version
        
        result = manager._register_schema(
            subject="test-subject",
            proto_content="schema content",
            schema_info={"package": "com.example"}
        )
        
        assert result["schema_id"] == 456
        assert result["version"] == 3
        assert result["subject"] == "test-subject"
        assert result["schema_type"] == "PROTOBUF"
    
    def test_register_schema_failure(self, manager):
        """Test schema registration failure."""
        manager.client.register_schema.side_effect = Exception("Registration failed")
        
        with pytest.raises(SchemaRegistrationError) as exc_info:
            manager._register_schema(
                subject="test-subject",
                proto_content="schema content",
                schema_info={}
            )
        
        assert exc_info.value.subject == "test-subject"
        assert "Registration failed" in str(exc_info.value.original_error)
    
    def test_list_subjects_success(self, manager):
        """Test successful subject listing."""
        manager.client.get_subjects.return_value = [
            "user-value", "order-value", "product-value"
        ]
        
        subjects = manager.list_subjects()
        
        assert len(subjects) == 3
        assert "user-value" in subjects
        assert "order-value" in subjects
        assert "product-value" in subjects
    
    def test_list_subjects_with_pattern(self, manager):
        """Test subject listing with pattern filter."""
        manager.client.get_subjects.return_value = [
            "user-value", "user-key", "order-value", "product-value"
        ]
        
        subjects = manager.list_subjects(pattern="user*")
        
        assert len(subjects) == 2
        assert "user-value" in subjects
        assert "user-key" in subjects
        assert "order-value" not in subjects
    
    def test_list_subjects_failure(self, manager):
        """Test subject listing failure."""
        manager.client.get_subjects.side_effect = Exception("Connection failed")
        
        subjects = manager.list_subjects()
        
        assert subjects == []


class TestSchemaMetadata:
    """Test schema metadata operations."""
    
    @pytest.fixture
    def manager(self):
        """Create SchemaRegistryManager with mocked dependencies."""
        with patch('testdatapy.schema.registry_manager.SchemaRegistryClient') as mock_client_class:
            with patch('testdatapy.schema.registry_manager.ProtobufCompiler') as mock_compiler_class:
                mock_client = Mock()
                mock_client_class.return_value = mock_client
                mock_client.get_subjects.return_value = []
                
                mock_compiler = Mock()
                mock_compiler_class.return_value = mock_compiler
                
                return SchemaRegistryManager("http://localhost:8081")
    
    def test_get_schema_metadata_success(self, manager):
        """Test successful schema metadata retrieval."""
        # Mock latest version
        mock_latest = Mock()
        mock_latest.version = 3
        mock_latest.schema_id = 789
        mock_latest.schema.schema_type = "PROTOBUF"
        mock_latest.schema.schema_str = "schema content"
        manager.client.get_latest_version.return_value = mock_latest
        
        # Mock all versions
        manager.client.get_versions.return_value = [1, 2, 3]
        
        # Mock cached info
        manager.get_cached_schema = Mock(return_value={
            "topic": "test-topic",
            "proto_file": "/path/to/file.proto",
            "cached_at": 1234567890
        })
        
        metadata = manager.get_schema_metadata("test-subject")
        
        assert metadata["subject"] == "test-subject"
        assert metadata["latest_version"] == 3
        assert metadata["latest_schema_id"] == 789
        assert metadata["schema_type"] == "PROTOBUF"
        assert metadata["all_versions"] == [1, 2, 3]
        assert metadata["total_versions"] == 3
        assert metadata["schema_content"] == "schema content"
        assert metadata["cached_locally"] is True
        assert metadata["cache_info"]["topic"] == "test-topic"
    
    def test_get_schema_metadata_not_cached(self, manager):
        """Test schema metadata retrieval without cache."""
        mock_latest = Mock()
        mock_latest.version = 1
        mock_latest.schema_id = 123
        mock_latest.schema.schema_type = "PROTOBUF"
        mock_latest.schema.schema_str = "schema"
        manager.client.get_latest_version.return_value = mock_latest
        
        manager.client.get_versions.return_value = [1]
        manager.get_cached_schema = Mock(return_value=None)
        
        metadata = manager.get_schema_metadata("test-subject")
        
        assert metadata["cached_locally"] is False
        assert "cache_info" not in metadata
    
    def test_get_schema_metadata_failure(self, manager):
        """Test schema metadata retrieval failure."""
        manager.client.get_latest_version.side_effect = Exception("Subject not found")
        
        with pytest.raises(ValidationError) as exc_info:
            manager.get_schema_metadata("non-existent-subject")
        
        assert "Cannot get metadata for subject non-existent-subject" in str(exc_info.value)


class TestSchemaCaching:
    """Test schema caching functionality."""
    
    @pytest.fixture
    def manager(self):
        """Create SchemaRegistryManager with mocked dependencies."""
        with patch('testdatapy.schema.registry_manager.SchemaRegistryClient') as mock_client_class:
            with patch('testdatapy.schema.registry_manager.ProtobufCompiler') as mock_compiler_class:
                mock_client = Mock()
                mock_client_class.return_value = mock_client
                mock_client.get_subjects.return_value = []
                
                mock_compiler = Mock()
                mock_compiler_class.return_value = mock_compiler
                
                return SchemaRegistryManager("http://localhost:8081")
    
    def test_cache_schema(self, manager):
        """Test schema caching."""
        schema_data = {
            "proto_content": "schema content",
            "schema_info": {"package": "com.example"},
            "topic": "test-topic"
        }
        
        manager._cache_schema("test-subject", schema_data)
        
        cached = manager._schema_cache["test-subject"]
        assert cached["proto_content"] == "schema content"
        assert cached["topic"] == "test-topic"
        assert "cached_at" in cached
        assert isinstance(cached["cached_at"], float)
    
    def test_get_cached_schema_exists(self, manager):
        """Test retrieval of cached schema."""
        schema_data = {"topic": "test-topic", "cached_at": time.time()}
        manager._schema_cache["test-subject"] = schema_data
        
        result = manager.get_cached_schema("test-subject")
        
        assert result == schema_data
    
    def test_get_cached_schema_not_exists(self, manager):
        """Test retrieval of non-cached schema."""
        result = manager.get_cached_schema("non-existent-subject")
        
        assert result is None
    
    def test_clear_cache_specific_subject(self, manager):
        """Test clearing cache for specific subject."""
        manager._schema_cache = {
            "subject1": {"data": "test1"},
            "subject2": {"data": "test2"}
        }
        
        manager.clear_cache("subject1")
        
        assert "subject1" not in manager._schema_cache
        assert "subject2" in manager._schema_cache
    
    def test_clear_cache_all(self, manager):
        """Test clearing entire cache."""
        manager._schema_cache = {
            "subject1": {"data": "test1"},
            "subject2": {"data": "test2"}
        }
        
        manager.clear_cache()
        
        assert len(manager._schema_cache) == 0
    
    def test_clear_cache_non_existent_subject(self, manager):
        """Test clearing cache for non-existent subject."""
        manager._schema_cache = {"subject1": {"data": "test1"}}
        
        # Should not raise error
        manager.clear_cache("non-existent")
        
        assert "subject1" in manager._schema_cache


class TestSchemaRegistryManagerEdgeCases:
    """Test edge cases and error conditions."""
    
    @pytest.fixture
    def manager(self):
        """Create SchemaRegistryManager with mocked dependencies."""
        with patch('testdatapy.schema.registry_manager.SchemaRegistryClient') as mock_client_class:
            with patch('testdatapy.schema.registry_manager.ProtobufCompiler') as mock_compiler_class:
                mock_client = Mock()
                mock_client_class.return_value = mock_client
                mock_client.get_subjects.return_value = []
                
                mock_compiler = Mock()
                mock_compiler_class.return_value = mock_compiler
                
                return SchemaRegistryManager("http://localhost:8081")
    
    def test_register_schema_with_large_content(self, manager):
        """Test registration with large schema content."""
        large_content = "x" * 10000  # Large schema content
        
        manager.client.register_schema.side_effect = Exception("Schema too large")
        
        with pytest.raises(SchemaRegistrationError) as exc_info:
            manager._register_schema("test-subject", large_content, {})
        
        # Should truncate schema content in error
        error_content = exc_info.value.schema_content
        assert len(error_content) <= 203  # 200 + "..."
        assert error_content.endswith("...")
    
    def test_extraction_with_malformed_regex(self, manager):
        """Test schema info extraction with content that might break regex."""
        proto_content = '''
syntax = "proto3";
package com.example;

// This message has unusual formatting
message WeirdMessage{string field=1;int32 another_field = 2;}

// Another message
message    NormalMessage   {
    string   id   =   1   ;
}
'''
        
        schema_info = manager._extract_schema_info(proto_content)
        
        assert "WeirdMessage" in schema_info["messages"]
        assert "NormalMessage" in schema_info["messages"]
        assert schema_info["package"] == "com.example"
    
    def test_subject_naming_with_special_characters(self, manager):
        """Test subject naming with special characters in topic/message names."""
        manager.subject_naming_strategy = "topic_record_name"
        
        subject = manager._generate_subject_name(
            topic="user-events.v1",
            message_name="User$Message"
        )
        
        assert subject == "user-events.v1-User$Message"
    
    def test_concurrent_cache_access(self, manager):
        """Test cache operations that might happen concurrently."""
        # Simulate concurrent operations
        manager._cache_schema("subject1", {"data": "test1"})
        manager._cache_schema("subject2", {"data": "test2"})
        
        result1 = manager.get_cached_schema("subject1")
        result2 = manager.get_cached_schema("subject2")
        
        assert result1["data"] == "test1"
        assert result2["data"] == "test2"
        
        manager.clear_cache("subject1")
        
        assert manager.get_cached_schema("subject1") is None
        assert manager.get_cached_schema("subject2") is not None