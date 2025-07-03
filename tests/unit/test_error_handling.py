"""Unit tests for comprehensive error handling and logging."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import pytest

from testdatapy.exceptions import (
    TestDataPyException,
    SchemaException,
    SchemaNotFoundError,
    SchemaCompilationError,
    SchemaValidationError,
    ProtobufException,
    ProtobufCompilerNotFoundError,
    ProtobufClassNotFoundError,
    ProtobufImportError,
    ProtobufSerializationError,
    SchemaRegistryException,
    SchemaRegistryConnectionError,
    SchemaRegistrationError,
    SchemaCompatibilityError,
    ConfigurationException,
    InvalidConfigurationError,
    MissingConfigurationError,
    ProducerException,
    ProducerConnectionError,
    MessageProductionError,
    handle_and_reraise
)
from testdatapy.logging_config import (
    SchemaOperationLogger,
    configure_logging,
    get_schema_logger,
    PerformanceTimer,
    analyze_log_performance
)


class TestCustomExceptions(unittest.TestCase):
    """Test custom exception classes."""
    
    def test_base_exception_initialization(self):
        """Test base TestDataPy exception initialization."""
        original_error = ValueError("Original error")
        suggestions = ["Try this", "Or that"]
        
        exc = TestDataPyException(
            message="Test error",
            details="Some details",
            suggestions=suggestions,
            original_error=original_error
        )
        
        self.assertEqual(exc.message, "Test error")
        self.assertEqual(exc.details, "Some details")
        self.assertEqual(exc.suggestions, suggestions)
        self.assertEqual(exc.original_error, original_error)
        self.assertEqual(str(exc), "Test error")
    
    def test_user_message_formatting(self):
        """Test user-friendly message formatting."""
        suggestions = ["Check your config", "Verify the file exists"]
        exc = TestDataPyException(
            message="Configuration error",
            suggestions=suggestions
        )
        
        user_msg = exc.get_user_message()
        self.assertIn("Configuration error", user_msg)
        self.assertIn("Suggestions:", user_msg)
        self.assertIn("1. Check your config", user_msg)
        self.assertIn("2. Verify the file exists", user_msg)
    
    def test_schema_not_found_error(self):
        """Test SchemaNotFoundError with search paths."""
        search_paths = ["/path1", "/path2"]
        exc = SchemaNotFoundError(
            schema_path="/missing/schema.proto",
            schema_type="protobuf",
            search_paths=search_paths
        )
        
        self.assertEqual(exc.schema_path, "/missing/schema.proto")
        self.assertEqual(exc.schema_type, "protobuf")
        self.assertIn("Protobuf file not found", str(exc))
        user_msg = exc.get_user_message()
        self.assertIn("Searched in paths: /path1, /path2", user_msg)
        self.assertIn("--schema-path", user_msg)
    
    def test_schema_compilation_error(self):
        """Test SchemaCompilationError with compiler output."""
        compiler_output = "Error: syntax error at line 5"
        exc = SchemaCompilationError(
            schema_path="/path/schema.proto",
            compilation_error="Syntax error",
            compiler_output=compiler_output
        )
        
        self.assertEqual(exc.schema_path, "/path/schema.proto")
        self.assertEqual(exc.compilation_error, "Syntax error")
        self.assertIn("Failed to compile schema", str(exc))
        self.assertIn(compiler_output, exc.details)
    
    def test_protobuf_compiler_not_found_error(self):
        """Test ProtobufCompilerNotFoundError suggestions."""
        exc = ProtobufCompilerNotFoundError()
        
        user_msg = exc.get_user_message()
        self.assertIn("protoc", user_msg)
        self.assertIn("brew install protobuf", user_msg)
        self.assertIn("apt-get install", user_msg)
        self.assertIn("PATH", user_msg)
    
    def test_protobuf_class_not_found_error(self):
        """Test ProtobufClassNotFoundError with module information."""
        exc = ProtobufClassNotFoundError(
            class_spec="customer_pb2.Customer",
            module_name="customer_pb2",
            search_paths=["/schemas"]
        )
        
        self.assertEqual(exc.class_spec, "customer_pb2.Customer")
        self.assertEqual(exc.module_name, "customer_pb2")
        user_msg = exc.get_user_message()
        self.assertIn("customer_pb2.Customer", user_msg)
        self.assertIn("python -c 'import customer_pb2'", user_msg)
    
    def test_protobuf_serialization_error(self):
        """Test ProtobufSerializationError with data context."""
        data = {"field1": "value1", "field2": 123}
        original_error = AttributeError("Field not found")
        
        class MockMessage:
            pass
        
        exc = ProtobufSerializationError(
            data=data,
            proto_class=MockMessage,
            serialization_error=original_error
        )
        
        self.assertEqual(exc.data, data)
        self.assertEqual(exc.proto_class, MockMessage)
        self.assertEqual(exc.original_error, original_error)
        self.assertIn("MockMessage", str(exc))
        user_msg = exc.get_user_message()
        self.assertIn("missing required fields", user_msg)
        self.assertIn("data types are compatible", user_msg)
    
    def test_schema_registry_connection_error(self):
        """Test SchemaRegistryConnectionError with connection details."""
        original_error = ConnectionError("Connection refused")
        exc = SchemaRegistryConnectionError(
            registry_url="http://localhost:8081",
            connection_error=original_error
        )
        
        self.assertEqual(exc.registry_url, "http://localhost:8081")
        self.assertEqual(exc.original_error, original_error)
        user_msg = exc.get_user_message()
        self.assertIn("http://localhost:8081", user_msg)
        self.assertIn("network connectivity", user_msg)
        self.assertIn("authentication", user_msg)
    
    def test_handle_and_reraise_utility(self):
        """Test handle_and_reraise utility function."""
        # Test re-raising TestDataPy exceptions unchanged
        original_exc = SchemaNotFoundError("test.proto", "protobuf")
        with self.assertRaises(SchemaNotFoundError) as cm:
            try:
                raise original_exc
            except Exception as e:
                handle_and_reraise(e, "test context")
        
        self.assertEqual(cm.exception, original_exc)
        
        # Test converting FileNotFoundError
        with self.assertRaises(SchemaNotFoundError):
            try:
                raise FileNotFoundError("missing.proto")
            except Exception as e:
                handle_and_reraise(e, "test context")
        
        # Test converting ImportError
        with self.assertRaises(ProtobufImportError):
            try:
                raise ImportError("No module named 'customer_pb2'")
            except Exception as e:
                handle_and_reraise(e, "customer_pb2")
        
        # Test generic exception handling
        with self.assertRaises(TestDataPyException):
            try:
                raise ValueError("Some other error")
            except Exception as e:
                handle_and_reraise(e, "generic context", ["Try something"])


class TestLoggingConfiguration(unittest.TestCase):
    """Test logging configuration and schema operation logging."""
    
    def test_schema_operation_logger_initialization(self):
        """Test SchemaOperationLogger initialization."""
        logger = SchemaOperationLogger("test.module")
        self.assertEqual(logger.logger.name, "test.module")
        self.assertEqual(logger._operation_context, {})
    
    def test_operation_context_management(self):
        """Test operation context setting and clearing."""
        logger = SchemaOperationLogger("test.module")
        
        # Set context
        logger.set_operation_context(operation="test", schema_path="/path/test.proto")
        self.assertEqual(logger._operation_context["operation"], "test")
        self.assertEqual(logger._operation_context["schema_path"], "/path/test.proto")
        
        # Add more context
        logger.set_operation_context(compiler="protoc")
        self.assertEqual(len(logger._operation_context), 3)
        self.assertEqual(logger._operation_context["compiler"], "protoc")
        
        # Clear context
        logger.clear_operation_context()
        self.assertEqual(logger._operation_context, {})
    
    def test_schema_compilation_logging(self):
        """Test schema compilation logging."""
        with patch('testdatapy.logging_config.logging.getLogger') as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            
            logger = SchemaOperationLogger("test.module")
            logger.logger = mock_logger
            
            # Test successful compilation
            logger.log_schema_compilation(
                schema_path="/path/test.proto",
                compiler="protoc",
                success=True,
                duration=1.5,
                output="Compilation successful"
            )
            
            # Should have 2 calls: info and debug (because output is provided)
            self.assertEqual(mock_logger.log.call_count, 2)
            
            # Check the first call (info level)
            first_call = mock_logger.log.call_args_list[0]
            self.assertEqual(first_call[0][0], 20)  # INFO level
            self.assertIn("Successfully compiled", first_call[0][1])
            self.assertIn("schema_operation", first_call[1]["extra"])
            
            # Check the second call (debug level for output)
            second_call = mock_logger.log.call_args_list[1]
            self.assertEqual(second_call[0][0], 10)  # DEBUG level
            self.assertIn("Compiler output", second_call[0][1])
            
            # Reset mock for next test
            mock_logger.reset_mock()
            
            # Test failed compilation
            logger.log_schema_compilation(
                schema_path="/path/test.proto",
                compiler="protoc",
                success=False,
                duration=0.5,
                error="Syntax error"
            )
            
            # Should have 1 call for failed compilation
            self.assertEqual(mock_logger.log.call_count, 1)
            call_args = mock_logger.log.call_args
            self.assertEqual(call_args[0][0], 40)  # ERROR level
            self.assertIn("Failed to compile", call_args[0][1])
    
    def test_schema_loading_logging(self):
        """Test schema loading logging."""
        with patch('testdatapy.logging_config.logging.getLogger') as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            
            logger = SchemaOperationLogger("test.module")
            logger.logger = mock_logger
            
            logger.log_schema_loading(
                schema_spec="customer_pb2.Customer",
                method="class",
                success=True,
                duration=0.1,
                class_name="Customer"
            )
            
            mock_logger.log.assert_called()
            call_args = mock_logger.log.call_args
            self.assertIn("Successfully loaded", call_args[0][1])
            context = call_args[1]["extra"]
            self.assertEqual(context["method"], "class")
            self.assertEqual(context["loaded_class"], "Customer")
    
    def test_schema_registry_operation_logging(self):
        """Test Schema Registry operation logging."""
        with patch('testdatapy.logging_config.logging.getLogger') as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            
            logger = SchemaOperationLogger("test.module")
            logger.logger = mock_logger
            
            logger.log_schema_registry_operation(
                operation="register",
                subject="test-subject",
                success=True,
                duration=0.5,
                schema_id=123
            )
            
            mock_logger.log.assert_called()
            call_args = mock_logger.log.call_args
            self.assertIn("register succeeded", call_args[0][1])
            context = call_args[1]["extra"]
            self.assertEqual(context["registry_operation"], "register")
            self.assertEqual(context["schema_id"], 123)
    
    def test_message_production_logging(self):
        """Test message production logging."""
        with patch('testdatapy.logging_config.logging.getLogger') as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            
            logger = SchemaOperationLogger("test.module")
            logger.logger = mock_logger
            
            logger.log_message_production(
                topic="test-topic",
                message_format="protobuf",
                success=True,
                duration=0.01,
                message_size=256
            )
            
            mock_logger.log.assert_called()
            call_args = mock_logger.log.call_args
            self.assertEqual(call_args[0][0], 10)  # DEBUG level for successful production
            self.assertIn("Message produced", call_args[0][1])
            context = call_args[1]["extra"]
            self.assertEqual(context["message_size_bytes"], 256)
    
    def test_performance_timer(self):
        """Test PerformanceTimer context manager."""
        with patch('testdatapy.logging_config.logging.getLogger') as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            
            logger = SchemaOperationLogger("test.module")
            logger.logger = mock_logger
            
            # Test successful operation
            with PerformanceTimer(logger, "test_operation", test_param="value"):
                pass
            
            # Should have start and end log calls
            self.assertEqual(mock_logger.log.call_count, 2)
            
            # Check start log
            start_call = mock_logger.log.call_args_list[0]
            self.assertEqual(start_call[0][0], 10)  # DEBUG level
            self.assertIn("Starting test_operation", start_call[0][1])
            
            # Check end log
            end_call = mock_logger.log.call_args_list[1]
            self.assertEqual(end_call[0][0], 20)  # INFO level
            self.assertIn("Completed test_operation", end_call[0][1])
            self.assertIn("duration_seconds", end_call[1]["extra"])
    
    def test_performance_timer_with_exception(self):
        """Test PerformanceTimer with exception."""
        with patch('testdatapy.logging_config.logging.getLogger') as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            
            logger = SchemaOperationLogger("test.module")
            logger.logger = mock_logger
            
            # Test operation with exception
            with self.assertRaises(ValueError):
                with PerformanceTimer(logger, "failing_operation"):
                    raise ValueError("Test error")
            
            # Check error log
            end_call = mock_logger.log.call_args_list[1]
            self.assertEqual(end_call[0][0], 40)  # ERROR level
            self.assertIn("Failed failing_operation", end_call[0][1])
            self.assertIn("error", end_call[1]["extra"])
    
    def test_get_schema_logger(self):
        """Test get_schema_logger factory function."""
        logger = get_schema_logger("test.module")
        self.assertIsInstance(logger, SchemaOperationLogger)
        self.assertEqual(logger.logger.name, "test.module")
    
    def test_configure_logging(self):
        """Test logging configuration."""
        with patch('testdatapy.logging_config.logging.basicConfig') as mock_basic_config:
            configure_logging(
                level="DEBUG",
                json_format=True,
                enable_schema_logging=True
            )
            
            mock_basic_config.assert_called_once()
            call_kwargs = mock_basic_config.call_args[1]
            self.assertEqual(call_kwargs['level'], 10)  # DEBUG level
            self.assertTrue('handlers' in call_kwargs)


class TestLogAnalysis(unittest.TestCase):
    """Test log analysis utilities."""
    
    def test_analyze_log_performance_with_json_logs(self):
        """Test performance analysis from JSON log file."""
        # Create temporary log file with JSON logs
        log_data = [
            {
                "timestamp": "2023-01-01 12:00:00",
                "level": "INFO",
                "context": {
                    "operation": "schema_compilation",
                    "duration_seconds": 1.5
                }
            },
            {
                "timestamp": "2023-01-01 12:00:01",
                "level": "INFO", 
                "context": {
                    "operation": "schema_compilation",
                    "duration_seconds": 2.0
                }
            },
            {
                "timestamp": "2023-01-01 12:00:02",
                "level": "INFO",
                "context": {
                    "operation": "schema_loading",
                    "duration_seconds": 0.5
                }
            }
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.log') as f:
            for entry in log_data:
                f.write(json.dumps(entry) + '\n')
            log_file = f.name
        
        try:
            stats = analyze_log_performance(log_file)
            
            # Check compilation stats
            self.assertIn("schema_compilation", stats)
            comp_stats = stats["schema_compilation"]
            self.assertEqual(comp_stats["count"], 2)
            self.assertEqual(comp_stats["total_time"], 3.5)
            self.assertEqual(comp_stats["avg_time"], 1.75)
            self.assertEqual(comp_stats["min_time"], 1.5)
            self.assertEqual(comp_stats["max_time"], 2.0)
            
            # Check loading stats
            self.assertIn("schema_loading", stats)
            load_stats = stats["schema_loading"]
            self.assertEqual(load_stats["count"], 1)
            self.assertEqual(load_stats["total_time"], 0.5)
            
        finally:
            Path(log_file).unlink()
    
    def test_analyze_log_performance_missing_file(self):
        """Test performance analysis with missing log file."""
        stats = analyze_log_performance("/nonexistent/log.file")
        self.assertIn("error", stats)
        self.assertIn("not found", stats["error"])


class TestIntegrationWithCLI(unittest.TestCase):
    """Test error handling integration with CLI functions."""
    
    @patch('subprocess.run')
    @patch('pathlib.Path.exists')
    def test_protobuf_compiler_not_found_in_cli(self, mock_exists, mock_subprocess):
        """Test ProtobufCompilerNotFoundError in CLI loading function."""
        from testdatapy.cli import _load_protobuf_class
        
        mock_exists.return_value = True
        mock_subprocess.side_effect = FileNotFoundError("protoc not found")
        
        with self.assertRaises(ProtobufCompilerNotFoundError) as cm:
            _load_protobuf_class(
                proto_class=None,
                proto_module=None,
                proto_file="/path/test.proto",
                schema_paths=()
            )
        
        exc = cm.exception
        user_msg = exc.get_user_message()
        self.assertIn("protoc", user_msg)
        self.assertIn("brew install protobuf", user_msg)
    
    @patch('pathlib.Path.exists')
    def test_schema_not_found_in_cli(self, mock_exists):
        """Test SchemaNotFoundError in CLI loading function."""
        from testdatapy.cli import _load_protobuf_class
        
        mock_exists.return_value = False
        
        with self.assertRaises(SchemaNotFoundError) as cm:
            _load_protobuf_class(
                proto_class=None,
                proto_module=None,
                proto_file="/nonexistent/test.proto",
                schema_paths=("/search/path1", "/search/path2")
            )
        
        exc = cm.exception
        self.assertEqual(exc.schema_path, "/nonexistent/test.proto")
        self.assertEqual(exc.schema_type, "protobuf schema")
        user_msg = exc.get_user_message()
        self.assertIn("/search/path1, /search/path2", user_msg)
    
    def test_protobuf_import_error_in_cli(self):
        """Test ProtobufImportError in CLI loading function."""
        from testdatapy.cli import _load_protobuf_class
        
        with self.assertRaises(ProtobufImportError) as cm:
            _load_protobuf_class(
                proto_class=None,
                proto_module="nonexistent_module",
                proto_file=None,
                schema_paths=("/path1", "/path2")
            )
        
        exc = cm.exception
        self.assertEqual(exc.module_name, "nonexistent_module")
        user_msg = exc.get_user_message()
        self.assertIn("nonexistent_module", user_msg)
        self.assertIn("/path1, /path2", user_msg)


if __name__ == '__main__':
    unittest.main()