"""Unit tests for schema operation logging integration."""

import pytest
import tempfile
import logging
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import json

from src.testdatapy.logging_config import (
    SchemaOperationLogger,
    PerformanceTimer,
    configure_logging,
    get_schema_logger,
    StructuredFormatter,
    SchemaOperationFilter
)
from src.testdatapy.schema.manager import SchemaManager
from src.testdatapy.schema.compiler import ProtobufCompiler


class TestSchemaOperationLogger:
    """Test SchemaOperationLogger functionality."""
    
    def test_logger_initialization(self):
        """Test logger initialization and context setting."""
        logger = get_schema_logger("test.module")
        
        assert isinstance(logger, SchemaOperationLogger)
        assert logger._operation_context == {}
        
        # Test context setting
        logger.set_operation_context(component="TestComponent", operation="test_op")
        assert logger._operation_context["component"] == "TestComponent"
        assert logger._operation_context["operation"] == "test_op"
        
        # Test context clearing
        logger.clear_operation_context()
        assert logger._operation_context == {}
    
    def test_operation_specific_logging(self):
        """Test operation-specific logging methods."""
        logger = get_schema_logger("test.module")
        
        # Mock the underlying logger
        with patch.object(logger.logger, 'log') as mock_log:
            logger.set_operation_context(test_context="value")
            
            # Test different log levels
            logger.debug("Debug message", extra_key="extra_value")
            logger.info("Info message", another_key="another_value")
            logger.warning("Warning message")
            logger.error("Error message")
            logger.critical("Critical message")
            
            # Verify calls were made
            assert mock_log.call_count == 5
            
            # Verify context is included
            for call in mock_log.call_args_list:
                args, kwargs = call
                context = kwargs.get('extra', {})
                assert context['schema_operation'] is True
                assert 'test_context' in context
    
    def test_schema_compilation_logging(self):
        """Test schema compilation logging method."""
        logger = get_schema_logger("test.module")
        
        with patch.object(logger, 'info') as mock_info, \
             patch.object(logger, 'error') as mock_error:
            
            # Test successful compilation
            logger.log_schema_compilation(
                schema_path="/test/schema.proto",
                compiler="protoc",
                success=True,
                duration=1.5,
                output="Compilation successful"
            )
            
            mock_info.assert_called_once()
            call_args = mock_info.call_args[1]
            assert call_args['operation'] == 'schema_compilation'
            assert call_args['schema_path'] == '/test/schema.proto'
            assert call_args['success'] is True
            assert call_args['duration_seconds'] == 1.5
            
            # Test failed compilation
            logger.log_schema_compilation(
                schema_path="/test/schema.proto",
                compiler="protoc",
                success=False,
                duration=0.5,
                error="Syntax error"
            )
            
            mock_error.assert_called_once()
            call_args = mock_error.call_args[1]
            assert call_args['success'] is False
            assert call_args['error'] == "Syntax error"
    
    def test_schema_loading_logging(self):
        """Test schema loading logging method."""
        logger = get_schema_logger("test.module")
        
        with patch.object(logger, 'info') as mock_info:
            logger.log_schema_loading(
                schema_spec="test.proto",
                method="file",
                success=True,
                duration=0.3,
                class_name="TestMessage"
            )
            
            mock_info.assert_called_once()
            call_args = mock_info.call_args[1]
            assert call_args['operation'] == 'schema_loading'
            assert call_args['loaded_class'] == 'TestMessage'
    
    def test_performance_timer(self):
        """Test PerformanceTimer context manager."""
        logger = get_schema_logger("test.module")
        
        with patch.object(logger, 'debug') as mock_debug, \
             patch.object(logger, 'info') as mock_info:
            
            # Test successful operation
            with PerformanceTimer(logger, "test_operation", test_param="value"):
                pass  # Simulate some work
            
            # Should have called debug for start and info for completion
            assert mock_debug.call_count == 1
            assert mock_info.call_count == 1
            
            # Verify start message
            start_call = mock_debug.call_args
            assert "Starting test_operation" in start_call[0][0]
            
            # Verify completion message
            completion_call = mock_info.call_args
            assert "Completed test_operation" in completion_call[0][0]
            assert 'duration_seconds' in completion_call[1]


class TestStructuredFormatter:
    """Test StructuredFormatter functionality."""
    
    def test_structured_formatting(self):
        """Test structured log formatting."""
        formatter = StructuredFormatter()
        
        # Create a log record
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        # Add custom attributes
        record.schema_operation = True
        record.test_context = "test_value"
        
        # Format the record
        formatted = formatter.format(record)
        
        # Verify format includes timestamp, level, logger, message
        assert "INFO" in formatted
        assert "test.logger" in formatted
        assert "Test message" in formatted
        assert "test_context=test_value" in formatted
    
    def test_json_formatting(self):
        """Test JSON formatting."""
        formatter = StructuredFormatter()
        formatter.json_format = True
        
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        formatted = formatter.format(record)
        
        # Should be valid JSON
        log_data = json.loads(formatted)
        assert log_data['level'] == 'INFO'
        assert log_data['logger'] == 'test.logger'
        assert log_data['message'] == 'Test message'


class TestSchemaOperationFilter:
    """Test SchemaOperationFilter functionality."""
    
    def test_filter_schema_operations(self):
        """Test filtering of schema operation logs."""
        filter_obj = SchemaOperationFilter()
        
        # Test record with schema_operation attribute
        schema_record = logging.LogRecord(
            name="testdatapy.schema.manager",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Test",
            args=(),
            exc_info=None
        )
        schema_record.schema_operation = True
        
        assert filter_obj.filter(schema_record) is True
        
        # Test record from schema module
        module_record = logging.LogRecord(
            name="testdatapy.schema.compiler",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Test",
            args=(),
            exc_info=None
        )
        
        assert filter_obj.filter(module_record) is True
        
        # Test record from non-schema module
        other_record = logging.LogRecord(
            name="other.module",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Test",
            args=(),
            exc_info=None
        )
        
        assert filter_obj.filter(other_record) is False


class TestSchemaManagerLogging:
    """Test SchemaManager logging integration."""
    
    def test_manager_initialization_logging(self):
        """Test that SchemaManager logs initialization properly."""
        with patch('src.testdatapy.schema.manager.get_schema_logger') as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            
            manager = SchemaManager()
            
            # Verify logger was obtained and configured
            mock_get_logger.assert_called_once()
            mock_logger.set_operation_context.assert_called_once_with(component='SchemaManager')
            mock_logger.info.assert_called_once()
    
    def test_proto_discovery_logging(self):
        """Test proto discovery logging."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a test proto file
            proto_file = Path(temp_dir) / "test.proto"
            proto_file.write_text('syntax = "proto3";')
            
            with patch('src.testdatapy.schema.manager.get_schema_logger') as mock_get_logger:
                mock_logger = Mock()
                mock_get_logger.return_value = mock_logger
                
                manager = SchemaManager()
                files = manager.discover_proto_files(temp_dir)
                
                # Verify logging calls were made
                assert mock_logger.debug.call_count >= 1
                assert mock_logger.info.call_count >= 2  # Init + discovery completion
                
                # Verify the discovery found our test file
                assert len(files) == 1
                assert "test.proto" in files[0]


class TestProtobufCompilerLogging:
    """Test ProtobufCompiler logging integration."""
    
    def test_compiler_initialization_logging(self):
        """Test that ProtobufCompiler logs initialization properly."""
        with patch('src.testdatapy.schema.compiler.get_schema_logger') as mock_get_logger, \
             patch('src.testdatapy.schema.compiler.shutil.which') as mock_which:
            
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            mock_which.return_value = "/usr/bin/protoc"
            
            with patch.object(ProtobufCompiler, 'validate_protoc_path'):
                compiler = ProtobufCompiler()
                
                # Verify logger was obtained and configured
                mock_get_logger.assert_called_once()
                mock_logger.set_operation_context.assert_called_once_with(component='ProtobufCompiler')
                mock_logger.info.assert_called()
    
    def test_protoc_detection_logging(self):
        """Test protoc detection logging."""
        with patch('src.testdatapy.schema.compiler.get_schema_logger') as mock_get_logger, \
             patch('src.testdatapy.schema.compiler.shutil.which') as mock_which:
            
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            mock_which.return_value = "/usr/bin/protoc"
            
            with patch.object(ProtobufCompiler, 'validate_protoc_path'):
                compiler = ProtobufCompiler()
                
                # Verify detection logging
                assert mock_logger.debug.call_count >= 1  # Detection start
                assert mock_logger.info.call_count >= 2  # Detection success + init


class TestLoggingConfiguration:
    """Test logging configuration functionality."""
    
    def test_configure_logging(self):
        """Test logging configuration."""
        # Test basic configuration
        configure_logging(level="DEBUG", json_format=False)
        
        # Verify root logger was configured
        root_logger = logging.getLogger()
        assert root_logger.level == logging.DEBUG
        
        # Test with file output
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            configure_logging(level="INFO", log_file=temp_file.name)
            
            # Verify file handler was added
            handlers = root_logger.handlers
            file_handlers = [h for h in handlers if isinstance(h, logging.FileHandler)]
            assert len(file_handlers) > 0
    
    def test_get_schema_logger(self):
        """Test get_schema_logger function."""
        logger = get_schema_logger("test.module")
        
        assert isinstance(logger, SchemaOperationLogger)
        assert logger.logger.name == "test.module"