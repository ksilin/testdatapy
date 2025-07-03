"""Comprehensive logging configuration for TestDataPy schema operations.

This module provides structured logging for schema operations, making debugging
and troubleshooting easier for users and developers.
"""

import logging
import logging.config
import sys
from pathlib import Path
from typing import Any, Dict, Optional
import json
import time


class SchemaOperationFilter(logging.Filter):
    """Custom filter for schema operation logs."""
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Filter log records for schema operations."""
        # Add schema operation context if available
        if hasattr(record, 'schema_operation'):
            return True
        
        # Check if the log is from schema-related modules
        schema_modules = [
            'testdatapy.schema',
            'testdatapy.producers.protobuf_producer',
            'testdatapy.cli',
            'testdatapy.exceptions'
        ]
        
        return any(record.name.startswith(module) for module in schema_modules)


class StructuredFormatter(logging.Formatter):
    """Structured formatter for better log parsing and analysis."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record with structured information."""
        # Base structured data
        log_data = {
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(record.created)),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
        }
        
        # Add exception information if present
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        
        # Add custom attributes
        custom_attrs = {}
        for key, value in record.__dict__.items():
            if key not in ['name', 'msg', 'args', 'levelname', 'levelno', 'pathname',
                          'filename', 'module', 'lineno', 'funcName', 'created',
                          'msecs', 'relativeCreated', 'thread', 'threadName',
                          'processName', 'process', 'message', 'exc_info', 'exc_text', 
                          'stack_info']:
                custom_attrs[key] = value
        
        if custom_attrs:
            log_data['context'] = custom_attrs
        
        # Format for different outputs
        if getattr(self, 'json_format', False):
            return json.dumps(log_data, default=str)
        else:
            # Human-readable format
            msg = f"{log_data['timestamp']} - {log_data['level']} - {log_data['logger']} - {log_data['message']}"
            if 'context' in log_data:
                context_str = ', '.join(f"{k}={v}" for k, v in log_data['context'].items())
                msg += f" [{context_str}]"
            if 'exception' in log_data:
                msg += f"\n{log_data['exception']}"
            return msg


class SchemaOperationLogger:
    """Specialized logger for schema operations with context tracking."""
    
    def __init__(self, name: str):
        """Initialize schema operation logger.
        
        Args:
            name: Logger name (typically module name)
        """
        self.logger = logging.getLogger(name)
        self._operation_context: Dict[str, Any] = {}
    
    def set_operation_context(self, **context: Any) -> None:
        """Set operation context for subsequent log messages.
        
        Args:
            **context: Context key-value pairs
        """
        self._operation_context.update(context)
    
    def clear_operation_context(self) -> None:
        """Clear operation context."""
        self._operation_context.clear()
    
    def _log_with_context(self, level: int, message: str, **extra: Any) -> None:
        """Log message with operation context.
        
        Args:
            level: Log level
            message: Log message
            **extra: Additional context
        """
        # Merge operation context with extra context
        context = {**self._operation_context, **extra}
        context['schema_operation'] = True
        
        self.logger.log(level, message, extra=context)
    
    def debug(self, message: str, **context: Any) -> None:
        """Log debug message with context."""
        self._log_with_context(logging.DEBUG, message, **context)
    
    def info(self, message: str, **context: Any) -> None:
        """Log info message with context."""
        self._log_with_context(logging.INFO, message, **context)
    
    def warning(self, message: str, **context: Any) -> None:
        """Log warning message with context."""
        self._log_with_context(logging.WARNING, message, **context)
    
    def error(self, message: str, **context: Any) -> None:
        """Log error message with context."""
        self._log_with_context(logging.ERROR, message, **context)
    
    def critical(self, message: str, **context: Any) -> None:
        """Log critical message with context."""
        self._log_with_context(logging.CRITICAL, message, **context)
    
    def log_schema_compilation(
        self,
        schema_path: str,
        compiler: str,
        success: bool,
        duration: float,
        output: Optional[str] = None,
        error: Optional[str] = None
    ) -> None:
        """Log schema compilation operation.
        
        Args:
            schema_path: Path to schema file
            compiler: Compiler used (e.g., 'protoc')
            success: Whether compilation succeeded
            duration: Compilation duration in seconds
            output: Compiler output
            error: Error message if compilation failed
        """
        context = {
            'operation': 'schema_compilation',
            'schema_path': schema_path,
            'compiler': compiler,
            'duration_seconds': round(duration, 3),
            'success': success
        }
        
        if success:
            self.info(f"Successfully compiled schema: {schema_path}", **context)
            if output:
                self.debug(f"Compiler output: {output}", **context)
        else:
            context['error'] = error
            self.error(f"Failed to compile schema: {schema_path}", **context)
    
    def log_schema_loading(
        self,
        schema_spec: str,
        method: str,
        success: bool,
        duration: float,
        class_name: Optional[str] = None,
        error: Optional[str] = None
    ) -> None:
        """Log schema loading operation.
        
        Args:
            schema_spec: Schema specification (file path, class name, etc.)
            method: Loading method ('file', 'class', 'module')
            success: Whether loading succeeded
            duration: Loading duration in seconds
            class_name: Loaded class name if successful
            error: Error message if loading failed
        """
        context = {
            'operation': 'schema_loading',
            'schema_spec': schema_spec,
            'method': method,
            'duration_seconds': round(duration, 3),
            'success': success
        }
        
        if success:
            context['loaded_class'] = class_name
            self.info(f"Successfully loaded schema: {schema_spec}", **context)
        else:
            context['error'] = error
            self.error(f"Failed to load schema: {schema_spec}", **context)
    
    def log_schema_registry_operation(
        self,
        operation: str,
        subject: str,
        success: bool,
        duration: float,
        schema_id: Optional[int] = None,
        error: Optional[str] = None
    ) -> None:
        """Log Schema Registry operation.
        
        Args:
            operation: Operation type ('register', 'get', 'check_compatibility')
            subject: Schema Registry subject
            success: Whether operation succeeded
            duration: Operation duration in seconds
            schema_id: Schema ID if applicable
            error: Error message if operation failed
        """
        context = {
            'operation': 'schema_registry',
            'registry_operation': operation,
            'subject': subject,
            'duration_seconds': round(duration, 3),
            'success': success
        }
        
        if success:
            if schema_id is not None:
                context['schema_id'] = schema_id
            self.info(f"Schema Registry {operation} succeeded for subject: {subject}", **context)
        else:
            context['error'] = error
            self.error(f"Schema Registry {operation} failed for subject: {subject}", **context)
    
    def log_message_production(
        self,
        topic: str,
        message_format: str,
        success: bool,
        duration: float,
        message_size: Optional[int] = None,
        error: Optional[str] = None
    ) -> None:
        """Log message production operation.
        
        Args:
            topic: Kafka topic
            message_format: Message format ('json', 'avro', 'protobuf')
            success: Whether production succeeded
            duration: Production duration in seconds
            message_size: Message size in bytes
            error: Error message if production failed
        """
        context = {
            'operation': 'message_production',
            'topic': topic,
            'format': message_format,
            'duration_seconds': round(duration, 3),
            'success': success
        }
        
        if success:
            if message_size is not None:
                context['message_size_bytes'] = message_size
            self.debug(f"Message produced to topic: {topic}", **context)
        else:
            context['error'] = error
            self.error(f"Failed to produce message to topic: {topic}", **context)


def configure_logging(
    level: str = "INFO",
    json_format: bool = False,
    log_file: Optional[str] = None,
    enable_schema_logging: bool = True
) -> None:
    """Configure comprehensive logging for TestDataPy.
    
    Args:
        level: Log level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
        json_format: Whether to use JSON format for logs
        log_file: Optional log file path
        enable_schema_logging: Whether to enable specialized schema logging
    """
    # Convert string level to logging constant
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    
    # Create formatters
    if json_format:
        formatter = StructuredFormatter()
        formatter.json_format = True
    else:
        formatter = StructuredFormatter()
    
    # Configure handlers
    handlers = []
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(numeric_level)
    handlers.append(console_handler)
    
    # File handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)  # Always debug level for files
        handlers.append(file_handler)
    
    # Schema operation filter
    if enable_schema_logging:
        schema_filter = SchemaOperationFilter()
        for handler in handlers:
            handler.addFilter(schema_filter)
    
    # Configure root logger
    logging.basicConfig(
        level=numeric_level,
        handlers=handlers,
        force=True  # Override existing configuration
    )
    
    # Set specific loggers
    loggers_config = {
        'testdatapy': numeric_level,
        'testdatapy.schema': logging.DEBUG if enable_schema_logging else numeric_level,
        'testdatapy.producers': logging.DEBUG if enable_schema_logging else numeric_level,
        'testdatapy.exceptions': logging.DEBUG if enable_schema_logging else numeric_level,
    }
    
    for logger_name, logger_level in loggers_config.items():
        logger = logging.getLogger(logger_name)
        logger.setLevel(logger_level)


def get_schema_logger(name: str) -> SchemaOperationLogger:
    """Get a schema operation logger for the given name.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        SchemaOperationLogger instance
    """
    return SchemaOperationLogger(name)


# Performance logging utilities

class PerformanceTimer:
    """Context manager for timing operations and logging performance."""
    
    def __init__(self, logger: SchemaOperationLogger, operation: str, **context: Any):
        """Initialize performance timer.
        
        Args:
            logger: Logger instance
            operation: Operation name
            **context: Additional context
        """
        self.logger = logger
        self.operation = operation
        self.context = context
        self.start_time = 0.0
        self.duration = 0.0
    
    def __enter__(self) -> 'PerformanceTimer':
        """Start timing."""
        self.start_time = time.time()
        self.logger.debug(f"Starting {self.operation}", **self.context)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """End timing and log results."""
        self.duration = time.time() - self.start_time
        
        context = {**self.context, 'duration_seconds': round(self.duration, 3)}
        
        if exc_type is None:
            self.logger.info(f"Completed {self.operation}", **context)
        else:
            context['error'] = str(exc_val) if exc_val else 'Unknown error'
            self.logger.error(f"Failed {self.operation}", **context)


# Log analysis utilities

def analyze_log_performance(log_file: str) -> Dict[str, Any]:
    """Analyze performance from log file.
    
    Args:
        log_file: Path to log file
        
    Returns:
        Performance analysis results
    """
    operations = {}
    
    try:
        with open(log_file, 'r') as f:
            for line in f:
                try:
                    # Try to parse as JSON
                    log_data = json.loads(line.strip())
                    if 'context' in log_data and 'operation' in log_data['context']:
                        op = log_data['context']['operation']
                        duration = log_data['context'].get('duration_seconds', 0)
                        
                        if op not in operations:
                            operations[op] = []
                        operations[op].append(duration)
                        
                except json.JSONDecodeError:
                    # Skip non-JSON lines
                    continue
                    
    except FileNotFoundError:
        return {"error": f"Log file not found: {log_file}"}
    
    # Calculate statistics
    stats = {}
    for op, durations in operations.items():
        stats[op] = {
            'count': len(durations),
            'total_time': sum(durations),
            'avg_time': sum(durations) / len(durations),
            'min_time': min(durations),
            'max_time': max(durations)
        }
    
    return stats