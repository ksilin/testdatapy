"""SchemaManager for centralized schema discovery, compilation, and management."""

import logging
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import List, Dict, Any, Optional, Union

from .exceptions import SchemaError, CompilationError, ValidationError
from ..logging_config import get_schema_logger, PerformanceTimer


class SchemaManager:
    """Centralized schema discovery, compilation, and management system."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize SchemaManager with optional configuration.
        
        Args:
            config: Optional configuration dictionary
            
        Raises:
            ValidationError: If configuration is invalid
        """
        self._logger = get_schema_logger(__name__)
        self._config = config or {}
        self._search_paths: List[tuple] = []  # List of (path, priority) tuples
        
        # Set operation context
        self._logger.set_operation_context(component='SchemaManager')
        
        # Validate configuration
        self._validate_config(self._config)
        
        # Initialize search paths from config
        if 'schema_paths' in self._config:
            for path in self._config['schema_paths']:
                self.add_search_path(path)
        
        self._logger.info("SchemaManager initialized", search_paths=len(self._search_paths))
    
    def _validate_config(self, config: Dict[str, Any]) -> None:
        """
        Validate configuration dictionary.
        
        Args:
            config: Configuration to validate
            
        Raises:
            ValidationError: If configuration is invalid
        """
        valid_keys = {'schema_paths', 'auto_compile', 'temp_compilation'}
        
        for key in config.keys():
            if key not in valid_keys:
                raise ValidationError(f"Invalid configuration key: {key}")
    
    def discover_proto_files(self, search_path: str, recursive: bool = True) -> List[str]:
        """
        Discover .proto files in specified directory.
        
        Args:
            search_path: Directory to search for proto files
            recursive: Whether to search recursively
            
        Returns:
            List of proto file paths
            
        Raises:
            ValidationError: If search path is invalid
        """
        with PerformanceTimer(self._logger, "proto_discovery", 
                             search_path=search_path, recursive=recursive):
            path = Path(search_path)
            
            if not path.exists():
                self._logger.error("Search path does not exist", search_path=search_path)
                raise ValidationError(f"Search path does not exist: {search_path}")
            
            if not path.is_dir():
                self._logger.error("Search path is not a directory", search_path=search_path)
                raise ValidationError(f"Search path is not a directory: {search_path}")
            
            self._logger.debug("Starting proto file discovery", 
                             search_path=search_path, recursive=recursive)
            
            proto_files = []
            
            if recursive:
                proto_files = list(path.rglob("*.proto"))
            else:
                proto_files = list(path.glob("*.proto"))
            
            proto_file_paths = [str(f) for f in proto_files]
            
            self._logger.info("Proto file discovery completed", 
                             search_path=search_path, 
                             files_found=len(proto_file_paths),
                             recursive=recursive)
            
            return proto_file_paths
    
    def validate_proto_syntax(self, proto_file: str) -> None:
        """
        Validate proto file syntax using protoc --dry-run.
        
        Args:
            proto_file: Path to proto file to validate
            
        Raises:
            ValidationError: If proto file is invalid or validation fails
        """
        with PerformanceTimer(self._logger, "proto_validation", schema_file=proto_file):
            proto_path = Path(proto_file)
            
            if not proto_path.exists():
                self._logger.error("Proto file does not exist", schema_file=proto_file)
                raise ValidationError(f"Proto file does not exist: {proto_file}")
            
            self._logger.debug("Starting proto syntax validation", schema_file=proto_file)
            
            # Use protoc to validate syntax by compiling to a temporary directory
            import tempfile
            with tempfile.TemporaryDirectory() as temp_dir:
                cmd = [
                    'protoc',
                    '--proto_path', str(proto_path.parent),
                    '--python_out', temp_dir,
                    str(proto_path)
                ]
                
                try:
                    start_time = time.time()
                    result = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True,
                        check=False
                    )
                    duration = time.time() - start_time
                    
                    if result.returncode != 0:
                        self._logger.error("Proto validation failed", 
                                         schema_file=proto_file,
                                         compiler_output=result.stderr,
                                         exit_code=result.returncode,
                                         duration_seconds=round(duration, 3))
                        raise ValidationError(f"Proto validation failed: {result.stderr}")
                    
                    self._logger.info("Proto syntax validation successful",
                                    schema_file=proto_file,
                                    duration_seconds=round(duration, 3))
                        
                except FileNotFoundError:
                    self._logger.error("protoc compiler not found", schema_file=proto_file)
                    raise ValidationError("protoc compiler not found. Please install Protocol Buffers compiler.")
                except Exception as e:
                    self._logger.error("Error during proto validation", 
                                     schema_file=proto_file, error=str(e))
                    raise ValidationError(f"Error during proto validation: {e}")
    
    def compile_lifecycle(
        self, 
        proto_file: str,
        output_dir: Optional[str] = None,
        cleanup_on_success: bool = False,
        cleanup_on_failure: bool = True
    ) -> str:
        """
        Manage proto compilation lifecycle with proper cleanup.
        
        Args:
            proto_file: Path to proto file to compile
            output_dir: Optional output directory (uses temp dir if not specified)
            cleanup_on_success: Whether to cleanup on successful compilation
            cleanup_on_failure: Whether to cleanup on failed compilation
            
        Returns:
            Path to compilation output directory
            
        Raises:
            CompilationError: If compilation fails
        """
        start_time = time.time()
        proto_path = Path(proto_file)
        
        if not proto_path.exists():
            self._logger.error("Proto file does not exist for compilation", schema_file=proto_file)
            raise CompilationError(f"Proto file does not exist: {proto_file}")
        
        # Create output directory
        if output_dir is None:
            output_dir = tempfile.mkdtemp(prefix='testdatapy_schema_')
            temp_output = True
        else:
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            temp_output = False
        
        self._logger.info("Starting proto compilation", 
                         schema_file=proto_file,
                         output_dir=output_dir,
                         temp_output=temp_output,
                         cleanup_on_success=cleanup_on_success,
                         cleanup_on_failure=cleanup_on_failure)
        
        # Compile proto file
        cmd = [
            'protoc',
            '--python_out', output_dir,
            '--proto_path', str(proto_path.parent),
            str(proto_path)
        ]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False
            )
            duration = time.time() - start_time
            
            if result.returncode != 0:
                if cleanup_on_failure:
                    self._cleanup_directory(output_dir)
                
                self._logger.log_schema_compilation(
                    schema_path=proto_file,
                    compiler='protoc',
                    success=False,
                    duration=duration,
                    error=result.stderr
                )
                raise CompilationError(f"Proto compilation failed: {result.stderr}")
            
            # Success
            self._logger.log_schema_compilation(
                schema_path=proto_file,
                compiler='protoc',
                success=True,
                duration=duration,
                output=result.stdout if result.stdout else "Compilation successful"
            )
            
            if cleanup_on_success:
                self._cleanup_directory(output_dir)
                self._logger.debug("Cleaned up output directory after successful compilation",
                                 output_dir=output_dir)
                return output_dir  # Return path even if cleaned up
            
            return output_dir
            
        except FileNotFoundError:
            duration = time.time() - start_time
            if cleanup_on_failure:
                self._cleanup_directory(output_dir)
            
            self._logger.log_schema_compilation(
                schema_path=proto_file,
                compiler='protoc',
                success=False,
                duration=duration,
                error="protoc compiler not found"
            )
            raise CompilationError("protoc compiler not found. Please install Protocol Buffers compiler.")
        except Exception as e:
            duration = time.time() - start_time
            if cleanup_on_failure:
                self._cleanup_directory(output_dir)
            
            self._logger.log_schema_compilation(
                schema_path=proto_file,
                compiler='protoc',
                success=False,
                duration=duration,
                error=str(e)
            )
            raise CompilationError(f"Error during proto compilation: {e}")
    
    def add_search_path(self, path: str, priority: int = 0) -> None:
        """
        Add schema search path with optional priority.
        
        Args:
            path: Path to add to search paths
            priority: Priority for ordering (higher priority = earlier in list)
            
        Raises:
            ValidationError: If path is invalid
        """
        resolved_path = Path(path).resolve()
        
        if not resolved_path.exists():
            self._logger.error("Search path does not exist", path=path)
            raise ValidationError(f"Search path does not exist: {path}")
        
        if not resolved_path.is_dir():
            self._logger.error("Search path is not a directory", path=path)
            raise ValidationError(f"Search path is not a directory: {path}")
        
        path_str = str(resolved_path)
        
        # Check if path already exists
        existing_paths = [p[0] for p in self._search_paths]
        if path_str not in existing_paths:
            # Insert based on priority (higher priority first)
            inserted = False
            for i, (existing_path, existing_priority) in enumerate(self._search_paths):
                if priority > existing_priority:
                    self._search_paths.insert(i, (path_str, priority))
                    inserted = True
                    break
            
            if not inserted:
                self._search_paths.append((path_str, priority))
            
            self._logger.info("Added search path", 
                            path=path_str, priority=priority, 
                            total_paths=len(self._search_paths))
        else:
            self._logger.debug("Search path already exists", path=path_str)
    
    def remove_search_path(self, path: str) -> None:
        """
        Remove path from search paths.
        
        Args:
            path: Path to remove from search paths
        """
        resolved_path = str(Path(path).resolve())
        
        # Find and remove the path tuple
        for i, (search_path, _) in enumerate(self._search_paths):
            if search_path == resolved_path:
                self._search_paths.pop(i)
                self._logger.info("Removed search path", 
                                path=resolved_path, remaining_paths=len(self._search_paths))
                break
        else:
            self._logger.warning("Search path not found for removal", path=resolved_path)
    
    def list_search_paths(self) -> List[str]:
        """
        List all configured search paths.
        
        Returns:
            List of search paths in priority order
        """
        return [path for path, _ in self._search_paths]
    
    def _cleanup_directory(self, directory: str) -> None:
        """
        Clean up directory and its contents.
        
        Args:
            directory: Directory to clean up
        """
        import shutil
        
        try:
            if Path(directory).exists():
                shutil.rmtree(directory)
                self._logger.debug("Cleaned up directory", directory=directory)
        except Exception as e:
            self._logger.warning("Failed to cleanup directory", directory=directory, error=str(e))