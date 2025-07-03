"""ProtobufCompiler for programmatic protoc integration."""

import logging
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from .exceptions import CompilationError, ValidationError
from ..logging_config import get_schema_logger, PerformanceTimer


class ProtobufCompiler:
    """Programmatic wrapper around protoc with error handling and cross-platform support."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize ProtobufCompiler with optional configuration.
        
        Args:
            config: Optional configuration dictionary
            
        Raises:
            ValidationError: If configuration is invalid
        """
        self._logger = get_schema_logger(__name__)
        self._config = config or {}
        
        # Set operation context
        self._logger.set_operation_context(component='ProtobufCompiler')
        
        # Validate configuration
        self._validate_config(self._config)
        
        # Set protoc path
        self._protoc_path = self._config.get('protoc_path')
        if not self._protoc_path:
            self._protoc_path = self.detect_protoc()
        
        self._logger.info("ProtobufCompiler initialized", 
                         protoc_path=self._protoc_path,
                         config_keys=list(self._config.keys()))
    
    def _validate_config(self, config: Dict[str, Any]) -> None:
        """
        Validate configuration dictionary.
        
        Args:
            config: Configuration to validate
            
        Raises:
            ValidationError: If configuration is invalid
        """
        valid_keys = {'protoc_path', 'include_paths', 'timeout', 'preserve_on_error'}
        
        for key in config.keys():
            if key not in valid_keys:
                raise ValidationError(f"Invalid configuration key: {key}")
    
    def detect_protoc(self) -> str:
        """
        Detect protoc compiler in system PATH.
        
        Returns:
            Path to protoc executable
            
        Raises:
            ValidationError: If protoc is not found
        """
        with PerformanceTimer(self._logger, "protoc_detection"):
            executable_name = self.get_protoc_executable_name()
            self._logger.debug("Detecting protoc compiler", executable_name=executable_name)
            
            protoc_path = shutil.which(executable_name)
            
            if not protoc_path:
                self._logger.error("protoc compiler not found in PATH", 
                                 executable_name=executable_name)
                raise ValidationError(
                    f"protoc compiler not found in PATH. "
                    f"Please install Protocol Buffers compiler. "
                    f"Looking for executable: {executable_name}"
                )
            
            self.validate_protoc_path(protoc_path)
            self._logger.info("protoc compiler detected", protoc_path=protoc_path)
            return protoc_path
    
    def get_protoc_executable_name(self) -> str:
        """
        Get platform-specific protoc executable name.
        
        Returns:
            Executable name for current platform
        """
        return 'protoc.exe' if platform.system() == 'Windows' else 'protoc'
    
    def validate_protoc_path(self, protoc_path: str) -> None:
        """
        Validate protoc executable path.
        
        Args:
            protoc_path: Path to protoc executable
            
        Raises:
            ValidationError: If protoc path is invalid
        """
        path = Path(protoc_path)
        
        if not path.exists():
            raise ValidationError(f"protoc path does not exist: {protoc_path}")
        
        if not os.access(path, os.X_OK):
            raise ValidationError(f"protoc path is not executable: {protoc_path}")
    
    def compile_proto(
        self,
        proto_file: str,
        output_dir: str,
        include_paths: Optional[List[str]] = None,
        timeout: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Compile proto file using protoc.
        
        Args:
            proto_file: Path to proto file to compile
            output_dir: Directory for compilation output
            include_paths: Optional list of include paths
            timeout: Optional timeout in seconds
            
        Returns:
            Compilation result dictionary
            
        Raises:
            ValidationError: If input parameters are invalid
            CompilationError: If compilation fails
        """
        start_time = time.time()
        
        # Validate inputs
        proto_path = Path(proto_file)
        if not proto_path.exists():
            self._logger.error("Proto file does not exist", proto_file=proto_file)
            raise ValidationError(f"Proto file does not exist: {proto_file}")
        
        output_path = Path(output_dir)
        if not output_path.exists():
            try:
                output_path.mkdir(parents=True, exist_ok=True)
                self._logger.debug("Created output directory", output_dir=output_dir)
            except Exception as e:
                self._logger.error("Cannot create output directory", 
                                 output_dir=output_dir, error=str(e))
                raise ValidationError(f"Cannot create output directory {output_dir}: {e}")
        
        # Build protoc command
        cmd = [
            self._protoc_path,
            '--python_out', str(output_path),
            '--proto_path', str(proto_path.parent)
        ]
        
        # Add include paths
        if include_paths:
            for include_path in include_paths:
                cmd.extend(['--proto_path', self.normalize_path(include_path)])
        
        cmd.append(str(proto_path))
        
        self._logger.info("Starting protoc compilation", 
                         proto_file=proto_file,
                         output_dir=output_dir,
                         include_paths=include_paths,
                         timeout=timeout or self._config.get('timeout', 60))
        
        # Execute compilation
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout or self._config.get('timeout', 60)
            )
            duration = time.time() - start_time
            
            if result.returncode != 0:
                self._logger.log_schema_compilation(
                    schema_path=proto_file,
                    compiler='protoc',
                    success=False,
                    duration=duration,
                    error=result.stderr
                )
                
                raise CompilationError(
                    f"Proto compilation failed: {result.stderr}",
                    schema_path=proto_file,
                    compiler_output=result.stderr,
                    exit_code=result.returncode
                )
            
            self._logger.log_schema_compilation(
                schema_path=proto_file,
                compiler='protoc',
                success=True,
                duration=duration,
                output=result.stdout if result.stdout else "Compilation successful"
            )
            
            return {
                'success': True,
                'output_dir': str(output_path),
                'stdout': result.stdout,
                'stderr': result.stderr
            }
            
        except subprocess.TimeoutExpired as e:
            duration = time.time() - start_time
            self._logger.log_schema_compilation(
                schema_path=proto_file,
                compiler='protoc',
                success=False,
                duration=duration,
                error=f"Timeout after {e.timeout} seconds"
            )
            raise CompilationError(f"Proto compilation timeout after {e.timeout} seconds")
        except PermissionError as e:
            duration = time.time() - start_time
            self._logger.log_schema_compilation(
                schema_path=proto_file,
                compiler='protoc',
                success=False,
                duration=duration,
                error=f"Permission denied: {e}"
            )
            raise CompilationError(f"Permission denied executing protoc: {e}")
        except Exception as e:
            duration = time.time() - start_time
            self._logger.log_schema_compilation(
                schema_path=proto_file,
                compiler='protoc',
                success=False,
                duration=duration,
                error=str(e)
            )
            raise CompilationError(f"Unexpected error during compilation: {e}")
    
    def resolve_dependencies(self, proto_file: str, search_paths: List[str]) -> List[str]:
        """
        Resolve proto file dependencies recursively.
        
        Args:
            proto_file: Main proto file to analyze
            search_paths: Directories to search for imports
            
        Returns:
            List of dependency proto files
            
        Raises:
            ValidationError: If dependencies cannot be resolved
            CompilationError: If circular dependencies are detected
        """
        with PerformanceTimer(self._logger, "dependency_resolution", 
                             proto_file=proto_file, search_paths=len(search_paths)):
            dependencies = []
            visited = set()
            visiting = set()
            
            self._logger.debug("Starting dependency resolution", 
                             proto_file=proto_file, 
                             search_paths=search_paths)
            
            def _resolve_recursive(current_file: str) -> None:
                if current_file in visiting:
                    self._logger.error("Circular dependency detected", 
                                     current_file=current_file, 
                                     visiting_chain=list(visiting))
                    raise CompilationError(f"Circular dependency detected involving: {current_file}")
                
                if current_file in visited:
                    return
                
                visiting.add(current_file)
                
                # Parse imports from proto file
                imports = self._parse_imports(current_file)
                self._logger.debug("Parsed imports", 
                                 current_file=current_file, 
                                 imports=imports)
                
                for import_file in imports:
                    # Find the imported file in search paths
                    resolved_import = self._find_imported_file(import_file, search_paths)
                    if not resolved_import:
                        self._logger.error("Cannot resolve import", 
                                         import_file=import_file,
                                         current_file=current_file,
                                         search_paths=search_paths)
                        raise ValidationError(f"Cannot resolve import '{import_file}' from {current_file}")
                    
                    dependencies.append(resolved_import)
                    _resolve_recursive(resolved_import)
                
                visiting.remove(current_file)
                visited.add(current_file)
            
            _resolve_recursive(proto_file)
            resolved_dependencies = list(dict.fromkeys(dependencies))  # Remove duplicates while preserving order
            
            self._logger.info("Dependency resolution completed", 
                            proto_file=proto_file,
                            dependencies_found=len(resolved_dependencies))
            
            return resolved_dependencies
    
    def _parse_imports(self, proto_file: str) -> List[str]:
        """
        Parse import statements from proto file.
        
        Args:
            proto_file: Proto file to parse
            
        Returns:
            List of imported file names
        """
        imports = []
        
        try:
            with open(proto_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line.startswith('import '):
                        # Extract import path: import "path/to/file.proto";
                        import_match = line.split('"')
                        if len(import_match) >= 2:
                            imports.append(import_match[1])
        except Exception as e:
            self._logger.warning(f"Error parsing imports from {proto_file}: {e}")
        
        return imports
    
    def _find_imported_file(self, import_path: str, search_paths: List[str]) -> Optional[str]:
        """
        Find imported proto file in search paths.
        
        Args:
            import_path: Relative path from import statement
            search_paths: Directories to search
            
        Returns:
            Absolute path to imported file, or None if not found
        """
        for search_path in search_paths:
            candidate = Path(search_path) / import_path
            if candidate.exists():
                return str(candidate.resolve())
        
        return None
    
    def topological_sort(self, dependencies: List[str], main_file: str) -> List[str]:
        """
        Sort dependencies in topological order.
        
        Args:
            dependencies: List of dependency files
            main_file: Main proto file
            
        Returns:
            Dependencies sorted in compilation order
        """
        # Simple topological sort based on dependency depth
        # In a real implementation, you'd build a proper dependency graph
        
        # For now, return dependencies in reverse order (deepest first)
        # This ensures that dependencies are compiled before their dependents
        return list(reversed(dependencies))
    
    def temp_compile(
        self,
        proto_file: str,
        cleanup: bool = False,
        preserve_on_error: bool = False
    ) -> Dict[str, Any]:
        """
        Compile proto to temporary directory.
        
        Args:
            proto_file: Proto file to compile
            cleanup: Whether to cleanup temporary directory after compilation
            preserve_on_error: Whether to preserve temporary directory on error
            
        Returns:
            Compilation result with temporary directory path
            
        Raises:
            CompilationError: If compilation fails
        """
        temp_dir = tempfile.mkdtemp(prefix='testdatapy_protobuf_')
        
        try:
            result = self.compile_proto(proto_file, temp_dir)
            result['temp_dir'] = temp_dir
            
            if cleanup:
                self._cleanup_directory(temp_dir)
            
            return result
            
        except Exception as e:
            if not preserve_on_error:
                self._cleanup_directory(temp_dir)
            raise
    
    @contextmanager
    def temp_compile_context(self, proto_file: str):
        """
        Context manager for temporary compilation with sys.path management.
        
        Args:
            proto_file: Proto file to compile
            
        Yields:
            Compilation result dictionary
        """
        temp_dir = tempfile.mkdtemp(prefix='testdatapy_protobuf_')
        original_sys_path = sys.path.copy()
        
        try:
            # Compile to temporary directory
            result = self.compile_proto(proto_file, temp_dir)
            result['temp_dir'] = temp_dir
            
            # Add to sys.path for imports
            if temp_dir not in sys.path:
                sys.path.insert(0, temp_dir)
            
            yield result
            
        finally:
            # Restore sys.path
            sys.path[:] = original_sys_path
            
            # Cleanup temporary directory
            self._cleanup_directory(temp_dir)
    
    def normalize_path(self, path: str) -> str:
        """
        Normalize path for current platform.
        
        Args:
            path: Path to normalize
            
        Returns:
            Normalized path
        """
        return str(Path(path).resolve())
    
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


