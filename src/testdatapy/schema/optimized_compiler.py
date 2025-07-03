"""Optimized ProtobufCompiler with caching and performance improvements.

This module extends the base ProtobufCompiler with caching capabilities
and performance optimizations for faster compilation operations.
"""

import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from .compiler import ProtobufCompiler
from .exceptions import CompilationError, ValidationError
from ..performance.compiler_cache import get_compiler_cache
from ..performance.performance_monitor import get_performance_monitor, monitored
from ..logging_config import get_schema_logger


class OptimizedProtobufCompiler(ProtobufCompiler):
    """ProtobufCompiler with caching and performance optimizations."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None, enable_cache: bool = True):
        """
        Initialize the optimized compiler.
        
        Args:
            config: Optional configuration dictionary
            enable_cache: Whether to enable compilation caching
        """
        super().__init__(config)
        self._logger = get_schema_logger(__name__)
        self._enable_cache = enable_cache
        
        # Performance components
        self._cache = get_compiler_cache() if enable_cache else None
        self._monitor = get_performance_monitor()
        
        # Performance statistics
        self._stats = {
            "cache_hits": 0,
            "cache_misses": 0,
            "compilations": 0,
            "total_compilation_time": 0.0,
            "total_cache_time": 0.0
        }
        
        self._logger.info("OptimizedProtobufCompiler initialized", 
                         cache_enabled=enable_cache)
    
    @monitored("optimized_compilation")
    def compile_proto(
        self,
        proto_file: str,
        output_dir: str,
        include_paths: Optional[List[str]] = None,
        timeout: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Compile proto file with caching optimization.
        
        Args:
            proto_file: Path to proto file to compile
            output_dir: Directory for compilation output
            include_paths: Optional list of include paths
            timeout: Optional timeout in seconds
            
        Returns:
            Compilation result dictionary
        """
        self._stats["compilations"] += 1
        
        if self._enable_cache and self._cache:
            cache_key = self._cache.get_cache_key(proto_file, include_paths)
            
            # Try cache first
            cache_start = time.perf_counter()
            cached_result = self._cache.get(cache_key)
            cache_time = time.perf_counter() - cache_start
            self._stats["total_cache_time"] += cache_time
            
            if cached_result:
                self._stats["cache_hits"] += 1
                self._logger.debug("Using cached compilation result", 
                                 proto_file=proto_file,
                                 cache_key=cache_key[:16])
                
                # Copy cached result to target output directory
                return self._apply_cached_result(cached_result, output_dir)
        
        # Cache miss or caching disabled - perform actual compilation
        if self._enable_cache:
            self._stats["cache_misses"] += 1
        
        compilation_start = time.perf_counter()
        
        try:
            result = super().compile_proto(proto_file, output_dir, include_paths, timeout)
            
            compilation_time = time.perf_counter() - compilation_start
            self._stats["total_compilation_time"] += compilation_time
            
            # Cache successful result
            if self._enable_cache and self._cache and result.get('success'):
                cache_key = self._cache.get_cache_key(proto_file, include_paths)
                self._cache.put(cache_key, self._prepare_for_cache(result, proto_file))
            
            return result
        
        except Exception as e:
            compilation_time = time.perf_counter() - compilation_start
            self._stats["total_compilation_time"] += compilation_time
            raise
    
    def _prepare_for_cache(self, result: Dict[str, Any], proto_file: str) -> Dict[str, Any]:
        """
        Prepare compilation result for caching.
        
        Args:
            result: Compilation result
            proto_file: Original proto file path
            
        Returns:
            Cacheable result data
        """
        # Store relative paths and essential data
        output_dir = Path(result['output_dir'])
        proto_path = Path(proto_file)
        
        # Find generated Python files
        generated_files = {}
        python_file_pattern = f"{proto_path.stem}_pb2.py"
        
        for py_file in output_dir.glob("*_pb2.py"):
            if py_file.name == python_file_pattern:
                with open(py_file, 'r', encoding='utf-8') as f:
                    generated_files[py_file.name] = f.read()
        
        return {
            'success': result['success'],
            'generated_files': generated_files,
            'proto_file': str(proto_path),
            'stdout': result.get('stdout', ''),
            'stderr': result.get('stderr', ''),
            'timestamp': time.time()
        }
    
    def _apply_cached_result(self, cached_result: Dict[str, Any], target_output_dir: str) -> Dict[str, Any]:
        """
        Apply cached compilation result to target directory.
        
        Args:
            cached_result: Cached result data
            target_output_dir: Target output directory
            
        Returns:
            Applied compilation result
        """
        output_path = Path(target_output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Write cached generated files to target directory
        for filename, content in cached_result['generated_files'].items():
            target_file = output_path / filename
            with open(target_file, 'w', encoding='utf-8') as f:
                f.write(content)
        
        return {
            'success': cached_result['success'],
            'output_dir': str(output_path),
            'stdout': cached_result['stdout'],
            'stderr': cached_result['stderr'],
            'from_cache': True,
            'cache_timestamp': cached_result['timestamp']
        }
    
    @monitored("batch_compilation") 
    def compile_multiple_protos(
        self,
        proto_files: List[str],
        output_dir: str,
        include_paths: Optional[List[str]] = None,
        timeout: Optional[int] = None,
        parallel: bool = True
    ) -> Dict[str, Any]:
        """
        Compile multiple proto files with optimization.
        
        Args:
            proto_files: List of proto files to compile
            output_dir: Directory for compilation output
            include_paths: Optional list of include paths
            timeout: Optional timeout in seconds
            parallel: Whether to use parallel compilation
            
        Returns:
            Batch compilation results
        """
        results = {
            'success': True,
            'compiled_files': [],
            'failed_files': [],
            'from_cache': [],
            'compilation_times': {},
            'total_time': 0
        }
        
        start_time = time.perf_counter()
        
        if parallel and len(proto_files) > 1:
            results.update(self._compile_parallel(proto_files, output_dir, include_paths, timeout))
        else:
            results.update(self._compile_sequential(proto_files, output_dir, include_paths, timeout))
        
        results['total_time'] = time.perf_counter() - start_time
        return results
    
    def _compile_sequential(
        self,
        proto_files: List[str],
        output_dir: str,
        include_paths: Optional[List[str]],
        timeout: Optional[int]
    ) -> Dict[str, Any]:
        """Compile proto files sequentially."""
        results = {
            'compiled_files': [],
            'failed_files': [],
            'from_cache': [],
            'compilation_times': {}
        }
        
        for proto_file in proto_files:
            file_start = time.perf_counter()
            
            try:
                result = self.compile_proto(proto_file, output_dir, include_paths, timeout)
                
                if result['success']:
                    results['compiled_files'].append(proto_file)
                    if result.get('from_cache'):
                        results['from_cache'].append(proto_file)
                else:
                    results['failed_files'].append(proto_file)
            
            except Exception as e:
                results['failed_files'].append(proto_file)
                self._logger.error("Proto compilation failed",
                                 proto_file=proto_file, error=str(e))
            
            file_time = time.perf_counter() - file_start
            results['compilation_times'][proto_file] = file_time
        
        return results
    
    def _compile_parallel(
        self,
        proto_files: List[str],
        output_dir: str,
        include_paths: Optional[List[str]],
        timeout: Optional[int]
    ) -> Dict[str, Any]:
        """Compile proto files in parallel."""
        import concurrent.futures
        import threading
        
        results = {
            'compiled_files': [],
            'failed_files': [],
            'from_cache': [],
            'compilation_times': {}
        }
        
        results_lock = threading.Lock()
        
        def compile_single(proto_file: str) -> Dict[str, Any]:
            """Compile a single proto file."""
            file_start = time.perf_counter()
            
            try:
                # Each thread gets its own compiler instance
                compiler = OptimizedProtobufCompiler(self._config, self._enable_cache)
                result = compiler.compile_proto(proto_file, output_dir, include_paths, timeout)
                
                file_time = time.perf_counter() - file_start
                
                with results_lock:
                    if result['success']:
                        results['compiled_files'].append(proto_file)
                        if result.get('from_cache'):
                            results['from_cache'].append(proto_file)
                    else:
                        results['failed_files'].append(proto_file)
                    
                    results['compilation_times'][proto_file] = file_time
                
                return result
            
            except Exception as e:
                file_time = time.perf_counter() - file_start
                
                with results_lock:
                    results['failed_files'].append(proto_file)
                    results['compilation_times'][proto_file] = file_time
                
                self._logger.error("Parallel proto compilation failed",
                                 proto_file=proto_file, error=str(e))
                raise
        
        # Use thread pool for parallel compilation
        max_workers = min(len(proto_files), os.cpu_count() or 4)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(compile_single, proto_file) for proto_file in proto_files]
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception:
                    pass  # Already logged in compile_single
        
        return results
    
    def get_performance_statistics(self) -> Dict[str, Any]:
        """Get performance statistics."""
        stats = self._stats.copy()
        
        if stats["compilations"] > 0:
            stats["avg_compilation_time"] = stats["total_compilation_time"] / stats["compilations"]
            stats["avg_cache_time"] = stats["total_cache_time"] / stats["compilations"]
            
            if self._enable_cache:
                total_cache_requests = stats["cache_hits"] + stats["cache_misses"]
                stats["cache_hit_rate"] = (
                    stats["cache_hits"] / total_cache_requests if total_cache_requests > 0 else 0
                )
        
        # Add cache statistics if available
        if self._cache:
            cache_stats = self._cache.get_statistics()
            stats["cache_stats"] = cache_stats
        
        return stats
    
    def clear_cache(self) -> None:
        """Clear compilation cache."""
        if self._cache:
            self._cache.clear()
            self._logger.info("Compilation cache cleared")
    
    def cleanup_cache(self) -> int:
        """Clean up expired cache entries."""
        if self._cache:
            return self._cache.cleanup_expired()
        return 0
    
    def warm_cache(self, proto_files: List[str], include_paths: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Warm up the cache by pre-compiling proto files.
        
        Args:
            proto_files: List of proto files to pre-compile
            include_paths: Optional include paths
            
        Returns:
            Cache warming results
        """
        if not self._enable_cache or not self._cache:
            return {"status": "caching_disabled"}
        
        import tempfile
        
        with tempfile.TemporaryDirectory() as temp_dir:
            results = self.compile_multiple_protos(
                proto_files, temp_dir, include_paths, parallel=True
            )
        
        return {
            "status": "completed",
            "files_cached": len(results['compiled_files']),
            "files_failed": len(results['failed_files']),
            "total_time": results['total_time']
        }