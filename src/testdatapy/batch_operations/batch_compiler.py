"""Batch schema compilation operations.

This module provides comprehensive batch processing capabilities for schema
compilation, registration, and validation operations.
"""

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Union
from dataclasses import dataclass, asdict
from enum import Enum

from ..logging_config import get_schema_logger, PerformanceTimer
from ..performance.performance_monitor import get_performance_monitor, monitored
from ..schema_cache import get_schema_cache
from ..schema.compiler import ProtobufCompiler


class BatchStatus(Enum):
    """Batch operation status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class BatchItem:
    """Individual item in a batch operation."""
    id: str
    schema_path: str
    schema_type: str
    status: BatchStatus
    result: Optional[Any] = None
    error: Optional[str] = None
    duration: float = 0.0
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = asdict(self)
        data['status'] = self.status.value
        return data


@dataclass
class BatchResult:
    """Result of a batch operation."""
    batch_id: str
    operation_type: str
    total_items: int
    successful_items: int
    failed_items: int
    cancelled_items: int
    total_duration: float
    items: List[BatchItem]
    metadata: Dict[str, Any]
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.total_items == 0:
            return 0.0
        return self.successful_items / self.total_items
    
    def get_successful_items(self) -> List[BatchItem]:
        """Get successfully processed items."""
        return [item for item in self.items if item.status == BatchStatus.COMPLETED]
    
    def get_failed_items(self) -> List[BatchItem]:
        """Get failed items."""
        return [item for item in self.items if item.status == BatchStatus.FAILED]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "batch_id": self.batch_id,
            "operation_type": self.operation_type,
            "total_items": self.total_items,
            "successful_items": self.successful_items,
            "failed_items": self.failed_items,
            "cancelled_items": self.cancelled_items,
            "total_duration": self.total_duration,
            "success_rate": self.success_rate,
            "items": [item.to_dict() for item in self.items],
            "metadata": self.metadata
        }


class BatchCompiler:
    """Batch schema compilation manager."""
    
    def __init__(self, max_workers: int = 4, use_cache: bool = True):
        """
        Initialize batch compiler.
        
        Args:
            max_workers: Maximum number of worker threads
            use_cache: Whether to use schema caching
        """
        self._logger = get_schema_logger(__name__)
        self._monitor = get_performance_monitor()
        self._max_workers = max_workers
        self._use_cache = use_cache
        
        # Initialize components
        self._compiler = ProtobufCompiler()
        self._cache = get_schema_cache() if use_cache else None
        
        # Active batches tracking
        self._active_batches: Dict[str, BatchResult] = {}
        
        self._logger.info("BatchCompiler initialized",
                         max_workers=max_workers,
                         use_cache=use_cache)
    
    @monitored("batch_compile")
    def compile_batch(self, schema_paths: List[str], 
                     include_paths: Optional[List[str]] = None,
                     output_dir: Optional[str] = None,
                     parallel: bool = True,
                     progress_callback: Optional[callable] = None) -> BatchResult:
        """
        Compile multiple schemas in batch.
        
        Args:
            schema_paths: List of schema file paths
            include_paths: Additional include paths for compilation
            output_dir: Output directory for compiled schemas
            parallel: Whether to use parallel processing
            progress_callback: Optional callback for progress updates
            
        Returns:
            Batch compilation result
        """
        batch_id = f"compile_{int(time.time() * 1000)}"
        start_time = time.perf_counter()
        
        # Create batch items
        items = []
        for i, schema_path in enumerate(schema_paths):
            item = BatchItem(
                id=f"{batch_id}_{i}",
                schema_path=schema_path,
                schema_type="protobuf",
                status=BatchStatus.PENDING,
                metadata={"include_paths": include_paths or []}
            )
            items.append(item)
        
        # Initialize batch result
        batch_result = BatchResult(
            batch_id=batch_id,
            operation_type="compile",
            total_items=len(items),
            successful_items=0,
            failed_items=0,
            cancelled_items=0,
            total_duration=0.0,
            items=items,
            metadata={
                "include_paths": include_paths or [],
                "output_dir": output_dir,
                "parallel": parallel,
                "max_workers": self._max_workers if parallel else 1
            }
        )
        
        self._active_batches[batch_id] = batch_result
        
        try:
            if parallel and len(items) > 1:
                self._compile_parallel(batch_result, include_paths, output_dir, progress_callback)
            else:
                self._compile_sequential(batch_result, include_paths, output_dir, progress_callback)
            
        except Exception as e:
            self._logger.error("Batch compilation failed", batch_id=batch_id, error=str(e))
            # Mark remaining items as failed
            for item in items:
                if item.status == BatchStatus.PENDING:
                    item.status = BatchStatus.FAILED
                    item.error = f"Batch failed: {str(e)}"
        
        finally:
            # Calculate final statistics
            batch_result.total_duration = time.perf_counter() - start_time
            batch_result.successful_items = len([i for i in items if i.status == BatchStatus.COMPLETED])
            batch_result.failed_items = len([i for i in items if i.status == BatchStatus.FAILED])
            batch_result.cancelled_items = len([i for i in items if i.status == BatchStatus.CANCELLED])
            
            # Remove from active batches
            self._active_batches.pop(batch_id, None)
        
        self._logger.info("Batch compilation completed",
                         batch_id=batch_id,
                         total_items=batch_result.total_items,
                         successful=batch_result.successful_items,
                         failed=batch_result.failed_items,
                         duration=batch_result.total_duration)
        
        return batch_result
    
    def _compile_sequential(self, batch_result: BatchResult, 
                           include_paths: Optional[List[str]],
                           output_dir: Optional[str],
                           progress_callback: Optional[callable]):
        """Compile schemas sequentially."""
        for i, item in enumerate(batch_result.items):
            try:
                item.status = BatchStatus.RUNNING
                if progress_callback:
                    progress_callback(batch_result.batch_id, i, len(batch_result.items), item)
                
                # Attempt to get from cache first
                compiled_path = None
                if self._cache:
                    cache_result = self._cache.get(item.schema_path, item.schema_type)
                    if cache_result:
                        compiled_path, cache_entry = cache_result
                        item.result = compiled_path
                        item.duration = 0.0  # Cache hit
                        item.metadata["from_cache"] = True
                        item.status = BatchStatus.COMPLETED
                        continue
                
                # Compile schema
                start_time = time.perf_counter()
                
                try:
                    compiled_path = self._compiler.compile_schema(
                        proto_file=item.schema_path,
                        include_paths=include_paths,
                        output_dir=output_dir
                    )
                    
                    item.duration = time.perf_counter() - start_time
                    item.result = compiled_path
                    item.metadata["from_cache"] = False
                    item.status = BatchStatus.COMPLETED
                    
                    # Store in cache
                    if self._cache and compiled_path:
                        self._cache.put(
                            schema_path=item.schema_path,
                            compiled_path=compiled_path,
                            compilation_duration=item.duration,
                            schema_type=item.schema_type
                        )
                    
                except Exception as e:
                    item.duration = time.perf_counter() - start_time
                    item.error = str(e)
                    item.status = BatchStatus.FAILED
                    self._logger.error("Schema compilation failed",
                                     schema_path=item.schema_path,
                                     error=str(e))
                
            except Exception as e:
                item.error = f"Unexpected error: {str(e)}"
                item.status = BatchStatus.FAILED
    
    def _compile_parallel(self, batch_result: BatchResult,
                         include_paths: Optional[List[str]],
                         output_dir: Optional[str],
                         progress_callback: Optional[callable]):
        """Compile schemas in parallel using thread pool."""
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            # Submit all compilation tasks
            future_to_item = {}
            
            for item in batch_result.items:
                future = executor.submit(
                    self._compile_single_item,
                    item,
                    include_paths,
                    output_dir
                )
                future_to_item[future] = item
                item.status = BatchStatus.RUNNING
            
            # Process completed tasks
            completed = 0
            for future in as_completed(future_to_item):
                item = future_to_item[future]
                
                try:
                    # Get result from future
                    future.result()  # This will raise exception if compilation failed
                    
                except Exception as e:
                    if item.status != BatchStatus.FAILED:  # May have been set in _compile_single_item
                        item.error = f"Future execution error: {str(e)}"
                        item.status = BatchStatus.FAILED
                
                completed += 1
                if progress_callback:
                    progress_callback(batch_result.batch_id, completed, len(batch_result.items), item)
    
    def _compile_single_item(self, item: BatchItem,
                           include_paths: Optional[List[str]],
                           output_dir: Optional[str]):
        """Compile a single schema item."""
        try:
            # Check cache first
            if self._cache:
                cache_result = self._cache.get(item.schema_path, item.schema_type)
                if cache_result:
                    compiled_path, cache_entry = cache_result
                    item.result = compiled_path
                    item.duration = 0.0
                    item.metadata["from_cache"] = True
                    item.status = BatchStatus.COMPLETED
                    return
            
            # Compile schema
            start_time = time.perf_counter()
            
            compiled_path = self._compiler.compile_schema(
                proto_file=item.schema_path,
                include_paths=include_paths,
                output_dir=output_dir
            )
            
            item.duration = time.perf_counter() - start_time
            item.result = compiled_path
            item.metadata["from_cache"] = False
            item.status = BatchStatus.COMPLETED
            
            # Store in cache
            if self._cache and compiled_path:
                self._cache.put(
                    schema_path=item.schema_path,
                    compiled_path=compiled_path,
                    compilation_duration=item.duration,
                    schema_type=item.schema_type
                )
            
        except Exception as e:
            item.duration = time.perf_counter() - time.perf_counter()  # Will be small
            item.error = str(e)
            item.status = BatchStatus.FAILED
            raise  # Re-raise for future handling
    
    @monitored("batch_discover")
    def discover_schemas(self, directory: Union[str, Path],
                        pattern: str = "*.proto",
                        recursive: bool = True) -> List[str]:
        """
        Discover schema files in a directory.
        
        Args:
            directory: Directory to search
            pattern: File pattern to match
            recursive: Whether to search recursively
            
        Returns:
            List of discovered schema file paths
        """
        directory = Path(directory)
        
        if not directory.exists():
            raise ValueError(f"Directory not found: {directory}")
        
        if recursive:
            schema_files = list(directory.rglob(pattern))
        else:
            schema_files = list(directory.glob(pattern))
        
        # Convert to string paths and sort
        schema_paths = [str(f) for f in schema_files]
        schema_paths.sort()
        
        self._logger.info("Schema discovery completed",
                         directory=str(directory),
                         pattern=pattern,
                         recursive=recursive,
                         found_count=len(schema_paths))
        
        return schema_paths
    
    def get_active_batches(self) -> Dict[str, BatchResult]:
        """Get currently active batch operations."""
        return self._active_batches.copy()
    
    def cancel_batch(self, batch_id: str) -> bool:
        """
        Cancel an active batch operation.
        
        Args:
            batch_id: Batch ID to cancel
            
        Returns:
            True if batch was cancelled, False if not found
        """
        if batch_id not in self._active_batches:
            return False
        
        batch_result = self._active_batches[batch_id]
        
        # Mark pending items as cancelled
        for item in batch_result.items:
            if item.status == BatchStatus.PENDING:
                item.status = BatchStatus.CANCELLED
        
        self._logger.info("Batch operation cancelled", batch_id=batch_id)
        return True
    
    def get_compilation_stats(self) -> Dict[str, Any]:
        """Get compilation statistics."""
        # Get cache statistics if available
        cache_stats = self._cache.get_stats() if self._cache else {}
        
        return {
            "max_workers": self._max_workers,
            "use_cache": self._use_cache,
            "cache_stats": cache_stats,
            "active_batches": len(self._active_batches)
        }


# Progress callback utilities

class ProgressTracker:
    """Helper class for tracking batch progress."""
    
    def __init__(self, show_details: bool = True):
        self.show_details = show_details
        self.last_update = 0
        
    def __call__(self, batch_id: str, completed: int, total: int, current_item: BatchItem):
        """Progress callback implementation."""
        now = time.time()
        
        # Throttle updates to avoid spam
        if now - self.last_update < 0.5:  # Update at most every 0.5 seconds
            return
        
        self.last_update = now
        
        percentage = (completed / total) * 100 if total > 0 else 0
        
        if self.show_details:
            status_symbol = {
                BatchStatus.COMPLETED: "✅",
                BatchStatus.FAILED: "❌",
                BatchStatus.RUNNING: "🔄",
                BatchStatus.PENDING: "⏳",
                BatchStatus.CANCELLED: "🚫"
            }.get(current_item.status, "❓")
            
            print(f"\r{status_symbol} [{completed}/{total}] ({percentage:.1f}%) - {Path(current_item.schema_path).name}", end="", flush=True)
        else:
            print(f"\rProgress: [{completed}/{total}] ({percentage:.1f}%)", end="", flush=True)


def create_progress_tracker(show_details: bool = True) -> ProgressTracker:
    """Create a progress tracker for batch operations."""
    return ProgressTracker(show_details=show_details)