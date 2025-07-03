"""Batch schema registry operations.

This module provides batch registration and management capabilities for
Schema Registry operations including compatibility checks and version management.
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Union
from dataclasses import dataclass, asdict
from enum import Enum

from ..logging_config import get_schema_logger
from ..performance.performance_monitor import get_performance_monitor, monitored
from ..schema.registry_manager import SchemaRegistryManager
from .batch_compiler import BatchStatus, BatchItem


class RegistryOperation(Enum):
    """Types of registry operations."""
    REGISTER = "register"
    UPDATE = "update"
    CHECK_COMPATIBILITY = "check_compatibility"
    DELETE = "delete"
    LIST_VERSIONS = "list_versions"


@dataclass
class RegistryItem(BatchItem):
    """Registry operation item extending BatchItem."""
    operation: RegistryOperation = None
    subject: str = ""
    schema_content: Optional[str] = None
    compatibility_level: Optional[str] = None
    version: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = super().to_dict()
        data['operation'] = self.operation.value
        data['subject'] = self.subject
        data['schema_content'] = self.schema_content
        data['compatibility_level'] = self.compatibility_level
        data['version'] = self.version
        return data


@dataclass
class RegistryResult:
    """Result of batch registry operations."""
    batch_id: str
    operation_type: str
    total_items: int
    successful_items: int
    failed_items: int
    cancelled_items: int
    total_duration: float
    items: List[RegistryItem]
    metadata: Dict[str, Any]
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.total_items == 0:
            return 0.0
        return self.successful_items / self.total_items
    
    def get_successful_items(self) -> List[RegistryItem]:
        """Get successfully processed items."""
        return [item for item in self.items if item.status == BatchStatus.COMPLETED]
    
    def get_failed_items(self) -> List[RegistryItem]:
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


class BatchRegistryManager:
    """Batch schema registry operations manager."""
    
    def __init__(self, schema_registry_url: str, max_workers: int = 4):
        """
        Initialize batch registry manager.
        
        Args:
            schema_registry_url: Schema Registry URL
            max_workers: Maximum number of worker threads
        """
        self._logger = get_schema_logger(__name__)
        self._monitor = get_performance_monitor()
        self._max_workers = max_workers
        
        # Initialize registry manager
        self._registry = SchemaRegistryManager(schema_registry_url)
        
        # Active batches tracking
        self._active_batches: Dict[str, RegistryResult] = {}
        
        self._logger.info("BatchRegistryManager initialized",
                         schema_registry_url=schema_registry_url,
                         max_workers=max_workers)
    
    @monitored("batch_register")
    def register_schemas_batch(self, schema_files: List[str],
                              subject_template: str = "{schema_name}-value",
                              compatibility_level: Optional[str] = None,
                              parallel: bool = True,
                              check_compatibility: bool = True,
                              progress_callback: Optional[callable] = None) -> RegistryResult:
        """
        Register multiple schemas in batch.
        
        Args:
            schema_files: List of schema file paths
            subject_template: Template for generating subjects (supports {schema_name})
            compatibility_level: Compatibility level to set
            parallel: Whether to use parallel processing
            check_compatibility: Whether to check compatibility before registration
            progress_callback: Optional callback for progress updates
            
        Returns:
            Batch registry result
        """
        batch_id = f"register_{int(time.time() * 1000)}"
        start_time = time.perf_counter()
        
        # Create registry items
        items = []
        for i, schema_file in enumerate(schema_files):
            schema_name = Path(schema_file).stem
            subject = subject_template.format(schema_name=schema_name)
            
            # Read schema content
            try:
                with open(schema_file, 'r') as f:
                    schema_content = f.read()
            except Exception as e:
                schema_content = None
                
            item = RegistryItem(
                id=f"{batch_id}_{i}",
                schema_path=schema_file,
                schema_type="protobuf",
                status=BatchStatus.PENDING,
                operation=RegistryOperation.REGISTER,
                subject=subject,
                schema_content=schema_content,
                compatibility_level=compatibility_level,
                metadata={
                    "check_compatibility": check_compatibility,
                    "subject_template": subject_template
                }
            )
            
            if schema_content is None:
                item.status = BatchStatus.FAILED
                item.error = f"Could not read schema file: {schema_file}"
            
            items.append(item)
        
        # Initialize batch result
        batch_result = RegistryResult(
            batch_id=batch_id,
            operation_type="register",
            total_items=len(items),
            successful_items=0,
            failed_items=0,
            cancelled_items=0,
            total_duration=0.0,
            items=items,
            metadata={
                "subject_template": subject_template,
                "compatibility_level": compatibility_level,
                "parallel": parallel,
                "check_compatibility": check_compatibility,
                "max_workers": self._max_workers if parallel else 1
            }
        )
        
        self._active_batches[batch_id] = batch_result
        
        try:
            if parallel and len(items) > 1:
                self._register_parallel(batch_result, progress_callback)
            else:
                self._register_sequential(batch_result, progress_callback)
                
        except Exception as e:
            self._logger.error("Batch registration failed", batch_id=batch_id, error=str(e))
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
        
        self._logger.info("Batch registration completed",
                         batch_id=batch_id,
                         total_items=batch_result.total_items,
                         successful=batch_result.successful_items,
                         failed=batch_result.failed_items,
                         duration=batch_result.total_duration)
        
        return batch_result
    
    @monitored("batch_compatibility_check")
    def check_compatibility_batch(self, schema_files: List[str],
                                 subject_template: str = "{schema_name}-value",
                                 parallel: bool = True,
                                 progress_callback: Optional[callable] = None) -> RegistryResult:
        """
        Check compatibility for multiple schemas in batch.
        
        Args:
            schema_files: List of schema file paths
            subject_template: Template for generating subjects
            parallel: Whether to use parallel processing
            progress_callback: Optional callback for progress updates
            
        Returns:
            Batch compatibility check result
        """
        batch_id = f"compatibility_{int(time.time() * 1000)}"
        start_time = time.perf_counter()
        
        # Create registry items
        items = []
        for i, schema_file in enumerate(schema_files):
            schema_name = Path(schema_file).stem
            subject = subject_template.format(schema_name=schema_name)
            
            # Read schema content
            try:
                with open(schema_file, 'r') as f:
                    schema_content = f.read()
            except Exception as e:
                schema_content = None
            
            item = RegistryItem(
                id=f"{batch_id}_{i}",
                schema_path=schema_file,
                schema_type="protobuf",
                status=BatchStatus.PENDING,
                operation=RegistryOperation.CHECK_COMPATIBILITY,
                subject=subject,
                schema_content=schema_content,
                metadata={"subject_template": subject_template}
            )
            
            if schema_content is None:
                item.status = BatchStatus.FAILED
                item.error = f"Could not read schema file: {schema_file}"
            
            items.append(item)
        
        # Initialize batch result
        batch_result = RegistryResult(
            batch_id=batch_id,
            operation_type="check_compatibility",
            total_items=len(items),
            successful_items=0,
            failed_items=0,
            cancelled_items=0,
            total_duration=0.0,
            items=items,
            metadata={
                "subject_template": subject_template,
                "parallel": parallel,
                "max_workers": self._max_workers if parallel else 1
            }
        )
        
        self._active_batches[batch_id] = batch_result
        
        try:
            if parallel and len(items) > 1:
                self._check_compatibility_parallel(batch_result, progress_callback)
            else:
                self._check_compatibility_sequential(batch_result, progress_callback)
                
        except Exception as e:
            self._logger.error("Batch compatibility check failed", batch_id=batch_id, error=str(e))
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
        
        self._logger.info("Batch compatibility check completed",
                         batch_id=batch_id,
                         total_items=batch_result.total_items,
                         successful=batch_result.successful_items,
                         failed=batch_result.failed_items,
                         duration=batch_result.total_duration)
        
        return batch_result
    
    def _register_sequential(self, batch_result: RegistryResult,
                           progress_callback: Optional[callable]):
        """Register schemas sequentially."""
        for i, item in enumerate(batch_result.items):
            if item.status != BatchStatus.PENDING:
                continue
                
            try:
                item.status = BatchStatus.RUNNING
                if progress_callback:
                    progress_callback(batch_result.batch_id, i, len(batch_result.items), item)
                
                self._register_single_item(item)
                
            except Exception as e:
                item.error = f"Unexpected error: {str(e)}"
                item.status = BatchStatus.FAILED
    
    def _register_parallel(self, batch_result: RegistryResult,
                          progress_callback: Optional[callable]):
        """Register schemas in parallel using thread pool."""
        pending_items = [item for item in batch_result.items if item.status == BatchStatus.PENDING]
        
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            # Submit all registration tasks
            future_to_item = {}
            
            for item in pending_items:
                future = executor.submit(self._register_single_item, item)
                future_to_item[future] = item
                item.status = BatchStatus.RUNNING
            
            # Process completed tasks
            completed = 0
            for future in as_completed(future_to_item):
                item = future_to_item[future]
                
                try:
                    future.result()  # This will raise exception if registration failed
                except Exception as e:
                    if item.status != BatchStatus.FAILED:
                        item.error = f"Future execution error: {str(e)}"
                        item.status = BatchStatus.FAILED
                
                completed += 1
                if progress_callback:
                    progress_callback(batch_result.batch_id, completed, len(pending_items), item)
    
    def _register_single_item(self, item: RegistryItem):
        """Register a single schema item."""
        try:
            start_time = time.perf_counter()
            
            # Check compatibility if requested
            if item.metadata.get("check_compatibility", False):
                try:
                    compatibility_result = self._registry.check_compatibility(
                        subject=item.subject,
                        schema_str=item.schema_content
                    )
                    item.metadata["compatibility_check"] = compatibility_result
                    
                    if not compatibility_result.get("is_compatible", False):
                        item.status = BatchStatus.FAILED
                        item.error = f"Schema is not compatible: {compatibility_result.get('message', 'Unknown reason')}"
                        return
                        
                except Exception as e:
                    # Continue with registration even if compatibility check fails
                    item.metadata["compatibility_check_error"] = str(e)
            
            # Register schema
            registration_result = self._registry.register_schema(
                subject=item.subject,
                schema_str=item.schema_content
            )
            
            item.duration = time.perf_counter() - start_time
            item.result = registration_result
            item.status = BatchStatus.COMPLETED
            
            # Set compatibility level if provided
            if item.compatibility_level:
                try:
                    self._registry.set_compatibility_level(
                        subject=item.subject,
                        level=item.compatibility_level
                    )
                    item.metadata["compatibility_level_set"] = item.compatibility_level
                except Exception as e:
                    item.metadata["compatibility_level_error"] = str(e)
            
        except Exception as e:
            item.duration = time.perf_counter() - start_time if 'start_time' in locals() else 0.0
            item.error = str(e)
            item.status = BatchStatus.FAILED
            raise
    
    def _check_compatibility_sequential(self, batch_result: RegistryResult,
                                      progress_callback: Optional[callable]):
        """Check compatibility sequentially."""
        for i, item in enumerate(batch_result.items):
            if item.status != BatchStatus.PENDING:
                continue
                
            try:
                item.status = BatchStatus.RUNNING
                if progress_callback:
                    progress_callback(batch_result.batch_id, i, len(batch_result.items), item)
                
                self._check_compatibility_single_item(item)
                
            except Exception as e:
                item.error = f"Unexpected error: {str(e)}"
                item.status = BatchStatus.FAILED
    
    def _check_compatibility_parallel(self, batch_result: RegistryResult,
                                    progress_callback: Optional[callable]):
        """Check compatibility in parallel using thread pool."""
        pending_items = [item for item in batch_result.items if item.status == BatchStatus.PENDING]
        
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            # Submit all compatibility check tasks
            future_to_item = {}
            
            for item in pending_items:
                future = executor.submit(self._check_compatibility_single_item, item)
                future_to_item[future] = item
                item.status = BatchStatus.RUNNING
            
            # Process completed tasks
            completed = 0
            for future in as_completed(future_to_item):
                item = future_to_item[future]
                
                try:
                    future.result()
                except Exception as e:
                    if item.status != BatchStatus.FAILED:
                        item.error = f"Future execution error: {str(e)}"
                        item.status = BatchStatus.FAILED
                
                completed += 1
                if progress_callback:
                    progress_callback(batch_result.batch_id, completed, len(pending_items), item)
    
    def _check_compatibility_single_item(self, item: RegistryItem):
        """Check compatibility for a single schema item."""
        try:
            start_time = time.perf_counter()
            
            # Check compatibility
            compatibility_result = self._registry.check_compatibility(
                subject=item.subject,
                schema_str=item.schema_content
            )
            
            item.duration = time.perf_counter() - start_time
            item.result = compatibility_result
            item.status = BatchStatus.COMPLETED
            
        except Exception as e:
            item.duration = time.perf_counter() - start_time if 'start_time' in locals() else 0.0
            item.error = str(e)
            item.status = BatchStatus.FAILED
            raise
    
    def get_active_batches(self) -> Dict[str, RegistryResult]:
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
        
        self._logger.info("Batch registry operation cancelled", batch_id=batch_id)
        return True
    
    def get_registry_stats(self) -> Dict[str, Any]:
        """Get registry operation statistics."""
        try:
            registry_info = self._registry.get_registry_info()
        except Exception as e:
            registry_info = {"error": str(e)}
        
        return {
            "max_workers": self._max_workers,
            "active_batches": len(self._active_batches),
            "registry_info": registry_info
        }