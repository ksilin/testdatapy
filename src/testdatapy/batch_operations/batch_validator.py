"""Batch validation operations for schemas and configurations.

This module provides comprehensive batch validation capabilities including
schema validation, configuration validation, and dependency checking.
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Union, Set
from dataclasses import dataclass, asdict
from enum import Enum

from ..logging_config import get_schema_logger
from ..performance.performance_monitor import get_performance_monitor, monitored
from ..validators.protobuf_validator import ProtobufConfigValidator
from ..schema.compiler import ProtobufCompiler
from .batch_compiler import BatchStatus, BatchItem


class ValidationLevel(Enum):
    """Validation levels."""
    BASIC = "basic"          # Syntax and basic structure
    COMPREHENSIVE = "comprehensive"  # Full validation including dependencies
    STRICT = "strict"        # Strict validation with warnings as errors


class ValidationType(Enum):
    """Types of validation operations."""
    SCHEMA_SYNTAX = "schema_syntax"
    SCHEMA_COMPILATION = "schema_compilation"
    CONFIGURATION = "configuration"
    DEPENDENCIES = "dependencies"
    COMPATIBILITY = "compatibility"
    NAMING_CONVENTIONS = "naming_conventions"


@dataclass
class ValidationIssue:
    """Individual validation issue."""
    severity: str  # error, warning, info
    type: str
    message: str
    file_path: Optional[str] = None
    line_number: Optional[int] = None
    column_number: Optional[int] = None
    suggestion: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class ValidationItem(BatchItem):
    """Validation item extending BatchItem."""
    validation_type: ValidationType = None
    validation_level: ValidationLevel = None
    issues: List[ValidationIssue] = None
    dependencies: List[str] = None
    
    def __post_init__(self):
        super().__post_init__()
        if self.issues is None:
            self.issues = []
        if self.dependencies is None:
            self.dependencies = []
    
    @property
    def has_errors(self) -> bool:
        """Check if item has validation errors."""
        return any(issue.severity == "error" for issue in self.issues)
    
    @property
    def has_warnings(self) -> bool:
        """Check if item has validation warnings."""
        return any(issue.severity == "warning" for issue in self.issues)
    
    def get_error_count(self) -> int:
        """Get number of errors."""
        return len([issue for issue in self.issues if issue.severity == "error"])
    
    def get_warning_count(self) -> int:
        """Get number of warnings."""
        return len([issue for issue in self.issues if issue.severity == "warning"])
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = super().to_dict()
        data['validation_type'] = self.validation_type.value
        data['validation_level'] = self.validation_level.value
        data['issues'] = [issue.to_dict() for issue in self.issues]
        data['dependencies'] = self.dependencies
        data['has_errors'] = self.has_errors
        data['has_warnings'] = self.has_warnings
        data['error_count'] = self.get_error_count()
        data['warning_count'] = self.get_warning_count()
        return data


@dataclass
class ValidationResult:
    """Result of batch validation operations."""
    batch_id: str
    operation_type: str
    validation_level: ValidationLevel
    total_items: int
    successful_items: int
    failed_items: int
    cancelled_items: int
    total_errors: int
    total_warnings: int
    total_duration: float
    items: List[ValidationItem]
    metadata: Dict[str, Any]
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.total_items == 0:
            return 0.0
        return self.successful_items / self.total_items
    
    @property
    def overall_status(self) -> str:
        """Get overall validation status."""
        if self.total_errors > 0:
            return "failed"
        elif self.total_warnings > 0:
            return "warning"
        else:
            return "passed"
    
    def get_successful_items(self) -> List[ValidationItem]:
        """Get successfully validated items."""
        return [item for item in self.items if item.status == BatchStatus.COMPLETED and not item.has_errors]
    
    def get_failed_items(self) -> List[ValidationItem]:
        """Get failed validation items."""
        return [item for item in self.items if item.status == BatchStatus.FAILED or item.has_errors]
    
    def get_items_with_warnings(self) -> List[ValidationItem]:
        """Get items with warnings only."""
        return [item for item in self.items if item.has_warnings and not item.has_errors]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "batch_id": self.batch_id,
            "operation_type": self.operation_type,
            "validation_level": self.validation_level.value,
            "total_items": self.total_items,
            "successful_items": self.successful_items,
            "failed_items": self.failed_items,
            "cancelled_items": self.cancelled_items,
            "total_errors": self.total_errors,
            "total_warnings": self.total_warnings,
            "total_duration": self.total_duration,
            "success_rate": self.success_rate,
            "overall_status": self.overall_status,
            "items": [item.to_dict() for item in self.items],
            "metadata": self.metadata
        }


class BatchValidator:
    """Batch validation operations manager."""
    
    def __init__(self, max_workers: int = 4):
        """
        Initialize batch validator.
        
        Args:
            max_workers: Maximum number of worker threads
        """
        self._logger = get_schema_logger(__name__)
        self._monitor = get_performance_monitor()
        self._max_workers = max_workers
        
        # Initialize validators
        self._config_validator = ProtobufConfigValidator()
        self._compiler = ProtobufCompiler()
        
        # Active batches tracking
        self._active_batches: Dict[str, ValidationResult] = {}
        
        # Validation rules
        self._naming_patterns = {
            "proto_file": r"^[a-z][a-z0-9_]*\.proto$",
            "message_name": r"^[A-Z][A-Za-z0-9]*$",
            "field_name": r"^[a-z][a-z0-9_]*$",
            "service_name": r"^[A-Z][A-Za-z0-9]*Service$"
        }
        
        self._logger.info("BatchValidator initialized", max_workers=max_workers)
    
    @monitored("batch_validate")
    def validate_schemas_batch(self, schema_files: List[str],
                              validation_level: ValidationLevel = ValidationLevel.COMPREHENSIVE,
                              include_paths: Optional[List[str]] = None,
                              check_dependencies: bool = True,
                              check_naming: bool = True,
                              parallel: bool = True,
                              progress_callback: Optional[callable] = None) -> ValidationResult:
        """
        Validate multiple schemas in batch.
        
        Args:
            schema_files: List of schema file paths
            validation_level: Level of validation to perform
            include_paths: Additional include paths
            check_dependencies: Whether to check dependencies
            check_naming: Whether to check naming conventions
            parallel: Whether to use parallel processing
            progress_callback: Optional callback for progress updates
            
        Returns:
            Batch validation result
        """
        batch_id = f"validate_{int(time.time() * 1000)}"
        start_time = time.perf_counter()
        
        # Discover dependencies if enabled
        dependency_map = {}
        if check_dependencies:
            dependency_map = self._discover_dependencies(schema_files, include_paths)
        
        # Create validation items
        items = []
        for i, schema_file in enumerate(schema_files):
            item = ValidationItem(
                id=f"{batch_id}_{i}",
                schema_path=schema_file,
                schema_type="protobuf",
                status=BatchStatus.PENDING,
                validation_type=ValidationType.SCHEMA_COMPILATION,
                validation_level=validation_level,
                dependencies=dependency_map.get(schema_file, []),
                metadata={
                    "include_paths": include_paths or [],
                    "check_dependencies": check_dependencies,
                    "check_naming": check_naming
                }
            )
            items.append(item)
        
        # Initialize batch result
        batch_result = ValidationResult(
            batch_id=batch_id,
            operation_type="validate_schemas",
            validation_level=validation_level,
            total_items=len(items),
            successful_items=0,
            failed_items=0,
            cancelled_items=0,
            total_errors=0,
            total_warnings=0,
            total_duration=0.0,
            items=items,
            metadata={
                "validation_level": validation_level.value,
                "include_paths": include_paths or [],
                "check_dependencies": check_dependencies,
                "check_naming": check_naming,
                "parallel": parallel,
                "max_workers": self._max_workers if parallel else 1
            }
        )
        
        self._active_batches[batch_id] = batch_result
        
        try:
            if parallel and len(items) > 1:
                self._validate_parallel(batch_result, include_paths, progress_callback)
            else:
                self._validate_sequential(batch_result, include_paths, progress_callback)
                
        except Exception as e:
            self._logger.error("Batch validation failed", batch_id=batch_id, error=str(e))
            # Mark remaining items as failed
            for item in items:
                if item.status == BatchStatus.PENDING:
                    item.status = BatchStatus.FAILED
                    item.error = f"Batch failed: {str(e)}"
        
        finally:
            # Calculate final statistics
            batch_result.total_duration = time.perf_counter() - start_time
            batch_result.successful_items = len([i for i in items if i.status == BatchStatus.COMPLETED and not i.has_errors])
            batch_result.failed_items = len([i for i in items if i.status == BatchStatus.FAILED or i.has_errors])
            batch_result.cancelled_items = len([i for i in items if i.status == BatchStatus.CANCELLED])
            batch_result.total_errors = sum(item.get_error_count() for item in items)
            batch_result.total_warnings = sum(item.get_warning_count() for item in items)
            
            # Remove from active batches
            self._active_batches.pop(batch_id, None)
        
        self._logger.info("Batch validation completed",
                         batch_id=batch_id,
                         total_items=batch_result.total_items,
                         successful=batch_result.successful_items,
                         failed=batch_result.failed_items,
                         errors=batch_result.total_errors,
                         warnings=batch_result.total_warnings,
                         duration=batch_result.total_duration)
        
        return batch_result
    
    @monitored("batch_validate_config")
    def validate_configurations_batch(self, config_files: List[str],
                                    validation_level: ValidationLevel = ValidationLevel.COMPREHENSIVE,
                                    parallel: bool = True,
                                    progress_callback: Optional[callable] = None) -> ValidationResult:
        """
        Validate multiple configuration files in batch.
        
        Args:
            config_files: List of configuration file paths
            validation_level: Level of validation to perform
            parallel: Whether to use parallel processing
            progress_callback: Optional callback for progress updates
            
        Returns:
            Batch validation result
        """
        batch_id = f"validate_config_{int(time.time() * 1000)}"
        start_time = time.perf_counter()
        
        # Create validation items
        items = []
        for i, config_file in enumerate(config_files):
            item = ValidationItem(
                id=f"{batch_id}_{i}",
                schema_path=config_file,
                schema_type="configuration",
                status=BatchStatus.PENDING,
                validation_type=ValidationType.CONFIGURATION,
                validation_level=validation_level,
                metadata={"config_type": self._detect_config_type(config_file)}
            )
            items.append(item)
        
        # Initialize batch result
        batch_result = ValidationResult(
            batch_id=batch_id,
            operation_type="validate_configurations",
            validation_level=validation_level,
            total_items=len(items),
            successful_items=0,
            failed_items=0,
            cancelled_items=0,
            total_errors=0,
            total_warnings=0,
            total_duration=0.0,
            items=items,
            metadata={
                "validation_level": validation_level.value,
                "parallel": parallel,
                "max_workers": self._max_workers if parallel else 1
            }
        )
        
        self._active_batches[batch_id] = batch_result
        
        try:
            if parallel and len(items) > 1:
                self._validate_config_parallel(batch_result, progress_callback)
            else:
                self._validate_config_sequential(batch_result, progress_callback)
                
        except Exception as e:
            self._logger.error("Batch config validation failed", batch_id=batch_id, error=str(e))
            # Mark remaining items as failed
            for item in items:
                if item.status == BatchStatus.PENDING:
                    item.status = BatchStatus.FAILED
                    item.error = f"Batch failed: {str(e)}"
        
        finally:
            # Calculate final statistics
            batch_result.total_duration = time.perf_counter() - start_time
            batch_result.successful_items = len([i for i in items if i.status == BatchStatus.COMPLETED and not i.has_errors])
            batch_result.failed_items = len([i for i in items if i.status == BatchStatus.FAILED or i.has_errors])
            batch_result.cancelled_items = len([i for i in items if i.status == BatchStatus.CANCELLED])
            batch_result.total_errors = sum(item.get_error_count() for item in items)
            batch_result.total_warnings = sum(item.get_warning_count() for item in items)
            
            # Remove from active batches
            self._active_batches.pop(batch_id, None)
        
        self._logger.info("Batch config validation completed",
                         batch_id=batch_id,
                         total_items=batch_result.total_items,
                         successful=batch_result.successful_items,
                         failed=batch_result.failed_items,
                         errors=batch_result.total_errors,
                         warnings=batch_result.total_warnings,
                         duration=batch_result.total_duration)
        
        return batch_result
    
    def _validate_sequential(self, batch_result: ValidationResult,
                           include_paths: Optional[List[str]],
                           progress_callback: Optional[callable]):
        """Validate schemas sequentially."""
        for i, item in enumerate(batch_result.items):
            try:
                item.status = BatchStatus.RUNNING
                if progress_callback:
                    progress_callback(batch_result.batch_id, i, len(batch_result.items), item)
                
                self._validate_single_schema(item, include_paths)
                
            except Exception as e:
                item.error = f"Unexpected error: {str(e)}"
                item.status = BatchStatus.FAILED
    
    def _validate_parallel(self, batch_result: ValidationResult,
                          include_paths: Optional[List[str]],
                          progress_callback: Optional[callable]):
        """Validate schemas in parallel using thread pool."""
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            # Submit all validation tasks
            future_to_item = {}
            
            for item in batch_result.items:
                future = executor.submit(self._validate_single_schema, item, include_paths)
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
                    progress_callback(batch_result.batch_id, completed, len(batch_result.items), item)
    
    def _validate_single_schema(self, item: ValidationItem, include_paths: Optional[List[str]]):
        """Validate a single schema item."""
        try:
            start_time = time.perf_counter()
            issues = []
            
            # Basic file existence check
            schema_path = Path(item.schema_path)
            if not schema_path.exists():
                issues.append(ValidationIssue(
                    severity="error",
                    type="file_not_found",
                    message=f"Schema file not found: {item.schema_path}",
                    file_path=item.schema_path
                ))
                item.issues = issues
                item.status = BatchStatus.FAILED
                return
            
            # Syntax validation
            syntax_issues = self._validate_syntax(item.schema_path)
            issues.extend(syntax_issues)
            
            # Compilation validation
            if item.validation_level in [ValidationLevel.COMPREHENSIVE, ValidationLevel.STRICT]:
                compilation_issues = self._validate_compilation(item.schema_path, include_paths)
                issues.extend(compilation_issues)
            
            # Naming convention validation
            if item.metadata.get("check_naming", False):
                naming_issues = self._validate_naming_conventions(item.schema_path)
                issues.extend(naming_issues)
            
            # Dependency validation
            if item.metadata.get("check_dependencies", False) and item.dependencies:
                dependency_issues = self._validate_dependencies(item.schema_path, item.dependencies)
                issues.extend(dependency_issues)
            
            item.duration = time.perf_counter() - start_time
            item.issues = issues
            
            # Determine status based on validation level
            has_errors = any(issue.severity == "error" for issue in issues)
            has_warnings = any(issue.severity == "warning" for issue in issues)
            
            if has_errors:
                item.status = BatchStatus.FAILED
            elif has_warnings and item.validation_level == ValidationLevel.STRICT:
                item.status = BatchStatus.FAILED
            else:
                item.status = BatchStatus.COMPLETED
            
        except Exception as e:
            item.duration = time.perf_counter() - start_time if 'start_time' in locals() else 0.0
            item.error = str(e)
            item.status = BatchStatus.FAILED
            raise
    
    def _validate_config_sequential(self, batch_result: ValidationResult,
                                  progress_callback: Optional[callable]):
        """Validate configurations sequentially."""
        for i, item in enumerate(batch_result.items):
            try:
                item.status = BatchStatus.RUNNING
                if progress_callback:
                    progress_callback(batch_result.batch_id, i, len(batch_result.items), item)
                
                self._validate_single_config(item)
                
            except Exception as e:
                item.error = f"Unexpected error: {str(e)}"
                item.status = BatchStatus.FAILED
    
    def _validate_config_parallel(self, batch_result: ValidationResult,
                                 progress_callback: Optional[callable]):
        """Validate configurations in parallel using thread pool."""
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            # Submit all validation tasks
            future_to_item = {}
            
            for item in batch_result.items:
                future = executor.submit(self._validate_single_config, item)
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
                    progress_callback(batch_result.batch_id, completed, len(batch_result.items), item)
    
    def _validate_single_config(self, item: ValidationItem):
        """Validate a single configuration item."""
        try:
            start_time = time.perf_counter()
            
            # Use existing config validator
            validation_result = self._config_validator.validate_config_file(item.schema_path)
            
            # Convert validation result to issues
            issues = []
            for issue in validation_result.get("issues", []):
                issues.append(ValidationIssue(
                    severity=issue.get("severity", "error"),
                    type=issue.get("type", "unknown"),
                    message=issue.get("message", ""),
                    file_path=item.schema_path,
                    line_number=issue.get("line_number"),
                    suggestion=issue.get("suggestion")
                ))
            
            item.duration = time.perf_counter() - start_time
            item.issues = issues
            
            # Determine status
            has_errors = any(issue.severity == "error" for issue in issues)
            has_warnings = any(issue.severity == "warning" for issue in issues)
            
            if has_errors:
                item.status = BatchStatus.FAILED
            elif has_warnings and item.validation_level == ValidationLevel.STRICT:
                item.status = BatchStatus.FAILED
            else:
                item.status = BatchStatus.COMPLETED
            
        except Exception as e:
            item.duration = time.perf_counter() - start_time if 'start_time' in locals() else 0.0
            item.error = str(e)
            item.status = BatchStatus.FAILED
            raise
    
    def _validate_syntax(self, schema_path: str) -> List[ValidationIssue]:
        """Basic syntax validation."""
        issues = []
        
        try:
            with open(schema_path, 'r') as f:
                content = f.read()
            
            # Basic protobuf syntax checks
            if not content.strip():
                issues.append(ValidationIssue(
                    severity="error",
                    type="empty_file",
                    message="Schema file is empty",
                    file_path=schema_path
                ))
            
            # Check for required syntax keyword
            if "syntax" not in content:
                issues.append(ValidationIssue(
                    severity="warning",
                    type="missing_syntax",
                    message="Missing syntax declaration",
                    file_path=schema_path,
                    suggestion="Add 'syntax = \"proto3\";' at the top of the file"
                ))
            
        except Exception as e:
            issues.append(ValidationIssue(
                severity="error",
                type="file_read_error",
                message=f"Could not read schema file: {str(e)}",
                file_path=schema_path
            ))
        
        return issues
    
    def _validate_compilation(self, schema_path: str, include_paths: Optional[List[str]]) -> List[ValidationIssue]:
        """Compilation validation."""
        issues = []
        
        try:
            # Attempt to compile the schema
            self._compiler.compile_schema(
                proto_file=schema_path,
                include_paths=include_paths
            )
            
        except Exception as e:
            issues.append(ValidationIssue(
                severity="error",
                type="compilation_error",
                message=f"Schema compilation failed: {str(e)}",
                file_path=schema_path
            ))
        
        return issues
    
    def _validate_naming_conventions(self, schema_path: str) -> List[ValidationIssue]:
        """Naming convention validation."""
        issues = []
        
        # Check file name convention
        file_name = Path(schema_path).name
        import re
        
        if not re.match(self._naming_patterns["proto_file"], file_name):
            issues.append(ValidationIssue(
                severity="warning",
                type="naming_convention",
                message=f"File name '{file_name}' doesn't follow naming convention",
                file_path=schema_path,
                suggestion="Use lowercase with underscores (e.g., 'my_schema.proto')"
            ))
        
        # Additional naming checks could be added here for message names, field names, etc.
        
        return issues
    
    def _validate_dependencies(self, schema_path: str, dependencies: List[str]) -> List[ValidationIssue]:
        """Dependency validation."""
        issues = []
        
        for dep in dependencies:
            dep_path = Path(dep)
            if not dep_path.exists():
                issues.append(ValidationIssue(
                    severity="error",
                    type="missing_dependency",
                    message=f"Dependency not found: {dep}",
                    file_path=schema_path
                ))
        
        return issues
    
    def _discover_dependencies(self, schema_files: List[str], 
                             include_paths: Optional[List[str]]) -> Dict[str, List[str]]:
        """Discover dependencies between schema files."""
        dependency_map = {}
        
        # Simple dependency discovery (could be enhanced with proper proto parsing)
        for schema_file in schema_files:
            dependencies = []
            try:
                with open(schema_file, 'r') as f:
                    content = f.read()
                
                # Look for import statements
                import re
                imports = re.findall(r'import\s+"([^"]+)";', content)
                
                for import_path in imports:
                    # Try to resolve the import
                    resolved_path = self._resolve_import_path(import_path, schema_file, include_paths)
                    if resolved_path:
                        dependencies.append(resolved_path)
                
                dependency_map[schema_file] = dependencies
                
            except Exception as e:
                self._logger.warning("Failed to discover dependencies",
                                   schema_file=schema_file, error=str(e))
                dependency_map[schema_file] = []
        
        return dependency_map
    
    def _resolve_import_path(self, import_path: str, schema_file: str,
                           include_paths: Optional[List[str]]) -> Optional[str]:
        """Resolve import path to actual file."""
        # Try relative to schema file
        schema_dir = Path(schema_file).parent
        candidate = schema_dir / import_path
        if candidate.exists():
            return str(candidate)
        
        # Try include paths
        if include_paths:
            for include_path in include_paths:
                candidate = Path(include_path) / import_path
                if candidate.exists():
                    return str(candidate)
        
        return None
    
    def _detect_config_type(self, config_file: str) -> str:
        """Detect configuration file type."""
        suffix = Path(config_file).suffix.lower()
        
        if suffix in ['.json']:
            return 'json'
        elif suffix in ['.yaml', '.yml']:
            return 'yaml'
        elif suffix in ['.toml']:
            return 'toml'
        elif suffix in ['.properties']:
            return 'properties'
        else:
            return 'unknown'
    
    def get_active_batches(self) -> Dict[str, ValidationResult]:
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
        
        self._logger.info("Batch validation operation cancelled", batch_id=batch_id)
        return True
    
    def get_validation_stats(self) -> Dict[str, Any]:
        """Get validation statistics."""
        return {
            "max_workers": self._max_workers,
            "active_batches": len(self._active_batches),
            "naming_patterns": self._naming_patterns
        }