"""Performance monitoring and profiling utilities.

This module provides comprehensive performance monitoring capabilities
to track and analyze performance of TestDataPy operations.
"""

import time
import psutil
import threading
from collections import defaultdict, deque
from contextlib import contextmanager
from functools import wraps
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass
from enum import Enum

from ..logging_config import get_schema_logger


class MetricType(Enum):
    """Types of performance metrics."""
    DURATION = "duration"
    MEMORY = "memory"  
    THROUGHPUT = "throughput"
    ERROR_RATE = "error_rate"
    CPU = "cpu"


@dataclass
class PerformanceMetric:
    """Individual performance metric."""
    name: str
    value: float
    timestamp: float
    metric_type: MetricType
    tags: Dict[str, str]


class PerformanceMonitor:
    """Comprehensive performance monitoring system."""
    
    def __init__(self, max_metrics: int = 10000, sampling_interval: float = 1.0):
        """
        Initialize the performance monitor.
        
        Args:
            max_metrics: Maximum number of metrics to retain
            sampling_interval: Interval for background sampling in seconds
        """
        self._logger = get_schema_logger(__name__)
        self._max_metrics = max_metrics
        self._sampling_interval = sampling_interval
        
        # Metrics storage
        self._metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=max_metrics))
        self._metric_lock = threading.RLock()
        
        # Operation tracking
        self._active_operations: Dict[str, Dict[str, Any]] = {}
        self._operation_stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "count": 0,
            "total_duration": 0.0,
            "min_duration": float('inf'),
            "max_duration": 0.0,
            "errors": 0
        })
        
        # Background monitoring
        self._monitoring_active = False
        self._monitor_thread: Optional[threading.Thread] = None
        
        # System baseline
        self._process = psutil.Process()
        self._baseline_memory = self._process.memory_info().rss
        
        self._logger.info("PerformanceMonitor initialized",
                         max_metrics=max_metrics,
                         sampling_interval=sampling_interval)
    
    def start_monitoring(self) -> None:
        """Start background performance monitoring."""
        if not self._monitoring_active:
            self._monitoring_active = True
            self._monitor_thread = threading.Thread(
                target=self._background_monitor,
                daemon=True
            )
            self._monitor_thread.start()
            self._logger.info("Background monitoring started")
    
    def stop_monitoring(self) -> None:
        """Stop background performance monitoring."""
        if self._monitoring_active:
            self._monitoring_active = False
            if self._monitor_thread:
                self._monitor_thread.join(timeout=2.0)
            self._logger.info("Background monitoring stopped")
    
    def _background_monitor(self) -> None:
        """Background monitoring loop."""
        while self._monitoring_active:
            try:
                # Sample system metrics
                self._sample_system_metrics()
                time.sleep(self._sampling_interval)
            except Exception as e:
                self._logger.warning("Background monitoring error", error=str(e))
    
    def _sample_system_metrics(self) -> None:
        """Sample current system metrics."""
        current_time = time.time()
        
        # Memory usage
        memory_info = self._process.memory_info()
        memory_usage = (memory_info.rss - self._baseline_memory) / (1024 * 1024)  # MB
        self.record_metric("system.memory_usage", memory_usage, MetricType.MEMORY)
        
        # CPU usage
        try:
            cpu_percent = self._process.cpu_percent()
            self.record_metric("system.cpu_usage", cpu_percent, MetricType.CPU)
        except Exception:
            pass  # CPU percent might not be available immediately
        
        # Thread count
        thread_count = threading.active_count()
        self.record_metric("system.thread_count", thread_count, MetricType.THROUGHPUT)
    
    def record_metric(self, name: str, value: float, metric_type: MetricType, 
                     tags: Optional[Dict[str, str]] = None) -> None:
        """
        Record a performance metric.
        
        Args:
            name: Metric name
            value: Metric value
            metric_type: Type of metric
            tags: Optional tags for metric
        """
        metric = PerformanceMetric(
            name=name,
            value=value,
            timestamp=time.time(),
            metric_type=metric_type,
            tags=tags or {}
        )
        
        with self._metric_lock:
            self._metrics[name].append(metric)
    
    @contextmanager
    def track_operation(self, operation_name: str, tags: Optional[Dict[str, str]] = None):
        """
        Context manager to track operation performance.
        
        Args:
            operation_name: Name of the operation
            tags: Optional tags for the operation
        """
        operation_id = f"{operation_name}_{time.time()}_{threading.get_ident()}"
        start_time = time.perf_counter()
        start_memory = self._process.memory_info().rss
        
        operation_data = {
            "start_time": start_time,
            "start_memory": start_memory,
            "tags": tags or {}
        }
        
        with self._metric_lock:
            self._active_operations[operation_id] = operation_data
        
        try:
            yield operation_id
            
            # Record successful completion
            end_time = time.perf_counter()
            duration = end_time - start_time
            
            self._record_operation_completion(operation_name, duration, False, tags)
            
        except Exception as e:
            # Record error
            end_time = time.perf_counter()
            duration = end_time - start_time
            
            self._record_operation_completion(operation_name, duration, True, tags)
            raise
        
        finally:
            with self._metric_lock:
                self._active_operations.pop(operation_id, None)
    
    def _record_operation_completion(self, operation_name: str, duration: float, 
                                   is_error: bool, tags: Optional[Dict[str, str]]) -> None:
        """Record completion of an operation."""
        with self._metric_lock:
            stats = self._operation_stats[operation_name]
            stats["count"] += 1
            stats["total_duration"] += duration
            stats["min_duration"] = min(stats["min_duration"], duration)
            stats["max_duration"] = max(stats["max_duration"], duration)
            
            if is_error:
                stats["errors"] += 1
        
        # Record metrics
        self.record_metric(f"operation.{operation_name}.duration", duration, 
                         MetricType.DURATION, tags)
        
        if is_error:
            self.record_metric(f"operation.{operation_name}.error", 1, 
                             MetricType.ERROR_RATE, tags)
    
    def get_operation_stats(self, operation_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get operation statistics.
        
        Args:
            operation_name: Specific operation name, or None for all operations
            
        Returns:
            Operation statistics
        """
        with self._metric_lock:
            if operation_name:
                if operation_name in self._operation_stats:
                    stats = self._operation_stats[operation_name].copy()
                    if stats["count"] > 0:
                        stats["avg_duration"] = stats["total_duration"] / stats["count"]
                        stats["error_rate"] = stats["errors"] / stats["count"]
                    return {operation_name: stats}
                else:
                    return {}
            else:
                result = {}
                for name, stats in self._operation_stats.items():
                    stats_copy = stats.copy()
                    if stats_copy["count"] > 0:
                        stats_copy["avg_duration"] = stats_copy["total_duration"] / stats_copy["count"]
                        stats_copy["error_rate"] = stats_copy["errors"] / stats_copy["count"]
                    result[name] = stats_copy
                return result
    
    def get_metrics(self, metric_name: Optional[str] = None, 
                   limit: Optional[int] = None) -> Dict[str, List[PerformanceMetric]]:
        """
        Get recorded metrics.
        
        Args:
            metric_name: Specific metric name, or None for all metrics
            limit: Maximum number of metrics to return per metric name
            
        Returns:
            Dictionary of metrics
        """
        with self._metric_lock:
            if metric_name:
                metrics = list(self._metrics.get(metric_name, []))
                if limit:
                    metrics = metrics[-limit:]
                return {metric_name: metrics}
            else:
                result = {}
                for name, metric_deque in self._metrics.items():
                    metrics = list(metric_deque)
                    if limit:
                        metrics = metrics[-limit:]
                    result[name] = metrics
                return result
    
    def get_metric_summary(self, metric_name: str, 
                          time_window: Optional[float] = None) -> Dict[str, Any]:
        """
        Get summary statistics for a metric.
        
        Args:
            metric_name: Name of the metric
            time_window: Time window in seconds (None for all data)
            
        Returns:
            Metric summary statistics
        """
        with self._metric_lock:
            metrics = list(self._metrics.get(metric_name, []))
            
            if not metrics:
                return {"count": 0}
            
            # Filter by time window if specified
            if time_window:
                cutoff_time = time.time() - time_window
                metrics = [m for m in metrics if m.timestamp >= cutoff_time]
            
            if not metrics:
                return {"count": 0, "note": "No metrics in time window"}
            
            values = [m.value for m in metrics]
            
            return {
                "count": len(values),
                "min": min(values),
                "max": max(values),
                "avg": sum(values) / len(values),
                "latest": values[-1] if values else None,
                "time_span": metrics[-1].timestamp - metrics[0].timestamp if len(metrics) > 1 else 0
            }
    
    def get_performance_report(self) -> Dict[str, Any]:
        """Generate comprehensive performance report."""
        report = {
            "timestamp": time.time(),
            "monitoring_active": self._monitoring_active,
            "active_operations": len(self._active_operations),
            "operation_stats": self.get_operation_stats(),
            "system_metrics": {},
            "top_operations": {}
        }
        
        # System metrics summary
        system_metrics = ["system.memory_usage", "system.cpu_usage", "system.thread_count"]
        for metric_name in system_metrics:
            summary = self.get_metric_summary(metric_name, time_window=300)  # Last 5 minutes
            if summary.get("count", 0) > 0:
                report["system_metrics"][metric_name] = summary
        
        # Top operations by total duration
        ops_by_duration = sorted(
            self._operation_stats.items(),
            key=lambda x: x[1]["total_duration"],
            reverse=True
        )
        
        report["top_operations"]["by_total_duration"] = dict(ops_by_duration[:10])
        
        # Top operations by average duration
        ops_by_avg_duration = [
            (name, stats) for name, stats in self._operation_stats.items()
            if stats["count"] > 0
        ]
        ops_by_avg_duration.sort(
            key=lambda x: x[1]["total_duration"] / x[1]["count"],
            reverse=True
        )
        
        report["top_operations"]["by_avg_duration"] = dict(ops_by_avg_duration[:10])
        
        return report
    
    def clear_metrics(self) -> None:
        """Clear all recorded metrics."""
        with self._metric_lock:
            self._metrics.clear()
            self._operation_stats.clear()
            self._active_operations.clear()
            self._logger.info("Performance metrics cleared")
    
    def profile_function(self, func: Callable, *args, **kwargs) -> Dict[str, Any]:
        """
        Profile a function call.
        
        Args:
            func: Function to profile
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Profiling results
        """
        function_name = getattr(func, '__name__', 'unknown')
        
        with self.track_operation(f"profile.{function_name}") as operation_id:
            start_memory = self._process.memory_info().rss
            start_time = time.perf_counter()
            
            try:
                result = func(*args, **kwargs)
                success = True
                error = None
            except Exception as e:
                result = None
                success = False
                error = str(e)
                raise
            finally:
                end_time = time.perf_counter()
                end_memory = self._process.memory_info().rss
                
                profiling_result = {
                    "function_name": function_name,
                    "success": success,
                    "duration": end_time - start_time,
                    "memory_delta": (end_memory - start_memory) / (1024 * 1024),  # MB
                    "args_count": len(args),
                    "kwargs_count": len(kwargs),
                    "error": error
                }
                
                # Record profiling metrics
                self.record_metric(f"profile.{function_name}.duration", 
                                 profiling_result["duration"], MetricType.DURATION)
                self.record_metric(f"profile.{function_name}.memory_delta", 
                                 profiling_result["memory_delta"], MetricType.MEMORY)
                
                return profiling_result


# Global performance monitor instance
_global_performance_monitor: Optional[PerformanceMonitor] = None


def get_performance_monitor() -> PerformanceMonitor:
    """Get the global performance monitor instance."""
    global _global_performance_monitor
    if _global_performance_monitor is None:
        _global_performance_monitor = PerformanceMonitor()
    return _global_performance_monitor


def monitored(operation_name: Optional[str] = None):
    """
    Decorator to monitor function performance.
    
    Args:
        operation_name: Custom operation name (defaults to function name)
        
    Returns:
        Decorated function with performance monitoring
    """
    def decorator(func: Callable) -> Callable:
        op_name = operation_name or func.__name__
        monitor = get_performance_monitor()
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            with monitor.track_operation(op_name):
                return func(*args, **kwargs)
        
        return wrapper
    
    return decorator