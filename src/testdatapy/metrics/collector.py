"""Metrics collection and reporting for TestDataPy."""
import time
from dataclasses import dataclass, field

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    Summary,
    start_http_server,
)


@dataclass
class MetricsCollector:
    """Collects and exposes metrics for monitoring."""

    # Registry for metrics
    registry: CollectorRegistry = field(default_factory=CollectorRegistry, init=False)
    
    # Prometheus metrics
    messages_produced: Counter = field(init=False)
    messages_failed: Counter = field(init=False)
    bytes_produced: Counter = field(init=False)
    produce_duration: Histogram = field(init=False)
    generation_rate: Gauge = field(init=False)
    producer_queue_size: Gauge = field(init=False)
    
    # Internal metrics
    _start_time: float = field(default_factory=time.time, init=False)
    _message_count: int = field(default=0, init=False)
    _error_count: int = field(default=0, init=False)
    _total_bytes: int = field(default=0, init=False)

    def __post_init__(self):
        """Initialize Prometheus metrics."""
        # Counters
        self.messages_produced = Counter(
            'testdatapy_messages_produced_total',
            'Total number of messages produced',
            ['topic', 'format', 'generator'],
            registry=self.registry
        )
        
        self.messages_failed = Counter(
            'testdatapy_messages_failed_total',
            'Total number of failed messages',
            ['topic', 'format', 'error_type'],
            registry=self.registry
        )
        
        self.bytes_produced = Counter(
            'testdatapy_bytes_produced_total',
            'Total bytes produced',
            ['topic', 'format'],
            registry=self.registry
        )
        
        # Histogram for latency
        self.produce_duration = Histogram(
            'testdatapy_produce_duration_seconds',
            'Message production duration',
            ['topic', 'format'],
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
            registry=self.registry
        )
        
        # Gauges
        self.generation_rate = Gauge(
            'testdatapy_generation_rate',
            'Current message generation rate per second',
            ['generator'],
            registry=self.registry
        )
        
        self.producer_queue_size = Gauge(
            'testdatapy_producer_queue_size',
            'Current producer queue size',
            ['topic'],
            registry=self.registry
        )
        
        # Summary for overall performance
        self.performance_summary = Summary(
            'testdatapy_performance',
            'Overall performance metrics',
            ['operation'],
            registry=self.registry
        )

    def start_metrics_server(self, port: int = 9090):
        """Start Prometheus metrics HTTP server.
        
        Args:
            port: Port to expose metrics on
        """
        start_http_server(port, registry=self.registry)

    def record_message_produced(
        self,
        topic: str,
        format: str,
        generator: str,
        size_bytes: int,
        duration: float
    ):
        """Record successful message production.
        
        Args:
            topic: Kafka topic
            format: Message format (json/avro)
            generator: Generator type (faker/csv)
            size_bytes: Message size in bytes
            duration: Production duration in seconds
        """
        self.messages_produced.labels(
            topic=topic,
            format=format,
            generator=generator
        ).inc()
        
        self.bytes_produced.labels(
            topic=topic,
            format=format
        ).inc(size_bytes)
        
        self.produce_duration.labels(
            topic=topic,
            format=format
        ).observe(duration)
        
        self._message_count += 1
        self._total_bytes += size_bytes
        
        # Update generation rate
        elapsed = time.time() - self._start_time
        if elapsed > 0:
            rate = self._message_count / elapsed
            self.generation_rate.labels(generator=generator).set(rate)

    def record_message_failed(
        self,
        topic: str,
        format: str,
        error_type: str
    ):
        """Record failed message production.
        
        Args:
            topic: Kafka topic
            format: Message format
            error_type: Type of error
        """
        self.messages_failed.labels(
            topic=topic,
            format=format,
            error_type=error_type
        ).inc()
        
        self._error_count += 1

    def update_queue_size(self, topic: str, size: int):
        """Update producer queue size.
        
        Args:
            topic: Kafka topic
            size: Current queue size
        """
        self.producer_queue_size.labels(topic=topic).set(size)

    def get_stats(self) -> dict[str, any]:
        """Get current statistics.
        
        Returns:
            Dictionary with current stats
        """
        elapsed = time.time() - self._start_time
        rate = self._message_count / elapsed if elapsed > 0 else 0
        
        return {
            "messages_produced": self._message_count,
            "messages_failed": self._error_count,
            "bytes_produced": self._total_bytes,
            "duration_seconds": elapsed,
            "rate_per_second": rate,
            "success_rate": (
                self._message_count / (self._message_count + self._error_count)
                if (self._message_count + self._error_count) > 0
                else 0
            )
        }

    def reset(self):
        """Reset internal counters."""
        self._start_time = time.time()
        self._message_count = 0
        self._error_count = 0
        self._total_bytes = 0


class DummyMetricsCollector(MetricsCollector):
    """Dummy metrics collector that does nothing (for when metrics are disabled)."""
    
    def __post_init__(self):
        """Skip Prometheus initialization."""
        pass
    
    def start_metrics_server(self, port: int = 9090):
        """No-op."""
        pass
    
    def record_message_produced(self, *args, **kwargs):
        """No-op."""
        pass
    
    def record_message_failed(self, *args, **kwargs):
        """No-op."""
        pass
    
    def update_queue_size(self, *args, **kwargs):
        """No-op."""
        pass


def create_metrics_collector(enabled: bool = True) -> MetricsCollector:
    """Create appropriate metrics collector based on configuration.
    
    Args:
        enabled: Whether metrics collection is enabled
        
    Returns:
        MetricsCollector instance
    """
    if enabled:
        return MetricsCollector()
    else:
        return DummyMetricsCollector()
