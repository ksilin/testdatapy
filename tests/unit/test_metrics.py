"""Unit tests for metrics collection."""
from unittest.mock import ANY, patch

from testdatapy.metrics.collector import MetricsCollector, create_metrics_collector


class TestMetricsCollector:
    """Test the MetricsCollector class."""

    def test_initialization(self):
        """Test metrics collector initialization."""
        collector = MetricsCollector()
        assert collector._message_count == 0
        assert collector._error_count == 0
        assert collector._total_bytes == 0

    @patch("testdatapy.metrics.collector.start_http_server")
    def test_start_metrics_server(self, mock_start_server):
        """Test starting metrics server."""
        collector = MetricsCollector()
        collector.start_metrics_server(port=9090)
        mock_start_server.assert_called_once_with(9090, registry=ANY)

    def test_record_message_produced(self):
        """Test recording successful message production."""
        collector = MetricsCollector()
        
        collector.record_message_produced(
            topic="test-topic",
            format="json",
            generator="faker",
            size_bytes=100,
            duration=0.01
        )
        
        assert collector._message_count == 1
        assert collector._total_bytes == 100

    def test_record_message_failed(self):
        """Test recording failed message production."""
        collector = MetricsCollector()
        
        collector.record_message_failed(
            topic="test-topic",
            format="json",
            error_type="TimeoutError"
        )
        
        assert collector._error_count == 1

    def test_update_queue_size(self):
        """Test updating queue size."""
        collector = MetricsCollector()
        collector.update_queue_size(topic="test-topic", size=10)
        # Just ensure no errors

    def test_get_stats(self):
        """Test getting statistics."""
        collector = MetricsCollector()
        
        # Record some metrics
        collector.record_message_produced(
            topic="test",
            format="json",
            generator="faker",
            size_bytes=100,
            duration=0.01
        )
        collector.record_message_produced(
            topic="test",
            format="json",
            generator="faker",
            size_bytes=200,
            duration=0.02
        )
        collector.record_message_failed(
            topic="test",
            format="json",
            error_type="Error"
        )
        
        stats = collector.get_stats()
        assert stats["messages_produced"] == 2
        assert stats["messages_failed"] == 1
        assert stats["bytes_produced"] == 300
        assert stats["success_rate"] == 2/3

    def test_reset(self):
        """Test resetting metrics."""
        collector = MetricsCollector()
        
        # Record some data
        collector.record_message_produced(
            topic="test",
            format="json",
            generator="faker",
            size_bytes=100,
            duration=0.01
        )
        
        # Reset
        collector.reset()
        
        assert collector._message_count == 0
        assert collector._error_count == 0
        assert collector._total_bytes == 0

    def test_create_metrics_collector_enabled(self):
        """Test creating enabled metrics collector."""
        collector = create_metrics_collector(enabled=True)
        assert isinstance(collector, MetricsCollector)

    def test_create_metrics_collector_disabled(self):
        """Test creating disabled metrics collector."""
        collector = create_metrics_collector(enabled=False)
        # Should be dummy collector
        collector.record_message_produced(
            topic="test",
            format="json",
            generator="faker",
            size_bytes=100,
            duration=0.01
        )
        # Should not fail but do nothing
