"""Unit tests for health checks."""
import time
from unittest.mock import MagicMock, patch

from testdatapy.health import (
    HealthCheck,
    HealthMonitor,
    HealthStatus,
    create_health_monitor,
)


class TestHealthCheck:
    """Test the HealthCheck class."""

    def test_initialization(self):
        """Test health check initialization."""
        check = HealthCheck(
            name="test",
            status=HealthStatus.HEALTHY,
            message="All good",
            check_interval=30.0
        )
        assert check.name == "test"
        assert check.status == HealthStatus.HEALTHY
        assert check.message == "All good"
        assert check.check_interval == 30.0

    def test_is_healthy(self):
        """Test health status checking."""
        check = HealthCheck("test", HealthStatus.HEALTHY)
        assert check.is_healthy() is True
        
        check.status = HealthStatus.UNHEALTHY
        assert check.is_healthy() is False
        
        check.status = HealthStatus.DEGRADED
        assert check.is_healthy() is False

    def test_needs_check(self):
        """Test if check is due."""
        check = HealthCheck("test", HealthStatus.HEALTHY, check_interval=1.0)
        check.last_check = time.time()
        
        # Just checked
        assert check.needs_check() is False
        
        # Wait a bit
        time.sleep(1.1)
        assert check.needs_check() is True


class TestHealthMonitor:
    """Test the HealthMonitor class."""

    @patch("testdatapy.health.Flask")
    def test_initialization(self, mock_flask):
        """Test health monitor initialization."""
        monitor = HealthMonitor(port=8080)
        assert monitor.port == 8080
        assert monitor.checks == {}
        assert monitor._running is False

    @patch("testdatapy.health.Flask")
    def test_add_check(self, mock_flask):
        """Test adding health check."""
        monitor = HealthMonitor()
        check = HealthCheck("test", HealthStatus.HEALTHY)
        
        monitor.add_check("test", check)
        assert "test" in monitor.checks
        assert monitor.checks["test"] == check

    @patch("testdatapy.health.Flask")
    def test_update_check(self, mock_flask):
        """Test updating health check."""
        monitor = HealthMonitor()
        check = HealthCheck("test", HealthStatus.HEALTHY)
        monitor.add_check("test", check)
        
        monitor.update_check("test", HealthStatus.UNHEALTHY, "Connection failed")
        
        assert monitor.checks["test"].status == HealthStatus.UNHEALTHY
        assert monitor.checks["test"].message == "Connection failed"
        assert monitor.checks["test"].last_check > 0

    @patch("testdatapy.health.Flask")
    @patch("testdatapy.health.threading.Thread")
    def test_start_stop(self, mock_thread, mock_flask):
        """Test starting and stopping monitor."""
        monitor = HealthMonitor()
        
        # Start
        monitor.start()
        assert monitor._running is True
        mock_thread.assert_called_once()
        
        # Stop
        monitor.stop()
        assert monitor._running is False

    @patch("testdatapy.health.Flask")
    @patch("confluent_kafka.Producer")
    def test_check_kafka_health(self, mock_producer, mock_flask):
        """Test Kafka health check."""
        monitor = HealthMonitor()
        
        # Success case
        mock_producer_instance = MagicMock()
        mock_producer_instance.list_topics.return_value = MagicMock(topics={"test": None})
        mock_producer.return_value = mock_producer_instance
        
        status = monitor.check_kafka_health("localhost:9092")
        assert status == HealthStatus.HEALTHY
        
        # Failure case
        mock_producer.side_effect = Exception("Connection failed")
        status = monitor.check_kafka_health("localhost:9092")
        assert status == HealthStatus.UNHEALTHY

    @patch("testdatapy.health.Flask")
    @patch("requests.get")
    def test_check_schema_registry_health(self, mock_get, mock_flask):
        """Test Schema Registry health check."""
        monitor = HealthMonitor()
        
        # Success case
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        status = monitor.check_schema_registry_health("http://localhost:8081")
        assert status == HealthStatus.HEALTHY
        
        # Failure case
        mock_get.side_effect = Exception("Connection failed")
        status = monitor.check_schema_registry_health("http://localhost:8081")
        assert status == HealthStatus.UNHEALTHY

    def test_create_health_monitor_enabled(self):
        """Test creating enabled health monitor."""
        with patch("testdatapy.health.HealthMonitor") as mock_monitor_class:
            create_health_monitor(enabled=True, port=8080)
            mock_monitor_class.assert_called_once_with(port=8080)

    def test_create_health_monitor_disabled(self):
        """Test creating disabled health monitor."""
        monitor = create_health_monitor(enabled=False)
        assert monitor is None
