"""Health check implementation for TestDataPy."""
import threading
import time
from dataclasses import dataclass
from enum import Enum

from flask import Flask, jsonify


class HealthStatus(Enum):
    """Health check status."""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED = "degraded"


@dataclass
class HealthCheck:
    """Health check for a component."""
    name: str
    status: HealthStatus
    message: str = ""
    last_check: float = 0
    check_interval: float = 30.0
    
    def is_healthy(self) -> bool:
        """Check if component is healthy."""
        return self.status == HealthStatus.HEALTHY
    
    def needs_check(self) -> bool:
        """Check if health check is due."""
        return time.time() - self.last_check > self.check_interval


class HealthMonitor:
    """Monitor health of TestDataPy components."""
    
    def __init__(self, port: int = 8080):
        """Initialize health monitor.
        
        Args:
            port: Port to expose health endpoints
        """
        self.port = port
        self.app = Flask(__name__)
        self.checks: dict[str, HealthCheck] = {}
        self._running = False
        self._thread = None
        
        # Setup Flask routes
        self._setup_routes()
    
    def _setup_routes(self):
        """Setup Flask routes for health checks."""
        @self.app.route('/health')
        def health():
            """Overall health endpoint."""
            all_healthy = all(check.is_healthy() for check in self.checks.values())
            status_code = 200 if all_healthy else 503
            
            return jsonify({
                "status": "healthy" if all_healthy else "unhealthy",
                "checks": {
                    name: {
                        "status": check.status.value,
                        "message": check.message,
                        "last_check": check.last_check
                    }
                    for name, check in self.checks.items()
                }
            }), status_code
        
        @self.app.route('/health/live')
        def liveness():
            """Kubernetes liveness probe endpoint."""
            return jsonify({"status": "ok"}), 200
        
        @self.app.route('/health/ready')
        def readiness():
            """Kubernetes readiness probe endpoint."""
            # Check if key components are ready
            kafka_ready = self.checks.get(
                "kafka", HealthCheck("kafka", HealthStatus.UNHEALTHY)
            ).is_healthy()
            
            if kafka_ready:
                return jsonify({"status": "ready"}), 200
            else:
                return jsonify({"status": "not ready"}), 503
    
    def start(self):
        """Start health monitoring."""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._run_server, daemon=True)
        self._thread.start()
    
    def stop(self):
        """Stop health monitoring."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
    
    def _run_server(self):
        """Run Flask server in background."""
        self.app.run(host='0.0.0.0', port=self.port, debug=False)
    
    def add_check(self, name: str, check: HealthCheck):
        """Add a health check.
        
        Args:
            name: Name of the check
            check: HealthCheck instance
        """
        self.checks[name] = check
    
    def update_check(self, name: str, status: HealthStatus, message: str = ""):
        """Update a health check status.
        
        Args:
            name: Name of the check
            status: New status
            message: Optional status message
        """
        if name in self.checks:
            self.checks[name].status = status
            self.checks[name].message = message
            self.checks[name].last_check = time.time()
    
    def check_kafka_health(self, bootstrap_servers: str) -> HealthStatus:
        """Check Kafka connectivity.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            
        Returns:
            Health status
        """
        try:
            from confluent_kafka import Producer
            
            # Try to create a producer
            producer = Producer({
                'bootstrap.servers': bootstrap_servers,
                'socket.timeout.ms': 5000,
                'api.version.request.timeout.ms': 5000
            })
            
            # Get metadata
            metadata = producer.list_topics(timeout=5)
            
            if metadata.topics:
                return HealthStatus.HEALTHY
            else:
                return HealthStatus.DEGRADED
                
        except Exception:
            return HealthStatus.UNHEALTHY
    
    def check_schema_registry_health(self, url: str) -> HealthStatus:
        """Check Schema Registry connectivity.
        
        Args:
            url: Schema Registry URL
            
        Returns:
            Health status
        """
        try:
            import requests
            
            response = requests.get(f"{url}/subjects", timeout=5)
            
            if response.status_code == 200:
                return HealthStatus.HEALTHY
            else:
                return HealthStatus.DEGRADED
                
        except Exception:
            return HealthStatus.UNHEALTHY


def create_health_monitor(enabled: bool = True, port: int = 8080) -> HealthMonitor | None:
    """Create health monitor if enabled.
    
    Args:
        enabled: Whether health monitoring is enabled
        port: Port to expose health endpoints
        
    Returns:
        HealthMonitor instance or None
    """
    if enabled:
        monitor = HealthMonitor(port=port)
        monitor.start()
        return monitor
    return None
