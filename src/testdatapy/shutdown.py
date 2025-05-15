"""Graceful shutdown handling for TestDataPy."""
import signal
import threading
from collections.abc import Callable


class ShutdownHandler:
    """Handle graceful shutdown of the application."""
    
    def __init__(self):
        """Initialize shutdown handler."""
        self._shutdown_event = threading.Event()
        self._cleanup_functions: list[Callable] = []
        self._original_handlers = {}
        
        # Register signal handlers
        self._register_signal_handlers()
    
    def _register_signal_handlers(self):
        """Register signal handlers for graceful shutdown."""
        signals = [signal.SIGTERM, signal.SIGINT]
        
        for sig in signals:
            # Store original handler
            self._original_handlers[sig] = signal.signal(sig, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals.
        
        Args:
            signum: Signal number
            frame: Current stack frame
        """
        print(f"\nReceived signal {signum}, initiating graceful shutdown...")
        self.shutdown()
    
    def register_cleanup(self, func: Callable):
        """Register a cleanup function to be called on shutdown.
        
        Args:
            func: Cleanup function to register
        """
        self._cleanup_functions.append(func)
    
    def wait_for_shutdown(self):
        """Wait for shutdown signal."""
        self._shutdown_event.wait()
    
    def shutdown(self):
        """Initiate graceful shutdown."""
        if self._shutdown_event.is_set():
            return  # Already shutting down
        
        # Set shutdown event
        self._shutdown_event.set()
        
        # Run cleanup functions
        print("Running cleanup functions...")
        for func in reversed(self._cleanup_functions):
            try:
                func()
            except Exception as e:
                print(f"Error during cleanup: {e}")
        
        # Restore original signal handlers
        for sig, handler in self._original_handlers.items():
            signal.signal(sig, handler)
        
        print("Graceful shutdown complete")
    
    def is_shutting_down(self) -> bool:
        """Check if shutdown is in progress.
        
        Returns:
            True if shutting down
        """
        return self._shutdown_event.is_set()


class GracefulProducer:
    """Producer wrapper with graceful shutdown support."""
    
    def __init__(self, producer, shutdown_handler: ShutdownHandler):
        """Initialize graceful producer.
        
        Args:
            producer: Kafka producer instance
            shutdown_handler: Shutdown handler
        """
        self.producer = producer
        self.shutdown_handler = shutdown_handler
        self._messages_in_flight = 0
        self._lock = threading.Lock()
        
        # Register cleanup
        shutdown_handler.register_cleanup(self._cleanup)
    
    def produce(self, *args, **kwargs):
        """Produce message with shutdown check.
        
        Args:
            *args: Producer arguments
            **kwargs: Producer keyword arguments
        """
        if self.shutdown_handler.is_shutting_down():
            raise RuntimeError("Cannot produce during shutdown")
        
        with self._lock:
            self._messages_in_flight += 1
        
        # Add delivery callback wrapper
        original_callback = kwargs.get('on_delivery')
        kwargs['on_delivery'] = self._create_callback_wrapper(original_callback)
        
        return self.producer.produce(*args, **kwargs)
    
    def _create_callback_wrapper(self, original_callback):
        """Create callback wrapper that tracks in-flight messages.
        
        Args:
            original_callback: Original delivery callback
            
        Returns:
            Wrapped callback
        """
        def wrapper(err, msg):
            with self._lock:
                self._messages_in_flight -= 1
            
            if original_callback:
                original_callback(err, msg)
        
        return wrapper
    
    def _cleanup(self):
        """Cleanup producer on shutdown."""
        print(f"Flushing {self._messages_in_flight} messages...")
        self.producer.flush(timeout=30.0)
        
        if hasattr(self.producer, 'close'):
            self.producer.close()
            
            
    def flush(self, timeout: float = 10.0):
        """Proxy flush to the underlying producer"""
        if hasattr(self.producer, 'flush'):
            return self.producer.flush(timeout)
        return 0


def create_shutdown_handler() -> ShutdownHandler:
    """Create and return a shutdown handler.
    
    Returns:
        ShutdownHandler instance
    """
    return ShutdownHandler()
