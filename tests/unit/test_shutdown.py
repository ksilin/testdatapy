"""Unit tests for shutdown handling."""
import signal
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from testdatapy.shutdown import GracefulProducer, ShutdownHandler


class TestShutdownHandler:
    """Test the ShutdownHandler class."""

    def test_initialization(self):
        """Test shutdown handler initialization."""
        handler = ShutdownHandler()
        assert handler._shutdown_event.is_set() is False
        assert handler._cleanup_functions == []
        assert handler._original_handlers != {}

    def test_register_cleanup(self):
        """Test registering cleanup functions."""
        handler = ShutdownHandler()
        
        def cleanup1():
            pass
        
        def cleanup2():
            pass
        
        handler.register_cleanup(cleanup1)
        handler.register_cleanup(cleanup2)
        
        assert len(handler._cleanup_functions) == 2
        assert cleanup1 in handler._cleanup_functions
        assert cleanup2 in handler._cleanup_functions

    def test_is_shutting_down(self):
        """Test checking shutdown status."""
        handler = ShutdownHandler()
        assert handler.is_shutting_down() is False
        
        handler.shutdown()
        assert handler.is_shutting_down() is True

    def test_wait_for_shutdown(self):
        """Test waiting for shutdown."""
        handler = ShutdownHandler()
        
        # Start a thread that waits
        def waiter():
            handler.wait_for_shutdown()
        
        thread = threading.Thread(target=waiter)
        thread.start()
        
        # Should be blocking
        time.sleep(0.1)
        assert thread.is_alive()
        
        # Trigger shutdown
        handler.shutdown()
        thread.join(timeout=1)
        assert not thread.is_alive()

    def test_shutdown_runs_cleanup(self):
        """Test shutdown runs cleanup functions."""
        handler = ShutdownHandler()
        
        cleanup_order = []
        
        def cleanup1():
            cleanup_order.append(1)
        
        def cleanup2():
            cleanup_order.append(2)
        
        handler.register_cleanup(cleanup1)
        handler.register_cleanup(cleanup2)
        
        handler.shutdown()
        
        # Cleanup should run in reverse order
        assert cleanup_order == [2, 1]

    def test_shutdown_handles_cleanup_errors(self):
        """Test shutdown handles errors in cleanup functions."""
        handler = ShutdownHandler()
        
        def failing_cleanup():
            raise Exception("Cleanup failed")
        
        def working_cleanup():
            pass
        
        handler.register_cleanup(failing_cleanup)
        handler.register_cleanup(working_cleanup)
        
        # Should not raise
        handler.shutdown()
        assert handler.is_shutting_down()

    @patch("signal.signal")
    def test_signal_handling(self, mock_signal):
        """Test signal handler registration."""
        handler = ShutdownHandler()
        
        # Should register handlers for SIGTERM and SIGINT
        assert mock_signal.call_count >= 2
        
        # Simulate signal
        handler._signal_handler(signal.SIGTERM, None)
        assert handler.is_shutting_down()


class TestGracefulProducer:
    """Test the GracefulProducer class."""

    def test_initialization(self):
        """Test graceful producer initialization."""
        mock_producer = MagicMock()
        handler = ShutdownHandler()
        
        graceful = GracefulProducer(mock_producer, handler)
        assert graceful.producer == mock_producer
        assert graceful.shutdown_handler == handler
        assert graceful._messages_in_flight == 0

    def test_produce_during_normal_operation(self):
        """Test producing during normal operation."""
        mock_producer = MagicMock()
        handler = ShutdownHandler()
        graceful = GracefulProducer(mock_producer, handler)
        
        graceful.produce(topic="test", key="key", value={"test": "data"})
        
        # Should call underlying producer
        mock_producer.produce.assert_called_once()
        assert graceful._messages_in_flight == 1

    def test_produce_during_shutdown(self):
        """Test producing during shutdown."""
        mock_producer = MagicMock()
        handler = ShutdownHandler()
        handler.shutdown()  # Set shutdown state
        
        graceful = GracefulProducer(mock_producer, handler)
        
        with pytest.raises(RuntimeError, match="Cannot produce during shutdown"):
            graceful.produce(topic="test", key="key", value={"test": "data"})

    def test_delivery_callback_wrapper(self):
        """Test delivery callback wrapper."""
        mock_producer = MagicMock()
        handler = ShutdownHandler()
        graceful = GracefulProducer(mock_producer, handler)
        
        original_callback = MagicMock()
        
        # Produce with callback
        graceful.produce(
            topic="test",
            key="key",
            value={"test": "data"},
            on_delivery=original_callback
        )
        
        # Get the wrapped callback
        call_args = mock_producer.produce.call_args
        wrapped_callback = call_args.kwargs["on_delivery"]
        
        # Simulate delivery
        wrapped_callback(None, MagicMock())
        
        # Should call original callback
        original_callback.assert_called_once()
        # Should decrement in-flight counter
        assert graceful._messages_in_flight == 0

    def test_cleanup(self):
        """Test cleanup on shutdown."""
        mock_producer = MagicMock()
        mock_producer.flush.return_value = 0
        
        handler = ShutdownHandler()
        graceful = GracefulProducer(mock_producer, handler)
        
        # Simulate some in-flight messages
        graceful._messages_in_flight = 5
        
        # Run cleanup
        graceful._cleanup()
        
        # Should flush producer
        mock_producer.flush.assert_called_once_with(timeout=30.0)
        
        # Should close if available
        if hasattr(mock_producer, "close"):
            mock_producer.close.assert_called_once()
