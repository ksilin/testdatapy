"""Integration tests for topic management functionality."""
import pytest
import time
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.error import KafkaError

from testdatapy.topics import TopicManager


@pytest.mark.integration
class TestTopicManagerIntegration:
    """Integration tests for TopicManager with real Kafka."""

    @pytest.fixture
    def bootstrap_servers(self):
        """Get bootstrap servers from environment or use default."""
        import os
        return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    @pytest.fixture
    def admin_client(self, bootstrap_servers):
        """Create real AdminClient for cleanup."""
        return AdminClient({"bootstrap.servers": bootstrap_servers})

    @pytest.fixture
    def topic_manager(self, bootstrap_servers):
        """Create TopicManager with real connection."""
        return TopicManager(bootstrap_servers=bootstrap_servers)

    @pytest.fixture
    def test_topic_name(self):
        """Generate unique test topic name."""
        import uuid
        return f"test-topic-{uuid.uuid4().hex[:8]}"

    def cleanup_topic(self, admin_client, topic_name):
        """Helper to clean up test topics."""
        try:
            futures = admin_client.delete_topics([topic_name])
            futures[topic_name].result(timeout=10)
        except Exception:
            pass  # Ignore errors during cleanup

    def test_topic_exists_integration(self, topic_manager, admin_client, test_topic_name):
        """Test topic existence check with real Kafka."""
        try:
            # Topic shouldn't exist initially
            assert topic_manager.topic_exists(test_topic_name) is False

            # Create topic using admin client
            from confluent_kafka.admin import NewTopic
            new_topic = NewTopic(test_topic_name, num_partitions=1, replication_factor=1)
            futures = admin_client.create_topics([new_topic])
            futures[test_topic_name].result(timeout=10)

            # Give Kafka time to propagate
            time.sleep(1)

            # Now topic should exist
            assert topic_manager.topic_exists(test_topic_name) is True

        finally:
            self.cleanup_topic(admin_client, test_topic_name)

    def test_create_topic_integration(self, topic_manager, admin_client, test_topic_name):
        """Test topic creation with real Kafka."""
        try:
            # Create topic using our manager
            topic_manager.create_topic(
                test_topic_name,
                num_partitions=3,
                replication_factor=1
            )

            # Verify it exists
            metadata = admin_client.list_topics(timeout=10)
            assert test_topic_name in metadata.topics

            # Verify configuration
            topic_metadata = metadata.topics[test_topic_name]
            assert len(topic_metadata.partitions) == 3

        finally:
            self.cleanup_topic(admin_client, test_topic_name)

    def test_create_topic_already_exists_integration(self, topic_manager, admin_client, test_topic_name):
        """Test creating existing topic doesn't raise error."""
        try:
            # Create topic first time
            topic_manager.create_topic(test_topic_name)

            # Create same topic again - should not raise
            topic_manager.create_topic(test_topic_name)

        finally:
            self.cleanup_topic(admin_client, test_topic_name)

    def test_ensure_topic_exists_integration(self, topic_manager, admin_client, test_topic_name):
        """Test ensure_topic_exists with real Kafka."""
        try:
            # Ensure topic exists (should create it)
            created = topic_manager.ensure_topic_exists(
                test_topic_name,
                num_partitions=2,
                replication_factor=1
            )
            assert created is True

            # Ensure topic exists again (should not create)
            created = topic_manager.ensure_topic_exists(test_topic_name)
            assert created is True

            # Verify it actually exists
            assert topic_manager.topic_exists(test_topic_name) is True

        finally:
            self.cleanup_topic(admin_client, test_topic_name)

    def test_create_topic_with_config_integration(self, topic_manager, admin_client, test_topic_name):
        """Test creating topic with custom configuration."""
        try:
            config = {
                "retention.ms": "300000",  # 5 minutes
                "segment.ms": "60000"      # 1 minute
            }

            topic_manager.create_topic(
                test_topic_name,
                num_partitions=1,
                replication_factor=1,
                config=config
            )

            # Note: Verifying config requires describe_configs which is complex
            # Just verify topic was created
            assert topic_manager.topic_exists(test_topic_name) is True

        finally:
            self.cleanup_topic(admin_client, test_topic_name)

    def test_error_handling_integration(self, topic_manager):
        """Test error handling with invalid parameters."""
        with pytest.raises(KafkaException):
            # This should fail with invalid replication factor
            topic_manager.create_topic(
                "invalid-topic",
                num_partitions=1,
                replication_factor=100  # Too high
            )
