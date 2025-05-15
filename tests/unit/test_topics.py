"""Tests for topic management functionality."""
import pytest
from unittest.mock import Mock, patch
from confluent_kafka.admin import AdminClient, KafkaException, NewTopic
from confluent_kafka.error import KafkaError

from testdatapy.topics import TopicManager


class TestTopicManager:
    """Test cases for TopicManager class."""

    @pytest.fixture
    def admin_client_mock(self):
        """Create a mock AdminClient."""
        return Mock(spec=AdminClient)

    @pytest.fixture
    def topic_manager(self, admin_client_mock):
        """Create TopicManager with mocked AdminClient."""
        with patch('testdatapy.topics.AdminClient', return_value=admin_client_mock):
            return TopicManager(bootstrap_servers="localhost:9092")

    def test_create_topic_manager(self):
        """Test TopicManager initialization."""
        with patch('testdatapy.topics.AdminClient') as admin_mock:
            manager = TopicManager(
                bootstrap_servers="localhost:9092",
                config={"security.protocol": "PLAINTEXT"}
            )
            admin_mock.assert_called_once_with({
                "bootstrap.servers": "localhost:9092",
                "security.protocol": "PLAINTEXT"
            })

    def test_topic_exists_true(self, topic_manager, admin_client_mock):
        """Test when topic exists."""
        # Mock list_topics to return our topic
        metadata_mock = Mock()
        metadata_mock.topics = {
            "existing-topic": Mock(),
            "another-topic": Mock()
        }
        admin_client_mock.list_topics.return_value = metadata_mock

        assert topic_manager.topic_exists("existing-topic") is True

    def test_topic_exists_false(self, topic_manager, admin_client_mock):
        """Test when topic doesn't exist."""
        metadata_mock = Mock()
        metadata_mock.topics = {
            "other-topic": Mock()
        }
        admin_client_mock.list_topics.return_value = metadata_mock

        assert topic_manager.topic_exists("non-existing-topic") is False

    def test_topic_exists_error(self, topic_manager, admin_client_mock):
        """Test when list_topics fails."""
        admin_client_mock.list_topics.side_effect = KafkaException(
            KafkaError(-1, "Connection error")
        )

        with pytest.raises(KafkaException):
            topic_manager.topic_exists("some-topic")

    def test_create_topic_success(self, topic_manager, admin_client_mock):
        """Test successful topic creation."""
        # Mock successful creation
        futures = {"test-topic": Mock()}
        futures["test-topic"].result.return_value = None
        admin_client_mock.create_topics.return_value = futures

        topic_manager.create_topic("test-topic", num_partitions=3, replication_factor=1)

        # Verify create_topics was called with correct parameters
        calls = admin_client_mock.create_topics.call_args
        new_topics = calls[0][0]
        assert len(new_topics) == 1
        assert new_topics[0].topic == "test-topic"
        assert new_topics[0].num_partitions == 3
        assert new_topics[0].replication_factor == 1

    def test_create_topic_already_exists(self, topic_manager, admin_client_mock):
        """Test creating topic that already exists."""
        # Mock topic already exists error
        error = KafkaError(36, "Topic already exists")  # Error code 36 is TOPIC_ALREADY_EXISTS
        futures = {"test-topic": Mock()}
        futures["test-topic"].result.side_effect = KafkaException(error)
        admin_client_mock.create_topics.return_value = futures

        # Should not raise exception for already exists
        topic_manager.create_topic("test-topic")

    def test_create_topic_error(self, topic_manager, admin_client_mock):
        """Test topic creation failure."""
        # Mock creation failure
        error = KafkaError(-1, "Creation failed")
        futures = {"test-topic": Mock()}
        futures["test-topic"].result.side_effect = KafkaException(error)
        admin_client_mock.create_topics.return_value = futures

        with pytest.raises(KafkaException):
            topic_manager.create_topic("test-topic")

    def test_ensure_topic_exists_creates_new(self, topic_manager, admin_client_mock):
        """Test ensure_topic_exists creates topic when it doesn't exist."""
        # Mock topic doesn't exist
        metadata_mock = Mock()
        metadata_mock.topics = {}
        admin_client_mock.list_topics.return_value = metadata_mock

        # Mock successful creation
        futures = {"new-topic": Mock()}
        futures["new-topic"].result.return_value = None
        admin_client_mock.create_topics.return_value = futures

        result = topic_manager.ensure_topic_exists("new-topic", num_partitions=2)

        assert result is True
        admin_client_mock.create_topics.assert_called_once()

    def test_ensure_topic_exists_already_exists(self, topic_manager, admin_client_mock):
        """Test ensure_topic_exists when topic already exists."""
        # Mock topic exists
        metadata_mock = Mock()
        metadata_mock.topics = {"existing-topic": Mock()}
        admin_client_mock.list_topics.return_value = metadata_mock

        result = topic_manager.ensure_topic_exists("existing-topic")

        assert result is True
        admin_client_mock.create_topics.assert_not_called()

    def test_ensure_topic_with_config(self, topic_manager, admin_client_mock):
        """Test creating topic with additional configuration."""
        # Mock topic doesn't exist
        metadata_mock = Mock()
        metadata_mock.topics = {}
        admin_client_mock.list_topics.return_value = metadata_mock

        # Mock successful creation
        futures = {"configured-topic": Mock()}
        futures["configured-topic"].result.return_value = None
        admin_client_mock.create_topics.return_value = futures

        config = {
            "retention.ms": "604800000",  # 7 days
            "compression.type": "gzip"
        }

        topic_manager.create_topic(
            "configured-topic",
            num_partitions=1,
            replication_factor=3,
            config=config
        )

        # Verify config was passed
        calls = admin_client_mock.create_topics.call_args
        new_topics = calls[0][0]
        assert new_topics[0].config == config
