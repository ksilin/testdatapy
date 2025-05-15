"""Topic management functionality for Kafka."""
import logging
from typing import Dict, Any

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaError

logger = logging.getLogger(__name__)


class TopicManager:
    """Manages Kafka topic operations."""

    def __init__(self, bootstrap_servers: str, config: Dict[str, Any] | None = None):
        """Initialize TopicManager.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            config: Additional configuration for AdminClient
        """
        self.bootstrap_servers = bootstrap_servers
        admin_config = {"bootstrap.servers": bootstrap_servers}
        if config:
            admin_config.update(config)
        self.admin_client = AdminClient(admin_config)

    def topic_exists(self, topic_name: str) -> bool:
        """Check if a topic exists.

        Args:
            topic_name: Name of the topic to check

        Returns:
            True if topic exists, False otherwise

        Raises:
            KafkaException: If unable to connect to Kafka
        """
        try:
            metadata = self.admin_client.list_topics(timeout=10)
            return topic_name in metadata.topics
        except Exception as e:
            logger.error(f"Error checking topic existence: {e}")
            raise KafkaException(e)

    def create_topic(
        self,
        topic_name: str,
        num_partitions: int = 1,
        replication_factor: int = 1,
        config: Dict[str, str] | None = None
    ) -> None:
        """Create a new topic.

        Args:
            topic_name: Name of the topic to create
            num_partitions: Number of partitions
            replication_factor: Replication factor
            config: Additional topic configuration

        Raises:
            KafkaException: If topic creation fails (except for already exists)
        """
        new_topic = NewTopic(
            topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            config=config or {}
        )

        futures = self.admin_client.create_topics([new_topic])
        try:
            futures[topic_name].result(timeout=10)
            logger.info(f"Topic '{topic_name}' created successfully")
        except KafkaException as e:
            error_code = e.args[0].code()
            # Ignore "topic already exists" error
            if error_code == KafkaError.TOPIC_ALREADY_EXISTS:
                logger.debug(f"Topic '{topic_name}' already exists")
            else:
                logger.error(f"Failed to create topic '{topic_name}': {e}")
                raise

    def ensure_topic_exists(
        self,
        topic_name: str,
        num_partitions: int = 1,
        replication_factor: int = 1,
        config: Dict[str, str] | None = None
    ) -> bool:
        """Ensure a topic exists, creating it if necessary.

        Args:
            topic_name: Name of the topic
            num_partitions: Number of partitions (used only if creating)
            replication_factor: Replication factor (used only if creating)
            config: Additional topic configuration (used only if creating)

        Returns:
            True if topic exists or was created successfully

        Raises:
            KafkaException: If unable to create topic or check existence
        """
        if self.topic_exists(topic_name):
            logger.debug(f"Topic '{topic_name}' already exists")
            return True

        logger.info(f"Topic '{topic_name}' doesn't exist, creating...")
        self.create_topic(
            topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            config=config
        )
        return True

    def delete_topic(self, topic_name: str) -> None:
        """Delete a topic.

        Args:
            topic_name: Name of the topic to delete

        Raises:
            KafkaException: If topic deletion fails
        """
        futures = self.admin_client.delete_topics([topic_name])
        try:
            futures[topic_name].result(timeout=10)
            logger.info(f"Topic '{topic_name}' deleted successfully")
        except KafkaException as e:
            logger.error(f"Failed to delete topic '{topic_name}': {e}")
            raise

    def list_topics(self) -> list[str]:
        """List all topics.

        Returns:
            List of topic names

        Raises:
            KafkaException: If unable to list topics
        """
        try:
            metadata = self.admin_client.list_topics(timeout=10)
            return list(metadata.topics.keys())
        except Exception as e:
            logger.error(f"Error listing topics: {e}")
            raise KafkaException(e)
