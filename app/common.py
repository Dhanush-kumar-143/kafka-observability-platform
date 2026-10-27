"""Shared helpers: logging and idempotent topic creation."""
import logging
import time

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from app import config

logger = logging.getLogger(__name__)


def setup_logging() -> None:
    logging.basicConfig(
        level=config.LOG_LEVEL,
        format="%(asctime)s %(levelname)s [%(threadName)s] %(name)s: %(message)s",
    )


def ensure_topics(retries: int = 30, delay_seconds: int = 5) -> None:
    """Wait for the broker, then create any missing topics."""
    admin = AdminClient({"bootstrap.servers": config.BOOTSTRAP_SERVERS})

    metadata = None
    for attempt in range(1, retries + 1):
        try:
            metadata = admin.list_topics(timeout=10)
            break
        except KafkaException as exc:
            logger.warning(
                "broker not ready (attempt %d/%d): %s", attempt, retries, exc
            )
            time.sleep(delay_seconds)
    if metadata is None:
        raise RuntimeError(f"Kafka not reachable at {config.BOOTSTRAP_SERVERS}")

    missing = [
        NewTopic(
            topic,
            num_partitions=config.TOPIC_PARTITIONS,
            replication_factor=config.TOPIC_REPLICATION_FACTOR,
        )
        for topic in config.TOPICS.values()
        if topic not in metadata.topics
    ]
    if not missing:
        logger.info("all topics already exist")
        return

    for topic, future in admin.create_topics(missing).items():
        try:
            future.result()
            logger.info("created topic %s", topic)
        except KafkaException as exc:
            # Producer and consumer start together; the loser of the race is fine.
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
