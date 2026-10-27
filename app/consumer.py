"""Multi-stream Kafka consumer.

One thread (and one consumer group) per stream so streams are processed in
parallel and lag is visible per stream. Offsets are committed after processing
(at-least-once). Set CONSUMER_DELAY_MS to slow processing and build lag on
purpose, which is handy for demonstrating the lag alerts.
"""
import json
import logging
import signal
import threading
import time

from confluent_kafka import Consumer, KafkaError
from prometheus_client import Counter, Histogram, start_http_server

from app import config
from app.common import ensure_topics, setup_logging

logger = logging.getLogger("consumer")

PROCESSED = Counter(
    "obs_consumer_messages_processed", "Messages processed", ["stream"]
)
ERRORS = Counter("obs_consumer_errors", "Consumer errors", ["stream", "stage"])
PROCESSING_SECONDS = Histogram(
    "obs_consumer_processing_seconds",
    "Time spent processing one message",
    ["stream"],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 10),
)
END_TO_END_SECONDS = Histogram(
    "obs_consumer_end_to_end_latency_seconds",
    "Time from event creation to processing completion",
    ["stream"],
    buckets=(0.05, 0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300),
)


def process_event(stream: str, event: dict) -> dict:
    """Light per-stream enrichment. Replace with real business logic."""
    data = event.get("data", {})
    if stream == "weather":
        return {"heat_alert": data.get("temperature_c", 0) >= 35}
    if stream == "crypto":
        return {"big_move": abs(data.get("change_24h_pct", 0)) >= 5}
    if stream == "news":
        return {"trending": data.get("score", 0) >= 300}
    if stream == "system":
        return {"cpu_high": data.get("cpu_pct", 0) >= 85}
    return {}


class StreamConsumer(threading.Thread):
    def __init__(self, stream: str, stop_event: threading.Event):
        super().__init__(name=f"consumer-{stream}", daemon=True)
        self.stream = stream
        self.topic = config.TOPICS[stream]
        self.stop_event = stop_event

    def run(self) -> None:
        group_id = f"{config.CONSUMER_GROUP_PREFIX}-{self.stream}"
        consumer = Consumer(
            {
                "bootstrap.servers": config.BOOTSTRAP_SERVERS,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "client.id": f"obs-{self.stream}",
            }
        )
        consumer.subscribe([self.topic])
        logger.info("started stream=%s topic=%s group=%s", self.stream, self.topic, group_id)

        try:
            while not self.stop_event.is_set():
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        ERRORS.labels(self.stream, "poll").inc()
                        logger.error("consumer error: %s", msg.error())
                    continue
                self._handle(consumer, msg)
        finally:
            consumer.close()

    def _handle(self, consumer: Consumer, msg) -> None:
        started = time.time()
        try:
            event = json.loads(msg.value())
            process_event(self.stream, event)
            if config.CONSUMER_DELAY_MS:
                time.sleep(config.CONSUMER_DELAY_MS / 1000.0)
            done = time.time()
            PROCESSING_SECONDS.labels(self.stream).observe(done - started)
            END_TO_END_SECONDS.labels(self.stream).observe(max(done - event["ts"], 0))
            PROCESSED.labels(self.stream).inc()
            consumer.commit(message=msg, asynchronous=False)
        except Exception:  # noqa: BLE001
            ERRORS.labels(self.stream, "process").inc()
            logger.exception(
                "failed to process message at %s[%s]@%s", msg.topic(), msg.partition(), msg.offset()
            )


def main() -> None:
    setup_logging()
    start_http_server(config.CONSUMER_METRICS_PORT)
    logger.info(
        "metrics on :%d  bootstrap=%s delay=%dms",
        config.CONSUMER_METRICS_PORT, config.BOOTSTRAP_SERVERS, config.CONSUMER_DELAY_MS,
    )

    ensure_topics()
    stop_event = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop_event.set())

    workers = [StreamConsumer(s, stop_event) for s in config.STREAMS]
    for worker in workers:
        worker.start()
    stop_event.wait()
    for worker in workers:
        worker.join(timeout=10)


if __name__ == "__main__":
    main()
