"""Multi-stream Kafka producer.

One thread per stream (weather, crypto, news, system) fetches data on its own
interval and publishes JSON events. Prometheus metrics are exposed on
PRODUCER_METRICS_PORT.
"""
import json
import logging
import signal
import threading
import time

from confluent_kafka import Producer
from prometheus_client import Counter, Histogram, start_http_server

from app import config
from app.common import ensure_topics, setup_logging
from app.streams import FETCHERS, build_event

logger = logging.getLogger("producer")

MESSAGES_SENT = Counter(
    "obs_producer_messages_sent", "Messages acknowledged by the broker", ["stream"]
)
PRODUCE_ERRORS = Counter(
    "obs_producer_errors", "Producer errors by stage", ["stream", "stage"]
)
FETCH_LATENCY = Histogram(
    "obs_producer_fetch_seconds",
    "Time spent fetching data from the source",
    ["stream"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)


class StreamWorker(threading.Thread):
    def __init__(self, stream: str, producer: Producer, stop_event: threading.Event):
        super().__init__(name=f"producer-{stream}", daemon=True)
        self.stream = stream
        self.topic = config.TOPICS[stream]
        self.interval = config.INTERVALS[stream]
        self.producer = producer
        self.stop_event = stop_event

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            PRODUCE_ERRORS.labels(self.stream, "delivery").inc()
            logger.error("delivery failed for %s: %s", self.topic, err)
        else:
            MESSAGES_SENT.labels(self.stream).inc()

    def run(self) -> None:
        fetch = FETCHERS[self.stream]
        logger.info("started stream=%s topic=%s interval=%ss", self.stream, self.topic, self.interval)
        while not self.stop_event.is_set():
            started = time.time()
            try:
                source, records = fetch()
                FETCH_LATENCY.labels(self.stream).observe(time.time() - started)
                for record in records:
                    key = str(record.get("key", self.stream))
                    event = build_event(self.stream, source, record)
                    self.producer.produce(
                        self.topic,
                        key=key.encode(),
                        value=json.dumps(event).encode(),
                        on_delivery=self._on_delivery,
                    )
                self.producer.poll(0)
            except BufferError:
                PRODUCE_ERRORS.labels(self.stream, "queue_full").inc()
                logger.warning("local producer queue is full")
                self.producer.poll(1)
            except Exception:  # noqa: BLE001
                PRODUCE_ERRORS.labels(self.stream, "fetch").inc()
                logger.exception("error in stream %s", self.stream)
            self.stop_event.wait(self.interval)


def main() -> None:
    setup_logging()
    start_http_server(config.PRODUCER_METRICS_PORT)
    logger.info(
        "metrics on :%d  bootstrap=%s", config.PRODUCER_METRICS_PORT, config.BOOTSTRAP_SERVERS
    )

    ensure_topics()
    producer = Producer(
        {
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 20,
            "client.id": "obs-producer",
        }
    )

    stop_event = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop_event.set())

    workers = [StreamWorker(s, producer, stop_event) for s in config.STREAMS]
    for worker in workers:
        worker.start()

    while not stop_event.is_set():
        producer.poll(0.5)

    logger.info("shutting down, flushing producer")
    producer.flush(10)


if __name__ == "__main__":
    main()
