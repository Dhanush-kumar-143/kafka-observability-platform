"""Central configuration, read from environment variables."""
import os


def _env_bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

STREAMS = ("weather", "crypto", "news", "system")
TOPIC_PREFIX = os.getenv("TOPIC_PREFIX", "obs")
TOPICS = {stream: f"{TOPIC_PREFIX}-{stream}" for stream in STREAMS}
TOPIC_PARTITIONS = int(os.getenv("TOPIC_PARTITIONS", "3"))
TOPIC_REPLICATION_FACTOR = int(os.getenv("TOPIC_REPLICATION_FACTOR", "1"))

# Seconds between fetches, per stream (override with env vars).
INTERVALS = {
    "weather": float(os.getenv("WEATHER_INTERVAL_SECONDS", "10")),
    "crypto": float(os.getenv("CRYPTO_INTERVAL_SECONDS", "10")),
    "news": float(os.getenv("NEWS_INTERVAL_SECONDS", "20")),
    "system": float(os.getenv("SYSTEM_INTERVAL_SECONDS", "1")),
}

# When true, no external API is called and data is generated locally.
SIMULATED_DATA = _env_bool("SIMULATED_DATA", False)
HTTP_TIMEOUT_SECONDS = float(os.getenv("HTTP_TIMEOUT_SECONDS", "5"))

WEATHER_CITY = os.getenv("WEATHER_CITY", "Mumbai")
WEATHER_LATITUDE = float(os.getenv("WEATHER_LATITUDE", "19.0760"))
WEATHER_LONGITUDE = float(os.getenv("WEATHER_LONGITUDE", "72.8777"))
CRYPTO_COINS = os.getenv("CRYPTO_COINS", "bitcoin,ethereum,solana").split(",")
NEWS_MAX_ITEMS = int(os.getenv("NEWS_MAX_ITEMS", "5"))

CONSUMER_GROUP_PREFIX = os.getenv("CONSUMER_GROUP_PREFIX", "obs-consumer")
# Artificial per-message delay. Raise it to build consumer lag on purpose.
CONSUMER_DELAY_MS = int(os.getenv("CONSUMER_DELAY_MS", "0"))

PRODUCER_METRICS_PORT = int(os.getenv("PRODUCER_METRICS_PORT", "8000"))
CONSUMER_METRICS_PORT = int(os.getenv("CONSUMER_METRICS_PORT", "8001"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
