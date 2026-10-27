"""Data sources for the four event streams.

Every fetch function returns ``(source, records)`` where ``records`` is a list of
dicts. Each record carries a ``key`` used as the Kafka message key, so related
events land on the same partition.

Real public APIs are used when reachable (no API keys needed):
  * weather - Open-Meteo
  * crypto  - CoinGecko
  * news    - Hacker News
  * system  - psutil (local host metrics)
If an API fails, or SIMULATED_DATA=true, realistic generated data is used so the
pipeline keeps flowing.
"""
import logging
import random
import socket
import time
from typing import Callable, Dict, List, Tuple

import psutil
import requests

from app import config

logger = logging.getLogger(__name__)

Record = Dict[str, object]
FetchResult = Tuple[str, List[Record]]

_seen_news_ids = set()


def build_event(stream: str, source: str, data: Record) -> Dict[str, object]:
    """Wrap a record in the common event envelope."""
    return {"stream": stream, "source": source, "ts": time.time(), "data": data}


# ------------------------------------------------------------------ weather
def simulate_weather() -> Record:
    return {
        "key": config.WEATHER_CITY,
        "city": config.WEATHER_CITY,
        "temperature_c": round(random.uniform(24.0, 36.0), 1),
        "humidity_pct": random.randint(45, 95),
        "wind_kmh": round(random.uniform(2.0, 25.0), 1),
    }


def fetch_weather() -> FetchResult:
    if config.SIMULATED_DATA:
        return "simulated", [simulate_weather()]
    try:
        resp = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": config.WEATHER_LATITUDE,
                "longitude": config.WEATHER_LONGITUDE,
                "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
            },
            timeout=config.HTTP_TIMEOUT_SECONDS,
        )
        resp.raise_for_status()
        cur = resp.json()["current"]
        record = {
            "key": config.WEATHER_CITY,
            "city": config.WEATHER_CITY,
            "temperature_c": cur["temperature_2m"],
            "humidity_pct": cur["relative_humidity_2m"],
            "wind_kmh": cur["wind_speed_10m"],
        }
        return "open-meteo", [record]
    except Exception as exc:  # noqa: BLE001 - any failure falls back to simulation
        logger.warning("weather API failed (%s); using simulated data", exc)
        return "simulated", [simulate_weather()]


# ------------------------------------------------------------------- crypto
_BASE_PRICES = {"bitcoin": 65000.0, "ethereum": 3200.0, "solana": 150.0}


def simulate_crypto() -> List[Record]:
    records = []
    for coin in config.CRYPTO_COINS:
        base = _BASE_PRICES.get(coin, 100.0)
        records.append(
            {
                "key": coin,
                "coin": coin,
                "price_usd": round(base * random.uniform(0.97, 1.03), 2),
                "change_24h_pct": round(random.uniform(-6.0, 6.0), 2),
            }
        )
    return records


def fetch_crypto() -> FetchResult:
    if config.SIMULATED_DATA:
        return "simulated", simulate_crypto()
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={
                "ids": ",".join(config.CRYPTO_COINS),
                "vs_currencies": "usd",
                "include_24hr_change": "true",
            },
            timeout=config.HTTP_TIMEOUT_SECONDS,
        )
        resp.raise_for_status()
        payload = resp.json()
        records = [
            {
                "key": coin,
                "coin": coin,
                "price_usd": values["usd"],
                "change_24h_pct": round(values.get("usd_24h_change", 0.0), 2),
            }
            for coin, values in payload.items()
        ]
        if not records:
            raise ValueError("empty response")
        return "coingecko", records
    except Exception as exc:  # noqa: BLE001
        logger.warning("crypto API failed (%s); using simulated data", exc)
        return "simulated", simulate_crypto()


# --------------------------------------------------------------------- news
_SIM_HEADLINES = [
    "Kubernetes 1.31 released with new scheduling features",
    "Why observability matters more than monitoring",
    "Terraform module patterns for multi-account AWS",
    "Apache Kafka KRaft mode is now the default",
    "Show HN: A tiny GitOps controller written in Go",
]


def simulate_news() -> List[Record]:
    headline = random.choice(_SIM_HEADLINES)
    item_id = random.randint(10_000_000, 99_999_999)
    return [
        {
            "key": str(item_id),
            "id": item_id,
            "title": headline,
            "score": random.randint(10, 800),
            "comments": random.randint(0, 300),
        }
    ]


def fetch_news() -> FetchResult:
    if config.SIMULATED_DATA:
        return "simulated", simulate_news()
    try:
        base = "https://hacker-news.firebaseio.com/v0"
        ids = requests.get(f"{base}/topstories.json", timeout=config.HTTP_TIMEOUT_SECONDS).json()
        records = []
        for item_id in ids[: config.NEWS_MAX_ITEMS]:
            if item_id in _seen_news_ids:
                continue
            item = requests.get(
                f"{base}/item/{item_id}.json", timeout=config.HTTP_TIMEOUT_SECONDS
            ).json()
            _seen_news_ids.add(item_id)
            records.append(
                {
                    "key": str(item_id),
                    "id": item_id,
                    "title": item.get("title", ""),
                    "score": item.get("score", 0),
                    "comments": item.get("descendants", 0),
                }
            )
        return "hacker-news", records
    except Exception as exc:  # noqa: BLE001
        logger.warning("news API failed (%s); using simulated data", exc)
        return "simulated", simulate_news()


# ------------------------------------------------------------------- system
def fetch_system() -> FetchResult:
    host = socket.gethostname()
    record = {
        "key": host,
        "host": host,
        "cpu_pct": psutil.cpu_percent(interval=None),
        "memory_pct": psutil.virtual_memory().percent,
        "disk_pct": psutil.disk_usage("/").percent,
    }
    return "psutil", [record]


FETCHERS: Dict[str, Callable[[], FetchResult]] = {
    "weather": fetch_weather,
    "crypto": fetch_crypto,
    "news": fetch_news,
    "system": fetch_system,
}
