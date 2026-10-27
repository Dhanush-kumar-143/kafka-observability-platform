import requests

from app import config, streams
from app.consumer import process_event


def _raise(*args, **kwargs):
    raise requests.ConnectionError("network disabled in tests")


def test_build_event_has_envelope_fields():
    event = streams.build_event("weather", "simulated", {"key": "x"})
    assert set(event) == {"stream", "source", "ts", "data"}
    assert event["stream"] == "weather"


def test_weather_falls_back_to_simulation(monkeypatch):
    monkeypatch.setattr(requests, "get", _raise)
    source, records = streams.fetch_weather()
    assert source == "simulated"
    assert {"key", "city", "temperature_c", "humidity_pct", "wind_kmh"} <= set(records[0])


def test_crypto_falls_back_to_simulation(monkeypatch):
    monkeypatch.setattr(requests, "get", _raise)
    source, records = streams.fetch_crypto()
    assert source == "simulated"
    assert len(records) == len(config.CRYPTO_COINS)
    assert all("price_usd" in r for r in records)


def test_news_falls_back_to_simulation(monkeypatch):
    monkeypatch.setattr(requests, "get", _raise)
    source, records = streams.fetch_news()
    assert source == "simulated"
    assert {"key", "title", "score"} <= set(records[0])


def test_system_metrics_are_percentages():
    source, records = streams.fetch_system()
    assert source == "psutil"
    for field in ("cpu_pct", "memory_pct", "disk_pct"):
        assert 0 <= records[0][field] <= 100


def test_news_deduplicates_already_seen_items(monkeypatch):
    streams._seen_news_ids.clear()

    class FakeResponse:
        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

    def fake_get(url, **kwargs):
        if url.endswith("topstories.json"):
            return FakeResponse([1, 2])
        return FakeResponse({"title": "t", "score": 1, "descendants": 0})

    monkeypatch.setattr(requests, "get", fake_get)
    _, first = streams.fetch_news()
    _, second = streams.fetch_news()
    assert len(first) == 2
    assert second == []


def test_process_event_flags():
    assert process_event("weather", {"data": {"temperature_c": 40}})["heat_alert"] is True
    assert process_event("crypto", {"data": {"change_24h_pct": -7}})["big_move"] is True
    assert process_event("news", {"data": {"score": 10}})["trending"] is False
    assert process_event("system", {"data": {"cpu_pct": 90}})["cpu_high"] is True
