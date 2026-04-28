import io
import json
from unittest.mock import patch
from urllib.error import HTTPError, URLError

import pytest

from sochdb.studio import StudioAPIError, StudioClient


class _MockResponse:
    def __init__(self, payload):
        self._payload = payload

    def read(self):
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_health_returns_json_payload():
    client = StudioClient("http://studio.example")

    with patch("sochdb.studio.request.urlopen", return_value=_MockResponse({"ok": True})):
        assert client.health() == {"ok": True}


def test_ingest_events_requires_api_key():
    client = StudioClient("http://studio.example")

    with pytest.raises(ValueError):
        client.ingest_events([{"type": "test"}])


def test_ingest_events_parses_success_response():
    client = StudioClient("http://studio.example", api_key="secret")

    payload = {"ok": True, "ingested": 2, "eventIds": ["evt_1", "evt_2"]}
    with patch("sochdb.studio.request.urlopen", return_value=_MockResponse(payload)):
        result = client.ingest_events([{"type": "retrieval"}, {"type": "trace"}], source="sdk-test")

    assert result.ok is True
    assert result.ingested == 2
    assert result.event_ids == ["evt_1", "evt_2"]


def test_ingest_events_surfaces_http_errors():
    client = StudioClient("http://studio.example", api_key="secret")
    error_body = io.BytesIO(json.dumps({"error": "bad api key"}).encode("utf-8"))
    http_error = HTTPError(
        url="http://studio.example/api/studio/ingest/events",
        code=401,
        msg="Unauthorized",
        hdrs=None,
        fp=error_body,
    )

    with patch("sochdb.studio.request.urlopen", side_effect=http_error):
        with pytest.raises(StudioAPIError, match="bad api key") as exc_info:
            client.ingest_events([{"type": "retrieval"}])

    assert exc_info.value.status_code == 401


def test_health_surfaces_network_errors():
    client = StudioClient("http://studio.example")

    with patch("sochdb.studio.request.urlopen", side_effect=URLError("connection refused")):
        with pytest.raises(StudioAPIError, match="Failed to reach Studio backend"):
            client.health()
