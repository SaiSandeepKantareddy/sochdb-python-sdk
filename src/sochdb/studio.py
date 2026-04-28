"""
Helpers for talking to the hosted Studio backend over HTTP.

This is intentionally lightweight and uses only the Python standard library so
SDK users do not need an additional HTTP client dependency for basic event
ingestion.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib import error, request


class StudioAPIError(RuntimeError):
    """Raised when the Studio backend returns an error response."""

    def __init__(self, status_code: Optional[int], message: str):
        self.status_code = status_code
        super().__init__(message)


@dataclass
class StudioEventIngestResult:
    """Response returned after ingesting Studio events."""

    ok: bool
    ingested: int
    event_ids: List[str]


class StudioClient:
    """Small HTTP client for the SochDB Studio backend."""

    def __init__(self, base_url: str, api_key: Optional[str] = None, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout

    def health(self) -> Dict[str, Any]:
        """Return the Studio backend health payload."""
        return self._request_json("GET", "/health")

    def ingest_events(
        self,
        events: List[Dict[str, Any]],
        source: str = "python-sdk",
        api_key: Optional[str] = None,
    ) -> StudioEventIngestResult:
        """
        Send events to the Studio backend.

        Args:
            events: Event payloads to ingest
            source: Logical source name shown in Studio
            api_key: Optional override for the client's API key
        """
        effective_api_key = api_key or self.api_key
        if not effective_api_key:
            raise ValueError("Studio API key is required for event ingestion")

        payload = {
            "source": source,
            "events": events,
        }
        data = self._request_json(
            "POST",
            "/api/studio/ingest/events",
            body=payload,
            api_key=effective_api_key,
        )
        return StudioEventIngestResult(
            ok=bool(data.get("ok", False)),
            ingested=int(data.get("ingested", 0)),
            event_ids=[str(event_id) for event_id in data.get("eventIds", [])],
        )

    def _request_json(
        self,
        method: str,
        path: str,
        body: Optional[Dict[str, Any]] = None,
        api_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        headers = {"Accept": "application/json"}
        data = None

        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"

        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        req = request.Request(url, data=data, headers=headers, method=method)

        try:
            with request.urlopen(req, timeout=self.timeout) as response:
                raw = response.read()
        except error.HTTPError as exc:
            message = self._extract_error_message(exc.read(), fallback=exc.reason)
            raise StudioAPIError(exc.code, message) from exc
        except error.URLError as exc:
            raise StudioAPIError(None, f"Failed to reach Studio backend: {exc.reason}") from exc

        if not raw:
            return {}

        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise StudioAPIError(None, "Studio backend returned invalid JSON") from exc

    @staticmethod
    def _extract_error_message(raw: bytes, fallback: str) -> str:
        if not raw:
            return fallback
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return fallback
        return str(parsed.get("error") or parsed.get("message") or fallback)
