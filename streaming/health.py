"""Lightweight health-check and metrics HTTP server for the producer container.

Runs in a daemon thread; does not block the event loop.

Endpoints
---------
GET /health   → 200 ``ok``  (used by Docker HEALTHCHECK and Kubernetes liveness)
GET /metrics  → 200 JSON    (events_sent, errors, last_event_time, status)
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

_state: dict[str, Any] = {
    "status": "ok",
    "events_sent": 0,
    "errors": 0,
    "last_event_time": None,
}
_lock = threading.Lock()


def record_emit(count: int) -> None:
    with _lock:
        _state["events_sent"] += count


def record_error() -> None:
    with _lock:
        _state["errors"] += 1
        _state["status"] = "degraded"


def set_last_event_time(ts: str) -> None:
    with _lock:
        _state["last_event_time"] = ts
        _state["status"] = "ok"


def get_state() -> dict[str, Any]:
    with _lock:
        return dict(_state)


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")

        elif self.path == "/metrics":
            body = json.dumps(get_state(), indent=2).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *_: Any) -> None:
        pass  # suppress per-request access logs from the health server


def start(port: int = 8080) -> None:
    """Start the health server in a background daemon thread.

    Called once from ``streaming.producer.main()``. The thread is daemonised
    so it terminates automatically when the main process exits.
    """
    server = HTTPServer(("0.0.0.0", port), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True, name="health-server")
    thread.start()
