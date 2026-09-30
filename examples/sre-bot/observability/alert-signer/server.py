"""Sign Alertmanager notifications for Curie hook ingress (#2572).

Alertmanager cannot HMAC the body. This adapter is the one supported source
binding: it injects a partition-safe ``curie_partition`` derived from groupKey,
assigns a stable delivery id, stamps the current unix time, signs
``f"{timestamp}.{delivery}.".encode() + body`` with the derived hook secret, and
POSTs to ``POST /hooks/{agent}/{hook}`` with ``X-Curie-Timestamp`` and
``X-Curie-Delivery-Id`` alongside the signature. Curie refuses a timestamp more
than five minutes from its clock, so the forwarder must be roughly in sync.

Environment:
  CURIE_HOOK_URL       Full ingest URL, including agent id and hook name
  CURIE_HOOK_SECRET    The derived per-agent hook secret
  LISTEN_ADDR          Bind address, default 0.0.0.0:8080
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


def partition_value(group_key: str) -> str:
    return hashlib.sha256(group_key.encode()).hexdigest()[:32]


def delivery_id(group_key: str, status: str, fingerprints: list[str], starts_at: list[str]) -> str:
    material = "|".join([group_key, status, *fingerprints, *starts_at])
    return hashlib.sha256(material.encode()).hexdigest()


def sign(secret: str, timestamp: str, delivery: str, body: bytes) -> str:
    material = f"{timestamp}.{delivery}.".encode() + body
    return "sha256=" + hmac.new(secret.encode(), material, hashlib.sha256).hexdigest()


def prepare(payload: dict[str, Any]) -> tuple[bytes, str, str, str]:
    """Return ``(body, signature, delivery, timestamp)`` for one forward."""

    group_key = str(payload.get("groupKey") or "")
    status = str(payload.get("status") or "")
    alerts = [alert for alert in payload.get("alerts") or [] if isinstance(alert, dict)]
    fingerprints = sorted(str(alert.get("fingerprint") or "") for alert in alerts)
    starts_at = sorted(str(alert.get("startsAt") or "") for alert in alerts)
    forwarded = dict(payload)
    forwarded["curie_partition"] = partition_value(group_key)
    body = json.dumps(forwarded, separators=(",", ":")).encode()
    delivery = delivery_id(group_key, status, fingerprints, starts_at)
    timestamp = str(int(time.time()))
    signature = sign(os.environ["CURIE_HOOK_SECRET"], timestamp, delivery, body)
    return body, signature, delivery, timestamp


def forward(body: bytes, signature: str, delivery: str, timestamp: str) -> int:
    request = urllib.request.Request(
        os.environ["CURIE_HOOK_URL"],
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Curie-Signature-256": signature,
            "X-Curie-Delivery-Id": delivery,
            "X-Curie-Timestamp": timestamp,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return int(response.status)
    except urllib.error.HTTPError as exc:
        return int(exc.code)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_POST(self) -> None:  # noqa: N802
        expected = os.environ.get("CURIE_SIGNER_TOKEN") or ""
        presented = self.headers.get("Authorization") or ""
        want = f"Bearer {expected}"
        if not expected or not hmac.compare_digest(presented.encode(), want.encode()):
            self.send_response(401)
            self.end_headers()
            return
        length = int(self.headers.get("Content-Length") or "0")
        raw = self.rfile.read(length)
        try:
            payload = json.loads(raw.decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("payload is not an object")
            body, signature, delivery, timestamp = prepare(payload)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, KeyError):
            self.send_response(400)
            self.end_headers()
            return
        status = forward(body, signature, delivery, timestamp)
        self.send_response(200 if 200 <= status < 300 else 502)
        self.end_headers()


def main() -> None:
    addr = os.environ.get("LISTEN_ADDR", "0.0.0.0:8080")
    host, port_s = addr.rsplit(":", 1)
    server = ThreadingHTTPServer((host, int(port_s)), Handler)
    server.serve_forever()


if __name__ == "__main__":
    main()
