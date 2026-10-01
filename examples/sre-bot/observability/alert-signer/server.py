"""Sign Alertmanager notifications for Curie hook ingress (#2572).

Alertmanager cannot HMAC the body. This adapter is the one supported source
binding: it injects a partition-safe ``curie_partition`` derived from groupKey,
assigns a stable delivery id, stamps the current unix time, and signs the
timestamp, delivery id, decoded hook name, requested tool policy and raw body.
The context is compact ASCII JSON for ``[hook, tool_access]``, with ``null``
when the policy is omitted. Its length frames the boundary before the body:
``f"{timestamp}.{delivery}.{len(context)}:".encode() + context + body``.
The hook and policy come from ``CURIE_HOOK_URL``, which is also the destination
for the POST. Duplicate or invalid ``tool_access`` query values are refused.
The request sends ``X-Curie-Timestamp`` and ``X-Curie-Delivery-Id`` alongside
the signature. Curie refuses a timestamp more than five minutes from its clock,
so the forwarder must be roughly in sync.
The delivery id must not contain "." (the signed-material delimiter); the hex ids
this adapter assigns never do.

Environment:
  CURIE_HOOK_URL       Full ingest URL, including hook and optional tool_access
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
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


def partition_value(group_key: str) -> str:
    return hashlib.sha256(group_key.encode()).hexdigest()[:32]


def delivery_id(group_key: str, status: str, fingerprints: list[str], starts_at: list[str]) -> str:
    material = "|".join([group_key, status, *fingerprints, *starts_at])
    return hashlib.sha256(material.encode()).hexdigest()


def sign(
    secret: str,
    timestamp: str,
    delivery: str,
    body: bytes,
    *,
    hook: str,
    tool_access: str | None,
) -> str:
    context = json.dumps([hook, tool_access], ensure_ascii=True, separators=(",", ":")).encode(
        "ascii"
    )
    material = f"{timestamp}.{delivery}.{len(context)}:".encode() + context + body
    return "sha256=" + hmac.new(secret.encode(), material, hashlib.sha256).hexdigest()


def prepare(payload: dict[str, Any]) -> tuple[bytes, str, str, str]:
    """Return ``(body, signature, delivery, timestamp)`` for one forward."""

    hook_url = urllib.parse.urlsplit(os.environ["CURIE_HOOK_URL"])
    hook = urllib.parse.unquote(hook_url.path.rsplit("/", 1)[-1])
    policies = urllib.parse.parse_qs(hook_url.query, keep_blank_values=True).get("tool_access", [])
    if len(policies) > 1 or (policies and policies[0] != "read-only"):
        raise ValueError("tool_access must be omitted or supplied once as read-only")
    tool_access = policies[0] if policies else None
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
    signature = sign(
        os.environ["CURIE_HOOK_SECRET"],
        timestamp,
        delivery,
        body,
        hook=hook,
        tool_access=tool_access,
    )
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
