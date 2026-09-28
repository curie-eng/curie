"""Alertmanager signer injects a legal partition and HMAC-signs the body."""

from __future__ import annotations

import hashlib
import hmac
import importlib.util
import json
import threading
import urllib.error
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "alert_signer",
    Path(__file__).with_name("server.py"),
)
assert _SPEC is not None and _SPEC.loader is not None
signer = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(signer)


def test_prepare_injects_partition_and_stable_delivery_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CURIE_HOOK_SECRET", "hook-secret")
    payload = {
        "groupKey": '{}:{alertname="Example"}',
        "status": "firing",
        "alerts": [
            {"fingerprint": "fp-b", "labels": {"curie_workload": "api"}},
            {"fingerprint": "fp-a", "labels": {"curie_workload": "api"}},
        ],
    }
    body, signature, delivery = signer.prepare(payload)
    forwarded = json.loads(body)
    assert (
        forwarded["curie_partition"]
        == hashlib.sha256(payload["groupKey"].encode()).hexdigest()[:32]
    )
    assert len(forwarded["curie_partition"]) == 32
    expected = "sha256=" + hmac.new(b"hook-secret", body, hashlib.sha256).hexdigest()
    assert signature == expected
    again = signer.prepare(payload)
    assert again[2] == delivery
    payload["alerts"][0]["startsAt"] = "2026-09-16T00:00:00Z"
    later = signer.prepare(payload)
    assert later[2] != delivery


def test_unsigned_curie_body_is_not_what_the_signer_forwards(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CURIE_HOOK_SECRET", "hook-secret")
    original = {"groupKey": "g", "status": "firing", "alerts": []}
    body, _signature, _delivery = signer.prepare(original)
    assert json.loads(body)["curie_partition"]
    assert b"curie_partition" in body


@contextmanager
def _http_server(handler: type[BaseHTTPRequestHandler]) -> Iterator[str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_signer_http_authentication_and_forwarding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    received: list[tuple[bytes, dict[str, str]]] = []

    class Ingress(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            pass

        def do_POST(self) -> None:  # noqa: N802
            body = self.rfile.read(int(self.headers["Content-Length"]))
            received.append((body, dict(self.headers.items())))
            self.send_response(202)
            self.end_headers()

    def post(url: str, body: bytes, token: str) -> int:
        request = urllib.request.Request(
            url,
            data=body,
            method="POST",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status
        except urllib.error.HTTPError as exc:
            return exc.code

    payload = {
        "groupKey": '{}:{alertname="Example"}',
        "status": "firing",
        "alerts": [{"fingerprint": "fp-a", "labels": {"curie_workload": "api"}}],
    }
    raw = json.dumps(payload).encode()
    monkeypatch.setenv("CURIE_HOOK_SECRET", "hook-secret")
    monkeypatch.setenv("CURIE_SIGNER_TOKEN", "signer-token")
    with _http_server(Ingress) as ingress_url:
        monkeypatch.setenv("CURIE_HOOK_URL", f"{ingress_url}/hooks/agent/alertmanager")
        with _http_server(signer.Handler) as signer_url:
            assert post(signer_url, raw, "wrong-token") == 401
            assert post(signer_url, b"[]", "signer-token") == 400
            assert received == []
            assert post(signer_url, raw, "signer-token") == 200
            assert post(signer_url, raw, "signer-token") == 200

    assert len(received) == 2
    first_body, first_headers = received[0]
    second_body, second_headers = received[1]
    assert first_body == second_body
    forwarded = json.loads(first_body)
    assert forwarded["curie_partition"] == signer.partition_value(payload["groupKey"])
    signature = first_headers["X-Curie-Signature-256"]
    delivery = first_headers["X-Curie-Delivery-Id"]
    assert signature == signer.sign("hook-secret", first_body)
    assert second_headers["X-Curie-Signature-256"] == signature
    assert second_headers["X-Curie-Delivery-Id"] == delivery
    assert first_headers["X-Curie-Delivery-Id"]
