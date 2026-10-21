"""Alertmanager signer injects a legal partition and HMAC-signs each delivery.

The signature covers timestamp, delivery id, decoded hook, requested policy and
forwarded body, the scheme ``curie_api.hook_signing`` verifies (#3554).
"""

from __future__ import annotations

import hashlib
import hmac
import importlib.util
import json
import logging
import threading
import urllib.error
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from curie_api import hook_signing, hook_source_signing

_SPEC = importlib.util.spec_from_file_location(
    "alert_signer",
    Path(__file__).with_name("server.py"),
)
assert _SPEC is not None and _SPEC.loader is not None
signer = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(signer)


def _expected_signature(
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
    material = (
        b"curie.hook.delivery.v2\n"
        + f"{timestamp}.{delivery}.{len(context)}:".encode()
        + context
        + body
    )
    return "sha256=" + hmac.new(secret.encode(), material, hashlib.sha256).hexdigest()


def test_prepare_injects_partition_and_stable_delivery_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CURIE_HOOK_SECRET", "hook-secret")
    monkeypatch.setenv("CURIE_HOOK_URL", "http://api.example.com/hooks/acme-agent/alertmanager")
    payload = {
        "groupKey": '{}:{alertname="Example"}',
        "status": "firing",
        "alerts": [
            {"fingerprint": "fp-b", "labels": {"curie_workload": "api"}},
            {"fingerprint": "fp-a", "labels": {"curie_workload": "api"}},
        ],
    }
    body, signature, delivery, timestamp = signer.prepare(payload)
    forwarded = json.loads(body)
    assert (
        forwarded["curie_partition"]
        == hashlib.sha256(payload["groupKey"].encode()).hexdigest()[:32]
    )
    assert len(forwarded["curie_partition"]) == 32
    assert timestamp.isascii() and timestamp.isdigit()
    expected = _expected_signature(
        "hook-secret", timestamp, delivery, body, hook="alertmanager", tool_access=None
    )
    assert signature == expected
    body_only = "sha256=" + hmac.new(b"hook-secret", body, hashlib.sha256).hexdigest()
    assert signature != body_only
    old_material = f"{timestamp}.{delivery}.".encode() + body
    old_signature = "sha256=" + hmac.new(b"hook-secret", old_material, hashlib.sha256).hexdigest()
    assert signature != old_signature
    again = signer.prepare(payload)
    assert again[2] == delivery
    payload["alerts"][0]["startsAt"] = "2026-09-16T00:00:00Z"
    later = signer.prepare(payload)
    assert later[2] != delivery


def test_unsigned_curie_body_is_not_what_the_signer_forwards(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CURIE_HOOK_SECRET", "hook-secret")
    monkeypatch.setenv("CURIE_HOOK_URL", "http://api.example.com/hooks/acme-agent/alertmanager")
    original = {"groupKey": "g", "status": "firing", "alerts": []}
    body, _signature, _delivery, _timestamp = signer.prepare(original)
    assert json.loads(body)["curie_partition"]
    assert b"curie_partition" in body


@pytest.mark.parametrize("tool_access", [None, "read-only"])
def test_prepare_signs_decoded_dotted_hook_and_requested_policy(
    monkeypatch: pytest.MonkeyPatch, tool_access: str | None
) -> None:
    query = "?kind=email&address=ops%40example.com"
    if tool_access is not None:
        query += "&tool_access=read%2Donly"
    monkeypatch.setenv("CURIE_HOOK_SECRET", "hook-secret")
    monkeypatch.setenv(
        "CURIE_HOOK_URL", f"http://api.example.com/hooks/acme-agent/alertmanager%2Erelease{query}"
    )
    payload = {"groupKey": "g", "status": "firing", "alerts": []}
    body, signature, delivery, timestamp = signer.prepare(payload)
    assert signature == _expected_signature(
        "hook-secret",
        timestamp,
        delivery,
        body,
        hook="alertmanager.release",
        tool_access=tool_access,
    )
    assert signature != _expected_signature(
        "hook-secret",
        timestamp,
        delivery,
        body,
        hook="alertmanager%2Erelease",
        tool_access=tool_access,
    )
    assert signature != _expected_signature(
        "hook-secret",
        timestamp,
        delivery,
        body,
        hook="alertmanager.release",
        tool_access="read-only" if tool_access is None else None,
    )


@pytest.mark.parametrize(
    "query",
    [
        "tool_access=read-write",
        "tool_access=READ_ONLY",
        "tool_access=",
        "tool_access=null",
        "tool_access=read-only&tool_access=read-only",
        "tool_access=read-only&tool_access=read-write",
        "tool_access=&tool_access=read-only",
    ],
)
def test_prepare_refuses_invalid_or_duplicate_policy(
    monkeypatch: pytest.MonkeyPatch, query: str
) -> None:
    monkeypatch.setenv("CURIE_HOOK_SECRET", "hook-secret")
    monkeypatch.setenv(
        "CURIE_HOOK_URL", f"http://api.example.com/hooks/acme-agent/alertmanager?{query}"
    )
    with pytest.raises(ValueError):
        signer.prepare({"groupKey": "g", "status": "firing", "alerts": []})


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


@pytest.mark.parametrize("tool_access", [None, "read-only"])
def test_signer_http_authentication_and_forwarding(
    monkeypatch: pytest.MonkeyPatch,
    tool_access: str | None,
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
        query = "" if tool_access is None else "?tool_access=read-only"
        monkeypatch.setenv(
            "CURIE_HOOK_URL", f"{ingress_url}/hooks/acme-agent/alertmanager.release{query}"
        )
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
    delivery = first_headers["X-Curie-Delivery-Id"]
    assert delivery
    # A retry keeps the delivery id and body, so Curie deduplicates it. Each
    # forward is stamped when it is prepared, so the two signatures may differ;
    # each must verify against its own timestamp header.
    assert second_headers["X-Curie-Delivery-Id"] == delivery
    for body, headers in received:
        timestamp = headers["X-Curie-Timestamp"]
        assert timestamp.isascii() and timestamp.isdigit()
        assert headers["X-Curie-Signature-256"] == _expected_signature(
            "hook-secret",
            timestamp,
            delivery,
            body,
            hook="alertmanager.release",
            tool_access=tool_access,
        )


# @spec HOOK-SOURCE-POLICY-6: the protected source mode. The fake ingress below
# authenticates with the API's own verifiers, so the wire bytes are the API's.
_SCOPED = "synthetic-scoped-source-key"
_SHARED = "synthetic-shared-agent-key"
_ALERT = {
    "groupKey": '{}:{alertname="Example"}',
    "status": "firing",
    "alerts": [{"fingerprint": "fp-a", "labels": {"curie_workload": "api"}}],
}
_SUPPORTED: dict[str, object] = {
    "requested_tool_access": "read-only",
    "effective_tool_access": "read-only",
    "source_generation": "3",
    "runtime_id": "00000000-0000-4000-8000-000000000001",
    "runtime_generation": "1",
    "qualification_id": "00000000-0000-4000-8000-000000000002",
    "supported": True,
    "reason": "supported",
}
_RECEIPT: dict[str, object] = {
    "event_id": "hook-event",
    "stream_id": "1-0",
    "duplicate": False,
    "conversation_id": "conversation",
    "tool_access": "read-only",
    "requested_tool_access": "read-only",
    "effective_tool_access": "read-only",
    "source_generation": "3",
    "acceptance_status": "accepted",
}


def _unavailable(reason: str, **members: object) -> dict[str, object]:
    return {
        **_SUPPORTED,
        "runtime_id": None,
        "runtime_generation": None,
        "qualification_id": None,
        "supported": False,
        "reason": reason,
        **members,
    }


class _Api:
    """A fake hook ingress that records requests and verifies like the API."""

    def __init__(
        self,
        support: tuple[int, object] = (200, _SUPPORTED),
        receipt: tuple[int, object] = (200, _RECEIPT),
        tool_access: str | None = "read-only",
    ) -> None:
        self.tool_access = tool_access
        self.support = support
        self.receipt = receipt
        self.requests: list[tuple[str, bytes, dict[str, str]]] = []

    def handler(self) -> type[BaseHTTPRequestHandler]:
        api = self

        class Ingress(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: object) -> None:
                pass

            def do_POST(self) -> None:  # noqa: N802
                body = self.rfile.read(int(self.headers["Content-Length"]))
                headers = dict(self.headers.items())
                api.requests.append((self.path, body, headers))
                probe = self.path.endswith("/support")
                verify = hook_source_signing.verify_support if probe else hook_signing.verify
                authentic = verify(
                    _SCOPED,
                    timestamp=headers.get("X-Curie-Timestamp"),
                    delivery_id=headers.get("X-Curie-Delivery-Id") or "",
                    hook="alertmanager.release",
                    tool_access=api.tool_access,
                    body=body,
                    header=headers.get("X-Curie-Signature-256"),
                )
                status, answer = (api.support if probe else api.receipt) if authentic else (401, {})
                raw = answer if isinstance(answer, bytes) else json.dumps(answer).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        return Ingress


@pytest.fixture
def protected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CURIE_HOOK_SECRET", raising=False)
    monkeypatch.setenv("CURIE_HOOK_SOURCE_MODE", "protected")
    monkeypatch.setenv("CURIE_HOOK_SOURCE_SECRET", _SCOPED)
    monkeypatch.setenv("CURIE_SIGNER_TOKEN", "signer-token")


def _relay(
    monkeypatch: pytest.MonkeyPatch, api: _Api, *, query: str = "?tool_access=read-only"
) -> int:
    with _http_server(api.handler()) as ingress_url:
        monkeypatch.setenv(
            "CURIE_HOOK_URL",
            f"{ingress_url}/hooks/acme-agent/alertmanager%2Erelease{query}",
        )
        with _http_server(signer.Handler) as signer_url:
            request = urllib.request.Request(
                signer_url,
                data=json.dumps(_ALERT).encode(),
                method="POST",
                headers={"Authorization": "Bearer signer-token"},
            )
            try:
                with urllib.request.urlopen(request, timeout=5) as response:
                    return int(response.status)
            except urllib.error.HTTPError as exc:
                return int(exc.code)


def _warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [record.getMessage() for record in caplog.records if record.name == "alert_signer"]


@pytest.mark.usefixtures("protected")
@pytest.mark.parametrize(
    "receipt",
    [
        (200, _RECEIPT),
        (200, {**_RECEIPT, "duplicate": True}),
        (202, {**_RECEIPT, "stream_id": None, "acceptance_status": "preparing"}),
    ],
)
def test_protected_source_probes_support_then_forwards_read_only(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    receipt: tuple[int, object],
) -> None:
    """@spec HOOK-SOURCE-POLICY-6: scoped read-only signature, probe first, receipt checked."""
    api = _Api(receipt=receipt)
    with caplog.at_level(logging.WARNING):
        assert _relay(monkeypatch, api) == 200
    assert _warnings(caplog) == []
    assert [path for path, _body, _headers in api.requests] == [
        "/hooks/acme-agent/alertmanager%2Erelease/support",
        "/hooks/acme-agent/alertmanager%2Erelease?tool_access=read-only",
    ]
    (_, probe_body, probe), (_, body, delivery) = api.requests
    assert probe_body == b'{"tool_access":"read-only"}'
    assert probe["X-Curie-Delivery-Id"] == delivery["X-Curie-Delivery-Id"]
    assert json.loads(body)["curie_partition"] == signer.partition_value(_ALERT["groupKey"])
    # Purpose separation: neither signature authenticates the other route, and
    # the same request under the shared agent key, or without the policy, fails.
    probe_args = {
        "timestamp": probe["X-Curie-Timestamp"],
        "delivery_id": probe["X-Curie-Delivery-Id"],
        "hook": "alertmanager.release",
        "body": probe_body,
        "header": probe["X-Curie-Signature-256"],
    }
    delivery_args = {
        "timestamp": delivery["X-Curie-Timestamp"],
        "delivery_id": delivery["X-Curie-Delivery-Id"],
        "hook": "alertmanager.release",
        "body": body,
        "header": delivery["X-Curie-Signature-256"],
    }
    assert hook_signing.verify(_SCOPED, tool_access="read-only", **delivery_args)
    assert not hook_signing.verify(_SCOPED, tool_access="read-only", **probe_args)
    assert not hook_source_signing.verify_support(
        _SCOPED, tool_access="read-only", **delivery_args
    )
    assert not hook_signing.verify(_SHARED, tool_access="read-only", **delivery_args)
    assert not hook_signing.verify(_SCOPED, tool_access=None, **delivery_args)


@pytest.mark.usefixtures("protected")
@pytest.mark.parametrize(
    ("support", "reason"),
    [
        ((503, _unavailable("source_unconfigured", effective_tool_access="read-only")),
         "support_source_unconfigured"),
        ((503, _unavailable("source_closed")), "support_source_closed"),
        ((503, _unavailable("runtime_unavailable")), "support_runtime_unavailable"),
        ((503, _unavailable("evidence_expired")), "support_evidence_expired"),
        ((503, _unavailable("configuration_unsupported")), "support_configuration_unsupported"),
        ((503, {"detail": "authority_unavailable"}), "support_malformed"),
        ((404, {"detail": "Not Found"}), "support_unavailable"),
        ((200, b"not json"), "support_unavailable"),
        ((200, {**_SUPPORTED, "reason": "probably"}), "support_malformed"),
        ((200, {**_SUPPORTED, "supported": False}), "support_malformed"),
        ((503, {**_SUPPORTED}), "support_malformed"),
        ((200, {**_SUPPORTED, "effective_tool_access": None}), "support_malformed"),
        ((200, {**_SUPPORTED, "runtime_generation": None}), "support_malformed"),
        ((200, {**_SUPPORTED, "source_generation": None}), "support_malformed"),
    ],
)
def test_protected_source_refuses_before_delivery_without_support(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    support: tuple[int, object],
    reason: str,
) -> None:
    """@spec HOOK-SOURCE-POLICY-6: no supported admission, no delivery attempt."""
    api = _Api(support=support)
    with caplog.at_level(logging.WARNING):
        assert _relay(monkeypatch, api) == 502
    assert [path.endswith("/support") for path, _body, _headers in api.requests] == [True]
    assert _warnings(caplog) == [f"protected hook delivery refused (reason={reason})"]


@pytest.mark.usefixtures("protected")
def test_protected_source_with_shared_agent_key_is_refused_before_delivery(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """@spec HOOK-SOURCE-POLICY-6: an unscoped key fails the probe; nothing is delivered."""
    monkeypatch.setenv("CURIE_HOOK_SOURCE_SECRET", _SHARED)
    api = _Api()
    with caplog.at_level(logging.WARNING):
        assert _relay(monkeypatch, api) == 502
    assert len(api.requests) == 1
    assert _warnings(caplog) == ["protected hook delivery refused (reason=support_unauthenticated)"]
    assert _SHARED not in caplog.text


@pytest.mark.usefixtures("protected")
@pytest.mark.parametrize(
    ("receipt", "reason"),
    [
        ({**_RECEIPT, "effective_tool_access": None, "tool_access": None},
         "receipt_not_read_only"),
        ({**_RECEIPT, "tool_access": None}, "receipt_not_read_only"),
        ({**_RECEIPT, "requested_tool_access": None}, "receipt_not_read_only"),
        ({key: value for key, value in _RECEIPT.items() if key != "effective_tool_access"},
         "receipt_not_read_only"),
        ({**_RECEIPT, "source_generation": None}, "receipt_unbound_source"),
        ({**_RECEIPT, "source_generation": "4"}, "receipt_unbound_source"),
        ({**_RECEIPT, "acceptance_status": "pending"}, "receipt_not_admitted"),
        (b"not json", "receipt_malformed"),
    ],
)
def test_protected_source_refuses_a_receipt_without_read_only_admission(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    receipt: object,
    reason: str,
) -> None:
    """@spec HOOK-SOURCE-POLICY-6: every receipt's effective policy is verified."""
    api = _Api(receipt=(200, receipt))
    with caplog.at_level(logging.WARNING):
        assert _relay(monkeypatch, api) == 502
    assert len(api.requests) == 2
    assert _warnings(caplog) == [f"protected hook delivery refused (reason={reason})"]


@pytest.mark.usefixtures("protected")
def test_protected_source_reports_an_ingress_refusal_as_502(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A refused delivery carries no receipt to verify; the source still sees 502."""
    api = _Api(receipt=(503, {"detail": "broker_unavailable"}))
    with caplog.at_level(logging.WARNING):
        assert _relay(monkeypatch, api) == 502
    assert len(api.requests) == 2
    assert _warnings(caplog) == []


@pytest.mark.parametrize(
    ("environment", "query"),
    [
        ({"CURIE_HOOK_SECRET": _SHARED}, "?tool_access=read-only"),
        ({}, ""),
        ({"CURIE_HOOK_SOURCE_SECRET": ""}, "?tool_access=read-only"),
        ({"CURIE_HOOK_SOURCE_MODE": "ordinary"}, "?tool_access=read-only"),
        ({"CURIE_HOOK_SOURCE_MODE": "Protected"}, "?tool_access=read-only"),
    ],
)
@pytest.mark.usefixtures("protected")
def test_invalid_source_configuration_sends_nothing(
    monkeypatch: pytest.MonkeyPatch, environment: dict[str, str], query: str
) -> None:
    """@spec HOOK-SOURCE-POLICY-6: no fallback to the shared key or an optional policy."""
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("CURIE_HOOK_URL", f"http://api.example.com/hooks/a/alertmanager{query}")
    with pytest.raises(ValueError):
        signer.target()
    api = _Api()
    assert _relay(monkeypatch, api, query=query) == 400
    assert api.requests == []


@pytest.mark.parametrize("tool_access", [None, "read-only"])
def test_ordinary_source_never_probes_or_reads_the_receipt(
    monkeypatch: pytest.MonkeyPatch, tool_access: str | None
) -> None:
    """The default mode keeps today's behavior: the shared key and no support probe."""
    monkeypatch.delenv("CURIE_HOOK_SOURCE_MODE", raising=False)
    monkeypatch.delenv("CURIE_HOOK_SOURCE_SECRET", raising=False)
    monkeypatch.setenv("CURIE_HOOK_SECRET", _SCOPED)
    monkeypatch.setenv("CURIE_SIGNER_TOKEN", "signer-token")
    query = "" if tool_access is None else "?tool_access=read-only"
    api = _Api(receipt=(202, {**_RECEIPT, "effective_tool_access": None}), tool_access=tool_access)
    assert _relay(monkeypatch, api, query=query) == 200
    assert [path.endswith("/support") for path, _body, _headers in api.requests] == [False]


@pytest.mark.parametrize(
    "environment",
    [
        {"CURIE_HOOK_SECRET": None},
        {"CURIE_HOOK_SOURCE_SECRET": _SCOPED},
        {"CURIE_HOOK_SOURCE_MODE": "protected"},
        {"CURIE_HOOK_SOURCE_MODE": "protected", "CURIE_HOOK_SOURCE_SECRET": _SCOPED},
    ],
)
def test_signer_refuses_to_start_on_an_incomplete_source_configuration(
    monkeypatch: pytest.MonkeyPatch, environment: dict[str, str | None]
) -> None:
    """A missing or mixed credential fails startup before the port is bound."""
    monkeypatch.setenv("CURIE_HOOK_URL", "http://api.example.com/hooks/a/alertmanager")
    monkeypatch.setenv("CURIE_HOOK_SECRET", _SHARED)
    monkeypatch.delenv("CURIE_HOOK_SOURCE_MODE", raising=False)
    monkeypatch.delenv("CURIE_HOOK_SOURCE_SECRET", raising=False)
    for name, value in environment.items():
        if value is None:
            monkeypatch.delenv(name)
        else:
            monkeypatch.setenv(name, value)
    bound: list[object] = []
    monkeypatch.setattr(signer, "ThreadingHTTPServer", lambda *args: bound.append(args))
    with pytest.raises((KeyError, ValueError)):
        signer.main()
    assert bound == []
