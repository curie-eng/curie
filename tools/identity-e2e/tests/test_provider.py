"""The scripted test provider, judged by the API's own lookup parser and trust loader.

The provider runs in-process over TLS with the private CA the harness creates.
The client trusts it through the API's ``load_provider_tls_context`` (hostname
verification on), and every answer is validated by the API's
``validate_lookup`` against a request shaped from the committed vectors, so a
provider that drifts from the recorded wire fails here, not in a cluster.
"""

from __future__ import annotations

import copy
import http.client
import json
import os
import ssl
import threading
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import fake_idp
import identity_assets as assets
import pytest
from curie_api.identity.http_initializer import load_provider_tls_context
from curie_api.schemas.http_identity import (
    FactsRefusal,
    FreshnessLimits,
    LookupRequestV1,
    ValidatedFacts,
    validate_lookup,
)

VECTORS = json.loads(assets.VECTORS.read_text())
BEARER = "synthetic-provider-bearer"
ADAPTER_SECRET = "synthetic-adapter-secret"
LIMITS = FreshnessLimits(
    max_identity_age_ms=60_000, max_membership_age_ms=60_000, max_clock_skew_ms=5_000
)


@pytest.fixture
def provider(tmp_path: Path) -> Iterator[SimpleNamespace]:
    """The provider on loopback ports, trusted only through the harness CA."""

    assets.make_certificates(tmp_path, ["localhost"])
    trust = tmp_path / "trust"
    trust.mkdir()
    bundle = trust / "provider-ca.pem"
    bundle.write_bytes((tmp_path / "ca.pem").read_bytes())
    os.chmod(bundle, 0o440)
    client_tls = load_provider_tls_context(str(trust))
    assert client_tls is not None
    state = fake_idp.Provider(
        roster=assets.DEFAULT_ROSTER,
        vectors=VECTORS,
        bearer=BEARER,
        adapter_secret=ADAPTER_SECRET,
    )
    lookup, sink = fake_idp.serve(
        state,
        host="127.0.0.1",
        lookup_port=0,
        sink_port=0,
        tls=fake_idp.server_tls(str(tmp_path / "server.pem"), str(tmp_path / "server.key")),
    )
    try:
        yield SimpleNamespace(
            state=state,
            tls=client_tls,
            lookup_port=lookup.server_address[1],
            sink_port=sink.server_address[1],
        )
    finally:
        for server in (lookup, sink):
            server.shutdown()
            server.server_close()


def lookup_request(native_id: str) -> LookupRequestV1:
    request = copy.deepcopy(VECTORS["request"])
    request["request_id"] = str(uuid.uuid4())
    request["scope"]["expected_issuer"] = assets.ISSUER
    request["scope"]["sender"].update(provider="email", native_id=native_id)
    request["scope"]["sender"]["namespace"].update(kind="email_domain", key=assets.DOMAIN)
    return LookupRequestV1.model_validate(request)


def post_lookup(
    provider: SimpleNamespace, request: LookupRequestV1, bearer: str = BEARER
) -> tuple[int, bytes]:
    connection = http.client.HTTPSConnection(
        "localhost", provider.lookup_port, context=provider.tls, timeout=10
    )
    try:
        connection.request(
            "POST",
            "/lookup",
            body=request.model_dump_json().encode(),
            headers={"Authorization": "Bearer " + bearer, "Content-Type": "application/json"},
        )
        response = connection.getresponse()
        return response.status, response.read()
    finally:
        connection.close()


def facts(provider: SimpleNamespace, native_id: str) -> ValidatedFacts | FactsRefusal:
    request = lookup_request(native_id)
    status, body = post_lookup(provider, request)
    assert status == 200, body
    return validate_lookup(body, request, LIMITS, now=datetime.now(UTC))


def test_a_rostered_human_resolves_through_the_api_parser(provider: SimpleNamespace) -> None:
    result = facts(provider, "alice@example.test")
    assert isinstance(result, ValidatedFacts), result
    assert result.outcome == "resolved"
    assert result.sender_kind == "human"
    assert result.identity is not None
    assert (result.identity.issuer, result.identity.subject) == (
        assets.ISSUER,
        "identity-e2e-alice",
    )
    assert result.identity.account_status == "active"
    assert result.groups is not None
    assert {(group.issuer, group.id) for group in result.groups} == {
        (assets.ISSUER, assets.MEMBER_GROUP)
    }


@pytest.mark.parametrize(
    ("native_id", "outcome", "sender_kind"),
    [
        ("carol@example.test", "inactive", "human"),
        ("robot@example.test", "resolved", "service"),
        ("nobody@example.test", "unresolved", "unknown"),
    ],
)
def test_scripted_controls_answer_their_recorded_outcomes(
    provider: SimpleNamespace, native_id: str, outcome: str, sender_kind: str
) -> None:
    result = facts(provider, native_id)
    assert isinstance(result, ValidatedFacts), result
    assert (result.outcome, result.sender_kind) == (outcome, sender_kind)


def test_a_wrong_bearer_is_refused_without_facts(provider: SimpleNamespace) -> None:
    status, body = post_lookup(provider, lookup_request("alice@example.test"), bearer="wrong")
    assert status == 401
    assert b"identity-e2e-alice" not in body
    assert provider.state.records()[-1] == {
        "type": "lookup",
        "status": 401,
        "at": provider.state.records()[-1]["at"],
    }


def test_an_untrusted_client_cannot_reach_the_lookup(provider: SimpleNamespace) -> None:
    default = ssl.create_default_context()
    connection = http.client.HTTPSConnection(
        "localhost", provider.lookup_port, context=default, timeout=10
    )
    with pytest.raises(ssl.SSLCertVerificationError):
        connection.request("POST", "/lookup", body=b"{}")
    connection.close()


def sink(provider: SimpleNamespace, method: str, path: str, **kwargs: Any) -> tuple[int, Any]:
    connection = http.client.HTTPConnection("127.0.0.1", provider.sink_port, timeout=10)
    try:
        connection.request(method, path, **kwargs)
        response = connection.getresponse()
        return response.status, json.loads(response.read())
    finally:
        connection.close()


def test_the_reply_sink_requires_the_adapter_secret_and_records_no_secret(
    provider: SimpleNamespace,
) -> None:
    event = {"event": "turn.completed", "target": {"conversation_id": "c1"}}
    raw = json.dumps(event).encode()
    refused, _ = sink(
        provider,
        "POST",
        "/reply/agent-a",
        body=raw,
        headers={"X-Curie-Adapter-Secret": "wrong", "Content-Type": "application/json"},
    )
    accepted, _ = sink(
        provider,
        "POST",
        "/reply/agent-a",
        body=raw,
        headers={"X-Curie-Adapter-Secret": ADAPTER_SECRET, "Content-Type": "application/json"},
    )
    status, records = sink(provider, "GET", "/records")
    assert (refused, accepted, status) == (401, 200, 200)
    replies = [r for r in records["records"] if r["type"] == "reply"]
    assert [(r["label"], r["status"]) for r in replies] == [("agent-a", 401), ("agent-a", 200)]
    assert replies[-1]["event"] == event
    text = json.dumps(records)
    assert ADAPTER_SECRET not in text and BEARER not in text


def test_the_provider_main_serves_from_its_files(tmp_path: Path) -> None:
    """The in-cluster entry point: files in, both listeners up, clean shutdown."""

    assets.make_certificates(tmp_path, ["localhost"])
    files = {
        "roster.json": json.dumps(assets.DEFAULT_ROSTER),
        "vectors.json": assets.VECTORS.read_text(),
        "bearer": BEARER,
        "adapter": ADAPTER_SECRET,
    }
    for name, text in files.items():
        (tmp_path / name).write_text(text)
    ready, stop = threading.Event(), threading.Event()

    def wait() -> None:
        ready.set()
        stop.wait(10)

    argv = [
        "--roster", str(tmp_path / "roster.json"), "--vectors", str(tmp_path / "vectors.json"),
        "--bearer-file", str(tmp_path / "bearer"),
        "--adapter-secret-file", str(tmp_path / "adapter"),
        "--cert", str(tmp_path / "server.pem"), "--key", str(tmp_path / "server.key"),
        "--host", "127.0.0.1", "--lookup-port", "0", "--sink-port", "0",
    ]  # fmt: skip
    result: list[int] = []
    thread = threading.Thread(target=lambda: result.append(fake_idp.main(argv, wait=wait)))
    thread.start()
    assert ready.wait(10)
    stop.set()
    thread.join(10)
    assert result == [0]
