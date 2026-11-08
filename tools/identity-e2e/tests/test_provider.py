"""The scripted test provider, judged by the API's own lookup parser and trust loader.

The provider runs in-process over TLS with the private CA the harness creates.
The client trusts it through the API's ``load_provider_tls_context`` (hostname
verification on), and every answer is validated by the API's
``validate_lookup`` against a request shaped from the committed vectors, so a
provider that drifts from the recorded wire fails here, not in a cluster.

The plain-HTTP sink listener also serves the current-authority endpoint a
human-restricted connector asks before each effect (ADR 0270 decision 5), and
the adapter-secret admin endpoints that revoke membership or take the
authority down while a scenario runs.
"""

from __future__ import annotations

import contextlib
import copy
import http.client
import json
import os
import socket
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
AUTHORITY_BEARER = "synthetic-authority-bearer"
TENANT = "5b1f0c9e-3d2a-4e8b-9f17-2c6a0d4e8b31"
OTHER_TENANT = "0d6e2a4c-8b1f-4c3d-9e5a-7f2b1c0d9e8a"
LIMITS = FreshnessLimits(
    max_identity_age_ms=60_000, max_membership_age_ms=60_000, max_clock_skew_ms=5_000
)


@contextlib.contextmanager
def serving(directory: Path, **overrides: Any) -> Iterator[SimpleNamespace]:
    """The provider on loopback ports, trusted only through the harness CA."""

    directory.mkdir(parents=True, exist_ok=True)
    assets.make_certificates(directory, ["localhost"])
    trust = directory / "trust"
    trust.mkdir()
    bundle = trust / "provider-ca.pem"
    bundle.write_bytes((directory / "ca.pem").read_bytes())
    os.chmod(bundle, 0o440)
    client_tls = load_provider_tls_context(str(trust))
    assert client_tls is not None
    options: dict[str, Any] = {
        # A copy: the admin endpoints change membership, and the roster is shared.
        "roster": copy.deepcopy(assets.DEFAULT_ROSTER),
        "vectors": VECTORS,
        "bearer": BEARER,
        "adapter_secret": ADAPTER_SECRET,
        "authority_bearer": AUTHORITY_BEARER,
        "tenant": TENANT,
        **overrides,
    }
    state = fake_idp.Provider(**options)
    lookup, sink = fake_idp.serve(
        state,
        host="127.0.0.1",
        lookup_port=0,
        sink_port=0,
        tls=fake_idp.server_tls(str(directory / "server.pem"), str(directory / "server.key")),
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


@pytest.fixture
def provider(tmp_path: Path) -> Iterator[SimpleNamespace]:
    with serving(tmp_path) as running:
        yield running


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


# --- the current-authority endpoint (ADR 0270 decision 5) ----------------------------

ALLOW = (200, {"decision": "allow"})
DENY = (200, {"decision": "deny"})


def principal(native_id: str, **overrides: str) -> dict[str, str]:
    subject = assets.DEFAULT_ROSTER["senders"][native_id]["subject"]
    return {"tenant_id": TENANT, "issuer": assets.ISSUER, "subject": subject, **overrides}


def sink_post(port: int, path: str, body: Any, headers: dict[str, str]) -> tuple[int, bytes]:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        connection.request(
            "POST",
            path,
            body=json.dumps(body).encode(),
            headers={"Content-Type": "application/json", **headers},
        )
        response = connection.getresponse()
        return response.status, response.read()
    finally:
        connection.close()


def decide(
    port: int, body: Any, authorization: str | None = "Bearer " + AUTHORITY_BEARER
) -> tuple[int, Any]:
    """``(status, parsed body)`` for a 200, ``(status, None)`` otherwise."""

    headers = {} if authorization is None else {"Authorization": authorization}
    status, raw = sink_post(port, "/authority", body, headers)
    return status, (json.loads(raw) if status == 200 else None)


def admin(port: int, path: str, body: Any, secret: str | None = ADAPTER_SECRET) -> int:
    headers = {} if secret is None else {"X-Curie-Adapter-Secret": secret}
    return sink_post(port, path, body, headers)[0]


def records_text(port: int) -> str:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        connection.request("GET", "/records")
        response = connection.getresponse()
        assert response.status == 200
        return json.dumps(json.loads(response.read())["records"])
    finally:
        connection.close()


@pytest.mark.parametrize("native_id", ["alice@example.test", "bob@example.test"])
def test_the_authority_allows_an_active_human_member_of_the_group(
    provider: SimpleNamespace, native_id: str
) -> None:
    assert decide(provider.sink_port, principal(native_id)) == ALLOW


DENIED: dict[str, dict[str, str]] = {
    "another tenant": principal("alice@example.test", tenant_id=OTHER_TENANT),
    "another issuer": principal(
        "alice@example.test", issuer="https://other.example.test/directory"
    ),
    "an unknown subject": principal("alice@example.test", subject="identity-e2e-mallory"),
    "a subject compared exactly": principal("alice@example.test", subject="IDENTITY-E2E-ALICE"),
    "a service account": principal("robot@example.test"),
    "a disabled human": principal("carol@example.test"),
    "an active human outside the group": principal("dana@example.test"),
}


@pytest.mark.parametrize("body", DENIED.values(), ids=list(DENIED))
def test_the_authority_denies_a_principal_missing_any_fact(
    provider: SimpleNamespace, body: dict[str, str]
) -> None:
    assert decide(provider.sink_port, body) == DENY
    assert decide(provider.sink_port, principal("alice@example.test")) == ALLOW


REFUSED_AUTHORIZATION: dict[str, str | None] = {
    "no header": None,
    "a wrong bearer": "Bearer synthetic-wrong-bearer",
    "the lookup bearer": "Bearer " + BEARER,
    "the adapter secret": "Bearer " + ADAPTER_SECRET,
    "the bearer without its scheme": AUTHORITY_BEARER,
}


@pytest.mark.parametrize(
    "authorization", REFUSED_AUTHORIZATION.values(), ids=list(REFUSED_AUTHORIZATION)
)
def test_the_authority_requires_its_own_bearer(
    provider: SimpleNamespace, authorization: str | None
) -> None:
    headers = {} if authorization is None else {"Authorization": authorization}
    status, raw = sink_post(
        provider.sink_port, "/authority", principal("alice@example.test"), headers
    )
    assert status == 401
    assert b"allow" not in raw and b"deny" not in raw
    assert decide(provider.sink_port, principal("alice@example.test")) == ALLOW


def test_revoked_membership_denies_until_restored_and_the_change_is_recorded(
    provider: SimpleNamespace,
) -> None:
    port = provider.sink_port
    alice = principal("alice@example.test")
    change = {"sender": "alice@example.test", "group": assets.MEMBER_GROUP}
    assert decide(port, alice) == ALLOW
    assert admin(port, "/admin/membership", {**change, "member": False}) == 200
    assert decide(port, alice) == DENY
    # Only the named sender changed.
    assert decide(port, principal("bob@example.test")) == ALLOW
    assert admin(port, "/admin/membership", {**change, "member": True}) == 200
    assert decide(port, alice) == ALLOW

    records = json.loads(records_text(port))
    changes = [
        json.dumps(record)
        for record in records
        if "alice@example.test" in json.dumps(record) and assets.MEMBER_GROUP in json.dumps(record)
    ]
    assert any("false" in text for text in changes), records
    assert any("true" in text for text in changes), records
    text = json.dumps(records)
    for secret in (BEARER, ADAPTER_SECRET, AUTHORITY_BEARER):
        assert secret not in text


def test_an_unavailable_authority_answers_503_until_it_is_available(
    provider: SimpleNamespace,
) -> None:
    port = provider.sink_port
    alice = principal("alice@example.test")
    assert admin(port, "/admin/authority", {"available": False}) == 200
    bearer = {"Authorization": "Bearer " + AUTHORITY_BEARER}
    status, raw = sink_post(port, "/authority", alice, bearer)
    assert status == 503
    assert b"allow" not in raw
    assert admin(port, "/admin/authority", {"available": True}) == 200
    assert decide(port, alice) == ALLOW

    records = json.loads(records_text(port))
    availability = [json.dumps(r) for r in records if "available" in json.dumps(r)]
    assert any("false" in text for text in availability), records
    assert any("true" in text for text in availability), records


ADMIN_CHANGES: dict[str, tuple[str, dict[str, Any], tuple[int, Any]]] = {
    "membership": (
        "/admin/membership",
        {"sender": "alice@example.test", "group": assets.MEMBER_GROUP, "member": False},
        DENY,
    ),
    "authority": ("/admin/authority", {"available": False}, (503, None)),
}
REFUSED_SECRETS: dict[str, str | None] = {
    "no header": None,
    "a wrong secret": "synthetic-wrong-secret",
    "the authority bearer": AUTHORITY_BEARER,
}


@pytest.mark.parametrize("secret", REFUSED_SECRETS.values(), ids=list(REFUSED_SECRETS))
@pytest.mark.parametrize("change", list(ADMIN_CHANGES))
def test_an_admin_change_requires_the_adapter_secret(
    provider: SimpleNamespace, change: str, secret: str | None
) -> None:
    port = provider.sink_port
    path, body, effect = ADMIN_CHANGES[change]
    alice = principal("alice@example.test")
    assert admin(port, path, body, secret=secret) == 401
    assert decide(port, alice) == ALLOW
    assert admin(port, path, body) == 200
    assert decide(port, alice) == effect
    text = records_text(port)
    for leaked in ("synthetic-wrong-secret", ADAPTER_SECRET, AUTHORITY_BEARER, BEARER):
        assert leaked not in text


def test_the_authority_group_is_configurable_and_membership_can_be_granted(
    tmp_path: Path,
) -> None:
    with serving(tmp_path, authority_group="identity-e2e-approvers") as running:
        port = running.sink_port
        dana = principal("dana@example.test")
        assert decide(port, principal("alice@example.test")) == DENY
        assert decide(port, dana) == DENY
        grant = {"sender": "dana@example.test", "group": "identity-e2e-approvers", "member": True}
        assert admin(port, "/admin/membership", grant) == 200
        assert decide(port, dana) == ALLOW
        assert decide(port, principal("alice@example.test")) == DENY


def free_ports(count: int) -> list[int]:
    """Distinct loopback ports, held together so no two are the same."""

    probes = [socket.socket() for _ in range(count)]
    try:
        for probe in probes:
            probe.bind(("127.0.0.1", 0))
        return [int(probe.getsockname()[1]) for probe in probes]
    finally:
        for probe in probes:
            probe.close()


def test_the_provider_main_serves_the_authority_from_its_flags(tmp_path: Path) -> None:
    """``--authority-bearer-file`` and ``--tenant``; the group defaults to the member group."""

    assets.make_certificates(tmp_path, ["localhost"])
    files = {
        "roster.json": json.dumps(assets.DEFAULT_ROSTER),
        "vectors.json": assets.VECTORS.read_text(),
        "bearer": BEARER,
        "adapter": ADAPTER_SECRET,
        "authority-bearer": AUTHORITY_BEARER + "\n",
    }
    for name, text in files.items():
        (tmp_path / name).write_text(text)
    sink_port, lookup_port = free_ports(2)
    answers: list[tuple[int, Any]] = []

    def wait() -> None:
        answers.append(decide(sink_port, principal("alice@example.test")))
        answers.append(decide(sink_port, principal("dana@example.test")))
        answers.append(decide(sink_port, principal("alice@example.test"), "Bearer " + BEARER))

    argv = [
        "--roster", str(tmp_path / "roster.json"), "--vectors", str(tmp_path / "vectors.json"),
        "--bearer-file", str(tmp_path / "bearer"),
        "--adapter-secret-file", str(tmp_path / "adapter"),
        "--authority-bearer-file", str(tmp_path / "authority-bearer"),
        "--tenant", TENANT,
        "--cert", str(tmp_path / "server.pem"), "--key", str(tmp_path / "server.key"),
        "--host", "127.0.0.1", "--lookup-port", str(lookup_port), "--sink-port", str(sink_port),
    ]  # fmt: skip
    assert fake_idp.main(argv, wait=wait) == 0
    assert answers == [ALLOW, DENY, (401, None)]
