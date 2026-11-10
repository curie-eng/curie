"""The reference human-restricted hosted connector (ADR 0270 decision 5), over real HTTP.

The connector trusts only the proxy-set ``X-Curie-Principal`` header and asks a
current-authority endpoint before every effect. Authority answers come from the
scripted provider's real ``POST /authority`` wherever the test needs a real
directory decision, and from a local stub only where the authority itself must
misbehave (an error status, a bad body, a hang, a closed port) or where the
test must see exactly what the connector sent.

Every refusal is paired with an allowed call on the same connector, so a
refusal cannot pass because the connector or its authority is simply down, and
every refusal checks the ledger stayed empty.
"""

from __future__ import annotations

import ast
import asyncio
import base64
import copy
import hashlib
import http.client
import json
import re
import secrets
import socket
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import fake_idp
import human_restricted_connector as hrc
import identity_assets as assets
import pytest
from curie_internal import approver_ticket
from identity_target import REPO
from mcp import ClientSession, types
from mcp.client.streamable_http import create_mcp_http_client, streamable_http_client
from nacl.signing import SigningKey

CORPUS = json.loads((REPO / "tests" / "vectors" / "connector-caller-token.json").read_text())
VECTORS = json.loads(assets.VECTORS.read_text())
PRINCIPAL_HEADER = "X-Curie-Principal"
TENANT = "5b1f0c9e-3d2a-4e8b-9f17-2c6a0d4e8b31"
OTHER_TENANT = "0d6e2a4c-8b1f-4c3d-9e5a-7f2b1c0d9e8a"
AUTHORITY_BEARER = "synthetic-authority-bearer"
LOOKUP_BEARER = "synthetic-provider-bearer"
ADAPTER_SECRET = "synthetic-adapter-secret"


def principal(native_id: str, **overrides: str) -> dict[str, str]:
    """The canonical principal the proxy would attest for a roster sender."""

    subject = assets.DEFAULT_ROSTER["senders"][native_id]["subject"]
    return {"tenant_id": TENANT, "issuer": assets.ISSUER, "subject": subject, **overrides}


def b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def encode(value: Any) -> str:
    """The proxy's encoding (ADR 0270 decision 4): compact sorted-key JSON, unpadded base64url."""

    text = json.dumps(value, separators=(",", ":"), sort_keys=True, ensure_ascii=False)
    return b64url(text.encode("utf-8"))


def fingerprint(header: str) -> str:
    return hashlib.sha256(header.encode("ascii")).hexdigest()[:16]


ALICE = encode(principal("alice@example.test"))
BOB = encode(principal("bob@example.test"))
DANA = encode(principal("dana@example.test"))
SUBJECTS = [entry["subject"] for entry in assets.DEFAULT_ROSTER["senders"].values()]
ALICE_SUBJECT = principal("alice@example.test")["subject"]


def ok(n: int, header: str) -> tuple[bool, str]:
    return False, f"effect={n} requester={fingerprint(header)}"


def refused(code: str) -> tuple[bool, str]:
    return True, f"refused {code}"


def impersonating(native_id: str) -> dict[str, str]:
    """Model-written arguments that name another person in every spelling."""

    named = principal(native_id)
    return {
        "note": "synthetic effect",
        "subject": named["subject"],
        "as_subject": named["subject"],
        "tenant_id": named["tenant_id"],
        "issuer": named["issuer"],
    }


def corpus_cases(section: str) -> list[dict[str, Any]]:
    return list(CORPUS.get(section, {}).get("cases", []))


def sendable(value: str) -> bool:
    """Whether an HTTP client can put ``value`` on the wire unchanged as one header.

    Values past 8 KiB are left to the reader test: an HTTP layer may refuse an
    oversized header before the tool runs, which is a refusal too.
    """

    return (
        0 < len(value) <= 8192
        and value.isascii()
        and value.isprintable()
        and value == value.strip()
    )


# --- HTTP plumbing -------------------------------------------------------------------


def port_of(server: ThreadingHTTPServer) -> int:
    return int(server.server_address[1])


def exchange(
    connection: http.client.HTTPConnection,
    method: str,
    path: str,
    body: bytes | None,
    headers: Sequence[tuple[str, str]] = (),
) -> SimpleNamespace:
    """One request with headers sent exactly as given, repeats and spelling kept."""

    connection.putrequest(method, path)
    sent = [("Accept", "application/json, text/event-stream"), *headers]
    if body is not None:
        sent += [("Content-Type", "application/json"), ("Content-Length", str(len(body)))]
    for name, value in sent:
        connection.putheader(name, value)
    connection.endheaders(body)
    response = connection.getresponse()
    return SimpleNamespace(status=response.status, headers=response.headers, body=response.read())


def send(
    port: int,
    message: dict[str, Any],
    headers: Sequence[tuple[str, str]] = (),
    *,
    connection: http.client.HTTPConnection | None = None,
) -> SimpleNamespace:
    own = connection is None
    connection = connection or http.client.HTTPConnection("127.0.0.1", port, timeout=15)
    try:
        return exchange(connection, "POST", "/mcp", json.dumps(message).encode(), headers)
    finally:
        if own:
            connection.close()


def rpc(
    port: int,
    method: str,
    params: dict[str, Any] | None = None,
    *,
    headers: Sequence[tuple[str, str]] = (),
    request_id: int = 1,
    connection: http.client.HTTPConnection | None = None,
) -> dict[str, Any]:
    message: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
    if params is not None:
        message["params"] = params
    reply = send(port, message, headers, connection=connection)
    assert reply.status == 200, reply.body
    assert (reply.headers.get("Content-Type") or "").startswith("application/json")
    answer = json.loads(reply.body)
    assert (answer["jsonrpc"], answer["id"]) == ("2.0", request_id)
    return answer


def record_effect(
    server: ThreadingHTTPServer,
    *principals: str,
    headers: Sequence[tuple[str, str]] | None = None,
    arguments: dict[str, Any] | None = None,
    connection: http.client.HTTPConnection | None = None,
    tool: str = "record_effect",
) -> tuple[bool, str]:
    """One ``tools/call`` of ``tool``, ``record_effect`` by default; ``(isError, text)``."""

    sent = list(headers) if headers is not None else [(PRINCIPAL_HEADER, p) for p in principals]
    params = {
        "name": tool,
        "arguments": arguments if arguments is not None else {"note": "synthetic effect"},
    }
    answer = rpc(port_of(server), "tools/call", params, headers=sent, connection=connection)
    assert "result" in answer, answer
    result = answer["result"]
    assert [item["type"] for item in result["content"]] == ["text"], result
    return result.get("isError", False), result["content"][0]["text"]


def ledger(server: ThreadingHTTPServer) -> list[tuple[int, str]]:
    return [(entry["n"], entry["requester"]) for entry in list(server.ledger)]  # type: ignore[attr-defined]


# --- fixtures ------------------------------------------------------------------------


@pytest.fixture(scope="module")
def provider_tls(tmp_path_factory: pytest.TempPathFactory) -> Path:
    directory = tmp_path_factory.mktemp("provider-tls")
    assets.make_certificates(directory, ["localhost"])
    return directory


@pytest.fixture
def idp(provider_tls: Path) -> Iterator[SimpleNamespace]:
    """The scripted provider; its plain-HTTP sink listener serves ``/authority``."""

    state = fake_idp.Provider(
        roster=copy.deepcopy(assets.DEFAULT_ROSTER),
        vectors=VECTORS,
        bearer=LOOKUP_BEARER,
        adapter_secret=ADAPTER_SECRET,
        authority_bearer=AUTHORITY_BEARER,
        tenant=TENANT,
    )
    tls = fake_idp.server_tls(str(provider_tls / "server.pem"), str(provider_tls / "server.key"))
    lookup, sink = fake_idp.serve(state, host="127.0.0.1", lookup_port=0, sink_port=0, tls=tls)
    port = sink.server_address[1]
    try:
        yield SimpleNamespace(state=state, port=port, url=f"http://127.0.0.1:{port}")
    finally:
        for server in (lookup, sink):
            server.shutdown()
            server.server_close()


def admin(idp: SimpleNamespace, path: str, body: dict[str, Any]) -> None:
    connection = http.client.HTTPConnection("127.0.0.1", idp.port, timeout=10)
    try:
        reply = exchange(
            connection,
            "POST",
            path,
            json.dumps(body).encode(),
            [("X-Curie-Adapter-Secret", ADAPTER_SECRET)],
        )
    finally:
        connection.close()
    assert reply.status == 200, reply.body


class StubAuthority:
    """An authority that misbehaves on demand and records what the connector sent."""

    ANSWERS: dict[str, tuple[int, bytes]] = {
        "allow": (200, b'{"decision":"allow"}'),
        "deny": (200, b'{"decision":"deny"}'),
        "error_with_an_allow_body": (500, b'{"decision":"allow"}'),
        "created_with_an_allow_body": (201, b'{"decision":"allow"}'),
        "not_json": (200, b"allow"),
        "empty_body": (200, b""),
        "not_an_object": (200, b'["allow"]'),
        "an_unknown_decision": (200, b'{"decision":"maybe"}'),
        "a_decision_that_is_not_a_string": (200, b'{"decision":true}'),
    }

    def __init__(self) -> None:
        self.mode = "allow"
        self.requests: list[dict[str, Any]] = []
        self.barrier: threading.Barrier | None = None
        self._closed = False
        stub = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
                pass

            def do_POST(self) -> None:  # noqa: N802
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                stub.requests.append(
                    {
                        "path": self.path,
                        "authorization": self.headers.get("Authorization"),
                        "body": raw,
                    }
                )
                # One request per connection, so closing the stub closes the authority.
                self.close_connection = True
                if stub.barrier is not None:
                    stub.barrier.wait(timeout=5)
                if stub.mode == "hang":
                    # Never answer. The read returns once the caller gives up and closes.
                    self.rfile.read(1)
                    return
                if stub.mode == "by_subject":
                    allowed = json.loads(raw)["subject"] == ALICE_SUBJECT
                    status, body = StubAuthority.ANSWERS["allow" if allowed else "deny"]
                else:
                    status, body = StubAuthority.ANSWERS[stub.mode]
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(body)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(
            target=self.server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
        ).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self.server.shutdown()
            self.server.server_close()


@pytest.fixture
def stub() -> Iterator[StubAuthority]:
    authority = StubAuthority()
    try:
        yield authority
    finally:
        authority.close()


@pytest.fixture
def connectors() -> Iterator[Callable[..., ThreadingHTTPServer]]:
    """Start connectors on loopback ports; every one is shut down afterwards."""

    started: list[ThreadingHTTPServer] = []

    def start(
        authority_url: str,
        *,
        bearer: str = AUTHORITY_BEARER,
        timeout_s: float = 2.0,
        ticket: dict[str, str] | None = None,
    ) -> ThreadingHTTPServer:
        server = hrc.make_server(
            "127.0.0.1",
            0,
            authority_url=authority_url,
            authority_bearer=bearer,
            timeout_s=timeout_s,
            **(ticket or {}),
        )
        assert isinstance(server, ThreadingHTTPServer)
        threading.Thread(
            target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
        ).start()
        started.append(server)
        return server

    try:
        yield start
    finally:
        for server in started:
            server.shutdown()
            server.server_close()


# --- the header reader over the shared corpus ----------------------------------------


def test_the_shared_corpus_carries_both_principal_header_sections() -> None:
    accepted = corpus_cases("principal_headers")
    malformed = corpus_cases("malformed_principal_headers")
    assert accepted, "tests/vectors/connector-caller-token.json has no principal_headers cases"
    assert malformed, "tests/vectors/connector-caller-token.json has no malformed_principal_headers"
    names = [case["name"] for case in accepted + malformed]
    assert len(names) == len(set(names))
    for case in accepted:
        assert set(case) >= {"name", "why", "principal", "header"}
        assert set(case["principal"]) == {"tenant_id", "issuer", "subject"}
    for case in malformed:
        assert set(case) >= {"name", "why", "value"}


@pytest.mark.parametrize("case", corpus_cases("principal_headers"), ids=lambda case: case["name"])
def test_a_corpus_principal_header_reads_as_its_principal(case: dict[str, Any]) -> None:
    assert hrc.read_principal_header(case["header"]) == case["principal"]


@pytest.mark.parametrize(
    "case", corpus_cases("malformed_principal_headers"), ids=lambda case: case["name"]
)
def test_a_corpus_malformed_principal_header_is_refused(case: dict[str, Any]) -> None:
    assert hrc.read_principal_header(case["value"]) is None


def test_the_headers_these_tests_send_are_the_proxy_encoding() -> None:
    """The synthetic roster headers below are spelled exactly as the proxy spells one."""

    cases = corpus_cases("principal_headers")
    assert cases
    for case in cases:
        assert encode(case["principal"]) == case["header"], case["name"]
    assert hrc.read_principal_header(ALICE) == principal("alice@example.test")


# --- the MCP surface -----------------------------------------------------------------


@pytest.mark.parametrize("version", ["2025-06-18", "2025-11-25"])
def test_initialize_echoes_the_client_protocol_and_declares_tools(
    connectors: Callable[..., ThreadingHTTPServer], stub: StubAuthority, version: str
) -> None:
    server = connectors(stub.url)
    params = {
        "protocolVersion": version,
        "capabilities": {},
        "clientInfo": {"name": "identity-e2e-test", "version": "0"},
    }
    result = rpc(port_of(server), "initialize", params)["result"]
    assert result["protocolVersion"] == version
    assert result["capabilities"] == {"tools": {}}
    assert isinstance(result["serverInfo"]["name"], str) and result["serverInfo"]["name"]
    assert isinstance(result["serverInfo"]["version"], str)


def test_the_initialized_notification_is_accepted_with_an_empty_body(
    connectors: Callable[..., ThreadingHTTPServer], stub: StubAuthority
) -> None:
    server = connectors(stub.url)
    reply = send(port_of(server), {"jsonrpc": "2.0", "method": "notifications/initialized"})
    assert (reply.status, reply.body) == (202, b"")


def test_ping_answers_an_empty_result(
    connectors: Callable[..., ThreadingHTTPServer], stub: StubAuthority
) -> None:
    server = connectors(stub.url)
    assert rpc(port_of(server), "ping", request_id=4)["result"] == {}


def test_tools_list_offers_record_effect_and_the_two_protected_execution_tools(
    connectors: Callable[..., ThreadingHTTPServer], stub: StubAuthority
) -> None:
    server = connectors(stub.url)
    tools = rpc(port_of(server), "tools/list")["result"]["tools"]
    assert [tool["name"] for tool in tools] == [
        "record_effect",
        "execute_attested",
        "execute_approved",
    ]
    for tool in tools:
        assert tool["inputSchema"]["type"] == "object", tool["name"]
    schema = tools[0]["inputSchema"]
    assert schema["properties"]["note"]["type"] == "string"


def test_get_on_mcp_is_method_not_allowed(
    connectors: Callable[..., ThreadingHTTPServer], stub: StubAuthority
) -> None:
    server = connectors(stub.url)
    connection = http.client.HTTPConnection("127.0.0.1", port_of(server), timeout=15)
    try:
        assert exchange(connection, "GET", "/mcp", None).status == 405
    finally:
        connection.close()


def test_an_unknown_method_is_a_json_rpc_method_not_found(
    connectors: Callable[..., ThreadingHTTPServer], stub: StubAuthority
) -> None:
    server = connectors(stub.url)
    reply = send(port_of(server), {"jsonrpc": "2.0", "id": 7, "method": "resources/list"})
    assert (reply.headers.get("Content-Type") or "").startswith("application/json")
    answer = json.loads(reply.body)
    assert answer["id"] == 7
    assert answer["error"]["code"] == -32601
    assert "result" not in answer


# --- record_effect against the scripted directory ------------------------------------


def test_an_allowed_human_applies_one_effect_each_call_attributed_to_the_header(
    connectors: Callable[..., ThreadingHTTPServer], idp: SimpleNamespace
) -> None:
    server = connectors(idp.url)
    assert record_effect(server, ALICE) == ok(1, ALICE)
    assert record_effect(server, BOB) == ok(2, BOB)
    assert ledger(server) == [(1, fingerprint(ALICE)), (2, fingerprint(BOB))]


@pytest.mark.parametrize("host", ["127.0.0.1", "127.0.0.53", "::1", "::ffff:127.0.0.1"])
def test_a_loopback_peer_is_the_caller_proxys_forward(host: str) -> None:
    assert hrc.from_caller_proxy(host)


@pytest.mark.parametrize(
    "host",
    ["10.0.0.7", "192.168.1.20", "::ffff:10.0.0.7", "2001:db8::7", "fe80::1%eth0", "", "pod"],
)
def test_any_other_peer_is_not_the_caller_proxy(host: str) -> None:
    assert not hrc.from_caller_proxy(host)


def report_peer_as(server: ThreadingHTTPServer, host: str) -> None:
    """Every accepted connection reports ``host`` as its peer, as a pod-network caller would.

    The connector's ``-direct`` Service reaches it on the pod address, not on
    the caller proxy's loopback forward.
    """

    accept = server.get_request

    def get_request() -> tuple[Any, Any]:
        connection, address = accept()
        return connection, (host, address[1])

    server.get_request = get_request  # type: ignore[method-assign]


def test_a_direct_caller_cannot_establish_a_human_with_the_header(
    stub: StubAuthority, connectors: Callable[..., ThreadingHTTPServer]
) -> None:
    """INTERFACE.md: only the caller proxy's loopback forward attests a requester.

    A caller on the pod network presents an allowed human's valid header. It
    refuses ``missing_requester`` before any authority request, and records no
    effect. Liveness: the same header on the loopback forward applies one.
    """

    direct = connectors(stub.url)
    report_peer_as(direct, "10.0.0.7")
    assert record_effect(direct, ALICE) == refused("missing_requester")
    assert ledger(direct) == []
    assert stub.requests == []

    forwarded = connectors(stub.url)
    assert record_effect(forwarded, ALICE) == ok(1, ALICE)
    assert ledger(forwarded) == [(1, fingerprint(ALICE))]


def test_a_call_without_a_principal_header_refuses_missing_requester(
    connectors: Callable[..., ThreadingHTTPServer], idp: SimpleNamespace
) -> None:
    server = connectors(idp.url)
    assert record_effect(server) == refused("missing_requester")
    assert ledger(server) == []
    assert record_effect(server, ALICE) == ok(1, ALICE)


REPEATED: dict[str, list[tuple[str, str]]] = {
    "the same principal twice": [(PRINCIPAL_HEADER, ALICE), (PRINCIPAL_HEADER, ALICE)],
    "two humans": [(PRINCIPAL_HEADER, ALICE), (PRINCIPAL_HEADER, BOB)],
    "mixed-case names": [(PRINCIPAL_HEADER, ALICE), ("x-curie-principal", ALICE)],
    "an unreadable second copy": [(PRINCIPAL_HEADER, ALICE), ("X-CURIE-PRINCIPAL", "%%%")],
}


@pytest.mark.parametrize("headers", REPEATED.values(), ids=list(REPEATED))
def test_more_than_one_principal_header_refuses_requester_invalid(
    connectors: Callable[..., ThreadingHTTPServer],
    idp: SimpleNamespace,
    headers: list[tuple[str, str]],
) -> None:
    server = connectors(idp.url)
    assert record_effect(server, headers=headers) == refused("requester_invalid")
    assert ledger(server) == []
    assert record_effect(server, ALICE) == ok(1, ALICE)


UNREADABLE: dict[str, str] = {
    "not base64url": "%%%not-a-principal%%%",
    "not json": b64url(b"identity-e2e-alice"),
    "not an object": b64url(b'["identity-e2e-alice"]'),
    "a missing subject": encode({"tenant_id": TENANT, "issuer": assets.ISSUER}),
    "an extra field": encode({**principal("alice@example.test"), "groups": [assets.MEMBER_GROUP]}),
    "an uppercase tenant": encode(principal("alice@example.test", tenant_id=TENANT.upper())),
    "an empty subject": encode(principal("alice@example.test", subject="")),
    "a subject over 256 characters": encode(principal("alice@example.test", subject="a" * 257)),
}


@pytest.mark.parametrize("value", UNREADABLE.values(), ids=list(UNREADABLE))
def test_an_unreadable_principal_header_refuses_requester_invalid(
    connectors: Callable[..., ThreadingHTTPServer], idp: SimpleNamespace, value: str
) -> None:
    server = connectors(idp.url)
    assert record_effect(server, value) == refused("requester_invalid")
    assert ledger(server) == []
    assert record_effect(server, ALICE) == ok(1, ALICE)


DENIED: dict[str, str] = {
    "a human outside the group": DANA,
    "a disabled human": encode(principal("carol@example.test")),
    "a service account": encode(principal("robot@example.test")),
    "another tenant": encode(principal("alice@example.test", tenant_id=OTHER_TENANT)),
    "another issuer": encode(
        principal("alice@example.test", issuer="https://other.example.test/directory")
    ),
    "an unknown subject": encode(principal("alice@example.test", subject="identity-e2e-mallory")),
}


@pytest.mark.parametrize("header", DENIED.values(), ids=list(DENIED))
def test_a_human_without_current_authority_refuses_not_authorized(
    connectors: Callable[..., ThreadingHTTPServer], idp: SimpleNamespace, header: str
) -> None:
    server = connectors(idp.url)
    assert record_effect(server, header) == refused("not_authorized")
    assert ledger(server) == []
    assert record_effect(server, ALICE) == ok(1, ALICE)


def test_model_written_arguments_never_grant_or_redirect_authority(
    connectors: Callable[..., ThreadingHTTPServer], idp: SimpleNamespace
) -> None:
    server = connectors(idp.url)
    assert record_effect(server, DANA, arguments=impersonating("alice@example.test")) == refused(
        "not_authorized"
    )
    assert record_effect(server, arguments=impersonating("alice@example.test")) == refused(
        "missing_requester"
    )
    assert ledger(server) == []
    # The allowed human naming someone else in arguments still acts as herself.
    assert record_effect(server, ALICE, arguments=impersonating("dana@example.test")) == ok(
        1, ALICE
    )
    assert ledger(server) == [(1, fingerprint(ALICE))]


def test_revoked_membership_refuses_until_it_is_restored(
    connectors: Callable[..., ThreadingHTTPServer], idp: SimpleNamespace
) -> None:
    """Authority is current per call: nothing about an earlier allow is cached."""

    server = connectors(idp.url)
    change = {"sender": "alice@example.test", "group": assets.MEMBER_GROUP}
    assert record_effect(server, ALICE) == ok(1, ALICE)
    admin(idp, "/admin/membership", {**change, "member": False})
    assert record_effect(server, ALICE) == refused("not_authorized")
    assert record_effect(server, BOB) == ok(2, BOB)
    admin(idp, "/admin/membership", {**change, "member": True})
    assert record_effect(server, ALICE) == ok(3, ALICE)
    assert ledger(server) == [
        (1, fingerprint(ALICE)),
        (2, fingerprint(BOB)),
        (3, fingerprint(ALICE)),
    ]


def test_an_unavailable_authority_refuses_until_it_answers_again(
    connectors: Callable[..., ThreadingHTTPServer], idp: SimpleNamespace
) -> None:
    server = connectors(idp.url)
    admin(idp, "/admin/authority", {"available": False})
    assert record_effect(server, ALICE) == refused("authority_unavailable")
    assert ledger(server) == []
    admin(idp, "/admin/authority", {"available": True})
    assert record_effect(server, ALICE) == ok(1, ALICE)


def test_a_connector_holding_the_wrong_authority_bearer_refuses_authority_unavailable(
    connectors: Callable[..., ThreadingHTTPServer], idp: SimpleNamespace
) -> None:
    wrong = connectors(idp.url, bearer="synthetic-wrong-bearer")
    right = connectors(idp.url)
    assert record_effect(wrong, ALICE) == refused("authority_unavailable")
    assert ledger(wrong) == []
    assert record_effect(right, ALICE) == ok(1, ALICE)


def test_a_principal_never_carries_to_a_later_request(
    connectors: Callable[..., ThreadingHTTPServer], idp: SimpleNamespace
) -> None:
    """Not through the connection, an MCP session id, or anything process-wide."""

    server = connectors(idp.url)
    connection = http.client.HTTPConnection("127.0.0.1", port_of(server), timeout=15)
    try:
        initialize = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "identity-e2e-test", "version": "0"},
            },
        }
        reply = send(
            port_of(server), initialize, [(PRINCIPAL_HEADER, ALICE)], connection=connection
        )
        assert reply.status == 200
        session_id = reply.headers.get("Mcp-Session-Id")
        session = [("Mcp-Session-Id", session_id)] if session_id else []
        with_alice = [*session, (PRINCIPAL_HEADER, ALICE)]
        with_dana = [*session, (PRINCIPAL_HEADER, DANA)]
        assert record_effect(server, headers=with_alice, connection=connection) == ok(1, ALICE)
        assert record_effect(server, headers=session, connection=connection) == refused(
            "missing_requester"
        )
        assert record_effect(server, headers=with_dana, connection=connection) == refused(
            "not_authorized"
        )
    finally:
        connection.close()
    assert record_effect(server) == refused("missing_requester")
    assert ledger(server) == [(1, fingerprint(ALICE))]


# --- record_effect against an authority that misbehaves ------------------------------


def test_the_authority_check_sends_exactly_the_header_principal_with_the_bearer(
    connectors: Callable[..., ThreadingHTTPServer], stub: StubAuthority
) -> None:
    server = connectors(stub.url)
    assert record_effect(server, ALICE, arguments=impersonating("dana@example.test")) == ok(
        1, ALICE
    )
    [request] = stub.requests
    assert request["path"] == "/authority"
    assert request["authorization"] == "Bearer " + AUTHORITY_BEARER
    assert json.loads(request["body"]) == principal("alice@example.test")


def test_header_refusals_never_reach_the_authority(
    connectors: Callable[..., ThreadingHTTPServer], stub: StubAuthority
) -> None:
    server = connectors(stub.url)
    assert record_effect(server) == refused("missing_requester")
    assert record_effect(server, ALICE, ALICE) == refused("requester_invalid")
    assert record_effect(server, "%%%") == refused("requester_invalid")
    assert stub.requests == []
    assert ledger(server) == []
    assert record_effect(server, ALICE) == ok(1, ALICE)
    assert len(stub.requests) == 1


@pytest.mark.parametrize("case", corpus_cases("principal_headers"), ids=lambda case: case["name"])
def test_a_corpus_principal_header_reaches_the_authority_unchanged(
    connectors: Callable[..., ThreadingHTTPServer], stub: StubAuthority, case: dict[str, Any]
) -> None:
    server = connectors(stub.url)
    assert record_effect(server, case["header"]) == ok(1, case["header"])
    assert json.loads(stub.requests[-1]["body"]) == case["principal"]


@pytest.mark.parametrize(
    "case",
    [case for case in corpus_cases("malformed_principal_headers") if sendable(case["value"])],
    ids=lambda case: case["name"],
)
def test_a_corpus_malformed_principal_header_refuses_over_http(
    connectors: Callable[..., ThreadingHTTPServer], stub: StubAuthority, case: dict[str, Any]
) -> None:
    server = connectors(stub.url)
    assert record_effect(server, case["value"]) == refused("requester_invalid")
    assert stub.requests == []
    assert ledger(server) == []
    assert record_effect(server, ALICE) == ok(1, ALICE)


def test_a_deny_from_the_authority_refuses_not_authorized(
    connectors: Callable[..., ThreadingHTTPServer], stub: StubAuthority
) -> None:
    server = connectors(stub.url)
    stub.mode = "deny"
    assert record_effect(server, ALICE) == refused("not_authorized")
    assert ledger(server) == []
    stub.mode = "allow"
    assert record_effect(server, ALICE) == ok(1, ALICE)


BROKEN = [mode for mode in StubAuthority.ANSWERS if mode not in {"allow", "deny"}]


@pytest.mark.parametrize("mode", BROKEN)
def test_a_broken_authority_answer_refuses_authority_unavailable(
    connectors: Callable[..., ThreadingHTTPServer], stub: StubAuthority, mode: str
) -> None:
    """Anything but a 200 carrying exactly allow or deny is not a decision."""

    server = connectors(stub.url)
    stub.mode = mode
    assert record_effect(server, ALICE) == refused("authority_unavailable")
    assert ledger(server) == []
    stub.mode = "allow"
    assert record_effect(server, ALICE) == ok(1, ALICE)


def test_an_unreachable_authority_refuses_authority_unavailable(
    connectors: Callable[..., ThreadingHTTPServer], stub: StubAuthority
) -> None:
    server = connectors(stub.url)
    assert record_effect(server, ALICE) == ok(1, ALICE)
    stub.close()
    assert record_effect(server, ALICE) == refused("authority_unavailable")
    assert ledger(server) == [(1, fingerprint(ALICE))]


def test_a_hanging_authority_refuses_within_the_timeout(
    connectors: Callable[..., ThreadingHTTPServer], stub: StubAuthority
) -> None:
    server = connectors(stub.url, timeout_s=0.5)
    stub.mode = "hang"
    started = time.monotonic()
    assert record_effect(server, ALICE) == refused("authority_unavailable")
    assert time.monotonic() - started < 3.0
    assert ledger(server) == []
    stub.mode = "allow"
    assert record_effect(server, ALICE) == ok(1, ALICE)


def test_concurrent_calls_each_keep_their_own_requester(
    connectors: Callable[..., ThreadingHTTPServer], stub: StubAuthority
) -> None:
    """Both calls are inside the authority check at once; neither sees the other's header."""

    server = connectors(stub.url, timeout_s=8.0)
    stub.mode = "by_subject"
    stub.barrier = threading.Barrier(2)
    results: dict[str, tuple[bool, str]] = {}

    def call(name: str, header: str) -> None:
        results[name] = record_effect(server, header)

    threads = [
        threading.Thread(target=call, args=(name, header), daemon=True)
        for name, header in (("alice", ALICE), ("dana", DANA))
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(20)
    assert results == {"alice": ok(1, ALICE), "dana": refused("not_authorized")}
    assert ledger(server) == [(1, fingerprint(ALICE))]
    assert sorted(json.loads(request["body"])["subject"] for request in stub.requests) == sorted(
        [principal("alice@example.test")["subject"], principal("dana@example.test")["subject"]]
    )


# --- logging -------------------------------------------------------------------------

_OUTCOME_LINE = re.compile(r"record_effect outcome=(\S+) requester=(\S+) grant=(\S+)")


def test_each_call_logs_one_outcome_line_and_never_the_principal(
    connectors: Callable[..., ThreadingHTTPServer],
    stub: StubAuthority,
    capfd: pytest.CaptureFixture[str],
) -> None:
    server = connectors(stub.url)
    stub.mode = "by_subject"
    assert record_effect(server) == refused("missing_requester")
    assert record_effect(server, ALICE) == ok(1, ALICE)
    assert record_effect(server, DANA) == refused("not_authorized")
    assert record_effect(server, ALICE, BOB) == refused("requester_invalid")
    stub.mode = "error_with_an_allow_body"
    assert record_effect(server, BOB) == refused("authority_unavailable")

    # The line may be written after the response; collect until all five arrive.
    deadline = time.monotonic() + 5
    err = ""
    while True:
        err += capfd.readouterr().err
        found = _OUTCOME_LINE.findall(err)
        if len(found) >= 5 or time.monotonic() > deadline:
            break
    assert [outcome for outcome, _, _ in found] == [
        "missing_requester",
        "ok",
        "not_authorized",
        "requester_invalid",
        "authority_unavailable",
    ]
    # record_effect reads no attestation, so its line never carries a grant.
    assert [grant for _, _, grant in found] == ["-"] * 5
    requesters = [requester for _, requester, _ in found]
    assert requesters[:3] == ["-", fingerprint(ALICE), fingerprint(DANA)]
    assert re.fullmatch(r"-|[0-9a-f]{16}", requesters[3])
    assert requesters[4] == fingerprint(BOB)
    for secret in (*SUBJECTS, ALICE, BOB, DANA, AUTHORITY_BEARER):
        assert secret not in err


# --- the real MCP client -------------------------------------------------------------


def test_the_mcp_client_library_drives_the_connector_with_its_headers(
    connectors: Callable[..., ThreadingHTTPServer], idp: SimpleNamespace
) -> None:
    """The client the runner dials connectors with, configured the way it configures it."""

    server = connectors(idp.url)
    url = f"http://127.0.0.1:{port_of(server)}/mcp"

    async def session(headers: dict[str, str] | None) -> tuple[Any, Any, Any]:
        async with create_mcp_http_client(headers=headers) as http_client:
            async with streamable_http_client(url, http_client=http_client) as (read, write):
                async with ClientSession(read, write) as client:
                    initialized = await client.initialize()
                    listed = await client.list_tools()
                    called = await client.call_tool("record_effect", {"note": "synthetic effect"})
                    return initialized, listed, called

    def run(headers: dict[str, str] | None) -> tuple[Any, Any, Any]:
        return asyncio.run(asyncio.wait_for(session(headers), timeout=20))

    initialized, listed, called = run({PRINCIPAL_HEADER: ALICE})
    assert initialized.server_info.name
    assert [tool.name for tool in listed.tools] == [
        "record_effect",
        "execute_attested",
        "execute_approved",
    ]
    assert isinstance(called, types.CallToolResult)
    assert not called.is_error
    assert [(item.type, item.text) for item in called.content] == [
        ("text", f"effect=1 requester={fingerprint(ALICE)}")
    ]

    _, _, missing = run(None)
    assert isinstance(missing, types.CallToolResult)
    assert missing.is_error is True
    assert [item.text for item in missing.content] == ["refused missing_requester"]
    assert ledger(server) == [(1, fingerprint(ALICE))]


# --- the protected execution tools (ADR 0271 decisions 3 and 7) ----------------------
#
# The caller proxy sets ``X-Curie-Grant-Verified: <jti>`` on its loopback forward
# only after it verified and spent a grant for exactly this call. The connector
# checks attestation, requester, the ticket (execute_approved only) and current
# authority, in that order, before the effect. A refusal's liveness pair usually
# reuses its grant id, which also proves a refusal never burns the id here.

GRANT_VERIFIED_HEADER = "X-Curie-Grant-Verified"
ATTESTED, APPROVED = "execute_attested", "execute_approved"
PROTECTED_TOOLS = [ATTESTED, APPROVED]
SCOPE, REQUESTER, DIGEST = (
    "ticket_scope_mismatch",
    "ticket_requester_mismatch",
    "ticket_arguments_mismatch",
)
SAMPLE = "7d3f8a2e-1b4c-4d5e-9f60-a1b2c3d4e5f6"
OTHER_GRANT = "0c9b8a7d-6e5f-4a3b-8c2d-1e0f9a8b7c6d"
UNATTESTED = [(PRINCIPAL_HEADER, ALICE)]
NOTE = {"note": "synthetic approval"}
OTHER_NOTE = {"note": "synthetic other effect"}
TICKET_SIGNER = SigningKey(hashlib.sha256(b"identity-e2e synthetic ticket key").digest())
OTHER_SIGNER = SigningKey(hashlib.sha256(b"identity-e2e synthetic unregistered key").digest())
ISSUED_AT = 1_700_000_000  # long past: no connector reapplies the preparation deadline
TICKET_CONFIG = {
    "ticket_public_key": base64.b64encode(bytes(TICKET_SIGNER.verify_key)).decode("ascii"),
    "ticket_key_id": "selector-identity-e2e",
    "ticket_route": "identity-e2e-access",
    "ticket_agent_id": "6f1d2c3b-4a5e-4f60-8b7a-9c8d7e6f5a4b",
}


def new_grant() -> str:
    """A grant id as the worker mints one: a canonical lowercase UUID."""

    return str(uuid.uuid4())


def grant_fingerprint(grant: str) -> str:
    return hashlib.sha256(grant.encode("ascii")).hexdigest()[:16]


def applied(n: int, header: str, grant: str) -> tuple[bool, str]:
    return False, f"effect={n} requester={fingerprint(header)} grant={grant_fingerprint(grant)}"


def attested(grant: str, header: str | None = ALICE) -> list[tuple[str, str]]:
    """What the caller proxy forwards after a verified spend: the grant id and the principal."""

    sent = [(GRANT_VERIFIED_HEADER, grant)]
    return sent if header is None else [*sent, (PRINCIPAL_HEADER, header)]


def claimed_by(native_id: str = "alice@example.test", **overrides: str) -> dict[str, Any]:
    return {"requester": principal(native_id, **overrides)}


def ticket_claims(
    execution: dict[str, Any], *, requester: dict[str, str] | None = None, **overrides: Any
) -> dict[str, Any]:
    """Closed ADR 0271 claims for ``execute_approved`` on this connector, over ``execution``."""

    person = requester or principal("alice@example.test")
    claims = {
        "version": 1,
        "purpose": "approval_request",
        "tenant_id": person["tenant_id"],
        "agent_id": TICKET_CONFIG["ticket_agent_id"],
        "connector": "human-restricted",
        "tool": APPROVED,
        "route": TICKET_CONFIG["ticket_route"],
        "key_id": TICKET_CONFIG["ticket_key_id"],
        "requester": person,
        "nonce": b64url(secrets.token_bytes(32)),
        "issued_at": ISSUED_AT,
        "expires_at": ISSUED_AT + 300,
        "arguments_sha256": approver_ticket.execution_arguments_sha256(execution),
        "approvers": [principal("bob@example.test", tenant_id=person["tenant_id"])],
    }
    return {**claims, **overrides}


def sign(claims: dict[str, Any], signer: SigningKey = TICKET_SIGNER) -> str:
    return approver_ticket.sign_ticket(claims, signing_key=signer)


def sign_raw(claims: dict[str, Any], **dumps: Any) -> str:
    """A ticket over ``json.dumps(claims, **dumps)``: bytes the product signer never writes."""

    message = "cat1." + b64url(json.dumps(claims, **dumps).encode("ascii"))
    return message + "." + b64url(TICKET_SIGNER.sign(message.encode("ascii")).signature)


def approved(
    execution: dict[str, Any] | None = None, *, signer: SigningKey = TICKET_SIGNER, **claims: Any
) -> dict[str, Any]:
    """``execution`` plus the ticket signed over it, as ``granted_arguments`` carries it."""

    execution = NOTE if execution is None else execution
    return {**execution, "approval_ticket": sign(ticket_claims(execution, **claims), signer)}


def protected_arguments(tool: str, **extra: Any) -> dict[str, Any]:
    """Arguments every check accepts for alice: with a valid ticket for ``execute_approved``."""

    execution = {**NOTE, **extra}
    return approved(execution) if tool == APPROVED else execution


def invoke(
    server: ThreadingHTTPServer,
    tool: str,
    arguments: dict[str, Any],
    headers: Sequence[tuple[str, str]],
    **kwargs: Any,
) -> tuple[bool, str]:
    return record_effect(server, headers=headers, arguments=arguments, tool=tool, **kwargs)


def tool_call(tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
    params = {"name": tool, "arguments": arguments}
    return {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": params}


def unanswered(
    server: ThreadingHTTPServer, message: dict[str, Any], headers: Sequence[tuple[str, str]]
) -> bytes:
    """Every byte the connector sends back before it closes the connection."""

    port, body = port_of(server), json.dumps(message).encode()
    head = [
        "POST /mcp HTTP/1.1",
        f"Host: 127.0.0.1:{port}",
        "Accept: application/json, text/event-stream",
        "Content-Type: application/json",
        f"Content-Length: {len(body)}",
        "Connection: close",  # a server that answers then closes, so a wrong answer is quick
        *(f"{name}: {value}" for name, value in headers),
    ]
    received = b""
    with socket.create_connection(("127.0.0.1", port), timeout=15) as sock:
        sock.sendall(("\r\n".join(head) + "\r\n\r\n").encode("ascii") + body)
        try:
            while chunk := sock.recv(65536):
                received += chunk
        except ConnectionResetError:
            pass
    return received


def effects(server: ThreadingHTTPServer) -> int:
    return len(server.ledger)  # type: ignore[attr-defined]


@pytest.fixture
def protected(
    connectors: Callable[..., ThreadingHTTPServer], stub: StubAuthority
) -> ThreadingHTTPServer:
    """A connector with ticket configuration whose authority is the recording stub."""

    return connectors(stub.url, ticket=TICKET_CONFIG)


def test_the_protected_tool_contract_names() -> None:
    assert hrc.CONNECTOR_NAME == "human-restricted"
    assert hrc.GRANT_VERIFIED_HEADER == GRANT_VERIFIED_HEADER
    assert [hrc.ATTESTED_TOOL, hrc.APPROVED_TOOL] == PROTECTED_TOOLS
    codes = (
        "attestation_missing attestation_invalid grant_replayed ticket_missing ticket_invalid"
        " ticket_scope_mismatch ticket_requester_mismatch ticket_arguments_mismatch"
        " ticket_unconfigured"
    ).split()
    assert [getattr(hrc, code.upper()) for code in codes] == codes


@pytest.mark.parametrize("tool", PROTECTED_TOOLS)
def test_an_attested_call_applies_one_effect_on_the_shared_ledger(
    protected: ThreadingHTTPServer, stub: StubAuthority, tool: str
) -> None:
    grant, arguments = new_grant(), protected_arguments(tool)
    assert record_effect(protected, BOB) == ok(1, BOB)
    assert invoke(protected, tool, arguments, attested(grant)) == applied(2, ALICE, grant)
    assert effects(protected) == 2
    assert json.loads(stub.requests[-1]["body"]) == principal("alice@example.test")


@pytest.mark.parametrize("tool", PROTECTED_TOOLS)
def test_a_call_without_an_attestation_refuses_attestation_missing(
    protected: ThreadingHTTPServer, stub: StubAuthority, tool: str
) -> None:
    """The handler refuses even when the proxy's gated list lacks the tool."""

    grant, arguments = new_grant(), protected_arguments(tool)
    assert invoke(protected, tool, arguments, UNATTESTED) == refused("attestation_missing")
    assert invoke(protected, tool, arguments, []) == refused("attestation_missing")
    assert (effects(protected), stub.requests) == (0, [])
    assert invoke(protected, tool, arguments, attested(grant)) == applied(1, ALICE, grant)


BAD_ATTESTATIONS: dict[str, list[tuple[str, str]]] = {
    "the same grant twice": [(GRANT_VERIFIED_HEADER, SAMPLE)] * 2,
    "two grants": [(GRANT_VERIFIED_HEADER, SAMPLE), (GRANT_VERIFIED_HEADER, OTHER_GRANT)],
    "mixed-case names": [(GRANT_VERIFIED_HEADER, SAMPLE), ("x-curie-grant-verified", SAMPLE)],
    "a malformed second copy": [(GRANT_VERIFIED_HEADER, SAMPLE), ("X-CURIE-GRANT-VERIFIED", "x")],
    "uppercase": [(GRANT_VERIFIED_HEADER, SAMPLE.upper())],
    "no hyphens": [(GRANT_VERIFIED_HEADER, SAMPLE.replace("-", ""))],
    "braces": [(GRANT_VERIFIED_HEADER, "{" + SAMPLE + "}")],
    "a urn": [(GRANT_VERIFIED_HEADER, "urn:uuid:" + SAMPLE)],
    "one digit short": [(GRANT_VERIFIED_HEADER, SAMPLE[:-1])],
    "a non-hex digit": [(GRANT_VERIFIED_HEADER, "g" + SAMPLE[1:])],
    "an approval row id": [(GRANT_VERIFIED_HEADER, "approval-42")],
    "an approved boolean": [(GRANT_VERIFIED_HEADER, "true")],
}


@pytest.mark.parametrize("tool", PROTECTED_TOOLS)
@pytest.mark.parametrize("headers", BAD_ATTESTATIONS.values(), ids=list(BAD_ATTESTATIONS))
def test_a_repeated_or_malformed_attestation_refuses_attestation_invalid(
    protected: ThreadingHTTPServer, stub: StubAuthority, tool: str, headers: list[tuple[str, str]]
) -> None:
    grant, arguments = new_grant(), protected_arguments(tool)
    for sent in ([*headers, *UNATTESTED], headers):  # with a requester and without one
        assert invoke(protected, tool, arguments, sent) == refused("attestation_invalid")
    assert (effects(protected), stub.requests) == (0, [])
    assert invoke(protected, tool, arguments, attested(grant)) == applied(1, ALICE, grant)


@pytest.mark.parametrize("tool", PROTECTED_TOOLS)
def test_a_direct_caller_cannot_present_an_attestation(
    connectors: Callable[..., ThreadingHTTPServer], stub: StubAuthority, tool: str
) -> None:
    """The header counts only on the proxy's loopback forward, as the principal does."""

    direct, forwarded = (connectors(stub.url, ticket=TICKET_CONFIG) for _ in range(2))
    report_peer_as(direct, "10.0.0.7")
    grant, arguments = new_grant(), protected_arguments(tool)
    assert invoke(direct, tool, arguments, attested(grant)) == refused("attestation_missing")
    assert (effects(direct), stub.requests) == (0, [])
    assert invoke(forwarded, tool, arguments, attested(grant)) == applied(1, ALICE, grant)


@pytest.mark.parametrize("tool", PROTECTED_TOOLS)
def test_an_attestation_never_carries_to_a_later_request_on_the_connection(
    protected: ThreadingHTTPServer, tool: str
) -> None:
    first, second, arguments = new_grant(), new_grant(), protected_arguments(tool)
    connection = http.client.HTTPConnection("127.0.0.1", port_of(protected), timeout=15)

    def call(headers: list[tuple[str, str]]) -> tuple[bool, str]:
        return invoke(protected, tool, arguments, headers, connection=connection)

    try:
        assert call(attested(first)) == applied(1, ALICE, first)
        assert call(UNATTESTED) == refused("attestation_missing")
        assert call(attested(second, header=None)) == refused("missing_requester")
        assert call(attested(second)) == applied(2, ALICE, second)
    finally:
        connection.close()


@pytest.mark.parametrize("tool", PROTECTED_TOOLS)
def test_a_grant_id_applies_at_most_one_effect(
    protected: ThreadingHTTPServer, stub: StubAuthority, tool: str
) -> None:
    grant, fresh, arguments = new_grant(), new_grant(), protected_arguments(tool)
    other = next(name for name in PROTECTED_TOOLS if name != tool)
    assert invoke(protected, tool, arguments, attested(grant)) == applied(1, ALICE, grant)
    asked = len(stub.requests)
    replays = [
        (tool, arguments, attested(grant)),
        (tool, arguments, attested(grant, BOB)),
        (other, protected_arguments(other), attested(grant)),
        # The replay check is part of attestation, so it comes before the requester.
        (tool, arguments, attested(grant, header=None)),
    ]
    for name, sent, headers in replays:
        assert invoke(protected, name, sent, headers) == refused("grant_replayed")
    assert (effects(protected), len(stub.requests)) == (1, asked)
    assert invoke(protected, tool, arguments, attested(fresh)) == applied(2, ALICE, fresh)


@pytest.mark.parametrize("tool", PROTECTED_TOOLS)
def test_a_refusal_after_attestation_does_not_burn_the_grant(
    protected: ThreadingHTTPServer, stub: StubAuthority, tool: str
) -> None:
    """Only an applied effect remembers a grant id; the proxy's spend stops a resend."""

    grant, arguments = new_grant(), protected_arguments(tool)
    refusals = [
        ("allow", attested(grant, header=None), "missing_requester"),
        ("allow", attested(grant, "%%%"), "requester_invalid"),
        ("deny", attested(grant), "not_authorized"),
        ("error_with_an_allow_body", attested(grant), "authority_unavailable"),
    ]
    for mode, headers, code in refusals:
        stub.mode = mode
        assert invoke(protected, tool, arguments, headers) == refused(code)
    assert (effects(protected), len(stub.requests)) == (0, 2)
    stub.mode = "allow"
    assert invoke(protected, tool, arguments, attested(grant)) == applied(1, ALICE, grant)


@pytest.mark.parametrize("tool", PROTECTED_TOOLS)
def test_concurrent_calls_with_one_grant_apply_one_effect(
    connectors: Callable[..., ThreadingHTTPServer], stub: StubAuthority, tool: str
) -> None:
    """Both pass attestation and sit in the authority check together; one applies."""

    server = connectors(stub.url, timeout_s=8.0, ticket=TICKET_CONFIG)
    stub.barrier = threading.Barrier(2)
    grant, arguments = new_grant(), protected_arguments(tool)
    results: list[tuple[bool, str]] = []

    def call() -> None:
        results.append(invoke(server, tool, arguments, attested(grant)))

    threads = [threading.Thread(target=call, daemon=True) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(20)
    assert sorted(results) == sorted([applied(1, ALICE, grant), refused("grant_replayed")])
    assert (effects(server), len(stub.requests)) == (1, 2)


@pytest.mark.parametrize("missing", ["all", *TICKET_CONFIG])
def test_a_connector_without_ticket_configuration_refuses_ticket_unconfigured(
    connectors: Callable[..., ThreadingHTTPServer], stub: StubAuthority, missing: str
) -> None:
    config = None if missing == "all" else {**TICKET_CONFIG, missing: ""}
    server, grant = connectors(stub.url, ticket=config), new_grant()
    assert invoke(server, APPROVED, approved(), attested(grant)) == refused("ticket_unconfigured")
    # The ticket argument is read first, configured or not.
    assert invoke(server, APPROVED, NOTE, attested(grant)) == refused("ticket_missing")
    assert (effects(server), stub.requests) == (0, [])
    # Ordinary approval needs no ticket configuration.
    assert invoke(server, ATTESTED, NOTE, attested(grant)) == applied(1, ALICE, grant)
    configured = connectors(stub.url, ticket=TICKET_CONFIG)
    assert invoke(configured, APPROVED, approved(), attested(grant)) == applied(1, ALICE, grant)


@pytest.mark.parametrize("value", ["absent", None, 42, {"ticket": "x"}], ids=str)
def test_an_approved_call_without_a_ticket_string_refuses_ticket_missing(
    protected: ThreadingHTTPServer, stub: StubAuthority, value: Any
) -> None:
    grant = new_grant()
    arguments = NOTE if value == "absent" else {**NOTE, "approval_ticket": value}
    assert invoke(protected, APPROVED, arguments, attested(grant)) == refused("ticket_missing")
    assert (effects(protected), stub.requests) == (0, [])
    assert invoke(protected, APPROVED, approved(), attested(grant)) == applied(1, ALICE, grant)


def _flip_signature_bit(execution: dict[str, Any]) -> str:
    prefix, payload, signature = sign(ticket_claims(execution)).split(".")
    raw = bytearray(base64.urlsafe_b64decode(signature + "=="))
    raw[0] ^= 1
    return f"{prefix}.{payload}.{b64url(bytes(raw))}"


def _swap_payload(execution: dict[str, Any]) -> str:
    prefix, _, signature = sign(ticket_claims(execution)).split(".")
    other = sign(ticket_claims(execution, route="other-route")).split(".")[1]
    return f"{prefix}.{other}.{signature}"


COMPACT: dict[str, Any] = {"sort_keys": True, "separators": (",", ":")}
INVALID_TICKETS: dict[str, Callable[[dict[str, Any]], str]] = {
    "a flipped signature bit": _flip_signature_bit,
    "a payload swapped under the signature": _swap_payload,
    "an unregistered key": lambda e: sign(ticket_claims(e), OTHER_SIGNER),
    "spaced JSON": lambda e: sign_raw(ticket_claims(e), sort_keys=True),
    "unsorted JSON": lambda e: sign_raw(ticket_claims(e), separators=(",", ":")),
    "an unknown member": lambda e: sign_raw({**ticket_claims(e), "approved": True}, **COMPACT),
    "a boolean version": lambda e: sign_raw({**ticket_claims(e), "version": True}, **COMPACT),
    "a truncated ticket": lambda e: sign(ticket_claims(e))[:-2],
    "another prefix": lambda e: "cat2" + sign(ticket_claims(e))[4:],
    "not a ticket": lambda e: "cat1.not-a-ticket",
}


@pytest.mark.parametrize("make", INVALID_TICKETS.values(), ids=list(INVALID_TICKETS))
def test_a_ticket_that_does_not_read_refuses_ticket_invalid(
    protected: ThreadingHTTPServer, stub: StubAuthority, make: Callable[[dict[str, Any]], str]
) -> None:
    grant, arguments = new_grant(), {**NOTE, "approval_ticket": make(NOTE)}
    assert invoke(protected, APPROVED, arguments, attested(grant)) == refused("ticket_invalid")
    assert (effects(protected), stub.requests) == (0, [])
    assert invoke(protected, APPROVED, approved(), attested(grant)) == applied(1, ALICE, grant)


TICKET_MISMATCHES: dict[str, tuple[str, dict[str, Any]]] = {
    "another key id": (SCOPE, {"key_id": "selector-unregistered"}),
    "another connector": (SCOPE, {"connector": "other-connector"}),
    "the ordinary execution tool": (SCOPE, {"tool": ATTESTED}),
    "record_effect": (SCOPE, {"tool": "record_effect"}),
    "another route": (SCOPE, {"route": "other-route"}),
    "another agent": (SCOPE, {"agent_id": "1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d"}),
    "another human": (REQUESTER, claimed_by("bob@example.test")),
    "another issuer": (REQUESTER, claimed_by(issuer="https://other.example.test/directory")),
    "another tenant": (REQUESTER, claimed_by(tenant_id=OTHER_TENANT)),
    "a case-folded subject": (REQUESTER, claimed_by(subject=ALICE_SUBJECT.upper())),
}


@pytest.mark.parametrize(
    ("code", "claims"), TICKET_MISMATCHES.values(), ids=list(TICKET_MISMATCHES)
)
def test_a_ticket_for_another_scope_or_requester_is_refused(
    protected: ThreadingHTTPServer, stub: StubAuthority, code: str, claims: dict[str, Any]
) -> None:
    """Scope is this connector's configuration; the requester is the proxy principal exactly."""

    grant = new_grant()
    assert invoke(protected, APPROVED, approved(**claims), attested(grant)) == refused(code)
    assert (effects(protected), stub.requests) == (0, [])
    assert invoke(protected, APPROVED, approved(), attested(grant)) == applied(1, ALICE, grant)


def test_a_ticket_is_good_only_for_the_requester_it_names(protected: ThreadingHTTPServer) -> None:
    first, second, bobs = new_grant(), new_grant(), approved(**claimed_by("bob@example.test"))
    assert invoke(protected, APPROVED, bobs, attested(first)) == refused(REQUESTER)
    assert invoke(protected, APPROVED, bobs, attested(first, BOB)) == applied(1, BOB, first)
    assert invoke(protected, APPROVED, approved(), attested(second, BOB)) == refused(REQUESTER)
    assert effects(protected) == 1


ARGUMENT_CHANGES: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {
    "a changed note": (NOTE, OTHER_NOTE),
    "an added argument": (NOTE, {**NOTE, "drop_reply": False}),
    "a removed argument": ({**NOTE, "drop_reply": False}, NOTE),
}


@pytest.mark.parametrize(("signed", "sent"), ARGUMENT_CHANGES.values(), ids=list(ARGUMENT_CHANGES))
def test_arguments_the_ticket_did_not_sign_refuse_ticket_arguments_mismatch(
    protected: ThreadingHTTPServer,
    stub: StubAuthority,
    signed: dict[str, Any],
    sent: dict[str, Any],
) -> None:
    grant = new_grant()
    arguments = {**sent, "approval_ticket": approved(signed)["approval_ticket"]}
    assert invoke(protected, APPROVED, arguments, attested(grant)) == refused(DIGEST)
    assert (effects(protected), stub.requests) == (0, [])
    # Only the top-level ticket is outside the digest: the sent arguments, signed, apply.
    assert invoke(protected, APPROVED, approved(sent), attested(grant)) == applied(1, ALICE, grant)


def test_the_approved_checks_run_in_order_before_authority(
    protected: ThreadingHTTPServer, stub: StubAuthority
) -> None:
    grant, bob = new_grant(), claimed_by("bob@example.test")
    garbage = {**NOTE, "approval_ticket": "cat1.not-a-ticket"}
    unregistered = {**NOTE, "approval_ticket": sign(ticket_claims(NOTE, **bob), OTHER_SIGNER)}
    in_order = [
        (garbage, UNATTESTED, "attestation_missing"),
        (garbage, attested(grant, header=None), "missing_requester"),
        (unregistered, attested(grant), "ticket_invalid"),
        (approved(route="other-route", **bob), attested(grant), SCOPE),
        ({**approved(**bob), **OTHER_NOTE}, attested(grant), REQUESTER),
    ]
    for arguments, headers, code in in_order:
        assert invoke(protected, APPROVED, arguments, headers) == refused(code)
    stub.mode = "deny"
    assert invoke(protected, APPROVED, garbage, attested(grant)) == refused("ticket_invalid")
    assert stub.requests == []
    assert invoke(protected, APPROVED, approved(), attested(grant)) == refused("not_authorized")
    assert effects(protected) == 0
    stub.mode = "allow"
    assert invoke(protected, APPROVED, approved(), attested(grant)) == applied(1, ALICE, grant)


@pytest.mark.parametrize("offset", [None, 0, 3600], ids=["long ago", "just now", "an hour ahead"])
def test_an_approved_call_never_reapplies_the_preparation_deadline(
    protected: ThreadingHTTPServer, offset: int | None
) -> None:
    """ADR 0271 decision 7: an approval answered late still executes."""

    issued_at = ISSUED_AT if offset is None else int(time.time()) + offset
    grant, arguments = new_grant(), approved(issued_at=issued_at, expires_at=issued_at + 300)
    assert invoke(protected, APPROVED, arguments, attested(grant)) == applied(1, ALICE, grant)


@pytest.mark.parametrize("tool", PROTECTED_TOOLS)
def test_drop_reply_applies_the_effect_and_closes_without_an_answer(
    protected: ThreadingHTTPServer, tool: str
) -> None:
    """The uncertain-outcome probe: the effect happened and the caller hears nothing."""

    grant, fresh, arguments = new_grant(), new_grant(), protected_arguments(tool, drop_reply=True)
    assert unanswered(protected, tool_call(tool, arguments), attested(grant)) == b""
    assert effects(protected) == 1
    # The applied effect remembered the grant id, and a refusal is answered.
    assert invoke(protected, tool, arguments, attested(grant)) == refused("grant_replayed")
    answered = protected_arguments(tool, drop_reply=False)
    assert invoke(protected, tool, answered, attested(fresh)) == applied(2, ALICE, fresh)


@pytest.mark.parametrize("tool", PROTECTED_TOOLS)
def test_drop_reply_on_a_refused_call_answers_the_refusal(
    protected: ThreadingHTTPServer, stub: StubAuthority, tool: str
) -> None:
    grant, arguments = new_grant(), protected_arguments(tool, drop_reply=True)
    assert invoke(protected, tool, arguments, UNATTESTED) == refused("attestation_missing")
    stub.mode = "deny"
    assert invoke(protected, tool, arguments, attested(grant)) == refused("not_authorized")
    assert effects(protected) == 0
    stub.mode = "allow"
    assert unanswered(protected, tool_call(tool, arguments), attested(grant)) == b""
    assert effects(protected) == 1


def test_record_effect_ignores_attestation_and_drop_reply(protected: ThreadingHTTPServer) -> None:
    """Its exact existing behavior: it reads no attestation, so it remembers no grant id."""

    grant, arguments = new_grant(), {**NOTE, "drop_reply": True}
    assert record_effect(protected, headers=attested(grant), arguments=arguments) == ok(1, ALICE)
    assert invoke(protected, ATTESTED, NOTE, attested(grant)) == applied(2, ALICE, grant)


_TOOL_LINE = re.compile(
    r"^(record_effect|execute_attested|execute_approved)"
    r" outcome=(\S+) requester=(\S+) grant=(\S+)$",
    re.MULTILINE,
)


def test_each_protected_call_logs_one_line_without_principal_ticket_or_grant(
    protected: ThreadingHTTPServer, stub: StubAuthority, capfd: pytest.CaptureFixture[str]
) -> None:
    granted, held, arguments = new_grant(), new_grant(), approved()
    assert invoke(protected, APPROVED, arguments, attested(granted)) == applied(1, ALICE, granted)
    assert invoke(protected, ATTESTED, NOTE, UNATTESTED) == refused("attestation_missing")
    stub.mode = "deny"
    assert invoke(protected, ATTESTED, NOTE, attested(held, DANA)) == refused("not_authorized")
    stub.mode = "allow"
    changed = {**arguments, **OTHER_NOTE}
    assert invoke(protected, APPROVED, changed, attested(held)) == refused(DIGEST)
    assert record_effect(protected, headers=attested(held)) == ok(2, ALICE)

    expected = [
        (APPROVED, "ok", fingerprint(ALICE), grant_fingerprint(granted)),
        (ATTESTED, "attestation_missing", "-", "-"),
        (ATTESTED, "not_authorized", fingerprint(DANA), grant_fingerprint(held)),
        (APPROVED, DIGEST, fingerprint(ALICE), grant_fingerprint(held)),
        ("record_effect", "ok", fingerprint(ALICE), "-"),
    ]
    deadline, err = time.monotonic() + 5, ""
    while True:  # a line may be written after its response
        err += capfd.readouterr().err
        found = _TOOL_LINE.findall(err)
        if len(found) >= len(expected) or time.monotonic() > deadline:
            break
    assert sorted(found) == sorted(expected)
    ticket = arguments["approval_ticket"]
    for secret in (granted, held, ticket, ticket.split(".")[1], ALICE, DANA, *SUBJECTS):
        assert secret not in err


def test_main_reads_the_ticket_configuration_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, Any] = {}

    class Unserved:
        def serve_forever(self) -> None:
            raise KeyboardInterrupt

        def server_close(self) -> None:
            pass

    def make_server(host: str, port: int, **config: Any) -> Unserved:
        seen.clear()
        seen.update(config)
        return Unserved()

    monkeypatch.setattr(hrc, "make_server", make_server)
    monkeypatch.setenv("AUTHORITY_URL", "http://authority.example.test")
    monkeypatch.setenv("AUTHORITY_BEARER", AUTHORITY_BEARER)
    monkeypatch.setenv("PORT", "8000")
    environment = {name.upper(): value for name, value in TICKET_CONFIG.items()}
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    assert hrc.main([]) == 0
    assert {name: seen.get(name) for name in TICKET_CONFIG} == TICKET_CONFIG
    # All four are optional: unset, the connector starts and refuses ticket_unconfigured.
    for name in environment:
        monkeypatch.delenv(name)
    assert hrc.main([]) == 0
    assert {name: seen.get(name, "") for name in TICKET_CONFIG} == dict.fromkeys(TICKET_CONFIG, "")


# --- the shipped source --------------------------------------------------------------


def test_the_module_ships_inline_as_standard_library_python_312() -> None:
    """It travels as a connectors.yaml env value into python:3.12-slim with nothing else."""

    text = Path(hrc.__file__).read_text(encoding="utf-8")
    assert "${" not in text
    tree = ast.parse(text, feature_version=(3, 12))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0 and node.module, "a relative import cannot run from an env value"
            imported.add(node.module.split(".")[0])
    assert imported - set(sys.stdlib_module_names) == set()
