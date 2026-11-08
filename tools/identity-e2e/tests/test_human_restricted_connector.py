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
import sys
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import fake_idp
import human_restricted_connector as hrc
import identity_assets as assets
import pytest
from identity_target import REPO
from mcp import ClientSession, types
from mcp.client.streamable_http import create_mcp_http_client, streamable_http_client

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
) -> tuple[bool, str]:
    """One ``tools/call record_effect``; ``(isError, text)``."""

    sent = list(headers) if headers is not None else [(PRINCIPAL_HEADER, p) for p in principals]
    params = {
        "name": "record_effect",
        "arguments": arguments if arguments is not None else {"note": "synthetic effect"},
    }
    answer = rpc(port_of(server), "tools/call", params, headers=sent, connection=connection)
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
        authority_url: str, *, bearer: str = AUTHORITY_BEARER, timeout_s: float = 2.0
    ) -> ThreadingHTTPServer:
        server = hrc.make_server(
            "127.0.0.1",
            0,
            authority_url=authority_url,
            authority_bearer=bearer,
            timeout_s=timeout_s,
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


def test_tools_list_offers_only_record_effect_with_a_note(
    connectors: Callable[..., ThreadingHTTPServer], stub: StubAuthority
) -> None:
    server = connectors(stub.url)
    tools = rpc(port_of(server), "tools/list")["result"]["tools"]
    assert [tool["name"] for tool in tools] == ["record_effect"]
    schema = tools[0]["inputSchema"]
    assert schema["type"] == "object"
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

_OUTCOME_LINE = re.compile(r"record_effect outcome=(\S+) requester=(\S+)")


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
    assert [outcome for outcome, _ in found] == [
        "missing_requester",
        "ok",
        "not_authorized",
        "requester_invalid",
        "authority_unavailable",
    ]
    requesters = [requester for _, requester in found]
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
    assert [tool.name for tool in listed.tools] == ["record_effect"]
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
