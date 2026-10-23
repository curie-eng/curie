"""The shared connector resolver's contract, end to end (ADR 0239).

"Bundle, identity and request contract" 4, 5 and 6 make ``POST /v1/resolve``
on a pod-local Unix socket the language-neutral admission point: an
invocation handle, a declared slot and an audit-only purpose in; binding,
account, revision, origins, expiry and credential fields (or identity-only
admission) out; every refusal before any credential is read. "Custody and
least privilege" 2 to 4 require the resolver to connect only as its
application's generated role and to verify that role's actual privileges
before it activates. "Static sources" 1, 4, 5 and 6 require the per-call
admission transaction, explicit reimport, fenced refresh and fail-closed
outages.

Name prefixes map to the plan's criteria and are ``-k`` selectors, so each
prefix word appears only in its own criterion's test names (and in no
parametrize id):

``test_wire_*``       AC1 the wire and the socket
``test_handle_*``     AC2 the invocation handle registry
``test_admission_*``  AC3 role custody, start refusal and the admission transaction
``test_seam_*``       AC4 the source plug-in seam and the refresh contract

Everything runs against the real API, a real Postgres 16 (``shared_database``)
and the real resolver over its socket. Only the external provider is faked.

Readings of the plan these tests settle (see also ``_resolver_harness``):

1. Identity-only requests carry ``IDENTITY_SLOT`` (the empty slot), because
   the API registers connector custody with no credential slots.
2. An absent ``handle`` key answers ``handle_missing`` (the ``parse_request``
   note), while any other missing, extra or mistyped key is ``malformed``.
3. A ``purpose`` that is blank, carries a control character or exceeds 256
   UTF-8 bytes is ``malformed``.
4. Start refusal codes follow the order the plan lists them (superuser,
   createrole, createdb, replication, bypassrls, ...), which decides the code
   for the owner DSN.
5. A database outage's first answers after ``LOGIN`` returns may still be the
   exact ``store_unavailable`` refusal while the pool replaces a connection
   the server closed; ``settle`` tolerates at most two of those.
"""

from __future__ import annotations

import asyncio
import base64
import concurrent.futures
import dataclasses
import gzip
import hashlib
import http.client
import json
import os
import re
import secrets
import socket
import stat
import tempfile
import threading
import time
import uuid
from typing import Any

import asyncpg  # type: ignore[import-untyped]
import pytest
from _fake_provider import TOKEN_PATH, FakeProvider
from _migration_support import sql_dicts
from _refresh_source import (
    ACCESS,
    PausingRefreshSource,
    ProviderRefreshSource,
    ShortLeaseRefreshSource,
    refresh_material,
)
from _resolver_harness import (
    IDENTITY_SLOT,
    PURPOSE,
    RESOLVER_APPLICATION_NAME,
    STATIC_FIELD,
    UNOBSERVED_MEMBERSHIP,
    Fleet,
    Member,
    ResolverHarness,
    SharedApp,
    blocking_pids,
    debug_capture,
    envelope,
    log_text,
    opened_fields,
    owner_dsn,
    parse_raw,
    request_head,
    resolve_body,
    settle,
    terminate_role_backends,
    wait_for_backend_blocked_by,
    wait_for_no_resolver_backends,
    wait_for_parked_resolver,
    wait_for_resolver_in_transaction,
)
from _shared_connector_support import (
    BEGIN_SQL,
    LOCK_WAIT_S,
    MARK_RECOVERY_SQL,
    SLOT,
    STATEMENT_WAIT_S,
    SessionPool,
    SharedCleanup,
    b64,
    binding_row,
    dsn_password,
    in_background,
    intent_row,
    quote_ident,
    random_installation,
    secret_sentinel,
)
from curie_connector_proxy.resolver import handles as handles_module
from curie_connector_proxy.resolver.admission import Admitted, admit
from curie_connector_proxy.resolver.config import ResolverConfig
from curie_connector_proxy.resolver.handles import (
    HANDLE_PREFIX,
    MAX_INVOCATIONS,
    MAX_SESSIONS,
    HandleRegistry,
    InvocationContext,
    Principal,
    RegistryRefusal,
)
from curie_connector_proxy.resolver.http import serve_unix
from curie_connector_proxy.resolver.refresh import RefreshCoordinator
from curie_connector_proxy.resolver.sources import (
    ConnectorIdentitySource,
    SourceRefusal,
    SourceRegistry,
    StaticSource,
    default_registry,
)
from curie_connector_proxy.resolver.wire import (
    MAX_REQUEST_BYTES,
    REFUSAL_STATUS,
    ResolveRefusal,
)

HANDLE_SHAPE = re.compile(r"^crh1\.[A-Za-z0-9_-]{43}$")
# The request the wire tests pad and reshape; built per test from a live handle.
FAST_REPLY_S = 5.0


def _counts(harness: ResolverHarness) -> tuple[int, int]:
    return harness.admissions, harness.releases


def _static(fleet: Fleet, **fields: str | bytes) -> tuple[SharedApp, Member, ResolverHarness]:
    app = fleet.application()
    member = app.add_member(fields=fields or None)
    return app, member, fleet.launch(app)


def _pure_registry(clock: Any = None) -> tuple[HandleRegistry, Principal]:
    installation, application = random_installation(), str(uuid.uuid4())
    registry = (
        HandleRegistry(installation=installation, application=application)
        if clock is None
        else HandleRegistry(installation=installation, application=application, clock=clock)
    )
    principal = Principal(
        installation=installation,
        application=application,
        agent_id=str(uuid.uuid4()),
        agent="agent-pure",
        policy_generation=1,
    )
    return registry, principal


def _context(principal: Principal, **options: Any) -> InvocationContext:
    options.setdefault("alias", "primary")
    options.setdefault("expires_at", int(time.time()) + 600)
    options.setdefault("binding", str(uuid.uuid4()))
    options.setdefault("membership", UNOBSERVED_MEMBERSHIP)
    return InvocationContext(principal=principal, **options)


def _refused(code: str) -> Any:
    return pytest.raises(RegistryRefusal, match=f"^{re.escape(code)}$")


def _no_secret(text: str, *secret_values: str) -> None:
    for value in secret_values:
        assert value not in text, "a secret value reached a response or log"


# =============================================================================
# AC1: the wire
# =============================================================================


def test_wire_static_release_carries_every_field_with_no_store(fleet: Fleet) -> None:
    sentinel = secret_sentinel("WIRE")
    certificate = b"\x00\x01binary-\xff" + secrets.token_bytes(8)
    app, member, harness = _static(fleet, **{STATIC_FIELD: sentinel, "client_cert": certificate})
    invocation = harness.register(member, expires_in=900)
    before = _counts(harness)

    reply = harness.resolve(invocation.handle)
    assert reply.status == 200, reply.text
    assert reply.headers.get("cache-control") == "no-store", reply.headers
    assert reply.headers.get("content-type", "").startswith("application/json")
    assert reply.json == {
        "admission": "credential",
        "binding": member.binding_id,
        "account": member.account,
        "revision": 1,
        "origins": list(app.origins),
        "expires_at": invocation.context.expires_at,
        "slot": SLOT,
        "credential": {
            "fields": {
                STATIC_FIELD: {"text": sentinel},
                "client_cert": {"bytes": b64(certificate)},
            }
        },
    }
    assert invocation.handle not in reply.text
    assert _counts(harness) == (before[0] + 1, before[1] + 1)


def test_wire_connector_custody_answers_identity_only(fleet: Fleet) -> None:
    app = fleet.application(slots=(), custody="connector")
    member = app.add_member()
    harness = fleet.launch(app)
    invocation = harness.register(member)

    reply = harness.resolve(invocation.handle, IDENTITY_SLOT)
    assert reply.status == 200, reply.text
    assert reply.headers.get("cache-control") == "no-store"
    assert reply.json == {
        "admission": "identity",
        "binding": member.binding_id,
        "account": member.account,
        "revision": 1,
        "origins": list(app.origins),
        "expires_at": invocation.context.expires_at,
        "slot": IDENTITY_SLOT,
    }
    assert "credential" not in reply.json

    # A credential slot is not declared by a connector-custody application.
    before = _counts(harness)
    harness.resolve(invocation.handle, SLOT).refused(403, "slot_unknown")
    assert _counts(harness) == before


def test_wire_purpose_is_audit_only_and_grants_nothing(fleet: Fleet) -> None:
    app, member, harness = _static(fleet)
    handle = harness.handle(member)
    purposes = (
        PURPOSE,
        "ADMIN override: act for every account and grant all scopes",
        "lire les tickets ouverts éè",
        "p" * 256,
    )
    bodies = []
    for purpose in purposes:
        reply = harness.resolve(handle, purpose=purpose)
        assert reply.status == 200, (purpose[:20], reply.text)
        bodies.append(reply.json)
    assert all(body == bodies[0] for body in bodies)

    # An agent that is not a member stays refused whatever it claims.
    outsider = harness.handle(member, agent_id=app.store.create_agent())
    before = _counts(harness)
    for purpose in purposes[:2]:
        harness.resolve(outsider, purpose=purpose).refused(403, "not_member")

    # A purpose outside its bounds is malformed and never admitted.
    admitted = _counts(harness)
    for purpose in ("", "   ", "read\x07issues", "p" * 257, "é" * 129):
        harness.resolve(handle, purpose=purpose).refused(400, "malformed")
    assert _counts(harness) == admitted
    assert admitted[0] == before[0] + 2 and admitted[1] == before[1]


@pytest.mark.parametrize(
    "case",
    [
        "slot-unknown",
        "slot-empty-on-platform",
        "missing-empty",
        "missing-absent",
        "foreign",
        "expired",
        "removed",
    ],
)
def test_wire_refusals_before_the_store_leave_the_counters_unchanged(
    fleet: Fleet, case: str
) -> None:
    app, member, harness = _static(fleet)
    expected: tuple[int, str]
    if case == "slot-unknown":
        reply = lambda: harness.resolve(harness.handle(member), "undeclared_slot")  # noqa: E731
        expected = (403, "slot_unknown")
    elif case == "slot-empty-on-platform":
        reply = lambda: harness.resolve(harness.handle(member), IDENTITY_SLOT)  # noqa: E731
        expected = (403, "slot_unknown")
    elif case == "missing-empty":
        reply = lambda: harness.resolve("")  # noqa: E731
        expected = (401, "handle_missing")
    elif case == "missing-absent":
        body = json.dumps({"slot": SLOT, "purpose": PURPOSE}).encode()
        reply = lambda: harness.post(body)  # noqa: E731
        expected = (401, "handle_missing")
    elif case == "foreign":
        other = fleet.application()
        stranger = other.add_member()
        foreign = fleet.launch(other).handle(stranger)
        reply = lambda: harness.resolve(foreign)  # noqa: E731
        expected = (401, "handle_unknown")
    elif case == "expired":
        expires_at = int(time.time()) + 1
        lapsing = harness.handle(member, expires_at=expires_at)
        while time.time() < expires_at + 0.05:
            time.sleep(0.05)
        reply = lambda: harness.resolve(lapsing)  # noqa: E731
        expected = (401, "handle_expired")
    else:
        invocation = harness.register(member)
        assert harness.handles.remove(invocation.handle) is True
        reply = lambda: harness.resolve(invocation.handle)  # noqa: E731
        expected = (401, "handle_unknown")

    before = _counts(harness)
    reply().refused(*expected)
    assert _counts(harness) == before

    # Liveness: the same resolver still admits a valid request.
    assert harness.resolve(harness.handle(member)).status == 200
    assert _counts(harness) == (before[0] + 1, before[1] + 1)


def test_wire_body_bounds_refuse_before_reading(fleet: Fleet) -> None:
    app, member, harness = _static(fleet)
    handle = harness.handle(member)
    valid = resolve_body(handle, SLOT)
    padded = valid + b" " * (MAX_REQUEST_BYTES - len(valid))
    assert len(padded) == MAX_REQUEST_BYTES == 65536
    before = _counts(harness)

    # Exactly 64 KiB is processed.
    exact, _ = harness.raw(request_head(len(padded)), padded)
    assert exact.status == 200, exact.text
    assert exact.json["binding"] == member.binding_id
    assert _counts(harness) == (before[0] + 1, before[1] + 1)
    after_exact = _counts(harness)

    # One byte over is refused from the head alone, with no body sent.
    over, elapsed = harness.raw(request_head(len(padded) + 1))
    over.refused(413, "request_too_large")
    assert elapsed < FAST_REPLY_S, elapsed
    over_sent, _ = harness.raw(request_head(len(padded) + 1), padded + b" ")
    over_sent.refused(413, "request_too_large")

    # Chunked framing is refused without reading a chunk.
    chunked = f"{len(valid):x}\r\n".encode() + valid + b"\r\n0\r\n\r\n"
    refused, elapsed = harness.raw(
        request_head(None, extra=["Transfer-Encoding: chunked"]), chunked
    )
    refused.refused(411, "length_required")
    assert elapsed < FAST_REPLY_S, elapsed

    # A compressed body is refused, never inflated.
    compressed = gzip.compress(valid)
    encoded, _ = harness.raw(
        request_head(len(compressed), extra=["Content-Encoding: gzip"]), compressed
    )
    encoded.refused(415, "unsupported_media_type")
    plain_text, _ = harness.raw(request_head(len(valid), content_type="text/plain"), valid)
    plain_text.refused(415, "unsupported_media_type")
    assert _counts(harness) == after_exact

    # Liveness: identity encoding and a charset parameter are accepted.
    for head in (
        request_head(len(valid), extra=["Content-Encoding: identity"]),
        request_head(len(valid), content_type="application/json; charset=utf-8"),
    ):
        accepted, _ = harness.raw(head, valid)
        assert accepted.status == 200, accepted.text
    assert _counts(harness) == (after_exact[0] + 2, after_exact[1] + 2)


def test_wire_malformed_bodies_and_lengths_refuse_400(fleet: Fleet) -> None:
    app, member, harness = _static(fleet)
    handle = harness.handle(member)
    bodies = (
        b"not json",
        b"[]",
        b"\xff\xfe{}",
        json.dumps({"handle": handle, "purpose": PURPOSE}).encode(),
        json.dumps({"handle": handle, "slot": SLOT}).encode(),
        json.dumps({"handle": handle, "slot": SLOT, "purpose": PURPOSE, "agent": "x"}).encode(),
        json.dumps({"handle": handle, "slot": 7, "purpose": PURPOSE}).encode(),
        json.dumps({"handle": 7, "slot": SLOT, "purpose": PURPOSE}).encode(),
        json.dumps({"handle": handle, "slot": SLOT, "purpose": None}).encode(),
    )
    before = _counts(harness)
    for body in bodies:
        reply = harness.post(body)
        reply.refused(400, "malformed")
        _no_secret(reply.text, handle)

    valid = resolve_body(handle, SLOT)
    for head in (
        request_head(None),
        request_head(f"{len(valid)}", extra=[f"Content-Length: {len(valid)}"]),
        request_head(f"+{len(valid)}"),
        request_head(f"{len(valid)}.0"),
    ):
        reply, _ = harness.raw(head, valid)
        reply.refused(400, "malformed")
    # A body shorter than its Content-Length is a short read.
    short, _ = harness.raw(request_head(len(valid) + 10), valid)
    short.refused(400, "malformed")
    assert _counts(harness) == before
    assert harness.resolve(handle).status == 200


def test_wire_only_post_to_the_resolve_path_is_served(fleet: Fleet) -> None:
    app, member, harness = _static(fleet)
    valid = resolve_body(harness.handle(member), SLOT)
    before = _counts(harness)
    for head, status, code in (
        (request_head(len(valid), method="GET"), 405, "method_not_allowed"),
        (request_head(len(valid), method="PUT"), 405, "method_not_allowed"),
        (request_head(len(valid), target="/v1/other"), 404, "not_found"),
        (request_head(len(valid), target="/"), 404, "not_found"),
        (request_head(len(valid), version="HTTP/1.0"), 400, "malformed"),
    ):
        reply, _ = harness.raw(head, valid)
        reply.refused(status, code)
        assert reply.headers.get("connection") == "close", reply.headers
        assert REFUSAL_STATUS[code] == status
    assert _counts(harness) == before

    served, _ = harness.raw(request_head(len(valid)), valid)
    assert served.status == 200, served.text
    assert served.headers.get("connection") == "close"
    assert served.headers.get("cache-control") == "no-store"
    assert int(served.headers["content-length"]) == len(served.body)


def test_wire_nothing_echoes_or_logs_the_invocation_or_plaintext(
    fleet: Fleet, caplog: pytest.LogCaptureFixture
) -> None:
    sentinel = secret_sentinel("ECHO")
    app, member, harness = _static(fleet, **{STATIC_FIELD: sentinel})
    handle = harness.handle(member)
    outsider = harness.handle(member, agent_id=app.store.create_agent())
    valid = resolve_body(handle, SLOT)
    responses: list[bytes] = []

    with debug_capture(caplog):
        # Malformed framing that carries both secrets.
        framing = (
            (request_head(handle), valid, 400, "malformed"),
            (request_head(len(valid), extra=[f"{sentinel} no colon"]), valid, 400, "malformed"),
            (
                request_head(len(valid), extra=["X-Note: ok", f" {sentinel}"]),
                valid,
                400,
                "malformed",
            ),
            (f"{handle} {sentinel}\r\n\r\n".encode(), b"", 400, "malformed"),
            (
                request_head(len(valid), extra=[f"X-Pad: {handle}{sentinel}" + "a" * 9000]),
                valid,
                431,
                "headers_too_large",
            ),
        )
        before = _counts(harness)
        for head, body, status, code in framing:
            reply, _ = harness.raw(head, body)
            reply.refused(status, code)
            responses.append(reply.body)
        assert _counts(harness) == before

        # Purpose equal to the handle and to the plaintext: admitted, never reflected.
        for purpose in (handle, sentinel):
            reply = harness.resolve(handle, purpose=purpose)
            assert reply.status == 200, reply.text
            assert reply.json["credential"]["fields"][STATIC_FIELD] == {"text": sentinel}
            assert reply.text.count(sentinel) == 1
            assert handle not in reply.text
        refusal = harness.resolve(outsider, purpose=handle)
        refusal.refused(403, "not_member")
        responses.append(refusal.body)
        harness.resolve(handle, purpose=sentinel, slot="undeclared_slot").refused(
            403, "slot_unknown"
        )

    for body in responses:
        _no_secret(body.decode("latin-1"), handle, outsider, sentinel)
    text = log_text(caplog)
    _no_secret(text, handle, outsider, sentinel)
    # Liveness of the audit trail: an INFO record names the binding and the
    # purpose only by digest.
    audit = [
        record
        for record in caplog.records
        if record.name.startswith("curie_connector_proxy.resolver") and record.levelname == "INFO"
    ]
    digest = hashlib.sha256(sentinel.encode()).hexdigest()[:16]
    assert any(
        member.binding_id in log_text_of(record) and digest in log_text_of(record)
        for record in audit
    ), "no audit record names the binding and the purpose digest"


def log_text_of(record: Any) -> str:
    return f"{record.getMessage()} {record.__dict__!r}"


def test_wire_socket_lives_alone_in_its_own_directory(fleet: Fleet) -> None:
    app = fleet.application()
    member = app.add_member()
    env, directory = fleet.env(app)
    socket_path = env["CURIE_SHARED_RESOLVER_SOCKET"]
    # A stale socket file from a previous process is replaced.
    stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    stale.bind(socket_path)
    stale.close()
    assert stat.S_ISSOCK(os.stat(socket_path).st_mode)

    harness = fleet.launch(app, env=(env, directory))
    mode = os.stat(socket_path).st_mode
    assert stat.S_ISSOCK(mode)
    assert stat.S_IMODE(mode) == 0o666
    parent = os.path.dirname(socket_path)
    assert os.listdir(parent) == [os.path.basename(socket_path)]
    for name in ("CURIE_SHARED_ENCRYPTION_KEY_FILE", "CURIE_SHARED_DATABASE_DSN_FILE"):
        assert os.path.dirname(env[name]) != parent
    assert harness.resolve(harness.handle(member)).status == 200

    for bad in ("/r.sock", "r.sock", "/tmp/" + "a" * 110 + "/r.sock"):
        with pytest.raises(ValueError, match="CURIE_SHARED_RESOLVER_SOCKET"):
            ResolverConfig.from_env({**env, "CURIE_SHARED_RESOLVER_SOCKET": bad})

    harness.close()
    assert not os.path.exists(socket_path)


CONNECTION_CAP = 4
BUSY_REFUSAL_S = 2.0


def _busy_attempt(path: str, head: bytes, body: bytes) -> tuple[bytes, float]:
    """Connect past the cap, send a whole request, read to EOF; answer the bytes and latency."""

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(BUSY_REFUSAL_S + 1.0)
    started = time.monotonic()
    chunks: list[bytes] = []
    try:
        sock.connect(path)
        try:
            sock.sendall(head + body)
        except (BrokenPipeError, ConnectionResetError):
            pass
        while True:
            try:
                chunk = sock.recv(65536)
            except ConnectionResetError:
                break
            except TimeoutError:
                pytest.fail("a connection past the cap was neither refused nor closed")
            if not chunk:
                break
            chunks.append(chunk)
    finally:
        sock.close()
    return b"".join(chunks), time.monotonic() - started


def test_wire_connection_cap_refuses_extra_connections_immediately(fleet: Fleet) -> None:
    """Idle connections cannot exhaust the resolver (security review S5).

    With ``max_connections=4`` and four connections that send nothing, a
    fifth is refused at once: closed with no response, or a fixed 503
    ``{"refusal": "busy"}``. Nothing it sent is admitted, and once the idle
    connections close, an ordinary resolve is served again.
    """

    app = fleet.application()
    member = app.add_member()
    harness = fleet.launch(app, start_options={"max_connections": CONNECTION_CAP})
    handle = harness.handle(member)
    assert harness.resolve(handle).status == 200
    body = resolve_body(handle, SLOT)
    before = _counts(harness)

    idle: list[socket.socket] = []
    try:
        for _ in range(CONNECTION_CAP):
            held = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            held.settimeout(STATEMENT_WAIT_S)
            held.connect(harness.socket_path)
            idle.append(held)
        # Let the server accept all four before the fifth arrives.
        time.sleep(0.5)

        data, elapsed = _busy_attempt(harness.socket_path, request_head(len(body)), body)
        assert elapsed < BUSY_REFUSAL_S, elapsed
        assert handle.encode() not in data
        if data:
            reply = parse_raw(data)
            reply.refused(503, "busy")
            assert REFUSAL_STATUS.get("busy") == 503
        assert _counts(harness) == before, "a connection past the cap was admitted"
    finally:
        for held in idle:
            held.close()

    # Liveness: with the idle connections gone, the resolver serves again.
    deadline = time.monotonic() + FAST_REPLY_S
    while True:
        try:
            reply = harness.resolve(handle)
        except (http.client.RemoteDisconnected, ConnectionResetError, BrokenPipeError):
            assert time.monotonic() < deadline, "the cap never released"
            time.sleep(0.1)
            continue
        if reply.status == 503 and reply.json == {"refusal": "busy"}:
            assert time.monotonic() < deadline, "the cap never released"
            time.sleep(0.1)
            continue
        break
    assert reply.status == 200, reply.text
    assert reply.json["binding"] == member.binding_id
    assert _counts(harness) == (before[0] + 1, before[1] + 1)


STALLED_RESPONSE_BYTES = 8 * 1024 * 1024
WRITE_DEADLINE_S = 0.5


class _LargeResponseResolver:
    """A stub resolver that answers one document far larger than any socket buffer."""

    def __init__(self) -> None:
        self.document = {"blob": "A" * STALLED_RESPONSE_BYTES}

    async def resolve(self, handle: str, slot: str, purpose: str) -> dict[str, Any]:
        return self.document


def _read_available(sock: socket.socket) -> bytes:
    """Read to EOF, a reset or the socket timeout; answer the bytes that arrived."""

    chunks: list[bytes] = []
    while True:
        try:
            chunk = sock.recv(1 << 20)
        except (ConnectionResetError, TimeoutError):
            break
        if not chunk:
            break
        chunks.append(chunk)
    return b"".join(chunks)


def test_wire_a_stalled_reader_is_aborted_at_the_write_deadline_and_frees_its_slot() -> None:
    """A peer that never reads cannot hold a connection slot past the write deadline.

    This is a component test of the HTTP layer, so the resolver behind it is a
    stub whose JSON response (about 8 MiB) is far above the Unix socket
    buffers. With ``max_connections=1`` and ``write_timeout=0.5``, a client
    sends a valid request and never reads. Within a bounded time the server
    aborts that transport, so a second client is accepted and receives the
    full response, while the stalled client sees an aborted connection with
    less than the full body. A reading client's full response arrives intact.
    """

    resolver = _LargeResponseResolver()
    body = resolve_body(HANDLE_PREFIX + "A" * 43, SLOT)
    request = request_head(len(body)) + body
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    clients: list[socket.socket] = []

    def on_loop(coroutine: Any) -> Any:
        return asyncio.run_coroutine_threadsafe(coroutine, loop).result(STATEMENT_WAIT_S)

    async def stop(server: asyncio.Server) -> None:
        server.close()
        await asyncio.wait_for(server.wait_closed(), STATEMENT_WAIT_S)

    with tempfile.TemporaryDirectory(prefix="crs") as directory:
        path = os.path.join(directory, "r.sock")
        server: asyncio.Server | None = None
        try:
            server = on_loop(
                serve_unix(
                    resolver,  # type: ignore[arg-type]
                    path,
                    max_connections=1,
                    write_timeout=WRITE_DEADLINE_S,
                )
            )
            stalled = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            clients.append(stalled)
            stalled.settimeout(FAST_REPLY_S)
            stalled.connect(path)
            stalled.sendall(request)
            started = time.monotonic()

            busy = 0
            while True:
                probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                clients.append(probe)
                probe.settimeout(FAST_REPLY_S)
                probe.connect(path)
                try:
                    probe.sendall(request)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                data = _read_available(probe)
                probe.close()
                if data:
                    reply = parse_raw(data)
                    if reply.status == 200:
                        break
                    reply.refused(503, "busy")
                busy += 1
                assert time.monotonic() - started < FAST_REPLY_S, (
                    "the stalled reader's slot was never freed"
                )
                time.sleep(0.1)

            # The slot was held while the reader stalled, then freed by the abort.
            assert busy >= 1, "the stalled connection did not hold the only slot"
            assert reply.json == resolver.document
            assert len(data) > STALLED_RESPONSE_BYTES

            # The stalled client was aborted: it gets less than the full response.
            leftover = _read_available(stalled)
            assert len(leftover) < len(data), "the stalled connection was never aborted"
        finally:
            for client in clients:
                client.close()
            try:
                if server is not None:
                    on_loop(stop(server))
            finally:
                loop.call_soon_threadsafe(loop.stop)
                thread.join(STATEMENT_WAIT_S)
                loop.close()


# =============================================================================
# AC2: the invocation registry
# =============================================================================


def test_handle_shape_is_crh1_and_256_random_bits() -> None:
    registry, principal = _pure_registry()
    seen: set[str] = set()
    for _ in range(512):
        invocation = registry.register(_context(principal))
        value = invocation.handle
        assert HANDLE_SHAPE.fullmatch(value), value
        assert value.startswith(HANDLE_PREFIX) and len(value) == 48
        raw = base64.urlsafe_b64decode(value[len(HANDLE_PREFIX) :] + "=")
        assert len(raw) == 32
        assert value not in repr(invocation) and value not in str(invocation)
        with pytest.raises(AttributeError):
            invocation.handle = "crh1." + "A" * 43  # type: ignore[misc]
        seen.add(value)
    assert len(seen) == 512
    assert registry.active_invocations == 512


def test_handle_context_reaches_the_resolver_through_the_endpoint(fleet: Fleet) -> None:
    app = fleet.application()
    first, second = app.add_member(), app.add_member()
    harness = fleet.launch(app)
    run, work_item = str(uuid.uuid4()), str(uuid.uuid4())
    invocation = harness.register(first, run=run, work_item=work_item, expires_in=777)
    other = harness.register(second)

    context = harness.handles.admit(invocation.handle)
    assert context == invocation.context
    assert (context.run, context.work_item) == (run, work_item)
    assert context.principal.agent_id == first.agent_id

    for registered, member in ((invocation, first), (other, second)):
        reply = harness.resolve(registered.handle)
        assert reply.status == 200, reply.text
        assert (reply.json["binding"], reply.json["account"]) == (
            member.binding_id,
            member.account,
        )
        assert reply.json["expires_at"] == registered.context.expires_at
        assert reply.json["credential"]["fields"][STATIC_FIELD] == {"text": member.text}

    # The alias and the policy generation are part of the context that authorizes.
    before = _counts(harness)
    harness.resolve(harness.handle(first, alias="secondary")).refused(403, "not_member")
    stale = harness.handle(first, generation=app.generation() - 1)
    harness.resolve(stale).refused(403, "policy_generation_mismatch")
    assert _counts(harness)[1] == before[1]


def test_handle_remove_close_and_session_close_revoke(fleet: Fleet) -> None:
    app, member, harness = _static(fleet)
    principal = harness.principal(member)
    harness.handles.open_session("session-a", principal)
    harness.handles.open_session("session-b", principal)
    removed = harness.register(member)
    closed = harness.register(member)
    in_session = harness.register(member, session_id="session-a")
    survivor = harness.register(member, session_id="session-b")
    for invocation in (removed, closed, in_session, survivor):
        assert harness.resolve(invocation.handle).status == 200

    assert harness.handles.remove(removed.handle) is True
    assert harness.handles.remove(removed.handle) is False
    closed.close()
    closed.close()
    harness.handles.close_session("session-a")
    assert harness.handles.sessions == 1

    before = _counts(harness)
    for invocation in (removed, closed, in_session):
        harness.resolve(invocation.handle).refused(401, "handle_unknown")
        with _refused("handle_unknown"):
            harness.handles.admit(invocation.handle)
    assert _counts(harness) == before
    assert harness.resolve(survivor.handle).status == 200


def test_handle_expiry_refuses_then_forgets(fleet: Fleet) -> None:
    app, member, harness = _static(fleet)
    expires_at = int(time.time()) + 2
    handle = harness.handle(member, expires_at=expires_at)
    assert harness.resolve(handle).status == 200
    while time.time() < expires_at + 0.05:
        time.sleep(0.05)
    before = _counts(harness)
    harness.resolve(handle).refused(401, "handle_expired")
    harness.resolve(handle).refused(401, "handle_unknown")
    assert _counts(harness) == before

    active = harness.handles.active_invocations
    with _refused("invalid_context"):
        harness.register(member, expires_at=int(time.time()))
    assert harness.handles.active_invocations == active


def test_handle_invocation_bound_refuses_without_drawing_randomness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [time.time()]
    registry, principal = _pure_registry(clock=lambda: now[0])
    draws: list[int] = []
    original = handles_module.secrets.token_bytes

    def spy(size: int | None = None) -> bytes:
        draws.append(size or 0)
        return original(size)

    monkeypatch.setattr(handles_module.secrets, "token_bytes", spy)
    lapse = int(now[0]) + 100
    for _ in range(MAX_INVOCATIONS):
        registry.register(_context(principal, expires_at=lapse))
    assert MAX_INVOCATIONS == 1024
    assert registry.active_invocations == 1024
    assert len(draws) == 1024 and set(draws) == {32}

    with _refused("invocations_full"):
        registry.register(_context(principal, expires_at=lapse + 500))
    assert registry.active_invocations == 1024
    assert len(draws) == 1024

    # The bound counts live invocations: once they lapse they are pruned.
    now[0] = lapse + 1
    registry.register(_context(principal, expires_at=lapse + 600))
    assert registry.active_invocations == 1
    assert len(draws) == 1025


def test_handle_session_bound_refuses_and_stores_nothing() -> None:
    registry, principal = _pure_registry()
    for index in range(MAX_SESSIONS):
        registry.open_session(f"s-{index}", principal)
    assert MAX_SESSIONS == 4096
    assert registry.sessions == 4096
    with _refused("sessions_full"):
        registry.open_session("s-overflow", principal)
    assert registry.sessions == 4096
    with _refused("session_unknown"):
        registry.register(_context(principal, session_id="s-overflow"))
    assert registry.active_invocations == 0

    # Liveness: an already open session still takes invocations, and closing
    # one makes room.
    registry.register(_context(principal, session_id="s-7"))
    registry.close_session("s-0")
    registry.open_session("s-overflow", principal)
    assert registry.sessions == 4096


def test_handle_session_id_is_never_identity(fleet: Fleet) -> None:
    app, member, harness = _static(fleet)
    principal = harness.principal(member)
    harness.handles.open_session("sess-identity", principal)
    harness.register(member, session_id="sess-identity")
    with _refused("handle_unknown"):
        harness.handles.admit("sess-identity")

    before = _counts(harness)
    for presented in (
        "sess-identity",
        HANDLE_PREFIX + "é" * 43,
        HANDLE_PREFIX + "A" * 200,
        HANDLE_PREFIX + "A" * 43,
    ):
        harness.resolve(presented).refused(401, "handle_unknown")
    assert _counts(harness) == before


def test_handle_session_principal_mismatch_refuses() -> None:
    registry, principal = _pure_registry()
    other = Principal(
        installation=principal.installation,
        application=principal.application,
        agent_id=str(uuid.uuid4()),
        agent="agent-other",
        policy_generation=1,
    )
    registry.open_session("shared-session", principal)
    registry.open_session("shared-session", principal)
    assert registry.sessions == 1
    with _refused("session_principal_mismatch"):
        registry.open_session("shared-session", other)
    with _refused("session_principal_mismatch"):
        registry.register(_context(other, session_id="shared-session"))
    assert registry.active_invocations == 0
    registry.register(_context(principal, session_id="shared-session"))
    assert registry.active_invocations == 1


def test_handle_foreign_principal_refuses() -> None:
    registry, principal = _pure_registry()
    for change in (
        {"installation": random_installation()},
        {"application": str(uuid.uuid4())},
    ):
        foreign = dataclasses.replace(principal, **change)
        with _refused("foreign_principal") as caught:
            registry.register(_context(foreign))
        assert caught.value.code == "foreign_principal"
        with _refused("foreign_principal"):
            registry.open_session("foreign-session", foreign)
    assert (registry.active_invocations, registry.sessions) == (0, 0)


@pytest.mark.parametrize(
    "variant",
    [
        "agent-not-uuid",
        "agent-uppercase",
        "run-unpaired",
        "work-item-unpaired",
        "alias-invalid",
        "generation-zero",
        "already-lapsed",
    ],
)
def test_handle_invalid_context_refuses(variant: str) -> None:
    registry, principal = _pure_registry()
    options: dict[str, Any] = {}
    who = principal
    if variant == "agent-not-uuid":
        who = dataclasses.replace(principal, agent_id="not-a-uuid")
    elif variant == "agent-uppercase":
        who = dataclasses.replace(principal, agent_id=principal.agent_id.upper())
    elif variant == "run-unpaired":
        options["run"] = str(uuid.uuid4())
    elif variant == "work-item-unpaired":
        options["work_item"] = str(uuid.uuid4())
    elif variant == "alias-invalid":
        options["alias"] = "Bad_Alias"
    elif variant == "generation-zero":
        who = dataclasses.replace(principal, policy_generation=0)
    else:
        options["expires_at"] = int(time.time()) - 1
    with _refused("invalid_context"):
        registry.register(_context(who, **options))
    assert registry.active_invocations == 0

    # Liveness: the paired, canonical context registers.
    registry.register(_context(principal, run=str(uuid.uuid4()), work_item=str(uuid.uuid4())))
    assert registry.active_invocations == 1


# =============================================================================
# AC3: custody, start refusal and the per-call transaction
# =============================================================================


def test_admission_connects_only_as_the_generated_role(fleet: Fleet) -> None:
    app = fleet.application()
    member = app.add_member()
    harness = fleet.launch(app, extra={"DATABASE_URL": owner_dsn()})
    assert harness.config.dsn == app.dsn
    key_text = base64.b64encode(bytes(app.recipient.private_key)).decode()
    password = dsn_password(app.dsn)
    assert key_text not in repr(harness.config) and password not in repr(harness.config)
    assert harness.resolve(harness.handle(member)).status == 200

    rows = sql_dicts(
        "SELECT usename::text AS usename FROM pg_stat_activity "
        "WHERE datname = current_database() AND application_name = :name",
        {"name": RESOLVER_APPLICATION_NAME},
    )
    assert rows, "the resolver holds no database session"
    assert {row["usename"] for row in rows} == {app.role_name}


def test_admission_start_refuses_the_owner_dsn(fleet: Fleet, owner_flags: dict[str, Any]) -> None:
    order = (
        ("rolsuper", "role_superuser"),
        ("rolcreaterole", "role_createrole"),
        ("rolcreatedb", "role_createdb"),
        ("rolreplication", "role_replication"),
        ("rolbypassrls", "role_bypassrls"),
    )
    expected = next((code for flag, code in order if owner_flags[flag]), None)
    assert expected is not None, "the test owner role carries none of the refused flags"
    app = fleet.application()
    member = app.add_member()
    code, _ = fleet.start_refused(app, dsn_text=owner_dsn(), encoding="managed-raw")
    assert code == expected
    wait_for_no_resolver_backends(str(sql_dicts("SELECT current_user AS u")[0]["u"]))

    harness = fleet.launch(app)
    assert harness.resolve(harness.handle(member)).status == 200


SHARED_BINDINGS = "curie.shared_connector_bindings"
COMMIT_FUNCTION = "curie.shared_connector_refresh_commit(text, uuid, bigint, text, bytea)"
WIDENINGS = {
    "createrole": (
        "ALTER ROLE {role} CREATEROLE",
        "ALTER ROLE {role} NOCREATEROLE",
        "role_createrole",
    ),
    "update-enabled": (
        f"GRANT UPDATE (enabled) ON {SHARED_BINDINGS} TO {{role}}",
        f"REVOKE UPDATE (enabled) ON {SHARED_BINDINGS} FROM {{role}}",
        "role_write_authority",
    ),
    "execute-revoked": (
        f"REVOKE EXECUTE ON FUNCTION {COMMIT_FUNCTION} FROM {{role}}",
        f"GRANT EXECUTE ON FUNCTION {COMMIT_FUNCTION} TO {{role}}",
        "role_missing_execute",
    ),
    "insert-lock-guard": (
        f"GRANT INSERT (lock_guard) ON {SHARED_BINDINGS} TO {{role}}",
        f"REVOKE INSERT (lock_guard) ON {SHARED_BINDINGS} FROM {{role}}",
        "role_write_authority",
    ),
    "member-of-role": (
        "GRANT {plain} TO {role}",
        "REVOKE {plain} FROM {role}",
        "role_inherits_membership",
    ),
    # cp5: the caller verify-key tables are read by the proxy through this
    # role, so SELECT on each is required like the other six tables.
    "caller-keys-select-revoked": (
        "REVOKE SELECT ON curie.shared_connector_caller_keys FROM {role}",
        "GRANT SELECT ON curie.shared_connector_caller_keys TO {role}",
        "role_missing_select",
    ),
    "caller-key-heartbeats-select-revoked": (
        "REVOKE SELECT ON curie.shared_connector_caller_key_heartbeats FROM {role}",
        "GRANT SELECT ON curie.shared_connector_caller_key_heartbeats TO {role}",
        "role_missing_select",
    ),
    # Any privilege outside the eight shared tables and four functions is
    # foreign authority, whether a table grant or schema CREATE (security
    # review S3).
    "select-on-agents": (
        "GRANT SELECT ON curie.agents TO {role}",
        "REVOKE SELECT ON curie.agents FROM {role}",
        "role_foreign_authority",
    ),
    "create-on-schema": (
        "GRANT CREATE ON SCHEMA curie TO {role}",
        "REVOKE CREATE ON SCHEMA curie FROM {role}",
        "role_foreign_authority",
    ),
}


@pytest.mark.parametrize("widening", list(WIDENINGS))
def test_admission_start_refuses_a_widened_role_and_starts_after_revert(
    fleet: Fleet, sessions: SessionPool, cleanup: SharedCleanup, widening: str
) -> None:
    app = fleet.application()
    member = app.add_member()
    owner = sessions.owner()
    plain = cleanup.role(f"sca_plain_{secrets.token_hex(6)}")
    owner.execute(f"CREATE ROLE {quote_ident(plain)} NOLOGIN")
    names = {"role": quote_ident(app.role_name), "plain": quote_ident(plain)}
    apply, revert, expected = WIDENINGS[widening]

    owner.execute(apply.format(**names))
    try:
        code, _ = fleet.start_refused(app)
        assert code == expected
        wait_for_no_resolver_backends(app.role_name)
    finally:
        owner.execute(revert.format(**names))

    harness = fleet.launch(app)
    assert harness.resolve(harness.handle(member)).status == 200


def test_admission_start_refuses_an_executable_definer_trigger_function(
    fleet: Fleet, sessions: SessionPool
) -> None:
    """A SECURITY DEFINER trigger function is foreign authority too (code review r2).

    A role that can execute one and create temp tables can attach it to its
    own temp table and fire privileged writes, so the trigger return type
    earns no exemption.
    """

    app = fleet.application()
    member = app.add_member()
    owner = sessions.owner()
    function = f"curie.sca_definer_trigger_{secrets.token_hex(6)}"
    role = quote_ident(app.role_name)

    try:
        owner.execute(
            f"CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql "
            "SECURITY DEFINER AS $$ BEGIN RETURN NEW; END $$"
        )
        owner.execute(f"REVOKE ALL ON FUNCTION {function}() FROM PUBLIC")
        owner.execute(f"GRANT EXECUTE ON FUNCTION {function}() TO {role}")
        code, _ = fleet.start_refused(app)
        assert code == "role_foreign_authority"
        wait_for_no_resolver_backends(app.role_name)
    finally:
        owner.execute(f"DROP FUNCTION IF EXISTS {function}()")

    harness = fleet.launch(app)
    assert harness.resolve(harness.handle(member)).status == 200


def test_admission_start_refuses_a_role_mapped_to_another_application(fleet: Fleet) -> None:
    mine, theirs = fleet.application(), fleet.application()
    member = mine.add_member()
    code, _ = fleet.start_refused(mine, keys=[theirs.recipient], dsn_text=theirs.sealed_dsn)
    assert code == "role_application_mismatch"
    wait_for_no_resolver_backends(theirs.role_name)
    harness = fleet.launch(mine)
    assert harness.resolve(harness.handle(member)).status == 200


@pytest.mark.parametrize(
    "name", ["CURIE_SHARED_SNAPSHOT_SEAL_FILE", "CURIE_CONNECTOR_CALLER_SIGNING_KEY"]
)
def test_admission_config_refuses_signer_or_snapshot_material(fleet: Fleet, name: str) -> None:
    app = fleet.application()
    member = app.add_member()
    sentinel = secret_sentinel("MOUNT")
    env, directory = fleet.env(app, extra={name: f"/run/secrets/{sentinel}"})
    with pytest.raises(ValueError) as refused:
        ResolverConfig.from_env(env)
    assert name in str(refused.value)
    assert sentinel not in str(refused.value)
    assert not os.path.exists(env["CURIE_SHARED_RESOLVER_SOCKET"])

    del env[name]
    harness = fleet.launch(app, env=(env, directory))
    assert harness.resolve(harness.handle(member)).status == 200


def test_admission_one_invocation_across_disables_holds_no_cached_authorization(
    fleet: Fleet,
) -> None:
    app = fleet.application()
    first, second = app.add_member(), app.add_member()
    harness = fleet.launch(app)
    one, two = harness.handle(first), harness.handle(second)
    assert harness.resolve(one).status == 200
    assert harness.resolve(two).status == 200

    disabled = app.store.disable_binding(first.binding_id)
    assert disabled.status_code == 200, disabled.text
    releases = harness.releases
    harness.resolve(one).refused(403, "binding_disabled")
    assert harness.resolve(two).status == 200

    disabled = app.store.disable_application(app.application_id)
    assert disabled.status_code == 200, disabled.text
    harness.resolve(two).refused(403, "application_disabled")
    harness.resolve(one).refused(403, "application_disabled")
    assert harness.releases == releases + 1


def test_admission_alias_and_generation_are_read_per_call(fleet: Fleet) -> None:
    app = fleet.application()
    member = app.add_member()
    harness = fleet.launch(app)
    old = harness.handle(member)
    assert harness.resolve(old).status == 200
    harness.resolve(harness.handle(member, alias="secondary")).refused(403, "not_member")

    widened = app.store.put_members(
        app.application_id,
        member.agent_id,
        [{"alias": "primary", "tools": None}, {"alias": "secondary", "tools": None}],
    )
    assert widened.status_code == 200, widened.text
    harness.resolve(old).refused(403, "policy_generation_mismatch")
    assert harness.resolve(harness.handle(member, alias="secondary")).status == 200

    removed = app.store.operator(
        "DELETE", f"/applications/{app.application_id}/members/{member.agent_id}"
    )
    assert removed.status_code == 200, removed.text
    harness.resolve(harness.handle(member)).refused(403, "not_member")


def test_admission_waits_on_a_binding_lock_then_sees_the_disable(
    fleet: Fleet, sessions: SessionPool
) -> None:
    app = fleet.application()
    member = app.add_member()
    harness = fleet.launch(app)
    handle = harness.handle(member)
    assert harness.resolve(handle).status == 200

    owner = sessions.owner()
    owner.execute("BEGIN")
    owner_pid = int(owner.fetchval("SELECT pg_backend_pid()"))
    owner.fetch(f"SELECT id FROM {SHARED_BINDINGS} WHERE id = $1 FOR UPDATE", member.binding_uuid)
    owner.execute(
        f"UPDATE {SHARED_BINDINGS} SET enabled = false, disabled_at = now() WHERE id = $1",
        member.binding_uuid,
    )
    pending = in_background(lambda: harness.resolve(handle))
    parked = wait_for_parked_resolver(app.role_name)
    assert owner_pid in blocking_pids(parked)
    time.sleep(1.0)
    assert not pending.done(), "the resolve finished while the binding lock was held"
    owner.execute("COMMIT")
    pending.result(timeout=STATEMENT_WAIT_S).refused(403, "binding_disabled")


def test_admission_share_lock_holds_off_an_application_disable(
    fleet: Fleet, sessions: SessionPool
) -> None:
    app = fleet.application()
    member = app.add_member()
    harness = fleet.launch(app)
    handle = harness.handle(member)

    owner = sessions.owner()
    owner.execute("BEGIN")
    owner.fetch(f"SELECT id FROM {SHARED_BINDINGS} WHERE id = $1 FOR UPDATE", member.binding_uuid)
    pending = in_background(lambda: harness.resolve(handle))
    resolver_pid = wait_for_parked_resolver(app.role_name)
    disable = in_background(lambda: app.store.disable_application(app.application_id))
    contender = wait_for_backend_blocked_by(resolver_pid)
    assert resolver_pid in blocking_pids(contender)
    time.sleep(1.0)
    assert not disable.done(), "the disable committed under the resolver's share lock"
    assert not pending.done()

    owner.execute("COMMIT")
    admitted = pending.result(timeout=STATEMENT_WAIT_S)
    assert admitted.status == 200, admitted.text
    assert admitted.json["binding"] == member.binding_id
    acknowledged = disable.result(timeout=STATEMENT_WAIT_S)
    assert acknowledged.status_code == 200, acknowledged.text
    assert acknowledged.json()["acknowledged"] is True
    harness.resolve(handle).refused(403, "application_disabled")


def test_admission_share_lock_holds_off_a_membership_change(
    fleet: Fleet, sessions: SessionPool
) -> None:
    app = fleet.application()
    member = app.add_member()
    harness = fleet.launch(app)
    generation = app.generation()
    old = harness.handle(member)

    owner = sessions.owner()
    owner.execute("BEGIN")
    owner.fetch(f"SELECT id FROM {SHARED_BINDINGS} WHERE id = $1 FOR UPDATE", member.binding_uuid)
    pending = in_background(lambda: harness.resolve(old))
    resolver_pid = wait_for_parked_resolver(app.role_name)
    change = in_background(
        lambda: app.store.put_members(
            app.application_id,
            member.agent_id,
            [{"alias": "primary", "tools": None}, {"alias": "secondary", "tools": None}],
        )
    )
    # The membership trigger's NO KEY UPDATE must wait on the RESOLVER's FOR
    # SHARE (a FOR KEY SHARE would not hold it off).
    contender = wait_for_backend_blocked_by(resolver_pid)
    assert resolver_pid in blocking_pids(contender)
    time.sleep(1.0)
    assert not change.done(), "the membership change committed under the resolver's lock"

    owner.execute("COMMIT")
    admitted = pending.result(timeout=STATEMENT_WAIT_S)
    assert admitted.status == 200, admitted.text
    changed = change.result(timeout=STATEMENT_WAIT_S)
    assert changed.status_code == 200, changed.text
    assert changed.json()["policy_generation"] > generation
    harness.resolve(old).refused(403, "policy_generation_mismatch")
    assert harness.resolve(harness.handle(member)).status == 200


REVOCATIONS_WHILE_PARKED = {
    "removed": (401, "handle_unknown"),
    "session-closed": (401, "handle_unknown"),
    "clock-expired": (401, "handle_expired"),
    "left-alone": None,
}


@pytest.mark.parametrize("revocation", list(REVOCATIONS_WHILE_PARKED))
def test_admission_revalidates_the_invocation_after_waiting_on_a_lock(
    fleet: Fleet, sessions: SessionPool, revocation: str
) -> None:
    """A handle revoked while its resolve waits on a lock never releases (review F1).

    The registry check before the database wait proves nothing about the
    moment of release: the resolve must re-check the handle after every
    blocking step and refuse with the registry's own code.
    """

    sentinel = secret_sentinel("PARKED")
    now = [time.time()]
    app = fleet.application()
    member = app.add_member(fields={STATIC_FIELD: sentinel})
    registry = HandleRegistry(
        installation=app.installation, application=app.application_id, clock=lambda: now[0]
    )
    harness = fleet.launch(app, handles=registry)
    principal = harness.principal(member)
    registry.open_session("parked-session", principal)
    expires_at = int(now[0]) + 600
    invocation = harness.register(
        member,
        expires_at=expires_at,
        session_id="parked-session" if revocation == "session-closed" else None,
    )
    handle = invocation.handle
    assert harness.resolve(handle).status == 200

    owner = sessions.owner()
    owner.execute("BEGIN")
    owner_pid = int(owner.fetchval("SELECT pg_backend_pid()"))
    owner.fetch(f"SELECT id FROM {SHARED_BINDINGS} WHERE id = $1 FOR UPDATE", member.binding_uuid)
    before = _counts(harness)
    pending = in_background(lambda: harness.resolve(handle))
    parked = wait_for_parked_resolver(app.role_name)
    assert owner_pid in blocking_pids(parked)
    assert not pending.done(), "the resolve finished while the binding lock was held"

    if revocation == "removed":
        assert registry.remove(handle) is True
    elif revocation == "session-closed":
        registry.close_session("parked-session")
    elif revocation == "clock-expired":
        now[0] = expires_at + 1
    owner.execute("COMMIT")
    reply = pending.result(timeout=STATEMENT_WAIT_S)

    expected = REVOCATIONS_WHILE_PARKED[revocation]
    if expected is None:
        # Liveness: the same parked resolve, its handle untouched, is released.
        assert reply.status == 200, reply.text
        assert reply.json["credential"]["fields"][STATIC_FIELD] == {"text": sentinel}
        assert harness.releases == before[1] + 1
        return
    reply.refused(*expected)
    _no_secret(reply.text, sentinel, handle)
    assert harness.releases == before[1], "a revoked handle released a credential"
    assert harness.admissions >= before[0]

    # Liveness: a fresh handle for the same member still releases.
    now[0] = time.time()
    fresh = harness.resolve(harness.handle(member, expires_at=int(now[0]) + 600))
    assert fresh.status == 200, fresh.text
    assert harness.releases == before[1] + 1


def test_admission_key_unavailable_refuses_without_plaintext(
    fleet: Fleet, caplog: pytest.LogCaptureFixture
) -> None:
    app, member, harness = _static(fleet)
    handle = harness.handle(member)
    assert harness.resolve(handle).status == 200
    newer = app.store.register_recipient(app.application_id)
    rotated = secret_sentinel("ROTATED")
    reimported = member.reimport(
        envelope({SLOT: {STATIC_FIELD: rotated}}), resource_version="101", recipient=newer
    )
    assert reimported.status_code == 200, reimported.text

    with debug_capture(caplog):
        before = _counts(harness)
        reply = harness.resolve(handle)
    reply.refused(503, "key_unavailable")
    assert _counts(harness) == (before[0] + 1, before[1])
    _no_secret(reply.text + log_text(caplog), rotated, handle)

    # Liveness: a key file carrying the new key (current first) releases it.
    rekeyed = fleet.launch(app, keys=[newer, app.recipient])
    released = rekeyed.resolve(rekeyed.handle(member))
    assert released.status == 200, released.text
    assert released.json["revision"] == 2
    assert released.json["credential"]["fields"][STATIC_FIELD] == {"text": rotated}


def test_admission_database_outage_fails_closed_and_recovers(
    fleet: Fleet, sessions: SessionPool
) -> None:
    app, member, harness = _static(fleet)
    handle = harness.handle(member)
    assert harness.resolve(handle).status == 200
    owner = sessions.owner()
    role = quote_ident(app.role_name)
    owner.execute(f"ALTER ROLE {role} NOLOGIN")
    try:
        terminate_role_backends(app.role_name)
        releases = harness.releases
        for _ in range(2):
            harness.resolve(handle).refused(503, "store_unavailable")
        assert harness.releases == releases
    finally:
        owner.execute(f"ALTER ROLE {role} LOGIN")
    recovered = settle(harness, handle)
    assert recovered.status == 200, recovered.text
    assert recovered.json["binding"] == member.binding_id


def test_admission_another_applications_binding_is_invisible(
    fleet: Fleet, sessions: SessionPool
) -> None:
    mine, theirs = fleet.application(), fleet.application()
    own = mine.add_member()
    stranger = theirs.add_member()
    # The other application's member also joins mine, but holds no binding here.
    joined = mine.store.put_members(
        mine.application_id, stranger.agent_id, [{"alias": "primary", "tools": None}]
    )
    assert joined.status_code == 200, joined.text
    harness = fleet.launch(mine)
    harness.resolve(harness.handle(own, agent_id=stranger.agent_id)).refused(403, "binding_missing")
    assert (
        sessions.open(mine.dsn).fetch(
            f"SELECT id FROM {SHARED_BINDINGS} WHERE id = $1", stranger.binding_uuid
        )
        == []
    )

    assert harness.resolve(harness.handle(own)).json["binding"] == own.binding_id
    other = fleet.launch(theirs)
    assert other.resolve(other.handle(stranger)).json["binding"] == stranger.binding_id


# --- transitions into recovery serialize with in-flight admissions -------------


def _wait_for_lapsed_lease(member: Member) -> None:
    deadline = time.monotonic() + LOCK_WAIT_S
    while not sql_dicts(
        "SELECT now() > lease_expires_at AS lapsed "
        "FROM curie.shared_connector_refresh_intents WHERE binding_id = :id",
        {"id": member.binding_uuid},
    )[0]["lapsed"]:
        assert time.monotonic() < deadline, "the one-second lease never lapsed"
        time.sleep(0.1)


def _waits_on_the_resolve(
    transition: concurrent.futures.Future[Any], waiter_pid: int, resolver_pid: int, name: str
) -> None:
    """``transition`` must be a lock waiter blocked by the paused resolve, and stay one."""

    deadline = time.monotonic() + LOCK_WAIT_S
    while resolver_pid not in blocking_pids(waiter_pid):
        if transition.done():
            answer = transition.exception() or transition.result()
            answer = answer["outcome"] if hasattr(answer, "keys") else answer
            raise AssertionError(
                f"{name} completed while a resolve held the binding (answered {answer!r})"
            )
        assert time.monotonic() < deadline, f"{name} neither waited on the resolve nor completed"
        time.sleep(0.05)
    time.sleep(1.0)
    assert not transition.done(), f"{name} completed while a resolve held the binding"


def _paused_resolve_against(
    fleet: Fleet,
    provider: FakeProvider,
    sessions: SessionPool,
    *,
    lapsed: bool,
    transition: Any,
    name: str,
) -> tuple[Member, ResolverHarness, str, Any]:
    """A resolve paused inside its source while ``transition`` runs on another connection.

    The intent is ``refreshing`` under another owner (begun through the
    application role) and the material is fresh, so the resolve is admitted and
    its source runs; the source then holds inside the admission transaction,
    after the application and binding locks. ``transition(session, began)``
    submits the transition into ``recovery_required`` on a second
    application-role connection, which must wait for the resolve's commit.
    ``lapsed`` begins with a one-second lease and lets it expire before the
    resolve starts. Answers the member, the harness, the released access token and the
    transition's result.
    """

    app, member, harness, source, access = _refreshing(
        fleet, provider, expires_in=3600, source_type=PausingRefreshSource
    )
    assert isinstance(source, PausingRefreshSource)
    began = sessions.open(app.dsn).fetchrow(
        BEGIN_SQL, member.grant_id, "crashed-owner", 1 if lapsed else 60
    )
    assert began is not None and began["outcome"] == "began"
    if lapsed:
        _wait_for_lapsed_lease(member)
    contender = sessions.open(app.dsn)
    contender_pid = int(contender.fetchval("SELECT pg_backend_pid()"))
    handle = harness.handle(member)

    source.arm()
    pending = in_background(lambda: harness.resolve(handle))
    try:
        source.wait_entered(LOCK_WAIT_S)
        resolver_pid = wait_for_resolver_in_transaction(app.role_name)
        transitioned = transition(contender, member, began)
        _waits_on_the_resolve(transitioned, contender_pid, resolver_pid, name)
    finally:
        source.open()
    # The resolve committed before the recovery did, so its release was admitted.
    assert _released_access(pending.result(timeout=STATEMENT_WAIT_S)) == access
    return member, harness, access, transitioned.result(timeout=STATEMENT_WAIT_S)


def test_admission_a_resolve_in_flight_holds_off_mark_recovery(
    fleet: Fleet, provider: FakeProvider, sessions: SessionPool
) -> None:
    """Marking recovery waits for an in-flight resolve to commit (review F-pkg-r1).

    ADR 0239 "Static sources" 5 and 6: once ``recovery_required`` has
    committed, no resolve may release the grant's token. A resolve admitted on
    an intent read before the mark must therefore commit before the mark can,
    or the credential leaves after recovery began. The mark serializes through
    the binding row the resolve holds ``FOR SHARE``.
    """

    member, harness, access, marked = _paused_resolve_against(
        fleet,
        provider,
        sessions,
        lapsed=False,
        transition=lambda session, member, began: session.submit(
            "fetchval", MARK_RECOVERY_SQL, member.grant_id, began["intent_id"], began["epoch"]
        ),
        name="mark_recovery",
    )
    assert marked == "recovery_required"
    intent = intent_row(member.binding_id)
    assert (intent["state"], intent["recovery_reason"]) == (
        "recovery_required",
        "uncertain_outcome",
    )
    after = harness.resolve(harness.handle(member))
    after.refused(409, "recovery_required")
    assert access not in after.text


def test_admission_a_resolve_in_flight_holds_off_a_lapsed_lease_turning_into_recovery(
    fleet: Fleet, provider: FakeProvider, sessions: SessionPool
) -> None:
    """The other path into recovery, a begin on an expired lease, waits the same way."""

    member, harness, access, begun = _paused_resolve_against(
        fleet,
        provider,
        sessions,
        lapsed=True,
        transition=lambda session, member, began: session.submit(
            "fetchrow", BEGIN_SQL, member.grant_id, "next-owner", 60
        ),
        name="refresh_begin",
    )
    assert begun is not None and begun["outcome"] == "recovery_required", begun
    intent = intent_row(member.binding_id)
    assert (intent["state"], intent["recovery_reason"]) == ("recovery_required", "lease_expired")
    after = harness.resolve(harness.handle(member))
    after.refused(409, "recovery_required")
    assert access not in after.text


def test_admission_begin_and_mark_recovery_do_not_wait_without_a_resolve_in_flight(
    fleet: Fleet, provider: FakeProvider, sessions: SessionPool
) -> None:
    """Liveness for the two cases above: only an in-flight admission holds a transition off.

    A resolve that already committed holds nothing, so after one, a begin on a
    ``ready`` intent still answers ``began``, and both paths into recovery
    answer at once. (The mark's sequential refusal is also covered by
    ``test_seam_recovery_refuses_fresh_material_before_any_source_runs``.)
    """

    app, marked, harness, source, access = _refreshing(fleet, provider, expires_in=3600)
    lapsing = app.add_member(
        plaintext=refresh_material(
            SLOT, access, expires_at=int(time.time()) + 3600, refresh=secret_sentinel("LAPSING")
        ),
        refresh_grant=True,
    )
    for member in (marked, lapsing):
        assert _released_access(harness.resolve(harness.handle(member))) == access
    role = sessions.open(app.dsn)

    def prompt(method: str, sql: str, *args: Any) -> Any:
        return role.submit(method, sql, *args).result(timeout=FAST_REPLY_S)

    began = prompt("fetchrow", BEGIN_SQL, marked.grant_id, "owner-one", 60)
    assert began is not None and began["outcome"] == "began", began
    assert (
        prompt("fetchval", MARK_RECOVERY_SQL, marked.grant_id, began["intent_id"], began["epoch"])
        == "recovery_required"
    )

    first = prompt("fetchrow", BEGIN_SQL, lapsing.grant_id, "owner-two", 1)
    assert first is not None and first["outcome"] == "began", first
    _wait_for_lapsed_lease(lapsing)
    second = prompt("fetchrow", BEGIN_SQL, lapsing.grant_id, "owner-three", 60)
    assert second is not None and second["outcome"] == "recovery_required", second

    for member, reason in ((marked, "uncertain_outcome"), (lapsing, "lease_expired")):
        intent = intent_row(member.binding_id)
        assert (intent["state"], intent["recovery_reason"]) == ("recovery_required", reason)
        reply = harness.resolve(harness.handle(member))
        reply.refused(409, "recovery_required")
        assert access not in reply.text
    assert source.release_calls == 2


# =============================================================================
# AC4: the source plug-in seam
# =============================================================================


def test_seam_registry_refuses_duplicate_kinds_and_strangers(provider: FakeProvider) -> None:
    registry = default_registry()
    assert registry.kinds() == frozenset({"static", "connector"})
    for duplicate in (StaticSource(), ConnectorIdentitySource()):
        with pytest.raises(ValueError):
            registry.register(duplicate)
    with pytest.raises(ValueError):
        registry.register(object())  # type: ignore[arg-type]
    assert registry.kinds() == frozenset({"static", "connector"})
    with pytest.raises(SourceRefusal) as missing:
        registry.get("refresh")
    assert missing.value.code == "source_unavailable"

    registry.register(ProviderRefreshSource(provider))
    assert registry.kinds() == frozenset({"static", "connector", "refresh"})
    assert isinstance(registry.get("refresh"), ProviderRefreshSource)
    with pytest.raises(ValueError):
        registry.register(ProviderRefreshSource(provider))


def test_seam_a_registry_without_static_answers_source_unavailable(fleet: Fleet) -> None:
    identity_only = SourceRegistry()
    identity_only.register(ConnectorIdentitySource())
    app = fleet.application()
    member = app.add_member()
    harness = fleet.launch(app, sources=identity_only)
    before = _counts(harness)
    harness.resolve(harness.handle(member)).refused(503, "source_unavailable")
    assert _counts(harness) == (before[0] + 1, before[1])

    # Liveness: the identity source it does hold still answers.
    custom = fleet.application(slots=(), custody="connector")
    holder = custom.add_member()
    identity = SourceRegistry()
    identity.register(ConnectorIdentitySource())
    other = fleet.launch(custom, sources=identity)
    assert other.resolve(other.handle(holder), IDENTITY_SLOT).json["admission"] == "identity"


def test_seam_static_reimport_releases_the_new_revision(fleet: Fleet) -> None:
    app, member, harness = _static(fleet)
    handle = harness.handle(member)
    first = harness.resolve(handle)
    assert (first.json["revision"], first.json["credential"]["fields"][STATIC_FIELD]) == (
        1,
        {"text": member.text},
    )
    old_text, new_text = member.text, secret_sentinel("REVISION2")
    assert member.reimport_text(new_text, resource_version="101").status_code == 200

    second = harness.resolve(handle)
    assert second.status == 200, second.text
    assert second.json["revision"] == 2
    assert second.json["credential"]["fields"][STATIC_FIELD] == {"text": new_text}
    assert old_text not in second.text


def test_seam_source_stale_refuses_until_explicit_reimport(fleet: Fleet) -> None:
    app, member, harness = _static(fleet)
    handle = harness.handle(member)
    assert member.source is not None
    uid, recorded = member.source["uid"], member.source["resource_version"]

    current = app.store.check(member.binding_id, uid, recorded)
    assert current.json()["status"] == "current", current.text
    assert harness.resolve(handle).status == 200

    stale = app.store.check(member.binding_id, uid, "205")
    assert stale.json()["status"] == "source_stale", stale.text
    harness.resolve(handle).refused(409, "source_stale")
    # The flag persists: a later matching check still reports it, and the
    # resolver still refuses, until an explicit reimport.
    again = app.store.check(member.binding_id, uid, recorded)
    assert again.json()["status"] == "source_stale", again.text
    harness.resolve(handle).refused(409, "source_stale")

    fresh = secret_sentinel("CAUGHT-UP")
    assert member.reimport_text(fresh, resource_version="205").status_code == 200
    reply = harness.resolve(handle)
    assert reply.status == 200, reply.text
    assert reply.json["revision"] == 2
    assert reply.json["credential"]["fields"][STATIC_FIELD] == {"text": fresh}


def test_seam_rendered_origins_must_equal_the_stored_ones(fleet: Fleet) -> None:
    app = fleet.application()
    member = app.add_member()
    harness = fleet.launch(app, origins=["https://other.example.com"])
    harness.resolve(harness.handle(member)).refused(503, "source_unavailable")
    matching = fleet.launch(app)
    assert matching.resolve(matching.handle(member)).json["origins"] == list(app.origins)


# --- refresh-capable sources ---------------------------------------------------


def _refreshing(
    fleet: Fleet,
    provider: FakeProvider,
    *,
    expires_in: int,
    source_type: type[ProviderRefreshSource] = ProviderRefreshSource,
) -> tuple[SharedApp, Member, ResolverHarness, ProviderRefreshSource, str]:
    app = fleet.application(origins=(provider.origin,))
    access = f"access-{secrets.token_hex(8)}"
    material = refresh_material(
        SLOT, access, expires_at=int(time.time()) + expires_in, refresh=secret_sentinel("REFRESH")
    )
    member = app.add_member(plaintext=material, refresh_grant=True)
    source = source_type(provider)
    registry = default_registry()
    registry.register(source)
    harness = fleet.launch(app, sources=registry)
    return app, member, harness, source, access


def _released_access(reply: Any) -> str:
    assert reply.status == 200, reply.text
    fields = reply.json["credential"]["fields"]
    assert set(fields) == {ACCESS}, fields
    return str(fields[ACCESS]["text"])


def test_seam_fresh_refresh_material_releases_without_a_provider_call(
    fleet: Fleet, provider: FakeProvider
) -> None:
    app, member, harness, source, access = _refreshing(fleet, provider, expires_in=3600)
    reply = harness.resolve(harness.handle(member))
    assert _released_access(reply) == access
    assert reply.json["revision"] == 1
    assert provider.requests == []
    assert source.refresh_calls == 0
    intent = intent_row(member.binding_id)
    assert (intent["state"], intent["epoch"]) == ("ready", 0)


def test_seam_expired_material_refreshes_once_and_commits_before_release(
    fleet: Fleet, provider: FakeProvider
) -> None:
    app, member, harness, source, access = _refreshing(fleet, provider, expires_in=-60)
    reply = harness.resolve(harness.handle(member))
    issued = _released_access(reply)
    assert len(provider.calls(TOKEN_PATH)) == 1 and source.refresh_calls == 1
    assert provider.issued == [issued]
    assert issued != access
    assert reply.json["revision"] == 2

    # The released token is exactly the stored replacement.
    stored = binding_row(member.binding_id)
    assert stored["credential_revision"] == 2
    assert opened_fields(app.recipient, stored["ciphertext"])[ACCESS] == {"text": issued}
    intent = intent_row(member.binding_id)
    assert (intent["state"], intent["epoch"]) == ("ready", 1)

    # Now fresh: the next call releases it without the provider.
    assert _released_access(harness.resolve(harness.handle(member))) == issued
    assert len(provider.calls(TOKEN_PATH)) == 1


def test_seam_concurrent_refreshes_call_the_provider_once_and_one_is_busy(
    fleet: Fleet, provider: FakeProvider
) -> None:
    app, member, harness, source, _ = _refreshing(fleet, provider, expires_in=-60)
    gate = provider.block(TOKEN_PATH)
    first = in_background(lambda: harness.resolve(harness.handle(member)))
    gate.wait_entered()
    harness.resolve(harness.handle(member)).refused(409, "refresh_busy")
    provider.unblock(TOKEN_PATH)
    issued = _released_access(first.result(timeout=STATEMENT_WAIT_S))
    assert len(provider.calls(TOKEN_PATH)) == 1 and source.refresh_calls == 1
    assert provider.issued == [issued]


def test_seam_provider_failure_enters_recovery_until_reimport(
    fleet: Fleet, provider: FakeProvider
) -> None:
    app, member, harness, source, _ = _refreshing(fleet, provider, expires_in=-60)
    provider.fail(TOKEN_PATH)
    handle = harness.handle(member)
    before = binding_row(member.binding_id)
    harness.resolve(handle).refused(409, "recovery_required")
    intent = intent_row(member.binding_id)
    assert (intent["state"], intent["recovery_reason"]) == (
        "recovery_required",
        "uncertain_outcome",
    )
    assert binding_row(member.binding_id)["ciphertext"] == before["ciphertext"]

    provider.fail(TOKEN_PATH, False)
    for _ in range(2):
        harness.resolve(handle).refused(409, "recovery_required")
    assert len(provider.calls(TOKEN_PATH)) == 1 and source.refresh_calls == 1

    access = f"access-{secrets.token_hex(8)}"
    material = refresh_material(
        SLOT, access, expires_at=int(time.time()) + 3600, refresh=secret_sentinel("RESET")
    )
    assert member.reimport(material, resource_version="101").status_code == 200
    assert _released_access(harness.resolve(handle)) == access
    assert len(provider.calls(TOKEN_PATH)) == 1


def test_seam_reimport_during_a_refresh_makes_it_stale_and_discards_the_token(
    fleet: Fleet, provider: FakeProvider
) -> None:
    app, member, harness, source, _ = _refreshing(fleet, provider, expires_in=-60)
    gate = provider.block(TOKEN_PATH)
    pending = in_background(lambda: harness.resolve(harness.handle(member)))
    gate.wait_entered()

    access = f"access-{secrets.token_hex(8)}"
    material = refresh_material(
        SLOT, access, expires_at=int(time.time()) + 3600, refresh=secret_sentinel("REIMPORT")
    )
    # The reimport must not wait on the in-flight refresh: no lock is held across it.
    reimport = in_background(lambda: member.reimport(material, resource_version="101"))
    assert reimport.result(timeout=LOCK_WAIT_S).status_code == 200
    provider.unblock(TOKEN_PATH)

    reply = pending.result(timeout=STATEMENT_WAIT_S)
    reply.refused(409, "refresh_stale")
    (issued,) = provider.issued
    assert issued not in reply.text
    stored = binding_row(member.binding_id)
    assert stored["credential_revision"] == 2
    assert opened_fields(app.recipient, stored["ciphertext"])[ACCESS] == {"text": access}
    assert intent_row(member.binding_id)["state"] == "ready"
    follow = harness.resolve(harness.handle(member))
    assert _released_access(follow) == access
    assert issued not in follow.text


async def _admitted_now(dsn: str, context: InvocationContext, slot: str) -> Admitted:
    """An ``Admitted`` snapshot taken now, in our own transaction on the application role."""

    connection = await asyncpg.connect(dsn, timeout=STATEMENT_WAIT_S)
    try:
        async with connection.transaction():
            admitted: Admitted = await admit(connection, context, slot)
            return admitted
    finally:
        await connection.close()


async def _delayed_contender(
    dsn: str, source: ProviderRefreshSource, admitted: Admitted, harness: ResolverHarness
) -> str | None:
    """The public coordinator, run with a stale ``Admitted``; answer the refusal code or None."""

    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2)
    try:
        coordinator = RefreshCoordinator(owner=f"delayed-contender-{secrets.token_hex(4)}")
        await coordinator.run(pool, source, admitted, SLOT, harness.config.keys)
    except ResolveRefusal as refusal:
        return refusal.code
    finally:
        await pool.close()
    return None


def _no_recovery(member: Member) -> None:
    intent = intent_row(member.binding_id)
    assert intent["state"] == "ready", intent
    assert intent["recovery_reason"] is None, intent


@pytest.mark.parametrize("moved_on_by", ["completed-refresh", "fresh-reimport"])
def test_seam_a_delayed_contender_does_not_refresh_replaced_material(
    fleet: Fleet, provider: FakeProvider, moved_on_by: str
) -> None:
    """A refresh begun on a revision that moved on re-reads the material first (review F2).

    The ``Admitted`` is captured while the material is expired. Before the
    contender begins, another resolve completes the refresh, or an operator
    reimports fresh material. The begun row then carries material that no
    longer needs a refresh, so the provider (whose refresh token may rotate)
    must not be called, and the intent goes back to ``ready``.
    """

    app, member, harness, source, _ = _refreshing(fleet, provider, expires_in=-60)
    stale = asyncio.run(_admitted_now(app.dsn, harness.context(member), SLOT))
    assert stale.credential_revision == 1 and stale.refresh is not None

    if moved_on_by == "completed-refresh":
        current = _released_access(harness.resolve(harness.handle(member)))
        assert provider.issued == [current]
    else:
        current = f"access-{secrets.token_hex(8)}"
        material = refresh_material(
            SLOT, current, expires_at=int(time.time()) + 3600, refresh=secret_sentinel("FRESH")
        )
        assert member.reimport(material, resource_version="101").status_code == 200
    calls, refreshes = len(provider.calls(TOKEN_PATH)), source.refresh_calls
    assert binding_row(member.binding_id)["credential_revision"] == 2

    outcome = asyncio.run(_delayed_contender(app.dsn, source, stale, harness))
    assert len(provider.calls(TOKEN_PATH)) == calls, f"the provider was called ({outcome})"
    assert source.refresh_calls == refreshes
    _no_recovery(member)
    stored = binding_row(member.binding_id)
    assert opened_fields(app.recipient, stored["ciphertext"])[ACCESS]["text"] == current

    follow = harness.resolve(harness.handle(member))
    assert _released_access(follow) == current
    assert len(provider.calls(TOKEN_PATH)) == calls
    _no_recovery(member)


def test_seam_a_delayed_contender_still_refreshes_reimported_expired_material(
    fleet: Fleet, provider: FakeProvider
) -> None:
    """Liveness for the case above: begun material that still needs a refresh gets one."""

    app, member, harness, source, _ = _refreshing(fleet, provider, expires_in=-60)
    stale = asyncio.run(_admitted_now(app.dsn, harness.context(member), SLOT))
    expired = refresh_material(
        SLOT,
        f"access-{secrets.token_hex(8)}",
        expires_at=int(time.time()) - 60,
        refresh=secret_sentinel("STILL-EXPIRED"),
    )
    assert member.reimport(expired, resource_version="101").status_code == 200
    assert provider.calls(TOKEN_PATH) == []

    outcome = asyncio.run(_delayed_contender(app.dsn, source, stale, harness))
    assert outcome is None, outcome
    assert len(provider.calls(TOKEN_PATH)) == 1 and source.refresh_calls == 1
    (issued,) = provider.issued
    _no_recovery(member)
    stored = binding_row(member.binding_id)
    assert stored["credential_revision"] > 2
    assert opened_fields(app.recipient, stored["ciphertext"])[ACCESS] == {"text": issued}

    assert _released_access(harness.resolve(harness.handle(member))) == issued
    assert len(provider.calls(TOKEN_PATH)) == 1


def test_seam_recovery_refuses_fresh_material_before_any_source_runs(
    fleet: Fleet, provider: FakeProvider, sessions: SessionPool
) -> None:
    app, member, harness, source, access = _refreshing(fleet, provider, expires_in=3600)
    role = sessions.open(app.dsn)
    began = role.fetchrow(BEGIN_SQL, member.grant_id, "another-owner", 60)
    assert began is not None and began["outcome"] == "began"
    # Refreshing under another owner does not refuse by itself: the material is fresh.
    assert _released_access(harness.resolve(harness.handle(member))) == access
    released = source.release_calls
    assert released == 1

    assert (
        role.fetchval(MARK_RECOVERY_SQL, member.grant_id, began["intent_id"], began["epoch"])
        == "recovery_required"
    )
    reply = harness.resolve(harness.handle(member))
    reply.refused(409, "recovery_required")
    assert access not in reply.text
    assert source.release_calls == released
    assert provider.requests == []


def test_seam_commit_lost_after_provider_consumption_becomes_recovery(
    fleet: Fleet, provider: FakeProvider, sessions: SessionPool
) -> None:
    app, member, harness, source, _ = _refreshing(
        fleet, provider, expires_in=-60, source_type=ShortLeaseRefreshSource
    )
    before = binding_row(member.binding_id)
    owner = sessions.owner()
    role = quote_ident(app.role_name)
    gate = provider.block(TOKEN_PATH)
    pending = in_background(lambda: harness.resolve(harness.handle(member)))
    gate.wait_entered()
    owner.execute(f"ALTER ROLE {role} NOLOGIN")
    replies = []
    try:
        terminate_role_backends(app.role_name)
        provider.unblock(TOKEN_PATH)
        lost = pending.result(timeout=STATEMENT_WAIT_S)
        lost.refused(503, "store_unavailable")
        replies.append(lost)
        intent = intent_row(member.binding_id)
        assert intent["state"] == "refreshing", intent
        assert binding_row(member.binding_id)["ciphertext"] == before["ciphertext"]
        deadline = time.monotonic() + LOCK_WAIT_S
        while not sql_dicts(
            "SELECT now() > lease_expires_at AS lapsed "
            "FROM curie.shared_connector_refresh_intents WHERE binding_id = :id",
            {"id": member.binding_uuid},
        )[0]["lapsed"]:
            assert time.monotonic() < deadline, "the two-second lease never lapsed"
            time.sleep(0.1)
    finally:
        owner.execute(f"ALTER ROLE {role} LOGIN")

    recovered = settle(harness, harness.handle(member))
    recovered.refused(409, "recovery_required")
    replies.append(recovered)
    later = harness.resolve(harness.handle(member))
    later.refused(409, "recovery_required")
    replies.append(later)

    assert len(provider.calls(TOKEN_PATH)) == 1 and source.refresh_calls == 1
    (issued,) = provider.issued
    for reply in replies:
        assert issued not in reply.text
    intent = intent_row(member.binding_id)
    assert (intent["state"], intent["recovery_reason"]) == ("recovery_required", "lease_expired")
    after = binding_row(member.binding_id)
    assert after["ciphertext"] == before["ciphertext"]
    assert after["credential_revision"] == 1
