"""The real shared connector resolver, driven from a test (ADR 0239).

``Fleet`` registers applications through the real API (``SharedStore``),
provisions each one's bundled database role, and launches the real resolver
for it: the frozen ``CURIE_SHARED_*`` environment, the X25519 key file and
the API's sealed DSN file written into a short private directory, then
``ResolverConfig.from_env`` and ``start_resolver`` on a dedicated event loop
thread. Tests talk to the resolver over its Unix socket only, with
``http.client`` for ordinary requests and raw bytes for framing cases.

Reading the plan, one thing it leaves open is settled here so that every test
uses one value:

``IDENTITY_SLOT``: the API only registers a connector-custody application with
NO credential slots (``store.register_application`` refuses custody
``connector`` with any slot), yet the wire request always carries a ``slot``
and the service refuses a slot outside the declared ones. The tests read the
identity-only request as carrying the empty slot ``""``, which no declared
slot can equal (slot names match ``[A-Za-z_][A-Za-z0-9_]{0,127}``): an
application with no declared slots admits exactly ``""``, and an application
with declared slots refuses ``""`` as ``slot_unknown``. The 200 body echoes
``"slot": ""``.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import http.client
import json
import logging
import os
import shutil
import socket
import tempfile
import threading
import time
import uuid
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest
from _migration_support import render_url, sql_dicts
from _shared_connector_support import (
    LOCK_WAIT_S,
    SLOT,
    STATEMENT_WAIT_S,
    Recipient,
    SessionPool,
    SharedCleanup,
    SharedStore,
    b64,
    captured_log_text,
    client_fingerprint,
    secret_sentinel,
    source_metadata,
)
from curie_api.config import get_settings
from curie_connector_proxy.resolver.config import ResolverConfig
from curie_connector_proxy.resolver.handles import (
    HandleRegistry,
    Invocation,
    InvocationContext,
    Principal,
)
from curie_connector_proxy.resolver.http import (
    ResolverRuntime,
    ResolverStartRefused,
    start_resolver,
)
from curie_connector_proxy.resolver.sources import SourceRegistry
from curie_connector_proxy.resolver.wire import RESOLVE_PATH
from sqlalchemy.engine import make_url

IDENTITY_SLOT = ""
# A membership stamp no row has, so a context is strictly bound to its generation.
UNOBSERVED_MEMBERSHIP = "0" * 64
RESOLVER_APPLICATION_NAME = "curie-shared-resolver"
STATIC_FIELD = "token"
DEFAULT_ORIGINS = ("https://api.example.com",)
PURPOSE = "list open issues"

# Every logger a leak could reach, captured at DEBUG.
CAPTURED_LOGGERS = (
    "",
    "curie_connector_proxy",
    "curie_connector_proxy.resolver",
    "aiohttp",
    "aiohttp.access",
    "aiohttp.server",
    "aiohttp.web",
    "asyncio",
    "asyncpg",
)


# --- material ---------------------------------------------------------------


def envelope(slots: Mapping[str, Mapping[str, str | bytes]]) -> bytes:
    """The plan's frozen v1 static envelope, before sealing."""

    return json.dumps(
        {
            "version": 1,
            "slots": {
                slot: {
                    name: {"bytes": b64(value)} if isinstance(value, bytes) else {"text": value}
                    for name, value in fields.items()
                }
                for slot, fields in slots.items()
            },
        },
        separators=(",", ":"),
    ).encode()


def opened_fields(recipient: Recipient, ciphertext: bytes, slot: str = SLOT) -> dict[str, Any]:
    """The stored ciphertext opened with the test's key, as its v1 slot fields."""

    document = json.loads(recipient.open(ciphertext))
    assert document["version"] == 1, document.keys()
    fields: dict[str, Any] = document["slots"][slot]
    return fields


# --- the socket -------------------------------------------------------------


class UnixHTTPConnection(http.client.HTTPConnection):
    """``http.client`` over AF_UNIX; one request per connection."""

    def __init__(self, path: str, timeout: float = STATEMENT_WAIT_S) -> None:
        super().__init__("localhost", timeout=timeout)
        self._unix_path = path

    def connect(self) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect(self._unix_path)
        self.sock = sock


@dataclass(frozen=True)
class Reply:
    status: int
    headers: dict[str, str]
    body: bytes

    @property
    def json(self) -> Any:
        return json.loads(self.body)

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", "replace")

    def refused(self, status: int, code: str) -> None:
        """Assert the exact refusal: status, the fixed body and no-store."""

        assert self.status == status, (self.status, self.text)
        assert self.json == {"refusal": code}, self.text
        assert self.headers.get("cache-control") == "no-store", self.headers
        assert self.headers.get("content-type", "").startswith("application/json"), self.headers


def parse_raw(data: bytes) -> Reply:
    head, separator, body = data.partition(b"\r\n\r\n")
    assert separator, f"no complete HTTP head in {len(data)} bytes"
    lines = head.decode("latin-1").split("\r\n")
    version, status, *_ = lines[0].split(" ", 2)
    assert version == "HTTP/1.1", lines[0]
    headers: dict[str, str] = {}
    for line in lines[1:]:
        name, _, value = line.partition(":")
        key = name.strip().lower()
        assert key not in headers, f"duplicate response header {key}"
        headers[key] = value.strip()
    if "content-length" in headers:
        assert len(body) == int(headers["content-length"]), (len(body), headers)
    return Reply(int(status), headers, body)


def raw_exchange(
    path: str,
    head: bytes,
    body: bytes = b"",
    *,
    timeout: float = STATEMENT_WAIT_S,
) -> tuple[Reply, float]:
    """Send ``head`` (then ``body``), read to EOF; answer the reply and its latency.

    The body is sent only after the head, and a peer that closed early (it
    refused before reading the body) is not an error here: the reply decides.
    """

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    started = time.monotonic()
    try:
        sock.connect(path)
        sock.sendall(head)
        if body:
            with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                sock.sendall(body)
        chunks: list[bytes] = []
        while True:
            try:
                chunk = sock.recv(65536)
            except ConnectionResetError:
                break
            if not chunk:
                break
            chunks.append(chunk)
        elapsed = time.monotonic() - started
    finally:
        sock.close()
    return parse_raw(b"".join(chunks)), elapsed


def request_head(
    length: int | str | None,
    *,
    method: str = "POST",
    target: str = RESOLVE_PATH,
    version: str = "HTTP/1.1",
    content_type: str | None = "application/json",
    extra: Sequence[str] = (),
) -> bytes:
    lines = [f"{method} {target} {version}", "Host: localhost"]
    if content_type is not None:
        lines.append(f"Content-Type: {content_type}")
    if length is not None:
        lines.append(f"Content-Length: {length}")
    lines.extend(extra)
    return ("\r\n".join(lines) + "\r\n\r\n").encode("utf-8")


def resolve_body(handle: str, slot: str, purpose: str = PURPOSE) -> bytes:
    return json.dumps({"handle": handle, "slot": slot, "purpose": purpose}).encode()


# --- applications and members -----------------------------------------------


@dataclass
class Member:
    app: SharedApp
    agent_id: str
    alias: str
    account: str
    binding: dict[str, Any]
    plaintext: bytes | None
    fields: dict[str, str | bytes]
    source: dict[str, Any] | None
    grant_id: str | None
    revision: int = 1

    @property
    def binding_id(self) -> str:
        return str(self.binding["binding_id"])

    @property
    def binding_uuid(self) -> uuid.UUID:
        return uuid.UUID(self.binding_id)

    @property
    def text(self) -> str:
        value = self.fields[STATIC_FIELD]
        assert isinstance(value, str)
        return value

    def reimport(
        self,
        plaintext: bytes,
        *,
        resource_version: str,
        recipient: Recipient | None = None,
    ) -> httpx.Response:
        assert self.source is not None, "connector custody has no source to reimport"
        source = source_metadata(uid=self.source["uid"], resource_version=resource_version)
        response = self.app.store.reimport(
            self.binding_id,
            self.revision,
            recipient or self.app.recipient,
            plaintext,
            source,
        )
        if response.status_code == 200:
            self.source = source
            self.plaintext = plaintext
            self.revision = int(response.json()["credential_revision"])
        return response

    def reimport_text(self, text: str, *, resource_version: str) -> httpx.Response:
        response = self.reimport(
            envelope({SLOT: {STATIC_FIELD: text}}), resource_version=resource_version
        )
        if response.status_code == 200:
            self.fields = {STATIC_FIELD: text}
        return response


@dataclass
class SharedApp:
    store: SharedStore
    application_id: str
    installation: str
    recipient: Recipient
    slots: tuple[str, ...]
    custody: str
    origins: tuple[str, ...]
    database: dict[str, Any]
    dsn: str
    members: list[Member] = field(default_factory=list)

    @property
    def role_name(self) -> str:
        return str(self.database["role_name"])

    @property
    def sealed_dsn(self) -> str:
        return str(self.database["sealed_dsn"])

    @property
    def slot(self) -> str:
        return self.slots[0] if self.slots else IDENTITY_SLOT

    def generation(self) -> int:
        return int(self.store.application(self.application_id)["policy_generation"])

    def add_member(
        self,
        *,
        alias: str = "primary",
        fields: Mapping[str, str | bytes] | None = None,
        plaintext: bytes | None = None,
        account: str | None = None,
        refresh_grant: bool = False,
    ) -> Member:
        store = self.store
        agent_id = store.create_agent()
        joined = store.put_members(self.application_id, agent_id, [{"alias": alias, "tools": None}])
        assert joined.status_code == 200, joined.text
        account = account or f"acct-{uuid.uuid4().hex[:12]}"
        if self.custody == "connector":
            response = store.post_binding(
                self.application_id,
                store.binding_body(agent_id, None, None, provider_account=account),
            )
            assert response.status_code == 201, response.text
            member = Member(self, agent_id, alias, account, response.json(), None, {}, None, None)
        else:
            chosen: dict[str, str | bytes] = dict(
                fields or {STATIC_FIELD: secret_sentinel("STATIC")}
            )
            material = plaintext if plaintext is not None else envelope({self.slot: chosen})
            source = source_metadata(uid=f"uid-{uuid.uuid4().hex[:12]}")
            grant_id = f"grant:{uuid.uuid4().hex}" if refresh_grant else None
            grant = (
                {"grant_id": grant_id, "client_config_fingerprint": client_fingerprint(grant_id)}
                if grant_id is not None
                else None
            )
            binding = store.import_binding(
                self.application_id,
                agent_id,
                self.recipient,
                material,
                provider_account=account,
                source=source,
                refresh_grant=grant,
            )
            member = Member(
                self, agent_id, alias, account, binding, material, chosen, source, grant_id
            )
        self.members.append(member)
        return member


# --- the resolver -----------------------------------------------------------


class ResolverHarness:
    """One real resolver on its own event loop thread."""

    def __init__(
        self,
        env: Mapping[str, str],
        *,
        directory: Path,
        app: SharedApp,
        sources: SourceRegistry | None,
        handles: HandleRegistry | None,
        start_options: Mapping[str, Any] | None = None,
    ) -> None:
        self.env = dict(env)
        self.directory = directory
        self.app = app
        self.config = ResolverConfig.from_env(self.env)
        self.socket_path = self.config.socket_path
        self._closed = False
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()
        try:
            self.runtime: ResolverRuntime = self._wait(
                start_resolver(
                    self.config, sources=sources, handles=handles, **dict(start_options or {})
                )
            )
        except BaseException:
            self._stop()
            raise
        self.handles: HandleRegistry = self.runtime.handles
        if handles is not None:
            assert self.handles is handles

    def _wait(self, coroutine: Any, timeout: float = STATEMENT_WAIT_S) -> Any:
        return asyncio.run_coroutine_threadsafe(coroutine, self._loop).result(timeout)

    @property
    def admissions(self) -> int:
        return int(self.runtime.resolver.admissions)

    @property
    def releases(self) -> int:
        return int(self.runtime.resolver.releases)

    def principal(
        self,
        member: Member,
        *,
        generation: int | None = None,
        installation: str | None = None,
        application: str | None = None,
        agent_id: str | None = None,
    ) -> Principal:
        return Principal(
            installation=self.app.installation if installation is None else installation,
            application=self.app.application_id if application is None else application,
            agent_id=member.agent_id if agent_id is None else agent_id,
            agent=f"agent-{member.agent_id[:8]}",
            policy_generation=self.app.generation() if generation is None else generation,
        )

    def context(
        self,
        member: Member,
        *,
        alias: str | None = None,
        expires_in: float = 600,
        expires_at: int | None = None,
        run: str | None = None,
        work_item: str | None = None,
        session_id: str | None = None,
        binding: str | None = None,
        membership: str = UNOBSERVED_MEMBERSHIP,
        **principal: Any,
    ) -> InvocationContext:
        return InvocationContext(
            principal=self.principal(member, **principal),
            alias=member.alias if alias is None else alias,
            expires_at=int(time.time() + expires_in) if expires_at is None else expires_at,
            binding=member.binding_id if binding is None else binding,
            membership=membership,
            run=run,
            work_item=work_item,
            session_id=session_id,
        )

    def register(self, member: Member, **options: Any) -> Invocation:
        return self.handles.register(self.context(member, **options))

    def handle(self, member: Member, **options: Any) -> str:
        return self.register(member, **options).handle

    def post(self, body: bytes, headers: Mapping[str, str] | None = None) -> Reply:
        connection = UnixHTTPConnection(self.socket_path)
        try:
            connection.request(
                "POST",
                RESOLVE_PATH,
                body=body,
                headers={"Content-Type": "application/json", **(headers or {})},
            )
            response = connection.getresponse()
            payload = response.read()
            headers_out = {name.lower(): value for name, value in response.getheaders()}
            return Reply(response.status, headers_out, payload)
        finally:
            connection.close()

    def resolve(self, handle: str, slot: str | None = None, purpose: str = PURPOSE) -> Reply:
        return self.post(resolve_body(handle, self.app.slot if slot is None else slot, purpose))

    def raw(self, head: bytes, body: bytes = b"", **options: Any) -> tuple[Reply, float]:
        return raw_exchange(self.socket_path, head, body, **options)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._wait(self.runtime.close())
        finally:
            self._stop()
        assert not os.path.exists(self.socket_path), "close() left the socket file behind"

    def _stop(self) -> None:
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=10)
        if not self._thread.is_alive():
            self._loop.close()


def owner_dsn() -> str:
    """The API's own (owner) DSN in plain libpq form."""

    return render_url(make_url(get_settings().database_url).set(drivername="postgresql"))


class Fleet:
    """Applications registered through the API, and resolvers launched for them."""

    def __init__(self, store: SharedStore, sessions: SessionPool, cleanup: SharedCleanup) -> None:
        self.store = store
        self.sessions = sessions
        self.cleanup = cleanup
        self._harnesses: list[ResolverHarness] = []
        self._directories: list[Path] = []

    def application(
        self,
        *,
        slots: Sequence[str] = (SLOT,),
        custody: str = "platform",
        origins: Sequence[str] = DEFAULT_ORIGINS,
    ) -> SharedApp:
        store = self.store
        application_id = str(uuid.uuid4())
        body = store.registration_body(application_id, slots=tuple(slots), custody=custody)
        body["application"]["provider_origins"] = list(origins)
        registered = store.operator("POST", "/applications", body)
        assert registered.status_code == 201, registered.text
        assert registered.json()["provider_origins"] == list(origins)
        recipient = store.register_recipient(application_id)
        database, dsn = store.provision_bundled(application_id, recipient)
        return SharedApp(
            store=store,
            application_id=application_id,
            installation=store.installation,
            recipient=recipient,
            slots=tuple(slots),
            custody=custody,
            origins=tuple(origins),
            database=database,
            dsn=dsn,
        )

    def directory(self) -> Path:
        # Short and private: an AF_UNIX path must stay within 107 bytes.
        path = Path(tempfile.mkdtemp(prefix="crs-", dir="/tmp"))
        self._directories.append(path)
        return path

    def env(
        self,
        app: SharedApp,
        *,
        keys: Sequence[Recipient] | None = None,
        dsn_text: str | None = None,
        encoding: str = "bundled-sealed",
        origins: Sequence[str] | None = None,
        slots: Sequence[str] | None = None,
        extra: Mapping[str, str] | None = None,
    ) -> tuple[dict[str, str], Path]:
        """The frozen render's proxy environment, with its files written."""

        directory = self.directory()
        secret_dir = directory / "s"
        socket_dir = directory / "k"
        secret_dir.mkdir(mode=0o700)
        socket_dir.mkdir(mode=0o700)
        key_file = secret_dir / "private"
        key_file.write_text(
            "".join(
                base64.b64encode(bytes(recipient.private_key)).decode("ascii") + "\n"
                for recipient in (keys if keys is not None else (app.recipient,))
            )
        )
        dsn_file = secret_dir / "dsn"
        dsn_file.write_text(app.sealed_dsn if dsn_text is None else dsn_text)
        socket_path = str(socket_dir / "r.sock")
        assert len(socket_path.encode()) <= 107, socket_path
        env = {
            "CURIE_SHARED_INSTALLATION": app.installation,
            "CURIE_SHARED_APPLICATION": app.application_id,
            "CURIE_SHARED_RESOLVER_SOCKET": socket_path,
            "CURIE_SHARED_DSN_ENCODING": encoding,
            "CURIE_SHARED_PROVIDER_ORIGINS": json.dumps(
                list(app.origins if origins is None else origins), separators=(",", ":")
            ),
            "CURIE_SHARED_CREDENTIAL_SLOTS": json.dumps(
                list(app.slots if slots is None else slots), separators=(",", ":")
            ),
            "CURIE_SHARED_ENCRYPTION_KEY_FILE": str(key_file),
            "CURIE_SHARED_DATABASE_DSN_FILE": str(dsn_file),
        }
        env.update(extra or {})
        return env, directory

    def launch(
        self,
        app: SharedApp,
        *,
        sources: SourceRegistry | None = None,
        handles: HandleRegistry | None = None,
        env: tuple[dict[str, str], Path] | None = None,
        start_options: Mapping[str, Any] | None = None,
        **env_options: Any,
    ) -> ResolverHarness:
        """Start a resolver; ``start_options`` are extra ``start_resolver`` keywords."""

        environment, directory = env if env is not None else self.env(app, **env_options)
        registry = handles or HandleRegistry(
            installation=app.installation, application=app.application_id
        )
        harness = ResolverHarness(
            environment,
            directory=directory,
            app=app,
            sources=sources,
            handles=registry,
            start_options=start_options,
        )
        self._harnesses.append(harness)
        return harness

    def start_refused(self, app: SharedApp, **env_options: Any) -> tuple[str, str]:
        """Start must refuse; answer its code and the socket path it must not create."""

        environment, directory = self.env(app, **env_options)
        socket_path = environment["CURIE_SHARED_RESOLVER_SOCKET"]
        with pytest.raises(ResolverStartRefused) as refused:
            self.launch(app, env=(environment, directory))
        code = str(refused.value.code)
        assert not os.path.exists(socket_path), "a refused start bound its socket"
        return code, socket_path

    def close(self) -> None:
        try:
            for harness in reversed(self._harnesses):
                harness.close()
        finally:
            for directory in self._directories:
                shutil.rmtree(directory, ignore_errors=True)


# --- database observations ---------------------------------------------------


def resolver_backends(role: str) -> list[dict[str, Any]]:
    return sql_dicts(
        "SELECT pid, usename::text AS usename, wait_event_type, state "
        "FROM pg_stat_activity WHERE datname = current_database() "
        "AND application_name = :name AND usename = :role",
        {"name": RESOLVER_APPLICATION_NAME, "role": role},
    )


def wait_for_no_resolver_backends(role: str, timeout: float = LOCK_WAIT_S) -> None:
    deadline = time.monotonic() + timeout
    while resolver_backends(role):
        if time.monotonic() > deadline:
            raise AssertionError(f"resolver backends remain: {resolver_backends(role)}")
        time.sleep(0.05)


def wait_for_parked_resolver(role: str, timeout: float = LOCK_WAIT_S) -> int:
    """The pid of the resolver backend (by application_name) waiting on a lock."""

    deadline = time.monotonic() + timeout
    while True:
        parked = [row for row in resolver_backends(role) if row["wait_event_type"] == "Lock"]
        if parked:
            assert len(parked) == 1, parked
            return int(parked[0]["pid"])
        if time.monotonic() > deadline:
            raise AssertionError(f"no resolver backend waits on a lock: {resolver_backends(role)}")
        time.sleep(0.05)


def wait_for_resolver_in_transaction(role: str, timeout: float = LOCK_WAIT_S) -> int:
    """The pid of the one resolver backend holding an open transaction."""

    deadline = time.monotonic() + timeout
    while True:
        backends = resolver_backends(role)
        held = [row for row in backends if row["state"] == "idle in transaction"]
        if held:
            assert len(held) == 1, held
            return int(held[0]["pid"])
        if time.monotonic() > deadline:
            raise AssertionError(f"no resolver backend holds a transaction: {backends}")
        time.sleep(0.05)


def blocking_pids(pid: int) -> set[int]:
    (row,) = sql_dicts("SELECT pg_blocking_pids(:pid) AS blockers", {"pid": pid})
    return {int(blocker) for blocker in row["blockers"]}


def wait_for_backend_blocked_by(blocker: int, timeout: float = LOCK_WAIT_S) -> int:
    """The pid of a backend whose ``pg_blocking_pids`` names ``blocker``."""

    deadline = time.monotonic() + timeout
    while True:
        rows = sql_dicts(
            "SELECT pid FROM pg_stat_activity "
            "WHERE datname = current_database() AND pid <> pg_backend_pid() "
            "AND :blocker = ANY(pg_blocking_pids(pid))",
            {"blocker": blocker},
        )
        if rows:
            return int(rows[0]["pid"])
        if time.monotonic() > deadline:
            raise AssertionError(f"no backend is blocked by pid {blocker}")
        time.sleep(0.05)


def terminate_role_backends(role: str) -> None:
    sql_dicts(
        "SELECT pg_terminate_backend(pid) AS terminated FROM pg_stat_activity "
        "WHERE usename = :role AND pid <> pg_backend_pid()",
        {"role": role},
    )


def settle(
    harness: ResolverHarness, handle: str, *, slot: str | None = None, store_retries: int = 2
) -> Reply:
    """Resolve, allowing at most ``store_retries`` exact ``store_unavailable`` answers first.

    After a database outage ends, a pool may still hand out one connection the
    server already closed; that answer must itself be the exact fail-closed
    refusal, and the outcome after it is what the caller asserts.
    """

    for _ in range(store_retries):
        reply = harness.resolve(handle, slot)
        if reply.status == 503 and reply.json == {"refusal": "store_unavailable"}:
            time.sleep(0.2)
            continue
        return reply
    return harness.resolve(handle, slot)


# --- log capture -------------------------------------------------------------


@contextlib.contextmanager
def debug_capture(caplog: pytest.LogCaptureFixture) -> Iterator[None]:
    """Every record from every logger a leak could reach, at DEBUG."""

    attached: list[logging.Logger] = []
    with contextlib.ExitStack() as stack:
        for name in CAPTURED_LOGGERS:
            stack.enter_context(caplog.at_level(logging.DEBUG, logger=name or None))
            logger = logging.getLogger(name or None)
            if name and caplog.handler not in logger.handlers:
                logger.addHandler(caplog.handler)
                attached.append(logger)
        try:
            yield
        finally:
            for logger in attached:
                logger.removeHandler(caplog.handler)


def log_text(caplog: pytest.LogCaptureFixture) -> str:
    return captured_log_text(caplog.records)
