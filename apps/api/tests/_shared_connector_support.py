"""Helpers for the shared connector store tests (ADR 0239).

The store, administration and migration tests drive the API through a real
TestClient, then prove database behavior through real asyncpg connections made
as the GENERATED application roles: the DSN each test connects with is the one
the API sealed, opened with the test's own X25519 private key. Nothing here
fakes an internal service.

Roles are cluster global, so every test uses a random installation ID and
random application UUIDs, and registers every role it (or the API on its
behalf) may create with ``SharedCleanup`` BEFORE creating anything.

Imported by test modules through the ``sys.path`` entry ``conftest.py`` adds
(see ``_migration_support``).
"""

from __future__ import annotations

import asyncio
import base64
import concurrent.futures
import contextlib
import hashlib
import json
import logging
import secrets
import string
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import SplitResult, unquote, urlsplit

import asyncpg
import httpx
import nacl.public
import pytest
from _migration_support import sql_dicts
from curie_api.config import get_settings
from curie_api.main import create_app
from fastapi.testclient import TestClient
from sqlalchemy.engine import URL, make_url

INSTALLATION_ENV = "CURIE_INSTALLATION_ID"
OBSERVED_ENV = "CURIE_INSTALLATION_ID_OBSERVED"

OPERATOR_PREFIX = "/shared-connectors"
WORKER_PREFIX = "/v1/internal/shared-connectors"

SERVER_IMAGE = "example.com/acme/server@sha256:" + "a" * 64
OTHER_SERVER_IMAGE = "example.com/acme/server@sha256:" + "c" * 64
PROXY_IMAGE = "example.com/acme/proxy@sha256:" + "b" * 64
DSN_SECRET = {"name": "acme-database", "key": "dsn"}
SLOT = "access_token"
TOOLS = ("issues.read", "issues.write", "issues.close")

SHARED_TABLES = (
    "shared_connector_applications",
    "shared_connector_recipients",
    "shared_connector_database_roles",
    "shared_connector_memberships",
    "shared_connector_bindings",
    "shared_connector_refresh_intents",
)
REFRESH_FUNCTIONS = (
    "shared_connector_refresh_begin",
    "shared_connector_refresh_commit",
    "shared_connector_refresh_mark_recovery",
)
SHARED_FUNCTIONS = (*REFRESH_FUNCTIONS, "shared_connector_reseal")

BEGIN_SQL = "SELECT * FROM curie.shared_connector_refresh_begin($1, $2, $3)"
COMMIT_SQL = "SELECT * FROM curie.shared_connector_refresh_commit($1, $2, $3, $4, $5)"
MARK_RECOVERY_SQL = "SELECT curie.shared_connector_refresh_mark_recovery($1, $2, $3)"
RESEAL_SQL = "SELECT curie.shared_connector_reseal($1, $2, $3, $4, $5)"

# Bounds every wait a test makes, so a hang fails instead of stalling the run.
STATEMENT_WAIT_S = 30.0
LOCK_WAIT_S = 15.0


def resolver_read_sql(lock: str = "SHARE") -> str:
    """The plan's resolver-style read, locking both rows in ``lock`` mode."""

    return (
        "SELECT a.enabled AS application_enabled, a.policy_generation, "
        "b.enabled AS binding_enabled, b.credential_revision "
        "FROM curie.shared_connector_bindings b "
        "JOIN curie.shared_connector_applications a ON a.id = b.application_id "
        f"WHERE b.id = $1 FOR {lock} OF a, b"
    )


# --- identities and payloads ------------------------------------------------


def random_installation(length: int = 32) -> str:
    """An opaque installation ID shaped like a fresh chart install's."""

    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def role_name_for(installation: str, application_id: str) -> str:
    identity = json.dumps([installation, application_id], separators=(",", ":")).encode()
    return "curie_sca_" + hashlib.sha256(identity).hexdigest()[:40]


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def secret_sentinel(label: str = "PLAINTEXT") -> str:
    return f"sk-live-SHARED-{label}-SENTINEL-{secrets.token_hex(8)}"


def b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


def source_fingerprint(seed: str) -> str:
    return "hmac-sha256:" + hashlib.sha256(seed.encode()).hexdigest()


def client_fingerprint(seed: str) -> str:
    return "sha256:" + hashlib.sha256(seed.encode()).hexdigest()


def application_spec(
    installation: str,
    application_id: str,
    *,
    slots: Sequence[str] = (SLOT,),
    dsn_encoding: str = "bundled-sealed",
    server_image: str = SERVER_IMAGE,
) -> dict[str, Any]:
    """A valid frozen ``SharedApplication`` in its JSON form."""

    return {
        "installation": installation,
        "application": application_id,
        "release": "acme",
        "namespace": "acme",
        "app_name": "curie",
        "server_image": server_image,
        "proxy_image": PROXY_IMAGE,
        "server_args": ["--transport", "streamable-http"],
        "encryption_key": {"name": "acme-encryption", "key": "private"},
        "database_dsn": dict(DSN_SECRET),
        "dsn_encoding": dsn_encoding,
        "grant_store": {"name": "acme-grants", "key": "url"},
        "provider_origins": ["https://api.example.com"],
        "credential_slots": list(slots),
        "database_target": {
            "namespace": "acme-data",
            "pod_labels": {"app.kubernetes.io/component": "database"},
            "port": 5432,
        },
    }


def shared_policy(tools: Sequence[str] = TOOLS, custody: str = "platform") -> dict[str, Any]:
    return {"tools": list(tools), "custody": custody}


def source_metadata(*, uid: str, resource_version: str = "100") -> dict[str, str]:
    return {
        "kind": "kubernetes-secret",
        "namespace": "acme",
        "name": "acme-agent-connector-secrets",
        "uid": uid,
        "resource_version": resource_version,
        "fingerprint": source_fingerprint(f"{uid}/{resource_version}"),
    }


def operator_headers() -> dict[str, str]:
    return {"X-API-Key": get_settings().api_key}


def worker_headers() -> dict[str, str]:
    return {"X-Curie-Worker-Token": get_settings().internal_worker_token}


@dataclass(frozen=True)
class Recipient:
    """A test-held X25519 keypair; the private key never leaves the test."""

    private_key: nacl.public.PrivateKey

    @classmethod
    def generate(cls) -> Recipient:
        return cls(nacl.public.PrivateKey.generate())

    @property
    def public_bytes(self) -> bytes:
        return bytes(self.private_key.public_key)

    @property
    def recipient_id(self) -> str:
        return hashlib.sha256(self.public_bytes).hexdigest()

    def seal(self, plaintext: bytes) -> bytes:
        return nacl.public.SealedBox(self.private_key.public_key).encrypt(plaintext)

    def envelope(self, plaintext: bytes) -> dict[str, str]:
        return {"recipient_id": self.recipient_id, "ciphertext": b64(self.seal(plaintext))}

    def open(self, sealed: bytes | str) -> bytes:
        raw = base64.b64decode(sealed, validate=True) if isinstance(sealed, str) else sealed
        return nacl.public.SealedBox(self.private_key).decrypt(raw)

    def registration(self, application_id: str) -> dict[str, Any]:
        return {
            "application_id": application_id,
            "recipient_id": self.recipient_id,
            "public_key": {"algorithm": "x25519-sealed-box", "key": b64(self.public_bytes)},
            "readback": {"public_key_sha256": self.recipient_id},
        }


# --- HTTP assertions --------------------------------------------------------


def detail_code(response: httpx.Response) -> str | None:
    try:
        detail = response.json().get("detail")
    except (ValueError, AttributeError):
        return None
    return detail if isinstance(detail, str) else None


def assert_refused(response: httpx.Response, status: int, code: str) -> None:
    assert response.status_code == status, response.text
    assert response.json() == {"detail": code}, response.text


def assert_no_store(response: httpx.Response) -> None:
    assert "no-store" in response.headers.get("cache-control", ""), (
        response.request.method,
        response.request.url.path,
        response.status_code,
        dict(response.headers),
    )


def assert_redacted_422(response: httpx.Response, *secret_values: str) -> None:
    """A 422 that names what failed without echoing what was sent."""

    assert response.status_code == 422, response.text
    for value in secret_values:
        assert value not in response.text, response.text
    detail = response.json()["detail"]
    if isinstance(detail, list):
        for error in detail:
            assert set(error) <= {"type", "loc", "msg"}, error


# --- owner-side reads -------------------------------------------------------


def binding_row(binding_id: str) -> dict[str, Any]:
    (row,) = sql_dicts(
        "SELECT * FROM curie.shared_connector_bindings WHERE id = :id",
        {"id": uuid.UUID(binding_id)},
    )
    return row


def intent_row(binding_id: str) -> dict[str, Any]:
    (row,) = sql_dicts(
        "SELECT * FROM curie.shared_connector_refresh_intents WHERE binding_id = :id",
        {"id": uuid.UUID(binding_id)},
    )
    return row


def application_row(application_id: str) -> dict[str, Any]:
    (row,) = sql_dicts(
        "SELECT * FROM curie.shared_connector_applications WHERE id = :id",
        {"id": uuid.UUID(application_id)},
    )
    return row


def database_role_rows(application_id: str) -> list[dict[str, Any]]:
    return sql_dicts(
        "SELECT * FROM curie.shared_connector_database_roles WHERE application_id = :id",
        {"id": uuid.UUID(application_id)},
    )


def role_exists(name: str) -> bool:
    return bool(
        sql_dicts("SELECT 1 AS present FROM pg_roles WHERE rolname = :name", {"name": name})
    )


def regclass(table: str) -> str | None:
    (row,) = sql_dicts("SELECT to_regclass(:name)::text AS name", {"name": f"curie.{table}"})
    return row["name"]


def shared_function_names() -> set[str]:
    rows = sql_dicts(
        "SELECT p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
        "WHERE n.nspname = 'curie' AND p.proname LIKE 'shared\\_connector%'"
    )
    return {row["proname"] for row in rows}


def shared_table_text() -> str:
    """Every row of the six tables, rendered whole as the owner sees it."""

    parts: list[str] = []
    for table in SHARED_TABLES:
        parts.extend(row["t"] for row in sql_dicts(f"SELECT t::text AS t FROM curie.{table} t"))
    return "\n".join(parts)


def owner_connect_kwargs() -> dict[str, Any]:
    url = make_url(get_settings().database_url)
    return {
        "user": url.username,
        "password": url.password,
        "host": url.host,
        "port": url.port,
        "database": url.database,
    }


def dsn_parts(dsn: str) -> SplitResult:
    return urlsplit(dsn)


def dsn_password(dsn: str) -> str:
    password = dsn_parts(dsn).password
    assert password, "the sealed DSN carries no password"
    return unquote(password)


def connect_as(dsn: str) -> str:
    """Open the DSN once and answer which role the server authenticated."""

    async def run() -> str:
        connection = await asyncpg.connect(dsn, timeout=STATEMENT_WAIT_S)
        try:
            return str(await connection.fetchval("SELECT current_user"))
        finally:
            await connection.close()

    return asyncio.run(run())


def wait_for_lock_waiters(count: int = 1, timeout: float = LOCK_WAIT_S) -> None:
    """Return once ``count`` backends in this database are waiting on a lock."""

    deadline = time.monotonic() + timeout
    while True:
        (row,) = sql_dicts(
            "SELECT count(*) AS waiting FROM pg_stat_activity "
            "WHERE datname = current_database() AND wait_event_type = 'Lock' "
            "AND pid <> pg_backend_pid()"
        )
        if row["waiting"] >= count:
            return
        if time.monotonic() > deadline:
            raise AssertionError(f"expected {count} backend(s) waiting on a lock, saw {row}")
        time.sleep(0.05)


def assert_still_blocked(
    future: concurrent.futures.Future[Any], *, waiters: int = 1, seconds: float = 1.0
) -> None:
    """Prove a submitted request is parked on a row lock, not just slow."""

    wait_for_lock_waiters(waiters)
    time.sleep(seconds)
    assert not future.done(), "the request finished while the conflicting lock was still held"


def in_background[T](call: Callable[[], T]) -> concurrent.futures.Future[T]:
    """Run ``call`` on a daemon thread; a hung request cannot block teardown."""

    future: concurrent.futures.Future[T] = concurrent.futures.Future()

    def run() -> None:
        try:
            future.set_result(call())
        except BaseException as error:  # noqa: BLE001 - handed to the waiting test
            future.set_exception(error)

    threading.Thread(target=run, daemon=True).start()
    return future


def expect_sqlstate(
    session: PgSession, sqlstate: str, query: str, *args: Any
) -> asyncpg.PostgresError:
    """Run ``query`` and answer the error it must raise with ``sqlstate``."""

    try:
        session.execute(query, *args)
    except asyncpg.PostgresError as error:
        assert error.sqlstate == sqlstate, (query, error.sqlstate, str(error))
        return error
    raise AssertionError(f"expected SQLSTATE {sqlstate} from {query!r}")


# --- real connections -------------------------------------------------------


class PgSession:
    """One real asyncpg connection owned by its own thread and event loop.

    Statements run synchronously from the test, or are submitted and left
    pending so the test can show they wait on a lock before releasing it.
    """

    def __init__(self, dsn: str | None = None, **connect: Any) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()
        self._pending: list[concurrent.futures.Future[Any]] = []
        self._closed = False
        try:
            self._conn: asyncpg.Connection = self._wait(
                asyncpg.connect(
                    dsn,
                    timeout=STATEMENT_WAIT_S,
                    server_settings={"lock_timeout": "20s"},
                    **connect,
                )
            )
        except BaseException:
            self._stop()
            raise

    def _wait(self, coroutine: Any, timeout: float = STATEMENT_WAIT_S) -> Any:
        return asyncio.run_coroutine_threadsafe(coroutine, self._loop).result(timeout)

    def submit(self, method: str, query: str, *args: Any) -> concurrent.futures.Future[Any]:
        future = asyncio.run_coroutine_threadsafe(
            getattr(self._conn, method)(query, *args), self._loop
        )
        self._pending.append(future)
        return future

    def execute(self, query: str, *args: Any) -> str:
        return str(self._wait(self._conn.execute(query, *args)))

    def fetch(self, query: str, *args: Any) -> list[asyncpg.Record]:
        return list(self._wait(self._conn.fetch(query, *args)))

    def fetchrow(self, query: str, *args: Any) -> asyncpg.Record | None:
        row: asyncpg.Record | None = self._wait(self._conn.fetchrow(query, *args))
        return row

    def fetchval(self, query: str, *args: Any) -> Any:
        return self._wait(self._conn.fetchval(query, *args))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if any(not future.done() for future in self._pending):
                self._loop.call_soon_threadsafe(self._conn.terminate)
            else:
                self._wait(self._conn.close(timeout=5), timeout=10)
        except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError):
            self._loop.call_soon_threadsafe(self._conn.terminate)
        finally:
            self._stop()

    def _stop(self) -> None:
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=10)
        if not self._thread.is_alive():
            self._loop.close()


class SessionPool:
    """Every connection a test opens, closed at teardown before roles drop."""

    def __init__(self) -> None:
        self._sessions: list[PgSession] = []

    def open(self, dsn: str) -> PgSession:
        session = PgSession(dsn)
        self._sessions.append(session)
        return session

    def owner(self) -> PgSession:
        session = PgSession(**owner_connect_kwargs())
        self._sessions.append(session)
        return session

    def close_all(self) -> None:
        for session in reversed(self._sessions):
            session.close()


# --- cleanup ----------------------------------------------------------------


class SharedCleanup:
    """Removes every role, database and application row a test registered.

    Register first, create second: a test that fails half way still leaves no
    cluster-global role behind. Only names registered here are ever dropped.
    """

    def __init__(self) -> None:
        self._admin: URL = make_url(get_settings().database_url)
        self._roles: list[tuple[str, str | None]] = []
        self._databases: list[str] = []
        self._installations: list[tuple[str, str | None]] = []

    @staticmethod
    def _current_database() -> str | None:
        return make_url(get_settings().database_url).database

    def role(self, name: str, database: str | None = None) -> str:
        self._roles.append((name, database or self._current_database()))
        return name

    def database(self, name: str) -> str:
        self._databases.append(name)
        return name

    def installation(self, installation: str) -> str:
        self._installations.append((installation, self._current_database()))
        return installation

    def run(self) -> None:
        asyncio.run(self._run())

    async def _connect(self, database: str | None) -> asyncpg.Connection | None:
        try:
            return await asyncpg.connect(
                user=self._admin.username,
                password=self._admin.password,
                host=self._admin.host,
                port=self._admin.port,
                database=database or "postgres",
                timeout=STATEMENT_WAIT_S,
            )
        except asyncpg.InvalidCatalogNameError:
            return None

    async def _run(self) -> None:
        for installation, database in self._installations:
            if database in self._databases:
                continue
            connection = await self._connect(database)
            if connection is None:
                continue
            try:
                if await connection.fetchval(
                    "SELECT to_regclass('curie.shared_connector_applications') IS NOT NULL"
                ):
                    await connection.execute(
                        "DELETE FROM curie.shared_connector_applications "
                        "WHERE installation_id = $1",
                        installation,
                    )
            finally:
                await connection.close()
        admin = await self._connect("postgres")
        assert admin is not None
        try:
            for name in self._databases:
                await admin.execute(f"DROP DATABASE IF EXISTS {quote_ident(name)} WITH (FORCE)")
            for role, database in reversed(self._roles):
                if not await admin.fetchval("SELECT 1 FROM pg_roles WHERE rolname = $1", role):
                    continue
                await admin.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE usename = $1 AND pid <> pg_backend_pid()",
                    role,
                )
                connection = await self._connect(database)
                if connection is not None:
                    try:
                        if await connection.fetchval("SELECT to_regnamespace('curie') IS NOT NULL"):
                            await connection.execute(
                                f"REVOKE ALL ON SCHEMA curie FROM {quote_ident(role)}"
                            )
                        await connection.execute(f"DROP OWNED BY {quote_ident(role)}")
                    finally:
                        await connection.close()
                await admin.execute(f"DROP ROLE IF EXISTS {quote_ident(role)}")
        finally:
            await admin.close()


# --- the API under test -----------------------------------------------------


@contextlib.contextmanager
def shared_app(
    monkeypatch: pytest.MonkeyPatch,
    installation: str | None,
    *,
    observed: str | None = "true",
    **environment: str,
) -> Iterator[TestClient]:
    """A real app built after the installation identity is configured."""

    for name, value in ((INSTALLATION_ENV, installation), (OBSERVED_ENV, observed)):
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    get_settings.cache_clear()
    try:
        with TestClient(create_app()) as client:
            yield client
    finally:
        get_settings.cache_clear()


def _shared_route(path: str) -> bool:
    return path.startswith((OPERATOR_PREFIX, WORKER_PREFIX))


class SharedStore:
    """The operator and worker flows, each asserting its documented status."""

    def __init__(self, client: TestClient, installation: str, cleanup: SharedCleanup) -> None:
        self.client = client
        self.installation = installation
        self.cleanup = cleanup
        self.responses: list[httpx.Response] = []

    def call(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        response = self.client.request(method, path, json=json, headers=headers)
        self.responses.append(response)
        code = detail_code(response)
        if _shared_route(path) and (
            response.is_success or (code is not None and code.startswith("shared_"))
        ):
            assert_no_store(response)
        return response

    def operator(self, method: str, path: str, body: Any = None) -> httpx.Response:
        return self.call(method, OPERATOR_PREFIX + path, json=body, headers=operator_headers())

    def worker(self, method: str, path: str, body: Any = None) -> httpx.Response:
        return self.call(method, WORKER_PREFIX + path, json=body, headers=worker_headers())

    # registration

    def registration_body(
        self,
        application_id: str,
        *,
        slots: Sequence[str] = (SLOT,),
        custody: str = "platform",
        dsn_encoding: str = "bundled-sealed",
        tools: Sequence[str] = TOOLS,
        installation: str | None = None,
        server_image: str = SERVER_IMAGE,
    ) -> dict[str, Any]:
        return {
            "application": application_spec(
                self.installation if installation is None else installation,
                application_id,
                slots=slots,
                dsn_encoding=dsn_encoding,
                server_image=server_image,
            ),
            "policy": shared_policy(tools, custody),
        }

    def register_application(
        self, application_id: str | None = None, **options: Any
    ) -> dict[str, Any]:
        body = self.registration_body(application_id or str(uuid.uuid4()), **options)
        response = self.operator("POST", "/applications", body)
        assert response.status_code == 201, response.text
        record: dict[str, Any] = response.json()
        return record

    def application(self, application_id: str) -> dict[str, Any]:
        response = self.operator("GET", f"/applications/{application_id}")
        assert response.status_code == 200, response.text
        record: dict[str, Any] = response.json()
        return record

    def disable_application(self, application_id: str) -> httpx.Response:
        return self.operator("POST", f"/applications/{application_id}/disable")

    def register_recipient(
        self, application_id: str, recipient: Recipient | None = None
    ) -> Recipient:
        recipient = recipient or Recipient.generate()
        response = self.worker(
            "POST",
            f"/applications/{application_id}/recipients",
            recipient.registration(application_id),
        )
        assert response.status_code == 201, response.text
        assert response.json() == {
            "application_id": application_id,
            "recipient_id": recipient.recipient_id,
            "current": True,
        }
        return recipient

    def retire(self, application_id: str, recipient_id: str) -> httpx.Response:
        return self.operator(
            "POST", f"/applications/{application_id}/recipients/{recipient_id}/retire"
        )

    # agents and members

    def create_agent(self) -> str:
        suffix = uuid.uuid4().hex
        response = self.call(
            "POST",
            "/agents",
            json={
                "name": f"shared-{suffix[:12]}",
                "channel": {"kind": "slack", "address": f"C{suffix[:10].upper()}"},
            },
            headers=operator_headers(),
        )
        assert response.status_code == 201, response.text
        return str(response.json()["id"])

    def put_members(
        self, application_id: str, agent_id: str, aliases: list[dict[str, Any]]
    ) -> httpx.Response:
        return self.operator(
            "PUT",
            f"/applications/{application_id}/members/{agent_id}",
            {"aliases": aliases},
        )

    # bindings

    def binding_body(
        self,
        agent_id: str,
        recipient: Recipient | None,
        plaintext: bytes | None,
        *,
        provider_account: str = "acct-primary",
        source: dict[str, Any] | None = None,
        refresh_grant: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        credential = (
            recipient.envelope(plaintext)
            if recipient is not None and plaintext is not None
            else None
        )
        return {
            "agent_id": agent_id,
            "provider_account": provider_account,
            "credential": credential,
            "source": source,
            "refresh_grant": refresh_grant,
        }

    def post_binding(self, application_id: str, body: dict[str, Any]) -> httpx.Response:
        return self.operator("POST", f"/applications/{application_id}/bindings", body)

    def import_binding(
        self,
        application_id: str,
        agent_id: str,
        recipient: Recipient,
        plaintext: bytes,
        **options: Any,
    ) -> dict[str, Any]:
        options.setdefault("source", source_metadata(uid=f"uid-{uuid.uuid4().hex[:12]}"))
        response = self.post_binding(
            application_id, self.binding_body(agent_id, recipient, plaintext, **options)
        )
        assert response.status_code == 201, response.text
        record: dict[str, Any] = response.json()
        return record

    def binding(self, binding_id: str) -> dict[str, Any]:
        response = self.operator("GET", f"/bindings/{binding_id}")
        assert response.status_code == 200, response.text
        record: dict[str, Any] = response.json()
        return record

    def reimport(
        self,
        binding_id: str,
        expected_revision: int,
        recipient: Recipient,
        plaintext: bytes,
        source: dict[str, Any],
    ) -> httpx.Response:
        return self.operator(
            "POST",
            f"/bindings/{binding_id}/reimport",
            {
                "expected_revision": expected_revision,
                "credential": recipient.envelope(plaintext),
                "source": source,
            },
        )

    def check(self, binding_id: str, uid: str, resource_version: str) -> httpx.Response:
        return self.operator(
            "POST",
            f"/bindings/{binding_id}/check",
            {"source": {"uid": uid, "resource_version": resource_version}},
        )

    def disable_binding(self, binding_id: str) -> httpx.Response:
        return self.operator("POST", f"/bindings/{binding_id}/disable")

    # database roles

    def provision(self, application_id: str, body: dict[str, Any]) -> httpx.Response:
        return self.operator("POST", f"/applications/{application_id}/database", body)

    def provision_bundled(
        self, application_id: str, recipient: Recipient
    ) -> tuple[dict[str, Any], str]:
        """Provision the application's role; answer the record and its opened DSN."""

        self.cleanup.role(role_name_for(self.installation, application_id))
        response = self.provision(application_id, {"mode": "bundled"})
        assert response.status_code == 201, response.text
        record: dict[str, Any] = response.json()
        return record, recipient.open(record["sealed_dsn"]).decode()

    def rotate(self, application_id: str, expected_dsn_revision: int) -> httpx.Response:
        return self.operator(
            "POST",
            f"/applications/{application_id}/database/rotate",
            {"expected_dsn_revision": expected_dsn_revision},
        )

    def report_status(
        self, application_id: str, status: str, reason: str | None, dsn_revision: int
    ) -> httpx.Response:
        return self.worker(
            "PUT",
            f"/applications/{application_id}/database/status",
            {"status": status, "reason": reason, "dsn_revision": dsn_revision},
        )


@dataclass
class World:
    """One registered application with a member, a binding and a provisioned role."""

    store: SharedStore
    application_id: str
    recipient: Recipient
    agent_id: str
    binding: dict[str, Any]
    plaintext: bytes
    source: dict[str, Any]
    grant_id: str | None
    role_name: str
    dsn: str

    @property
    def binding_id(self) -> str:
        return str(self.binding["binding_id"])

    @property
    def binding_uuid(self) -> uuid.UUID:
        return uuid.UUID(self.binding_id)

    def reimport(self, expected_revision: int, plaintext: bytes, **source: str) -> httpx.Response:
        self.source = source_metadata(
            uid=source.get("uid", self.source["uid"]),
            resource_version=source.get("resource_version", self.source["resource_version"]),
        )
        return self.store.reimport(
            self.binding_id, expected_revision, self.recipient, plaintext, self.source
        )


def build_world(store: SharedStore, *, refresh: bool = True) -> World:
    application_id = store.register_application()["application_id"]
    recipient = store.register_recipient(application_id)
    agent_id = store.create_agent()
    member = store.put_members(application_id, agent_id, [{"alias": "primary", "tools": None}])
    assert member.status_code == 200, member.text
    plaintext = json.dumps({SLOT: secret_sentinel()}).encode()
    source = source_metadata(uid=f"uid-{uuid.uuid4().hex[:12]}")
    grant_id = f"grant:{uuid.uuid4().hex}" if refresh else None
    refresh_grant = (
        {"grant_id": grant_id, "client_config_fingerprint": client_fingerprint(grant_id)}
        if grant_id is not None
        else None
    )
    binding = store.import_binding(
        application_id,
        agent_id,
        recipient,
        plaintext,
        source=source,
        refresh_grant=refresh_grant,
    )
    database, dsn = store.provision_bundled(application_id, recipient)
    return World(
        store=store,
        application_id=application_id,
        recipient=recipient,
        agent_id=agent_id,
        binding=binding,
        plaintext=plaintext,
        source=source,
        grant_id=grant_id,
        role_name=str(database["role_name"]),
        dsn=dsn,
    )


# --- log capture ------------------------------------------------------------


@contextlib.contextmanager
def capture_debug_logs(caplog: pytest.LogCaptureFixture) -> Iterator[None]:
    """Capture DEBUG records from root, ``curie_api`` and ``sqlalchemy``.

    ``configure_logging`` turns propagation off on ``curie_api``, so pytest's
    root handler is attached to it by hand (as ``test_hook_partition`` does).
    """

    service = logging.getLogger("curie_api")
    service.addHandler(caplog.handler)
    try:
        with (
            caplog.at_level(logging.DEBUG),
            caplog.at_level(logging.DEBUG, logger="curie_api"),
            caplog.at_level(logging.DEBUG, logger="sqlalchemy"),
        ):
            yield
    finally:
        service.removeHandler(caplog.handler)


def captured_log_text(records: Sequence[logging.LogRecord]) -> str:
    formatter = logging.Formatter()
    parts: list[str] = []
    for record in records:
        try:
            parts.append(record.getMessage())
        except (TypeError, ValueError):
            parts.append(str(record.msg))
        parts.append(repr(record.__dict__))
        if record.exc_info:
            parts.append(formatter.formatException(record.exc_info))
    return "\n".join(parts)
