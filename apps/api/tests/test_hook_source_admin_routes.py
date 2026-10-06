"""Source policy administration routes over HTTP, @spec PROTECTED-HOOK-SOURCE-3/5/6/7/9/10.

Every case drives the real API (``create_app`` lifespan, migrated isolated
Postgres, the app's Valkey) through the five administrative routes under the
platform API key, against a separate disposable TLS Valkey whose default user
is disabled. Two distinct named principals are provisioned on it with the
closed ``source_writer`` and ``control_reader`` recipes; the fixture
administrator seeds records, injects faults and inspects state and never
appears in the runtime directory. The provisioner directory is written
privately per test (0700 directory, 0600 files) and named to the API through
``CURIE_PROTECTED_RUNTIME_DIR``.

The disposable broker is the protected hooks package's own owned container
helper (``packages/protected-hooks/tests/admission_broker.py``), loaded the way
the support probe broker suite loads it. Faults are real store operations on
owned resources only: ACL changes on owned users, owned key deletion and owned
container restart. Product code gains no failure injection parameter.

Broker use is measured from the broker's own ``total_connections_received``
counter: the fixture administrator reuses one pooled connection, so any
increase across a request is a connection the API opened.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import uuid
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest
from _migration_support import sql_dicts
from curie_api import hook_signing
from curie_api.auth import CONSOLE_SESSION_COOKIE
from curie_api.config import get_settings
from curie_protected_hooks.broker_metadata import metadata_acl_rules
from curie_protected_hooks.source_policy_records import policy_fingerprint
from test_hook_source_support import (  # noqa: F401  (support_db is a fixture)
    BODIES,
    HOOK,
    _intent,
    _sql_state,
    legacy_secret,
    post_support,
    probe_app,
    scoped_secret,
    support_db,
    support_headers,
)
from test_hook_source_support_broker import (
    BUNDLE,
    ENV,
    OTHER,
    QUALIFICATION,
    RUNTIME,
    _load_admission_broker,
    manifest_value,
    unblock_fifos,
    use_runtime_dir,
)

_broker = _load_admission_broker()
pytestmark = pytest.mark.usefixtures("support_db")
admission_service = _broker.admission_service
FixtureSecret = _broker.FixtureSecret
canonical = _broker.canonical
broker_snapshot = _broker.snapshot

API_KEY_DETAIL = "missing or invalid API key"
WRITER_FILE = "source_writer.json"
TIMEOUT = 45
PROTECTED_TARGET = dict(
    mode="protected",
    tool_access="read-only",
    runtime_id=RUNTIME,
    qualification_id=QUALIFICATION,
    bundle_digest=BUNDLE,
)
ORDINARY_TARGET = dict(
    mode="ordinary", tool_access=None, runtime_id=None, qualification_id=None, bundle_digest=None
)


# -- owned principals and provisioner files -------------------------------------------------


class Principal:
    """Disposable named broker principal, @spec PROTECTED-HOOK-LANE-3 PROTECTED-HOOK-SOURCE-6."""

    def __init__(self, role: str) -> None:
        """@spec PROTECTED-HOOK-LANE-3."""
        self.role = role
        self.username = role.replace("_", "-") + "-" + secrets.token_hex(6)
        self.password = FixtureSecret(secrets.token_hex(24))

    def __repr__(self) -> str:
        """@spec PROTECTED-HOOK-LANE-3."""
        return "<fixture-principal>"

    def install(self, broker: Any) -> None:
        """Only the provisioner installs the closed recipe, @spec PROTECTED-HOOK-LANE-3."""
        broker.command(
            "ACL",
            "SETUSER",
            self.username,
            "reset",
            "on",
            ">" + self.password,
            *metadata_acl_rules(self.role),
        )


def _remove_principal(broker: Any, principal: Principal) -> None:
    """Owned principal cleanup that tolerates an owned restart, @spec PROTECTED-HOOK-LANE-3."""
    users = [
        user.decode() if isinstance(user, bytes) else user
        for user in broker.command("ACL", "USERS")
    ]
    if principal.username in users:
        broker.command("CLIENT", "KILL", "USER", principal.username, "SKIPME", "yes")
        broker.command("ACL", "DELUSER", principal.username)


@pytest.fixture
def principals(admission_service: Any) -> Iterator[Any]:
    """Fresh broker keys plus distinct writer and reader principals.

    @spec PROTECTED-HOOK-LANE-3 @spec PROTECTED-HOOK-SOURCE-6.
    """
    broker = admission_service
    broker.command("FLUSHDB")
    broker.reader = Principal("control_reader")
    broker.writer = Principal("source_writer")
    assert broker.reader.username != broker.writer.username
    broker.reader.install(broker)
    broker.writer.install(broker)
    try:
        yield broker
    finally:
        try:
            _remove_principal(broker, broker.reader)
        finally:
            _remove_principal(broker, broker.writer)
            broker.command("FLUSHDB")


class Provisioned:
    """One provisioner runtime directory for the disposable broker.

    ``files`` overrides exact bytes per file (``None`` = absent) and
    ``after_write`` applies layout changes such as a FIFO. @spec PROTECTED-HOOK-SOURCE-6/9.
    """

    def __init__(self, broker: Any, directory: Path) -> None:
        """@spec PROTECTED-HOOK-SOURCE-6/9."""
        self.directory = directory
        self.manifest = manifest_value(
            port=broker.port, pin=broker.pin, run_id=broker.command("INFO", "server")["run_id"]
        )
        self.ca_pem = str(broker.ca_pem)
        self.bootstrap: dict[str, Any] = {
            "schema_version": 1,
            "max_readiness_ms": "60000",
            "control_reader": {
                "username": broker.reader.username,
                "password": broker.reader.password,
            },
        }
        self.writer: dict[str, Any] = {
            "schema_version": 1,
            "source_writer": {
                "username": broker.writer.username,
                "password": broker.writer.password,
            },
        }
        self.files: dict[str, bytes | None] = {}
        self.after_write: list[Callable[[Path], None]] = []
        self.forbidden_values = [
            broker.reader.username,
            broker.reader.password,
            broker.writer.username,
            broker.writer.password,
            self.ca_pem,
            broker.pin,
            f"127.0.0.1:{broker.port}",
            str(directory),
        ]

    def __repr__(self) -> str:
        """@spec PROTECTED-HOOK-LANE-3."""
        return "<fixture-provisioned-runtime>"

    def write(self) -> None:
        """Provisioner-owned private directory, @spec PROTECTED-HOOK-SOURCE-6/9."""
        if self.directory.exists():
            unblock_fifos(self.directory)
            for path in self.directory.iterdir():
                if path.is_dir() and not path.is_symlink():
                    path.rmdir()
                else:
                    path.unlink()
        self.directory.mkdir(mode=0o700, exist_ok=True)
        contents: dict[str, bytes | None] = {
            "manifest.json": canonical(self.manifest),
            "ca.pem": self.ca_pem.encode("ascii"),
            "bootstrap.json": json.dumps(self.bootstrap).encode(),
            WRITER_FILE: json.dumps(self.writer).encode(),
        }
        contents.update(self.files)
        for name, payload in contents.items():
            if payload is not None:
                path = self.directory / name
                path.write_bytes(payload)
                path.chmod(0o600)
        for change in self.after_write:
            change(self.directory)


def connections(broker: Any) -> int:
    """Connections the broker has accepted so far, @spec PROTECTED-HOOK-SOURCE-3/10."""
    return int(broker.command("INFO", "stats")["total_connections_received"])


def source_key(agent: str) -> str:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    return f"protected:source:{agent}:{HOOK}"


def source_record(broker: Any, agent: str) -> dict[str, Any] | None:
    """The broker source record as the fixture administrator reads it.

    @spec PROTECTED-HOOK-SOURCE-6/7.
    """
    raw = broker.command("GET", source_key(agent))
    return None if raw is None else json.loads(raw)


def tombstone_fingerprint(agent: str, generation: int, operation: str, legacy: int) -> str:
    """SOURCE-6 fingerprint of a committed ordinary row, @spec PROTECTED-HOOK-SOURCE-6."""
    return policy_fingerprint(
        dict(
            agent_id=agent,
            hook=HOOK,
            generation=str(generation),
            operation_id=operation,
            legacy_generation=str(legacy),
            **ORDINARY_TARGET,
        )
    )


def counter(agent: str) -> int:
    """The agent's legacy hook counter, @spec PROTECTED-HOOK-SOURCE-5."""
    return int(
        sql_dicts("SELECT hook_generation FROM curie.agents WHERE id=:a", {"a": agent})[0][
            "hook_generation"
        ]
    )


def set_counter(agent: str, value: int) -> None:
    """@spec PROTECTED-HOOK-SOURCE-5."""
    sql_dicts("UPDATE curie.agents SET hook_generation=:v WHERE id=:a", {"a": agent, "v": value})


def ledger(agent: str) -> list[tuple[str, int, str, str]]:
    """(operation, generation, status, intent) per attempt, @spec PROTECTED-HOOK-SOURCE-10."""
    return [
        (str(row["operation_id"]), int(row["generation"]), row["status"], row["intent_sha256"])
        for row in sql_dicts(
            "SELECT * FROM curie.hook_source_operations WHERE agent_id=:a ORDER BY generation",
            {"a": agent},
        )
    ]


def policy_row(agent: str) -> dict[str, Any] | None:
    """The committed policy row with string identities, @spec PROTECTED-HOOK-SOURCE-2/10."""
    rows = sql_dicts(
        "SELECT * FROM curie.hook_source_policies WHERE agent_id=:a AND hook=:h",
        {"a": agent, "h": HOOK},
    )
    if not rows:
        return None
    row = rows[0]
    return {
        "generation": int(row["generation"]),
        "operation_id": str(row["operation_id"]),
        "mode": row["mode"],
        "tool_access": row["tool_access"],
        "runtime_id": None if row["runtime_id"] is None else str(row["runtime_id"]),
        "qualification_id": (
            None if row["qualification_id"] is None else str(row["qualification_id"])
        ),
        "bundle_digest": row["bundle_digest"],
        "legacy_generation": int(row["legacy_generation"]),
    }


def seed_attempt(agent: str, generation: int, target: dict[str, Any], status: str) -> str:
    """One ledger attempt, @spec PROTECTED-HOOK-SOURCE-10."""
    operation = str(uuid.uuid4())
    sql_dicts(
        "INSERT INTO curie.hook_source_operations "
        "(agent_id,hook,operation_id,intent_sha256,status,generation) "
        "VALUES (:agent,:hook,:operation,:intent,:status,:generation)",
        dict(
            agent=agent,
            hook=HOOK,
            operation=operation,
            intent=_intent(target),
            status=status,
            generation=generation,
        ),
    )
    return operation


def seed_row(agent: str, generation: int, target: dict[str, Any]) -> str:
    """Committed row whose legacy counter equals the agent's, @spec PROTECTED-HOOK-SOURCE-2/5/10."""
    operation = seed_attempt(agent, generation, target, "committed")
    sql_dicts(
        "INSERT INTO curie.hook_source_policies "
        "(agent_id,hook,operation_id,generation,mode,tool_access,runtime_id,"
        "qualification_id,bundle_digest,legacy_generation) "
        "VALUES (:agent,:hook,:operation,:generation,:mode,:tool_access,:runtime_id,"
        ":qualification_id,:bundle_digest,:legacy)",
        dict(
            agent=agent,
            hook=HOOK,
            operation=operation,
            generation=generation,
            legacy=counter(agent),
            **target,
        ),
    )
    return operation


def publish_tombstone(broker: Any, agent: str, generation: int, operation: str) -> None:
    """Seed the published ordinary record as the writer would.

    @spec PROTECTED-HOOK-SOURCE-6.
    """
    fingerprint = tombstone_fingerprint(agent, generation, operation, counter(agent))
    broker.command(
        "SET",
        source_key(agent),
        canonical(
            {
                "floor": str(generation),
                "operation_id": operation,
                "active": {
                    "generation": str(generation),
                    "operation_id": operation,
                    "mode": "ordinary",
                    "policy_fingerprint": fingerprint,
                },
            }
        ),
    )


# -- HTTP ----------------------------------------------------------------------------------


def headers() -> dict[str, str]:
    """Platform API key, @spec PROTECTED-HOOK-SOURCE-3."""
    return {"X-API-Key": get_settings().api_key}


def route(agent: str, suffix: str = "", hook: str = HOOK) -> str:
    """@spec PROTECTED-HOOK-SOURCE-3."""
    return f"/agents/{agent}/hooks/{hook}/source-policy{suffix}"


def put_body(expected: str, operation: str, **changes: Any) -> dict[str, Any]:
    """HookSourcePolicyWrite naming the deployment's one runtime, @spec PROTECTED-HOOK-SOURCE-3."""
    body: dict[str, Any] = {
        "expected_generation": expected,
        "operation_id": operation,
        "runtime_id": RUNTIME,
        "qualification_id": QUALIFICATION,
        "bundle_digest": BUNDLE,
    }
    body.update(changes)
    return body


async def get_policy(client: httpx.AsyncClient, agent: str) -> httpx.Response:
    """@spec PROTECTED-HOOK-SOURCE-3."""
    return await client.get(route(agent), headers=headers())


async def put_policy(
    client: httpx.AsyncClient, agent: str, expected: str, operation: str, **changes: Any
) -> httpx.Response:
    """@spec PROTECTED-HOOK-SOURCE-3."""
    return await client.put(
        route(agent), headers=headers(), json=put_body(expected, operation, **changes)
    )


async def delete_policy(
    client: httpx.AsyncClient, agent: str, expected: str, operation: str
) -> httpx.Response:
    """DELETE takes its CAS fields as query parameters, @spec PROTECTED-HOOK-SOURCE-3."""
    return await client.delete(
        route(agent),
        headers=headers(),
        params={"expected_generation": expected, "operation_id": operation},
    )


async def rotate_policy(
    client: httpx.AsyncClient, agent: str, expected: str, operation: str
) -> httpx.Response:
    """@spec PROTECTED-HOOK-SOURCE-3."""
    return await client.post(
        route(agent, "/rotate"),
        headers=headers(),
        json={"expected_generation": expected, "operation_id": operation},
    )


async def get_secret(client: httpx.AsyncClient, agent: str) -> httpx.Response:
    """@spec PROTECTED-HOOK-SOURCE-3."""
    return await client.get(route(agent, "/secret"), headers=headers())


def assert_refusal(
    response: httpx.Response, status: int, code: str, committed: str | None = None
) -> None:
    """The specified source refusal body on a handler response, @spec PROTECTED-HOOK-SOURCE-3."""
    assert response.status_code == status, response.text
    assert response.json() == {"detail": {"code": code, "committed_generation": committed}}
    assert response.headers.get("cache-control") == "no-store", response.headers


def assert_validation_list(response: httpx.Response) -> None:
    """FastAPI's ordinary request shape 422, @spec PROTECTED-HOOK-SOURCE-3."""
    assert response.status_code == 422, response.text
    assert isinstance(response.json()["detail"], list), response.text


def assert_uniform_401(response: httpx.Response) -> None:
    """The existing require_api_key refusal, @spec PROTECTED-HOOK-SOURCE-3."""
    assert response.status_code == 401, response.text
    assert response.json() == {"detail": API_KEY_DETAIL}


def dto(
    agent: str,
    *,
    generation: str,
    legacy: str,
    activation: str = "closed",
    reason: str | None = None,
    protected: bool = False,
    references: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Expected HookSourcePolicyOut without ``updated_at``, @spec PROTECTED-HOOK-SOURCE-3."""
    refs = references or dict(
        runtime_id=RUNTIME, qualification_id=QUALIFICATION, bundle_digest=BUNDLE
    )
    return {
        "agent_id": agent,
        "hook": HOOK,
        "generation": generation,
        "mode": "protected" if protected else "ordinary",
        "tool_access": "read-only" if protected else None,
        "runtime_id": refs["runtime_id"] if protected else None,
        "qualification_id": refs["qualification_id"] if protected else None,
        "bundle_digest": refs["bundle_digest"] if protected else None,
        "legacy_generation": legacy,
        "activation": activation,
        "refusal_reason": reason,
    }


def assert_dto(response: httpx.Response, want: dict[str, Any], *, absent: bool = False) -> None:
    """Exact DTO with a present (or, for no row, null) audit time, @spec PROTECTED-HOOK-SOURCE-3."""
    assert response.status_code == 200, response.text
    assert response.headers.get("cache-control") == "no-store", response.headers
    body = response.json()
    updated = body.pop("updated_at")
    assert body == want
    assert (updated is None) == absent


class Captured(logging.Handler):
    """Every API log record during a scenario, @spec PROTECTED-HOOK-SOURCE-3."""

    def __init__(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-3."""
        super().__init__(level=logging.DEBUG)
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        """@spec PROTECTED-HOOK-SOURCE-3."""
        try:
            self.lines.append(record.getMessage() + repr(record.__dict__))
        except Exception:  # noqa: BLE001  A capturing log handler must never raise.
            self.lines.append(repr(record.__dict__))


@asynccontextmanager
async def captured_logs() -> AsyncIterator[Captured]:
    """Attach to root and the non-propagating API logger, @spec PROTECTED-HOOK-SOURCE-3."""
    handler = Captured()
    loggers = [logging.getLogger(), logging.getLogger("curie_api")]
    levels = [logger.level for logger in loggers]
    for logger in loggers:
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
    try:
        yield handler
    finally:
        for logger, level in zip(loggers, levels, strict=True):
            logger.removeHandler(handler)
            logger.setLevel(level)


def assert_no_secret(
    texts: list[str], agent: str, rt: Provisioned | None, generations: list[int]
) -> None:
    """No source key, legacy key, platform key or provisioning material.

    @spec PROTECTED-HOOK-SOURCE-3.
    """
    forbidden = [get_settings().api_key, legacy_secret(agent)]
    forbidden += [legacy_secret(agent, value) for value in range(0, 4)]
    forbidden += [scoped_secret(agent, value) for value in generations]
    if rt is not None:
        forbidden += [value for value in rt.forbidden_values if value]
    for text in texts:
        for value in forbidden:
            assert value not in text, "a response or log carries key or provisioning material"


@asynccontextmanager
async def admin_app(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, broker: Any | None
) -> AsyncIterator[tuple[Any, httpx.AsyncClient, str, Provisioned | None]]:
    """App, client, fresh agent and (with a broker) a provisioned runtime directory.

    @spec PROTECTED-HOOK-SOURCE-3/6/9/10.
    """
    rt: Provisioned | None = None
    if broker is not None:
        directory = tmp_path / ("runtime-" + secrets.token_hex(4))
        use_runtime_dir(monkeypatch, directory)
        rt = Provisioned(broker, directory)
        rt.write()
    try:
        async with probe_app() as (app, client, agent):
            yield app, client, agent, rt
    finally:
        if rt is not None and rt.directory.exists():
            unblock_fifos(rt.directory)


def run(scenario: Callable[[], Any]) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3."""
    asyncio.run(asyncio.wait_for(scenario(), TIMEOUT))


async def state(agent: str) -> Any:
    """Policy, ledger, counter, runs and workspaces, @spec PROTECTED-HOOK-SOURCE-10."""
    return await asyncio.to_thread(_sql_state, agent)


# -- GET -----------------------------------------------------------------------------------


@pytest.mark.parametrize("kind", ["never", "pending"])
def test_get_without_row_reports_closed_generation_zero_and_locked_counter(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    """No row: generation 0, null or pending_history reason, the agent's locked counter.

    A valid provisioned runtime makes the absence of broker use meaningful.
    @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-5 @spec PROTECTED-HOOK-SOURCE-10.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, _rt):
            await asyncio.to_thread(set_counter, agent, 7)
            if kind == "pending":
                await asyncio.to_thread(seed_attempt, agent, 9, PROTECTED_TARGET, "pending")
            before, opened = await state(agent), connections(principals)
            response = await get_policy(client, agent)
            assert_dto(
                response,
                dto(
                    agent,
                    generation="0",
                    legacy="7",
                    reason="pending_history" if kind == "pending" else None,
                ),
                absent=True,
            )
            assert connections(principals) == opened, "GET of no row opened a broker connection"
            assert await state(agent) == before

    run(scenario)


@pytest.mark.parametrize("references", ["deployment", "foreign"])
def test_get_protected_row_is_closed_publication_deferred_without_broker(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, references: str
) -> None:
    """A committed protected row reports closed with publication_deferred, no broker use.

    @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-6.
    """
    refs = (
        dict(runtime_id=RUNTIME, qualification_id=QUALIFICATION, bundle_digest=BUNDLE)
        if references == "deployment"
        else dict(runtime_id=OTHER, qualification_id=str(uuid.uuid4()), bundle_digest="b" * 64)
    )

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, _rt):
            await asyncio.to_thread(set_counter, agent, 4)
            target = dict(mode="protected", tool_access="read-only", **refs)
            await asyncio.to_thread(seed_row, agent, 9, target)
            before, opened = await state(agent), connections(principals)
            response = await get_policy(client, agent)
            assert_dto(
                response,
                dto(
                    agent,
                    generation="9",
                    legacy="4",
                    reason="publication_deferred",
                    protected=True,
                    references=refs,
                ),
            )
            assert connections(principals) == opened, "GET of a protected row opened a broker"
            assert await state(agent) == before

    run(scenario)


def _tombstone_case(name: str) -> Callable[[Any, str, int, str, Provisioned], None]:
    """Broker or runtime fact that differs from a published tombstone.

    @spec PROTECTED-HOOK-SOURCE-3.
    """

    def apply(broker: Any, agent: str, generation: int, op: str, rt: Provisioned) -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/6."""
        record = source_record(broker, agent)
        assert record is not None
        if name == "key_missing":
            broker.command("DEL", source_key(agent))
        elif name == "floor_ahead":
            other = str(uuid.uuid4())
            broker.command(
                "SET",
                source_key(agent),
                canonical({"floor": str(generation + 1), "operation_id": other, "active": None}),
            )
        elif name == "active_null":
            record["active"] = None
            broker.command("SET", source_key(agent), canonical(record))
        elif name == "fingerprint_differs":
            record["active"]["policy_fingerprint"] = "0" * 64
            broker.command("SET", source_key(agent), canonical(record))
        elif name == "mode_protected":
            record["active"]["mode"] = "protected"
            broker.command("SET", source_key(agent), canonical(record))
        elif name == "reader_disabled":
            broker.command("ACL", "SETUSER", broker.reader.username, "off")
        elif name == "reader_password":
            rt.bootstrap["control_reader"]["password"] = FixtureSecret(secrets.token_hex(24))
            rt.write()
        elif name == "run_id_differs":
            rt.manifest["broker_identity"]["run_id"] = "a" * 40
            rt.write()
        elif name == "manifest_invalid":
            rt.files["manifest.json"] = b"{}"
            rt.write()
        elif name == "ca_missing":
            rt.files["ca.pem"] = None
            rt.write()
        elif name != "published":
            raise AssertionError(name)

    return apply


TOMBSTONE_CASES = [
    ("published", "active", None),
    ("key_missing", "closed", "source_closed"),
    ("floor_ahead", "closed", "source_closed"),
    ("active_null", "closed", "source_closed"),
    ("fingerprint_differs", "closed", "source_closed"),
    ("mode_protected", "closed", "source_closed"),
    ("reader_disabled", "closed", "broker_unavailable"),
    ("reader_password", "closed", "broker_unavailable"),
    ("run_id_differs", "closed", "broker_unavailable"),
    ("manifest_invalid", "closed", "runtime_unavailable"),
    ("ca_missing", "closed", "runtime_unavailable"),
]


@pytest.mark.parametrize(
    ("case", "activation", "reason"), TOMBSTONE_CASES, ids=[c[0] for c in TOMBSTONE_CASES]
)
def test_get_tombstone_activation_reads_one_authenticated_reader_session(
    principals: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
    activation: str,
    reason: str | None,
) -> None:
    """A tombstone is active only when the reader sees its exact published record.

    Otherwise closed with the first applicable reason; GET never writes.
    @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-9.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/6."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, rt):
            assert rt is not None
            await asyncio.to_thread(set_counter, agent, 2)
            op = await asyncio.to_thread(seed_row, agent, 9, ORDINARY_TARGET)
            await asyncio.to_thread(publish_tombstone, principals, agent, 9, op)
            _tombstone_case(case)(principals, agent, 9, op, rt)
            before = await state(agent), broker_snapshot(principals)
            async with captured_logs() as logs:
                response = await get_policy(client, agent)
            assert_dto(
                response,
                dto(agent, generation="9", legacy="2", activation=activation, reason=reason),
            )
            assert (await state(agent), broker_snapshot(principals)) == before
            assert_no_secret([response.text, *logs.lines], agent, rt, [9])

    run(scenario)


def test_get_tombstone_without_runtime_setting_is_runtime_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Setting unset: the tombstone reports closed runtime_unavailable.

    @spec PROTECTED-HOOK-SOURCE-3/10.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/10."""
        async with admin_app(tmp_path, monkeypatch, None) as (_app, client, agent, _rt):
            await asyncio.to_thread(seed_row, agent, 9, ORDINARY_TARGET)
            before = await state(agent)
            response = await get_policy(client, agent)
            assert_dto(
                response, dto(agent, generation="9", legacy="0", reason="runtime_unavailable")
            )
            assert await state(agent) == before

    run(scenario)


def test_get_unknown_agent_is_404_source_agent_not_found(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3."""
        async with admin_app(tmp_path, monkeypatch, None) as (_app, client, _agent, _rt):
            response = await get_policy(client, str(uuid.uuid4()))
            assert_refusal(response, 404, "source_agent_not_found")

    run(scenario)


# -- PUT and rotate: commit, then deferred publication -------------------------------------


def test_protected_put_commits_then_answers_publication_deferred(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PUT registers, reserves, commits the row with the SOURCE-5 counter bump, then 503.

    The broker holds the reservation with no active record; GET stays closed
    with publication_deferred and reports the advanced legacy counter.
    @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-5 @spec PROTECTED-HOOK-SOURCE-6
    @spec PROTECTED-HOOK-SOURCE-7 @spec PROTECTED-HOOK-SOURCE-10.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/5/6/10."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, rt):
            await asyncio.to_thread(set_counter, agent, 3)
            op = str(uuid.uuid4())
            async with captured_logs() as logs:
                response = await put_policy(client, agent, "0", op)
                assert_refusal(response, 503, "source_publication_deferred", "1")
                assert await asyncio.to_thread(ledger, agent) == [
                    (op, 1, "committed", _intent(PROTECTED_TARGET))
                ]
                assert await asyncio.to_thread(policy_row, agent) == {
                    "generation": 1,
                    "operation_id": op,
                    **PROTECTED_TARGET,
                    "legacy_generation": 4,
                }
                assert await asyncio.to_thread(counter, agent) == 4
                assert source_record(principals, agent) == {
                    "floor": "1",
                    "operation_id": op,
                    "active": None,
                }
                read = await get_policy(client, agent)
                assert_dto(
                    read,
                    dto(
                        agent,
                        generation="1",
                        legacy="4",
                        reason="publication_deferred",
                        protected=True,
                    ),
                )
            assert_no_secret([response.text, read.text, *logs.lines], agent, rt, [1])

    run(scenario)


def test_protected_put_revokes_a_published_tombstone(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reservation revokes the active ordinary record above every durable attempt.

    @spec PROTECTED-HOOK-SOURCE-5 @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-7
    @spec PROTECTED-HOOK-SOURCE-10.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-6/7."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, _rt):
            old = await asyncio.to_thread(seed_row, agent, 9, ORDINARY_TARGET)
            await asyncio.to_thread(publish_tombstone, principals, agent, 9, old)
            stale_pending = await asyncio.to_thread(
                seed_attempt, agent, 12, ORDINARY_TARGET, "pending"
            )
            op = str(uuid.uuid4())
            response = await put_policy(client, agent, "9", op)
            assert_refusal(response, 503, "source_publication_deferred", "13")
            assert source_record(principals, agent) == {
                "floor": "13",
                "operation_id": op,
                "active": None,
            }
            assert await asyncio.to_thread(ledger, agent) == [
                (old, 9, "committed", _intent(ORDINARY_TARGET)),
                (stale_pending, 12, "pending", _intent(ORDINARY_TARGET)),
                (op, 13, "committed", _intent(PROTECTED_TARGET)),
            ]
            assert await asyncio.to_thread(counter, agent) == 1
            assert_dto(
                await get_policy(client, agent),
                dto(
                    agent,
                    generation="13",
                    legacy="1",
                    reason="publication_deferred",
                    protected=True,
                ),
            )

    run(scenario)


def test_rotate_keeps_the_protected_target_and_answers_publication_deferred(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rotate allocates a fresh generation for the same target without a counter bump.

    @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-5 @spec PROTECTED-HOOK-SOURCE-6
    @spec PROTECTED-HOOK-SOURCE-10.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/6."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, _rt):
            await asyncio.to_thread(set_counter, agent, 5)
            first = await asyncio.to_thread(seed_row, agent, 9, PROTECTED_TARGET)
            op = str(uuid.uuid4())
            response = await rotate_policy(client, agent, "9", op)
            assert_refusal(response, 503, "source_publication_deferred", "10")
            assert await asyncio.to_thread(policy_row, agent) == {
                "generation": 10,
                "operation_id": op,
                **PROTECTED_TARGET,
                "legacy_generation": 5,
            }
            assert await asyncio.to_thread(counter, agent) == 5
            assert await asyncio.to_thread(ledger, agent) == [
                (first, 9, "committed", _intent(PROTECTED_TARGET)),
                (op, 10, "committed", _intent(PROTECTED_TARGET)),
            ]
            assert source_record(principals, agent) == {
                "floor": "10",
                "operation_id": op,
                "active": None,
            }
            assert_dto(
                await get_policy(client, agent),
                dto(
                    agent,
                    generation="10",
                    legacy="5",
                    reason="publication_deferred",
                    protected=True,
                ),
            )

    run(scenario)


@pytest.mark.parametrize("kind", ["absent", "pending", "tombstone"])
def test_rotate_of_an_ordinary_or_absent_row_is_409_without_effects(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-10."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, _rt):
            if kind == "pending":
                await asyncio.to_thread(seed_attempt, agent, 9, PROTECTED_TARGET, "pending")
            if kind == "tombstone":
                await asyncio.to_thread(seed_row, agent, 9, ORDINARY_TARGET)
            expected = "9" if kind == "tombstone" else "0"
            before = await state(agent), broker_snapshot(principals)
            opened = connections(principals)
            response = await rotate_policy(client, agent, expected, str(uuid.uuid4()))
            assert_refusal(response, 409, "source_rotation_conflict")
            assert (await state(agent), broker_snapshot(principals)) == before
            assert connections(principals) == opened

    run(scenario)


# -- references --------------------------------------------------------------------------------


REFERENCE_CHANGES = [
    ("runtime", {"runtime_id": OTHER}),
    ("qualification", {"qualification_id": str(uuid.UUID(int=12))}),
    ("bundle", {"bundle_digest": "b" * 64}),
]


@pytest.mark.parametrize("agent_kind", ["fresh", "unknown", "stale"])
@pytest.mark.parametrize(
    "changes", [c[1] for c in REFERENCE_CHANGES], ids=[c[0] for c in REFERENCE_CHANGES]
)
def test_foreign_reference_is_422_before_any_broker_call_registration_or_sql_write(
    principals: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    changes: dict[str, str],
    agent_kind: str,
) -> None:
    """Step 5 precedes the gate: also before the 404 and the stale CAS 409.

    @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-10.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/10."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, rt):
            target = str(uuid.uuid4()) if agent_kind == "unknown" else agent
            expected = "5" if agent_kind == "stale" else "0"
            before = await state(agent), broker_snapshot(principals)
            opened = connections(principals)
            async with captured_logs() as logs:
                response = await put_policy(client, target, expected, str(uuid.uuid4()), **changes)
            assert_refusal(response, 422, "unknown_source_reference")
            assert connections(principals) == opened, "a broker connection preceded step 5"
            assert (await state(agent), broker_snapshot(principals)) == before
            assert_no_secret([response.text, *logs.lines], agent, rt, [])

    run(scenario)


@pytest.mark.parametrize(
    "changes", [c[1] for c in REFERENCE_CHANGES], ids=[c[0] for c in REFERENCE_CHANGES]
)
def test_rotate_of_a_row_naming_another_runtime_is_422_without_history(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, changes: dict[str, str]
) -> None:
    """Rotate applies the step 5 check to the current row, @spec PROTECTED-HOOK-SOURCE-3/10."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/10."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, _rt):
            await asyncio.to_thread(seed_row, agent, 9, {**PROTECTED_TARGET, **changes})
            before = await state(agent), broker_snapshot(principals)
            opened = connections(principals)
            response = await rotate_policy(client, agent, "9", str(uuid.uuid4()))
            assert_refusal(response, 422, "unknown_source_reference")
            assert connections(principals) == opened
            assert (await state(agent), broker_snapshot(principals)) == before

    run(scenario)


def test_unknown_agent_put_with_deployment_references_is_404(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, _rt):
            before = broker_snapshot(principals)
            response = await put_policy(client, str(uuid.uuid4()), "0", str(uuid.uuid4()))
            assert_refusal(response, 404, "source_agent_not_found")
            assert broker_snapshot(principals) == before
            assert await asyncio.to_thread(ledger, agent) == []

    run(scenario)


# -- DELETE --------------------------------------------------------------------------------


def test_delete_publishes_the_ordinary_tombstone_and_replays_idempotently(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DELETE of a protected row commits and publishes a tombstone; 200 active.

    The counter does not move; an exact replay (stale expected generation)
    returns the same DTO and changes nothing. @spec PROTECTED-HOOK-SOURCE-3
    @spec PROTECTED-HOOK-SOURCE-5 @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-7.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/6/7."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, rt):
            await asyncio.to_thread(set_counter, agent, 6)
            protected = await asyncio.to_thread(seed_row, agent, 9, PROTECTED_TARGET)
            op = str(uuid.uuid4())
            async with captured_logs() as logs:
                response = await delete_policy(client, agent, "9", op)
            want = dto(agent, generation="10", legacy="6", activation="active")
            assert_dto(response, want)
            assert await asyncio.to_thread(policy_row, agent) == {
                "generation": 10,
                "operation_id": op,
                **ORDINARY_TARGET,
                "legacy_generation": 6,
            }
            assert await asyncio.to_thread(counter, agent) == 6
            assert await asyncio.to_thread(ledger, agent) == [
                (protected, 9, "committed", _intent(PROTECTED_TARGET)),
                (op, 10, "committed", _intent(ORDINARY_TARGET)),
            ]
            assert source_record(principals, agent) == {
                "floor": "10",
                "operation_id": op,
                "active": {
                    "generation": "10",
                    "operation_id": op,
                    "mode": "ordinary",
                    "policy_fingerprint": tombstone_fingerprint(agent, 10, op, 6),
                },
            }
            assert_dto(await get_policy(client, agent), want)
            before = await state(agent), broker_snapshot(principals)
            replay = await delete_policy(client, agent, "9", op)
            assert_dto(replay, want)
            assert (await state(agent), broker_snapshot(principals)) == before
            assert_no_secret([response.text, replay.text, *logs.lines], agent, rt, [9, 10])

    run(scenario)


@pytest.mark.parametrize("expected", ["0", "4"])
def test_delete_with_only_pending_history_commits_a_tombstone_without_rotation(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, expected: str
) -> None:
    """Pending history and no row: a fresh tombstone above every attempt, counter unchanged.

    A stale expected generation is still 409 (only an absent row without
    history is source_not_configured). @spec PROTECTED-HOOK-SOURCE-3
    @spec PROTECTED-HOOK-SOURCE-5 @spec PROTECTED-HOOK-SOURCE-7 @spec PROTECTED-HOOK-SOURCE-10.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/10."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, _rt):
            await asyncio.to_thread(set_counter, agent, 2)
            pending = await asyncio.to_thread(seed_attempt, agent, 9, PROTECTED_TARGET, "pending")
            op = str(uuid.uuid4())
            before = await state(agent), broker_snapshot(principals)
            response = await delete_policy(client, agent, expected, op)
            if expected != "0":
                assert_refusal(response, 409, "stale_source_generation")
                assert (await state(agent), broker_snapshot(principals)) == before
                return
            assert_dto(response, dto(agent, generation="10", legacy="2", activation="active"))
            assert await asyncio.to_thread(counter, agent) == 2
            assert await asyncio.to_thread(ledger, agent) == [
                (pending, 9, "pending", _intent(PROTECTED_TARGET)),
                (op, 10, "committed", _intent(ORDINARY_TARGET)),
            ]
            record = source_record(principals, agent)
            assert record is not None and record["floor"] == "10"
            assert record["active"] == {
                "generation": "10",
                "operation_id": op,
                "mode": "ordinary",
                "policy_fingerprint": tombstone_fingerprint(agent, 10, op, 2),
            }

    run(scenario)


@pytest.mark.parametrize("expected", ["0", "5"])
def test_delete_of_an_absent_row_without_history_is_409_source_not_configured(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, expected: str
) -> None:
    """After the agent lookup and before CAS; no broker call or write.

    @spec PROTECTED-HOOK-SOURCE-3/10.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/10."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, _rt):
            before = await state(agent), broker_snapshot(principals)
            opened = connections(principals)
            response = await delete_policy(client, agent, expected, str(uuid.uuid4()))
            assert_refusal(response, 409, "source_not_configured")
            assert connections(principals) == opened
            assert (await state(agent), broker_snapshot(principals)) == before
            unknown = await delete_policy(client, str(uuid.uuid4()), expected, str(uuid.uuid4()))
            assert_refusal(unknown, 404, "source_agent_not_found")

    run(scenario)


# -- CAS, replay and operation conflicts ------------------------------------------------------


def test_stale_cas_is_409_without_broker_call_or_write(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-10."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, _rt):
            op = await asyncio.to_thread(seed_row, agent, 9, ORDINARY_TARGET)
            await asyncio.to_thread(publish_tombstone, principals, agent, 9, op)
            before = await state(agent), broker_snapshot(principals)
            opened = connections(principals)
            for response in (
                await put_policy(client, agent, "8", str(uuid.uuid4())),
                await delete_policy(client, agent, "0", str(uuid.uuid4())),
            ):
                assert_refusal(response, 409, "stale_source_generation")
            assert connections(principals) == opened
            assert (await state(agent), broker_snapshot(principals)) == before

    run(scenario)


def test_exact_protected_replay_answers_the_committed_generation_without_new_state(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same operation and intent, even with stale expected generation, allocates nothing.

    After a manifest change the same replay is 422 while GET still shows the
    committed generation. @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-7
    @spec PROTECTED-HOOK-SOURCE-10.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-7."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, rt):
            assert rt is not None
            op = str(uuid.uuid4())
            assert_refusal(
                await put_policy(client, agent, "0", op), 503, "source_publication_deferred", "1"
            )
            before = await state(agent), broker_snapshot(principals)
            for expected in ("0", "5"):
                replay = await put_policy(client, agent, expected, op)
                assert_refusal(replay, 503, "source_publication_deferred", "1")
                assert (await state(agent), broker_snapshot(principals)) == before
            rt.manifest["qualification_id"] = str(uuid.UUID(int=13))
            rt.write()
            assert_refusal(
                await put_policy(client, agent, "0", op), 422, "unknown_source_reference"
            )
            assert (await state(agent), broker_snapshot(principals)) == before
            assert_dto(
                await get_policy(client, agent),
                dto(
                    agent, generation="1", legacy="1", reason="publication_deferred", protected=True
                ),
            )

    run(scenario)


@pytest.mark.parametrize("kind", ["changed", "historical", "pending"])
def test_operation_conflicts_are_409_with_no_broker_call(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    """Different intent under the current operation, a historical or a pending operation.

    @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-7 @spec PROTECTED-HOOK-SOURCE-10.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-7."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, _rt):
            if kind == "pending":
                reused = await asyncio.to_thread(
                    seed_attempt, agent, 3, PROTECTED_TARGET, "pending"
                )
                current = await asyncio.to_thread(seed_row, agent, 9, ORDINARY_TARGET)
            elif kind == "historical":
                reused = await asyncio.to_thread(
                    seed_attempt, agent, 3, PROTECTED_TARGET, "committed"
                )
                current = await asyncio.to_thread(seed_row, agent, 9, ORDINARY_TARGET)
            else:
                current = await asyncio.to_thread(seed_row, agent, 9, PROTECTED_TARGET)
                reused = current
            await asyncio.to_thread(publish_tombstone, principals, agent, 9, current)
            before = await state(agent), broker_snapshot(principals)
            opened = connections(principals)
            if kind == "changed":
                response = await delete_policy(client, agent, "9", reused)
            else:
                response = await put_policy(client, agent, "9", reused)
            assert_refusal(response, 409, "source_operation_conflict")
            assert connections(principals) == opened
            assert (await state(agent), broker_snapshot(principals)) == before

    run(scenario)


# -- lost reservation and broker restart ------------------------------------------------------


def _reprovision_after_restart(broker: Any, rt: Provisioned) -> None:
    """Restart the owned broker, reinstall the same principals; the manifest is unchanged.

    @spec PROTECTED-HOOK-SOURCE-6/7.
    """
    broker.restart()
    broker.reader.install(broker)
    broker.writer.install(broker)


@pytest.mark.parametrize("loss", ["key_deleted", "restart"])
def test_lost_tombstone_reservation_refuses_with_the_committed_generation(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, loss: str
) -> None:
    """A readable record without the committed reservation is source_reservation_lost.

    Only a fresh DELETE recovers, above every durable attempt.
    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-7.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-7."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, rt):
            assert rt is not None
            await asyncio.to_thread(seed_row, agent, 9, PROTECTED_TARGET)
            op = str(uuid.uuid4())
            assert_dto(
                await delete_policy(client, agent, "9", op),
                dto(agent, generation="10", legacy="0", activation="active"),
            )
            if loss == "key_deleted":
                principals.command("DEL", source_key(agent))
            else:
                await asyncio.to_thread(_reprovision_after_restart, principals, rt)
                # The provisioner records the new epoch; the reservation is gone.
                rt.manifest["broker_identity"]["run_id"] = principals.command("INFO", "server")[
                    "run_id"
                ]
                rt.write()
            before = await state(agent)
            replay = await delete_policy(client, agent, "9", op)
            assert_refusal(replay, 503, "source_reservation_lost", "10")
            assert await state(agent) == before
            assert_dto(
                await get_policy(client, agent),
                dto(agent, generation="10", legacy="0", reason="source_closed"),
            )
            fresh = str(uuid.uuid4())
            assert_dto(
                await delete_policy(client, agent, "10", fresh),
                dto(agent, generation="11", legacy="0", activation="active"),
            )

    run(scenario)


def test_broker_restart_is_broker_unavailable_before_registration(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A changed live run_id under the same principals refuses before any history.

    The writer cannot see run_id; the control reader's identity read detects
    the restart, reported on this path as broker_unavailable.
    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-7 @spec PROTECTED-HOOK-SOURCE-10.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-6/10."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, rt):
            assert rt is not None
            await asyncio.to_thread(_reprovision_after_restart, principals, rt)
            before = await state(agent), broker_snapshot(principals)
            async with captured_logs() as logs:
                response = await put_policy(client, agent, "0", str(uuid.uuid4()))
            assert_refusal(response, 503, "broker_unavailable")
            assert (await state(agent), broker_snapshot(principals)) == before
            assert_no_secret([response.text, *logs.lines], agent, rt, [1])

    run(scenario)


@pytest.mark.parametrize("principal", ["writer", "reader"])
def test_disabled_principal_refuses_before_registration(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, principal: str
) -> None:
    """Both distinct principals are required; a failed connection precedes registration.

    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-10 @spec PROTECTED-HOOK-LANE-3.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-6/10."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, _rt):
            principals.command("ACL", "SETUSER", getattr(principals, principal).username, "off")
            before = await state(agent), broker_snapshot(principals)
            response = await put_policy(client, agent, "0", str(uuid.uuid4()))
            assert_refusal(response, 503, "broker_unavailable")
            assert (await state(agent), broker_snapshot(principals)) == before

    run(scenario)


# -- secret route ----------------------------------------------------------------------------


@pytest.mark.parametrize("kind", ["absent", "pending", "tombstone", "protected"])
def test_secret_route_refuses_every_state_and_never_returns_a_key(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    """409 source_not_protected for absent, history only and tombstone; 503 deferred for protected.

    No broker connection, no write and no key in the response or logs.
    @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-6.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/6."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, rt):
            if kind == "pending":
                await asyncio.to_thread(seed_attempt, agent, 9, PROTECTED_TARGET, "pending")
            elif kind == "tombstone":
                op = await asyncio.to_thread(seed_row, agent, 9, ORDINARY_TARGET)
                await asyncio.to_thread(publish_tombstone, principals, agent, 9, op)
            elif kind == "protected":
                await asyncio.to_thread(seed_row, agent, 9, PROTECTED_TARGET)
            before = await state(agent), broker_snapshot(principals)
            opened = connections(principals)
            async with captured_logs() as logs:
                response = await get_secret(client, agent)
            if kind == "protected":
                assert_refusal(response, 503, "source_publication_deferred")
            else:
                assert_refusal(response, 409, "source_not_protected")
            assert "secret" not in response.json()
            assert connections(principals) == opened
            assert (await state(agent), broker_snapshot(principals)) == before
            assert_no_secret([response.text, *logs.lines], agent, rt, [9])

    run(scenario)


# -- no provisioned runtime ------------------------------------------------------------------


def _unset(rt: Provisioned, monkeypatch: pytest.MonkeyPatch) -> None:
    """@spec PROTECTED-HOOK-SOURCE-10."""
    monkeypatch.delenv(ENV, raising=False)
    get_settings.cache_clear()


def _directory_missing(rt: Provisioned, monkeypatch: pytest.MonkeyPatch) -> None:
    """@spec PROTECTED-HOOK-SOURCE-10."""
    monkeypatch.setenv(ENV, str(rt.directory.parent / ("absent-" + secrets.token_hex(4))))
    get_settings.cache_clear()


def _file_missing(name: str) -> Callable[[Provisioned, pytest.MonkeyPatch], None]:
    """@spec PROTECTED-HOOK-SOURCE-10."""

    def apply(rt: Provisioned, monkeypatch: pytest.MonkeyPatch) -> None:
        """@spec PROTECTED-HOOK-SOURCE-10."""
        rt.files[name] = None
        rt.write()

    return apply


def _manifest_invalid(rt: Provisioned, monkeypatch: pytest.MonkeyPatch) -> None:
    """@spec PROTECTED-HOOK-SOURCE-10."""
    rt.files["manifest.json"] = b'{"schema_version":1}'
    rt.write()


UNPROVISIONED: list[tuple[str, Callable[[Provisioned, pytest.MonkeyPatch], None]]] = [
    ("setting-unset", _unset),
    ("directory-missing", _directory_missing),
    ("manifest-missing", _file_missing("manifest.json")),
    ("ca-missing", _file_missing("ca.pem")),
    ("bootstrap-missing", _file_missing("bootstrap.json")),
    ("writer-missing", _file_missing(WRITER_FILE)),
    ("manifest-invalid", _manifest_invalid),
]


async def _mutations(client: httpx.AsyncClient, agent: str) -> list[httpx.Response]:
    """Requests that would otherwise reach 404, 409 or 422 after step 4.

    @spec PROTECTED-HOOK-SOURCE-3/10.
    """
    unknown = str(uuid.uuid4())
    return [
        await put_policy(client, agent, "0", str(uuid.uuid4())),
        await put_policy(client, agent, "0", str(uuid.uuid4()), runtime_id=OTHER),
        await put_policy(client, unknown, "0", str(uuid.uuid4())),
        await put_policy(client, agent, "7", str(uuid.uuid4())),
        await delete_policy(client, agent, "0", str(uuid.uuid4())),
        await delete_policy(client, unknown, "0", str(uuid.uuid4())),
        await rotate_policy(client, agent, "0", str(uuid.uuid4())),
    ]


@pytest.mark.parametrize("fault", [c[1] for c in UNPROVISIONED], ids=[c[0] for c in UNPROVISIONED])
def test_unprovisioned_runtime_refuses_every_mutation_before_any_history(
    principals: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: Callable[[Provisioned, pytest.MonkeyPatch], None],
) -> None:
    """503 runtime_unavailable with null generation before 404, 409, 422 and any effect.

    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-SOURCE-10.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-10."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, rt):
            assert rt is not None
            fault(rt, monkeypatch)
            before = await state(agent), broker_snapshot(principals)
            opened = connections(principals)
            for response in await _mutations(client, agent):
                assert_refusal(response, 503, "runtime_unavailable")
            assert connections(principals) == opened
            assert (await state(agent), broker_snapshot(principals)) == before

    run(scenario)


# -- source_writer.json grammar --------------------------------------------------------------


def _writer(**changes: Any) -> Callable[[Provisioned, Any], None]:
    """Top-level member changes (``None`` removes), @spec PROTECTED-HOOK-SOURCE-6."""

    def apply(rt: Provisioned, broker: Any) -> None:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        for key, value in changes.items():
            if value is None:
                rt.writer.pop(key)
            else:
                rt.writer[key] = value

    return apply


def _writer_fields(**changes: Any) -> Callable[[Provisioned, Any], None]:
    """Credential member changes (``None`` removes), @spec PROTECTED-HOOK-SOURCE-6."""

    def apply(rt: Provisioned, broker: Any) -> None:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        for key, value in changes.items():
            if value is None:
                rt.writer["source_writer"].pop(key)
            else:
                rt.writer["source_writer"][key] = value

    return apply


def _writer_raw(payload: bytes) -> Callable[[Provisioned, Any], None]:
    """@spec PROTECTED-HOOK-SOURCE-6."""

    def apply(rt: Provisioned, broker: Any) -> None:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        rt.files[WRITER_FILE] = payload

    return apply


def _writer_duplicate(rt: Provisioned, broker: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    body = json.dumps(rt.writer)
    rt.files[WRITER_FILE] = ('{"schema_version":1,' + body[1:]).encode()


def _writer_float_version(rt: Provisioned, broker: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    body = json.dumps({**rt.writer, "schema_version": 9}).replace(
        '"schema_version": 9', '"schema_version": 1.0'
    )
    rt.files[WRITER_FILE] = body.encode()


def _writer_reader_username(rt: Provisioned, broker: Any) -> None:
    """Same username as the control reader, @spec PROTECTED-HOOK-SOURCE-6."""
    rt.writer["source_writer"]["username"] = broker.reader.username
    rt.writer["source_writer"]["password"] = broker.reader.password


def _writer_oversize(rt: Provisioned, broker: Any) -> None:
    """Valid JSON padded past the shared bound, @spec PROTECTED-HOOK-SOURCE-6."""
    rt.files[WRITER_FILE] = json.dumps(rt.writer).encode() + b" " * (65536 + 1024)


def _writer_layout(kind: str) -> Callable[[Provisioned, Any], None]:
    """A FIFO or directory where the writer file belongs, @spec PROTECTED-HOOK-SOURCE-6."""

    def change(directory: Path) -> None:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        path = directory / WRITER_FILE
        path.unlink()
        if kind == "fifo":
            os.mkfifo(path, 0o600)
        else:
            path.mkdir(mode=0o700)

    def apply(rt: Provisioned, broker: Any) -> None:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        rt.after_write.append(change)

    return apply


INVALID_WRITERS: list[tuple[str, Callable[[Provisioned, Any], None]]] = [
    ("extra-member", _writer(extra=True)),
    ("missing-version", _writer(schema_version=None)),
    ("missing-writer", _writer(source_writer=None)),
    ("other-version", _writer(schema_version=2)),
    ("string-version", _writer(schema_version="1")),
    ("float-version", _writer_float_version),
    ("boolean-version", _writer(schema_version=True)),
    ("duplicate-member", _writer_duplicate),
    ("writer-not-object", _writer(source_writer=["user", "secret"])),
    ("writer-extra-member", _writer_fields(role="source_writer")),
    ("missing-username", _writer_fields(username=None)),
    ("missing-password", _writer_fields(password=None)),
    ("empty-username", _writer_fields(username="")),
    ("empty-password", _writer_fields(password="")),
    ("numeric-username", _writer_fields(username=7)),
    ("default-username", _writer_fields(username="default")),
    ("reader-username", _writer_reader_username),
    ("top-level-array", _writer_raw(b"[]")),
    ("not-json", _writer_raw(b"schema_version=1")),
    ("not-utf8", _writer_raw(b'{"schema_version":1,"source_writer":"\xff"}')),
    ("oversize", _writer_oversize),
    ("fifo", _writer_layout("fifo")),
    ("directory", _writer_layout("directory")),
]


@pytest.mark.parametrize(
    "invalid", [c[1] for c in INVALID_WRITERS], ids=[c[0] for c in INVALID_WRITERS]
)
def test_invalid_source_writer_file_makes_mutations_runtime_unavailable(
    principals: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    invalid: Callable[[Provisioned, Any], None],
) -> None:
    """Strict ``{schema_version: 1, source_writer: {username, password}}`` or step 4 refuses.

    The refusal precedes the gate, history and every broker connection.
    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-10.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-6/10."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, rt):
            assert rt is not None
            invalid(rt, principals)
            rt.write()
            before = await state(agent), broker_snapshot(principals)
            opened = connections(principals)
            async with captured_logs() as logs:
                response = await put_policy(client, agent, "0", str(uuid.uuid4()))
            assert_refusal(response, 503, "runtime_unavailable")
            assert connections(principals) == opened
            assert (await state(agent), broker_snapshot(principals)) == before
            assert_no_secret([response.text, *logs.lines], agent, rt, [1])

    run(scenario)


def _writer_unreadable(rt: Provisioned, broker: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""

    def change(directory: Path) -> None:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        (directory / WRITER_FILE).chmod(0)

    rt.after_write.append(change)


def _writer_absent(rt: Provisioned, broker: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    rt.files[WRITER_FILE] = None


WRITER_IGNORED: list[tuple[str, Callable[[Provisioned, Any], None]]] = [
    ("fifo", _writer_layout("fifo")),
    ("unreadable", _writer_unreadable),
    ("missing", _writer_absent),
]


async def second_agent(client: httpx.AsyncClient) -> str:
    """Another agent in the same app, @spec PROTECTED-HOOK-SOURCE-3."""
    response = await client.post(
        "/agents",
        headers=headers(),
        json={
            "name": "acme-admin-" + uuid.uuid4().hex,
            "channel": {
                "kind": "email",
                "address": "admin-" + uuid.uuid4().hex[:12] + "@example.test",
                "endpoint": "http://adapter.example.test",
                "adapter": "mail",
            },
        },
    )
    assert response.status_code == 201, response.text
    return str(response.json()["id"])


@pytest.mark.parametrize(
    "variant", [c[1] for c in WRITER_IGNORED], ids=[c[0] for c in WRITER_IGNORED]
)
def test_get_secret_and_probe_never_open_the_writer_file(
    principals: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    variant: Callable[[Provisioned, Any], None],
) -> None:
    """GET (tombstone activation included), secret and probe answer exactly as with a valid file.

    @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-9.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-6/9."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, rt):
            assert rt is not None
            await asyncio.to_thread(seed_row, agent, 9, PROTECTED_TARGET)
            tombstoned = await second_agent(client)
            op = await asyncio.to_thread(seed_row, tombstoned, 9, ORDINARY_TARGET)
            await asyncio.to_thread(publish_tombstone, principals, tombstoned, 9, op)
            body = BODIES[None]

            async def answers() -> list[tuple[int, Any]]:
                """@spec PROTECTED-HOOK-SOURCE-3/9."""
                probe = await post_support(
                    client,
                    agent,
                    body,
                    support_headers(
                        scoped_secret(agent, 9),
                        requested=None,
                        body=body,
                        delivery="admin-" + secrets.token_hex(4),
                    ),
                )
                read = await get_policy(client, agent)
                tombstone = await get_policy(client, tombstoned)
                secret = await get_secret(client, agent)
                return [
                    (probe.status_code, probe.json()),
                    (read.status_code, {**read.json(), "updated_at": None}),
                    (tombstone.status_code, {**tombstone.json(), "updated_at": None}),
                    (secret.status_code, secret.json()),
                ]

            baseline = await answers()
            assert baseline[1][0] == 200, baseline
            assert baseline[1][1]["refusal_reason"] == "publication_deferred"
            assert baseline[2][0] == 200, baseline
            assert baseline[2][1]["activation"] == "active", baseline
            assert baseline[3] == (
                503,
                {"detail": {"code": "source_publication_deferred", "committed_generation": None}},
            )
            variant(rt, principals)
            rt.write()
            assert await answers() == baseline

    run(scenario)


# -- authentication and request shape --------------------------------------------------------


def _requests(agent: str) -> list[tuple[str, str, dict[str, Any]]]:
    """(method, path, request options) for the five routes, @spec PROTECTED-HOOK-SOURCE-3."""
    mutation = {"expected_generation": "0", "operation_id": str(uuid.uuid4())}
    return [
        ("GET", route(agent), {}),
        ("PUT", route(agent), {"json": put_body("0", str(uuid.uuid4()))}),
        ("DELETE", route(agent), {"params": mutation}),
        ("POST", route(agent, "/rotate"), {"json": mutation}),
        ("GET", route(agent, "/secret"), {}),
    ]


def _ingress_headers(agent: str, kind: str) -> dict[str, str]:
    """Credentials of other routes that must authenticate nothing here.

    @spec PROTECTED-HOOK-SOURCE-3.
    """
    if kind == "none":
        return {}
    if kind == "wrong_key":
        return {"X-API-Key": "not-the-platform-key"}
    if kind == "legacy_hook_key":
        return {"X-API-Key": legacy_secret(agent)}
    if kind == "scoped_source_key":
        return {"X-API-Key": scoped_secret(agent, 9)}
    stamp = "1700000000"
    signed = hook_signing.sign(
        legacy_secret(agent),
        timestamp=stamp,
        delivery_id="admin-delivery",
        hook=HOOK,
        tool_access=None,
        body=b"{}",
    )
    return {
        hook_signing.TIMESTAMP_HEADER: stamp,
        hook_signing.DELIVERY_HEADER: "admin-delivery",
        hook_signing.SIGNATURE_HEADER: signed,
    }


@pytest.mark.parametrize(
    "kind", ["none", "wrong_key", "legacy_hook_key", "scoped_source_key", "hook_signature"]
)
def test_ingress_credentials_cannot_call_the_administrative_routes(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    """Uniform 401 on every route, before request shape 422 and any effect.

    @spec PROTECTED-HOOK-SOURCE-3.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, _rt):
            await asyncio.to_thread(seed_row, agent, 9, PROTECTED_TARGET)
            before = await state(agent), broker_snapshot(principals)
            opened = connections(principals)
            for method, path, options in _requests(agent):
                response = await client.request(
                    method, path, headers=_ingress_headers(agent, kind), **options
                )
                assert_uniform_401(response)
                assert_no_secret([response.text], agent, None, [9])
            shape = await client.put(
                route("not-a-uuid", hook="Bad_Hook"),
                headers=_ingress_headers(agent, kind),
                json={"unexpected": True},
            )
            assert_uniform_401(shape)
            assert connections(principals) == opened
            assert (await state(agent), broker_snapshot(principals)) == before

    run(scenario)


def _shape_cases(agent: str) -> list[tuple[str, str, dict[str, Any]]]:
    """Request shape violations answered by FastAPI's validation list.

    @spec PROTECTED-HOOK-SOURCE-3.
    """
    op = str(uuid.uuid4())
    return [
        ("GET", route("not-a-uuid"), {}),
        ("GET", route(agent, hook="Bad_Hook"), {}),
        ("GET", route(agent, "/secret", hook="Bad_Hook"), {}),
        ("PUT", route(agent), {"json": {**put_body("0", op), "unexpected": True}}),
        ("PUT", route(agent), {"json": put_body("00", op)}),
        ("PUT", route(agent), {"json": put_body("0", op.upper())}),
        ("PUT", route(agent), {"json": put_body("0", op, bundle_digest="F" * 64)}),
        ("PUT", route(agent), {"json": {**put_body("0", op), "tool_access": "read-write"}}),
        ("PUT", route(agent), {"json": put_body(0, op)}),
        ("DELETE", route(agent), {"params": {"expected_generation": "0"}}),
        ("DELETE", route(agent), {"params": {"expected_generation": "-1", "operation_id": op}}),
        (
            "DELETE",
            route(agent),
            {"params": {"expected_generation": "0", "operation_id": op, "extra": "1"}},
        ),
        ("POST", route(agent, "/rotate"), {"json": {"expected_generation": "0"}}),
        (
            "POST",
            route(agent, "/rotate"),
            {"json": {"expected_generation": "0", "operation_id": op, "mode": "protected"}},
        ),
    ]


def test_request_shape_violations_are_fastapi_422_before_any_source_read(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-10."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, _rt):
            before = await state(agent), broker_snapshot(principals)
            opened = connections(principals)
            for method, path, options in _shape_cases(agent):
                response = await client.request(method, path, headers=headers(), **options)
                assert_validation_list(response)
            assert connections(principals) == opened
            assert (await state(agent), broker_snapshot(principals)) == before

    run(scenario)


async def _console_cookie(client: httpx.AsyncClient) -> str:
    """A live console session minted through the console routes, @spec PROTECTED-HOOK-SOURCE-3."""
    minted = await client.post(
        "/console/login-codes", headers=headers(), json={"subject": "U0EXAMPLE1"}
    )
    assert minted.status_code == 201, minted.text
    exchanged = await client.post("/console/session", json={"code": minted.json()["code"]})
    assert exchanged.status_code == 200, exchanged.text
    for value in exchanged.headers.get_list("set-cookie"):
        name, _, rest = value.partition("=")
        if name.strip() == CONSOLE_SESSION_COOKIE:
            return rest.split(";", 1)[0]
    raise AssertionError("console session cookie was not issued")


def test_console_session_authenticates_and_cross_origin_mutation_is_refused(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A live console session reads; a cookie mutation from another origin is 403 with no effect.

    @spec PROTECTED-HOOK-SOURCE-3.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, _rt):
            token = await _console_cookie(client)
            client.cookies.clear()
            cookie = {"Cookie": f"{CONSOLE_SESSION_COOKIE}={token}"}
            read = await client.get(route(agent), headers=cookie)
            assert_dto(read, dto(agent, generation="0", legacy="0"), absent=True)
            before = await state(agent), broker_snapshot(principals)
            refused = await client.put(
                route(agent),
                headers={**cookie, "Origin": "https://evil.example"},
                json=put_body("0", str(uuid.uuid4())),
            )
            assert refused.status_code == 403, refused.text
            assert (await state(agent), broker_snapshot(principals)) == before
            accepted = await client.put(
                route(agent),
                headers={**cookie, "Origin": "http://test"},
                json=put_body("0", str(uuid.uuid4())),
            )
            assert_refusal(accepted, 503, "source_publication_deferred", "1")

    run(scenario)


# -- the whole route lifecycle ---------------------------------------------------------------


def test_route_lifecycle_keeps_exact_durable_state(
    principals: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pending history, DELETE, PUT, rotate, DELETE: generations, counter and records.

    @spec PROTECTED-HOOK-SOURCE-3 @spec PROTECTED-HOOK-SOURCE-5 @spec PROTECTED-HOOK-SOURCE-6
    @spec PROTECTED-HOOK-SOURCE-7 @spec PROTECTED-HOOK-SOURCE-10.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-3/5/6/7/10."""
        async with admin_app(tmp_path, monkeypatch, principals) as (_app, client, agent, rt):
            await asyncio.to_thread(seed_attempt, agent, 4, PROTECTED_TARGET, "pending")
            ops = [str(uuid.uuid4()) for _ in range(4)]
            responses = []
            async with captured_logs() as logs:
                responses.append(await delete_policy(client, agent, "0", ops[0]))
                assert_dto(
                    responses[-1], dto(agent, generation="5", legacy="0", activation="active")
                )
                responses.append(await put_policy(client, agent, "5", ops[1]))
                assert_refusal(responses[-1], 503, "source_publication_deferred", "6")
                assert (source_record(principals, agent) or {}).get("active") is None
                responses.append(await rotate_policy(client, agent, "6", ops[2]))
                assert_refusal(responses[-1], 503, "source_publication_deferred", "7")
                responses.append(await delete_policy(client, agent, "7", ops[3]))
                assert_dto(
                    responses[-1], dto(agent, generation="8", legacy="1", activation="active")
                )
                responses.append(await get_policy(client, agent))
                assert_dto(
                    responses[-1], dto(agent, generation="8", legacy="1", activation="active")
                )
                responses.append(await get_secret(client, agent))
                assert_refusal(responses[-1], 409, "source_not_protected")
            assert await asyncio.to_thread(counter, agent) == 1
            assert [entry[1:3] for entry in await asyncio.to_thread(ledger, agent)] == [
                (4, "pending"),
                (5, "committed"),
                (6, "committed"),
                (7, "committed"),
                (8, "committed"),
            ]
            assert_no_secret(
                [response.text for response in responses] + logs.lines, agent, rt, [5, 6, 7, 8]
            )

    run(scenario)
