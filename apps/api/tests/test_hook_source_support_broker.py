"""Support probe broker evaluation of a protected row, @spec PROTECTED-HOOK-SOURCE-9.

Every case runs the real API (migrated Postgres, the app's Valkey) against a
separate disposable TLS Valkey whose default user is disabled. The broker is
seeded the way the out of band provisioner, verifier and source writer would
(exact closed record encodings under the admission control keys), and the API
reads it only through the provisioner-supplied bootstrap directory named by
``CURIE_PROTECTED_RUNTIME_DIR`` (LANE-2/3 authenticated metadata reader).

The disposable broker fixture is imported from the protected hooks package's
own test helper (``packages/protected-hooks/tests/admission_broker.py``), which
owns the container by exact recorded ID and removes it at module teardown. That
directory is not a package on the API test path, so it is loaded by file path.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import importlib.util
import json
import os
import secrets
import socket
import ssl
import stat
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from _migration_support import sql_dicts
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from curie_api.config import get_settings
from curie_protected_hooks.broker_metadata import metadata_acl_rules
from curie_protected_hooks.broker_transport import AuthenticatedMetadataReader
from curie_protected_hooks.source_policy_records import policy_fingerprint
from curie_protected_hooks.source_policy_sql import SourceGate
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool
from test_hook_source_support import (  # noqa: F401  (support_db is a fixture)
    BODIES,
    GENERATION,
    HOOK,
    _intent,
    effects,
    expected,
    legacy_secret,
    post_support,
    probe_app,
    scoped_secret,
    seed,
    support_db,
    support_headers,
)


def _load_admission_broker() -> Any:
    """Shared disposable TLS broker helper, @spec PROTECTED-HOOK-LANE-2/3."""
    path = (
        Path(__file__).resolve().parents[3]
        / "packages"
        / "protected-hooks"
        / "tests"
        / "admission_broker.py"
    )
    name = "_api_protected_hooks_admission_broker"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None, "admission broker helper absent"
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_broker = _load_admission_broker()
pytestmark = pytest.mark.usefixtures("support_db")
admission_service = _broker.admission_service
FixtureSecret = _broker.FixtureSecret
canonical = _broker.canonical
broker_snapshot = _broker.snapshot

ENV = "CURIE_PROTECTED_RUNTIME_DIR"
RUNTIME = "33333333-3333-4333-8333-333333333333"
QUALIFICATION = "44444444-4444-4444-8444-444444444444"
BUNDLE = "f" * 64
OTHER = "66666666-6666-4666-8666-666666666666"
MAX_READINESS_MS = 60000


# -- independent fixture material ---------------------------------------------------


def standalone_ca() -> tuple[Any, Any]:
    """A CA (key, certificate) no broker certificate chains to, @spec PROTECTED-HOOK-LANE-2/3."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.now(UTC)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "fixture-untrusted-ca")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), False)
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(key.public_key()), False)
        .add_extension(
            x509.KeyUsage(True, False, False, False, False, True, True, False, False), True
        )
        .sign(key, hashes.SHA256())
    )
    return key, cert


def standalone_ca_pem() -> str:
    """PEM of a CA no broker certificate chains to, @spec PROTECTED-HOOK-LANE-2/3."""
    return standalone_ca()[1].public_bytes(serialization.Encoding.PEM).decode()


def manifest_value(*, port: int, pin: str, run_id: str) -> dict[str, Any]:
    """Closed v1 manifest for one loopback broker, @spec PROTECTED-HOOK-LANE-2."""
    return {
        "schema_version": 1,
        "runtime_id": RUNTIME,
        "runtime_generation": "1",
        "broker_identity": {
            "instance_id": "11111111-1111-4111-8111-111111111111",
            "endpoint": {"host": "127.0.0.1", "port": port},
            "tls_server_name": "127.0.0.1",
            "tls_spki_sha256": pin,
            "run_id": run_id,
            "database": 0,
        },
        "worker_image_digest": "sha256:" + "d" * 64,
        "runner_image_digest": "sha256:" + "e" * 64,
        "bundle_digest": {"sha256": BUNDLE, "object_identity": "bundle/example"},
        "execution_config_digest": "1" * 64,
        "qualification_id": QUALIFICATION,
        "substrate": {
            "kind": "docker",
            "authority_domain_id": "55555555-5555-4555-8555-555555555555",
            "launch_identity": "launch/example",
            "launch_config_sha256": "2" * 64,
        },
        "guard_identity": {
            "control_id": "22222222-2222-4222-8222-222222222222",
            "revision": "1",
            "config_sha256": "c" * 64,
        },
        "credential_refs": {
            role: {"id": "credential/example-" + role, "generation": "1"}
            for role in ("enqueue", "worker", "verifier")
        },
    }


def seed_protected_row(agent: str, operation: str, row: dict[str, Any]) -> None:
    """Committed protected policy row with a known operation, @spec PROTECTED-HOOK-SOURCE-2/10."""
    target = dict(mode="protected", tool_access="read-only", **row)
    sql_dicts(
        "INSERT INTO curie.hook_source_operations "
        "(agent_id,hook,operation_id,intent_sha256,status,generation) "
        "VALUES (:agent,:hook,:operation,:intent,'committed',:generation)",
        dict(
            agent=agent,
            hook=HOOK,
            operation=operation,
            intent=_intent(target),
            generation=GENERATION,
        ),
    )
    sql_dicts(
        "INSERT INTO curie.hook_source_policies "
        "(agent_id,hook,operation_id,generation,mode,tool_access,runtime_id,"
        "qualification_id,bundle_digest,legacy_generation) "
        "VALUES (:agent,:hook,:operation,:generation,:mode,:tool_access,:runtime_id,"
        ":qualification_id,:bundle_digest,0)",
        dict(agent=agent, hook=HOOK, operation=operation, generation=GENERATION, **target),
    )


class ReaderPrincipal:
    """Disposable control reader principal, @spec PROTECTED-HOOK-LANE-3."""

    def __init__(self, username: str, password: str) -> None:
        """@spec PROTECTED-HOOK-LANE-3."""
        self.username = username
        self.password = password

    def __repr__(self) -> str:
        """@spec PROTECTED-HOOK-LANE-3."""
        return "<fixture-reader>"


@pytest.fixture
def runtime_broker(admission_service: Any) -> Iterator[Any]:
    """Fresh broker keys and an owned control reader principal, @spec PROTECTED-HOOK-LANE-3.

    Only the provisioner installs the closed ``control_reader`` recipe; the
    principal is deleted (and its sessions killed) after the test.
    """
    broker = admission_service
    broker.command("FLUSHDB")
    reader = ReaderPrincipal("reader-" + secrets.token_hex(6), FixtureSecret(secrets.token_hex(24)))
    broker.command(
        "ACL",
        "SETUSER",
        reader.username,
        "on",
        ">" + reader.password,
        *metadata_acl_rules("control_reader"),
    )
    broker.reader = reader
    try:
        yield broker
    finally:
        broker.command("CLIENT", "KILL", "USER", reader.username, "SKIPME", "yes")
        broker.command("ACL", "DELUSER", reader.username)


class Runtime:
    """One provisioned tuple: bootstrap files plus broker control/source records.

    Defaults form a fully valid selected tuple for the committed row; a case
    mutates exactly one fact. @spec PROTECTED-HOOK-SOURCE-9.
    """

    def __init__(
        self,
        *,
        agent: str,
        directory: Path,
        port: int,
        pin: str,
        run_id: str,
        ca_pem: str,
        username: str,
        password: str,
        now_ms: int,
    ) -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        self.agent = agent
        self.directory = directory
        self.operation = str(uuid.uuid4())
        self.row: dict[str, Any] = dict(
            runtime_id=RUNTIME, qualification_id=QUALIFICATION, bundle_digest=BUNDLE
        )
        self.now_ms = now_ms
        self.manifest = manifest_value(port=port, pin=pin, run_id=run_id)
        self.ca_pem = ca_pem
        self.bootstrap: dict[str, Any] = {
            "schema_version": 1,
            "max_readiness_ms": str(MAX_READINESS_MS),
            "control_reader": {"username": username, "password": password},
        }
        # name -> exact bytes (None = absent) overriding the derived value.
        self.raw: dict[str, bytes | None] = {}
        self.files: dict[str, bytes | None] = {}
        self.absent = False
        # Provisioner layout changes applied after the files are written.
        self.after_write: list[Callable[[Path], None]] = []
        self.rebuild()

    def __repr__(self) -> str:
        """@spec PROTECTED-HOOK-LANE-3."""
        return "<fixture-runtime>"

    @property
    def manifest_digest(self) -> str:
        """@spec PROTECTED-HOOK-LANE-2."""
        return hashlib.sha256(canonical(self.manifest)).hexdigest()

    def rebuild(self) -> None:
        """Derive qualification, readiness and selection from the manifest.

        @spec PROTECTED-HOOK-LANE-2 @spec PROTECTED-HOOK-SOURCE-9.
        """
        m, md = self.manifest, self.manifest_digest
        self.qualification = {
            key: copy.deepcopy(m[key])
            for key in (
                "schema_version",
                "runtime_id",
                "runtime_generation",
                "qualification_id",
                "broker_identity",
                "execution_config_digest",
                "guard_identity",
            )
        }
        self.qualification.update(
            qualification_generation="1",
            manifest_digest=md,
            measurement_record_id="measurement/qualification",
        )
        issued = self.now_ms - 1000
        self.readiness = {
            key: copy.deepcopy(self.qualification[key])
            for key in (
                "schema_version",
                "runtime_id",
                "runtime_generation",
                "qualification_id",
                "qualification_generation",
                "broker_identity",
                "guard_identity",
                "manifest_digest",
            )
        }
        self.readiness.update(
            verifier_identity=copy.deepcopy(m["credential_refs"]["verifier"]),
            issued_at_ms=str(issued),
            expires_at_ms=str(issued + MAX_READINESS_MS),
            measurement_record_id="measurement/readiness",
        )
        self.selection = dict(
            schema_version=1,
            runtime_id=m["runtime_id"],
            runtime_generation=m["runtime_generation"],
            manifest_digest=md,
            qualification_id=m["qualification_id"],
            qualification_generation="1",
            broker_run_id=m["broker_identity"]["run_id"],
            admission_open=True,
        )

    def source_record(
        self,
        *,
        floor: str | None = None,
        operation: str | None = None,
        active: dict[str, Any] | None | bool = True,
    ) -> dict[str, Any]:
        """Source record matching the row unless overridden, @spec PROTECTED-HOOK-SOURCE-6."""
        op = operation or self.operation
        generation = floor or str(GENERATION)
        fingerprint = policy_fingerprint(
            dict(
                agent_id=self.agent,
                hook=HOOK,
                generation=str(GENERATION),
                operation_id=self.operation,
                mode="protected",
                tool_access="read-only",
                legacy_generation="0",
                **self.row,
            )
        )
        record: dict[str, Any] = {"floor": generation, "operation_id": op, "active": None}
        if active is True:
            record["active"] = dict(
                generation=generation,
                operation_id=op,
                mode="protected",
                policy_fingerprint=fingerprint,
            )
        elif isinstance(active, dict):
            record["active"] = dict(
                generation=generation,
                operation_id=op,
                mode="protected",
                policy_fingerprint=fingerprint,
            )
            record["active"].update(active)
        return record

    def keys(self) -> dict[str, bytes]:
        """Exact broker key/value map the provisioner would write, @spec PROTECTED-HOOK-SOURCE-9."""
        runtime, s = self.manifest["runtime_id"], self.selection
        pieces: dict[str, tuple[str, Any]] = {
            "source": (f"protected:source:{self.agent}:{HOOK}", self.source_record()),
            "selection": (f"protected:control:selection:{runtime}", s),
            "manifest": (f"protected:control:manifest:{s['manifest_digest']}", self.manifest),
            "qualification": (
                "protected:control:qualification:"
                f"{s['qualification_id']}:{s['qualification_generation']}",
                self.qualification,
            ),
            "readiness": (
                f"protected:control:readiness:{runtime}:{s['runtime_generation']}",
                self.readiness,
            ),
        }
        result: dict[str, bytes] = {}
        for name, (key, value) in pieces.items():
            raw = self.raw[name] if name in self.raw else canonical(value)
            if raw is not None:
                result[key] = raw
        return result

    def write_bootstrap(self) -> None:
        """Provisioner-owned bootstrap directory, @spec PROTECTED-HOOK-SOURCE-9."""
        if self.absent:
            return
        self.directory.mkdir(mode=0o700, exist_ok=True)
        contents: dict[str, bytes | None] = {
            "manifest.json": canonical(self.manifest),
            "ca.pem": self.ca_pem.encode("ascii"),
            "bootstrap.json": json.dumps(self.bootstrap).encode(),
        }
        contents.update(self.files)
        for name, payload in contents.items():
            path = self.directory / name
            if path.exists():
                path.chmod(0o600)
                path.unlink()
            if payload is not None:
                path.write_bytes(payload)
                path.chmod(0o600)
        for hook in self.after_write:
            hook(self.directory)

    def forbidden(self) -> list[str]:
        """Strings no response may contain, @spec PROTECTED-HOOK-SOURCE-9."""
        identity = self.manifest["broker_identity"]
        host, port = identity["endpoint"]["host"], identity["endpoint"]["port"]
        ca_lines = [line for line in self.ca_pem.splitlines() if "CERTIFICATE" not in line]
        # A case may delete or corrupt a credential field; every present,
        # nonempty string credential is still forbidden.
        reader = self.bootstrap.get("control_reader")
        credentials = [
            value
            for value in (
                (reader.get("username"), reader.get("password")) if isinstance(reader, dict) else ()
            )
            if isinstance(value, str) and value
        ]
        return [
            *credentials,
            self.ca_pem,
            ca_lines[0],
            host,
            f"{host}:{port}",
            f":{port}",
            identity["tls_spki_sha256"],
            str(self.directory),
            self.row["bundle_digest"],
        ]


def broker_now_ms(broker: Any) -> int:
    """Broker-authoritative time, @spec PROTECTED-HOOK-LANE-2/3."""
    now = broker.command("TIME")
    return int(now[0]) * 1000 + int(now[1]) // 1000


def runtime_for(broker: Any, agent: str, directory: Path) -> Runtime:
    """A fully valid tuple on the disposable broker, @spec PROTECTED-HOOK-SOURCE-9."""
    return Runtime(
        agent=agent,
        directory=directory,
        port=broker.port,
        pin=broker.pin,
        run_id=broker.command("INFO", "server")["run_id"],
        ca_pem=str(broker.ca_pem),
        username=broker.reader.username,
        password=broker.reader.password,
        now_ms=broker_now_ms(broker),
    )


def runtime_without_broker(
    agent: str, directory: Path, port: int, *, pin: str = "0" * 64, ca_pem: str | None = None
) -> Runtime:
    """A valid bootstrap pointing at an owned non-broker endpoint, @spec PROTECTED-HOOK-SOURCE-9."""
    return Runtime(
        agent=agent,
        directory=directory,
        port=port,
        pin=pin,
        run_id="a" * 40,
        ca_pem=ca_pem or standalone_ca_pem(),
        username="reader-" + secrets.token_hex(6),
        password=FixtureSecret(secrets.token_hex(24)),
        now_ms=int(time.time() * 1000),
    )


def use_runtime_dir(monkeypatch: pytest.MonkeyPatch, directory: Path) -> None:
    """Name the bootstrap directory to the API, @spec PROTECTED-HOOK-SOURCE-9."""
    monkeypatch.setenv(ENV, str(directory))
    get_settings.cache_clear()


def runtime_members(rt: Runtime) -> dict[str, Any]:
    """Validated selected-tuple members, @spec PROTECTED-HOOK-SOURCE-9."""
    return {
        "runtime_id": rt.manifest["runtime_id"],
        "runtime_generation": rt.manifest["runtime_generation"],
        "qualification_id": rt.manifest["qualification_id"],
    }


def reader_sessions(broker: Any) -> list[Any]:
    """Live sessions of the control reader principal, @spec PROTECTED-HOOK-LANE-2/3."""
    return [
        client
        for client in broker.admin.client_list()
        if client.get("user") == broker.reader.username
    ]


def hold_readers(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Keep a strong reference to every reader the probe opens.

    A pass-through wrapper around ``AuthenticatedMetadataReader.connect`` (it
    still performs the real connect). Holding the reader means garbage
    collection cannot close its socket, so a session that disappears was
    closed by the probe itself. @spec PROTECTED-HOOK-SOURCE-9.
    """
    held: list[Any] = []
    original = AuthenticatedMetadataReader.connect

    def connect(cls: Any, *args: Any, **kwargs: Any) -> Any:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        reader = original(*args, **kwargs)
        held.append(reader)
        return reader

    monkeypatch.setattr(AuthenticatedMetadataReader, "connect", classmethod(connect))
    return held


def release_readers(held: list[Any]) -> None:
    """Test cleanup of held readers (close is idempotent), @spec PROTECTED-HOOK-SOURCE-9."""
    for reader in held:
        try:
            reader.close()
        except Exception:  # noqa: BLE001  Cleanup of held readers must not mask the test result.
            pass
    held.clear()


async def wait_reader_closed(broker: Any) -> None:
    """The probe closes its reader, @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-LANE-2/3."""
    deadline = time.monotonic() + 2
    while reader_sessions(broker):
        if time.monotonic() > deadline:
            pytest.fail("probe left a control reader session open", pytrace=False)
        await asyncio.sleep(0.02)


async def probe_with(
    client: Any, agent: str, requested: str | None = None, delivery: str = "support-delivery"
) -> Any:
    """Signed support probe with the current scoped key, @spec PROTECTED-HOOK-SOURCE-9."""
    body = BODIES[requested]
    return await post_support(
        client,
        agent,
        body,
        support_headers(scoped_secret(agent), requested=requested, body=body, delivery=delivery),
    )


def assert_safe(response: Any, rt: Runtime, agent: str) -> None:
    """Never echo bootstrap content, endpoint or path, @spec PROTECTED-HOOK-SOURCE-9."""
    for leaked in (
        *rt.forbidden(),
        scoped_secret(agent),
        legacy_secret(agent),
        get_settings().api_key,
    ):
        assert leaked not in response.text, "response echoes provisioning material"


def run_case(
    broker: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutate: Callable[[Runtime, Any], None],
    reason: str,
    members: bool,
    requested: str | None = None,
) -> None:
    """Seed, probe once, assert reason, members, no writes, no echo, closed reader.

    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-LANE-2/3.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        directory = tmp_path / ("runtime-" + secrets.token_hex(4))
        use_runtime_dir(monkeypatch, directory)
        held = hold_readers(monkeypatch)
        try:
            async with probe_app() as (app, client, agent):
                rt = runtime_for(broker, agent, directory)
                mutate(rt, broker)
                await asyncio.to_thread(seed_protected_row, agent, rt.operation, rt.row)
                for key, value in rt.keys().items():
                    broker.command("SET", key, value)
                rt.write_bootstrap()
                before = await effects(app, agent), broker_snapshot(broker)
                response = await probe_with(client, agent, requested)
                assert response.status_code == 503, response.text
                want = expected(requested, "read-only", str(GENERATION), reason)
                if members:
                    want.update(runtime_members(rt))
                assert response.json() == want
                assert_safe(response, rt, agent)
                # Readers are still referenced here: only an explicit close ends the session.
                await wait_reader_closed(broker)
                assert (await effects(app, agent), broker_snapshot(broker)) == before
        finally:
            release_readers(held)

    asyncio.run(asyncio.wait_for(scenario(), 30))


# -- case mutations --------------------------------------------------------------------


def _valid(rt: Runtime, broker: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9."""


def _set(**changes: Any) -> Callable[[Runtime, Any], None]:
    """Apply attribute path changes, @spec PROTECTED-HOOK-SOURCE-9."""

    def apply(rt: Runtime, broker: Any) -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        for path, value in changes.items():
            target_name, _, field = path.partition("__")
            target = getattr(rt, target_name)
            if field:
                target[field] = value
            else:
                setattr(rt, target_name, value)

    return apply


def _manifest_identity(field: str, value: Any) -> Callable[[Runtime, Any], None]:
    """Consistent tuple bound to another identity, @spec PROTECTED-HOOK-LANE-2."""

    def apply(rt: Runtime, broker: Any) -> None:
        """@spec PROTECTED-HOOK-LANE-2."""
        rt.manifest["broker_identity"][field] = value
        rt.rebuild()

    return apply


def _raw(name: str, value: bytes | None) -> Callable[[Runtime, Any], None]:
    """Exact broker bytes for one record, @spec PROTECTED-HOOK-SOURCE-9."""

    def apply(rt: Runtime, broker: Any) -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        rt.raw[name] = value

    return apply


def _source(**kwargs: Any) -> Callable[[Runtime, Any], None]:
    """A source record differing from the committed row, @spec PROTECTED-HOOK-SOURCE-6."""

    def apply(rt: Runtime, broker: Any) -> None:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        rt.raw["source"] = canonical(rt.source_record(**kwargs))

    return apply


def _acl(*rules: str) -> Callable[[Runtime, Any], None]:
    """Provisioner misconfigures the reader principal, @spec PROTECTED-HOOK-LANE-3."""

    def apply(rt: Runtime, broker: Any) -> None:
        """@spec PROTECTED-HOOK-LANE-3."""
        broker.command("ACL", "SETUSER", broker.reader.username, *rules)

    return apply


def _untrusted_ca(rt: Runtime, broker: Any) -> None:
    """@spec PROTECTED-HOOK-LANE-2/3."""
    rt.ca_pem = standalone_ca_pem()


def _wrong_password(rt: Runtime, broker: Any) -> None:
    """@spec PROTECTED-HOOK-LANE-3."""
    rt.bootstrap["control_reader"]["password"] = FixtureSecret(secrets.token_hex(24))


def _selection(**changes: Any) -> Callable[[Runtime, Any], None]:
    """@spec PROTECTED-HOOK-SOURCE-9."""

    def apply(rt: Runtime, broker: Any) -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        rt.selection.update(changes)

    return apply


def _selection_extra(rt: Runtime, broker: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    rt.raw["selection"] = canonical({**rt.selection, "extra": True})


def _selection_open_string(rt: Runtime, broker: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    rt.raw["selection"] = canonical({**rt.selection, "admission_open": "true"})


def _manifest_control_differs(rt: Runtime, broker: Any) -> None:
    """A different valid manifest under the bootstrap digest key, @spec PROTECTED-HOOK-SOURCE-9."""
    other = copy.deepcopy(rt.manifest)
    other["worker_image_digest"] = "sha256:" + "9" * 64
    rt.raw["manifest"] = canonical(other)


def _expired(rt: Runtime, broker: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    rt.readiness.update(
        issued_at_ms=str(rt.now_ms - 2 * MAX_READINESS_MS),
        expires_at_ms=str(rt.now_ms - MAX_READINESS_MS),
    )


def _future_issued(rt: Runtime, broker: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-LANE-2."""
    rt.readiness.update(
        issued_at_ms=str(rt.now_ms + MAX_READINESS_MS),
        expires_at_ms=str(rt.now_ms + 2 * MAX_READINESS_MS),
    )


def _short_max(rt: Runtime, broker: Any) -> None:
    """Readiness window longer than the bootstrap bound, @spec PROTECTED-HOOK-LANE-2."""
    rt.bootstrap["max_readiness_ms"] = "1000"


def _qualification_digest(rt: Runtime, broker: Any) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    rt.qualification["manifest_digest"] = "0" * 64


def _verifier_differs(rt: Runtime, broker: Any) -> None:
    """@spec PROTECTED-HOOK-LANE-2."""
    rt.readiness["verifier_identity"] = {"id": "credential/example-verifier", "generation": "2"}


def _closed(rt: Runtime, broker: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    rt.selection["admission_open"] = False


def _row(**changes: Any) -> Callable[[Runtime, Any], None]:
    """Row references differ from the selected tuple (admission also closed).

    Closing admission proves step 10 precedes step 11. @spec PROTECTED-HOOK-SOURCE-9.
    """

    def apply(rt: Runtime, broker: Any) -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        rt.row.update(changes)
        rt.selection["admission_open"] = False

    return apply


def _malformed(name: str) -> Callable[[Runtime, Any], None]:
    """A present record that is valid JSON but not its closed shape.

    A malformed control record counts as absent at its own step.
    @spec PROTECTED-HOOK-SOURCE-9.
    """

    def apply(rt: Runtime, broker: Any) -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        rt.raw[name] = canonical({**getattr(rt, name), "extra": True})

    return apply


NEW_OPERATION = "77777777-7777-4777-8777-777777777777"

# (id, mutation, reason, runtime members present)
STEPS: list[tuple[str, Callable[[Runtime, Any], None], str, bool]] = [
    # 1. bootstrap runtime differs from the row: one runtime per deployment.
    ("1-row-runtime-differs", _set(row__runtime_id=OTHER), "configuration_unsupported", False),
    # 2. connect/authenticate/run_id confirmation, or any later read, fails.
    ("2-wrong-password", _wrong_password, "broker_unavailable", False),
    ("2-reader-disabled", _acl("off"), "broker_unavailable", False),
    ("2-untrusted-ca", _untrusted_ca, "broker_unavailable", False),
    (
        "2-pin-mismatch",
        _manifest_identity("tls_spki_sha256", "0" * 64),
        "broker_unavailable",
        False,
    ),
    ("2-run-id-not-live", _manifest_identity("run_id", "0" * 40), "broker_unavailable", False),
    (
        "2-control-read-refused",
        _acl("resetkeys", "%R~protected:source:*"),
        "broker_unavailable",
        False,
    ),
    ("2-time-refused", _acl("-time"), "broker_unavailable", False),
    ("2-source-record-malformed", _raw("source", b"{not json"), "broker_unavailable", False),
    # 3. no active source record, or it differs from the committed row.
    ("3-source-absent", _raw("source", None), "source_closed", False),
    (
        "3-source-reserved-only",
        _source(floor="10", operation=NEW_OPERATION, active=None),
        "source_closed",
        False,
    ),
    ("3-source-generation-differs", _source(floor="10"), "source_closed", False),
    ("3-source-operation-differs", _source(operation=NEW_OPERATION), "source_closed", False),
    ("3-source-mode-ordinary", _source(active={"mode": "ordinary"}), "source_closed", False),
    (
        "3-source-fingerprint-differs",
        _source(active={"policy_fingerprint": "0" * 64}),
        "source_closed",
        False,
    ),
    # 4. selection absent/malformed/other manifest, or manifest control differs.
    ("4-selection-absent", _raw("selection", None), "runtime_unavailable", False),
    ("4-selection-not-json", _raw("selection", b"{not json"), "runtime_unavailable", False),
    ("4-selection-extra-field", _selection_extra, "runtime_unavailable", False),
    ("4-selection-open-not-bool", _selection_open_string, "runtime_unavailable", False),
    (
        "4-selection-other-manifest",
        _selection(manifest_digest="0" * 64),
        "runtime_unavailable",
        False,
    ),
    ("4-manifest-control-absent", _raw("manifest", None), "runtime_unavailable", False),
    ("4-manifest-control-differs", _manifest_control_differs, "runtime_unavailable", False),
    # Malformed counts as absent at its own step (selection/manifest: 4).
    ("4-selection-malformed", _malformed("selection"), "runtime_unavailable", False),
    ("4-manifest-control-not-json", _raw("manifest", b"{not json"), "runtime_unavailable", False),
    ("4-manifest-control-malformed", _malformed("manifest"), "runtime_unavailable", False),
    # 5. selection names another broker epoch.
    (
        "5-selection-run-id-differs",
        _selection(broker_run_id="0" * 40),
        "broker_identity_mismatch",
        False,
    ),
    # 6. selected qualification absent.
    ("6-qualification-absent", _raw("qualification", None), "qualification_unavailable", False),
    (
        "6-qualification-not-json",
        _raw("qualification", b"{not json"),
        "qualification_unavailable",
        False,
    ),
    ("6-qualification-malformed", _malformed("qualification"), "qualification_unavailable", False),
    # 7. selected readiness absent.
    ("7-readiness-absent", _raw("readiness", None), "evidence_missing", False),
    ("7-readiness-not-json", _raw("readiness", b"{not json"), "evidence_missing", False),
    ("7-readiness-malformed", _malformed("readiness"), "evidence_missing", False),
    # 8. broker time at or after expiry (also invalid under step 9: 8 wins).
    ("8-readiness-expired", _expired, "evidence_expired", False),
    # 9. validate_authority refuses, or selection generations differ.
    ("9-window-exceeds-bootstrap-max", _short_max, "qualification_unavailable", False),
    ("9-readiness-issued-in-future", _future_issued, "qualification_unavailable", False),
    ("9-qualification-other-manifest", _qualification_digest, "qualification_unavailable", False),
    ("9-readiness-other-verifier", _verifier_differs, "qualification_unavailable", False),
    (
        "9-selection-qualification-generation",
        _selection(qualification_generation="2"),
        "qualification_unavailable",
        False,
    ),
    (
        "9-selection-runtime-id-differs",
        _selection(runtime_id=OTHER),
        "qualification_unavailable",
        False,
    ),
    (
        "9-selection-qualification-id-differs",
        _selection(qualification_id=OTHER),
        "qualification_unavailable",
        False,
    ),
    (
        "9-selection-runtime-generation",
        _selection(runtime_generation="2"),
        "qualification_unavailable",
        False,
    ),
    # 10. row references differ from the selected tuple (before step 11).
    (
        "10-row-qualification-differs",
        _row(qualification_id=OTHER),
        "configuration_unsupported",
        True,
    ),
    ("10-row-bundle-differs", _row(bundle_digest="a" * 64), "configuration_unsupported", True),
    # 11. admission closed.
    ("11-admission-closed", _closed, "runtime_unavailable", True),
]


@pytest.mark.parametrize(
    "mutate,reason,members", [case[1:] for case in STEPS], ids=[case[0] for case in STEPS]
)
def test_first_failing_step_decides_the_reason(
    runtime_broker: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutate: Callable[[Runtime, Any], None],
    reason: str,
    members: bool,
) -> None:
    """Each ordered step 1..11 reports its exact reason over an otherwise valid tuple.

    Runtime members appear only once steps 4 through 9 validated the tuple. No
    broker key, app Valkey key or SQL row changes, the reader is closed, and no
    bootstrap secret, CA, endpoint or path is echoed.
    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-LANE-2 @spec PROTECTED-HOOK-LANE-3.
    """
    run_case(runtime_broker, tmp_path, monkeypatch, mutate, reason, members)


@pytest.mark.parametrize("requested", [None, "read-only"])
def test_fully_valid_tuple_without_enqueue_file_reports_runtime_unavailable_with_members(
    runtime_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, requested: str | None
) -> None:
    """Steps 1 through 11 pass but no ``enqueue.json`` exists: step 12 answers 503
    runtime_unavailable with members from the tuple; the final configuration_unsupported
    of the base contract is removed. The 200 ``supported`` answer is covered by
    ``test_hook_source_support_parity.py``; runtime_generation is a canonical decimal string.
    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-LANE-2/3.
    """
    run_case(
        runtime_broker,
        tmp_path,
        monkeypatch,
        _valid,
        "runtime_unavailable",
        True,
        requested=requested,
    )


def _noncanonical_manifest(rt: Runtime, broker: Any) -> None:
    """Reordered keys and whitespace, same canonical bytes, @spec PROTECTED-HOOK-SOURCE-9."""
    reordered = dict(reversed(list(rt.manifest.items())))
    rt.files["manifest.json"] = json.dumps(reordered, indent=2).encode() + b"\n"
    assert rt.files["manifest.json"] != canonical(rt.manifest)


def _extra_bootstrap_file(rt: Runtime, broker: Any) -> None:
    """An unrelated file beside the three bootstrap files, @spec PROTECTED-HOOK-SOURCE-9."""
    rt.files["README.txt"] = b"provisioner notes\n"


BOOTSTRAP_FILES = ("manifest.json", "ca.pem", "bootstrap.json")


def _symlinked_files(rt: Runtime, broker: Any) -> None:
    """Secret-volume layout: each file is a symlink to a regular file via ``..data``.

    @spec PROTECTED-HOOK-SOURCE-9.
    """

    def link(directory: Path) -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        data = directory / "..2026_10_05_00_00_00.000000001"
        data.mkdir(mode=0o700)
        for name in BOOTSTRAP_FILES:
            (directory / name).rename(data / name)
        (directory / "..data").symlink_to(data.name)
        for name in BOOTSTRAP_FILES:
            (directory / name).symlink_to(Path("..data") / name)

    rt.after_write.append(link)


@pytest.mark.parametrize(
    "mutate",
    [_noncanonical_manifest, _extra_bootstrap_file, _symlinked_files],
    ids=["noncanonical-manifest-json", "extra-bootstrap-file", "symlinks-to-regular-files"],
)
def test_tolerated_bootstrap_variants_reach_the_valid_outcome(
    runtime_broker: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutate: Callable[[Runtime, Any], None],
) -> None:
    """Manifests compare by canonical bytes; extra files are ignored; symlinks to
    regular files are followed.

    Each variant over an otherwise valid tuple passes steps 1 through 11 and,
    with no ``enqueue.json`` in this suite's runtime, reaches step 12's
    runtime_unavailable with the runtime members, as ``_valid`` does.
    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-LANE-2.
    """
    run_case(runtime_broker, tmp_path, monkeypatch, mutate, "runtime_unavailable", True)


# -- bootstrap directory -----------------------------------------------------------------


def _file(name: str, payload: bytes | None) -> Callable[[Runtime, Any], None]:
    """@spec PROTECTED-HOOK-SOURCE-9."""

    def apply(rt: Runtime, broker: Any) -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        rt.files[name] = payload

    return apply


def _bootstrap(**changes: Any) -> Callable[[Runtime, Any], None]:
    """@spec PROTECTED-HOOK-SOURCE-9."""

    def apply(rt: Runtime, broker: Any) -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        rt.bootstrap.update(changes)

    return apply


def _reader_fields(**changes: Any) -> Callable[[Runtime, Any], None]:
    """@spec PROTECTED-HOOK-SOURCE-9."""

    def apply(rt: Runtime, broker: Any) -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        for key, value in changes.items():
            if value is None:
                rt.bootstrap["control_reader"].pop(key)
            else:
                rt.bootstrap["control_reader"][key] = value

    return apply


def _bootstrap_extra(rt: Runtime, broker: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    rt.bootstrap["extra"] = True


def _manifest_extra(rt: Runtime, broker: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-LANE-2."""
    rt.files["manifest.json"] = canonical({**rt.manifest, "extra": True})


def _directory_missing(rt: Runtime, broker: Any) -> None:
    """The named directory does not exist, @spec PROTECTED-HOOK-SOURCE-9."""
    rt.absent = True


BOOTSTRAP: list[tuple[str, Callable[[Runtime, Any], None]]] = [
    ("directory-missing", _directory_missing),
    ("manifest-missing", _file("manifest.json", None)),
    ("ca-missing", _file("ca.pem", None)),
    ("bootstrap-missing", _file("bootstrap.json", None)),
    ("bootstrap-not-json", _file("bootstrap.json", b"{not json")),
    ("bootstrap-not-object", _file("bootstrap.json", b"[]")),
    ("bootstrap-extra-field", _bootstrap_extra),
    ("bootstrap-schema-version-2", _bootstrap(schema_version=2)),
    ("bootstrap-max-integer", _bootstrap(max_readiness_ms=MAX_READINESS_MS)),
    ("bootstrap-max-zero", _bootstrap(max_readiness_ms="0")),
    ("bootstrap-max-noncanonical", _bootstrap(max_readiness_ms="060000")),
    ("bootstrap-reader-missing-password", _reader_fields(password=None)),
    ("bootstrap-reader-extra-field", _reader_fields(role="control_reader")),
    ("manifest-not-json", _file("manifest.json", b"{not json")),
    ("manifest-extra-field", _manifest_extra),
    ("ca-not-pem", _file("ca.pem", b"not a certificate\n")),
]


@pytest.mark.parametrize("mutate", [case[1] for case in BOOTSTRAP], ids=[c[0] for c in BOOTSTRAP])
def test_missing_or_invalid_bootstrap_reports_runtime_unavailable(
    runtime_broker: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutate: Callable[[Runtime, Any], None],
) -> None:
    """A missing directory or file, or an invalid file: runtime_unavailable.

    The broker holds an otherwise fully valid tuple, so only the bootstrap
    decides. @spec PROTECTED-HOOK-SOURCE-9.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        root = tmp_path / ("runtime-" + secrets.token_hex(4))
        use_runtime_dir(monkeypatch, root)
        async with probe_app() as (app, client, agent):
            rt = runtime_for(runtime_broker, agent, root)
            mutate(rt, runtime_broker)
            await asyncio.to_thread(seed_protected_row, agent, rt.operation, rt.row)
            for key, value in rt.keys().items():
                runtime_broker.command("SET", key, value)
            rt.write_bootstrap()
            assert root.exists() != rt.absent
            before = await effects(app, agent), broker_snapshot(runtime_broker)
            response = await probe_with(client, agent)
            assert response.status_code == 503, response.text
            assert response.json() == expected(
                None, "read-only", str(GENERATION), "runtime_unavailable"
            )
            assert_safe(response, rt, agent)
            assert (await effects(app, agent), broker_snapshot(runtime_broker)) == before

    asyncio.run(asyncio.wait_for(scenario(), 30))


def test_unreadable_manifest_reports_runtime_unavailable(
    runtime_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unreadable bootstrap file is runtime_unavailable, @spec PROTECTED-HOOK-SOURCE-9."""
    if os.geteuid() == 0:
        pytest.skip("root reads a mode 000 file; directory-as-file covers unreadable for root")

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        root = tmp_path / ("runtime-" + secrets.token_hex(4))
        use_runtime_dir(monkeypatch, root)
        async with probe_app() as (app, client, agent):
            rt = runtime_for(runtime_broker, agent, root)
            await asyncio.to_thread(seed_protected_row, agent, rt.operation, rt.row)
            for key, value in rt.keys().items():
                runtime_broker.command("SET", key, value)
            rt.write_bootstrap()
            (root / "manifest.json").chmod(0o000)
            try:
                response = await probe_with(client, agent)
            finally:
                (root / "manifest.json").chmod(0o600)
            assert response.status_code == 503, response.text
            assert response.json() == expected(
                None, "read-only", str(GENERATION), "runtime_unavailable"
            )

    asyncio.run(asyncio.wait_for(scenario(), 30))


def test_bootstrap_is_read_afresh_on_each_evaluation(
    runtime_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A provisioner rotation is seen without restarting the API.

    The restored bootstrap passes steps 1 through 11, so the second answer
    carries the runtime members; without ``enqueue.json`` step 12 reports
    runtime_unavailable. @spec PROTECTED-HOOK-SOURCE-9.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        root = tmp_path / ("runtime-" + secrets.token_hex(4))
        use_runtime_dir(monkeypatch, root)
        async with probe_app() as (app, client, agent):
            rt = runtime_for(runtime_broker, agent, root)
            await asyncio.to_thread(seed_protected_row, agent, rt.operation, rt.row)
            for key, value in rt.keys().items():
                runtime_broker.command("SET", key, value)
            rt.files["bootstrap.json"] = None
            rt.write_bootstrap()
            first = await probe_with(client, agent, delivery="first")
            assert first.status_code == 503, first.text
            assert first.json() == expected(
                None, "read-only", str(GENERATION), "runtime_unavailable"
            )
            rt.files.clear()
            rt.write_bootstrap()
            second = await probe_with(client, agent, delivery="second")
            assert second.status_code == 503, second.text
            want = expected(None, "read-only", str(GENERATION), "runtime_unavailable")
            want.update(runtime_members(rt))
            assert second.json() == want

    asyncio.run(asyncio.wait_for(scenario(), 30))


# -- endpoints that are not this broker --------------------------------------------------


class OwnedListener:
    """Loopback TCP endpoint that accepts and never speaks TLS.

    Counts accepted connections and holds them open until released, so a
    reader opened against it stalls in its TLS handshake. @spec PROTECTED-HOOK-SOURCE-9.
    """

    def __init__(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(8)
        self.listener.settimeout(0.1)
        self.port = self.listener.getsockname()[1]
        self.accepted = threading.Event()
        self.connections: list[socket.socket] = []
        self.stopping = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        while not self.stopping.is_set():
            try:
                peer, _ = self.listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            self.connections.append(peer)
            self.accepted.set()

    @property
    def count(self) -> int:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        return len(self.connections)

    def release(self) -> None:
        """Close accepted peers so a stalled handshake fails now, @spec PROTECTED-HOOK-SOURCE-9."""
        for peer in list(self.connections):
            peer.close()

    def __enter__(self) -> OwnedListener:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        self.thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        """Only owned sockets and thread, @spec PROTECTED-HOOK-SOURCE-9."""
        self.stopping.set()
        self.thread.join(timeout=2)
        self.release()
        self.listener.close()


def closed_port() -> int:
    """A loopback port with no listener, @spec PROTECTED-HOOK-SOURCE-9."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def test_unreachable_broker_endpoint_reports_broker_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A valid bootstrap whose endpoint refuses connections: broker_unavailable.

    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-LANE-2/3.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        root = tmp_path / ("runtime-" + secrets.token_hex(4))
        use_runtime_dir(monkeypatch, root)
        async with probe_app() as (app, client, agent):
            rt = runtime_without_broker(agent, root, closed_port())
            await asyncio.to_thread(seed_protected_row, agent, rt.operation, rt.row)
            rt.write_bootstrap()
            before = await effects(app, agent)
            response = await probe_with(client, agent)
            assert response.status_code == 503, response.text
            assert response.json() == expected(
                None, "read-only", str(GENERATION), "broker_unavailable"
            )
            assert_safe(response, rt, agent)
            assert await effects(app, agent) == before

    asyncio.run(asyncio.wait_for(scenario(), 30))


def test_default_reader_username_is_invalid_bootstrap_without_connecting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ``default`` control reader username: runtime_unavailable, no broker connection.

    The bootstrap names an owned endpoint that counts accepted connections.
    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-LANE-2/3.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        root = tmp_path / ("runtime-" + secrets.token_hex(4))
        use_runtime_dir(monkeypatch, root)
        with OwnedListener() as listener:
            async with probe_app() as (app, client, agent):
                rt = runtime_without_broker(agent, root, listener.port)
                rt.bootstrap["control_reader"]["username"] = "default"
                await asyncio.to_thread(seed_protected_row, agent, rt.operation, rt.row)
                rt.write_bootstrap()
                before = await effects(app, agent)
                response = await probe_with(client, agent)
                assert response.status_code == 503, response.text
                assert response.json() == expected(
                    None, "read-only", str(GENERATION), "runtime_unavailable"
                )
                assert await effects(app, agent) == before
            assert listener.count == 0, "probe opened a broker connection"

    asyncio.run(asyncio.wait_for(scenario(), 30))


def test_runtime_mismatch_is_decided_before_broker_io(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Step 1 precedes step 2: decided from the bootstrap with no broker connection.

    @spec PROTECTED-HOOK-SOURCE-9.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        root = tmp_path / ("runtime-" + secrets.token_hex(4))
        use_runtime_dir(monkeypatch, root)
        with OwnedListener() as listener:
            async with probe_app() as (app, client, agent):
                rt = runtime_without_broker(agent, root, listener.port)
                rt.row["runtime_id"] = OTHER
                await asyncio.to_thread(seed_protected_row, agent, rt.operation, rt.row)
                rt.write_bootstrap()
                response = await probe_with(client, agent)
                assert response.status_code == 503, response.text
                assert response.json() == expected(
                    None, "read-only", str(GENERATION), "configuration_unsupported"
                )
            assert listener.count == 0, "probe opened a broker connection before step 1"

    asyncio.run(asyncio.wait_for(scenario(), 30))


@pytest.mark.parametrize("kind", ["never", "ordinary", "pending", "historical"])
def test_non_protected_rows_never_open_a_reader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    """Unconfigured, tombstoned and pending-history rows keep their reasons, no connection.

    The bootstrap names an owned endpoint that counts accepted connections.
    @spec PROTECTED-HOOK-SOURCE-9.
    """
    reasons = {
        "never": (None, None, "source_unconfigured"),
        "ordinary": (None, str(GENERATION), "source_closed"),
        "pending": ("read-only", None, "source_closed"),
        "historical": ("read-only", None, "source_closed"),
    }

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        root = tmp_path / ("runtime-" + secrets.token_hex(4))
        use_runtime_dir(monkeypatch, root)
        with OwnedListener() as listener:
            async with probe_app() as (app, client, agent):
                runtime_without_broker(agent, root, listener.port).write_bootstrap()
                if kind != "never":
                    await asyncio.to_thread(seed, agent, kind)
                body = BODIES[None]
                response = await post_support(
                    client,
                    agent,
                    body,
                    support_headers(legacy_secret(agent), requested=None, body=body),
                )
                effective, generation, reason = reasons[kind]
                assert response.status_code == 503, response.text
                assert response.json() == expected(None, effective, generation, reason)
            assert listener.count == 0, "probe opened a broker connection"

    asyncio.run(asyncio.wait_for(scenario(), 30))


# -- a broker that stalls ----------------------------------------------------------------


class StallingTLSBroker:
    """Owned loopback TLS endpoint that passes the reader's pin check, then stalls.

    It completes TLS with a leaf whose SPKI the manifest pins, then answers the
    reader's HELLO one byte every 0.2 s without ever finishing the reply, so no
    single socket read times out: only an overall evaluation budget ends it.
    Counts accepted connections; ``release`` closes every owned socket.
    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-LANE-2/3.
    """

    def __init__(self, private: Path) -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        private.mkdir(mode=0o700, exist_ok=True)
        ca_key, ca_cert = standalone_ca()
        self.ca_pem = ca_cert.public_bytes(serialization.Encoding.PEM).decode()
        self.pin = _broker.certificate(private, ca_key, ca_cert)
        self.context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self.context.load_cert_chain(str(private / "server.crt"), str(private / "server.key"))
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(16)
        self.listener.settimeout(0.1)
        self.port = self.listener.getsockname()[1]
        self.lock = threading.Lock()
        self.accepted = 0
        self.sockets: list[socket.socket] = []
        self.threads: list[threading.Thread] = []
        self.stopping = threading.Event()

    def __repr__(self) -> str:
        """@spec PROTECTED-HOOK-LANE-3."""
        return "<owned-stalling-broker>"

    def _accept(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        while not self.stopping.is_set():
            try:
                peer, _ = self.listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            worker = threading.Thread(target=self._stall, args=(peer,), daemon=True)
            with self.lock:
                self.accepted += 1
                self.sockets.append(peer)
                self.threads.append(worker)
            worker.start()

    def _stall(self, peer: socket.socket) -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        try:
            peer.settimeout(5)
            secured = self.context.wrap_socket(peer, server_side=True)
            with self.lock:
                self.sockets.append(secured)
            secured.settimeout(0.5)
            try:
                secured.recv(65536)
            except OSError:
                pass
            secured.sendall(b"+")
            while not self.stopping.wait(0.2):
                secured.sendall(b"A")
        except (OSError, ValueError):
            pass

    @property
    def count(self) -> int:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        with self.lock:
            return self.accepted

    def release(self) -> None:
        """Close every owned connection, @spec PROTECTED-HOOK-SOURCE-9."""
        self.stopping.set()
        with self.lock:
            owned = list(self.sockets)
        for sock in owned:
            try:
                sock.close()
            except OSError:
                pass

    def __enter__(self) -> StallingTLSBroker:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        self.acceptor = threading.Thread(target=self._accept, daemon=True)
        self.acceptor.start()
        return self

    def __exit__(self, *_: object) -> None:
        """Only owned sockets and threads, @spec PROTECTED-HOOK-SOURCE-9."""
        self.release()
        self.acceptor.join(timeout=2)
        with self.lock:
            workers = list(self.threads)
        for worker in workers:
            worker.join(timeout=2)
        self.listener.close()


async def wait_stalled(endpoint: Any, tasks: list[asyncio.Task[Any]], count: int) -> None:
    """Until ``count`` probes reached the endpoint; fail if one finished first.

    @spec PROTECTED-HOOK-SOURCE-9.
    """
    deadline = time.monotonic() + 10
    while endpoint.count < count:
        for task in tasks:
            if task.done():
                response = task.result()
                pytest.fail(
                    "probe finished without stalling on the broker: "
                    f"{response.status_code} {response.json().get('reason')}",
                    pytrace=False,
                )
        if time.monotonic() > deadline:
            pytest.fail("probes never reached the broker endpoint", pytrace=False)
        await asyncio.sleep(0.02)


async def cancel_all(tasks: list[asyncio.Task[Any]]) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    for task in tasks:
        if not task.done():
            task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def stalled_runtime(agent: str, root: Path, endpoint: StallingTLSBroker) -> Runtime:
    """A valid bootstrap whose pinned broker stalls, @spec PROTECTED-HOOK-SOURCE-9."""
    return runtime_without_broker(
        agent, root, endpoint.port, pin=endpoint.pin, ca_pem=endpoint.ca_pem
    )


def test_source_gate_is_released_before_broker_io(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """While a probe's broker read is stalled, the agent's gate is free.

    No production code is mocked: the bootstrap names an owned endpoint that
    completes pinned TLS and never finishes a reply. Once the probe is stalled
    there, an independent gate (its own engine, same advisory lock) acquires the
    same agent's gate while the probe is still pending. Releasing the endpoint
    then ends the probe: broker_unavailable.
    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-SOURCE-2.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9/2."""
        root = tmp_path / ("runtime-" + secrets.token_hex(4))
        use_runtime_dir(monkeypatch, root)
        observer = create_async_engine(get_settings().database_url, poolclass=NullPool)
        tasks: list[asyncio.Task[Any]] = []
        try:
            with StallingTLSBroker(tmp_path / "tls") as endpoint:
                async with probe_app() as (app, client, agent):
                    rt = stalled_runtime(agent, root, endpoint)
                    await asyncio.to_thread(seed_protected_row, agent, rt.operation, rt.row)
                    rt.write_bootstrap()
                    tasks.append(asyncio.create_task(probe_with(client, agent)))
                    await wait_stalled(endpoint, tasks, 1)
                    async with asyncio.timeout(4):
                        async with SourceGate(observer).hold(uuid.UUID(agent)):
                            assert not tasks[0].done(), "probe finished before the gate check"
                    endpoint.release()
                    response = await asyncio.wait_for(tasks[0], 15)
                    assert response.status_code == 503, response.text
                    assert response.json() == expected(
                        None, "read-only", str(GENERATION), "broker_unavailable"
                    )
        finally:
            await cancel_all(tasks)
            await observer.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 60))


def test_request_transaction_is_ended_before_broker_io(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No database connection waits on the broker in an open transaction.

    While the probe is stalled on the broker, no backend of this test database
    other than the observer is ``idle in transaction``; the probe still returns.
    @spec PROTECTED-HOOK-SOURCE-9.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        root = tmp_path / ("runtime-" + secrets.token_hex(4))
        use_runtime_dir(monkeypatch, root)
        observer = create_async_engine(get_settings().database_url, poolclass=NullPool)
        tasks: list[asyncio.Task[Any]] = []
        try:
            with StallingTLSBroker(tmp_path / "tls") as endpoint:
                async with probe_app() as (app, client, agent):
                    rt = stalled_runtime(agent, root, endpoint)
                    await asyncio.to_thread(seed_protected_row, agent, rt.operation, rt.row)
                    rt.write_bootstrap()
                    tasks.append(asyncio.create_task(probe_with(client, agent)))
                    await wait_stalled(endpoint, tasks, 1)
                    async with observer.connect() as conn:
                        idle = await conn.scalar(
                            text(
                                "SELECT count(*) FROM pg_stat_activity "
                                "WHERE datname=current_database() "
                                "AND state='idle in transaction' "
                                "AND pid<>pg_backend_pid()"
                            )
                        )
                    assert not tasks[0].done(), "probe finished before the database check"
                    assert idle == 0, (
                        "a database connection is idle in transaction during broker I/O"
                    )
                    endpoint.release()
                    response = await asyncio.wait_for(tasks[0], 15)
                    assert response.status_code == 503, response.text
                    assert response.json() == expected(
                        None, "read-only", str(GENERATION), "broker_unavailable"
                    )
        finally:
            await cancel_all(tasks)
            await observer.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 60))


def test_stalled_broker_is_bounded_by_the_evaluation_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A broker that never finishes a reply yields broker_unavailable within the budget.

    The budget is five seconds; only a generous upper bound is asserted.
    @spec PROTECTED-HOOK-SOURCE-9.
    """
    bound = 5 + 7

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        root = tmp_path / ("runtime-" + secrets.token_hex(4))
        use_runtime_dir(monkeypatch, root)
        tasks: list[asyncio.Task[Any]] = []
        try:
            with StallingTLSBroker(tmp_path / "tls") as endpoint:
                async with probe_app() as (app, client, agent):
                    rt = stalled_runtime(agent, root, endpoint)
                    await asyncio.to_thread(seed_protected_row, agent, rt.operation, rt.row)
                    rt.write_bootstrap()
                    started = time.monotonic()
                    tasks.append(asyncio.create_task(probe_with(client, agent)))
                    await wait_stalled(endpoint, tasks, 1)
                    try:
                        response = await asyncio.wait_for(
                            asyncio.shield(tasks[0]), bound - (time.monotonic() - started)
                        )
                    except TimeoutError:
                        pytest.fail("stalled evaluation exceeded its budget", pytrace=False)
                    assert response.status_code == 503, response.text
                    assert response.json() == expected(
                        None, "read-only", str(GENERATION), "broker_unavailable"
                    )
                    assert endpoint.count == 1
        finally:
            await cancel_all(tasks)

    asyncio.run(asyncio.wait_for(scenario(), 60))


def test_evaluations_beyond_four_fail_fast_without_connecting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With four evaluations stalled on the broker, a fifth does not queue or connect.

    It reports 503 broker_unavailable well under the five second budget, and the
    endpoint sees no fifth connection. @spec PROTECTED-HOOK-SOURCE-9.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        root = tmp_path / ("runtime-" + secrets.token_hex(4))
        use_runtime_dir(monkeypatch, root)
        tasks: list[asyncio.Task[Any]] = []
        try:
            with StallingTLSBroker(tmp_path / "tls") as endpoint:
                async with probe_app() as (app, client, agent):
                    rt = stalled_runtime(agent, root, endpoint)
                    await asyncio.to_thread(seed_protected_row, agent, rt.operation, rt.row)
                    rt.write_bootstrap()
                    for index in range(4):
                        tasks.append(
                            asyncio.create_task(probe_with(client, agent, delivery=f"cap-{index}"))
                        )
                    await wait_stalled(endpoint, tasks, 4)
                    try:
                        fifth = await asyncio.wait_for(
                            probe_with(client, agent, delivery="cap-4"), 3
                        )
                    except TimeoutError:
                        pytest.fail(
                            "fifth concurrent evaluation queued or connected "
                            f"({endpoint.count} connections)",
                            pytrace=False,
                        )
                    assert fifth.status_code == 503, fifth.text
                    assert fifth.json() == expected(
                        None, "read-only", str(GENERATION), "broker_unavailable"
                    )
                    assert endpoint.count == 4, "fifth evaluation connected"
                    endpoint.release()
                    for response in await asyncio.gather(
                        *(asyncio.wait_for(task, 15) for task in tasks)
                    ):
                        assert response.json()["reason"] == "broker_unavailable"
        finally:
            await cancel_all(tasks)

    asyncio.run(asyncio.wait_for(scenario(), 60))


# -- special bootstrap files ---------------------------------------------------------------


def _fifo(name: str) -> Callable[[Path], None]:
    """Replace one bootstrap file with a FIFO, @spec PROTECTED-HOOK-SOURCE-9."""

    def apply(directory: Path) -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        (directory / name).unlink()
        os.mkfifo(directory / name, 0o600)

    return apply


def _directory_as(name: str) -> Callable[[Path], None]:
    """A directory where a file belongs (unreadable for root too), @spec PROTECTED-HOOK-SOURCE-9."""

    def apply(directory: Path) -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        (directory / name).unlink()
        (directory / name).mkdir(mode=0o700)

    return apply


def _symlink_to_directory(directory: Path) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    target = directory / "target-directory"
    target.mkdir(mode=0o700)
    (directory / "manifest.json").unlink()
    (directory / "manifest.json").symlink_to(target.name)


def _symlink_to_fifo(directory: Path) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    os.mkfifo(directory / "target-fifo", 0o600)
    (directory / "manifest.json").unlink()
    (directory / "manifest.json").symlink_to("target-fifo")


def _oversized_bootstrap(directory: Path) -> None:
    """Valid JSON padded to 4 MiB, far past any bootstrap bound, @spec PROTECTED-HOOK-SOURCE-9."""
    path = directory / "bootstrap.json"
    path.write_bytes(path.read_bytes() + b" " * (4 * 1024 * 1024))


def unblock_fifos(directory: Path) -> None:
    """Open and close a writer on each owned FIFO so a blocked reader sees EOF.

    @spec PROTECTED-HOOK-SOURCE-9.
    """
    for path in directory.iterdir():
        if not stat.S_ISFIFO(path.lstat().st_mode):
            continue
        for flags in (os.O_WRONLY | os.O_NONBLOCK, os.O_RDWR | os.O_NONBLOCK):
            try:
                os.close(os.open(path, flags))
                break
            except OSError:
                continue


SPECIAL: list[tuple[str, Callable[[Path], None]]] = [
    ("fifo-manifest", _fifo("manifest.json")),
    ("fifo-ca", _fifo("ca.pem")),
    ("fifo-bootstrap", _fifo("bootstrap.json")),
    ("directory-as-manifest", _directory_as("manifest.json")),
    ("symlink-to-directory", _symlink_to_directory),
    ("symlink-to-fifo", _symlink_to_fifo),
    ("oversized-bootstrap", _oversized_bootstrap),
]


@pytest.mark.parametrize("special", [case[1] for case in SPECIAL], ids=[c[0] for c in SPECIAL])
def test_special_or_oversized_bootstrap_files_are_invalid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, special: Callable[[Path], None]
) -> None:
    """A FIFO, directory, symlink to either, or an oversized file: prompt runtime_unavailable.

    Files must be regular after symlink resolution, opened without blocking and
    bounded in size. A FIFO left blocking is unblocked by the test after the
    bound so nothing hangs. @spec PROTECTED-HOOK-SOURCE-9.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        root = tmp_path / ("runtime-" + secrets.token_hex(4))
        use_runtime_dir(monkeypatch, root)
        tasks: list[asyncio.Task[Any]] = []
        with OwnedListener() as listener:
            async with probe_app() as (app, client, agent):
                rt = runtime_without_broker(agent, root, listener.port)
                rt.after_write.append(special)
                await asyncio.to_thread(seed_protected_row, agent, rt.operation, rt.row)
                rt.write_bootstrap()
                tasks.append(asyncio.create_task(probe_with(client, agent)))
                try:
                    done, _ = await asyncio.wait(tasks, timeout=3)
                    if not done:
                        pytest.fail("probe blocked on a special bootstrap file", pytrace=False)
                    response = tasks[0].result()
                finally:
                    deadline = time.monotonic() + 10
                    while not tasks[0].done() and time.monotonic() < deadline:
                        unblock_fifos(root)
                        await asyncio.sleep(0.05)
                    await cancel_all(tasks)
                assert response.status_code == 503, response.text
                assert response.json() == expected(
                    None, "read-only", str(GENERATION), "runtime_unavailable"
                )
            assert listener.count == 0, "probe connected with an invalid bootstrap"

    asyncio.run(asyncio.wait_for(scenario(), 40))


def test_malformed_ca_is_refused_in_bounded_time(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ~64 KiB ``ca.pem`` of BEGIN plus newlines without END is refused promptly.

    Validation is linear in size, so it costs about what a short invalid CA
    costs; only an upper bound relative to that control is asserted.
    @spec PROTECTED-HOOK-SOURCE-9.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        root = tmp_path / ("runtime-" + secrets.token_hex(4))
        use_runtime_dir(monkeypatch, root)
        with OwnedListener() as listener:
            async with probe_app() as (app, client, agent):
                rt = runtime_without_broker(agent, root, listener.port)
                await asyncio.to_thread(seed_protected_row, agent, rt.operation, rt.row)
                timings = []
                header = b"-----BEGIN CERTIFICATE-----"
                for index, ca in enumerate(
                    (b"not a certificate\n", header + b"\n" * (65000 - len(header)))
                ):
                    rt.files["ca.pem"] = ca
                    rt.write_bootstrap()
                    started = time.monotonic()
                    response = await probe_with(client, agent, delivery=f"ca-{index}")
                    timings.append(time.monotonic() - started)
                    assert response.status_code == 503, response.text
                    assert response.json() == expected(
                        None, "read-only", str(GENERATION), "runtime_unavailable"
                    )
                control, malformed = timings
                assert malformed < control + 1.0, (
                    f"malformed CA took {malformed:.2f}s (control {control:.2f}s)"
                )
            assert listener.count == 0

    asyncio.run(asyncio.wait_for(scenario(), 60))
