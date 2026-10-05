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
import secrets
import socket
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
from curie_protected_hooks.source_policy_records import policy_fingerprint
from curie_protected_hooks.source_policy_sql import SourceGate
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


def standalone_ca_pem() -> str:
    """A CA no broker certificate chains to, @spec PROTECTED-HOOK-LANE-2/3."""
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
        .sign(key, hashes.SHA256())
    )
    return cert.public_bytes(serialization.Encoding.PEM).decode()


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


def runtime_without_broker(agent: str, directory: Path, port: int) -> Runtime:
    """A valid bootstrap pointing at an owned non-broker endpoint, @spec PROTECTED-HOOK-SOURCE-9."""
    return Runtime(
        agent=agent,
        directory=directory,
        port=port,
        pin="0" * 64,
        run_id="a" * 40,
        ca_pem=standalone_ca_pem(),
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
            await wait_reader_closed(broker)
            assert (await effects(app, agent), broker_snapshot(broker)) == before

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
def test_fully_valid_tuple_reports_unsupported_with_runtime_members(
    runtime_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, requested: str | None
) -> None:
    """Every step passes: 503 configuration_unsupported, members from the tuple.

    Ingress does not yet admit protected deliveries, so the probe never claims
    support; runtime_generation is a canonical decimal string.
    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-LANE-2/3.
    """
    run_case(
        runtime_broker,
        tmp_path,
        monkeypatch,
        _valid,
        "configuration_unsupported",
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


@pytest.mark.parametrize(
    "mutate",
    [_noncanonical_manifest, _extra_bootstrap_file],
    ids=["noncanonical-manifest-json", "extra-bootstrap-file"],
)
def test_tolerated_bootstrap_variants_reach_the_valid_outcome(
    runtime_broker: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutate: Callable[[Runtime, Any], None],
) -> None:
    """Manifests compare by canonical bytes; extra bootstrap files are ignored.

    Either variant over an otherwise valid tuple still reaches
    configuration_unsupported with the runtime members.
    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-LANE-2.
    """
    run_case(runtime_broker, tmp_path, monkeypatch, mutate, "configuration_unsupported", True)


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

    @spec PROTECTED-HOOK-SOURCE-9.
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
            want = expected(None, "read-only", str(GENERATION), "configuration_unsupported")
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
    """Step 1 precedes step 2: a stalling endpoint cannot turn it into broker_unavailable.

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
                started = time.monotonic()
                response = await post_support(
                    client,
                    agent,
                    body,
                    support_headers(legacy_secret(agent), requested=None, body=body),
                )
                elapsed = time.monotonic() - started
                effective, generation, reason = reasons[kind]
                assert response.status_code == 503, response.text
                assert response.json() == expected(None, effective, generation, reason)
                assert elapsed < 1.5
            assert listener.count == 0, "probe opened a broker connection"

    asyncio.run(asyncio.wait_for(scenario(), 30))


def test_source_gate_is_released_before_broker_io(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """While a probe's broker handshake is stalled, the agent's gate is free.

    No production code is mocked: the bootstrap names an owned endpoint that
    accepts and never answers TLS. Once it has accepted the probe's connection,
    an independent gate (its own engine, same advisory lock) must acquire the
    same agent's gate before the probe finishes. Releasing the endpoint then
    fails the handshake: broker_unavailable.
    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-SOURCE-2.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9/2."""
        root = tmp_path / ("runtime-" + secrets.token_hex(4))
        use_runtime_dir(monkeypatch, root)
        observer = create_async_engine(get_settings().database_url, poolclass=NullPool)
        task: asyncio.Task[Any] | None = None
        try:
            with OwnedListener() as listener:
                async with probe_app() as (app, client, agent):
                    rt = runtime_without_broker(agent, root, listener.port)
                    await asyncio.to_thread(seed_protected_row, agent, rt.operation, rt.row)
                    rt.write_bootstrap()
                    task = asyncio.create_task(probe_with(client, agent))
                    waiter = asyncio.create_task(asyncio.to_thread(listener.accepted.wait, 10))
                    done, _ = await asyncio.wait(
                        {task, waiter}, return_when=asyncio.FIRST_COMPLETED
                    )
                    if task in done:
                        response = task.result()
                        listener.accepted.set()
                        await waiter
                        pytest.fail(
                            "probe finished without opening a broker connection: "
                            f"{response.status_code} {response.json().get('reason')}",
                            pytrace=False,
                        )
                    assert await waiter, "probe never reached the broker endpoint"
                    gate = SourceGate(observer)
                    async with asyncio.timeout(1.5):
                        async with gate.hold(uuid.UUID(agent)):
                            assert not task.done(), "probe finished before the gate check"
                    listener.release()
                    response = await asyncio.wait_for(task, 10)
                    assert response.status_code == 503, response.text
                    assert response.json() == expected(
                        None, "read-only", str(GENERATION), "broker_unavailable"
                    )
        finally:
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await observer.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 40))
