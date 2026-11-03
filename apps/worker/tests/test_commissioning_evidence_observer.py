"""Actual read-only evidence transport; synthetic B2 records, no series acceptance."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import importlib
import importlib.util
import json
from typing import Any

import pytest
from curie_protected_hooks.authority_records import parse_manifest
from curie_protected_hooks.commissioning_records import parse_commissioning_permit
from curie_protected_hooks.commissioning_series_bootstrap import parse_series_bootstrap

from . import test_commissioning_worker_entry as helpers

wb = helpers.wb
_package = helpers._name
contract = importlib.import_module(_package + ".test_commissioning_series_guard")
state_tests = importlib.import_module(_package + ".test_commissioning_evidence_records")
encoded = helpers.canonical
pworker_service = wb.pworker_service
pytestmark = pytest.mark.parametrize("pworker_service", [True], indirect=True)


def module() -> Any:
    name = "curie_worker.commissioning_evidence_observer"
    assert importlib.util.find_spec(name) is not None, (
        "independent evidence observer not implemented"
    )
    return importlib.import_module(name)


def test_owned_fixture_retains_real_bytes_after_broker_restart(pworker_service):
    key = "protected:commissioning:journal:example-persistence"
    pworker_service.command("SET", key, b"original-retained-value")
    before = pworker_service.command("INFO", "server")["run_id"]
    pworker_service.restart()
    assert pworker_service.command("INFO", "server")["run_id"] != before
    assert pworker_service.command("GET", key) == b"original-retained-value"
    pworker_service.command("DEL", key)


class SeriesFixture:
    """Independent generic record construction, not a B2 campaign issuer."""

    def __init__(self, broker):
        from curie_protected_hooks.commissioning_evidence_acl import evidence_observer_acl_rules

        self.broker = broker
        self.principal = wb.WorkerPrincipal(
            broker, role="evidence-observer", rules=evidence_observer_acl_rules()
        )
        self.principal.install()
        b, g, _ = contract.tuple_values()
        m = broker.manifest().as_dict()
        for key in ("runtime_id", "runtime_generation", "execution_config_digest"):
            b[key] = m[key]
        b["production_credentials"] = copy.deepcopy(m["credential_refs"])
        b["evidence_observer_identity"] = {"id": self.principal.username, "generation": "1"}
        g["bootstrap_digest"] = hashlib.sha256(encoded(b)).hexdigest()
        for key in ("enqueue", "worker", "verifier"):
            g["broker_acl"][key]["credential"] = copy.deepcopy(b["production_credentials"][key])
        g["broker_acl"]["evidence_observer"]["credential"] = copy.deepcopy(
            b["evidence_observer_identity"]
        )
        m["guard_identity"]["config_sha256"] = hashlib.sha256(encoded(g)).hexdigest()
        self.bootstrap = parse_series_bootstrap(encoded(b))
        self.guard = encoded(g)
        self.manifest = parse_manifest(encoded(m))
        seconds, micros = broker.command("TIME")
        now = int(seconds) * 1000 + int(micros) // 1000
        duration = min(20000, int(b["max_permit_ms"]))
        p = importlib.import_module(_package + ".test_commissioning_records").permit_value()
        p.update(
            runtime_id=m["runtime_id"],
            runtime_generation=m["runtime_generation"],
            execution_config_digest=m["execution_config_digest"],
            manifest_digest=self.manifest.digest,
            bootstrap_digest=self.bootstrap.digest,
            broker_identity=copy.deepcopy(m["broker_identity"]),
            guard_identity=copy.deepcopy(m["guard_identity"]),
            issuer_identity=b["issuer_identity"],
            control_identity=b["control_identity"],
            candidate_credentials=b["candidate_credentials"],
            sources=copy.deepcopy(b["source_scope"]),
            targets=copy.deepcopy(b["target_scope"]),
            issued_at_ms=str(now - 1),
            expires_at_ms=str(now + duration - 1),
            limits={k: "5" for k in p["limits"]},
        )
        self.permit = parse_commissioning_permit(encoded(p))
        self.fixture = state_tests.state_fixture(self.permit)
        suffix = p["campaign_id"] + ":" + p["campaign_generation"]
        self.keys = (
            f"protected:commissioning:permit:{suffix}",
            f"protected:commissioning:journal:{suffix}",
            f"protected:commissioning:ownership:{suffix}",
            f"protected:commissioning:reservation:{suffix}:{state_tests.DELIVERY}",
            f"protected:commissioning:reservation:{suffix}:{state_tests.LAUNCH_RESERVATION}",
            f"protected:commissioning:ownership:{suffix}:{state_tests.LAUNCH}",
            f"protected:commissioning:admission:{suffix}:{state_tests.DELIVERY}",
        )
        for key, value in zip(self.keys, self.fixture["values"], strict=True):
            broker.command("SET", key, encoded(value))

    def binding(self):
        return module().SeriesEvidenceBinding(self.bootstrap, self.guard, self.manifest)

    def inventory(self):
        from curie_protected_hooks.commissioning_evidence_records import EvidenceInventory

        return EvidenceInventory(**self.fixture["inventory"])

    async def open(self, **changes):
        return await module().RetainedEvidenceObserver.connect(
            self.binding(),
            changes.get("credential", self.principal.credential()),
            changes.get("ca_pem", self.broker.ca_pem),
        )


@pytest.fixture
def series(pworker_service):
    pworker_service.command("FLUSHDB")
    original = (pworker_service.private / "users.acl").read_bytes()
    fixture = None
    try:
        fixture = SeriesFixture(pworker_service)
        yield fixture
    finally:
        if fixture is not None:
            fixture.principal.remove()
        (pworker_service.private / "users.acl").write_bytes(original)
        pworker_service.command("FLUSHDB")


def test_actual_observer_reads_all_families_and_its_own_client_identity(series):
    async def scenario():
        observer = await series.open()
        try:
            result = await observer.read(series.permit, series.inventory())
            assert result.state.permit.canonical_bytes == series.permit.canonical_bytes
            assert (
                len(result.state.reservations) == 2
                and len(result.state.ownership) == len(result.state.admissions) == 1
            )
            assert result.actual_broker_identity == series.manifest.as_dict()["broker_identity"]
            assert result.state.permit.as_dict()["broker_identity"] == result.actual_broker_identity
            assert any(
                c.client_id == observer.client_id and c.username == series.principal.username
                for c in result.clients
            )
            assert result.broker_time_ms > 0
            assert series.principal.username not in repr(observer) + repr(result)
            result.actual_broker_identity["run_id"] = "changed"
            assert (await observer.clients()).actual_broker_identity == series.manifest.as_dict()[
                "broker_identity"
            ]
        finally:
            await observer.close()
        with pytest.raises(module().EvidenceObservationRefused):
            await observer.read(series.permit, series.inventory())

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "fault", ["missing", "wrong-type", "corrupt", "foreign-c", "incomplete-inventory"]
)
def test_invalid_observation_poison_survives_record_restoration(series, fault):
    async def scenario():
        observer = await series.open()
        key = series.keys[1]
        original = series.broker.command("GET", key)
        inventory = series.inventory()
        if fault == "missing":
            series.broker.command("DEL", key)
        elif fault == "wrong-type":
            series.broker.command("DEL", key)
            series.broker.command("LPUSH", key, "invalid")
        elif fault == "corrupt":
            series.broker.command("SET", key, b"{")
        elif fault == "foreign-c":
            key = series.keys[5]
            original = series.broker.command("GET", key)
            changed = json.loads(original)
            changed["campaign"]["campaign_generation"] = "999"
            series.broker.command("SET", key, encoded(changed))
        else:
            from curie_protected_hooks.commissioning_evidence_records import EvidenceInventory

            inventory = EvidenceInventory()
        try:
            with pytest.raises(module().EvidenceObservationRefused):
                await observer.read(series.permit, inventory)
            series.broker.command("DEL", key)
            series.broker.command("SET", key, original)
            with pytest.raises(module().EvidenceObservationRefused):
                await observer.read(series.permit, series.inventory())
        finally:
            await observer.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("fault", ["username", "password", "ca", "spki", "v1"])
def test_binding_and_authenticated_identity_fail_closed(series, fault):
    async def scenario():
        api = module()
        credential = series.principal.credential()
        ca = series.broker.ca_pem
        if fault in ("username", "password"):
            from curie_protected_hooks.worker_transport import WorkerCredential

            credential = WorkerCredential(
                "example-foreign" if fault == "username" else credential.username,
                "example-wrong-password" if fault == "password" else credential.password,
            )
        elif fault == "ca":
            ca = "invalid-ca"
        elif fault == "spki":
            m = series.manifest.as_dict()
            m["broker_identity"]["tls_spki_sha256"] = "f" * 64
            series.manifest = parse_manifest(encoded(m))
            p = series.permit.as_dict()
            p["broker_identity"] = copy.deepcopy(m["broker_identity"])
            p["manifest_digest"] = series.manifest.digest
            series.permit = parse_commissioning_permit(encoded(p))
        else:
            b = series.bootstrap.as_dict()
            b["schema_version"] = 1
            with pytest.raises(ValueError):
                parse_series_bootstrap(encoded(b))
            with pytest.raises(api.EvidenceObservationRefused):
                api.SeriesEvidenceBinding(object(), series.guard, series.manifest)
            return
        with pytest.raises(api.EvidenceObservationRefused):
            observer = await series.open(credential=credential, ca_pem=ca)
            await observer.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("fault", ["kill", "acl"])
def test_lost_session_cannot_reconnect_after_restoration(series, fault):
    async def scenario():
        observer = await series.open()
        try:
            if fault == "kill":
                series.broker.command("CLIENT", "KILL", "ID", observer.client_id)
            else:
                # Root permissions OR a selector grants access; revoke both.
                # https://valkey.io/commands/acl-setuser/
                series.broker.command(
                    "ACL", "SETUSER", series.principal.username, "clearselectors", "-get"
                )
            with pytest.raises(module().EvidenceObservationRefused):
                await observer.read(series.permit, series.inventory())
            if fault == "acl":
                series.principal.install()
            with pytest.raises(module().EvidenceObservationRefused):
                await observer.clients()
        finally:
            await observer.close()

    asyncio.run(scenario())


def test_actual_broker_restart_retains_original_c_and_changes_observed_epoch(series):
    async def scenario():
        observer = await series.open()
        original = series.broker.command("GET", series.keys[0])
        old_epoch = series.manifest.as_dict()["broker_identity"]["run_id"]
        # Persist only this fixture's private ACL file; no credential is emitted.
        rows = series.broker.command("ACL", "LIST")
        wb.helpers.write_private(
            series.broker.private / "users.acl",
            "\n".join(row.decode() if isinstance(row, bytes) else row for row in rows) + "\n",
        )
        series.broker.restart()
        try:
            with pytest.raises(module().EvidenceObservationRefused):
                await observer.clients()
        finally:
            await observer.close()
        fresh = await series.open()
        try:
            result = await fresh.read(series.permit, series.inventory())
            assert result.actual_broker_identity["run_id"] != old_epoch
            assert result.state.permit.as_dict()["broker_identity"]["run_id"] == old_epoch
            assert result.state.permit.canonical_bytes == original
            assert series.broker.command("GET", series.keys[0]) == original
        finally:
            await fresh.close()

    asyncio.run(scenario())


def test_actual_server_pause_is_bounded_and_poisoned(series):
    import time

    async def scenario():
        observer = await series.open()
        try:
            series.broker.command("CLIENT", "PAUSE", 5000, "ALL")
            started = time.monotonic()
            with pytest.raises(module().EvidenceObservationRefused):
                await observer.clients()
            assert 0 < time.monotonic() - started < 4.8
            with pytest.raises(module().EvidenceObservationRefused):
                await observer.clients()
        finally:
            await observer.close()

    try:
        asyncio.run(scenario())
    finally:
        series.broker.ready()


def test_readonly_principal_cannot_write_scan_or_get_client_id(series):
    from redis.exceptions import ResponseError

    client = series.principal.raw(protocol=2)
    try:
        for command in [
            ("SET", series.keys[0], "invalid"),
            ("SCAN", 0),
            ("CLIENT", "ID"),
            ("GET", "curie:runs"),
        ]:
            with pytest.raises(ResponseError):
                client.execute_command(*command)
    finally:
        client.close()


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "id=1 user=example id=2\n",
        "id=1 user=example\nid=1 user=other\n",
        "id=01 user=example\n",
        "id=-1 user=example\n",
        "id=1\n",
        "id=1 user=\n",
        "id=1 user=example malformed\n",
        "x" * 1048577,
        "id=1 user=example extra=" + "x" * 4096,
    ],
)
def test_malformed_client_wire_is_refused(raw, pworker_service):
    with pytest.raises(module().EvidenceObservationRefused):
        module()._clients(raw)


@pytest.mark.parametrize(
    "field",
    [
        "runtime_id",
        "runtime_generation",
        "execution_config_digest",
        "manifest_digest",
        "bootstrap_digest",
        "broker_identity",
        "guard_identity",
        "issuer_identity",
        "control_identity",
        "candidate_credentials",
        "sources",
        "targets",
        "duration",
    ],
)
def test_foreign_permit_is_refused_before_retained_reads(series, field):
    async def scenario():
        p = series.permit.as_dict()
        if field == "duration":
            p["expires_at_ms"] = str(
                int(p["issued_at_ms"]) + int(series.bootstrap.as_dict()["max_permit_ms"]) + 1
            )
        elif field == "runtime_generation":
            p[field] = "99"
        elif field == "runtime_id":
            p[field] = "00000000-0000-4000-8000-000000000099"
        elif field.endswith("digest"):
            p[field] = "e" * 64
        elif field == "broker_identity":
            p[field]["run_id"] = "e" * 40
        elif field == "guard_identity":
            p[field]["config_sha256"] = "e" * 64
        elif field == "candidate_credentials":
            p[field]["worker"]["generation"] = "99"
        elif field in ("issuer_identity", "control_identity"):
            p[field]["generation"] = "99"
        elif field == "sources":
            p[field][0]["policy_fingerprint"] = "e" * 64
        else:
            p[field] = ["example-foreign-target"]
        changed = parse_commissioning_permit(encoded(p))
        observer = await series.open()
        try:
            with pytest.raises(module().EvidenceObservationRefused):
                await observer.read(changed, series.inventory())
            with pytest.raises(module().EvidenceObservationRefused):
                await observer.clients()
        finally:
            await observer.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("operation", ["clients", "read", "connect", "socket-timeout", "spki"])
def test_late_tls_response_cannot_extend_public_deadline(series, operation):
    """Real encrypted TCP relay delays each reply; no broker response is mocked."""
    import time

    async def scenario():
        delayed = False
        tasks = set()
        writers = set()
        client_eof = asyncio.Event()

        async def relay(reader, writer, *, delay):
            responses = 0
            while chunk := await reader.read(65536):
                if delay and delayed:
                    if operation == "spki":
                        if responses:
                            await asyncio.sleep(3)
                    else:
                        await asyncio.sleep(3 if operation == "socket-timeout" else 1.15)
                responses += 1
                writer.write(chunk)
                await writer.drain()
            if not delay:
                client_eof.set()
                writer.transport.abort()

        async def accept(reader, writer):
            task = asyncio.current_task()
            tasks.add(task)
            upstream = None
            try:
                peer, upstream = await asyncio.open_connection("127.0.0.1", series.broker.port)
                writers.update((writer, upstream))
                await asyncio.gather(
                    relay(reader, upstream, delay=False), relay(peer, writer, delay=True)
                )
            except (ConnectionError, asyncio.CancelledError):
                pass
            finally:
                writer.close()
                if upstream is not None:
                    upstream.close()
                tasks.discard(task)

        server = await asyncio.start_server(accept, "127.0.0.1", 0)
        observer = None
        try:
            m = series.manifest.as_dict()
            m["broker_identity"]["endpoint"]["port"] = server.sockets[0].getsockname()[1]
            if operation == "spki":
                m["broker_identity"]["tls_spki_sha256"] = "e" * 64
            series.manifest = parse_manifest(encoded(m))
            p = series.permit.as_dict()
            p["broker_identity"] = m["broker_identity"]
            p["manifest_digest"] = series.manifest.digest
            series.permit = parse_commissioning_permit(encoded(p))
            fixture = state_tests.state_fixture(series.permit)
            for key, value in zip(series.keys, fixture["values"], strict=True):
                series.broker.command("SET", key, encoded(value))
            if operation not in ("connect", "spki"):
                observer = await series.open()
            delayed = True
            started = time.monotonic()
            with pytest.raises(module().EvidenceObservationRefused):
                if operation in ("clients", "socket-timeout"):
                    await observer.clients()
                elif operation == "read":
                    await observer.read(series.permit, series.inventory())
                else:
                    observer = await series.open()
            assert time.monotonic() - started < 4.35
            if operation in ("socket-timeout", "spki"):
                # Observe actual TCP closure before relay cleanup can conceal a leak.
                await asyncio.wait_for(client_eof.wait(), 0.3)
            delayed = False
            if observer is not None:
                with pytest.raises(module().EvidenceObservationRefused):
                    await observer.clients()
        finally:
            try:
                if observer is not None:
                    await observer.close()
            finally:
                server.close()
                await server.wait_closed()
                for writer in writers:
                    writer.transport.abort()
                pending = tuple(tasks)
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

    asyncio.run(scenario())
