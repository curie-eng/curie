"""Worker protected callers refuse commissioning records, @spec PROTECTED-COMMISSIONING-4.

Worker composition, the sandbox substrate and the mediated launch server each
meet one valid commissioning record (``commissioning_record_vectors`` of the
protected hooks tests) where they read a production record: a provisioner file,
a retained control record, the admission binding, a stream field or a launch
request body. Every other record stays genuine production, so each case is a
mixed production and commissioning reference set. Each caller refuses before a
positive claim, launch or start: no Kubernetes object, owned launch or runtime.
Every suite runs the actual owned TLS broker and launch pair of
``test_protected_retirement_server``.
"""

import asyncio
import dataclasses
import importlib
import json
import uuid

import pytest
from curie_protected_hooks.worker_verification import Dispatchable, verify_delivery
from curie_worker.kernel.routing import _thread_key_for
from curie_worker.protected_launch_client import LaunchClientError
from curie_worker.protected_launch_records import canonical_json
from curie_worker.protected_launch_server import CAPABILITY_HEADER
from curie_worker.protected_retirement import OwnedRetirementProof
from curie_worker.protected_substrate import ProtectedSandboxSubstrate

from .protected_configuration_vectors import canonical
from .test_protected_retirement_server import actual_pair, wb

pworker_service = wb.pworker_service
owned_worker = wb.owned_worker
# Registered by the launch integration suite as the protected hooks test package.
vectors = importlib.import_module("protected_launch_broker_tests.commissioning_record_vectors")
KINDS = vectors.KINDS


def control_keys(manifest, delivery):
    """The retained records every accepted-delivery verification reads."""
    m = manifest.as_dict()
    runtime = m["runtime_id"] + ":" + m["runtime_generation"]
    return {
        "manifest": "protected:control:manifest:" + manifest.digest,
        "readiness": "protected:control:readiness:" + runtime,
        "qualification": "protected:control:qualification:" + m["qualification_id"] + ":1",
        "binding": "protected:admission:binding:" + delivery.turn.event_id,
    }


def substitutions(broker, manifest, delivery):
    """Yield once per key and kind with that record substituted, then restore it."""
    records = vectors.commissioning_records(manifest.as_dict())
    for name, key in control_keys(manifest, delivery).items():
        original = broker.command("GET", key)
        assert original is not None, name
        for kind in KINDS:
            broker.command("SET", key, records[kind])
            try:
                yield name, kind
            finally:
                broker.command("SET", key, original)


def nothing_launched(external, client):
    assert external.pod is None and external.objects == {}
    assert not client._owned


def state(server, broker, external):
    """Every store a refused request could change: the issuer's registry, the broker
    (key, type and value) and the fake Kubernetes and runner calls."""
    registry = list(server._registry._db.iterdump())
    keys = sorted(broker.command("KEYS", "*"))
    values = {key: (broker.command("TYPE", key), broker.command("DUMP", key)) for key in keys}
    return registry, values, dict(external.objects), list(external.events)


async def post(client, path, body, headers=None):
    response = await client._transport.request("POST", path, body, headers or {}, 5)
    try:
        return response.status, await response.read()
    finally:
        response.close()


async def observe(client, launch_id, headers):
    response = await client._transport.request("GET", "/v1/launches/" + launch_id, None, headers, 5)
    try:
        assert response.status == 200
        return json.loads(await response.read())
    finally:
        response.close()


def code(value):
    return canonical_json({"schema_version": 1, "code": value})


def test_substrate_claim_refuses_every_commissioning_substitution(owned_worker, tmp_path):
    """No retained record substitution yields a sandbox handle or launch.

    Each refused claim leaves the registry, the broker and the fake Kubernetes
    and runner calls unchanged. The same claim succeeds and retires once every
    record is restored. @spec PROTECTED-COMMISSIONING-4.
    """

    async def run():
        async with actual_pair(owned_worker, tmp_path) as (
            server,
            client,
            external,
            delivery,
            lease,
            request,
        ):
            substrate = ProtectedSandboxSubstrate(client)
            thread_key = _thread_key_for(delivery.turn)
            broker = owned_worker.broker
            for name, kind in substitutions(broker, client._manifest, delivery):
                before = state(server, broker, external)
                with pytest.raises(LaunchClientError):
                    await substrate.claim_async(thread_key, remaining_s=10, delivery=delivery)
                assert substrate.lookup(thread_key) is None, (name, kind)
                assert state(server, broker, external) == before, (name, kind)
                nothing_launched(external, client)
            handle = await substrate.claim_async(thread_key, remaining_s=10, delivery=delivery)
            assert substrate.lookup(thread_key) is handle
            assert type(await substrate.retire_delivery(delivery)) is OwnedRetirementProof

    asyncio.run(run())


def test_launch_server_refuses_commissioning_records_before_any_effect(owned_worker, tmp_path):
    """The issuer refuses every commissioning record before it creates or starts work.

    A commissioning record as a create body, and a valid production create while
    a record the issuer reads is a commissioning record, refuse with a fixed code.
    The intact request then gets 202 on the same path. Against that live launch,
    a commissioning record as an admit, start, interrupt, timeout or retire body
    refuses after authorization and before any operation. Each refusal leaves the
    registry, the broker and the fake Kubernetes and runner calls unchanged.
    @spec PROTECTED-COMMISSIONING-4.
    """

    async def run():
        async with actual_pair(owned_worker, tmp_path) as (
            server,
            client,
            external,
            delivery,
            lease,
            request,
        ):
            broker = owned_worker.broker
            records = vectors.commissioning_records(client._manifest.as_dict())
            for kind in KINDS:
                before = state(server, broker, external)
                # 400 invalid, not 403: the pinned worker principal was authenticated.
                assert await post(client, "/v1/launches", records[kind]) == (400, code("invalid"))
                assert state(server, broker, external) == before, kind
            nothing_launched(external, client)
            observed = {}
            for name, kind in substitutions(broker, client._manifest, delivery):
                body = canonical_json({**request, "request_id": str(uuid.uuid4())})
                before = state(server, broker, external)
                observed[name, kind] = await post(client, "/v1/launches", body)
                assert state(server, broker, external) == before, (name, kind)
            assert observed == {key: (503, code("unavailable")) for key in observed}
            nothing_launched(external, client)
            refused = state(server, broker, external)
            status, raw = await post(
                client, "/v1/launches", canonical_json({**request, "request_id": str(uuid.uuid4())})
            )
            assert status == 202, raw
            created = json.loads(raw)
            launch_id = created["launch_id"]
            headers = {CAPABILITY_HEADER: created["control_capability"]}
            for _ in range(200):
                if (await observe(client, launch_id, headers))["state"] == "idle":
                    break
                await asyncio.sleep(0.05)
            else:
                raise AssertionError("the intact launch never became idle")
            # The same snapshot sees the accepted create: the registry and fake cluster moved.
            accepted = state(server, broker, external)
            assert accepted[0] != refused[0] and accepted[2] != refused[2]
            path = "/v1/launches/" + launch_id + "/"
            for kind in KINDS:
                for operation in ("admit", "start", "interrupt", "timeout", "retire"):
                    before = state(server, broker, external)
                    assert await post(client, path + operation, records[kind], headers) == (
                        400,
                        code("invalid"),
                    ), (kind, operation)
                    assert state(server, broker, external) == before, (kind, operation)
            assert (await observe(client, launch_id, headers))["state"] == "idle"
            retire = canonical_json({"schema_version": 1, "operation_id": str(uuid.uuid4())})
            assert (await post(client, path + "retire", retire, headers))[0] in {200, 202}

    asyncio.run(run())


def test_verification_refuses_commissioning_stream_fields(owned_worker, tmp_path):
    """A stream entry carrying a commissioning record never becomes dispatchable.

    @spec PROTECTED-COMMISSIONING-4.
    """

    async def run():
        async with actual_pair(owned_worker, tmp_path) as (
            server,
            client,
            external,
            delivery,
            lease,
            request,
        ):
            fields = dict(request["source_fields"])
            assert (
                type(
                    await verify_delivery(client._broker, fields, trusted_manifest=client._manifest)
                )
                is Dispatchable
            )
            records = vectors.commissioning_records(client._manifest.as_dict())
            before = state(server, owned_worker.broker, external)
            for kind in KINDS:
                for field in ("payload", "protected_envelope"):
                    verdict = await verify_delivery(
                        client._broker,
                        {**fields, field: records[kind].decode()},
                        trusted_manifest=client._manifest,
                    )
                    assert type(verdict) is not Dispatchable, (kind, field)
            assert state(server, owned_worker.broker, external) == before
            nothing_launched(external, client)

    asyncio.run(run())


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("field", ["payload", "protected_envelope", "only"])
def test_protected_consumer_never_claims_a_commissioning_stream_entry(owned_worker, field, kind):
    """The parking-only consumer reads, verifies and refuses before any claim.

    The admitted entry is replaced by one whose payload or protected envelope is
    a commissioning record, or whose only field is one. Reading it puts it in
    this consumer's pending list, as reading any entry does. With an envelope it
    then stays pending with no lease, retry charge, dead letter or kernel call.
    Without one it is immutable ordinary-row poison, which the consumer settles
    to its dead letter under its own lease without executing it.
    @spec PROTECTED-COMMISSIONING-4 @spec PROTECTED-HOOK-LANE-6/7.
    """
    from .test_protected_consumer import assembled, dispatch, no_execution, proof, read_pending

    async def go():
        broker = owned_worker.broker
        _, original, _ = proof.admitted(broker)
        raw = vectors.commissioning_records(broker.manifest().as_dict())[kind].decode()
        fields = {"payload": raw} if field == "only" else {**original, field: raw}
        broker.command("XTRIM", proof.wb.STREAM, "MAXLEN", "=", 0)
        broker.command("XADD", proof.wb.STREAM, "*", *sum(([k, v] for k, v in fields.items()), []))
        async with assembled(owned_worker) as h:
            entry_id, actual = await read_pending(h)
            assert actual == fields
            runs = h.client.lane().runs
            await dispatch(h, entry_id, fields)
            pending = await runs.xpending_range(
                h.config.stream, h.config.consumer_group, "-", "+", 10
            )
            dead = await runs.xrange(h.config.dead_letter_stream_name())
            if field == "only":
                assert pending == []
                assert len(dead) == 1 and dead[0][1]["payload"] == raw
                assert dead[0][1]["dl_reason"] == "protected-proof-refused"
            else:
                assert [(row["message_id"], row["times_delivered"]) for row in pending] == [
                    (entry_id, 1)
                ]
                assert dead == []
            assert not await h.consumer._leases.has_state(
                h.config.stream, h.config.consumer_group, entry_id
            )
            no_execution(h)

    asyncio.run(go())


class EngineReached(Exception):
    """Composition passed every record read; raised instead of opening a database."""


def test_worker_composition_refuses_commissioning_files_and_records(
    owned_worker, tmp_path, monkeypatch
):
    """``open_runtime`` refuses before it opens a database, launch client or runtime.

    The unmodified directory reaches the database step, so only the substituted
    file or retained record decides each refusal. @spec PROTECTED-COMMISSIONING-4.
    """
    from curie_worker import protected_entry

    def no_engine(*_args, **_kwargs):
        raise EngineReached()

    monkeypatch.setattr(protected_entry, "create_async_engine", no_engine)

    async def run():
        async with actual_pair(owned_worker, tmp_path, intake=True) as (
            server,
            client,
            external,
            delivery,
            unused,
            request,
        ):
            # The provisioner directory already holds the pair's measurements and projections.
            directory = tmp_path
            credential = owned_worker.credential()
            files = {
                "manifest.json": client._manifest.canonical_bytes,
                "execution_config.json": client._execution.canonical_bytes,
                "worker.json": canonical(
                    {
                        "schema_version": 1,
                        "credential_ref": client._manifest.as_dict()["credential_refs"]["worker"],
                        "worker": {
                            "username": credential.username,
                            "password": credential.password,
                        },
                    }
                ),
                "ca.pem": owned_worker.broker.ca_pem.encode("ascii"),
                "process.json": canonical(
                    {
                        "schema_version": 1,
                        "database_url": "postgresql+asyncpg://synthetic@127.0.0.1:1/synthetic",
                        "db_schema": "synthetic",
                        "api_base_url": "https://api.example.com",
                        "internal_worker_token": "synthetic-worker-token",
                        "adapter_credentials": {},
                        "slack": {
                            "token": "synthetic-slack-token",
                            "base_url": "",
                            "identity_tokens": {},
                            "trusted_origins": [],
                        },
                        "launch_tls": dataclasses.asdict(client._tls),
                    }
                ),
            }
            for name, raw in files.items():
                (directory / name).write_bytes(raw)

            async def open_once():
                async with protected_entry.open_runtime(str(directory)):
                    raise AssertionError("a runtime opened")

            with pytest.raises(EngineReached):
                await open_once()
            before = state(server, owned_worker.broker, external)
            records = vectors.commissioning_records(client._manifest.as_dict())
            for name in ("manifest.json", "execution_config.json", "worker.json"):
                for kind in KINDS:
                    (directory / name).write_bytes(records[kind])
                    with pytest.raises(protected_entry.WorkerBootstrapInvalid):
                        await open_once()
                (directory / name).write_bytes(files[name])
            for name, kind in substitutions(owned_worker.broker, client._manifest, delivery):
                if name == "binding":
                    continue
                with pytest.raises(Exception) as caught:
                    await open_once()
                assert type(caught.value) is not EngineReached, (name, kind)
            assert state(server, owned_worker.broker, external) == before
            with pytest.raises(EngineReached):
                await open_once()
            nothing_launched(external, client)

    asyncio.run(run())
