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
import uuid

import pytest
from curie_protected_hooks.worker_verification import Dispatchable, verify_delivery
from curie_worker.kernel.routing import _thread_key_for
from curie_worker.protected_launch_client import LaunchClientError
from curie_worker.protected_launch_records import canonical_json
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


def test_substrate_claim_refuses_every_commissioning_substitution(owned_worker, tmp_path):
    """No retained record substitution yields a sandbox handle or launch.

    The same claim succeeds and retires once every record is restored.
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
            substrate = ProtectedSandboxSubstrate(client)
            thread_key = _thread_key_for(delivery.turn)
            for name, kind in substitutions(owned_worker.broker, client._manifest, delivery):
                with pytest.raises(LaunchClientError):
                    await substrate.claim_async(thread_key, remaining_s=10, delivery=delivery)
                assert substrate.lookup(thread_key) is None, (name, kind)
                nothing_launched(external, client)
            handle = await substrate.claim_async(thread_key, remaining_s=10, delivery=delivery)
            assert substrate.lookup(thread_key) is handle
            assert type(await substrate.retire_delivery(delivery)) is OwnedRetirementProof

    asyncio.run(run())


def test_launch_server_refuses_commissioning_request_bodies(owned_worker, tmp_path):
    """A commissioning record is never a launch or admit request.

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
            records = vectors.commissioning_records(client._manifest.as_dict())
            paths = ("/v1/launches", "/v1/launches/" + str(uuid.uuid4()) + "/admit")
            for kind in KINDS:
                for path in paths:
                    response = await client._transport.request("POST", path, records[kind], {}, 5)
                    try:
                        # 400 invalid, not 403: the pinned worker principal was authenticated.
                        assert response.status == 400, (kind, path)
                        assert await response.read() == canonical_json(
                            {"schema_version": 1, "code": "invalid"}
                        )
                    finally:
                        response.close()
                    nothing_launched(external, client)

    asyncio.run(run())


def test_launch_server_refuses_production_requests_over_commissioning_records(
    owned_worker, tmp_path
):
    """The issuer's own verification refuses a valid request when a record it reads is
    a commissioning record, @spec PROTECTED-COMMISSIONING-4."""

    async def run():
        async with actual_pair(owned_worker, tmp_path) as (
            server,
            client,
            external,
            delivery,
            lease,
            request,
        ):
            for name, kind in substitutions(owned_worker.broker, client._manifest, delivery):
                body = canonical_json({**request, "request_id": str(uuid.uuid4())})
                response = await client._transport.request("POST", "/v1/launches", body, {}, 5)
                try:
                    assert response.status not in {200, 201, 202}, (name, kind)
                finally:
                    response.close()
                nothing_launched(external, client)
            # The restored records launch through the same issuer path, then retire.
            substrate = ProtectedSandboxSubstrate(client)
            await substrate.claim_async(
                _thread_key_for(delivery.turn), remaining_s=10, delivery=delivery
            )
            assert type(await substrate.retire_delivery(delivery)) is OwnedRetirementProof

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
            for kind in KINDS:
                for field in ("payload", "protected_envelope"):
                    verdict = await verify_delivery(
                        client._broker,
                        {**fields, field: records[kind].decode()},
                        trusted_manifest=client._manifest,
                    )
                    assert type(verdict) is not Dispatchable, (kind, field)
            nothing_launched(external, client)

    asyncio.run(run())


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
            with pytest.raises(EngineReached):
                await open_once()
            nothing_launched(external, client)

    asyncio.run(run())
