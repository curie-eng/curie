"""Actual issuer/client retirement evidence; external Kubernetes/runner alone are synthetic."""

from __future__ import annotations

import asyncio
import hashlib
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from aiohttp import web
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from curie_protected_hooks.artifact_measurement import ArtifactMeasurementDirectory
from curie_protected_hooks.worker_transport import AuthenticatedWorkerClient
from curie_protected_hooks.worker_verification import (
    Dispatchable,
    verification_acl_rules,
    verify_delivery,
)
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.kernel.routing import _thread_key_for
from curie_worker.protected_binding import protected_delivery_scope
from curie_worker.protected_configuration import parse_execution_config
from curie_worker.protected_execution import protected_execution_scope
from curie_worker.protected_kubernetes_projection import ProjectionDirectory
from curie_worker.protected_launch_client import LaunchTLSConfig, ProtectedLaunchClient
from curie_worker.protected_launch_kubernetes import KubernetesLaunchAPI, KubernetesTLS
from curie_worker.protected_launch_records import LaunchRequest, canonical_json
from curie_worker.protected_launch_server import (
    ProtectedLaunchServer,
    ProvisionedRuntime,
    ProvisionedWorker,
    ServerConfig,
    public_key_identity,
)
from curie_worker.protected_retirement import OwnedPendingCleanupProof, OwnedRetirementProof
from curie_worker.protected_substrate import ProtectedSandboxSubstrate

from .protected_configuration_vectors import canonical
from .protected_launch_endpoint_helpers import certificates, endpoint
from .test_protected_launch_server_integration import (
    ExternalWorkload,
    ScopedFixtureCredentials,
    inputs,
    wb,
)

pworker_service = wb.pworker_service
owned_worker = wb.owned_worker


class ExternalProofWorkload(ExternalWorkload):
    async def kubernetes(self, request):
        response = await super().kubernetes(request)
        if request.method == "POST" and request.path.endswith("/pods"):
            self.launch_id = self.pod["metadata"]["labels"]["curietech.ai/protected-launch"]
        return response


@asynccontextmanager
async def actual_pair(owned, root, *, intake=False):
    owned.rules = verification_acl_rules("worker")
    owned.install()
    tls = certificates(root / "tls")
    registry = root / "registry"
    registry.mkdir(mode=0o700)
    manifest, profile, request, qualification, measurements, projections = inputs(
        owned.broker, root
    )
    broker = await AuthenticatedWorkerClient.connect(
        manifest, owned.credential(), owned.broker.ca_pem
    )
    ml, pl = ArtifactMeasurementDirectory(str(measurements)), ProjectionDirectory(str(projections))
    external = ExternalProofWorkload()
    runner_app = web.Application()
    runner_app.router.add_route("*", "/{tail:.*}", external.runner)
    runner_owner = web.AppRunner(runner_app, access_log=None)
    await runner_owner.setup()
    server_owner = None
    client = None
    try:
        await web.TCPSite(runner_owner, "127.0.0.1", 8080).start()
        async with endpoint(tls, external.kubernetes) as kube_origin:
            execution = parse_execution_config(canonical(profile), manifest=manifest)
            runtime = ProvisionedRuntime(manifest, broker, execution, ml, pl)

            def pin(role):
                cert = x509.load_pem_x509_certificate((tls / (role + ".pem")).read_bytes())
                return public_key_identity(cert.public_bytes(serialization.Encoding.DER))

            worker = ProvisionedWorker(
                "worker/example",
                pin("worker"),
                manifest.as_dict()["runtime_id"],
                manifest.as_dict()["substrate"]["authority_domain_id"],
                frozenset(
                    {
                        "create",
                        "observe",
                        "start",
                        "admit",
                        "deny",
                        "status",
                        "capacity-status",
                        "interrupt",
                        "timeout",
                        "retire",
                    }
                ),
            )
            server = ProtectedLaunchServer(
                ServerConfig(
                    str(registry),
                    "127.0.0.1",
                    8081,
                    str(tls / "ca.pem"),
                    str(tls / "server.pem"),
                    str(tls / "server.key"),
                    pin("server"),
                    "http",
                    "svc.cluster.local",
                    None,
                ),
                runtimes=(runtime,),
                workers=(worker,),
                credentials=ScopedFixtureCredentials(),
                kubernetes=KubernetesLaunchAPI(
                    KubernetesTLS(
                        kube_origin,
                        str(tls / "ca.pem"),
                        str(tls / "worker.pem"),
                        str(tls / "worker.key"),
                        "synthetic-kubernetes",
                    ),
                    authority_domain_id=worker.authority_domain_id,
                ),
            )
            external.server = server
            external.pause_create_response = False
            external.create_response_ready = asyncio.Event()
            external.release_create_response = asyncio.Event()

            @web.middleware
            async def schedule_create_response(request, handler):
                response = await handler(request)
                if (
                    request.method == "POST"
                    and request.path == "/v1/launches"
                    and external.pause_create_response
                ):
                    external.create_response_ready.set()
                    await external.release_create_response.wait()
                return response

            server.application.middlewares.append(schedule_create_response)
            server_owner = web.AppRunner(
                server.application, access_log=None, handler_cancellation=True
            )
            await server_owner.setup()
            site = web.TCPSite(server_owner, "127.0.0.1", 0, ssl_context=server._tls)
            await site.start()
            origin = "https://127.0.0.1:" + str(site._server.sockets[0].getsockname()[1])
            measurement = ml.load(manifest=manifest, qualification=qualification)
            projection = pl.load(manifest=manifest, measurement=measurement, execution=execution)
            client = ProtectedLaunchClient(
                LaunchTLSConfig(
                    origin,
                    str(tls / "ca.pem"),
                    "127.0.0.1",
                    pin("server"),
                    str(tls / "worker.pem"),
                    str(tls / "worker.key"),
                    worker.principal_id,
                ),
                broker=broker,
                manifest=manifest,
                qualification=qualification,
                execution=execution,
                measurement=measurement,
                projection=projection,
            )
            delivery = await verify_delivery(
                broker, request["source_fields"], trusted_manifest=manifest
            )
            assert type(delivery) is Dispatchable
            generation = hashlib.sha256(
                canonical(
                    [manifest.as_dict()["runtime_id"], manifest.as_dict()["runtime_generation"]]
                )
            ).hexdigest()
            cfg = wb.lane_config(
                owned.broker,
                key_prefix="protected:lane:worker:" + generation,
                consumer_group="protected-" + generation,
                delivery_lease_ttl_s=15,
                delivery_lease_heartbeat_s=5,
            )
            runs = broker.lane().runs
            if intake:
                # The actual consumer owns group creation, first read, lease
                # acquisition and context installation in intake verification.
                yield server, client, external, delivery, None, request
                return
            await runs.xgroup_create(cfg.stream, cfg.consumer_group, id="0")
            rows = await runs.xreadgroup(
                cfg.consumer_group, cfg.consumer_name, {cfg.stream: ">"}, count=1
            )
            lease = await DeliveryLeaseStore(runs, cfg).acquire(
                cfg.stream, cfg.consumer_group, rows[0][1][0][0], consumer=cfg.consumer_name
            )
            assert lease is not None
            async with protected_delivery_scope(delivery):
                async with protected_execution_scope(
                    broker, trusted_manifest=manifest, config=cfg, lease=lease
                ):
                    yield server, client, external, delivery, lease, request
    finally:
        if client is not None:
            await client.close()
        if server_owner is not None:
            await server_owner.cleanup()
        await runner_owner.cleanup()
        ml.close()
        pl.close()
        await broker.close()


@pytest.mark.parametrize(
    "mode", ["retired", "incomplete", "missing-receipt", "sibling", "pending-claim"]
)
def test_actual_issuer_retirement_proof_ownership_and_refusals(owned_worker, tmp_path, mode):
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
            if mode == "missing-receipt":
                external.fault_mode = "delayed-ready"
                claim = asyncio.create_task(
                    client.create(
                        LaunchRequest(canonical_json(request)), delivery=delivery, remaining_s=10
                    )
                )
                try:
                    for _ in range(60):
                        if client._owned and external.pod is not None:
                            owned = next(iter(client._owned.values()))
                            if (
                                server._registry.get(
                                    "worker/example", owned.capability.launch_id
                                ).objects.pod_uid
                                is not None
                            ):
                                break
                        await asyncio.sleep(0.05)
                    assert client._owned and external.pod is not None
                    owned = next(iter(client._owned.values()))
                    assert owned.observation.response.as_dict()["receipt"] is None
                    claim.cancel()
                    result = await asyncio.gather(claim, return_exceptions=True)
                    assert isinstance(result[0], asyncio.CancelledError)
                    proof = await client.retire_verified(owned.capability)
                    assert type(proof) is OwnedPendingCleanupProof
                    assert client.is_owned_retirement(proof, delivery=delivery)
                    assert (
                        server._registry.get("worker/example", owned.capability.launch_id).state
                        == "retired"
                    )
                    assert client.owned_retirement_for_delivery(delivery) is proof
                finally:
                    claim.cancel()
                    await asyncio.gather(claim, return_exceptions=True)
                return
            handle = await substrate.claim_async(
                _thread_key_for(delivery.turn), remaining_s=10, delivery=delivery
            )
            receipt = client._target(
                substrate.capability_for(handle)
            ).observation.response.as_dict()["receipt"]
            assert receipt is not None
            if mode == "incomplete":
                external.fault_mode = "no-termination"
                proof = await substrate.retire_delivery(delivery)
                assert proof is None
                assert client.owned_retirement_for_delivery(delivery) is None
                row = server._registry.get("worker/example", handle.claim_name)
                assert row.state == "retiring" and row.termination is None
                return
            # Losing current positive authority must not disable the owned stop path.
            if mode == "retired":
                lease.lost.set()
                owned_worker.broker.command(
                    "DEL", "protected:admission:binding:" + delivery.turn.event_id
                )
            proof = await substrate.retire_delivery(delivery)
            assert type(proof) is OwnedRetirementProof
            assert (
                proof.as_dict()["issued_receipt_sha256"]
                == hashlib.sha256(canonical_json(receipt)).hexdigest()
            )
            assert server._registry.get("worker/example", handle.claim_name).state == "retired"
            assert client.is_owned_retirement(proof, delivery=delivery)
            assert not client.is_owned_retirement(
                OwnedRetirementProof(proof.raw), delivery=delivery
            )
            copied_delivery = Dispatchable(
                delivery.turn, delivery.envelope, delivery.intent, delivery.source_fields
            )
            assert not client.is_owned_retirement(proof, delivery=copied_delivery)
            assert await substrate.retire_delivery(delivery) is proof  # actual handle removed
            if mode == "pending-claim":
                external.fault_mode = "delayed-ready"
                external.pause_create_response = True
                claim = asyncio.create_task(
                    substrate.claim_async(
                        _thread_key_for(delivery.turn), remaining_s=10, delivery=delivery
                    )
                )
                try:
                    await asyncio.wait_for(external.create_response_ready.wait(), 3)
                    assert _thread_key_for(delivery.turn) in substrate._claims
                    assert len(client._owned) == 1
                    # The issuer has accepted a subsequent launch, but its response
                    # has not yet registered in the client's private launch map.
                    assert client.owned_retirement_for_delivery(delivery) is None
                    assert not client.is_owned_retirement(proof, delivery=delivery)
                    assert await substrate.retire_delivery(delivery) is None
                    claim.cancel()
                    result = await asyncio.gather(claim, return_exceptions=True)
                    assert isinstance(result[0], asyncio.CancelledError)
                    assert _thread_key_for(delivery.turn) not in substrate._claims
                    assert len(client._owned) == 1
                    # Unknown acceptance survives cancellation; absence of a
                    # registered response cannot restore the earlier proof.
                    assert client.owned_retirement_for_delivery(delivery) is None
                    assert not client.is_owned_retirement(proof, delivery=delivery)
                    assert await substrate.retire_delivery(delivery) is None
                finally:
                    external.release_create_response.set()
                    claim.cancel()
                    await asyncio.gather(claim, return_exceptions=True)
                return
            if mode == "sibling":
                external.fault_mode = "delayed-ready"
                second = dict(request, request_id=str(uuid4()))
                claim = asyncio.create_task(
                    client.create(
                        LaunchRequest(canonical_json(second)), delivery=delivery, remaining_s=10
                    )
                )
                try:
                    for _ in range(60):
                        if len(client._owned) == 2:
                            break
                        await asyncio.sleep(0.05)
                    assert len(client._owned) == 2
                    assert client.owned_retirement_for_delivery(delivery) is None
                    assert await substrate.retire_delivery(delivery) is None
                    # Completion validation must refuse an older individual proof
                    # while a subsequent owned launch has unconfirmed cleanup.
                    assert not client.is_owned_retirement(proof, delivery=delivery)
                finally:
                    claim.cancel()
                    await asyncio.gather(claim, return_exceptions=True)

    asyncio.run(run())
