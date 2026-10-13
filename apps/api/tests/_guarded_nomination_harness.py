"""Single producer/consumer source fixture; only Kube/runner/reply peers synthetic."""

from __future__ import annotations

import asyncio
import base64
from contextlib import asynccontextmanager

from aci_protocol import Final, TextDelta
from aiohttp import web
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from curie_protected_hooks.artifact_measurement import ArtifactMeasurementDirectory
from curie_protected_hooks.execution_namespace import derive_lane_namespace
from curie_protected_hooks.worker_transport import AuthenticatedWorkerClient
from curie_protected_hooks.worker_verification import verification_acl_rules
from curie_worker.protected_configuration import parse_execution_config
from curie_worker.protected_kubernetes_projection import ProjectionDirectory
from curie_worker.protected_launch_client import LaunchTLSConfig, ProtectedLaunchClient
from curie_worker.protected_launch_kubernetes import KubernetesLaunchAPI, KubernetesTLS
from curie_worker.protected_launch_server import (
    ProtectedLaunchServer,
    ProvisionedRuntime,
    ProvisionedWorker,
    ServerConfig,
    public_key_identity,
)
from curie_worker.protected_nomination import HttpNominationSubmitter
from curie_worker.protected_run import ProtectedRuntime

from apps.worker.tests.protected_configuration_vectors import canonical
from apps.worker.tests.protected_launch_endpoint_helpers import certificates, endpoint
from apps.worker.tests.test_protected_launch_server_integration import inputs
from apps.worker.tests.test_protected_retirement_server import ExternalProofWorkload, wb


class ScopedCredentials:
    def __init__(self, agent):
        self.agent = agent

    def model_credential(self, reference):
        assert reference == {"id": "credential/example-model", "generation": "1"}
        return "SYNTHETIC-MODEL-CREDENTIAL"

    def history_token(self, reference, *, agent_id, transcript_binding, expires_at):
        assert agent_id == self.agent and transcript_binding.startswith("@protected:")
        return "SYNTHETIC-HISTORY-CREDENTIAL"


class NominationWorkload(ExternalProofWorkload):
    def __init__(self, output):
        super().__init__()
        self.output = output

    async def runner(self, request):
        if request.path != "/v1/event":
            return await super().runner(request)
        secret = next(value for (kind, _), value in self.objects.items() if kind == "secrets")
        token = base64.b64decode(secret["data"]["runner-token"]).decode()
        assert request.headers.get("Authorization") == "Bearer " + token
        assert "X-Curie-Worker-Token" not in request.headers
        assert self.epoch is None
        self.events.append(("RUNNER_POST", request.path))
        self.start_entered.set()
        await self.start_release.wait()
        self.epoch, self.decision = "a" * 43, "pending"
        response = web.StreamResponse(
            headers={"Content-Type": "application/x-ndjson", "X-Curie-Turn-Epoch": self.epoch}
        )
        await response.prepare(request)
        await self.finish.wait()
        for frame in (
            TextDelta(text=self.output[:25]),
            TextDelta(text=self.output[25:]),
            Final(text=self.output),
        ):
            await response.write((frame.model_dump_json() + "\n").encode())
        await response.write_eof()
        return response


@asynccontextmanager
async def guarded_pair(owned, root, profile, engine, router, api_origin, worker_token, output):
    """No admission/group/read/lease/manual context is installed by this helper."""
    owned.rules = verification_acl_rules("worker")
    owned.install()
    tls = certificates(root / "tls")
    registry = root / "registry"
    registry.mkdir(mode=0o700)
    manifest, profile, request, qualification, measurements, projections = inputs(
        owned.broker, root, profile_override=profile, admit=False
    )
    assert request is None
    ml, pl = ArtifactMeasurementDirectory(str(measurements)), ProjectionDirectory(str(projections))
    broker = None
    client = None
    server_owner = None
    runtime = None
    consumer_task = None
    external = NominationWorkload(output)
    runner_app = web.Application()
    runner_app.router.add_route("*", "/{tail:.*}", external.runner)
    runner_owner = web.AppRunner(runner_app, access_log=None)
    await runner_owner.setup()
    try:
        broker = await AuthenticatedWorkerClient.connect(
            manifest, owned.credential(), owned.broker.ca_pem
        )
        await web.TCPSite(runner_owner, "127.0.0.1", 8080).start()
        async with endpoint(tls, external.kubernetes) as kube_origin:
            execution = parse_execution_config(canonical(profile), manifest=manifest)

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
                runtimes=(ProvisionedRuntime(manifest, broker, execution, ml, pl),),
                workers=(worker,),
                credentials=ScopedCredentials(profile["agent_id"]),
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
                    "worker/example",
                ),
                broker=broker,
                manifest=manifest,
                qualification=qualification,
                execution=execution,
                measurement=measurement,
                projection=projection,
            )
            ns = derive_lane_namespace(manifest)
            cfg = wb.lane_config(
                owned.broker,
                key_prefix=ns.prefix,
                consumer_group=ns.group,
                dead_letter_stream=ns.dead,
                max_attempts=1,
                read_block_ms=1000,
                delivery_lease_ttl_s=15,
                delivery_lease_heartbeat_s=5,
            )
            runtime = ProtectedRuntime(
                broker=broker,
                manifest=manifest,
                engine=engine,
                db_schema="curie",
                execution=execution,
                launch_client=client,
                config=cfg,
                reply_sink=router,
                nomination_submitter=HttpNominationSubmitter(api_origin, worker_token),
            )

            def start_consumer():
                nonlocal consumer_task
                assert consumer_task is None, "one actual consumer run per fixture"
                consumer_task = asyncio.create_task(runtime.run())
                return consumer_task

            yield server, client, external, runtime, start_consumer
    finally:
        if consumer_task is not None:
            assert runtime is not None
            runtime.request_stop()
            consumer_task.cancel()
            await asyncio.gather(consumer_task, return_exceptions=True)
            assert consumer_task.done()
        if runtime is not None:
            await runtime.close()
        if client is not None:
            await client.close()
        if server_owner is not None:
            await server_owner.cleanup()
        await runner_owner.cleanup()
        ml.close()
        pl.close()
        if broker is not None:
            await broker.close()
