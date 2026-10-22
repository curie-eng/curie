"""Commissioning authorizer and mediator over the shared launch mechanics.

@spec PROTECTED-COMMISSIONING-8. Actual mechanics, registry, HTTPS mTLS and
Kubernetes client; only the external Kubernetes API, the runner and the
campaign broker control are synthetic. The broker control is proved against a
real Valkey in protected-hooks; Kubernetes create, ambiguous create and NotFound
are additionally observed on a disposable cluster (see the PR evidence).
"""

from __future__ import annotations

import ast
import asyncio
import hashlib
import inspect
import json
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest
from aiohttp import web
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from curie_worker import commissioning_launch as launch
from curie_worker.commissioning_registry import GuardCreateWitness, object_names
from curie_worker.protected_kubernetes_projection import parse_projection
from curie_worker.protected_launch_kubernetes import (
    KubernetesLaunchAPI,
    KubernetesTLS,
    LaunchControlUnavailable,
)
from curie_worker.protected_launch_mechanics import LaunchAuthorizer, ProtectedLaunchMechanics
from curie_worker.protected_launch_records import canonical_json
from curie_worker.protected_launch_server import ServerConfig, public_key_identity

from .protected_launch_endpoint_helpers import certificates, context, endpoint
from .test_protected_kubernetes_projection import projection_value
from .test_protected_launch_server_integration import ExternalWorkload

OWNER = "worker/example"
GUARD = {
    "control_id": "22222222-2222-4222-8222-222222222222",
    "revision": "1",
    "config_sha256": "c" * 64,
}
PERMIT = "a" * 64
ROUTE = "/v1/commissioning/launches"


@dataclass(frozen=True)
class Plan:
    launch_id: str
    launch_request_digest: str
    projection_digest: str
    launch_reservation_id: str


class FakeControl:
    """Synthetic campaign control; records ownership in the external event order."""

    permit_digest = PERMIT
    worker_principal_id = OWNER
    worker_credential = {"id": OWNER, "generation": "1"}
    entry_point = "commissioning-worker-v1"
    guard_identity = GUARD

    def __init__(self, events: list[Any], expires_at_ms: int) -> None:
        self.events, self.expires_at_ms = events, expires_at_ms
        self.ownership: dict[str, tuple[str, str | None]] = {}
        self.charged: dict[str, str] = {}
        self.plans = 0
        self.owned = asyncio.Event()
        self.release = asyncio.Event()
        self.release.set()

    async def plan(self, raw: bytes) -> Plan:
        self.plans += 1
        value = json.loads(raw)
        return Plan(
            value["launch_id"],
            hashlib.sha256(raw).hexdigest(),
            value["projection_digest"],
            value["launch_reservation_id"],
        )

    async def reserve(self, plan: Plan) -> bool:
        if plan.launch_reservation_id not in self.charged:
            self.events.append(("RESERVE", plan.launch_reservation_id))
            self.charged[plan.launch_reservation_id] = "reserved"
        return self.charged[plan.launch_reservation_id] == "reserved"

    async def own(self, plan: Plan, *, namespace: str, pod_name: str) -> None:
        assert plan.launch_id not in self.ownership
        assert self.charged.get(plan.launch_reservation_id) == "reserved"
        self.charged[plan.launch_reservation_id] = "dispatched"
        self.events.append(("OWN", namespace, pod_name))
        self.ownership[plan.launch_id] = ("creating", None)
        self.owned.set()
        await self.release.wait()

    async def advance(self, launch_id: str, state: str, pod_uid: str | None) -> None:
        current, uid = self.ownership[launch_id]
        if (current, uid) == (state, pod_uid):
            return  # a repeat of the current state is inert, as on the broker
        assert current != "retired" and (uid is None or uid == pod_uid)
        self.events.append(("ADVANCE", state, pod_uid))
        self.ownership[launch_id] = (state, pod_uid)

    async def unsettled(self) -> tuple[str, ...]:
        return tuple(k for k, (s, _) in self.ownership.items() if s != "retired")


class Identity:
    """Stand-in for the provisioner-owned binding of a client key to B's candidate."""

    def __init__(self, root: Path, *, accept: bool = True) -> None:
        self.pin, self.accept, self.calls = pin(root, "worker"), accept, []

    def authenticates(self, certificate: bytes, *, credential: Any, entry_point: str) -> bool:
        self.calls.append((credential, entry_point))
        return self.accept and public_key_identity(certificate) == self.pin


class Assignments:
    async def assign(self, plan: Plan, *, expires_at_ms: int) -> launch.CampaignRunnerAssignment:
        return launch.CampaignRunnerAssignment(
            "commissioning-" + plan.launch_id,
            "https://api.example.test/history/" + plan.launch_id,
            "synthetic-history-credential",
            "synthetic-gateway-credential",
            ("credential/example-history", "1"),
            ("credential/example-model", "1"),
        )


class Witness:
    """A stand-in for the guard-owned witness of what it admitted per create."""

    def __init__(self, uid: Any, *, operation: str | None = None) -> None:
        self.uid, self.operation, self.calls, self.returned = uid, operation, [], []

    async def admitted(self, **binding: str) -> GuardCreateWitness | None:
        self.calls.append(binding)
        value = self.uid() if callable(self.uid) else self.uid
        self.returned.append(value)
        if value is None:
            return None
        return GuardCreateWitness(
            binding["kind"],
            value,
            self.operation or binding["create_operation_id"],
            GUARD["control_id"],
            GUARD["revision"],
            GUARD["config_sha256"],
        )


class Cluster(ExternalWorkload):
    """The synthetic external API, with a lost create response or a squatted name."""

    async def kubernetes(self, request: web.Request) -> web.StreamResponse:
        if request.method == "POST" and request.path.endswith("/pods"):
            if self.fault_mode == "ambiguous-create":
                await super().kubernetes(request)
                return web.Response(status=504)
            if self.fault_mode == "squatted":
                return web.json_response({"kind": "Status", "reason": "AlreadyExists"}, status=409)
        return await super().kubernetes(request)


def measured() -> launch.MeasuredLaunch:
    value = projection_value()
    platform = {
        **value["platform"],
        "oci_digest": value["image"]["digest"],
        "pinned_image_reference": value["image"]["reference"],
        "observed_image_id": value["image"]["reference"],
        "image_config_id": None,
    }
    return launch.MeasuredLaunch(parse_projection(canonical_json(value)), platform, 300)


def request_bytes(launch_id: str | None = None) -> bytes:
    return canonical_json(
        {
            "launch_id": launch_id or str(uuid4()),
            "launch_reservation_id": str(uuid4()),
            "projection_digest": measured().projection.digest,
        }
    )


def pin(root: Path, role: str) -> str:
    cert = x509.load_pem_x509_certificate((root / (role + ".pem")).read_bytes())
    return public_key_identity(cert.public_bytes(serialization.Encoding.DER))


@asynccontextmanager
async def mediator(tmp_path: Path, cluster: Cluster, **options: Any) -> Any:
    root = certificates(tmp_path / "tls")
    registry = tmp_path / "registry"
    registry.mkdir(mode=0o700, exist_ok=True)
    expires = options.pop("expires_at_ms", int(time.time() * 1000) + 2 * 3600 * 1000)
    control = options.pop("control", None) or FakeControl(cluster.events, expires)
    identity = options.pop("identity", "default")
    if identity == "default":
        identity = Identity(root)
    assignments = options.pop("assignments", None) or Assignments()
    runner_app = web.Application()
    runner_app.router.add_route("*", "/{tail:.*}", cluster.runner)
    runner_owner = web.AppRunner(runner_app, access_log=None)
    await runner_owner.setup()
    try:
        await web.TCPSite(runner_owner, "127.0.0.1", 8080).start()
        async with endpoint(root, cluster.kubernetes) as origin:
            server = launch.CommissioningLaunchServer(
                ServerConfig(
                    str(registry),
                    "127.0.0.1",
                    8081,
                    str(root / "ca.pem"),
                    str(root / "server.pem"),
                    str(root / "server.key"),
                    pin(root, "server"),
                    "http",
                    "svc.cluster.local",
                    None,
                ),
                control=control,
                identity=identity,
                measured=measured(),
                assignments=assignments,
                kubernetes=KubernetesLaunchAPI(
                    KubernetesTLS(
                        origin,
                        str(root / "ca.pem"),
                        str(root / "worker.pem"),
                        str(root / "worker.key"),
                        "synthetic-kubernetes",
                    ),
                    authority_domain_id="55555555-5555-4555-8555-555555555555",
                ),
                **options,
            )
            cluster.server = server
            owner = web.AppRunner(server.application, access_log=None, handler_cancellation=True)
            await owner.setup()
            site = web.TCPSite(owner, "127.0.0.1", 0, ssl_context=server._tls)
            await site.start()
            url = "https://127.0.0.1:" + str(site._server.sockets[0].getsockname()[1])
            try:
                yield server, control, url, root
            finally:
                await owner.cleanup()
    finally:
        await runner_owner.cleanup()


async def settled(server: Any, launch_id: str) -> None:
    live = server._live[launch_id]
    if live.job is not None:
        await asyncio.wait_for(asyncio.gather(live.job, return_exceptions=True), 30)


def test_the_commissioning_authorizer_is_admitted_by_exact_class(tmp_path):
    root = certificates(tmp_path / "tls")
    kubernetes = KubernetesLaunchAPI(
        KubernetesTLS(
            "https://127.0.0.1:6443",
            str(root / "ca.pem"),
            str(root / "worker.pem"),
            str(root / "worker.key"),
            "synthetic-control",
        ),
        authority_domain_id="55555555-5555-4555-8555-555555555555",
    )
    registry = tmp_path / "registry"
    registry.mkdir(mode=0o700)

    def mechanics(authorizer: Any) -> ProtectedLaunchMechanics:
        return ProtectedLaunchMechanics(
            authorizer,
            kubernetes=kubernetes,
            registry_root=str(registry),
            runner_scheme="http",
            runner_dns_suffix="svc.cluster.local",
            runner_ca_file=None,
        )

    accepted = mechanics(launch.CommissioningLaunchAuthorizer(PERMIT))
    assert type(accepted.registry).__name__ == "CommissioningLaunchRegistry"
    assert accepted.registry.permit_digest == PERMIT
    asyncio.run(accepted.close(None))  # type: ignore[arg-type]

    class Forged(LaunchAuthorizer):
        __module__ = "curie_worker.commissioning_launch"
        __qualname__ = "CommissioningLaunchAuthorizer"

    with pytest.raises(LaunchControlUnavailable):
        mechanics(Forged())
    with pytest.raises(TypeError):

        class _Derived(launch.CommissioningLaunchAuthorizer):  # type: ignore[misc]
            pass


def test_the_commissioning_authorizer_constructs_no_production_type():
    tree = ast.parse(inspect.getsource(launch))
    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef)
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
    }
    used: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            used.add(node.id)
        elif isinstance(node, ast.Attribute):
            used.add(node.attr)
        elif isinstance(node, ast.alias):
            used.add(node.name)
        elif isinstance(node, ast.ImportFrom):
            used.add(node.module or "")
        elif isinstance(node, ast.Constant) and type(node.value) is str:
            if id(node) not in docstrings:
                used.add(node.value)
    forbidden = {
        "LaunchReceipt",
        "LaunchRequest",
        "LaunchOwnership",
        "LaunchSuccess",
        "LaunchPendingSuccess",
        "ProductionLaunchAuthorizer",
        "Dispatchable",
        "parse_qualification",
        "parse_readiness",
    }
    assert not used & forbidden
    assert not [name for name in used if "protected:control:" in name]
    assert not [name for name in used if name.startswith("curie_protected_hooks")]


def test_turn_operations_refuse_in_this_slice():
    authorizer = launch.CommissioningLaunchAuthorizer(PERMIT)

    async def scenario() -> None:
        for call in (
            authorizer.reverify(None),  # type: ignore[arg-type]
            authorizer.accept_reverification(None, None),  # type: ignore[arg-type]
            authorizer.require_unsettled(None),  # type: ignore[arg-type]
        ):
            with pytest.raises(launch._Refusal):
                await call
        with pytest.raises(launch._Refusal):
            authorizer.validate_start(None, None)  # type: ignore[arg-type]

    asyncio.run(scenario())


def test_create_records_ownership_before_any_create_then_binds_and_retires(tmp_path):
    async def scenario() -> None:
        cluster = Cluster()
        async with mediator(tmp_path, cluster) as (server, control, url, root):
            raw = request_bytes()
            launch_id = json.loads(raw)["launch_id"]
            cluster.launch_id = launch_id
            async with httpx.AsyncClient(verify=context(root), trust_env=False) as http:
                created = await http.post(url + ROUTE, content=raw)
                assert created.status_code == 202, created.json()
                capability = created.json()["control_capability"]
                await settled(server, launch_id)
                pod, service, secret = object_names(launch_id)
                kinds = [e for e in cluster.events if e[0] in {"OWN", "POST"}]
                assert kinds[0] == ("OWN", "acme-runtime", pod)
                assert [e[1].rsplit("/", 1)[1] for e in kinds[1:]] == [
                    "secrets",
                    "services",
                    "pods",
                ]
                pod_uid = cluster.objects[("pods", pod)]["metadata"]["uid"]
                assert ("ADVANCE", "bound", pod_uid) in cluster.events
                # A retried create replays the live launch and never owns twice.
                again = await http.post(url + ROUTE, content=raw)
                assert again.status_code == 200 and again.json()["launch_id"] == launch_id
                headers = {launch.CAPABILITY_HEADER: capability}
                observed = await http.get(url + ROUTE + "/" + launch_id, headers=headers)
                assert observed.status_code == 200
                body = observed.json()
                assert body["state"] == "idle"
                assert body["receipt"]["workload"]["pod_uid"] == pod_uid
                assert (await http.get(url + ROUTE + "/" + launch_id)).status_code == 403
                start = await http.post(url + ROUTE + "/" + launch_id + "/start", headers=headers)
                assert start.status_code == 404
                retired = await http.post(
                    url + ROUTE + "/" + launch_id + "/retire", headers=headers
                )
                assert retired.status_code == 200, retired.json()
                assert retired.json()["state"] == "retired"
            assert control.ownership[launch_id] == ("retired", pod_uid)
            assert not [k for k in cluster.objects if k[0] in {"pods", "services", "secrets"}]

    asyncio.run(scenario())


@pytest.mark.parametrize("role", ["unknown", "expiring-after-permit"])
def test_candidate_mtls_refuses_before_any_reservation(tmp_path, role):
    async def scenario() -> None:
        cluster = Cluster()
        options: dict[str, Any] = {}
        if role == "expiring-after-permit":
            # The fixture certificate expires in one hour; this permit earlier.
            options["expires_at_ms"] = int(time.time() * 1000) + 1800 * 1000
        async with mediator(tmp_path, cluster, **options) as (_, control, url, root):
            client = "unknown" if role == "unknown" else "worker"
            async with httpx.AsyncClient(verify=context(root, client), trust_env=False) as http:
                response = await http.post(url + ROUTE, content=request_bytes())
            assert response.status_code == 403 and response.json()["code"] == "unauthorized"
            assert control.plans == 0 and control.ownership == {}
            assert not [e for e in cluster.events if e[0] == "POST"]

    asyncio.run(scenario())


def test_an_ambiguous_create_is_never_adopted_without_a_witness(tmp_path):
    async def scenario() -> None:
        cluster = Cluster()
        cluster.fault_mode = "ambiguous-create"
        async with mediator(tmp_path, cluster) as (server, control, url, root):
            raw = request_bytes()
            launch_id = json.loads(raw)["launch_id"]
            cluster.launch_id = launch_id
            async with httpx.AsyncClient(verify=context(root), trust_env=False) as http:
                created = await http.post(url + ROUTE, content=raw)
                assert created.status_code == 202
                await settled(server, launch_id)
                # A retire request on an uncertain launch stays uncertain.
                headers = {launch.CAPABILITY_HEADER: created.json()["control_capability"]}
                retire = await http.post(url + ROUTE + "/" + launch_id + "/retire", headers=headers)
                assert retire.status_code == 202 and retire.json()["state"] == "uncertain"
            pod = object_names(launch_id)[0]
            row = server._registry.get(OWNER, launch_id)
            assert row.state == "uncertain" and row.objects.pod_uid is None
            # The same-name Pod exists and is never deleted on a name match.
            assert ("pods", pod) in cluster.objects
            assert not [e for e in cluster.events if e[0] == "DELETE" and "/pods/" in e[1]]
            assert control.ownership[launch_id] == ("uncertain", None)

    asyncio.run(scenario())


def test_an_ambiguous_create_resolves_through_the_guard_witness(tmp_path):
    async def scenario() -> None:
        cluster = Cluster()
        cluster.fault_mode = "ambiguous-create"
        names: dict[str, str] = {}
        witness = Witness(
            lambda: (
                cluster.objects[("pods", names["pod"])]["metadata"]["uid"]
                if ("pods", names["pod"]) in cluster.objects
                else None
            )
        )
        async with mediator(tmp_path, cluster, witness=witness) as (server, control, url, root):
            raw = request_bytes()
            launch_id = json.loads(raw)["launch_id"]
            names["pod"] = object_names(launch_id)[0]
            cluster.launch_id = launch_id
            async with httpx.AsyncClient(verify=context(root), trust_env=False) as http:
                assert (await http.post(url + ROUTE, content=raw)).status_code == 202
            await settled(server, launch_id)
            uid = witness.returned[0]
            assert server._registry.get(OWNER, launch_id).state == "retired"
            assert control.ownership[launch_id] == ("retired", uid)
            assert witness.calls[0] == {
                "kind": "pod",
                "namespace": "acme-runtime",
                "name": names["pod"],
                "create_operation_id": json.loads(raw)["launch_reservation_id"],
            }

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "witnessed", [None, "foreign", "operation"], ids=["no-witness", "other-uid", "other-operation"]
)
def test_a_squatted_name_is_never_adopted(tmp_path, witnessed):
    async def scenario() -> None:
        cluster = Cluster()
        cluster.fault_mode = "squatted"
        raw = request_bytes()
        launch_id = json.loads(raw)["launch_id"]
        pod = object_names(launch_id)[0]
        foreign = str(uuid4())
        cluster.objects[("pods", pod)] = {
            "metadata": {
                "namespace": "acme-runtime",
                "name": pod,
                "uid": foreign,
                "resourceVersion": "1",
                "finalizers": ["curietech.ai/protected-launch"],
            }
        }
        options: dict[str, Any] = {}
        if witnessed == "foreign":
            options["witness"] = Witness(str(uuid4()))
        elif witnessed == "operation":
            # The squatting Pod's own UID, witnessed for another create operation.
            options["witness"] = Witness(foreign, operation=str(uuid4()))
        async with mediator(tmp_path, cluster, **options) as (server, control, url, root):
            cluster.launch_id = launch_id
            async with httpx.AsyncClient(verify=context(root), trust_env=False) as http:
                assert (await http.post(url + ROUTE, content=raw)).status_code == 202
            await settled(server, launch_id)
            row = server._registry.get(OWNER, launch_id)
            assert row.state == "uncertain" and row.objects.pod_uid is None
            assert cluster.objects[("pods", pod)]["metadata"]["uid"] == foreign
            assert not [e for e in cluster.events if e[0] in {"DELETE", "PATCH"} and pod in e[1]]
            assert control.ownership[launch_id] == ("uncertain", None)

    asyncio.run(scenario())


def test_restart_settles_durable_ownership_without_a_local_row(tmp_path):
    async def scenario() -> None:
        cluster = Cluster()
        control = FakeControl(cluster.events, int(time.time() * 1000) + 2 * 3600 * 1000)
        orphan = str(uuid4())
        control.ownership[orphan] = ("creating", None)
        async with mediator(tmp_path, cluster, control=control) as (server, *_):
            await server.start_cleanup()
            for _ in range(60):
                if control.ownership[orphan][0] == "uncertain":
                    break
                await asyncio.sleep(0.05)
        assert control.ownership[orphan] == ("uncertain", None)

    asyncio.run(scenario())


def test_the_settle_pass_never_marks_a_launch_mid_create_uncertain(tmp_path):
    """Ownership is recorded before the local row; settle must not judge that gap."""

    async def scenario() -> None:
        cluster = Cluster()
        async with mediator(tmp_path, cluster) as (server, control, url, root):
            control.release.clear()
            raw = request_bytes()
            launch_id = json.loads(raw)["launch_id"]
            cluster.launch_id = launch_id
            async with httpx.AsyncClient(verify=context(root), trust_env=False) as http:
                creating = asyncio.create_task(http.post(url + ROUTE, content=raw))
                await asyncio.wait_for(control.owned.wait(), 10)
                assert server._registry.find(OWNER, launch_id) is None
                settle = asyncio.create_task(server._settle_once())
                await asyncio.sleep(0.2)
                assert control.ownership[launch_id] == ("creating", None)
                control.release.set()
                assert (await creating).status_code == 202
                await asyncio.wait_for(settle, 10)
                await settled(server, launch_id)
            assert control.ownership[launch_id][0] == "bound"

    asyncio.run(scenario())


def test_a_request_after_the_permit_expired_refuses(tmp_path):
    async def scenario() -> None:
        cluster = Cluster()
        expired = int(time.time() * 1000) - 1
        async with mediator(tmp_path, cluster, expires_at_ms=expired) as (_, control, url, root):
            async with httpx.AsyncClient(verify=context(root), trust_env=False) as http:
                response = await http.post(url + ROUTE, content=request_bytes())
            assert response.status_code == 403 and control.plans == 0

    asyncio.run(scenario())


def test_the_permit_expiry_check_refuses_on_its_own(tmp_path):
    """The mediator refuses once the permit expired, independent of the certificate rule.

    TLS already refuses an expired certificate, so this drives the mediator's
    own check directly with the candidate key in a certificate that ended
    before the permit did: only the permit's expiry can refuse it.
    """
    from datetime import UTC, datetime, timedelta

    from cryptography.hazmat.primitives import hashes
    from cryptography.x509.oid import NameOID

    async def scenario() -> None:
        cluster = Cluster()
        async with mediator(tmp_path, cluster) as (server, control, _, root):
            key = serialization.load_pem_private_key((root / "worker.key").read_bytes(), None)
            ended = datetime.now(UTC) - timedelta(minutes=5)
            name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "worker")])
            der = (
                x509.CertificateBuilder()
                .subject_name(name)
                .issuer_name(name)
                .public_key(key.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(ended - timedelta(hours=1))
                .not_valid_after(ended)
                .sign(key, hashes.SHA256())
                .public_bytes(serialization.Encoding.DER)
            )

            class Transport:
                def get_extra_info(self, name: str) -> Any:
                    assert name == "ssl_object"
                    return self

                def getpeercert(self, binary_form: bool) -> bytes:
                    return der

            request = type("Request", (), {"transport": Transport()})()
            control.expires_at_ms = int(time.time() * 1000) + 3600 * 1000
            assert server._owner(request) == OWNER
            # The certificate still ends before the permit; only the permit expired.
            control.expires_at_ms = int(time.time() * 1000) - 1000
            assert int(ended.timestamp() * 1000) <= control.expires_at_ms
            with pytest.raises(launch._Refusal):
                server._owner(request)

    asyncio.run(scenario())


@pytest.mark.parametrize("identity", [None, "refusing"], ids=["absent", "refusing"])
def test_the_candidate_identity_is_bound_by_the_provisioner_or_refuses(tmp_path, identity):
    async def scenario() -> None:
        cluster = Cluster()
        root = certificates(tmp_path / "tls")
        chosen = None if identity is None else Identity(root, accept=False)
        async with mediator(tmp_path, cluster, identity=chosen) as (_, control, url, root):
            async with httpx.AsyncClient(verify=context(root), trust_env=False) as http:
                response = await http.post(url + ROUTE, content=request_bytes())
            assert response.status_code == 403 and control.plans == 0
            if chosen is not None:
                # The records, never the caller, name the credential and entry point.
                assert chosen.calls == [(control.worker_credential, control.entry_point)]

    asyncio.run(scenario())


def test_a_failed_assignment_charges_once_and_owns_nothing(tmp_path):
    class Failing:
        def __init__(self) -> None:
            self.failures = 1

        async def assign(self, plan: Any, *, expires_at_ms: int) -> Any:
            if self.failures:
                self.failures -= 1
                raise RuntimeError("assignment unavailable")
            return await Assignments().assign(plan, expires_at_ms=expires_at_ms)

    async def scenario() -> None:
        cluster = Cluster()
        async with mediator(tmp_path, cluster, assignments=Failing()) as (
            server,
            control,
            url,
            root,
        ):
            raw = request_bytes()
            value = json.loads(raw)
            launch_id = value["launch_id"]
            cluster.launch_id = launch_id
            async with httpx.AsyncClient(verify=context(root), trust_env=False) as http:
                assert (await http.post(url + ROUTE, content=raw)).status_code == 503
                # The launch is charged, but nothing is owned, recorded or created,
                # so nothing is left uncertain.
                assert control.charged == {value["launch_reservation_id"]: "reserved"}
                assert control.ownership == {} and await control.unsettled() == ()
                assert server._registry.find(OWNER, launch_id) is None
                assert not [e for e in cluster.events if e[0] == "POST"]
                # A retry replays the same charge and launches.
                created = await http.post(url + ROUTE, content=raw)
                assert created.status_code == 202, created.json()
                await settled(server, launch_id)
            assert [e for e in cluster.events if e[0] == "RESERVE"] == [
                ("RESERVE", value["launch_reservation_id"])
            ]
            assert control.ownership[launch_id][0] == "bound"

    asyncio.run(scenario())


def test_concurrent_creates_of_one_launch_assign_once(tmp_path):
    class Gated:
        def __init__(self) -> None:
            self.calls = 0
            self.entered = asyncio.Event()
            self.release = asyncio.Event()

        async def assign(self, plan: Any, *, expires_at_ms: int) -> Any:
            self.calls += 1
            self.entered.set()
            await self.release.wait()
            return await Assignments().assign(plan, expires_at_ms=expires_at_ms)

    async def scenario() -> None:
        cluster = Cluster()
        gated = Gated()
        async with mediator(tmp_path, cluster, assignments=gated) as (server, control, url, root):
            raw = request_bytes()
            launch_id = json.loads(raw)["launch_id"]
            cluster.launch_id = launch_id
            async with httpx.AsyncClient(verify=context(root), trust_env=False) as http:
                first = asyncio.create_task(http.post(url + ROUTE, content=raw))
                await asyncio.wait_for(gated.entered.wait(), 5)
                second = asyncio.create_task(http.post(url + ROUTE, content=raw))
                await asyncio.sleep(0.2)
                # The second request waits for the first instead of minting credentials.
                assert gated.calls == 1 and not second.done()
                gated.release.set()
                answers = sorted([(await first).status_code, (await second).status_code])
                await settled(server, launch_id)
            assert set(answers) <= {200, 202} and gated.calls == 1
            assert [e for e in cluster.events if e[0] == "RESERVE"] == [
                ("RESERVE", json.loads(raw)["launch_reservation_id"])
            ]

    asyncio.run(scenario())
