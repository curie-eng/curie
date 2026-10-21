"""The production API ingress on a disposable minikube profile, @spec PROTECTED-COMMISSIONING-6.

Opt-in with ``CURIE_COMMISSIONING_LOCAL_CLUSTER=1``. The test creates its own
profile, builds the API image from this checkout into the profile's registry,
installs the broker, builds B, G and M against its live identity, writes M and
the closed selection, then installs Postgres, S3, Valkey, the schema and the
API with the rendered runtime directory. Teardown removes every owned resource
and confirms absence.
"""

from __future__ import annotations

import base64
import json
import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from curie_protected_hooks.admission_records import parse_selection
from curie_protected_hooks.commissioning_acl import GuardRecipes
from curie_protected_hooks.commissioning_local_bootstrap import (
    INSTALLER_SECRET,
    AdministratorBootstrap,
    ProvisionerGrant,
    worker_namespace,
)
from curie_protected_hooks.commissioning_local_broker import (
    BrokerUser,
    LocalBroker,
    broker_principals,
    check_guard_binding,
)
from curie_protected_hooks.commissioning_local_installation import OWNER_LABEL, LocalCluster
from curie_protected_hooks.commissioning_local_ownership import OwnershipLedger, fresh_owner
from curie_protected_hooks.commissioning_local_worker import WORKER_NAME, LocalWorker
from curie_protected_hooks.commissioning_local_workloads import API_NAME, LocalIngress
from curie_protected_hooks.commissioning_provisioner import construct
from curie_protected_hooks.runtime_directory import (
    parse_bootstrap,
    parse_enqueue,
    parse_source_writer,
)

from .commissioning_worker_vectors import worker_profile
from .test_commissioning_local_broker import IMAGE
from .test_commissioning_provisioner import profile, profile_fields

pytestmark = pytest.mark.skipif(
    os.environ.get("CURIE_COMMISSIONING_LOCAL_CLUSTER") != "1",
    reason="opt-in: creates and deletes its own minikube profile",
)
ROOT = Path(__file__).resolve().parents[3]


@dataclass
class Installed:
    cluster: LocalCluster
    broker: LocalBroker
    grant: ProvisionerGrant
    # The administrator's view, used here only to act as each principal.
    users: dict[str, BrokerUser]
    manifest: Any
    namespace: str
    api_image: str
    ingress: LocalIngress
    provisioner: LocalCluster
    worker_image: str
    worker_files: dict[str, bytes]


@pytest.fixture(scope="module")
def installed(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Installed]:
    """Cleanup is registered before the profile exists, and checks absence."""
    base: Path = tmp_path_factory.mktemp("commissioning-ingress")
    root = base / "run"
    owner = fresh_owner()
    ledger = OwnershipLedger.create(root, owner=owner)
    administrator = base / "administrator"
    administrator.mkdir(mode=0o700)
    # The administrator's view: profile, registry, image build and bootstrap.
    cluster = LocalCluster(ledger, kubeconfig=administrator / "kubeconfig")
    namespace = owner + "-protected"
    broker: LocalBroker | None = None
    try:
        cluster.start()
        cluster.enable_registry()
        api_image = cluster.publish_image(
            "api", dockerfile=ROOT / "apps" / "api" / "Dockerfile", context=ROOT
        )
        worker_image = cluster.publish_image(
            "worker", dockerfile=ROOT / "apps" / "worker" / "Dockerfile", context=ROOT
        )
        recipes = GuardRecipes(profile_fields()["qualification_id"])
        principals = broker_principals(
            recipes, profile(), provisioner_id="commissioning/example-provisioner"
        )
        admin = AdministratorBootstrap(
            cluster,
            namespace=namespace,
            principals=principals,
            baseline_dir=administrator / "baseline",
            provisioner_dir=root,
        )
        admin.issue()
        grant = admin.grant()
        # Everything after this runs with the provisioner's own ServiceAccount identity.
        provisioner = LocalCluster(ledger, kubeconfig=grant.kubeconfig)
        broker = LocalBroker(provisioner, namespace=namespace)
        broker.install(image=IMAGE, installer_secret=INSTALLER_SECRET)
        admin.enroll(broker)
        users = dict(admin.credentials())
        identity = broker.observe(grant.provisioner)
        chosen = worker_profile(broker_identity=identity)
        result = construct(chosen, acl=recipes)
        check_guard_binding(grant.principals, result.guard_config)
        broker.install_control_records(
            grant.provisioner, result.manifest, qualification_generation="1"
        )
        ingress = LocalIngress(provisioner, namespace=namespace)
        ingress.install(
            api_image=api_image, manifest=result.manifest, ca_pem=broker.ca_pem, grant=grant
        )
        worker_files, template = LocalWorker(provisioner, namespace=namespace).install(
            worker_image=worker_image,
            manifest=result.manifest,
            execution_config=chosen.execution_config,
            projection=chosen.projection,
            measurement_file=result.measurement_file,
            ca_pem=broker.ca_pem,
            grant=grant,
            secrets=ingress.secrets,
        )
        # Protected workloads are installation: the administrator creates the worker.
        admin.install_worker(template)
        del admin
        yield Installed(
            cluster,
            broker,
            grant,
            users,
            result.manifest,
            namespace,
            api_image,
            ingress,
            provisioner,
            worker_image,
            worker_files,
        )
    finally:
        if broker is not None:
            broker.close()
        cluster.remove_all()
        assert ledger.all_removed()
        assert cluster.runner.probe(cluster.commands.minikube_argv("status"), timeout=60) == 85


def get(installed: Installed, resource: str) -> dict[str, Any]:
    raw = installed.cluster.kubectl(
        "get", resource, "--namespace", installed.namespace, "--output", "json"
    )
    value: dict[str, Any] = json.loads(raw)
    return value


def test_the_api_runs_the_published_image_with_the_runtime_directory(installed: Installed) -> None:
    deployment = get(installed, "deployment/" + API_NAME)
    assert deployment["status"]["availableReplicas"] == 1
    pods = get(installed, "pods")["items"]
    (api,) = [p for p in pods if p["metadata"]["name"].startswith(API_NAME + "-")]
    (status,) = api["status"]["containerStatuses"]
    assert status["ready"] is True and status["restartCount"] == 0
    assert status["imageID"].endswith(installed.api_image.split("@", 1)[1])
    migrate = get(installed, "pod/curie-migrate")
    assert migrate["status"]["phase"] == "Succeeded"


def test_the_runtime_secret_is_what_the_api_reader_accepts(installed: Installed) -> None:
    secret = get(installed, "secret/" + API_NAME + "-protected-runtime")
    files = {k: base64.b64decode(v) for k, v in secret["data"].items()}
    assert set(files) == {
        "manifest.json",
        "ca.pem",
        "bootstrap.json",
        "enqueue.json",
        "source_writer.json",
    }
    assert files["manifest.json"] == installed.manifest.canonical_bytes
    bootstrap = parse_bootstrap(files["manifest.json"], files["ca.pem"], files["bootstrap.json"])
    assert bootstrap.credential.username == installed.users["control_reader"].username
    enqueue = parse_enqueue(files["enqueue.json"], bootstrap)
    assert enqueue.username == installed.users["enqueue"].username
    writer = parse_source_writer(files["source_writer.json"], bootstrap.credential)
    assert writer.username == installed.users["source_writer"].username
    decoded = b"".join(files.values()).decode()
    for name in ("provisioner", "observer", "qualification_publisher", "issuer", "control"):
        assert installed.users[name].password not in decoded


def test_m_and_a_closed_selection_are_installed_before_the_campaign(installed: Installed) -> None:
    m = installed.manifest.as_dict()
    reader = installed.broker.session(installed.users["worker"])
    try:
        stored = reader.get("protected:control:manifest:" + installed.manifest.digest)
        assert stored == installed.manifest.canonical_bytes
        selection = parse_selection(reader.get("protected:control:selection:" + m["runtime_id"]))
        assert selection["admission_open"] is False
        assert selection["broker_run_id"] == m["broker_identity"]["run_id"]
        assert selection["manifest_digest"] == installed.manifest.digest
        # No Qualification or Readiness exists before the campaign.
        qualification = f"{selection['qualification_id']}:{selection['qualification_generation']}"
        assert reader.get("protected:control:qualification:" + qualification) is None
        readiness = f"{m['runtime_id']}:{selection['runtime_generation']}"
        assert reader.get("protected:control:readiness:" + readiness) is None
    finally:
        reader.close()


def test_production_refuses_before_the_campaign(installed: Installed) -> None:
    """Clause 6: with no Qualification or Readiness, production authority is unavailable.

    The protected source policy commits but its publication evaluation, read
    through the production API's runtime directory from the protected broker,
    refuses ``qualification_unavailable``; the source therefore stays closed
    and the signed support probe answers ``source_closed``.
    """
    import time
    import uuid

    import httpx
    from curie_api import hook_signing, hook_source_signing
    from curie_protected_hooks.commissioning_local_broker import _start_forward, _stop

    key = installed.ingress.secrets.api_key
    commands = installed.cluster.commands
    argv = commands.kubectl_argv(
        "port-forward", "service/" + API_NAME, ":8000", "--namespace", installed.namespace
    )
    process, port = _start_forward(argv, commands.environment(os.environ), target_port=8000)
    m = installed.manifest.as_dict()
    hook = "alerts"
    try:
        client = httpx.Client(
            base_url=f"http://127.0.0.1:{port}", headers={"X-API-Key": key}, timeout=30
        )
        created = client.post(
            "/agents",
            json={
                "name": "commissioning-" + uuid.uuid4().hex[:8],
                "channel": {"kind": "slack", "address": "C0EXAMPLE2"},
            },
        )
        assert created.status_code == 201, created.text
        agent = created.json()["id"]
        put = client.put(
            f"/agents/{agent}/hooks/{hook}/source-policy",
            json={
                "expected_generation": "0",
                "operation_id": str(uuid.uuid4()),
                "runtime_id": m["runtime_id"],
                "qualification_id": m["qualification_id"],
                "bundle_digest": m["bundle_digest"]["sha256"],
            },
        )
        # The row commits, then publication refuses: production authority is unavailable.
        assert put.status_code == 503, put.text
        detail = put.json()["detail"]
        assert detail["code"] == "qualification_unavailable"
        generation = int(detail["committed_generation"])
        secret = hook_source_signing.derive(key, agent_id=agent, hook=hook, generation=generation)
        body, stamp, delivery = b"{}", str(int(time.time())), "commissioning-probe"
        headers = {
            "Content-Type": "application/json",
            hook_signing.TIMESTAMP_HEADER: stamp,
            hook_signing.DELIVERY_HEADER: delivery,
            hook_signing.SIGNATURE_HEADER: hook_source_signing.sign_support(
                secret,
                timestamp=stamp,
                delivery_id=delivery,
                hook=hook,
                tool_access=None,
                body=body,
            ),
        }
        probe = client.post(f"/hooks/{agent}/{hook}/support", content=body, headers=headers)
        assert probe.status_code == 503, probe.text
        assert probe.json()["supported"] is False
        assert probe.json()["reason"] == "source_closed"
        # A wrong key is refused before any broker read.
        headers[hook_signing.SIGNATURE_HEADER] = "sha256=" + "0" * 64
        assert (
            client.post(f"/hooks/{agent}/{hook}/support", content=body, headers=headers).status_code
            == 401
        )
    finally:
        _stop(process)


def worker_get(installed: Installed, resource: str) -> dict[str, Any]:
    raw = installed.cluster.kubectl(
        "get", resource, "--namespace", worker_namespace(installed.namespace), "--output", "json"
    )
    value: dict[str, Any] = json.loads(raw)
    return value


def _worker_logs(installed: Installed) -> bytes:
    """Every log the provisioner can read for the worker Pod, until the refusal shows.

    The current container may be waiting in back-off and the previous one's
    log may not be retrievable at every instant, so each read is tolerated and
    the whole read is retried.
    """
    import time

    from curie_protected_hooks.commissioning_local_installation import InstallationRefused

    namespace = worker_namespace(installed.namespace)
    deadline = time.monotonic() + 120
    while True:
        name = _worker_pod(installed)["metadata"]["name"]
        texts = []
        for container, extra in (("stage", ()), ("worker", ()), ("worker", ("--previous",))):
            try:
                texts.append(
                    installed.provisioner.kubectl(
                        "logs",
                        "pod/" + name,
                        "--namespace",
                        namespace,
                        "--container",
                        container,
                        *extra,
                    )  # fmt: skip
                )
            except InstallationRefused:
                continue
        logs = b"\n".join(texts)
        if b"protected worker stopped without confirmed completion" in logs:
            return logs
        assert time.monotonic() < deadline, "no worker refusal in its logs"
        time.sleep(5)


def _worker_pod(installed: Installed) -> dict[str, Any]:
    pods = worker_get(installed, "pods")["items"]
    (pod,) = [p for p in pods if p["metadata"]["name"].startswith(WORKER_NAME + "-")]
    return pod


def test_the_worker_runs_its_final_template_and_refuses_before_the_campaign(
    installed: Installed,
) -> None:
    """Clause 6: with no Readiness or Qualification the production worker refuses
    on every start, at its final template, image and mounts."""
    import time

    deadline = time.monotonic() + 300
    while True:
        pod = _worker_pod(installed)
        statuses = pod["status"].get("containerStatuses") or []
        terminated = [
            s.get("lastState", {}).get("terminated") or s.get("state", {}).get("terminated")
            for s in statuses
        ]
        if any(t and t.get("exitCode") == 1 for t in terminated):
            break
        assert time.monotonic() < deadline, "the worker never exited"
        time.sleep(5)
    (init,) = pod["status"]["initContainerStatuses"]
    assert init["state"]["terminated"]["exitCode"] == 0
    (status,) = pod["status"]["containerStatuses"]
    assert status["imageID"].endswith(installed.worker_image.split("@", 1)[1])
    assert status["ready"] is False
    # The provisioner reads the worker's status and logs, nothing more.
    logs = _worker_logs(installed)
    assert b"protected worker stopped without confirmed completion" in logs


def test_the_worker_files_parse_and_readiness_is_absent_on_the_broker(
    installed: Installed,
) -> None:
    """The refusal is the missing Readiness, not a broken file: a probe with the
    worker's image and volumes runs its loaders and reaches the broker as the
    production worker."""
    import copy

    deployment = worker_get(installed, "deployment/" + WORKER_NAME)
    spec = copy.deepcopy(deployment["spec"]["template"]["spec"])
    (worker,) = spec["containers"]
    worker["command"] = ["python", "-c", _PROBE, "/run/curie/protected-worker"]
    spec["restartPolicy"] = "Never"
    name = "curie-worker-probe"
    namespace = worker_namespace(installed.namespace)
    probe = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {OWNER_LABEL: installed.cluster.owner},
        },
        "spec": spec,
    }
    # The administrator runs the probe: the provisioner cannot create Pods here.
    installed.cluster.create(probe)
    installed.cluster.kubectl(
        "wait", "--for=jsonpath={.status.phase}=Succeeded", "pod/" + name,
        "--namespace", namespace, "--timeout", "240s", timeout=270,
    )  # fmt: skip
    logs = installed.provisioner.kubectl("logs", "pod/" + name, "--namespace", namespace).decode()
    assert logs.strip().splitlines()[-1] == (
        "bootstrap=ok process=ok launch=ok broker=ok readiness=absent qualification=absent"
    )


_PROBE = """
import asyncio, ssl, sys
from curie_worker.protected_entry import load_process_settings, _open_root, _read
from curie_worker.protected_worker_bootstrap import load_worker_bootstrap
from curie_protected_hooks.worker_transport import AuthenticatedWorkerClient

async def main(directory):
    bootstrap = load_worker_bootstrap(directory)
    tls = load_process_settings(directory).launch_tls
    # The mounted client key matches its certificate and the pinned CA loads.
    context = ssl.create_default_context(cafile=tls.ca_file)
    context.load_cert_chain(tls.client_certificate, tls.client_key)
    root = _open_root(directory)
    ca = _read(root, "ca.pem").decode("ascii")
    broker = await AuthenticatedWorkerClient.connect(bootstrap.manifest, bootstrap.credential, ca)
    try:
        m = bootstrap.manifest.as_dict()
        readiness = await broker.read_control(
            "protected:control:readiness:" + m["runtime_id"] + ":" + m["runtime_generation"]
        )
        qualification = await broker.read_control(
            "protected:control:qualification:" + m["qualification_id"] + ":1"
        )
    finally:
        await broker.close()
    print(
        "bootstrap=ok process=ok launch=ok broker=ok readiness="
        + ("absent" if readiness is None else "present")
        + " qualification="
        + ("absent" if qualification is None else "present")
    )

asyncio.run(main(sys.argv[1]))
"""


def test_the_worker_secret_holds_only_its_own_credentials(installed: Installed) -> None:
    decoded = b"".join(installed.worker_files.values()).decode("ascii", "replace")
    assert installed.users["worker"].password in decoded
    for name in ("observer", "qualification_publisher", "provisioner", "issuer", "control"):
        assert installed.users[name].password not in decoded, name
    # The provisioner rendered no private key: the worker's client key is the
    # administrator's Secret, mounted beside the worker; the mediator's waits in
    # the credentials namespace.
    assert "PRIVATE KEY" not in decoded


def test_the_provisioner_identity_is_refused_both_launch_secrets(installed: Installed) -> None:
    from curie_protected_hooks.commissioning_local_bootstrap import (
        MEDIATOR_SECRET,
        WORKER_CLIENT_SECRET,
        credentials_namespace,
    )

    commands, runner = installed.provisioner.commands, installed.provisioner.runner
    for name, namespace in (
        (WORKER_CLIENT_SECRET, worker_namespace(installed.namespace)),
        (MEDIATOR_SECRET, credentials_namespace(installed.namespace)),
    ):
        argv = commands.kubectl_argv(
            "get", "secret/" + name, "--namespace", namespace, "--output", "json"
        )
        assert runner.probe(argv, timeout=60) != 0, name


def test_the_provisioner_cannot_create_a_pod_or_deployment_beside_the_worker_key(
    installed: Installed,
) -> None:
    """The worker namespace holds the worker's client key; the provisioner may write only
    its runtime Secret there and read status, so no Pod of its making can mount the key."""
    from curie_protected_hooks.commissioning_local_bootstrap import WORKER_CLIENT_SECRET
    from curie_protected_hooks.commissioning_local_installation import InstallationRefused

    namespace = worker_namespace(installed.namespace)
    reader = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "read-worker-key",
            "namespace": namespace,
            "labels": {OWNER_LABEL: installed.cluster.owner},
        },
        "spec": {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "securityContext": {
                "runAsNonRoot": True,
                "runAsUser": 1000,
                "seccompProfile": {"type": "RuntimeDefault"},
            },
            "containers": [
                {
                    "name": "read",
                    "image": installed.worker_image,
                    "command": ["cat", "/key/launch-client.key"],
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "readOnlyRootFilesystem": True,
                        "capabilities": {"drop": ["ALL"]},
                    },
                    "volumeMounts": [{"name": "key", "mountPath": "/key"}],
                }
            ],
            "volumes": [{"name": "key", "secret": {"secretName": WORKER_CLIENT_SECRET}}],
        },
    }
    with pytest.raises(InstallationRefused):
        installed.provisioner.create(reader)
    copied = worker_get(installed, "deployment/" + WORKER_NAME)
    template = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "other-worker",
            "namespace": namespace,
            "labels": {OWNER_LABEL: installed.cluster.owner},
        },
        "spec": copied["spec"],
    }
    with pytest.raises(InstallationRefused):
        installed.provisioner.create(template)
    for verb in (("get", "secrets"), ("get", "secret/" + WORKER_CLIENT_SECRET)):
        argv = installed.provisioner.commands.kubectl_argv(
            *verb, "--namespace", namespace, "--output", "json"
        )
        assert installed.provisioner.runner.probe(argv, timeout=60) != 0
    # Status reads stay allowed.
    argv = installed.provisioner.commands.kubectl_argv(
        "get", "deployment/" + WORKER_NAME, "--namespace", namespace, "--output", "json"
    )
    assert installed.provisioner.runner.probe(argv, timeout=60) == 0


def test_the_worker_logs_carry_no_secret(installed: Installed) -> None:
    """The provisioner may read the worker's logs, so they must hold no credential or key."""
    from curie_protected_hooks.commissioning_local_bootstrap import WORKER_CLIENT_SECRET

    namespace = worker_namespace(installed.namespace)
    logs = _worker_logs(installed)
    assert b"protected worker stopped without confirmed completion" in logs
    client = json.loads(
        installed.cluster.kubectl(
            "get", "secret/" + WORKER_CLIENT_SECRET, "--namespace", namespace, "--output", "json"
        )
    )["data"]
    secrets = [
        installed.users["worker"].password.encode(),
        base64.b64decode(client["launch-client.key"]),
        installed.ingress.secrets.internal_worker_token.encode(),
        installed.ingress.secrets.postgres_password.encode(),
        b"PRIVATE KEY",
    ]
    for secret in secrets:
        assert secret not in logs
