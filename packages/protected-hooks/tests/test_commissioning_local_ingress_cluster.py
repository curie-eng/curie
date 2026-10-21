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
)
from curie_protected_hooks.commissioning_local_broker import (
    BrokerUser,
    LocalBroker,
    broker_principals,
    check_guard_binding,
)
from curie_protected_hooks.commissioning_local_installation import LocalCluster
from curie_protected_hooks.commissioning_local_ownership import OwnershipLedger, fresh_owner
from curie_protected_hooks.commissioning_local_workloads import API_NAME, LocalIngress
from curie_protected_hooks.commissioning_provisioner import construct
from curie_protected_hooks.runtime_directory import (
    parse_bootstrap,
    parse_enqueue,
    parse_source_writer,
)

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
        del admin
        identity = broker.observe(grant.provisioner)
        result = construct(profile(broker_identity=identity), acl=recipes)
        check_guard_binding(grant.principals, result.guard_config)
        broker.install_control_records(
            grant.provisioner, result.manifest, qualification_generation="1"
        )
        ingress = LocalIngress(provisioner, namespace=namespace)
        ingress.install(
            api_image=api_image, manifest=result.manifest, ca_pem=broker.ca_pem, grant=grant
        )
        yield Installed(
            cluster, broker, grant, users, result.manifest, namespace, api_image, ingress
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
