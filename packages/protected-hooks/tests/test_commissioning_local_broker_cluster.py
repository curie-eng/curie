"""The local broker on a disposable minikube profile, @spec PROTECTED-COMMISSIONING-1/5.

Opt-in with ``CURIE_COMMISSIONING_LOCAL_CLUSTER=1``. The test creates its own
``curie-commission-<hex>`` profile through ``LocalCluster`` with a private
kubeconfig, installs the broker, and removes every owned resource in teardown,
confirming absence. It never reads the default kubeconfig or current context.
"""

from __future__ import annotations

import base64
import json
import os
import re
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from curie_protected_hooks.authority_records import _BROKER, _object
from curie_protected_hooks.commissioning_local_bootstrap import (
    INSTALLER_SECRET,
    PROVISIONER_ACCOUNT,
    AdministratorBootstrap,
    ProvisionerGrant,
    credential_secret_name,
    credentials_namespace,
    read_baseline,
)
from curie_protected_hooks.commissioning_local_broker import (
    BROKER_NAME,
    BrokerUser,
    LocalBroker,
    broker_principals,
    check_guard_binding,
    server_name,
)
from curie_protected_hooks.commissioning_local_installation import (
    OWNER_LABEL,
    LocalCluster,
)
from curie_protected_hooks.commissioning_local_ownership import OwnershipLedger, fresh_owner
from redis.exceptions import AuthenticationError, NoPermissionError

from .test_commissioning_local_broker import CANDIDATES, IMAGE, provisioned
from .test_commissioning_provisioner import profile

pytestmark = pytest.mark.skipif(
    os.environ.get("CURIE_COMMISSIONING_LOCAL_CLUSTER") != "1",
    reason="opt-in: creates and deletes its own minikube profile",
)


@dataclass
class Installed:
    # The administrator's view of the profile: bootstrap and teardown only.
    cluster: LocalCluster
    provisioner: LocalCluster
    broker: LocalBroker
    grant: ProvisionerGrant
    # The administrator's view, used here only to act as each principal.
    users: dict[str, BrokerUser]
    baseline: dict[str, Any]
    baseline_dir: Path
    identity: dict[str, Any]
    namespace: str
    recipes: Any


@pytest.fixture(scope="module")
def installed(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Installed]:
    """Cleanup is registered before the profile exists, and checks absence."""
    base: Path = tmp_path_factory.mktemp("commissioning-local")
    root = base / "run"
    owner = fresh_owner()
    ledger = OwnershipLedger.create(root, owner=owner)
    # The administrator's kubeconfig and baseline live apart from the provisioner's files.
    administrator = base / "administrator"
    administrator.mkdir(mode=0o700)
    cluster = LocalCluster(ledger, kubeconfig=administrator / "kubeconfig")
    namespace = owner + "-protected"
    broker: LocalBroker | None = None
    try:
        cluster.start()
        _, recipes = provisioned()
        principals = broker_principals(
            recipes, profile(), provisioner_id="commissioning/example-provisioner"
        )
        baseline_dir = administrator / "baseline"
        admin = AdministratorBootstrap(
            cluster,
            namespace=namespace,
            principals=principals,
            baseline_dir=baseline_dir,
            provisioner_dir=root,
        )
        admin.issue()
        grant = admin.grant()
        # The provisioner works only through its own ServiceAccount identity.
        provisioner = LocalCluster(ledger, kubeconfig=grant.kubeconfig)
        broker = LocalBroker(provisioner, namespace=namespace)
        broker.install(image=IMAGE, installer_secret=INSTALLER_SECRET)
        baseline = admin.enroll(broker)
        identity = broker.observe(grant.provisioner)
        users = dict(admin.credentials())
        del admin
        yield Installed(
            cluster,
            provisioner,
            broker,
            grant,
            users,
            baseline,
            baseline_dir,
            identity,
            namespace,
            recipes,
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


def test_identity_is_the_live_pinned_broker(installed: Installed) -> None:
    identity = installed.identity
    _object(identity, _BROKER)
    assert identity["endpoint"] == {"host": server_name(installed.namespace), "port": 6379}
    assert identity["tls_server_name"] == identity["endpoint"]["host"]
    assert re.fullmatch(r"[0-9a-f]{40}", identity["run_id"])
    client = installed.broker.session(installed.users["provisioner"], run_id=identity["run_id"])
    client.close()
    # M can only be built now: it pins this live identity. G hashes the installed users.
    from curie_protected_hooks.commissioning_provisioner import construct

    result = construct(profile(broker_identity=identity), acl=installed.recipes)
    assert result.manifest.as_dict()["broker_identity"] == identity
    check_guard_binding(installed.grant.principals, result.guard_config)


def test_owned_objects_match_the_ledger_and_the_pinned_image(installed: Installed) -> None:
    keys = set(installed.cluster.objects())
    ns = installed.namespace
    rbac = "rbac.authorization.k8s.io/v1"
    held = credentials_namespace(ns)
    assert keys == {
        f"v1/Namespace//{ns}",
        f"v1/Namespace//{held}",
        f"v1/ServiceAccount/{ns}/{PROVISIONER_ACCOUNT}",
        f"{rbac}/Role/{ns}/{PROVISIONER_ACCOUNT}",
        f"{rbac}/RoleBinding/{ns}/{PROVISIONER_ACCOUNT}",
        f"v1/Secret/{ns}/{BROKER_NAME}-tls",
        f"v1/Secret/{ns}/{INSTALLER_SECRET}",
        f"v1/Service/{ns}/{BROKER_NAME}",
        f"v1/Pod/{ns}/{BROKER_NAME}",
        *(f"v1/Secret/{held}/{credential_secret_name(name)}" for name in installed.users),
    }
    for key in keys:
        live = installed.cluster.read(key)
        assert live is not None
        assert live["metadata"]["labels"][OWNER_LABEL] == installed.cluster.owner
        assert (
            live["metadata"]["uid"] == installed.cluster.ledger.identity("kubernetes", key)["uid"]
        )
    pod = get(installed, "pod/" + BROKER_NAME)
    assert pod["spec"]["restartPolicy"] == "Never"
    (status,) = pod["status"]["containerStatuses"]
    assert status["imageID"].endswith(IMAGE.split("@", 1)[1])
    assert status["restartCount"] == 0


def test_each_credential_is_in_its_own_secret_and_nowhere_else(installed: Installed) -> None:
    tls = get(installed, "secret/" + BROKER_NAME + "-tls")
    decoded = "".join(base64.b64decode(v).decode() for v in tls["data"].values())
    for name, user in installed.users.items():
        assert user.password not in decoded
        own = json.loads(
            installed.cluster.kubectl(
                "get", "secret/" + credential_secret_name(name),
                "--namespace", credentials_namespace(installed.namespace), "--output", "json",
            )
        )  # fmt: skip
        assert base64.b64decode(own["data"]["password"]).decode() == user.password
    pod = json.dumps(get(installed, "pod/" + BROKER_NAME)["spec"])
    assert all(user.password not in pod for user in installed.users.values())


def test_the_provisioner_side_never_holds_the_observer_or_publisher_password(
    installed: Installed,
) -> None:
    """Design decisions 7 and 8: the checker's and the publisher's secrets stay with the admin."""
    held = repr(vars(installed.provisioner)) + repr(vars(installed.broker))
    held += repr({n: (u.username, u.password) for n, u in installed.grant.workload.items()})
    held += installed.grant.provisioner.password
    held += json.dumps(read_baseline(installed.baseline_dir))
    for name in ("observer", "qualification_publisher", "issuer", "control", *CANDIDATES):
        assert installed.users[name].password not in held, name


def test_the_provisioner_identity_has_no_path_to_other_credentials(
    installed: Installed,
) -> None:
    """Design decisions 7 and 8 in Kubernetes. The credential Secrets live in a
    namespace where the provisioner's identity holds no right at all, so it can
    neither read them nor start a Pod that mounts them; a Pod in its own
    namespace cannot mount another namespace's Secret."""
    from curie_protected_hooks.commissioning_local_installation import InstallationRefused

    provisioner, held = installed.provisioner, credentials_namespace(installed.namespace)

    def allowed(*args: str) -> bool:
        argv = provisioner.commands.kubectl_argv(*args, "--output", "json")
        return provisioner.runner.probe(argv, timeout=60) == 0

    for name in installed.users:
        assert not allowed("get", "secret/" + credential_secret_name(name), "--namespace", held)
        assert not allowed(
            "get", "secret/" + credential_secret_name(name), "--namespace", installed.namespace
        )
    assert not allowed("get", "secrets", "--namespace", held)
    assert not allowed("get", "pods", "--namespace", held)
    assert not allowed("get", "secrets", "--namespace", installed.namespace)
    assert allowed("get", "secret/" + BROKER_NAME + "-tls", "--namespace", installed.namespace)
    assert allowed("get", "pods", "--namespace", installed.namespace)
    # The administrator's identity reads the same Secret, so the refusals are RBAC.
    assert installed.cluster.runner.probe(
        installed.cluster.commands.kubectl_argv(
            "get", "secret/" + credential_secret_name("observer"), "--namespace", held,
            "--output", "json",
        ),
        timeout=60,
    ) == 0  # fmt: skip
    for name in ("observer", "qualification_publisher"):
        secret = credential_secret_name(name)
        reader = _reader_pod(installed, "read-" + name.replace("_", "-"), secret)
        # In the credentials namespace the API server refuses the Pod outright.
        with pytest.raises(InstallationRefused):
            provisioner.create({**reader, "metadata": {**reader["metadata"], "namespace": held}})
        # In its own namespace the Pod is admitted but the Secret is not there: it never runs.
        provisioner.create(reader)
        argv = provisioner.commands.kubectl_argv(
            "wait", "--for=jsonpath={.status.phase}=Succeeded", "pod/" + reader["metadata"]["name"],
            "--namespace", installed.namespace, "--timeout", "30s",
        )  # fmt: skip
        assert provisioner.runner.probe(argv, timeout=60) != 0
        live = get(installed, "pod/" + reader["metadata"]["name"])
        assert live["status"]["phase"] == "Pending"
        logs = provisioner.commands.kubectl_argv(
            "logs", "pod/" + reader["metadata"]["name"], "--namespace", installed.namespace
        )
        assert provisioner.runner.probe(logs, timeout=60) != 0


def _reader_pod(installed: Installed, name: str, secret: str) -> dict[str, Any]:
    """A restricted Pod that would print a credential if it could mount it."""
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": installed.namespace,
            "labels": {OWNER_LABEL: installed.cluster.owner},
        },
        "spec": {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "securityContext": {
                "runAsNonRoot": True,
                "runAsUser": 999,
                "seccompProfile": {"type": "RuntimeDefault"},
            },
            "containers": [
                {
                    "name": "read",
                    "image": IMAGE,
                    "command": ["cat", "/credential/password"],
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "readOnlyRootFilesystem": True,
                        "capabilities": {"drop": ["ALL"]},
                    },
                    "volumeMounts": [{"name": "credential", "mountPath": "/credential"}],
                }
            ],
            "volumes": [{"name": "credential", "secret": {"secretName": secret}}],
        },
    }


def test_the_baseline_is_what_the_observer_reads_and_holds_no_password(
    installed: Installed,
) -> None:
    recorded = read_baseline(installed.baseline_dir)
    assert set(recorded) == set(installed.baseline)
    observer = installed.broker.session(installed.users["observer"])
    try:
        for name, state in installed.baseline.items():
            live = observer.acl_getuser(installed.users[name].username)
            assert set(live["passwords"]) == state.password_hashes
            assert sorted(state.password_hashes) == recorded[name]["password_hashes"]
            assert ("on" in live["flags"]) is state.enabled is recorded[name]["enabled"]
            assert state.enabled is (name not in CANDIDATES)
        assert "off" in observer.acl_getuser("default")["flags"]
    finally:
        observer.close()


def test_the_forward_survives_many_connections(installed: Installed) -> None:
    """kubectl logs a line per connection; 2,500 lines exceed any pipe buffer."""
    for _ in range(2500):
        installed.broker.session(installed.users["provisioner"]).close()
    client = installed.broker.session(installed.users["provisioner"])
    try:
        assert client.info("server")["run_id"] == installed.identity["run_id"]
    finally:
        client.close()


def test_users_authenticate_and_the_observer_sees_their_state(installed: Installed) -> None:
    for name, user in installed.users.items():
        if name in CANDIDATES:
            continue
        client = installed.broker.session(user)
        try:
            assert client.ping() is True, name
        finally:
            client.close()
    observer = installed.broker.session(installed.users["observer"])
    try:
        for name, user in installed.users.items():
            if name in CANDIDATES:
                continue
            state = observer.acl_getuser(user.username)
            assert "on" in state["flags"] and len(state["passwords"]) == 1
        assert "off" in observer.acl_getuser("default")["flags"]
        with pytest.raises(NoPermissionError):
            observer.get("protected:control:selection:x")
    finally:
        observer.close()


def test_boundaries_hold_on_the_installed_broker(installed: Installed) -> None:
    wrong = BrokerUser(installed.users["worker"].username, (), "not-the-password")
    with pytest.raises(AuthenticationError):
        installed.broker.session(wrong).ping()
    worker = installed.broker.session(installed.users["worker"])
    try:
        with pytest.raises(NoPermissionError):
            worker.get("protected:commissioning:permit:x")
    finally:
        worker.close()


def test_a_recorded_object_is_never_created_twice(installed: Installed) -> None:
    namespace = {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": installed.namespace,
            "labels": {OWNER_LABEL: installed.cluster.owner},
        },
    }
    with pytest.raises(Exception, match="refused"):
        installed.cluster.create(namespace)


def test_candidates_are_off_until_a_campaign_then_confined_to_its_lane(
    installed: Installed,
) -> None:
    """Design decision 9 on the installed broker."""
    from curie_protected_hooks.commissioning_acl import derive_commissioning_lane

    from .test_commissioning_acl import permit

    observer = installed.broker.session(installed.users["observer"])
    try:
        for name in CANDIDATES:
            assert "off" in observer.acl_getuser(installed.users[name].username)["flags"]
            with pytest.raises(AuthenticationError):
                installed.broker.session(installed.users[name]).ping()
        record = permit()
        lane = derive_commissioning_lane(record)
        foreign = derive_commissioning_lane(permit(campaign_generation="2"))
        installed.broker.start_campaign(
            installed.grant.provisioner, installed.grant.principals, record
        )
        for name in CANDIDATES:
            state = observer.acl_getuser(installed.users[name].username)
            assert "on" in state["flags"]
            assert set(state["passwords"]) == installed.baseline[name].password_hashes
    finally:
        observer.close()
    enqueue = installed.broker.session(installed.users["candidate_enqueue"])
    try:
        assert enqueue.xadd(lane.stream, {"k": "v"})
        with pytest.raises(NoPermissionError):
            enqueue.xadd(foreign.stream, {"k": "v"})
        with pytest.raises(NoPermissionError):
            enqueue.xadd("curie:runs", {"k": "v"})
    finally:
        enqueue.close()
