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
    root: Path = tmp_path_factory.mktemp("commissioning-local") / "run"
    owner = fresh_owner()
    ledger = OwnershipLedger.create(root, owner=owner)
    cluster = LocalCluster(ledger)
    namespace = owner + "-protected"
    broker: LocalBroker | None = None
    try:
        cluster.start()
        _, recipes = provisioned()
        principals = broker_principals(
            recipes, profile(), provisioner_id="commissioning/example-provisioner"
        )
        baseline_dir = root.parent / "admin"
        admin = AdministratorBootstrap(
            cluster, namespace=namespace, principals=principals, baseline_dir=baseline_dir
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
    assert keys == {
        f"v1/Namespace//{ns}",
        f"v1/ServiceAccount/{ns}/{PROVISIONER_ACCOUNT}",
        f"{rbac}/Role/{ns}/{PROVISIONER_ACCOUNT}",
        f"{rbac}/RoleBinding/{ns}/{PROVISIONER_ACCOUNT}",
        f"v1/Secret/{ns}/{BROKER_NAME}-tls",
        f"v1/Secret/{ns}/{INSTALLER_SECRET}",
        f"v1/Service/{ns}/{BROKER_NAME}",
        f"v1/Pod/{ns}/{BROKER_NAME}",
        *(f"v1/Secret/{ns}/{credential_secret_name(name)}" for name in installed.users),
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
        own = get(installed, "secret/" + credential_secret_name(name))
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


def test_the_provisioner_identity_is_refused_other_principals_secrets(
    installed: Installed,
) -> None:
    """Design decisions 7 and 8 in Kubernetes: the API server refuses the provisioner's
    identity every credential Secret, and lets it read only its own broker TLS Secret."""
    commands, runner = installed.provisioner.commands, installed.provisioner.runner

    def readable(*args: str) -> bool:
        argv = commands.kubectl_argv(*args, "--namespace", installed.namespace, "--output", "json")
        return runner.probe(argv, timeout=60) == 0

    for name in installed.users:
        assert not readable("get", "secret/" + credential_secret_name(name)), name
    assert not readable("get", "secret/" + INSTALLER_SECRET)
    assert not readable("get", "secrets")
    assert readable("get", "secret/" + BROKER_NAME + "-tls")
    assert readable("get", "pods")
    # The same reads succeed with the administrator's identity, so the refusal is RBAC.
    admin = installed.cluster
    observer = credential_secret_name("observer")
    argv = admin.commands.kubectl_argv(
        "get", "secret/" + observer, "--namespace", installed.namespace, "--output", "json"
    )
    assert admin.runner.probe(argv, timeout=60) == 0


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
