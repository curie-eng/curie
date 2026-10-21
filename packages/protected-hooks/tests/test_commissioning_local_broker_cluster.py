"""The local broker on a disposable minikube profile, @spec PROTECTED-COMMISSIONING-1/5.

Opt-in with ``CURIE_COMMISSIONING_LOCAL_CLUSTER=1``. The test creates its own
``curie-commission-<hex>`` profile through ``LocalCluster`` with a private
kubeconfig, installs the broker, and removes every owned resource in teardown,
confirming absence. It never reads the default kubeconfig or current context.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from curie_protected_hooks.authority_records import _BROKER, _object
from curie_protected_hooks.commissioning_local_broker import (
    BROKER_NAME,
    BrokerUser,
    LocalBroker,
    broker_users,
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
    cluster: LocalCluster
    broker: LocalBroker
    users: dict[str, BrokerUser]
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
    broker = LocalBroker(cluster, namespace=namespace)
    try:
        cluster.start()
        _, recipes = provisioned()
        users = broker_users(
            recipes,
            profile(),
            provisioner_id="commissioning/example-provisioner",
        )
        identity = broker.install(image=IMAGE, users=users)
        yield Installed(cluster, broker, users, identity, namespace, recipes)
    finally:
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
    check_guard_binding(installed.users, result.guard_config)


def test_owned_objects_match_the_ledger_and_the_pinned_image(installed: Installed) -> None:
    keys = set(installed.cluster.objects())
    assert keys == {
        f"v1/Namespace//{installed.namespace}",
        f"v1/Secret/{installed.namespace}/{BROKER_NAME}-tls",
        f"v1/Service/{installed.namespace}/{BROKER_NAME}",
        f"v1/Pod/{installed.namespace}/{BROKER_NAME}",
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


def test_secret_holds_password_hashes_only(installed: Installed) -> None:
    secret = get(installed, "secret/" + BROKER_NAME + "-tls")
    decoded = "".join(base64.b64decode(v).decode() for v in secret["data"].values())
    for user in installed.users.values():
        assert user.password not in decoded
        assert "#" + hashlib.sha256(user.password.encode()).hexdigest() in decoded


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
            assert "on" in state["flags"]
            assert state["passwords"] == [hashlib.sha256(user.password.encode()).hexdigest()]
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
        installed.broker.start_campaign(installed.users, record)
        for name in CANDIDATES:
            state = observer.acl_getuser(installed.users[name].username)
            assert "on" in state["flags"]
            assert state["passwords"] == [
                hashlib.sha256(installed.users[name].password.encode()).hexdigest()
            ]
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
