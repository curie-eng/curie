"""Provisioner ownership ledger and explicit-context argv, @spec PROTECTED-COMMISSIONING-5."""

from __future__ import annotations

import importlib
import importlib.util
import json
import stat
from pathlib import Path
from typing import Any

import pytest

OWNER = "curie-commission-0123456789ab"


def module() -> Any:
    name = "curie_protected_hooks.commissioning_local_ownership"
    assert importlib.util.find_spec(name) is not None, "ownership ledger not implemented"
    return importlib.import_module(name)


def ledger(tmp_path: Path) -> Any:
    return module().OwnershipLedger.create(tmp_path / "run", owner=OWNER)


def test_fresh_owner_is_a_profile_name_and_never_repeats() -> None:
    api = module()
    owners = {api.fresh_owner() for _ in range(64)}
    assert len(owners) == 64
    assert all(api.valid_owner(owner) for owner in owners)


@pytest.mark.parametrize(
    "owner", ["", "curie-commission-0123", "curie-commission-0123456789AB", "minikube", 7]
)
def test_ledger_refuses_an_owner_outside_the_owned_namespace(tmp_path: Path, owner: Any) -> None:
    api = module()
    with pytest.raises(api.OwnershipRefused):
        api.OwnershipLedger.create(tmp_path / "run", owner=owner)


def test_ledger_directory_is_private_and_never_reused(tmp_path: Path) -> None:
    api = module()
    owned = ledger(tmp_path)
    assert stat.S_IMODE((tmp_path / "run").stat().st_mode) == 0o700
    assert stat.S_IMODE((tmp_path / "run" / "ownership.json").stat().st_mode) == 0o600
    assert owned.owner == OWNER
    with pytest.raises(api.OwnershipRefused):
        api.OwnershipLedger.create(tmp_path / "run", owner=OWNER)


def test_registration_is_durable_before_the_create_ticket_exists(tmp_path: Path) -> None:
    api = module()
    owned = ledger(tmp_path)
    ticket = owned.intend("docker-network", OWNER)
    reopened = api.OwnershipLedger.open(tmp_path / "run")
    assert [(e.kind, e.key, e.state) for e in reopened.entries()] == [
        ("docker-network", OWNER, "intended")
    ]
    owned.bind(ticket, id="network-1")
    reopened = api.OwnershipLedger.open(tmp_path / "run")
    assert reopened.entries()[0].state == "created"
    assert reopened.identity("docker-network", OWNER) == {"id": "network-1"}


def test_binding_needs_the_ticket_issued_by_this_ledger(tmp_path: Path) -> None:
    api = module()
    first = ledger(tmp_path)
    second = api.OwnershipLedger.create(tmp_path / "other", owner="curie-commission-ba9876543210")
    ticket = second.intend("image-tag", "registry/example:owned")
    first.intend("image-tag", "registry/example:owned")
    with pytest.raises(api.OwnershipRefused):
        first.bind(ticket, id="sha256:" + "a" * 64)
    with pytest.raises(api.OwnershipRefused):
        first.bind(("image-tag", "registry/example:owned"), id="x")


def test_a_resource_is_registered_once_and_never_adopted(tmp_path: Path) -> None:
    api = module()
    owned = ledger(tmp_path)
    ticket = owned.intend("kubernetes", "/api/v1/namespaces/owned")
    with pytest.raises(api.OwnershipRefused):
        owned.intend("kubernetes", "/api/v1/namespaces/owned")
    owned.bind(ticket, uid="uid-1")
    # A same-name object observed later is never rebound to a new identity.
    with pytest.raises(api.OwnershipRefused):
        owned.bind(ticket, uid="uid-2")
    with pytest.raises(api.OwnershipRefused):
        owned.resolve("kubernetes", "/api/v1/namespaces/owned", uid="uid-2")
    assert owned.identity("kubernetes", "/api/v1/namespaces/owned") == {"uid": "uid-1"}


def test_ambiguous_create_stays_uncertain_until_resolved(tmp_path: Path) -> None:
    api = module()
    owned = ledger(tmp_path)
    ticket = owned.intend("kubernetes", "/api/v1/namespaces/owned/pods/control")
    owned.uncertain("kubernetes", "/api/v1/namespaces/owned/pods/control")
    assert owned.entries()[0].state == "uncertain"
    assert not owned.all_removed()
    with pytest.raises(api.OwnershipRefused):
        owned.bind(ticket, uid="uid-1")
    owned.resolve("kubernetes", "/api/v1/namespaces/owned/pods/control", uid="uid-1")
    assert owned.entries()[0].state == "created"


def test_intended_entry_is_never_marked_removed_without_an_identity_or_resolution(
    tmp_path: Path,
) -> None:
    api = module()
    owned = ledger(tmp_path)
    owned.intend("docker-volume", "volume-a")
    with pytest.raises(api.OwnershipRefused):
        owned.removed("docker-volume", "volume-a")
    owned.uncertain("docker-volume", "volume-a")
    owned.removed("docker-volume", "volume-a")
    assert owned.all_removed()


def test_removed_is_terminal(tmp_path: Path) -> None:
    api = module()
    owned = ledger(tmp_path)
    ticket = owned.intend("docker-container", "registry")
    owned.bind(ticket, id="container-1")
    owned.removed("docker-container", "registry")
    for action in (
        lambda: owned.removed("docker-container", "registry"),
        lambda: owned.resolve("docker-container", "registry", id="container-1"),
        lambda: owned.uncertain("docker-container", "registry"),
    ):
        with pytest.raises(api.OwnershipRefused):
            action()


def test_cleanup_order_is_newest_first_and_skips_removed(tmp_path: Path) -> None:
    owned = ledger(tmp_path)
    for kind, key in (
        ("docker-network", OWNER),
        ("minikube-profile", OWNER),
        ("image-tag", "owned:a"),
    ):
        owned.bind(owned.intend(kind, key), id=key)
    owned.removed("image-tag", "owned:a")
    assert [(e.kind, e.key) for e in owned.pending_reverse()] == [
        ("minikube-profile", OWNER),
        ("docker-network", OWNER),
    ]


@pytest.mark.parametrize(
    ("kind", "key"),
    [("unknown", "x"), ("image-tag", ""), ("image-tag", "x" * 513), ("image-tag", 7)],
)
def test_registration_refuses_unknown_kinds_and_bad_keys(
    tmp_path: Path, kind: str, key: Any
) -> None:
    api = module()
    with pytest.raises(api.OwnershipRefused):
        ledger(tmp_path).intend(kind, key)


@pytest.mark.parametrize("identity", [{}, {"uid": ""}, {"uid": 7}, {"": "x"}, {"uid": "x" * 513}])
def test_identity_values_are_nonempty_bounded_strings(
    tmp_path: Path, identity: dict[str, Any]
) -> None:
    api = module()
    owned = ledger(tmp_path)
    ticket = owned.intend("kubernetes", "/api/v1/namespaces/owned")
    with pytest.raises(api.OwnershipRefused):
        owned.bind(ticket, **identity)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda v: v.update(extra=1),
        lambda v: v.update(owner="minikube"),
        lambda v: v["entries"].append({"kind": "image-tag", "key": "a", "state": "gone"}),
        lambda v: v["entries"].append(
            {"kind": "image-tag", "key": "a", "state": "intended", "identity": {}, "x": 1}
        ),
        lambda v: v["entries"].extend(
            [{"kind": "image-tag", "key": "a", "state": "intended", "identity": {}}] * 2
        ),
        lambda v: v["entries"].append(
            {"kind": "image-tag", "key": "a", "state": "created", "identity": {}}
        ),
    ],
)
def test_open_refuses_a_ledger_that_is_not_closed(tmp_path: Path, mutate: Any) -> None:
    api = module()
    ledger(tmp_path)
    path = tmp_path / "run" / "ownership.json"
    value = json.loads(path.read_text())
    mutate(value)
    path.write_text(json.dumps(value))
    with pytest.raises(api.OwnershipRefused):
        api.OwnershipLedger.open(tmp_path / "run")


def test_refusal_messages_never_echo_input(tmp_path: Path) -> None:
    api = module()
    with pytest.raises(api.OwnershipRefused) as raised:
        ledger(tmp_path).intend("secret-kind", "secret-key")
    assert "secret" not in str(raised.value)


def test_argv_always_pins_the_private_kubeconfig_and_profile(tmp_path: Path) -> None:
    api = module()
    commands = api.ClusterCommands(OWNER, kubeconfig=tmp_path / "run" / "kubeconfig")
    assert commands.kubectl_argv("get", "pods", "-n", "owned") == [
        "kubectl",
        "--context",
        OWNER,
        "--kubeconfig",
        str(tmp_path / "run" / "kubeconfig"),
        "get",
        "pods",
        "-n",
        "owned",
    ]
    assert commands.minikube_argv("status") == ["minikube", "-p", OWNER, "status"]
    assert commands.environment_overrides() == {"KUBECONFIG": str(tmp_path / "run" / "kubeconfig")}


@pytest.mark.parametrize(
    "args",
    [
        ("--context", "prod"),
        ("--context=prod",),
        ("--kubeconfig", "/home/user/.kube/config"),
        ("--kubeconfig=/x",),
        ("--cluster=x",),
        ("--user=admin",),
        ("config", "use-context", "prod"),
        ("get", "pods", "--context=prod"),
    ],
)
def test_kubectl_refuses_caller_supplied_cluster_selection(
    tmp_path: Path, args: tuple[str, ...]
) -> None:
    api = module()
    commands = api.ClusterCommands(OWNER, kubeconfig=tmp_path / "kubeconfig")
    with pytest.raises(api.OwnershipRefused):
        commands.kubectl_argv(*args)


@pytest.mark.parametrize(
    "args", [("-p", "other"), ("--profile", "x"), ("--profile=x",), ("delete", "--all")]
)
def test_minikube_refuses_other_profiles_and_global_deletes(
    tmp_path: Path, args: tuple[str, ...]
) -> None:
    api = module()
    commands = api.ClusterCommands(OWNER, kubeconfig=tmp_path / "kubeconfig")
    with pytest.raises(api.OwnershipRefused):
        commands.minikube_argv(*args)


@pytest.mark.parametrize(
    "args",
    [("system", "prune"), ("image", "prune", "-f"), ("volume", "prune"), ("network", "prune")],
)
def test_docker_refuses_global_prune(tmp_path: Path, args: tuple[str, ...]) -> None:
    api = module()
    commands = api.ClusterCommands(OWNER, kubeconfig=tmp_path / "kubeconfig")
    with pytest.raises(api.OwnershipRefused):
        commands.docker_argv(*args)
    assert commands.docker_argv("network", "inspect", OWNER) == [
        "docker",
        "network",
        "inspect",
        OWNER,
    ]


@pytest.mark.parametrize(
    "argv",
    [
        ["--context", "prod"],
        ["--context=prod"],
        ["--kubeconfig", "/x"],
        ["--target", "t", "--kubeconfig=/x"],
        ["-p", "other"],
        ["--profile=x"],
    ],
)
def test_operator_arguments_refuse_cluster_selection(argv: list[str]) -> None:
    api = module()
    with pytest.raises(api.OwnershipRefused):
        api.refuse_cluster_selection(argv)


def test_operator_arguments_without_selection_pass() -> None:
    module().refuse_cluster_selection(["--target", "fixture/readonly", "--contextual-note"])


def test_commands_refuse_a_profile_outside_the_owned_namespace(tmp_path: Path) -> None:
    api = module()
    with pytest.raises(api.OwnershipRefused):
        api.ClusterCommands("minikube", kubeconfig=tmp_path / "kubeconfig")
