"""Local installation: register before create, never adopt, remove only owned UIDs.

@spec PROTECTED-COMMISSIONING-5. A fake runner stands in for kubectl and
minikube; the cluster test drives the same code against a real profile.
"""

from __future__ import annotations

import importlib
import importlib.util
import json
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import pytest

OWNER = "curie-commission-0123456789ab"
OTHER = "curie-commission-ba9876543210"


def module() -> Any:
    name = "curie_protected_hooks.commissioning_local_installation"
    assert importlib.util.find_spec(name) is not None, "local installation not implemented"
    return importlib.import_module(name)


def ownership() -> Any:
    return importlib.import_module("curie_protected_hooks.commissioning_local_ownership")


class FakeRunner:
    """Records argv with the ledger state seen at call time; answers by subcommand."""

    def __init__(self, ledger: Any) -> None:
        self.ledger = ledger
        self.calls: list[tuple[list[str], list[tuple[str, str, str]]]] = []
        self.objects: dict[str, dict[str, Any]] = {}
        self.fail: Callable[[list[str]], bool] = lambda argv: False
        self.status = 85
        self.uid = 0

    def _seen(self) -> list[tuple[str, str, str]]:
        return [(e.kind, e.key, e.state) for e in self.ledger.entries()]

    def probe(self, argv: Sequence[str], *, timeout: float) -> int:
        self.calls.append((list(argv), self._seen()))
        return self.status if argv[0] == "minikube" else 1

    def run(self, argv: Sequence[str], *, stdin: bytes | None = None, timeout: float) -> bytes:
        argv = list(argv)
        self.calls.append((argv, self._seen()))
        if self.fail(argv):
            raise module().InstallationRefused()
        if argv[0] == "minikube":
            return b""
        verb = argv[5]
        if verb == "create":
            assert stdin is not None
            value = json.loads(stdin)
            self.uid += 1
            value["metadata"]["uid"] = f"uid-{self.uid}"
            self.objects[module().object_key(value)] = value
            return json.dumps(value).encode()
        if verb == "get" and argv[6:8] == ["namespace", "kube-system"]:
            return json.dumps({"metadata": {"uid": "cluster-uid"}}).encode()
        if verb == "get":
            for key, value in self.objects.items():
                resource = key.split("/")[-3].lower() + "/" + key.split("/")[-1]
                if resource == argv[6]:
                    return json.dumps(value).encode()
            return b""
        if verb == "delete":
            for key in list(self.objects):
                if key.split("/")[-3].lower() + "/" + key.split("/")[-1] == argv[6]:
                    del self.objects[key]
            return b""
        raise AssertionError(argv)


def installation(tmp_path: Path) -> tuple[Any, FakeRunner]:
    ledger = ownership().OwnershipLedger.create(tmp_path / "run", owner=OWNER)
    runner = FakeRunner(ledger)
    return module().LocalCluster(ledger, runner=runner), runner


def config_map(name: str = "fixture", owner: str = OWNER) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": "owned",
            "labels": {module().OWNER_LABEL: owner},
        },
        "data": {"a": "b"},
    }


def test_every_command_is_pinned_to_the_installation_profile(tmp_path: Path) -> None:
    cluster, runner = installation(tmp_path)
    cluster.start()
    cluster.create(config_map())
    kubeconfig = str(tmp_path / "run" / "kubeconfig")
    for argv, _ in runner.calls:
        if argv[0] == "kubectl":
            assert argv[1:5] == ["--context", OWNER, "--kubeconfig", kubeconfig]
        else:
            assert argv[:3] == ["minikube", "-p", OWNER]
    assert cluster.commands.environment({"KUBECONFIG": "/home/x/.kube/config"})["KUBECONFIG"] == (
        kubeconfig
    )


def test_start_registers_the_profile_before_minikube_runs(tmp_path: Path) -> None:
    cluster, runner = installation(tmp_path)
    cluster.start()
    argv, seen = runner.calls[0]
    assert argv[3] == "start" and "--kubernetes-version" in argv
    assert seen == [("minikube-profile", OWNER, "intended")]
    entry = cluster.ledger.entries()[0]
    assert entry.state == "created" and entry.identity == {"owner": OWNER, "uid": "cluster-uid"}


def test_failed_start_is_uncertain_not_created(tmp_path: Path) -> None:
    cluster, runner = installation(tmp_path)
    runner.fail = lambda argv: argv[0] == "minikube"
    with pytest.raises(module().InstallationRefused):
        cluster.start()
    assert [e.state for e in cluster.ledger.entries()] == ["uncertain"]


def test_create_registers_intent_first_and_binds_the_observed_uid(tmp_path: Path) -> None:
    cluster, runner = installation(tmp_path)
    uid = cluster.create(config_map())
    argv, seen = runner.calls[-1]
    assert argv[5:] == ["create", "--filename", "-", "--output", "json"]
    assert seen == [("kubernetes", "v1/ConfigMap/owned/fixture", "intended")]
    assert cluster.ledger.identity("kubernetes", "v1/ConfigMap/owned/fixture") == {
        "owner": OWNER,
        "uid": uid,
    }


@pytest.mark.parametrize("owner", [OTHER, None])
def test_create_refuses_an_object_without_this_owner_label(tmp_path: Path, owner: Any) -> None:
    cluster, runner = installation(tmp_path)
    manifest = config_map()
    if owner is None:
        del manifest["metadata"]["labels"]
    else:
        manifest["metadata"]["labels"][module().OWNER_LABEL] = owner
    with pytest.raises(module().InstallationRefused):
        cluster.create(manifest)
    assert runner.calls == [] and cluster.ledger.entries() == ()


def test_failed_create_is_uncertain_and_a_second_create_refuses(tmp_path: Path) -> None:
    cluster, runner = installation(tmp_path)
    runner.fail = lambda argv: "create" in argv
    with pytest.raises(module().InstallationRefused):
        cluster.create(config_map())
    assert [e.state for e in cluster.ledger.entries()] == ["uncertain"]
    runner.fail = lambda argv: False
    with pytest.raises(ownership().OwnershipRefused):
        cluster.create(config_map())


def test_remove_all_deletes_owned_objects_newest_first_and_confirms_absence(
    tmp_path: Path,
) -> None:
    cluster, runner = installation(tmp_path)
    cluster.start()
    cluster.create(config_map("first"))
    cluster.create(config_map("second"))
    cluster.remove_all()
    deletes = [argv[6] for argv, _ in runner.calls if argv[0] == "kubectl" and argv[5] == "delete"]
    assert deletes == ["configmap/second", "configmap/first"]
    assert runner.calls[-1][0][:4] == ["docker", "inspect", "--type", "container"]
    assert cluster.ledger.all_removed() and runner.objects == {}


def test_remove_all_refuses_a_foreign_object_at_a_recorded_name(tmp_path: Path) -> None:
    cluster, runner = installation(tmp_path)
    cluster.create(config_map())
    runner.objects["v1/ConfigMap/owned/fixture"]["metadata"]["uid"] = "replaced"
    with pytest.raises(module().InstallationRefused):
        cluster.remove_all()
    assert "v1/ConfigMap/owned/fixture" in runner.objects
    assert not any(argv[5:6] == ["delete"] for argv, _ in runner.calls if argv[0] == "kubectl")


def test_uncertain_create_is_resolved_only_by_the_owner_label(tmp_path: Path) -> None:
    cluster, runner = installation(tmp_path)
    original = runner.run

    def created_then_lost(argv: Sequence[str], **kwargs: Any) -> bytes:
        result = original(argv, **kwargs)
        if "create" in argv:
            raise module().InstallationRefused()
        return result

    runner.run = created_then_lost  # type: ignore[method-assign]
    with pytest.raises(module().InstallationRefused):
        cluster.create(config_map())
    runner.run = original  # type: ignore[method-assign]
    runner.objects["v1/ConfigMap/owned/fixture"]["metadata"]["labels"][module().OWNER_LABEL] = OTHER
    with pytest.raises(ownership().OwnershipRefused):
        cluster.remove_all()
    assert "v1/ConfigMap/owned/fixture" in runner.objects
    runner.objects["v1/ConfigMap/owned/fixture"]["metadata"]["labels"][module().OWNER_LABEL] = OWNER
    cluster.remove_all()
    assert runner.objects == {} and cluster.ledger.all_removed()


def test_profile_absence_requires_minikube_to_report_it_unknown(tmp_path: Path) -> None:
    cluster, runner = installation(tmp_path)
    cluster.start()
    runner.status = 7
    with pytest.raises(module().InstallationRefused):
        cluster.remove_all()
    assert not cluster.ledger.all_removed()
    runner.status = 85
    cluster.remove_all()
    assert cluster.ledger.all_removed()


def test_refusals_and_reprs_are_redacted(tmp_path: Path) -> None:
    cluster, _ = installation(tmp_path)
    assert repr(cluster) == "LocalCluster()"
    assert str(module().InstallationRefused()) == "Commissioning local installation refused"


def test_real_runner_uses_an_owned_empty_docker_configuration(tmp_path: Path) -> None:
    """The operator's registry credentials and credential helpers are never read."""
    ledger = ownership().OwnershipLedger.create(tmp_path / "run", owner=OWNER)
    cluster = module().LocalCluster(ledger)
    environment = cluster.runner._environment
    docker = tmp_path / "run" / "docker"
    assert environment["DOCKER_CONFIG"] == str(docker)
    assert (docker / "config.json").read_text() == "{}\n"
    assert oct(docker.stat().st_mode & 0o777) == "0o700"
    assert environment["KUBECONFIG"] == str(tmp_path / "run" / "kubeconfig")
    assert not any(k.startswith(("MINIKUBE_HOME", "DOCKER_HOST")) for k in environment)
