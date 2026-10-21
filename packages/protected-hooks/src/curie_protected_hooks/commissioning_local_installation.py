"""Local commissioning installation on an owned minikube profile, @spec PROTECTED-COMMISSIONING-5.

Design decision 1 makes the provisioner the only creator of the protected
lane. This module is its local substrate: it starts a minikube profile named
after the installation owner, with a private kubeconfig, and creates and
removes Kubernetes objects only through the ownership ledger. Every object is
registered before it is created, created with ``kubectl create`` so a
same-name object refuses instead of being adopted, and bound to its observed
UID. Removal acts only on a recorded UID carrying the owner label and records
absence only after an independent read finds the object gone.

Every command goes through ``ClusterCommands``: the profile and kubeconfig are
the installation's own, and ambient cluster or daemon selection is dropped.
Nothing here reads the operator's kubeconfig or current context.
"""

from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NoReturn, Protocol

from .commissioning_local_ownership import ClusterCommands, OwnershipLedger, valid_owner

__all__ = [
    "OWNER_LABEL",
    "CommandRunner",
    "InstallationRefused",
    "LocalCluster",
    "SubprocessRunner",
    "object_key",
]

# @spec PROTECTED-COMMISSIONING-5
OWNER_LABEL = "commissioning.curietech.ai/owner"
_KUBECONFIG = "kubeconfig"
_LOG = "commands.log"
_DOCKER_CONFIG = "docker"
# Pinned profile shape: one node, the Docker driver and a fixed control plane.
KUBERNETES_VERSION = "v1.35.1"
_START = (
    "--driver",
    "docker",
    "--kubernetes-version",
    KUBERNETES_VERSION,
    "--memory",
    "3072",
    "--cpus",
    "2",
    "--wait",
    "all",
    "--interactive=false",
)


class InstallationRefused(RuntimeError):
    """Redacted installation refusal, @spec PROTECTED-COMMISSIONING-5."""

    def __init__(self) -> None:
        """Never echo an argument, object or command output, @spec PROTECTED-COMMISSIONING-5."""
        super().__init__("Commissioning local installation refused")


def _refuse() -> NoReturn:
    raise InstallationRefused() from None


def _require(condition: bool) -> None:
    if not condition:
        _refuse()


class CommandRunner(Protocol):
    """Runs one built argv and returns its standard output, @spec PROTECTED-COMMISSIONING-5."""

    def run(self, argv: Sequence[str], *, stdin: bytes | None = None, timeout: float) -> bytes:
        """Raise ``InstallationRefused`` on a nonzero exit or timeout."""
        ...

    def probe(self, argv: Sequence[str], *, timeout: float) -> int:
        """The exit code of a read-only check; a timeout refuses."""
        ...


class SubprocessRunner:
    """Run argv in the installation's pinned environment, @spec PROTECTED-COMMISSIONING-5.

    Standard error and the exit code go to a private log in the ledger
    directory; standard input, which can carry Secret material, is never
    logged. Docker reads an owned, empty client configuration, so the
    operator's registry credentials and credential helpers are never used.
    """

    def __init__(self, commands: ClusterCommands, *, log: Path) -> None:
        """@spec PROTECTED-COMMISSIONING-5."""
        docker = log.parent / _DOCKER_CONFIG
        docker.mkdir(mode=0o700, exist_ok=True)
        _require(docker.is_dir() and not docker.is_symlink())
        config = docker / "config.json"
        if not config.exists():
            descriptor = os.open(
                config, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600
            )
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                stream.write("{}\n")
        self._environment = commands.environment(os.environ)
        self._environment["DOCKER_CONFIG"] = str(docker)
        self._log = log

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-5."""
        return "SubprocessRunner()"

    def _record(self, argv: Sequence[str], code: int | str, stderr: bytes) -> None:
        descriptor = os.open(
            self._log, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600
        )
        with os.fdopen(descriptor, "a", encoding="utf-8") as stream:
            command = " ".join([*argv[:1], *[a for a in argv[1:] if not a.startswith("/")][:6]])
            stream.write(f"{command} -> {code}\n")
            if stderr:
                stream.write(stderr.decode("utf-8", "replace")[-4000:] + "\n")

    def probe(self, argv: Sequence[str], *, timeout: float) -> int:
        """@spec PROTECTED-COMMISSIONING-5."""
        try:
            completed = subprocess.run(  # noqa: S603  argv comes from ClusterCommands' allowlists.
                list(argv), capture_output=True, timeout=timeout, env=self._environment, check=False
            )
        except (OSError, subprocess.TimeoutExpired):
            self._record(argv, "timeout", b"")
            _refuse()
        self._record(argv, completed.returncode, completed.stderr)
        return completed.returncode

    def run(self, argv: Sequence[str], *, stdin: bytes | None = None, timeout: float) -> bytes:
        """@spec PROTECTED-COMMISSIONING-5."""
        try:
            completed = subprocess.run(  # noqa: S603  argv comes from ClusterCommands' allowlists.
                list(argv),
                input=stdin,
                capture_output=True,
                timeout=timeout,
                env=self._environment,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            self._record(argv, "timeout", b"")
            _refuse()
        self._record(argv, completed.returncode, completed.stderr)
        _require(completed.returncode == 0)
        return completed.stdout


def object_key(manifest: Mapping[str, Any]) -> str:
    """The ledger key of one Kubernetes object, @spec PROTECTED-COMMISSIONING-5."""
    metadata = manifest["metadata"]
    namespace = metadata.get("namespace", "")
    return "/".join((manifest["apiVersion"], manifest["kind"], namespace, metadata["name"]))


def _resource(key: str) -> tuple[str, str, str]:
    """``kind/name`` and namespace for kubectl from a ledger key."""
    api_version, kind, namespace, name = key.rsplit("/", 3)
    group = api_version.split("/")[0] if "/" in api_version else ""
    resource = kind.lower() + ("." + group if group else "")
    return resource + "/" + name, namespace, api_version


def _namespace_args(namespace: str) -> tuple[str, ...]:
    return ("--namespace", namespace) if namespace else ()


@dataclass(frozen=True, slots=True)
class _Observed:
    uid: str
    owner: str


def _observed(raw: bytes, key: str) -> _Observed:
    value = json.loads(raw)
    _require(type(value) is dict and object_key(value) == key)
    metadata = value["metadata"]
    uid, labels = metadata.get("uid"), metadata.get("labels") or {}
    _require(type(labels) is dict)
    owner = labels.get(OWNER_LABEL)
    if type(uid) is not str or not uid or type(owner) is not str:
        _refuse()
    return _Observed(uid, owner)


class LocalCluster:
    """One installation's minikube profile and the objects it owns.

    The profile name is the ledger owner, so ``ClusterCommands`` pins both the
    minikube profile and the kubectl context to it. @spec PROTECTED-COMMISSIONING-5.
    """

    def __init__(self, ledger: OwnershipLedger, *, runner: CommandRunner | None = None) -> None:
        """@spec PROTECTED-COMMISSIONING-5."""
        _require(type(ledger) is OwnershipLedger and valid_owner(ledger.owner))
        self.ledger = ledger
        self.owner = ledger.owner
        self.kubeconfig = ledger.root / _KUBECONFIG
        self.commands = ClusterCommands(self.owner, kubeconfig=self.kubeconfig)
        self.runner = runner or SubprocessRunner(self.commands, log=ledger.root / _LOG)

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-5."""
        return "LocalCluster()"

    def kubectl(self, *args: str, stdin: bytes | None = None, timeout: float = 60) -> bytes:
        """@spec PROTECTED-COMMISSIONING-5."""
        return self.runner.run(self.commands.kubectl_argv(*args), stdin=stdin, timeout=timeout)

    def start(self) -> None:
        """Register, then start the profile; bind it to the cluster's own identity.

        The identity is the ``kube-system`` namespace UID, which a different or
        recreated cluster does not share. @spec PROTECTED-COMMISSIONING-5.
        """
        ticket = self.ledger.intend("minikube-profile", self.owner)
        try:
            self.runner.run(self.commands.minikube_argv("start", *_START), timeout=900)
            raw = self.kubectl("get", "namespace", "kube-system", "--output", "json")
            uid = json.loads(raw)["metadata"]["uid"]
            _require(type(uid) is str and bool(uid))
        except Exception:  # noqa: BLE001  An interrupted start is uncertain, never created.
            self.ledger.uncertain("minikube-profile", self.owner)
            _refuse()
        self.ledger.bind(ticket, owner=self.owner, uid=uid)

    def create(self, manifest: Mapping[str, Any], *, timeout: float = 60) -> str:
        """Register, create and bind one labelled object; return its UID.

        ``kubectl create`` refuses an existing name, so nothing is adopted.
        @spec PROTECTED-COMMISSIONING-5.
        """
        labels = manifest.get("metadata", {}).get("labels", {})
        _require(labels.get(OWNER_LABEL) == self.owner)
        key = object_key(manifest)
        ticket = self.ledger.intend("kubernetes", key)
        body = json.dumps(manifest, sort_keys=True).encode()
        try:
            raw = self.kubectl(
                "create", "--filename", "-", "--output", "json", stdin=body, timeout=timeout
            )
            observed = _observed(raw, key)
            _require(observed.owner == self.owner)
        except Exception:  # noqa: BLE001  An ambiguous create is recorded, never retried blind.
            self.ledger.uncertain("kubernetes", key)
            _refuse()
        self.ledger.bind(ticket, owner=observed.owner, uid=observed.uid)
        return observed.uid

    def read(self, key: str) -> dict[str, Any] | None:
        """The live object at a ledger key, or None when absent, @spec PROTECTED-COMMISSIONING-5."""
        resource, namespace, _ = _resource(key)
        raw = self.kubectl(
            "get", resource, *_namespace_args(namespace), "--ignore-not-found", "--output", "json"
        )
        if not raw.strip():
            return None
        value: dict[str, Any] = json.loads(raw)
        return value

    def _remove_object(self, key: str, state: str, identity: Mapping[str, str]) -> None:
        live = self.read(key)
        if live is not None:
            observed = _observed(json.dumps(live).encode(), key)
            if state == "uncertain":
                # Only an object carrying this owner's label is resolved and removed.
                self.ledger.resolve("kubernetes", key, owner=observed.owner, uid=observed.uid)
            else:
                _require(observed.uid == identity.get("uid") and observed.owner == self.owner)
            resource, namespace, _ = _resource(key)
            self.kubectl(
                "delete", resource, *_namespace_args(namespace), "--timeout", "120s", timeout=180
            )
            _require(self.read(key) is None)
        self.ledger.removed("kubernetes", key)

    def _remove_profile(self) -> None:
        self.runner.run(self.commands.minikube_argv("delete"), timeout=300)
        # minikube exits 85 for a profile it does not know; any other answer is not absence.
        _require(self.runner.probe(self.commands.minikube_argv("status"), timeout=60) == 85)
        _require(
            self.runner.probe(
                self.commands.docker_argv("inspect", "--type", "container", self.owner), timeout=60
            )
            != 0
        )
        self.ledger.removed("minikube-profile", self.owner)

    def remove_all(self) -> None:
        """Remove every owned resource, newest first, and confirm absence.

        A foreign object at a recorded name refuses and is left in place.
        @spec PROTECTED-COMMISSIONING-5.
        """
        for entry in self.ledger.pending_reverse():
            if entry.state == "intended":
                # A create that never returned may still have happened.
                self.ledger.uncertain(entry.kind, entry.key)
                entry = next(e for e in self.ledger.entries() if e.key == entry.key)
            if entry.kind == "kubernetes":
                self._remove_object(entry.key, entry.state, entry.identity)
            elif entry.kind == "minikube-profile":
                self._remove_profile()
            else:
                _refuse()
        _require(self.ledger.all_removed())

    def objects(self) -> Iterator[str]:
        """Ledger keys of created Kubernetes objects, @spec PROTECTED-COMMISSIONING-5."""
        for entry in self.ledger.entries():
            if entry.kind == "kubernetes" and entry.state == "created":
                yield entry.key
