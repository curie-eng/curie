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
import re
import secrets
import stat
import subprocess
import urllib.request
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
_REGISTRY_PORT = 80
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


def _private_directory(path: Path) -> bool:
    """A real directory of the current user, closed to group and others."""
    try:
        status = os.lstat(path)
    except OSError:
        return False
    return (
        stat.S_ISDIR(status.st_mode)
        and status.st_uid == os.getuid()
        and status.st_mode & 0o077 == 0
    )


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

    def __init__(
        self,
        ledger: OwnershipLedger,
        *,
        runner: CommandRunner | None = None,
        kubeconfig: Path | None = None,
    ) -> None:
        """Bound to one kubeconfig in a private directory, @spec PROTECTED-COMMISSIONING-5.

        The administrator's cluster holds the admin kubeconfig minikube writes,
        in the administrator's own directory; the provisioner's holds the
        ServiceAccount kubeconfig the bootstrap mints in the provisioner's
        directory. Neither is given the other's path.
        """
        _require(type(ledger) is OwnershipLedger and valid_owner(ledger.owner))
        self.ledger = ledger
        self.owner = ledger.owner
        self.kubeconfig = kubeconfig or ledger.root / _KUBECONFIG
        _require(_private_directory(self.kubeconfig.parent))
        self.commands = ClusterCommands(self.owner, kubeconfig=self.kubeconfig)
        self.runner = runner or SubprocessRunner(self.commands, log=ledger.root / _LOG)

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-5."""
        return "LocalCluster()"

    def kubectl(self, *args: str, stdin: bytes | None = None, timeout: float = 60) -> bytes:
        """@spec PROTECTED-COMMISSIONING-5."""
        return self.runner.run(self.commands.kubectl_argv(*args), stdin=stdin, timeout=timeout)

    def _profile_absent(self) -> bool:
        """minikube exits 85 for a profile it does not know, and no node container exists."""
        known = self.runner.probe(self.commands.minikube_argv("status"), timeout=60)
        container = self.runner.probe(
            self.commands.docker_argv("inspect", "--type", "container", self.owner), timeout=60
        )
        return known == 85 and container != 0

    def _cluster_uid(self) -> str:
        raw = self.kubectl("get", "namespace", "kube-system", "--output", "json")
        uid = json.loads(raw)["metadata"]["uid"]
        _require(type(uid) is str and bool(uid))
        return str(uid)

    def start(self) -> None:
        """Confirm the profile is absent, register it, start it, bind its identity.

        A same-name profile or node container refuses before anything is
        registered, so no profile is adopted. The identity is the
        ``kube-system`` namespace UID, which a different or recreated cluster
        does not share. After an interrupted start the identity is bound when
        the cluster answers; otherwise the entry stays uncertain and is never
        deleted. @spec PROTECTED-COMMISSIONING-5.
        """
        _require(self._profile_absent())
        ticket = self.ledger.intend("minikube-profile", self.owner)
        try:
            self.runner.run(self.commands.minikube_argv("start", *_START), timeout=900)
            uid = self._cluster_uid()
        except Exception:  # noqa: BLE001  An interrupted start is uncertain, never created.
            self.ledger.uncertain("minikube-profile", self.owner)
            try:
                # The name was absent before this start, so a cluster at it is this one.
                self.ledger.resolve(
                    "minikube-profile", self.owner, owner=self.owner, uid=self._cluster_uid()
                )
            except Exception:  # noqa: BLE001  Unreadable: stays uncertain.
                pass
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

    def _remove_profile(self, state: str, identity: Mapping[str, str]) -> None:
        """Delete only the profile whose live cluster UID is the bound one.

        An uncertain entry has no bound identity: it is recorded absent when
        the profile is gone, and otherwise refuses for the operator.
        """
        if not self._profile_absent():
            _require(state == "created")
            _require(self._cluster_uid() == identity.get("uid"))
            self.runner.run(self.commands.minikube_argv("delete"), timeout=300)
        _require(self._profile_absent())
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
                self._remove_profile(entry.state, entry.identity)
            elif entry.kind == "image-tag":
                self._remove_image(entry.key, entry.state, entry.identity)
            else:
                _refuse()
        _require(self.ledger.all_removed())

    def enable_registry(self) -> None:
        """The profile's own image registry, for images pinned by digest.

        @spec PROTECTED-COMMISSIONING-5.
        """
        self.runner.run(self.commands.minikube_argv("addons", "enable", "registry"), timeout=300)
        for workload in ("deployment/registry", "daemonset/registry-proxy"):
            self.kubectl(
                "rollout", "status", workload, "--namespace", "kube-system", "--timeout", "300s",
                timeout=330,
            )  # fmt: skip

    def publish_image(self, name: str, *, dockerfile: Path, context: Path) -> str:
        """Build an image from this checkout and return its registry-pinned reference.

        The host tag is registered before the build and removed with the
        installation. The image is pushed to the profile's registry, so the
        returned ``repository@sha256:...`` is a manifest digest the node pulls.
        @spec PROTECTED-COMMISSIONING-5.
        """
        _require(re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", name) is not None)
        repository = f"localhost:5000/{self.owner}/{name}"
        tag = repository + ":" + secrets.token_hex(8)
        ticket = self.ledger.intend("image-tag", tag)
        try:
            self.runner.run(
                self.commands.docker_argv("build", "-t", tag, "-f", str(dockerfile), str(context)),
                timeout=1800,
            )
            image_id = (
                self.runner.run(
                    self.commands.docker_argv("image", "inspect", "--format", "{{.Id}}", tag),
                    timeout=60,
                )
                .decode()
                .strip()
            )
            _require(re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is not None)
        except Exception:  # noqa: BLE001  An interrupted build may have tagged the image.
            self.ledger.uncertain("image-tag", tag)
            _refuse()
        self.ledger.bind(ticket, owner=self.owner, id=image_id)
        self.runner.run(self.commands.minikube_argv("image", "load", tag), timeout=900)
        self.runner.run(self.commands.minikube_argv("image", "push", tag), timeout=900)
        reference = repository + "@" + self._registry_digest(repository, tag.rsplit(":", 1)[1])
        _require(
            re.fullmatch(re.escape(repository) + r"@sha256:[0-9a-f]{64}", reference) is not None
        )
        return reference

    def _registry_digest(self, repository: str, tag: str) -> str:
        """The manifest digest the profile's registry serves for a pushed tag.

        Read over this installation's own port-forward to the registry Service;
        it is the digest the node pulls. @spec PROTECTED-COMMISSIONING-5.
        """
        from .commissioning_local_broker import _start_forward, _stop

        argv = self.commands.kubectl_argv(
            "port-forward",
            "service/registry",
            f":{_REGISTRY_PORT}",
            "--namespace",
            "kube-system",
        )
        process, port = _start_forward(
            argv, self.commands.environment(os.environ), target_port=_REGISTRY_PORT
        )
        try:
            path = repository.split("/", 1)[1]
            request = urllib.request.Request(  # noqa: S310  loopback forward to the owned registry.
                f"http://127.0.0.1:{port}/v2/{path}/manifests/{tag}",
                method="HEAD",
                headers={
                    "Accept": "application/vnd.docker.distribution.manifest.v2+json, "
                    "application/vnd.oci.image.manifest.v1+json"
                },
            )
            with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
                digest = response.headers.get("Docker-Content-Digest", "")
        except Exception:  # noqa: BLE001  One redacted refusal.
            _refuse()
        finally:
            _stop(process)
        _require(re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is not None)
        return str(digest)

    def _remove_image(self, key: str, state: str, identity: Mapping[str, str]) -> None:
        # Only the exact recorded tag is untagged; the image layers stay shared.
        argv = self.commands.docker_argv("image", "inspect", "--format", "{{.Id}}", key)
        if self.runner.probe(argv, timeout=60) == 0:
            if state == "created":
                observed = self.runner.run(argv, timeout=60).decode().strip()
                _require(observed == identity.get("id"))
            self.runner.run(self.commands.docker_argv("image", "rm", key), timeout=120)
        _require(self.runner.probe(argv, timeout=60) != 0)
        self.ledger.removed("image-tag", key)

    def objects(self) -> Iterator[str]:
        """Ledger keys of created Kubernetes objects, @spec PROTECTED-COMMISSIONING-5."""
        for entry in self.ledger.entries():
            if entry.kind == "kubernetes" and entry.state == "created":
                yield entry.key
