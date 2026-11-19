"""Owned-namespace helpers for the factory runner toolchain live proof.

The proof creates a namespace, runs a pod in it and deletes the namespace. This
module makes sure the proof only ever deletes a namespace it created itself and
only reports cleanup as done when the cluster says the namespace is gone.

Every kubectl call goes through `kubectl`, which always passes `--context`.
"""

from __future__ import annotations

import re
import subprocess
import tempfile
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Literal

CONTEXT_ENV = "CURIE_FACTORY_KUBE_CONTEXT"
IDENTITY_ENV = "CURIE_FACTORY_TEST_IDENTITY"
PROOF_ENV = "CURIE_FACTORY_RUNNER_PROOF"
PROOF_REQUIRED = "required"

NAMESPACE_PREFIX = "toolchain-proof-"
_DNS_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_MAX_LABEL = 63

FailureKind = Literal["not_found", "authorization", "transport"]


class ProofConfigError(Exception):
    """The proof was not given the inputs it needs."""


class NamespaceExists(Exception):
    """The namespace already exists, so this run does not own it."""


class NamespaceProbeError(Exception):
    """The namespace could not be probed or created for a reason other than existence."""


class CleanupUnverified(Exception):
    """The cluster did not confirm that the owned namespace is gone."""


@dataclass(frozen=True)
class ProofTarget:
    context: str
    identity: str
    namespace: str


@dataclass(frozen=True)
class OwnedNamespace:
    context: str
    name: str


def resolve_target(env: Mapping[str, str]) -> ProofTarget:
    context = env.get(CONTEXT_ENV, "").strip()
    identity = env.get(IDENTITY_ENV, "").strip()
    missing = [
        name for name, value in ((CONTEXT_ENV, context), (IDENTITY_ENV, identity)) if not value
    ]
    if missing:
        raise ProofConfigError("set " + " and ".join(missing) + " for the factory runner proof")
    namespace = NAMESPACE_PREFIX + identity
    if len(namespace) > _MAX_LABEL or not _DNS_LABEL.match(namespace):
        raise ProofConfigError(
            f"{IDENTITY_ENV} {identity!r} does not form a valid namespace name "
            f"({namespace!r}): use lowercase letters, digits and hyphens, "
            f"at most {_MAX_LABEL - len(NAMESPACE_PREFIX)} characters"
        )
    return ProofTarget(context=context, identity=identity, namespace=namespace)


def kubectl(
    context: str,
    *args: str,
    timeout: int = 180,
    input_text: str | None = None,
    stdin: IO[bytes] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["kubectl", "--context", context, *args],
        input=input_text,
        stdin=stdin,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
    )


def classify_failure(result: subprocess.CompletedProcess[str]) -> FailureKind:
    """Classify a non-zero kubectl read by the API status reason in its stderr."""
    stderr = result.stderr or ""
    if "(NotFound)" in stderr:
        return "not_found"
    if "(Forbidden)" in stderr or "(Unauthorized)" in stderr:
        return "authorization"
    return "transport"


def _detail(result: subprocess.CompletedProcess[str]) -> str:
    return f"{classify_failure(result)}: {(result.stderr or '').strip()}"


def _get_namespace(context: str, name: str) -> subprocess.CompletedProcess[str]:
    return kubectl(context, "get", "namespace", name, "-o", "name", timeout=30)


def create_owned_namespace(target: ProofTarget) -> OwnedNamespace:
    name = target.namespace
    probe = _get_namespace(target.context, name)
    if probe.returncode == 0:
        raise NamespaceExists(f"namespace {name} already exists and is not owned by this run")
    if classify_failure(probe) != "not_found":
        raise NamespaceProbeError(f"could not probe namespace {name} ({_detail(probe)})")
    created = kubectl(target.context, "create", "namespace", name)
    if created.returncode != 0:
        stderr = (created.stderr or "").strip()
        if "AlreadyExists" in stderr:
            raise NamespaceExists(f"namespace {name} appeared during creation: {stderr}")
        raise NamespaceProbeError(f"could not create namespace {name} ({_detail(created)})")
    return OwnedNamespace(context=target.context, name=name)


def delete_owned_namespace(
    owned: OwnedNamespace, *, deadline_seconds: float = 180, poll_seconds: float = 2
) -> None:
    deleted = kubectl(owned.context, "delete", "namespace", owned.name, "--wait=false")
    if deleted.returncode != 0 and classify_failure(deleted) != "not_found":
        raise CleanupUnverified(f"delete namespace {owned.name} failed ({_detail(deleted)})")
    deadline = time.monotonic() + deadline_seconds
    while True:
        remaining = _get_namespace(owned.context, owned.name)
        if remaining.returncode != 0:
            if classify_failure(remaining) == "not_found":
                return
            raise CleanupUnverified(
                f"could not verify namespace {owned.name} is gone ({_detail(remaining)})"
            )
        if time.monotonic() >= deadline:
            raise CleanupUnverified(f"namespace {owned.name} remained after teardown")
        time.sleep(poll_seconds)


def copy_checkout(
    owned: OwnedNamespace,
    pod: str,
    container: str,
    repo_root: Path,
    *,
    archive_timeout: int = 300,
    copy_timeout: int = 600,
) -> None:
    """Stream `git archive HEAD` of repo_root into /workspace of the pod container."""
    with tempfile.TemporaryDirectory(prefix="curie-factory-proof-") as directory:
        archive = Path(directory) / "checkout.tar"
        packed = subprocess.run(
            ["git", "archive", "--format=tar", "--output", str(archive), "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
            timeout=archive_timeout,
        )
        if packed.returncode != 0:
            raise RuntimeError(
                f"archive tracked repository checkout exited {packed.returncode}:\n"
                f"stdout: {packed.stdout}\nstderr: {packed.stderr}"
            )
        with archive.open("rb") as source:
            copied = kubectl(
                owned.context,
                "-n",
                owned.name,
                "exec",
                "-i",
                pod,
                "-c",
                container,
                "--",
                "tar",
                "--no-overwrite-dir",
                "--touch",
                "--no-same-permissions",
                "-xf",
                "-",
                "-C",
                "/workspace",
                timeout=copy_timeout,
                stdin=source,
            )
        if copied.returncode != 0:
            raise RuntimeError(
                f"copy checkout into runner pod exited {copied.returncode}:\n"
                f"stdout: {copied.stdout}\nstderr: {copied.stderr}"
            )
