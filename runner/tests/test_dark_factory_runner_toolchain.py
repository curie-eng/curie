"""Proof that the factory runner layer installs this repository's toolchains.

Set CURIE_FACTORY_RUNNER_IMAGE to an image reachable by the cluster and
CURIE_FACTORY_KUBE_CONTEXT to its explicit context. Set
CURIE_FACTORY_TEST_IDENTITY to a caller-chosen identity (lowercase letters,
digits and hyphens); the proof runs in the namespace
`toolchain-proof-<identity>`, which it must create itself. A namespace that
already exists fails the proof and is never adopted or deleted, and teardown
fails unless the cluster reports the namespace as NotFound. Set
CURIE_FACTORY_RUNNER_PROOF=required when this proof is a required gate; a missing
image, or an unreachable cluster then fails.
"""

from __future__ import annotations

import ipaddress
import json
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "tools/runner-toolchain-proof"))

import toolchain_proof  # noqa: E402

SKILL = REPO_ROOT / "examples/dark-factory/skills/implement-issue/SKILL.md"
POD = "factory-runner-toolchain"
IMAGE_ENV = "CURIE_FACTORY_RUNNER_IMAGE"
PROOF_ENV = "CURIE_FACTORY_RUNNER_PROOF"
REGISTRY_CIDRS_ENV = "CURIE_FACTORY_REGISTRY_CIDRS"


def _unavailable(reason: str) -> None:
    if os.environ.get(PROOF_ENV) == "required":
        pytest.fail(reason)
    pytest.skip(reason)


def _assert_ok(result: subprocess.CompletedProcess[str], command: str) -> str:
    assert result.returncode == 0, (
        f"{command} exited {result.returncode}:\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    return result.stdout.strip()


def _pod_manifest(image: str, namespace: str) -> dict[str, object]:
    security = {
        "allowPrivilegeEscalation": False,
        "readOnlyRootFilesystem": True,
        "capabilities": {"drop": ["ALL"]},
    }
    mounts = [
        {"name": "scratch", "mountPath": "/tmp"},
        {"name": "home", "mountPath": "/home/runner"},
    ]
    container = {
        "image": image,
        "imagePullPolicy": "IfNotPresent",
        "command": ["sh", "-c", "sleep 3600"],
        "securityContext": security,
    }
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": POD,
            "namespace": namespace,
            "labels": {"app": POD},
        },
        "spec": {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "securityContext": {
                "runAsNonRoot": True,
                "runAsUser": 1000,
                "runAsGroup": 1000,
                "fsGroup": 1000,
            },
            "containers": [
                {
                    **container,
                    "name": "staging",
                    "volumeMounts": [
                        *mounts,
                        {"name": "workspace", "mountPath": "/workspace"},
                    ],
                },
                {
                    **container,
                    "name": "runner",
                    "volumeMounts": [
                        *mounts,
                        {"name": "workspace", "mountPath": "/workspace"},
                    ],
                },
            ],
            "volumes": [
                {"name": "workspace", "emptyDir": {}},
                {"name": "scratch", "emptyDir": {}},
                {"name": "home", "emptyDir": {}},
            ],
        },
    }


def _registry_cidrs() -> list[str]:
    raw = os.environ.get(REGISTRY_CIDRS_ENV, "").strip()
    if not raw:
        _unavailable(f"set {REGISTRY_CIDRS_ENV} to the current registry CIDRs")
    cidrs: list[str] = []
    for value in raw.split(","):
        try:
            network = ipaddress.ip_network(value.strip(), strict=True)
        except ValueError as exc:
            pytest.fail(f"invalid {REGISTRY_CIDRS_ENV} entry {value!r}: {exc}")
        if network.prefixlen == 0:
            pytest.fail(f"{REGISTRY_CIDRS_ENV} must not allow all HTTPS destinations")
        cidrs.append(str(network))
    return cidrs


def _network_policies(cidrs: list[str], namespace: str) -> list[dict[str, Any]]:
    base = {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
    }
    return [
        {
            **base,
            "metadata": {"name": "default-deny", "namespace": namespace},
            "spec": {
                "podSelector": {},
                "policyTypes": ["Ingress", "Egress"],
            },
        },
        {
            **base,
            "metadata": {"name": "factory-registry-egress", "namespace": namespace},
            "spec": {
                "podSelector": {"matchLabels": {"app": POD}},
                "policyTypes": ["Egress"],
                "egress": [
                    {
                        "to": [
                            {
                                "namespaceSelector": {
                                    "matchLabels": {
                                        "kubernetes.io/metadata.name": "kube-system"
                                    }
                                }
                            }
                        ],
                        "ports": [
                            {"protocol": "UDP", "port": 53},
                            {"protocol": "TCP", "port": 53},
                        ],
                    },
                    {
                        "to": [{"ipBlock": {"cidr": cidr}} for cidr in cidrs],
                        "ports": [{"protocol": "TCP", "port": 443}],
                    },
                ],
            },
        },
    ]


def _exec(owned: toolchain_proof.OwnedNamespace, script: str, *, timeout: int = 900) -> str:
    result = toolchain_proof.kubectl(
        owned.context,
        "-n", owned.name, "exec", POD, "-c", "runner", "--", "sh", "-ec", script,
        timeout=timeout,
    )
    return _assert_ok(result, script)


@pytest.fixture
def owned_namespace() -> Iterator[toolchain_proof.OwnedNamespace]:
    image = os.environ.get(IMAGE_ENV, "").strip()
    if not image:
        _unavailable(f"set {IMAGE_ENV} to the built factory runner layer image")
    try:
        # Hand over only the two proof inputs: a failure traceback or its locals
        # must never carry the rest of the process environment.
        target = toolchain_proof.resolve_target(
            {
                name: os.environ.get(name, "")
                for name in (toolchain_proof.CONTEXT_ENV, toolchain_proof.IDENTITY_ENV)
            }
        )
    except toolchain_proof.ProofConfigError as exc:
        pytest.fail(str(exc))
    if shutil.which("kubectl") is None or shutil.which("git") is None:
        _unavailable("kubectl and git are required for the factory runner pod proof")
    _registry_cidrs()
    reachable = toolchain_proof.kubectl(target.context, "cluster-info", timeout=30)
    if reachable.returncode != 0:
        _unavailable(f"Kubernetes context {target.context} is unavailable: {reachable.stderr}")
    try:
        owned = toolchain_proof.create_owned_namespace(target)
    except (toolchain_proof.NamespaceExists, toolchain_proof.NamespaceProbeError) as exc:
        pytest.fail(str(exc))
    # Cleanup runs after the yield, so it is registered before any policy or pod
    # is created and a CleanupUnverified surfaces as a teardown error.
    try:
        yield owned
    finally:
        toolchain_proof.delete_owned_namespace(owned)


def test_factory_skill_allows_locked_installs_and_reports_service_gaps() -> None:
    skill = SKILL.read_text(encoding="utf-8")
    step = skill.split("## 6. Implement and check", 1)[1].split(
        "## 7. Diff review", 1
    )[0]
    for command in (
        "uv sync --frozen",
        "cargo fetch --locked",
        "pnpm install --frozen-lockfile",
    ):
        assert command in step
    assert re.search(r"(?:ad hoc|unpinned).*install", step, re.IGNORECASE)
    assert re.search(r"Postgres|dev stack", step, re.IGNORECASE)
    assert re.search(r"run.*(?:available|can run).*check", step, re.IGNORECASE | re.DOTALL)
    assert re.search(r"(?:pull request|PR) body", step, re.IGNORECASE)


def test_factory_skill_names_registry_egress_cause_with_guidance() -> None:
    """#3761: an unreachable registry is reported as a named environment cause."""
    skill = SKILL.read_text(encoding="utf-8")
    step = skill.split("## 6. Implement and check", 1)[1].split(
        "## 7. Diff review", 1
    )[0]
    assert "registry_egress_unreachable" in step
    # The probe distinguishes per-address reachability, which is what makes a
    # partial CIDR allowlist visible.
    assert "getent ahosts <host>" in step
    assert "--resolve <host>:443:<address>" in step
    assert "agentSandbox.registryEgress" in step
    assert re.search(r"addresses, not hostnames", step)


def test_factory_skill_budgets_against_the_reported_deadline() -> None:
    skill = SKILL.read_text(encoding="utf-8")
    budget = skill.split("## Time budget", 1)[1].split("## Trust boundary", 1)[0]
    for required in ("get_issue", "execution_deadline", "execution_seconds", "date -u"):
        assert required in budget
    for obsolete in ("10800", "3 hours", "minute 15", "minute 150", "minute 165"):
        assert obsolete not in skill
    checkpoints = re.split(r"[.!?](?:\s+|$)", budget)
    for formula, milestone in (
        (r"S\s*/\s*12", r"acceptance criteria"),
        (r"deadline\s*-\s*S\s*/\s*6", r"implement"),
        (r"deadline\s*-\s*S\s*/\s*12", r"stop"),
    ):
        assert any(
            re.search(formula, checkpoint) and re.search(milestone, checkpoint, re.IGNORECASE)
            for checkpoint in checkpoints
        ), (formula, milestone)
    step = skill.split("## 6. Implement and check", 1)[1].split("## 7. Diff review", 1)[0]
    probe = next(
        block
        for block in re.findall(r"```bash\n(.*?)\n```", step, re.DOTALL)
        if "registry_host=" in block
    )
    for required in ("${execution_deadline:?", "${execution_seconds:?"):
        assert required in probe


def test_factory_runner_pod_resolves_repository_lockfiles(
    owned_namespace: toolchain_proof.OwnedNamespace,
) -> None:
    owned = owned_namespace
    namespace = owned.name
    image = os.environ[IMAGE_ENV].strip()
    cidrs = _registry_cidrs()

    for policy in _network_policies(cidrs, namespace):
        _assert_ok(
            toolchain_proof.kubectl(
                owned.context,
                "-n", namespace, "create", "-f", "-",
                input_text=json.dumps(policy),
            ),
            f"create network policy {policy['metadata']['name']}",
        )
    _assert_ok(
        toolchain_proof.kubectl(
            owned.context,
            "-n", namespace, "create", "-f", "-",
            input_text=json.dumps(_pod_manifest(image, namespace)),
        ),
        "create factory runner pod",
    )
    _assert_ok(
        toolchain_proof.kubectl(
            owned.context,
            "-n", namespace, "wait", f"pod/{POD}",
            "--for=condition=Ready", "--timeout=300s", timeout=330,
        ),
        "wait for factory runner pod",
    )
    pod = json.loads(
        _assert_ok(
            toolchain_proof.kubectl(
                owned.context, "-n", namespace, "get", "pod", POD, "-o", "json"
            ),
            "inspect factory runner pod",
        )
    )
    image_ids = [
        status.get("imageID")
        for status in pod["status"].get("containerStatuses", [])
    ]
    assert len(image_ids) == 2 and all(image_ids), image_ids
    print(f"factory runner pod image IDs: {image_ids}")

    toolchain_proof.copy_checkout(owned, POD, "staging", REPO_ROOT)
    assert _exec(owned, "id -u") == "1000"
    _exec(
        owned,
        "touch /workspace/.toolchain-write-probe && rm /workspace/.toolchain-write-probe",
    )
    assert re.match(r"^Python 3\.13\.", _exec(owned, "python3 --version"))
    for command in ("uv --version", "cargo --version", "pnpm --version"):
        assert _exec(owned, command), command
    assert re.match(r"^cargo-nextest 0\.9\.146\b", _exec(owned, "cargo nextest --version"))

    _exec(
        owned,
        "cd /workspace && "
        "UV_PROJECT_ENVIRONMENT=/workspace/.venv "
        "UV_CACHE_DIR=/workspace/.cache/uv "
        "UV_PYTHON_DOWNLOADS=never uv sync --frozen",
    )
    _exec(
        owned,
        "cd /workspace/cli && "
        "CARGO_HOME=/workspace/.cargo "
        "CARGO_TARGET_DIR=/workspace/.cargo-target cargo fetch --locked",
    )
    _exec(
        owned,
        "cd /workspace/cli && "
        "CARGO_HOME=/workspace/.cargo "
        "CARGO_TARGET_DIR=/workspace/.cargo-target "
        "cargo test --no-run --locked",
        timeout=1800,
    )
    _exec(
        owned,
        "cd /workspace/apps/ui && CI=true pnpm install --frozen-lockfile",
    )
