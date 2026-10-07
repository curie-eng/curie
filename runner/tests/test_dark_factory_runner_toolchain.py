"""Proof that the factory runner layer installs this repository's toolchains.

Set CURIE_FACTORY_RUNNER_IMAGE to an image reachable by the k8 cluster. Set
CURIE_FACTORY_RUNNER_PROOF=required when this proof is a required gate; missing
configuration, a missing image, or an unreachable cluster then fails.
"""

from __future__ import annotations

import ipaddress
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SKILL = REPO_ROOT / "examples/dark-factory/skills/implement-issue/SKILL.md"
CONTEXT = "k8"
NAMESPACE = "test-3499-factory-runner-checks"
POD = "factory-runner-toolchain"
IMAGE_ENV = "CURIE_FACTORY_RUNNER_IMAGE"
PROOF_ENV = "CURIE_FACTORY_RUNNER_PROOF"
REGISTRY_CIDRS_ENV = "CURIE_FACTORY_REGISTRY_CIDRS"


def _unavailable(reason: str) -> None:
    if os.environ.get(PROOF_ENV) == "required":
        pytest.fail(reason)
    pytest.skip(reason)


def _kubectl(
    *args: str, timeout: int = 180, input_text: str | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["kubectl", "--context", CONTEXT, *args],
        input=input_text,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
    )


def _assert_ok(result: subprocess.CompletedProcess[str], command: str) -> str:
    assert result.returncode == 0, (
        f"{command} exited {result.returncode}:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )
    return result.stdout.strip()


def _pod_manifest(image: str) -> dict[str, object]:
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
            "namespace": NAMESPACE,
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


def _network_policies(cidrs: list[str]) -> list[dict[str, Any]]:
    base = {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
    }
    return [
        {
            **base,
            "metadata": {"name": "default-deny", "namespace": NAMESPACE},
            "spec": {
                "podSelector": {},
                "policyTypes": ["Ingress", "Egress"],
            },
        },
        {
            **base,
            "metadata": {"name": "factory-registry-egress", "namespace": NAMESPACE},
            "spec": {
                "podSelector": {"matchLabels": {"app": POD}},
                "policyTypes": ["Egress"],
                "egress": [
                    {
                        "to": [
                            {
                                "namespaceSelector": {
                                    "matchLabels": {"kubernetes.io/metadata.name": "kube-system"}
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


def _copy_checkout() -> None:
    with tempfile.TemporaryDirectory(prefix="curie-factory-proof-") as directory:
        archive = Path(directory) / "checkout.tar"
        packed = subprocess.run(
            [
                "git",
                "archive",
                "--format=tar",
                "--output",
                str(archive),
                "HEAD",
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
            timeout=300,
        )
        _assert_ok(packed, "archive tracked repository checkout")
        with archive.open("rb") as source:
            copied = subprocess.run(
                [
                    "kubectl",
                    "--context",
                    CONTEXT,
                    "-n",
                    NAMESPACE,
                    "exec",
                    "-i",
                    POD,
                    "-c",
                    "staging",
                    "--",
                    "tar",
                    "--no-overwrite-dir",
                    "--touch",
                    "--no-same-permissions",
                    "-xf",
                    "-",
                    "-C",
                    "/workspace",
                ],
                stdin=source,
                capture_output=True,
                text=True,
                check=False,
                timeout=600,
            )
        _assert_ok(copied, "copy checkout into runner pod")


def _exec(script: str, *, timeout: int = 900) -> str:
    result = _kubectl(
        "-n",
        NAMESPACE,
        "exec",
        POD,
        "-c",
        "runner",
        "--",
        "sh",
        "-ec",
        script,
        timeout=timeout,
    )
    return _assert_ok(result, script)


def _delete_namespace() -> None:
    deleted = _kubectl("delete", "namespace", NAMESPACE, "--wait=false")
    _assert_ok(deleted, f"delete namespace {NAMESPACE}")
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        remaining = _kubectl("get", "namespace", NAMESPACE, "-o", "name", timeout=30)
        if remaining.returncode != 0:
            return
        time.sleep(2)
    pytest.fail(f"namespace {NAMESPACE} remained after teardown")


def test_factory_skill_allows_locked_installs_and_reports_service_gaps() -> None:
    skill = SKILL.read_text(encoding="utf-8")
    step = skill.split("## 6. Implement and check", 1)[1].split("## 7. Diff review", 1)[0]
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
    step = skill.split("## 6. Implement and check", 1)[1].split("## 7. Diff review", 1)[0]
    assert "registry_egress_unreachable" in step
    # The probe distinguishes per-address reachability, which is what makes a
    # partial CIDR allowlist visible.
    assert "getent ahosts <host>" in step
    assert "--resolve <host>:443:<address>" in step
    assert "agentSandbox.registryEgress" in step
    assert re.search(r"addresses, not hostnames", step)


def test_factory_runner_pod_resolves_repository_lockfiles() -> None:
    image = os.environ.get(IMAGE_ENV, "").strip()
    if not image:
        _unavailable(f"set {IMAGE_ENV} to the built factory runner layer image")
    if shutil.which("kubectl") is None or shutil.which("git") is None:
        _unavailable("kubectl and git are required for the factory runner pod proof")
    cidrs = _registry_cidrs()
    reachable = _kubectl("cluster-info", timeout=30)
    if reachable.returncode != 0:
        _unavailable(f"Kubernetes context {CONTEXT} is unavailable: {reachable.stderr}")

    created = _kubectl("create", "namespace", NAMESPACE)
    _assert_ok(created, f"create namespace {NAMESPACE}")
    try:
        for policy in _network_policies(cidrs):
            _assert_ok(
                _kubectl(
                    "-n",
                    NAMESPACE,
                    "create",
                    "-f",
                    "-",
                    input_text=json.dumps(policy),
                ),
                f"create network policy {policy['metadata']['name']}",
            )
        _assert_ok(
            _kubectl(
                "-n",
                NAMESPACE,
                "create",
                "-f",
                "-",
                input_text=json.dumps(_pod_manifest(image)),
            ),
            "create factory runner pod",
        )
        _assert_ok(
            _kubectl(
                "-n",
                NAMESPACE,
                "wait",
                f"pod/{POD}",
                "--for=condition=Ready",
                "--timeout=300s",
                timeout=330,
            ),
            "wait for factory runner pod",
        )
        pod = json.loads(
            _assert_ok(
                _kubectl("-n", NAMESPACE, "get", "pod", POD, "-o", "json"),
                "inspect factory runner pod",
            )
        )
        image_ids = [status.get("imageID") for status in pod["status"].get("containerStatuses", [])]
        assert len(image_ids) == 2 and all(image_ids), image_ids
        print(f"factory runner pod image IDs: {image_ids}")

        _copy_checkout()
        assert _exec("id -u") == "1000"
        _exec("touch /workspace/.toolchain-write-probe && rm /workspace/.toolchain-write-probe")
        assert re.match(r"^Python 3\.13\.", _exec("python3 --version"))
        for command in ("uv --version", "cargo --version", "pnpm --version"):
            assert _exec(command), command

        _exec(
            "cd /workspace && "
            "UV_PROJECT_ENVIRONMENT=/workspace/.venv "
            "UV_CACHE_DIR=/workspace/.cache/uv "
            "UV_PYTHON_DOWNLOADS=never uv sync --frozen"
        )
        _exec(
            "cd /workspace/cli && "
            "CARGO_HOME=/workspace/.cargo "
            "CARGO_TARGET_DIR=/workspace/.cargo-target cargo fetch --locked"
        )
        _exec(
            "cd /workspace/cli && "
            "CARGO_HOME=/workspace/.cargo "
            "CARGO_TARGET_DIR=/workspace/.cargo-target "
            "cargo test --no-run --locked",
            timeout=1800,
        )
        _exec("cd /workspace/apps/ui && CI=true pnpm install --frozen-lockfile")
    finally:
        _delete_namespace()
