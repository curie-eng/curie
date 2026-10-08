"""Runner restart logs through the chart worker's real Kubernetes identity.

The caller owns the kind cluster and its private kubeconfig. This test owns a
separate namespace, installs only the chart's worker RBAC, and removes that
namespace even when setup or an assertion fails.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import yaml
from curie_worker.sandbox.k8s import KubernetesSandboxClient
from kubernetes import client as k8s_client

pytestmark = pytest.mark.skipif(
    os.environ.get("CURIE_SANDBOX_E2E") != "1",
    reason="real kind proof; set CURIE_SANDBOX_E2E=1 and explicit context/private KUBECONFIG",
)

_BUSYBOX_IMAGE = (
    "busybox:1.36.1@sha256:"
    "73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662"
)


def _kubectl(context: str, kubeconfig: Path, *args: str, manifest: str | None = None) -> str:
    return subprocess.run(
        ["kubectl", "--context", context, "--kubeconfig", str(kubeconfig), *args],
        input=manifest,
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    ).stdout.strip()


def _worker_rbac(namespace: str) -> list[dict[str, Any]]:
    chart = Path(__file__).resolve().parents[4] / "charts" / "curie"
    rendered = subprocess.run(
        [
            "helm",
            "template",
            "runner-tail",
            str(chart),
            "--namespace",
            namespace,
            "--kube-version",
            "1.35.0",
            "--show-only",
            "templates/worker.yaml",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    ).stdout
    objects = [
        obj
        for obj in yaml.safe_load_all(rendered)
        if isinstance(obj, dict) and obj.get("kind") in {"ServiceAccount", "Role", "RoleBinding"}
    ]
    assert sorted(obj["kind"] for obj in objects) == ["Role", "RoleBinding", "ServiceAccount"]
    assert all(
        obj["metadata"]["labels"]["app.kubernetes.io/component"] == "worker"
        for obj in objects
    )
    return objects


@dataclass
class _RunnerLogProof:
    client: KubernetesSandboxClient
    namespace: str
    pod_name: str
    marker: str


@pytest.fixture
def restarted_runner(request: pytest.FixtureRequest) -> _RunnerLogProof:
    for executable in ("kubectl", "helm", "kind"):
        assert shutil.which(executable), f"required executable is unavailable: {executable}"
    context = os.environ.get("CURIE_SANDBOX_E2E_CONTEXT", "")
    kubeconfig_value = os.environ.get("KUBECONFIG", "")
    assert context, "CURIE_SANDBOX_E2E_CONTEXT must select the run-owned kind context"
    assert context.startswith("kind-"), "runner log proof requires a kind cluster"
    assert kubeconfig_value and ":" not in kubeconfig_value, "set one private KUBECONFIG file"
    kubeconfig = Path(kubeconfig_value).resolve()
    assert kubeconfig.is_file(), "the private KUBECONFIG file is unavailable"
    assert kubeconfig != (Path.home() / ".kube" / "config").resolve(), "use a private kubeconfig"
    assert kubeconfig.stat().st_mode & 0o077 == 0, "private kubeconfig must deny group/world access"
    configuration = yaml.safe_load(kubeconfig.read_text())
    assert configuration["current-context"] == context, "private kubeconfig context must match"
    assert [entry["name"] for entry in configuration["contexts"]] == [context], (
        "private kubeconfig must contain only the selected kind context"
    )
    namespace = f"test-4171-runner-log-tail-{os.getpid()}"
    assert not _kubectl(
        context, kubeconfig, "get", "namespace", namespace, "--ignore-not-found=true", "-o", "name"
    ), "test namespace already exists; preserve its owner"

    def cleanup_namespace() -> None:
        _kubectl(
            context,
            kubeconfig,
            "delete",
            "namespace",
            namespace,
            "--ignore-not-found=true",
            "--wait=true",
            "--timeout=90s",
        )
        assert not _kubectl(
            context,
            kubeconfig,
            "get",
            "namespace",
            namespace,
            "--ignore-not-found=true",
            "-o",
            "name",
        ), "owned namespace survived teardown"

    # Register before creation so partial setup also removes owned resources.
    request.addfinalizer(cleanup_namespace)
    _kubectl(context, kubeconfig, "create", "namespace", namespace)
    objects = _worker_rbac(namespace)
    _kubectl(
        context,
        kubeconfig,
        "-n",
        namespace,
        "apply",
        "-f",
        "-",
        manifest=yaml.safe_dump_all(objects),
    )
    service_account = next(
        obj["metadata"]["name"] for obj in objects if obj["kind"] == "ServiceAccount"
    )
    pod_name = "runner-log-tail"
    marker = f"runner-log-tail-4171-{uuid.uuid4().hex}"
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": pod_name, "namespace": namespace},
        "spec": {
            "restartPolicy": "Always",
            "automountServiceAccountToken": False,
            "volumes": [{"name": "restart-state", "emptyDir": {}}],
            "containers": [
                {
                    "name": "runner",
                    "image": _BUSYBOX_IMAGE,
                    # Only the crashed instance prints the marker. Reading
                    # current logs instead of previous logs cannot satisfy it.
                    "command": [
                        "sh",
                        "-c",
                        "if [ -f /state/started ]; then echo current-runner; sleep 300; "
                        f"else touch /state/started; printf '%s\\n' '{marker}'; "
                        "sleep 1; exit 3; fi",
                    ],
                    "volumeMounts": [{"name": "restart-state", "mountPath": "/state"}],
                    "resources": {
                        "requests": {"cpu": "10m", "memory": "8Mi"},
                        "limits": {"cpu": "100m", "memory": "32Mi"},
                    },
                }
            ],
        },
    }
    _kubectl(
        context, kubeconfig, "-n", namespace, "apply", "-f", "-", manifest=yaml.safe_dump(pod)
    )
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        observed = json.loads(
            _kubectl(context, kubeconfig, "-n", namespace, "get", "pod", pod_name, "-o", "json")
        )
        statuses = observed.get("status", {}).get("containerStatuses", [])
        if any(
            status["name"] == "runner"
            and status.get("restartCount", 0) >= 1
            and status.get("lastState", {}).get("terminated", {}).get("exitCode") == 3
            for status in statuses
        ):
            break
        time.sleep(0.5)
    else:
        raise RuntimeError("runner did not exit 3 and restart within the setup budget")

    client = KubernetesSandboxClient(namespace, kubeconfig=str(kubeconfig))
    api_client = client._core_api.api_client  # noqa: SLF001
    request.addfinalizer(api_client.close)
    selected_context = configuration["contexts"][0]["context"]
    server = next(
        cluster["cluster"]["server"]
        for cluster in configuration["clusters"]
        if cluster["name"] == selected_context["cluster"]
    )
    assert api_client.configuration.host == server, (
        "client did not select the explicit kind cluster"
    )
    api_client.default_headers["Impersonate-User"] = (
        f"system:serviceaccount:{namespace}:{service_account}"
    )
    return _RunnerLogProof(client=client, namespace=namespace, pod_name=pod_name, marker=marker)


def test_chart_worker_reads_restarted_runner_log_with_read_only_rbac(
    restarted_runner: _RunnerLogProof,
) -> None:
    proof = restarted_runner

    tail = proof.client.pod_log_tail(proof.pod_name, request_timeout_seconds=5.0)

    assert tail is not None
    assert proof.marker in tail
    # Drive adjacent API operations with the same identity. The chart's new
    # log grant must not authorize listing or mutating the pod itself.
    core_api = proof.client._core_api  # noqa: SLF001
    with pytest.raises(k8s_client.ApiException) as denied_list:
        core_api.list_namespaced_pod(proof.namespace, _request_timeout=5.0)
    assert denied_list.value.status == 403
    with pytest.raises(k8s_client.ApiException) as denied_delete:
        core_api.delete_namespaced_pod(proof.pod_name, proof.namespace, _request_timeout=5.0)
    assert denied_delete.value.status == 403
