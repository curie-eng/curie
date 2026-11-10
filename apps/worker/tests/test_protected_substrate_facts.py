"""Control-plane snapshot parsing, not live substrate qualification.

Provider shape: https://kubernetes.io/docs/reference/kubernetes-api/core/pod-v1/
ContainerStatus.imageID may differ from the PodSpec image; neither is inferred
from runner status, labels, or an expected digest supplied by a caller.
"""
from __future__ import annotations

import copy
import importlib

import pytest
from curie_worker.sandbox.types import SandboxHandle


def api():
    module = importlib.util.find_spec("curie_worker.protected_substrate_facts")
    assert module is not None, "protected substrate facts parser is absent"
    return importlib.import_module(module.name)


def handle():
    return SandboxHandle(
        thread_key="protected-thread", claim_name="claim-example", sandbox_name="pod-example",
        namespace="example", service_fqdn="runner.example.com", port=8080,
        session_id="session-example", token="token-example",
    )


def snapshot():
    return {
        "metadata": {"namespace": "example", "name": "pod-example", "uid": "pod-uid-example"},
        "spec": {"containers": [{"name": "runner", "image": "acme-runner@sha256:" + "a" * 64}]},
        "status": {
            "phase": "Running",
            "containerStatuses": [{
                "name": "runner", "imageID": "containerd://sha256:" + "b" * 64,
                "containerID": "containerd://" + "c" * 64,
                "restartCount": 0, "ready": True,
                "state": {"running": {"startedAt": "2026-01-01T00:00:00Z"}},
            }],
        },
    }


def read(pod):
    return api().parse_kubernetes_runner_facts(
        pod, handle=handle(), pod_uid="pod-uid-example", container_id="containerd://" + "c" * 64,
    )


def test_observed_image_identity_is_kept_distinct_from_declared_manifest_reference():
    facts = read(snapshot())
    assert facts.declared_image == "acme-runner@sha256:" + "a" * 64
    assert facts.image_id == "containerd://sha256:" + "b" * 64
    assert facts.container_id == "containerd://" + "c" * 64
    assert facts.pod_uid == "pod-uid-example"
    assert facts.restart_count == 0
    # An observed runtime ID is not rewritten to match the desired manifest.
    assert not hasattr(facts, "qualified")


@pytest.mark.parametrize("case", [
    "namespace", "name", "uid", "container-id", "duplicate-spec", "duplicate-status",
    "not-running", "not-ready", "restarting", "bool-restart", "missing-image-id", "wrong-phase",
])
def test_incomplete_or_replaced_runtime_snapshot_is_refused(case):
    pod = snapshot()
    status = pod["status"]["containerStatuses"][0]
    if case in {"namespace", "name", "uid"}:
        pod["metadata"][case] = "other"
    elif case == "container-id":
        status["containerID"] = "containerd://" + "d" * 64
    elif case == "duplicate-spec":
        pod["spec"]["containers"].append(copy.deepcopy(pod["spec"]["containers"][0]))
    elif case == "duplicate-status":
        pod["status"]["containerStatuses"].append(copy.deepcopy(status))
    elif case == "not-running":
        status["state"] = {"terminated": {"exitCode": 0}}
    elif case == "not-ready":
        status["ready"] = False
    elif case == "restarting":
        status["restartCount"] = 1
    elif case == "bool-restart":
        status["restartCount"] = False
    elif case == "missing-image-id":
        status.pop("imageID")
    else:
        pod["status"]["phase"] = "Pending"
    with pytest.raises(api().SubstrateFactsUnavailable):
        read(pod)
