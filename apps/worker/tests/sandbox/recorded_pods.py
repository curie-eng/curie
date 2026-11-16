"""Recorded kubelet pod shapes and the production Kubernetes client over them (#5427).

``recorded_pod_shapes.json`` holds pods a kubelet actually ended, trimmed to
the fields diagnosis reads. These helpers turn a shape into the client's own
models and build a ``KubernetesSandboxClient`` whose only doubles are the
CoreV1Api and CustomObjectsApi objects handed to it.
"""

from __future__ import annotations

import copy
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from curie_worker.sandbox.k8s import KubernetesSandboxClient
from kubernetes import client as k8s_client

SHAPES: dict[str, Any] = json.loads(
    (Path(__file__).parent / "recorded_pod_shapes.json").read_text()
)["pods"]
MI = 1024 * 1024
NAMESPACE = "test-ns"
POOL = "test-pool"
TEMPLATE = "test-template"


def shape(name: str) -> dict[str, Any]:
    """A private copy of one recorded pod."""

    return copy.deepcopy(SHAPES[name])


def pod(recorded: dict[str, Any]) -> k8s_client.V1Pod:
    """A recorded pod as the client's own V1Pod model."""

    body = {key: value for key, value in recorded.items() if key != "events"}
    # The recording trimmed fields diagnosis never reads; the client model
    # requires these four on every container status, so they come back as the
    # API server always sends them.
    for status in body["status"].get("containerStatuses", []):
        status.setdefault("image", "runner:recorded")
        status.setdefault("imageID", "runner@sha256:" + "0" * 64)
        status.setdefault("ready", False)
        status.setdefault("restartCount", 0)
    model: k8s_client.V1Pod = k8s_client.ApiClient()._ApiClient__deserialize(body, "V1Pod")  # type: ignore[attr-defined]
    return model


def event(raw: dict[str, Any], *, uid: str | None, at: datetime) -> Any:
    """A recorded event as the client's own model, about the pod with ``uid``."""

    body = {
        **raw,
        "metadata": {"name": "event"},
        "involvedObject": {**raw["involvedObject"], "uid": uid},
        "lastTimestamp": at.isoformat(),
    }
    return k8s_client.ApiClient()._ApiClient__deserialize(body, "CoreV1Event")  # type: ignore[attr-defined]


def client(api: Any) -> KubernetesSandboxClient:
    """The production client over ``api``, which answers both API groups."""

    built = KubernetesSandboxClient.__new__(KubernetesSandboxClient)
    built._api = api  # type: ignore[attr-defined]
    built._core_api = api  # type: ignore[attr-defined]
    built._namespace = NAMESPACE  # type: ignore[attr-defined]
    return built


def templates(spec: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    """A pool and the template it names, as the API server stores them."""

    return {
        ("sandboxwarmpools", POOL): {"spec": {"sandboxTemplateRef": {"name": TEMPLATE}}},
        ("sandboxtemplates", TEMPLATE): {"spec": {"podTemplate": {"spec": spec}}},
    }
