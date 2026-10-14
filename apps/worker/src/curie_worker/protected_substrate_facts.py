"""Parse substrate observations without treating them as execution authority.

A future reader must obtain the Pod from the authenticated control plane and
bind its ownership/template/launch settings and runtime-specific image mapping.
This parser does not qualify a substrate, validate a bearer token or authorize
model execution. It keeps declared and observed image identities separate.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .sandbox.types import SandboxHandle


class SubstrateFactsUnavailable(RuntimeError):
    """Unavailable or mismatched facts, redacted to avoid leaking source data."""

    def __init__(self) -> None:
        super().__init__("protected substrate facts are unavailable")


@dataclass(frozen=True, repr=False)
class KubernetesRunnerFacts:
    pod_uid: str
    container_id: str
    declared_image: str
    image_id: str
    restart_count: int


def _object(value: object) -> dict[str, Any]:
    if type(value) is not dict:
        raise SubstrateFactsUnavailable()
    return value


def _text(value: object) -> str:
    if (
        type(value) is not str or not value or len(value) > 2048
        or any(ord(ch) <= 0x20 for ch in value)
    ):
        raise SubstrateFactsUnavailable()
    return value


def _runner(value: object) -> dict[str, Any]:
    if type(value) is not list:
        raise SubstrateFactsUnavailable()
    matches = [_object(item) for item in value if _object(item).get("name") == "runner"]
    if len(matches) != 1:
        raise SubstrateFactsUnavailable()
    return matches[0]


def parse_kubernetes_runner_facts(
    pod: object,
    *,
    handle: SandboxHandle,
    pod_uid: str,
    container_id: str,
) -> KubernetesRunnerFacts:
    """Bind one running snapshot to previously observed Pod/container identities.

    Provider fields follow the Kubernetes Pod API, including ContainerStatus:
    https://kubernetes.io/docs/reference/kubernetes-api/core/pod-v1/
    imageID can differ from the PodSpec image and has runtime-specific meaning.
    Preserve it verbatim; a manifest/platform/config mapping is separate proof.
    Restarted containers require fresh cold-create authority, never this snapshot.
    """
    if type(handle) is not SandboxHandle:
        raise SubstrateFactsUnavailable()
    expected_uid, expected_container = _text(pod_uid), _text(container_id)
    value = _object(pod)
    metadata = _object(value.get("metadata"))
    if (
        _text(metadata.get("namespace")) != handle.namespace
        or _text(metadata.get("name")) != handle.sandbox_name
        or _text(metadata.get("uid")) != expected_uid
        or metadata.get("deletionTimestamp") is not None
    ):
        raise SubstrateFactsUnavailable()
    spec = _runner(_object(value.get("spec")).get("containers"))
    status = _object(value.get("status"))
    running = _runner(status.get("containerStatuses"))
    state = _object(running.get("state"))
    if (
        status.get("phase") != "Running"
        or running.get("ready") is not True
        or set(state) != {"running"}
        or type(state["running"]) is not dict
        or type(running.get("restartCount")) is not int
        or running["restartCount"] != 0
        or _text(running.get("containerID")) != expected_container
    ):
        raise SubstrateFactsUnavailable()
    return KubernetesRunnerFacts(
        pod_uid=expected_uid,
        container_id=expected_container,
        declared_image=_text(spec.get("image")),
        image_id=_text(running.get("imageID")),
        restart_count=0,
    )
