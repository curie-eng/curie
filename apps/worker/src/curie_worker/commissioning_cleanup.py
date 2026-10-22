"""Retire a campaign Pod whose runner container never started. @spec PROTECTED-COMMISSIONING-8.

The shared mechanics retire a launch by proving that its recorded runner
container terminated before they remove the protected finalizer. A Pod deleted
before that container existed has no container identity to prove, so the
shared path stops there and the launch would stay ``retiring`` with the
finalizer held. This module finishes such a launch under the commissioning
authorizer only; the production path is unchanged.

It records that the runner never started, observed on the exact owned Pod UID
after its deletion and only once no container can start any more: the Pod was
never bound to a node (a terminating Pod cannot be bound), or the kubelet
reported the deleted Pod in a terminal phase. No container status may carry a
container ID, a restart or a running state. That fact is durable before the
finalizer-only update, the Pod is then confirmed absent, and the Service and
Secret are deleted by UID before the cleanup receipt. Absence alone, a pending
kubelet, another UID or any container identity never pass; the launch stays
``retiring`` and blocks acceptance.
"""

from __future__ import annotations

from typing import Any

from .commissioning_registry import CommissioningLaunchRegistry, UnstartedObservation
from .protected_launch_kubernetes import (
    FINALIZER,
    KubernetesLaunchAPI,
    object_identity,
    present,
    require,
    unavailable,
)
from .protected_launch_registry import CleanupReceipt

__all__ = ["UnstartedObservation", "retire_unstarted", "unstarted_observation"]

_TERMINAL_PHASES = frozenset({"Failed", "Succeeded"})
_STATUS_LISTS = ("initContainerStatuses", "containerStatuses", "ephemeralContainerStatuses")


def _never_started(status: Any) -> bool:
    if type(status) is not dict or status.get("containerID") or status.get("lastState"):
        return False
    if status.get("restartCount", 0) != 0 or status.get("started") is True:
        return False
    state = status.get("state") or {}
    if type(state) is not dict or not set(state) <= {"waiting", "terminated"}:
        return False
    terminated = state.get("terminated")
    return terminated is None or (
        type(terminated) is dict
        and not terminated.get("containerID")
        and not terminated.get("startedAt")
    )


def unstarted_observation(
    pod: Any, *, namespace: str, pod_name: str, pod_uid: str
) -> UnstartedObservation:
    """The never-started fact of the exact deleted owned Pod, or a fixed refusal."""
    try:
        uid, version = object_identity(pod, namespace=namespace, name=pod_name)
        meta = pod["metadata"]
        require(uid == pod_uid and meta.get("finalizers") == [FINALIZER])
        require(meta.get("deletionTimestamp") is not None)
        status = pod.get("status") or {}
        require(type(status) is dict)
        for member in _STATUS_LISTS:
            statuses = status.get(member) or []
            require(type(statuses) is list and all(_never_started(s) for s in statuses))
        if not pod.get("spec", {}).get("nodeName"):
            return UnstartedObservation(pod_uid, version, "unscheduled")
        require(status.get("phase") in _TERMINAL_PHASES)
        return UnstartedObservation(pod_uid, version, "terminal")
    except Exception:  # noqa: BLE001 - fixed fail-closed diagnostic
        unavailable()


async def retire_unstarted(
    kubernetes: KubernetesLaunchAPI,
    registry: CommissioningLaunchRegistry,
    owner: str,
    launch_id: str,
) -> None:
    """Finish one retiring launch whose runner never started; incomplete proof raises.

    The caller holds the launch's mechanics mutex. Every step re-reads the
    exact recorded objects, so a restarted mediator resumes after any step.
    """
    row = registry.get(owner, launch_id)
    o = row.objects
    require(row.state == "retiring" and row.termination is None and o.container_id is None)
    namespace, pod_name, pod_uid = present(o.namespace), present(o.pod_name), present(o.pod_uid)
    require(present(o.service_uid) is not None and present(o.secret_uid) is not None)
    if row.unstarted is None:
        pod = await kubernetes.read(namespace, "pods", pod_name)
        require(
            pod is not None
            and object_identity(pod, namespace=namespace, name=pod_name)[0] == pod_uid
        )
        if present(pod)["metadata"].get("deletionTimestamp") is None:
            require(present(pod)["metadata"].get("finalizers") == [FINALIZER])
            await kubernetes.delete(namespace, "pods", pod_name, pod_uid)
            pod = await kubernetes.read(namespace, "pods", pod_name)
        observation = unstarted_observation(
            pod, namespace=namespace, pod_name=pod_name, pod_uid=pod_uid
        )
        row = registry.record_unstarted(owner, launch_id, row.revision, observation)
    pod = await kubernetes.read(namespace, "pods", pod_name)
    if pod is not None:
        if pod["metadata"].get("finalizers"):
            # The fact still holds at the version the finalizer-only update tests.
            current = unstarted_observation(
                pod, namespace=namespace, pod_name=pod_name, pod_uid=pod_uid
            )
            await kubernetes.remove_finalizer(
                namespace, pod_name, pod_uid, current.resource_version
            )
        else:
            require(object_identity(pod, namespace=namespace, name=pod_name)[0] == pod_uid)
        require(await kubernetes.read(namespace, "pods", pod_name) is None)
    for kind in ("service", "secret"):
        name, uid = getattr(o, kind + "_name"), getattr(o, kind + "_uid")
        doc = await kubernetes.read(namespace, kind + "s", name)
        if doc is not None:
            require(doc["metadata"]["uid"] == uid)
            await kubernetes.delete(namespace, kind + "s", name, uid)
            require(await kubernetes.read(namespace, kind + "s", name) is None)
    row = registry.get(owner, launch_id)
    registry.record_cleanup_receipt(
        owner,
        launch_id,
        row.revision,
        CleanupReceipt(pod_uid, present(o.service_uid), present(o.secret_uid)),
    )
