"""Primitive probe lifecycle and the primitive completions fact. @spec PROTECTED-COMMISSIONING-8.

ADR 0236 places the primitive probe registry among the in-cluster producers:
before a permit exists, the control process launches probe workloads from the
measured template, runs one scripted primitive against each in the actual
runner process, and retires them. This module is that lifecycle over
``ProbeRegistry``, and the ``primitive_completions`` fact producer.

Every effect follows the campaign ownership rules. The row and names are
durable before the first create; each UID comes from its own create response;
a create that does not return is ``uncertain`` and is never resolved by a
name lookup. Retirement deletes the exact owned Pod by UID, records either the
termination of the recorded runner container or the never-started
observation (the campaign registry's facts, read by the campaign cleanup's
own observer), re-checks that fact at the version the finalizer-only update
tests, removes the finalizer and then confirms the Pod absent, before the
Service and Secret are deleted by UID and confirmed absent.

The probes hold no permit, launch request or reservation, and nothing here
reaches the launch mechanics or a launch server. A probe's Secret carries the
three keys the guard admits; their values are the caller's and are not stored.
A completion is the digest a primitive returns; the producer binds it only
together with a fresh observation that every probe object is gone.
"""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass, field
from typing import Any, Protocol
from uuid import uuid4

from .commissioning_cleanup import unstarted_observation
from .commissioning_probe_registry import Kind, ProbeRegistry, ProbeSnapshot
from .protected_kubernetes_projection import Projection, render_pod
from .protected_launch_documents import FINALIZER, secret_document, service_document
from .protected_launch_kubernetes import (
    object_identity,
    present,
    require,
    termination_observation,
    unavailable,
)
from .protected_launch_records import canonical_json
from .protected_launch_registry import CleanupReceipt, _scalar

__all__ = [
    "FACT",
    "PrimitiveCompletions",
    "PrimitiveProbeRuntime",
    "ProbeAssignment",
    "ProbeKubernetes",
    "ProbePrimitive",
    "ProbeTarget",
]

FACT = "primitive_completions"
_CREATE_ORDER: tuple[Kind, ...] = ("secret", "service", "pod")
_STATUS_LISTS = ("initContainerStatuses", "containerStatuses", "ephemeralContainerStatuses")


class ProbeKubernetes(Protocol):
    """The bounded launch API subset the probes use, under the mediator identity."""

    async def create(
        self, namespace: str, kind: str, document: dict[str, Any]
    ) -> dict[str, Any]: ...

    async def read(self, namespace: str, kind: str, name: str) -> dict[str, Any] | None: ...

    async def node(self, name: str) -> dict[str, Any]: ...

    async def delete(self, namespace: str, kind: str, name: str, uid: str) -> None: ...

    async def remove_finalizer(self, namespace: str, name: str, uid: str, version: str) -> None: ...


@dataclass(frozen=True, slots=True, repr=False)
class ProbeAssignment:
    """The delivery values one probe is created with; none is stored."""

    session_id: str
    history_ref: str
    secret_source: dict[str, str] = field(repr=False)

    def __repr__(self) -> str:
        return "<ProbeAssignment>"


@dataclass(frozen=True, slots=True)
class ProbeTarget:
    """Where the observed original runner of one probe answers. Not a capability."""

    probe_id: str
    namespace: str
    service_name: str
    pod_uid: str
    pod_ip: str
    container_id: str


class ProbePrimitive(Protocol):
    """One scripted primitive. Returns the SHA-256 hex digest of its result; raises otherwise."""

    async def run(self, target: ProbeTarget) -> str: ...


class PrimitiveProbeRuntime:
    """Launch, exercise and retire this installation's primitive probes."""

    def __init__(
        self,
        registry: ProbeRegistry,
        kubernetes: ProbeKubernetes,
        *,
        projection: Projection,
        namespace: str,
    ) -> None:
        require(type(registry) is ProbeRegistry and type(projection) is Projection)
        require(projection.as_dict()["namespace"] == namespace)
        _scalar(namespace, "name")
        self.registry, self._kube = registry, kubernetes
        self._projection, self._namespace = projection, namespace
        self._mutexes: dict[str, asyncio.Lock] = {}

    def _mutex(self, probe_id: str) -> asyncio.Lock:
        return self._mutexes.setdefault(probe_id, asyncio.Lock())

    async def launch(self, probe_id: str, assignment: ProbeAssignment) -> ProbeSnapshot:
        """Create the Secret, Service and Pod of one probe; ``live`` once all UIDs are bound.

        A create that raises may still have happened, so the probe becomes
        ``uncertain``; any later refusal retires what was created.
        """
        require(type(assignment) is ProbeAssignment)
        async with self._mutex(probe_id):
            row = self.registry.create(
                probe_id, namespace=self._namespace, create_operation_id=str(uuid4())
            )
            o = row.objects
            documents: dict[Kind, dict[str, Any]] = {
                "secret": secret_document(
                    self._namespace, present(o.secret_name), assignment.secret_source
                ),
                "service": service_document(self._namespace, present(o.service_name), probe_id),
                "pod": render_pod(
                    self._projection,
                    launch_id=probe_id,
                    pod_name=present(o.pod_name),
                    secret_name=present(o.secret_name),
                    session_id=assignment.session_id,
                    history_ref=assignment.history_ref,
                ),
            }
            for kind in _CREATE_ORDER:
                document, name = documents[kind], present(getattr(o, kind + "_name"))
                try:
                    created = await self._kube.create(self._namespace, kind + "s", document)
                except Exception:  # noqa: BLE001 - an unanswered create is ambiguous
                    self.registry.mark_uncertain(probe_id, row.revision)
                    unavailable()
                try:
                    uid, version = object_identity(created, namespace=self._namespace, name=name)
                    row = self.registry.record_object_uid(
                        probe_id, row.revision, kind, uid, version
                    )
                except Exception:  # noqa: BLE001 - an unreadable response is ambiguous too
                    self.registry.mark_uncertain(probe_id, self.registry.get(probe_id).revision)
                    unavailable()
            return self.registry.transition(probe_id, row.revision, "live")

    async def observe_runner(self, probe_id: str) -> ProbeTarget:
        """The probe's original runner, observed now; the first observation is recorded."""
        async with self._mutex(probe_id):
            return await self._observe_runner(probe_id)

    async def _observe_runner(self, probe_id: str) -> ProbeTarget:
        row = self.registry.get(probe_id)
        o = row.objects
        require(row.state == "live")
        pod = await self._kube.read(self._namespace, "pods", present(o.pod_name))
        uid, _ = object_identity(pod, namespace=self._namespace, name=present(o.pod_name))
        pod = present(pod)
        meta, status = pod["metadata"], pod.get("status") or {}
        require(uid == o.pod_uid and meta.get("deletionTimestamp") is None)
        require(meta.get("finalizers") == [FINALIZER])
        require(status.get("phase") == "Running")
        statuses = present(status.get("containerStatuses"))
        require(type(statuses) is list and len(statuses) == 1)
        require(not status.get("initContainerStatuses"))
        require(not status.get("ephemeralContainerStatuses"))
        c = statuses[0]
        require(c.get("name") == "runner" and c.get("ready") is True)
        require(type(c.get("restartCount")) is int and c["restartCount"] == 0)
        require(set(c.get("state") or {}) == {"running"})
        digest = self._projection.as_dict()["image"]["digest"]
        require(type(c.get("imageID")) is str and c["imageID"].endswith("@" + digest))
        container_id: str = c.get("containerID")
        _scalar(container_id, "container")
        if o.container_id is None:
            node = await self._kube.node(pod["spec"]["nodeName"])
            row = self.registry.record_container_identity(
                probe_id, row.revision, container_id, node["metadata"]["uid"]
            )
        else:
            require(o.container_id == container_id)
        pod_ip = present(status.get("podIP"))
        require(type(pod_ip) is str and bool(pod_ip))
        return ProbeTarget(
            probe_id, self._namespace, present(o.service_name), uid, pod_ip, container_id
        )

    async def exercise(self, probe_id: str, primitive: ProbePrimitive) -> str:
        """Run one primitive against the live original runner and record its digest.

        The runner is observed before and after; a replaced or restarted
        container, or a primitive that raises, records nothing.
        """
        async with self._mutex(probe_id):
            target = await self._observe_runner(probe_id)
            try:
                digest = await primitive.run(target)
            except Exception:  # noqa: BLE001 - primitive detail never escapes
                unavailable()
            _scalar(digest, "digest")
            require(await self._observe_runner(probe_id) == target)
            row = self.registry.get(probe_id)
            self.registry.record_completion(probe_id, row.revision, digest)
            return digest

    async def retire(self, probe_id: str) -> bool:
        """Retire one probe; ``True`` only once its cleanup receipt is recorded.

        Incomplete proof leaves the probe ``retiring``; an ``uncertain`` probe
        is never retired here.
        """
        async with self._mutex(probe_id):
            try:
                await self._retire(probe_id)
            except Exception:  # noqa: BLE001 - incomplete proof stays durable and retiring
                pass
            return self.registry.get(probe_id).state == "retired"

    async def _retire(self, probe_id: str) -> None:
        row = self.registry.get(probe_id)
        if row.state == "retired":
            return
        require(row.state != "uncertain")
        if row.state != "retiring":
            row = self.registry.transition(probe_id, row.revision, "retiring")
        o = row.objects
        ns = self._namespace
        pod_name, pod_uid = present(o.pod_name), present(o.pod_uid)
        service_uid, secret_uid = present(o.service_uid), present(o.secret_uid)
        if row.termination is None and row.unstarted is None:
            pod = await self._kube.read(ns, "pods", pod_name)
            require(object_identity(pod, namespace=ns, name=pod_name)[0] == pod_uid)
            if present(pod)["metadata"].get("deletionTimestamp") is None:
                require(present(pod)["metadata"].get("finalizers") == [FINALIZER])
                await self._kube.delete(ns, "pods", pod_name, pod_uid)
                pod = await self._kube.read(ns, "pods", pod_name)
            row = await self._prove(row, pod)
        pod = await self._kube.read(ns, "pods", pod_name)
        if pod is not None:
            require(object_identity(pod, namespace=ns, name=pod_name)[0] == pod_uid)
            if pod["metadata"].get("finalizers"):
                # The recorded fact still holds at the version the update tests.
                version = self._still_proved(row, pod)
                await self._kube.remove_finalizer(ns, pod_name, pod_uid, version)
            require(await self._kube.read(ns, "pods", pod_name) is None)
        for kind in ("service", "secret"):
            name, uid = getattr(o, kind + "_name"), getattr(o, kind + "_uid")
            doc = await self._kube.read(ns, kind + "s", name)
            if doc is not None:
                require(object_identity(doc, namespace=ns, name=name)[0] == uid)
                await self._kube.delete(ns, kind + "s", name, uid)
                require(await self._kube.read(ns, kind + "s", name) is None)
        row = self.registry.get(probe_id)
        self.registry.record_cleanup_receipt(
            probe_id, row.revision, CleanupReceipt(pod_uid, service_uid, secret_uid)
        )

    async def _prove(self, row: ProbeSnapshot, pod: Any) -> ProbeSnapshot:
        """Record the termination of the original runner, or that it never started."""
        o = row.objects
        ns, pod_name, pod_uid = self._namespace, present(o.pod_name), present(o.pod_uid)
        if o.container_id is None:
            identities: list[str] = [
                status["containerID"]
                for member in _STATUS_LISTS
                for status in (present(pod).get("status") or {}).get(member) or []
                if type(status) is dict and status.get("containerID")
            ]
            if identities:
                # A container started after all; prove that exact one terminated.
                require(len(identities) == 1)
                node = await self._kube.node(present(pod)["spec"]["nodeName"])
                row = self.registry.record_container_identity(
                    row.probe_id, row.revision, identities[0], node["metadata"]["uid"]
                )
            else:
                fact = unstarted_observation(pod, namespace=ns, pod_name=pod_name, pod_uid=pod_uid)
                return self.registry.record_unstarted(row.probe_id, row.revision, fact)
        proof = termination_observation(
            pod,
            namespace=ns,
            pod_name=pod_name,
            pod_uid=pod_uid,
            container_id=present(row.objects.container_id),
        )
        return self.registry.record_termination(row.probe_id, row.revision, proof)

    def _still_proved(self, row: ProbeSnapshot, pod: Any) -> str:
        o = row.objects
        ns, pod_name, pod_uid = self._namespace, present(o.pod_name), present(o.pod_uid)
        if row.unstarted is not None:
            fact = unstarted_observation(pod, namespace=ns, pod_name=pod_name, pod_uid=pod_uid)
            return fact.resource_version
        require(row.termination is not None)
        proof = termination_observation(
            pod,
            namespace=ns,
            pod_name=pod_name,
            pod_uid=pod_uid,
            container_id=present(o.container_id),
        )
        return proof.resource_version

    async def absent(self, row: ProbeSnapshot) -> bool:
        """Whether a read now finds none of this probe's objects at their recorded names."""
        for kind in _CREATE_ORDER:
            name = present(getattr(row.objects, kind + "_name"))
            if await self._kube.read(self._namespace, kind + "s", name) is not None:
                return False
        return True

    async def retire_all(self) -> bool:
        """Retire every probe; ``True`` only when every row is retired."""
        for row in self.registry.probes():
            if row.state not in {"retired", "uncertain"}:
                await self.retire(row.probe_id)
        return all(row.state == "retired" for row in self.registry.probes())


class PrimitiveCompletions:
    """The ``primitive_completions`` fact producer of ADR 0236, observed afresh per call.

    It refuses unless exactly the predeclared probes exist, each retired with a
    recorded completion and the termination of the runner that produced it,
    and a read now finds none of their objects. A probe whose runner never
    started has no completion and refuses. The digest covers each probe's
    completion, Pod UID and terminated container, bound to the registry's scope.
    """

    def __init__(self, runtime: PrimitiveProbeRuntime, probe_ids: tuple[str, ...]) -> None:
        require(type(runtime) is PrimitiveProbeRuntime and type(probe_ids) is tuple)
        require(0 < len(probe_ids) and len(set(probe_ids)) == len(probe_ids))
        for probe_id in probe_ids:
            _scalar(probe_id, "uuid")
        self._runtime, self._probe_ids = runtime, tuple(sorted(probe_ids))

    @property
    def fact(self) -> str:
        return FACT

    async def observe(self) -> str:
        try:
            return await self._observe()
        except Exception:  # noqa: BLE001 - an unavailable observation is a refusal
            unavailable()

    async def _observe(self) -> str:
        runtime = self._runtime
        rows = runtime.registry.probes()
        require(tuple(row.probe_id for row in rows) == self._probe_ids)
        entries = []
        for row in rows:
            # A completion needs a recorded runner, so its proof is a termination.
            require(row.state == "retired" and row.retirement_outcome == "cleanup-recorded")
            require(row.completion_digest is not None and row.termination is not None)
            require(await runtime.absent(row))
            termination = present(row.termination)
            entries.append(
                {
                    "probe_id": row.probe_id,
                    "pod_uid": termination.pod_uid,
                    "container_id": termination.container_id,
                    "exit_code": termination.exit_code,
                    "completion_sha256": row.completion_digest,
                }
            )
        document = {"scope_sha256": runtime.registry.scope_digest, "probes": entries}
        return hashlib.sha256(canonical_json(document)).hexdigest()
