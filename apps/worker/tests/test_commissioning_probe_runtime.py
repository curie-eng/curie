"""Primitive probe lifecycle and the primitive completions fact. @spec PROTECTED-COMMISSIONING-8.

An in-memory API server stands in for Kubernetes. It refuses a same-name
create, applies UID and resource-version preconditions, and keeps a deleted Pod
with the protected finalizer until the finalizer-only update removes it. These
tests prove the ownership order and refusals only; they measure no cluster.
"""

from __future__ import annotations

import asyncio
import hashlib
import itertools
import uuid
from copy import deepcopy
from typing import Any

import pytest
from curie_worker.commissioning_probe_registry import ProbeRegistry
from curie_worker.commissioning_probe_runtime import (
    FACT,
    PrimitiveCompletions,
    PrimitiveProbeRuntime,
    ProbeAssignment,
    ProbeTarget,
)
from curie_worker.protected_kubernetes_projection import parse_projection
from curie_worker.protected_launch_documents import FINALIZER
from curie_worker.protected_launch_kubernetes import LaunchControlUnavailable, unavailable

from .protected_configuration_vectors import canonical
from .test_commissioning_control_profile import profile
from .test_protected_kubernetes_projection import projection_value

BOOT = "b0000000-0000-4000-8000-000000000001"
LATER_BOOT = "b0000000-0000-4000-8000-000000000002"
PROFILE = profile()
SCOPE = PROFILE.digest
PROBES = ("10000000-0000-4000-8000-000000000001", "10000000-0000-4000-8000-000000000002")
NODE = "40000000-0000-4000-8000-000000000001"
NAMESPACE = "acme-runtime"
IMAGE_DIGEST = "sha256:" + "e" * 64
SOURCE = {"runner-token": "r", "history-token": "h", "model-credential": "m"}
ASSIGNMENT = ProbeAssignment("session", "history", SOURCE)


def projection():
    return parse_projection(canonical(projection_value()))


class Api:
    """A bounded API server: names are unique, deletes honour the finalizer."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], dict[str, Any]] = {}
        self.versions = itertools.count(10)
        self.events: list[tuple[str, str, str]] = []
        self.fail_create: str | None = None
        self.answer_create: bool = True
        self.kubelet = "runs"
        self.patches: list[tuple[str, str]] = []

    def _version(self) -> str:
        return str(next(self.versions))

    async def create(self, namespace: str, kind: str, document: dict[str, Any]) -> dict[str, Any]:
        name = document["metadata"]["name"]
        self.events.append(("create", kind, name))
        if (kind, name) in self.objects or kind == self.fail_create:
            unavailable()
        value = deepcopy(document)
        value["metadata"].update(uid=str(uuid.uuid4()), resourceVersion=self._version())
        if kind == "pods":
            value["spec"]["nodeName"] = "node-a"
            value["status"] = {"phase": "Pending"}
        self.objects[(kind, name)] = value
        if not self.answer_create:
            unavailable()
        return deepcopy(value)

    async def read(self, namespace: str, kind: str, name: str) -> dict[str, Any] | None:
        value = self.objects.get((kind, name))
        return None if value is None else deepcopy(value)

    async def node(self, name: str) -> dict[str, Any]:
        assert name == "node-a"
        return {"metadata": {"uid": NODE, "name": name}}

    async def delete(self, namespace: str, kind: str, name: str, uid: str) -> None:
        self.events.append(("delete", kind, name))
        value = self.objects.get((kind, name))
        if value is None:
            return
        if value["metadata"]["uid"] != uid:
            unavailable()
        if kind != "pods":
            del self.objects[(kind, name)]
            return
        value["metadata"]["deletionTimestamp"] = "2026-10-22T00:00:00Z"
        value["metadata"]["resourceVersion"] = self._version()
        if self.kubelet == "runs":
            status = value["status"]["containerStatuses"][0]
            status["state"] = {
                "terminated": {
                    "containerID": status["containerID"],
                    "exitCode": 137,
                    "startedAt": "2026-10-22T00:00:00Z",
                    "finishedAt": "2026-10-22T00:00:05Z",
                }
            }
            status["ready"] = False
            value["status"]["phase"] = "Failed"
        elif self.kubelet == "never":
            value["status"] = {
                "phase": "Failed",
                "containerStatuses": [
                    {
                        "name": "runner",
                        "ready": False,
                        "restartCount": 0,
                        "state": {"waiting": {"reason": "ContainerCreating"}},
                    }
                ],
            }

    async def remove_finalizer(self, namespace: str, name: str, uid: str, version: str) -> None:
        value = self.objects[("pods", name)]
        if (
            value["metadata"]["uid"] != uid
            or value["metadata"]["resourceVersion"] != version
            or value["metadata"].get("finalizers") != [FINALIZER]
        ):
            unavailable()
        self.patches.append((name, uid))
        del self.objects[("pods", name)]

    def run(self, pod_name: str, container: str = "containerd://" + "c" * 64) -> None:
        pod = self.objects[("pods", pod_name)]
        pod["status"] = {
            "phase": "Running",
            "podIP": "10.0.0.7",
            "containerStatuses": [
                {
                    "name": "runner",
                    "ready": True,
                    "restartCount": 0,
                    "containerID": container,
                    "imageID": "registry.example.com/runner@" + IMAGE_DIGEST,
                    "state": {"running": {"startedAt": "2026-10-22T00:00:00Z"}},
                }
            ],
        }
        pod["metadata"]["resourceVersion"] = self._version()


class Primitive:
    def __init__(self, result: str = "d" * 64, fail: bool = False) -> None:
        self.result, self.fail = result, fail
        self.targets: list[ProbeTarget] = []

    async def run(self, target: ProbeTarget) -> str:
        self.targets.append(target)
        if self.fail:
            raise RuntimeError("primitive detail")
        return self.result


def runtime(tmp_path, api, *, boot=BOOT):
    path = tmp_path / "probes"
    path.mkdir(mode=0o700, exist_ok=True)
    registry = ProbeRegistry.open(str(path), boot_id=boot, scope_digest=SCOPE)
    return PrimitiveProbeRuntime(registry, api, profile=PROFILE, projection=projection())


def pod_name(probe_id):
    return "pl-p-" + uuid.UUID(probe_id).hex


def test_a_probe_runs_one_primitive_and_retires_over_its_runners_termination(tmp_path):
    async def scenario() -> None:
        api = Api()
        probes = runtime(tmp_path, api)
        row = await probes.launch(PROBES[0], ASSIGNMENT)
        assert row.state == "live"
        # Ownership precedes every create; the Pod is created last.
        assert [event[1] for event in api.events] == ["secrets", "services", "pods"]
        for kind in ("pod", "service", "secret"):
            uid = getattr(row.objects, kind + "_uid")
            assert (
                api.objects[(kind + "s", getattr(row.objects, kind + "_name"))]["metadata"]["uid"]
                == uid
            )
        api.run(pod_name(PROBES[0]))
        primitive = Primitive()
        assert await probes.exercise(PROBES[0], primitive) == "d" * 64
        target = primitive.targets[0]
        assert (target.pod_uid, target.pod_ip) == (row.objects.pod_uid, "10.0.0.7")
        assert await probes.retire(PROBES[0])
        final = probes.registry.get(PROBES[0])
        assert final.state == "retired" and final.termination is not None
        assert final.termination.exit_code == 137
        assert api.patches == [(pod_name(PROBES[0]), row.objects.pod_uid)]
        assert not api.objects

    asyncio.run(scenario())


def test_a_same_name_object_is_never_adopted(tmp_path):
    async def scenario() -> None:
        api = Api()
        squatter = {"metadata": {"name": pod_name(PROBES[0]), "uid": str(uuid.uuid4())}}
        api.objects[("pods", pod_name(PROBES[0]))] = squatter
        probes = runtime(tmp_path, api)
        with pytest.raises(LaunchControlUnavailable):
            await probes.launch(PROBES[0], ASSIGNMENT)
        row = probes.registry.get(PROBES[0])
        assert row.state == "uncertain" and row.objects.pod_uid is None
        # Uncertain is never retired and never touches the object at that name.
        assert not await probes.retire(PROBES[0])
        assert api.objects[("pods", pod_name(PROBES[0]))] is squatter
        assert not [e for e in api.events if e[0] == "delete" and e[1] == "pods"]

    asyncio.run(scenario())


def test_an_unanswered_create_is_uncertain_even_though_it_happened(tmp_path):
    async def scenario() -> None:
        api = Api()
        api.answer_create = False
        probes = runtime(tmp_path, api)
        with pytest.raises(LaunchControlUnavailable):
            await probes.launch(PROBES[0], ASSIGNMENT)
        row = probes.registry.get(PROBES[0])
        assert row.state == "uncertain" and row.objects.secret_uid is None
        assert ("secrets", "pl-c-" + uuid.UUID(PROBES[0]).hex) in api.objects

    asyncio.run(scenario())


def test_a_failed_primitive_or_replaced_runner_records_nothing(tmp_path):
    async def scenario() -> None:
        api = Api()
        probes = runtime(tmp_path, api)
        await probes.launch(PROBES[0], ASSIGNMENT)
        with pytest.raises(LaunchControlUnavailable):
            # Not running yet.
            await probes.exercise(PROBES[0], Primitive())
        api.run(pod_name(PROBES[0]))
        with pytest.raises(LaunchControlUnavailable) as caught:
            await probes.exercise(PROBES[0], Primitive(fail=True))
        assert "primitive detail" not in repr(caught.value.__context__)
        api.run(pod_name(PROBES[0]), container="containerd://" + "f" * 64)
        with pytest.raises(LaunchControlUnavailable):
            # The recorded runner was replaced.
            await probes.exercise(PROBES[0], Primitive())
        assert probes.registry.get(PROBES[0]).completion_digest is None

    asyncio.run(scenario())


def test_a_runner_that_never_started_retires_over_the_never_started_fact(tmp_path):
    async def scenario() -> None:
        api = Api()
        api.kubelet = "never"
        probes = runtime(tmp_path, api)
        await probes.launch(PROBES[0], ASSIGNMENT)
        assert await probes.retire(PROBES[0])
        row = probes.registry.get(PROBES[0])
        assert row.unstarted is not None and row.unstarted.evidence == "terminal"
        assert row.termination is None and not api.objects

    asyncio.run(scenario())


def test_a_kubelet_that_has_not_finished_keeps_the_finalizer(tmp_path):
    async def scenario() -> None:
        api = Api()
        api.kubelet = "pending"
        probes = runtime(tmp_path, api)
        await probes.launch(PROBES[0], ASSIGNMENT)
        api.run(pod_name(PROBES[0]))
        # Deleted while running, but the kubelet has not reported termination.
        assert not await probes.retire(PROBES[0])
        row = probes.registry.get(PROBES[0])
        assert row.state == "retiring" and row.termination is None
        assert not api.patches
        assert api.objects[("pods", pod_name(PROBES[0]))]["metadata"]["finalizers"] == [FINALIZER]

    asyncio.run(scenario())


def test_a_restarted_control_process_retires_without_resuming(tmp_path):
    async def scenario() -> None:
        api = Api()
        probes = runtime(tmp_path, api)
        await probes.launch(PROBES[0], ASSIGNMENT)
        api.run(pod_name(PROBES[0]))
        await probes.observe_runner(PROBES[0])
        probes.registry.close()
        restarted = runtime(tmp_path, api, boot=LATER_BOOT)
        assert restarted.registry.get(PROBES[0]).state == "retiring"
        with pytest.raises(LaunchControlUnavailable):
            await restarted.exercise(PROBES[0], Primitive())
        assert await restarted.retire_all()
        assert not api.objects

    asyncio.run(scenario())


def test_the_completions_fact_needs_every_probe_completed_retired_and_absent(tmp_path):
    async def scenario() -> None:
        api = Api()
        probes = runtime(tmp_path, api)
        fact = PrimitiveCompletions(probes, PROBES)
        assert fact.fact == FACT == "primitive_completions"
        for probe_id in PROBES:
            await probes.launch(probe_id, ASSIGNMENT)
            api.run(pod_name(probe_id))
            await probes.exercise(probe_id, Primitive())
        with pytest.raises(LaunchControlUnavailable):
            # Completed but not yet retired.
            await fact.observe()
        assert await probes.retire_all()
        first = await fact.observe()
        assert first == await fact.observe()
        assert len(first) == 64 and int(first, 16) >= 0
        # A same-name object reappearing is observed on the next call.
        api.objects[("services", "pl-s-" + uuid.UUID(PROBES[1]).hex)] = {"metadata": {}}
        with pytest.raises(LaunchControlUnavailable):
            await fact.observe()
        # Exactly the predeclared probes.
        with pytest.raises(LaunchControlUnavailable):
            await PrimitiveCompletions(probes, PROBES[:1]).observe()

    asyncio.run(scenario())


def test_a_probe_without_a_completion_refuses_the_fact(tmp_path):
    async def scenario() -> None:
        api = Api()
        api.kubelet = "never"
        probes = runtime(tmp_path, api)
        await probes.launch(PROBES[0], ASSIGNMENT)
        assert await probes.retire(PROBES[0])
        with pytest.raises(LaunchControlUnavailable):
            await PrimitiveCompletions(probes, PROBES[:1]).observe()

    asyncio.run(scenario())


def test_the_digest_binds_scope_and_completions(tmp_path):
    async def scenario(result: str, scope_dir: str) -> str:
        api = Api()
        path = tmp_path / scope_dir
        path.mkdir(mode=0o700)
        registry = ProbeRegistry.open(str(path), boot_id=BOOT, scope_digest=SCOPE)
        probes = PrimitiveProbeRuntime(registry, api, profile=PROFILE, projection=projection())
        await probes.launch(PROBES[0], ASSIGNMENT)
        api.run(pod_name(PROBES[0]))
        await probes.exercise(PROBES[0], Primitive(result))
        await probes.retire(PROBES[0])
        return await PrimitiveCompletions(probes, PROBES[:1]).observe()

    one = asyncio.run(scenario("d" * 64, "one"))
    two = asyncio.run(scenario(hashlib.sha256(b"other").hexdigest(), "two"))
    assert one != two


def test_the_runtime_is_bound_to_the_profile_its_registry_pins(tmp_path):
    path = tmp_path / "probes"
    path.mkdir(mode=0o700)
    registry = ProbeRegistry.open(str(path), boot_id=BOOT, scope_digest="b" * 64)
    with pytest.raises(LaunchControlUnavailable):
        PrimitiveProbeRuntime(registry, Api(), profile=PROFILE, projection=projection())
    other = projection_value()
    other["namespace"] = "elsewhere"
    with pytest.raises(LaunchControlUnavailable):
        PrimitiveProbeRuntime(
            registry, Api(), profile=PROFILE, projection=parse_projection(canonical(other))
        )
    assert repr(ASSIGNMENT) == "<ProbeAssignment>"


class Hanging(Api):
    """Pod creates and, once armed, reads never answer."""

    def __init__(self, *, hang_create: bool = False) -> None:
        super().__init__()
        self.hang_create, self.hang_reads = hang_create, False

    async def create(self, namespace: str, kind: str, document: dict[str, Any]) -> dict[str, Any]:
        if kind == "pods" and self.hang_create:
            # The API server persisted the Pod; its answer never arrives.
            await super().create(namespace, kind, document)
            await asyncio.Event().wait()
        return await super().create(namespace, kind, document)

    async def read(self, namespace: str, kind: str, name: str) -> dict[str, Any] | None:
        if self.hang_reads:
            await asyncio.Event().wait()
        return await super().read(namespace, kind, name)


def test_the_completions_fact_refuses_at_its_deadline(tmp_path, monkeypatch):
    from curie_worker.managed_cni_probe_kit import PRODUCER_CONSTANTS

    async def scenario() -> None:
        api = Hanging()
        probes = runtime(tmp_path, api)
        await probes.launch(PROBES[0], ASSIGNMENT)
        api.run(pod_name(PROBES[0]))
        await probes.exercise(PROBES[0], Primitive())
        assert await probes.retire(PROBES[0])
        fact = PrimitiveCompletions(probes, PROBES[:1])
        await fact.observe()
        monkeypatch.setitem(PRODUCER_CONSTANTS, "per_call_deadline_ms", 50)
        api.hang_reads = True
        started = asyncio.get_running_loop().time()
        with pytest.raises(LaunchControlUnavailable):
            await fact.observe()
        assert asyncio.get_running_loop().time() - started < 1

    asyncio.run(scenario())


def test_a_create_cancelled_in_flight_is_uncertain_and_only_fenced(tmp_path):
    async def scenario() -> None:
        api = Hanging(hang_create=True)
        probes = runtime(tmp_path, api)
        task = asyncio.create_task(probes.launch(PROBES[0], ASSIGNMENT))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        row = probes.registry.get(PROBES[0])
        assert row.state == "uncertain" and row.objects.pod_uid is None
        pod = pod_name(PROBES[0])
        assert ("pods", pod) in api.objects
        # Never retired; the Secret and Service are deleted by their recorded
        # UIDs, and the Pod whose UID was never recorded is left untouched.
        assert not await probes.retire_all()
        assert probes.registry.get(PROBES[0]).state == "uncertain"
        assert set(api.objects) == {("pods", pod)}
        assert "deletionTimestamp" not in api.objects[("pods", pod)]["metadata"]

    asyncio.run(scenario())


def test_a_crash_after_a_create_leaves_the_probe_uncertain_after_restart(tmp_path):
    async def scenario() -> None:
        api = Api()
        probes = runtime(tmp_path, api)
        row = probes.registry.create(
            PROBES[0], namespace=NAMESPACE, create_operation_id=str(uuid.uuid4())
        )
        # The create reached the API server; the process died before recording it.
        secret = "pl-c-" + uuid.UUID(PROBES[0]).hex
        await api.create(NAMESPACE, "secrets", {"metadata": {"name": secret}})
        assert row.state == "preparing"
        probes.registry.close()
        restarted = runtime(tmp_path, api, boot=LATER_BOOT)
        assert restarted.registry.get(PROBES[0]).state == "uncertain"
        assert not await restarted.retire_all()
        assert ("secrets", secret) in api.objects

    asyncio.run(scenario())


def test_a_retiring_probe_with_an_unrecorded_uid_settles_uncertain(tmp_path):
    async def scenario() -> None:
        api = Api()
        probes = runtime(tmp_path, api)
        row = probes.registry.create(
            PROBES[0], namespace=NAMESPACE, create_operation_id=str(uuid.uuid4())
        )
        row = probes.registry.record_object_uid(
            PROBES[0], row.revision, "secret", str(uuid.uuid4()), "1"
        )
        probes.registry.transition(PROBES[0], row.revision, "retiring")
        assert not await probes.retire(PROBES[0])
        assert probes.registry.get(PROBES[0]).state == "uncertain"

    asyncio.run(scenario())


def test_completions_from_an_earlier_boot_refuse(tmp_path):
    async def scenario() -> None:
        api = Api()
        probes = runtime(tmp_path, api)
        await probes.launch(PROBES[0], ASSIGNMENT)
        api.run(pod_name(PROBES[0]))
        await probes.exercise(PROBES[0], Primitive())
        assert await probes.retire(PROBES[0])
        await PrimitiveCompletions(probes, PROBES[:1]).observe()
        probes.registry.close()
        restarted = runtime(tmp_path, api, boot=LATER_BOOT)
        with pytest.raises(LaunchControlUnavailable):
            await PrimitiveCompletions(restarted, PROBES[:1]).observe()

    asyncio.run(scenario())
