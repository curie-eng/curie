"""Resource gated factory retries, worker half (#5427, ADR 0269).

Every test drives the real consumer, kernel, dispatch client, API routes, SQL
and reconciler pass, as ``test_factory_retry`` does. The Kubernetes control
plane is the harness's in-memory claim model, with three of its answers
replaced by the production ``KubernetesSandboxClient`` reading client models:
the template a claim resolves, the limits a created pod holds, and what ended a
pod. A created pod's spec is its template plus the agent's override, resolved
by that client, and a failed pod is a recorded kubelet status over that spec.
The quota footprint a claim is admitted under stays the harness's fixed one;
the sandbox suite covers the admission ceiling comparison.
"""

from __future__ import annotations

import asyncio
import copy
import json
from datetime import UTC, datetime
from typing import Any

import pytest
from aci_protocol import ErrorEvent, Final, SessionStatus, SideEffectFlag
from curie_worker.sandbox.k8s import KubernetesSandboxClient
from curie_worker.sandbox.types import DeliveredLimits, SandboxTermination
from kubernetes import client as k8s_client

from apps.worker.tests.factory_retry_fixtures import (
    WAIT_S,
    Call,
    FactoryRetryCase,
    Item,
    clean_failure,
    factory_retry,
    factory_retry_database,
)
from apps.worker.tests.kernel.conftest import FakeK8s
from apps.worker.tests.queue_fixtures import wait_until
from apps.worker.tests.sandbox.recorded_pods import (
    MI,
    NAMESPACE,
    TEMPLATE,
    client,
    event,
    pod,
    shape,
    templates,
)

__all__ = ["factory_retry", "factory_retry_database"]

RUNNING: dict[str, Any] = {"phase": "Running"}


def _node_disruption() -> dict[str, Any]:
    """The recorded storage probe as the taint manager deletes it.

    The kubelet's eviction reason and message are gone; the pod carries a
    deletion timestamp and the DisruptionTarget condition the control plane
    writes before it deletes a pod from a NoExecute tainted node.
    """

    disrupted = shape("ephemeral-probe")
    disrupted["status"].pop("reason")
    disrupted["status"].pop("message")
    disrupted["metadata"]["deletionTimestamp"] = datetime.now(UTC).isoformat()
    disrupted["status"]["conditions"].append(
        {"type": "DisruptionTarget", "status": "True", "reason": "DeletionByTaintManager"}
    )
    return disrupted


class _Cut:
    """A runner stream that breaks where this frame would be written.

    The fake runner's handler raises on it after the response has started,
    so the server closes the connection with the chunked body unfinished: the
    worker sees the transport drop a killed runner pod leaves.
    """

    def model_dump_json(self) -> str:
        raise ConnectionResetError("the runner process was killed")


class RecordedCluster:
    """The harness claim model with recorded pods behind its sandboxes."""

    def __init__(self, fake: FakeK8s, template_spec: dict[str, Any]) -> None:
        self.fake = fake
        self.objects = templates(copy.deepcopy(template_spec))
        self.k8s: KubernetesSandboxClient = client(self)
        # sandbox name -> the pod spec its claim resolved when it was created.
        self.specs: dict[str, dict[str, Any]] = {}
        # The recorded pod every sandbox reads as while set: what ended it.
        self.failure: dict[str, Any] | None = None
        # While set, the control plane accepts a claim delete and never finishes it.
        self.hold_deletes = False
        self.held: list[str] = []
        self._create_claim = fake.create_claim
        self._delete_claim = fake.delete_claim
        fake.create_claim = self.create_claim  # type: ignore[method-assign]
        fake.delete_claim = self.delete_claim  # type: ignore[method-assign]
        fake.claim_limits = self.k8s.claim_limits  # type: ignore[method-assign]
        fake.pod_limits = self.k8s.pod_limits  # type: ignore[method-assign]
        fake.pod_termination = self.pod_termination  # type: ignore[method-assign]

    @property
    def template(self) -> dict[str, Any]:
        spec: dict[str, Any] = self.objects[("sandboxtemplates", TEMPLATE)]["spec"]
        return spec["podTemplate"]["spec"]  # type: ignore[no-any-return]

    def create_claim(
        self,
        name: str,
        *,
        pool: str,
        runner_resources: dict[str, object] | None = None,
        agent_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        spec = self.k8s._claim_pod_spec(
            pool, runner_resources, agent_name, request_timeout_seconds=1.0
        )
        self._create_claim(
            name, pool=pool, runner_resources=runner_resources, agent_name=agent_name, **kwargs
        )
        self.specs[f"sbx-{name}"] = copy.deepcopy(spec)

    def delete_claim(self, name: str, *, request_timeout_seconds: float) -> None:
        if self.hold_deletes:
            self.held.append(name)
            return
        self._delete_claim(name, request_timeout_seconds=request_timeout_seconds)

    def release_held(self) -> None:
        """The held deletes finish."""

        self.hold_deletes = False
        for name in self.held:
            self._delete_claim(name, request_timeout_seconds=1.0)

    def pod_termination(
        self, name: str, *, request_timeout_seconds: float, since: datetime
    ) -> SandboxTermination | None:
        self.fake.termination_queries.append(name)
        return self.k8s.pod_termination(
            name, since=since, request_timeout_seconds=request_timeout_seconds
        )

    def _recorded(self, name: str) -> dict[str, Any]:
        failure = self.failure
        recorded = copy.deepcopy(failure) if failure is not None else {"metadata": {}}
        recorded["metadata"]["name"] = name
        recorded["metadata"].setdefault("uid", f"uid-{name}")
        recorded["spec"] = copy.deepcopy(self.specs[name])
        if failure is None:
            recorded["status"] = RUNNING
        return recorded

    # -- the CoreV1Api and CustomObjectsApi boundary ------------------------------

    def read_namespaced_pod(self, name: str, namespace: str, *, _request_timeout: float) -> Any:
        assert namespace == NAMESPACE and _request_timeout > 0
        if name not in self.specs:
            raise k8s_client.ApiException(status=404, reason="NotFound")
        return pod(self._recorded(name))

    def list_namespaced_event(
        self, namespace: str, *, field_selector: str, limit: int, _request_timeout: float
    ) -> Any:
        assert namespace == NAMESPACE and limit > 0 and _request_timeout > 0
        name = field_selector.rsplit("involvedObject.name=", 1)[1]
        if self.failure is None or name not in self.specs:
            return k8s_client.CoreV1EventList(items=[])
        uid = self._recorded(name)["metadata"]["uid"]
        now = datetime.now(UTC)
        return k8s_client.CoreV1EventList(
            items=[
                event(
                    {**raw, "involvedObject": {**raw["involvedObject"], "name": name}},
                    uid=uid,
                    at=now,
                )
                for raw in self.failure.get("events", [])
            ]
        )

    def get_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str, name: str, **_: Any
    ) -> dict[str, Any]:
        del group, version
        assert namespace == NAMESPACE
        found = self.objects.get((plural, name))
        if found is None:
            raise k8s_client.ApiException(status=404, reason="NotFound")
        return copy.deepcopy(found)


def _override(*, cpu: str = "200m", memory: str, storage: str) -> dict[str, Any]:
    return {
        "requests": {"cpu": "50m", "memory": "16Mi", "ephemeral-storage": "4Mi"},
        "limits": {"cpu": cpu, "memory": memory, "ephemeral-storage": storage},
    }


async def _set_override(case: FactoryRetryCase, resources: dict[str, Any] | None) -> None:
    """The operator's runner resource override on the agent, as the API stores it."""

    await case.execute(
        "UPDATE curie.agents SET runner_resources = CAST(:value AS jsonb) WHERE id = :id",
        {"value": None if resources is None else json.dumps(resources), "id": case.agent_id},
    )


async def _fails(
    case: FactoryRetryCase,
    cluster: RecordedCluster,
    item: Item,
    failure: dict[str, Any],
) -> Call:
    """Run ``item``'s turn until its stream drops on a pod that ``failure`` ended."""

    cluster.failure = failure
    case.script(end="drop")
    await case.publish_wake(item.request_id)
    finish = await case.finished(item.request_id)
    await case.settle()
    cluster.failure = None
    return finish


async def _waits(case: FactoryRetryCase, request_id: Any, reason: str) -> None:
    """One delivery of ``request_id`` defers ``reason`` and creates nothing."""

    claims = len(case.h.fake_k8s.claim_envs)
    opened = len(case.h.runner.opened)
    defers = len(case.calls_to("defer", request_id))
    before = await case.row(request_id)
    await case.publish_wake(request_id)
    await wait_until(
        lambda: len(case.calls_to("defer", request_id)) > defers,
        f"{request_id} to defer {reason}",
        timeout=WAIT_S,
    )
    await case.settle()
    deferral = case.calls_to("defer", request_id)[-1]
    assert (deferral.status, deferral.body["reason"], deferral.body["capacity"]) == (
        200,
        reason,
        False,
    )
    assert len(case.h.fake_k8s.claim_envs) == claims
    assert len(case.h.runner.opened) == opened
    assert case.calls_to("start", request_id) == []
    after = await case.row(request_id)
    assert after["status"] == "waiting"
    assert after["blocked_on"] == before["blocked_on"]
    # The wait is uncounted: neither a failed start nor a capacity wait.
    assert (after["start_deferrals"], after["capacity_deferrals"]) == (
        before["start_deferrals"],
        before["capacity_deferrals"],
    )


async def _starts(case: FactoryRetryCase, request_id: Any) -> Call:
    """One delivery of ``request_id`` claims one sandbox and starts once."""

    claims = len(case.h.fake_k8s.claim_envs)
    case.script(*clean_failure())
    await case.publish_wake(request_id)
    await case.finished(request_id)
    await case.settle()
    assert len(case.h.fake_k8s.claim_envs) == claims + 1
    (start,) = case.calls_to("start", request_id)
    assert start.status == 200
    row = await case.row(request_id)
    assert row["blocked_on"] is None and row["execution_attempts"] == 1
    return start


def _limits(
    *, memory: int | None, storage: int | None, scratch: dict[str, int], turns: int
) -> dict[str, Any]:
    return DeliveredLimits(
        memory_bytes=memory, ephemeral_storage_bytes=storage, scratch_bytes=scratch, max_turns=turns
    ).to_wire()


def _gate(row: dict[str, Any]) -> dict[str, Any]:
    gate = row["resource_gate"]
    assert isinstance(gate, dict), row
    return {key: gate[key] for key in ("condition", "dimension", "volume", "failed_limit")}


# -- memory and pod ephemeral storage: the override raises the limit -----------------------

LIMIT_CASES = [
    pytest.param(
        {
            "failure": "oom-probe",
            "cause": "sandbox_memory_exhausted",
            "gate": {
                "condition": "memory_limit",
                "dimension": "memory",
                "volume": None,
                "failed_limit": 48 * MI,
            },
            "first": {"memory": 48 * MI, "storage": 64 * MI},
            "unmet": [
                _override(memory="48Mi", storage="64Mi"),
                _override(memory="32Mi", storage="1Gi"),
                _override(cpu="400m", memory="48Mi", storage="64Mi"),
            ],
            "raised": _override(memory="96Mi", storage="64Mi"),
            "delivered": {"memory": 96 * MI, "storage": 64 * MI},
        },
        id="memory",
    ),
    pytest.param(
        {
            "failure": "ephemeral-probe",
            "cause": "sandbox_storage_exhausted",
            "gate": {
                "condition": "ephemeral_storage_limit",
                "dimension": "ephemeral_storage",
                "volume": None,
                "failed_limit": 12 * MI,
            },
            "first": {"memory": 64 * MI, "storage": 12 * MI},
            "unmet": [
                _override(memory="64Mi", storage="12Mi"),
                _override(memory="1Gi", storage="8Mi"),
                _override(cpu="400m", memory="64Mi", storage="12Mi"),
            ],
            "raised": _override(memory="64Mi", storage="24Mi"),
            "delivered": {"memory": 64 * MI, "storage": 24 * MI},
        },
        id="ephemeral-storage",
    ),
]


@pytest.mark.parametrize("limit", LIMIT_CASES)
def test_limit_exhaustion_is_gated_until_a_claim_would_deliver_strictly_more(
    factory_retry: FactoryRetryCase, limit: dict[str, Any]
) -> None:
    case = factory_retry
    cluster = RecordedCluster(case.h.fake_k8s, shape(limit["failure"])["spec"])
    turns = case.h.config.work_item_max_turns

    async def exercise() -> None:
        item = case.item
        finish = await _fails(case, cluster, item, shape(limit["failure"]))
        assert finish.facts == {
            "cause": limit["cause"],
            "retry_safety": "no_effects",
            "retry_condition": None,
        }
        assert finish.body["exhausted_volume"] is None
        (start,) = case.calls_to("start", item.request_id)
        assert start.body["delivered_limits"] == _limits(**limit["first"], scratch={}, turns=turns)
        predecessor = (await case.row(item.request_id))["runtime_claim_name"]
        await wait_until(
            lambda: predecessor in case.h.fake_k8s.deleted_claims,
            "the failed claim to be deleted",
            timeout=WAIT_S,
        )
        successor = await case.successor(item.request_id)
        assert successor is not None and successor["status"] == "waiting", successor
        assert successor["blocked_on"] == limit["gate"]["condition"]
        assert _gate(successor) == limit["gate"]
        assert await case.used(item.work_item_id) == 1
        waiting = successor["id"]

        # The template is unchanged: the next claim would fail the same way.
        await _waits(case, waiting, "resource_gate_unmet")
        # An equal limit, a smaller one, and a change to another dimension only.
        for resources in limit["unmet"]:
            await _set_override(case, resources)
            await _waits(case, waiting, "resource_gate_unmet")
        # A larger scratch volume says nothing about this dimension.
        await _set_override(case, None)
        cluster.template["volumes"].append({"name": "cache", "emptyDir": {"sizeLimit": "1Gi"}})
        await _waits(case, waiting, "resource_gate_unmet")

        # Raised, but a live route still holds the thread.
        await _set_override(case, limit["raised"])
        await asyncio.to_thread(case.h.substrate.claim, item.thread)
        await _waits(case, waiting, "resource_gate_unmet")
        await asyncio.to_thread(case.h.substrate.release, item.thread, wait_gone=True)

        start = await _starts(case, waiting)
        assert start.body["delivered_limits"] == _limits(
            **limit["delivered"], scratch={"cache": 1024 * MI}, turns=turns
        )
        assert len(await case.requests(item.work_item_id)) == 3

    case.run(exercise())


# -- scratch: only a larger template sizeLimit releases it -----------------------------------


def test_scratch_exhaustion_is_released_only_by_a_larger_template_size_limit(
    factory_retry: FactoryRetryCase,
) -> None:
    case = factory_retry
    cluster = RecordedCluster(case.h.fake_k8s, shape("scratch-probe")["spec"])
    turns = case.h.config.work_item_max_turns

    async def exercise() -> None:
        item = case.item
        finish = await _fails(case, cluster, item, shape("scratch-probe"))
        assert finish.facts == {
            "cause": "sandbox_scratch_exhausted",
            "retry_safety": "no_effects",
            "retry_condition": None,
        }
        assert finish.body["exhausted_volume"] == "workspace"
        (start,) = case.calls_to("start", item.request_id)
        assert start.body["delivered_limits"] == _limits(
            memory=64 * MI, storage=64 * MI, scratch={"workspace": 8 * MI}, turns=turns
        )
        successor = await case.successor(item.request_id)
        assert successor is not None and successor["blocked_on"] == "scratch_limit"
        assert _gate(successor) == {
            "condition": "scratch_limit",
            "dimension": "scratch",
            "volume": "workspace",
            "failed_limit": 8 * MI,
        }
        waiting = successor["id"]

        await _waits(case, waiting, "resource_gate_unmet")
        # The override replaces container resources and never a volume's sizeLimit.
        await _set_override(case, _override(memory="1Gi", storage="1Gi"))
        await _waits(case, waiting, "resource_gate_unmet")
        # Another volume growing is not this volume growing.
        cluster.template["volumes"].append({"name": "cache", "emptyDir": {"sizeLimit": "1Gi"}})
        await _waits(case, waiting, "resource_gate_unmet")

        cluster.template["volumes"][0]["emptyDir"]["sizeLimit"] = "16Mi"
        start = await _starts(case, waiting)
        assert start.body["delivered_limits"] == _limits(
            memory=1024 * MI,
            storage=1024 * MI,
            scratch={"workspace": 16 * MI, "cache": 1024 * MI},
            turns=turns,
        )

    case.run(exercise())


# -- what never becomes a gated retry -----------------------------------------------------


def test_side_effect_before_an_oom_kill_reports_flagged_and_admits_nothing(
    factory_retry: FactoryRetryCase,
) -> None:
    case = factory_retry
    cluster = RecordedCluster(case.h.fake_k8s, shape("oom-probe")["spec"])

    async def exercise() -> None:
        item = case.item
        cluster.failure = shape("oom-probe")
        case.script(SideEffectFlag(tool="deploy"), end="stall", tail=(_Cut(),))
        event_id = await case.publish_wake(item.request_id)
        await case.held()
        # The runner pod dies only once the worker has recorded the side effect.
        await wait_until(
            lambda: bool(case.redis.exists(case.h.config.side_effect_key(event_id))),
            "the side effect marker",
            timeout=WAIT_S,
        )
        case.release_turn()
        finish = await case.finished(item.request_id)
        await case.settle()
        cluster.failure = None
        assert finish.facts == {
            "cause": "sandbox_memory_exhausted",
            "retry_safety": "flagged",
            "retry_condition": None,
        }
        assert await case.successor(item.request_id) is None
        assert await case.used(item.work_item_id) == 0

    case.run(exercise())


def test_model_text_naming_an_oom_kill_diagnoses_nothing(factory_retry: FactoryRetryCase) -> None:
    case = factory_retry
    cluster = RecordedCluster(case.h.fake_k8s, shape("oom-probe")["spec"])

    async def exercise() -> None:
        item = case.item
        queried = len(case.h.fake_k8s.termination_queries)
        case.script(
            ErrorEvent(
                message="Container runner was OOMKilled after exceeding its 48Mi memory limit.",
                classification="runner-error",
            ),
            Final(text="The run failed.", status=SessionStatus.CLASSIFIED_FAILURE),
        )
        await case.publish_wake(item.request_id)
        finish = await case.finished(item.request_id)
        await case.settle()
        assert finish.facts["cause"] == "runner_error"
        assert finish.body["exhausted_volume"] is None
        # The runner ended its own turn, so the control plane was never asked.
        assert len(case.h.fake_k8s.termination_queries) == queried
        assert cluster.failure is None
        successor = await case.successor(item.request_id)
        assert successor is None or successor["resource_gate"] is None, successor

    case.run(exercise())


# -- node disruption: an ungated successor that still fences its predecessor ----------------


def test_node_disruption_successor_is_scheduled_and_waits_for_the_predecessor_claim(
    factory_retry: FactoryRetryCase,
) -> None:
    case = factory_retry
    cluster = RecordedCluster(case.h.fake_k8s, shape("ephemeral-probe")["spec"])

    async def exercise() -> None:
        item = case.item
        # The control plane accepts the failed claim's delete and never finishes it.
        cluster.hold_deletes = True
        finish = await _fails(case, cluster, item, _node_disruption())
        assert finish.facts == {
            "cause": "sandbox_node_disrupted",
            "retry_safety": "no_effects",
            "retry_condition": None,
        }
        predecessor = (await case.row(item.request_id))["runtime_claim_name"]
        await wait_until(
            lambda: predecessor in cluster.held,
            "the failed claim's delete to be accepted",
            timeout=WAIT_S,
        )
        assert predecessor in case.h.fake_k8s.claims
        successor = await case.successor(item.request_id)
        assert successor is not None and successor["status"] == "waiting", successor
        assert (successor["blocked_on"], successor["resource_gate"]) == (None, None)
        waiting = successor["id"]

        await _waits(case, waiting, "predecessor_runtime_present")
        await _waits(case, waiting, "predecessor_runtime_present")

        cluster.release_held()
        assert predecessor not in case.h.fake_k8s.claims
        await _starts(case, waiting)

    case.run(exercise())


# -- the turn budget: the worker's own configuration raises it ------------------------------


def test_turn_limit_gate_opens_when_the_work_item_turn_budget_is_raised(
    factory_retry: FactoryRetryCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    case = factory_retry
    turns = case.h.config.work_item_max_turns

    async def exercise() -> None:
        item = case.item
        case.script(
            ErrorEvent(message="Example provider error.", classification="max-turns"),
            Final(text="The run failed.", status=SessionStatus.CLASSIFIED_FAILURE),
        )
        await case.publish_wake(item.request_id)
        finish = await case.finished(item.request_id)
        await case.settle()
        assert finish.facts == {
            "cause": "max_turns",
            "retry_safety": "no_effects",
            "retry_condition": None,
        }
        (start,) = case.calls_to("start", item.request_id)
        assert start.body["delivered_limits"]["max_turns"] == turns
        successor = await case.successor(item.request_id)
        assert successor is not None and successor["blocked_on"] == "turn_limit"
        assert _gate(successor) == {
            "condition": "turn_limit",
            "dimension": "max_turns",
            "volume": None,
            "failed_limit": turns,
        }
        waiting = successor["id"]

        await _waits(case, waiting, "resource_gate_unmet")
        # A worker restarted with a larger CURIE_WORK_ITEM_MAX_TURNS.
        monkeypatch.setattr(
            case.h.kernel,
            "_config",
            case.h.config.model_copy(update={"work_item_max_turns": turns + 1}),
        )
        start = await _starts(case, waiting)
        assert start.body["delivered_limits"]["max_turns"] == turns + 1

    case.run(exercise())
