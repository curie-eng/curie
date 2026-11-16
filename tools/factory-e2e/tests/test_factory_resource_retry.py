"""Live candidate-install proof for #5427 acceptance criterion 2 (ADR 0269).

A factory request whose runner ran out of memory, or whose pod the kubelet
evicted for ephemeral storage, must not run again under the same limit. Its
one automatic successor waits, sandbox-free, until the agent's runner limit
for that dimension is strictly larger, then starts exactly once with the
raised limit delivered.

Everything inside the changed path is the candidate: the API, worker,
reconciler, scheduler and resource resolution run from the caller's candidate
images, installed by the candidate chart and driven through the candidate
CLI. The only fixtures are the external model (the runner's offline fake tier,
told what to exhaust by ``FAKE_MODEL_DIRECTIVE`` set per agent through
``agentSandbox.runnerEnv``, because a factory turn's text is only the issue
link) and the external forge (the recorded TLS GitHub stub, run as one Pod in
the owned namespace on the candidate worker image).

Runbook
-------
Run only against a throwaway cluster the caller owns::

    CURIE_FACTORY_RESOURCE_RETRY_E2E=1 \\
    CURIE_FACTORY_KUBE_CONTEXT=<kube context> \\
    CURIE_FACTORY_RESOURCE_RETRY_NAMESPACE=<new namespace> \\
    CURIE_FACTORY_RESOURCE_RETRY_RELEASE=curie-<suffix> \\
    CURIE_FACTORY_RESOURCE_RETRY_PULL_POLICY=IfNotPresent \\
    CURIE_C4_CANDIDATE_CLI=<absolute path to the compiled candidate curie> \\
    CURIE_C4_CANDIDATE_IMAGES='<registry>/curie-{component}:<tag or @digest>' \\
    uv run pytest -q tools/factory-e2e/tests/test_factory_resource_retry.py --reruns 0

Inputs, all required once the flag is set (a missing or malformed one is a
refusal, never a skip):

1. ``CURIE_FACTORY_KUBE_CONTEXT``: the kube context every kubectl and helm
   call names. The cluster must run Kubernetes 1.30 or newer, already run the
   agent-sandbox controller and serve every CRD in ``charts/curie/crds``; this
   test never installs a cluster-wide CRD or controller.
2. ``CURIE_FACTORY_RESOURCE_RETRY_NAMESPACE``: a namespace that must not
   exist yet. The test creates it and deletes it.
3. ``CURIE_FACTORY_RESOURCE_RETRY_RELEASE``: the Helm release, ``curie-`` and
   a suffix, at most 30 characters, never ``curie``. The chart names its
   cluster-scoped objects from it.
4. ``CURIE_C4_CANDIDATE_CLI``: the compiled candidate CLI.
5. ``CURIE_C4_CANDIDATE_IMAGES``: an image reference template with
   ``{component}``, expanded for ``api``, ``worker`` and ``runner``. Each
   reference must be a registry tag or digest this host can inspect with
   ``docker buildx imagetools inspect`` and the cluster can pull.
6. ``CURIE_FACTORY_RESOURCE_RETRY_PULL_POLICY``: ``Always`` or
   ``IfNotPresent``, applied to every candidate image.

Before anything is created the test proves the CLI runs, every image resolves
to a digest, the context answers, the namespace is absent and no object the
release would create at cluster scope exists. Every cleanup is registered
before the resource it removes is created and runs on every exit: stop the
port-forward and the pod watch, uninstall the release, delete the namespace,
delete any remaining cluster-scoped object of this release (the rendered
ValidatingAdmissionPolicies and bindings such as ``<release>-runner-resources``,
the per-release PriorityClasses ``<release>-platform`` and
``<release>-sandbox``, the publication namespace, and any ClusterRole or
binding the render names), then prove each one absent.

Sizes stay small so no shared node fills: memory limits 448Mi then 1408Mi
around a 768 MiB hold, ephemeral-storage limits 256Mi then 896Mi around a
384 MiB scratch file in the runner's 512Mi /tmp volume, so the kubelet's
pod-level ephemeral-storage eviction fires, not the volume's own limit.
"""

from __future__ import annotations

import copy
import json
import os
import re
import shutil
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import candidate_install as ci
import pytest
import resource_retry_support as rrs

FLAG = "CURIE_FACTORY_RESOURCE_RETRY_E2E"
if os.environ.get(FLAG) != "1":
    pytest.skip(f"set {FLAG}=1 to run the live resource retry proof", allow_module_level=True)

NAMES = ci.InputNames(
    flag=FLAG,
    namespace="CURIE_FACTORY_RESOURCE_RETRY_NAMESPACE",
    release="CURIE_FACTORY_RESOURCE_RETRY_RELEASE",
    pull_policy="CURIE_FACTORY_RESOURCE_RETRY_PULL_POLICY",
)
CLAIM_RESOURCE = ci.CLAIM_RESOURCE
ADMISSION_CLAIM_LABEL = ci.ADMISSION_CLAIM_LABEL
DIRECTIVE_ENV = "FAKE_MODEL_DIRECTIVE"
SEED_FILES = {
    "README.md": "# acme-bot\n\nFixture repository for the resource retry proof.\n",
    "example.py": 'def example() -> str:\n    return "example"\n',
}
SEED_ISSUE = "Resource retry proof fixture.\n"
# The API re-offers a gated successor this often (WORK_ITEM_RESOURCE_GATE_PROBE_SECONDS).
PROBE_SECONDS = 20
# How long a waiting successor is watched for a start that must not happen.
GATE_WINDOW_SECONDS = 3 * PROBE_SECONDS + 10
SETTLE_SECONDS = 2 * PROBE_SECONDS + 10
TERMINAL = ci.TERMINAL
_QUANTITY = re.compile(r"^([0-9]+)(Ki|Mi|Gi|Ti|k|M|G|T)?$")
_SCALE = {
    None: 1,
    "Ki": 1024,
    "Mi": 1024**2,
    "Gi": 1024**3,
    "Ti": 1024**4,
    "k": 1000,
    "M": 1000**2,
    "G": 1000**3,
    "T": 1000**4,
}


def quantity_bytes(raw: Any) -> int:
    """A Kubernetes memory or storage quantity in bytes. Pure."""

    match = _QUANTITY.match(str(raw).strip())
    if match is None:
        raise ValueError(f"unsupported quantity {raw!r}")
    return int(match.group(1)) * _SCALE[match.group(2)]


# --------------------------------------------------------------------------
# The two scenarios
# --------------------------------------------------------------------------


def _resources(
    *, memory_request: str, memory_limit: str, storage_request: str, storage_limit: str
) -> dict[str, Any]:
    return {
        "requests": {
            "cpu": "100m",
            "memory": memory_request,
            "ephemeral-storage": storage_request,
        },
        "limits": {"cpu": "1", "memory": memory_limit, "ephemeral-storage": storage_limit},
    }


@dataclass(frozen=True)
class Phase:
    agent: str
    label: str
    directive: str
    cause: str
    condition: str
    dimension: str
    delivered_key: str
    low: dict[str, Any]
    high: dict[str, Any]
    low_bytes: int
    high_bytes: int
    # How long the failing attempt may take to end, kubelet cadence included.
    failure_timeout: float


MEMORY = Phase(
    agent="rr-memory",
    label="rr-memory-work",
    directive="[fake:hold-memory 768]",
    cause="sandbox_memory_exhausted",
    condition="memory_limit",
    dimension="memory",
    delivered_key="memory_bytes",
    low=_resources(
        memory_request="128Mi", memory_limit="448Mi", storage_request="64Mi", storage_limit="512Mi"
    ),
    high=_resources(
        memory_request="128Mi", memory_limit="1408Mi", storage_request="64Mi", storage_limit="512Mi"
    ),
    low_bytes=448 * 1024**2,
    high_bytes=1408 * 1024**2,
    failure_timeout=900,
)
STORAGE = Phase(
    agent="rr-storage",
    label="rr-storage-work",
    directive="[fake:fill-scratch 384]",
    cause="sandbox_storage_exhausted",
    condition="ephemeral_storage_limit",
    dimension="ephemeral_storage",
    delivered_key="ephemeral_storage_bytes",
    low=_resources(
        memory_request="128Mi", memory_limit="768Mi", storage_request="64Mi", storage_limit="256Mi"
    ),
    high=_resources(
        memory_request="128Mi", memory_limit="768Mi", storage_request="64Mi", storage_limit="896Mi"
    ),
    low_bytes=256 * 1024**2,
    high_bytes=896 * 1024**2,
    failure_timeout=1200,
)
PHASES = (MEMORY, STORAGE)


def runner_limit(pod: dict[str, Any], dimension: str) -> int:
    """The limit the kubelet enforces for ``dimension`` on this pod's spec. Pure.

    Memory is the runner container's own limit. Ephemeral storage is the sum of
    the regular containers' limits, the quantity a pod-level eviction compares.
    """

    containers = (pod.get("spec") or {}).get("containers") or []
    if dimension == "memory":
        runner = next((c for c in containers if c.get("name") == "runner"), None)
        if runner is None:
            raise ValueError("the sandbox pod has no runner container")
        return quantity_bytes(runner["resources"]["limits"]["memory"])
    return sum(quantity_bytes(c["resources"]["limits"]["ephemeral-storage"]) for c in containers)


def exhaustion_seen(pods: list[dict[str, Any]], dimension: str) -> bool:
    """Whether any observed state of the pod shows the kernel or kubelet ending it. Pure."""

    for pod in pods:
        status = pod.get("status") or {}
        if dimension == "memory":
            for container in status.get("containerStatuses") or []:
                if container.get("name") != "runner":
                    continue
                for key in ("state", "lastState"):
                    terminated = (container.get(key) or {}).get("terminated") or {}
                    if terminated.get("reason") == "OOMKilled":
                        return True
        elif status.get("reason") == "Evicted" and "ephemeral local storage" in str(
            status.get("message") or ""
        ):
            return True
    return False


# The kubelet's own storage eviction sentences (pkg/kubelet/eviction), matched whole.
_POD_STORAGE_EVICTION = re.compile(
    r"Pod ephemeral local storage usage exceeds the total limit of containers (\S+)\. ?"
)
_RUNNER_STORAGE_EVICTION = re.compile(
    r'Container runner exceeded its local ephemeral storage limit "(\S+)"\. ?'
)


def _sentence_bytes(pattern: re.Pattern[str], message: Any) -> int | None:
    match = pattern.fullmatch(str(message or ""))
    if match is None:
        return None
    try:
        return quantity_bytes(match.group(1))
    except ValueError:
        return None


def eviction_event_seen(events: list[dict[str, Any]], pods: list[dict[str, Any]]) -> bool:
    """Whether the kubelet's Evicted event names one of ``pods`` and its storage limit. Pure.

    An evicted runner pod is replaced in seconds, so the watch may never see
    its Evicted status. The core/v1 Event the kubelet wrote is the same
    observation: reason Evicted, about a pod UID observed for the failed
    claim, and either the pod-level sentence stating that pod's containers'
    total or the runner's container-level sentence stating that pod's runner
    limit.
    """

    limits: dict[str, tuple[int | None, int | None]] = {}
    for pod in pods:
        uid = (pod.get("metadata") or {}).get("uid")
        if not isinstance(uid, str) or not uid:
            continue
        try:
            total: int | None = runner_limit(pod, "ephemeral_storage")
        except (KeyError, TypeError, ValueError):
            total = None
        runner: int | None = None
        for container in (pod.get("spec") or {}).get("containers") or []:
            raw = ((container.get("resources") or {}).get("limits") or {}).get("ephemeral-storage")
            if container.get("name") == "runner" and raw is not None:
                runner = quantity_bytes(raw)
        limits[uid] = (total, runner)
    for event in events:
        involved = event.get("involvedObject") or {}
        bound = limits.get(involved.get("uid"))
        if event.get("reason") != "Evicted" or involved.get("kind") != "Pod" or bound is None:
            continue
        total, runner = bound
        message = event.get("message")
        pod_level = _sentence_bytes(_POD_STORAGE_EVICTION, message)
        if pod_level is not None and pod_level == total:
            return True
        container_level = _sentence_bytes(_RUNNER_STORAGE_EVICTION, message)
        if container_level is not None and container_level == runner:
            return True
    return False


def _brief(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    keys = (
        "id",
        "sequence",
        "status",
        "terminal_cause",
        "retry_safety",
        "blocked_on",
        "start_deferrals",
        "last_deferral_reason",
        "last_start_failure_detail",
        "termination_observation",
        "started_at",
        "runtime_claim_name",
        "delivered_limits",
        "resource_gate",
        "retry_of_request_id",
        "dispatch_generation",
        "dispatch_not_before",
        "version",
    )
    return {key: row.get(key) for key in keys}


# --------------------------------------------------------------------------
# The live harness
# --------------------------------------------------------------------------


@dataclass
class Live(ci.CandidateInstall):
    agents: dict[str, str] = field(default_factory=dict)
    log_name: str = "resource-retry"

    def values(self) -> dict[str, Any]:
        values = self.base_values()
        agents = [phase.agent for phase in PHASES]
        sandbox = values["agentSandbox"]
        sandbox["runnerEnv"] = {phase.agent: {DIRECTIVE_ENV: phase.directive} for phase in PHASES}
        sandbox["agentResourceProfiles"] = dict.fromkeys(agents, "standard")
        values["factory"]["sandboxEgress"] = dict.fromkeys(agents, True)
        api_env = list(values["api"].get("extraEnv") or [])
        api_env.append(
            {"name": "WORK_ITEM_RESOURCE_GATE_PROBE_SECONDS", "value": str(PROBE_SECONDS)}
        )
        values["api"]["extraEnv"] = api_env
        return values

    def deploy_agents(self) -> None:
        for phase in PHASES:
            self.agents[phase.agent] = self.create_agent(phase.agent, phase.label)
            self.set_resources(phase, phase.low)
            self.deploy_bundle(phase.agent)
            self.log(f"agent {phase.agent} deployed with {phase.dimension} limit {phase.low_bytes}")

    def set_resources(self, phase: Phase, resources: dict[str, Any]) -> None:
        self.api(
            "PATCH",
            f"/agents/{self.agents[phase.agent]}",
            {"runner_resources": copy.deepcopy(resources)},
        )

    def phase_issue(self, phase: Phase) -> int:
        return self.seed_issue(f"Resource retry proof: {phase.dimension}", SEED_ISSUE, phase.label)

    def phase_requests(self, phase: Phase, issue: int) -> list[dict[str, Any]]:
        return self.requests(self.agents[phase.agent], issue)


def wait_for(what: str, timeout: float, probe: Any, observed: Any) -> Any:
    return ci.wait_for(what, timeout, probe, observed)


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------


@pytest.fixture
def inputs() -> ci.Inputs:
    try:
        return ci.read_inputs(dict(os.environ), NAMES)
    except ci.Refused as exc:
        refusal = str(exc)
    pytest.fail(refusal, pytrace=False)


@pytest.fixture(autouse=True)
def candidate_curie(inputs: ci.Inputs, monkeypatch: pytest.MonkeyPatch) -> Path:
    """The real compiled candidate CLI, in place of conftest's fake CURIE_BIN."""

    monkeypatch.setenv("CURIE_BIN", inputs.cli)
    return Path(inputs.cli)


@pytest.fixture
def live(inputs: ci.Inputs) -> Iterator[Live]:
    workdir = Path(tempfile.mkdtemp(prefix="resource-retry-"))
    workdir.chmod(0o700)
    harness = Live(inputs=inputs, workdir=workdir)
    harness.teardown.push("private workdir", lambda: shutil.rmtree(workdir, ignore_errors=True))
    try:
        try:
            harness.preflight()
        except ci.Refused as exc:
            refusal: str | None = f"refused: {exc}"
        else:
            refusal = None
        if refusal is not None:
            pytest.fail(refusal, pytrace=False)
        yield harness
    finally:
        results = harness.teardown.run()
        failed = [f"{r['step']}: {r['detail']}" for r in results if not r["ok"]]
        for result in results:
            harness.log(f"teardown {result['step']}: {'ok' if result['ok'] else 'FAILED'}")
        if failed:
            pytest.fail("teardown did not prove every owned resource absent: " + "; ".join(failed))


# --------------------------------------------------------------------------
# The proof
# --------------------------------------------------------------------------


def prove_phase(live: Live, phase: Phase) -> None:
    issue = live.phase_issue(phase)
    live.log(f"{phase.dimension}: issue {issue} labelled {phase.label}")

    def rows() -> list[dict[str, Any]]:
        return live.phase_requests(phase, issue)

    def brief() -> list[dict[str, Any] | None]:
        return [_brief(row) for row in rows()]

    first = wait_for("the labelled issue to be admitted", 300, lambda: rows()[:1], brief)[0]
    first = wait_for(
        f"the first {phase.dimension} attempt to end",
        phase.failure_timeout,
        lambda: next(
            (r for r in rows() if r["id"] == first["id"] and r["status"] in TERMINAL), None
        ),
        brief,
    )
    assert first["terminal_cause"] == phase.cause and first["retry_safety"] == "no_effects", (
        f"the first attempt ended {first['terminal_cause']!r} with safety "
        f"{first['retry_safety']!r}, expected {phase.cause!r} and 'no_effects': {_brief(first)}"
    )
    claim = first.get("runtime_claim_name")
    assert claim, f"the failed attempt recorded no sandbox claim: {_brief(first)}"
    failed_pods = live.pods_for(str(claim))
    assert failed_pods, f"no pod of claim {claim} was observed"
    spec_limit = runner_limit(failed_pods[0], phase.dimension)
    seen = exhaustion_seen(failed_pods, phase.dimension)
    evictions: list[dict[str, Any]] = []
    if not seen and phase.dimension == "ephemeral_storage":
        # The evicted pod can vanish before the watch sees its Evicted status;
        # the kubelet's own event about that pod's UID is the same observation.
        evictions = json.loads(
            live.kubectl(
                "-n",
                live.inputs.namespace,
                "get",
                "events",
                "--field-selector",
                "reason=Evicted",
                "-o",
                "json",
            )
        )["items"]
        seen = eviction_event_seen(evictions, failed_pods)
    assert seen, (
        f"no observed state of claim {claim}'s pod shows the {phase.dimension} exhaustion: "
        f"pod reasons {[(p.get('status') or {}).get('reason') for p in failed_pods]}, "
        f"pod uids {sorted({(p.get('metadata') or {}).get('uid') for p in failed_pods} - {None})}, "
        f"Evicted events {[(e.get('involvedObject') or {}).get('uid') for e in evictions]} "
        f"{[e.get('message') for e in evictions]}"
    )
    delivered = (first.get("delivered_limits") or {}).get(phase.delivered_key)
    assert delivered == spec_limit == phase.low_bytes, (
        f"the failed attempt delivered {phase.delivered_key}={delivered}, its pod spec enforced "
        f"{spec_limit}, the override set {phase.low_bytes}"
    )

    def successors() -> list[dict[str, Any]]:
        return [r for r in rows() if r.get("retry_of_request_id") == first["id"]]

    found = wait_for("the gated successor", 300, successors, brief)
    assert len(found) == 1, f"expected one successor, observed {[_brief(r) for r in found]}"
    successor = found[0]
    gate = successor.get("resource_gate") or {}
    assert (
        successor["status"] == "waiting"
        and successor["blocked_on"] == phase.condition
        and gate.get("condition") == phase.condition
        and gate.get("dimension") == phase.dimension
        and gate.get("failed_limit") == delivered
        and gate.get("source_request_id") == first["id"]
    ), f"the successor is not gated on the failed {phase.dimension} limit: {_brief(successor)}"

    # One probe period and more: the successor stays waiting, uncounted, sandbox-free.
    claims_before = live.sandbox_claims()
    pod_claims_before = live.sandbox_pod_claims()
    deferrals = successor["start_deferrals"]
    probed = False
    # A resource gate re-probe defers the row by bumping its dispatch generation
    # and not-before; the row version does not move.
    probes = {(successor.get("dispatch_generation"), successor.get("dispatch_not_before"))}

    def still_gated() -> None:
        nonlocal probed
        current = next(r for r in rows() if r["id"] == successor["id"])
        assert (
            current["status"] == "waiting"
            and current["blocked_on"] == phase.condition
            and current["start_deferrals"] == deferrals
            and not current.get("runtime_claim_name")
            and not current.get("started_at")
        ), f"the gated successor moved under the unchanged limit: {_brief(current)}"
        if current.get("last_deferral_reason") == "resource_gate_unmet":
            probes.add((current.get("dispatch_generation"), current.get("dispatch_not_before")))
            probed = probed or len(probes) > 1

    rrs.observe_for(GATE_WINDOW_SECONDS, still_gated, interval=3)
    assert probed, (
        f"no resource_gate_unmet probe was observed within {GATE_WINDOW_SECONDS}s: "
        f"{_brief(next(r for r in rows() if r['id'] == successor['id']))}"
    )
    new_claims = live.sandbox_claims() - claims_before
    new_pods = live.sandbox_pod_claims() - pod_claims_before
    assert not new_claims and not new_pods, (
        f"the gated wait was not sandbox-free: new claims {sorted(new_claims)}, "
        f"new sandbox pods for claims {sorted(new_pods)}"
    )
    assert len(rows()) == 2, f"a request beyond the one successor appeared: {brief()}"
    live.log(f"{phase.dimension}: successor held sandbox-free; raising the limit")

    live.set_resources(phase, phase.high)
    started = wait_for(
        "the successor to start under the raised limit",
        10 * PROBE_SECONDS + 600,
        lambda: next(
            (r for r in rows() if r["id"] == successor["id"] and r.get("started_at")), None
        ),
        brief,
    )
    raised = (started.get("delivered_limits") or {}).get(phase.delivered_key)
    assert started.get("blocked_on") is None and raised == phase.high_bytes, (
        f"the successor started with {phase.delivered_key}={raised} and blocked_on "
        f"{started.get('blocked_on')!r}, expected {phase.high_bytes} and none: {_brief(started)}"
    )
    new_claim = started.get("runtime_claim_name")
    assert new_claim and new_claim != claim, (
        f"the successor names no fresh claim: {_brief(started)}"
    )
    new_pods_seen = wait_for(
        f"a pod of claim {new_claim}", 300, lambda: live.pods_for(str(new_claim)), brief
    )
    assert runner_limit(new_pods_seen[-1], phase.dimension) == phase.high_bytes, (
        f"the successor's pod enforces {runner_limit(new_pods_seen[-1], phase.dimension)}, "
        f"not the raised {phase.high_bytes}"
    )
    runner_digests = wait_for(
        "the successor runner's image id",
        300,
        lambda: ci.pod_digests(live.pods_for(str(new_claim)), "runner"),
        brief,
    )
    assert runner_digests <= live.digests["runner"], (
        f"the successor runner runs {sorted(runner_digests)}, not the candidate runner digests"
    )

    ended = wait_for(
        "the successor to end",
        1800,
        lambda: next(
            (r for r in rows() if r["id"] == successor["id"] and r["status"] in TERMINAL), None
        ),
        brief,
    )
    live.log(f"{phase.dimension}: successor ended {ended['status']} ({ended['terminal_cause']})")

    def no_further_attempt() -> None:
        current = rows()
        assert len(current) == 2, (
            f"exactly one fresh attempt was expected; the work item holds {len(current)} "
            f"requests: {[_brief(r) for r in current]}"
        )

    # A third request would be admitted within a probe period of the end.
    rrs.observe_for(SETTLE_SECONDS, no_further_attempt, interval=5)
    final = rows()
    assert len(final) == 2, (
        f"exactly one fresh attempt was expected; the work item holds {len(final)} requests: "
        f"{[_brief(r) for r in final]}"
    )
    assert ended.get("terminal_cause") != phase.cause, (
        f"the successor exhausted {phase.dimension} again under the raised limit: {_brief(ended)}"
    )


def test_resource_exhaustion_waits_for_a_larger_delivered_limit(
    live: Live, candidate_curie: Path
) -> None:
    assert os.environ["CURIE_BIN"] == str(candidate_curie)
    live.create_namespace()
    live.start_stub(SEED_FILES, SEED_ISSUE)
    live.install(live.values())
    live.port_forward()
    live.start_pod_watch()
    live.deploy_agents()
    for phase in PHASES:
        prove_phase(live, phase)
