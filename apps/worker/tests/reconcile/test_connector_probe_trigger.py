"""The reconcile-side capability probe trigger. @spec ACTION-EXECUTOR-13.

When the connector reconcile observes a hosted connector rolled out at a digest
with no capability row, a wrapper around the reconcile pass asks the probe route
of ACTION-EXECUTOR-1 for a probe, with exactly ``{agent_id, connector, digest}``.

The hook surface these tests pin (see
``.projects/plans/task-executor-probe.tests.md``):

* ``curie_worker.connector_probe.ProbeTrigger(requester=..., capabilities=...)``;
* ``ConnectorReconcileLoop(..., probe_trigger=<ProbeTrigger | None>)``, default
  None, which is today's loop exactly;
* a requester: ``await request(*, agent_id, connector, digest)``;
* capabilities: ``await exists(*, agent_id, connector, digest) -> bool``;
* ``curie_worker.connector_probe.HttpProbeRequester(api_base_url=, api_key=,
  worker_token=, client=)`` posting to ``/connector-capabilities/probes``;
* ``curie_worker.connector_probe.DbCapabilityRows`` (its ``exists`` is the
  capability read ``run.py`` composes).

The observation is the one the reconcile already makes: the agent's owned
objects as ``ConnectorClient.list_owned`` returns them. The reconcile's
decisions and writes are not the hook's to change, and a probe request that
fails is a fact about the probe, never about the pass.
"""

from __future__ import annotations

import copy
import json
import uuid
from typing import Any

import httpx
import pytest
from curie_worker.connector_agent import RenderedConnectors
from curie_worker.connector_loop import AgentTarget, ConnectorReconcileLoop, PassSummary
from plugin_format import connector_render
from plugin_format.connectors import validate_connectors

from .test_connector_agent import FakeClient, live_copy

pytestmark = pytest.mark.anyio

AGENT = "acme-bot"
RELEASE = "curie"
NAMESPACE = "curie"
DIGEST = "sha256:" + "ab" * 32
NEXT_DIGEST = "sha256:" + "ef" * 32
PROXY_DIGEST = "sha256:" + "cd" * 32
PINNED = f"registry.example/connectors/grafana@{DIGEST}"
TAGGED = "registry.example/connectors/grafana:1.4.2"
PUBLIC_KEY = "A6EHv/POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg="
PROBE_KEYS = {"agent_id", "connector", "digest"}


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


# -- the cluster, as the reconcile reads it -------------------------------------


def rendered(connector: str = "grafana", image: str = PINNED) -> list[dict[str, Any]]:
    """The real render of one hosted connector, so the Deployment is the
    product's own shape: ``server`` container, ``caller-proxy`` sidecar
    pinned to a DIFFERENT digest."""

    declared, errors = validate_connectors(
        {"connectors": {connector: {"image": image, "port": 8000}}}
    )
    assert errors == [], errors
    return connector_render.render(
        release=RELEASE,
        agent=AGENT,
        namespace=NAMESPACE,
        app_name="curie",
        connector=connector,
        spec=declared.connectors[connector],
        secret_name="curie-acme-bot-connector-secrets",
        proxy=connector_render.ConnectorProxy(
            image=f"registry.example/proxy@{PROXY_DIGEST}", public_keys=(PUBLIC_KEY,)
        ),
    )


def served(
    objects: list[dict[str, Any]],
    *,
    rolled_out: bool = True,
) -> list[dict[str, Any]]:
    """The objects as the cluster returns them: owned, hashed, and each
    Deployment with a generation and a status. ``rolled_out=False`` is a surge
    in flight: three of four rollout conditions hold, but an old pod still
    serves, so ``status.replicas`` is above spec (measurement M5)."""

    live = []
    for obj in objects:
        stored = live_copy(copy.deepcopy(obj), agent=AGENT)
        if stored["kind"] == "Deployment":
            wanted = stored["spec"].get("replicas", 1)
            stored["metadata"]["generation"] = 4
            stored["status"] = {
                "observedGeneration": 4,
                "replicas": wanted if rolled_out else wanted + 1,
                "updatedReplicas": wanted,
                "availableReplicas": wanted,
                "readyReplicas": wanted,
            }
        live.append(stored)
    return live


class Source:
    def __init__(self, manifests: list[dict[str, Any]]) -> None:
        self._manifests = manifests

    def rendered(self, *, agent_id: str, version_id: str) -> RenderedConnectors:
        return RenderedConnectors(manifests=copy.deepcopy(self._manifests))


class RecordingClient(FakeClient):
    """``FakeClient`` that also keeps every write byte for byte."""

    def __init__(self, live: list[dict[str, Any]]) -> None:
        super().__init__(live)
        self.writes: list[str] = []

    def apply(self, namespace: str, obj: dict[str, Any]) -> None:
        self.writes.append("apply " + json.dumps(obj, sort_keys=True, separators=(",", ":")))
        super().apply(namespace, obj)

    def delete(self, namespace: str, kind: str, name: str) -> None:
        self.writes.append(f"delete {kind}/{name}")
        super().delete(namespace, kind, name)


# -- the probe side -------------------------------------------------------------


class Requester:
    def __init__(self, fail: BaseException | None = None) -> None:
        self.sent: list[dict[str, Any]] = []
        self._fail = fail

    async def request(self, **body: Any) -> None:
        self.sent.append(dict(body))
        if self._fail is not None:
            raise self._fail


class Capabilities:
    """The capability rows, keyed exactly as the API keys them."""

    def __init__(self, rows: set[tuple[str, str, str]] | None = None) -> None:
        self.rows = rows if rows is not None else set()

    async def exists(self, *, agent_id: str, connector: str, digest: str) -> bool:
        return (agent_id, connector, digest) in self.rows


class RaisingCapabilities:
    async def exists(self, **_: Any) -> bool:
        raise RuntimeError("database unavailable")


def trigger(requester: Any, capabilities: Any) -> Any:
    from curie_worker.connector_probe import ProbeTrigger

    return ProbeTrigger(requester=requester, capabilities=capabilities)


class StubTargetsLoop(ConnectorReconcileLoop):
    """The real loop and the real ``_reconcile_one``; only the database query
    for targets is swapped out, as in ``test_connector_loop``."""

    def __init__(self, targets: list[AgentTarget], **kw: Any) -> None:
        super().__init__(engine=None, namespace=NAMESPACE, db_schema="curie", **kw)  # type: ignore[arg-type]
        self._targets = targets

    async def targets(self) -> list[AgentTarget]:  # type: ignore[override]
        return list(self._targets)


TARGET = AgentTarget(agent_id=uuid.uuid4(), agent_name=AGENT, version_id=uuid.uuid4())


def loop(
    live: list[dict[str, Any]],
    *,
    desired: list[dict[str, Any]] | None = None,
    requester: Any = None,
    capabilities: Any = None,
    hooked: bool = True,
) -> tuple[StubTargetsLoop, RecordingClient]:
    client = RecordingClient(live)
    kw: dict[str, Any] = {}
    if hooked:
        kw["probe_trigger"] = trigger(requester, capabilities or Capabilities())
    built = StubTargetsLoop(
        [TARGET],
        source=Source(rendered() if desired is None else desired),
        client=client,
        **kw,
    )
    return built, client


def expected(digest: str = DIGEST, connector: str = "grafana") -> dict[str, str]:
    return {"agent_id": str(TARGET.agent_id), "connector": connector, "digest": digest}


# --------------------------------------------------------------------------- #
# One request for a rolled-out digest with no capability row
# --------------------------------------------------------------------------- #
async def test_a_rolled_out_pinned_digest_without_a_row_is_probed_once() -> None:
    requester = Requester()
    built, _ = loop(served(rendered()), requester=requester)

    await built.one_pass()

    assert requester.sent == [expected()]


async def test_the_request_carries_exactly_the_three_keys() -> None:
    # ACTION-EXECUTOR-1: no tool, no arguments, no pass id, no version.
    requester = Requester()
    built, _ = loop(served(rendered()), requester=requester)

    await built.one_pass()

    assert len(requester.sent) == 1
    assert set(requester.sent[0]) == PROBE_KEYS
    assert all(isinstance(value, str) for value in requester.sent[0].values())


async def test_it_is_not_resent_on_later_passes_while_no_row_has_landed() -> None:
    # A probe takes longer than a pass. The API adopts a pending probe, but the
    # trigger must not lean on that: one request per digest, not one per minute.
    requester = Requester()
    built, _ = loop(served(rendered()), requester=requester)

    for _ in range(5):
        await built.one_pass()

    assert requester.sent == [expected()]


async def test_a_digest_that_already_has_a_row_is_not_probed() -> None:
    requester = Requester()
    rows = Capabilities({(str(TARGET.agent_id), "grafana", DIGEST)})
    built, _ = loop(served(rendered()), requester=requester, capabilities=rows)

    await built.one_pass()
    await built.one_pass()

    assert requester.sent == []


async def test_a_row_for_another_digest_does_not_count() -> None:
    # The row is keyed on the digest: the previous image's answer says nothing
    # about this one.
    requester = Requester()
    rows = Capabilities({(str(TARGET.agent_id), "grafana", NEXT_DIGEST)})
    built, _ = loop(served(rendered()), requester=requester, capabilities=rows)

    await built.one_pass()

    assert requester.sent == [expected()]


async def test_a_new_digest_rolled_out_later_is_probed_too() -> None:
    requester = Requester()
    client = RecordingClient(served(rendered()))
    built = StubTargetsLoop(
        [TARGET],
        source=Source(rendered()),
        client=client,
        probe_trigger=trigger(requester, Capabilities()),
    )
    await built.one_pass()

    next_image = f"registry.example/connectors/grafana@{NEXT_DIGEST}"
    client._live = served(rendered(image=next_image))
    built._source = Source(rendered(image=next_image))
    await built.one_pass()

    assert requester.sent == [expected(), expected(NEXT_DIGEST)]


async def test_a_failed_request_is_tried_again_on_the_next_pass() -> None:
    # A request that never reached the API recorded nothing; remembering it as
    # sent would leave the digest unprobed until the worker restarts.
    requester = Requester(fail=httpx.ConnectError("api unreachable"))
    built, _ = loop(served(rendered()), requester=requester)

    await built.one_pass()
    await built.one_pass()

    assert requester.sent == [expected(), expected()]


# --------------------------------------------------------------------------- #
# Never for what is not a rolled-out, pinned, hosted connector
# --------------------------------------------------------------------------- #
async def test_a_tag_referenced_image_is_never_probed() -> None:
    # What a tag names can move, so no tool list can be attributed to it.
    requester = Requester()
    objects = rendered(image=TAGGED)
    built, _ = loop(served(objects), desired=objects, requester=requester)

    await built.one_pass()

    assert requester.sent == []


async def test_a_rollout_in_progress_is_never_probed() -> None:
    requester = Requester()
    built, _ = loop(served(rendered(), rolled_out=False), requester=requester)

    await built.one_pass()

    assert requester.sent == []


async def test_a_deployment_not_yet_observed_by_its_controller_is_never_probed() -> None:
    requester = Requester()
    live = served(rendered())
    for obj in live:
        if obj["kind"] == "Deployment":
            obj["metadata"]["generation"] = 5  # spec changed, controller still at 4
    built, _ = loop(live, requester=requester)

    await built.one_pass()

    assert requester.sent == []


async def test_a_declared_connector_not_yet_live_is_never_probed() -> None:
    # The render pins a digest, but nothing serves it yet: the probe is about
    # what the cluster serves, never about what the bundle declares.
    requester = Requester()
    built, client = loop([], requester=requester)

    await built.one_pass()

    assert client.applied, "the reconcile should have applied the declared connector"
    assert requester.sent == []


async def test_a_remote_connector_is_never_probed() -> None:
    # A remote connector renders no object at all; there is nothing hosted.
    requester = Requester()
    built, _ = loop([], desired=[], requester=requester)

    await built.one_pass()

    assert requester.sent == []


async def test_a_name_outside_the_connector_grammar_is_never_probed() -> None:
    # Plugin MCP servers (``mcp__plugin_<...>``) are never hosted connectors;
    # their names carry an underscore the connector grammar refuses. An owned
    # Deployment naming one is not a probe target, whatever it serves.
    requester = Requester()
    live = served(rendered())
    for obj in live:
        if obj["kind"] != "Deployment":
            continue
        for container in obj["spec"]["template"]["spec"]["containers"]:
            for env in container.get("env") or []:
                if env.get("name") == "CURIE_CALLER_PROXY_CONNECTOR":
                    env["value"] = "plugin_grafana"
    built, _ = loop(live, desired=[], requester=requester)

    await built.one_pass()

    assert requester.sent == []


async def test_an_owned_deployment_without_a_server_container_is_never_probed() -> None:
    requester = Requester()
    live = served(rendered())
    for obj in live:
        if obj["kind"] == "Deployment":
            containers = obj["spec"]["template"]["spec"]["containers"]
            obj["spec"]["template"]["spec"]["containers"] = [
                c for c in containers if c.get("name") != "server"
            ]
    built, _ = loop(live, desired=[], requester=requester)

    await built.one_pass()

    assert requester.sent == []


async def test_each_connector_of_an_agent_is_probed_on_its_own() -> None:
    requester = Requester()
    loki_image = f"registry.example/connectors/loki@{NEXT_DIGEST}"
    objects = rendered() + rendered("loki", image=loki_image)
    built, _ = loop(served(objects), desired=objects, requester=requester)

    await built.one_pass()

    assert sorted(requester.sent, key=lambda b: b["connector"]) == [
        expected(DIGEST, "grafana"),
        expected(NEXT_DIGEST, "loki"),
    ]


# --------------------------------------------------------------------------- #
# The reconcile is unchanged
# --------------------------------------------------------------------------- #
def _scenario() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """A pass with real work: grafana served and probe-eligible, loki newly
    declared (applies), and a stale owned Service no longer declared (delete)."""

    loki = rendered("loki", image=f"registry.example/connectors/loki@{NEXT_DIGEST}")
    stale = {"apiVersion": "v1", "kind": "Service", "metadata": {"name": "stale-svc"}}
    live = served(rendered()) + served([stale])
    return live, rendered() + loki


def _summary(summary: PassSummary) -> tuple[int, ...]:
    return (
        summary.reconciled,
        summary.applied,
        summary.deleted,
        summary.skipped,
        summary.failed,
    )


async def _run(requester: Any, capabilities: Any, *, hooked: bool) -> tuple[Any, list[str]]:
    live, desired = _scenario()
    built, client = loop(
        live, desired=desired, requester=requester, capabilities=capabilities, hooked=hooked
    )
    first = await built.one_pass()
    second = await built.one_pass()
    return (_summary(first), _summary(second)), client.writes


@pytest.mark.parametrize(
    ("requester", "capabilities"),
    [
        pytest.param(Requester(), Capabilities(), id="probe sent"),
        pytest.param(
            Requester(fail=httpx.ConnectError("down")), Capabilities(), id="request fails"
        ),
        pytest.param(Requester(fail=RuntimeError("boom")), Capabilities(), id="request raises"),
        pytest.param(Requester(), RaisingCapabilities(), id="capability read fails"),
    ],
)
async def test_the_reconcile_decisions_and_writes_are_identical_with_the_hook(
    requester: Any, capabilities: Any
) -> None:
    baseline = await _run(None, None, hooked=False)
    hooked = await _run(requester, capabilities, hooked=True)

    assert baseline[1], "the scenario must make real writes, or identity proves nothing"
    assert hooked == baseline


async def test_the_hook_does_observe_the_scenario() -> None:
    # Guards the identity test above against a hook that is never consulted.
    requester = Requester()
    await _run(requester, Capabilities(), hooked=True)

    assert requester.sent == [expected()]


@pytest.mark.parametrize(
    "failure",
    [httpx.ConnectError("down"), httpx.ReadTimeout("slow"), RuntimeError("boom")],
    ids=["connect error", "timeout", "unexpected"],
)
async def test_a_failed_probe_request_never_fails_the_pass(failure: BaseException) -> None:
    requester = Requester(fail=failure)
    built, _ = loop(served(rendered()), requester=requester)

    summary = await built.one_pass()

    assert requester.sent == [expected()], "the request should have been attempted"
    assert summary.failed == 0


async def test_an_http_error_status_from_the_probe_route_never_fails_the_pass() -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"detail": "unavailable"})

    from curie_worker.connector_probe import HttpProbeRequester

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as client:
        requester = HttpProbeRequester(
            api_base_url="http://api:8000",
            api_key="k",
            worker_token="t",
            client=client,
        )
        built, _ = loop(served(rendered()), requester=requester)
        summary = await built.one_pass()

    assert summary.failed == 0


async def test_a_failed_capability_read_never_fails_the_pass_and_sends_nothing() -> None:
    # Unknown is not "absent": the request waits for a pass that can read.
    requester = Requester()
    built, _ = loop(served(rendered()), requester=requester, capabilities=RaisingCapabilities())

    summary = await built.one_pass()

    assert summary.failed == 0
    assert requester.sent == []


async def test_a_failing_agent_reconcile_does_not_stop_probes_for_the_others() -> None:
    other = AgentTarget(agent_id=uuid.uuid4(), agent_name="other-bot", version_id=uuid.uuid4())

    class OneBadSource(Source):
        def rendered(self, *, agent_id: str, version_id: str) -> RenderedConnectors:
            if agent_id == str(other.agent_id):
                raise RuntimeError("render failed")
            return super().rendered(agent_id=agent_id, version_id=version_id)

    requester = Requester()
    built = StubTargetsLoop(
        [other, TARGET],
        source=OneBadSource(rendered()),
        client=RecordingClient(served(rendered())),
        probe_trigger=trigger(requester, Capabilities()),
    )

    summary = await built.one_pass()

    assert summary.failed == 1
    assert requester.sent == [expected()]


# --------------------------------------------------------------------------- #
# The wire: exactly three keys, to the probe route, under the worker token
# --------------------------------------------------------------------------- #
async def test_the_http_requester_posts_exactly_the_three_keys_under_the_worker_token() -> None:
    from curie_worker.connector_probe import HttpProbeRequester

    seen: list[httpx.Request] = []

    def answer(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(201, json={"execution_id": str(uuid.uuid4()), "state": "requested"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(answer)) as client:
        requester = HttpProbeRequester(
            api_base_url="http://api:8000/",
            api_key="k",
            worker_token="worker-token",
            client=client,
        )
        await requester.request(
            agent_id=str(TARGET.agent_id), connector="grafana", digest=DIGEST
        )

    assert len(seen) == 1
    request = seen[0]
    assert request.method == "POST"
    assert request.url == httpx.URL("http://api:8000/connector-capabilities/probes")
    assert request.headers["X-Curie-Worker-Token"] == "worker-token"
    assert json.loads(request.content) == expected()
    assert request.url.query == b""


async def test_the_http_requester_treats_an_adopted_probe_as_success() -> None:
    # 200 is the API adopting a pending or confirmed probe for the same triple.
    from curie_worker.connector_probe import HttpProbeRequester

    def adopted(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"execution_id": str(uuid.uuid4()), "state": "claimed"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(adopted)) as client:
        requester = HttpProbeRequester(
            api_base_url="http://api:8000", api_key="k", worker_token="t", client=client
        )
        await requester.request(agent_id=str(TARGET.agent_id), connector="grafana", digest=DIGEST)


# --------------------------------------------------------------------------- #
# Composition: off unless the executor is on and the worker token is present
# --------------------------------------------------------------------------- #
def _compose(monkeypatch: pytest.MonkeyPatch, **config: Any) -> tuple[Any, list[httpx.Request]]:
    from curie_worker import connector_k8s
    from curie_worker.config import WorkerConfig
    from curie_worker.connector_loop import HttpManifestSource
    from curie_worker.run import _build_connector_loop

    live = served(rendered())
    monkeypatch.setattr(connector_k8s, "KubernetesConnectorClient", lambda **_: FakeClient(live))

    async def targets(self: Any) -> list[AgentTarget]:
        return [TARGET]

    monkeypatch.setattr(ConnectorReconcileLoop, "targets", targets)
    monkeypatch.setattr(
        HttpManifestSource,
        "rendered",
        lambda self, *, agent_id, version_id: RenderedConnectors(manifests=rendered()),
    )

    sent: list[httpx.Request] = []

    async def async_send(self: Any, request: httpx.Request, **_: Any) -> httpx.Response:
        sent.append(request)
        return httpx.Response(201, json={"execution_id": str(uuid.uuid4()), "state": "requested"})

    def sync_send(self: Any, request: httpx.Request, **_: Any) -> httpx.Response:
        sent.append(request)
        return httpx.Response(201, json={})

    monkeypatch.setattr(httpx.AsyncClient, "send", async_send)
    monkeypatch.setattr(httpx.Client, "send", sync_send)

    worker = WorkerConfig(
        connector_reconcile_enabled=True,
        connector_namespace=NAMESPACE,
        connector_release=RELEASE,
        connector_app_name="curie",
        **config,
    )
    return _build_connector_loop(worker, engine=None), sent  # type: ignore[arg-type]


def _probes(sent: list[httpx.Request]) -> list[httpx.Request]:
    return [r for r in sent if r.url.path.endswith("/connector-capabilities/probes")]


@pytest.mark.parametrize(
    "config",
    [
        pytest.param({"action_executor_enabled": False}, id="executor off"),
        pytest.param(
            {"action_executor_enabled": True, "internal_worker_token": ""},
            id="executor on without a worker token",
        ),
    ],
)
async def test_no_probe_is_requested_unless_the_executor_can_run_it(
    monkeypatch: pytest.MonkeyPatch, config: dict[str, Any]
) -> None:
    built, sent = _compose(monkeypatch, **config)
    assert built is not None

    await built.one_pass()
    await built.one_pass()

    assert _probes(sent) == []


async def test_the_worker_composes_the_trigger_when_the_executor_is_on(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from curie_worker import connector_probe

    async def no_row(self: Any, **_: Any) -> bool:
        return False

    monkeypatch.setattr(connector_probe.DbCapabilityRows, "exists", no_row)
    built, sent = _compose(monkeypatch, action_executor_enabled=True)
    assert built is not None

    await built.one_pass()
    await built.one_pass()

    probes = _probes(sent)
    assert len(probes) == 1
    assert json.loads(probes[0].content) == expected()
    assert probes[0].headers["X-Curie-Worker-Token"] == "curie-dev-worker-token"


# --------------------------------------------------------------------------- #
# Review round 1: the resend window, the memo bound, and what is excluded
# --------------------------------------------------------------------------- #
WINDOW = 3600.0


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def timed_trigger(
    requester: Any, capabilities: Any, clock: Clock, **kw: Any
) -> Any:
    from curie_worker.connector_probe import ProbeTrigger

    return ProbeTrigger(
        requester=requester,
        capabilities=capabilities,
        resend_after_seconds=WINDOW,
        clock=clock,
        **kw,
    )


def live_deployment(
    connector: str = "grafana", digest: str = DIGEST, name: str | None = None
) -> dict[str, Any]:
    image = f"registry.example/connectors/{connector}@{digest}"
    found = [o for o in served(rendered(connector, image=image)) if o["kind"] == "Deployment"]
    deployment = found[0]
    if name is not None:
        deployment["metadata"]["name"] = name
    return deployment


async def _offer(trigger_: Any, *deployments: dict[str, Any]) -> None:
    await trigger_.after_reconcile(
        agent_id=str(TARGET.agent_id), agent_name=AGENT, observed=list(deployments)
    )


async def test_no_resend_before_the_window() -> None:
    requester, clock = Requester(), Clock()
    probe = timed_trigger(requester, Capabilities(), clock)

    await _offer(probe, live_deployment())
    clock.now += WINDOW - 1
    await _offer(probe, live_deployment())

    assert requester.sent == [expected()]


async def test_resent_after_the_window_while_no_row_has_landed() -> None:
    # A probe that ended refused or failed records no row; the API starts a
    # new attempt when asked again.
    requester, clock = Requester(), Clock()
    probe = timed_trigger(requester, Capabilities(), clock)

    await _offer(probe, live_deployment())
    clock.now += WINDOW
    await _offer(probe, live_deployment())

    assert requester.sent == [expected(), expected()]


async def test_a_landed_row_stops_the_resend_and_clears_the_entry() -> None:
    requester, clock = Requester(), Clock()
    rows = Capabilities()
    probe = timed_trigger(requester, rows, clock)

    await _offer(probe, live_deployment())
    rows.rows.add((str(TARGET.agent_id), "grafana", DIGEST))
    clock.now += WINDOW
    await _offer(probe, live_deployment())
    assert requester.sent == [expected()], "a row landed, so nothing is due"

    # Cleared, not merely skipped: were the row to vanish, the triple is due at
    # once rather than after another window counted from the first send.
    rows.rows.clear()
    clock.now += 1
    await _offer(probe, live_deployment())
    assert requester.sent == [expected(), expected()]


async def test_the_oldest_remembered_request_is_forgotten_first() -> None:
    capacity = 4096
    requester, clock = Requester(), Clock()
    probe = timed_trigger(requester, Capabilities(), clock)
    template = live_deployment()

    def at(index: int) -> dict[str, Any]:
        deployment = copy.deepcopy(template)
        digest = "sha256:" + f"{index:064x}"
        for container in deployment["spec"]["template"]["spec"]["containers"]:
            if container["name"] == "server":
                container["image"] = f"registry.example/connectors/grafana@{digest}"
        return deployment

    for index in range(capacity + 1):
        clock.now += 0.001
        await _offer(probe, at(index))
    assert len(requester.sent) == capacity + 1
    requester.sent.clear()

    clock.now += 1  # still well inside the window
    # Remembered ones first: resending the oldest re-remembers it and evicts
    # the next oldest, which would blur what is being checked.
    await _offer(probe, at(1))
    await _offer(probe, at(capacity))
    await _offer(probe, at(0))

    assert [body["digest"] for body in requester.sent] == ["sha256:" + f"{0:064x}"], (
        "only the oldest triple should have been forgotten"
    )


async def test_a_deployment_the_same_pass_deletes_is_not_probed() -> None:
    # The render no longer declares grafana, so the pass deletes its
    # Deployment: probing it would ask about an image that is going away.
    requester = Requester()
    live = served(rendered())
    built, client = loop(live, desired=[], requester=requester)

    await built.one_pass()

    assert ("Deployment", "curie-acme-bot-mcp-grafana") in client.deleted
    assert requester.sent == []


async def test_a_deployment_not_at_its_rendered_name_is_not_probed() -> None:
    requester, clock = Requester(), Clock()
    probe = timed_trigger(requester, Capabilities(), clock, release=RELEASE)

    await _offer(probe, live_deployment(name="curie-other-bot-mcp-grafana"))
    assert requester.sent == [], "the proxy env alone must not name a connector"

    await _offer(probe, live_deployment())
    assert requester.sent == [expected()], "the rendered name is probed"


async def test_a_bad_deployment_name_does_not_skip_the_agents_other_probes() -> None:
    # A connector forging a second ``-mcp-`` makes ``object_name`` raise. That
    # Deployment is not a probe target, and the agent's other connectors in
    # the same pass still are.
    requester, clock = Requester(), Clock()
    probe = timed_trigger(requester, Capabilities(), clock, release=RELEASE)
    forged = live_deployment(name="curie-acme-bot-mcp-x-mcp-y")
    for container in forged["spec"]["template"]["spec"]["containers"]:
        for env in container.get("env") or []:
            if env.get("name") == "CURIE_CALLER_PROXY_CONNECTOR":
                env["value"] = "x-mcp-y"

    try:
        await _offer(probe, forged, live_deployment())
    except Exception:  # noqa: BLE001 -- the loop would contain it; the probe is still lost
        pass

    assert requester.sent == [expected()]
