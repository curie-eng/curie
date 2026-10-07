"""Attributing a connector digest without the kernel. @spec ACTION-EXECUTOR-12.

The kernel's record call passes no deployment, so the worker composes a
recorder wrapper around ``ActionClient`` that implements the same
``ActionRecorder`` protocol. On the opening frame and again on the closing frame
it reads the target connector's Deployment by name, and it records
``connector`` and ``connector_digest`` only when both reads show the same
generation, a completed rollout and an image pinned by ``@sha256:``.

These tests sit at the two boundaries the wrapper touches and fake nothing
between them: the Kubernetes apps API (a fake ``AppsV1Api`` whose only granted
verb is a single-object ``read_namespaced_deployment``) and the platform API
(a real ``ActionClient`` over an ``httpx.MockTransport``), so what is pinned is
what reaches the ledger, not what some intermediate object was handed.
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable, Iterator
from typing import Any

import httpx
import pytest
from aci_protocol import SideEffectFlag
from curie_worker.action_digest import READ_TIMEOUT_SECONDS, DigestAttributingRecorder
from curie_worker.actions import ActionBackendError, ActionClient, RecordedAction
from kubernetes import client as k8s_client
from kubernetes.client.exceptions import ApiException

pytestmark = pytest.mark.anyio

NAMESPACE = "curie"
AGENT_ID = "11111111-1111-4111-8111-111111111111"
CONNECTOR = "grafana"
TOOL = f"mcp__{CONNECTOR}__scale_deployment"
DEPLOYMENT = f"rel-ops-agent-mcp-{CONNECTOR}"
DIGEST = "sha256:" + "ab" * 32
PROXY_DIGEST = "sha256:" + "cd" * 32
PINNED = f"registry.example/connectors/grafana@{DIGEST}"
TAGGED = "registry.example/connectors/grafana:1.4.2"

# The wrapper may add at most this much per read on top of the bound. Generous
# enough for a loaded CI box, small enough that "waited for the slow read" fails.
SLACK_SECONDS = 0.75
# How long a hanging fake blocks before giving up on its own; far past the bound.
HANG_SECONDS = 8.0


# -- the Kubernetes boundary --------------------------------------------------


def deployment(
    *,
    generation: int = 3,
    observed: int | None = 3,
    spec_replicas: int = 2,
    updated: int | None = 2,
    available: int | None = 2,
    replicas: int | None = 2,
    image: str = PINNED,
    status: bool = True,
) -> dict[str, Any]:
    """A connector Deployment as the API server returns it (camelCase wire form).

    The caller proxy sidecar is pinned to a DIFFERENT digest, so a wrapper that
    reads the wrong container records the wrong image and the happy path fails.
    """

    body: dict[str, Any] = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": DEPLOYMENT, "namespace": NAMESPACE, "generation": generation},
        "spec": {
            "replicas": spec_replicas,
            "selector": {"matchLabels": {"app.kubernetes.io/name": DEPLOYMENT}},
            "template": {
                "metadata": {"labels": {"app.kubernetes.io/name": DEPLOYMENT}},
                "spec": {
                    "containers": [
                        {"name": "caller-proxy", "image": f"registry.example/proxy@{PROXY_DIGEST}"},
                        {"name": "server", "image": image},
                    ]
                },
            },
        },
    }
    if status:
        fields = {
            "observedGeneration": observed,
            "replicas": replicas,
            "updatedReplicas": updated,
            "availableReplicas": available,
            "readyReplicas": available,
        }
        body["status"] = {k: v for k, v in fields.items() if v is not None}
    return body


class _Raw:
    """What ``_preload_content=False`` hands back: the undecoded body."""

    def __init__(self, body: dict[str, Any]) -> None:
        self.data = json.dumps(body).encode()
        self.status = 200

    def getheaders(self) -> dict[str, str]:
        return {}


class Hang:
    """A read the API server never answers within the bound."""


Script = dict[str, Any] | BaseException | type[Hang]


class FakeAppsV1Api:
    """The apps API, with the single-object get as the only verb that answers.

    Each ``read_namespaced_deployment`` consumes the next scripted outcome: a
    Deployment body, an exception to raise, or ``Hang``. Any other attribute --
    ``list_namespaced_deployment`` above all -- is recorded as a violation and
    raises; the violation list is asserted on rather than the raise, because the
    wrapper is required to swallow read failures.
    """

    def __init__(self, *outcomes: Script) -> None:
        self._outcomes = list(outcomes)
        self.reads: list[dict[str, Any]] = []
        self.violations: list[str] = []
        self.release = threading.Event()

    def read_namespaced_deployment(self, name: str, namespace: str, **kwargs: Any) -> Any:
        self.reads.append({"name": name, "namespace": namespace, "kwargs": kwargs})
        outcome: Script = self._outcomes.pop(0) if self._outcomes else deployment()
        if outcome is Hang:
            self.release.wait(HANG_SECONDS)
            outcome = deployment()
        if isinstance(outcome, BaseException):
            raise outcome
        assert isinstance(outcome, dict)
        raw = _Raw(outcome)
        if kwargs.get("_preload_content") is False:
            return raw
        # Typed path: the same body through the client's own deserializer.
        return k8s_client.ApiClient().deserialize(raw, "V1Deployment")

    def __getattr__(self, attr: str) -> Any:
        self.violations.append(attr)
        raise AssertionError(f"the digest read may only get one Deployment by name, not {attr}")


# -- the ledger boundary ------------------------------------------------------


class Ledger:
    """The platform API's /actions endpoint, as the worker reaches it."""

    def __init__(self, *, fail_complete: bool = False) -> None:
        self.posts: list[dict[str, Any]] = []
        self._fail_complete = fail_complete

    def __call__(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        self.posts.append({"path": request.url.path, "body": body})
        if request.url.path.endswith("/complete"):
            if self._fail_complete:
                return httpx.Response(503, text="ledger down")
            return httpx.Response(200, json={"id": "a1", "undoable": False, **(body or {})})
        return httpx.Response(201, json={"id": "a1", "status": "pending"})

    def completion(self) -> dict[str, Any]:
        bodies = [p["body"] for p in self.posts if p["path"].endswith("/complete")]
        assert len(bodies) == 1, self.posts
        body: dict[str, Any] = bodies[0]
        return body


Resolver = Callable[[str | None, str], Any]


def _resolver(name: str | None = DEPLOYMENT, *, raises: bool = False) -> tuple[Resolver, list[Any]]:
    seen: list[Any] = []

    async def resolve(agent_id: str | None, connector: str) -> str | None:
        seen.append((agent_id, connector))
        if raises:
            raise RuntimeError("agent lookup failed")
        return name

    return resolve, seen


@pytest.fixture
def released() -> Iterator[list[FakeAppsV1Api]]:
    """Unblock every hanging fake at teardown so no thread outlives the test."""

    fakes: list[FakeAppsV1Api] = []
    yield fakes
    for fake in fakes:
        fake.release.set()


def _frames(tool: str = TOOL) -> tuple[SideEffectFlag, SideEffectFlag]:
    opening = SideEffectFlag(tool=tool, call_id="toolu_01", arguments={"replicas": 3})
    closing = SideEffectFlag(
        tool=tool,
        call_id="toolu_01",
        arguments={"replicas": 3},
        result={"replicas": 3},
        failed=False,
    )
    return opening, closing


async def _round_trip(
    apps: Any,
    *,
    tool: str = TOOL,
    resolver: Resolver | None = None,
    ledger: Ledger | None = None,
) -> tuple[dict[str, Any], Ledger, list[float]]:
    """Drive one call through the wrapper exactly as the kernel does.

    Returns the completion body the ledger received, the ledger, and the wall
    time of ``record`` and ``complete`` separately.
    """

    ledger = ledger or Ledger()
    resolve = resolver or _resolver()[0]
    opening, closing = _frames(tool)
    async with httpx.AsyncClient(transport=httpx.MockTransport(ledger)) as http:
        recorder = DigestAttributingRecorder(
            ActionClient(api_base_url="http://api", api_key="k", client=http),
            deployments=apps,
            namespace=NAMESPACE,
            deployment_name=resolve,
        )
        started = time.monotonic()
        recorded = await recorder.record(
            opening, event_id="event-1", conversation_id="C1", agent_id=AGENT_ID
        )
        opened = time.monotonic()
        assert isinstance(recorded, RecordedAction)
        assert recorded.id == "a1"
        if isinstance(apps, FakeAppsV1Api):
            # The opening read is taken on the opening frame, not deferred.
            assert len(apps.reads) <= 1
        row = await recorder.complete(recorded.id, closing)
        closed = time.monotonic()
    assert isinstance(row, dict) and row["id"] == "a1"
    return ledger.completion(), ledger, [opened - started, closed - opened]


def _attributed(body: dict[str, Any]) -> tuple[Any, Any]:
    return body.get("connector"), body.get("connector_digest")


NULL = (None, None)


# -- the positive case --------------------------------------------------------


async def test_a_call_inside_a_completed_rollout_records_the_server_digest() -> None:
    apps = FakeAppsV1Api(deployment(), deployment())
    resolve, asked = _resolver()

    body, ledger, _ = await _round_trip(apps, resolver=resolve)

    assert _attributed(body) == (CONNECTOR, DIGEST)
    # Exactly two reads, one per frame, each a get of THIS connector by name.
    assert [(r["name"], r["namespace"]) for r in apps.reads] == [(DEPLOYMENT, NAMESPACE)] * 2
    assert apps.violations == []
    # The name is resolved for the agent and the connector the tool names.
    assert asked and all(a == (AGENT_ID, CONNECTOR) for a in asked)
    # The rest of the closing frame still reaches the ledger untouched.
    assert body["result"] == {"replicas": 3}
    assert body["failed"] is False
    assert ledger.posts[0]["path"] == "/actions"


async def test_the_opening_read_happens_on_the_opening_frame() -> None:
    """Two reads bracket the call; one taken late at completion is not a bracket."""

    apps = FakeAppsV1Api(deployment(), deployment())
    opening, closing = _frames()
    async with httpx.AsyncClient(transport=httpx.MockTransport(Ledger())) as http:
        recorder = DigestAttributingRecorder(
            ActionClient(api_base_url="http://api", api_key="k", client=http),
            deployments=apps,
            namespace=NAMESPACE,
            deployment_name=_resolver()[0],
        )
        recorded = await recorder.record(
            opening, event_id="event-1", conversation_id="C1", agent_id=AGENT_ID
        )
        assert len(apps.reads) == 1
        await recorder.complete(recorded.id, closing)
    assert len(apps.reads) == 2


async def test_an_image_with_tag_and_digest_is_pinned_by_its_digest() -> None:
    image = f"registry.example/connectors/grafana:1.4.2@{DIGEST}"
    apps = FakeAppsV1Api(deployment(image=image), deployment(image=image))

    body, _, _ = await _round_trip(apps)

    assert _attributed(body) == (CONNECTOR, DIGEST)


async def test_a_read_well_inside_the_bound_still_records() -> None:
    """The bound is two seconds, not 'whatever is fast'."""

    class SlowButInBound(FakeAppsV1Api):
        def read_namespaced_deployment(self, name: str, namespace: str, **kwargs: Any) -> Any:
            time.sleep(0.5)
            return super().read_namespaced_deployment(name, namespace, **kwargs)

    apps = SlowButInBound(deployment(), deployment())

    body, _, _ = await _round_trip(apps)

    assert _attributed(body) == (CONNECTOR, DIGEST)


# -- a straddled or incomplete rollout ----------------------------------------


async def test_a_call_that_straddles_a_generation_records_null() -> None:
    apps = FakeAppsV1Api(
        deployment(generation=3, observed=3),
        deployment(generation=4, observed=4),
    )

    body, _, _ = await _round_trip(apps)

    assert _attributed(body) == NULL


INCOMPLETE = {
    # The controller has not yet seen the newest spec.
    "observedGeneration behind generation": {"generation": 4, "observed": 3},
    "updatedReplicas short of spec": {"updated": 1},
    "availableReplicas short of spec": {"available": 1},
    # Surge: the first three hold while an old pod still serves (measurement M5).
    "status.replicas above spec during surge": {"replicas": 3},
    "status.replicas below spec": {"replicas": 1},
    "no status at all": {"status": False},
    "observedGeneration missing": {"observed": None},
    "updatedReplicas missing": {"updated": None},
    "availableReplicas missing": {"available": None},
    "status.replicas missing": {"replicas": None},
}


@pytest.mark.parametrize("which", ["opening", "closing"])
@pytest.mark.parametrize("override", list(INCOMPLETE.values()), ids=list(INCOMPLETE))
async def test_each_rollout_condition_failing_alone_records_null(
    override: dict[str, Any], which: str
) -> None:
    # The same generation on both reads, so only the named condition differs
    # from the positive case.
    generation = override.get("generation", 3)
    good = deployment(generation=generation, observed=generation)
    bad = deployment(**override)
    apps = FakeAppsV1Api(*((bad, good) if which == "opening" else (good, bad)))

    body, _, _ = await _round_trip(apps)

    assert _attributed(body) == NULL


async def test_a_surge_that_settles_by_the_closing_read_still_records_null() -> None:
    """Both reads must show a completed rollout, not just the last one."""

    apps = FakeAppsV1Api(deployment(replicas=3), deployment())

    body, _, _ = await _round_trip(apps)

    assert _attributed(body) == NULL


# -- images and connectors that carry no attributable digest ------------------


@pytest.mark.parametrize(
    "image",
    [
        TAGGED,
        "registry.example/connectors/grafana",
        "registry.example/connectors/grafana:latest",
    ],
    ids=["tag", "bare", "latest"],
)
async def test_an_image_not_pinned_by_sha256_records_null(image: str) -> None:
    apps = FakeAppsV1Api(deployment(image=image), deployment(image=image))

    body, _, _ = await _round_trip(apps)

    assert _attributed(body) == NULL


@pytest.mark.parametrize(
    "tool",
    ["mcp__plugin_ops_grafana__scale_deployment", "Bash", "mcp__grafana", ""],
    ids=["plugin-mcp-server", "builtin", "no-upstream-tool", "empty"],
)
async def test_a_tool_that_is_not_a_hosted_connector_records_null_without_reading(
    tool: str,
) -> None:
    apps = FakeAppsV1Api()

    body, _, _ = await _round_trip(apps, tool=tool)

    assert _attributed(body) == NULL
    assert apps.reads == []
    assert apps.violations == []


async def test_the_local_tier_without_a_cluster_client_records_null() -> None:
    body, ledger, _ = await _round_trip(None)

    assert _attributed(body) == NULL
    # The call is still recorded and completed in full.
    assert [p["path"] for p in ledger.posts] == ["/actions", "/actions/a1/complete"]


async def test_an_unresolvable_deployment_name_records_null_without_reading() -> None:
    apps = FakeAppsV1Api()
    resolve, _ = _resolver(None)

    body, _, _ = await _round_trip(apps, resolver=resolve)

    assert _attributed(body) == NULL
    assert apps.reads == []


async def test_a_resolver_that_raises_records_null_and_does_not_fail_the_call() -> None:
    apps = FakeAppsV1Api()
    resolve, _ = _resolver(raises=True)

    body, _, _ = await _round_trip(apps, resolver=resolve)

    assert _attributed(body) == NULL


# -- failed reads never fail the record or the turn ----------------------------


FAILURES: dict[str, BaseException] = {
    "403 forbidden": ApiException(status=403, reason="Forbidden"),
    "404 not found": ApiException(status=404, reason="Not Found"),
    "500 server error": ApiException(status=500, reason="Internal Server Error"),
    "transport error": ConnectionError("connection reset"),
    "anything else": RuntimeError("unexpected"),
    "malformed body": ValueError("not json"),
}


@pytest.mark.parametrize("which", ["opening", "closing", "both"])
@pytest.mark.parametrize("error", list(FAILURES.values()), ids=list(FAILURES))
async def test_a_failed_read_records_null_and_never_raises(
    error: BaseException, which: str
) -> None:
    good = deployment()
    outcomes: dict[str, tuple[Script, Script]] = {
        "opening": (error, good),
        "closing": (good, error),
        "both": (error, error),
    }
    apps = FakeAppsV1Api(*outcomes[which])

    body, ledger, _ = await _round_trip(apps)

    assert _attributed(body) == NULL
    assert [p["path"] for p in ledger.posts] == ["/actions", "/actions/a1/complete"]


async def test_a_deployment_with_no_server_container_records_null() -> None:
    body_without_server = deployment()
    body_without_server["spec"]["template"]["spec"]["containers"] = [
        {"name": "caller-proxy", "image": f"registry.example/proxy@{PROXY_DIGEST}"}
    ]
    apps = FakeAppsV1Api(body_without_server, body_without_server)

    body, _, _ = await _round_trip(apps)

    assert _attributed(body) == NULL


async def test_completing_an_action_this_wrapper_never_opened_records_null() -> None:
    """No opening read means no bracket; a lone closing read proves nothing."""

    apps = FakeAppsV1Api(deployment(), deployment())
    _, closing = _frames()
    ledger = Ledger()
    async with httpx.AsyncClient(transport=httpx.MockTransport(ledger)) as http:
        recorder = DigestAttributingRecorder(
            ActionClient(api_base_url="http://api", api_key="k", client=http),
            deployments=apps,
            namespace=NAMESPACE,
            deployment_name=_resolver()[0],
        )
        await recorder.complete("a1", closing)

    assert _attributed(ledger.completion()) == NULL
    assert len(apps.reads) <= 1


async def test_a_ledger_failure_still_propagates() -> None:
    """Swallowing digest failures is not swallowing the ledger.

    The kernel's ``_record_action`` is deliberately not best effort: losing the
    account of what changed fails the turn. The wrapper must not turn that into
    a success.
    """

    apps = FakeAppsV1Api(deployment(), deployment())
    with pytest.raises(ActionBackendError):
        await _round_trip(apps, ledger=Ledger(fail_complete=True))


# -- the time bound -----------------------------------------------------------


def test_the_read_bound_is_two_seconds() -> None:
    assert READ_TIMEOUT_SECONDS == 2.0


@pytest.mark.parametrize("which", ["opening", "closing"])
async def test_a_read_past_the_bound_records_null_within_the_bound(
    which: str, released: list[FakeAppsV1Api]
) -> None:
    outcomes: tuple[Script, Script] = (
        (Hang, deployment()) if which == "opening" else (deployment(), Hang)
    )
    apps = FakeAppsV1Api(*outcomes)
    released.append(apps)

    body, _, (record_seconds, complete_seconds) = await _round_trip(apps)

    assert _attributed(body) == NULL
    slow = record_seconds if which == "opening" else complete_seconds
    assert slow <= READ_TIMEOUT_SECONDS + SLACK_SECONDS
    assert record_seconds + complete_seconds <= READ_TIMEOUT_SECONDS + 2 * SLACK_SECONDS


async def test_hung_reads_keep_the_whole_action_within_four_seconds(
    released: list[FakeAppsV1Api],
) -> None:
    """Whatever reads a hung API server causes, the action adds at most two bounds.

    (Whether a hung opening read is followed by a closing read is the wrapper's
    choice; the closing-read hang is pinned on its own above.)
    """

    apps = FakeAppsV1Api(Hang, Hang)
    released.append(apps)

    started = time.monotonic()
    body, ledger, (record_seconds, complete_seconds) = await _round_trip(apps)
    total = time.monotonic() - started

    assert _attributed(body) == NULL
    assert record_seconds <= READ_TIMEOUT_SECONDS + SLACK_SECONDS
    assert complete_seconds <= READ_TIMEOUT_SECONDS + SLACK_SECONDS
    assert total <= 2 * READ_TIMEOUT_SECONDS + 2 * SLACK_SECONDS
    # The turn's record and completion both still landed.
    assert [p["path"] for p in ledger.posts] == ["/actions", "/actions/a1/complete"]


# -- a timed-out read does not keep its thread past the bound -------------------
#
# ``asyncio.wait_for`` frees the turn at the bound, but the sync client keeps
# running in its thread. The kubernetes client turns a float ``_request_timeout``
# into a per-attempt urllib3 timeout and, by default, retries a read timeout, so
# one stalled read would hold a shared executor thread for several bounds. The
# client the worker builds must make one attempt only.


class _SilentApiServer:
    """A TCP listener that accepts connections and never answers; counts attempts."""

    def __init__(self) -> None:
        import socket

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(16)
        self._sock.settimeout(0.2)
        self.port = self._sock.getsockname()[1]
        self.connections = 0
        self._held: list[Any] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except OSError:
                continue
            self.connections += 1
            self._held.append(conn)

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=2)
        for conn in self._held:
            conn.close()
        self._sock.close()


@pytest.fixture
def silent_api_server() -> Iterator[_SilentApiServer]:
    server = _SilentApiServer()
    yield server
    server.close()


@pytest.fixture
def local_cluster_config(
    monkeypatch: pytest.MonkeyPatch, silent_api_server: _SilentApiServer
) -> None:
    """Point the worker's cluster config at the silent server; never at a kubeconfig."""

    from kubernetes import config as k8s_config

    monkeypatch.setattr(k8s_client.Configuration, "_default", None)

    def in_cluster() -> None:
        configuration = k8s_client.Configuration()
        configuration.host = f"http://127.0.0.1:{silent_api_server.port}"
        k8s_client.Configuration.set_default(configuration)

    def no_kubeconfig(*_a: Any, **_k: Any) -> None:
        raise AssertionError("the test must never load a real kubeconfig")

    monkeypatch.setattr(k8s_config, "load_incluster_config", in_cluster)
    monkeypatch.setattr(k8s_config, "load_kube_config", no_kubeconfig)


@pytest.mark.usefixtures("local_cluster_config")
async def test_a_timed_out_read_makes_one_attempt_and_releases_its_thread(
    silent_api_server: _SilentApiServer,
) -> None:
    import asyncio

    from curie_worker.connector_k8s import connector_deployments_api

    api = connector_deployments_api()

    body, _, (record_seconds, _) = await _round_trip(api)
    assert _attributed(body) == NULL
    assert record_seconds <= READ_TIMEOUT_SECONDS + SLACK_SECONDS

    # Long enough for a retried attempt (one more bound) to have connected.
    await asyncio.sleep(READ_TIMEOUT_SECONDS + 1.5)
    assert silent_api_server.connections == 1


# -- composition: the wrapper exists only where the Role grants the get ---------


class _GateConfig:
    def __init__(self, *, reconcile: bool, executor: bool) -> None:
        self.connector_reconcile_enabled = reconcile
        self.action_executor_enabled = executor
        self.connector_namespace = NAMESPACE
        self.connector_release = "rel"
        self.db_schema = "curie"
        self.internal_worker_token = "wt"


@pytest.mark.parametrize(
    ("reconcile", "executor"),
    [(False, False), (True, False), (False, True)],
    ids=["neither", "reconciler_only", "executor_only"],
)
def test_without_both_gates_the_ledger_client_is_not_wrapped(
    monkeypatch: pytest.MonkeyPatch, reconcile: bool, executor: bool
) -> None:
    from curie_worker import connector_k8s, run

    built: list[str] = []
    monkeypatch.setattr(
        connector_k8s, "connector_deployments_api", lambda **_k: built.append("apps")
    )
    inner = ActionClient(api_base_url="http://api", api_key="k", client=httpx.AsyncClient())

    composed = run._build_action_recorder(
        _GateConfig(reconcile=reconcile, executor=executor),  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
        inner,
    )

    assert composed is inner
    # No cluster client is even built: the Role has no get to use.
    assert built == []


async def test_with_both_gates_the_composed_recorder_attributes_through_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from curie_worker import action_digest, connector_k8s, run

    apps = FakeAppsV1Api(deployment(), deployment())
    monkeypatch.setattr(connector_k8s, "connector_deployments_api", lambda **_k: apps)
    engine = object()
    factory_calls: list[Any] = []
    resolve, asked = _resolver()

    def factory(eng: Any, *, db_schema: str, release: str) -> Resolver:
        factory_calls.append((eng, db_schema, release))
        return resolve

    monkeypatch.setattr(action_digest, "agent_deployment_resolver", factory)

    ledger = Ledger()
    opening, closing = _frames()
    async with httpx.AsyncClient(transport=httpx.MockTransport(ledger)) as http:
        composed = run._build_action_recorder(
            _GateConfig(reconcile=True, executor=True),  # type: ignore[arg-type]
            engine,  # type: ignore[arg-type]
            ActionClient(api_base_url="http://api", api_key="k", client=http),
        )
        recorded = await composed.record(
            opening, event_id="event-1", conversation_id="C1", agent_id=AGENT_ID
        )
        await composed.complete(recorded.id, closing)

    assert factory_calls == [(engine, "curie", "rel")]
    assert asked and asked[0] == (AGENT_ID, CONNECTOR)
    assert [r["namespace"] for r in apps.reads] == [NAMESPACE, NAMESPACE]
    assert _attributed(ledger.completion()) == (CONNECTOR, DIGEST)
