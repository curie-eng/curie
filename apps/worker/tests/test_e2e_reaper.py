"""The worker hosted end to end namespace reaper (#3245, ADR 0176 decision 4).

Teardown must not depend on the agent calling env_destroy. These tests drive
``E2EReaperLoop`` against a fake test cluster and a fake request status lookup.
"""

from __future__ import annotations

import asyncio
import base64
import json
import sys
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from curie_e2e_connector import kube as kube_module
from curie_e2e_connector.contract import (
    CLOSE_SETTLE_S,
    EXPIRES_ANNOTATION,
    IMAGES_CONFIGMAP,
    IMAGES_CONFIGMAP_KEY,
    OWNER_LABEL,
    RUN_LABEL,
)
from curie_e2e_connector.kube import ClusterApi, ClusterError, HttpxCluster
from curie_e2e_connector.reaper import Scope, SweepTarget
from curie_e2e_connector.registry import (
    DockerRegistry,
    RegistryApi,
    RegistryError,
    RegistrySettings,
)
from curie_worker import e2e_reaper as e2e_reaper_module
from curie_worker.config import WorkerConfig
from curie_worker.e2e_reaper import (
    TERMINAL_STATUSES,
    E2EReaperLoop,
    connector_secret_clusters,
    request_status_lookup,
)
from curie_worker.run import _build_e2e_reaper
from curie_worker.workitem_dispatch import WorkItemConflict, WorkItemTransportError

# importlib import mode does not add this test directory to sys.path.
sys.path.insert(0, str(Path(__file__).parent))

from otel_fixtures import Metric, Probe, install  # noqa: E402

NOW = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)
PAST = "2026-10-02T11:00:00Z"
FUTURE = "2026-10-02T13:00:00Z"
PREFIX = "curie-e2e-"
OWNER = "acme"
ATTRS = {"service.name": "curie-worker"}
LAST_SUCCESS = "curie.e2e.reaper.last_success"
EXPIRED = "curie.e2e.namespaces.expired"
OVERDUE = "curie.e2e.namespaces.overdue"
# Five minutes past its TTL: expired, but inside the ten minute overdue grace.
JUST_PAST = "2026-10-02T11:55:00Z"

RUN_DONE = uuid.UUID("aaaaaaaa-bbbb-4ccc-8ddd-000000000001")
RUN_LIVE = uuid.UUID("aaaaaaaa-bbbb-4ccc-8ddd-000000000002")
RUN_FOREIGN = uuid.UUID("aaaaaaaa-bbbb-4ccc-8ddd-000000000003")
RUN_FLAKY = uuid.UUID("aaaaaaaa-bbbb-4ccc-8ddd-000000000004")
RUN_OLD = uuid.UUID("aaaaaaaa-bbbb-4ccc-8ddd-000000000005")


def scope() -> Scope:
    return Scope(namespace_prefix=PREFIX, owner_label_key=OWNER_LABEL, owner_label_value=OWNER)


def ns(
    name: str, *, run: str | None, owner: str | None = OWNER, expires: str = FUTURE
) -> dict[str, Any]:
    labels: dict[str, str] = {}
    if owner is not None:
        labels[OWNER_LABEL] = owner
    if run is not None:
        labels[RUN_LABEL] = run
    return {
        "metadata": {
            "name": name,
            "labels": labels,
            "annotations": {EXPIRES_ANNOTATION: expires},
        },
        "status": {"phase": "Active"},
    }


def retention_job_delete(name: str) -> tuple[str, str]:
    """Teardown stops build Jobs before it deletes anything else (#3246)."""

    return (
        "DELETE",
        f"/apis/batch/v1/namespaces/{name}/jobs"
        "?labelSelector=curietech.ai%2Fe2e-build&propagationPolicy=Background",
    )


def reap_calls(name: str) -> list[tuple[str, str]]:
    return [
        retention_job_delete(name),
        ("DELETE", f"/apis/extensions.agents.x-k8s.io/v1beta1/namespaces/{name}/sandboxclaims"),
        ("DELETE", f"/apis/batch/v1/namespaces/{name}/jobs?propagationPolicy=Background"),
        ("DELETE", f"/api/v1/namespaces/{name}/persistentvolumeclaims"),
        ("DELETE", f"/api/v1/namespaces/{name}"),
    ]


class FakeCluster(ClusterApi):
    """Serves every namespace regardless of selector; records (method, path)."""

    def __init__(self, items: list[dict[str, Any]]) -> None:
        self.items = items
        self.calls: list[tuple[str, str]] = []
        self.list_status = 200
        self.status: dict[tuple[str, str], int] = {}
        # Per namespace image ledger. Absent means a settled, empty ledger: the
        # close was written a minute ago and nothing was recorded, so one pass
        # reaps the namespace exactly as it did before image retention.
        self.ledgers: dict[str, dict[str, Any]] = {}
        self.build_pods: dict[str, list[dict[str, Any]]] = {}
        self.log: list[str] | None = None

    def request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> tuple[int, dict[str, Any]]:
        self.calls.append((method, path))
        if self.log is not None:
            self.log.append(f"cluster {method} {path}")
        configmap = f"/configmaps/{IMAGES_CONFIGMAP}"
        if method == "GET" and path.endswith(configmap):
            name = path.split("/")[4]
            ledger = self.ledgers.get(name) or ledger_body(closed_ago=60)
            return 200, {
                "metadata": {"resourceVersion": "1"},
                "data": {IMAGES_CONFIGMAP_KEY: json.dumps(ledger)},
            }
        if method == "GET" and "/pods?labelSelector=" in path:
            return 200, {"items": self.build_pods.get(path.split("/")[4], [])}
        if method == "GET" and path.startswith("/api/v1/namespaces?"):
            if self.list_status != 200:
                return self.list_status, {}
            return 200, {"kind": "NamespaceList", "items": self.items}
        if method == "DELETE":
            return self.status.get((method, path), 200), {}
        return 500, {}

    def touched(self, name: str) -> bool:
        return any(f"/{name}" in path for _, path in self.calls)


class LiveCluster(FakeCluster):
    """A fake whose deletes take effect: later lists no longer return what was deleted.

    Each namespace starts with one recorded object in every child collection; a
    child collection delete empties it.
    """

    def __init__(self, items: list[dict[str, Any]]) -> None:
        super().__init__(items)
        self.children: dict[str, dict[str, int]] = {
            item["metadata"]["name"]: {
                path: 1 for _, path in reap_calls(item["metadata"]["name"])[1:-1]
            }
            for item in items
        }

    def request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> tuple[int, dict[str, Any]]:
        code, payload = super().request(method, path, body)
        if method == "DELETE" and 200 <= code < 300:
            for collections in self.children.values():
                if path in collections:
                    collections[path] = 0
            prefix = "/api/v1/namespaces/"
            if path.startswith(prefix) and "/" not in path[len(prefix) :]:
                gone = path[len(prefix) :]
                self.items = [item for item in self.items if item["metadata"]["name"] != gone]
        return code, payload

    def listed(self) -> list[str]:
        return [item["metadata"]["name"] for item in self.items]


def ledger_body(
    *, closed_ago: float | None = 60, repositories: list[str] | None = None
) -> dict[str, Any]:
    body: dict[str, Any] = {"repositories": list(repositories or [])}
    if closed_ago is not None:
        body["closing_at"] = (NOW - timedelta(seconds=closed_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return body


REGISTRY_PREFIX = "registry.test:5000/e2e"
DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64


class FakeRegistry(RegistryApi):
    """A registry keyed by repository; records every call into a shared log."""

    def __init__(
        self,
        *,
        prefix: str = "",
        tags: dict[str, dict[str, str]] | None = None,
        fail: bool = False,
    ) -> None:
        self.settings = RegistrySettings(prefix=prefix, insecure=False, token_hosts=())
        self.tags = tags or {}
        self.fail = fail
        self.calls: list[tuple[str, str]] = []
        self.log: list[str] | None = None

    def _note(self, op: str, *args: str) -> None:
        self.calls.append((op, *args))
        if self.log is not None:
            self.log.append(f"registry {op} {' '.join(args)}")

    def list_tags(self, repo: str) -> list[str]:
        self._note("list_tags", repo)
        if self.fail:
            raise RegistryError("registry unreachable host=registry.test")
        return sorted(self.tags.get(repo, {}))

    def resolve(self, repo: str, tag: str) -> str | None:
        self._note("resolve", repo, tag)
        return self.tags.get(repo, {}).get(tag)

    def delete_manifest(self, repo: str, digest: str) -> None:
        self._note("delete", repo, digest)


class FakeStatus:
    def __init__(self, statuses: dict[uuid.UUID, str | None]) -> None:
        self.statuses = statuses
        self.failing: set[uuid.UUID] = set()
        self.lookups: list[uuid.UUID] = []

    async def __call__(self, request_id: uuid.UUID) -> str | None:
        self.lookups.append(request_id)
        if request_id in self.failing:
            raise WorkItemTransportError("work-item dispatch endpoint is unreachable")
        return self.statuses.get(request_id)


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


def targets(clusters: list[FakeCluster], registry: FakeRegistry | None = None) -> list[SweepTarget]:
    return [
        SweepTarget(cluster=cluster, registry=registry or FakeRegistry()) for cluster in clusters
    ]


def make_loop(
    clusters: list[FakeCluster],
    status: FakeStatus,
    clock: Clock | None = None,
    registry: FakeRegistry | None = None,
) -> E2EReaperLoop:
    return E2EReaperLoop(
        clusters=lambda: targets(clusters, registry),
        request_status=status,
        scope=scope(),
        interval_s=60.0,
        wall_clock=clock or Clock(NOW),
    )


@pytest.fixture
def probe(monkeypatch: pytest.MonkeyPatch) -> Probe:
    return install(monkeypatch, e2e_reaper_module)


def test_terminal_statuses_are_the_four_finished_request_states() -> None:
    assert TERMINAL_STATUSES == frozenset({"completed", "failed", "expired", "cancelled"})


def test_a_completed_run_without_env_destroy_is_reaped_and_its_unlabelled_neighbour_is_not(
    probe: Probe,
) -> None:
    done = f"{PREFIX}{RUN_DONE}"
    live = f"{PREFIX}{RUN_LIVE}"
    unlabelled = f"{PREFIX}{RUN_FOREIGN}"
    cluster = FakeCluster(
        [
            ns(done, run=str(RUN_DONE), expires=FUTURE),
            ns(live, run=str(RUN_LIVE), expires=FUTURE),
            # Same prefix, no owner label, expired, and its run finished too.
            ns(unlabelled, run=str(RUN_FOREIGN), owner=None, expires=PAST),
        ]
    )
    status = FakeStatus({RUN_DONE: "completed", RUN_LIVE: "running", RUN_FOREIGN: "completed"})

    ok = asyncio.run(make_loop([cluster], status).sweep_once())

    assert ok is True
    deletes = [call for call in cluster.calls if call[0] == "DELETE"]
    assert deletes == reap_calls(done)
    assert not cluster.touched(unlabelled)
    assert not cluster.touched(live)
    assert RUN_FOREIGN not in status.lookups
    assert set(status.lookups) == {RUN_DONE, RUN_LIVE}


def test_a_completed_run_namespace_is_gone_after_one_pass_and_neighbours_survive_two(
    probe: Probe,
) -> None:
    """AC5 across passes: the agent never called env_destroy and the TTL has not passed."""

    done = f"{PREFIX}{RUN_DONE}"
    live = f"{PREFIX}{RUN_LIVE}"
    unlabelled = f"{PREFIX}{RUN_FOREIGN}"
    cluster = LiveCluster(
        [
            ns(done, run=str(RUN_DONE), expires=FUTURE),
            ns(live, run=str(RUN_LIVE), expires=FUTURE),
            ns(unlabelled, run=str(RUN_FOREIGN), owner=None, expires=PAST),
        ]
    )
    status = FakeStatus({RUN_DONE: "completed", RUN_LIVE: "running", RUN_FOREIGN: "completed"})
    loop = make_loop([cluster], status)

    async def drive() -> tuple[bool, bool]:
        first = await loop.sweep_once()
        after_first = len(cluster.calls)
        lookups_first = len(status.lookups)
        second = await loop.sweep_once()
        second_calls = cluster.calls[after_first:]
        assert [call for call in second_calls if call[0] == "DELETE"] == []
        assert RUN_DONE not in status.lookups[lookups_first:]
        return first, second

    assert asyncio.run(drive()) == (True, True)
    assert done not in cluster.listed()
    assert all(count == 0 for count in cluster.children[done].values())
    assert set(cluster.listed()) == {live, unlabelled}
    assert all(count == 1 for count in cluster.children[live].values())
    assert all(count == 1 for count in cluster.children[unlabelled].values())
    assert [call for call in cluster.calls if call[0] == "DELETE"] == reap_calls(done)
    assert not cluster.touched(live)
    assert not cluster.touched(unlabelled)
    assert RUN_LIVE in status.lookups
    assert RUN_FOREIGN not in status.lookups


@pytest.mark.parametrize("terminal", sorted(TERMINAL_STATUSES))
def test_every_terminal_status_reaps_an_unexpired_namespace(terminal: str, probe: Probe) -> None:
    name = f"{PREFIX}{RUN_DONE}"
    cluster = FakeCluster([ns(name, run=str(RUN_DONE), expires=FUTURE)])
    ok = asyncio.run(make_loop([cluster], FakeStatus({RUN_DONE: terminal})).sweep_once())
    assert ok is True
    assert [call for call in cluster.calls if call[0] == "DELETE"] == reap_calls(name)


@pytest.mark.parametrize("live_status", ["running", "queued", None])
def test_a_non_terminal_or_unknown_run_keeps_its_unexpired_namespace(
    live_status: str | None, probe: Probe
) -> None:
    name = f"{PREFIX}{RUN_LIVE}"
    cluster = FakeCluster([ns(name, run=str(RUN_LIVE), expires=FUTURE)])
    ok = asyncio.run(make_loop([cluster], FakeStatus({RUN_LIVE: live_status})).sweep_once())
    assert ok is True
    assert [call for call in cluster.calls if call[0] == "DELETE"] == []


def test_a_non_uuid_run_label_is_never_looked_up_and_only_ttl_applies(probe: Probe) -> None:
    keep = f"{PREFIX}keep"
    old = f"{PREFIX}old"
    cluster = FakeCluster(
        [
            ns(keep, run="not-a-uuid", expires=FUTURE),
            ns(old, run="also-not-a-uuid", expires=PAST),
            ns(f"{PREFIX}norun", run=None, expires=FUTURE),
        ]
    )
    status = FakeStatus({})
    ok = asyncio.run(make_loop([cluster], status).sweep_once())
    assert ok is True
    assert status.lookups == []
    assert [call for call in cluster.calls if call[0] == "DELETE"] == reap_calls(old)


def test_each_distinct_run_is_looked_up_once_per_pass(probe: Probe) -> None:
    cluster = FakeCluster(
        [
            ns(f"{PREFIX}{RUN_LIVE}", run=str(RUN_LIVE)),
            ns(f"{PREFIX}{RUN_LIVE}-b", run=str(RUN_LIVE)),
        ]
    )
    status = FakeStatus({RUN_LIVE: "running"})
    asyncio.run(make_loop([cluster], status).sweep_once())
    assert status.lookups == [RUN_LIVE]


def test_a_status_transport_error_fails_the_pass_but_ttl_reaping_still_happens(
    probe: Probe,
) -> None:
    flaky = f"{PREFIX}{RUN_FLAKY}"
    old = f"{PREFIX}{RUN_OLD}"
    cluster = FakeCluster(
        [
            ns(flaky, run=str(RUN_FLAKY), expires=FUTURE),
            ns(old, run=str(RUN_OLD), expires=PAST),
        ]
    )
    status = FakeStatus({RUN_FLAKY: "completed", RUN_OLD: "running"})
    status.failing.add(RUN_FLAKY)

    ok = asyncio.run(make_loop([cluster], status).sweep_once())

    assert ok is False
    assert [call for call in cluster.calls if call[0] == "DELETE"] == reap_calls(old)
    assert not cluster.touched(flaky)


def test_a_listing_error_fails_the_pass_and_other_clusters_are_still_swept(
    probe: Probe,
) -> None:
    broken = FakeCluster([])
    broken.list_status = 500
    old = f"{PREFIX}{RUN_OLD}"
    healthy = FakeCluster([ns(old, run=str(RUN_OLD), expires=PAST)])

    ok = asyncio.run(make_loop([broken, healthy], FakeStatus({})).sweep_once())

    assert ok is False
    assert [call for call in healthy.calls if call[0] == "DELETE"] == reap_calls(old)


def test_a_failed_namespace_delete_fails_the_pass(probe: Probe) -> None:
    old = f"{PREFIX}{RUN_OLD}"
    cluster = FakeCluster([ns(old, run=str(RUN_OLD), expires=PAST)])
    cluster.status[("DELETE", f"/api/v1/namespaces/{old}")] = 500
    ok = asyncio.run(make_loop([cluster], FakeStatus({})).sweep_once())
    assert ok is False


def test_no_test_cluster_configured_is_a_successful_empty_pass(probe: Probe) -> None:
    ok = asyncio.run(make_loop([], FakeStatus({})).sweep_once())
    assert ok is True
    assert probe.points(EXPIRED) == [Metric(EXPIRED, 0.0, ATTRS)]
    assert probe.points(OVERDUE) == [Metric(OVERDUE, 0.0, ATTRS)]
    assert [point.value for point in probe.points(LAST_SUCCESS)] == [NOW.timestamp()]


def test_last_success_is_zero_until_a_pass_succeeds_and_survives_a_later_failure(
    probe: Probe,
) -> None:
    old = f"{PREFIX}{RUN_OLD}"
    cluster = FakeCluster([ns(old, run=str(RUN_OLD), expires=PAST)])
    clock = Clock(NOW)
    loop = make_loop([cluster], FakeStatus({}), clock)

    async def drive() -> list[bool]:
        results = []
        cluster.list_status = 500
        results.append(await loop.sweep_once())
        cluster.list_status = 200
        results.append(await loop.sweep_once())
        clock.now = NOW + timedelta(minutes=5)
        cluster.list_status = 500
        results.append(await loop.sweep_once())
        return results

    assert asyncio.run(drive()) == [False, True, False]
    points = probe.points(LAST_SUCCESS)
    assert [point.value for point in points] == [0.0, NOW.timestamp(), NOW.timestamp()]
    assert all(point.attributes == ATTRS for point in points)


def test_last_success_advances_with_each_good_pass(probe: Probe) -> None:
    clock = Clock(NOW)
    loop = make_loop([FakeCluster([])], FakeStatus({}), clock)

    async def drive() -> None:
        await loop.sweep_once()
        clock.now = NOW + timedelta(seconds=60)
        await loop.sweep_once()

    asyncio.run(drive())
    assert [point.value for point in probe.points(LAST_SUCCESS)] == [
        NOW.timestamp(),
        NOW.timestamp() + 60,
    ]


def test_expired_gauge_sums_expired_namespaces_across_clusters(probe: Probe) -> None:
    first = FakeCluster(
        [
            ns(f"{PREFIX}a", run=None, expires=PAST),
            ns(f"{PREFIX}b", run=None, expires=PAST),
            ns(f"{PREFIX}{RUN_DONE}", run=str(RUN_DONE), expires=FUTURE),
        ]
    )
    second = FakeCluster(
        [
            ns(f"{PREFIX}c", run=None, expires=PAST),
            ns(f"{PREFIX}d", run=None, expires=FUTURE),
            # Foreign namespaces are never counted.
            ns(f"{PREFIX}e", run=None, owner="someone-else", expires=PAST),
            ns("team-prod", run=None, expires=PAST),
        ]
    )
    status = FakeStatus({RUN_DONE: "completed"})

    ok = asyncio.run(make_loop([first, second], status).sweep_once())

    assert ok is True
    points = probe.points(EXPIRED)
    assert [point.value for point in points] == [3.0]
    assert points[0].attributes == ATTRS


def test_overdue_gauge_sums_namespaces_past_the_grace_across_clusters(probe: Probe) -> None:
    first = FakeCluster(
        [
            ns(f"{PREFIX}a", run=None, expires=PAST),
            # Expired but within the grace: counted expired, not overdue.
            ns(f"{PREFIX}b", run=None, expires=JUST_PAST),
            ns(f"{PREFIX}{RUN_DONE}", run=str(RUN_DONE), expires=FUTURE),
        ]
    )
    second = FakeCluster(
        [
            ns(f"{PREFIX}c", run=None, expires=PAST),
            ns(f"{PREFIX}d", run=None, expires=JUST_PAST),
            ns(f"{PREFIX}e", run=None, owner="someone-else", expires=PAST),
        ]
    )

    ok = asyncio.run(make_loop([first, second], FakeStatus({RUN_DONE: "completed"})).sweep_once())

    assert ok is True
    assert probe.points(EXPIRED) == [Metric(EXPIRED, 4.0, ATTRS)]
    assert probe.points(OVERDUE) == [Metric(OVERDUE, 2.0, ATTRS)]


def test_a_listing_failure_records_neither_namespace_gauge(probe: Probe) -> None:
    broken = FakeCluster([])
    broken.list_status = 500
    healthy = FakeCluster([ns(f"{PREFIX}a", run=None, expires=PAST)])

    ok = asyncio.run(make_loop([broken, healthy], FakeStatus({})).sweep_once())

    assert ok is False
    assert probe.points(EXPIRED) == []
    assert probe.points(OVERDUE) == []
    assert [point.value for point in probe.points(LAST_SUCCESS)] == [0.0]


def test_run_forever_sweeps_until_stopped(probe: Probe) -> None:
    cluster = FakeCluster([])
    loop = E2EReaperLoop(
        clusters=lambda: targets([cluster]),
        request_status=FakeStatus({}),
        scope=scope(),
        interval_s=0.01,
        wall_clock=Clock(NOW),
    )

    async def drive() -> None:
        stop = asyncio.Event()
        task = asyncio.create_task(loop.run_forever(stop))
        await asyncio.sleep(0.1)
        stop.set()
        await asyncio.wait_for(task, timeout=2)

    asyncio.run(drive())
    lists = [call for call in cluster.calls if call[0] == "GET"]
    assert len(lists) >= 2


class GatedStatus(FakeStatus):
    """A status lookup that blocks until ``gate`` is set; records a cancellation."""

    def __init__(self, statuses: dict[uuid.UUID, str | None]) -> None:
        super().__init__(statuses)
        self.gate = asyncio.Event()
        self.cancelled = False

    async def __call__(self, request_id: uuid.UUID) -> str | None:
        try:
            await self.gate.wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        return await super().__call__(request_id)


def _counting(loop: E2EReaperLoop) -> list[int]:
    real = loop.sweep_once
    calls = [0]

    async def sweep_once() -> bool:
        calls[0] += 1
        return await real()

    loop.sweep_once = sweep_once  # type: ignore[method-assign]
    return calls


async def _until(condition: Any, timeout: float = 1.0) -> None:
    async with asyncio.timeout(timeout):
        while not condition():
            await asyncio.sleep(0.005)


EARLIER = NOW - timedelta(hours=1)


@pytest.mark.parametrize("earlier_success", [False, True])
def test_a_slow_pass_is_never_cancelled_or_overlapped_and_reports_its_stale_timestamp(
    probe: Probe, earlier_success: bool
) -> None:
    # The SDK's synchronous gauge drops its value after each collection, so a
    # slow pass must keep the gauge recorded: stale, never absent. It must also
    # run to completion, or slow lookups would keep deletion from ever running.
    clock = Clock(EARLIER)
    clusters: list[FakeCluster] = []
    status = GatedStatus({RUN_DONE: "completed"})
    loop = E2EReaperLoop(
        clusters=lambda: targets(clusters),
        request_status=status,
        scope=scope(),
        interval_s=0.02,
        wall_clock=clock,
    )
    expected_stale = 0.0
    if earlier_success:
        asyncio.run(loop.sweep_once())
        expected_stale = EARLIER.timestamp()
    clock.now = NOW
    cluster = LiveCluster([ns(f"{PREFIX}done", run=str(RUN_DONE))])
    clusters.append(cluster)
    calls = _counting(loop)
    before = len(probe.points(LAST_SUCCESS))

    async def drive() -> None:
        stop = asyncio.Event()
        task = asyncio.create_task(loop.run_forever(stop))
        await _until(lambda: len(probe.points(LAST_SUCCESS)) - before >= 3)
        assert calls[0] == 1
        assert cluster.listed() == [f"{PREFIX}done"]
        stale = probe.points(LAST_SUCCESS)[before:]
        assert all(point == Metric(LAST_SUCCESS, expected_stale, ATTRS) for point in stale)

        status.gate.set()
        await _until(lambda: calls[0] >= 2)
        assert cluster.listed() == []
        assert Metric(LAST_SUCCESS, NOW.timestamp(), ATTRS) in probe.points(LAST_SUCCESS)
        stop.set()
        await asyncio.wait_for(task, timeout=1)

    asyncio.run(drive())


def test_stop_while_a_pass_runs_cancels_it_and_returns_promptly(probe: Probe) -> None:
    status = GatedStatus({RUN_DONE: "completed"})
    cluster = LiveCluster([ns(f"{PREFIX}done", run=str(RUN_DONE))])
    loop = E2EReaperLoop(
        clusters=lambda: targets([cluster]),
        request_status=status,
        scope=scope(),
        interval_s=0.02,
        wall_clock=Clock(NOW),
    )
    calls = _counting(loop)

    async def drive() -> None:
        stop = asyncio.Event()
        task = asyncio.create_task(loop.run_forever(stop))
        await _until(lambda: len(probe.points(LAST_SUCCESS)) >= 2)
        stop.set()
        await asyncio.wait_for(task, timeout=0.2)

    asyncio.run(drive())
    assert status.cancelled
    assert calls[0] == 1
    assert cluster.listed() == [f"{PREFIX}done"]


def test_the_slow_pass_warning_defaults_to_five_intervals_with_a_five_minute_floor() -> None:
    def threshold(interval: float) -> float:
        loop = E2EReaperLoop(
            clusters=lambda: [],
            request_status=FakeStatus({}),
            scope=scope(),
            interval_s=interval,
        )
        return loop._slow_pass

    assert threshold(10.0) == 300.0
    assert threshold(120.0) == 600.0


SETTINGS = RegistrySettings(prefix="", insecure=False, token_hosts=())


def _kubeconfig(server: str) -> str:
    return (
        "apiVersion: v1\n"
        f"clusters:\n- cluster: {{server: '{server}'}}\n  name: t\n"
        "users:\n- user: {token: cluster-token}\n  name: u\n"
    )


def _b64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


class FakeCoreV1:
    def __init__(self, items: list[Any]) -> None:
        self.items = items
        self.namespaces: list[str] = []

    def list_namespaced_secret(self, namespace: str, **_kwargs: Any) -> Any:
        self.namespaces.append(namespace)
        return SimpleNamespace(items=self.items)


def _secret(name: str, data: dict[str, str] | None) -> Any:
    return SimpleNamespace(metadata=SimpleNamespace(name=name), data=data)


def test_connector_secret_clusters_reads_only_this_releases_connector_secrets() -> None:
    a = _kubeconfig("https://cluster-a.example:6443")
    b = _kubeconfig("https://cluster-b.example:6443")
    c = _kubeconfig("https://cluster-c.example:6443")
    d = _kubeconfig("https://cluster-d.example:6443")
    core = FakeCoreV1(
        [
            _secret("curie-alpha-connector-secrets", {"E2E_CLUSTER_KUBECONFIG": _b64(a)}),
            # Same kubeconfig contents through a second agent: one client.
            _secret("curie-beta-connector-secrets", {"E2E_CLUSTER_KUBECONFIG": _b64(a)}),
            _secret(
                "curie-gamma-connector-secrets",
                {"E2E_CLUSTER_KUBECONFIG": _b64(b), "OTHER": _b64("x")},
            ),
            _secret("curie-delta-connector-secrets", {"GITHUB_TOKEN": _b64("t")}),
            _secret("curie-epsilon-connector-secrets", None),
            _secret("other-alpha-connector-secrets", {"E2E_CLUSTER_KUBECONFIG": _b64(c)}),
            _secret("curie-alpha-secrets", {"E2E_CLUSTER_KUBECONFIG": _b64(d)}),
        ]
    )

    found = connector_secret_clusters(
        core, namespace="curie-system", release="curie", timeout=5.0, registry=SETTINGS
    )

    assert core.namespaces == ["curie-system"]
    assert len(found) == 2
    assert all(isinstance(target, SweepTarget) for target in found)
    clusters = [target.cluster for target in found]
    assert all(isinstance(cluster, HttpxCluster) for cluster in clusters)
    servers = {str(cluster._client.base_url).rstrip("/") for cluster in clusters}
    assert servers == {"https://cluster-a.example:6443", "https://cluster-b.example:6443"}
    for cluster in clusters:
        cluster._client.close()


def test_connector_secret_clusters_with_no_matching_secret_is_empty() -> None:
    core = FakeCoreV1([_secret("curie-alpha-connector-secrets", {"GITHUB_TOKEN": _b64("t")})])
    assert (
        connector_secret_clusters(
            core, namespace="ns", release="curie", timeout=5.0, registry=SETTINGS
        )
        == []
    )


_EXEC_KUBECONFIG = (
    "apiVersion: v1\n"
    "clusters:\n- cluster: {server: 'https://cluster-x.example:6443'}\n  name: t\n"
    "users:\n- name: u\n  user:\n    exec:\n      apiVersion: client.authentication.k8s.io/v1\n"
    "      command: aws-secret-helper\n"
)


def test_connector_secret_clusters_reports_a_refused_kubeconfig_instead_of_dropping_it() -> None:
    good = _kubeconfig("https://cluster-a.example:6443")
    core = FakeCoreV1(
        [
            _secret("curie-alpha-connector-secrets", {"E2E_CLUSTER_KUBECONFIG": _b64(good)}),
            _secret("curie-beta-connector-secrets", {"E2E_CLUSTER_KUBECONFIG": "%%not-base64%%"}),
            _secret(
                "curie-gamma-connector-secrets",
                {"E2E_CLUSTER_KUBECONFIG": _b64(_EXEC_KUBECONFIG)},
            ),
            _secret(
                "curie-delta-connector-secrets",
                {"E2E_CLUSTER_KUBECONFIG": base64.b64encode(b"\xff\xfe").decode("ascii")},
            ),
        ]
    )

    found = connector_secret_clusters(
        core, namespace="ns", release="curie", timeout=5.0, registry=SETTINGS
    )

    assert len(found) == 4
    assert all(isinstance(target, SweepTarget) for target in found)
    clusters = [target.cluster for target in found]
    valid = [cluster for cluster in clusters if isinstance(cluster, HttpxCluster)]
    refused = [cluster for cluster in clusters if not isinstance(cluster, HttpxCluster)]
    assert len(valid) == 1
    assert str(valid[0]._client.base_url).rstrip("/") == "https://cluster-a.example:6443"
    valid[0]._client.close()
    messages = []
    for cluster in refused:
        with pytest.raises(ClusterError) as excinfo:
            cluster.request("GET", "/api/v1/namespaces")
        messages.append(str(excinfo.value))
    for secret_name in (
        "curie-beta-connector-secrets",
        "curie-gamma-connector-secrets",
        "curie-delta-connector-secrets",
    ):
        assert sum(secret_name in message for message in messages) == 1, messages
    for message in messages:
        assert "aws-secret-helper" not in message
        assert "cluster-x.example" not in message
        assert "not-base64" not in message


def _mock_cluster_clients(
    monkeypatch: pytest.MonkeyPatch,
    items_by_server: dict[str, list[dict[str, Any]]],
    ledgers: dict[str, dict[str, Any]] | None = None,
) -> list[tuple[str, str, str]]:
    """Make valid kubeconfigs yield clients served in process; refusals stay real.

    ``ledgers`` maps a namespace to its image ledger; any other namespace has a
    settled, empty one.
    """

    calls: list[tuple[str, str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        server = f"{request.url.scheme}://{request.url.host}:{request.url.port}"
        path = request.url.raw_path.decode()
        calls.append((server, request.method, path))
        if request.method == "GET" and path.endswith(f"/configmaps/{IMAGES_CONFIGMAP}"):
            ledger = (ledgers or {}).get(path.split("/")[4]) or ledger_body(closed_ago=60)
            return httpx.Response(
                200,
                json={
                    "metadata": {"resourceVersion": "1"},
                    "data": {IMAGES_CONFIGMAP_KEY: json.dumps(ledger)},
                },
            )
        if request.method == "GET" and "/pods?labelSelector=" in path:
            return httpx.Response(200, json={"items": []})
        if request.method == "GET":
            return httpx.Response(200, json={"items": items_by_server[server]})
        return httpx.Response(200, json={})

    def client(text: str, timeout: float) -> httpx.Client:
        server, _token, _verify = kube_module.load_kubeconfig_text(text)
        return httpx.Client(base_url=server, transport=httpx.MockTransport(handler))

    monkeypatch.setattr(e2e_reaper_module, "client_from_kubeconfig_text", client)
    return calls


def _secret_loop(core: FakeCoreV1, clock: Clock) -> E2EReaperLoop:
    return E2EReaperLoop(
        clusters=lambda: connector_secret_clusters(
            core, namespace="ns", release="curie", timeout=5.0, registry=SETTINGS
        ),
        request_status=FakeStatus({}),
        scope=scope(),
        interval_s=60.0,
        wall_clock=clock,
    )


def test_a_pass_whose_only_credential_is_refused_fails_and_recovers_once_repaired(
    probe: Probe, monkeypatch: pytest.MonkeyPatch
) -> None:
    server = "https://cluster-a.example:6443"
    old = f"{PREFIX}{RUN_OLD}"
    calls = _mock_cluster_clients(monkeypatch, {server: [ns(old, run=str(RUN_OLD), expires=PAST)]})
    core = FakeCoreV1(
        [
            _secret(
                "curie-alpha-connector-secrets",
                {"E2E_CLUSTER_KUBECONFIG": _b64(_EXEC_KUBECONFIG)},
            )
        ]
    )
    clock = Clock(NOW)
    loop = _secret_loop(core, clock)

    async def drive() -> list[bool]:
        results = [await loop.sweep_once()]
        core.items = [
            _secret(
                "curie-alpha-connector-secrets",
                {"E2E_CLUSTER_KUBECONFIG": _b64(_kubeconfig(server))},
            )
        ]
        clock.now = NOW + timedelta(minutes=1)
        results.append(await loop.sweep_once())
        return results

    assert asyncio.run(drive()) == [False, True]
    assert [point.value for point in probe.points(LAST_SUCCESS)] == [
        0.0,
        (NOW + timedelta(minutes=1)).timestamp(),
    ]
    # The refused pass held both namespace gauges; the repaired pass recorded them.
    assert [point.value for point in probe.points(EXPIRED)] == [1.0]
    assert [point.value for point in probe.points(OVERDUE)] == [1.0]
    assert (server, "DELETE", f"/api/v1/namespaces/{old}") in calls


def test_a_refused_credential_fails_the_pass_but_the_valid_cluster_is_still_swept(
    probe: Probe, monkeypatch: pytest.MonkeyPatch
) -> None:
    server = "https://cluster-a.example:6443"
    old = f"{PREFIX}{RUN_OLD}"
    calls = _mock_cluster_clients(monkeypatch, {server: [ns(old, run=str(RUN_OLD), expires=PAST)]})
    core = FakeCoreV1(
        [
            _secret(
                "curie-alpha-connector-secrets",
                {"E2E_CLUSTER_KUBECONFIG": _b64(_kubeconfig(server))},
            ),
            _secret("curie-beta-connector-secrets", {"E2E_CLUSTER_KUBECONFIG": "%%not-base64%%"}),
        ]
    )

    ok = asyncio.run(_secret_loop(core, Clock(NOW)).sweep_once())

    assert ok is False
    assert [(method, path) for _, method, path in calls if method == "DELETE"] == reap_calls(old)
    assert [point.value for point in probe.points(LAST_SUCCESS)] == [0.0]
    assert probe.points(EXPIRED) == []
    assert probe.points(OVERDUE) == []


def _forbid_kube_config(monkeypatch: pytest.MonkeyPatch) -> None:
    from kubernetes import config as k8s_config

    def refuse(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("a misconfigured reaper must not load a kube config")

    monkeypatch.setattr(k8s_config, "load_incluster_config", refuse)
    monkeypatch.setattr(k8s_config, "load_kube_config", refuse)


def _worker_config(*, reaper: bool, reconcile: bool) -> WorkerConfig:
    return WorkerConfig(
        e2e_reaper_enabled=reaper,
        connector_reconcile_enabled=reconcile,
        connector_namespace="curie-system",
        connector_release="curie",
        connector_app_name="curie",
    )


def test_build_e2e_reaper_is_none_when_the_reaper_is_disabled() -> None:
    config = _worker_config(reaper=False, reconcile=True)
    assert _build_e2e_reaper(config, SimpleNamespace()) is None  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("reconcile", "work_items"),
    [
        pytest.param(False, SimpleNamespace(), id="connector-reconcile-off"),
        pytest.param(True, None, id="no-worker-token"),
    ],
)
def test_a_misconfigured_reaper_still_runs_and_every_pass_fails(
    reconcile: bool, work_items: Any, probe: Probe, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A reaper that cannot work must still report, so CurieE2EReaperStalled fires."""

    _forbid_kube_config(monkeypatch)
    config = _worker_config(reaper=True, reconcile=reconcile)

    loop = _build_e2e_reaper(config, work_items)

    assert isinstance(loop, E2EReaperLoop)

    async def drive() -> list[bool]:
        return [await loop.sweep_once(), await loop.sweep_once()]

    assert asyncio.run(drive()) == [False, False]
    assert [point.value for point in probe.points(LAST_SUCCESS)] == [0.0, 0.0]
    assert probe.points(EXPIRED) == []
    assert probe.points(OVERDUE) == []


class FakeWorkItemClient:
    def __init__(self) -> None:
        self.known = uuid.uuid4()
        self.missing = uuid.uuid4()
        self.down = uuid.uuid4()

    async def get_request(self, request_id: uuid.UUID) -> Any:
        if request_id == self.known:
            return SimpleNamespace(status="completed")
        if request_id == self.missing:
            raise WorkItemConflict("not_found")
        raise WorkItemTransportError("work-item dispatch endpoint is unreachable")


def test_request_status_lookup_maps_not_found_to_none_and_propagates_transport() -> None:
    client = FakeWorkItemClient()
    lookup = request_status_lookup(client)

    assert asyncio.run(lookup(client.known)) == "completed"
    assert asyncio.run(lookup(client.missing)) is None
    with pytest.raises(WorkItemTransportError):
        asyncio.run(lookup(client.down))


# ---------------------------------------------------------------------------
# Image retention wiring (#3246, ADR 0176 decision 6)
# ---------------------------------------------------------------------------

PUSH_SECRET_KEY = "E2E_REGISTRY_PUSH_CONFIG"
PUSH_USER = "pusher"
PUSH_PASSWORD = "push-password-sentinel"
IMAGE_SETTINGS = RegistrySettings(
    prefix=REGISTRY_PREFIX, insecure=True, token_hosts=("auth.registry.test",)
)


def _push_config(user: str = PUSH_USER, password: str = PUSH_PASSWORD) -> str:
    auth = base64.b64encode(f"{user}:{password}".encode()).decode("ascii")
    return json.dumps({"auths": {"registry.test:5000": {"auth": auth}}})


def _both(kubeconfig: str, push: str | None) -> dict[str, str]:
    data = {"E2E_CLUSTER_KUBECONFIG": _b64(kubeconfig)}
    if push is not None:
        data[PUSH_SECRET_KEY] = _b64(push)
    return data


def _close(found: list[SweepTarget]) -> None:
    for target in found:
        client = getattr(target.cluster, "_client", None)
        if client is not None:
            client.close()


def test_the_cluster_source_stays_a_callable_returning_sweep_targets_every_pass(
    probe: Probe,
) -> None:
    cluster = FakeCluster([])
    invocations = [0]

    def source() -> list[SweepTarget]:
        invocations[0] += 1
        return targets([cluster])

    loop = E2EReaperLoop(
        clusters=source,
        request_status=FakeStatus({}),
        scope=scope(),
        interval_s=60.0,
        wall_clock=Clock(NOW),
    )

    async def drive() -> list[bool]:
        return [await loop.sweep_once(), await loop.sweep_once()]

    assert asyncio.run(drive()) == [True, True]
    assert invocations[0] == 2


def test_the_push_config_is_read_from_the_same_secret_and_reaches_the_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[tuple[str, str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(
            (
                request.method,
                str(request.url),
                request.headers.get("authorization"),
            )
        )
        return httpx.Response(200, json={"name": "x", "tags": ["build-a"]})

    def factory(*_args: Any, **_kwargs: Any) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)

    monkeypatch.setattr(e2e_reaper_module, "registry_client", factory)
    # The registry owns only <prefix><run uuid> namespace segments.
    run_ns = f"{PREFIX}{RUN_DONE}"
    kubeconfig = _kubeconfig("https://cluster-a.example:6443")
    core = FakeCoreV1([_secret("curie-alpha-connector-secrets", _both(kubeconfig, _push_config()))])

    found = connector_secret_clusters(
        core, namespace="ns", release="curie", timeout=5.0, registry=IMAGE_SETTINGS
    )
    try:
        assert len(found) == 1
        registry = found[0].registry
        assert isinstance(registry, DockerRegistry)
        assert registry.settings == IMAGE_SETTINGS
        assert registry.list_tags(f"{REGISTRY_PREFIX}/{run_ns}/app") == ["build-a"]
    finally:
        _close(found)
    expected = "Basic " + base64.b64encode(f"{PUSH_USER}:{PUSH_PASSWORD}".encode()).decode()
    assert seen
    assert all(auth == expected for _method, _url, auth in seen)
    assert all(
        url.startswith(f"http://registry.test:5000/v2/e2e/{run_ns}/app/") for _m, url, _a in seen
    )


def test_a_secret_without_a_push_config_still_yields_a_target_with_no_credentials() -> None:
    kubeconfig = _kubeconfig("https://cluster-a.example:6443")
    core = FakeCoreV1([_secret("curie-alpha-connector-secrets", _both(kubeconfig, None))])

    found = connector_secret_clusters(
        core, namespace="ns", release="curie", timeout=5.0, registry=SETTINGS
    )
    try:
        assert len(found) == 1
        assert isinstance(found[0].cluster, HttpxCluster)
        assert isinstance(found[0].registry, RegistryApi)
        assert found[0].registry.settings.prefix == ""
    finally:
        _close(found)


def test_the_same_kubeconfig_with_different_push_configs_gives_one_composite_target() -> None:
    from curie_e2e_connector.registry import CompositeRegistry

    kubeconfig = _kubeconfig("https://cluster-a.example:6443")
    core = FakeCoreV1(
        [
            _secret(
                "curie-alpha-connector-secrets",
                _both(kubeconfig, _push_config(password="one")),
            ),
            _secret(
                "curie-beta-connector-secrets",
                _both(kubeconfig, _push_config(password="two")),
            ),
        ]
    )
    found = connector_secret_clusters(
        core, namespace="ns", release="curie", timeout=5.0, registry=IMAGE_SETTINGS
    )
    try:
        assert len(found) == 1
        assert isinstance(found[0].registry, CompositeRegistry)
    finally:
        _close(found)


def test_agents_with_and_without_a_push_config_on_one_kubeconfig_share_one_target() -> None:
    kubeconfig = _kubeconfig("https://cluster-a.example:6443")
    core = FakeCoreV1(
        [
            _secret("curie-alpha-connector-secrets", _both(kubeconfig, _push_config())),
            _secret("curie-beta-connector-secrets", _both(kubeconfig, _push_config())),
            _secret("curie-gamma-connector-secrets", _both(kubeconfig, None)),
        ]
    )
    found = connector_secret_clusters(
        core, namespace="ns", release="curie", timeout=5.0, registry=IMAGE_SETTINGS
    )
    try:
        assert len(found) == 1
    finally:
        _close(found)


def test_two_secrets_sharing_a_cluster_count_an_expired_namespace_once(
    probe: Probe, monkeypatch: pytest.MonkeyPatch
) -> None:
    server = "https://cluster-a.example:6443"
    old = f"{PREFIX}{RUN_OLD}"
    _mock_cluster_clients(monkeypatch, {server: [ns(old, run=str(RUN_OLD), expires=PAST)]})
    kubeconfig = _kubeconfig(server)
    core = FakeCoreV1(
        [
            _secret(
                "curie-alpha-connector-secrets",
                _both(kubeconfig, _push_config(password="one")),
            ),
            _secret(
                "curie-beta-connector-secrets",
                _both(kubeconfig, _push_config(password="two")),
            ),
        ]
    )
    loop = E2EReaperLoop(
        clusters=lambda: connector_secret_clusters(
            core, namespace="ns", release="curie", timeout=5.0, registry=IMAGE_SETTINGS
        ),
        request_status=FakeStatus({}),
        scope=scope(),
        interval_s=60.0,
        wall_clock=Clock(NOW),
    )

    assert asyncio.run(loop.sweep_once()) is True

    assert [point.value for point in probe.points(EXPIRED)] == [1.0]
    assert [point.value for point in probe.points(OVERDUE)] == [1.0]


@pytest.mark.parametrize(
    "bad_push",
    ["%%not-base64%%", base64.b64encode(b"{not json: push-text-sentinel").decode("ascii")],
    ids=["not-base64", "not-json"],
)
def test_an_undecodable_push_config_fails_only_namespaces_with_a_ledger_and_never_logs_its_text(
    bad_push: str,
    probe: Probe,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = "https://cluster-a.example:6443"
    empty = f"{PREFIX}{RUN_OLD}"
    imaged = f"{PREFIX}{RUN_FLAKY}"
    repo = f"{REGISTRY_PREFIX}/{imaged}/app"
    calls = _mock_cluster_clients(
        monkeypatch,
        {
            server: [
                ns(empty, run=str(RUN_OLD), expires=PAST),
                ns(imaged, run=str(RUN_FLAKY), expires=PAST),
            ]
        },
        ledgers={imaged: ledger_body(closed_ago=60, repositories=[repo])},
    )
    core = FakeCoreV1(
        [
            _secret(
                "curie-alpha-connector-secrets",
                {"E2E_CLUSTER_KUBECONFIG": _b64(_kubeconfig(server)), PUSH_SECRET_KEY: bad_push},
            )
        ]
    )
    loop = E2EReaperLoop(
        clusters=lambda: connector_secret_clusters(
            core, namespace="ns", release="curie", timeout=5.0, registry=IMAGE_SETTINGS
        ),
        request_status=FakeStatus({}),
        scope=scope(),
        interval_s=60.0,
        wall_clock=Clock(NOW),
    )

    with caplog.at_level("DEBUG"):
        ok = asyncio.run(loop.sweep_once())

    assert ok is False
    deleted = [path for _server, method, path in calls if method == "DELETE"]
    assert f"/api/v1/namespaces/{empty}" in deleted
    assert f"/api/v1/namespaces/{imaged}" not in deleted
    assert not any(path.startswith(f"/api/v1/namespaces/{imaged}/") for path in deleted)
    assert "push-text-sentinel" not in caplog.text
    assert "not-base64" not in caplog.text
    assert "curie-alpha-connector-secrets" in caplog.text


def test_a_refused_push_config_names_only_the_secret_in_the_registry_error() -> None:
    kubeconfig = _kubeconfig("https://cluster-a.example:6443")
    core = FakeCoreV1(
        [
            _secret(
                "curie-alpha-connector-secrets",
                _both(kubeconfig, "{not json: push-text-sentinel"),
            )
        ]
    )
    found = connector_secret_clusters(
        core, namespace="ns", release="curie", timeout=5.0, registry=IMAGE_SETTINGS
    )
    try:
        assert len(found) == 1
        with pytest.raises(RegistryError) as excinfo:
            found[0].registry.list_tags(f"{REGISTRY_PREFIX}/ns/app")
        assert "curie-alpha-connector-secrets" in str(excinfo.value)
        assert "push-text-sentinel" not in str(excinfo.value)
    finally:
        _close(found)


def test_a_namespace_with_images_deletes_them_before_any_child_or_namespace_delete(
    probe: Probe,
) -> None:
    name = f"{PREFIX}{RUN_OLD}"
    repo = f"{REGISTRY_PREFIX}/{name}/app"
    cluster = FakeCluster([ns(name, run=str(RUN_OLD), expires=PAST)])
    cluster.ledgers[name] = ledger_body(closed_ago=60, repositories=[repo])
    registry = FakeRegistry(
        prefix=REGISTRY_PREFIX,
        tags={repo: {"staging-a": DIGEST_A, "build-a": DIGEST_B}},
    )
    log: list[str] = []
    cluster.log = log
    registry.log = log

    ok = asyncio.run(make_loop([cluster], FakeStatus({}), registry=registry).sweep_once())

    assert ok is True
    assert sorted(call for call in registry.calls if call[0] == "delete") == [
        ("delete", repo, DIGEST_A),
        ("delete", repo, DIGEST_B),
    ]
    last_registry = max(i for i, line in enumerate(log) if line.startswith("registry "))
    first_child = min(
        i
        for i, line in enumerate(log)
        if "sandboxclaims" in line or "persistentvolumeclaims" in line
    )
    namespace_delete = log.index(f"cluster DELETE /api/v1/namespaces/{name}")
    assert last_registry < first_child < namespace_delete


def test_a_registry_failure_keeps_the_namespace_and_the_pass_unclean_but_isolates_the_next(
    probe: Probe,
) -> None:
    broken = f"{PREFIX}{RUN_OLD}"
    healthy = f"{PREFIX}{RUN_FLAKY}"
    first = FakeCluster([ns(broken, run=str(RUN_OLD), expires=PAST)])
    first.ledgers[broken] = ledger_body(
        closed_ago=60, repositories=[f"{REGISTRY_PREFIX}/{broken}/app"]
    )
    second = FakeCluster([ns(healthy, run=str(RUN_FLAKY), expires=PAST)])
    failing = FakeRegistry(prefix=REGISTRY_PREFIX, fail=True)
    fine = FakeRegistry(prefix=REGISTRY_PREFIX)
    loop = E2EReaperLoop(
        clusters=lambda: [
            SweepTarget(cluster=first, registry=failing),
            SweepTarget(cluster=second, registry=fine),
        ],
        request_status=FakeStatus({}),
        scope=scope(),
        interval_s=60.0,
        wall_clock=Clock(NOW),
    )

    ok = asyncio.run(loop.sweep_once())

    assert ok is False
    assert [call for call in first.calls if call[0] == "DELETE"] == [retention_job_delete(broken)]
    assert [call for call in second.calls if call[0] == "DELETE"] == reap_calls(healthy)
    assert [point.value for point in probe.points(LAST_SUCCESS)] == [0.0]


def test_a_running_build_defers_the_namespace_without_failing_the_pass(probe: Probe) -> None:
    name = f"{PREFIX}{RUN_OLD}"
    cluster = FakeCluster([ns(name, run=str(RUN_OLD), expires=PAST)])
    cluster.build_pods[name] = [{"status": {"phase": "Running"}}]

    ok = asyncio.run(make_loop([cluster], FakeStatus({})).sweep_once())

    assert ok is True
    deletes = [call for call in cluster.calls if call[0] == "DELETE"]
    assert ("DELETE", f"/api/v1/namespaces/{name}") not in deletes
    assert [point.value for point in probe.points(LAST_SUCCESS)] == [NOW.timestamp()]


def test_a_namespace_inside_the_close_settle_window_is_deferred_with_no_delete(
    probe: Probe,
) -> None:
    name = f"{PREFIX}{RUN_OLD}"
    cluster = FakeCluster([ns(name, run=str(RUN_OLD), expires=PAST)])
    cluster.ledgers[name] = ledger_body(closed_ago=CLOSE_SETTLE_S - 1)

    ok = asyncio.run(make_loop([cluster], FakeStatus({})).sweep_once())

    assert ok is True
    assert [call for call in cluster.calls if call[0] == "DELETE"] == []


def test_an_unset_registry_with_an_empty_ledger_reaps_in_one_pass_as_before(probe: Probe) -> None:
    """Liveness: no registry configured changes nothing about reaping."""

    name = f"{PREFIX}{RUN_OLD}"
    cluster = FakeCluster([ns(name, run=str(RUN_OLD), expires=PAST)])
    registry = FakeRegistry(prefix="")

    ok = asyncio.run(make_loop([cluster], FakeStatus({}), registry=registry).sweep_once())

    assert ok is True
    assert [call for call in cluster.calls if call[0] == "DELETE"] == reap_calls(name)
    assert registry.calls == []


class _Captured:
    def __init__(self) -> None:
        self.kwargs: dict[str, Any] = {}


def _patch_reaper_build(monkeypatch: pytest.MonkeyPatch) -> _Captured:
    from kubernetes import client as k8s_client
    from kubernetes import config as k8s_config

    captured = _Captured()
    monkeypatch.setattr(k8s_config, "load_incluster_config", lambda: None)
    monkeypatch.setattr(k8s_client, "CoreV1Api", lambda: object())

    def fake(core: Any, **kwargs: Any) -> list[SweepTarget]:
        captured.kwargs = kwargs
        return []

    monkeypatch.setattr(e2e_reaper_module, "connector_secret_clusters", fake)
    return captured


def test_the_worker_config_reads_the_three_registry_env_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "CURIE_E2E_REGISTRY",
        "CURIE_E2E_REGISTRY_INSECURE",
        "CURIE_E2E_REGISTRY_TOKEN_HOSTS",
    ):
        monkeypatch.delenv(name, raising=False)
    defaults = WorkerConfig()
    assert defaults.e2e_registry == ""
    assert defaults.e2e_registry_insecure is False
    assert defaults.e2e_registry_token_hosts == ""
    monkeypatch.setenv("CURIE_E2E_REGISTRY", REGISTRY_PREFIX)
    monkeypatch.setenv("CURIE_E2E_REGISTRY_INSECURE", "true")
    monkeypatch.setenv("CURIE_E2E_REGISTRY_TOKEN_HOSTS", "a.test, b.test:8443")
    config = WorkerConfig()
    assert config.e2e_registry == REGISTRY_PREFIX
    assert config.e2e_registry_insecure is True
    assert config.e2e_registry_token_hosts == "a.test, b.test:8443"


def test_build_e2e_reaper_hands_the_registry_settings_to_the_cluster_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _patch_reaper_build(monkeypatch)
    config = WorkerConfig(
        e2e_reaper_enabled=True,
        connector_reconcile_enabled=True,
        connector_namespace="curie-system",
        connector_release="curie",
        connector_app_name="curie",
        e2e_registry=REGISTRY_PREFIX,
        e2e_registry_insecure=True,
        e2e_registry_token_hosts="a.test, b.test:8443",
    )

    loop = _build_e2e_reaper(config, SimpleNamespace())  # type: ignore[arg-type]

    assert isinstance(loop, E2EReaperLoop)
    assert loop._clusters() == []
    assert captured.kwargs["registry"] == RegistrySettings(
        prefix=REGISTRY_PREFIX, insecure=True, token_hosts=("a.test", "b.test:8443")
    )


def test_build_e2e_reaper_with_the_registry_unset_passes_an_empty_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _patch_reaper_build(monkeypatch)
    config = _worker_config(reaper=True, reconcile=True)

    loop = _build_e2e_reaper(config, SimpleNamespace())  # type: ignore[arg-type]

    assert isinstance(loop, E2EReaperLoop)
    loop._clusters()
    assert captured.kwargs["registry"] == RegistrySettings(
        prefix="", insecure=False, token_hosts=()
    )


def test_build_e2e_reaper_normalizes_a_trailing_slash_and_owns_the_connector_repository(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from curie_e2e_connector.registry import owns_repository

    captured = _patch_reaper_build(monkeypatch)
    monkeypatch.setenv("CURIE_E2E_REGISTRY", "registry.example.com/e2e/")
    config = WorkerConfig(
        e2e_reaper_enabled=True,
        connector_reconcile_enabled=True,
        connector_namespace="curie-system",
        connector_release="curie",
        connector_app_name="curie",
    )

    loop = _build_e2e_reaper(config, SimpleNamespace())  # type: ignore[arg-type]

    assert isinstance(loop, E2EReaperLoop)
    loop._clusters()
    settings = captured.kwargs["registry"]
    assert settings.prefix == "registry.example.com/e2e"
    namespace = f"{PREFIX}{RUN_OLD}"
    assert owns_repository(settings, namespace, f"registry.example.com/e2e/{namespace}/app")
