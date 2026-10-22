"""Campaign supervisor: cancellation on expiry, exhaustion, revocation or broker loss.

@spec PROTECTED-COMMISSIONING-3 PROTECTED-COMMISSIONING-8. The supervisor's
own decisions run against synthetic campaign and launch views; the composed
cases drive the actual commissioning mediator, registry and mechanics over the
synthetic external Kubernetes API of the launch tests.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any

import httpx
import pytest
from curie_worker import commissioning_supervisor as supervisor
from curie_worker.commissioning_registry import object_names

from .protected_launch_endpoint_helpers import context
from .test_commissioning_launch import OWNER, ROUTE, Cluster, mediator, request_bytes, settled

FAR = int(time.time() * 1000) + 3600 * 1000


@dataclass
class State:
    state: str = "running"
    exhausted: str | None = None
    broker_time_ms: int = 0
    generation: int = 2


class Campaign:
    """Synthetic campaign journal view; ``lost`` models a broker that refuses or vanished."""

    def __init__(self, events: list[Any], *, expires_at_ms: int = FAR, **state: Any) -> None:
        self.events, self.expires_at_ms = events, expires_at_ms
        self.state = State(broker_time_ms=int(time.time() * 1000), **state)
        self.lost = False

    async def status(self) -> State:
        if self.lost:
            raise ConnectionError("broker lost")
        return self.state

    async def begin_close(self) -> None:
        self.events.append(("BEGIN_CLOSE", self.state.state))
        if self.lost:
            raise ConnectionError("broker lost")
        if self.state.state in {"prepared", "running"}:
            self.state.state = "closing"


class Launches:
    """Synthetic mediator view: one ``close_campaign`` answer per call."""

    def __init__(self, events: list[Any], answers: list[Any] | None = None) -> None:
        self.events, self.answers = events, list(answers or [()])

    def fence(self) -> None:
        self.events.append(("FENCE",))

    async def close_campaign(self) -> tuple[str, ...]:
        self.events.append(("CLOSE_CAMPAIGN",))
        answer = self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]
        if isinstance(answer, BaseException):
            raise answer
        return tuple(answer)


def build(campaign: Campaign, launches: Any, **options: Any) -> supervisor.CampaignSupervisor:
    values = {"interval_s": 0.01, "join_s": 0.2, "close_s": 1.0, **options}
    return supervisor.CampaignSupervisor(campaign, launches, **values)


async def session(events: list[Any], name: str) -> None:
    try:
        await asyncio.Event().wait()
    finally:
        events.append(("JOINED", name))


@pytest.mark.parametrize(
    ("cause", "reason"),
    [
        ("broker-expiry", "expired"),
        ("local-expiry", "expired"),
        ("exhaustion", "exhausted"),
        ("revocation", "revoked"),
        ("closed-elsewhere", "closing"),
        ("broker-loss", "broker_lost"),
    ],
)
def test_each_stop_cause_fences_then_joins_then_retires(cause, reason):
    async def scenario() -> None:
        events: list[Any] = []
        now = int(time.time() * 1000)
        campaign = Campaign(events, expires_at_ms=now + 60_000)
        launches = Launches(events)
        clock = None
        sup = build(campaign, launches, clock=lambda: clock or time.time())
        sup.own(session(events, "sdk"))
        sup.own(session(events, "runner"))
        running = asyncio.create_task(sup.run())
        await asyncio.sleep(0.05)
        assert not running.done()
        if cause == "broker-expiry":
            campaign.state.broker_time_ms = now + 60_000
        elif cause == "local-expiry":
            clock = (now + 60_000) / 1000
        elif cause == "exhaustion":
            campaign.state.state, campaign.state.exhausted = "closing", "launches"
        elif cause == "revocation":
            sup.revoke()
        elif cause == "closed-elsewhere":
            campaign.state.state = "failed"
        else:
            campaign.lost = True
        report = await asyncio.wait_for(running, 5)
        assert report.reason == reason
        assert events[0] == ("FENCE",)
        joined = [e for e in events if e[0] == "JOINED"]
        assert sorted(joined) == [("JOINED", "runner"), ("JOINED", "sdk")]
        # Every owned session is joined before any workload is retired.
        assert events.index(("CLOSE_CAMPAIGN",)) > max(events.index(e) for e in joined)
        closes = [e for e in events if e[0] == "BEGIN_CLOSE"]
        if reason in {"expired", "revoked"}:
            assert closes == [("BEGIN_CLOSE", "running")]
        else:
            assert closes == []
        assert report.joined is True
        assert report.unsettled == ()
        assert report.complete is True
        with pytest.raises(supervisor.SupervisorRefused):
            sup.own(session(events, "late"))

    asyncio.run(scenario())


def test_a_session_that_resists_cancellation_leaves_cleanup_incomplete():
    async def scenario() -> None:
        events: list[Any] = []
        campaign = Campaign(events)
        sup = build(campaign, Launches(events))
        release = asyncio.Event()

        async def stubborn() -> None:
            while not release.is_set():
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    continue

        task = sup.own(stubborn())
        await asyncio.sleep(0)  # the session is running when it is cancelled
        sup.revoke()
        report = await asyncio.wait_for(sup.run(), 5)
        # Workloads are still retired: their termination is observed independently.
        assert ("CLOSE_CAMPAIGN",) in events
        assert report.joined is False and report.complete is False
        release.set()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())


def test_ownership_that_is_not_retired_leaves_cleanup_incomplete():
    async def scenario() -> None:
        events: list[Any] = []
        launch_id = "b0000000-0000-4000-8000-000000000001"
        sup = build(Campaign(events), Launches(events, [(launch_id,)]))
        sup.revoke()
        report = await asyncio.wait_for(sup.run(), 5)
        # Closing retries within its budget, then reports what is left; never assumed.
        assert events.count(("CLOSE_CAMPAIGN",)) > 1
        assert report.unsettled == (launch_id,) and report.complete is False

    asyncio.run(scenario())


def test_a_lost_broker_still_retires_this_process_launches_but_never_completes():
    async def scenario() -> None:
        events: list[Any] = []
        campaign = Campaign(events)
        campaign.lost = True
        sup = build(campaign, Launches(events, [ConnectionError("broker lost")]))
        report = await asyncio.wait_for(sup.run(), 5)
        assert report.reason == "broker_lost"
        assert ("CLOSE_CAMPAIGN",) in events
        assert report.unsettled is None and report.complete is False

    asyncio.run(scenario())


def test_a_restarted_control_resumes_a_closing_campaign_from_durable_ownership():
    async def scenario() -> None:
        events: list[Any] = []
        campaign = Campaign(events, state="closing")
        sup = build(campaign, Launches(events))
        report = await asyncio.wait_for(sup.run(), 5)
        assert report.reason == "closing" and report.complete is True
        assert events == [("FENCE",), ("CLOSE_CAMPAIGN",)]

    asyncio.run(scenario())


def test_a_prepared_campaign_is_watched_not_closed():
    async def scenario() -> None:
        events: list[Any] = []
        campaign = Campaign(events, state="prepared")
        sup = build(campaign, Launches(events))
        running = asyncio.create_task(sup.run())
        await asyncio.sleep(0.05)
        assert not running.done() and events == []
        campaign.state.state = "running"
        await asyncio.sleep(0.05)
        assert not running.done()
        sup.revoke()
        assert (await asyncio.wait_for(running, 5)).reason == "revoked"

    asyncio.run(scenario())


def test_the_supervisor_runs_once():
    async def scenario() -> None:
        events: list[Any] = []
        sup = build(Campaign(events), Launches(events))
        sup.revoke()
        await asyncio.wait_for(sup.run(), 5)
        with pytest.raises(supervisor.SupervisorRefused):
            await sup.run()

    asyncio.run(scenario())


def test_expiry_during_a_live_launch_retires_it_and_fences_new_creates(tmp_path):
    async def scenario() -> None:
        cluster = Cluster()
        async with mediator(tmp_path, cluster) as (server, control, url, root):
            raw = request_bytes()
            launch_id = json.loads(raw)["launch_id"]
            cluster.launch_id = launch_id
            events: list[Any] = []
            now = int(time.time() * 1000)
            campaign = Campaign(events, expires_at_ms=now + 60_000)
            sup = build(campaign, server)
            async with httpx.AsyncClient(verify=context(root), trust_env=False) as http:
                created = await http.post(url + ROUTE, content=raw)
                assert created.status_code == 202, created.json()
                await settled(server, launch_id)
                pod_uid = control.ownership[launch_id][1]
                assert control.ownership[launch_id] == ("bound", pod_uid)
                sdk = sup.own(session(events, "sdk"))
                running = asyncio.create_task(sup.run())
                campaign.state.broker_time_ms = now + 60_000
                report = await asyncio.wait_for(running, 30)
                assert report.reason == "expired" and report.complete is True, report
                assert sdk.done()
                assert control.ownership[launch_id] == ("retired", pod_uid)
                assert server._registry.get(OWNER, launch_id).state == "retired"
                assert not [k for k in cluster.objects if k[0] in {"pods", "services", "secrets"}]
                # A fenced mediator charges and creates nothing more.
                before = len(cluster.events)
                fenced = await http.post(url + ROUTE, content=request_bytes())
                assert fenced.status_code == 503
                assert not [e for e in cluster.events[before:] if e[0] in {"RESERVE", "OWN"}]

    asyncio.run(scenario())


def test_an_interrupted_closing_resumes_in_a_new_control_process(tmp_path):
    async def scenario() -> None:
        cluster = Cluster()
        raw = request_bytes()
        launch_id = json.loads(raw)["launch_id"]
        cluster.launch_id = launch_id
        events: list[Any] = []
        async with mediator(tmp_path, cluster) as (server, control, url, root):
            async with httpx.AsyncClient(verify=context(root), trust_env=False) as http:
                assert (await http.post(url + ROUTE, content=raw)).status_code == 202
                await settled(server, launch_id)
            pod_uid = control.ownership[launch_id][1]
            # The first closing is interrupted before Kubernetes confirms anything.
            cluster.fault_mode = "no-termination"
            sup = build(Campaign(events), server, close_s=0.1)
            sup.revoke()
            first = await asyncio.wait_for(sup.run(), 30)
            assert first.complete is False and first.unsettled == (launch_id,)
            assert server._registry.get(OWNER, launch_id).state == "retiring"
        assert control.ownership[launch_id] == ("retiring", pod_uid)
        # A new control process reopens the same registry and broker ownership.
        cluster.fault_mode = None
        pod = cluster.objects[("pods", object_names(launch_id)[0])]
        status = pod["status"]["containerStatuses"][0]
        status["state"] = {
            "terminated": {
                "containerID": status["containerID"],
                "exitCode": 137,
                "startedAt": "2026-10-12T00:00:00Z",
                "finishedAt": "2026-10-12T00:00:02Z",
            }
        }
        async with mediator(tmp_path, cluster, control=control) as (server, _, _url, _root):
            assert server._live == {}
            sup = build(Campaign(events, state="closing"), server)
            report = await asyncio.wait_for(sup.run(), 30)
            assert report.reason == "closing" and report.complete is True, report
            assert control.ownership[launch_id] == ("retired", pod_uid)
            assert not [k for k in cluster.objects if k[0] in {"pods", "services", "secrets"}]

    asyncio.run(scenario())
