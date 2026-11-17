"""Workspace timeouts wait without spending start attempts (#5155)."""

from __future__ import annotations

import asyncio
import threading

import pytest
from aci_protocol import Final, SessionStatus, TextDelta

from apps.worker.tests.queue_fixtures import wait_until
from apps.worker.tests.start_wait_fixtures import (
    StartWaitCase,
    start_wait,
    start_wait_database,
    timeout_lookalike,
)

__all__ = ["start_wait", "start_wait_database"]


@pytest.mark.parametrize("start_wait", ["lookalike"], indirect=True)
def test_timeout_text_without_timeout_type_still_counts(start_wait: StartWaitCase) -> None:
    case = start_wait
    case.run(case.deliver())
    payload = case.payloads[0]
    assert payload["start_failure_detail"] == timeout_lookalike()
    assert payload["reason"].startswith("not_started:")
    assert payload["capacity"] is False
    assert case.run(case.state())["start_deferrals"] == 1
    assert case.h.runner.opened == []


def test_saturated_capacity_does_no_workspace_work_and_later_starts(
    start_wait: StartWaitCase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = start_wait

    async def exercise() -> None:
        await asyncio.to_thread(case.h.substrate.claim, "blocking-one")
        creating, release_creation = threading.Event(), threading.Event()
        create = case.h.fake_k8s.create_claim

        def delayed_create(*args: object, **kwargs: object) -> None:
            creating.set()
            if not release_creation.wait(10):
                raise RuntimeError("external control-plane gate was not released")
            create(*args, **kwargs)

        monkeypatch.setattr(case.h.fake_k8s, "create_claim", delayed_create)
        pending_claim = asyncio.create_task(
            asyncio.to_thread(case.h.substrate.claim, "blocking-two")
        )
        try:
            assert await asyncio.to_thread(creating.wait, 5)
            before = case.ledger()
            assert len(before) == 2 and len(case.h.fake_k8s.claims) == 1
            await case.deliver()
            waiting = await case.state()
            assert waiting["status"] == "waiting"
            assert waiting["capacity_deferrals"] == 1 and waiting["start_deferrals"] == 0
            assert waiting["wait_deadline"] == case.initial["wait_deadline"]
            assert case.payloads[-1]["capacity"] is True
            assert case.issuer.redemptions == 0
            assert case.host.git_requests == []
            assert case.uploads == [] and case.keys() == []
            assert case.ledger() == before
            assert len(case.h.fake_k8s.claims) == 1
            assert case.h.runner.opened == []
        finally:
            release_creation.set()
            await pending_claim
            monkeypatch.setattr(case.h.fake_k8s, "create_claim", create)
        assert await asyncio.to_thread(case.h.substrate.release, "blocking-one")
        await case.deliver()
        assert case.issuer.redemptions == 1
        assert case.host.git_requests
        assert case.uploads
        assert len(case.h.runner.opened) >= 1
        final = await case.state()
        assert final["execution_attempts"] == 1
        assert final["started_at"] is not None
        assert final["wait_deadline"] == case.initial["wait_deadline"]

    case.run(exercise())


def test_existing_workspace_route_is_adopted_while_capacity_is_full(
    start_wait: StartWaitCase,
) -> None:
    case = start_wait

    async def exercise() -> None:
        case.h.runner.default_script = [TextDelta(text="Working.")]
        case.h.runner.tail = [Final(text="Done.", status=SessionStatus.DONE)]
        case.h.runner.hold = asyncio.Event()
        running = asyncio.create_task(case.deliver())
        try:
            await wait_until(lambda: bool(case.h.runner.queried))
            route = case.h.substrate.adopt(case.thread)
            assert route is not None
            await asyncio.to_thread(case.h.substrate.claim, "blocking-one")
            before = (case.issuer.redemptions, len(case.host.git_requests), len(case.uploads))
            await case.h.kernel.process_event(case.event("existing-route-followup", "Keep going."))
            assert case.h.runner.steers == ["Keep going."]
            assert case.h.substrate.adopt(case.thread) == route
            assert (
                case.issuer.redemptions,
                len(case.host.git_requests),
                len(case.uploads),
            ) == before
            assert len(case.h.fake_k8s.claims) == 2
            assert case.payloads == []
        finally:
            case.h.runner.hold.set()
            await running

    case.run(exercise())
