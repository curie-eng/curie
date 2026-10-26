"""ADR 0248 x lazy attachment access: a schedule resume that steers a live
session renews the opener's attachment authority only inside the schedule guard.

A refused continuation (the schedule changed between intake and the guarded
steer) must never renew the live turn's attachment lease or fetch its context:
the guard runs first and only an allowed steer touches the attachment control.

Driven through the kernel's real ``process_event``. The opener is a real
approval resume of a cron firing that opens a lazy-attachment turn on the
schedule's conversation and is held live, so the attachment authority marker
in Valkey is the one the opener's ``with_attachment_access`` recorded for the
actual route. The follow-up is a chained approval resume of that firing, which
carries its own attachment turn (set per attempt in ``kernel/attempt.py`` from
the lazy config) and steers the retained live route.

One seam is set directly: ``h.kernel._attachment_access``. The harness builds
no attachment access API client (``run.py`` wires the HTTP client to the real
API), so the test installs a recording control in its place, as the lazy
attachment routing tests do. Schedules, firings and approvals are real
Postgres rows; Valkey is real.
"""

from __future__ import annotations

import asyncio
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from aci_protocol import Final, SessionStatus, TextDelta
from curie_worker.attachment_access_client import AttachmentAccessMint

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from queue_fixtures import qevent  # noqa: E402
from test_agent_schedule_guards import SLOT, seeded  # noqa: E402
from test_agent_schedule_resume_kernel import (  # noqa: E402
    RESUME_TEXT,
    _cancel,
    _CountingRecorder,
    _outcomes,
    _resume,
    _SeedBinding,
)

OPENER_TEXT = "opener"


class Control:
    """Records every call the kernel makes on the attachment access API."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def register(self, **body: Any) -> None:
        self.calls.append(("register", body))

    async def context(self, **body: Any) -> AttachmentAccessMint:
        self.calls.append(("context", body))
        return AttachmentAccessMint("opaque", int(time.time()) + 600, 90)

    async def renew(self, **body: Any) -> bool:
        self.calls.append(("renew", body))
        return True

    async def revoke(self, **body: Any) -> bool:
        self.calls.append(("revoke", body))
        return True


def _lazy_harness(make_harness: Any, seed: Any, recorder: Any) -> Any:
    return make_harness(
        binding=_SeedBinding(seed),
        hook_runs=recorder,
        attachment_enabled=True,
        attachment_mode="lazy",
        internal_worker_token="worker",
        max_attempts=1,
    )


async def _steer_resume_into_live_opener(
    make_harness: Any, *, cancel_before: int | None
) -> dict[str, Any]:
    """Open a held lazy-attachment turn with one approval resume of the firing,
    then steer a chained approval resume into it. With ``cancel_before``, the
    schedule is cancelled just before that guard entry takes the lock."""

    async with seeded() as seed:
        await seed.firing(SLOT, 1)
        first = await seed.approval(seed.event_id(SLOT))
        second = await seed.approval(f"approval-{first}-resolved")
        recorder = _CountingRecorder(
            seed,
            change_before=cancel_before,
            change=(lambda: _cancel(seed)) if cancel_before is not None else None,
        )
        async with _lazy_harness(make_harness, seed, recorder) as h:
            control = Control()
            h.kernel._attachment_access = control
            h.runner.attachment_access_enforced = True
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=SessionStatus.DONE)]
            opener_event = _resume(seed, first).model_copy(update={"text": OPENER_TEXT})
            opener = asyncio.create_task(h.kernel.process_event(opener_event))
            try:
                async with asyncio.timeout(5):
                    while not h.runner.turn_active:
                        await asyncio.sleep(0.01)
                opener_calls = list(control.calls)
                assert [op for op, _ in opener_calls if op != "register"] == ["context"], (
                    "the opener did not mint lazy attachment authority; "
                    f"the steer would not reach renew: {opener_calls!r}"
                )
                event = _resume(seed, second)
                await h.kernel.process_event(event)
                result = {
                    "guards": recorder.guards,
                    "opener_context": next(b for op, b in opener_calls if op == "context"),
                    "resume_calls": control.calls[len(opener_calls) :],
                    "steers": list(h.runner.steers),
                    "steer_bodies": list(h.runner.steer_bodies),
                    "opened": list(h.runner.opened),
                    "outcomes": _outcomes(h, event.event_id),
                    "event_id": event.event_id,
                }
            finally:
                hold.set()
                await opener
            return result


def test_allowed_schedule_resume_renews_attachment_access_then_steers(make_harness) -> None:
    async def go() -> None:
        r = await _steer_resume_into_live_opener(make_harness, cancel_before=None)

        assert r["steers"] == [RESUME_TEXT]
        assert r["opened"] == [OPENER_TEXT], "the resume opened a turn instead of steering"
        assert r["outcomes"] != ["dropped"]
        ops = [op for op, _ in r["resume_calls"] if op != "register"]
        assert ops == ["renew", "context"], ops
        renewal = next(b for op, b in r["resume_calls"] if op == "renew")
        assert renewal["owner"] == r["opener_context"]["owner"]
        assert renewal["current_event_id"] == r["event_id"]
        capability = r["steer_bodies"][0].get("attachment_access")
        assert isinstance(capability, dict) and capability.get("token") == "opaque", capability

    asyncio.run(go())


def test_refused_schedule_resume_never_renews_attachment_access(make_harness) -> None:
    async def go() -> None:
        # The allowed run's last guard entry is the one around the steer.
        entries = (await _steer_resume_into_live_opener(make_harness, cancel_before=None))["guards"]
        r = await _steer_resume_into_live_opener(make_harness, cancel_before=entries)

        assert r["guards"] == entries
        assert not any(op == "renew" for op, _ in r["resume_calls"]), (
            f"a refused schedule resume renewed the live attachment lease: {r['resume_calls']!r}"
        )
        assert not any(op == "context" for op, _ in r["resume_calls"]), (
            f"a refused schedule resume fetched attachment context: {r['resume_calls']!r}"
        )
        assert r["steers"] == [], "steered after the cancel committed"
        assert r["opened"] == [OPENER_TEXT]
        assert r["outcomes"] == ["dropped"]

    asyncio.run(go())


def test_ordinary_live_steer_still_renews_attachment_access(make_harness) -> None:
    # Without a schedule resume the steer path is next's: no guard, and the
    # follow-up renews the opener's attachment authority.
    async def go() -> None:
        async with seeded() as seed:
            recorder = _CountingRecorder(seed)
            async with _lazy_harness(make_harness, seed, recorder) as h:
                control = Control()
                h.kernel._attachment_access = control
                h.runner.attachment_access_enforced = True
                hold = asyncio.Event()
                h.runner.hold = hold
                h.runner.default_script = [TextDelta(text="working")]
                h.runner.tail = [Final(text="done", status=SessionStatus.DONE)]
                thread = f"th-attachment-steer-{uuid.uuid4().hex}"
                opener = asyncio.create_task(
                    h.kernel.process_event(qevent(OPENER_TEXT, thread=thread, channel=seed.address))
                )
                try:
                    async with asyncio.timeout(5):
                        while not h.runner.turn_active:
                            await asyncio.sleep(0.01)
                    before = len(control.calls)
                    await h.kernel.process_event(
                        qevent("follow up", thread=thread, channel=seed.address)
                    )
                    ops = [op for op, _ in control.calls[before:] if op != "register"]
                    assert recorder.guards == 0
                    assert h.runner.steers == ["follow up"]
                    assert ops == ["renew", "context"], ops
                finally:
                    hold.set()
                    await opener

    asyncio.run(go())
