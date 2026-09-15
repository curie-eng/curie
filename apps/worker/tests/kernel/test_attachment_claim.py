"""Where the attachment lane meets the kernel's claim and reap paths (#2567, S3).

Two joins, both against the real Valkey and the real G1 substrate (only Slack
and the model are faked, as everywhere in this suite):

1. **Claim.** A turn carrying ``QueuedTurn.attachments`` must have them resolved
   into a signed reference BEFORE the sandbox is claimed, and that reference must
   arrive in the claim env. A turn carrying none must produce the claim it
   produces today, without the lane being consulted at all -- that is the
   common case and the regression this file guards hardest.

2. **Reap.** The attachment retention ledger is a SIBLING of the workspace one
   and is swept from the SAME ``reap_orphans`` tick, reusing the per-thread lock
   fence. No second scheduler, and no overloading the thread-ownership authority
   with a foreign object class. ``test_kernel.py``'s
   ``test_workspace_reaper_holds_the_route_lock_during_exact_ledger_recheck``
   is the shape the third test here mirrors, because the reason is identical:
   the object deletes between ``begin`` and ``finish`` can outlive the original
   lease, so the lock must be renewed across the whole critical section.

The lane itself is a test double here on purpose. Its own behavior -- the bounded
download, the digest, the refusal, the prefixes -- is pinned in
``tests/test_attachments.py`` and ``tests/test_attachment_retention.py`` against
a conforming object-store port. What is under test in this file is only the
kernel's wiring: what it calls, when, and what it puts in the claim env.
"""

from __future__ import annotations

import asyncio
import inspect
import threading
import time
from collections.abc import Callable, Sequence
from typing import Any

from aci_protocol import Attachment, Final, QueuedTurn, ReplyHandle, SessionStatus, TurnSource
from curie_worker.kernel import Kernel

DONE = SessionStatus.DONE
ATTACHMENTS_REF_ENV = "CURIE_ATTACHMENTS_REF"
REF_VALUE = "opaque-presigned-attachment-reference"


def _qevent(
    text: str,
    *,
    thread: str = "th-att",
    attachments: Sequence[Attachment] = (),
) -> QueuedTurn:
    return QueuedTurn(
        event_id=f"ev-{thread}-{len(text)}-{len(attachments)}",
        conversation_id=thread,
        author="U1",
        text=text,
        reply_handle=ReplyHandle(kind="slack", channel="C1", placeholder="p-1"),
        received_at="2026-07-05T00:00:00+00:00",
        source=TurnSource.SLACK,
        attachments=list(attachments),
    )


def _thread_key(thread: str) -> str:
    return f"slack:C1:{thread}"


async def _wait_until(pred: Callable[[], bool], what: str, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"timed out waiting for {what}")


class _FakeAttachmentLane:
    """The kernel-facing surface of ``AttachmentCoordinator``, recorded.

    ``resolve`` returns an object with the one method the claim path needs
    (``claim_env``), so a kernel that resolves but forgets to merge the result
    into the boot env fails the claim-env assertion rather than passing on the
    strength of the call alone.
    """

    def __init__(self, *, ref_value: str = REF_VALUE) -> None:
        self.resolve_calls: list[dict[str, Any]] = []
        self._ref_value = ref_value

    def resolve(
        self,
        *,
        thread_key: str,
        agent_id: str | None = None,
        attachments: Sequence[Attachment],
        **extra: Any,
    ) -> Any:
        self.resolve_calls.append(
            {
                "thread_key": thread_key,
                "agent_id": agent_id,
                "names": [ref.name for ref in attachments],
                "ids": [ref.id for ref in attachments],
                "extra": extra,
            }
        )
        env = {ATTACHMENTS_REF_ENV: self._ref_value} if attachments else {}
        return _Prepared(env)

    # The reap half. Unused by the claim tests; present because the kernel holds
    # ONE optional collaborator for this lane, exactly as it holds one for the
    # workspace lane.
    def enumerate_expired(self) -> list[str]:
        return []

    def begin_expired_reap(self, thread_key: str) -> object | None:
        return None

    def finish_expired_reap(self, candidate: object) -> bool:
        return True


class _Prepared:
    def __init__(self, env: dict[str, str]) -> None:
        self._env = env
        self.refs: tuple[Any, ...] = ()

    def claim_env(self) -> dict[str, str]:
        return dict(self._env)


def _claim_env(h: Any) -> dict[str, str]:
    envs = [env for env in h.fake_k8s.claim_envs if env is not None]
    assert envs, "no sandbox was claimed"
    return envs[-1]


def test_a_turn_with_no_attachments_claims_exactly_the_env_it_claims_today(
    make_harness,
) -> None:
    """The common case, untouched: the lane is not even consulted.

    Asserted two ways so the guard cannot be satisfied by an empty-valued entry:
    the lane records no ``resolve`` call, and the claim env carries no
    attachment key at all -- not the key with an empty value, which an init
    container would read as "there is work here".

    The signature check ahead of it pins the wiring shape: the lane is an
    OPTIONAL injected collaborator named ``attachments``, exactly as the
    workspace lane is ``workspace``, so a deployment without it runs unchanged
    instead of a turn discovering a missing attribute. The tests below reach the
    field as ``_attachments`` to match ``_workspace``.
    """

    assert "attachments" in inspect.signature(Kernel.__init__).parameters, (
        "Kernel must take the attachment lane as an optional keyword collaborator, "
        "the way it takes `workspace`"
    )

    async def go() -> None:
        async with make_harness() as h:
            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            h.runner.default_script = [Final(text="ok", status=DONE)]

            await h.kernel.process_event(_qevent("plain question", thread="tNoFiles"))

            assert lane.resolve_calls == [], (
                "a turn with no attachments must not touch the attachment lane"
            )
            env = _claim_env(h)
            assert ATTACHMENTS_REF_ENV not in env
            assert h.sink.last_text == "ok"

    asyncio.run(go())


def test_a_resolved_attachment_reference_reaches_the_claim_env(make_harness) -> None:
    """Resolved before the claim, and carried on it.

    Order is the load-bearing part: the reference has to exist before
    ``substrate.claim`` runs, because the claim env is how it is delivered.
    Resolving after the claim would boot the sandbox and then have nowhere to
    put the capability.
    """

    async def go() -> None:
        async with make_harness() as h:
            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            h.runner.default_script = [Final(text="read it", status=DONE)]

            await h.kernel.process_event(
                _qevent(
                    "what does this say?",
                    thread="tOneFile",
                    attachments=[Attachment(id="F1", name="report.csv", mime_type="text/csv")],
                )
            )

            assert len(lane.resolve_calls) == 1, lane.resolve_calls
            call = lane.resolve_calls[0]
            assert call["thread_key"] == _thread_key("tOneFile")
            assert call["ids"] == ["F1"]
            assert call["names"] == ["report.csv"]
            assert _claim_env(h)[ATTACHMENTS_REF_ENV] == REF_VALUE

    asyncio.run(go())


def test_a_turn_with_files_is_answered_normally_when_the_lane_is_switched_off(
    make_harness,
) -> None:
    """The shipped default (``worker.attachments.enabled: false``) end to end.

    Off is a real state, not an absence of tests: ``run.build`` wires no
    coordinator, so ``Kernel._attachments`` is None and a message that DOES
    carry files has to behave exactly as v0.8.8 behaves -- answered, text only,
    files ignored, no error and no attachment key on the claim. The failure this
    guards is an ``assert lane is not None`` or an unguarded attribute reaching
    the turn path: every uploaded file would then dead-letter the turn on a
    deployment that had deliberately switched the feature off.
    """

    async def go() -> None:
        async with make_harness() as h:
            assert h.kernel._attachments is None, (  # noqa: SLF001 -- the state IS the subject
                "the harness pre-wires a lane; this test is about its absence"
            )
            h.runner.default_script = [Final(text="answered without the file", status=DONE)]

            await h.kernel.process_event(
                _qevent(
                    "what does this say?",
                    thread="tLaneOff",
                    attachments=[
                        Attachment(id="F1", name="report.csv", mime_type="text/csv"),
                        Attachment(id="F2", name="report.csv", mime_type="text/csv"),
                    ],
                )
            )

            assert h.sink.last_text == "answered without the file", (
                "a turn carrying files errored or was never answered with the lane off"
            )
            # Read straight off the substrate rather than through _claim_env:
            # with the lane off the kernel never touches boot_env at all, so the
            # claim carries None -- not an empty dict, which is what a lane that
            # resolved to nothing produces. Both are attachment-free; only this
            # one proves the turn path was not entered.
            assert h.fake_k8s.claim_envs and all(
                env is None or ATTACHMENTS_REF_ENV not in env for env in h.fake_k8s.claim_envs
            ), (
                "the lane is off and the claim still carries an attachment "
                f"capability: {h.fake_k8s.claim_envs}"
            )

    asyncio.run(go())


def test_the_attachment_ledger_is_swept_from_the_same_reap_tick_under_the_route_lock(
    make_harness,
) -> None:
    """One tick, one lock fence, two ledgers.

    ``reap_orphans`` is the existing periodic tick; the attachment sweep joins it
    rather than arriving with a scheduler of its own. The lock assertion is the
    same one the workspace sweep carries: object deletion happens between
    ``begin`` and ``finish`` and can outlive the original lease, so a competing
    claimant on that thread must stay fenced for the whole critical section --
    otherwise a fresh resolve can land between the two halves and have its ledger
    deleted by the finish.
    """

    async def go() -> None:
        async with make_harness(
            lock_ttl_ms=90,
            lock_acquire_timeout_s=1.0,
            lock_poll_interval_s=0.01,
        ) as h:
            thread = _thread_key("tAttachmentReap")
            entered = threading.Event()
            gate = threading.Event()
            finished: list[object] = []

            class GatedAttachmentLane:
                def resolve(self, **_kwargs: Any) -> Any:
                    raise AssertionError("the reap tick must not resolve anything")

                def enumerate_expired(self) -> list[str]:
                    return [thread]

                def begin_expired_reap(self, thread_key: str) -> object:
                    assert thread_key == thread
                    entered.set()
                    gate.wait(timeout=5.0)
                    return object()

                def finish_expired_reap(self, candidate: object) -> bool:
                    finished.append(candidate)
                    return True

            h.kernel._attachments = GatedAttachmentLane()  # type: ignore[attr-defined]

            reaping = asyncio.create_task(h.kernel.reap_orphans())
            await _wait_until(
                entered.is_set,
                "reap_orphans to sweep the attachment ledger in the same tick",
            )

            contender = asyncio.create_task(h.kernel._lock.acquire(h.config.lock_key(thread)))
            try:
                await asyncio.sleep(0.25)
                assert not contender.done(), (
                    "the attachment sweep mutated its ledger outside the route lock"
                )
            finally:
                gate.set()
            await reaping
            assert len(finished) == 1, "the sweep never reached its fenced ledger delete"
            token = await contender
            await h.kernel._lock.release(h.config.lock_key(thread), token)

    asyncio.run(go())
