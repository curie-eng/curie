"""Every boot for a thread rebuilds the thread's files: the kernel half (ADR 0205, #4079).

Against the real Valkey and the real G1 substrate over the fake Kubernetes
client; Slack and the model are faked as everywhere in this suite. The API's
thread attachment ledger is faked at the kernel's collaborator seam (its HTTP
client is pinned in ``tests/test_attachment_ledger_client.py``), and so is the
attachment lane in most tests here, as ``test_attachment_claim.py`` fakes it.
Three tests wire the REAL ``AttachmentCoordinator`` over the conforming
``RetainingObjectStore`` so the integration itself is pinned, not only the
calls.

Contract pinned here (the implementer follows these names; the lane side is
pinned in ``tests/test_attachment_thread_set.py``):

* ``Kernel.__init__`` takes ``attachment_ledger`` (a
  ``ThreadAttachmentLedgerClient`` or anything with its two async methods),
  default None, stored as ``self._attachment_ledger``. With no ledger wired the
  kernel behaves exactly as before (``resolve`` for a file turn, nothing for a
  text turn), so ``test_attachment_claim.py`` stays as it is.
* Ledger calls: ``await ledger.query(agent_id=str(agent_id), thread_key=...)``
  and ``await ledger.append(agent_id=str(agent_id), thread_key=...,
  event_id=qevent.event_id, refs=prepared.append_refs)``. Any exception from
  either is a ledger failure.
* Lane call, with a ledger wired: ``lane.prepare_thread_set(thread_key=,
  agent_id=str(agent_id), ledger_refs=, current=, identity=, handle=, routes=,
  deadline_epoch=, ledger_unavailable=)``, run off the event loop. ``routes``
  come from ``await self._binding.routes_for_agent(agent_id)`` (objects with
  ``kind``/``adapter``/``endpoint``). ``deadline_epoch`` is wall-clock now plus
  at most ``config.attachment_thread_prepare_timeout_seconds`` (and never past
  the turn's remaining budget).
* A file-carrying turn queries the ledger before it prepares, outside the route
  lock; a failed query refuses the turn before any claim with
  ``AttachmentResolutionError("ledger", ...)`` (logged as ``stage=ledger``).
  After the files are definitely installed it appends ``append_refs`` once; a
  failed append logs a WARNING naming the thread and the turn completes. A turn
  refused after preparing (route changed) appends nothing and discards the set.
* A text-only turn that will BOOT a sandbox (fresh claim, suspended resume,
  turn-budget or workspace handoff) queries the ledger and merges
  ``prepared.claim_env()`` into the claim env; a failed query still boots, with
  ``prepare_thread_set(ledger_refs=(), ledger_unavailable=True)``. A turn that
  adopts a live route, steers it, or continues a sweep queries nothing and
  prepares nothing.

Round 2 (#4141):

* Stale route: a text turn whose locked lookup still names a live route but
  whose sandbox is gone by the time it is claimed (``docker rm``, a pod no
  longer Running) boots a fresh sandbox, and that boot carries the thread set.
  A genuinely live adopt still carries nothing.
* Ledger rows carry ``event_id``; the kernel passes ``event_id=qevent.event_id``
  to ``prepare_thread_set``. A row with the same (event_id, file_id) is this
  message's file under its recorded name; the same file id on a later event is
  a new file with a new name, and its earlier row is an earlier file.
* An append answered 409 ``thread_attachment.name_mismatch`` logs a WARNING
  naming the code and the thread, and the turn completes.
* Mixed rollout: a query answered 404 (an API without the routes) means "no
  ledger": the file turn proceeds with its own files only and appends nothing,
  a text boot carries no attachment env, and a WARNING is logged. Any other
  query failure keeps the ADR behavior.
* A failed append counts ``curie.attachments.ledger.append`` with
  ``outcome=failure`` (``service.name=curie-worker``), declared in
  ``packages/telemetry``.
"""

from __future__ import annotations

import asyncio
import base64
import inspect
import json
import sys
import threading
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from aci_protocol import Attachment, Final, QueuedTurn, SessionStatus, TextDelta
from curie_worker.attachments import AttachmentCoordinator, AttachmentLimits
from curie_worker.behaviorpacks import BehaviorPacks
from curie_worker.kernel import routing
from curie_worker.kernel.core import Kernel
from curie_worker.ledger_client import ThreadAttachmentLedgerClient

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

from attachment_fixtures import FakeSlackFiles, RetainingObjectStore  # noqa: E402
from queue_fixtures import qevent  # noqa: E402
from queue_fixtures import wait_until as _wait_until  # noqa: E402

DONE = SessionStatus.DONE
REF_ENV = "CURIE_ATTACHMENTS_REF"
MANIFEST_ENV = "CURIE_ATTACHMENTS_MANIFEST"
MAX_TURNS_ENV = "CURIE_MAX_TURNS"
AGENT_UUID = uuid.UUID("33333333-3333-4333-8333-333333333333")
AGENT = str(AGENT_UUID)
CHANGED_FILE_REPLY = (
    "I could not add the file because the thread changed while I was fetching it. "
    "Please send the whole message again. "
    "The text of this message was not processed."
)


def _thread_key(thread: str) -> str:
    return f"slack:C1:{thread}"


def _event(
    text: str,
    *,
    thread: str,
    attachments: Sequence[Attachment] = (),
    event_id: str | None = None,
    placeholder: str = "p-1",
) -> QueuedTurn:
    return qevent(
        text,
        thread=thread,
        attachments=attachments,
        placeholder=placeholder,
        event_id=event_id or f"ev-{thread}-{uuid.uuid4().hex[:8]}",
    )


# --- collaborators ------------------------------------------------------------------


class _Binding:
    """A bound agent, so the ledger has an agent to key the thread on.

    ``max_turns`` changes the boot env between turns, which is how the turn
    budget fence (#3071) replaces an idle live route with a handoff.
    """

    def __init__(self) -> None:
        self.resolved = SimpleNamespace(
            agent_id=AGENT_UUID,
            agent_name="test-agent",
            deployment_id=None,
            workspace_enabled=False,
            endpoint=None,
            adapter=None,
        )
        self.max_turns: str | None = None
        self.routes_calls: list[uuid.UUID] = []

    async def resolve(self, _kind: str, _adapter: str | None, _channel: str) -> Any:
        return self.resolved

    def boot_env(self, _resolved: object, thread_key: str, **_: object) -> dict[str, str]:
        env = {
            "CURIE_SESSION_ID": f"session:{thread_key}",
            "CURIE_HISTORY_REF": f"history:{thread_key}",
            "CURIE_RUNNER_TOKEN": "rebuild-test-token",
        }
        if self.max_turns is not None:
            env[MAX_TURNS_ENV] = self.max_turns
        return env

    def packs_for(self, _resolved: object) -> BehaviorPacks:
        return BehaviorPacks()

    async def routes_for_agent(self, agent_id: uuid.UUID) -> list[Any]:
        self.routes_calls.append(agent_id)
        return [SimpleNamespace(kind="slack", adapter=None, endpoint=None)]


class _LedgerDown(RuntimeError):
    pass


class _FakeLedger:
    """The API ledger's contract: ordered, idempotent per (event_id, file_id)."""

    def __init__(self, harness: Any = None) -> None:
        self.rows: dict[tuple[str, str], list[tuple[str, Any]]] = {}
        self.query_calls: list[dict[str, Any]] = []
        self.append_calls: list[dict[str, Any]] = []
        self.fail_query = False
        self.fail_append = False
        self.harness = harness

    def seed(self, thread_key: str, refs: Sequence[Any], *, event_id: str = "ev-seed") -> None:
        self.rows.setdefault((AGENT, thread_key), []).extend((event_id, ref) for ref in refs)

    def refs(self, thread_key: str) -> list[Any]:
        return [ref for _event_id, ref in self.rows.get((AGENT, thread_key), [])]

    async def query(self, *, agent_id: str, thread_key: str) -> tuple[Any, ...]:
        self.query_calls.append({"agent_id": agent_id, "thread_key": thread_key})
        if self.fail_query:
            raise _LedgerDown("ledger read refused")
        # The API answers every row with the event that recorded it (#4141).
        return tuple(
            replace(ref, event_id=event_id)
            for event_id, ref in self.rows.get((agent_id, thread_key), [])
        )

    async def append(
        self, *, agent_id: str, thread_key: str, event_id: str, refs: Sequence[Any]
    ) -> int:
        self.append_calls.append(
            {
                "agent_id": agent_id,
                "thread_key": thread_key,
                "event_id": event_id,
                "file_ids": [ref.file_id for ref in refs],
                "claims_at_append": (
                    len(self.harness.fake_k8s.claim_envs) if self.harness is not None else None
                ),
            }
        )
        if self.fail_append:
            raise _LedgerDown("ledger append refused")
        held = self.rows.setdefault((agent_id, thread_key), [])
        seen = {(held_event, ref.file_id) for held_event, ref in held}
        added = 0
        for ref in refs:
            if (event_id, ref.file_id) in seen:
                continue
            held.append((event_id, ref))
            added += 1
        return added


@dataclass(frozen=True)
class _Ref:
    """What the fake lane records: the ledger ref shape, reduced."""

    file_id: str
    disk_name: str
    event_id: str | None = None


class _FakeThreadSet:
    def __init__(self, call: dict[str, Any]) -> None:
        current = list(call.get("current") or ())
        current_ids = {item.id for item in current}
        event_id = call.get("event_id")
        earlier = [
            ref
            for ref in call["ledger_refs"]
            if not (ref.event_id == event_id and ref.file_id in current_ids)
        ]
        self.names = [ref.disk_name for ref in earlier] + [item.name for item in current]
        self.append_refs = tuple(_Ref(item.id, item.name) for item in current)
        self.ledger_unavailable = bool(call.get("ledger_unavailable"))
        self.object_keys = tuple(f"attachments/{AGENT}/fake/{name}" for name in self.names)

    def claim_env(self) -> dict[str, str]:
        if not self.names and not self.ledger_unavailable:
            return {}
        env = {
            MANIFEST_ENV: json.dumps(
                {
                    "v": 1,
                    "files": [{"name": name, "current": False} for name in self.names],
                    "unavailable": [],
                    "omitted": [],
                    "ledger_unavailable": self.ledger_unavailable,
                }
            )
        }
        if self.names:
            env[REF_ENV] = "thread-set:" + ",".join(self.names)
        return env


class _FakeThreadLane:
    """The kernel-facing surface of ``AttachmentCoordinator`` with a ledger wired."""

    def __init__(
        self,
        *,
        entered: threading.Event | None = None,
        gate: threading.Event | None = None,
    ) -> None:
        self.prepare_calls: list[dict[str, Any]] = []
        self.prepared: list[_FakeThreadSet] = []
        self.discard_calls: list[dict[str, Any]] = []
        self._entered = entered
        self._gate = gate

    def prepare_thread_set(self, **kwargs: Any) -> _FakeThreadSet:
        self.prepare_calls.append(dict(kwargs))
        if self._entered is not None:
            self._entered.set()
        if self._gate is not None:
            assert self._gate.wait(timeout=5.0), "prepare gate timed out"
        prepared = _FakeThreadSet(kwargs)
        self.prepared.append(prepared)
        return prepared

    def resolve(self, **_kwargs: Any) -> Any:
        raise AssertionError(
            "with a ledger wired a file turn prepares the thread set, never a lone resolve"
        )

    def discard_prepared(self, *, thread_key: str, prepared: Any) -> None:
        self.discard_calls.append({"thread_key": thread_key, "prepared": prepared})

    def enumerate_expired(self) -> list[str]:
        return []

    def begin_expired_reap(self, _thread_key: str) -> object | None:
        return None

    def finish_expired_reap(self, _candidate: object) -> bool:
        return True


def _wire(h: Any, *, lane: Any | None = None) -> tuple[Any, _FakeLedger]:
    lane = lane if lane is not None else _FakeThreadLane()
    ledger = _FakeLedger(h)
    h.kernel._attachments = lane  # type: ignore[attr-defined]
    h.kernel._attachment_ledger = ledger  # type: ignore[attr-defined]
    return lane, ledger


def _real_lane(payloads: dict[str, list[bytes]]) -> tuple[AttachmentCoordinator, Any]:
    store = RetainingObjectStore()
    lane = AttachmentCoordinator(
        files=FakeSlackFiles(payloads),
        objects=store,
        limits=AttachmentLimits(max_file_bytes=1024, read_chunk_bytes=16),
    )
    return lane, store


def _ref_entries(env: dict[str, str]) -> list[dict[str, Any]]:
    value = env[REF_ENV]
    return json.loads(base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)))


def _claim_envs(h: Any) -> list[dict[str, str]]:
    return [dict(env or {}) for env in h.fake_k8s.claim_envs]


def _answer_everywhere(h: Any, text: str = "ok") -> None:
    h.runner.default_script = [Final(text=text, status=DONE)]
    for runner in h.runners.values():
        runner.default_script = [Final(text=text, status=DONE)]


# --- wiring ------------------------------------------------------------------------


def test_the_kernel_takes_the_ledger_as_an_optional_collaborator() -> None:
    assert "attachment_ledger" in inspect.signature(Kernel.__init__).parameters


def test_a_file_turn_prepares_the_thread_set_and_appends_after_install(make_harness) -> None:
    async def go() -> None:
        async with make_harness(binding=_Binding()) as h:
            lane, ledger = _wire(h)
            _answer_everywhere(h)
            event = _event(
                "read it",
                thread="tFileAppend",
                attachments=[Attachment(id="F1", name="a.txt", mime_type="text/plain")],
            )
            started = time.time()

            await h.kernel.process_event(event)

            thread_key = _thread_key("tFileAppend")
            assert ledger.query_calls == [{"agent_id": AGENT, "thread_key": thread_key}]
            (call,) = lane.prepare_calls
            assert call["thread_key"] == thread_key
            assert call["agent_id"] == AGENT
            assert [item.id for item in call["current"]] == ["F1"]
            assert call["event_id"] == event.event_id
            assert list(call["ledger_refs"]) == []
            assert call["ledger_unavailable"] is False
            assert call["deadline_epoch"] is not None
            assert (
                call["deadline_epoch"]
                <= time.time() + h.config.attachment_thread_prepare_timeout_seconds
            )
            assert call["deadline_epoch"] >= started
            assert _claim_envs(h)[-1][REF_ENV] == "thread-set:a.txt"
            assert ledger.append_calls == [
                {
                    "agent_id": AGENT,
                    "thread_key": thread_key,
                    "event_id": event.event_id,
                    "file_ids": ["F1"],
                    "claims_at_append": 1,
                }
            ], "the append follows the install, never precedes the claim"
            assert lane.discard_calls == []
            assert h.sink.last_text == "ok"

    asyncio.run(go())


# --- every boot rebuilds the whole set ------------------------------------------------


def test_a_second_file_turn_boots_the_earlier_file_and_the_new_one_under_fixed_names(
    make_harness,
) -> None:
    """Real coordinator: the earlier file is re-minted from its parked copy and
    keeps its name; the new upload with the same name never takes it."""

    async def go() -> None:
        async with make_harness(binding=_Binding(), per_sandbox_runners=2) as h:
            lane, store = _real_lane({"F1": [b"first-report"], "F2": [b"second-report"]})
            _lane, ledger = _wire(h, lane=lane)
            _answer_everywhere(h)
            thread = "tSecondFile"

            await h.kernel.process_event(
                _event(
                    "first",
                    thread=thread,
                    attachments=[Attachment(id="F1", name="report.pdf")],
                )
            )
            first_env = _claim_envs(h)[-1]
            assert [(e["n"], e["c"]) for e in _ref_entries(first_env)] == [("report.pdf", 1)]

            await h.kernel.process_event(
                _event(
                    "second",
                    thread=thread,
                    attachments=[Attachment(id="F2", name="report.pdf")],
                )
            )

            envs = _claim_envs(h)
            assert len(envs) == 2, "the second file turn boots a replacement sandbox"
            assert [(e["n"], e["c"]) for e in _ref_entries(envs[-1])] == [
                ("report.pdf", 0),
                ("report-2.pdf", 1),
            ]
            manifest = json.loads(envs[-1][MANIFEST_ENV])
            assert manifest["files"] == [
                {"name": "report.pdf", "current": False},
                {"name": "report-2.pdf", "current": True},
            ]
            assert [ref.disk_name for ref in ledger.refs(_thread_key(thread))] == [
                "report.pdf",
                "report-2.pdf",
            ]
            parked = [key for key in store.objects if key.startswith("attachments/")]
            assert len(parked) == 2, "the earlier file was re-minted, not fetched again"

    asyncio.run(go())


def test_a_text_follow_up_after_the_route_is_released_boots_the_whole_thread_set(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(binding=_Binding()) as h:
            lane, ledger = _wire(h)
            _answer_everywhere(h)
            thread_key = _thread_key("tReleased")
            ledger.seed(thread_key, [_Ref("F1", "a.txt"), _Ref("F2", "b.txt")])

            await h.kernel.process_event(_event("first", thread="tReleased"))
            await asyncio.to_thread(h.substrate.release, thread_key, wait_gone=True)
            await h.kernel.process_event(_event("again", thread="tReleased"))

            envs = _claim_envs(h)
            assert len(envs) == 2
            assert [env.get(REF_ENV) for env in envs] == ["thread-set:a.txt,b.txt"] * 2
            assert len(lane.prepare_calls) == 2
            for call in lane.prepare_calls:
                assert [ref.file_id for ref in call["ledger_refs"]] == ["F1", "F2"]
                assert list(call.get("current") or ()) == []
            assert ledger.append_calls == [], "a text turn records nothing"

    asyncio.run(go())


def test_a_turn_budget_handoff_boots_the_whole_thread_set(make_harness) -> None:
    async def go() -> None:
        binding = _Binding()
        async with make_harness(binding=binding, per_sandbox_runners=2) as h:
            lane, ledger = _wire(h)
            _answer_everywhere(h)
            thread_key = _thread_key("tBudget")
            ledger.seed(thread_key, [_Ref("F1", "a.txt")])

            await h.kernel.process_event(_event("first", thread="tBudget"))
            first = h.substrate.lookup(thread_key)
            binding.max_turns = "7"
            await h.kernel.process_event(_event("second", thread="tBudget"))

            second = h.substrate.lookup(thread_key)
            assert first is not None and second is not None
            assert second.claim_name != first.claim_name, "the turn budget fence replaced it"
            envs = _claim_envs(h)
            assert envs[-1][MAX_TURNS_ENV] == "7"
            assert envs[-1][REF_ENV] == "thread-set:a.txt"
            assert len(lane.prepare_calls) == 2

    asyncio.run(go())


def test_a_turn_budget_handoff_with_the_real_lane_remints_the_parked_files(
    make_harness,
) -> None:
    async def go() -> None:
        binding = _Binding()
        async with make_harness(binding=binding, per_sandbox_runners=2) as h:
            lane, _store = _real_lane({"F1": [b"report"]})
            _lane, ledger = _wire(h, lane=lane)
            _answer_everywhere(h)
            thread = "tBudgetReal"

            await h.kernel.process_event(
                _event("first", thread=thread, attachments=[Attachment(id="F1", name="r.pdf")])
            )
            binding.max_turns = "7"
            await h.kernel.process_event(_event("what did it say?", thread=thread))

            envs = _claim_envs(h)
            assert len(envs) == 2
            assert [(e["n"], e["c"]) for e in _ref_entries(envs[-1])] == [("r.pdf", 0)]
            assert len(ledger.append_calls) == 1

    asyncio.run(go())


def test_a_suspended_resume_boots_the_whole_thread_set(make_harness) -> None:
    async def go() -> None:
        async with make_harness(binding=_Binding(), per_sandbox_runners=2) as h:
            lane, ledger = _wire(h)
            _answer_everywhere(h)
            thread_key = _thread_key("tSuspended")
            ledger.seed(thread_key, [_Ref("F1", "a.txt")])

            await h.kernel.process_event(_event("first", thread="tSuspended"))
            await asyncio.to_thread(
                h.substrate.suspend, thread_key, history_ref=f"history:{thread_key}"
            )
            await h.kernel.process_event(_event("resume", thread="tSuspended"))

            envs = _claim_envs(h)
            assert len(envs) == 2, "the suspended thread was resumed on a new pod"
            assert envs[-1][REF_ENV] == "thread-set:a.txt"
            assert len(lane.prepare_calls) == 2

    asyncio.run(go())


def test_a_text_turn_on_a_thread_without_files_costs_one_ledger_read_and_no_env(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(binding=_Binding()) as h:
            _lane, ledger = _wire(h)
            _answer_everywhere(h)

            await h.kernel.process_event(_event("hello", thread="tNoFilesEver"))

            assert len(ledger.query_calls) == 1
            env = _claim_envs(h)[-1]
            assert REF_ENV not in env
            assert MANIFEST_ENV not in env

    asyncio.run(go())


# --- what never builds ------------------------------------------------------------------


def test_a_warm_adopt_reads_nothing_and_carries_no_attachment_env(make_harness) -> None:
    async def go() -> None:
        async with make_harness(binding=_Binding()) as h:
            lane, ledger = _wire(h)
            _answer_everywhere(h)
            thread_key = _thread_key("tAdopt")
            ledger.seed(thread_key, [_Ref("F1", "a.txt")])

            await h.kernel.process_event(_event("first", thread="tAdopt"))
            assert len(lane.prepare_calls) == 1
            await h.kernel.process_event(_event("second", thread="tAdopt"))

            assert len(_claim_envs(h)) == 1, "the follow-up adopted the live sandbox"
            assert len(lane.prepare_calls) == 1
            assert len(ledger.query_calls) == 1
            assert h.runner.opened == ["first", "second"]

    asyncio.run(go())


def test_a_steer_reads_nothing_and_prepares_nothing(make_harness) -> None:
    async def go() -> None:
        async with make_harness(binding=_Binding()) as h:
            lane, ledger = _wire(h)
            thread_key = _thread_key("tSteer")
            ledger.seed(thread_key, [_Ref("F1", "a.txt")])
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=DONE)]

            first = asyncio.create_task(h.kernel.process_event(_event("first", thread="tSteer")))
            await _wait_until(lambda: h.runner.turn_active)
            await h.kernel.process_event(_event("steer this", thread="tSteer"))
            hold.set()
            await first

            assert h.runner.steers == ["steer this"]
            assert len(lane.prepare_calls) == 1
            assert len(ledger.query_calls) == 1

    asyncio.run(go())


# --- refusals, redelivery, and failures -------------------------------------------------


def test_a_route_changed_refusal_appends_nothing_and_discards_the_set(make_harness) -> None:
    async def go() -> None:
        async with make_harness(binding=_Binding(), per_sandbox_runners=2) as h:
            entered, gate = threading.Event(), threading.Event()
            lane, ledger = _wire(h, lane=_FakeThreadLane(entered=entered, gate=gate))
            _answer_everywhere(h)
            event = _event(
                "read this",
                thread="tChanged",
                placeholder="p-file",
                attachments=[Attachment(id="F4", name="changed.txt")],
            )
            task = asyncio.create_task(h.kernel.process_event(event))
            try:
                await _wait_until(entered.is_set, "the thread set to start preparing")
                await asyncio.to_thread(
                    h.substrate.claim,
                    _thread_key("tChanged"),
                    env={"EXTERNAL_REPLACEMENT": "1"},
                )
            finally:
                gate.set()
            await task

            assert ledger.append_calls == []
            assert lane.discard_calls == [
                {"thread_key": _thread_key("tChanged"), "prepared": lane.prepared[0]}
            ]
            updates = [text for _channel, ref, text in h.sink.updates if ref == "p-file"]
            assert updates[-1] == CHANGED_FILE_REPLY

    asyncio.run(go())


def test_a_redelivered_file_turn_is_recorded_once_under_one_name(make_harness) -> None:
    """Real coordinator: the retried delivery finds its own file in the ledger
    and boots it once, as this message's file, under the recorded name."""

    async def go() -> None:
        async with make_harness(binding=_Binding(), per_sandbox_runners=2) as h:
            lane, _store = _real_lane({"F1": [b"payload"]})
            _lane, ledger = _wire(h, lane=lane)
            _answer_everywhere(h)
            event = _event(
                "read it",
                thread="tRedeliver",
                event_id="redelivered-file",
                attachments=[Attachment(id="F1", name="a.txt")],
            )

            await h.kernel.process_event(event)
            await h.async_redis.delete(h.config.done_key(event.event_id))
            await h.kernel.process_event(event)

            envs = _claim_envs(h)
            assert len(envs) == 2, "the redelivery booted again"
            assert [(e["n"], e["c"]) for e in _ref_entries(envs[-1])] == [("a.txt", 1)]
            assert [ref.disk_name for ref in ledger.refs(_thread_key("tRedeliver"))] == [
                "a.txt"
            ]
            assert {call["event_id"] for call in ledger.append_calls} == {"redelivered-file"}

    asyncio.run(go())


def test_an_append_failure_after_install_is_logged_and_the_turn_completes(
    make_harness, caplog
) -> None:
    async def go() -> None:
        async with make_harness(binding=_Binding()) as h:
            _lane, ledger = _wire(h)
            ledger.fail_append = True
            _answer_everywhere(h, "read it")
            event = _event(
                "read this",
                thread="tAppendFails",
                attachments=[Attachment(id="F1", name="a.txt")],
            )

            with caplog.at_level("WARNING", logger="curie_worker.kernel"):
                await h.kernel.process_event(event)

            assert len(ledger.append_calls) == 1
            assert h.sink.last_text == "read it"
            assert await h.async_redis.exists(h.config.done_key(event.event_id))
            warnings = [
                record
                for record in caplog.records
                if record.levelname == "WARNING"
                and "ledger" in record.getMessage()
                and _thread_key("tAppendFails") in record.getMessage()
            ]
            assert warnings, "a failed append must be visible in the log"

    asyncio.run(go())


def test_a_failed_ledger_read_on_a_text_turn_boots_and_says_so(make_harness) -> None:
    async def go() -> None:
        async with make_harness(binding=_Binding()) as h:
            lane, ledger = _wire(h)
            ledger.fail_query = True
            _answer_everywhere(h, "answered")

            await h.kernel.process_event(_event("hello", thread="tLedgerDownText"))

            (call,) = lane.prepare_calls
            assert call["ledger_unavailable"] is True
            assert list(call["ledger_refs"]) == []
            env = _claim_envs(h)[-1]
            assert REF_ENV not in env
            assert json.loads(env[MANIFEST_ENV])["ledger_unavailable"] is True
            assert h.sink.last_text == "answered"

    asyncio.run(go())


def test_a_failed_ledger_read_refuses_a_file_turn_before_any_claim(
    make_harness, caplog
) -> None:
    async def go() -> None:
        async with make_harness(binding=_Binding(), max_attempts=3) as h:
            lane, ledger = _wire(h)
            ledger.fail_query = True
            _answer_everywhere(h)
            event = _event(
                "read this",
                thread="tLedgerDownFile",
                attachments=[Attachment(id="F1", name="a.txt")],
            )

            with caplog.at_level("WARNING", logger="curie_worker.kernel"):
                await h.kernel.process_event(event)

            assert len(ledger.query_calls) == 1
            assert lane.prepare_calls == []
            assert h.fake_k8s.claim_envs == []
            assert h.runner.opened == []
            assert h.sink.last_text is not None
            assert "file" in h.sink.last_text.lower()
            assert "stage=ledger" in caplog.text
            assert await h.async_redis.exists(h.config.done_key(event.event_id))
            completions = [c for c in h.sink.completions if c.event_id == event.event_id]
            assert len(completions) == 1
            assert completions[0].outcome == "delivered"

    asyncio.run(go())


# --- a sweep continuation never builds ------------------------------------------------


def test_a_sweep_continuation_reads_nothing_and_prepares_nothing(
    make_hook_run, make_harness
) -> None:
    """ADR-0160: a continuation adopts the claim it holds and never boots."""

    from sweep_fixtures import (
        cron_event,
        enqueue,
        read_next,
        run_slice_to_budget_cut,
        sweep_case,
    )

    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            h = c.h
            first = await enqueue(h, cron_event(c.run))

            async def save() -> None:
                c.checkpoint(covered=("slack",), uncovered=("github", "notes"))

            await run_slice_to_budget_cut(h, first, at_cut=save)
            lane, ledger = _wire(h)
            ledger.seed(routing._thread_key_for(first.event), [_Ref("F1", "a.txt")])
            c.checkpoint(covered=("slack", "github", "notes"), uncovered=())
            h.runner.hold = None
            h.runner.tail = []
            h.runner.turn_scripts = [[Final(text="done", status=DONE)]]
            second = await read_next(h)
            assert second is not None and ":sweep:" in second.event.event_id

            await h.kernel.process_event(second.event, lease=second.lease)

            assert len(h.fake_k8s.claim_envs) == 1, "the continuation created a claim"
            assert ledger.query_calls == []
            assert lane.prepare_calls == []

    asyncio.run(go())


# --- round 2 (#4141) -------------------------------------------------------------------


def _kill_after_next_lookup(
    h: Any, monkeypatch: pytest.MonkeyPatch, thread_key: str, shape: str
) -> list[int]:
    """Let the kernel's next lookup see the live route, then take the sandbox away.

    ``docker``: the container is removed, ``get_sandbox`` answers None, as
    ``DockerSandboxClient`` does for a removed runner. ``k8s``: the sandbox is
    no longer Running. Either way the route is still recorded when the kernel
    decided, and the claim that follows has to cold-create.
    """

    fired: list[int] = []
    hidden: set[str] = set()
    real_lookup = h.substrate.lookup
    real_get_sandbox = h.fake_k8s.get_sandbox

    def get_sandbox(name: str, *, request_timeout_seconds: float) -> Any:
        if name in hidden:
            return None
        return real_get_sandbox(name, request_timeout_seconds=request_timeout_seconds)

    def lookup(key: str) -> Any:
        handle = real_lookup(key)
        if key == thread_key and handle is not None and not fired:
            fired.append(1)
            if shape == "docker":
                hidden.add(handle.sandbox_name)
            else:
                h.fake_k8s.set_sandbox_mode(handle.sandbox_name, "Suspended")
        return handle

    monkeypatch.setattr(h.fake_k8s, "get_sandbox", get_sandbox)
    monkeypatch.setattr(h.substrate, "lookup", lookup)
    return fired


@pytest.mark.parametrize("shape", ["docker", "k8s"])
def test_a_dead_sandbox_behind_a_live_route_boots_with_the_whole_thread_set(
    make_harness, monkeypatch, shape: str
) -> None:
    async def go() -> None:
        async with make_harness(binding=_Binding(), per_sandbox_runners=2) as h:
            lane, ledger = _wire(h)
            _answer_everywhere(h)
            thread = f"tStale{shape}"
            thread_key = _thread_key(thread)
            ledger.seed(thread_key, [_Ref("F1", "a.txt"), _Ref("F2", "b.txt")])

            await h.kernel.process_event(_event("first", thread=thread))
            await h.kernel.process_event(_event("second", thread=thread))
            assert len(_claim_envs(h)) == 1, "a genuinely live route is adopted"
            assert len(lane.prepare_calls) == 1, "and the adopt carries nothing"
            old = h.substrate.lookup(thread_key)
            assert old is not None

            fired = _kill_after_next_lookup(h, monkeypatch, thread_key, shape)
            await h.kernel.process_event(_event("third", thread=thread))

            assert fired, "the kernel never looked the route up"
            envs = _claim_envs(h)
            assert len(envs) == 2, "the dead sandbox was replaced by a fresh claim"
            assert envs[-1].get(REF_ENV) == "thread-set:a.txt,b.txt", (
                "the fresh sandbox booted without the thread's earlier files"
            )
            new = h.substrate.lookup(thread_key)
            assert new is not None and new.claim_name != old.claim_name
            assert h.runners[h.fake_k8s.assigned_ports[new.sandbox_name]].opened == ["third"]

    asyncio.run(go())


def test_the_same_file_on_a_later_message_is_a_new_file_with_a_new_name(
    make_harness,
) -> None:
    """Real coordinator: only (event_id, file_id) identifies a redelivery."""

    async def go() -> None:
        async with make_harness(binding=_Binding(), per_sandbox_runners=2) as h:
            lane, _store = _real_lane({"F1": [b"report"]})
            _lane, ledger = _wire(h, lane=lane)
            _answer_everywhere(h)
            thread = "tReattached"
            for event_id in ("first-message", "later-message"):
                await h.kernel.process_event(
                    _event(
                        "look",
                        thread=thread,
                        event_id=event_id,
                        attachments=[Attachment(id="F1", name="report.pdf")],
                    )
                )

            envs = _claim_envs(h)
            assert [(e["n"], e["c"]) for e in _ref_entries(envs[-1])] == [
                ("report.pdf", 0),
                ("report-2.pdf", 1),
            ]
            held = ledger.rows[(AGENT, _thread_key(thread))]
            assert [(event_id, ref.disk_name) for event_id, ref in held] == [
                ("first-message", "report.pdf"),
                ("later-message", "report-2.pdf"),
            ]

    asyncio.run(go())


class _Api:
    """The API's two internal routes, answered over ``httpx.MockTransport``."""

    def __init__(
        self,
        *,
        query_status: int = 200,
        append_status: int = 200,
        append_body: dict[str, Any] | None = None,
    ) -> None:
        self.query_status = query_status
        self.append_status = append_status
        self.append_body = append_body
        self.queries: list[dict[str, Any]] = []
        self.appends: list[dict[str, Any]] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        if request.url.path.endswith("/query"):
            self.queries.append(body)
            if self.query_status != 200:
                return httpx.Response(self.query_status, json={"detail": "Not Found"})
            return httpx.Response(200, json={"refs": []})
        self.appends.append(body)
        if self.append_status != 200:
            return httpx.Response(self.append_status, json=self.append_body or {})
        return httpx.Response(200, json={"appended": len(body["refs"])})

    def client(self) -> tuple[ThreadAttachmentLedgerClient, httpx.AsyncClient]:
        http = httpx.AsyncClient(transport=httpx.MockTransport(self.handler))
        return (
            ThreadAttachmentLedgerClient(
                api_base_url="http://curie-api:8000",
                worker_token="worker-token",
                client=http,
                retry_backoff_s=0.0,
            ),
            http,
        )


def test_a_name_mismatch_on_append_is_logged_and_the_turn_completes(
    make_harness, caplog
) -> None:
    async def go() -> None:
        api = _Api(
            append_status=409,
            append_body={
                "detail": {
                    "code": "thread_attachment.name_mismatch",
                    "message": "disk_name differs from the recorded one",
                }
            },
        )
        client, http = api.client()
        async with http, make_harness(binding=_Binding()) as h:
            lane, _store = _real_lane({"F1": [b"bytes"]})
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            h.kernel._attachment_ledger = client  # type: ignore[attr-defined]
            _answer_everywhere(h, "read it")
            event = _event(
                "read", thread="tNameMismatch", attachments=[Attachment(id="F1", name="a.txt")]
            )

            with caplog.at_level("WARNING", logger="curie_worker"):
                await h.kernel.process_event(event)

            assert len(api.appends) == 1
            assert h.sink.last_text == "read it"
            assert await h.async_redis.exists(h.config.done_key(event.event_id))
            warnings = [
                record.getMessage()
                for record in caplog.records
                if record.levelname == "WARNING" and "name_mismatch" in record.getMessage()
            ]
            assert warnings and _thread_key("tNameMismatch") in warnings[0]

    asyncio.run(go())


def test_an_api_without_the_ledger_routes_leaves_a_file_turn_as_it_was(
    make_harness, caplog
) -> None:
    """Mixed rollout: a 404 is "no ledger", never a refusal of the person's file."""

    async def go() -> None:
        api = _Api(query_status=404)
        client, http = api.client()
        async with http, make_harness(binding=_Binding()) as h:
            lane, _store = _real_lane({"F1": [b"bytes"]})
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            h.kernel._attachment_ledger = client  # type: ignore[attr-defined]
            _answer_everywhere(h, "read it")
            event = _event(
                "read", thread="tNoRoutesFile", attachments=[Attachment(id="F1", name="a.txt")]
            )

            with caplog.at_level("WARNING", logger="curie_worker"):
                await h.kernel.process_event(event)

            envs = _claim_envs(h)
            assert len(envs) == 1, "the file turn was refused"
            assert [entry["n"] for entry in _ref_entries(envs[0])] == ["a.txt"]
            assert MANIFEST_ENV not in envs[0] or not json.loads(envs[0][MANIFEST_ENV]).get(
                "ledger_unavailable"
            )
            assert api.appends == [], "with no ledger there is nothing to append to"
            assert h.sink.last_text == "read it"
            assert any(
                record.levelname == "WARNING" and "ledger" in record.getMessage().lower()
                for record in caplog.records
            )

    asyncio.run(go())


def test_an_api_without_the_ledger_routes_boots_a_text_turn_without_attachment_env(
    make_harness,
) -> None:
    async def go() -> None:
        api = _Api(query_status=404)
        client, http = api.client()
        async with http, make_harness(binding=_Binding()) as h:
            lane, _store = _real_lane({})
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            h.kernel._attachment_ledger = client  # type: ignore[attr-defined]
            _answer_everywhere(h)

            await h.kernel.process_event(_event("hello", thread="tNoRoutesText"))

            env = _claim_envs(h)[-1]
            assert REF_ENV not in env
            assert MANIFEST_ENV not in env, "a missing route is not an unavailable ledger"

    asyncio.run(go())


def test_any_other_ledger_failure_still_refuses_a_file_turn(make_harness, caplog) -> None:
    async def go() -> None:
        api = _Api(query_status=503)
        client, http = api.client()
        async with http, make_harness(binding=_Binding()) as h:
            lane, _store = _real_lane({"F1": [b"bytes"]})
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            h.kernel._attachment_ledger = client  # type: ignore[attr-defined]
            _answer_everywhere(h)
            event = _event(
                "read", thread="tLedger503", attachments=[Attachment(id="F1", name="a.txt")]
            )

            with caplog.at_level("WARNING", logger="curie_worker.kernel"):
                await h.kernel.process_event(event)

            assert h.fake_k8s.claim_envs == []
            assert "stage=ledger" in caplog.text

    asyncio.run(go())


def _record_metrics(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict[str, str]]]:
    """Wrap ``record_metric`` everywhere the kernel can reach it; the real
    declaration check still runs on every point."""

    import curie_telemetry
    from curie_worker.kernel import attachments as kernel_attachments
    from curie_worker.kernel import claim as kernel_claim
    from curie_worker.kernel import log as kernel_log

    points: list[tuple[str, dict[str, str]]] = []
    real = curie_telemetry.record_metric

    def recording(
        name: str, value: float = 1, *, attributes: dict[str, str] | None = None
    ) -> None:
        points.append((name, dict(attributes or {})))
        real(name, value, attributes=attributes)

    for module in (curie_telemetry, kernel_log, kernel_attachments, kernel_claim):
        if hasattr(module, "record_metric"):
            monkeypatch.setattr(module, "record_metric", recording)
    return points


def test_a_failed_append_is_counted(make_harness, monkeypatch) -> None:
    async def go() -> None:
        points = _record_metrics(monkeypatch)
        async with make_harness(binding=_Binding()) as h:
            _lane, ledger = _wire(h)
            ledger.fail_append = True
            _answer_everywhere(h)

            await h.kernel.process_event(
                _event("read", thread="tCounted", attachments=[Attachment(id="F1", name="a")])
            )

            appended = [
                attrs for name, attrs in points if name == "curie.attachments.ledger.append"
            ]
            assert appended == [{"service.name": "curie-worker", "outcome": "failure"}]

    asyncio.run(go())
