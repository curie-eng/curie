"""#5354: a transcript append that never reached the state API is retried.

A connect failure (``aiohttp.ClientConnectorError``) proves the request never
reached the API, so nothing was written and a later attempt cannot duplicate
the record. The runner keeps such a record, retries it in order once the API is
back, and only then reports ``history_durable``. An append the API refused with
a 4xx (here a 400) stays a sticky loss for the process: the request was
understood and refused, so a retry cannot succeed. A 5xx or a cut connection is
retried too since #5366 (``test_history_refused_append_5366``).

The state API here is the shared fake key (``runner_state_fake.CappedCasState``,
which replays ``get_state`` and ``append_state`` in
apps/api/src/curie_api/routers/state.py) served on a fixed loopback port. The
outage is real: the server is stopped, so the store's own aiohttp client gets a
real connection refusal, and recovery starts the server on that same port.
"""

from __future__ import annotations

import asyncio
import logging
import socket

import anyio
import pytest
from aci_protocol import Event, Final, SessionStatus, parse_ndjson
from aiohttp import web
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner import session as session_module
from curie_runner.fake import FakeModelSession, default_turn
from curie_runner.history import HistoryRecord, StateApiTranscriptStore
from curie_runner.session import SessionRunner
from runner_state_fake import TRANSCRIPT_KEY, CappedCasState

_SESSION_ID = "session-outage-PLACEHOLDER"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class _StateApi:
    """The fake transcript key, served on one port that can go down and come back."""

    def __init__(self) -> None:
        self.state = CappedCasState()
        self.port = _free_port()
        self._runner: web.AppRunner | None = None

    @property
    def key_url(self) -> str:
        return f"http://127.0.0.1:{self.port}{TRANSCRIPT_KEY}"

    async def start(self) -> None:
        assert self._runner is None
        runner = web.AppRunner(self.state.app())
        await runner.setup()
        await web.TCPSite(runner, "127.0.0.1", self.port, reuse_address=True).start()
        self._runner = runner

    async def stop(self) -> None:
        if self._runner is not None:
            runner, self._runner = self._runner, None
            await runner.cleanup()

    def posted_users(self) -> list[str]:
        """The ``user`` of every append the API received, refused or not."""
        return [body["item"]["user"] for method, body, _ in self.state.requests if method == "POST"]

    def stored_users(self) -> list[str]:
        return [item["user"] for item in self.state.value or []]


class _AttemptLog(StateApiTranscriptStore):
    """The real store, noting which record each append call carried."""

    def __init__(self, key_url: str) -> None:
        super().__init__(key_url, token=None)
        self.attempted: list[str] = []
        self._waiters: list[tuple[int, anyio.Event]] = []

    async def append(self, record: HistoryRecord) -> bool:
        self.attempted.append(getattr(record, "user", type(record).__name__))
        for count, reached in self._waiters:
            if len(self.attempted) >= count:
                reached.set()
        return await super().append(record)

    async def attempts_reach(self, count: int, *, timeout: float = 5.0) -> None:
        """Wait, bounded, until ``count`` append calls have been made."""
        if len(self.attempted) >= count:
            return
        reached = anyio.Event()
        self._waiters.append((count, reached))
        try:
            with anyio.fail_after(timeout):
                await reached.wait()
        except TimeoutError:
            raise AssertionError(
                f"only {len(self.attempted)} append attempts after {timeout}s, wanted {count}"
            ) from None


def _fast_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    # raising=False keeps the fix pin honest on a source without the retry: the
    # tests then fail on their assertions, not on a missing attribute.
    monkeypatch.setattr(session_module, "_HISTORY_RETRY_INITIAL_S", 0.01, raising=False)
    monkeypatch.setattr(session_module, "_HISTORY_RETRY_MAX_S", 0.05, raising=False)


class _HeldStore(StateApiTranscriptStore):
    """The real store, holding one chosen record's next append until released.

    The hold sits where the request would start, so while it is held the
    runner sees that append as in flight and nothing has reached the API.
    """

    def __init__(self, key_url: str) -> None:
        super().__init__(key_url, token=None)
        self.hold: str | None = None
        self.held = anyio.Event()
        self.release = anyio.Event()

    async def append(self, record: HistoryRecord) -> bool:
        if self.hold is not None and getattr(record, "user", None) == self.hold:
            self.hold = None
            self.held.set()
            await self.release.wait()
        return await super().append(record)

    async def wait_held(self, *, timeout: float = 5.0) -> None:
        with anyio.fail_after(timeout):
            await self.held.wait()


def _max_turns_turn() -> list:
    """A turn that ends in a max-turns failure, which the transcript never records."""
    return [
        AssistantMessage(content=[TextBlock(text="still probing")], model="stub-model"),
        ResultMessage(
            subtype="error_max_turns",
            duration_ms=1,
            duration_api_ms=1,
            is_error=True,
            num_turns=20,
            session_id="sdk-session",
            result=None,
        ),
    ]


def _runner(store: StateApiTranscriptStore, script_factory=default_turn) -> SessionRunner:
    fake = FakeModelSession(script_factory)
    return SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=lambda: fake,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="history-outage-test",
        session_id=_SESSION_ID,
        history_store=store,
    )


async def _turn(runner: SessionRunner, text: str) -> SessionStatus:
    lines = [
        line async for line in runner.run_turn(Event(type="message", text=text, user="U", ts="1"))
    ]
    finals = [event for event in parse_ndjson("".join(lines)) if isinstance(event, Final)]
    assert len(finals) == 1
    return finals[0].status


async def _retry_settled(runner: SessionRunner, *, timeout: float = 5.0) -> None:
    """Wait, bounded, for the runner's background append retry to finish.

    Read with getattr so a source without the retry fails on the product
    assertions that follow, not here. Once the task is done, no append can be
    sent until another turn runs, so an absence asserted after this is exact.
    """
    task = getattr(runner, "_history_retry_task", None)
    if task is not None:
        await asyncio.wait_for(asyncio.shield(task), timeout=timeout)


async def _booted_store(api: _StateApi, store: StateApiTranscriptStore) -> None:
    """Boot reads the (empty) transcript while the API is up, as the runner does."""
    await api.start()
    assert await store.load() == []


def test_a_connect_failed_append_lands_after_the_api_returns_without_a_new_turn(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _fast_retry(monkeypatch)
    api = _StateApi()

    async def go() -> None:
        store = _AttemptLog(api.key_url)
        await _booted_store(api, store)
        await api.stop()
        runner = _runner(store)
        await runner.start()
        try:
            with caplog.at_level(logging.WARNING, logger="curie_runner.session"):
                assert await _turn(runner, "first question") is SessionStatus.DONE
            assert runner.history_durable is False
            # The turn's own attempt and two retries fail while the API is down.
            await store.attempts_reach(3)
            assert runner.history_durable is False
            assert api.posted_users() == []

            await api.start()
            await _retry_settled(runner)
            # The retry is done, so no duplicate can follow the one that landed.
            assert runner.history_durable is True
            assert api.posted_users() == ["first question"]
            assert api.stored_users() == ["first question"]
            assert runner.history_durable is True
        finally:
            await runner.close()
            await api.stop()

    anyio.run(go)
    warnings = [
        record.getMessage() for record in caplog.records if record.levelno == logging.WARNING
    ]
    assert any(
        "history append" in message
        and _SESSION_ID in message
        and "error_class=ClientConnectorError" in message
        for message in warnings
    ), warnings


def test_records_deferred_by_an_outage_land_in_turn_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fast_retry(monkeypatch)
    api = _StateApi()

    async def go() -> None:
        store = _AttemptLog(api.key_url)
        await _booted_store(api, store)
        await api.stop()
        runner = _runner(store)
        await runner.start()
        try:
            assert await _turn(runner, "first question") is SessionStatus.DONE
            assert await _turn(runner, "second question") is SessionStatus.DONE
            assert runner.history_durable is False
            attempted_while_down = list(store.attempted)
            # The later record never went ahead of the earlier deferred one.
            assert "second question" not in attempted_while_down
            assert attempted_while_down[0] == "first question"

            await api.start()
            await _retry_settled(runner)
            assert runner.history_durable is True
            assert api.posted_users() == ["first question", "second question"]
            assert api.stored_users() == ["first question", "second question"]
        finally:
            await runner.close()
            await api.stop()

    anyio.run(go)


def test_an_append_the_api_answered_with_400_stays_a_sticky_loss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fast_retry(monkeypatch)
    api = _StateApi()

    async def go() -> None:
        store = StateApiTranscriptStore(api.key_url, token=None)
        await _booted_store(api, store)
        runner = _runner(store)
        await runner.start()
        try:
            api.state.force_append_status = 400
            assert await _turn(runner, "first question") is SessionStatus.DONE
            assert api.posted_users() == ["first question"]
            assert runner.history_durable is False
            api.state.force_append_status = None

            # The API refused the request with a 4xx, so it is never sent again.
            await _retry_settled(runner)
            assert api.posted_users() == ["first question"]

            assert await _turn(runner, "second question") is SessionStatus.DONE
            await _retry_settled(runner)
            assert api.posted_users() == ["first question", "second question"]
            assert api.stored_users() == ["second question"]
            # The missing first turn is not repaired by a later success.
            assert runner.history_durable is False
        finally:
            await runner.close()
            await api.stop()

    anyio.run(go)


def test_a_deferred_record_refused_with_400_on_retry_becomes_a_real_loss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fast_retry(monkeypatch)
    api = _StateApi()

    async def go() -> None:
        store = StateApiTranscriptStore(api.key_url, token=None)
        await _booted_store(api, store)
        await api.stop()
        runner = _runner(store)
        await runner.start()
        try:
            assert await _turn(runner, "first question") is SessionStatus.DONE
            assert runner.history_durable is False

            # The API comes back but refuses the retried append with a 4xx.
            api.state.force_append_status = 400
            await api.start()
            await _retry_settled(runner)
            # One refusal ends the retry: exactly one attempt reached the API.
            assert api.posted_users() == ["first question"]
            assert api.stored_users() == []
            assert runner.history_durable is False

            api.state.force_append_status = None
            assert await _turn(runner, "second question") is SessionStatus.DONE
            await _retry_settled(runner)
            assert api.posted_users() == ["first question", "second question"]
            assert api.stored_users() == ["second question"]
            assert runner.history_durable is False
        finally:
            await runner.close()
            await api.stop()

    anyio.run(go)


def test_close_during_an_outage_stops_the_retry_promptly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fast_retry(monkeypatch)
    api = _StateApi()

    async def go() -> None:
        store = _AttemptLog(api.key_url)
        await _booted_store(api, store)
        await api.stop()
        runner = _runner(store)
        await runner.start()
        assert await _turn(runner, "first question") is SessionStatus.DONE
        # The retry is really running: the turn's own attempt plus two more.
        await store.attempts_reach(3)
        task = getattr(runner, "_history_retry_task", None)

        with anyio.fail_after(1.0):
            await runner.close()
        # close() returned with the retry task finished, so nothing is left to
        # send another attempt.
        assert task is not None and task.done()
        attempts = len(store.attempted)
        for _ in range(10):
            await anyio.sleep(0)
        assert len(store.attempted) == attempts
        assert runner.history_durable is False

    anyio.run(go)


def test_an_unrecorded_turn_during_an_in_flight_retry_keeps_the_fence_shut(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A deferred record whose retry is in flight is still not durable.

    A later turn that ends without recording a terminal (a max-turns failure)
    settles the fence from what is deferred; the in-flight record must count.
    """
    _fast_retry(monkeypatch)
    api = _StateApi()
    scripts = iter((default_turn(), _max_turns_turn()))

    async def go() -> None:
        store = _HeldStore(api.key_url)
        await _booted_store(api, store)
        await api.stop()
        runner = _runner(store, script_factory=lambda: next(scripts))
        await runner.start()
        try:
            assert await _turn(runner, "first question") is SessionStatus.DONE
            store.hold = "first question"
            await store.wait_held()

            await _turn(runner, "probe until done")
            assert runner.history_durable is False

            await api.start()
            store.release.set()
            await _retry_settled(runner)
            assert api.posted_users() == ["first question"]
            assert runner.history_durable is True
        finally:
            store.release.set()
            await runner.close()
            await api.stop()

    anyio.run(go)


def test_a_turn_cut_off_while_waiting_to_append_is_deferred_not_lost(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The persistence budget can expire before this turn's request starts.

    Here the retry holds the append lock with an earlier record in flight, so
    the second turn's append waits and its budget cancels it. Nothing was sent,
    so the record is kept and lands after the earlier one.
    """
    _fast_retry(monkeypatch)
    api = _StateApi()

    async def go() -> None:
        store = _HeldStore(api.key_url)
        await _booted_store(api, store)
        await api.stop()
        runner = _runner(store)
        await runner.start()
        try:
            assert await _turn(runner, "first question") is SessionStatus.DONE
            store.hold = "first question"
            await store.wait_held()

            monkeypatch.setattr(session_module, "_HISTORY_PERSISTENCE_BUDGET_SECONDS", 0.05)
            assert await _turn(runner, "second question") is SessionStatus.DONE
            assert runner.history_durable is False
            monkeypatch.setattr(session_module, "_HISTORY_PERSISTENCE_BUDGET_SECONDS", 15.0)

            await api.start()
            store.release.set()
            await _retry_settled(runner)
            assert api.posted_users() == ["first question", "second question"]
            assert api.stored_users() == ["first question", "second question"]
            assert runner.history_durable is True
        finally:
            store.release.set()
            await runner.close()
            await api.stop()

    anyio.run(go)
