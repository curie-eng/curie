"""#5366: a transcript append whose outcome is unknown is retried, never duplicated.

#5354 retried only an append that never connected. An append the API answered
with a 5xx, or whose connection was cut after the request was sent, may already
have been written, and without an idempotency key a retry could store the turn
twice, so it stayed a sticky loss: ``history_durable`` never came back and the
worker fenced every later turn of the thread.

Each turn record now carries a ``record_id`` minted once per turn, and the state
API returns the stored entry unchanged when that id is already stored. A retry
is safe, so the runner defers such a record on the #5354 retry path and it lands
exactly once when the API recovers. A 4xx refusal (here a 400) is unchanged: the
request was understood and refused, a retry cannot succeed, and it stays a
sticky loss.

The state API is a small fake on a loopback port that replays ``get_state`` and
``append_state`` in apps/api/src/curie_api/routers/state.py, including the
replay rule in ``transcripts.append`` (apps/api/src/curie_api/transcripts.py).
The runner's real ``StateApiTranscriptStore`` talks to it over real HTTP.
"""

from __future__ import annotations

import asyncio
import socket
from typing import Any

import anyio
import pytest
from aci_protocol import Event, Final, SessionStatus, parse_ndjson
from aiohttp import web
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner import session as session_module
from curie_runner.fake import FakeModelSession
from curie_runner.history import HarnessReplayState, StateApiTranscriptStore, TurnRecord
from curie_runner.session import SessionRunner

_SESSION_ID = "session-refused-PLACEHOLDER"
_KEY_PATH = "/agents/A/state/transcript/t-5366"
_CAP_HEADERS = {"X-Curie-Transcript-Max-Bytes": "65536"}

_HEALTHY = "healthy"
# Answers 500 and writes nothing: the API with its Postgres gone.
_REFUSE_500 = "refuse-500"
# Writes the item, then answers 500: the commit landed, the response failed.
_COMMIT_THEN_500 = "commit-then-500"
# Writes the item, then closes the connection with no response.
_COMMIT_THEN_CUT = "commit-then-cut"
# Answers 400 and writes nothing: a request the API understood and refused.
_REFUSE_400 = "refuse-400"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class _ReplayingStateApi:
    """One transcript key on a fake state API with the #5366 replay rule.

    Replays ``get_state`` and ``append_state`` (apps/api/src/curie_api/routers/
    state.py) and ``transcripts.append`` (apps/api/src/curie_api/transcripts.py):
    every read advertises the transcript cap header, an unwritten key answers
    404, and a successful append answers the ``StateEntryOut`` body with the
    version bumped. An item whose nonempty ``record_id`` is already stored is
    not appended again; the stored entry is returned with 200 and an unchanged
    version.

    ``mode`` applies to every request until it is set back to healthy, and
    ``posted`` records each append's item as received, whatever the answer.
    """

    def __init__(self) -> None:
        self.port = _free_port()
        self.items: list[dict[str, Any]] = []
        self.version = 0
        self.posted: list[dict[str, Any]] = []
        self.mode = _HEALTHY
        self._runner: web.AppRunner | None = None

    @property
    def key_url(self) -> str:
        return f"http://127.0.0.1:{self.port}{_KEY_PATH}"

    def _entry(self) -> dict[str, Any]:
        return {
            "namespace": "transcript",
            "key": _KEY_PATH.rsplit("/", 1)[-1],
            "value": list(self.items),
            "version": self.version,
            "updated_at": "2026-01-01T00:00:00Z",
        }

    def _store(self, item: dict[str, Any]) -> None:
        record_id = item.get("record_id")
        if isinstance(record_id, str) and record_id:
            if any(stored.get("record_id") == record_id for stored in self.items):
                return
        self.items.append(item)
        self.version += 1

    def _app(self) -> web.Application:
        app = web.Application()

        async def get_key(_request: web.Request) -> web.Response:
            if self.mode in (_REFUSE_500, _COMMIT_THEN_500):
                return web.json_response({"detail": "Internal Server Error"}, status=500)
            if not self.items:
                return web.json_response({"detail": "not found"}, status=404, headers=_CAP_HEADERS)
            return web.json_response(self._entry(), headers=_CAP_HEADERS)

        async def append_key(request: web.Request) -> web.StreamResponse:
            body = await request.json()
            item = body["item"]
            self.posted.append(item)
            if self.mode == _REFUSE_500:
                return web.json_response({"detail": "Internal Server Error"}, status=500)
            if self.mode == _REFUSE_400:
                return web.json_response({"detail": "bad request"}, status=400)
            self._store(item)
            if self.mode == _COMMIT_THEN_500:
                return web.json_response({"detail": "Internal Server Error"}, status=500)
            if self.mode == _COMMIT_THEN_CUT:
                # Committed; now the connection drops before a single response
                # byte. Closing the transport sends FIN with nothing buffered,
                # so the client sees ServerDisconnectedError. The response
                # below is never written: aiohttp drops or refuses writes to a
                # closing transport and treats it as a client disconnect.
                transport = request.transport
                assert transport is not None
                transport.close()
                return web.Response(status=200)
            return web.json_response(self._entry())

        app.router.add_get(_KEY_PATH, get_key)
        app.router.add_post(f"{_KEY_PATH}/append", append_key)
        return app

    async def start(self) -> None:
        assert self._runner is None
        runner = web.AppRunner(self._app())
        await runner.setup()
        await web.TCPSite(runner, "127.0.0.1", self.port, reuse_address=True).start()
        self._runner = runner

    async def stop(self) -> None:
        if self._runner is not None:
            runner, self._runner = self._runner, None
            await runner.cleanup()

    def stored_users(self) -> list[str]:
        return [item["user"] for item in self.items]

    def posted_for(self, user: str) -> list[dict[str, Any]]:
        return [item for item in self.posted if item.get("user") == user]


class _CheckpointingFake(FakeModelSession):
    """The fake model plus a native replay export that records checkpoint resets."""

    def __init__(self) -> None:
        super().__init__()
        self.full_checkpoint_requests = 0

    async def export_replay_state(self) -> HarnessReplayState:
        return HarnessReplayState(
            harness="claude",
            kind="checkpoint",
            entries=({"type": "assistant", "uuid": "native-1", "payload": "native"},),
        )

    def request_full_checkpoint(self) -> None:
        self.full_checkpoint_requests += 1


def _fast_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(session_module, "_HISTORY_RETRY_INITIAL_S", 0.01)
    monkeypatch.setattr(session_module, "_HISTORY_RETRY_MAX_S", 0.05)


def _runner(
    store: StateApiTranscriptStore, session: FakeModelSession | None = None
) -> SessionRunner:
    fake = session if session is not None else FakeModelSession()
    return SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=lambda: fake,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="history-refused-test",
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

    Read with getattr so a source without a retry for this failure fails on
    the product assertions that follow, not here. Once the task is done, no
    append is sent until another turn runs, so counts asserted after it are
    exact.
    """
    task = getattr(runner, "_history_retry_task", None)
    if task is not None:
        await asyncio.wait_for(asyncio.shield(task), timeout=timeout)


async def _booted(api: _ReplayingStateApi) -> StateApiTranscriptStore:
    """Boot reads the (empty) transcript while the API is up, as the runner does."""
    await api.start()
    store = StateApiTranscriptStore(api.key_url, token=None)
    assert await store.load() == []
    return store


def _record_ids(items: list[dict[str, Any]]) -> list[Any]:
    return [item.get("record_id") for item in items]


@pytest.mark.parametrize(
    "mode",
    [
        pytest.param(_REFUSE_500, id="500-not-written"),
        pytest.param(_COMMIT_THEN_500, id="500-after-commit"),
        pytest.param(_COMMIT_THEN_CUT, id="disconnect-after-commit"),
    ],
)
def test_an_append_with_an_unknown_outcome_lands_exactly_once_after_recovery(
    monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    _fast_retry(monkeypatch)
    api = _ReplayingStateApi()

    async def go() -> None:
        store = await _booted(api)
        runner = _runner(store)
        await runner.start()
        try:
            api.mode = mode
            assert await _turn(runner, "first question") is SessionStatus.DONE
            assert runner.history_durable is False
            assert api.posted_for("first question")

            api.mode = _HEALTHY
            await _retry_settled(runner)
            assert api.stored_users() == ["first question"]
            assert runner.history_durable is True

            # Later turns append normally after the recovered one.
            assert await _turn(runner, "second question") is SessionStatus.DONE
            await _retry_settled(runner)
            assert api.stored_users() == ["first question", "second question"]
            assert runner.history_durable is True
        finally:
            await runner.close()
            await api.stop()

    anyio.run(go)


def test_a_400_refusal_stays_a_sticky_loss(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unchanged by #5366: a request the API refused with a 4xx is never retried.

    The current contract (session.py ``_record_turn``): the refused record is
    dropped, ``history_durable`` goes false for the life of the process, and
    later turns still append. A later success does not repair the missing
    first turn, so the fence stays shut.
    """
    _fast_retry(monkeypatch)
    api = _ReplayingStateApi()

    async def go() -> None:
        store = await _booted(api)
        runner = _runner(store)
        await runner.start()
        try:
            api.mode = _REFUSE_400
            assert await _turn(runner, "first question") is SessionStatus.DONE
            assert runner.history_durable is False
            api.mode = _HEALTHY

            await _retry_settled(runner)
            # The refused request is never sent again.
            assert len(api.posted_for("first question")) == 1
            assert api.items == []
            assert runner.history_durable is False

            assert await _turn(runner, "second question") is SessionStatus.DONE
            await _retry_settled(runner)
            assert len(api.posted_for("first question")) == 1
            assert api.stored_users() == ["second question"]
            assert runner.history_durable is False
        finally:
            await runner.close()
            await api.stop()

    anyio.run(go)


def test_a_turn_keeps_one_record_id_across_attempts_and_turns_differ(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fast_retry(monkeypatch)
    api = _ReplayingStateApi()

    async def go() -> None:
        store = await _booted(api)
        runner = _runner(store)
        await runner.start()
        try:
            api.mode = _REFUSE_500
            assert await _turn(runner, "first question") is SessionStatus.DONE
            api.mode = _HEALTHY
            await _retry_settled(runner)
            assert await _turn(runner, "second question") is SessionStatus.DONE
            await _retry_settled(runner)
        finally:
            await runner.close()
            await api.stop()

    anyio.run(go)

    first_attempts = api.posted_for("first question")
    # The refused attempt plus at least the retry that landed.
    assert len(first_attempts) >= 2, first_attempts
    first_ids = set(_record_ids(first_attempts))
    assert len(first_ids) == 1, first_ids
    (first_id,) = first_ids
    assert isinstance(first_id, str) and first_id

    second_ids = set(_record_ids(api.posted_for("second question")))
    assert len(second_ids) == 1, second_ids
    (second_id,) = second_ids
    assert isinstance(second_id, str) and second_id
    assert second_id != first_id

    # The id round-trips through the transcript record's own codec.
    stored = api.items[0]
    record = TurnRecord.from_dict(stored)
    assert record.record_id == first_id
    assert TurnRecord.from_dict(record.to_dict()).record_id == first_id
    assert record.to_dict()["record_id"] == first_id
    # A record without an id keeps the item shape it had before #5366.
    assert "record_id" not in TurnRecord(user="u", assistant="a").to_dict()


def test_a_retried_record_with_native_replay_requests_a_full_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retried append may have landed earlier through capacity compaction,
    which drops native replay, so the runner cannot know the checkpoint was
    kept. It asks the session for a full checkpoint on its next export.
    """
    _fast_retry(monkeypatch)
    api = _ReplayingStateApi()
    fake = _CheckpointingFake()

    async def go() -> None:
        store = await _booted(api)
        # The API goes away: the turn's own append cannot connect.
        await api.stop()
        runner = _runner(store, fake)
        await runner.start()
        try:
            assert await _turn(runner, "first question") is SessionStatus.DONE
            assert runner.history_durable is False
            assert fake.full_checkpoint_requests == 0

            await api.start()
            await _retry_settled(runner)
            assert api.stored_users() == ["first question"]
            assert api.items[0]["harness_replay"] is not None
            assert runner.history_durable is True
            assert fake.full_checkpoint_requests == 1
        finally:
            await runner.close()
            await api.stop()

    anyio.run(go)


class _LateCommitCompactionApi:
    """A transcript key whose appends are refused at the reserve, for compaction.

    ``POST <key>/append`` always answers 413 with the reserve refusal detail of
    ``enforce_thread_cap`` in apps/api/src/curie_api/transcripts.py, the
    store's compaction trigger. ``late_commit``, when set, is stored right
    after that refusal: a cancelled earlier attempt's compaction PUT that
    commits between the retry's 413 and its compaction read. GET answers the
    stored value with the cap header, and PUT is the API's compare-and-set
    write (409 on a stale ``expected_version``); every PUT body is recorded.
    """

    def __init__(self, seed: list[dict[str, Any]]) -> None:
        self.value: list[dict[str, Any]] = list(seed)
        self.version = 1
        self.late_commit: dict[str, Any] | None = None
        self.puts: list[dict[str, Any]] = []
        self._runner: web.AppRunner | None = None
        self.url = ""

    def _entry(self) -> dict[str, Any]:
        return {
            "namespace": "transcript",
            "key": _KEY_PATH.rsplit("/", 1)[-1],
            "value": list(self.value),
            "version": self.version,
            "updated_at": "2026-01-01T00:00:00Z",
        }

    def _app(self) -> web.Application:
        app = web.Application()

        async def get_key(_request: web.Request) -> web.Response:
            return web.json_response(self._entry(), headers=_CAP_HEADERS)

        async def append_key(request: web.Request) -> web.Response:
            body = await request.json()
            assert body["reserve_bytes"] > 0
            if self.late_commit is not None:
                self.value.append(self.late_commit)
                self.version += 1
                self.late_commit = None
            return web.json_response(
                {"detail": "value leaves under the reserve of the per-thread transcript cap"},
                status=413,
            )

        async def put_key(request: web.Request) -> web.Response:
            body = await request.json()
            self.puts.append(body)
            if body.get("expected_version") != self.version:
                return web.json_response({"detail": "version mismatch"}, status=409)
            self.value = list(body["value"])
            self.version += 1
            return web.json_response(self._entry())

        app.router.add_get(_KEY_PATH, get_key)
        app.router.add_post(f"{_KEY_PATH}/append", append_key)
        app.router.add_put(_KEY_PATH, put_key)
        return app

    async def start(self) -> None:
        runner = web.AppRunner(self._app())
        await runner.setup()
        port = _free_port()
        await web.TCPSite(runner, "127.0.0.1", port, reuse_address=True).start()
        self._runner = runner
        self.url = f"http://127.0.0.1:{port}{_KEY_PATH}"

    async def stop(self) -> None:
        if self._runner is not None:
            runner, self._runner = self._runner, None
            await runner.cleanup()


_RECORD_ID = "rid-5366-PLACEHOLDER"


def _prior_turn() -> dict[str, Any]:
    return TurnRecord(
        user="earlier question", assistant="earlier answer", ts="2026-01-01T00:00:00Z"
    ).to_dict()


def _retried_turn() -> TurnRecord:
    return TurnRecord(
        user="late question",
        assistant="late answer",
        ts="2026-01-01T00:01:00Z",
        record_id=_RECORD_ID,
    )


def _with_record_id(value: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [item for item in value if item.get("record_id") == _RECORD_ID]


def test_a_retry_whose_compaction_finds_its_record_already_stored_writes_nothing() -> None:
    """Review F1: a late compaction commit must not store the retried turn twice."""
    record = _retried_turn()
    api = _LateCommitCompactionApi([_prior_turn()])
    # The cancelled earlier attempt's compaction lands after the retry's 413.
    api.late_commit = record.to_dict()

    async def go() -> bool:
        await api.start()
        try:
            store = StateApiTranscriptStore(api.url, token=None)
            return await store.append(record)
        finally:
            await api.stop()

    retained = anyio.run(go)

    assert api.puts == []
    assert len(_with_record_id(api.value)) == 1
    # Whether native replay survived the earlier write is unknown.
    assert retained is False


def test_a_retry_whose_compaction_lacks_its_record_still_compacts_once() -> None:
    """The liveness twin: without the record stored, compaction writes it once."""
    record = _retried_turn()
    api = _LateCommitCompactionApi([_prior_turn()])

    async def go() -> bool:
        await api.start()
        try:
            store = StateApiTranscriptStore(api.url, token=None)
            return await store.append(record)
        finally:
            await api.stop()

    retained = anyio.run(go)

    assert len(api.puts) == 1
    assert len(_with_record_id(api.puts[0]["value"])) == 1
    assert len(_with_record_id(api.value)) == 1
    assert retained is False
